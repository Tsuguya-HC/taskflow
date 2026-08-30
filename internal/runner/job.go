/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package runner turns a task, a flow and a handler into the Job that runs one
// phase.
//
// Like transition and taskstate it is a pure function over values, so what the
// controller is about to create can be examined without a cluster.
//
// The handler's jobTemplate is carried through as written. Labels a policy
// selects on, service accounts, images, commands — none of that is decided
// here: what a pod must carry to run in a given cluster is that cluster's
// business, and this package has no opinion about which policy engine is
// listening. What it adds is the little that execution itself requires:
// somewhere to hang ownership, a name that repeats, the deadline, and enough
// context for the handler's own containers to know which task and phase they
// are serving.
package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
)

// LabelTaskUID, the annotation names and the FLOW_* environment variable
// names are re-exported from internal/contract, which is the canonical
// place they are documented — that package is the one a Pod-side binary
// like cmd/sidecar can depend on without pulling in this one's client-go
// dependency. They stay defined here too so the controller's own callers
// and tests keep reading runner.X.
const (
	LabelTaskUID = contract.LabelTaskUID

	AnnotationPhase     = contract.AnnotationPhase
	AnnotationRunID     = contract.AnnotationRunID
	AnnotationPrevRunID = contract.AnnotationPrevRunID

	EnvTaskUID     = contract.EnvTaskUID
	EnvPhase       = contract.EnvPhase
	EnvInput       = contract.EnvInput
	EnvDirectories = contract.EnvDirectories

	annotationPrefix = contract.AnnotationPrefix

	// maxNameLength is the limit Kubernetes puts on an object name.
	maxNameLength = 63
	// phaseHashLength is how much of the phase digest goes into the name.
	phaseHashLength = 8
	// taskHashLength is how much of the task name's digest survives a
	// truncation, so the part that gets cut is not the only thing telling
	// two task names apart.
	taskHashLength = 8
)

// Input is everything the Job is built from.
type Input struct {
	Task    *flowv1alpha1.Task
	Handler *flowv1alpha1.TaskHandler
	// Phase being run. Taken from the flow's binding rather than from the
	// handler, so a handler bound to the wrong phase fails validation rather
	// than quietly running under its own name.
	Phase flowv1alpha1.Phase
	// RunID of this attempt.
	RunID int32
	// PrevRunID is 0 when this is the first attempt.
	PrevRunID int32
	// Directories the flow declares for this phase — the only answers the
	// run can give. Order is not significant; they are sorted on the way
	// into the environment so the same declaration always renders the same.
	Directories []string
}

// ErrReservedField reports a jobTemplate that sets something the design keeps
// for itself.
var ErrReservedField = errors.New("jobTemplate sets a reserved field")

// JobName is the name the Job for this attempt will always have. It is
// deterministic so that creating it twice is a conflict rather than a second
// Job, which is what makes a controller restart harmless.
//
// The phase is hashed rather than spelled: status names are free strings and
// 調査 is not a legal object name. Whoever wants to read it looks at the
// annotation.
//
// When the task name has to be cut to fit the limit, the truncation drops a
// hash of the full name in alongside it rather than just chopping the tail.
// Object names commonly carry their distinguishing part at the end — a
// generateName suffix, for instance — and two task names that agree up to
// the cut would otherwise collide on the exact same Job name.
func JobName(taskName string, phase flowv1alpha1.Phase, runID int32) string {
	phaseSum := sha256.Sum256([]byte(phase))
	suffix := fmt.Sprintf("-%d-%s", runID, hex.EncodeToString(phaseSum[:])[:phaseHashLength])
	prefix := taskName
	if len(prefix)+len(suffix) > maxNameLength {
		taskSum := sha256.Sum256([]byte(taskName))
		taskHash := hex.EncodeToString(taskSum[:])[:taskHashLength]
		budget := maxNameLength - len(suffix) - len(taskHash) - 1 // -1 for the separator before the hash
		budget = min(max(budget, 0), len(taskName))
		prefix = taskName[:budget] + "-" + taskHash
	}
	return prefix + suffix
}

// BuildJob returns the Job that runs one phase of one task.
func BuildJob(in Input) (*batchv1.Job, error) {
	if in.Task == nil || in.Handler == nil {
		return nil, errors.New("runner: task and handler are both required")
	}
	if in.Handler.Spec.Phase != in.Phase {
		return nil, fmt.Errorf("runner: handler %q fills phase %q, not %q",
			in.Handler.Name, in.Handler.Spec.Phase, in.Phase)
	}
	if in.Handler.Spec.JobTemplate == nil {
		return nil, fmt.Errorf("runner: handler %q has no jobTemplate", in.Handler.Name)
	}
	if err := checkReserved(in.Handler.Spec.JobTemplate); err != nil {
		return nil, err
	}

	// A deep copy: the handler is a cached object shared with everything else
	// reading it, and the caller would not expect building a Job to edit it.
	tpl := in.Handler.Spec.JobTemplate.Template.DeepCopy()

	// The framework's annotations go on the pod as well as the Job: a
	// container reads them through the downward API, and that reads the pod
	// it is in, never the Job above it. The template's own annotations are
	// kept, but one under the framework's prefix is refused rather than
	// overwritten — the alternative is a value that silently differs from
	// what the YAML says.
	podAnnotations, err := withFrameworkAnnotations(tpl.Metadata.Annotations, annotations(in))
	if err != nil {
		return nil, err
	}

	spec := batchv1.JobSpec{
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      tpl.Metadata.Labels,
				Annotations: podAnnotations,
			},
			Spec: tpl.Spec,
		},
		// Job-internal retry is off. An attempt that failed for reasons
		// outside the handler's judgement is re-run by the controller under a
		// new runID, so it gets fresh directories rather than reading what the
		// last one left.
		BackoffLimit: ptr(int32(0)),
	}
	if in.Handler.Spec.Timeout != nil {
		spec.ActiveDeadlineSeconds = ptr(int64(in.Handler.Spec.Timeout.Seconds()))
	}
	injectEnv(&spec.Template.Spec, in)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        JobName(in.Task.Name, in.Phase, in.RunID),
			Namespace:   in.Task.Namespace,
			Labels:      map[string]string{LabelTaskUID: string(in.Task.UID)},
			Annotations: annotations(in),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         flowv1alpha1.SchemeGroupVersion.String(),
				Kind:               "Task",
				Name:               in.Task.Name,
				UID:                in.Task.UID,
				Controller:         ptr(true),
				BlockOwnerDeletion: ptr(true),
			}},
		},
		Spec: spec,
	}, nil
}

// OwnedByTask reports whether job is controlled by the task with this UID,
// rather than merely bearing the deterministic name that task would have
// picked. It is the read side of the ownerReference BuildJob writes: the name
// is deterministic, not exclusive, so whoever finds a Job under it has to ask
// this before trusting what's there.
func OwnedByTask(job *batchv1.Job, taskUID types.UID) bool {
	for _, ref := range job.OwnerReferences {
		if ref.Controller != nil && *ref.Controller && ref.UID == taskUID {
			return true
		}
	}
	return false
}

// withFrameworkAnnotations lays the framework's annotations over a template's,
// refusing any the template already claims under the framework's prefix.
func withFrameworkAnnotations(template, framework map[string]string) (map[string]string, error) {
	for k := range template {
		if strings.HasPrefix(k, annotationPrefix) {
			return nil, fmt.Errorf("%w: annotation %q is the framework's to set", ErrReservedField, k)
		}
	}
	out := maps.Clone(template)
	if out == nil {
		out = make(map[string]string, len(framework))
	}
	maps.Copy(out, framework)
	return out, nil
}

func annotations(in Input) map[string]string {
	a := map[string]string{
		AnnotationPhase: string(in.Phase),
		AnnotationRunID: strconv.Itoa(int(in.RunID)),
	}
	if in.PrevRunID > 0 {
		a[AnnotationPrevRunID] = strconv.Itoa(int(in.PrevRunID))
	}
	return a
}

// checkReserved refuses a template that sets what the controller relies on.
//
// One row, where the design's table has six. The other five — backoffLimit,
// ttlSecondsAfterFinished, activeDeadlineSeconds, completions, parallelism —
// are no longer fields of JobTemplate at all, so there is nothing to check.
// restartPolicy survives because it belongs to the pod, and a pod that
// restarts in place would find the previous attempt's directories.
func checkReserved(t *flowv1alpha1.JobTemplate) error {
	if t.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		return fmt.Errorf("%w: restartPolicy must be Never — a restarted pod would find the last attempt's directories", ErrReservedField)
	}
	return nil
}

// injectEnv adds the task's identity to every container. Existing entries are
// left alone: a handler that already sets FLOW_PHASE means it, and silently
// overwriting would leave the YAML disagreeing with what ran.
//
// FLOW_PHASE and FLOW_INPUT carry free strings their authors control — the
// flow's phase name and the task's spec.input — so both have $(...) escaped.
// Kubernetes expands $(VAR_NAME) in an env value against everything resolved
// before it: all of the container's envFrom, then env entries earlier in the
// list. The injected vars also go in front of the handler's env, which closes
// the env-list route on its own — but a secret pulled in via envFrom is
// resolvable from the very first env entry, so against that route ordering
// does nothing and the escaping is the defense that actually holds.
func injectEnv(pod *corev1.PodSpec, in Input) {
	env := []corev1.EnvVar{
		{Name: EnvTaskUID, Value: string(in.Task.UID)},
		{Name: EnvPhase, Value: escapeVarRefs(string(in.Phase))},
		{Name: EnvDirectories, Value: escapeVarRefs(directoriesJSON(in.Directories))},
	}
	if in.Task.Spec.Input != nil {
		env = append(env, corev1.EnvVar{Name: EnvInput, Value: escapeVarRefs(string(in.Task.Spec.Input.Raw))})
	}
	for i := range pod.InitContainers {
		pod.InitContainers[i].Env = merge(pod.InitContainers[i].Env, env)
	}
	for i := range pod.Containers {
		pod.Containers[i].Env = merge(pod.Containers[i].Env, env)
	}
}

// directoriesJSON renders the declared directories as a JSON array, sorted so
// the value is a function of the declaration and not of map iteration order.
// JSON rather than a delimiter because the names are free strings: nothing
// stops a flow declaring a directory with a comma in it, and a format that
// can carry any name beats one that has to forbid some.
func directoriesJSON(dirs []string) string {
	sorted := slices.Clone(dirs)
	slices.Sort(sorted)
	if sorted == nil {
		sorted = []string{}
	}
	b, err := json.Marshal(sorted)
	if err != nil {
		// A []string cannot fail to marshal.
		panic(err)
	}
	return string(b)
}

// escapeVarRefs turns $(...) into $$(...) so Kubernetes' env-var expansion
// leaves it as a literal $(...) instead of trying to resolve it against
// another variable in the same container.
func escapeVarRefs(s string) string {
	return strings.ReplaceAll(s, "$(", "$$(")
}

// merge puts add in front of existing, dropping anything add already has a
// same-named entry for — a handler's own value wins, both because it is left
// untouched and because it stays after what injectEnv adds.
func merge(existing, add []corev1.EnvVar) []corev1.EnvVar {
	have := make(map[string]bool, len(existing))
	for _, e := range existing {
		have[e.Name] = true
	}
	var prepend []corev1.EnvVar
	for _, e := range add {
		if !have[e.Name] {
			prepend = append(prepend, e)
		}
	}
	if len(prepend) == 0 {
		return existing
	}
	return append(prepend, existing...)
}

func ptr[T any](v T) *T { return &v }

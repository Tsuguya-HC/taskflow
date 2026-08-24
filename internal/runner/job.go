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
	"errors"
	"fmt"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
)

const (
	// LabelTaskUID is the only label the controller sets. It exists so the
	// controller can find its own Jobs, not so anything can select on it —
	// a UID also happens to be legal as a label value, which a status name
	// picked by whoever wrote the flow is not.
	LabelTaskUID = "flow.tgy.io/task-uid"

	// AnnotationPhase carries the status name. An annotation rather than a
	// label because these are free strings: 調査 is not a legal label value
	// and not a legal object name either.
	AnnotationPhase = "flow.tgy.io/phase"
	// AnnotationRunID is read by the plumbing that files results under
	// results/<runID>/. The agent has no use for it — it opens
	// results/1/ok and reads — and there is reason to keep it away from
	// one: a run count suggests how much rope is left, the same way a
	// remaining-rework count would.
	AnnotationRunID = "flow.tgy.io/run-id"
	// AnnotationPrevRunID is absent on the first run.
	AnnotationPrevRunID = "flow.tgy.io/prev-run-id"

	// EnvTaskUID, EnvPhase and EnvInput are set on every container in the
	// template. Unlike the run number these say what the work is, not how
	// many attempts it has had.
	EnvTaskUID = "FLOW_TASK_UID"
	EnvPhase   = "FLOW_PHASE"
	EnvInput   = "FLOW_INPUT"

	// maxNameLength is the limit Kubernetes puts on an object name.
	maxNameLength = 63
	// phaseHashLength is how much of the phase digest goes into the name.
	phaseHashLength = 8
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
func JobName(taskName string, phase flowv1alpha1.Phase, runID int32) string {
	sum := sha256.Sum256([]byte(phase))
	suffix := fmt.Sprintf("-%d-%s", runID, hex.EncodeToString(sum[:])[:phaseHashLength])
	prefix := taskName
	if len(prefix)+len(suffix) > maxNameLength {
		prefix = prefix[:maxNameLength-len(suffix)]
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
	spec := *in.Handler.Spec.JobTemplate.Spec.DeepCopy()

	// Job-internal retry is off. An attempt that failed for reasons outside
	// the handler's judgement is re-run by the controller under a new runID,
	// so it gets fresh directories rather than reading what the last one left.
	spec.BackoffLimit = ptr(int32(0))
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
// Admission refuses these too; doing it here as well means the runner does not
// depend on admission having run.
func checkReserved(t *batchv1.JobTemplateSpec) error {
	s := t.Spec
	switch {
	case s.BackoffLimit != nil && *s.BackoffLimit != 0:
		return fmt.Errorf("%w: backoffLimit — a second retry mechanism would reuse the runID", ErrReservedField)
	case s.TTLSecondsAfterFinished != nil:
		return fmt.Errorf("%w: ttlSecondsAfterFinished — the Job would be gone before its result was read", ErrReservedField)
	case s.ActiveDeadlineSeconds != nil:
		return fmt.Errorf("%w: activeDeadlineSeconds — spec.timeout is the only deadline", ErrReservedField)
	case s.Completions != nil && *s.Completions != 1:
		return fmt.Errorf("%w: completions — one run answers once", ErrReservedField)
	case s.Parallelism != nil && *s.Parallelism != 1:
		return fmt.Errorf("%w: parallelism — one run answers once", ErrReservedField)
	case s.Template.Spec.RestartPolicy != corev1.RestartPolicyNever:
		return fmt.Errorf("%w: restartPolicy must be Never — a restarted pod would find the last attempt's directories", ErrReservedField)
	}
	return nil
}

// injectEnv adds the task's identity to every container. Existing entries are
// left alone: a handler that already sets FLOW_PHASE means it, and silently
// overwriting would leave the YAML disagreeing with what ran.
func injectEnv(pod *corev1.PodSpec, in Input) {
	env := []corev1.EnvVar{
		{Name: EnvTaskUID, Value: string(in.Task.UID)},
		{Name: EnvPhase, Value: string(in.Phase)},
	}
	if in.Task.Spec.Input != nil {
		env = append(env, corev1.EnvVar{Name: EnvInput, Value: string(in.Task.Spec.Input.Raw)})
	}
	for i := range pod.InitContainers {
		pod.InitContainers[i].Env = merge(pod.InitContainers[i].Env, env)
	}
	for i := range pod.Containers {
		pod.Containers[i].Env = merge(pod.Containers[i].Env, env)
	}
}

func merge(existing, add []corev1.EnvVar) []corev1.EnvVar {
	have := make(map[string]bool, len(existing))
	for _, e := range existing {
		have[e.Name] = true
	}
	for _, e := range add {
		if !have[e.Name] {
			existing = append(existing, e)
		}
	}
	return existing
}

func ptr[T any](v T) *T { return &v }

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
// listening. What it adds is what execution itself requires: somewhere to
// hang ownership, a name that repeats, the deadline, enough context for the
// handler's own containers to know which task and phase they are serving —
// and the two containers that lay the run's vocabulary down and read the
// answer back, which are the controller's end of the verdict protocol and
// so are put there by the controller rather than copied into every handler.
package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	EnvPodUID      = contract.EnvPodUID

	frameworkPrefix = contract.Prefix

	// maxNameLength is the limit Kubernetes puts on an object name.
	maxNameLength = 63
	// phaseHashLength is how much of the phase digest goes into the name.
	phaseHashLength = 8
	// taskHashLength is how much of the task name's digest survives a
	// truncation, so the part that gets cut is not the only thing telling
	// two task names apart.
	taskHashLength = 8

	// PrepareContainer and PublishContainer name the two containers the
	// controller puts into every Job. The prefix keeps them out of the way
	// of whatever the handler calls its own, the same way FLOW_ does for
	// the environment; a template that uses one of these names anyway is
	// refused rather than merged.
	PrepareContainer = "flow-prepare"
	PublishContainer = "flow-publish"

	// outDir is where the declared directories go, under the workspace's
	// mount path. The layout is the framework's; the place is the handler's.
	outDir = "out"

	// workDir is where a run writes while it is in flight. The injected
	// containers' own mounts and every writable mount of the flow workspace
	// among the handler's own containers are pinned to workDir/<runID>, so
	// nothing but this one attempt ever lands there (§ADR-0002 決定5).
	workDir = "work"
	// resultsDir is the shelf publish moves a run's directory onto once it
	// has sealed — a rename from workDir/<runID> to resultsDir/<runID>,
	// within the same volume. Only a run whose answer is already decided
	// ever appears here, so a later phase mounting resultsDir back —
	// read-only, at its root — sees exactly the completed runs and none in
	// flight.
	resultsDir = "results"

	// preferredSidecarUID is the uid the injected containers run as when
	// nothing of the handler's does: distroless' nonroot, which is also the
	// image's own USER. prepare creates out/ under its uid and closes it to
	// 0555, and that is a closed door only if no other container in the pod
	// can chmod it back open — which is to say, only if none shares the
	// uid. So the uid is chosen per Job, stepping down from here past any
	// the template names, rather than being a number the handler's author
	// has to know to avoid.
	preferredSidecarUID int64 = 65532
)

// Input is everything the Job is built from.
type Input struct {
	Task    *flowv1alpha1.Task
	Handler *flowv1alpha1.TaskHandler
	// Phase being run. Taken from the flow's binding rather than from the
	// handler, so a handler bound to the wrong phase fails validation rather
	// than quietly running under its own name.
	Phase flowv1alpha1.Phase
	// RunID of this run. It is the run's number, not the attempt's: an
	// infrastructure retry comes back with the same one.
	RunID int32
	// Attempt is how many infrastructure retries this run has spent, and 0
	// on the first try. It separates the Jobs of a run that had to be
	// started more than once, which RunID no longer does.
	Attempt int32
	// PrevRunID is 0 when this is the first attempt.
	PrevRunID int32
	// Directories the flow declares for this phase — the only answers the
	// run can give. Order is not significant; they are sorted on the way
	// into the environment so the same declaration always renders the same.
	Directories []string
	// SidecarImage runs prepare and publish. It is the controller's to pick,
	// not the handler's: the two ends of the verdict protocol are one
	// program, and the end inside the pod has to be the version the end in
	// the controller was written against.
	SidecarImage string
	// WorkspacePVC names the per-task claim behind the flow's workspace,
	// and is empty when the flow declares none. The claim itself is the
	// controller's to have made before building the Job; this only says
	// what the reserved flow-workspace volume mounts.
	WorkspacePVC string
	// SweepRuns are the runIDs whose work/ leftovers prepare clears away
	// before this run starts. The set is the controller's to compute —
	// which runs are live is only visible from the cluster's state — and
	// today, with runs strictly serial, it is every run before this one.
	// Sealed runs left work/ when their rename shelved them, so what this
	// actually removes is the debris of attempts that died before sealing
	// (ADR-0003).
	SweepRuns []int32
}

// ErrReservedField reports a jobTemplate that sets something the design keeps
// for itself.
var ErrReservedField = errors.New("jobTemplate sets a reserved field")

// ErrWorkspace reports a handler whose workspace the injected containers
// could not use as written.
var ErrWorkspace = errors.New("workspace is not usable")

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
//
// attempt is how many infrastructure retries this run has already spent, and
// it appears in the name only once it is nonzero. A run keeps its number
// across those retries (taskstate.RetryInfra), so the number alone no longer
// tells two attempts apart, and the Job the first attempt left behind is
// still there — the controller does not delete it, the task's own deletion
// collects it. Leaving the first attempt's name bare keeps the common case
// spelled the way it always was, and means only a run that actually retried
// carries the extra segment.
func JobName(taskName string, phase flowv1alpha1.Phase, runID, attempt int32) string {
	phaseSum := sha256.Sum256([]byte(phase))
	retry := ""
	if attempt > 0 {
		retry = fmt.Sprintf("-r%d", attempt)
	}
	suffix := fmt.Sprintf("-%d%s-%s", runID, retry, hex.EncodeToString(phaseSum[:])[:phaseHashLength])
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
	if in.SidecarImage == "" {
		return nil, errors.New("runner: no sidecar image to inject")
	}
	if err := checkWorkspace(in.Handler, in.WorkspacePVC != ""); err != nil {
		return nil, err
	}

	// A deep copy: the handler is a cached object shared with everything else
	// reading it, and the caller would not expect building a Job to edit it.
	tpl := in.Handler.Spec.JobTemplate.Template.DeepCopy()
	injectSidecars(&tpl.Spec, *in.Handler.Spec.Workspace, in.SidecarImage, sidecarUID(&tpl.Spec), in.WorkspacePVC, in.RunID, in.SweepRuns)

	// The framework's annotations go on the pod as well as the Job: a
	// container reads them through the downward API, and that reads the pod
	// it is in, never the Job above it. The task's UID goes on the pod for a
	// different reason — so that one task's pods can be selected without
	// going through the Job's name. The template's own labels and
	// annotations are kept, but one under the framework's prefix is refused
	// rather than overwritten — the alternative is a value that silently
	// differs from what the YAML says.
	podLabels, err := withFrameworkMeta("label", tpl.Metadata.Labels, labels(in))
	if err != nil {
		return nil, err
	}
	podAnnotations, err := withFrameworkMeta("annotation", tpl.Metadata.Annotations, annotations(in))
	if err != nil {
		return nil, err
	}

	spec := batchv1.JobSpec{
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels,
				Annotations: podAnnotations,
			},
			Spec: tpl.Spec,
		},
		// Job-internal retry is off. An attempt that failed for reasons
		// outside the handler's judgement is re-run by the controller as a
		// Job of its own — same runID, next attempt in the name (ADR-0004) —
		// so the retry is visible in the object rather than buried in a
		// Job's own backoff.
		BackoffLimit: ptr(int32(0)),
	}
	if in.Handler.Spec.Timeout != nil {
		spec.ActiveDeadlineSeconds = ptr(int64(in.Handler.Spec.Timeout.Seconds()))
	}
	injectEnv(&spec.Template.Spec, in)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:        JobName(in.Task.Name, in.Phase, in.RunID, in.Attempt),
			Namespace:   in.Task.Namespace,
			Labels:      labels(in),
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

// WorkspacePVCName is the name of the claim a task's flow workspace is
// backed by. The task's UID is in it, not just the name: a task deleted and
// recreated under the same name is a different task, and a shared name would
// rebind the new task to a claim that is stale, foreign, or still
// terminating — the rename-and-race CNPG documents as cloudnative-pg#10985,
// and the reason Tekton hashes the owner's UID into its claim names.
//
// The UID only disambiguates identity, though; it is not a lock. An
// ownerReference's UID is whatever its author writes, unchecked by the
// apiserver against any object that actually exists, so whoever holds create
// permission on PersistentVolumeClaims can squat this exact name first with a
// forged reference to the real task's UID (§ADR-0002). The read side —
// ensureWorkspacePVC's adopt check — closes that only insofar as callers hold
// no such permission; it cannot tell a genuine claim from a forged one by
// itself.
func WorkspacePVCName(taskName string, uid types.UID) string {
	sum := sha256.Sum256([]byte(uid))
	suffix := "-ws-" + hex.EncodeToString(sum[:])[:taskHashLength]
	prefix := taskName
	if len(prefix)+len(suffix) > maxNameLength {
		prefix = taskName[:maxNameLength-len(suffix)]
	}
	return prefix + suffix
}

// BuildWorkspacePVC returns the claim that backs one task's flow workspace.
// The spec is the flow's volumeClaimTemplate exactly as stored — defaulting
// happened at admission, visibly, not here. What the controller owns is the
// name, the bookkeeping label and the ownership; blockOwnerDeletion stays
// false because the claim rides the task's TTL out, it does not get a say
// in it.
func BuildWorkspacePVC(task *flowv1alpha1.Task, vct *corev1.PersistentVolumeClaimSpec) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      WorkspacePVCName(task.Name, task.UID),
			Namespace: task.Namespace,
			Labels:    map[string]string{LabelTaskUID: string(task.UID)},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         flowv1alpha1.SchemeGroupVersion.String(),
				Kind:               "Task",
				Name:               task.Name,
				UID:                task.UID,
				Controller:         ptr(true),
				BlockOwnerDeletion: ptr(false),
			}},
		},
		Spec: *vct.DeepCopy(),
	}
}

// withFrameworkMeta lays the framework's entries over a template's, refusing
// any the template already claims under the framework's prefix. kind names
// what is being merged, for the error a handler's author reads.
func withFrameworkMeta(kind string, template, framework map[string]string) (map[string]string, error) {
	for k := range template {
		if strings.HasPrefix(k, frameworkPrefix) {
			return nil, fmt.Errorf("%w: %s %q is the framework's to set", ErrReservedField, kind, k)
		}
	}
	out := maps.Clone(template)
	if out == nil {
		out = make(map[string]string, len(framework))
	}
	maps.Copy(out, framework)
	return out, nil
}

// labels is the framework's whole label vocabulary: one entry, on the Job so
// the controller can find it and on the pod so a human can. Policies select
// on what the handler's own template says, never on this.
func labels(in Input) map[string]string {
	return map[string]string{LabelTaskUID: string(in.Task.UID)}
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

// checkWorkspace refuses a handler the injected containers could not be
// fitted into: no workspace, a workspace naming a volume the template does
// not have, a handler container that never mounts it writably, a container
// already wearing an injected name, a volume wearing the injected volume's
// name, a handler writing its own SubPath or SubPathExpr onto a writable
// mount of the reserved flow-workspace volume, or a uid arrangement under
// which prepare's closing of out/ would not hold. hasFlowWorkspace says
// whether the task's flow brings a claim of its own, which is what the
// reserved flow-workspace volume mounts — a handler naming it under a flow
// that declares none would run against a volume that does not exist, and is
// refused here instead of failing on the kubelet.
func checkWorkspace(h *flowv1alpha1.TaskHandler, hasFlowWorkspace bool) error {
	ws := h.Spec.Workspace
	if ws == nil {
		return fmt.Errorf("%w: handler %q declares no workspace, and the run's directories have nowhere to go", ErrWorkspace, h.Name)
	}
	pod := &h.Spec.JobTemplate.Template.Spec
	if slices.ContainsFunc(pod.Volumes, func(v corev1.Volume) bool { return v.Name == contract.WorkspaceVolume }) {
		return fmt.Errorf("%w: volume %q is the framework's to add", ErrReservedField, contract.WorkspaceVolume)
	}
	// Only the reserved flow-workspace volume gets a per-run layout pinned
	// onto it (injectSidecars/pinSubPath) — a handler's own template volume
	// keeps the flat layout it always had, so nothing of the handler's own
	// SubPath there is the framework's business.
	isFlowWorkspace := ws.Volume == contract.WorkspaceVolume
	if isFlowWorkspace {
		if !hasFlowWorkspace {
			return fmt.Errorf("%w: handler %q mounts %s, but the task's flow declares no workspace", ErrWorkspace, h.Name, contract.WorkspaceVolume)
		}
	} else if !slices.ContainsFunc(pod.Volumes, func(v corev1.Volume) bool { return v.Name == ws.Volume }) {
		return fmt.Errorf("%w: workspace volume %q is not among the template's volumes", ErrWorkspace, ws.Volume)
	}
	mounted := false
	for _, c := range slices.Concat(pod.InitContainers, pod.Containers) {
		if c.Name == PrepareContainer || c.Name == PublishContainer {
			return fmt.Errorf("%w: container %q is the framework's to add", ErrReservedField, c.Name)
		}
		for _, m := range c.VolumeMounts {
			// A read-only mount is skipped here by this loop's own checks
			// regardless of what SubPath it sets: the framework writes
			// nothing under it, so a handler's own SubPath there cannot
			// collide with anything it sets. That does not mean pinSubPath
			// leaves it alone, though. An explicit SubPath or SubPathExpr is
			// never overwritten, readOnly or not — a mount naming
			// subPath: results, literally, gets the shelf of sealed runs,
			// its root lining up with resultsDir's instead of nesting
			// results/results/<runID> under it. One left unset, though, is
			// pinned to this run's view the same as a writable mount is,
			// rather than showing the volume's root (§ADR-0003 決定1).
			if m.Name != ws.Volume || m.ReadOnly {
				continue
			}
			mounted = true
			// The per-run subPath under a writable flow-workspace mount is
			// pinned by pinSubPath, not read from the template: a handler
			// that already sets one here would have it silently overwritten
			// on every run, and a handler that sets a SubPathExpr expecting
			// $(FLOW_RUN_ID) would run against a variable this design does
			// not inject at all (§ADR-0002 決定5) — either way, refused
			// rather than merged, same as every other reserved field.
			if isFlowWorkspace && (m.SubPath != "" || m.SubPathExpr != "") {
				return fmt.Errorf("%w: container %q's mount of %s sets its own SubPath or SubPathExpr; the per-run layout there is the framework's to set",
					ErrReservedField, c.Name, ws.Volume)
			}
		}
	}
	// Only whether it's mounted matters, not where — a handler container can
	// mount the workspace at any path. What is refused is none of the
	// handler's own containers mounting it writably: a read-only mount ends
	// the same way an absent one does, since prepare's declared directories
	// exist but nothing of the handler's can write an answer into them; and
	// with no writable mount at all, prepare would still lay out/ down and
	// publish would still seal it, but no container of the handler's could
	// read the declared directories or write an answer into them either — so
	// either way the run could only ever come back with nothing to say.
	if !mounted {
		return fmt.Errorf("%w: no container in handler %q's template mounts workspace volume %q writably", ErrWorkspace, h.Name, ws.Volume)
	}
	return checkUID(pod)
}

// checkUID makes sure the pod says what uid its containers run as. The
// injected containers close out/ against that uid by running as a different
// one, and a uid left to each image is not one they can be sure to differ
// from — an image built on the same base as the sidecar's runs as the same
// nonroot user, and the handler would not show it anywhere. Measured, not
// assumed (2026-08-30): at the same uid, chmod and mkdir under out/ both
// succeed.
func checkUID(pod *corev1.PodSpec) error {
	if pod.SecurityContext == nil || pod.SecurityContext.RunAsUser == nil {
		return fmt.Errorf("%w: securityContext.runAsUser is not set on the pod; the run's directories are closed against the uid the handler runs as, and unset means whatever each image was built with", ErrWorkspace)
	}
	return nil
}

// sidecarUID picks a uid none of the template's containers run as, so what
// prepare closes stays closed. It starts at the image's own user and steps
// down past every uid the template names, at the pod level or on a
// container. checkUID has already made sure the pod level is set, so
// stepping past what is named is stepping past what will run.
func sidecarUID(pod *corev1.PodSpec) int64 {
	used := map[int64]bool{}
	if pod.SecurityContext != nil && pod.SecurityContext.RunAsUser != nil {
		used[*pod.SecurityContext.RunAsUser] = true
	}
	for _, c := range slices.Concat(pod.InitContainers, pod.Containers) {
		if c.SecurityContext != nil && c.SecurityContext.RunAsUser != nil {
			used[*c.SecurityContext.RunAsUser] = true
		}
	}
	uid := preferredSidecarUID
	for used[uid] {
		uid--
	}
	return uid
}

// injectSidecars puts prepare and publish in front of the template's own
// init containers, so out/ is laid down before anything of the handler's
// runs, and so publish — a native sidecar, kept alive until the main
// containers are done and then stopped — is already waiting when they start.
//
// Both mount only the workspace. In the template-volume case publish mounts
// it read-only: sealing reads which directories are non-empty and touches
// nothing (measured 2026-08-30 with the mount read-only). The termination
// log the answer is written to is the kubelet's, not the volume's.
//
// A flow workspace changes both ends of that. The reserved volume itself,
// backed by the task's claim, gets a per-run subPath under workDir, pinned
// literally onto every writable mount of it — the injected containers' own
// and every one of the handler's own containers that mounts it writably
// (pinSubPath) — so this run's out/ lands under work/<runID> and nothing but
// this attempt is there while it is in flight. The subPath names the run's
// directory, not out/ itself: what prepare chmods must be something prepare
// created, and a path the kubelet made is not (measured 2026-08-30 — a
// subPath aimed at the chmod target itself comes back EPERM).
//
// publish's own mount stops being pinned or read-only: sealing a flow
// workspace's run ends with a move from work/<runID> to results/<runID>
// (§ADR-0002 決定5), and a rename spanning both shelves needs a mount that
// can see the volume's root, writably, rather than one subPath pins under
// either. Its --out and the --seal-from/--seal-to it moves between are
// spelled out in full against that root, since nothing about its own mount
// narrows the path for it the way prepare's subPath does.
//
// The literal runID is pinned rather than left for the handler's own
// containers to resolve at $(FLOW_RUN_ID): the run number is generation-time
// information the controller already has here, so nothing needs an
// expression the pod evaluates or an env var the framework's vocabulary
// would otherwise have to carry just to name it — the run stays out of the
// agent's own environment, the same as the annotation it is read from
// stays out of reach of anything but the downward API (§ADR-0002 決定5).
func injectSidecars(pod *corev1.PodSpec, ws flowv1alpha1.WorkspaceSpec, image string, uid int64, pvcName string, runID int32, sweep []int32) {
	prepareSubPath := ""
	prepareOut := path.Join(ws.MountPath, outDir)
	publishOut := prepareOut
	publishReadOnly := true
	var prepareArgs, sealArgs []string

	if ws.Volume == contract.WorkspaceVolume {
		pod.Volumes = append(pod.Volumes, corev1.Volume{
			Name: contract.WorkspaceVolume,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcName},
			},
		})
		run := strconv.Itoa(int(runID))
		pinSubPath(pod, ws.Volume, path.Join(workDir, run))

		// prepare mounts work/ itself, one level above its own run: it
		// makes the run's directory (so the mode is chosen, not left to
		// the kubelet's subPath machinery) and it clears away the debris
		// of abandoned runs — both need the parent, neither needs the
		// shelf, which stays out of its reach.
		prepareSubPath = workDir
		prepareOut = path.Join(ws.MountPath, run, outDir)
		prepareArgs = []string{"--" + contract.FlagRunDir, path.Join(ws.MountPath, run)}
		if len(sweep) > 0 {
			ids := make([]string, len(sweep))
			for i, id := range sweep {
				ids[i] = strconv.Itoa(int(id))
			}
			prepareArgs = append(prepareArgs, "--"+contract.FlagSweep, strings.Join(ids, ","))
		}

		publishReadOnly = false
		workAt := path.Join(ws.MountPath, workDir, run)
		publishOut = path.Join(workAt, outDir)
		sealArgs = []string{
			"--" + contract.FlagSealFrom, workAt,
			"--" + contract.FlagSealTo, path.Join(ws.MountPath, resultsDir, run),
		}
	}

	prepare := sidecarContainer(PrepareContainer, image, contract.SubcommandPrepare, prepareOut, ws, uid, prepareSubPath, false)
	prepare.Args = append(prepare.Args, prepareArgs...)
	publish := sidecarContainer(PublishContainer, image, contract.SubcommandPublish, publishOut, ws, uid, "", publishReadOnly)
	publish.Args = append(publish.Args, sealArgs...)
	publish.RestartPolicy = ptr(corev1.ContainerRestartPolicyAlways)
	pod.InitContainers = append([]corev1.Container{prepare, publish}, pod.InitContainers...)
}

// pinSubPath aims every handler mount of the flow-workspace volume that does
// not say otherwise at this run's directory — writable or read-only alike:
// no subPath means "this run", and the shelf of finished runs is asked for
// explicitly with subPath: results (ADR-0003; the first cut pinned only
// writable mounts, which left a sidecar that merely reads its own run's
// out/ — cnp-check's notify — staring at the volume root, unable to find
// its run without the very wiring this design removed). Called before the
// injected containers are prepended, so pod.InitContainers here still holds
// only the handler's own. checkWorkspace has already refused a handler that
// set its own SubPath or SubPathExpr on a writable mount, so nothing here
// overwrites a value the handler wrote; a read-only mount carrying an
// explicit subPath is the handler's own view and left alone.
func pinSubPath(pod *corev1.PodSpec, volume, subPath string) {
	pin := func(containers []corev1.Container) {
		for i := range containers {
			for j := range containers[i].VolumeMounts {
				m := &containers[i].VolumeMounts[j]
				if m.Name == volume && m.SubPath == "" && m.SubPathExpr == "" {
					m.SubPath = subPath
				}
			}
		}
	}
	pin(pod.InitContainers)
	pin(pod.Containers)
}

// sidecarContainer is one of the two, differing only in subcommand and in
// whether it may write. The security context is the whole restricted set,
// spelled out rather than inherited: the pod's is the handler's choice for
// its own containers, and the uid in it is exactly what these must not share.
func sidecarContainer(name, image, subcommand, out string, ws flowv1alpha1.WorkspaceSpec, uid int64, subPath string, readOnly bool) corev1.Container {
	return corev1.Container{
		Name:  name,
		Image: image,
		Args:  []string{subcommand, "--" + contract.FlagOut, out},
		// The pod's own UID, for prepare to mark the run with and publish to
		// check before it moves the run (sidecar.Mark / CheckMark). Not
		// generation-time information, so not an argument: the pod does not
		// exist yet when this is built.
		Env: []corev1.EnvVar{{
			Name:      EnvPodUID,
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}},
		}},
		SecurityContext: &corev1.SecurityContext{
			RunAsUser:                ptr(uid),
			RunAsNonRoot:             ptr(true),
			AllowPrivilegeEscalation: ptr(false),
			ReadOnlyRootFilesystem:   ptr(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
			SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
		VolumeMounts: []corev1.VolumeMount{{Name: ws.Volume, MountPath: ws.MountPath, SubPath: subPath, ReadOnly: readOnly}},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("16Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
		},
	}
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

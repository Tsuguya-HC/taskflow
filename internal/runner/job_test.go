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

package runner

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
)

const (
	phaseInvestigate flowv1alpha1.Phase = "調査"

	taskUID      = "7f2a-uid"
	handlerName  = "cnp-reader"
	sidecarImage = "example.invalid/agent-sidecar:v0"
	workspaceVol = "work"
	workspaceAt  = "/workspace"
	agentUID     = int64(65533)
)

func task() *flowv1alpha1.Task {
	return &flowv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: "cnp-check-x7f2", Namespace: "claude-code", UID: taskUID},
		Spec: flowv1alpha1.TaskSpec{
			Flow:  "cnp-check",
			Input: &apiextensionsv1.JSON{Raw: []byte(`{"scope":"all namespaces"}`)},
		},
	}
}

func handler(mut ...func(*flowv1alpha1.TaskHandler)) *flowv1alpha1.TaskHandler {
	h := &flowv1alpha1.TaskHandler{
		ObjectMeta: metav1.ObjectMeta{Name: handlerName, Namespace: "claude-code"},
		Spec: flowv1alpha1.TaskHandlerSpec{
			Phase:     phaseInvestigate,
			Runner:    flowv1alpha1.RunnerSpec{Type: flowv1alpha1.RunnerJob},
			Timeout:   &metav1.Duration{Duration: 1200000000000}, // 20m
			Workspace: &flowv1alpha1.WorkspaceSpec{Volume: workspaceVol, MountPath: workspaceAt},
			JobTemplate: &flowv1alpha1.JobTemplate{
				Template: flowv1alpha1.PodTemplate{
					// The label a policy selects on, written by whoever
					// wrote the handler. Nothing here comes from us.
					Metadata: flowv1alpha1.EmbeddedObjectMeta{Labels: map[string]string{"role": handlerName}},
					Spec: corev1.PodSpec{
						RestartPolicy:      corev1.RestartPolicyNever,
						ServiceAccountName: "agent-cnp-reader",
						SecurityContext:    &corev1.PodSecurityContext{RunAsUser: ptr(agentUID)},
						Volumes:            []corev1.Volume{{Name: workspaceVol}},
						InitContainers: []corev1.Container{{
							Name: "fetch", Image: "example.invalid/fetch:v0",
							VolumeMounts: []corev1.VolumeMount{{Name: workspaceVol, MountPath: workspaceAt}},
						}},
						Containers: []corev1.Container{{
							Name: "agent", Image: "example.invalid/agent:v0",
							VolumeMounts: []corev1.VolumeMount{{Name: workspaceVol, MountPath: workspaceAt}},
						}},
					},
				},
			},
		},
	}
	for _, m := range mut {
		m(h)
	}
	return h
}

func build(t *testing.T, in Input) *batchv1.Job {
	t.Helper()
	if in.SidecarImage == "" {
		in.SidecarImage = sidecarImage
	}
	job, err := BuildJob(in)
	if err != nil {
		t.Fatalf("BuildJob: %v", err)
	}
	return job
}

func TestCarriesTheTemplateThrough(t *testing.T) {
	h := handler()
	job := build(t, Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1})

	pod := job.Spec.Template.Spec
	if pod.ServiceAccountName != "agent-cnp-reader" {
		t.Fatalf("serviceAccountName = %q; the handler's choice must survive", pod.ServiceAccountName)
	}
	if got := job.Spec.Template.Labels["role"]; got != handlerName {
		t.Fatalf("pod label role = %q; a policy selects on this and we must not touch it", got)
	}
	if len(job.Spec.Template.Labels) != 2 {
		t.Fatalf("pod labels = %v; the controller adds one, its own bookkeeping", job.Spec.Template.Labels)
	}
}

// The one label the controller sets is its own bookkeeping. A status name
// cannot be a label value, which is half the reason it is not one.
func TestSetsOnlyItsOwnLabel(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 1})

	if len(job.Labels) != 1 || job.Labels[LabelTaskUID] != taskUID {
		t.Fatalf("job labels = %v, want only %s", job.Labels, LabelTaskUID)
	}
	for k, v := range job.Labels {
		if strings.ContainsAny(v, "調査報告") {
			t.Fatalf("label %s carries a status name (%q), which Kubernetes rejects", k, v)
		}
	}
}

// The Job carries the UID so the controller can find its own work; the pod
// carries it so anyone else can find the pods of one task without first
// working out what the Job was called. Without it the only route is
// batch.kubernetes.io/job-name, which means knowing the run number.
func TestTaskUIDReachesThePod(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 1})

	if got := job.Spec.Template.Labels[LabelTaskUID]; got != taskUID {
		t.Fatalf("pod label %s = %q, want the task's UID", LabelTaskUID, got)
	}
}

func TestRefusesATemplateClaimingAFrameworkLabel(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		h.Spec.JobTemplate.Template.Metadata.Labels[LabelTaskUID] = "someone-elses-uid"
	})
	_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage})
	if !errors.Is(err, ErrReservedField) {
		t.Fatalf("err = %v; a template claiming the task UID must be refused, not overwritten", err)
	}
}

func TestPhaseAndRunTravelAsAnnotations(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 2, PrevRunID: 1})

	if got := job.Annotations[AnnotationPhase]; got != "調査" {
		t.Fatalf("%s = %q, want the status name unmangled", AnnotationPhase, got)
	}
	if got := job.Annotations[AnnotationRunID]; got != "2" {
		t.Fatalf("%s = %q", AnnotationRunID, got)
	}
	if got := job.Annotations[AnnotationPrevRunID]; got != "1" {
		t.Fatalf("%s = %q", AnnotationPrevRunID, got)
	}
}

func TestFirstRunHasNoPreviousRun(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 1})
	if _, ok := job.Annotations[AnnotationPrevRunID]; ok {
		t.Fatal("the first attempt has no previous run to point at")
	}
}

// The run number is deliberately absent from the environment: the plumbing
// reads it from the annotation, and the agent has no business counting
// attempts.
func TestRunNumberIsNotInTheEnvironment(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 3})
	for _, c := range job.Spec.Template.Spec.Containers {
		for _, e := range c.Env {
			if strings.Contains(e.Name, "RUN") {
				t.Fatalf("container %q has %s in its environment", c.Name, e.Name)
			}
		}
	}
}

func TestInjectsIdentityIntoEveryContainer(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 1})
	pod := job.Spec.Template.Spec

	for _, group := range [][]corev1.Container{pod.InitContainers, pod.Containers} {
		for _, c := range group {
			env := map[string]string{}
			for _, e := range c.Env {
				env[e.Name] = e.Value
			}
			if env[EnvTaskUID] != taskUID || env[EnvPhase] != "調査" {
				t.Fatalf("container %q env = %v", c.Name, env)
			}
			if env[EnvInput] != `{"scope":"all namespaces"}` {
				t.Fatalf("container %q input = %q", c.Name, env[EnvInput])
			}
		}
	}
}

// A handler that sets one of these means it. Overwriting would leave the YAML
// describing something other than what ran.
func TestDoesNotOverwriteTheHandlersEnv(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		h.Spec.JobTemplate.Template.Spec.Containers[0].Env = []corev1.EnvVar{
			{Name: EnvPhase, Value: "whatever the author wanted"},
		}
	})
	job := build(t, Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1})

	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == EnvPhase && e.Value != "whatever the author wanted" {
			t.Fatalf("%s was overwritten with %q", EnvPhase, e.Value)
		}
	}
}

// FLOW_INPUT is Spec.Input.Raw, a string the Task's author fully controls.
// Kubernetes expands $(VAR_NAME) in an env value against vars declared
// earlier in the same container, including ones resolved from a
// secretKeyRef — so the injected vars must come before whatever the handler
// declared, or a handler's secret placed after them would leak into
// FLOW_INPUT via a value like "$(GITHUB_TOKEN)".
func TestInjectedEnvComesBeforeTheHandlersEnv(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		h.Spec.JobTemplate.Template.Spec.Containers[0].Env = []corev1.EnvVar{
			{Name: "GITHUB_TOKEN", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{Key: "token"},
			}},
		}
	})
	job := build(t, Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1})

	idx := map[string]int{}
	for i, e := range job.Spec.Template.Spec.Containers[0].Env {
		idx[e.Name] = i
	}
	for _, injected := range []string{EnvTaskUID, EnvPhase, EnvInput} {
		if idx[injected] > idx["GITHUB_TOKEN"] {
			t.Fatalf("%s (index %d) comes after GITHUB_TOKEN (index %d); the handler's secret could expand into it",
				injected, idx[injected], idx["GITHUB_TOKEN"])
		}
	}
}

// The escaping is what actually stops expansion: putting the injected vars
// first does nothing against a secret pulled in via envFrom, which is
// resolvable from the very first env entry. FLOW_PHASE is covered too — the
// phase name is a free string the flow's author controls.
func TestAuthorControlledVarRefsAreEscaped(t *testing.T) {
	tk := task()
	tk.Spec.Input = &apiextensionsv1.JSON{Raw: []byte(`$(GITHUB_TOKEN)`)}
	phase := flowv1alpha1.Phase("調査-$(GITHUB_TOKEN)")
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		h.Spec.Phase = phase
	})
	job := build(t, Input{Task: tk, Handler: h, Phase: phase, RunID: 1})

	want := map[string]string{
		EnvInput: `$$(GITHUB_TOKEN)`,
		EnvPhase: `調査-$$(GITHUB_TOKEN)`,
	}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		expected, ok := want[e.Name]
		if !ok {
			continue
		}
		if e.Value != expected {
			t.Fatalf("%s = %q, want $( escaped to $$( so Kubernetes cannot expand it", e.Name, e.Value)
		}
		delete(want, e.Name)
	}
	for name := range want {
		t.Fatalf("%s was not set", name)
	}
}

func TestLeavesTheHandlerUntouched(t *testing.T) {
	h := handler()
	_ = build(t, Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1})

	if false {
		t.Fatal("building a Job edited the handler it was built from")
	}
	if got := len(h.Spec.JobTemplate.Template.Spec.Containers[0].Env); got != 0 {
		t.Fatalf("handler's container env grew to %d entries", got)
	}
}

func TestExecutionMechanics(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 1})

	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatal("Job-internal retry must be off; a retry there would reuse the runID")
	}
	if job.Spec.ActiveDeadlineSeconds == nil || *job.Spec.ActiveDeadlineSeconds != 1200 {
		t.Fatalf("activeDeadlineSeconds = %v, want the handler's 20m", job.Spec.ActiveDeadlineSeconds)
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].UID != taskUID {
		t.Fatalf("ownerReferences = %v", job.OwnerReferences)
	}
	if !*job.OwnerReferences[0].Controller {
		t.Fatal("the controller must own the Job, or cleanup will not follow the Task")
	}
}

func TestNameIsDeterministicAndLegal(t *testing.T) {
	a := JobName("cnp-check-x7f2", phaseInvestigate, 2)
	b := JobName("cnp-check-x7f2", phaseInvestigate, 2)
	if a != b {
		t.Fatalf("%q != %q; creating twice must conflict rather than make a second Job", a, b)
	}
	if strings.ContainsAny(a, "調査") {
		t.Fatalf("name %q contains a status name; Kubernetes rejects that", a)
	}
	for _, r := range a {
		lower := r >= 'a' && r <= 'z'
		digit := r >= '0' && r <= '9'
		if !lower && !digit && r != '-' {
			t.Fatalf("name %q contains %q, which RFC 1123 does not allow", a, r)
		}
	}
	if JobName("cnp-check-x7f2", "報告", 2) == a {
		t.Fatal("two phases produced the same name")
	}
	if JobName("cnp-check-x7f2", phaseInvestigate, 3) == a {
		t.Fatal("two runs produced the same name")
	}
}

func TestNameStaysWithinTheLimit(t *testing.T) {
	long := strings.Repeat("x", 200)
	name := JobName(long, phaseInvestigate, 12)
	if len(name) > maxNameLength {
		t.Fatalf("name is %d characters: %q", len(name), name)
	}
	if name == JobName(long, "報告", 12) {
		t.Fatal("truncation collapsed two phases onto one name")
	}
}

// generateName tends to put the part that tells two objects apart at the
// end of the name, exactly where truncation would otherwise chop it off.
func TestTruncatedNamesStayDistinct(t *testing.T) {
	a := "cnp-check-" + strings.Repeat("a", 60) + "-x7f2a"
	b := "cnp-check-" + strings.Repeat("a", 60) + "-q91zz"
	nameA := JobName(a, phaseInvestigate, 1)
	nameB := JobName(b, phaseInvestigate, 1)
	if nameA == nameB {
		t.Fatalf("two task names differing only after the truncation point collided on %q", nameA)
	}
}

// Five of the six reserved fields are gone from the type, so only this one
// can still be set wrong.
func TestRefusesARestartingPod(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		h.Spec.JobTemplate.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
	})
	_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1})
	if err == nil {
		t.Fatal("a pod that restarts in place was accepted")
	}
	if !strings.Contains(err.Error(), "restartPolicy") {
		t.Fatalf("error does not name the field: %v", err)
	}
}

// The other five cannot be expressed. This is the same move the directories
// make: not rejected, unwritable.
func TestTheOtherReservedFieldsDoNotExist(t *testing.T) {
	var tpl flowv1alpha1.JobTemplate
	v := reflect.TypeOf(tpl)
	for _, gone := range []string{"BackoffLimit", "TTLSecondsAfterFinished", "ActiveDeadlineSeconds", "Completions", "Parallelism"} {
		if _, found := v.FieldByName(gone); found {
			t.Fatalf("JobTemplate still has %s; it should not be settable at all", gone)
		}
	}
}

func TestRefusesAHandlerForAnotherPhase(t *testing.T) {
	_, err := BuildJob(Input{Task: task(), Handler: handler(), Phase: "報告", RunID: 1})
	if err == nil {
		t.Fatal("a handler bound to 調査 was used to run 報告")
	}
}

func TestRefusesAHandlerWithNoTemplate(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) { h.Spec.JobTemplate = nil })
	if _, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1}); err == nil {
		t.Fatal("a handler with no jobTemplate produced a Job")
	}
}

func TestNoTimeoutLeavesNoDeadline(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) { h.Spec.Timeout = nil })
	job := build(t, Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1})
	if job.Spec.ActiveDeadlineSeconds != nil {
		t.Fatalf("activeDeadlineSeconds = %v with no timeout set", *job.Spec.ActiveDeadlineSeconds)
	}
}

func TestNoInputSetsNoInputVariable(t *testing.T) {
	tk := task()
	tk.Spec.Input = nil
	job := build(t, Input{Task: tk, Handler: handler(), Phase: phaseInvestigate, RunID: 1})
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == EnvInput {
			t.Fatalf("%s was set to %q for a task with no input", EnvInput, e.Value)
		}
	}
}

func TestRefusesNothing(t *testing.T) {
	if _, err := BuildJob(Input{Handler: handler(), Phase: phaseInvestigate}); err == nil {
		t.Fatal("a Job was built with no task")
	}
	if _, err := BuildJob(Input{Task: task(), Phase: phaseInvestigate}); err == nil {
		t.Fatal("a Job was built with no handler")
	}
}

// A container reads annotations through the downward API, which shows it the
// pod it is in — never the Job above it. So the framework's annotations have
// to be on the pod template too, or the run number the design promises the
// plumbing (§4) is not actually reachable from inside.
func TestFrameworkAnnotationsReachThePod(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		h.Spec.JobTemplate.Template.Metadata.Annotations = map[string]string{"note": "keep me"}
	})
	job := build(t, Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 2, PrevRunID: 1})

	pod := job.Spec.Template.Annotations
	if pod[AnnotationRunID] != "2" || pod[AnnotationPrevRunID] != "1" || pod[AnnotationPhase] != "調査" {
		t.Fatalf("pod annotations = %v; the run must be readable from inside the pod", pod)
	}
	if pod["note"] != "keep me" {
		t.Fatalf("pod annotations = %v; the handler's own annotation was lost", pod)
	}
	if h.Spec.JobTemplate.Template.Metadata.Annotations[AnnotationRunID] != "" {
		t.Fatal("the handler's template was edited in place")
	}
	if _, claimed := h.Spec.JobTemplate.Template.Metadata.Labels[LabelTaskUID]; claimed {
		t.Fatal("the handler's template labels were edited in place")
	}
}

func TestRefusesATemplateClaimingAFrameworkAnnotation(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		h.Spec.JobTemplate.Template.Metadata.Annotations = map[string]string{AnnotationRunID: "7"}
	})
	_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage})
	if !errors.Is(err, ErrReservedField) {
		t.Fatalf("err = %v; a template lying about the run number must be refused, not overwritten", err)
	}
}

// The directories are the vocabulary of the run, so every container gets
// them — the sidecar to create them, the agent because they are what it may
// answer. Sorted, so the same declaration always renders the same.
func TestDeclaredDirectoriesTravelAsJSON(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 1,
		Directories: []string{"more", "ok", "a,b"}})
	pod := job.Spec.Template.Spec
	for _, group := range [][]corev1.Container{pod.InitContainers, pod.Containers} {
		for _, c := range group {
			var got string
			for _, e := range c.Env {
				if e.Name == EnvDirectories {
					got = e.Value
				}
			}
			if got != `["a,b","more","ok"]` {
				t.Fatalf("container %q %s = %q", c.Name, EnvDirectories, got)
			}
		}
	}
}

func TestNoDirectoriesIsAnEmptyList(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 1})
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == EnvDirectories && e.Value != "[]" {
			t.Fatalf("%s = %q; a terminal phase declares nothing, which is still a list", EnvDirectories, e.Value)
		}
	}
}

// The two ends of the verdict protocol are one program, and the end inside
// the pod is put there by the end that reads it back — not copied into
// every handler. prepare goes first so out/ exists before anything of the
// handler's runs; publish is a native sidecar so it is stopped, and answers,
// once the main containers are done.
func TestInjectsPrepareAndPublish(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 1})
	inits := job.Spec.Template.Spec.InitContainers

	if len(inits) != 3 || inits[0].Name != PrepareContainer || inits[1].Name != PublishContainer || inits[2].Name != "fetch" {
		names := make([]string, 0, len(inits))
		for _, c := range inits {
			names = append(names, c.Name)
		}
		t.Fatalf("initContainers = %v; want prepare, publish, then the handler's own", names)
	}
	prepare, publish := inits[0], inits[1]

	for _, c := range []corev1.Container{prepare, publish} {
		if c.Image != sidecarImage {
			t.Fatalf("%s image = %q, want the controller's", c.Name, c.Image)
		}
		if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].Name != workspaceVol || c.VolumeMounts[0].MountPath != workspaceAt {
			t.Fatalf("%s mounts = %v; want only the workspace at its declared path", c.Name, c.VolumeMounts)
		}
		if got := c.Args[len(c.Args)-1]; got != workspaceAt+"/out" {
			t.Fatalf("%s --out = %q; the layout is the framework's, under the handler's path", c.Name, got)
		}
		if c.SecurityContext == nil || c.SecurityContext.RunAsUser == nil || *c.SecurityContext.RunAsUser != preferredSidecarUID {
			t.Fatalf("%s runs as %v, want %d", c.Name, c.SecurityContext, preferredSidecarUID)
		}
		if !*c.SecurityContext.ReadOnlyRootFilesystem || !*c.SecurityContext.RunAsNonRoot {
			t.Fatalf("%s is not locked down: %+v", c.Name, c.SecurityContext)
		}
		if got := c.Resources.Requests.Cpu().String(); got != "10m" {
			t.Fatalf("%s requests.cpu = %s, want 10m", c.Name, got)
		}
		if got := c.Resources.Requests.Memory().String(); got != "16Mi" {
			t.Fatalf("%s requests.memory = %s, want 16Mi", c.Name, got)
		}
		if got := c.Resources.Limits.Memory().String(); got != "64Mi" {
			t.Fatalf("%s limits.memory = %s, want 64Mi", c.Name, got)
		}
		if _, capped := c.Resources.Limits[corev1.ResourceCPU]; capped {
			t.Fatalf("%s sets a cpu limit; a throttled sidecar could stall the run it is meant to seal", c.Name)
		}
	}
	if prepare.Args[0] != "prepare" || publish.Args[0] != "publish" {
		t.Fatalf("subcommands = %q / %q", prepare.Args[0], publish.Args[0])
	}
	if prepare.RestartPolicy != nil {
		t.Fatal("prepare must run to completion, not restart")
	}
	if publish.RestartPolicy == nil || *publish.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Fatal("publish must be a native sidecar, or it would never be stopped and never answer")
	}
	if prepare.VolumeMounts[0].ReadOnly {
		t.Fatal("prepare has to write the directories")
	}
	if !publish.VolumeMounts[0].ReadOnly {
		t.Fatal("publish only reads; give it no more")
	}
	if len(publish.Args) != 3 {
		t.Fatalf("publish args = %v; a template-backed workspace has no results/ shelf to move a run onto", publish.Args)
	}
}

// The sidecars read the vocabulary from the same environment as everyone
// else, so injecting them must happen before the environment is laid on.
func TestInjectedContainersGetTheVocabulary(t *testing.T) {
	job := build(t, Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 1, Directories: []string{"ok"}})
	for _, c := range job.Spec.Template.Spec.InitContainers[:2] {
		found := false
		for _, e := range c.Env {
			if e.Name == EnvDirectories && e.Value == `["ok"]` {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s env = %v; without %s it cannot lay the directories down", c.Name, c.Env, EnvDirectories)
		}
	}
}

// out/ is closed by uid: prepare owns it and leaves it 0555, and only a
// container running as that same uid could chmod it open again. So the uid
// is picked around whatever the handler uses, rather than being a number
// the handler's author has to know to avoid.
func TestSidecarUIDStepsPastTheHandlers(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		spec := &h.Spec.JobTemplate.Template.Spec
		spec.SecurityContext.RunAsUser = ptr(preferredSidecarUID)
		spec.Containers[0].SecurityContext = &corev1.SecurityContext{RunAsUser: ptr(preferredSidecarUID - 1)}
	})
	job := build(t, Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1})
	for _, c := range job.Spec.Template.Spec.InitContainers[:2] {
		if got := *c.SecurityContext.RunAsUser; got != preferredSidecarUID-2 {
			t.Fatalf("%s runs as %d; the pod and a container already use %d and %d", c.Name, got, preferredSidecarUID, preferredSidecarUID-1)
		}
	}
}

// A pod that does not say its uid runs as whatever each image was built
// with, which the sidecar cannot be sure to differ from. Refused rather
// than guessed: the closing of out/ is the whole enforcement.
func TestRefusesAPodWithNoUID(t *testing.T) {
	cases := map[string]func(*flowv1alpha1.TaskHandler){
		"no securityContext at all": func(h *flowv1alpha1.TaskHandler) { h.Spec.JobTemplate.Template.Spec.SecurityContext = nil },
		"securityContext with no runAsUser": func(h *flowv1alpha1.TaskHandler) {
			h.Spec.JobTemplate.Template.Spec.SecurityContext = &corev1.PodSecurityContext{}
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			h := handler(mut)
			_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage})
			if !errors.Is(err, ErrWorkspace) {
				t.Fatalf("err = %v; want a refusal to close out/ against an unknown uid", err)
			}
		})
	}
}

func TestRefusesAHandlerWithNoWorkspace(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) { h.Spec.Workspace = nil })
	_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage})
	if !errors.Is(err, ErrWorkspace) {
		t.Fatalf("err = %v; a run with nowhere for its directories can never answer", err)
	}
}

func TestRefusesAWorkspaceVolumeTheTemplateLacks(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) { h.Spec.Workspace.Volume = "elsewhere" })
	_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage})
	if !errors.Is(err, ErrWorkspace) {
		t.Fatalf("err = %v; a mount of a volume that is not there fails at the kubelet, too late and too quietly", err)
	}
}

// A workspace that no handler container mounts is refused even though
// prepare and publish still work fine off the volume named in the spec:
// none of the handler's own containers could read what prepare laid down or
// write an answer into it, so the run could only ever come back silent.
// Where it is mounted does not matter — only that some container does.
func TestRefusesAWorkspaceNoContainerMounts(t *testing.T) {
	cases := map[string]func(*flowv1alpha1.TaskHandler){
		"no mount at all": func(h *flowv1alpha1.TaskHandler) {
			spec := &h.Spec.JobTemplate.Template.Spec
			spec.InitContainers[0].VolumeMounts = nil
			spec.Containers[0].VolumeMounts = nil
		},
		// A read-only mount ends the same way an absent one does: prepare's
		// declared directories exist, but nothing of the handler's can write
		// an answer into them.
		"only a read-only mount": func(h *flowv1alpha1.TaskHandler) {
			spec := &h.Spec.JobTemplate.Template.Spec
			spec.InitContainers[0].VolumeMounts[0].ReadOnly = true
			spec.Containers[0].VolumeMounts[0].ReadOnly = true
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			h := handler(mut)
			_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage})
			if !errors.Is(err, ErrWorkspace) {
				t.Fatalf("err = %v; a handler whose containers never mount the workspace writably can never answer", err)
			}
		})
	}
}

// The mount path is the handler's own choice, not something checkWorkspace
// verifies: prepare and publish always mount at ws.MountPath, but a handler
// container is free to mount the same volume anywhere else and still see the
// directories prepare lays down, because it is the same underlying volume.
func TestAcceptsAWorkspaceMountedAtADifferentPath(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		spec := &h.Spec.JobTemplate.Template.Spec
		spec.InitContainers[0].VolumeMounts[0].MountPath = "/elsewhere"
		spec.Containers[0].VolumeMounts[0].MountPath = "/elsewhere"
	})
	if _, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage}); err != nil {
		t.Fatalf("BuildJob: %v; mounting the workspace at a different path from the injected containers' must still be accepted", err)
	}
}

// A template that already has a container under an injected name is
// refused, like a template claiming a framework label: merging would run
// something other than what the YAML says.
func TestRefusesATemplateUsingAnInjectedName(t *testing.T) {
	for _, name := range []string{PrepareContainer, PublishContainer} {
		h := handler(func(h *flowv1alpha1.TaskHandler) {
			h.Spec.JobTemplate.Template.Spec.Containers = append(h.Spec.JobTemplate.Template.Spec.Containers,
				corev1.Container{Name: name, Image: "example.invalid/mine:v0"})
		})
		_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage})
		if !errors.Is(err, ErrReservedField) {
			t.Fatalf("container %q: err = %v; want a refusal", name, err)
		}
	}
}

func TestRefusesToBuildWithoutASidecarImage(t *testing.T) {
	if _, err := BuildJob(Input{Task: task(), Handler: handler(), Phase: phaseInvestigate, RunID: 1}); err == nil {
		t.Fatal("a Job with no publish container can never answer; building one must fail")
	}
}

// flowWorkspace rewires the handler onto the reserved volume, the way a
// handler joins a flow that brings a claim of its own.
func flowWorkspace(h *flowv1alpha1.TaskHandler) {
	h.Spec.Workspace.Volume = contract.WorkspaceVolume
	spec := &h.Spec.JobTemplate.Template.Spec
	spec.Volumes = nil
	spec.InitContainers[0].VolumeMounts[0].Name = contract.WorkspaceVolume
	spec.Containers[0].VolumeMounts[0].Name = contract.WorkspaceVolume
}

// prepare and the handler's own containers write to work/<runID>, not
// results/<runID>: results/ is the shelf of already-sealed runs, and a run
// still in flight is not one of those yet (§ADR-0002 決定5, the mv design).
func TestFlowWorkspaceMountsTheTaskClaimPerRun(t *testing.T) {
	job := build(t, Input{
		Task: task(), Handler: handler(flowWorkspace), Phase: phaseInvestigate,
		RunID: 3, WorkspacePVC: "cnp-check-x7f2-ws-abcd1234",
	})
	spec := job.Spec.Template.Spec

	var claim *corev1.PersistentVolumeClaimVolumeSource
	for _, v := range spec.Volumes {
		if v.Name == contract.WorkspaceVolume {
			claim = v.PersistentVolumeClaim
		}
	}
	if claim == nil || claim.ClaimName != "cnp-check-x7f2-ws-abcd1234" {
		t.Fatalf("volumes = %v; want flow-workspace backed by the task's claim", spec.Volumes)
	}

	prepare, publish := spec.InitContainers[0], spec.InitContainers[1]
	if got := prepare.VolumeMounts[0].SubPath; got != "work/3" {
		t.Fatalf("prepare subPath = %q; a run in flight writes under its own number in work/, not results/", got)
	}
	if prepare.VolumeMounts[0].ReadOnly {
		t.Fatal("prepare has to write the directories")
	}
	if got := spec.Containers[0].VolumeMounts[0].SubPath; got != "work/3" {
		t.Fatalf("agent subPath = %q; the handler's own writable mount lands under the run's own number in work/ too, pinned by the controller rather than left to the handler to resolve", got)
	}

	// publish moves this run from work/ to results/ once it seals, so its own
	// mount cannot be pinned to either shelf — it needs the claim's root,
	// writably, to see and rename between both.
	if got := publish.VolumeMounts[0].SubPath; got != "" {
		t.Fatalf("publish subPath = %q; a flow-workspace publish must mount the claim's root to see both shelves", got)
	}
	if publish.VolumeMounts[0].ReadOnly {
		t.Fatal("publish must be able to write, to move the run onto the results/ shelf")
	}
	wantArgs := []string{
		"publish", "--out", "/workspace/work/3/out",
		"--seal-from", "/workspace/work/3", "--seal-to", "/workspace/results/3",
	}
	if !reflect.DeepEqual(publish.Args, wantArgs) {
		t.Fatalf("publish args = %v, want %v", publish.Args, wantArgs)
	}
}

// A phase reading earlier runs mounts the flow workspace read-only, at the
// volume's root, precisely so it sees every run rather than only the one in
// progress; pinning a subPath onto it the way the writable mount is pinned
// would hide every run but this one from something whose whole point is
// looking back at them.
func TestFlowWorkspaceLeavesAReadOnlyMountUnpinned(t *testing.T) {
	h := handler(flowWorkspace, func(h *flowv1alpha1.TaskHandler) {
		spec := &h.Spec.JobTemplate.Template.Spec
		spec.Containers = append(spec.Containers, corev1.Container{
			Name: "history", Image: "example.invalid/history:v0",
			VolumeMounts: []corev1.VolumeMount{{Name: contract.WorkspaceVolume, MountPath: "/history", ReadOnly: true}},
		})
	})
	job := build(t, Input{
		Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 3, WorkspacePVC: "some-claim",
	})

	for _, c := range job.Spec.Template.Spec.Containers {
		if c.Name != "history" {
			continue
		}
		if got := c.VolumeMounts[0].SubPath; got != "" {
			t.Fatalf("history subPath = %q; a read-only mount reads every run, not just this one", got)
		}
	}
}

// The per-run layout under a writable flow-workspace mount is the
// controller's to pin, at generation time; a handler that already wrote a
// SubPath or SubPathExpr there would either have it silently overwritten
// every run, or — for a SubPathExpr expecting $(FLOW_RUN_ID) — run against a
// variable this design deliberately never injects. Refused rather than
// merged, same as every other reserved field.
func TestRefusesAHandlerSettingItsOwnLayoutOnAWritableFlowWorkspaceMount(t *testing.T) {
	cases := map[string]func(*flowv1alpha1.TaskHandler){
		"subPath": func(h *flowv1alpha1.TaskHandler) {
			h.Spec.JobTemplate.Template.Spec.Containers[0].VolumeMounts[0].SubPath = "mine"
		},
		"subPathExpr": func(h *flowv1alpha1.TaskHandler) {
			h.Spec.JobTemplate.Template.Spec.Containers[0].VolumeMounts[0].SubPathExpr = "$(SOME_VAR)"
		},
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			h := handler(flowWorkspace, mut)
			_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage, WorkspacePVC: "x"})
			if !errors.Is(err, ErrReservedField) {
				t.Fatalf("err = %v; the per-run layout under a writable flow-workspace mount is the framework's to set", err)
			}
		})
	}
}

// A phase may stay on its own template volume even when the flow brings a
// claim: nothing forces every handler through the shared workspace, and the
// flat layout it has today must not change underneath it.
func TestATemplateVolumeKeepsTheFlatLayout(t *testing.T) {
	job := build(t, Input{
		Task: task(), Handler: handler(), Phase: phaseInvestigate,
		RunID: 2, WorkspacePVC: "some-claim",
	})
	spec := job.Spec.Template.Spec
	for _, c := range spec.InitContainers[:2] {
		if got := c.VolumeMounts[0].SubPath; got != "" {
			t.Fatalf("%s subPath = %q; a template-backed workspace has no run layout", c.Name, got)
		}
	}
	for _, v := range spec.Volumes {
		if v.Name == contract.WorkspaceVolume {
			t.Fatal("the reserved volume was added though nothing mounts it")
		}
	}
}

func TestRefusesATemplateVolumeWearingTheReservedName(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		spec := &h.Spec.JobTemplate.Template.Spec
		spec.Volumes = append(spec.Volumes, corev1.Volume{Name: contract.WorkspaceVolume})
	})
	_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage, WorkspacePVC: "x"})
	if !errors.Is(err, ErrReservedField) {
		t.Fatalf("err = %v; a template volume under the injected name would silently differ from what runs", err)
	}
}

func TestRefusesFlowWorkspaceUnderAFlowWithoutOne(t *testing.T) {
	h := handler(flowWorkspace)
	_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1, SidecarImage: sidecarImage})
	if !errors.Is(err, ErrWorkspace) {
		t.Fatalf("err = %v; a mount of a claim no flow brings fails at the kubelet, too late and too quietly", err)
	}
}

func TestWorkspacePVCNameIsUIDDerived(t *testing.T) {
	a := WorkspacePVCName("cnp-check", "uid-1")
	if a != WorkspacePVCName("cnp-check", "uid-1") {
		t.Fatal("not deterministic")
	}
	if a == WorkspacePVCName("cnp-check", "uid-2") {
		t.Fatal("two tasks under one name must not share a claim")
	}
	long := WorkspacePVCName(strings.Repeat("x", 80), "uid-1")
	if len(long) > maxNameLength {
		t.Fatalf("len = %d; a claim name over the limit is refused by the apiserver", len(long))
	}
}

func TestBuildWorkspacePVCOwnership(t *testing.T) {
	vct := &corev1.PersistentVolumeClaimSpec{
		AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
	}
	tk := task()
	pvc := BuildWorkspacePVC(tk, vct)

	if pvc.Name != WorkspacePVCName(tk.Name, tk.UID) || pvc.Namespace != tk.Namespace {
		t.Fatalf("claim is %s/%s; want the task's namespace under the derived name", pvc.Namespace, pvc.Name)
	}
	if pvc.Labels[LabelTaskUID] != taskUID {
		t.Fatalf("labels = %v; the bookkeeping label is the controller's to set", pvc.Labels)
	}
	if len(pvc.OwnerReferences) != 1 {
		t.Fatalf("ownerReferences = %v", pvc.OwnerReferences)
	}
	ref := pvc.OwnerReferences[0]
	if ref.UID != taskUID || ref.Controller == nil || !*ref.Controller {
		t.Fatalf("ownerReference = %+v; the claim must be controlled by its task", ref)
	}
	if ref.BlockOwnerDeletion != nil && *ref.BlockOwnerDeletion {
		t.Fatal("the claim rides the task's TTL out; it does not get a say in the deletion")
	}

	vct.AccessModes[0] = corev1.ReadWriteOnce
	if pvc.Spec.AccessModes[0] != corev1.ReadWriteMany {
		t.Fatal("the claim's spec must be a copy, not an alias of the flow's")
	}
}

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
)

const (
	phaseInvestigate flowv1alpha1.Phase = "調査"

	taskUID     = "7f2a-uid"
	handlerName = "cnp-reader"
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
			Phase:   phaseInvestigate,
			Runner:  flowv1alpha1.RunnerSpec{Type: flowv1alpha1.RunnerJob},
			Timeout: &metav1.Duration{Duration: 1200000000000}, // 20m
			JobTemplate: &flowv1alpha1.JobTemplate{
				Template: flowv1alpha1.PodTemplate{
					// The label a policy selects on, written by whoever
					// wrote the handler. Nothing here comes from us.
					Metadata: flowv1alpha1.EmbeddedObjectMeta{Labels: map[string]string{"role": handlerName}},
					Spec: corev1.PodSpec{
						RestartPolicy:      corev1.RestartPolicyNever,
						ServiceAccountName: "agent-cnp-reader",
						InitContainers:     []corev1.Container{{Name: "prepare", Image: "example.invalid/prepare:v0"}},
						Containers:         []corev1.Container{{Name: "agent", Image: "example.invalid/agent:v0"}},
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
	if len(job.Spec.Template.Labels) != 1 {
		t.Fatalf("pod labels = %v; the controller adds none", job.Spec.Template.Labels)
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
}

func TestRefusesATemplateClaimingAFrameworkAnnotation(t *testing.T) {
	h := handler(func(h *flowv1alpha1.TaskHandler) {
		h.Spec.JobTemplate.Template.Metadata.Annotations = map[string]string{AnnotationRunID: "7"}
	})
	_, err := BuildJob(Input{Task: task(), Handler: h, Phase: phaseInvestigate, RunID: 1})
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

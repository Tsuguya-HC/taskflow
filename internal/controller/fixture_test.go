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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

const (
	phaseInvestigate flowv1alpha1.Phase = "調査"
	phaseReport      flowv1alpha1.Phase = "報告"
	// phaseBroken is an ending a flow can declare to be a failure — an
	// ordinary name its author chose, like every other phase here.
	phaseBroken flowv1alpha1.Phase = "失敗"
)

const (
	handlerName   = "sample-handler"
	sidecarImage  = "example.invalid/agent-sidecar:v0"
	workspaceVol  = "work"
	workspacePath = "/workspace"
)

// Every spec gets its own names. Sharing them let one spec's leftover Job —
// nothing deletes those — decide the next spec's outcome, which is how two of
// these passed alone and failed together.
var specCounter int

// fixture is one spec's worth of objects, all named after the spec so specs
// cannot collide, and all cleaned up when the spec ends.
type fixture struct {
	ctx        context.Context
	name       string
	reconciler *TaskReconciler
	// events is where the reconciler's own event recorder writes. envtest
	// runs no event sink worth reading back, and a spec wants to assert what
	// was announced rather than that something was; a fake recorder is the
	// only way to see it.
	events *events.FakeRecorder
	// taskUID is set by makeTask once the Task exists. DeferCleanup unwinds
	// LIFO, so the Job cleanup registered in newFixture runs last, after the
	// Task itself is already gone. Reading its UID at cleanup time would find
	// nothing; capturing it here, in a field the closure reads when it finally
	// runs, is what lets the cleanup match anything at all.
	taskUID types.UID
}

func newFixture() *fixture {
	specCounter++
	fx := &fixture{
		ctx:  context.Background(),
		name: fmt.Sprintf("run-%d", specCounter),
	}
	// Buffered well past what any one spec produces: a FakeRecorder drops
	// events once its channel is full, which would turn "nothing was
	// announced" into a passing assertion for the wrong reason.
	fx.events = events.NewFakeRecorder(16)
	fx.reconciler = &TaskReconciler{
		Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: fx.events, SidecarImage: sidecarImage,
		// The suite's client talks to the API server directly, with no cache
		// in front of it, which is exactly what this field asks for.
		APIReader: k8sClient,
	}
	DeferCleanup(func() {
		// The reconciler's Jobs outlive their Task here: envtest has no
		// garbage collector, so ownerReferences do not remove them.
		// Background propagation is explicit: the default leaves an "orphan"
		// finalizer on the Job that, again for lack of a garbage collector,
		// nothing ever clears, and the Job never actually goes away.
		if fx.taskUID == "" {
			return
		}
		_ = k8sClient.DeleteAllOf(fx.ctx, &batchv1.Job{},
			client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID)},
			client.PropagationPolicy(metav1.DeletePropagationBackground))
		// The verdict boxes go the same way and for the same reason: they
		// are owned by the Task, and nothing here collects owned objects.
		_ = k8sClient.DeleteAllOf(fx.ctx, &corev1.ConfigMap{},
			client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID)})
	})
	return fx
}

func (fx *fixture) makeFlow(mut ...func(*flowv1alpha1.TaskFlow)) *flowv1alpha1.TaskFlow {
	f := &flowv1alpha1.TaskFlow{
		ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
		Spec: flowv1alpha1.TaskFlowSpec{
			Profile: flowv1alpha1.ProfileInvestigate,
			Start:   phaseInvestigate,
			Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
				phaseInvestigate: {Handler: fx.name, Next: map[flowv1alpha1.Phase]string{phaseReport: "ok"}},
			},
			ReworkBudget: 2,
		},
	}
	for _, m := range mut {
		m(f)
	}
	Expect(k8sClient.Create(fx.ctx, f)).To(Succeed())
	DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, f) })
	return f
}

// announced drains whatever the reconciler recorded, so a spec can say what
// was said rather than only that something was. Reading a channel that is
// empty must not block, so it stops as soon as nothing more is waiting.
func (fx *fixture) announced() []string {
	var out []string
	for {
		select {
		case e := <-fx.events.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

// directoriesOf is the vocabulary the controller injected into the Job: the
// directories the run will find in front of it, and so the only answers it
// can give. Reading it back from the Job is how a spec checks what the flow's
// declaration actually reaches the pod as.
func directoriesOf(job *batchv1.Job) []string {
	for _, c := range job.Spec.Template.Spec.Containers {
		for _, e := range c.Env {
			if e.Name != runner.EnvDirectories {
				continue
			}
			var dirs []string
			Expect(json.Unmarshal([]byte(e.Value), &dirs)).To(Succeed())
			return dirs
		}
	}
	Fail("the Job carries no " + runner.EnvDirectories)
	return nil
}

// jobRunner is the handler shape every spec but the State ones wants: a pod
// to run, and the workspace the injected containers are fitted into.
func jobRunner() func(*flowv1alpha1.TaskHandler) {
	return func(h *flowv1alpha1.TaskHandler) {
		h.Spec.Runner = flowv1alpha1.RunnerSpec{Type: flowv1alpha1.RunnerJob}
		h.Spec.Workspace = &flowv1alpha1.WorkspaceSpec{Volume: workspaceVol, MountPath: workspacePath}
		h.Spec.JobTemplate = &flowv1alpha1.JobTemplate{
			Template: flowv1alpha1.PodTemplate{
				Metadata: flowv1alpha1.EmbeddedObjectMeta{Labels: map[string]string{"role": handlerName}},
				Spec: corev1.PodSpec{
					RestartPolicy:   corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{RunAsUser: ptr.To(int64(65533))},
					Volumes:         []corev1.Volume{{Name: workspaceVol}},
					Containers: []corev1.Container{{
						Name: agentName, Image: agentImage,
						VolumeMounts: []corev1.VolumeMount{{Name: workspaceVol, MountPath: workspacePath}},
					}},
				},
			},
		}
		h.Spec.Timeout = nil
	}
}

func (fx *fixture) makeHandler(mut ...func(*flowv1alpha1.TaskHandler)) {
	h := &flowv1alpha1.TaskHandler{
		ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
		Spec:       flowv1alpha1.TaskHandlerSpec{Phase: phaseInvestigate},
	}
	jobRunner()(h)
	for _, m := range mut {
		m(h)
	}
	Expect(k8sClient.Create(fx.ctx, h)).To(Succeed())
	DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, h) })
}

func (fx *fixture) makeTask() *flowv1alpha1.Task {
	tk := &flowv1alpha1.Task{
		ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
		Spec:       flowv1alpha1.TaskSpec{Flow: fx.name},
	}
	Expect(k8sClient.Create(fx.ctx, tk)).To(Succeed())
	fx.taskUID = tk.UID
	DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, tk) })
	return tk
}

func (fx *fixture) reconcile() reconcile.Result {
	res, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
	})
	Expect(err).NotTo(HaveOccurred())
	return res
}

func (fx *fixture) get() *flowv1alpha1.Task {
	var tk flowv1alpha1.Task
	Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, &tk)).To(Succeed())
	return &tk
}

// stateRunner turns the fixture's handler into one the framework does not
// start: no pod to describe, and a deadline of its own since nothing else
// would end the wait.
func stateRunner(timeout time.Duration) func(*flowv1alpha1.TaskHandler) {
	return func(h *flowv1alpha1.TaskHandler) {
		h.Spec.Runner = flowv1alpha1.RunnerSpec{Type: flowv1alpha1.RunnerState}
		h.Spec.JobTemplate, h.Spec.Workspace = nil, nil
		h.Spec.Timeout = &metav1.Duration{Duration: timeout}
	}
}

// box fetches the place the starting phase's first run is answered in, and
// fails the spec when it is not there. Later runs and the cleanup run go
// through boxFor, which takes both.
func (fx *fixture) box() *corev1.ConfigMap {
	return fx.boxFor(phaseInvestigate, 1)
}

func (fx *fixture) boxFor(phase flowv1alpha1.Phase, runID int32) *corev1.ConfigMap {
	var box corev1.ConfigMap
	Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
		Name: runner.VerdictBoxName(fx.name, fx.taskUID, phase, runID), Namespace: resourceNamespace,
	}, &box)).To(Succeed())
	return &box
}

// answer writes into that place the way whoever is answering would.
func (fx *fixture) answer(verdict, reason string) {
	fx.answerFor(phaseInvestigate, 1, verdict, reason)
}

func (fx *fixture) answerFor(phase flowv1alpha1.Phase, runID int32, verdict, reason string) {
	box := fx.boxFor(phase, runID)
	box.Data = map[string]string{contract.KeyVerdict: verdict}
	if reason != "" {
		box.Data[contract.KeyReason] = reason
	}
	Expect(k8sClient.Update(fx.ctx, box)).To(Succeed())
}

// job fetches the Job for the first attempt at one run of the starting phase.
func (fx *fixture) job(runID int32) *batchv1.Job {
	return fx.jobAttempt(runID, 0)
}

// jobAttempt fetches the Job for one attempt at one run of the starting
// phase. A run keeps its number across infrastructure retries, so the
// attempt is what tells the second Job from the first.
func (fx *fixture) jobAttempt(runID, attempt int32) *batchv1.Job {
	var job batchv1.Job
	Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
		Name: runner.JobName(fx.name, phaseInvestigate, runID, attempt), Namespace: resourceNamespace,
	}, &job)).To(Succeed())
	return &job
}

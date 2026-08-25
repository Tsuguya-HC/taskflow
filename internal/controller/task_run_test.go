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
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
	"github.com/Tsuguya/taskflow/internal/runner"
)

var _ = Describe("starting a task", func() {
	ctx := context.Background()
	var fx *fixture
	var name string
	var reconciler *TaskReconciler

	BeforeEach(func() {
		fx = newFixture()
		name = fx.name
		reconciler = fx.reconciler
	})

	makeFlow := func(mut ...func(*flowv1alpha1.TaskFlow)) *flowv1alpha1.TaskFlow { return fx.makeFlow(mut...) }
	makeHandler := func() { fx.makeHandler() }
	makeTask := func() *flowv1alpha1.Task { return fx.makeTask() }
	reconcileOnce := func() { fx.reconcile() }
	get := func() *flowv1alpha1.Task { return fx.get() }

	It("puts a fresh task on the phase the flow starts at", func() {
		makeFlow()
		makeHandler()
		makeTask()

		reconcileOnce()

		tk := get()
		Expect(tk.Status.Phase).To(Equal(phaseInvestigate))
		Expect(tk.Status.RunID).To(BeEquivalentTo(1))
		Expect(tk.Status.ReworkBudget).To(BeEquivalentTo(2), "the budget is taken from the flow")
		Expect(tk.Status.CurrentRun).NotTo(BeNil())
	})

	It("creates the Job for the phase in flight", func() {
		makeFlow()
		makeHandler()
		makeTask()

		reconcileOnce() // settles the starting phase
		reconcileOnce() // creates the Job

		var job batchv1.Job
		jobName := runner.JobName(name, phaseInvestigate, 1)
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jobName, Namespace: resourceNamespace}, &job)).To(Succeed())

		Expect(job.Annotations[runner.AnnotationPhase]).To(Equal(string(phaseInvestigate)),
			"the status name travels as an annotation, being illegal as a label")
		Expect(job.Labels).To(HaveLen(1), "the controller adds one label of its own and no more")
		Expect(job.Spec.Template.Labels).To(HaveKeyWithValue("role", handlerName),
			"whatever a policy selects on comes from the handler untouched")
		Expect(job.OwnerReferences).To(HaveLen(1))

		Expect(get().Status.CurrentRun.JobName).To(Equal(jobName),
			"the run in status must name the Job it belongs to, not just its phase and runID")
	})

	// The name is derived rather than generated, so a controller that restarts
	// mid-flight collides with its own Job instead of starting a second one.
	It("does not start a second Job for the same run", func() {
		makeFlow()
		makeHandler()
		makeTask()

		reconcileOnce()
		reconcileOnce()
		reconcileOnce()
		reconcileOnce()

		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(get().UID)})).To(Succeed())
		Expect(jobs.Items).To(HaveLen(1))
	})

	It("fails a task whose flow is gone", func() {
		makeTask() // no flow

		reconcileOnce()

		tk := get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(tk.Status.CurrentRun).To(BeNil())

		// A second reconcile must not rewrite a task that already failed —
		// fail's idempotency guard, otherwise untested past the first call.
		reconcileOnce()
		Expect(get().ResourceVersion).To(Equal(tk.ResourceVersion), "an already-failed task must not be written again")
	})

	// Creation-time validation should catch this, but the controller must not
	// be the part that assumes admission ran.
	It("fails a task whose flow starts nowhere", func() {
		makeFlow(func(f *flowv1alpha1.TaskFlow) { f.Spec.Start = "存在しない" })
		makeTask()

		reconcileOnce()

		Expect(get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
	})

	It("fails a task whose handler is missing", func() {
		makeFlow()
		makeTask() // no handler

		reconcileOnce()
		reconcileOnce()

		tk := get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx,
			types.NamespacedName{Name: runner.JobName(name, phaseInvestigate, 1), Namespace: resourceNamespace},
			&batchv1.Job{}))).To(BeTrue(), "no Job should exist for a phase that cannot run")
	})

	It("leaves a finished task alone", func() {
		makeFlow()
		makeHandler()
		makeTask()

		reconcileOnce()
		tk := get()
		tk.Status.Phase = phaseReport // unbound in this flow, so terminal
		tk.Status.CurrentRun = nil
		Expect(k8sClient.Status().Update(ctx, tk)).To(Succeed())

		reconcileOnce()

		Expect(get().Status.Phase).To(Equal(phaseReport),
			"a phase with no binding and nothing in flight already finished — it must not become Failed")

		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(tk.UID)})).To(Succeed())
		Expect(jobs.Items).To(BeEmpty(), "nothing runs after the flow ends")
	})

	// TaskFlow is mutable and independent of Task, so GitOps can delete or
	// rename it out from under a task that already finished. Neither of the
	// framework's own terminal phases needs the flow at all to know it is
	// done, so this must never reach fail().
	It("leaves an Escalated task alone when its flow disappears", func() {
		flow := makeFlow()
		makeHandler()
		makeTask()

		reconcileOnce()
		tk := get()
		tk.Status.Phase = flowv1alpha1.PhaseEscalated
		tk.Status.CurrentRun = nil
		Expect(k8sClient.Status().Update(ctx, tk)).To(Succeed())

		Expect(k8sClient.Delete(ctx, flow)).To(Succeed())

		reconcileOnce()

		Expect(get().Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated),
			"a deleted flow must not overwrite an Escalated task with Failed")
	})

	// Unlike Escalated, a flow-defined terminal status (an ordinary name with
	// no outgoing binding) can only be recognized as terminal by fail's own
	// idempotency guard once the flow that declared it is gone — there is no
	// binding table left to consult.
	It("leaves a flow-defined terminal task alone when its flow disappears", func() {
		flow := makeFlow()
		makeHandler()
		makeTask()

		reconcileOnce()
		tk := get()
		tk.Status.Phase = phaseReport // unbound in this flow, so terminal
		tk.Status.CurrentRun = nil
		Expect(k8sClient.Status().Update(ctx, tk)).To(Succeed())

		Expect(k8sClient.Delete(ctx, flow)).To(Succeed())

		reconcileOnce()

		Expect(get().Status.Phase).To(Equal(phaseReport),
			"a deleted flow must not overwrite a status the flow itself declared terminal")
	})

	// #16 aside, a flow is mutable on its own: nothing stops editing the
	// bindings of a flow a task is already running under. A run in flight
	// (CurrentRun set) tells this apart from a phase that was already
	// terminal on arrival — begin and Advance never set CurrentRun without
	// first confirming a binding, so losing it here can only mean the
	// definition moved out from under a run, which §5 "実行時の矛盾は修復せず
	// Failed" says is a structural fault, not a quiet finish.
	It("fails when the current phase's binding disappears while a run is in flight", func() {
		flow := makeFlow()
		makeHandler()
		makeTask()

		reconcileOnce() // begins the task on phaseInvestigate, setting CurrentRun

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(flow), flow)).To(Succeed())
		flow.Spec.Bindings = map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
			phaseReport: {Handler: name, Next: map[flowv1alpha1.Phase]string{"おわり": "ok"}},
		}
		Expect(k8sClient.Update(ctx, flow)).To(Succeed())

		reconcileOnce()

		Expect(get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed),
			"the phase in flight lost its binding out from under it")
	})

	// The Job name is deterministic, not exclusive — anything with create
	// permission on Jobs in this namespace could have taken it first. This is
	// external interference, not a broken flow definition, so it must not
	// fail the task.
	It("refuses a Job under its name that some other owner already claimed", func() {
		flow := makeFlow()
		makeHandler()
		tk := makeTask()

		controllerTrue := true
		jobName := runner.JobName(name, phaseInvestigate, 1)
		stray := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      jobName,
				Namespace: resourceNamespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: flowv1alpha1.SchemeGroupVersion.String(),
					Kind:       "Task",
					Name:       "someone-elses-task",
					UID:        types.UID("not-" + string(tk.UID)),
					Controller: &controllerTrue,
				}},
			},
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: agentName, Image: agentImage}},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, stray)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, stray) })

		run := &flowv1alpha1.RunRef{Phase: phaseInvestigate, RunID: 1}
		_, err := reconciler.ensureJob(ctx, tk, flow, run)

		Expect(err).To(HaveOccurred())
		var broken brokenFlow
		Expect(errors.As(err, &broken)).To(BeFalse(), "another owner claiming the name is interference, not a flow definition problem")
		Expect(err.Error()).To(ContainSubstring(jobName), "the error must name the Job in question")
		Expect(err.Error()).To(ContainSubstring("someone-elses-task"), "the error must name the actual owner")
	})

	// BuildJob's own validation (a handler bound to a phase other than the one
	// it declares) must actually fail the task when the controller walks into
	// it, not just when runner is tested in isolation.
	It("fails when a handler's phase disagrees with its binding", func() {
		makeFlow()
		h := &flowv1alpha1.TaskHandler{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskHandlerSpec{
				Phase:  phaseReport, // the binding for phaseInvestigate names this handler
				Runner: flowv1alpha1.RunnerSpec{Type: flowv1alpha1.RunnerJob},
				JobTemplate: &flowv1alpha1.JobTemplate{
					Template: flowv1alpha1.PodTemplate{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers:    []corev1.Container{{Name: agentName, Image: agentImage}},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, h)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, h) })
		makeTask()

		reconcileOnce() // begins the task on phaseInvestigate
		reconcileOnce() // tries to build the Job, and the mismatch surfaces here

		Expect(get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
	})

	// The r.Get at the top of ensureJob only ever misses a Job that is
	// genuinely absent when reconciles run one at a time; two reconciles
	// racing to create the same deterministically-named Job is what actually
	// drives Create into AlreadyExists. An interceptor stands in for that
	// race: it forces the first Get to report NotFound once, even though the
	// Job it's about to try creating already exists, so the real apiserver's
	// Create answers with the real AlreadyExists.
	It("treats a Job another reconcile just created as its own", func() {
		makeFlow()
		makeHandler()
		tk := makeTask()

		reconcileOnce() // begins the task

		var handler flowv1alpha1.TaskHandler
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: resourceNamespace}, &handler)).To(Succeed())
		job, err := runner.BuildJob(runner.Input{Task: tk, Handler: &handler, Phase: phaseInvestigate, RunID: 1})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(ctx, job)).To(Succeed())

		watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		forcedNotFound := false
		intercepted := interceptor.NewClient(watchClient, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*batchv1.Job); ok && key.Name == job.Name && !forcedNotFound {
					forcedNotFound = true
					return apierrors.NewNotFound(batchv1.Resource("jobs"), key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})

		racer := &TaskReconciler{Client: intercepted, Scheme: k8sClient.Scheme()}
		_, err = racer.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: resourceNamespace}})
		Expect(err).NotTo(HaveOccurred())
		Expect(forcedNotFound).To(BeTrue(), "the race this test drives at was never exercised")

		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(tk.UID)})).To(Succeed())
		Expect(jobs.Items).To(HaveLen(1), "losing the create race must not produce a second Job")
	})

	// Regression for the cleanup in BeforeEach doing nothing: DeferCleanup runs
	// LIFO, so by the time it fires the Task is already deleted and a lookup
	// by name would find no UID to match against. This drives the same
	// sequence — delete the Task, then delete Jobs by its UID — directly, so
	// a UID resolved too late (after deletion) would leave the Job behind and
	// fail this.
	It("cleans up its Jobs once the task is gone, even though the task is deleted first", func() {
		makeFlow()
		makeHandler()
		tk := makeTask()

		reconcileOnce()
		reconcileOnce() // creates the Job

		jobName := runner.JobName(name, phaseInvestigate, 1)
		var job batchv1.Job
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: jobName, Namespace: resourceNamespace}, &job)).To(Succeed())

		Expect(k8sClient.Delete(ctx, tk)).To(Succeed())
		Expect(k8sClient.DeleteAllOf(ctx, &batchv1.Job{},
			client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(tk.UID)},
			client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())

		Eventually(func() bool {
			return apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Name: jobName, Namespace: resourceNamespace}, &job))
		}).Should(BeTrue(), "the Job must be gone once cleanup runs by the UID captured before deletion")
	})
})

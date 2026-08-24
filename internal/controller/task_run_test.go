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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
	"github.com/Tsuguya/taskflow/internal/runner"
)

const (
	phaseInvestigate flowv1alpha1.Phase = "調査"
	phaseReport      flowv1alpha1.Phase = "報告"
	handlerName                         = "cnp-reader"
)

// Every spec gets its own names. Sharing them let one spec's leftover Job —
// nothing deletes those — decide the next spec's outcome, which is how two of
// these passed alone and failed together.
var specCounter int

var _ = Describe("starting a task", func() {
	ctx := context.Background()
	var name string
	var reconciler *TaskReconciler

	BeforeEach(func() {
		specCounter++
		name = fmt.Sprintf("run-%d", specCounter)
		reconciler = &TaskReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		DeferCleanup(func() {
			// The reconciler's Jobs outlive their Task here: envtest has no
			// garbage collector, so ownerReferences do not remove them.
			_ = k8sClient.DeleteAllOf(ctx, &batchv1.Job{},
				client.InNamespace(resourceNamespace),
				client.MatchingLabels{runner.LabelTaskUID: string(currentUID(ctx, name))})
		})
	})

	// Everything here is named after the test so specs cannot collide.
	makeFlow := func(mut ...func(*flowv1alpha1.TaskFlow)) *flowv1alpha1.TaskFlow {
		f := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskFlowSpec{
				Profile: flowv1alpha1.ProfileInvestigate,
				Start:   phaseInvestigate,
				Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
					phaseInvestigate: {Handler: name, Next: map[flowv1alpha1.Phase]string{phaseReport: "ok"}},
				},
				ReworkBudget: 2,
			},
		}
		for _, m := range mut {
			m(f)
		}
		Expect(k8sClient.Create(ctx, f)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, f) })
		return f
	}

	makeHandler := func() {
		h := &flowv1alpha1.TaskHandler{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskHandlerSpec{
				Phase:  phaseInvestigate,
				Runner: flowv1alpha1.RunnerSpec{Type: flowv1alpha1.RunnerJob},
				JobTemplate: &flowv1alpha1.JobTemplate{
					Template: flowv1alpha1.PodTemplate{
						Metadata: flowv1alpha1.EmbeddedObjectMeta{Labels: map[string]string{"role": handlerName}},
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
	}

	makeTask := func() *flowv1alpha1.Task {
		tk := &flowv1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: resourceNamespace},
			Spec:       flowv1alpha1.TaskSpec{Flow: name},
		}
		Expect(k8sClient.Create(ctx, tk)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, tk) })
		return tk
	}

	reconcileOnce := func() {
		_, err := reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())
	}

	get := func() *flowv1alpha1.Task {
		var tk flowv1alpha1.Task
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: resourceNamespace}, &tk)).To(Succeed())
		return &tk
	}

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

		var jobs batchv1.JobList
		Expect(k8sClient.List(ctx, &jobs, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(tk.UID)})).To(Succeed())
		Expect(jobs.Items).To(BeEmpty(), "nothing runs after the flow ends")
	})
})

// currentUID is the task's UID, or empty when the task is already gone.
func currentUID(ctx context.Context, name string) types.UID {
	var tk flowv1alpha1.Task
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: resourceNamespace}, &tk); err != nil {
		return ""
	}
	return tk.UID
}

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
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
)

// Snapshotting the flow at start pins #180's write side: begin copies the
// flow spec and every bound handler's spec into one ControllerRevision instead
// of only writing status. A begin that only writes status passes the suite
// today and fails every spec here, which is what keeps these red until the
// implementation lands. The snapshot's exact encoding is deliberately not
// pinned — only that both specs are in it — so the specs below read the raw
// payload for markers only the flow or only a handler spec can contain.
var _ = Describe("snapshotting the flow at start", func() {
	var fx *fixture

	BeforeEach(func() {
		fx = newFixture()
	})

	// A begin that writes status without creating anything owned by the task
	// leaves no revision behind, so the lookup below comes back empty.
	It("creates one ControllerRevision holding the flow spec and every bound handler's spec", func() {
		reportHandler := fx.name + "-report"
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings[phaseReport] = flowv1alpha1.PhaseBinding{
				Handler: reportHandler,
				Next:    map[flowv1alpha1.Phase]string{"完了": "ok"},
			}
		})
		fx.makeHandler()
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = reportHandler
			h.Spec.Phase = phaseReport
			h.Spec.JobTemplate.Template.Spec.Containers[0].Image = "example.invalid/agent:v1"
		})
		tk := fx.makeTask()

		fx.reconcile()

		revs := fx.revisionsOwnedBy(tk.UID)
		Expect(revs).To(HaveLen(1))
		rev := revs[0]
		Expect(rev.Name).To(ContainSubstring(fx.name))
		ctrl := metav1.GetControllerOf(&rev)
		Expect(ctrl).NotTo(BeNil())
		Expect(ctrl.Kind).To(Equal("Task"))
		Expect(ctrl.Name).To(Equal(fx.name))
		Expect(ctrl.UID).To(Equal(tk.UID))
		data := string(rev.Data.Raw)
		Expect(data).To(ContainSubstring(`"start":"`+string(phaseInvestigate)+`"`),
			"the flow spec is in the copy, not just the handler specs")
		Expect(data).To(ContainSubstring(agentImage),
			"the handler bound to the starting phase is in the copy")
		Expect(data).To(ContainSubstring("example.invalid/agent:v1"),
			"every binding's handler is in the copy, not just the starting phase's")
	})

	// A begin that does not resolve handlers starts the task and leaves the
	// missing handler for a later reconcile to trip over, so the task is on
	// its starting phase instead of Failed after one reconcile.
	It("fails before the first run when a bound handler is missing", func() {
		fx.makeFlow()
		fx.makeTask() // no handler

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(taskstate.Current(&tk.Status)).To(BeNil())
		jobKey := types.NamespacedName{
			Name:      runner.JobName(fx.name, phaseInvestigate, 1, 0),
			Namespace: resourceNamespace,
		}
		var job batchv1.Job
		Expect(apierrors.IsNotFound(k8sClient.Get(fx.ctx, jobKey, &job))).To(BeTrue(),
			"no Job should exist for a phase that cannot run")
	})

	// A begin that treats a missing finally handler like a missing bound one
	// fails the task here instead of starting it.
	It("begins when only the finally handler is missing", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: fx.name + "-cleanup", Done: answerDone}
		})
		fx.makeHandler()
		fx.makeTask() // no cleanup handler exists

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseInvestigate),
			"a missing cleanup handler does not fail the task")
		revs := fx.revisionsOwnedBy(tk.UID)
		Expect(revs).To(HaveLen(1))
		Expect(string(revs[0].Data.Raw)).To(ContainSubstring(`"done":"`+answerDone+`"`),
			"the flow spec is copied whole, including the finally stanza")
	})

	// A begin that copies the flow spec but skips resolving finally leaves no
	// trace of the cleanup handler's own spec in the copy.
	It("includes the finally handler's spec when it exists", func() {
		const cleanupImage = "example.invalid/agent:cleanup"
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: fx.name + "-cleanup", Done: answerDone}
		})
		fx.makeHandler()
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = cleanupNameFor(fx.name)
			h.Spec.Phase = flowv1alpha1.PhaseFinally
			h.Spec.JobTemplate.Template.Spec.Containers[0].Image = cleanupImage
		})
		tk := fx.makeTask()

		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate))
		revs := fx.revisionsOwnedBy(tk.UID)
		Expect(revs).To(HaveLen(1))
		Expect(string(revs[0].Data.Raw)).To(ContainSubstring(cleanupImage),
			"a finally handler that exists is part of the copy")
	})

	// A begin that never measures the copy starts the task whatever the flow
	// and handlers weigh, so the task below is on its starting phase.
	It("fails before the first run when the copy does not fit in one object", func() {
		const (
			bigHandlers = 40
			fillBytes   = 64 * 1024
		)
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Start = "phase-0"
			f.Spec.Bindings = map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{}
			for i := range bigHandlers {
				phase := flowv1alpha1.Phase(fmt.Sprintf("phase-%d", i))
				f.Spec.Bindings[phase] = flowv1alpha1.PhaseBinding{
					Handler: fmt.Sprintf("%s-h-%d", fx.name, i),
					Next:    map[flowv1alpha1.Phase]string{answerDone: "ok"},
				}
			}
		})
		for i := range bigHandlers {
			fx.makeHandler(oversizedHandlerFor(fx.name, i, fillBytes))
		}
		tk := fx.makeTask()

		fx.reconcile()

		got := fx.get()
		Expect(got.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(taskstate.Current(&got.Status)).To(BeNil())
		Expect(fx.revisionsOwnedBy(tk.UID)).To(BeEmpty(),
			"a copy that fits nowhere leaves nothing behind")
	})

	// A begin that rewrites the revision on every reconcile, or a drive that
	// reads it back, changes the payload or the Job below once the live
	// handler moves on.
	It("never touches the revision again once it exists", func() {
		fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		fx.reconcile()

		revs := fx.revisionsOwnedBy(tk.UID)
		Expect(revs).To(HaveLen(1))
		before := revs[0].ResourceVersion
		dataBefore := string(revs[0].Data.Raw)
		Expect(dataBefore).To(ContainSubstring(agentImage))

		var h flowv1alpha1.TaskHandler
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, &h)).To(Succeed())
		h.Spec.JobTemplate.Template.Spec.Containers[0].Image = "example.invalid/agent:v1"
		Expect(k8sClient.Update(fx.ctx, &h)).To(Succeed())

		fx.reconcile()

		var after appsv1.ControllerRevision
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: revs[0].Name, Namespace: resourceNamespace}, &after)).To(Succeed())
		Expect(after.ResourceVersion).To(Equal(before))
		Expect(string(after.Data.Raw)).To(Equal(dataBefore),
			"the copy is frozen at start: the handler edit must not reach it")

		var job batchv1.Job
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, phaseInvestigate, 1, 0), Namespace: resourceNamespace,
		}, &job)).To(Succeed())
		var image string
		for _, c := range job.Spec.Template.Spec.Containers {
			if c.Name == agentName {
				image = c.Image
			}
		}
		Expect(image).To(Equal("example.invalid/agent:v1"),
			"the run in flight follows the live handler, not the copy")
	})

	// A begin that takes whatever sits under the derived name starts the
	// recreated task on a stale copy instead of writing its own.
	It("does not adopt a revision left under the same name by a deleted task", func() {
		fx.makeFlow()
		fx.makeHandler()
		first := fx.makeTask()

		fx.reconcile()

		Expect(fx.revisionsOwnedBy(first.UID)).To(HaveLen(1))
		Expect(k8sClient.Delete(fx.ctx, first)).To(Succeed())

		fx.makeTask() // same name, a new UID
		second := fx.get()
		Expect(second.UID).NotTo(Equal(first.UID))

		reconcileRaw := func() error {
			_, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
			})
			return err
		}
		if err := reconcileRaw(); err != nil || fx.get().Status.Phase == "" {
			// A name derived from the task alone still collides here: envtest
			// runs no garbage collector, so the first task's revision is
			// still under it. A controller that reports that clash and
			// retries gets the collection production would have done on its
			// own, then one more chance. (A name carrying the UID never
			// collides, and skips this branch entirely.)
			fx.deleteRevisionsOwnedBy(first.UID)
			fx.reconcile()
		}

		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate))
		own := fx.revisionsOwnedBy(second.UID)
		Expect(own).To(HaveLen(1))
		Expect(string(own[0].Data.Raw)).To(ContainSubstring(agentImage),
			"the recreated task runs on its own copy, not the deleted task's")
	})
})

// revisionsOwnedBy lists the ControllerRevisions in the test namespace this
// task owns. Looking the copy up through the ownerReference is what lets a
// spec find it without any pointer in status — the same property the issue
// requires of the implementation.
func (fx *fixture) revisionsOwnedBy(uid types.UID) []appsv1.ControllerRevision {
	var list appsv1.ControllerRevisionList
	Expect(k8sClient.List(fx.ctx, &list, client.InNamespace(resourceNamespace))).To(Succeed())
	var out []appsv1.ControllerRevision
	for _, rev := range list.Items {
		for _, ref := range rev.OwnerReferences {
			if ref.Controller != nil && *ref.Controller && ref.UID == uid {
				out = append(out, rev)
			}
		}
	}
	return out
}

// deleteRevisionsOwnedBy stands in for the garbage collector envtest does not
// run, so a spec can give a retrying controller the collection production
// would have done on its own.
func (fx *fixture) deleteRevisionsOwnedBy(uid types.UID) {
	for _, rev := range fx.revisionsOwnedBy(uid) {
		Expect(k8sClient.Delete(fx.ctx, &rev)).To(Succeed())
	}
}

// cleanupNameFor names the finally handler apart from the phase handler,
// because the flow names the two separately.
func cleanupNameFor(taskName string) string {
	return taskName + "-cleanup"
}

// oversizedHandlerFor builds one handler whose spec carries fillBytes of
// padding inside the pod template's metadata, so N of them together weigh
// more than one object may hold while each one alone is an ordinary write.
// The padding sits in the spec rather than the object's own annotations so it
// counts toward a copy of the specs.
func oversizedHandlerFor(taskName string, i, fillBytes int) func(*flowv1alpha1.TaskHandler) {
	return func(h *flowv1alpha1.TaskHandler) {
		h.Name = fmt.Sprintf("%s-h-%d", taskName, i)
		h.Spec.Phase = flowv1alpha1.Phase(fmt.Sprintf("phase-%d", i))
		h.Spec.JobTemplate.Template.Metadata.Annotations = map[string]string{
			"fill": strings.Repeat("x", fillBytes),
		}
	}
}

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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/snapshot"
)

// These specs pin the copy the task's start writes (#180).
var _ = Describe("the start-time copy of the flow and its handlers", func() {
	var fx *fixture

	BeforeEach(func() {
		fx = newFixture()
		DeferCleanup(func() {
			// envtest runs no garbage collector, so an owned revision
			// stays after its task is gone (#180).
			if fx.taskUID == "" {
				return
			}
			var list appsv1.ControllerRevisionList
			Expect(k8sClient.List(fx.ctx, &list, client.InNamespace(resourceNamespace))).To(Succeed())
			for i := range list.Items {
				rev := &list.Items[i]
				for _, o := range rev.OwnerReferences {
					if string(o.UID) == string(fx.taskUID) {
						_ = k8sClient.Delete(fx.ctx, rev)
						break
					}
				}
			}
		})
	})

	revisionsOf := func(task *flowv1alpha1.Task) []appsv1.ControllerRevision {
		var list appsv1.ControllerRevisionList
		Expect(k8sClient.List(fx.ctx, &list, client.InNamespace(resourceNamespace))).To(Succeed())
		var out []appsv1.ControllerRevision
		for _, rev := range list.Items {
			if metav1.IsControlledBy(&rev, task) {
				out = append(out, rev)
			}
		}
		return out
	}

	named := func(prefix string) string { return fx.name + "-" + prefix }

	It("copies the flow and the bound handler into one revision before the first run", func() {
		flow := fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		fx.reconcile() // begin

		started := fx.get()
		Expect(started.Status.Phase).To(Equal(phaseInvestigate))

		revs := revisionsOf(tk)
		Expect(revs).To(HaveLen(1), "starting a task must leave exactly one owned ControllerRevision behind")
		rev := revs[0]
		Expect(metav1.IsControlledBy(&rev, tk)).To(BeTrue(),
			"the revision must be owned by the task so it is collected with it")
		got, err := snapshot.Decode(rev.Data.Raw)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Flow).To(Equal(flow.Spec),
			"the copy must carry the flow spec the task started from")

		Expect(got.Handlers).To(HaveKey(fx.name),
			"a revision that embeds only the flow, no handlers, is caught here")
	})

	It("copies every binding's handler and the finally handler", func() {
		handler2 := named("second")
		cleanup := named("cleanup")
		fx.makeFlow(
			func(f *flowv1alpha1.TaskFlow) {
				f.Spec.Bindings[phaseReport] = flowv1alpha1.PhaseBinding{
					Handler: handler2, Next: map[flowv1alpha1.Phase]string{"done": "ok"},
				}
			},
			func(f *flowv1alpha1.TaskFlow) {
				f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: cleanup, Done: "cleaned"}
			},
		)
		fx.makeHandler()
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = handler2
			h.Spec.Phase = phaseReport
		})
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = cleanup
			h.Spec.Phase = flowv1alpha1.PhaseFinally
		})
		tk := fx.makeTask()

		fx.reconcile() // begin

		revs := revisionsOf(tk)
		Expect(revs).To(HaveLen(1))
		got, err := snapshot.Decode(revs[0].Data.Raw)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.Handlers).To(HaveLen(3))
		Expect(got.Handlers).To(HaveKey(fx.name))
		Expect(got.Handlers).To(HaveKey(handler2))
		Expect(got.Handlers).To(HaveKey(cleanup))
		Expect(got.FinallyAbsent).To(BeFalse())
	})

	It("fails before the first run when a binding names a handler that does not exist", func() {
		fx.makeFlow()
		// No handler: the flow's binding names fx.name, which is absent.
		tk := fx.makeTask()

		fx.reconcile() // begin

		got := fx.get()
		Expect(got.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed),
			"a missing bound handler must stop the task at start, before any run")
		Expect(revisionsOf(tk)).To(BeEmpty(),
			"a task that fails at start must leave no copy behind")
	})

	It("still starts when only the finally handler is missing, and leaves it out of the copy", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: named("cleanup"), Done: "cleaned"}
		})
		fx.makeHandler()
		tk := fx.makeTask() // no cleanup handler exists

		fx.reconcile() // begin

		started := fx.get()
		Expect(started.Status.Phase).To(Equal(phaseInvestigate),
			"a missing finally handler must not stop the task from starting")

		revs := revisionsOf(tk)
		Expect(revs).To(HaveLen(1))
		got, err := snapshot.Decode(revs[0].Data.Raw)
		Expect(err).NotTo(HaveOccurred())
		Expect(got.FinallyAbsent).To(BeTrue(),
			"a start that copies an empty handler for the absent finally is caught here")
		Expect(got.Handlers).To(HaveKey(fx.name))
	})

	It("never touches the revision again once it exists", func() {
		fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		fx.reconcile() // begin writes the copy
		before := revisionsOf(tk)
		Expect(before).To(HaveLen(1))

		fx.reconcile() // driveRun: must not rewrite the copy

		after := revisionsOf(tk)
		Expect(after).To(HaveLen(1))
		Expect(after[0].ResourceVersion).To(Equal(before[0].ResourceVersion),
			"the revision is written once at start and never modified after")
	})

	It("does not adopt a revision left under the same name by a deleted task", func() {
		fx.makeFlow()
		fx.makeHandler()
		first := fx.makeTask()

		fx.reconcile() // begin writes the first task's copy
		Expect(revisionsOf(first)).To(HaveLen(1))

		// revisionsOf matches on the owner UID, so one revision for the
		// new UID says its start wrote its own rather than adopting the stale copy.
		Expect(k8sClient.Delete(fx.ctx, first)).To(Succeed())
		second := fx.makeTask()
		Expect(second.UID).NotTo(Equal(first.UID))

		fx.reconcile() // begin for the second generation

		fresh := revisionsOf(second)
		Expect(fresh).To(HaveLen(1), "the recreated task must get a copy of its own")
	})

	It("fails before the first run when the copy does not fit in one object", func() {
		// Each half below fits in its own object; the two together do not
		// fit in one. A run's own vocabulary is free text the copy carries.
		big := strings.Repeat("d", 900*1024)
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings[phaseInvestigate] = flowv1alpha1.PhaseBinding{
				Handler: fx.name, Next: map[flowv1alpha1.Phase]string{phaseReport: big},
			}
		})
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Spec.JobTemplate.Template.Spec.Containers[0].Env = []corev1.EnvVar{
				{Name: "FILLER", Value: big},
			}
		})
		tk := fx.makeTask()

		fx.reconcile() // begin

		got := fx.get()
		Expect(got.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed),
			"a copy that does not fit in one object must stop the task at start")
		Expect(revisionsOf(tk)).To(BeEmpty(),
			"a task that fails at start must leave no copy behind")
	})
})

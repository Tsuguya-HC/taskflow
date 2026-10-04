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
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/metrics"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// 始まって止まっていない Task は、写しの上で走る。写しを作った事実は
// DefinitionsPinned 条件に残り、写しが無くて marker もある Task は Failed に
// なる。写しも marker も無い Task は、live の定義から写しを作る (#183)。
var _ = Describe("a task's record that its definitions are copied", func() {
	var fx *fixture
	var clock time.Time

	const (
		timeout      = time.Hour
		succeededTTL = time.Hour
		failedTTL    = 168 * time.Hour
		editedImage  = "example.invalid/agent:edited"
	)

	BeforeEach(func() {
		fx = newFixture()
		clock = time.Now().Truncate(time.Second)
		fx.reconciler.Now = func() time.Time { return clock }
	})

	withTTL := func(f *flowv1alpha1.TaskFlow) {
		f.Spec.TTL = &flowv1alpha1.TTLSpec{
			Succeeded: &metav1.Duration{Duration: succeededTTL},
			Failed:    &metav1.Duration{Duration: failedTTL},
		}
	}

	liveKey := func() types.NamespacedName {
		return types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}
	}

	cleanupName := func() string { return fx.name + "-cleanup" }

	withFinally := func(f *flowv1alpha1.TaskFlow) {
		f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: cleanupName(), Done: doneSwept}
	}

	makeCleanupHandler := func() {
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = cleanupName()
			h.Spec.Phase = flowv1alpha1.PhaseFinally
		})
	}

	// withBrokenBinding binds a phase the task is not at to a handler that
	// does not exist: the fault begin finds, and finds only by looking at
	// every binding rather than at the phase the task stands on.
	withBrokenBinding := func(f *flowv1alpha1.TaskFlow) {
		f.Spec.Bindings[phaseReport] = flowv1alpha1.PhaseBinding{
			Handler: fx.name + "-gone",
			Next:    map[flowv1alpha1.Phase]string{phaseDone: "ok"},
		}
	}

	copiedIn := func() snapshot {
		revs := fx.revisions()
		Expect(revs).To(HaveLen(1), "the copy was made")
		var got snapshot
		Expect(json.Unmarshal(revs[0].Data.Raw, &got)).To(Succeed())
		return got
	}

	lostCounter := func() prometheus.Counter {
		return metrics.TaskOutcomes.With(prometheus.Labels{
			metrics.LabelFlow: metrics.FlowUnresolved, metrics.LabelPhase: string(flowv1alpha1.PhaseFailed),
			metrics.LabelSeverity: string(transition.EndingFailed),
		})
	}

	Context("a task that has its copy and no marker", func() {
		It("is marked, with the copy left as it is and the task carried on", func() {
			fx.makeFlow()
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.dropMarker()
			stamp := fx.revisions()[0].ResourceVersion

			fx.reconcile()
			fx.reconcile()

			tk := fx.get()
			Expect(pinnedOf(tk)).NotTo(BeNil(), "a task that began before the marker existed gets it")
			Expect(pinnedOf(tk).Status).To(Equal(metav1.ConditionTrue))
			Expect(pinnedOf(tk).Reason).To(Equal(reasonCopied))
			Expect(fx.revisions()).To(HaveLen(1))
			Expect(fx.revisions()[0].ResourceVersion).To(Equal(stamp), "the copy is not touched")
			Expect(tk.Status.Phase).To(Equal(phaseInvestigate))
			Expect(jobsOf(fx)).To(HaveLen(1), "the task goes on from where it was")
		})

		// Only a True marker counts, and nothing reads its reason.
		DescribeTable("takes a condition for the marker only when it is True",
			func(status metav1.ConditionStatus, reason string, present bool) {
				fx.makeFlow()
				fx.makeHandler()
				fx.makeTask()
				fx.reconcile() // begin
				fx.dropCopy()
				fx.setPinned(status, reason)

				fx.reconcile()

				tk := fx.get()
				if present {
					Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed), "the task had a copy, and has lost it")
					Expect(readyOf(tk).Reason).To(Equal(reasonLost))
					return
				}
				Expect(tk.Status.Phase).To(Equal(phaseInvestigate), "a marker that is not True says nothing, so the task is migrated")
				Expect(fx.revisions()).To(HaveLen(1))
				Expect(pinnedOf(tk).Status).To(Equal(metav1.ConditionTrue))
				Expect(pinnedOf(tk).Reason).To(Equal(reasonCopied))
			},
			Entry("True, whatever the reason", metav1.ConditionTrue, "SomethingElse", true),
			Entry("False", metav1.ConditionFalse, reasonCopied, false),
			Entry("Unknown", metav1.ConditionUnknown, reasonCopied, false),
		)
	})

	Context("migration of a task with no copy and no marker", func() {
		It("makes the copy from the flow and the handlers a binding names, as they are then", func() {
			fx.makeFlow(withBrokenBinding)
			fx.makeHandler()
			fx.makeHandler(func(h *flowv1alpha1.TaskHandler) { h.Name = fx.name + "-gone" })
			fx.bareTask()
			fx.editHandler(func(h *flowv1alpha1.TaskHandler) {
				h.Spec.JobTemplate.Template.Spec.Containers[0].Image = editedImage
			})
			var live flowv1alpha1.TaskFlow
			Expect(k8sClient.Get(fx.ctx, liveKey(), &live)).To(Succeed())

			fx.reconcile()

			got := copiedIn()
			Expect(got.Flow).To(Equal(live.Spec))
			Expect(got.Handlers).To(HaveLen(2), "one for each binding")
			Expect(got.Handlers[fx.name].JobTemplate.Template.Spec.Containers[0].Image).To(Equal(editedImage))
			Expect(pinnedOf(fx.get())).NotTo(BeNil(), "the marker follows the copy")
			Expect(pinnedOf(fx.get()).Status).To(Equal(metav1.ConditionTrue))
			Expect(pinnedOf(fx.get()).Reason).To(Equal(reasonCopied))
		})

		It("copies the finally handler when it exists", func() {
			fx.makeFlow(withFinally)
			fx.makeHandler()
			makeCleanupHandler()
			fx.bareTask()

			fx.reconcile()

			Expect(copiedIn().Handlers).To(HaveKey(cleanupName()))
		})

		It("leaves the finally handler out when it does not exist, and does not fail the task", func() {
			fx.makeFlow(withFinally)
			fx.makeHandler()
			fx.bareTask()

			fx.reconcile()

			Expect(copiedIn().Handlers).NotTo(HaveKey(cleanupName()))
			Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate))
			Expect(pinnedOf(fx.get())).NotTo(BeNil())
		})

		It("is not reached by an edit of the live definitions made after it", func() {
			fx.makeFlow()
			fx.makeHandler(stateRunner(timeout))
			fx.bareTask()
			fx.reconcile() // migrates
			fx.reconcile() // whichever of the two opens the place run 1 is answered in
			Expect(pinnedOf(fx.get())).NotTo(BeNil(), "the task migrated")

			var flow flowv1alpha1.TaskFlow
			Expect(k8sClient.Get(fx.ctx, liveKey(), &flow)).To(Succeed())
			flow.Spec.Bindings[phaseInvestigate] = flowv1alpha1.PhaseBinding{
				Handler: fx.name, Next: map[flowv1alpha1.Phase]string{phaseElsewhere: "ok"},
			}
			Expect(k8sClient.Update(fx.ctx, &flow)).To(Succeed())
			fx.deleteHandler()
			fx.answer("ok", "")
			fx.reconcile()

			Expect(fx.get().Status.Phase).To(Equal(phaseReport), "ok leads where the copy says, not where the live flow now says")
		})

		It("fails the task when a binding names a handler that does not exist, as begin does", func() {
			fx.makeFlow(withTTL, withBrokenBinding)
			fx.makeHandler()
			fx.bareTask()

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
			Expect(readyOf(tk).Message).To(ContainSubstring(fx.name + "-gone"))
			Expect(tk.Status.ExpiresAt).NotTo(BeNil(), "the date comes from the live flow, as when begin fails")
			Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
			Expect(fx.revisions()).To(BeEmpty())
			Expect(pinnedOf(tk)).To(BeNil(), "no copy was made, so there is nothing to say was")
		})

		It("owes the live flow's cleanup run when it fails", func() {
			fx.makeFlow(withTTL, withFinally, withBrokenBinding)
			fx.makeHandler()
			makeCleanupHandler()
			fx.bareTask()

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
			Expect(taskstate.InFinally(&tk.Status)).To(BeTrue(), "the live flow declares a cleanup, and begin's failure owes it too")
			Expect(tk.Status.ExpiresAt).To(BeNil(), "the date waits for the cleanup run")
		})

		// 変異: 何も走っていない Task は fail() の Idle の守りに止められる。
		It("fails a task with nothing in flight", func() {
			fx.makeFlow(withTTL, withBrokenBinding)
			fx.makeHandler()
			fx.bareTask()
			fx.idle()

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed), "recovery state is not a reason to keep running on live definitions")
			Expect(readyOf(tk).Message).To(ContainSubstring(fx.name + "-gone"))
			Expect(tk.Status.ExpiresAt).NotTo(BeNil())
			Expect(jobsOf(fx)).To(BeEmpty())
		})

		// 1 handler は載る大きさにして、束ねた写しだけが載らないようにする。
		DescribeTable("fails the task when the copy does not fit in one object",
			func(big int) {
				blobOf := func(seed byte) string {
					blob := make([]byte, 900<<10)
					for i := range blob {
						blob[i] = seed + byte(i%26)
					}
					return string(blob)
				}
				fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
					for i := range big {
						binding := f.Spec.Bindings[phaseInvestigate]
						binding.Handler = fmt.Sprintf("%s-big-%d", fx.name, i)
						f.Spec.Bindings[flowv1alpha1.Phase(fmt.Sprintf("束-%d", i))] = binding
					}
				})
				fx.makeHandler()
				for i := range big {
					name := fmt.Sprintf("%s-big-%d", fx.name, i)
					seed := byte('a' + i)
					fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
						h.Name = name
						c := &h.Spec.JobTemplate.Template.Spec.Containers[0]
						c.Env = append(c.Env, corev1.EnvVar{Name: "BLOB", Value: blobOf(seed)})
					})
				}
				fx.bareTask()

				fx.reconcile()

				tk := fx.get()
				Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
				Expect(readyOf(tk).Message).To(ContainSubstring("do not fit in one object"))
				Expect(fx.revisions()).To(BeEmpty())
				Expect(pinnedOf(tk)).To(BeNil())
			},
			Entry("refused by the storage under the apiserver", 2),
			Entry("refused by the apiserver's body limit", 4),
		)

		// 変異: flow が無いときの失敗の経路を移行の後ろに回す。
		It("changes nothing about a task with a run in flight when the live flow does not exist", func() {
			fx.bareTask()

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
			Expect(readyOf(tk).Message).To(ContainSubstring("does not exist"))
			Expect(tk.Status.ExpiresAt).To(BeNil(), "there is no flow to read a ttl from")
			Expect(fx.revisions()).To(BeEmpty())
			Expect(pinnedOf(tk)).To(BeNil())
		})

		// 変異: fail() の Idle の守りを外す。
		It("leaves a task with nothing in flight alone when the live flow does not exist", func() {
			fx.bareTask()
			fx.idle()
			before := fx.get()

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status).To(Equal(before.Status))
			Expect(fx.revisions()).To(BeEmpty())
		})
	})

	Context("a task whose copy was deleted", func() {
		// 変異: 写しが無ければ live の定義で走らせる。
		// 変異: Failed にしても live の finally の掃除を負わせる。
		It("fails with DefinitionsLost, owing no cleanup run and no date, while a run is in flight", func() {
			fx.makeFlow(withTTL, withFinally)
			fx.makeHandler()
			makeCleanupHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.dropCopy()
			before := testutil.ToFloat64(lostCounter())

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
			Expect(readyOf(tk)).NotTo(BeNil())
			Expect(readyOf(tk).Status).To(Equal(metav1.ConditionFalse))
			Expect(readyOf(tk).Reason).To(Equal(reasonLost))
			Expect(readyOf(tk).Message).NotTo(BeEmpty())
			Expect(tk.Status.CurrentRuns).To(BeEmpty(), "the flow declares finally, but the live one is not what this task pinned")
			Expect(tk.Status.ExpiresAt).To(BeNil(), "no ttl is written at the moment it fails")
			Expect(jobsOf(fx)).To(BeEmpty(), "neither the run nor a cleanup was started")
			Expect(fx.revisions()).To(BeEmpty(), "a lost copy is not made again")
			Expect(testutil.ToFloat64(lostCounter())).To(Equal(before+1), "counted under the flow label of a flow that is unknown")

			fx.reconcile()

			tk = fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
			Expect(readyOf(tk).Reason).To(Equal(reasonLost))
			Expect(tk.Status.ExpiresAt).NotTo(BeNil(), "the reserved-phase branch dates it from the live flow of the same name")
			Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
			Expect(jobsOf(fx)).To(BeEmpty())
			Expect(testutil.ToFloat64(lostCounter())).To(Equal(before+1), "backfilling the date does not count the ending again")
		})

		// 変異: 実行中の Task でも live の flow を先に読む。
		It("fails with DefinitionsLost, not with a missing flow, when the live flow is gone too", func() {
			flow := fx.makeFlow()
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.dropCopy()
			Expect(k8sClient.Delete(fx.ctx, flow)).To(Succeed())

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
			Expect(readyOf(tk).Reason).To(Equal(reasonLost))
			Expect(tk.Status.ExpiresAt).To(BeNil(), "there is no flow to read a ttl from")
		})

		// 変異: fail() の Idle の守りに止められる。
		It("fails with DefinitionsLost when nothing is in flight", func() {
			fx.makeFlow()
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.idle()
			fx.dropCopy()

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed), "recovery state is not a reason to keep running on live definitions")
			Expect(readyOf(tk).Reason).To(Equal(reasonLost))
			Expect(jobsOf(fx)).To(BeEmpty())
		})

		// 変異: fail() の Idle の守りを外す。
		It("is left alone when nothing is in flight and the live flow is gone, for it cannot be told from a task that stopped", func() {
			flow := fx.makeFlow()
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.idle()
			fx.dropCopy()
			Expect(k8sClient.Delete(fx.ctx, flow)).To(Succeed())
			before := fx.get()

			fx.reconcile()

			Expect(fx.get().Status).To(Equal(before.Status))
		})

		// A task that stopped is owed nothing by its copy: what it still owes is
		// read from the live definitions, and losing the copy does not turn it
		// into a failure.
		It("is not failed when it had already stopped, and is dated from the live flow", func() {
			fx.makeFlow(withTTL)
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			stopped := fx.get()
			stopped.Status.Phase = phaseReport // the flow binds nothing here
			stopped.Status.CurrentRuns = nil
			Expect(k8sClient.Status().Update(fx.ctx, stopped)).To(Succeed())
			fx.dropCopy()

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(phaseReport))
			Expect(readyOf(tk)).To(BeNil())
			Expect(tk.Status.ExpiresAt).NotTo(BeNil())
			Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(succeededTTL)))
			Expect(fx.revisions()).To(BeEmpty())
		})
	})
})

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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/metrics"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// 開始して止まっていない Task は、必ず自分の写しで動く (#183)。写しができたことは
// condition DefinitionsPinned に残り、写しの無い Task は、印が無ければ作り直され
// （古い Task）、印があれば Failed になる（写しが消された Task）。止まった Task は
// どちらでもなく、残りの仕事を live の定義から読む。
var _ = Describe("a task's copy of its definitions", func() {
	var fx *fixture
	var clock time.Time

	const (
		timeout      = time.Hour
		succeededTTL = time.Hour
		failedTTL    = 168 * time.Hour
		editedImage  = "example.invalid/agent:edited"
		laterImage   = "example.invalid/agent:later"
	)

	BeforeEach(func() {
		fx = newFixture()
		clock = time.Now().Truncate(time.Second)
		fx.reconciler.Now = func() time.Time { return clock }
		DeferCleanup(func() {
			if fx.taskUID == "" {
				return
			}
			// envtest has no garbage collector, so a task's revisions outlive it.
			_ = k8sClient.DeleteAllOf(fx.ctx, &appsv1.ControllerRevision{},
				client.InNamespace(resourceNamespace),
				client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID)})
		})
	})

	withTTL := func(f *flowv1alpha1.TaskFlow) {
		f.Spec.TTL = &flowv1alpha1.TTLSpec{
			Succeeded: &metav1.Duration{Duration: succeededTTL},
			Failed:    &metav1.Duration{Duration: failedTTL},
		}
	}

	cleanupName := func() string { return fx.name + "-cleanup" }

	withCleanup := func(f *flowv1alpha1.TaskFlow) {
		f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: cleanupName(), Done: answerCleaned}
	}

	// cleanupHandler is the handler the flow's finally names, as a Job.
	cleanupHandler := func(h *flowv1alpha1.TaskHandler) {
		h.Name = cleanupName()
		h.Spec.Phase = flowv1alpha1.PhaseFinally
	}

	// withMissingHandler binds a second phase to a handler nothing made, so a
	// copy of the flow cannot be built: begin refuses such a flow, and so must
	// whatever makes the copy later.
	withMissingHandler := func(f *flowv1alpha1.TaskFlow) {
		f.Spec.Bindings[phaseReport] = flowv1alpha1.PhaseBinding{
			Handler: fx.name + "-gone",
			Next:    map[flowv1alpha1.Phase]string{phaseDone: "ok"},
		}
	}

	revisionsOf := func() []appsv1.ControllerRevision {
		var list appsv1.ControllerRevisionList
		Expect(k8sClient.List(fx.ctx, &list, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID)})).To(Succeed())
		return list.Items
	}

	storedHandler := func(name string) flowv1alpha1.TaskHandlerSpec {
		var h flowv1alpha1.TaskHandler
		Expect(k8sClient.Get(fx.ctx, client.ObjectKey{Name: name, Namespace: resourceNamespace}, &h)).To(Succeed())
		return h.Spec
	}

	readyOf := func(tk *flowv1alpha1.Task) *metav1.Condition {
		return meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
	}

	failedOutcome := func(flow string) prometheus.Labels {
		return prometheus.Labels{
			metrics.LabelFlow: flow, metrics.LabelPhase: string(flowv1alpha1.PhaseFailed),
			metrics.LabelSeverity: string(transition.EndingFailed),
		}
	}

	// begin から先に、写しはあるが印の無い Task。
	Context("a task that began after the copy existed and before the marker did", func() {
		// 変異: 印を begin でしか書かない。
		It("is marked on its next reconcile, and the copy is left as it is", func() {
			fx.makeFlow()
			fx.makeHandler(stateRunner(timeout))
			fx.makeTask()
			fx.reconcile() // begin
			fx.dropMarker()
			Expect(pinned(fx.get())).To(BeFalse(), "the spec starts from a task with no marker")
			revs := revisionsOf()
			Expect(revs).To(HaveLen(1))

			fx.reconcile()

			expectPinned(fx.get())
			after := revisionsOf()
			Expect(after).To(HaveLen(1))
			Expect(after[0].ResourceVersion).To(Equal(revs[0].ResourceVersion), "the copy it has is not made again")
		})

		It("carries on from its copy once marked", func() {
			fx.makeFlow()
			fx.makeHandler(stateRunner(timeout))
			fx.makeTask()
			fx.reconcile() // begin
			fx.dropMarker()

			fx.reconcile() // marks the task, and may go on to open the verdict box
			fx.reconcile() // opens the place run 1 is answered in
			fx.answer("ok", "")
			fx.reconcile()

			tk := fx.get()
			expectPinned(tk)
			Expect(tk.Status.Phase).To(Equal(phaseReport))
		})
	})

	// begin より前に始まった（写しも印も無い）Task は、次の reconcile で写しを作る。
	Context("a task that began before the copy existed", func() {
		imageOf := func() string { return fx.job(1).Spec.Template.Spec.Containers[0].Image }

		// 変異: 写しは作るが印を書かない / 写しを作る前に印を書く / 写しを作らず live を読み続ける。
		DescribeTable("gets a copy made from the definitions as they are now, then the marker",
			func(inFlight bool) {
				flow := fx.makeFlow(withTTL)
				fx.makeHandler()
				tk := fx.startedWithoutBegin(inFlight)
				fx.editHandler(func(h *flowv1alpha1.TaskHandler) {
					h.Spec.JobTemplate.Template.Spec.Containers[0].Image = editedImage
				})

				fx.reconcile()

				got := fx.get()
				expectPinned(got)
				Expect(got.Status.Phase).To(Equal(phaseInvestigate), "nothing about the task but the marker changed")
				held, ok := fx.copyHeld()
				Expect(ok).To(BeTrue(), "the copy is made at the migration")
				Expect(held.Flow).To(Equal(flow.Spec))
				Expect(held.Handlers).To(Equal(map[string]flowv1alpha1.TaskHandlerSpec{fx.name: storedHandler(fx.name)}))
				Expect(held.Handlers[fx.name].JobTemplate.Template.Spec.Containers[0].Image).To(Equal(editedImage),
					"the handler as it is at the migration, not as it was when the task started")
				revs := revisionsOf()
				Expect(revs).To(HaveLen(1))
				Expect(metav1.IsControlledBy(&revs[0], tk)).To(BeTrue(), "the copy goes with the task")
			},
			Entry("with a run in flight", true),
			Entry("with nothing in flight", false),
		)

		// 変異: 写しを作った後も live の handler / flow を読む。
		DescribeTable("is not reached by an edit of the live definitions made after the migration",
			func(inFlight bool) {
				flow := fx.makeFlow(withTTL)
				fx.makeHandler()
				fx.startedWithoutBegin(inFlight)
				fx.editHandler(func(h *flowv1alpha1.TaskHandler) {
					h.Spec.JobTemplate.Template.Spec.Containers[0].Image = editedImage
				})
				fx.reconcile() // migrates

				fx.editHandler(func(h *flowv1alpha1.TaskHandler) {
					h.Spec.JobTemplate.Template.Spec.Containers[0].Image = laterImage
				})
				Expect(k8sClient.Delete(fx.ctx, flow)).To(Succeed())
				for range 3 {
					fx.reconcile()
				}

				Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate), "a deleted live flow is no reason to fail a task with a copy")
				Expect(imageOf()).To(Equal(editedImage))
			},
			Entry("with a run in flight", true),
			Entry("with nothing in flight", false),
		)

		// 変異: finally の handler を写しに入れない / 無いのに入れようとして Failed にする。
		It("has the finally handler in its copy when it exists", func() {
			fx.makeFlow(withCleanup)
			fx.makeHandler()
			fx.makeHandler(cleanupHandler)
			fx.startedWithoutBegin(true)

			fx.reconcile()

			held, ok := fx.copyHeld()
			Expect(ok).To(BeTrue())
			Expect(held.Handlers).To(HaveKeyWithValue(cleanupName(), storedHandler(cleanupName())))
		})

		It("has a copy without the finally handler when it does not exist", func() {
			fx.makeFlow(withCleanup)
			fx.makeHandler()
			fx.startedWithoutBegin(true)

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(phaseInvestigate), "a missing cleanup handler does not fail the task")
			expectPinned(tk)
			held, ok := fx.copyHeld()
			Expect(ok).To(BeTrue())
			Expect(held.Handlers).To(HaveLen(1), "the copy is made without the missing handler")
		})

		// 変異: 写せない定義の Task を Failed にしない（活きた run や、何も走っていない
		// 状態では特に）。
		DescribeTable("fails when a binding names a handler that does not exist",
			func(inFlight bool) {
				fx.makeFlow(withTTL, withMissingHandler)
				fx.makeHandler()
				fx.startedWithoutBegin(inFlight)

				fx.reconcile()

				tk := fx.get()
				Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
				ready := readyOf(tk)
				Expect(ready).NotTo(BeNil())
				Expect(ready.Status).To(Equal(metav1.ConditionFalse))
				Expect(ready.Reason).To(Equal("FlowBroken"), "the reason begin gives")
				Expect(ready.Message).To(ContainSubstring(fx.name + "-gone"))
				Expect(ready.Message).To(ContainSubstring("does not exist"))
				Expect(tk.Status.CurrentRuns).To(BeEmpty())
				Expect(tk.Status.ExpiresAt).NotTo(BeNil(), "the ttl is the live flow's, as when begin fails")
				Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
				Expect(pinned(tk)).To(BeFalse(), "no copy was made, so there is nothing to say was copied")
				Expect(revisionsOf()).To(BeEmpty())
				Expect(jobsOf(fx)).To(BeEmpty())
			},
			Entry("with a run in flight", true),
			Entry("with nothing in flight", false),
		)

		// 変異: 大きすぎる写しの拒否の形の判定を 1 つ落とす。
		DescribeTable("fails when the copy does not fit in one object",
			func(big int, inFlight bool) {
				blobOf := func(seed byte) string {
					blob := make([]byte, 900<<10)
					for i := range blob {
						blob[i] = seed + byte(i%26)
					}
					return string(blob)
				}
				fx.makeFlow(withTTL, func(f *flowv1alpha1.TaskFlow) {
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
				fx.startedWithoutBegin(inFlight)

				fx.reconcile()

				tk := fx.get()
				Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
				Expect(readyOf(tk).Message).To(ContainSubstring("do not fit in one object"))
				Expect(pinned(tk)).To(BeFalse())
				Expect(revisionsOf()).To(BeEmpty())
			},
			Entry("refused by the storage under the apiserver, with a run in flight", 2, true),
			Entry("refused by the apiserver's body limit, with a run in flight", 4, true),
			Entry("refused by the storage under the apiserver, with nothing in flight", 2, false),
		)

		// 今のコードでも通る（要求は「何も変わらない」こと）。untestable.md に理由。
		It("fails a run in flight with no ttl, and makes no copy, when the live flow does not exist", func() {
			fx.startedWithoutBegin(true)

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
			Expect(readyOf(tk).Message).To(ContainSubstring("does not exist in this namespace"))
			Expect(tk.Status.ExpiresAt).To(BeNil())
			Expect(pinned(tk)).To(BeFalse())
			Expect(revisionsOf()).To(BeEmpty())
		})

		It("is left as it is, with no copy made, when nothing is in flight and the live flow does not exist", func() {
			fx.startedWithoutBegin(false)

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(phaseInvestigate))
			Expect(tk.Status.CurrentRuns).To(BeEmpty())
			Expect(tk.Status.ExpiresAt).To(BeNil())
			Expect(pinned(tk)).To(BeFalse())
			Expect(revisionsOf()).To(BeEmpty())
		})
	})

	// 印があるのに写しが無い Task は、写しが消された。
	Context("a task whose copy was deleted", func() {
		lost := func(tk *flowv1alpha1.Task) {
			ExpectWithOffset(1, tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
			ready := readyOf(tk)
			ExpectWithOffset(1, ready).NotTo(BeNil())
			ExpectWithOffset(1, ready.Status).To(Equal(metav1.ConditionFalse))
			ExpectWithOffset(1, ready.Reason).To(Equal(lostReason))
			ExpectWithOffset(1, ready.Message).To(ContainSubstring("copy"), "the message says the task's copy is missing")
		}

		// 変異: 写しが無ければ live の flow に戻る / 掃除の run を live の finally から負う /
		// live の flow が無いと「flow does not exist」で落とす / 計数を flow 名のラベルで行う。
		DescribeTable("fails with a run in flight, owing no cleanup run and no ttl yet",
			func(deleteLiveFlow bool) {
				flow := fx.makeFlow(withTTL, withCleanup)
				fx.makeHandler()
				fx.makeHandler(cleanupHandler)
				fx.makeTask()
				fx.reconcile() // begin
				fx.reconcile() // creates the Job
				Expect(jobsOf(fx)).To(HaveLen(1))
				fx.markPinned()
				fx.dropCopy()
				if deleteLiveFlow {
					Expect(k8sClient.Delete(fx.ctx, flow)).To(Succeed())
				}
				unresolved := failedOutcome(metrics.FlowUnresolved)
				named := failedOutcome(fx.name)
				beforeUnresolved := testutil.ToFloat64(metrics.TaskOutcomes.With(unresolved))
				beforeNamed := testutil.ToFloat64(metrics.TaskOutcomes.With(named))

				fx.reconcile()

				tk := fx.get()
				lost(tk)
				Expect(tk.Status.CurrentRuns).To(BeEmpty(), "the live finally handler is something the task never pinned: no cleanup run is owed")
				Expect(tk.Status.ExpiresAt).To(BeNil(), "no ttl is written at the moment it fails")
				Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(unresolved))).To(Equal(beforeUnresolved+1),
					"counted under the label for a flow that could not be resolved")
				Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(named))).To(Equal(beforeNamed))

				fx.reconcile()

				again := fx.get()
				Expect(again.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
				Expect(readyOf(again).Reason).To(Equal(lostReason), "failing again does not rewrite why it failed")
				Expect(again.Status.CurrentRuns).To(BeEmpty())
				Expect(jobsOf(fx)).To(HaveLen(1), "no cleanup Job is made from the live handler")
				if deleteLiveFlow {
					Expect(again.Status.ExpiresAt).To(BeNil(), "no live flow to read a ttl from")
					return
				}
				Expect(again.Status.ExpiresAt).NotTo(BeNil(), "the live flow of the same name gives the date once the task has stopped")
				Expect(again.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
			},
			Entry("while the live flow exists", false),
			Entry("while the live flow is gone as well", true),
		)

		// 変異: fail() の Idle の guard を通る経路で落とす。
		It("fails with nothing in flight too", func() {
			fx.makeFlow(withTTL)
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			tk := fx.get()
			tk.Status.CurrentRuns = nil
			Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
			fx.markPinned()
			fx.dropCopy()

			fx.reconcile()

			lost(fx.get())
			Expect(jobsOf(fx)).To(BeEmpty(), "the task is not recovered from the live definitions")
		})

		// 今のコードでも通る: 止まったのか分からない Task は、今まで通り触らない。
		It("is left alone when nothing is in flight and the live flow is gone too", func() {
			flow := fx.makeFlow()
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			tk := fx.get()
			tk.Status.CurrentRuns = nil
			Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
			fx.dropCopy()
			Expect(k8sClient.Delete(fx.ctx, flow)).To(Succeed())

			fx.reconcile()

			Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate), "it cannot be told apart from a task that stopped at an unbound phase")
		})
	})

	// 止まった Task は、写しが無くても Failed にならず、残りの仕事を live から読む。
	Context("a stopped task with the marker whose copy was deleted", func() {
		// stoppedAt puts a task that began, and so carries the marker, at phase with
		// nothing in flight and no date, and deletes its copy.
		stoppedAt := func(phase flowv1alpha1.Phase) {
			fx.makeFlow(withTTL)
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.markPinned()
			tk := fx.get()
			tk.Status.Phase = phase
			tk.Status.CurrentRuns = nil
			tk.Status.ExpiresAt = nil
			Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
			fx.dropCopy()
		}

		// 変異: 写しが無ければ止まっていても Failed にする / 写しを作り直す。
		It("is dated from the live flow when it stopped at Escalated", func() {
			stoppedAt(flowv1alpha1.PhaseEscalated)

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
			Expect(readyOf(tk)).To(BeNil(), "nothing was said about the task")
			Expect(tk.Status.ExpiresAt).NotTo(BeNil())
			Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
			_, copied := fx.copyHeld()
			Expect(copied).To(BeFalse(), "a stopped task is not given a copy")
		})

		It("is dated from the live flow when it stopped at a phase the flow leaves unbound", func() {
			stoppedAt(phaseReport)

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(phaseReport))
			Expect(readyOf(tk)).To(BeNil())
			Expect(tk.Status.ExpiresAt).NotTo(BeNil())
			Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(succeededTTL)))
			_, copied := fx.copyHeld()
			Expect(copied).To(BeFalse())
		})

		It("still has its cleanup run driven from the live definitions", func() {
			fx.makeFlow(withCleanup)
			fx.makeHandler(stateRunner(timeout))
			fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
				cleanupHandler(h)
				stateRunner(timeout)(h)
			})
			fx.makeTask()
			fx.reconcile()
			fx.reconcile()
			fx.answer("ok", "")
			fx.reconcile() // the task reaches its ending, owing a cleanup
			fx.markPinned()
			Expect(taskstate.InFinally(&fx.get().Status)).To(BeTrue())
			fx.dropCopy()

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(phaseReport), "the ending stands")
			Expect(readyOf(tk)).To(BeNil(), "a finished task is not turned into a failed one")
			Expect(fx.boxFor(flowv1alpha1.PhaseFinally, 2)).NotTo(BeNil(), "the cleanup run is opened from the live handler")
			_, copied := fx.copyHeld()
			Expect(copied).To(BeFalse())
		})
	})
})

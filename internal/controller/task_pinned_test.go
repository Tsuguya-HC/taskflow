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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/metrics"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// 開始した Task は、定義の写しを持つ。写しを持たずマーカー (DefinitionsPinned) も
// 無い Task は、次の reconcile で写しを作る。マーカーがあるのに写しが無い Task は
// Failed にする (#183)。
var _ = Describe("a started task and its copy of the definitions", func() {
	var fx *fixture
	var clock time.Time

	const (
		succeededTTL = time.Hour
		failedTTL    = 168 * time.Hour
	)

	BeforeEach(func() {
		fx = newFixture()
		clock = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
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

	cleanupName := func() string { return fx.name + "-cleanup" }

	withTTL := func(f *flowv1alpha1.TaskFlow) {
		f.Spec.TTL = &flowv1alpha1.TTLSpec{
			Succeeded: &metav1.Duration{Duration: succeededTTL},
			Failed:    &metav1.Duration{Duration: failedTTL},
		}
	}

	withCleanup := func(f *flowv1alpha1.TaskFlow) {
		withTTL(f)
		f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: cleanupName(), Done: dirDone}
	}

	makeCleanupHandler := func() {
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = cleanupName()
			h.Spec.Phase = flowv1alpha1.PhaseFinally
		})
	}

	ready := func(tk *flowv1alpha1.Task) *metav1.Condition {
		return meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
	}

	copiedIn := func(rev appsv1.ControllerRevision) snapshot {
		var got snapshot
		Expect(json.Unmarshal(rev.Data.Raw, &got)).To(Succeed())
		return got
	}

	storedHandler := func(name string) flowv1alpha1.TaskHandlerSpec {
		var h flowv1alpha1.TaskHandler
		Expect(k8sClient.Get(fx.ctx, client.ObjectKey{Name: name, Namespace: resourceNamespace}, &h)).To(Succeed())
		return h.Spec
	}

	// idle makes the begun task one with nothing in flight, the state a crash
	// between begin's write and the Job leaves.
	idle := func() {
		tk := fx.get()
		taskstate.SetCurrent(&tk.Status, nil)
		Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
	}

	outcome := func(flow string) prometheus.Labels {
		return prometheus.Labels{
			metrics.LabelFlow: flow, metrics.LabelPhase: string(flowv1alpha1.PhaseTaskFailed), metrics.LabelSeverity: string(transition.EndingTaskFailed), metrics.LabelOutcome: string(transition.OutcomeStructural),
		}
	}

	Context("that has its copy and no marker", func() {
		// 変異: 写しがあるのでマーカーを足さない・足すときに写しを作り直す。
		It("gets the marker, with the copy left as it is and the task carrying on", func() {
			fx.makeFlow()
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.unpin()     // began after the copy existed and before anything marked it
			before := fx.revisions()
			Expect(before).To(HaveLen(1))

			fx.reconcile()
			fx.reconcile() // creates the Job, if the first only marked

			got := fx.get()
			marker := pinnedOf(got)
			Expect(marker).NotTo(BeNil())
			Expect(marker.Status).To(Equal(metav1.ConditionTrue))
			Expect(marker.Reason).To(Equal(reasonCopied))
			after := fx.revisions()
			Expect(after).To(HaveLen(1))
			Expect(after[0].Name).To(Equal(before[0].Name))
			Expect(after[0].ResourceVersion).To(Equal(before[0].ResourceVersion), "the copy is not touched")
			Expect(got.Status.Phase).To(Equal(phaseInvestigate))
			Expect(got.Status.RunID).To(BeEquivalentTo(1))
			Expect(got.Status.History).To(BeEmpty(), "nothing else about the task changes")
			Expect(jobsOf(fx)).To(HaveLen(1), "the task carries on")
		})
	})

	Context("that has stopped, has its copy and no marker", func() {
		// 変異: 止まった Task にもマーカーを足す。
		It("is not marked, whether it is on its cleanup run or idle at a phase nothing binds", func() {
			fx.makeFlow(withCleanup)
			fx.makeHandler()
			makeCleanupHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.unpin()
			tk := fx.get()
			tk.Status.Phase = phaseReport
			taskstate.SetCurrent(&tk.Status, &flowv1alpha1.RunRef{Phase: flowv1alpha1.PhaseFinally, RunID: 2})
			Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())

			fx.reconcile()
			Expect(pinnedOf(fx.get())).To(BeNil(), "on its cleanup run")

			idle()
			fx.reconcile()
			Expect(pinnedOf(fx.get())).To(BeNil(), "idle at a phase nothing binds")
		})
	})

	Context("that has neither a copy nor a marker", func() {
		// 変異: 移行が写しを作らない・マーカーを書かない・finally を落とす/無いのに
		// 要求する・flow か handler を live から読み続ける。
		DescribeTable("is migrated: a copy is made from the live definitions, then marked, and live edits no longer reach it",
			func(cleanupExists bool) {
				flow := fx.makeFlow(withCleanup)
				fx.makeHandler()
				if cleanupExists {
					makeCleanupHandler()
				}
				tk := fx.makeBareTask(false)

				fx.reconcile() // makes the copy

				got := fx.get()
				Expect(got.Status.Phase).To(Equal(phaseInvestigate))
				marker := pinnedOf(got)
				Expect(marker).NotTo(BeNil(), "the copy is marked once it exists")
				Expect(marker.Status).To(Equal(metav1.ConditionTrue))
				Expect(marker.Reason).To(Equal(reasonCopied))
				revs := fx.revisions()
				Expect(revs).To(HaveLen(1))
				Expect(revs[0].Name).To(Equal(runner.SnapshotRevisionName(tk.Name, tk.UID)))
				Expect(metav1.IsControlledBy(&revs[0], tk)).To(BeTrue())
				copied := copiedIn(revs[0])
				Expect(copied.Flow).To(Equal(flow.Spec))
				want := map[string]flowv1alpha1.TaskHandlerSpec{fx.name: storedHandler(fx.name)}
				if cleanupExists {
					want[cleanupName()] = storedHandler(cleanupName())
				}
				Expect(copied.Handlers).To(Equal(want),
					"every handler a binding names, and the finally handler only if it exists")
				stamp := revs[0].ResourceVersion

				fx.editHandler(func(h *flowv1alpha1.TaskHandler) {
					h.Spec.JobTemplate.Template.Spec.Containers[0].Image = "example.invalid/agent:edited"
				})
				Expect(k8sClient.Delete(fx.ctx, flow)).To(Succeed())
				fx.reconcile()
				fx.reconcile() // creates the Job, if the first did not

				got = fx.get()
				Expect(got.Status.Phase).To(Equal(phaseInvestigate), "a deleted live flow is no reason to fail a task that has its copy")
				Expect(jobsOf(fx)).To(HaveLen(1))
				Expect(fx.job(1).Spec.Template.Spec.Containers[0].Image).To(Equal(agentImage))
				Expect(fx.revisions()[0].ResourceVersion).To(Equal(stamp), "the copy is made once")
			},
			Entry("with a finally handler that exists", true),
			Entry("with a finally handler that does not exist", false),
		)

		// 変異: 移行の失敗を握りつぶす・理由を変える・ttl や掃除を live の flow から
		// 引かない・写しやマーカーを残す。
		Context("when a binding names a handler that does not exist", func() {
			failsWith := func(tk *flowv1alpha1.Task) {
				GinkgoHelper()
				Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))
				cond := ready(tk)
				Expect(cond).NotTo(BeNil())
				Expect(cond.Status).To(Equal(metav1.ConditionFalse))
				Expect(cond.Reason).To(Equal(reasonBroken), "the reason begin gives for the same fault")
				Expect(cond.Message).To(ContainSubstring(fx.name))
				Expect(cond.Message).To(ContainSubstring("does not exist"))
				Expect(fx.revisions()).To(BeEmpty(), "nothing was made, so nothing is kept")
				Expect(pinnedOf(tk)).To(BeNil())
			}

			DescribeTable("goes to Failed, dated and cleaned up by the live flow",
				func(idleTask bool) {
					fx.makeFlow(withTTL) // no handler for it is made
					fx.makeBareTask(idleTask)

					fx.reconcile()

					tk := fx.get()
					failsWith(tk)
					Expect(tk.Status.CurrentRuns).To(BeEmpty())
					Expect(tk.Status.ExpiresAt).NotTo(BeNil())
					Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
				},
				Entry("with the first run in flight", false),
				Entry("with nothing in flight, which fail() alone would leave alone", true),
			)

			It("owes the cleanup run the live flow declares", func() {
				fx.makeFlow(withCleanup)
				makeCleanupHandler()
				fx.makeBareTask(false)

				fx.reconcile()

				tk := fx.get()
				failsWith(tk)
				Expect(taskstate.InFinally(&tk.Status)).To(BeTrue(), "as when begin fails")
				Expect(tk.Status.ExpiresAt).To(BeNil(), "dated once the cleanup run is done")
			})
		})

		// 1 object に収まらない写しは、Failed にする。拒否の形は大きさで違う。
		DescribeTable("goes to Failed when the copy does not fit in one object",
			func(big int) {
				fx.makeBulky(big)
				fx.makeBareTask(false)

				fx.reconcile()

				tk := fx.get()
				Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))
				Expect(ready(tk).Reason).To(Equal(reasonBroken))
				Expect(ready(tk).Message).To(ContainSubstring("do not fit in one object"))
				Expect(fx.revisions()).To(BeEmpty())
				Expect(pinnedOf(tk)).To(BeNil())
			},
			Entry("refused by the storage under the apiserver", 2),
			Entry("refused by the apiserver's body limit", 4),
		)

		// 今ある振る舞いを固定する: flow が無ければ何も移行しない。
		// 変異: fail() の Idle の門を外す・走行中でも失敗させない。
		Context("when the live flow does not exist", func() {
			It("fails a task with a run in flight, undated, and makes no copy", func() {
				labels := outcome(metrics.FlowUnresolved)
				before := testutil.ToFloat64(metrics.TaskOutcomes.With(labels))
				fx.makeBareTask(false)

				fx.reconcile()

				tk := fx.get()
				Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))
				Expect(ready(tk).Message).To(ContainSubstring("does not exist"))
				Expect(tk.Status.ExpiresAt).To(BeNil())
				Expect(fx.revisions()).To(BeEmpty())
				Expect(pinnedOf(tk)).To(BeNil())
				Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(labels))).To(Equal(before + 1))
			})

			It("leaves a task with nothing in flight as it is, and makes no copy", func() {
				fx.makeBareTask(true)
				was := fx.get()

				fx.reconcile()

				tk := fx.get()
				Expect(tk.Status).To(Equal(was.Status))
				Expect(fx.revisions()).To(BeEmpty())
			})
		})
	})

	Context("that has its marker and no copy", func() {
		// begin までは通常どおり進め、マーカーは手で付ける (begin が書くかは別の spec)。
		lostCopy := func(mut ...func(*flowv1alpha1.TaskFlow)) {
			fx.makeFlow(mut...)
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.markPinned()
			fx.dropCopy()
		}

		// 変異: 写しが無いときに live から写しを作り直す・live の flow から掃除と
		// ttl を引く・FlowUnresolved 以外で数える・理由を FlowBroken にする。
		It("goes to Failed with a run in flight, owed no cleanup and given no date yet", func() {
			lostCopy(withCleanup)
			makeCleanupHandler()
			labels := outcome(metrics.FlowUnresolved)
			before := testutil.ToFloat64(metrics.TaskOutcomes.With(labels))

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))
			cond := ready(tk)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal(reasonLost))
			Expect(cond.Message).NotTo(BeEmpty())
			Expect(tk.Status.CurrentRuns).To(BeEmpty(), "the live finally handler is not something the task pinned: no cleanup run is owed")
			Expect(tk.Status.ExpiresAt).To(BeNil(), "no ttl is written at the moment it fails")
			Expect(fx.revisions()).To(BeEmpty(), "a task that lost its copy is not given another")
			Expect(jobsOf(fx)).To(BeEmpty())
			Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(labels))).To(Equal(before+1),
				"counted under the fixed label, as for a flow that is unknown")

			// A task that has stopped with no copy takes its date from the live flow.
			fx.reconcile()

			tk = fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))
			Expect(ready(tk).Reason).To(Equal(reasonLost), "it never changes")
			Expect(tk.Status.ExpiresAt).NotTo(BeNil())
			Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
			Expect(taskstate.InFinally(&tk.Status)).To(BeFalse())
			Expect(jobsOf(fx)).To(BeEmpty(), "no cleanup run was started from the live definitions")
			Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(labels))).To(Equal(before+1), "backfilling the date does not count it again")
		})

		// 変異: 走行中の判定のために live の flow を読む。
		It("goes to Failed with this reason, not \"does not exist\", when the live flow is gone too", func() {
			lostCopy()
			Expect(k8sClient.Delete(fx.ctx, &flowv1alpha1.TaskFlow{
				ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
			})).To(Succeed())

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))
			Expect(ready(tk).Reason).To(Equal(reasonLost))
			Expect(tk.Status.ExpiresAt).To(BeNil())
		})

		// 変異: fail() の Idle の門を通る経路で失敗させる。
		It("goes to Failed with nothing in flight, which fail() alone would leave alone", func() {
			lostCopy(withTTL)
			idle()

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))
			Expect(ready(tk).Reason).To(Equal(reasonLost))
			Expect(tk.Status.ExpiresAt).To(BeNil())
			Expect(fx.revisions()).To(BeEmpty())

			fx.reconcile()

			Expect(fx.get().Status.ExpiresAt).NotTo(BeNil(), "dated from the live flow, as any stopped task with no copy is")
		})

		// 今ある振る舞い: 何も走っておらず live の flow も無い Task は、止まっているのか
		// 見分けられないので触らない。
		// 変異: fail() の Idle の門を外す・Idle でも失敗させる。
		It("is left alone with nothing in flight when the live flow is gone, there being no telling it from one that stopped", func() {
			lostCopy()
			idle()
			Expect(k8sClient.Delete(fx.ctx, &flowv1alpha1.TaskFlow{
				ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
			})).To(Succeed())
			was := fx.get()

			fx.reconcile()

			Expect(fx.get().Status).To(Equal(was.Status))
			Expect(fx.revisions()).To(BeEmpty())
		})
	})
})

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
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
)

// 開始した Task は、そのとき写した定義から flow を読む (#181)。写しを持たず
// マーカーも無い Task は、次の reconcile で live の定義から写しを作る (#183)。
var _ = Describe("a task running from its copy of the flow", func() {
	var fx *fixture
	var clock time.Time

	const (
		timeout      = time.Hour
		succeededTTL = time.Hour
		failedTTL    = 168 * time.Hour
	)

	BeforeEach(func() {
		fx = newFixture()
		// The wall clock, held still: a State run's deadline is read off the real
		// time its box was made, so a made-up date would time the run out.
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

	// awaitingAnswer is a task that began through begin, so has its copy, and
	// whose first run waits in a verdict box. A State run is the one that
	// needs nothing but the flow and the box to be answered.
	awaitingAnswer := func() *flowv1alpha1.TaskFlow {
		flow := fx.makeFlow(withTTL)
		fx.makeHandler(stateRunner(timeout))
		fx.makeTask()
		fx.reconcile() // begin
		fx.reconcile() // opens the place run 1 is answered in
		return flow
	}

	// 変異: 遷移表を live の flow から読む。
	It("takes its transitions from the copy when the live flow is edited", func() {
		flow := awaitingAnswer()

		Expect(k8sClient.Get(fx.ctx, client.ObjectKeyFromObject(flow), flow)).To(Succeed())
		flow.Spec.Bindings[phaseInvestigate] = flowv1alpha1.PhaseBinding{
			Handler: fx.name, Next: map[flowv1alpha1.Phase]string{"別の報告": "ok"},
		}
		Expect(k8sClient.Update(fx.ctx, flow)).To(Succeed())

		fx.answer("ok", "")
		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(phaseReport), "ok leads where the copy says, not where the live flow now says")
	})

	// 変異: flow が無ければ TaskFailed にする（コピーを見ない）。
	It("keeps running when the live flow is deleted", func() {
		flow := awaitingAnswer()
		Expect(k8sClient.Delete(fx.ctx, flow)).To(Succeed())

		fx.answer("ok", "")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "a deleted flow is no reason to fail a task that has its copy")
		Expect(tk.Status.ExpiresAt).NotTo(BeNil(), "the date comes from the copy's ttl")
		Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(succeededTTL)))
	})

	// 変異: 停止済みの Task の経路だけ live の flow を読む。
	It("dates a stopped task from the copy when the live flow is deleted", func() {
		flow := fx.makeFlow(withTTL)
		fx.makeHandler()
		tk := fx.makeTask()
		fx.reconcile() // begin

		// A task that stopped before expiresAt existed to date it, the shape
		// the reserved-phase branch backfills.
		stopped := fx.get()
		stopped.Status.Phase = flowv1alpha1.PhaseTaskFailed
		stopped.Status.CurrentRuns = nil
		Expect(k8sClient.Status().Update(fx.ctx, stopped)).To(Succeed())
		Expect(k8sClient.Delete(fx.ctx, flow)).To(Succeed())

		fx.reconcile()

		got := fx.get()
		Expect(got.UID).To(Equal(tk.UID))
		Expect(got.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))
		Expect(got.Status.ExpiresAt).NotTo(BeNil(), "nothing but the copy is left to read a ttl from")
		Expect(got.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
	})

	// swapCopy replaces the copy begin wrote with one holding spec: a revision's
	// data cannot be updated, so the task's own is deleted and made again.
	swapCopy := func(spec flowv1alpha1.TaskFlowSpec) {
		tk := fx.get()
		old := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{
			Name: runner.SnapshotRevisionName(tk.Name, tk.UID), Namespace: tk.Namespace,
		}}
		Expect(k8sClient.Delete(fx.ctx, old)).To(Succeed())
		data, err := json.Marshal(snapshot{Flow: spec, Handlers: map[string]flowv1alpha1.TaskHandlerSpec{}})
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Create(fx.ctx, runner.BuildSnapshotRevision(tk, data))).To(Succeed())
	}

	// 変異: 写しから作る flow の Name を task.Spec.Flow 以外（revision 名や空）にする。
	It("names the flow in its messages for the flow the task asked for", func() {
		awaitingAnswer()
		swapCopy(flowv1alpha1.TaskFlowSpec{Start: phaseInvestigate})

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed), "the copy has no binding for the phase in flight")
		Expect(tk.Status.Conditions).To(ContainElement(HaveField("Message",
			ContainSubstring(fmt.Sprintf("lost its binding in flow %q while", tk.Spec.Flow)))))
	})

	// 変異: start と bindings の「どちらかが空」で読めない写しとして扱う。
	It("reads a copy that has bindings but no start", func() {
		flow := awaitingAnswer()
		swapCopy(flowv1alpha1.TaskFlowSpec{
			Profile:         flow.Spec.Profile,
			Bindings:        flow.Spec.Bindings,
			MaxRunsPerPhase: flow.Spec.MaxRunsPerPhase,
		})

		fx.answer("ok", "")
		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(phaseReport), "a copy with bindings is a flow whatever its start says")
	})

	// 変異: start と bindings の「どちらかが空」で読めない写しとして扱う。
	It("reads a copy that has a start but no bindings", func() {
		awaitingAnswer()
		swapCopy(flowv1alpha1.TaskFlowSpec{Start: phaseInvestigate})

		_, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})

		Expect(err).NotTo(HaveOccurred(), "a copy with a start is a flow, not an unreadable one")
		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))
	})

	// A task is driven through the copy only when the copy can be read, and
	// a reader that cannot read it must not be mistaken for a task that has
	// none.
	It("refuses to run through a cache it cannot have", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile() // begin
		fx.reconciler.APIReader = nil

		_, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})

		Expect(err).To(MatchError(ContainSubstring("uncached reader")))
		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate), "the live flow is not a fallback")
		Expect(jobsOf(fx)).To(BeEmpty(), "nothing was started from the live objects either")
	})

	// 変異: 読めなかったら live の flow に戻る。
	It("returns a failing get of the copy rather than falling back to the live flow", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile() // begin

		boom := errors.New("the apiserver said no")
		watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		fx.reconciler.APIReader = interceptor.NewClient(watchClient, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*appsv1.ControllerRevision); ok {
					return boom
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})

		_, err = fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})

		Expect(err).To(MatchError(ContainSubstring(boom.Error())))
		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate), "a read that failed decides nothing about the task")
		Expect(jobsOf(fx)).To(BeEmpty())
	})

	Context("a task whose status was written without begin", func() {
		var reader *recordingReader

		// bareTask is a task past begin that has no revision of its own and no
		// marker: the shape every task started before the copy existed has.
		bareTask := func() *flowv1alpha1.Task {
			tk := fx.makeBareTask(false)
			reader = &recordingReader{Reader: k8sClient}
			fx.reconciler.APIReader = reader
			return tk
		}

		revisionBy := func(tk *flowv1alpha1.Task, owner types.UID, data string) *appsv1.ControllerRevision {
			rev := runner.BuildSnapshotRevision(tk, []byte(data))
			rev.OwnerReferences[0].UID = owner
			Expect(k8sClient.Create(fx.ctx, rev)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, rev) })
			return rev
		}

		const otherUIDsCopy = `{"flow":{"start":"調査","bindings":{"調査":{"handler":"elsewhere","next":{"別の報告":"ok"}}}},"handlers":{}}`

		// 変異: 名前だけで写しを採る（持ち主の UID を見ない）。移行が別の持ち主の
		// 写しを採る・上書きする。
		It("is not advanced, and leaves a revision of another task's UID alone, when it would be migrated under that name", func() {
			fx.makeFlow()
			fx.makeHandler(stateRunner(timeout))
			tk := bareTask()
			other := revisionBy(tk, types.UID("not-"+string(tk.UID)), otherUIDsCopy)
			stamp := other.ResourceVersion

			_, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
			})

			Expect(err).To(HaveOccurred(), "a name someone else holds is retried until it is free, not taken over")
			got := fx.get()
			Expect(got.Status.Phase).To(Equal(phaseInvestigate))
			Expect(got.Status.Conditions).To(BeEmpty(), "neither failed nor marked")
			Expect(jobsOf(fx)).To(BeEmpty())
			Expect(boxesFor(fx)).To(BeEmpty())
			var now appsv1.ControllerRevision
			Expect(k8sClient.Get(fx.ctx, client.ObjectKeyFromObject(other), &now)).To(Succeed())
			Expect(now.ResourceVersion).To(Equal(stamp), "the other task's revision is never touched")
			Expect(now.Data.Raw).To(Equal([]byte(otherUIDsCopy)))
			Expect(reader.revisions).To(ContainElement(runner.SnapshotRevisionName(tk.Name, tk.UID)),
				"the copy was looked for under its name; a task that never looked would pass for the wrong reason")
		})

		// 変異: 名前だけで写しを採る（持ち主の UID を見ない）。
		It("fails, for a marked task, rather than read a revision of another task's UID under its name", func() {
			fx.makeFlow()
			fx.makeHandler(stateRunner(timeout))
			tk := bareTask()
			fx.markPinned()
			other := revisionBy(tk, types.UID("not-"+string(tk.UID)), otherUIDsCopy)
			stamp := other.ResourceVersion

			fx.reconcile()

			got := fx.get()
			Expect(got.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed), "that revision is no copy of this task's")
			Expect(meta.FindStatusCondition(got.Status.Conditions, taskstate.ConditionReady).Reason).To(Equal(reasonLost))
			var now appsv1.ControllerRevision
			Expect(k8sClient.Get(fx.ctx, client.ObjectKeyFromObject(other), &now)).To(Succeed())
			Expect(now.ResourceVersion).To(Equal(stamp), "the other task's revision is never touched")
			Expect(reader.revisions).To(ContainElement(runner.SnapshotRevisionName(tk.Name, tk.UID)))
		})

		// 変異: 移行が写しを作らない・マーカーを書かない・写しから進めない。
		It("is migrated when no revision exists under its name", func() {
			fx.makeFlow()
			fx.makeHandler(stateRunner(timeout))
			tk := bareTask()

			fx.reconcile() // makes the copy
			fx.reconcile() // opens the place run 1 is answered in
			fx.answer("ok", "")
			fx.reconcile()

			got := fx.get()
			Expect(got.Status.Phase).To(Equal(phaseReport))
			Expect(reader.revisions).To(ContainElement(runner.SnapshotRevisionName(tk.Name, tk.UID)))
			revs := fx.revisions()
			Expect(revs).To(HaveLen(1), "the copy is made from the live flow at migration")
			Expect(revs[0].Name).To(Equal(runner.SnapshotRevisionName(tk.Name, tk.UID)))
			Expect(metav1.IsControlledBy(&revs[0], tk)).To(BeTrue())
			Expect(pinnedOf(got)).NotTo(BeNil(), "the copy is marked once it exists")
		})

		// 変異: 読めない写しを「写し無し」として live に戻る。
		DescribeTable("returns an error, and does not fail the task, for a copy that cannot be read",
			func(data string) {
				fx.makeFlow()
				fx.makeHandler(stateRunner(timeout))
				tk := bareTask()
				revisionBy(tk, tk.UID, data)

				_, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
				})

				Expect(err).To(HaveOccurred())
				Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate), "a copy nobody can read is not a reason to fail the task")
			},
			Entry("data that does not decode into a snapshot", `{"flow":"not a flow"}`),
			Entry("a flow with no start and no bindings", `{"flow":{},"handlers":{}}`),
		)
	})
})

// recordingReader remembers which ControllerRevisions it was asked for.
type recordingReader struct {
	client.Reader
	revisions []string
}

func (r *recordingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*appsv1.ControllerRevision); ok {
		r.revisions = append(r.revisions, key.Name)
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

// boxesFor is every verdict box the fixture's task has.
func boxesFor(fx *fixture) []corev1.ConfigMap {
	var boxes corev1.ConfigMapList
	Expect(k8sClient.List(fx.ctx, &boxes, client.InNamespace(resourceNamespace),
		client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID)})).To(Succeed())
	return boxes.Items
}

// jobsOf is every Job the fixture's task has.
func jobsOf(fx *fixture) []batchv1.Job {
	var jobs batchv1.JobList
	Expect(k8sClient.List(fx.ctx, &jobs, client.InNamespace(resourceNamespace),
		client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID)})).To(Succeed())
	return jobs.Items
}

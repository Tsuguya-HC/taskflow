package controller

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

// Task 開始時に flow と handler の定義を 1 つの ControllerRevision に写す
// (#180)。写しから読む振る舞いはまだ無い。
var _ = Describe("the revision a task starts from", func() {
	var fx *fixture

	BeforeEach(func() {
		fx = newFixture()
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

	revisionsOf := func(uid types.UID) []appsv1.ControllerRevision {
		var list appsv1.ControllerRevisionList
		Expect(k8sClient.List(fx.ctx, &list, client.InNamespace(resourceNamespace))).To(Succeed())
		var out []appsv1.ControllerRevision
		for _, rev := range list.Items {
			for _, owner := range rev.OwnerReferences {
				if owner.UID == uid {
					out = append(out, rev)
				}
			}
		}
		return out
	}

	copiedIn := func(rev appsv1.ControllerRevision) snapshot {
		var got snapshot
		Expect(json.Unmarshal(rev.Data.Raw, &got)).To(Succeed())
		return got
	}

	stored := func(name string) flowv1alpha1.TaskHandlerSpec {
		var h flowv1alpha1.TaskHandler
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: name, Namespace: resourceNamespace}, &h)).To(Succeed())
		return h.Spec
	}

	// 変異: 写す中身を空にする・別の flow を写す・handler の spec を落とす。
	It("writes one owned revision holding the flow and handler specs", func() {
		flow := fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		fx.reconcile()

		revs := revisionsOf(tk.UID)
		Expect(revs).To(HaveLen(1), "begin must copy the flow and handler specs into one ControllerRevision")
		rev := revs[0]
		Expect(metav1.IsControlledBy(&rev, tk)).To(BeTrue(), "the copy goes with the task")
		Expect(rev.Labels).To(HaveKeyWithValue(runner.LabelTaskUID, string(tk.UID)))
		got := copiedIn(rev)
		Expect(got.Flow).To(Equal(flow.Spec))
		Expect(got.Handlers).To(Equal(map[string]flowv1alpha1.TaskHandlerSpec{fx.name: stored(fx.name)}))
	})

	// 変異: begin の status 書き込みにマーカーを載せない・Copied 以外の reason・
	// True 以外の status を書く。
	It("marks the task as having its copy in the write that begins it", func() {
		fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		fx.reconcile()

		got := fx.get()
		Expect(got.Status.Phase).To(Equal(phaseInvestigate))
		Expect(revisionsOf(tk.UID)).To(HaveLen(1))
		marker := pinnedOf(got)
		Expect(marker).NotTo(BeNil(), "a task that began on a copy says so")
		Expect(marker.Status).To(Equal(metav1.ConditionTrue))
		Expect(marker.Reason).To(Equal(reasonCopied))
	})

	It("begins only the starting phase and nothing else", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseInvestigate))
		Expect(tk.Status.RunID).To(BeEquivalentTo(1))
		Expect(tk.Status.History).To(BeEmpty(), "the copy is not a run: recording anything says a verdict was reached")
	})

	It("never modifies the revision afterwards", func() {
		fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		fx.reconcile()
		first := revisionsOf(tk.UID)
		Expect(first).To(HaveLen(1))
		stamp := first[0].ResourceVersion

		fx.reconcile()
		fx.reconcile()

		again := revisionsOf(tk.UID)
		Expect(again).To(HaveLen(1))
		Expect(again[0].ResourceVersion).To(Equal(stamp), "a later reconcile must leave the copy untouched")
	})

	// start 以外の束縛も対象。変異は存在確認の省略。
	It("fails before the first run when a binding names a missing handler", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings[phaseReport] = flowv1alpha1.PhaseBinding{
				Handler: fx.name + "-gone",
				Next:    map[flowv1alpha1.Phase]string{phaseDone: "ok"},
			}
		})
		fx.makeHandler()
		fx.makeTask()

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(revisionsOf(tk.UID)).To(BeEmpty(), "a task that never began has nothing to hold still")
		Expect(pinnedOf(tk)).To(BeNil(), "no copy was made, so nothing is marked")
	})

	// finally の欠落は既存の記録のまま: 「records a missing cleanup handler
	// without touching the ending」が守る性質。
	It("begins without the finally handler when it is missing", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: fx.name + "-cleanup-gone", Done: "swept"}
		})
		fx.makeHandler()
		tk := fx.makeTask()

		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate), "a missing cleanup handler does not fail the task")
		revs := revisionsOf(tk.UID)
		Expect(revs).To(HaveLen(1))
		Expect(copiedIn(revs[0]).Handlers).To(HaveLen(1), "the copy is written without the missing handler")
	})

	It("carries the finally handler when it exists", func() {
		cleanup := fx.name + "-cleanup"
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: cleanup, Done: "swept"}
		})
		fx.makeHandler()
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) { h.Name = cleanup })
		tk := fx.makeTask()

		fx.reconcile()

		revs := revisionsOf(tk.UID)
		Expect(revs).To(HaveLen(1))
		Expect(copiedIn(revs[0]).Handlers).To(HaveKeyWithValue(cleanup, stored(cleanup)))
	})

	// 1 object に収まらない写しは、最初の run の前に落とす。拒否は大きさで形が
	// 違う: 保存側の 500 と、apiserver の 413。handler 1 つは載る大きさにして、
	// 束ねた写しだけが載らないようにする。変異は拒否の形の判定を 1 つ落とす。
	DescribeTable("fails before the first run when the copy does not fit in one object",
		func(big int) {
			fx.makeBulky(big)
			tk := fx.makeTask()

			fx.reconcile()

			Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed),
				"a copy that fits in no single object must fail the task before its first run")
			Expect(revisionsOf(tk.UID)).To(BeEmpty())
			Expect(pinnedOf(fx.get())).To(BeNil(), "no copy was made, so nothing is marked")
		},
		Entry("refused by the storage under the apiserver", 2),
		Entry("refused by the apiserver's body limit", 4),
	)

	// 名前は UID から導くので、作り直した Task どうしは衝突しない。衝突するのは
	// 同じ名前を別の所有者が先に取ったときで、それは使わず、触らずに待つ。
	// 変異は所有者の突き合わせの省略。
	It("neither adopts nor touches a revision another owner holds under its name", func() {
		fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()
		predecessor := tk.DeepCopy()
		predecessor.UID = "a-predecessor"
		squatter := runner.BuildSnapshotRevision(predecessor, []byte(`{}`))
		squatter.Name = runner.SnapshotRevisionName(tk.Name, tk.UID)
		Expect(k8sClient.Create(fx.ctx, squatter)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, squatter) })
		stamp := squatter.ResourceVersion

		_, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})

		Expect(err).To(HaveOccurred(), "a name someone else holds is retried, not taken over")
		Expect(fx.get().Status.Phase).To(BeEmpty(), "the task does not begin on a copy that is not its own")
		var now appsv1.ControllerRevision
		Expect(k8sClient.Get(fx.ctx, client.ObjectKeyFromObject(squatter), &now)).To(Succeed())
		Expect(now.ResourceVersion).To(Equal(stamp))
	})

	// begin の status 書き込みが落ちて再 begin すると、前回の自分の写しが既にある。
	// それは作り直さず、そのまま使う。変異は所有者の突き合わせの省略。
	It("adopts its own revision left by a begin whose status write did not land", func() {
		flow := fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()
		data, err := json.Marshal(snapshot{Flow: flow.Spec})
		Expect(err).NotTo(HaveOccurred())
		own := runner.BuildSnapshotRevision(tk, data)
		Expect(k8sClient.Create(fx.ctx, own)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, own) })
		stamp := own.ResourceVersion

		fx.reconcile()

		got := fx.get()
		Expect(got.Status.Phase).To(Equal(phaseInvestigate))
		Expect(pinnedOf(got)).NotTo(BeNil(), "taking over its own copy is a begin like any other, and marks the task")
		var now appsv1.ControllerRevision
		Expect(k8sClient.Get(fx.ctx, client.ObjectKeyFromObject(own), &now)).To(Succeed())
		Expect(now.ResourceVersion).To(Equal(stamp))
	})
})

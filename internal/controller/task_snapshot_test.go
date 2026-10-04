package controller

import (
	"bytes"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// Task 開始時に flow と handler の定義を 1 つの ControllerRevision に写す
// (#180)。ここで固定するのは、作業ツリーにまだ無いものだけ。写しから読む
// 振る舞いはこの回に入れない。
var _ = Describe("the revision a task starts from", func() {
	var fx *fixture

	BeforeEach(func() {
		fx = newFixture()
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

	// 写しは 1 つだけ Task が持ち、付け替える取っ手は無い: 同じ名前で所有者が
	// 違うものは別人なので、検索は UID で当てる。
	It("writes one owned revision when a task begins", func() {
		fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		fx.reconcile()

		revs := revisionsOf(tk.UID)
		Expect(revs).To(HaveLen(1), "begin must copy the flow and handler specs into one ControllerRevision")
		rev := revs[0]
		Expect(rev.OwnerReferences).To(HaveLen(1))
		Expect(rev.OwnerReferences[0].UID).To(Equal(tk.UID), "the copy goes with the task")
		Expect(rev.OwnerReferences[0].Controller).NotTo(BeNil())
		Expect(*rev.OwnerReferences[0].Controller).To(BeTrue())
		Expect(rev.Data.Raw).NotTo(BeEmpty(), "the copy holds the flow and handler specs, not an empty shell")
		Expect(bytes.Contains(rev.Data.Raw, []byte(fx.name))).To(BeTrue(),
			"the copy holds this task's definitions rather than some other task's")
	})

	// begin の後も走るのは Job を作る側のまま。写しが増えても開始の合図は
	// 増えない。
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

	// 書いたものは不変: 次の reconcile が触らない。変異は Update 一発。
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

	// bindings が名指す handler の欠落は、最初の run の前に落とす。start 以外
	// の束縛も対象。変異は存在確認の省略。
	It("fails before the first run when a binding names a missing handler", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings[phaseReport] = flowv1alpha1.PhaseBinding{
				Handler: fx.name + "-gone",
				Next:    map[flowv1alpha1.Phase]string{unreachedPhase: "ok"},
			}
		})
		fx.makeHandler()
		fx.makeTask()

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(revisionsOf(tk.UID)).To(BeEmpty(), "a task that never began has nothing to hold still")
	})

	// finally の欠落は既存の記録のまま: 「records a missing cleanup handler
	// without touching the ending」が守る性質。変異は finally の存在確認。
	It("begins without failing when only the finally handler is missing", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: fx.name + "-cleanup-gone", Done: "swept"}
		})
		fx.makeHandler()
		tk := fx.makeTask()

		fx.reconcile()

		tkNow := fx.get()
		Expect(tkNow.Status.Phase).To(Equal(phaseInvestigate), "a missing cleanup handler does not fail the task")
		Expect(revisionsOf(tk.UID)).To(HaveLen(1), "the copy is written without the missing handler")
	})

	// 1 object に収まらない写しは、最初の run の前に落とす。変異は上限確認の
	// 省略。大きさは apiserver が受け付けない量: handler 自体は作れるが、
	// それを丸ごと載せた写しは載せられない、の間を狙う。handler 1 つでは
	// 3MiB の壁に届かないので、bindings が名指す handler を束にして大きく
	// する。
	It("fails before the first run when the copy does not fit in one object", func() {
		// handler 1 つは apiserver に載るが、4 つ束ねた写しは載らない量。写し
		// が重複排除で小さくなる実装に寄らないよう、handler は別名・別内容で
		// 4 つ作る。
		blobOf := func(seed byte) string {
			blob := make([]byte, 1<<20)
			for i := range blob {
				blob[i] = seed + byte(i%26)
			}
			return string(blob)
		}
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			for i := range 4 {
				phase := flowv1alpha1.Phase(fmt.Sprintf("束-%d", i))
				binding := f.Spec.Bindings[phaseInvestigate]
				binding.Handler = fmt.Sprintf("%s-big-%d", fx.name, i)
				f.Spec.Bindings[phase] = binding
			}
		})
		for i := range 4 {
			name := fmt.Sprintf("%s-big-%d", fx.name, i)
			seed := byte('a' + i)
			fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
				h.Name = name
				for j := range h.Spec.JobTemplate.Template.Spec.Containers {
					c := &h.Spec.JobTemplate.Template.Spec.Containers[j]
					c.Env = append(c.Env, corev1.EnvVar{Name: "BLOB", Value: blobOf(seed)})
				}
			})
		}
		fx.makeTask()

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed),
			"a copy that fits in no single object must fail the task before its first run")
		Expect(revisionsOf(tk.UID)).To(BeEmpty())
	})

	// 名前は Task から導くので作り直しは衝突する: UID の違う所有者のものは
	// 使わない。変異は UID の突き合わせの省略。
	It("does not reuse a revision owned by a different UID", func() {
		fx.makeFlow()
		fx.makeHandler()
		first := fx.makeTask()

		fx.reconcile()
		firstRevs := revisionsOf(first.UID)
		Expect(firstRevs).To(HaveLen(1))

		Expect(k8sClient.Delete(fx.ctx, first)).To(Succeed())
		second := fx.makeTask()

		fx.reconcile()

		secondRevs := revisionsOf(second.UID)
		Expect(secondRevs).To(HaveLen(1), "the recreated task must hold a copy of its own, not its predecessor's")
		Expect(revisionsOf(first.UID)).To(HaveLen(1),
			"the predecessor's copy stays where it was: the task owns it, and only its deletion takes it away")
	})
})

const unreachedPhase flowv1alpha1.Phase = "おわり-届かない"

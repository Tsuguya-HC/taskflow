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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// A run the framework does not start (ADR-0011). The controller opens a place
// for the answer, says there what may be answered, and reads it back — with
// no idea who wrote it, which is the whole point of the runner being named
// after the state rather than the actor.
var _ = Describe("a run nothing starts", func() {
	var fx *fixture
	const timeout = time.Hour

	BeforeEach(func() { fx = newFixture() })

	// start puts a task on the starting phase with a State handler and runs
	// the reconcile that opens its box.
	start := func() {
		fx.makeFlow()
		fx.makeHandler(stateRunner(timeout))
		fx.makeTask()
		fx.reconcile() // settles the starting phase
		fx.reconcile() // opens the place the answer goes
	}

	It("opens a place for the answer instead of starting a Job", func() {
		start()

		box := fx.box()
		Expect(box.Data).To(BeEmpty(), "the box is opened empty; the answer is somebody else's to write")
		Expect(box.Annotations).To(HaveKeyWithValue(contract.AnnotationPhase, string(phaseInvestigate)))

		var choices []string
		Expect(json.Unmarshal([]byte(box.Annotations[contract.AnnotationChoices]), &choices)).To(Succeed())
		Expect(choices).To(ConsistOf("ok"),
			"the vocabulary comes from the flow's own next, put where whoever answers will see it")

		Expect(box.Labels).To(SatisfyAll(
			HaveKeyWithValue(runner.LabelManagedBy, runner.ManagedBy),
			HaveKeyWithValue(runner.LabelTaskUID, string(fx.taskUID)),
			HaveKeyWithValue(runner.LabelRunID, "1"),
		), "an object the framework made, selectable as one (ADR-0011 決定5)")
		Expect(box.OwnerReferences).To(HaveLen(1), "it goes when the task goes")

		var jobs batchv1.JobList
		Expect(k8sClient.List(fx.ctx, &jobs, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID)})).To(Succeed())
		Expect(jobs.Items).To(BeEmpty(), "nothing is started for this run")
	})

	It("writes down where the answer goes, and when the wait runs out", func() {
		start()

		run := fx.get().Status.CurrentRun
		Expect(run).NotTo(BeNil())
		Expect(run.VerdictBox).To(Equal(runner.VerdictBoxName(fx.name, fx.taskUID, phaseInvestigate, 1)),
			"whoever answers finds the place from the task, without being told a naming rule")
		Expect(run.JobName).To(BeEmpty(), "a run with a box has no Job")
		Expect(run.Deadline).NotTo(BeNil())
		Expect(run.Deadline.Time).To(BeTemporally("~",
			fx.box().CreationTimestamp.Add(timeout), time.Second),
			"the wait is counted from the box, so a restart lands on the same instant")
	})

	It("comes back to look rather than watching", func() {
		fx.makeFlow()
		fx.makeHandler(stateRunner(timeout))
		fx.makeTask()
		fx.reconcile()

		Expect(fx.reconcile().RequeueAfter).To(Equal(verdictPoll))
		Expect(fx.reconcile().RequeueAfter).To(Equal(verdictPoll), "and again, while nothing has been written")
	})

	// verdictPoll is the usual wait, not the only one: with a short deadline
	// closing in, coming back sooner than the deadline is what the box has to
	// offer — waiting the full interval past a deadline this close would poll
	// after silence had already become the answer.
	It("polls sooner than usual as a short deadline closes in", func() {
		const short = 10 * time.Second
		fx.makeFlow()
		fx.makeHandler(stateRunner(short))
		fx.makeTask()
		fx.reconcile() // settles the starting phase
		fx.reconcile() // opens the place the answer goes, and dates it

		deadline := fx.get().Status.CurrentRun.Deadline.Time
		fx.reconciler.Now = func() time.Time { return deadline.Add(-3 * time.Second) }

		Expect(fx.reconcile().RequeueAfter).To(Equal(3*time.Second),
			"this close to a deadline shorter than verdictPoll, the wait is what remains, not the usual interval")
	})

	It("does not open a second box for the same run", func() {
		start()
		created := fx.box().UID

		fx.reconcile()

		Expect(fx.box().UID).To(Equal(created), "the name is deterministic and the run keeps it")
	})

	// The name is written down before the object is created, so a crash
	// between the two leaves a run pointing at a box that is not there. That
	// is the one case where a missing box is repaired rather than refused:
	// this run has never been dated, so it was never answered in.
	It("opens the box a crash left unopened", func() {
		fx.makeFlow()
		fx.makeHandler(stateRunner(timeout))
		fx.makeTask()
		fx.reconcile() // settles the starting phase

		tk := fx.get()
		tk.Status.CurrentRun.VerdictBox = runner.VerdictBoxName(fx.name, fx.taskUID, phaseInvestigate, 1)
		Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())

		Expect(fx.reconcile().RequeueAfter).To(Equal(verdictPoll))

		Expect(fx.box().Data).To(BeEmpty())
		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate), "the run carries on where it left off")
		Expect(fx.get().Status.CurrentRun.Deadline).NotTo(BeNil(), "and is dated once the box is really there")
	})

	// The handler is read once, to learn how long the wait may be. It being
	// gone at that moment is the same broken definition it is anywhere else.
	It("fails a run whose handler went away before it could be dated", func() {
		fx.makeFlow()
		fx.makeHandler(stateRunner(timeout))
		fx.makeTask()
		fx.reconcile() // settles the starting phase

		tk := fx.get()
		tk.Status.CurrentRun.VerdictBox = runner.VerdictBoxName(fx.name, fx.taskUID, phaseInvestigate, 1)
		Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
		Expect(k8sClient.Delete(fx.ctx, &flowv1alpha1.TaskHandler{
			ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
		})).To(Succeed())

		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
	})

	// dateRun's guard against a State handler with no timeout is unreachable
	// through a live apiserver: the CRD's own CEL rule already refuses one at
	// admission, on both create and update. An interceptor stands in for a
	// handler admission never saw, the same technique task_run_test.go's
	// create-race test uses to reach a branch envtest would not otherwise
	// take — without reaching for a fake client that would leave the rest of
	// this suite's guarantees about a real apiserver behind.
	It("fails a run whose handler declares no timeout when dateRun reads it", func() {
		fx.makeFlow()
		fx.makeHandler(stateRunner(timeout))
		fx.makeTask()
		fx.reconcile() // settles the starting phase

		watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		intercepted := interceptor.NewClient(watchClient, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				h, ok := obj.(*flowv1alpha1.TaskHandler)
				if !ok || key.Name != fx.name {
					return c.Get(ctx, key, obj, opts...)
				}
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				h.Spec.Timeout = nil
				return nil
			},
		})
		racer := &TaskReconciler{
			Client: intercepted, Scheme: k8sClient.Scheme(), SidecarImage: sidecarImage, APIReader: k8sClient,
		}

		_, err = racer.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady).Message).
			To(ContainSubstring("declares no timeout"))
	})

	// The CRD's enum constrains what a handler may say, but the runtime does
	// not assume that check has run (ADR-0006 決定5) — a third runner type
	// this binary predates, or a rolling update briefly disagreeing with the
	// CRD, must fail rather than being read as whichever branch an if/else
	// happens to fall through to. Same interceptor technique as the timeout
	// spec above, standing in for a handler this process reads before it
	// agrees with the schema.
	It("fails a run whose handler names a runner type this controller does not know", func() {
		fx.makeFlow()
		fx.makeHandler(stateRunner(timeout))
		fx.makeTask()
		fx.reconcile() // settles the starting phase

		watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		intercepted := interceptor.NewClient(watchClient, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				h, ok := obj.(*flowv1alpha1.TaskHandler)
				if !ok || key.Name != fx.name {
					return c.Get(ctx, key, obj, opts...)
				}
				if err := c.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				h.Spec.Runner.Type = "Bogus"
				return nil
			},
		})
		racer := &TaskReconciler{
			Client: intercepted, Scheme: k8sClient.Scheme(), SidecarImage: sidecarImage, APIReader: k8sClient,
		}

		_, err = racer.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})
		Expect(err).NotTo(HaveOccurred())

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady).Message).
			To(ContainSubstring("Bogus"))

		var boxes corev1.ConfigMapList
		Expect(k8sClient.List(fx.ctx, &boxes, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID)})).To(Succeed())
		Expect(boxes.Items).To(BeEmpty(),
			"an unrecognized runner type must not fall through to opening anything for this run")
	})

	It("moves on the word that was written", func() {
		start()
		fx.answer("ok", "見ました")

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport))
		Expect(tk.Status.History).To(HaveLen(1))
		Expect(tk.Status.History[0].Directory).To(Equal("ok"))
		Expect(tk.Status.History[0].Outcome).To(Equal(string(transition.OutcomeDeclared)))
		Expect(tk.Status.History[0].Reason).To(ContainSubstring("見ました"),
			"the line beside the answer is what a human gets about this run")
	})

	It("escalates a word that is not one of the choices", func() {
		start()
		fx.answer("approved", "")

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(tk.Status.History[0].Outcome).To(Equal(string(transition.OutcomeNoAnswer)))
		Expect(tk.Status.History[0].Reason).To(ContainSubstring("approved"))
	})

	It("keeps waiting when the answer is written empty", func() {
		start()
		fx.answer("  ", "")

		Expect(fx.reconcile().RequeueAfter).To(Equal(verdictPoll))
		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate),
			"an empty word is no word: no declared directory can be spelled that way")
	})

	It("escalates a run nobody answered in time", func() {
		start()
		fx.reconciler.Now = func() time.Time { return time.Now().Add(timeout + time.Minute) }

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(tk.Status.History[0].Outcome).To(Equal(string(transition.OutcomeNoAnswer)),
			"silence is not an approval (P6)")
		Expect(tk.Status.History[0].Reason).To(ContainSubstring(
			runner.VerdictBoxName(fx.name, fx.taskUID, phaseInvestigate, 1)),
			"the reason says where nobody wrote")
	})

	// The fencing of ADR-0011 決定3. An answer already sitting in the place a
	// run is about to be answered in cannot be that run's answer, and the
	// controller has no way to tell one put there in advance from one written
	// the moment the run opened — so it refuses to read either.
	It("fails a run whose answer was waiting before it began", func() {
		fx.makeFlow()
		fx.makeHandler(stateRunner(timeout))
		task := fx.makeTask()
		fx.reconcile() // settles the starting phase, opens nothing yet

		squatter := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      runner.VerdictBoxName(fx.name, fx.taskUID, phaseInvestigate, 1),
				Namespace: resourceNamespace,
				Labels:    map[string]string{runner.LabelTaskUID: string(task.UID)},
			},
			Data: map[string]string{contract.KeyVerdict: "ok"},
		}
		Expect(k8sClient.Create(fx.ctx, squatter)).To(Succeed())

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed),
			"the place was taken before the run began, and the pre-written answer is not read")
		Expect(tk.Status.History).To(BeEmpty(), "nothing was decided")
		Expect(meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady).Message).
			To(ContainSubstring("already existed before the run began"))
	})

	// The repair path — a name already in status, its box not there yet, and
	// this run never dated — cannot tell a squatter from its own earlier
	// create having landed despite an error this process saw. An interceptor
	// stands in for that landing: it forces ensureVerdictBox's Get to report
	// NotFound once even though the box is already there, the same technique
	// task_run_test.go's create-race test uses for the Job, so the real
	// apiserver's Create answers with the real AlreadyExists.
	It("retries rather than failing when its own create landed late", func() {
		fx.makeFlow()
		fx.makeHandler(stateRunner(timeout))
		fx.makeTask()
		fx.reconcile() // settles the starting phase

		tk := fx.get()
		boxName := runner.VerdictBoxName(fx.name, fx.taskUID, phaseInvestigate, 1)
		tk.Status.CurrentRun.VerdictBox = boxName
		Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())

		landedLate := runner.BuildVerdictBox(tk, phaseInvestigate, 1, []string{"ok"})
		Expect(k8sClient.Create(fx.ctx, landedLate)).To(Succeed())

		watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
		Expect(err).NotTo(HaveOccurred())
		forcedNotFound := false
		intercepted := interceptor.NewClient(watchClient, interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok && key.Name == boxName && !forcedNotFound {
					forcedNotFound = true
					return apierrors.NewNotFound(corev1.Resource("configmaps"), key.Name)
				}
				return c.Get(ctx, key, obj, opts...)
			},
		})
		racer := &TaskReconciler{
			Client: intercepted, Scheme: k8sClient.Scheme(), SidecarImage: sidecarImage, APIReader: intercepted,
		}

		_, err = racer.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})
		Expect(err).To(HaveOccurred(), "the repair path retries rather than failing for good")
		Expect(forcedNotFound).To(BeTrue(), "the race this test drives at was never exercised")
		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate),
			"not failed — the next reconcile's Get is what actually decides this")

		// And the next ordinary reconcile does settle it: the Get finds the
		// box, sees this task owns it, and dates the run from it.
		Expect(fx.reconcile().RequeueAfter).To(Equal(verdictPoll))
		Expect(fx.get().Status.CurrentRun.Deadline).NotTo(BeNil())
	})

	It("fails a run whose place was taken away while it waited", func() {
		start()
		Expect(k8sClient.Delete(fx.ctx, fx.box())).To(Succeed())

		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed),
			"a run the framework can no longer be answered about is not a run still being considered")
	})

	It("fails a run whose box belongs to something else", func() {
		start()

		box := fx.box()
		box.OwnerReferences = nil
		Expect(k8sClient.Update(fx.ctx, box)).To(Succeed())

		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
	})

	// A forged ownerReference passes IsControlledBy — it is free-form
	// metadata its author can write to name any UID they like — but it
	// cannot produce the UID the apiserver assigned to the object this run's
	// own Create actually made. Deleting the real box and standing a new one
	// up under the same name, ownerReference forged to name the real task,
	// is exactly what VerdictBoxUID is there to catch.
	It("fails a run whose box was replaced under the same name", func() {
		start()

		Expect(k8sClient.Delete(fx.ctx, fx.box())).To(Succeed())

		task := fx.get()
		controller := true
		replaced := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      runner.VerdictBoxName(fx.name, fx.taskUID, phaseInvestigate, 1),
				Namespace: resourceNamespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: flowv1alpha1.GroupVersion.String(),
					Kind:       "Task",
					Name:       task.Name,
					UID:        task.UID,
					Controller: &controller,
				}},
			},
			Data: map[string]string{contract.KeyVerdict: "ok"},
		}
		Expect(k8sClient.Create(fx.ctx, replaced)).To(Succeed())

		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed),
			"the name and a forged ownerReference are not the object this run's own create made")
	})

	// A run already under way is driven by what it has, not by what the
	// handler says today (ADR-0007): the box is the record that this run is
	// answered rather than started.
	It("keeps driving a State run whose handler has since become a Job", func() {
		start()
		var handler flowv1alpha1.TaskHandler
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, &handler)).To(Succeed())
		jobRunner()(&handler)
		Expect(k8sClient.Update(fx.ctx, &handler)).To(Succeed())

		fx.answer("ok", "")
		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(phaseReport), "the run finished the way it started")
		var jobs batchv1.JobList
		Expect(k8sClient.List(fx.ctx, &jobs, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID), runner.LabelRunID: "1"})).To(Succeed())
		Expect(jobs.Items).To(BeEmpty(), "run 1 was never started, and does not become started")
	})

	It("refuses to read a box through a cache it cannot have", func() {
		fx.makeFlow()
		fx.makeHandler(stateRunner(timeout))
		fx.makeTask()
		fx.reconcile()
		fx.reconciler.APIReader = nil

		_, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})
		Expect(err).To(MatchError(ContainSubstring("uncached reader")))
		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate), "nothing was decided about the run")
	})

	// A run whose vocabulary is edited away has nothing left to judge an
	// answer against. For the cleanup run that is not a Failed — the ending
	// is already decided and does not move (ADR-0009 決定2) — it is a cleanup
	// that did not happen.
	It("records a cleanup run whose declaration went away while it waited", func() {
		cleanup := fx.name + "-cleanup"
		flow := fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: cleanup, Done: "cleaned"}
		})
		fx.makeHandler(stateRunner(timeout))
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = cleanup
			h.Spec.Phase = flowv1alpha1.PhaseFinally
			stateRunner(timeout)(h)
		})
		fx.makeTask()

		fx.reconcile()
		fx.reconcile()
		fx.answer("ok", "")
		fx.reconcile() // the task reaches its ending, owing a cleanup
		fx.reconcile() // opens the place the cleanup run is answered in

		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, flow)).To(Succeed())
		flow.Spec.Finally = nil
		Expect(k8sClient.Update(fx.ctx, flow)).To(Succeed())

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "the ending stands")
		Expect(tk.Status.History[1].Outcome).To(Equal(string(transition.OutcomeNoAnswer)))
		Expect(meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady).Reason).
			To(Equal(taskstate.ReasonFinallyFailed))
	})

	// The cleanup run is a run like any other (ADR-0009 決定3), so it is
	// answered the same way — and the ending it follows does not move while
	// it waits.
	It("is answered the same way for the cleanup run", func() {
		cleanup := fx.name + "-cleanup"
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: cleanup, Done: "cleaned"}
			f.Spec.TTL = &flowv1alpha1.TTLSpec{Succeeded: &metav1.Duration{Duration: time.Hour}}
		})
		fx.makeHandler(stateRunner(timeout))
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = cleanup
			h.Spec.Phase = flowv1alpha1.PhaseFinally
			stateRunner(timeout)(h)
		})
		fx.makeTask()

		fx.reconcile() // settles the starting phase
		fx.reconcile() // opens the place run 1 is answered in
		fx.answer("ok", "")
		fx.reconcile() // settles run 1: the task reaches its ending, owing a cleanup
		fx.reconcile() // opens the place the cleanup run is answered in

		Expect(fx.get().Status.Phase).To(Equal(phaseReport), "the ending does not move while the cleanup waits")
		Expect(fx.get().Status.ExpiresAt).To(BeNil(), "and it is not dated until the cleanup settles")
		fx.answerFor(flowv1alpha1.PhaseFinally, 2, "cleaned", "")

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport))
		Expect(tk.Status.CurrentRun).To(BeNil())
		Expect(tk.Status.History).To(HaveLen(2))
		Expect(tk.Status.History[1].Phase).To(Equal(flowv1alpha1.PhaseFinally))
		Expect(tk.Status.History[1].Directory).To(Equal("cleaned"))
		Expect(tk.Status.ExpiresAt).NotTo(BeNil(), "the cleanup settled, so the task is dated")
	})
})

// The shelf a run without a pod leaves is laid by the next run that has one
// (ADR-0011 決定7): the numbers on results/ have to run without gaps, because
// a hole is only readable by whoever already knows it is there (ADR-0004).
var _ = Describe("the shelf a run nothing started leaves behind", func() {
	var fx *fixture
	const timeout = time.Hour

	BeforeEach(func() { fx = newFixture() })

	// A flow of two phases: the first is answered from outside, the second
	// runs a pod on the flow's own workspace — which is the only kind of
	// volume that has a shelf at all.
	twoPhases := func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Workspace = &flowv1alpha1.FlowWorkspace{}
			f.Spec.Bindings[phaseReport] = flowv1alpha1.PhaseBinding{
				Handler: fx.name + "-job",
				Next:    map[flowv1alpha1.Phase]string{"おわり": "sent"},
			}
		})
		fx.makeHandler(stateRunner(timeout))
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = fx.name + "-job"
			h.Spec.Phase = phaseReport
			h.Spec.Workspace.Volume = contract.WorkspaceVolume
			spec := &h.Spec.JobTemplate.Template.Spec
			spec.Volumes = nil
			spec.Containers[0].VolumeMounts[0].Name = contract.WorkspaceVolume
		})
		fx.makeTask()
	}

	It("records how each run was driven, so the shelf can be reconstructed", func() {
		twoPhases()
		fx.reconcile() // settles the starting phase
		fx.reconcile() // opens the place run 1 is answered in
		fx.answer("ok", "")
		fx.reconcile() // settles run 1

		history := fx.get().Status.History
		Expect(history).To(HaveLen(1))
		Expect(history[0].Runner).To(Equal(flowv1alpha1.RunnerState),
			"nothing ran, and the line a human reads says so")
	})

	It("tells the next run's prepare to lay it", func() {
		twoPhases()
		fx.reconcile()
		fx.reconcile()
		fx.answer("ok", "")
		fx.reconcile() // settles run 1 and moves to 報告
		fx.reconcile() // creates the Job for run 2

		var job batchv1.Job
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, phaseReport, 2, 0), Namespace: resourceNamespace,
		}, &job)).To(Succeed())

		prepare := job.Spec.Template.Spec.InitContainers[0]
		Expect(prepare.Args).To(ContainElements(
			"--"+contract.FlagShelve, workspacePath+"/results/1/ok"),
			"run 1 sealed nothing, so run 2 lays what it would have left")
	})

	// A run with a pod seals its own directory, so nothing is laid for it —
	// and a shelf entry the framework fabricated for a run that was supposed
	// to write one would be evidence of something that never happened.
	It("lays nothing for a run that had a pod of its own", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Workspace = &flowv1alpha1.FlowWorkspace{}
			f.Spec.Bindings[phaseInvestigate] = flowv1alpha1.PhaseBinding{
				Handler: fx.name,
				Next:    map[flowv1alpha1.Phase]string{phaseInvestigate: "more", phaseReport: "ok"},
			}
		})
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Spec.Workspace.Volume = contract.WorkspaceVolume
			spec := &h.Spec.JobTemplate.Template.Spec
			spec.Volumes = nil
			spec.Containers[0].VolumeMounts[0].Name = contract.WorkspaceVolume
		})
		fx.makeTask()

		fx.reconcile() // settles the starting phase
		fx.reconcile() // creates the Job for run 1
		finishJobWith(fx, fx.job(1), "more")
		fx.reconcile() // the rework lands on run 2
		fx.reconcile() // creates the Job for run 2

		prepare := fx.job(2).Spec.Template.Spec.InitContainers[0]
		Expect(prepare.Args).NotTo(ContainElement("--"+contract.FlagShelve),
			"run 1 had a pod, and that pod's publish shelved it")
	})
})

// finishJobWith is the Job controller's part, written by hand: the pod it
// would have made, the message the handler left behind, and the conditions in
// the order the apiserver validates them.
func finishJobWith(fx *fixture, job *batchv1.Job, message string) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name,
			Namespace: resourceNamespace,
			Labels:    map[string]string{batchv1.ControllerUidLabel: string(job.UID)},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: agentName, Image: agentImage}},
		},
	}
	Expect(k8sClient.Create(fx.ctx, pod)).To(Succeed())
	DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, pod) })
	pod.Status.Phase = corev1.PodSucceeded
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  agentName,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: message}},
	}}
	Expect(k8sClient.Status().Update(fx.ctx, pod)).To(Succeed())

	now := metav1.Now()
	job.Status.StartTime = &now
	job.Status.CompletionTime = &now
	job.Status.Conditions = append(job.Status.Conditions,
		batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
		batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
	Expect(k8sClient.Status().Update(fx.ctx, job)).To(Succeed())
}

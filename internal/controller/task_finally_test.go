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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/metrics"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// The cleanup run is the only run that starts after a task has stopped, so
// these specs are about two things at once: that it runs at all, and that
// running it leaves the ending exactly as the work left it (ADR-0009).
var _ = Describe("the cleanup run that follows an ending", func() {
	var fx *fixture
	var clock time.Time

	const (
		succeededTTL = time.Hour
		failedTTL    = 168 * time.Hour
		dirDone      = "cleaned"
	)

	// cleanupHandler is a second TaskHandler, named apart from the phase
	// handler because the flow names the two separately. It declares
	// phase: Finally, which the framework allows and does not check: what a
	// handler says it is for is documentation, and the flow decides where it
	// is actually used (ADR-0006 決定4).
	cleanupName := func() string { return fx.name + "-cleanup" }

	BeforeEach(func() {
		fx = newFixture()
		clock = time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
		fx.reconciler.Now = func() time.Time { return clock }
	})

	withCleanup := func(f *flowv1alpha1.TaskFlow) {
		f.Spec.TTL = &flowv1alpha1.TTLSpec{
			Succeeded: &metav1.Duration{Duration: succeededTTL},
			Failed:    &metav1.Duration{Duration: failedTTL},
		}
		f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: cleanupName(), Done: dirDone}
	}

	makeCleanupHandler := func() {
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = cleanupName()
			h.Spec.Phase = flowv1alpha1.PhaseFinally
		})
	}

	// finish is the Job controller's part, written by hand: the pod it would
	// have made, the message the handler left in its termination message, and
	// the conditions in the order the apiserver validates them.
	finish := func(job *batchv1.Job, message string) {
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

	// runPhase drives the task's first and only phase to the ending message
	// says, leaving the task stopped — and, when its flow declares one, owed a
	// cleanup run.
	runPhase := func(message string) {
		fx.reconcile() // begin
		fx.reconcile() // create the Job
		finish(fx.job(1), message)
		fx.reconcile() // settle
	}

	// cleanupJob is the Job of the cleanup run. It is run 2 here: the phase
	// spent run 1, and the cleanup is a run like any other (ADR-0009 決定3),
	// which is exactly why the name resolves at all.
	cleanupJob := func() *batchv1.Job {
		var job batchv1.Job
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, flowv1alpha1.PhaseFinally, 2, 0), Namespace: resourceNamespace,
		}, &job)).To(Succeed())
		return &job
	}

	// envOf is what one variable reached the handler's container as. The
	// cleanup run is told about the ending it follows, and this is the only
	// route those three values take.
	envOf := func(job *batchv1.Job, name string) string {
		for _, c := range job.Spec.Template.Spec.Containers {
			if c.Name != agentName {
				continue
			}
			for _, e := range c.Env {
				if e.Name == name {
					return e.Value
				}
			}
		}
		Fail("the Job carries no " + name)
		return ""
	}

	It("starts the cleanup run when the task stops, without moving the ending", func() {
		fx.makeFlow(withCleanup)
		fx.makeHandler()
		makeCleanupHandler()
		fx.makeTask()
		runPhase("ok\nnothing to report")

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "報告 is where the work ended")
		Expect(taskstate.InFinally(&tk.Status)).To(BeTrue(), "a stopped task owed a cleanup run holds its ref")
		Expect(tk.Status.CurrentRun.RunID).To(BeEquivalentTo(2))
		Expect(tk.Status.ExpiresAt).To(BeNil(),
			"dating the task now would be dating it for good — a cleanup run could be deleted out from under itself")

		fx.reconcile() // creates the cleanup Job
		job := cleanupJob()
		Expect(directoriesOf(job)).To(ConsistOf(dirDone),
			"the cleanup run has one thing to say, so it is given one directory to say it in")
		Expect(job.Spec.Template.Spec.Containers[0].Image).To(Equal(agentImage))
		// What the cleanup run is told about the ending it follows. FLOW_PHASE
		// still says what this run is; where the task stopped is a separate
		// value, because they are separate facts here and nowhere else.
		Expect(envOf(job, runner.EnvPhase)).To(Equal(string(flowv1alpha1.PhaseFinally)))
		Expect(envOf(job, runner.EnvEnding)).To(Equal(string(transition.EndingUndeclared)),
			"this flow never declared what 報告 means, and the cleanup run is told exactly that")
		Expect(envOf(job, runner.EnvEndingPhase)).To(Equal(string(phaseReport)))
		Expect(envOf(job, runner.EnvEndingOutcome)).To(Equal(string(transition.OutcomeDeclared)))

		finish(job, dirDone+"\nremoved 2 branches")
		fx.reconcile()

		tk = fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "the cleanup run never revises where the task ended")
		Expect(tk.Status.CurrentRun).To(BeNil(), "nothing follows the cleanup run")
		Expect(tk.Status.History).To(HaveLen(2))
		h := tk.Status.History[1]
		Expect(h.Phase).To(Equal(flowv1alpha1.PhaseFinally))
		Expect(h.RunID).To(BeEquivalentTo(2))
		Expect(h.Directory).To(Equal(dirDone))
		Expect(h.Outcome).To(Equal(string(transition.OutcomeDeclared)))
		Expect(h.Reason).To(ContainSubstring("removed 2 branches"))
		Expect(meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)).To(BeNil(),
			"this task ended well and was tidied up after; there is nothing to say")
		Expect(tk.Status.ExpiresAt).NotTo(BeNil())
		Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(succeededTTL)))

		Expect(testutil.ToFloat64(metrics.FinallyOutcomes.With(prometheus.Labels{
			metrics.LabelFlow: fx.name, metrics.LabelOutcome: string(transition.OutcomeDeclared),
		}))).To(BeNumerically("==", 1))
		// The ending was counted once, when the work reached it, and the
		// cleanup run must not have counted a second one. Folding the two
		// numbers together is the mistake this design set out not to inherit.
		Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(prometheus.Labels{
			metrics.LabelFlow: fx.name, metrics.LabelPhase: string(phaseReport),
			metrics.LabelSeverity: string(transition.EndingUndeclared),
		}))).To(BeNumerically("==", 1))
	})

	// Escalated is the motive for the whole feature: nothing can be bound to
	// it, so until now a task that ended there had no run left in which to put
	// anything back.
	It("runs after an escalation, which nothing could be bound to", func() {
		fx.makeFlow(withCleanup)
		fx.makeHandler()
		makeCleanupHandler()
		fx.makeTask()
		runPhase("") // exit 0, said nothing

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(taskstate.InFinally(&tk.Status)).To(BeTrue())

		fx.reconcile()
		Expect(envOf(cleanupJob(), runner.EnvEnding)).To(Equal(string(transition.EndingEscalated)))
		Expect(envOf(cleanupJob(), runner.EnvEndingOutcome)).To(Equal(string(transition.OutcomeNoAnswer)),
			"a cleanup run may report differently for a run that said nothing, so it is told which it was")
		finish(cleanupJob(), dirDone+"\nthe branch is gone")
		fx.reconcile()

		tk = fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated),
			"the escalation stands; somebody still has to come and look at it")
		ready := meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Reason).To(Equal(string(transition.OutcomeNoAnswer)),
			"the reason a human is wanted is still the run that said nothing, not the cleanup")
		Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)),
			"an escalation tidied up after is no less an escalation")
	})

	It("says so when the cleanup run does not report it cleaned up", func() {
		fx.makeFlow(withCleanup)
		fx.makeHandler()
		makeCleanupHandler()
		fx.makeTask()
		runPhase("ok\nnothing to report")
		fx.reconcile()

		finish(cleanupJob(), "") // exit 0, said nothing
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "the work still ended where it ended")
		Expect(tk.Status.History).To(HaveLen(2))
		Expect(tk.Status.History[1].Directory).To(BeEmpty())
		Expect(tk.Status.History[1].Outcome).To(Equal(string(transition.OutcomeNoAnswer)))

		ready := meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(taskstate.ReasonFinallyFailed),
			"a cleanup that did not happen is its own reason, not one of the ending's")
		Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)),
			"a mess swept away within the hour is a mess nobody sees")
		Expect(fx.announced()).To(ContainElement(ContainSubstring("did not report it cleaned up")))
		Expect(testutil.ToFloat64(metrics.FinallyOutcomes.With(prometheus.Labels{
			metrics.LabelFlow: fx.name, metrics.LabelOutcome: string(transition.OutcomeNoAnswer),
		}))).To(BeNumerically("==", 1))
	})

	// A definition problem found by the cleanup run is not the task's ending
	// being wrong — the ending was decided before this run started. It is
	// recorded as a cleanup that did not happen, which is what it is.
	It("records a missing cleanup handler without touching the ending", func() {
		fx.makeFlow(withCleanup)
		fx.makeHandler()
		fx.makeTask() // no cleanup handler exists
		runPhase("ok\nnothing to report")

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "a missing cleanup handler does not fail the task")
		Expect(tk.Status.CurrentRun).To(BeNil())
		Expect(tk.Status.History).To(HaveLen(2))
		Expect(tk.Status.History[1].Phase).To(Equal(flowv1alpha1.PhaseFinally))
		Expect(tk.Status.History[1].Reason).To(ContainSubstring("does not exist"))
		ready := meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Reason).To(Equal(taskstate.ReasonFinallyFailed))
		Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
	})

	// A flow broken before anything could run has an ending with no run behind
	// it. History's last line is then some earlier run's verdict — or there is
	// no line at all — and either way it is not this ending's, so the cleanup
	// run is told nothing rather than something untrue (P8).
	It("tells the cleanup run nothing when no run reached the ending", func() {
		fx.makeFlow(withCleanup, func(f *flowv1alpha1.TaskFlow) { f.Spec.Start = "nowhere" })
		makeCleanupHandler()
		fx.makeTask()
		fx.reconcile() // the flow starts at a phase nothing binds

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(taskstate.InFinally(&tk.Status)).To(BeTrue(),
			"a definition being wrong is no reason to leave behind whatever the task already made")
		Expect(tk.Status.History).To(BeEmpty())

		fx.reconcile()
		var job batchv1.Job
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, flowv1alpha1.PhaseFinally, 1, 0), Namespace: resourceNamespace,
		}, &job)).To(Succeed())
		Expect(envOf(&job, runner.EnvEnding)).To(Equal(string(transition.EndingFailed)))
		Expect(envOf(&job, runner.EnvEndingPhase)).To(Equal(string(flowv1alpha1.PhaseFailed)))
		Expect(envOf(&job, runner.EnvEndingOutcome)).To(BeEmpty())
	})

	// Whether a cleanup is owed is read off the task, not off the flow, so a
	// flow that declares one today reaches the tasks it starts tomorrow and
	// leaves the ones that already stopped alone (ADR-0009 決定7). The task
	// stopped with expiresAt already stamped, so the second reconcile short-
	// circuits at that check before terminal() is ever reached — the flow's
	// edit is not read at all. It is the pairing of no ref and a date already
	// there that keeps a late-added finally from reaching a task like this
	// one; the assertions below are what shows that holds.
	It("does not hand a cleanup run to a task that had already stopped", func() {
		flow := fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.TTL = &flowv1alpha1.TTLSpec{
				Succeeded: &metav1.Duration{Duration: succeededTTL},
				Failed:    &metav1.Duration{Duration: failedTTL},
			}
		})
		fx.makeHandler()
		makeCleanupHandler()
		fx.makeTask()
		runPhase("ok\nnothing to report")

		stopped := fx.get()
		Expect(stopped.Status.CurrentRun).To(BeNil())
		Expect(stopped.Status.ExpiresAt).NotTo(BeNil(), "a task owed no cleanup is dated the moment it stops")

		flow.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: cleanupName(), Done: dirDone}
		Expect(k8sClient.Update(fx.ctx, flow)).To(Succeed())
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.CurrentRun).To(BeNil())
		Expect(tk.Status.History).To(HaveLen(1), "nothing ran; the task was finished with before the flow said finally")
		Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", stopped.Status.ExpiresAt.Time))
		var job batchv1.Job
		err := k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, flowv1alpha1.PhaseFinally, 2, 0), Namespace: resourceNamespace,
		}, &job)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "no cleanup run was started")
	})

	// outcomeOf looks the ending's run up by number rather than reading
	// history's last line, because a run that never settles leaves the two
	// disagreeing: the tail is then some earlier run's verdict, not this
	// ending's (決定 6, P8).
	It("tells the cleanup run nothing when the run before it never settled, rather than history's last line", func() {
		reportHandler := fx.name + "-report"
		flow := fx.makeFlow(withCleanup, func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings[phaseReport] = flowv1alpha1.PhaseBinding{
				Handler: reportHandler,
				Next:    map[flowv1alpha1.Phase]string{"完了": "ok"},
			}
		})
		fx.makeHandler()
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = reportHandler
			h.Spec.Phase = phaseReport
		})
		makeCleanupHandler()
		fx.makeTask()
		runPhase("ok\nnothing to report") // run 1 (調査) settles, Declared

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport))
		Expect(tk.Status.CurrentRun.RunID).To(BeEquivalentTo(2))
		Expect(tk.Status.History).To(HaveLen(1))

		fx.reconcile() // creates run 2's Job (報告)
		var reportJob batchv1.Job
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, phaseReport, 2, 0), Namespace: resourceNamespace,
		}, &reportJob)).To(Succeed())

		// 報告 loses its binding while run 2 is in flight: the reconcile that
		// reads its answer finds nothing left to read it against, so run 2
		// never settles — history stays at one line, run 1's.
		delete(flow.Spec.Bindings, phaseReport)
		Expect(k8sClient.Update(fx.ctx, flow)).To(Succeed())

		finish(&reportJob, "ok\nnothing to report")
		fx.reconcile()

		tk = fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(tk.Status.History).To(HaveLen(1), "run 2 never settled, so it left nothing behind")
		Expect(taskstate.InFinally(&tk.Status)).To(BeTrue())
		Expect(tk.Status.CurrentRun.RunID).To(BeEquivalentTo(3))

		fx.reconcile() // creates the cleanup Job, run 3
		var cleanup batchv1.Job
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, flowv1alpha1.PhaseFinally, 3, 0), Namespace: resourceNamespace,
		}, &cleanup)).To(Succeed())

		Expect(envOf(&cleanup, runner.EnvEnding)).To(Equal(string(transition.EndingFailed)))
		Expect(envOf(&cleanup, runner.EnvEndingPhase)).To(Equal(string(flowv1alpha1.PhaseFailed)))
		Expect(envOf(&cleanup, runner.EnvEndingOutcome)).To(BeEmpty(),
			"history's one line is run 1's verdict, not this ending's — reporting nothing is the honest answer")
	})

	// runSpec's finally branch has its own way to say no run may be read: a
	// flow that no longer declares one at all. A cleanup run already in
	// flight when that happens must fail the way any other broken definition
	// found mid-run does, not crash on a nil spec.finally.
	It("fails the cleanup run safely when spec.finally is edited away while it is in flight", func() {
		flow := fx.makeFlow(withCleanup)
		fx.makeHandler()
		makeCleanupHandler()
		fx.makeTask()
		runPhase("ok\nnothing to report")
		fx.reconcile() // creates the cleanup Job
		job := cleanupJob()

		flow.Spec.Finally = nil
		Expect(k8sClient.Update(fx.ctx, flow)).To(Succeed())

		finish(job, dirDone+"\nremoved 2 branches")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "a decided ending does not move")
		Expect(tk.Status.History).To(HaveLen(2))
		Expect(tk.Status.History[1].Reason).To(ContainSubstring("no longer says what run"))
		ready := meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Reason).To(Equal(taskstate.ReasonFinallyFailed))
		Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
	})

	// The cleanup run is a run like any other (ADR-0009 決定3), including the
	// infrastructure retry allowance a phase's run gets when nothing ever
	// pulled — that allowance must survive resolving the cleanup handler
	// through spec.finally rather than through the bindings table.
	It("retries the cleanup run under the same runID when the handler never got to run", func() {
		fx.makeFlow(withCleanup)
		fx.makeHandler()
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = cleanupName()
			h.Spec.Phase = flowv1alpha1.PhaseFinally
			h.Spec.MaxInfraRetries = 1
		})
		fx.makeTask()
		runPhase("ok\nnothing to report")
		fx.reconcile() // creates the cleanup Job
		job := cleanupJob()

		// The pod exists, but no container in it ever terminated — never
		// pulled, the same infrastructure shape task_finish_test.go's own
		// infra-retry spec uses.
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

		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = append(job.Status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded},
			batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded})
		Expect(k8sClient.Status().Update(fx.ctx, job)).To(Succeed())

		fx.reconcile() // records the infra retry rather than settling the cleanup as failed

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "a decided ending does not move")
		Expect(taskstate.InFinally(&tk.Status)).To(BeTrue())
		Expect(tk.Status.CurrentRun.RunID).To(BeEquivalentTo(2), "nothing was decided, so no run was spent")
		Expect(tk.Status.CurrentRun.InfraRetries).To(BeEquivalentTo(1))

		fx.reconcile() // creates the retry's Job
		var retry batchv1.Job
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, flowv1alpha1.PhaseFinally, 2, 1), Namespace: resourceNamespace,
		}, &retry)).To(Succeed())

		// The retry fails the same way, and the allowance — 1 — is spent.
		retryPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      retry.Name,
				Namespace: resourceNamespace,
				Labels:    map[string]string{batchv1.ControllerUidLabel: string(retry.UID)},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: agentName, Image: agentImage}},
			},
		}
		Expect(k8sClient.Create(fx.ctx, retryPod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, retryPod) })

		retryNow := metav1.Now()
		retry.Status.StartTime = &retryNow
		retry.Status.Conditions = append(retry.Status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded},
			batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded})
		Expect(k8sClient.Status().Update(fx.ctx, &retry)).To(Succeed())

		fx.reconcile()

		tk = fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "a decided ending does not move")
		Expect(tk.Status.History).To(HaveLen(2))
		Expect(tk.Status.History[1].Reason).To(ContainSubstring("never started"))
		Expect(tk.Status.History[1].Outcome).To(Equal(string(transition.OutcomeNoAnswer)))
		ready := meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Reason).To(Equal(taskstate.ReasonFinallyFailed))
	})

	// The declared directory a cleanup run may answer with travels two
	// separate routes: FLOW_DIRECTORIES, baked into the Job once at creation,
	// and the directories driveRun reads through runSpec again when the Job
	// finishes, to judge the answer against. This spec pins the two to the
	// same declaration (spec.finally.done) by changing it after the Job
	// exists — if settling still read the value baked in at creation, the
	// pod's answer, written under the new name, would come back NoAnswer.
	It("collects the cleanup run's answer against the declaration read at settling time, not the one baked into the Job", func() {
		flow := fx.makeFlow(withCleanup)
		fx.makeHandler()
		makeCleanupHandler()
		fx.makeTask()
		runPhase("ok\nnothing to report")
		fx.reconcile() // creates the cleanup Job, FLOW_DIRECTORIES baked to dirDone
		job := cleanupJob()
		Expect(directoriesOf(job)).To(ConsistOf(dirDone))

		const renamedDone = "cleaned-v2"
		flow.Spec.Finally.Done = renamedDone
		Expect(k8sClient.Update(fx.ctx, flow)).To(Succeed())

		finish(job, renamedDone+"\nremoved 2 branches")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.History).To(HaveLen(2))
		h := tk.Status.History[1]
		Expect(h.Directory).To(Equal(renamedDone))
		Expect(h.Outcome).To(Equal(string(transition.OutcomeDeclared)),
			"the pod named the directory spec.finally.done carries now; settling re-read that "+
				"declaration rather than trusting FLOW_DIRECTORIES as it was baked into the Job")
		Expect(meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)).To(BeNil(),
			"a cleanup that reported done leaves nothing to say")
	})

	// A task waiting on its cleanup run has a currentRun, which is otherwise
	// the mark of a run in flight — and a flow deleted out from under a run in
	// flight fails the task. It must not do that here: the task already
	// finished, and a deleted flow is no reason to rewrite how.
	It("keeps the ending when the flow is deleted while a cleanup run is owed", func() {
		flow := fx.makeFlow(withCleanup)
		fx.makeHandler()
		makeCleanupHandler()
		fx.makeTask()
		runPhase("ok\nnothing to report")
		Expect(k8sClient.Delete(fx.ctx, flow)).To(Succeed())

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "a deleted flow cannot turn a task that finished into one that failed")
		Expect(tk.Status.History).To(HaveLen(1))
		Expect(tk.Status.ExpiresAt).To(BeNil(),
			"there is no flow left to read a ttl or a cleanup run from, so the task waits for one to come back")
	})
})

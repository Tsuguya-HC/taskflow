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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// A fork (ADR-0013): 調査 chooses among security and logic, tests always
// runs, and all of them meet at 報告.
var _ = Describe("a fork", func() {
	const (
		security = phaseSecurity
		logic    = phaseLogic
		tests    = phaseTests
		dirDone  = answerDone
		dirStuck = answerStuck
	)
	var fx *fixture
	BeforeEach(func() { fx = newFixture() })

	// Handler names are object names, which a phase named in Japanese is not.
	handlerFor := func(phase flowv1alpha1.Phase) string {
		if phase == phaseReport {
			return fx.name + "-report"
		}
		return fx.name + "-" + string(phase)
	}

	// setUp makes the fork's flow and a handler for every phase of it.
	setUp := func(mut ...func(*flowv1alpha1.TaskHandler)) {
		toReport := map[flowv1alpha1.Phase]string{phaseReport: dirDone, flowv1alpha1.PhaseEscalated: dirStuck}
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings = map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
				phaseInvestigate: {
					Handler: fx.name,
					Next: map[flowv1alpha1.Phase]string{
						security: string(security), logic: string(logic), flowv1alpha1.PhaseEscalated: dirStuck,
					},
					Join: &flowv1alpha1.JoinSpec{Phase: phaseReport, Always: []flowv1alpha1.Phase{tests}},
				},
				security:    {Handler: handlerFor(security), Next: toReport},
				logic:       {Handler: handlerFor(logic), Next: toReport},
				tests:       {Handler: handlerFor(tests), Next: toReport},
				phaseReport: {Handler: handlerFor(phaseReport), Next: map[flowv1alpha1.Phase]string{phaseDone: "ok"}},
			}
		})
		fx.makeHandler()
		for _, phase := range []flowv1alpha1.Phase{security, logic, tests, phaseReport} {
			fx.makeHandler(append([]func(*flowv1alpha1.TaskHandler){func(h *flowv1alpha1.TaskHandler) {
				h.Name = handlerFor(phase)
				h.Spec.Phase = phase
			}}, mut...)...)
		}
	}

	jobOf := func(phase flowv1alpha1.Phase, runID int32) *batchv1.Job {
		var job batchv1.Job
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, phase, runID, 0), Namespace: resourceNamespace,
		}, &job)).To(Succeed())
		return &job
	}

	// answer finishes job as though its publish had named message.
	answer := func(job *batchv1.Job, message string) {
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

	// forked brings a task to its branches' Jobs: the fork's run answered with
	// the branches it chose, and the next reconcile started them.
	forked := func(chose string) {
		fx.makeTask()
		fx.reconcile() // settles the starting phase
		fx.reconcile() // creates the fork's Job
		fork := jobOf(phaseInvestigate, 1)
		Expect(fork.Spec.Template.Spec.InitContainers[1].Args).To(ContainElement("--"+contract.FlagMany),
			"the fork's publish is told it may answer with more than one directory")
		answer(fork, chose)
		fx.reconcile() // settles the fork's run
		fx.reconcile() // starts the branches
	}

	phasesInFlight := func() []flowv1alpha1.Phase {
		runs := fx.get().Status.CurrentRuns
		out := make([]flowv1alpha1.Phase, 0, len(runs))
		for _, r := range runs {
			out = append(out, r.Phase)
		}
		return out
	}

	It("runs every branch it chose and always, then meets at the join", func() {
		setUp()
		forked(string(logic) + "/" + string(security) + "\nboth need a look")

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseInvestigate), "the task stands at the fork while its branches run")
		Expect(tk.Status.History[0].Reason).To(ContainSubstring("both need a look"), "what the fork's run said is kept beside its answer")
		Expect(tk.Status.History).To(HaveLen(1))
		Expect(tk.Status.History[0].Directory).To(Equal("logic/security"))
		Expect(phasesInFlight()).To(Equal([]flowv1alpha1.Phase{logic, security, tests}))

		// Numbered in branch order after the fork's own run.
		logicJob, securityJob, testsJob := jobOf(logic, 2), jobOf(security, 3), jobOf(tests, 4)
		Expect(securityJob.Spec.Template.Annotations).To(HaveKeyWithValue(contract.AnnotationPrevRunID, "1"),
			"a branch was led to by its fork's run")
		Expect(securityJob.Spec.Template.Spec.InitContainers[1].Args).NotTo(ContainElement("--"+contract.FlagMany),
			"a branch answers with exactly one directory")

		answer(logicJob, dirDone)
		answer(testsJob, dirDone)
		fx.reconcile()
		Expect(phasesInFlight()).To(Equal([]flowv1alpha1.Phase{security}), "two have met at the join, one is still running")
		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate))

		answer(securityJob, dirDone)
		fx.reconcile()
		tk = fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport), "the last branch to arrive starts the join")
		Expect(taskstate.Current(&tk.Status)).To(Equal(&flowv1alpha1.RunRef{Phase: phaseReport, RunID: 5}))
		Expect(tk.Status.History).To(HaveLen(4))

		fx.reconcile()
		join := jobOf(phaseReport, 5)
		Expect(join.Spec.Template.Annotations).To(HaveKeyWithValue(contract.AnnotationPrevRunID, "4"),
			"the join was led to by the last of its branches")
	})

	It("stops at the first branch to escalate, cancelling the ones still running", func() {
		setUp()
		forked(string(logic) + "/" + string(security))

		answer(jobOf(logic, 2), dirStuck)
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(tk.Status.CurrentRuns).To(BeEmpty())
		lines := make([]string, 0, len(tk.Status.History))
		for _, h := range tk.Status.History {
			lines = append(lines, string(h.Phase)+":"+h.Outcome)
		}
		Expect(lines).To(Equal([]string{
			string(phaseInvestigate) + ":" + string(transition.OutcomeDeclared),
			string(security) + ":" + string(transition.OutcomeCancelled),
			string(tests) + ":" + string(transition.OutcomeCancelled),
			string(logic) + ":" + string(transition.OutcomeDeclined),
		}), "the branches still running are cancelled, and the one that decided the ending is the last line")

		for phase, runID := range map[flowv1alpha1.Phase]int32{security: 3, tests: 4} {
			var job batchv1.Job
			err := k8sClient.Get(fx.ctx, types.NamespacedName{
				Name: runner.JobName(fx.name, phase, runID, 0), Namespace: resourceNamespace,
			}, &job)
			Expect(apierrors.IsNotFound(err) || job.DeletionTimestamp != nil).To(BeTrue(),
				"%s's Job is stopped: nothing it answers can change where the task went", phase)
		}
		Expect(jobOf(logic, 2).DeletionTimestamp).To(BeNil(), "the branch that answered keeps its Job and its pod's log")
	})

	It("waits for a branch still running until its deadline", func() {
		setUp(func(h *flowv1alpha1.TaskHandler) { h.Spec.Timeout = &metav1.Duration{Duration: time.Hour} })
		forked(string(security))

		res := fx.reconcile()
		Expect(res.RequeueAfter).To(BeNumerically(">", 0), "it comes back by the soonest branch deadline")
		Expect(res.RequeueAfter).To(BeNumerically("<=", time.Hour+deadlineGrace))
		Expect(phasesInFlight()).To(Equal([]flowv1alpha1.Phase{security, tests}))
	})

	It("fails a fork whose branch is not a Job run", func() {
		setUp(func(h *flowv1alpha1.TaskHandler) {
			if h.Spec.Phase == security {
				stateRunner(time.Hour)(h)
			}
		})
		forked(string(security))

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(tk.Status.Conditions).To(ContainElement(HaveField("Message", ContainSubstring("run as Jobs"))))
	})

	// fail marks job failed the way the Job controller does, with a pod whose
	// containers never ran unless ran says otherwise.
	fail := func(job *batchv1.Job, reason string) {
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
			batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: reason},
			batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: reason})
		Expect(k8sClient.Status().Update(fx.ctx, job)).To(Succeed())
	}

	It("starts a branch that never ran again under its own number", func() {
		setUp(func(h *flowv1alpha1.TaskHandler) { h.Spec.MaxInfraRetries = 1 })
		forked(string(security))

		fail(jobOf(security, 2), batchv1.JobReasonBackoffLimitExceeded)
		fx.reconcile()
		Expect(taskstate.Run(&fx.get().Status, security)).To(Equal(&flowv1alpha1.RunRef{Phase: security, RunID: 2, InfraRetries: 1}),
			"nothing was decided, so the branch keeps its number and counts the attempt")
		Expect(fx.get().Status.History).To(HaveLen(1))

		fx.reconcile()
		var again batchv1.Job
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, security, 2, 1), Namespace: resourceNamespace,
		}, &again)).To(Succeed(), "the next attempt gets a Job of its own")
	})

	It("stops the fork when a branch never started and its retries are spent", func() {
		setUp()
		forked(string(security))

		fail(jobOf(security, 2), batchv1.JobReasonBackoffLimitExceeded)
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		last := tk.Status.History[len(tk.Status.History)-1]
		Expect(last.Phase).To(Equal(security))
		Expect(last.Outcome).To(Equal(string(transition.OutcomeNoAnswer)))
		Expect(last.Reason).To(ContainSubstring("never started"))
	})

	It("stops the fork when a branch's Job runs out of time", func() {
		setUp()
		forked(string(security))

		fail(jobOf(security, 2), batchv1.JobReasonDeadlineExceeded)
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(tk.Status.History[len(tk.Status.History)-1].Phase).To(Equal(security))
	})

	It("stops the fork when a branch is still running past its deadline", func() {
		setUp(func(h *flowv1alpha1.TaskHandler) { h.Spec.Timeout = &metav1.Duration{Duration: time.Minute} })
		forked(string(security))
		fx.reconciler.Now = func() time.Time { return time.Now().Add(time.Hour) }

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(tk.Status.History[len(tk.Status.History)-1].Outcome).To(Equal(string(transition.OutcomeNoAnswer)))
	})

	It("fails a task whose flow stops forking while its branches run", func() {
		setUp()
		forked(string(security))

		var flow flowv1alpha1.TaskFlow
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, &flow)).To(Succeed())
		b := flow.Spec.Bindings[phaseInvestigate]
		b.Join = nil
		flow.Spec.Bindings[phaseInvestigate] = b
		Expect(k8sClient.Update(fx.ctx, &flow)).To(Succeed())

		fx.reconcile()
		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(tk.Status.Conditions).To(ContainElement(HaveField("Message", ContainSubstring("no longer forks"))))
	})

})

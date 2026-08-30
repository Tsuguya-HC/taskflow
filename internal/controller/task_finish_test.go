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
	"strings"
	"time"
	"unicode/utf8"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/metrics"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// envtest runs no Job controller and no kubelet, so what those would write —
// pods under a Job, container states, Job conditions — is written here by
// hand. That is the point: each spec states exactly what the cluster reported
// and asks what the task made of it.
var _ = Describe("finishing a run", func() {
	var fx *fixture

	BeforeEach(func() {
		fx = newFixture()
	})

	// start brings a task to the point where its first Job exists.
	start := func() *batchv1.Job {
		fx.reconcile() // settles the starting phase
		fx.reconcile() // creates the Job
		return fx.job(1)
	}

	// podOf creates the pod the Job controller would have, labelled the way
	// it labels them, with the container states given.
	podOf := func(job *batchv1.Job, suffix string, statuses ...corev1.ContainerStatus) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      job.Name + suffix,
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
		if len(statuses) == 0 {
			return
		}
		pod.Status.Phase = corev1.PodSucceeded
		pod.Status.ContainerStatuses = statuses
		Expect(k8sClient.Status().Update(fx.ctx, pod)).To(Succeed())
	}

	terminated := func(container, message string) corev1.ContainerStatus {
		return corev1.ContainerStatus{
			Name:  container,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: message}},
		}
	}

	// finish marks the Job the way the Job controller does: Complete with no
	// reason, or Failed with one. The apiserver validates the shape — the
	// terminal condition must follow its interim one (SuccessCriteriaMet /
	// FailureTarget) and the timestamps must be there — so this writes the
	// whole sequence, not just the condition the controller reads.
	finish := func(job *batchv1.Job, failure string) {
		now := metav1.Now()
		job.Status.StartTime = &now
		if failure == "" {
			job.Status.CompletionTime = &now
			job.Status.Conditions = append(job.Status.Conditions,
				batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
				batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		} else {
			job.Status.Conditions = append(job.Status.Conditions,
				batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: failure},
				batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: failure})
		}
		Expect(k8sClient.Status().Update(fx.ctx, job)).To(Succeed())
	}

	It("moves along the declared edge when exactly one container names a directory", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		job := start()

		podOf(job, "", terminated(agentName, "ok\nnothing to report"))
		finish(job, "")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport))
		Expect(tk.Status.CurrentRun).To(BeNil(), "報告 has no binding, so the task is done")
		Expect(tk.Status.History).To(HaveLen(1))
		h := tk.Status.History[0]
		Expect(h.Phase).To(Equal(phaseInvestigate))
		Expect(h.RunID).To(BeEquivalentTo(1))
		Expect(h.Directory).To(Equal("ok"))
		Expect(h.Outcome).To(Equal(string(transition.OutcomeDeclared)))
		Expect(h.Reason).To(ContainSubstring("nothing to report"), "what the handler said after its directory is kept for a human")
		Expect(h.FinishedAt).NotTo(BeNil())

		// This flow never declared what 報告 means, so the ending the metric
		// records has to say so rather than guessing at Success — that silence
		// is the whole point of Undeclared existing as a value of its own.
		Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(prometheus.Labels{
			metrics.LabelFlow: fx.name, metrics.LabelPhase: string(phaseReport), metrics.LabelSeverity: string(transition.EndingUndeclared),
		}))).To(BeNumerically("==", 1), "a flow that has not declared its endings must still show up in the metric")
	})

	// The one reserved name a flow may send work to. Declaring it is what
	// puts an escalate directory in front of the run, so "I will not decide
	// this" becomes something the handler writes and explains rather than
	// something inferred from its silence. Running it against a real
	// apiserver also shows that nothing in the CRD refuses Escalated as a
	// key of next — the reservation is on binding it, not on reaching it.
	It("records a declared escalation as the handler's own conclusion", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings[phaseInvestigate].Next[flowv1alpha1.PhaseEscalated] = "escalate"
		})
		fx.makeHandler()
		fx.makeTask()
		job := start()

		Expect(directoriesOf(job)).To(ConsistOf("escalate", "ok"),
			"the run cannot write into a directory the flow never declared")

		podOf(job, "", terminated(agentName, "escalate\nthe policy is ambiguous; a human should decide"))
		finish(job, "")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(tk.Status.CurrentRun).To(BeNil())
		Expect(tk.Status.History).To(HaveLen(1))
		h := tk.Status.History[0]
		Expect(h.Directory).To(Equal("escalate"))
		Expect(h.Outcome).To(Equal(string(transition.OutcomeDeclined)),
			"a run that chose to escalate is not the same event as one that said nothing")
		Expect(h.Reason).To(ContainSubstring("a human should decide"))

		ready := meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(string(transition.OutcomeDeclined)))

		Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(prometheus.Labels{
			metrics.LabelFlow: fx.name, metrics.LabelPhase: string(flowv1alpha1.PhaseEscalated), metrics.LabelSeverity: string(transition.EndingEscalated),
		}))).To(BeNumerically("==", 1), "the framework's own ending must be counted too, not only a flow's declared ones")
	})

	// A task can finish perfectly normally and still be carrying bad news:
	// the run completed, the handler concluded, and the edge it took was one
	// the flow declared. Nothing about that is visible unless the flow says
	// which of its endings mean trouble — so when it does, the framework
	// speaks up in the two places somebody would actually be watching.
	It("announces an ending the flow declared to be a failure", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings[phaseInvestigate].Next[phaseBroken] = "error"
			f.Spec.Terminals = map[flowv1alpha1.Phase]flowv1alpha1.TerminalSeverity{
				phaseReport: flowv1alpha1.TerminalSuccess,
				phaseBroken: flowv1alpha1.TerminalFailure,
			}
		})
		fx.makeHandler()
		fx.makeTask()
		job := start()

		podOf(job, "", terminated(agentName, "error\n3 namespaces have no policy at all"))
		finish(job, "")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseBroken))
		Expect(tk.Status.CurrentRun).To(BeNil(), "失敗 has no binding, so the task is done")
		Expect(tk.Status.History).To(HaveLen(1))
		Expect(tk.Status.History[0].Outcome).To(Equal(string(transition.OutcomeDeclared)),
			"the move itself was an ordinary declared edge; what makes it news is the flow calling it a failure")

		ready := meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Status).To(Equal(metav1.ConditionFalse))
		Expect(ready.Reason).To(Equal(taskstate.ReasonHandlerFailed))

		Expect(fx.announced()).To(ContainElement(And(
			HavePrefix("Warning "+taskstate.ReasonHandlerFailed),
			ContainSubstring("3 namespaces have no policy at all"),
		)), "the reason the handler gave has to reach whoever is watching events")

		// Each spec gets its own flow name, so this counter starts at zero
		// and one ending is the whole of what it should have seen.
		Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(prometheus.Labels{
			metrics.LabelFlow: fx.name, metrics.LabelPhase: string(phaseBroken), metrics.LabelSeverity: string(transition.EndingFailure),
		}))).To(BeNumerically("==", 1), "an alert rule has nothing else to fire on")
	})

	// The same run, the same edge — only the flow's word for the ending
	// differs. A success is not news, and the framework has nothing to add.
	It("says nothing about an ending the flow declared to be a success", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Terminals = map[flowv1alpha1.Phase]flowv1alpha1.TerminalSeverity{
				phaseReport: flowv1alpha1.TerminalSuccess,
			}
		})
		fx.makeHandler()
		fx.makeTask()
		job := start()

		podOf(job, "", terminated(agentName, "ok\nnothing to report"))
		finish(job, "")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport))
		Expect(meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)).To(BeNil(),
			"nobody has to look at a task that ended the way its flow wanted")
		Expect(fx.announced()).To(BeEmpty())

		// Counted all the same: the metric is how many tasks ended and how,
		// not how many went wrong.
		Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(prometheus.Labels{
			metrics.LabelFlow: fx.name, metrics.LabelPhase: string(phaseReport), metrics.LabelSeverity: string(transition.EndingSuccess),
		}))).To(BeNumerically("==", 1))
	})

	// A CRD's maxLength validates by rune count (utf8.RuneCountInString), not
	// byte count — three-byte characters make that distinction visible: 1200
	// of them is 3600 bytes but only 1200 runes, so a byte-counted cap would
	// truncate far short of Sanitize's own 1024-rune limit, and a byte-counted
	// CRD maxLength=2048 would reject 3072 bytes' worth of already-sanitized
	// text that is well within 2048 runes. Running the whole path against a
	// real apiserver is what proves both ends agree on runes.
	It("keeps a long multi-byte reason within the CRD's rune-counted maxLength", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		job := start()

		podOf(job, "", terminated(agentName, "ok\n"+strings.Repeat("あ", 1200)))
		finish(job, "")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport))
		Expect(tk.Status.History).To(HaveLen(1))

		prefix := "declared edge to " + string(phaseReport) + ": "
		wantRunes := utf8.RuneCountInString(prefix) + 1024
		Expect(utf8.RuneCountInString(tk.Status.History[0].Reason)).To(Equal(wantRunes),
			"1200 runes truncated to Sanitize's 1024, not silently cut at 1024 bytes (341 runes) or rejected by the CRD")
	})

	It("escalates a Job that succeeded without any container answering", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		job := start()

		podOf(job, "", terminated(agentName, "")) // exit 0, said nothing
		finish(job, "")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated), "exit 0 is not a verdict")
		Expect(tk.Status.History).To(HaveLen(1))
		Expect(tk.Status.History[0].Outcome).To(Equal(string(transition.OutcomeNoAnswer)))
		Expect(tk.Status.History[0].Directory).To(BeEmpty())
		cond := meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(string(transition.OutcomeNoAnswer)))
	})

	It("escalates a Job whose pod is already gone", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		job := start()

		finish(job, "") // Complete, but nothing to read it from
		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
	})

	It("reads the answer out of a Job that failed, if the handler ran", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		job := start()

		// The agent exited non-zero, but its sidecar sealed a directory and
		// said so. The exit code is not the verdict; the directory is.
		podOf(job, "",
			corev1.ContainerStatus{Name: agentName, State: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}},
			terminated("publish", "ok"))
		finish(job, batchv1.JobReasonBackoffLimitExceeded)
		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(phaseReport))
	})

	It("escalates a timed-out run even if a directory was written on the way out", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		job := start()

		podOf(job, "", terminated("publish", "ok"))
		finish(job, batchv1.JobReasonDeadlineExceeded)
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(tk.Status.History[0].Reason).To(ContainSubstring("timed out"))
	})

	It("reworks along an edge back to a visited phase, spending budget", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings[phaseInvestigate] = flowv1alpha1.PhaseBinding{
				Handler: fx.name,
				Next:    map[flowv1alpha1.Phase]string{phaseReport: "ok", phaseInvestigate: "more"},
			}
		})
		fx.makeHandler()
		fx.makeTask()
		job := start()

		podOf(job, "", terminated(agentName, "more"))
		finish(job, "")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseInvestigate))
		Expect(tk.Status.RunID).To(BeEquivalentTo(2))
		Expect(tk.Status.ReworkBudget).To(BeEquivalentTo(1), "a self-loop is a rework and costs one")
		Expect(tk.Status.History[0].Outcome).To(Equal(string(transition.OutcomeRework)))
		Expect(tk.Status.CurrentRun).NotTo(BeNil())
		Expect(tk.Status.CurrentRun.RunID).To(BeEquivalentTo(2))

		fx.reconcile()
		second := fx.job(2)
		Expect(second.Annotations["flow.tgy.io/prev-run-id"]).To(Equal("1"))
	})

	It("retries under a new runID when the handler never got to run", func() {
		fx.makeFlow()
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) { h.Spec.MaxInfraRetries = 1 })
		fx.makeTask()
		job := start()

		podOf(job, "") // exists, but no container ever terminated: never pulled
		finish(job, batchv1.JobReasonBackoffLimitExceeded)
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseInvestigate), "the same phase, tried again")
		Expect(tk.Status.RunID).To(BeEquivalentTo(2))
		Expect(tk.Status.History).To(BeEmpty(), "nothing was decided, so nothing is recorded")
		Expect(tk.Status.CurrentRun.InfraRetries).To(BeEquivalentTo(1))
		Expect(tk.Status.ReworkBudget).To(BeEquivalentTo(2), "an infrastructure retry costs no budget")

		fx.reconcile()
		second := fx.job(2)

		// The retry fails the same way, and the allowance is spent.
		finish(second, batchv1.JobReasonBackoffLimitExceeded)
		fx.reconcile()

		tk = fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(tk.Status.History).To(HaveLen(1))
		Expect(tk.Status.History[0].RunID).To(BeEquivalentTo(2))
		Expect(tk.Status.History[0].Reason).To(ContainSubstring("never started"))
	})

	It("fails when the handler disappears before an infrastructure retry can be judged", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		job := start()

		Expect(k8sClient.Delete(fx.ctx, &flowv1alpha1.TaskHandler{
			ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
		})).To(Succeed())

		podOf(job, "") // exists, but no container ever terminated: never pulled
		finish(job, batchv1.JobReasonBackoffLimitExceeded)
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		cond := meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))

		// This Failed came from r.fail(), not from settle() reading the flow's
		// own table — the one path that used to leave the metric silent about a
		// flow broken badly enough to lose a handler mid-run.
		Expect(testutil.ToFloat64(metrics.TaskOutcomes.With(prometheus.Labels{
			metrics.LabelFlow: fx.name, metrics.LabelPhase: string(flowv1alpha1.PhaseFailed), metrics.LabelSeverity: string(transition.EndingFailed),
		}))).To(BeNumerically("==", 1), "a Failed reached through r.fail() must be counted, not only one reached through settle()")
	})

	It("escalates straight away when the handler allows no infrastructure retries", func() {
		fx.makeFlow()
		fx.makeHandler() // maxInfraRetries defaults to 0
		fx.makeTask()
		job := start()

		finish(job, batchv1.JobReasonBackoffLimitExceeded)
		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
	})

	It("fails when the answer arrives after the flow stopped explaining it", func() {
		flow := fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		job := start()

		// Two statuses now share the directory the run is about to name.
		Expect(k8sClient.Get(fx.ctx, client.ObjectKeyFromObject(flow), flow)).To(Succeed())
		flow.Spec.Bindings[phaseInvestigate] = flowv1alpha1.PhaseBinding{
			Handler: fx.name,
			Next:    map[flowv1alpha1.Phase]string{phaseReport: "ok", "別の報告": "ok"},
		}
		Expect(k8sClient.Update(fx.ctx, flow)).To(Succeed())

		podOf(job, "", terminated(agentName, "ok"))
		finish(job, "")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(tk.Status.History[0].Outcome).To(Equal(string(transition.OutcomeStructural)))
		cond := meta.FindStatusCondition(tk.Status.Conditions, taskstate.ConditionReady)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(string(transition.OutcomeStructural)))
	})

	Context("with a timeout declared", func() {
		const timeout = 10 * time.Minute

		BeforeEach(func() {
			fx.makeFlow()
			fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
				h.Spec.Timeout = &metav1.Duration{Duration: timeout}
			})
			fx.makeTask()
		})

		It("records the deadline on the run and asks to be woken for it", func() {
			fx.reconcile()
			res := fx.reconcile()
			job := fx.job(1)

			run := fx.get().Status.CurrentRun
			Expect(run.Deadline).NotTo(BeNil())
			Expect(run.Deadline.Time).To(BeTemporally("~", job.CreationTimestamp.Add(timeout), time.Second),
				"the deadline is the Job's own, read off the Job")
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(res.RequeueAfter).To(BeNumerically("<=", timeout+deadlineGrace))
		})

		It("escalates a run still in flight past its deadline", func() {
			job := start()
			fx.reconciler.Now = func() time.Time {
				return job.CreationTimestamp.Add(timeout + deadlineGrace + time.Second)
			}

			// The Job never reported: its pod is stuck terminating, say.
			podOf(job, "", terminated("publish", "ok"))
			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
			Expect(tk.Status.CurrentRun).To(BeNil())
			Expect(tk.Status.History[0].Outcome).To(Equal(string(transition.OutcomeNoAnswer)))
			Expect(tk.Status.History[0].Reason).To(ContainSubstring("timed out"))
		})

		It("leaves a run alone while the grace past its deadline is still running", func() {
			job := start()
			fx.reconciler.Now = func() time.Time {
				return job.CreationTimestamp.Add(timeout + deadlineGrace/2)
			}

			res := fx.reconcile()

			Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate))
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
		})
	})

	It("does not count the same pod twice when the Job replaced it", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		job := start()

		// KEP-3939: a pod replaced while terminating leaves two under the Job.
		// Both answering is exactly the case a human should see.
		podOf(job, "-a", terminated(agentName, "ok"))
		podOf(job, "-b", terminated(agentName, "ok"))
		finish(job, "")
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(tk.Status.History[0].Reason).To(ContainSubstring(fmt.Sprintf("%s-a", job.Name)))
	})
})

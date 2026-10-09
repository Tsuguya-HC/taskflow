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
	"bytes"
	"encoding/json"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// The three specs below pin the TaskFailed outcome wiring this PR added:
// a declining fork branch counts as Declined (#10), the no-run-to-settle
// log carries the outcome key (#11), and a broken definition counts as
// Structural (#12).
var _ = Describe("the TaskFailed outcome wiring", func() {
	var fx *fixture
	BeforeEach(func() { fx = newFixture() })

	handlerFor := func(phase flowv1alpha1.Phase) string {
		if phase == phaseReport {
			return fx.name + "-report"
		}
		return fx.name + "-" + string(phase)
	}

	jobOf := func(phase flowv1alpha1.Phase, runID int32) *batchv1.Job {
		var job batchv1.Job
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: runner.JobName(fx.name, phase, runID, 0), Namespace: resourceNamespace,
		}, &job)).To(Succeed())
		return &job
	}

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

	// Catches driveBranches always announcing Structural: without reading
	// the deciding history line, a branch that declined counts wrong (#10).
	It("counts a fork branch that declines as Declined", func() {
		toReport := map[flowv1alpha1.Phase]string{phaseReport: answerDone, flowv1alpha1.PhaseTaskFailed: answerStuck}
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings = map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
				phaseInvestigate: {
					Handler: fx.name,
					Next: map[flowv1alpha1.Phase]string{
						phaseSecurity: string(phaseSecurity), phaseLogic: string(phaseLogic), flowv1alpha1.PhaseTaskFailed: answerStuck,
					},
					Join: &flowv1alpha1.JoinSpec{Phase: phaseReport, Always: []flowv1alpha1.Phase{phaseTests}},
				},
				phaseSecurity: {Handler: handlerFor(phaseSecurity), Next: toReport},
				phaseLogic:    {Handler: handlerFor(phaseLogic), Next: toReport},
				phaseTests:    {Handler: handlerFor(phaseTests), Next: toReport},
				phaseReport:   {Handler: handlerFor(phaseReport), Next: map[flowv1alpha1.Phase]string{phaseDone: "ok"}},
			}
		})
		fx.makeHandler()
		for _, phase := range []flowv1alpha1.Phase{phaseSecurity, phaseLogic, phaseTests, phaseReport} {
			fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
				h.Name = handlerFor(phase)
				h.Spec.Phase = phase
			})
		}

		fx.makeTask()
		fx.reconcile()
		fx.reconcile()
		fork := jobOf(phaseInvestigate, 1)
		Expect(fork.Spec.Template.Spec.InitContainers[1].Args).To(ContainElement("--" + contract.FlagMany))
		answer(fork, string(phaseLogic)+"/"+string(phaseSecurity))
		fx.reconcile()
		fx.reconcile()

		answer(jobOf(phaseLogic, 2), answerStuck)
		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))
		got := collectedOutcomes()
		key := outcome{fx.name, string(flowv1alpha1.PhaseTaskFailed), string(transition.EndingTaskFailed), string(transition.OutcomeDeclined)}
		Expect(got[key]).To(Equal(1.0), "the branch that declined decides the ending, so its outcome is what counts")
	})

	// Catches the outcome key going missing from the no-run-to-settle log:
	// without it the FlowBroken path logs no outcome (#11).
	It("logs the outcome key when a task fails without a run to settle", func() {
		flow := fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		var buf bytes.Buffer
		logger := ctrlzap.New(ctrlzap.WriteTo(&buf), ctrlzap.JSONEncoder())
		ctx := logf.IntoContext(fx.ctx, logger)

		Expect(fx.reconciler.failStartedAs(ctx, tk, &flow.Spec, "SomeReadyReason", "some message")).To(Succeed())

		found := false
		for line := range strings.SplitSeq(buf.String(), "\n") {
			if line == "" {
				continue
			}
			var entry map[string]any
			Expect(json.Unmarshal([]byte(line), &entry)).To(Succeed())
			if entry["msg"] == "task failed without a run to settle" {
				if outcomeValue, ok := entry["outcome"]; ok && outcomeValue == string(transition.OutcomeStructural) {
					found = true
				}
			}
		}
		Expect(found).To(BeTrue(), "the no-run-to-settle path must log its outcome")
	})

	// Catches the FlowBroken announce counting the wrong outcome: a task
	// with no handler to run stops on a broken definition (#12).
	It("counts a FlowBroken ending as Structural", func() {
		fx.makeFlow()
		forgetFlowMetrics(fx.name)
		fx.makeBareTask(false)

		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))

		got := collectedOutcomes()
		key := outcome{fx.name, string(flowv1alpha1.PhaseTaskFailed), string(transition.EndingTaskFailed), string(transition.OutcomeStructural)}
		Expect(got[key]).To(Equal(1.0), "a broken definition stops the task as Structural")
	})
})

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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
)

// The flow name used across these examples, matching design.md §16.
const exampleFlow = "cnp-check"

// The examples in design.md §16 are the acceptance criterion for the types:
// if what the document tells someone to write does not apply, the document is
// wrong or the types are. These create them against a real API server so the
// generated schema — enums, required fields, the embedded JobTemplateSpec —
// is what gets tested, not a struct literal that only the compiler saw.
var _ = Describe("the API accepts the shapes design.md documents", func() {
	ctx := context.Background()

	It("accepts the cnp-check TaskFlow", func() {
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: exampleFlow, Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskFlowSpec{
				Profile: flowv1alpha1.ProfileInvestigate,
				Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
					flowv1alpha1.PhasePlanning: {
						Handler:  "claude-planner",
						Outcomes: map[flowv1alpha1.Verdict]flowv1alpha1.Phase{flowv1alpha1.VerdictPass: flowv1alpha1.PhaseReview},
					},
					flowv1alpha1.PhaseReview: {
						Handler: "claude-reviewer",
						Outcomes: map[flowv1alpha1.Verdict]flowv1alpha1.Phase{
							flowv1alpha1.VerdictPass:     flowv1alpha1.PhaseDone,
							flowv1alpha1.VerdictRework:   flowv1alpha1.PhasePlanning,
							flowv1alpha1.VerdictEscalate: flowv1alpha1.PhaseEscalated,
						},
					},
				},
				ReworkBudget: 2,
				TTL: &flowv1alpha1.TTLSpec{
					Succeeded: &metav1.Duration{Duration: 3600000000000},
				},
			},
		}
		Expect(k8sClient.Create(ctx, flow)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, flow) })
	})

	It("accepts a TaskHandler carrying a whole jobTemplate", func() {
		h := &flowv1alpha1.TaskHandler{
			ObjectMeta: metav1.ObjectMeta{Name: "claude-reviewer", Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskHandlerSpec{
				Phase:  flowv1alpha1.PhaseReview,
				Runner: flowv1alpha1.RunnerSpec{Type: flowv1alpha1.RunnerJob},
				JobTemplate: &batchv1.JobTemplateSpec{
					Spec: batchv1.JobSpec{
						Template: corev1.PodTemplateSpec{
							// The label a network policy selects on is written
							// here, by the handler author. Nothing in the
							// controller puts it there — that is the whole
							// point of taking a jobTemplate.
							ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"claude-code": "true"}},
							Spec: corev1.PodSpec{
								RestartPolicy:      corev1.RestartPolicyNever,
								ServiceAccountName: "agent-readonly",
								Containers:         []corev1.Container{{Name: "agent", Image: "example.invalid/agent:v0"}},
							},
						},
					},
				},
				Timeout:         &metav1.Duration{Duration: 1800000000000},
				MaxInfraRetries: 2,
			},
		}
		Expect(k8sClient.Create(ctx, h)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, h) })
	})

	It("accepts a Task of four lines, with arbitrary input", func() {
		task := &flowv1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: "cnp-check-x7f2", Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskSpec{
				Flow:     exampleFlow,
				Input:    &apiextensionsv1.JSON{Raw: []byte(`{"scope":"all namespaces"}`)},
				DedupKey: "issue-1234",
			},
		}
		Expect(k8sClient.Create(ctx, task)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, task) })
	})
})

var _ = Describe("the API refuses what the design forbids", func() {
	ctx := context.Background()

	It("refuses a profile that is not built in", func() {
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-profile", Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskFlowSpec{
				Profile: "whatever",
				Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
					flowv1alpha1.PhaseReview: {Handler: "h", Outcomes: map[flowv1alpha1.Verdict]flowv1alpha1.Phase{flowv1alpha1.VerdictPass: flowv1alpha1.PhaseDone}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, flow)).NotTo(Succeed())
	})

	It("refuses a handler bound to a terminal phase", func() {
		h := &flowv1alpha1.TaskHandler{
			ObjectMeta: metav1.ObjectMeta{Name: "handler-for-done", Namespace: resourceNamespace},
			Spec:       flowv1alpha1.TaskHandlerSpec{Phase: flowv1alpha1.PhaseDone},
		}
		Expect(k8sClient.Create(ctx, h)).NotTo(Succeed())
	})

	It("refuses a flow with no bindings at all", func() {
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "empty-flow", Namespace: resourceNamespace},
			Spec:       flowv1alpha1.TaskFlowSpec{Profile: flowv1alpha1.ProfileInvestigate},
		}
		Expect(k8sClient.Create(ctx, flow)).NotTo(Succeed())
	})

	It("refuses a task with no flow", func() {
		task := &flowv1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: "flowless", Namespace: resourceNamespace},
			Spec:       flowv1alpha1.TaskSpec{},
		}
		Expect(k8sClient.Create(ctx, task)).NotTo(Succeed())
	})
})

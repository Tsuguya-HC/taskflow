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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
)

// The flow name used across these examples, matching design.md §16.
const (
	exampleFlow = "cnp-check"
	agentName   = "agent"
	labelKeeper = "label-keeper"
	agentImage  = "example.invalid/agent:v0"
)

// The directory that selects the terminal status in these examples.
const dirSent = "sent"

// The terminal status these examples end at — an ordinary name, not a
// framework reserved one (see design.md §5).
const phaseDone flowv1alpha1.Phase = "おわり"

// These do not reproduce design.md §16 verbatim — names, handlers and budget
// differ. What they check is the shape the design requires: a declaration
// decides its own vocabulary, the two reserved names cannot be bound, and the
// required fields are enforced. They run against a real API server so the
// generated schema — enums, required fields, the embedded PodSpec under the
// curated JobTemplate type — is what gets tested, not a struct literal that
// only the compiler saw.
var _ = Describe("the API accepts the shapes design.md documents", func() {
	ctx := context.Background()

	It("accepts the cnp-check TaskFlow", func() {
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: exampleFlow, Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskFlowSpec{
				Profile: flowv1alpha1.ProfileInvestigate,
				Start:   "調査",
				// Statuses named by whoever wrote the flow, and directories
				// named by them too. Nothing here comes from the framework.
				Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
					"調査": {
						Handler: "claude-planner",
						Next: map[flowv1alpha1.Phase]string{
							"報告": "ok",
							"調査": "more",
						},
					},
					"報告": {
						Handler: "claude-reviewer",
						Next:    map[flowv1alpha1.Phase]string{phaseDone: dirSent},
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
				Phase:  "報告",
				Runner: flowv1alpha1.RunnerSpec{Type: flowv1alpha1.RunnerJob},
				JobTemplate: &flowv1alpha1.JobTemplate{
					Template: flowv1alpha1.PodTemplate{
						// The label a network policy selects on is written
						// here, by the handler author. Nothing in the
						// controller puts it there — that is the whole point
						// of taking a template at all.
						Metadata: flowv1alpha1.EmbeddedObjectMeta{Labels: map[string]string{"claude-code": "true"}},
						Spec: corev1.PodSpec{
							RestartPolicy:      corev1.RestartPolicyNever,
							ServiceAccountName: "agent-readonly",
							Containers:         []corev1.Container{{Name: agentName, Image: agentImage}},
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

	It("defaults an omitted Runner to Job", func() {
		h := &flowv1alpha1.TaskHandler{
			ObjectMeta: metav1.ObjectMeta{Name: "defaults-runner", Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskHandlerSpec{
				Phase: "報告",
				// Runner deliberately omitted — this is what
				// +kubebuilder:default={type: Job} on RunnerSpec exists to cover.
			},
		}
		Expect(k8sClient.Create(ctx, h)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, h) })

		got := &flowv1alpha1.TaskHandler{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(h), got)).To(Succeed())
		Expect(got.Spec.Runner.Type).To(Equal(flowv1alpha1.RunnerJob))
	})

	// The labels a network policy selects on live here, written by whoever
	// wrote the handler. If storage drops them the pod is not selected, and in
	// a default-deny namespace that shows up as traffic disappearing rather
	// than as an error.
	It("keeps the pod labels the handler wrote", func() {
		h := &flowv1alpha1.TaskHandler{
			ObjectMeta: metav1.ObjectMeta{Name: labelKeeper, Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskHandlerSpec{
				Phase: "調査",
				JobTemplate: &flowv1alpha1.JobTemplate{
					Template: flowv1alpha1.PodTemplate{
						Metadata: flowv1alpha1.EmbeddedObjectMeta{
							Labels:      map[string]string{"role": "cnp-reader"},
							Annotations: map[string]string{"note": "keep me"},
						},
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers:    []corev1.Container{{Name: agentName, Image: agentImage}},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, h)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(ctx, h) })

		// Read it back untyped first, to tell server-side pruning apart from
		// anything the Go client might be doing on the way in.
		raw := &unstructured.Unstructured{}
		raw.SetGroupVersionKind(flowv1alpha1.SchemeGroupVersion.WithKind("TaskHandler"))
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: labelKeeper, Namespace: resourceNamespace}, raw)).To(Succeed())
		meta, _, _ := unstructured.NestedMap(raw.Object, "spec", "jobTemplate", "spec", "template", "metadata")
		GinkgoWriter.Printf("サーバが保持している template.metadata = %v\n", meta)

		var got flowv1alpha1.TaskHandler
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: labelKeeper, Namespace: resourceNamespace}, &got)).To(Succeed())
		Expect(got.Spec.JobTemplate.Template.Metadata.Labels).To(HaveKeyWithValue("role", "cnp-reader"))
		Expect(got.Spec.JobTemplate.Template.Metadata.Annotations).To(HaveKeyWithValue("note", "keep me"))
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

// invalid asserts that err is not merely any error but a validation
// rejection carrying reason as part of its message — otherwise a test could
// stay green while refusing for an entirely different cause than the one it
// claims to cover.
func invalid(err error, reason string) {
	ExpectWithOffset(1, apierrors.IsInvalid(err)).To(BeTrue(), "want a validation error, got %v", err)
	ExpectWithOffset(1, err.Error()).To(ContainSubstring(reason))
}

var _ = Describe("the API refuses what the design forbids", func() {
	ctx := context.Background()

	It("refuses a profile that is not built in", func() {
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-profile", Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskFlowSpec{
				Profile: "whatever",
				Start:   "報告",
				Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
					"報告": {Handler: "h", Next: map[flowv1alpha1.Phase]string{phaseDone: dirSent}},
				},
			},
		}
		invalid(k8sClient.Create(ctx, flow), "spec.profile")
	})

	DescribeTable("refuses a handler bound to a name the framework owns",
		func(reserved flowv1alpha1.Phase) {
			h := &flowv1alpha1.TaskHandler{
				ObjectMeta: metav1.ObjectMeta{Name: "handler-for-" + strings.ToLower(string(reserved)), Namespace: resourceNamespace},
				Spec:       flowv1alpha1.TaskHandlerSpec{Phase: reserved},
			}
			invalid(k8sClient.Create(ctx, h), "a handler cannot fill them")
		},
		Entry("Escalated", flowv1alpha1.PhaseEscalated),
		Entry("Failed", flowv1alpha1.PhaseFailed),
	)

	It("refuses a flow with no bindings at all", func() {
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "empty-flow", Namespace: resourceNamespace},
			Spec:       flowv1alpha1.TaskFlowSpec{Profile: flowv1alpha1.ProfileInvestigate, Start: "調査"},
		}
		// bindings is absent altogether, not merely empty, so this is rejected
		// by the required-field check rather than by MinProperties. That path
		// is covered separately by "refuses a binding with an empty next map".
		invalid(k8sClient.Create(ctx, flow), "bindings")
	})

	It("refuses a binding with an empty next map", func() {
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "empty-next", Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskFlowSpec{
				Profile:  flowv1alpha1.ProfileInvestigate,
				Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{"報告": {Handler: "h", Next: map[flowv1alpha1.Phase]string{}}},
			},
		}
		// Next has no declared destination, so the run it would dispatch could
		// never produce a directory that maps anywhere — MinProperties=1 stops
		// that at creation instead of at the first Escalated.
		invalid(k8sClient.Create(ctx, flow), "next")
	})

	It("refuses a binding with an empty handler name", func() {
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "empty-handler", Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskFlowSpec{
				Profile:  flowv1alpha1.ProfileInvestigate,
				Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{"報告": {Handler: "", Next: map[flowv1alpha1.Phase]string{phaseDone: dirSent}}},
			},
		}
		invalid(k8sClient.Create(ctx, flow), "handler")
	})

	It("refuses a flow that says nowhere to start", func() {
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "startless", Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskFlowSpec{
				Profile: flowv1alpha1.ProfileInvestigate,
				Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
					"調査": {Handler: "h", Next: map[flowv1alpha1.Phase]string{"おわり": "ok"}},
				},
			},
		}
		invalid(k8sClient.Create(ctx, flow), "spec.start")
	})

	It("refuses a task with no flow", func() {
		task := &flowv1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: "flowless", Namespace: resourceNamespace},
			Spec:       flowv1alpha1.TaskSpec{},
		}
		invalid(k8sClient.Create(ctx, task), "spec.flow")
	})

	DescribeTable("refuses a value that violates a bound or enum",
		func(create func() error, reason string) {
			invalid(create(), reason)
		},
		Entry("TaskFlow.reworkBudget below zero", func() error {
			flow := &flowv1alpha1.TaskFlow{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-rework-budget", Namespace: resourceNamespace},
				Spec: flowv1alpha1.TaskFlowSpec{
					Profile:      flowv1alpha1.ProfileInvestigate,
					Bindings:     map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{"報告": {Handler: "h", Next: map[flowv1alpha1.Phase]string{phaseDone: dirSent}}},
					ReworkBudget: -1,
				},
			}
			return k8sClient.Create(ctx, flow)
		}, "reworkBudget"),
		Entry("TaskFlow.maxInFlight below one", func() error {
			zero := int32(0)
			flow := &flowv1alpha1.TaskFlow{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-max-in-flight", Namespace: resourceNamespace},
				Spec: flowv1alpha1.TaskFlowSpec{
					Profile:     flowv1alpha1.ProfileInvestigate,
					Bindings:    map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{"報告": {Handler: "h", Next: map[flowv1alpha1.Phase]string{phaseDone: dirSent}}},
					MaxInFlight: &zero,
				},
			}
			return k8sClient.Create(ctx, flow)
		}, "maxInFlight"),
		Entry("TaskHandler.runner.type outside the enum", func() error {
			// Job and External are the only runners this design admits — Argo
			// was deliberately not made a third (§4 "Argo を runner に採らない").
			h := &flowv1alpha1.TaskHandler{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-runner-type", Namespace: resourceNamespace},
				Spec: flowv1alpha1.TaskHandlerSpec{
					Phase:  "報告",
					Runner: flowv1alpha1.RunnerSpec{Type: "Argo"},
				},
			}
			return k8sClient.Create(ctx, h)
		}, "runner.type"),
		Entry("TaskHandler.phase empty", func() error {
			h := &flowv1alpha1.TaskHandler{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-empty-phase", Namespace: resourceNamespace},
				Spec:       flowv1alpha1.TaskHandlerSpec{Phase: ""},
			}
			return k8sClient.Create(ctx, h)
		}, "spec.phase"),
		Entry("TaskHandler.maxInfraRetries below zero", func() error {
			h := &flowv1alpha1.TaskHandler{
				ObjectMeta: metav1.ObjectMeta{Name: "bad-max-infra-retries", Namespace: resourceNamespace},
				Spec: flowv1alpha1.TaskHandlerSpec{
					Phase:           "報告",
					MaxInfraRetries: -1,
				},
			}
			return k8sClient.Create(ctx, h)
		}, "maxInfraRetries"),
	)
})

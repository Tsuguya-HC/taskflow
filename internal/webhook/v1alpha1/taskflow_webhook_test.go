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

package v1alpha1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// What flowcheck refuses is its own package's business, and tested there
// against values. These specs answer the question unit tests cannot: whether
// the refusal is actually wired to the API server — the manifest's path
// matching the handler's, the webhook registered for both verbs, the object
// arriving decoded. A rule nobody calls passes every test it has.
var _ = Describe("TaskFlow validating webhook", func() {
	const (
		phaseInvestigate flowv1alpha1.Phase = "調査"
		phaseReport      flowv1alpha1.Phase = "報告"
		phaseDone        flowv1alpha1.Phase = "おわり"
	)

	// wellFormed is the design's own example: 調査 reports or asks for another
	// round, 報告 ends at a phase nothing binds.
	wellFormed := func(name string) *flowv1alpha1.TaskFlow {
		return &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: flowv1alpha1.TaskFlowSpec{
				Profile: flowv1alpha1.ProfileInvestigate,
				Start:   phaseInvestigate,
				Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
					phaseInvestigate: {
						Handler: "cnp-reader",
						Next: map[flowv1alpha1.Phase]string{
							phaseReport:      "ok",
							phaseInvestigate: "more",
						},
					},
					phaseReport: {
						Handler: "discord",
						Next:    map[flowv1alpha1.Phase]string{phaseDone: "sent"},
					},
				},
			},
		}
	}

	It("admits a flow that holds together", func() {
		flow := wellFormed("accepted")
		Expect(k8sClient.Create(ctx, flow)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, flow)).To(Succeed()) })
	})

	It("refuses a flow whose start binds nothing, naming the field", func() {
		flow := wellFormed("unbound-start")
		flow.Spec.Start = "着手"
		err := k8sClient.Create(ctx, flow)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.start"))
	})

	It("refuses a flow that cannot finish", func() {
		flow := wellFormed("never-finishes")
		flow.Spec.Bindings[phaseReport].Next[phaseInvestigate] = "back"
		delete(flow.Spec.Bindings[phaseReport].Next, phaseDone)
		err := k8sClient.Create(ctx, flow)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.bindings"))
	})

	// The same rules on update, because a flow is edited by GitOps rolling
	// out a new definition rather than by being deleted and written again.
	It("refuses an edit that breaks a flow already accepted", func() {
		flow := wellFormed("edited")
		Expect(k8sClient.Create(ctx, flow)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, flow)).To(Succeed()) })

		flow.Spec.Bindings[phaseInvestigate].Next[flowv1alpha1.PhaseFailed] = "broken"
		err := k8sClient.Update(ctx, flow)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Failed"))
	})

	It("admits an edit that leaves the flow coherent", func() {
		flow := wellFormed("edited-well")
		Expect(k8sClient.Create(ctx, flow)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, flow)).To(Succeed()) })

		flow.Spec.Bindings[phaseInvestigate].Next[flowv1alpha1.PhaseEscalated] = "escalate"
		Expect(k8sClient.Update(ctx, flow)).To(Succeed())
	})
})

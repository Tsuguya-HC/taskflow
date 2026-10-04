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
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

// 開始した Task は、handler も写しから読む (#196)。写しを持たない Task は今まで通り
// live の TaskHandler を読む。
var _ = Describe("a task running from its copy of the handlers", func() {
	var fx *fixture

	const (
		editedImage = "example.invalid/agent:edited"
		timeout     = time.Hour
	)

	BeforeEach(func() { fx = newFixture() })

	imageOf := func(fx *fixture) string {
		return fx.job(1).Spec.Template.Spec.Containers[0].Image
	}

	boxesOf := func(fx *fixture) []corev1.ConfigMap {
		var boxes corev1.ConfigMapList
		Expect(k8sClient.List(fx.ctx, &boxes, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID)})).To(Succeed())
		return boxes.Items
	}

	// 変異: ensureJob が handler を live から読む。
	It("builds the Job from the copy's handler when the live handler is edited", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile() // begin
		fx.editHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Spec.JobTemplate.Template.Spec.Containers[0].Image = editedImage
		})

		fx.reconcile() // creates the Job

		Expect(imageOf(fx)).To(Equal(agentImage), "a new image on the live handler does not reach a task that has begun")
	})

	// 変異: ensureJob と runnerOf のどちらかが handler を live から読む。
	It("builds the Job from the copy's handler when the live handler is deleted", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile() // begin
		fx.deleteHandler()

		fx.reconcile() // creates the Job

		Expect(fx.get().Status.Phase).To(Equal(phaseInvestigate), "a deleted handler is no reason to fail a task that has its copy")
		Expect(imageOf(fx)).To(Equal(agentImage))
	})

	// 変異: runnerOf が handler を live から読む。
	It("starts the kind of run the copy's handler says when the live handler changes kind", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile() // begin
		fx.editHandler(stateRunner(timeout))

		fx.reconcile()

		Expect(jobsOf(fx)).To(HaveLen(1), "the copy's handler runs as a Job")
		Expect(boxesOf(fx)).To(BeEmpty(), "and nothing is opened for an answer the live handler's runner would wait on")
	})

	// 変異: handlers を持たない写しを「写し無し」と同じ nil のまま返し、live の handler を読む。
	// handlers のキーが無い、または null の写しは、持っていない写しであって、写しの無い Task ではない。
	It("does not read the live handler when the copy holds no handlers", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile() // begin
		fx.rewriteCopy(func(snap *snapshot) { snap.Handlers = nil })
		fx.editHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Spec.JobTemplate.Template.Spec.Containers[0].Image = editedImage
		})

		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed), "the copy has no handler, and the live one is not a stand-in for it")
		Expect(jobsOf(fx)).To(BeEmpty())
	})

	// 写しを持たない Task が live を読む性質は、上の 3 つの裏返し。
	Context("with no copy", func() {
		// 変異: 写しの有無にかかわらず handler を最初に読んだ値で固定する。
		It("builds the Job from the live handler's current image", func() {
			fx.makeFlow()
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.dropCopy()
			fx.editHandler(func(h *flowv1alpha1.TaskHandler) {
				h.Spec.JobTemplate.Template.Spec.Containers[0].Image = editedImage
			})

			fx.reconcile() // creates the Job

			Expect(imageOf(fx)).To(Equal(editedImage))
		})

		// 変異: handler が無くても Job を作る。
		It("fails when the live handler is deleted", func() {
			fx.makeFlow()
			fx.makeHandler()
			fx.makeTask()
			fx.reconcile() // begin
			fx.dropCopy()
			fx.deleteHandler()

			fx.reconcile()

			Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
			Expect(jobsOf(fx)).To(BeEmpty())
		})
	})
})

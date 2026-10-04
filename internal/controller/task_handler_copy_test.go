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
	"encoding/json"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

// 開始した Task は、handler も写しから読む (#196)。写しも marker も持たない Task は、
// 移行の時点の live の TaskHandler を写して、そこから読む (#183)。
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

	// 写しも marker も持たない Task の移行 (#183)。
	Context("with no copy and no marker", func() {
		// 変異: 写しの有無にかかわらず handler を最初に読んだ値で固定する。
		// 変異: 移行で写しを作らず live の handler を読み続ける。
		It("makes the copy from the live handler as it is at migration", func() {
			fx.makeFlow()
			fx.makeHandler()
			fx.bareTask()
			fx.editHandler(func(h *flowv1alpha1.TaskHandler) {
				h.Spec.JobTemplate.Template.Spec.Containers[0].Image = editedImage
			})

			fx.reconcile() // migrates
			fx.reconcile() // creates the Job, if the migration did not

			Expect(imageOf(fx)).To(Equal(editedImage))
			revs := fx.revisions()
			Expect(revs).To(HaveLen(1), "the copy was made")
			var snap snapshot
			Expect(json.Unmarshal(revs[0].Data.Raw, &snap)).To(Succeed())
			Expect(snap.Handlers[fx.name].JobTemplate.Template.Spec.Containers[0].Image).To(Equal(editedImage),
				"the copy holds the handler as it was when the task was migrated")
		})

		// 移行の後は live の handler を読まない。
		// 変異: 移行した Task が handler を live から読む。
		It("no longer reads the live handler once it has migrated", func() {
			fx.makeFlow()
			fx.makeHandler(stateRunner(timeout))
			fx.bareTask()
			fx.reconcile() // migrates
			fx.reconcile()
			Expect(pinnedOf(fx.get())).NotTo(BeNil(), "the task migrated")
			fx.deleteHandler()

			fx.answer("ok", "")
			fx.reconcile()

			Expect(fx.get().Status.Phase).To(Equal(phaseReport), "a deleted handler is no reason to fail a task that has its copy")
		})

		// 変異: 移行が in flight の phase の handler しか見ない。
		It("fails when a handler any binding names is missing, though the one in flight exists", func() {
			fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
				f.Spec.Bindings[phaseReport] = flowv1alpha1.PhaseBinding{
					Handler: fx.name + "-gone",
					Next:    map[flowv1alpha1.Phase]string{phaseDone: "ok"},
				}
			})
			fx.makeHandler()
			fx.bareTask()

			fx.reconcile()

			tk := fx.get()
			Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
			Expect(readyOf(tk).Message).To(And(ContainSubstring(fx.name+"-gone"), ContainSubstring("does not exist")),
				"the reason is the one begin gives for the same fault")
			Expect(jobsOf(fx)).To(BeEmpty())
			Expect(fx.revisions()).To(BeEmpty(), "no copy of definitions that cannot be held")
			Expect(pinnedOf(tk)).To(BeNil())
		})
	})
})

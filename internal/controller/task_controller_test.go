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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

// Shared by the tests in this package.
const resourceNamespace = "default"

var _ = Describe("Task Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		task := &flowv1alpha1.Task{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Task")
			err := k8sClient.Get(ctx, typeNamespacedName, task)
			if err != nil && errors.IsNotFound(err) {
				resource := &flowv1alpha1.Task{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					// spec.flow is required: a task that names no flow has
					// no graph to walk.
					Spec: flowv1alpha1.TaskSpec{Flow: exampleFlow},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &flowv1alpha1.Task{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Task")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &TaskReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})

// Task has no graceful-deletion machinery of its own, so a finalizer is the
// only way to hold an object at DeletionTimestamp != nil for a test to
// reconcile against, rather than have it vanish immediately.
var _ = Describe("reconciling a task marked for deletion", func() {
	var fx *fixture

	BeforeEach(func() {
		fx = newFixture()
	})

	It("does nothing: no status update, no Job", func() {
		fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		const finalizer = "test.flow.tgy.io/hold"
		tk.Finalizers = append(tk.Finalizers, finalizer)
		Expect(k8sClient.Update(fx.ctx, tk)).To(Succeed())
		Expect(k8sClient.Delete(fx.ctx, tk)).To(Succeed())
		DeferCleanup(func() {
			held := fx.get()
			held.Finalizers = nil
			Expect(k8sClient.Update(fx.ctx, held)).To(Succeed())
		})

		deleting := fx.get()
		Expect(deleting.DeletionTimestamp.IsZero()).To(BeFalse(), "the finalizer must have held it at delete, not removed it")

		res := fx.reconcile()
		Expect(res).To(Equal(reconcile.Result{}))
		Expect(fx.get().Status.Phase).To(BeEmpty(), "a task being deleted must not be dispatched")

		var jobs batchv1.JobList
		Expect(k8sClient.List(fx.ctx, &jobs, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(deleting.UID)})).To(Succeed())
		Expect(jobs.Items).To(BeEmpty(), "a task being deleted must get no Job")
	})
})

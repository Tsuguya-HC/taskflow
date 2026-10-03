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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/Tsuguya-HC/taskflow/internal/snapshot"
)

// This spec pins the ownership check on the start-time copy's name (#180): a
// revision already sitting under the task's derived name that the task does
// not own stops the start with an error instead of being adopted.
var _ = Describe("the start-time copy's ownership check", func() {
	It("refuses a revision squatted under the task's own derived name", func() {
		fx := newFixture()
		fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		squat := &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      snapshot.RevisionName(tk.Name, tk.UID),
				Namespace: resourceNamespace,
			},
			// Data is mandatory on a ControllerRevision; an empty object
			// carries one byte so the squat itself is accepted and the
			// start's ownership check is what refuses it.
			Data: runtime.RawExtension{Raw: []byte("{}")},
		}
		Expect(k8sClient.Create(fx.ctx, squat)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, squat) })

		_, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})
		Expect(err).To(HaveOccurred(), "starting over a revision the task does not own must not proceed silently")
		Expect(fx.get().Status.Phase).To(BeEmpty(), "a refused start writes neither a copy nor a phase")
	})
})

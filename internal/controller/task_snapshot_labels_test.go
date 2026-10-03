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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

// The snapshot revision carries the whole copy, so it is one of the objects
// the design's label rule covers: the revision went in with no task-uid label,
// and then no label selector reached it — the listing below came back empty
// where a Job answers one DeleteAllOf line.
var _ = Describe("snapshot revision labels", func() {
	var fx *fixture

	BeforeEach(func() {
		fx = newFixture()
	})

	// Listing by the task-uid label is how the user side finds everything the
	// framework made for one task at once; the label below is what lets this
	// revision answer that listing like the Job and the verdict box do.
	It("is listed by the task-uid label like the Job and the verdict box", func() {
		fx.makeFlow()
		fx.makeHandler()
		tk := fx.makeTask()

		fx.reconcile()

		var found appsv1.ControllerRevisionList
		Expect(k8sClient.List(fx.ctx, &found,
			client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(tk.UID)},
		)).To(Succeed())
		Expect(found.Items).To(HaveLen(1))

		var rev appsv1.ControllerRevision
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{
			Name: found.Items[0].Name, Namespace: resourceNamespace,
		}, &rev)).To(Succeed())
		Expect(rev.Labels).To(HaveKeyWithValue(runner.LabelManagedBy, runner.ManagedBy))
	})
})

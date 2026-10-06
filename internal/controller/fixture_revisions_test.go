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
	"k8s.io/apimachinery/pkg/types"

	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

// The fixture's idea of which revisions are a task's copy has to be the
// controller's: a revision is the task's only when the task controls it, not
// when it merely carries the task's UID in a label.
var _ = Describe("the fixture's list of a task's copies", func() {
	It("leaves out a revision that carries the task's label but is controlled by another UID", func() {
		fx := newFixture()
		fx.makeFlow()
		fx.makeHandler(stateRunner(time.Hour))
		tk := fx.makeBareTask(false)

		rev := runner.BuildSnapshotRevision(tk, []byte(`{}`))
		rev.OwnerReferences[0].UID = types.UID("not-" + string(tk.UID))
		Expect(k8sClient.Create(fx.ctx, rev)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, rev) })

		Expect(fx.revisions()).To(BeEmpty())
	})
})

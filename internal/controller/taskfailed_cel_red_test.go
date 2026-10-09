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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// Spelled as a literal so this file still compiles once the old constants
// are gone (#217).
const redTerminal = flowv1alpha1.Phase("TaskFailed")

var _ = Describe("the API around the single terminal", func() {
	ctx := context.Background()

	// The CEL rule on TaskHandlerSpec.Phase is a literal copy of the
	// reserved names, so it has to move with them (#217).
	It("refuses a handler for the single terminal, naming it", func() {
		h := &flowv1alpha1.TaskHandler{
			ObjectMeta: metav1.ObjectMeta{Name: "handler-for-taskfailed", Namespace: resourceNamespace},
			Spec:       flowv1alpha1.TaskHandlerSpec{Phase: redTerminal},
		}
		invalid(k8sClient.Create(ctx, h), "TaskFailed")
	})

	// Old names stay refused after leaving the reserved set, with a pointer
	// to the single terminal, or stale flows pass as unbound endings (#217).
	DescribeTable("refuses a handler bound to an old terminal, naming the single one",
		func(old flowv1alpha1.Phase) {
			h := &flowv1alpha1.TaskHandler{
				ObjectMeta: metav1.ObjectMeta{Name: "handler-for-" + string(old), Namespace: resourceNamespace},
				Spec:       flowv1alpha1.TaskHandlerSpec{Phase: old},
			}
			invalid(k8sClient.Create(ctx, h), "TaskFailed")
		},
		Entry("Escalated", flowv1alpha1.Phase("Escalated")),
		Entry("Failed", flowv1alpha1.Phase("Failed")),
	)

	// The terminals CEL rule is a literal copy too, and must keep refusing
	// the single terminal's meaning as the flow's to declare (#217).
	It("refuses terminals naming the single terminal", func() {
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: "terminal-taskfailed", Namespace: resourceNamespace},
			Spec: flowv1alpha1.TaskFlowSpec{
				Profile:   flowv1alpha1.ProfileInvestigate,
				Start:     "報告",
				Bindings:  map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{"報告": {Handler: "h", Next: map[flowv1alpha1.Phase]string{phaseDone: dirSent}}},
				Terminals: map[flowv1alpha1.Phase]flowv1alpha1.TerminalSeverity{redTerminal: flowv1alpha1.TerminalSuccess},
			},
		}
		invalid(k8sClient.Create(ctx, flow), "not the flow's to declare")
	})
})

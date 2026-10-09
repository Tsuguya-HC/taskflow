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
	"bytes"
	"encoding/json"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// Catches fail() logging no outcome key while failStartedAs/failBranchesAs
// do: without it a TaskFailed reached through fail() is invisible to an
// outcome-keyed query (#218-1).
var _ = Describe("fail() terminal log", func() {
	var fx *fixture
	BeforeEach(func() { fx = newFixture() })

	It("logs an outcome key when a task fails because its flow is gone", func() {
		tk := fx.makeTask()

		var buf bytes.Buffer
		logger := ctrlzap.New(ctrlzap.WriteTo(&buf), ctrlzap.JSONEncoder())
		ctx := logf.IntoContext(fx.ctx, logger)

		Expect(fx.reconciler.fail(ctx, tk, nil, "flow does not exist")).To(Succeed())
		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseTaskFailed))

		found := false
		for line := range strings.SplitSeq(buf.String(), "\n") {
			if line == "" {
				continue
			}
			var entry map[string]any
			Expect(json.Unmarshal([]byte(line), &entry)).To(Succeed())
			if _, ok := entry["outcome"]; ok {
				found = true
			}
		}
		Expect(found).To(BeTrue(), "fail() must log an outcome key like failStartedAs/failBranchesAs do")
	})
})

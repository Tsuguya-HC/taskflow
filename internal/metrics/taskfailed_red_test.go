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

package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// Catches TaskOutcomes missing the outcome label that distinguishes work
// that never concluded from a broken definition (#217). Asking for a child
// with the new label panics on the current three-label series, which is the
// red: it must count instead.
func TestTaskOutcomesCarryTheOutcomeLabel(t *testing.T) {
	labels := prometheus.Labels{
		LabelFlow: "red-taskfailed-outcome", LabelPhase: "TaskFailed",
		LabelSeverity: "TaskFailed", LabelOutcome: "NoAnswer",
	}
	t.Cleanup(func() {
		TaskOutcomes.DeletePartialMatch(prometheus.Labels{LabelFlow: "red-taskfailed-outcome"})
	})

	defer func() {
		if recover() != nil {
			t.Fatalf("TaskOutcomes.With(outcome) panicked: no outcome label")
		}
	}()
	TaskOutcomes.With(labels).Inc()
}

// Catches the descriptor still listing only the three old labels (#217).
// Matched with the surrounding format, not a substring: the metric's own
// name contains "outcome", so "outcome" alone would pass already.
func TestTaskOutcomesDescriptorNamesTheOutcomeLabel(t *testing.T) {
	ch := make(chan *prometheus.Desc, 1)
	TaskOutcomes.Describe(ch)
	select {
	case d := <-ch:
		if !strings.Contains(d.String(), "variableLabels: {flow,phase,severity,outcome}") {
			t.Fatalf("descriptor = %v, want the outcome label listed", d)
		}
	default:
		t.Fatal("TaskOutcomes described nothing")
	}
}

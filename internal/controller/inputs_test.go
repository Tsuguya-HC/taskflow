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
	"reflect"
	"testing"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

// With runs strictly serial, what led to a run is the answer the run numbered
// one less wrote — and nothing when it wrote none, or there was none.
func TestInputsFor(t *testing.T) {
	history := []flowv1alpha1.HistoryEntry{
		{Phase: "調査", RunID: 1, Directory: nextMore, Outcome: "Rework"},
		{Phase: "調査", RunID: 2, Directory: "ok", Outcome: "Declared"},
		{Phase: "報告", RunID: 3, Outcome: "NoAnswer"},
		{Phase: flowv1alpha1.PhaseFinally, RunID: 4, Directory: "tidied", Outcome: "Declared"},
	}
	cases := map[string]struct {
		run  flowv1alpha1.RunRef
		want []runner.InputEntry
	}{
		"a task's first run": {
			run:  flowv1alpha1.RunRef{Phase: "調査", RunID: 1},
			want: nil,
		},
		"the run after a rework": {
			run:  flowv1alpha1.RunRef{Phase: "調査", RunID: 2},
			want: []runner.InputEntry{{Phase: "調査", RunID: 1, Directory: nextMore}},
		},
		"the run after a forward move": {
			run:  flowv1alpha1.RunRef{Phase: "報告", RunID: 3},
			want: []runner.InputEntry{{Phase: "調査", RunID: 2, Directory: "ok"}},
		},
		// The run before wrote nothing: there is no answer to show.
		"the cleanup run after silence": {
			run:  flowv1alpha1.RunRef{Phase: flowv1alpha1.PhaseFinally, RunID: 4},
			want: nil,
		},
		// A cleanup run leads nowhere, so its own line is never an input.
		"after a cleanup run": {
			run:  flowv1alpha1.RunRef{Phase: "調査", RunID: 5},
			want: nil,
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			task := &flowv1alpha1.Task{Status: flowv1alpha1.TaskStatus{History: history}}
			if got := inputsFor(task, &c.run); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("inputsFor = %+v, want %+v", got, c.want)
			}
		})
	}
}

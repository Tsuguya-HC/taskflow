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
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// With runs strictly serial, sweepRuns is every run before the current one —
// the set prepare may find abandoned in work/ (a sealed one has already been
// renamed onto the shelf, so sweeping it again is a no-op).
func TestSweepRuns(t *testing.T) {
	cases := []struct {
		current int32
		want    []int32
	}{
		{current: 1, want: nil},
		{current: 2, want: []int32{1}},
		{current: 3, want: []int32{1, 2}},
	}
	for _, c := range cases {
		if got := sweepRuns(c.current, nil); !reflect.DeepEqual(got, c.want) {
			t.Errorf("sweepRuns(%d) = %v, want %v", c.current, got, c.want)
		}
	}
}

// A fork's other branches in flight are live, and are not debris to sweep.
func TestSweepRunsLeavesLiveBranchesAlone(t *testing.T) {
	if got := sweepRuns(5, []int32{2, 4, 6}); !reflect.DeepEqual(got, []int32{1, 3}) {
		t.Fatalf("sweepRuns = %v, want [1 3]", got)
	}
}

// unsweptRuns must keep a cancelled branch out of the sweep list even once
// the finally run is what's asking: currentRuns names only the cleanup run
// by then, but the cancelled branch's Job was only asked to go away
// (cancelBranches, background propagation), and its pod keeps sealing its
// publish for the rest of its grace period regardless of what currentRuns
// says.
func TestUnsweptRunsKeepsACancelledBranchUntilItIsReallyGone(t *testing.T) {
	task := &flowv1alpha1.Task{Status: flowv1alpha1.TaskStatus{
		CurrentRuns: []flowv1alpha1.RunRef{{Phase: flowv1alpha1.PhaseFinally, RunID: 5}},
		History: []flowv1alpha1.HistoryEntry{
			{Phase: "security", RunID: 3, Outcome: string(transition.OutcomeCancelled)},
			{Phase: "logic", RunID: 2, Outcome: string(transition.OutcomeDeclined)},
		},
	}}
	run := &flowv1alpha1.RunRef{Phase: flowv1alpha1.PhaseFinally, RunID: 5}
	got := unsweptRuns(task, run)
	want := []int32{3}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unsweptRuns = %v, want %v", got, want)
	}
	if swept := sweepRuns(5, got); !reflect.DeepEqual(swept, []int32{1, 2, 4}) {
		t.Fatalf("sweepRuns = %v, want [1 2 4]: the cancelled branch (3) left alone, the rest swept", swept)
	}
}

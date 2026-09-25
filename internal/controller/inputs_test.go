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
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// A serial flow: what led to a run is the answer on the last line of history,
// and nothing when that line wrote none or there is none.
func TestLedByInASerialFlow(t *testing.T) {
	flow := &flowv1alpha1.TaskFlowSpec{Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		"調査": {Handler: "h", Next: map[flowv1alpha1.Phase]string{"報告": "ok", "調査": nextMore}},
		"報告": {Handler: "h", Next: map[flowv1alpha1.Phase]string{"おわり": "sent"}},
	}}
	history := []flowv1alpha1.HistoryEntry{
		{Phase: "調査", RunID: 1, Directory: nextMore, Outcome: string(transition.OutcomeRework)},
		{Phase: "調査", RunID: 2, Directory: "ok", Outcome: string(transition.OutcomeDeclared)},
		{Phase: "報告", RunID: 3, Outcome: string(transition.OutcomeNoAnswer)},
		{Phase: flowv1alpha1.PhaseFinally, RunID: 4, Directory: "tidied", Outcome: string(transition.OutcomeDeclared)},
	}
	cases := map[string]struct {
		upTo     int
		run      flowv1alpha1.RunRef
		wantPrev int32
		want     []runner.InputEntry
	}{
		"a task's first run":           {upTo: 0, run: flowv1alpha1.RunRef{Phase: "調査", RunID: 1}},
		"the run after a rework":       {upTo: 1, run: flowv1alpha1.RunRef{Phase: "調査", RunID: 2}, wantPrev: 1, want: []runner.InputEntry{{Phase: "調査", RunID: 1, Directory: nextMore}}},
		"the run after a forward move": {upTo: 2, run: flowv1alpha1.RunRef{Phase: "報告", RunID: 3}, wantPrev: 2, want: []runner.InputEntry{{Phase: "調査", RunID: 2, Directory: "ok"}}},
		// The run before wrote nothing: there is no answer to show.
		"the cleanup run after silence": {upTo: 3, run: flowv1alpha1.RunRef{Phase: flowv1alpha1.PhaseFinally, RunID: 4}, wantPrev: 3},
		// A cleanup run leads nowhere, so its own line is never an input.
		"after a cleanup run": {upTo: 4, run: flowv1alpha1.RunRef{Phase: "調査", RunID: 5}, wantPrev: 3},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			task := &flowv1alpha1.Task{Status: flowv1alpha1.TaskStatus{History: history[:c.upTo]}}
			prev, got := ledBy(task, flow, &c.run)
			if prev != c.wantPrev || !reflect.DeepEqual(got, c.want) {
				t.Fatalf("ledBy = %d, %+v; want %d, %+v", prev, got, c.wantPrev, c.want)
			}
		})
	}
}

// The review fork's phases and answers, shared with task_fork_test.go.
const (
	phasePick     flowv1alpha1.Phase = "pick"
	phaseSecurity flowv1alpha1.Phase = "security"
	phaseLogic    flowv1alpha1.Phase = "logic"
	phaseTests    flowv1alpha1.Phase = "tests"
	phaseSort     flowv1alpha1.Phase = "sort"
	answerDone                       = "done"
	answerStuck                      = "stuck"
)

// The review fork: pick chooses security and logic, tests always runs, and all
// three meet at sort.
func reviewFork() *flowv1alpha1.TaskFlowSpec {
	toSort := map[flowv1alpha1.Phase]string{phaseSort: answerDone, flowv1alpha1.PhaseEscalated: answerStuck}
	return &flowv1alpha1.TaskFlowSpec{Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		phasePick: {
			Handler: "h",
			Next:    map[flowv1alpha1.Phase]string{phaseSecurity: string(phaseSecurity), phaseLogic: string(phaseLogic)},
			Join:    &flowv1alpha1.JoinSpec{Phase: phaseSort, Always: []flowv1alpha1.Phase{phaseTests}},
		},
		phaseSecurity: {Handler: "h", Next: toSort},
		phaseLogic:    {Handler: "h", Next: toSort},
		phaseTests:    {Handler: "h", Next: toSort},
		phaseSort:     {Handler: "h", Next: map[flowv1alpha1.Phase]string{answerDone: "ok"}},
	}}
}

func TestLedByAtAFork(t *testing.T) {
	flow := reviewFork()
	forked := []flowv1alpha1.HistoryEntry{{Phase: phasePick, RunID: 1, Directory: string(phaseSecurity), Outcome: string(transition.OutcomeDeclared)}}

	t.Run("a chosen branch is shown the directory that chose it", func(t *testing.T) {
		task := &flowv1alpha1.Task{Status: flowv1alpha1.TaskStatus{Phase: phasePick, History: forked}}
		prev, got := ledBy(task, flow, &flowv1alpha1.RunRef{Phase: phaseSecurity, RunID: 2})
		if want := []runner.InputEntry{{Phase: phasePick, RunID: 1, Directory: string(phaseSecurity)}}; prev != 1 || !reflect.DeepEqual(got, want) {
			t.Fatalf("ledBy = %d, %+v; want 1, %+v", prev, got, want)
		}
	})
	t.Run("an always branch was chosen by nothing, but was still started by the fork", func(t *testing.T) {
		task := &flowv1alpha1.Task{Status: flowv1alpha1.TaskStatus{Phase: phasePick, History: append(forked,
			flowv1alpha1.HistoryEntry{Phase: phaseSecurity, RunID: 2, Directory: answerDone, Outcome: string(transition.OutcomeDeclared)})}}
		prev, got := ledBy(task, flow, &flowv1alpha1.RunRef{Phase: phaseTests, RunID: 3})
		if prev != 1 || got != nil {
			t.Fatalf("ledBy = %d, %+v; want the fork's run and nothing to show", prev, got)
		}
	})
	t.Run("the join is shown every branch that met there", func(t *testing.T) {
		task := &flowv1alpha1.Task{Status: flowv1alpha1.TaskStatus{Phase: phaseSort, History: append(forked,
			flowv1alpha1.HistoryEntry{Phase: phaseTests, RunID: 3, Directory: answerDone, Outcome: string(transition.OutcomeDeclared)},
			flowv1alpha1.HistoryEntry{Phase: phaseSecurity, RunID: 2, Directory: answerDone, Outcome: string(transition.OutcomeDeclared)})}}
		prev, got := ledBy(task, flow, &flowv1alpha1.RunRef{Phase: phaseSort, RunID: 4})
		want := []runner.InputEntry{{Phase: phaseSecurity, RunID: 2, Directory: answerDone}, {Phase: phaseTests, RunID: 3, Directory: answerDone}}
		if prev != 3 || !reflect.DeepEqual(got, want) {
			t.Fatalf("ledBy = %d, %+v; want 3, %+v", prev, got, want)
		}
	})
	t.Run("the cleanup run after a branch stopped the task is shown every branch sent to Escalated", func(t *testing.T) {
		task := &flowv1alpha1.Task{Status: flowv1alpha1.TaskStatus{Phase: flowv1alpha1.PhaseEscalated, History: []flowv1alpha1.HistoryEntry{
			{Phase: phasePick, RunID: 1, Directory: "logic/security", Outcome: string(transition.OutcomeDeclared)},
			{Phase: phaseTests, RunID: 4, Directory: answerDone, Outcome: string(transition.OutcomeDeclared)},
			{Phase: phaseSecurity, RunID: 3, Directory: answerStuck, Outcome: string(transition.OutcomeDeclined)},
			{Phase: phaseLogic, RunID: 2, Directory: answerStuck, Outcome: string(transition.OutcomeDeclined)},
		}}}
		prev, got := ledBy(task, flow, &flowv1alpha1.RunRef{Phase: flowv1alpha1.PhaseFinally, RunID: 5})
		want := []runner.InputEntry{{Phase: phaseLogic, RunID: 2, Directory: answerStuck}, {Phase: phaseSecurity, RunID: 3, Directory: answerStuck}}
		if prev != 2 || !reflect.DeepEqual(got, want) {
			t.Fatalf("ledBy = %d, %+v; want 2 (the deciding branch), %+v", prev, got, want)
		}
		if got := endingOutcome(task, flow, &flowv1alpha1.RunRef{Phase: flowv1alpha1.PhaseFinally, RunID: 5}); got != string(transition.OutcomeDeclined) {
			t.Fatalf("endingOutcome = %q, want the deciding branch's", got)
		}
	})
}

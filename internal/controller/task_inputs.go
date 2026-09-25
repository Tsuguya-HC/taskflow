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
	"cmp"
	"slices"
	"strings"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// ledBy is what led to run, read off history: the answers shown in its inputs
// view (ADR-0013 決定6), and the run its prev-run-id annotation names (決定8).
// Three shapes, one per way a run can be reached:
//
//   - a branch was chosen by its fork's run: the directory that chose it, and
//     nothing for a branch the join's always list started;
//   - a join run is reached by its branches: every one that met there;
//   - the next phase of a serial flow, or a rework, follows the run on the
//     last line of history;
//   - the cleanup run follows the run that decided the ending (decidingLine),
//     and is shown nothing when none did. After a fork's branch stopped the
//     task it is shown every branch sent to Escalated in the same step, not
//     only the one that decided it.
//
// prev is 0 for a task's first run.
func ledBy(task *flowv1alpha1.Task, flow *flowv1alpha1.TaskFlowSpec, run *flowv1alpha1.RunRef) (prev int32, inputs []runner.InputEntry) {
	history := task.Status.History
	if fork := task.Status.Phase; run.Phase != fork && isBranchOf(flow, fork, run.Phase) {
		for _, h := range slices.Backward(history) {
			if h.Phase != fork || h.RunID >= run.RunID {
				continue
			}
			dir := flow.Bindings[fork].Next[run.Phase]
			if dir != "" && slices.Contains(strings.Split(h.Directory, contract.DirectorySeparator), dir) {
				inputs = []runner.InputEntry{{Phase: fork, RunID: h.RunID, Directory: dir}}
			}
			return h.RunID, inputs
		}
		return lastRun(history), nil
	}

	if arrived := arrivals(history, flow, run); len(arrived) > 0 {
		for _, h := range arrived {
			prev = max(prev, h.RunID)
			inputs = append(inputs, runner.InputEntry{Phase: h.Phase, RunID: h.RunID, Directory: h.Directory})
		}
		slices.SortFunc(inputs, func(a, b runner.InputEntry) int { return cmp.Compare(a.Phase, b.Phase) })
		return prev, inputs
	}

	if run.Phase.IsFinally() {
		i := decidingLine(task, flow, run)
		if i < 0 {
			return lastRun(history), nil
		}
		deciding := history[i]
		if fork, ok := forkOf(flow, deciding.Phase); ok {
			return deciding.RunID, escalatedBranches(history[:i+1], flow, fork)
		}
		return deciding.RunID, answerOf(deciding)
	}
	i := lastNonFinally(history)
	if i < 0 {
		return lastRun(history), nil
	}
	return history[i].RunID, answerOf(history[i])
}

// decidingLine is the index in history of the run that decided the ending the
// cleanup run follows, or -1 when no run did — the task was stopped on its
// definition while a run was still in flight, and what that run would have
// answered was never recorded (ADR-0009 決定6, P8: the tail of history is then
// some earlier run's line, not this ending's, and is not passed off as it).
//
// The deciding run is recorded last (ADR-0013 決定8), so it is the last line
// that is not a cleanup run's — when that line is what ended the task. In a
// serial flow that is the run numbered one before the cleanup run. A fork's
// branch is numbered by where it stood among its siblings instead, so a
// branch's line counts when it is one that stopped the task: not one that
// reached the join, and not one cancelled.
func decidingLine(task *flowv1alpha1.Task, flow *flowv1alpha1.TaskFlowSpec, cleanup *flowv1alpha1.RunRef) int {
	history := task.Status.History
	i := lastNonFinally(history)
	if i < 0 {
		return -1
	}
	last := history[i]
	if last.RunID == cleanup.RunID-1 {
		return i
	}
	if _, ok := forkOf(flow, last.Phase); ok {
		switch transition.Outcome(last.Outcome) {
		case transition.OutcomeDeclared, transition.OutcomeRework, transition.OutcomeCancelled:
			return -1
		}
		return i
	}
	return -1
}

// arrivals is the branches that met at run's phase: reading back from the end
// of history, every line that is a branch of a fork joining at run's phase,
// up to the first that is not. A run reached some other way — a rework from
// later in the flow into the same phase — has no such lines behind it, and is
// read as a serial run instead.
func arrivals(history []flowv1alpha1.HistoryEntry, flow *flowv1alpha1.TaskFlowSpec, run *flowv1alpha1.RunRef) []flowv1alpha1.HistoryEntry {
	var arrived []flowv1alpha1.HistoryEntry
	for _, h := range slices.Backward(history) {
		fork, ok := forkOf(flow, h.Phase)
		if !ok || flow.Bindings[fork].Join.Phase != run.Phase || h.RunID >= run.RunID {
			break
		}
		if h.Directory != "" {
			arrived = append(arrived, h)
		}
	}
	return arrived
}

// escalatedBranches is every branch of fork that the step which stopped the
// task sent to Escalated, read back from the end of lines: the branches of the
// fork recorded since its own last line, less those that were cancelled or had
// reached the join (ADR-0013 決定4・6).
func escalatedBranches(lines []flowv1alpha1.HistoryEntry, flow *flowv1alpha1.TaskFlowSpec, fork flowv1alpha1.Phase) []runner.InputEntry {
	var inputs []runner.InputEntry
	for _, h := range slices.Backward(lines) {
		if !isBranchOf(flow, fork, h.Phase) {
			break
		}
		switch transition.Outcome(h.Outcome) {
		case transition.OutcomeCancelled, transition.OutcomeDeclared, transition.OutcomeRework:
			continue
		}
		inputs = append(inputs, answerOf(h)...)
	}
	slices.SortFunc(inputs, func(a, b runner.InputEntry) int { return cmp.Compare(a.Phase, b.Phase) })
	return inputs
}

// answerOf is one line's answer as an input: the directory it wrote, when it
// wrote exactly one. A fork's line names several joined, which is not one
// place on the shelf to show.
func answerOf(h flowv1alpha1.HistoryEntry) []runner.InputEntry {
	if h.Directory == "" || strings.Contains(h.Directory, contract.DirectorySeparator) {
		return nil
	}
	return []runner.InputEntry{{Phase: h.Phase, RunID: h.RunID, Directory: h.Directory}}
}

// isBranchOf reports whether phase is one of fork's branches in flow.
func isBranchOf(flow *flowv1alpha1.TaskFlowSpec, fork, phase flowv1alpha1.Phase) bool {
	return forks(flow, fork) && slices.Contains(transition.Branches(flow.Bindings, fork), phase)
}

// forkOf is the fork phase is a branch of, if any. Admission lets a phase be a
// branch of one fork only; the first in name order is taken if a flow edited
// since says otherwise.
func forkOf(flow *flowv1alpha1.TaskFlowSpec, phase flowv1alpha1.Phase) (flowv1alpha1.Phase, bool) {
	var found []flowv1alpha1.Phase
	for fork := range flow.Bindings {
		if isBranchOf(flow, fork, phase) {
			found = append(found, fork)
		}
	}
	if len(found) == 0 {
		return "", false
	}
	return slices.Min(found), true
}

// lastNonFinally is the index of the last line in history that is not a
// cleanup run's, or -1.
func lastNonFinally(history []flowv1alpha1.HistoryEntry) int {
	for i, h := range slices.Backward(history) {
		if !h.Phase.IsFinally() {
			return i
		}
	}
	return -1
}

// lastRun is the run on the last line of history, or 0 when there is none.
func lastRun(history []flowv1alpha1.HistoryEntry) int32 {
	if len(history) == 0 {
		return 0
	}
	return history[len(history)-1].RunID
}

// endingOutcome is how the run that decided the ending the cleanup run
// follows was accounted for, and empty when no run decided it (decidingLine).
func endingOutcome(task *flowv1alpha1.Task, flow *flowv1alpha1.TaskFlowSpec, cleanup *flowv1alpha1.RunRef) string {
	if i := decidingLine(task, flow, cleanup); i >= 0 {
		return task.Status.History[i].Outcome
	}
	return ""
}

// liveRuns is the numbers of every run in flight besides run — a fork's other
// branches — which prepare must not sweep away as though they were debris.
func liveRuns(task *flowv1alpha1.Task, run *flowv1alpha1.RunRef) []int32 {
	var live []int32
	for _, r := range task.Status.CurrentRuns {
		if r.Phase != run.Phase {
			live = append(live, r.RunID)
		}
	}
	return live
}

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

// Package taskstate records what happened. Where the transition package
// decides, this writes the decision down: the counters, the history, and the
// pointer to the run in flight.
//
// Like transition it is pure, so the bookkeeping that makes cycles terminate
// can be tested without a cluster.
package taskstate

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
	"github.com/Tsuguya/taskflow/internal/transition"
)

// Visited is every phase this task has already run, derived from history
// rather than stored beside it. Two records of the same fact drift; this one
// cannot disagree with the history a human reads.
func Visited(status *flowv1alpha1.TaskStatus, bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding) map[flowv1alpha1.Phase]bool {
	seen := make(map[flowv1alpha1.Phase]bool, len(status.History)+1)
	for _, h := range status.History {
		seen[h.Phase] = true
	}
	// The phase in flight has run even though it has not been recorded yet,
	// which is what makes a self-loop count as a rework.
	if status.Phase != "" && !transition.IsTerminal(bindings, status.Phase) {
		seen[status.Phase] = true
	}
	return seen
}

// Advance records the completed run and moves the task to res.Next.
//
// runID rises on every run — reworks and infrastructure retries alike —
// because it names the run's directory and its child objects. reworkBudget
// only falls, and only when a rework is actually taken; that asymmetry is
// what bounds a cycle.
func Advance(
	status *flowv1alpha1.TaskStatus,
	bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding,
	directory string,
	res transition.Result,
	ref string,
	now metav1.Time,
) {
	status.History = append(status.History, flowv1alpha1.HistoryEntry{
		Phase:      status.Phase,
		RunID:      status.RunID,
		Directory:  directory,
		Outcome:    string(res.Outcome),
		Ref:        ref,
		FinishedAt: &now,
	})

	status.Phase = res.Next
	status.ReworkBudget = res.Budget

	// A stopped task has nothing in flight. Leaving a stale currentRun here
	// would let a late answer look like it belonged to something, and
	// incrementing runID here would leave status.runID naming a run that
	// never happened — "last run" would stop being true.
	if transition.IsTerminal(bindings, res.Next) {
		status.CurrentRun = nil
		return
	}
	status.RunID++
	status.CurrentRun = &flowv1alpha1.RunRef{Phase: res.Next, RunID: status.RunID}
}

// RetryInfra prepares another attempt at the same phase after a failure that
// was not the handler's judgement — an image that would not pull, an evicted
// pod. It costs a runID, so the retry gets fresh directories rather than
// whatever the last attempt left behind, and it costs no budget, because
// nothing was decided.
//
// Nothing is appended to history: no verdict was reached, and a history of
// non-events makes the record harder to read, not easier.
func RetryInfra(status *flowv1alpha1.TaskStatus) {
	status.RunID++
	retries := int32(0)
	if status.CurrentRun != nil {
		retries = status.CurrentRun.InfraRetries + 1
	}
	status.CurrentRun = &flowv1alpha1.RunRef{
		Phase:        status.Phase,
		RunID:        status.RunID,
		InfraRetries: retries,
	}
}

// InfraRetriesExhausted reports whether another infrastructure retry is
// allowed. When it is not, the task escalates rather than failing: something
// outside the handler kept it from running, and that is for a human to look
// at.
func InfraRetriesExhausted(status *flowv1alpha1.TaskStatus, max int32) bool {
	if status.CurrentRun == nil {
		return false
	}
	return status.CurrentRun.InfraRetries >= max
}

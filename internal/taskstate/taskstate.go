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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// ConditionReady is the single condition a task carries: whether the
// framework can go on with it.
const ConditionReady = "Ready"

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
//
// ttl is the flow's, read the same instant the task lands on a terminal
// phase: a write that settles the run and one that dates its cleanup must
// not be two steps a caller could do in either order, or forget to pair.
func Advance(
	status *flowv1alpha1.TaskStatus,
	bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding,
	directory string,
	res transition.Result,
	ref string,
	ttl *flowv1alpha1.TTLSpec,
	now metav1.Time,
) {
	status.History = append(status.History, flowv1alpha1.HistoryEntry{
		Phase:      status.Phase,
		RunID:      status.RunID,
		Directory:  directory,
		Outcome:    string(res.Outcome),
		Reason:     res.Detail,
		Ref:        ref,
		FinishedAt: &now,
	})

	status.Phase = res.Next
	status.ReworkBudget = res.Budget

	// Whether the task landed on Escalated/Failed because the framework
	// could not go on, or because the flow itself declared that edge with
	// next, nothing moves forward until a human looks. The history entry
	// says so too, but a condition is where kubectl and anything watching
	// for stuck tasks look first; res.Outcome as the reason is what tells
	// the two paths apart.
	if res.Next.IsReserved() {
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:    ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  string(res.Outcome),
			Message: res.Detail,
		})
	}

	// A stopped task has nothing in flight. Leaving a stale currentRun here
	// would let a late answer look like it belonged to something, and
	// incrementing runID here would leave status.runID naming a run that
	// never happened — "last run" would stop being true.
	if transition.IsTerminal(bindings, res.Next) {
		status.CurrentRun = nil
		Expire(status, bindings, ttl, now)
		return
	}
	status.RunID++
	status.CurrentRun = &flowv1alpha1.RunRef{Phase: res.Next, RunID: status.RunID}
}

// Expire stamps the deletion time of a task that has stopped, and does
// nothing to one that has not. Which of the two durations applies is the
// framework's call, not the flow's: Escalated and Failed take ttl.failed —
// even when the flow itself declared the edge with next — because both mean
// a human has to look before the task can move again; every other terminal
// phase needs no such review and takes ttl.succeeded. A nil ttl
// or a nil duration leaves the task to be cleaned up by hand.
//
// A date already stamped is never moved: Expire is called both the instant
// a task lands on a terminal phase and, for one that landed there before
// this existed, on a later reconcile that only means to backfill what that
// first call missed. Idempotence here is what lets the second kind of call
// be unconditional rather than needing its own "already has one" guard.
func Expire(
	status *flowv1alpha1.TaskStatus,
	bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding,
	ttl *flowv1alpha1.TTLSpec,
	now metav1.Time,
) {
	if status.ExpiresAt != nil || ttl == nil || !transition.IsTerminal(bindings, status.Phase) {
		return
	}
	d := ttl.Succeeded
	if status.Phase.IsReserved() {
		d = ttl.Failed
	}
	if d == nil {
		return
	}
	t := metav1.NewTime(now.Add(d.Duration))
	status.ExpiresAt = &t
}

// Begin puts a fresh task on the flow's starting phase.
func Begin(status *flowv1alpha1.TaskStatus, start flowv1alpha1.Phase, budget int32) {
	status.Phase = start
	status.RunID = 1
	status.ReworkBudget = budget
	status.CurrentRun = &flowv1alpha1.RunRef{Phase: start, RunID: 1}
}

// Fail stops a task whose flow is broken. Nothing is retried: the fault is in
// the definition rather than in the work, and guessing at a repair would hide
// it.
//
// ttl is the flow's, or nil when the fault is that there is no flow to read
// one from; Expire then leaves the task alone. That is a wait, not a dead
// end: the controller keeps looking for a flow of that name on every later
// reconcile and backfills the date once one appears.
func Fail(status *flowv1alpha1.TaskStatus, reason string, ttl *flowv1alpha1.TTLSpec, now metav1.Time) {
	status.Phase = flowv1alpha1.PhaseFailed
	status.CurrentRun = nil
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:    ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "FlowBroken",
		Message: reason,
	})
	// Failed is reserved, so it is terminal on its own say-so; Expire needs
	// no binding table to see that (transition.IsTerminal).
	Expire(status, nil, ttl, now)
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

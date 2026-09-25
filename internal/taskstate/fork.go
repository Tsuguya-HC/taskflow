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

package taskstate

import (
	"cmp"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// What a fork does to status (ADR-0013). While its branches run the task
// stands at the fork — status.phase names it — and currentRuns holds one run
// per branch. This is the one place the invariant stated in this package's
// doc does not hold: the runs in flight name the fork's branches, not
// status.phase.
//
// Each piece below is one step the controller takes, in the order it takes
// them; none decides where the task goes — transition does that — and none
// talks to the cluster.

// Run is the run in flight for phase, or nil when none is.
func Run(status *flowv1alpha1.TaskStatus, phase flowv1alpha1.Phase) *flowv1alpha1.RunRef {
	for i := range status.CurrentRuns {
		if status.CurrentRuns[i].Phase == phase {
			return &status.CurrentRuns[i]
		}
	}
	return nil
}

// SetRun writes run into currentRuns in the place of the run for the same
// phase, or beside the others when there is none, keeping them in phase order.
func SetRun(status *flowv1alpha1.TaskStatus, run flowv1alpha1.RunRef) {
	if existing := Run(status, run.Phase); existing != nil {
		*existing = run
	} else {
		status.CurrentRuns = append(status.CurrentRuns, run)
		slices.SortFunc(status.CurrentRuns, func(a, b flowv1alpha1.RunRef) int {
			return cmp.Compare(a.Phase, b.Phase)
		})
	}
}

// SettleFork records the fork's own run and takes the task where it said:
// its branches, each a run of its own numbered in branch order (ADR-0013
// 決定5), or the ending it stopped at. The task stays at the fork while its
// branches run.
func SettleFork(
	status *flowv1alpha1.TaskStatus,
	flow *flowv1alpha1.TaskFlowSpec,
	directory string,
	res transition.ForkResult,
	now metav1.Time,
) {
	fork := Current(status)
	if fork == nil {
		fork = &flowv1alpha1.RunRef{Phase: status.Phase, RunID: status.RunID}
	}
	record(status, fork, directory, res.Outcome, res.Detail, now)
	if len(res.Branches) == 0 {
		move(status, flow, transition.Result{Next: res.Next, Outcome: res.Outcome, Detail: res.Detail}, now)
		return
	}
	runs := make([]flowv1alpha1.RunRef, 0, len(res.Branches))
	for _, b := range res.Branches {
		status.RunID++
		runs = append(runs, flowv1alpha1.RunRef{Phase: b, RunID: status.RunID})
	}
	status.CurrentRuns = runs
}

// SettledBranch is one branch's run and where it went, gathered for
// SettleBranches so every branch that settled in the same reconcile is acted
// on together rather than one call at a time deciding its own piece.
type SettledBranch struct {
	Run       flowv1alpha1.RunRef
	Directory string
	Result    transition.Result
}

// SettleBranches records every branch that settled this reconcile and takes
// the task where they leave it (ADR-0013 決定4, 決定8). Which branches
// settled together, and in what order the caller happened to learn of them,
// must not change what the task ends up with — a fork's branches run beside
// each other, so the shape of the result is this function's to decide, not
// something the caller can get right by choosing a calling order:
//
//   - if every settled branch reached the join, each is recorded, in phase
//     order, and taken out of the runs in flight. If that empties
//     currentRuns, the join's own run starts in this same call: a task must
//     never sit with none of its branches in flight while still standing at
//     the fork they left, since that is the shape Reconcile's recovery path
//     (task_controller.go) reads as "written but never started" and drives
//     all over again. If branches that did not settle this reconcile are
//     still running, nothing moves yet — the task stays at the fork.
//   - if one of them instead decided the task's ending (Escalated, Failed,
//     ... — anything but the join; the first such branch in phase order,
//     when more than one did) — every other settled branch is recorded
//     first, in phase order, each under its own outcome; every branch still
//     in currentRuns that did not settle this reconcile is recorded next, in
//     phase order, as Cancelled — nothing it could say would change where
//     the task is going; and the deciding branch is recorded last, so the
//     last line in history is always the run that decided the ending, fork
//     or not, which is where the cleanup run reads it from (決定6). The task
//     then moves to the ending the deciding branch reached.
//
// An entry in settled is discarded before anything above runs, as if it had
// not been named, in any of three cases: its phase names no run in currentRuns
// at all (settled after the join it was headed for had already started, say);
// its RunID does not match the run currentRuns has for that phase (a stale or
// mistaken ref); or its phase was already matched by an earlier entry in this
// same settled that passed both those checks — only the first entry for a
// given branch that names a real, current run is kept, and every later entry
// for that same phase is as if absent, whether or not it would itself have
// matched. SettleBranches never acts on more than one run per phase, and
// never on the caller's say-so alone: what it records for an entry that does
// survive is the run currentRuns itself holds — the caller's copy of Run and
// Directory and Result are read, but Runner and everything else about the run
// in history come from status, not from what the caller happened to pass.
func SettleBranches(
	status *flowv1alpha1.TaskStatus,
	flow *flowv1alpha1.TaskFlowSpec,
	settled []SettledBranch,
	now metav1.Time,
) {
	seen := make(map[flowv1alpha1.Phase]bool, len(settled))
	valid := make([]SettledBranch, 0, len(settled))
	for _, sb := range settled {
		if seen[sb.Run.Phase] {
			continue
		}
		r := Run(status, sb.Run.Phase)
		if r == nil || r.RunID != sb.Run.RunID {
			continue
		}
		seen[sb.Run.Phase] = true
		valid = append(valid, SettledBranch{Run: *r, Directory: sb.Directory, Result: sb.Result})
	}
	if len(valid) == 0 {
		return
	}
	slices.SortFunc(valid, func(a, b SettledBranch) int {
		return cmp.Compare(a.Run.Phase, b.Run.Phase)
	})

	var joinPhase flowv1alpha1.Phase
	if binding, bound := flow.Bindings[status.Phase]; bound && binding.Join != nil {
		joinPhase = binding.Join.Phase
	}

	decidingIdx := -1
	for i, sb := range valid {
		if sb.Result.Next != joinPhase {
			decidingIdx = i
			break
		}
	}

	if decidingIdx == -1 {
		for _, sb := range valid {
			record(status, &sb.Run, sb.Directory, sb.Result.Outcome, sb.Result.Detail, now)
			status.CurrentRuns = slices.DeleteFunc(status.CurrentRuns, func(r flowv1alpha1.RunRef) bool {
				return r.Phase == sb.Run.Phase
			})
		}
		if len(status.CurrentRuns) != 0 {
			return
		}
		status.CurrentRuns = nil
		status.Phase = joinPhase
		status.RunID++
		SetCurrent(status, &flowv1alpha1.RunRef{Phase: joinPhase, RunID: status.RunID})
		return
	}

	deciding := valid[decidingIdx]
	settledPhase := make(map[flowv1alpha1.Phase]bool, len(valid))
	for _, sb := range valid {
		settledPhase[sb.Run.Phase] = true
	}
	for i, sb := range valid {
		if i == decidingIdx {
			continue
		}
		record(status, &sb.Run, sb.Directory, sb.Result.Outcome, sb.Result.Detail, now)
	}
	for _, run := range status.CurrentRuns {
		if run.Phase == deciding.Run.Phase || settledPhase[run.Phase] {
			continue
		}
		record(status, &run, "", transition.OutcomeCancelled,
			"cancelled: "+string(deciding.Run.Phase)+" already sent the task to "+string(deciding.Result.Next), now)
	}
	status.CurrentRuns = nil
	record(status, &deciding.Run, deciding.Directory, deciding.Result.Outcome, deciding.Result.Detail, now)
	move(status, flow, deciding.Result, now)
}

// CancelBranches records every run still in flight as Cancelled and clears
// them — the shape SettleBranches (決定4) gives a branch that was not the one
// to decide, except here nothing decided anything: the flow's own definition
// broke while they ran, so every branch stops the same way, under the same
// reason. It returns what it cancelled, so the caller can stop their Jobs
// the way any other cancelled branch's is.
func CancelBranches(status *flowv1alpha1.TaskStatus, reason string, now metav1.Time) []flowv1alpha1.RunRef {
	cancelled := slices.Clone(status.CurrentRuns)
	for i := range cancelled {
		record(status, &cancelled[i], "", transition.OutcomeCancelled, reason, now)
	}
	status.CurrentRuns = nil
	return cancelled
}

// Branching reports whether the runs in flight are a fork's branches: the
// task stands at a phase none of them names. That is the one shape in which
// status.phase and the runs part other than the cleanup run (ADR-0013 決定8),
// and the one Reconcile hands to the branch driver rather than the serial
// one.
func Branching(status *flowv1alpha1.TaskStatus) bool {
	if Idle(status) || InFinally(status) {
		return false
	}
	return Run(status, status.Phase) == nil
}

// RetryRun is RetryInfra for one run among several: the branch named phase is
// started again under the same number, its attempt counted, with nothing of
// the attempt that never started carried over (ADR-0004). A phase with no run
// in flight is left alone.
func RetryRun(status *flowv1alpha1.TaskStatus, phase flowv1alpha1.Phase) {
	run := Run(status, phase)
	if run == nil {
		return
	}
	*run = flowv1alpha1.RunRef{Phase: run.Phase, RunID: run.RunID, InfraRetries: run.InfraRetries + 1}
}

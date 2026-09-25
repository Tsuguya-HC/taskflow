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
// per branch. That is the one place the invariant in this package's doc
// parts: the runs in flight name the fork's branches, not status.phase.
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
	status.CurrentRun = legacyMirror(status)
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
	status.CurrentRun = legacyMirror(status)
}

// SettleBranch records one branch's run and takes it out of the runs in
// flight. Where the branch said to go is the caller's to act on — a branch
// that reached the join waits for the others, and one that did not stops the
// whole task (StopFork) — so nothing moves here.
func SettleBranch(
	status *flowv1alpha1.TaskStatus,
	run flowv1alpha1.RunRef,
	directory string,
	res transition.Result,
	now metav1.Time,
) {
	record(status, &run, directory, res.Outcome, res.Detail, now)
	status.CurrentRuns = slices.DeleteFunc(status.CurrentRuns, func(r flowv1alpha1.RunRef) bool {
		return r.Phase == run.Phase
	})
	if len(status.CurrentRuns) == 0 {
		status.CurrentRuns = nil
	}
	status.CurrentRun = legacyMirror(status)
}

// StopFork takes the task to the ending a branch decided — deciding, which
// answered directory and was sent to res.Next by the flow's own rules — while
// other branches may still be in flight (ADR-0013 決定4). Those are recorded
// as Cancelled: nothing they could say would change where the task is going.
// deciding is recorded after them, last, so that the last line in history is
// always the run that decided the ending, fork or not — which is where the
// cleanup run reads the ending from (決定6). Branches that settled alongside
// it without deciding anything are the caller's to record first, with
// SettleBranch.
func StopFork(
	status *flowv1alpha1.TaskStatus,
	flow *flowv1alpha1.TaskFlowSpec,
	deciding flowv1alpha1.RunRef,
	directory string,
	res transition.Result,
	now metav1.Time,
) {
	for _, run := range status.CurrentRuns {
		if run.Phase != deciding.Phase {
			record(status, &run, "", transition.OutcomeCancelled,
				"cancelled: "+string(deciding.Phase)+" already sent the task to "+string(res.Next), now)
		}
	}
	status.CurrentRuns = nil
	record(status, &deciding, directory, res.Outcome, res.Detail, now)
	move(status, flow, res, now)
}

// StartJoin is every branch having reached the join: the task moves there, and
// one run of it starts.
func StartJoin(status *flowv1alpha1.TaskStatus, join flowv1alpha1.Phase) {
	status.Phase = join
	status.RunID++
	SetCurrent(status, &flowv1alpha1.RunRef{Phase: join, RunID: status.RunID})
}

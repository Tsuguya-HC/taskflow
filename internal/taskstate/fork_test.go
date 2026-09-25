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
	"slices"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

const (
	phasePick     flowv1alpha1.Phase = "観点出し"
	phaseSecurity flowv1alpha1.Phase = "security"
	phaseLogic    flowv1alpha1.Phase = "logic"
	phaseSort     flowv1alpha1.Phase = "仕分け"
)

func forkFlow() *flowv1alpha1.TaskFlowSpec {
	toSort := map[flowv1alpha1.Phase]string{phaseSort: "done", flowv1alpha1.PhaseEscalated: "stuck"}
	return &flowv1alpha1.TaskFlowSpec{Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		phasePick: {
			Handler: "pick",
			Next:    map[flowv1alpha1.Phase]string{phaseSecurity: "security", phaseLogic: "logic"},
			Join:    &flowv1alpha1.JoinSpec{Phase: phaseSort},
		},
		phaseSecurity: {Handler: "s", Next: toSort},
		phaseLogic:    {Handler: "l", Next: toSort},
		phaseSort:     {Handler: "sort", Next: map[flowv1alpha1.Phase]string{phaseDone: "ok"}},
	}}
}

// atFork is a task whose fork's run is in flight, as run 1.
func atFork() *flowv1alpha1.TaskStatus {
	s := &flowv1alpha1.TaskStatus{}
	Begin(s, phasePick)
	return s
}

func phasesOf(runs []flowv1alpha1.RunRef) []flowv1alpha1.Phase {
	var out []flowv1alpha1.Phase
	for _, r := range runs {
		out = append(out, r.Phase)
	}
	return out
}

// The fork's own run is recorded, the task stays at the fork, and each branch
// gets a run of its own, numbered in branch order.
func TestAForkStartsItsBranches(t *testing.T) {
	s := atFork()
	SettleFork(s, forkFlow(), "logic/security", transition.ForkResult{
		Branches: []flowv1alpha1.Phase{phaseLogic, phaseSecurity}, Outcome: transition.OutcomeDeclared,
	}, at)

	if s.Phase != phasePick {
		t.Fatalf("phase = %q, want the task to stand at the fork while its branches run", s.Phase)
	}
	want := []flowv1alpha1.RunRef{{Phase: phaseLogic, RunID: 2}, {Phase: phaseSecurity, RunID: 3}}
	if !slices.Equal(s.CurrentRuns, want) || s.RunID != 3 {
		t.Fatalf("runs = %+v, runID = %d; want %+v and 3", s.CurrentRuns, s.RunID, want)
	}
	if len(s.History) != 1 || s.History[0].Phase != phasePick || s.History[0].RunID != 1 || s.History[0].Directory != "logic/security" {
		t.Fatalf("history = %+v, want the fork's own run as run 1", s.History)
	}
	if s.CurrentRun != nil {
		t.Fatal("two runs in flight have no single-run mirror")
	}

	// While the branches run it is they, and not the fork, that are in
	// flight: the fork has run once, each branch once.
	runs := Runs(s, forkFlow().Bindings)
	if runs[phasePick] != 1 || runs[phaseLogic] != 1 || runs[phaseSecurity] != 1 {
		t.Fatalf("runs = %v, want the fork and each branch counted once", runs)
	}
}

// Branches settle one at a time; the last to reach the join starts it.
func TestBranchesMeetAtTheJoin(t *testing.T) {
	s := atFork()
	SettleFork(s, forkFlow(), "logic/security", transition.ForkResult{
		Branches: []flowv1alpha1.Phase{phaseLogic, phaseSecurity}, Outcome: transition.OutcomeDeclared,
	}, at)
	arrived := transition.Result{Next: phaseSort, Outcome: transition.OutcomeDeclared}

	SettleBranch(s, *Run(s, phaseSecurity), "done", arrived, at)
	if got := phasesOf(s.CurrentRuns); !slices.Equal(got, []flowv1alpha1.Phase{phaseLogic}) {
		t.Fatalf("runs in flight = %v, want logic still running", got)
	}
	SettleBranch(s, *Run(s, phaseLogic), "done", arrived, at)
	if s.CurrentRuns != nil {
		t.Fatalf("runs in flight = %+v, want none once every branch has settled", s.CurrentRuns)
	}

	StartJoin(s, phaseSort)
	if s.Phase != phaseSort || s.RunID != 4 {
		t.Fatalf("phase = %q, runID = %d; want the join as run 4", s.Phase, s.RunID)
	}
	if got := Current(s); got == nil || *got != (flowv1alpha1.RunRef{Phase: phaseSort, RunID: 4}) {
		t.Fatalf("run in flight = %+v, want the join's", got)
	}
	var lines []flowv1alpha1.Phase
	for _, h := range s.History {
		lines = append(lines, h.Phase)
	}
	if !slices.Equal(lines, []flowv1alpha1.Phase{phasePick, phaseSecurity, phaseLogic}) {
		t.Fatalf("history = %v, want the fork then each branch in the order it settled", lines)
	}
}

// A branch that escalates stops the task: the others still running are
// cancelled, and the branch that decided it is the last line in history.
func TestABranchThatEscalatesStopsTheFork(t *testing.T) {
	s := atFork()
	SettleFork(s, forkFlow(), "logic/security", transition.ForkResult{
		Branches: []flowv1alpha1.Phase{phaseLogic, phaseSecurity}, Outcome: transition.OutcomeDeclared,
	}, at)
	stuck := transition.Result{Next: flowv1alpha1.PhaseEscalated, Outcome: transition.OutcomeDeclined, Detail: "gave up"}
	flow := forkFlow()
	flow.TTL = ttl(time.Hour, 168*time.Hour)

	StopFork(s, flow, *Run(s, phaseLogic), "stuck", stuck, at)

	if s.Phase != flowv1alpha1.PhaseEscalated || s.CurrentRuns != nil || s.ExpiresAt == nil {
		t.Fatalf("phase = %q, runs = %+v, expiresAt = %v; want a stopped task", s.Phase, s.CurrentRuns, s.ExpiresAt)
	}
	cancelled := s.History[len(s.History)-2]
	if cancelled.Phase != phaseSecurity || cancelled.Outcome != string(transition.OutcomeCancelled) || cancelled.Directory != "" {
		t.Fatalf("second-to-last line = %+v, want security cancelled", cancelled)
	}
	if last := s.History[len(s.History)-1]; last.Phase != phaseLogic || last.Outcome != string(transition.OutcomeDeclined) {
		t.Fatalf("last line = %+v, want the branch that decided the ending", last)
	}
	if !meta.IsStatusConditionFalse(s.Conditions, ConditionReady) {
		t.Fatal("an escalated fork needs a human, and says so")
	}
}

// A fork that stops at its own run — nothing started — ends like any run.
func TestAForkThatStopsStartsNothing(t *testing.T) {
	s := atFork()
	SettleFork(s, forkFlow(), "", transition.ForkResult{
		Next: flowv1alpha1.PhaseEscalated, Outcome: transition.OutcomeNoAnswer, Detail: "silence",
	}, at)
	if s.Phase != flowv1alpha1.PhaseEscalated || s.CurrentRuns != nil || s.RunID != 1 {
		t.Fatalf("phase = %q, runs = %+v, runID = %d; want Escalated with nothing started", s.Phase, s.CurrentRuns, s.RunID)
	}
}

func TestSetRunKeepsOneRunPerPhaseInOrder(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{}
	SetRun(s, flowv1alpha1.RunRef{Phase: phaseSecurity, RunID: 3})
	SetRun(s, flowv1alpha1.RunRef{Phase: phaseLogic, RunID: 2})
	SetRun(s, flowv1alpha1.RunRef{Phase: phaseSecurity, RunID: 3, JobName: "j"})
	want := []flowv1alpha1.RunRef{{Phase: phaseLogic, RunID: 2}, {Phase: phaseSecurity, RunID: 3, JobName: "j"}}
	if !slices.Equal(s.CurrentRuns, want) {
		t.Fatalf("runs = %+v, want %+v", s.CurrentRuns, want)
	}
	if Run(s, "どこにも無い") != nil {
		t.Fatal("a phase with no run in flight has none")
	}
}

// A run settled before it was ever written down — the gap Reconcile's
// recovery closes — is recorded under the phase and number status names,
// fork or not.
func TestARunNotYetWrittenDownIsRecordedUnderStatus(t *testing.T) {
	serial := &flowv1alpha1.TaskStatus{Phase: phaseInvestigate, RunID: 3}
	Advance(serial, spec(), dirOK, transition.Result{Next: phaseReport, Outcome: transition.OutcomeDeclared}, at)
	if h := serial.History[0]; h.Phase != phaseInvestigate || h.RunID != 3 {
		t.Fatalf("recorded %+v, want 調査 run 3", h)
	}

	fork := &flowv1alpha1.TaskStatus{Phase: phasePick, RunID: 2}
	SettleFork(fork, forkFlow(), "logic", transition.ForkResult{Branches: []flowv1alpha1.Phase{phaseLogic}}, at)
	if h := fork.History[0]; h.Phase != phasePick || h.RunID != 2 {
		t.Fatalf("recorded %+v, want the fork as run 2", h)
	}
}

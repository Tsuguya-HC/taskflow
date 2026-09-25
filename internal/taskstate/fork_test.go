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
	phaseStyle    flowv1alpha1.Phase = "style"
	phaseSort     flowv1alpha1.Phase = "仕分け"

	dirStuck = "stuck"
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

// afterFork is a task standing at the fork (run 1, recorded) with branches in
// flight, one run each, numbered from 2 in the order given.
func afterFork(branches ...flowv1alpha1.Phase) *flowv1alpha1.TaskStatus {
	s := atFork()
	SettleFork(s, forkFlow(), "", transition.ForkResult{Branches: branches, Outcome: transition.OutcomeDeclared}, at)
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

// historyPhases is the phase named on each history line, in order.
func historyPhases(s *flowv1alpha1.TaskStatus) []flowv1alpha1.Phase {
	var out []flowv1alpha1.Phase
	for _, h := range s.History {
		out = append(out, h.Phase)
	}
	return out
}

func atJoin() transition.Result {
	return transition.Result{Next: phaseSort, Outcome: transition.OutcomeDeclared}
}

func escalated(detail string) transition.Result {
	return transition.Result{Next: flowv1alpha1.PhaseEscalated, Outcome: transition.OutcomeDeclined, Detail: detail}
}

// Branches settle a few at a time; only once every one of them has reached
// the join does its run start, in the same call that takes the last branch
// out of the runs in flight — there is no status in between where the fork's
// branches have all settled but the join has not yet started.
func TestBranchesMeetAtTheJoin(t *testing.T) {
	s := afterFork(phaseLogic, phaseSecurity)
	flow := forkFlow()

	SettleBranches(s, flow, []SettledBranch{
		{Run: *Run(s, phaseSecurity), Directory: dirDone, Result: atJoin()},
	}, at)
	if got := phasesOf(s.CurrentRuns); !slices.Equal(got, []flowv1alpha1.Phase{phaseLogic}) {
		t.Fatalf("runs in flight = %v, want logic still running", got)
	}
	if s.Phase != phasePick {
		t.Fatalf("phase = %q, want the task still standing at the fork while logic runs", s.Phase)
	}

	SettleBranches(s, flow, []SettledBranch{
		{Run: *Run(s, phaseLogic), Directory: dirDone, Result: atJoin()},
	}, at)
	if s.Phase != phaseSort || s.RunID != 4 {
		t.Fatalf("phase = %q, runID = %d; want the join started as run 4", s.Phase, s.RunID)
	}
	if got := Current(s); got == nil || *got != (flowv1alpha1.RunRef{Phase: phaseSort, RunID: 4}) {
		t.Fatalf("run in flight = %+v, want the join's", got)
	}
	if !slices.Equal(historyPhases(s), []flowv1alpha1.Phase{phasePick, phaseSecurity, phaseLogic}) {
		t.Fatalf("history = %v, want the fork then each branch in the order it settled", historyPhases(s))
	}
}

// Every branch settling at once, all headed for the join, starts it in that
// one call — regardless of the order the caller happened to list them in.
func TestAllBranchesSettlingAtOnceStartTheJoin(t *testing.T) {
	s := afterFork(phaseLogic, phaseSecurity, phaseStyle)
	flow := forkFlow()

	// Listed out of phase order on purpose: the result must not depend on it.
	SettleBranches(s, flow, []SettledBranch{
		{Run: *Run(s, phaseStyle), Directory: "d1", Result: atJoin()},
		{Run: *Run(s, phaseLogic), Directory: "d2", Result: atJoin()},
		{Run: *Run(s, phaseSecurity), Directory: "d3", Result: atJoin()},
	}, at)

	if got := phasesOf(s.CurrentRuns); s.Phase != phaseSort || !slices.Equal(got, []flowv1alpha1.Phase{phaseSort}) {
		t.Fatalf("phase = %q, runs = %v; want only the join's own run in flight", s.Phase, got)
	}
	if !slices.Equal(historyPhases(s), []flowv1alpha1.Phase{phasePick, phaseLogic, phaseSecurity, phaseStyle}) {
		t.Fatalf("history = %v, want each branch recorded in phase order regardless of call order", historyPhases(s))
	}
}

// A branch that escalates while another is still running stops the task: the
// still-running one is cancelled, and the branch that decided the ending is
// the last line in history — after any other branch that settled alongside
// it without deciding anything.
func TestABranchThatEscalatesStopsTheForkWhileAnotherStillRuns(t *testing.T) {
	s := afterFork(phaseLogic, phaseSecurity, phaseStyle)
	flow := forkFlow()
	flow.TTL = ttl(time.Hour, 168*time.Hour)

	// logic reached the join, security escalates, style is still running and
	// is not named in settled at all.
	SettleBranches(s, flow, []SettledBranch{
		{Run: *Run(s, phaseSecurity), Directory: dirStuck, Result: escalated("gave up")},
		{Run: *Run(s, phaseLogic), Directory: dirDone, Result: atJoin()},
	}, at)

	if s.Phase != flowv1alpha1.PhaseEscalated || s.CurrentRuns != nil || s.ExpiresAt == nil {
		t.Fatalf("phase = %q, runs = %+v, expiresAt = %v; want a stopped task", s.Phase, s.CurrentRuns, s.ExpiresAt)
	}
	want := []flowv1alpha1.Phase{phasePick, phaseLogic, phaseStyle, phaseSecurity}
	if !slices.Equal(historyPhases(s), want) {
		t.Fatalf("history = %v, want %v: the branch that reached the join, then the one still running as cancelled, then the one that decided the ending", historyPhases(s), want)
	}
	if h := s.History[2]; h.Phase != phaseStyle || h.Outcome != string(transition.OutcomeCancelled) || h.Directory != "" {
		t.Fatalf("cancelled line = %+v, want style cancelled with no directory", h)
	}
	if last := s.History[3]; last.Outcome != string(transition.OutcomeDeclined) {
		t.Fatalf("last line = %+v, want the branch that decided the ending, with its own outcome", last)
	}
	if !meta.IsStatusConditionFalse(s.Conditions, ConditionReady) {
		t.Fatal("an escalated fork needs a human, and says so")
	}
}

// Two branches escalate in the same reconcile: the one that sorts first by
// phase name is the one taken to have decided the ending, and is recorded
// last; the other is recorded under its own outcome, not Cancelled, since it
// too settled this reconcile rather than being cut short.
func TestTwoBranchesEscalateInTheSameReconcile(t *testing.T) {
	s := afterFork(phaseLogic, phaseSecurity)
	flow := forkFlow()

	SettleBranches(s, flow, []SettledBranch{
		{Run: *Run(s, phaseSecurity), Directory: "stuck-security", Result: escalated("security gave up")},
		{Run: *Run(s, phaseLogic), Directory: "stuck-logic", Result: escalated("logic gave up")},
	}, at)

	want := []flowv1alpha1.Phase{phasePick, phaseSecurity, phaseLogic}
	if !slices.Equal(historyPhases(s), want) {
		t.Fatalf("history = %v, want %v: security (later name) recorded first, logic (earlier name) last as the decider", historyPhases(s), want)
	}
	if h := s.History[1]; h.Outcome != string(transition.OutcomeDeclined) || h.Directory != "stuck-security" {
		t.Fatalf("security's line = %+v, want its own outcome and directory, not Cancelled", h)
	}
	if h := s.History[2]; h.Directory != "stuck-logic" {
		t.Fatalf("logic's line = %+v, want it recorded as the decider with its own directory", h)
	}
}

// The branch that decided the ending can sort before the branches that
// reached the join; it is still recorded last, not first.
func TestDecidingBranchIsRecordedLastEvenWhenItsNameSortsFirst(t *testing.T) {
	s := afterFork(phaseLogic, phaseSecurity, phaseStyle)
	flow := forkFlow()

	SettleBranches(s, flow, []SettledBranch{
		{Run: *Run(s, phaseLogic), Directory: dirStuck, Result: escalated("gave up")},
		{Run: *Run(s, phaseSecurity), Directory: "d1", Result: atJoin()},
		{Run: *Run(s, phaseStyle), Directory: "d2", Result: atJoin()},
	}, at)

	want := []flowv1alpha1.Phase{phasePick, phaseSecurity, phaseStyle, phaseLogic}
	if !slices.Equal(historyPhases(s), want) {
		t.Fatalf("history = %v, want %v: logic sorts first but is still the last line", historyPhases(s), want)
	}
	if s.Phase != flowv1alpha1.PhaseEscalated {
		t.Fatalf("phase = %q, want Escalated", s.Phase)
	}
}

// The last branch settling can itself be the one that does not reach the
// join; the task moves straight to its ending, never sitting at the fork
// with nothing in flight.
func TestLastBranchNotReachingTheJoinStopsTheFork(t *testing.T) {
	s := afterFork(phaseLogic)
	flow := forkFlow()

	SettleBranches(s, flow, []SettledBranch{
		{Run: *Run(s, phaseLogic), Directory: dirStuck, Result: escalated("gave up")},
	}, at)

	if s.Phase != flowv1alpha1.PhaseEscalated || s.CurrentRuns != nil {
		t.Fatalf("phase = %q, runs = %+v; want the task stopped, not standing at the fork with nothing in flight", s.Phase, s.CurrentRuns)
	}
}

// A run named in settled that is not one of the runs in flight — settled
// twice, or named after the join already started — is discarded rather than
// acted on.
func TestSettleBranchesDiscardsARunNotInFlight(t *testing.T) {
	s := afterFork(phaseLogic)
	flow := forkFlow()
	stray := flowv1alpha1.RunRef{Phase: phaseSecurity, RunID: 99}

	SettleBranches(s, flow, []SettledBranch{
		{Run: stray, Directory: dirDone, Result: atJoin()},
	}, at)

	if s.Phase != phasePick || len(s.History) != 1 {
		t.Fatalf("phase = %q, history = %+v; want no effect from a run not in flight", s.Phase, s.History)
	}
	if got := phasesOf(s.CurrentRuns); !slices.Equal(got, []flowv1alpha1.Phase{phaseLogic}) {
		t.Fatalf("runs in flight = %v, want logic untouched", got)
	}
}

// A run named in settled whose RunID does not match the one currentRuns
// holds for that phase — stale or mistaken — is discarded the same way a
// phase with no run at all is: left running, not recorded.
func TestSettleBranchesDiscardsARunWithTheWrongRunID(t *testing.T) {
	s := afterFork(phaseLogic)
	flow := forkFlow()
	wrong := *Run(s, phaseLogic)
	wrong.RunID++

	SettleBranches(s, flow, []SettledBranch{
		{Run: wrong, Directory: dirDone, Result: atJoin()},
	}, at)

	if s.Phase != phasePick || len(s.History) != 1 {
		t.Fatalf("phase = %q, history = %+v; want no effect from a run whose number does not match", s.Phase, s.History)
	}
	if got := phasesOf(s.CurrentRuns); !slices.Equal(got, []flowv1alpha1.Phase{phaseLogic}) {
		t.Fatalf("runs in flight = %v, want logic untouched", got)
	}
}

// A stale entry for a branch must not shadow a later, correct one for the
// same phase: the phase is only taken once a real, current run has actually
// matched it, not on the first entry that merely names it.
func TestSettleBranchesLetsACorrectEntryThroughAfterAStaleOneForTheSamePhase(t *testing.T) {
	s := afterFork(phaseLogic, phaseSecurity)
	flow := forkFlow()
	stale := *Run(s, phaseSecurity)
	stale.RunID++
	correct := *Run(s, phaseSecurity)

	SettleBranches(s, flow, []SettledBranch{
		{Run: stale, Directory: "stale-directory", Result: atJoin()},
		{Run: correct, Directory: dirDone, Result: atJoin()},
	}, at)

	if h := Run(s, phaseSecurity); h != nil {
		t.Fatalf("security still in flight = %+v, want it settled by the correct entry", h)
	}
	found := false
	for _, h := range s.History {
		if h.Phase != phaseSecurity {
			continue
		}
		found = true
		if h.Directory != dirDone {
			t.Fatalf("security's recorded directory = %q, want %q from the correct entry, not the stale one", h.Directory, dirDone)
		}
	}
	if !found {
		t.Fatal("security never recorded: the stale entry must not have shadowed the correct one")
	}
	if got := phasesOf(s.CurrentRuns); !slices.Equal(got, []flowv1alpha1.Phase{phaseLogic}) {
		t.Fatalf("runs in flight = %v, want only logic still running", got)
	}
}

// The same branch named twice in one call settles once: the second entry is
// discarded, so history gets one line for it and Runs counts it once, not
// twice — the caller passing it twice must not let it look like a rework.
func TestSettleBranchesSettlesTheSameBranchOnceEvenIfListedTwice(t *testing.T) {
	s := afterFork(phaseLogic, phaseSecurity)
	flow := forkFlow()

	SettleBranches(s, flow, []SettledBranch{
		{Run: *Run(s, phaseSecurity), Directory: "first", Result: atJoin()},
		{Run: *Run(s, phaseSecurity), Directory: "second", Result: atJoin()},
	}, at)

	n := 0
	for _, h := range s.History {
		if h.Phase == phaseSecurity {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("security appears %d times in history, want exactly 1", n)
	}
	if runs := Runs(s, flow.Bindings); runs[phaseSecurity] != 1 {
		t.Fatalf("Runs[security] = %d, want 1: listing it twice must not look like a rework", runs[phaseSecurity])
	}
	if got := phasesOf(s.CurrentRuns); !slices.Equal(got, []flowv1alpha1.Phase{phaseLogic}) {
		t.Fatalf("runs in flight = %v, want only logic still running", got)
	}
}

// What gets recorded, and what starts the join, is the run currentRuns
// itself holds — not the caller's copy. A caller whose ref disagrees with
// status about how the run was driven is overruled by status.
func TestSettleBranchesRecordsTheRunStatusHoldsNotTheCallersCopy(t *testing.T) {
	s := afterFork(phaseLogic)
	flow := forkFlow()
	SetRun(s, flowv1alpha1.RunRef{Phase: phaseLogic, RunID: Run(s, phaseLogic).RunID, VerdictBox: "box"})
	stale := flowv1alpha1.RunRef{Phase: phaseLogic, RunID: Run(s, phaseLogic).RunID, JobName: "job"}

	SettleBranches(s, flow, []SettledBranch{
		{Run: stale, Directory: dirDone, Result: atJoin()},
	}, at)

	if h := s.History[len(s.History)-1]; h.Runner != flowv1alpha1.RunnerState {
		t.Fatalf("recorded runner = %q, want %q: status said VerdictBox, the caller's stale JobName must not win", h.Runner, flowv1alpha1.RunnerState)
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

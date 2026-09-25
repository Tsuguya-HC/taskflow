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

package transition

import (
	"slices"
	"strings"
	"testing"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// The review ADR-0013 was written for: pick perspectives, review each at once,
// sort the findings. tests runs whatever was picked.
const (
	phasePick     flowv1alpha1.Phase = "観点出し"
	phaseSecurity flowv1alpha1.Phase = "security"
	phaseLogic    flowv1alpha1.Phase = "logic"
	phaseTests    flowv1alpha1.Phase = "tests"
	phaseSort     flowv1alpha1.Phase = "仕分け"
)

const (
	dirAgain    = "again"
	dirDup      = "dup"
	dirBroken   = "broken"
	dirSecurity = "security"
	dirLogic    = "logic"
	dirStuck    = "stuck"
)

func forkFlow() map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding {
	toSort := func() map[flowv1alpha1.Phase]string {
		return map[flowv1alpha1.Phase]string{phaseSort: "done", flowv1alpha1.PhaseEscalated: dirStuck}
	}
	return map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		phasePick: {
			Handler: "pick",
			Next: map[flowv1alpha1.Phase]string{
				phaseSecurity: dirSecurity, phaseLogic: dirLogic, flowv1alpha1.PhaseEscalated: dirStuck,
			},
			Join: &flowv1alpha1.JoinSpec{Phase: phaseSort, Always: []flowv1alpha1.Phase{phaseTests}},
		},
		phaseSecurity: {Handler: "review-security", Next: toSort()},
		phaseLogic:    {Handler: "review-logic", Next: toSort()},
		phaseTests:    {Handler: "review-tests", Next: toSort()},
		phaseSort:     {Handler: "sort", Next: map[flowv1alpha1.Phase]string{phaseDone: "ok", phasePick: dirAgain}},
	}
}

func fork(dir string) ForkResult {
	return Fork(Input{Bindings: forkFlow(), Phase: phasePick, Directory: dir, Runs: ranOnce(phasePick), MaxRuns: 2})
}

func TestBranchesAreTheChoicesAndTheAlwaysOnes(t *testing.T) {
	want := []flowv1alpha1.Phase{phaseLogic, phaseSecurity, phaseTests}
	if got := Branches(forkFlow(), phasePick); !slices.Equal(got, want) {
		t.Fatalf("branches = %v, want %v", got, want)
	}
	if got := Branches(forkFlow(), phaseSort); got != nil {
		t.Fatalf("branches of a phase that does not fork = %v, want none", got)
	}
}

// Every chosen branch starts, and always starts beside them.
func TestAForkStartsWhatItChoseAndWhatAlwaysRuns(t *testing.T) {
	got := fork(dirSecurity)
	if want := []flowv1alpha1.Phase{phaseSecurity, phaseTests}; !slices.Equal(got.Branches, want) || got.Next != "" {
		t.Fatalf("got %+v, want branches %v and no stop", got, want)
	}
	if got.Outcome != OutcomeDeclared {
		t.Fatalf("outcome = %q, want Declared", got.Outcome)
	}

	both := fork(dirLogic + "/" + dirSecurity)
	if want := []flowv1alpha1.Phase{phaseLogic, phaseSecurity, phaseTests}; !slices.Equal(both.Branches, want) {
		t.Fatalf("branches = %v, want %v", both.Branches, want)
	}
}

func TestAForkThatStops(t *testing.T) {
	cases := map[string]struct {
		dir     string
		next    flowv1alpha1.Phase
		outcome Outcome
	}{
		"nothing written":              {dir: "", next: flowv1alpha1.PhaseEscalated, outcome: OutcomeNoAnswer},
		"a word nothing declares":      {dir: "maybe", next: flowv1alpha1.PhaseEscalated, outcome: OutcomeNoAnswer},
		"escalating on purpose":        {dir: dirStuck, next: flowv1alpha1.PhaseEscalated, outcome: OutcomeDeclined},
		"escalating beside a branch":   {dir: dirSecurity + "/" + dirStuck, next: flowv1alpha1.PhaseEscalated, outcome: OutcomeNoAnswer},
		"a branch at its run limit":    {dir: dirLogic, next: flowv1alpha1.PhaseEscalated, outcome: OutcomeRunLimitReached},
		"one word, two statuses":       {dir: dirDup, next: flowv1alpha1.PhaseFailed, outcome: OutcomeStructural},
		"Failed named as a status":     {dir: dirBroken, next: flowv1alpha1.PhaseFailed, outcome: OutcomeStructural},
		"an edge straight to the join": {dir: "none", next: flowv1alpha1.PhaseFailed, outcome: OutcomeStructural},
		"a branch nothing binds":       {dir: "later", next: flowv1alpha1.PhaseFailed, outcome: OutcomeStructural},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			b := forkFlow()
			b[phasePick].Next["重複"] = dirDup
			b[phasePick].Next["二重"] = dirDup
			b[phasePick].Next[flowv1alpha1.PhaseFailed] = dirBroken
			b[phasePick].Next[phaseSort] = "none"
			b[phasePick].Next["未実装"] = "later"
			runs := ranOnce(phasePick)
			runs[phaseLogic] = 2
			got := Fork(Input{Bindings: b, Phase: phasePick, Directory: c.dir, Runs: runs, MaxRuns: 2})
			if got.Next != c.next || got.Outcome != c.outcome || len(got.Branches) != 0 {
				t.Fatalf("got %+v, want %s/%s with nothing started", got, c.next, c.outcome)
			}
		})
	}
}

// A fork is one decision: an always branch at its limit stops it as surely as
// a chosen one would, rather than starting the rest.
func TestAnAlwaysBranchAtItsLimitStopsTheFork(t *testing.T) {
	runs := ranOnce(phasePick)
	runs[phaseTests] = 2
	got := Fork(Input{Bindings: forkFlow(), Phase: phasePick, Directory: dirSecurity, Runs: runs, MaxRuns: 2})
	if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeRunLimitReached || !strings.Contains(got.Detail, string(phaseTests)) {
		t.Fatalf("got %+v, want Escalated/RunLimitReached naming tests", got)
	}
}

// Going round the fork again is recorded as a rework, the same as any edge
// back to a phase that has run.
func TestAForkRoundAgainIsARework(t *testing.T) {
	runs := ranOnce(phasePick, phaseSecurity, phaseTests)
	got := Fork(Input{Bindings: forkFlow(), Phase: phasePick, Directory: dirSecurity, Runs: runs, MaxRuns: 2})
	if got.Outcome != OutcomeRework || len(got.Branches) != 2 {
		t.Fatalf("got %+v, want the branches started again as a rework", got)
	}
}

func TestAForkThatIsNotOne(t *testing.T) {
	for name, in := range map[string]Input{
		"a phase with no binding": {Bindings: forkFlow(), Phase: "どこにも無い", Directory: dirSecurity, MaxRuns: 2},
		"a phase with no join":    {Bindings: forkFlow(), Phase: phaseSort, Directory: "ok", MaxRuns: 2},
		"a join nobody binds": func() Input {
			b := forkFlow()
			delete(b, phaseSort)
			return Input{Bindings: b, Phase: phasePick, Directory: dirSecurity, MaxRuns: 2}
		}(),
		"a limit below one": {Bindings: forkFlow(), Phase: phasePick, Directory: dirSecurity},
	} {
		t.Run(name, func(t *testing.T) {
			if got := Fork(in); got.Next != flowv1alpha1.PhaseFailed || got.Outcome != OutcomeStructural {
				t.Fatalf("got %+v, want Failed/Structural", got)
			}
		})
	}
}

// StopsAFork is false for exactly the three outcomes that mean a branch's
// line is not what decided anything: Declared and Rework both mean it
// reached the join, and Cancelled means it was stopped before it could
// answer at all. Every other outcome stopped the fork.
func TestOutcomeStopsAFork(t *testing.T) {
	cases := map[Outcome]bool{
		OutcomeDeclared:        false,
		OutcomeRework:          false,
		OutcomeCancelled:       false,
		OutcomeRunLimitReached: true,
		OutcomeNoAnswer:        true,
		OutcomeDeclined:        true,
		OutcomeStructural:      true,
	}
	for outcome, want := range cases {
		if got := outcome.StopsAFork(); got != want {
			t.Errorf("%s.StopsAFork() = %v, want %v", outcome, got, want)
		}
	}
}

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
	"testing"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
)

// A flow named in the author's own words, to keep honest the claim that the
// framework does not supply the vocabulary.
const (
	phaseInvestigate flowv1alpha1.Phase = "調査"
	phaseReport      flowv1alpha1.Phase = "報告"
	phaseDone        flowv1alpha1.Phase = "おわり"
)

func cnpCheck() map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding {
	return map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		phaseInvestigate: {
			Handler: "cnp-reader",
			Next: map[flowv1alpha1.Phase]string{
				phaseReport:      dirOK,
				phaseInvestigate: dirMore,
			},
		},
		phaseReport: {
			Handler: "discord",
			Next:    map[flowv1alpha1.Phase]string{phaseDone: dirSent},
		},
	}
}

// The directories the example flow declares.
const (
	dirOK   = "ok"
	dirMore = "more"
	dirSent = "sent"
)

func visited(phases ...flowv1alpha1.Phase) map[flowv1alpha1.Phase]bool {
	v := map[flowv1alpha1.Phase]bool{}
	for _, p := range phases {
		v[p] = true
	}
	return v
}

func TestDeclaredEdges(t *testing.T) {
	got := Next(Input{Bindings: cnpCheck(), Phase: phaseInvestigate, Directory: dirOK, Visited: visited(phaseInvestigate), Budget: 2})
	if got.Next != phaseReport || got.Outcome != OutcomeDeclared {
		t.Fatalf("got %q/%q, want 報告/Declared (%s)", got.Next, got.Outcome, got.Detail)
	}
	if got.Budget != 2 {
		t.Fatalf("budget = %d, want it untouched", got.Budget)
	}
}

// A status nobody bound is where the flow ends. Nothing declares it terminal,
// and there is no reserved name for success — "おわり" is just a status with
// nowhere to go.
func TestUnboundStatusIsWhereItStops(t *testing.T) {
	b := cnpCheck()
	if !IsTerminal(b, phaseDone) {
		t.Fatal("a status with no binding is the end of the flow")
	}
	if IsTerminal(b, phaseReport) {
		t.Fatal("a bound status is not the end")
	}
	for _, r := range flowv1alpha1.ReservedPhases {
		if !IsTerminal(b, r) {
			t.Fatalf("%q is always the end", r)
		}
	}
}

// The declaration is also the mount list: these are the only directories that
// will exist, so they are the only answers a handler can give.
func TestDirectoriesComeFromTheDeclaration(t *testing.T) {
	dirs := Directories(cnpCheck(), phaseInvestigate)
	slices.Sort(dirs)
	if !slices.Equal(dirs, []string{dirMore, dirOK}) {
		t.Fatalf("directories = %v, want [more ok]", dirs)
	}
	if Directories(cnpCheck(), "見たことない") != nil {
		t.Fatal("an unbound phase has no directories")
	}
}

func TestNoSingleAnswerEscalates(t *testing.T) {
	for _, why := range []string{"nothing was written", "two directories were written", "the run timed out"} {
		t.Run(why, func(t *testing.T) {
			got := Next(Input{Bindings: cnpCheck(), Phase: phaseInvestigate, NoAnswer: why, Visited: visited(phaseInvestigate), Budget: 2})
			if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeNoAnswer {
				t.Fatalf("got %q/%q, want Escalated/NoAnswer", got.Next, got.Outcome)
			}
			if got.Detail != why {
				t.Fatalf("detail = %q, want the reason carried through", got.Detail)
			}
		})
	}
}

// The fail-closed path (P6) needs a message even when the caller did not
// bother to say why — an empty NoAnswer must not become an empty Detail.
func TestNoSingleAnswerWithoutReasonGetsADefaultMessage(t *testing.T) {
	got := Next(Input{Bindings: cnpCheck(), Phase: phaseInvestigate, Visited: visited(phaseInvestigate), Budget: 2})
	if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeNoAnswer {
		t.Fatalf("got %q/%q, want Escalated/NoAnswer", got.Next, got.Outcome)
	}
	if got.Detail != "the run produced no single answer" {
		t.Fatalf("detail = %q, want the default message", got.Detail)
	}
}

// The handler cannot invent this — the directory would not exist — but a flow
// edited under a running task can leave one behind.
func TestUndeclaredDirectoryEscalates(t *testing.T) {
	got := Next(Input{Bindings: cnpCheck(), Phase: phaseInvestigate, Directory: "looks-fine", Visited: visited(phaseInvestigate), Budget: 2})
	if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeNoAnswer {
		t.Fatalf("got %q/%q, want Escalated/NoAnswer", got.Next, got.Outcome)
	}
}

func TestBrokenFlowFails(t *testing.T) {
	t.Run("a phase with no binding", func(t *testing.T) {
		got := Next(Input{Bindings: cnpCheck(), Phase: "存在しない", Directory: dirOK, Budget: 2})
		if got.Next != flowv1alpha1.PhaseFailed || got.Outcome != OutcomeStructural {
			t.Fatalf("got %q/%q, want Failed/Structural", got.Next, got.Outcome)
		}
	})

	// Creation refuses this; a flow edited afterwards can still carry it, and
	// picking one of the two would be worse than stopping.
	t.Run("two statuses sharing a directory", func(t *testing.T) {
		b := cnpCheck()
		b[phaseInvestigate].Next["中止"] = dirOK
		got := Next(Input{Bindings: b, Phase: phaseInvestigate, Directory: dirOK, Visited: visited(phaseInvestigate), Budget: 2})
		if got.Next != flowv1alpha1.PhaseFailed || got.Outcome != OutcomeStructural {
			t.Fatalf("got %q/%q, want Failed/Structural", got.Next, got.Outcome)
		}
	})
}

func TestReworkSpendsBudget(t *testing.T) {
	got := Next(Input{Bindings: cnpCheck(), Phase: phaseInvestigate, Directory: dirMore, Visited: visited(phaseInvestigate), Budget: 2})
	if got.Next != phaseInvestigate || got.Outcome != OutcomeRework {
		t.Fatalf("got %q/%q, want 調査/Rework", got.Next, got.Outcome)
	}
	if got.Budget != 1 {
		t.Fatalf("budget = %d, want 1", got.Budget)
	}
}

func TestReworkWithoutBudgetEscalates(t *testing.T) {
	got := Next(Input{Bindings: cnpCheck(), Phase: phaseInvestigate, Directory: dirMore, Visited: visited(phaseInvestigate), Budget: 0})
	if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeBudgetExhausted {
		t.Fatalf("got %q/%q, want Escalated/BudgetExhausted", got.Next, got.Outcome)
	}
}

// The first version of this rule decremented unconditionally, which made
// reworkBudget: 0 mean "cannot run at all" instead of "never goes back".
func TestForwardEdgesIgnoreBudget(t *testing.T) {
	got := Next(Input{Bindings: cnpCheck(), Phase: phaseInvestigate, Directory: dirOK, Visited: visited(phaseInvestigate), Budget: 0})
	if got.Next != phaseReport || got.Outcome != OutcomeDeclared {
		t.Fatalf("got %q/%q, want 報告/Declared with no budget", got.Next, got.Outcome)
	}
	if got.Budget != 0 {
		t.Fatalf("budget = %d, want it untouched at 0 — a forward edge must not spend it", got.Budget)
	}
}

// Budget only ever decreases, so a cycle cannot run forever.
func TestCycleTerminates(t *testing.T) {
	budget := int32(2)
	phase := phaseInvestigate
	seen := visited(phaseInvestigate)

	for range 50 {
		got := Next(Input{Bindings: cnpCheck(), Phase: phase, Directory: dirMore, Visited: seen, Budget: budget})
		budget = got.Budget
		phase = got.Next
		seen[phase] = true
		if IsTerminal(cnpCheck(), phase) {
			if phase != flowv1alpha1.PhaseEscalated {
				t.Fatalf("terminated at %q, want Escalated once the budget ran out", phase)
			}
			if budget != 0 {
				t.Fatalf("ended with budget %d, want 0", budget)
			}
			return
		}
	}
	t.Fatal("did not terminate")
}

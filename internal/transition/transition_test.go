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
	"testing"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
)

// The flow from design.md §16, used as the ordinary case.
func cnpCheck() map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding {
	return map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		flowv1alpha1.PhasePlanning: {
			Handler:  "claude-planner",
			Outcomes: map[flowv1alpha1.Verdict]flowv1alpha1.Phase{flowv1alpha1.VerdictPass: flowv1alpha1.PhaseReview},
		},
		flowv1alpha1.PhaseReview: {
			Handler: "claude-reviewer",
			Outcomes: map[flowv1alpha1.Verdict]flowv1alpha1.Phase{
				flowv1alpha1.VerdictPass:     flowv1alpha1.PhaseDone,
				flowv1alpha1.VerdictRework:   flowv1alpha1.PhasePlanning,
				flowv1alpha1.VerdictEscalate: flowv1alpha1.PhaseEscalated,
			},
		},
	}
}

func visited(phases ...flowv1alpha1.Phase) map[flowv1alpha1.Phase]bool {
	v := map[flowv1alpha1.Phase]bool{}
	for _, p := range phases {
		v[p] = true
	}
	return v
}

func TestDeclaredEdges(t *testing.T) {
	cases := []struct {
		name    string
		phase   flowv1alpha1.Phase
		verdict flowv1alpha1.Verdict
		visited map[flowv1alpha1.Phase]bool
		want    flowv1alpha1.Phase
	}{
		{"planning passes to review", flowv1alpha1.PhasePlanning, flowv1alpha1.VerdictPass, visited(flowv1alpha1.PhasePlanning), flowv1alpha1.PhaseReview},
		{"review passes to done", flowv1alpha1.PhaseReview, flowv1alpha1.VerdictPass, visited(flowv1alpha1.PhasePlanning, flowv1alpha1.PhaseReview), flowv1alpha1.PhaseDone},
		{"review escalates", flowv1alpha1.PhaseReview, flowv1alpha1.VerdictEscalate, visited(flowv1alpha1.PhasePlanning, flowv1alpha1.PhaseReview), flowv1alpha1.PhaseEscalated},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Next(Input{Bindings: cnpCheck(), Phase: tc.phase, Verdict: tc.verdict, Visited: tc.visited, Budget: 2})
			if got.Next != tc.want {
				t.Fatalf("next = %q, want %q (%s)", got.Next, tc.want, got.Detail)
			}
			if got.Outcome != OutcomeDeclared {
				t.Fatalf("outcome = %q, want %q", got.Outcome, OutcomeDeclared)
			}
			if got.Budget != 2 {
				t.Fatalf("budget = %d, want it untouched at 2", got.Budget)
			}
		})
	}
}

// The four rows the design builds into the controller. Each is asserted
// against a flow that does not mention them, and — where a flow could try to
// say otherwise — against one that does.
func TestFailureSystemIsBuiltIn(t *testing.T) {
	t.Run("unknown verdict escalates", func(t *testing.T) {
		got := Next(Input{Bindings: cnpCheck(), Phase: flowv1alpha1.PhaseReview, Verdict: "looks-fine-to-me", Visited: visited(flowv1alpha1.PhaseReview), Budget: 2})
		if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeUnknownVerdict {
			t.Fatalf("got %q/%q, want Escalated/UnknownVerdict", got.Next, got.Outcome)
		}
	})

	t.Run("timeout escalates", func(t *testing.T) {
		got := Next(Input{Bindings: cnpCheck(), Phase: flowv1alpha1.PhaseReview, Verdict: flowv1alpha1.VerdictTimeout, Visited: visited(flowv1alpha1.PhaseReview), Budget: 2})
		if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeReserved {
			t.Fatalf("got %q/%q, want Escalated/Reserved", got.Next, got.Outcome)
		}
	})

	t.Run("indeterminate escalates", func(t *testing.T) {
		got := Next(Input{Bindings: cnpCheck(), Phase: flowv1alpha1.PhaseReview, Verdict: flowv1alpha1.VerdictIndeterminate, Visited: visited(flowv1alpha1.PhaseReview), Budget: 2})
		if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeReserved {
			t.Fatalf("got %q/%q, want Escalated/Reserved", got.Next, got.Outcome)
		}
	})

	t.Run("a structural break fails rather than escalating", func(t *testing.T) {
		got := Next(Input{Bindings: cnpCheck(), Phase: flowv1alpha1.PhaseImplementing, Verdict: flowv1alpha1.VerdictPass, Budget: 2})
		if got.Next != flowv1alpha1.PhaseFailed || got.Outcome != OutcomeStructural {
			t.Fatalf("got %q/%q, want Failed/Structural", got.Next, got.Outcome)
		}
	})

	t.Run("a destination that can never run fails", func(t *testing.T) {
		b := cnpCheck()
		b[flowv1alpha1.PhaseReview].Outcomes[flowv1alpha1.VerdictRework] = flowv1alpha1.PhaseImplementing // never bound
		got := Next(Input{Bindings: b, Phase: flowv1alpha1.PhaseReview, Verdict: flowv1alpha1.VerdictRework, Visited: visited(flowv1alpha1.PhaseReview), Budget: 2})
		if got.Next != flowv1alpha1.PhaseFailed || got.Outcome != OutcomeStructural {
			t.Fatalf("got %q/%q, want Failed/Structural", got.Next, got.Outcome)
		}
	})
}

// P6 is the rule this design will not bend, so it is tested against a flow
// actively trying to break it. Admission rejects such a flow, but the
// transition must not be the thing relying on admission having run.
func TestReservedVerdictsCannotBeRedirected(t *testing.T) {
	for _, reserved := range flowv1alpha1.ReservedVerdicts {
		t.Run(string(reserved), func(t *testing.T) {
			b := cnpCheck()
			b[flowv1alpha1.PhaseReview].Outcomes[reserved] = flowv1alpha1.PhaseDone // a flow claiming "unknown means shipped"
			got := Next(Input{Bindings: b, Phase: flowv1alpha1.PhaseReview, Verdict: reserved, Visited: visited(flowv1alpha1.PhaseReview), Budget: 2})
			if got.Next != flowv1alpha1.PhaseEscalated {
				t.Fatalf("a flow redirected %q to %q; the controller must still escalate", reserved, got.Next)
			}
		})
	}
}

func TestReworkSpendsBudget(t *testing.T) {
	in := Input{
		Bindings: cnpCheck(),
		Phase:    flowv1alpha1.PhaseReview,
		Verdict:  flowv1alpha1.VerdictRework,
		Visited:  visited(flowv1alpha1.PhasePlanning, flowv1alpha1.PhaseReview),
		Budget:   2,
	}
	got := Next(in)
	if got.Next != flowv1alpha1.PhasePlanning || got.Outcome != OutcomeRework {
		t.Fatalf("got %q/%q, want Planning/Rework", got.Next, got.Outcome)
	}
	if got.Budget != 1 {
		t.Fatalf("budget = %d, want 1", got.Budget)
	}
}

func TestReworkWithoutBudgetEscalates(t *testing.T) {
	got := Next(Input{
		Bindings: cnpCheck(),
		Phase:    flowv1alpha1.PhaseReview,
		Verdict:  flowv1alpha1.VerdictRework,
		Visited:  visited(flowv1alpha1.PhasePlanning, flowv1alpha1.PhaseReview),
		Budget:   0,
	})
	if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeBudgetExhausted {
		t.Fatalf("got %q/%q, want Escalated/BudgetExhausted", got.Next, got.Outcome)
	}
	if got.Budget != 0 {
		t.Fatalf("budget = %d, want it to stay 0", got.Budget)
	}
}

// The first version of this rule decremented unconditionally, which made
// reworkBudget: 0 mean "cannot run at all" instead of "never goes back".
func TestForwardEdgesIgnoreBudget(t *testing.T) {
	got := Next(Input{
		Bindings: cnpCheck(),
		Phase:    flowv1alpha1.PhasePlanning,
		Verdict:  flowv1alpha1.VerdictPass,
		Visited:  visited(flowv1alpha1.PhasePlanning),
		Budget:   0,
	})
	if got.Next != flowv1alpha1.PhaseReview {
		t.Fatalf("next = %q, want Review even with no budget (%s)", got.Next, got.Detail)
	}
	if got.Outcome != OutcomeDeclared {
		t.Fatalf("outcome = %q, want Declared", got.Outcome)
	}
}

// A self-loop is a rework: the phase being left has already been visited.
// Nothing annotates it as one, which is the point — an annotation can be
// forgotten and this cannot.
func TestSelfLoopIsRework(t *testing.T) {
	b := map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		flowv1alpha1.PhaseReview: {
			Handler: "reviewer",
			Outcomes: map[flowv1alpha1.Verdict]flowv1alpha1.Phase{
				flowv1alpha1.VerdictRework: flowv1alpha1.PhaseReview,
				flowv1alpha1.VerdictPass:   flowv1alpha1.PhaseDone,
			},
		},
	}
	got := Next(Input{Bindings: b, Phase: flowv1alpha1.PhaseReview, Verdict: flowv1alpha1.VerdictRework, Visited: visited(flowv1alpha1.PhaseReview), Budget: 1})
	if got.Outcome != OutcomeRework || got.Budget != 0 {
		t.Fatalf("got %q with budget %d, want Rework spending to 0", got.Outcome, got.Budget)
	}
}

// Budget only ever decreases, so a cycle cannot run forever. Driving the §16
// flow round the loop shows it reaching a human rather than spinning.
func TestCycleTerminates(t *testing.T) {
	budget := int32(2)
	phase := flowv1alpha1.PhaseReview
	seen := visited(flowv1alpha1.PhasePlanning, flowv1alpha1.PhaseReview)

	const limit = 50 // if the loop is not decreasing, stop rather than hang
	for range limit {
		verdict := flowv1alpha1.VerdictRework
		if phase == flowv1alpha1.PhasePlanning {
			verdict = flowv1alpha1.VerdictPass // planner always hands back to review
		}
		got := Next(Input{Bindings: cnpCheck(), Phase: phase, Verdict: verdict, Visited: seen, Budget: budget})
		budget = got.Budget
		phase = got.Next
		seen[phase] = true
		if phase.IsTerminal() {
			if phase != flowv1alpha1.PhaseEscalated {
				t.Fatalf("terminated at %q, want Escalated once the budget ran out", phase)
			}
			if budget != 0 {
				t.Fatalf("ended with budget %d, want 0", budget)
			}
			return
		}
	}
	t.Fatalf("did not terminate within %d transitions", limit)
}

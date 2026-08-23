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

// Package transition decides where a task goes next.
//
// It is a pure function over values: no client, no context, no clock. That is
// deliberate — this is the one part of the controller whose correctness can be
// established without running anything, and the reason the design refused an
// expression language. An outcomes table can be checked exhaustively; a
// when: string can only be tried.
package transition

import (
	"slices"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
)

// Outcome is why a task moved, alongside where to.
type Outcome string

const (
	// OutcomeDeclared followed an edge the flow declared.
	OutcomeDeclared Outcome = "Declared"
	// OutcomeRework followed a declared edge back to a phase already visited,
	// spending a unit of budget.
	OutcomeRework Outcome = "Rework"
	// OutcomeBudgetExhausted wanted to rework but had nothing left to spend.
	OutcomeBudgetExhausted Outcome = "BudgetExhausted"
	// OutcomeReserved is a verdict the controller owns: timeout or
	// indeterminate. Never overridable by a flow.
	OutcomeReserved Outcome = "Reserved"
	// OutcomeUnknownVerdict is a token the binding does not map. Unknown is
	// not approval.
	OutcomeUnknownVerdict Outcome = "UnknownVerdict"
	// OutcomeStructural is a broken graph rather than a bad judgement. It is
	// not repaired and not handed to a human as work — it is a spec defect.
	OutcomeStructural Outcome = "Structural"
)

// Input is everything the decision depends on. Passing the visited set and the
// budget in, rather than reading them from a status, keeps the function
// testable and keeps the caller honest about what it is asserting.
type Input struct {
	// Bindings is the flow's topology.
	Bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding
	// Phase the task is leaving.
	Phase flowv1alpha1.Phase
	// Verdict it left with.
	Verdict flowv1alpha1.Verdict
	// Visited is every phase this task has already run. A destination in here
	// makes the edge a rework, with no annotation required and no way to
	// forget one.
	Visited map[flowv1alpha1.Phase]bool
	// Budget remaining for reworks.
	Budget int32
}

// Result is where the task goes and what it cost.
type Result struct {
	Next flowv1alpha1.Phase
	// Outcome explains the move; it is what gets recorded and surfaced.
	Outcome Outcome
	// Budget after the move. Only ever decreases.
	Budget int32
	// Detail is a short human-facing reason, for a status condition.
	Detail string
}

// Next decides the phase to run after Phase reported Verdict.
//
// The failure system is built in and cannot be declared away:
//
//	a phase with no binding          -> Failed     (the graph is broken)
//	a destination that cannot run    -> Failed     (likewise)
//	timeout or indeterminate         -> Escalated  (the controller owns these)
//	a verdict the binding omits      -> Escalated  (unknown is not approval)
//	a rework with no budget left     -> Escalated
//
// Every other move follows the flow's own table.
func Next(in Input) Result {
	binding, bound := in.Bindings[in.Phase]
	if !bound {
		// Reachable when a flow is edited under a running task, which the
		// design treats as a different task rather than something to repair.
		return Result{
			Next:    flowv1alpha1.PhaseFailed,
			Outcome: OutcomeStructural,
			Budget:  in.Budget,
			Detail:  "phase " + string(in.Phase) + " has no binding in this flow",
		}
	}

	// Reserved verdicts are checked before the table, not after, so that a
	// binding which somehow carries one — admission rejects them, but this
	// must not depend on admission having run — cannot redirect it.
	if slices.Contains(flowv1alpha1.ReservedVerdicts, in.Verdict) {
		return Result{
			Next:    flowv1alpha1.PhaseEscalated,
			Outcome: OutcomeReserved,
			Budget:  in.Budget,
			Detail:  "verdict " + string(in.Verdict) + " is decided by the controller and routes to a human",
		}
	}

	dest, declared := binding.Outcomes[in.Verdict]
	if !declared {
		return Result{
			Next:    flowv1alpha1.PhaseEscalated,
			Outcome: OutcomeUnknownVerdict,
			Budget:  in.Budget,
			Detail:  "no outcome declared for verdict " + string(in.Verdict),
		}
	}

	// A destination that is neither terminal nor bound would stall at the next
	// step. Creation-time validation rejects this; catching it here as well
	// means a flow edited afterwards fails loudly instead of hanging.
	if _, destBound := in.Bindings[dest]; !destBound && !dest.IsTerminal() {
		return Result{
			Next:    flowv1alpha1.PhaseFailed,
			Outcome: OutcomeStructural,
			Budget:  in.Budget,
			Detail:  "destination " + string(dest) + " is neither terminal nor bound",
		}
	}

	if in.Visited[dest] {
		if in.Budget <= 0 {
			return Result{
				Next:    flowv1alpha1.PhaseEscalated,
				Outcome: OutcomeBudgetExhausted,
				Budget:  in.Budget,
				Detail:  "rework to " + string(dest) + " with no budget left",
			}
		}
		return Result{
			Next:    dest,
			Outcome: OutcomeRework,
			Budget:  in.Budget - 1,
			Detail:  "rework to already-visited " + string(dest),
		}
	}

	return Result{
		Next:    dest,
		Outcome: OutcomeDeclared,
		Budget:  in.Budget,
		Detail:  "declared edge to " + string(dest),
	}
}

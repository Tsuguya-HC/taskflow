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
// expression language. A table can be checked exhaustively; a when: string can
// only be tried.
package transition

import (
	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
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
	// OutcomeNoAnswer is a run that produced no single directory — none, or
	// several, or it ran out of time. Not an approval; a human looks at it.
	OutcomeNoAnswer Outcome = "NoAnswer"
	// OutcomeStructural is a broken flow rather than a bad judgement. It is
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
	// Directory the handler wrote into. Empty means the run gave no single
	// answer; NoAnswer then says why, for the record a human reads.
	Directory string
	// NoAnswer explains an empty Directory: nothing written, more than one
	// written, timed out.
	NoAnswer string
	// Visited is every phase this task has already run. A destination in here
	// makes the edge a rework, with no annotation required and none to forget.
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

// Next decides the phase to run after Phase finished writing into Directory.
//
// The framework's own answers are built in and cannot be declared away:
//
//	a phase with no binding      -> Failed     (the flow is broken)
//	two statuses, one directory  -> Failed     (likewise; undecidable)
//	no single directory written  -> Escalated  (nothing was decided)
//	a directory the flow omits   -> Escalated  (it cannot explain what arrived)
//	a rework with no budget left -> Escalated
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

	if in.Directory == "" {
		detail := in.NoAnswer
		if detail == "" {
			detail = "the run produced no single answer"
		}
		return Result{
			Next:    flowv1alpha1.PhaseEscalated,
			Outcome: OutcomeNoAnswer,
			Budget:  in.Budget,
			Detail:  detail,
		}
	}

	// The map is keyed by destination, so finding the destination means
	// scanning it. Two statuses sharing a directory is refused at creation;
	// if a flow edited afterwards still has it, the answer is undecidable and
	// guessing between them would be worse than stopping.
	var dest flowv1alpha1.Phase
	found := 0
	for phase, dir := range binding.Next {
		if dir == in.Directory {
			dest = phase
			found++
		}
	}
	switch {
	case found > 1:
		return Result{
			Next:    flowv1alpha1.PhaseFailed,
			Outcome: OutcomeStructural,
			Budget:  in.Budget,
			Detail:  "directory " + in.Directory + " selects more than one status",
		}
	case found == 0:
		return Result{
			Next:    flowv1alpha1.PhaseEscalated,
			Outcome: OutcomeNoAnswer,
			Budget:  in.Budget,
			Detail:  "no status is declared for directory " + in.Directory,
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

// Directories is the set the framework creates for a run of phase. Those are
// the only ones that will exist, so they are also the only answers the handler
// can give.
func Directories(bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding, phase flowv1alpha1.Phase) []string {
	binding, bound := bindings[phase]
	if !bound {
		return nil
	}
	dirs := make([]string, 0, len(binding.Next))
	for _, dir := range binding.Next {
		dirs = append(dirs, dir)
	}
	return dirs
}

// IsTerminal reports whether a task that reached phase has stopped: a status
// with no binding is where the flow ends, and the framework's own two answers
// always end it.
func IsTerminal(bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding, phase flowv1alpha1.Phase) bool {
	if phase.IsReserved() {
		return true
	}
	_, bound := bindings[phase]
	return !bound
}

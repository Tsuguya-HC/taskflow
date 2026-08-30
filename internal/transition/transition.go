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
	// OutcomeDeclined followed a declared edge to Escalated: the flow gave
	// this phase a directory meaning "I will not decide this", and the run
	// wrote into it. It ends the same way NoAnswer does — a human takes it
	// from here — but it is the opposite kind of event. Silence is what a
	// run that crashed, ran out of turns or said nothing leaves behind;
	// this is a run that finished, chose, and left a report saying why.
	OutcomeDeclined Outcome = "Declined"
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
//	a phase with no binding        -> Failed     (the flow is broken)
//	two statuses, one directory    -> Failed     (likewise; undecidable)
//	Failed named as a destination  -> Failed     (likewise; not an answer)
//	no single directory written    -> Escalated  (nothing was decided)
//	a directory the flow omits     -> Escalated  (it cannot explain what arrived)
//	a rework with no budget left   -> Escalated
//
// Every other move follows the flow's own table — including an edge the flow
// declared to Escalated, which is the one reserved name it may name as a
// destination.
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

	// Escalated is the one reserved name a flow may send work to. Declaring
	// it gives the phase a directory for "I will not decide this", so a run
	// that cannot conclude has somewhere to say so — and to leave a report —
	// instead of only being able to fall silent. What it cannot do is bind
	// Escalated to a handler, which is the thing the reservation is actually
	// protecting: no answer must never be one line away from the success
	// path (§5). This is a destination, so that concern does not arise.
	//
	// It skips the visited and budget questions below because Escalated is
	// terminal on its own say-so: there is no run after it to spend on.
	if dest == flowv1alpha1.PhaseEscalated {
		return Result{
			Next:    flowv1alpha1.PhaseEscalated,
			Outcome: OutcomeDeclined,
			Budget:  in.Budget,
			Detail:  "escalated on purpose, by writing into " + in.Directory,
		}
	}

	// Failed is not. It means the definition is broken, which is never
	// something the work gets to conclude, so a flow naming it as a
	// destination is itself the defect. Admission is meant to refuse that
	// flow at creation (#17); until it does — and for a flow edited after a
	// task started — the task stops here rather than reaching Failed under
	// an outcome that would read like a declared edge.
	if dest == flowv1alpha1.PhaseFailed {
		return Result{
			Next:    flowv1alpha1.PhaseFailed,
			Outcome: OutcomeStructural,
			Budget:  in.Budget,
			Detail:  "directory " + in.Directory + " is declared to reach Failed, which is the framework's own",
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

// Ending is what a task's stopping place means to whoever is watching. It is
// a different question from which phase it stopped at, and one the framework
// cannot answer alone: 失敗 and おわり are both just names, and only the flow
// knows which of them is bad news.
type Ending string

const (
	// EndingRunning is what a phase that has not stopped returns. Not an
	// ending at all; the zero value, so a caller that forgets to check
	// gets nothing rather than a wrong answer.
	EndingRunning Ending = ""

	// EndingEscalated and EndingFailed are the framework's own two, and are
	// not the flow's to redefine.
	EndingEscalated Ending = "Escalated"
	EndingFailed    Ending = "Failed"

	// EndingSuccess and EndingFailure are what the flow declared in
	// terminals.
	EndingSuccess Ending = Ending(flowv1alpha1.TerminalSuccess)
	EndingFailure Ending = Ending(flowv1alpha1.TerminalFailure)

	// EndingUndeclared is an ending the flow reached without ever saying
	// what it means. It is neither an error nor a synonym for success: a
	// flow written before terminals existed says nothing, and the framework
	// reports the silence rather than filling it in (P8). It also gives
	// whoever is reading the metric a way to find the flows still owing a
	// declaration.
	EndingUndeclared Ending = "Undeclared"
)

// EndingOf reports what reaching phase means, or EndingRunning when it is not
// somewhere a task stops.
//
// The framework's two reserved names answer for themselves and are checked
// first: what Escalated means does not depend on a flow, which is what lets a
// task that reached it be reported even after the flow is gone.
func EndingOf(
	bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding,
	terminals map[flowv1alpha1.Phase]flowv1alpha1.TerminalSeverity,
	phase flowv1alpha1.Phase,
) Ending {
	switch {
	case phase == flowv1alpha1.PhaseEscalated:
		return EndingEscalated
	case phase == flowv1alpha1.PhaseFailed:
		return EndingFailed
	case !IsTerminal(bindings, phase):
		return EndingRunning
	}
	switch terminals[phase] {
	case flowv1alpha1.TerminalSuccess:
		return EndingSuccess
	case flowv1alpha1.TerminalFailure:
		return EndingFailure
	}
	return EndingUndeclared
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

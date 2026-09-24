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
	"fmt"
	"slices"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// Outcome is why a task moved, alongside where to.
type Outcome string

const (
	// OutcomeDeclared followed an edge the flow declared.
	OutcomeDeclared Outcome = "Declared"
	// OutcomeRework followed a declared edge back to a phase that has already
	// run. It costs nothing of its own: what bounds a cycle is how many times
	// each phase may run, and this outcome only records that the edge went
	// back rather than forward.
	OutcomeRework Outcome = "Rework"
	// OutcomeRunLimitReached wanted to move to a phase that has already run
	// as many times as the flow allows.
	OutcomeRunLimitReached Outcome = "RunLimitReached"
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

// Input is everything the decision depends on. Passing the run counts and the
// limit in, rather than reading them from a status, keeps the function
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
	// Runs is how many times each phase of this task has run, the run being
	// settled included. A destination with a count makes the edge a rework,
	// with no annotation required and none to forget.
	Runs map[flowv1alpha1.Phase]int32
	// MaxRuns is the flow's limit on runs of any one phase.
	MaxRuns int32
}

// Result is where the task goes and why.
type Result struct {
	Next flowv1alpha1.Phase
	// Outcome explains the move; it is what gets recorded and surfaced.
	Outcome Outcome
	// Detail is a short human-facing reason, for a status condition.
	Detail string
}

// Next decides the phase to run after Phase finished writing into Directory.
//
// The framework's own answers are built in and cannot be declared away:
//
//	two statuses, one directory    -> Failed     (undecidable)
//	Failed named as a destination  -> Failed     (not an answer)
//	no single directory written    -> Escalated  (nothing was decided)
//	a phase at its run limit       -> Escalated
//
// Two further cases are answered below that the controller never actually
// asks about, because Reconcile and collect settle them before a run gets
// here: a phase with no binding, and a directory the flow omits. They are
// guards rather than paths, and the comment on each says what removing it
// would cost. Both are exercised by this package's tests, which call Next
// directly and are therefore not bound by what the controller happens to
// pass — the reason those tests are not proof that the branch is live.
//
// Every other move follows the flow's own table — including an edge the flow
// declared to Escalated, which is the one reserved name it may name as a
// destination.
func Next(in Input) Result {
	binding, bound := in.Bindings[in.Phase]
	if !bound {
		// The controller never presents this: Reconcile checks the same
		// thing before it has a run to settle, and answers it better than
		// this can — it can see whether a run was in flight, which is what
		// separates a flow edited under a running task from one that simply
		// stopped here. Removing this guard would not remove the case,
		// only the answer: the lookup above hands back a zero binding, and
		// the task would leave as NoAnswer — a broken definition reported
		// as something a human should read the handler's output about.
		// Why the controller's check makes this one unreachable is written
		// down in the taskstate package, next to the invariant it depends
		// on.
		return Result{
			Next:    flowv1alpha1.PhaseFailed,
			Outcome: OutcomeStructural,
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
			Detail:  "directory " + in.Directory + " selects more than one status",
		}
	case found == 0:
		// Also never presented by the controller: collect is handed this
		// same binding's directories in the same reconcile and refuses
		// anything outside them, so a directory that reaches here is always
		// one of these. Unlike the guard above, removing this one fails
		// silently rather than loudly — dest stays empty, an empty phase
		// binds nothing, and a phase nothing binds is where a flow ends, so
		// the task would stop as though it had finished — and for a flow
		// that somehow has no ttl at all, which the schema's default
		// normally prevents, worse still: an empty phase reads as a task
		// that never started, and the next reconcile begins it again from
		// the flow's start.
		return Result{
			Next:    flowv1alpha1.PhaseEscalated,
			Outcome: OutcomeNoAnswer,
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
	// It skips the run limit below because Escalated is terminal on its own
	// say-so: there is no run after it to count.
	if dest == flowv1alpha1.PhaseEscalated {
		return Result{
			Next:    flowv1alpha1.PhaseEscalated,
			Outcome: OutcomeDeclined,
			Detail:  "escalated on purpose, by writing into " + in.Directory,
		}
	}

	// Failed is not. It means the definition is broken, which is never
	// something the work gets to conclude, so a flow naming it as a
	// destination is itself the defect. Admission now refuses that flow at
	// creation (#17 / ADR-0006), but this check stays anyway: nothing here
	// may depend on admission having run (ADR-0006 decision 5), and a flow
	// can still be edited after a task has already started (#19). So the
	// task stops here rather than reaching Failed under an outcome that
	// would read like a declared edge.
	if dest == flowv1alpha1.PhaseFailed {
		return Result{
			Next:    flowv1alpha1.PhaseFailed,
			Outcome: OutcomeStructural,
			Detail:  "directory " + in.Directory + " is declared to reach Failed, which is the framework's own",
		}
	}

	// A limit below one would refuse every phase, the start's successor
	// included, and a flow that can run nothing past its first phase is not
	// what anybody wrote. The schema's minimum and default keep it from
	// arriving here; this answers it anyway rather than escalating every
	// task of the flow as though the work had run out of rounds.
	if in.MaxRuns < 1 {
		return Result{
			Next:    flowv1alpha1.PhaseFailed,
			Outcome: OutcomeStructural,
			Detail:  fmt.Sprintf("maxRunsPerPhase is %d, which lets no phase run", in.MaxRuns),
		}
	}

	// A destination nothing binds never runs, so its count is always zero
	// and the limit never stops a task from reaching an ending.
	n := in.Runs[dest]
	if n >= in.MaxRuns {
		return Result{
			Next:    flowv1alpha1.PhaseEscalated,
			Outcome: OutcomeRunLimitReached,
			Detail:  fmt.Sprintf("%s has already run %d of %d times", dest, n, in.MaxRuns),
		}
	}
	if n > 0 {
		return Result{
			Next:    dest,
			Outcome: OutcomeRework,
			Detail:  fmt.Sprintf("rework to %s, which has run %d of %d times", dest, n, in.MaxRuns),
		}
	}

	return Result{
		Next:    dest,
		Outcome: OutcomeDeclared,
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
	EndingEscalated Ending = Ending(flowv1alpha1.PhaseEscalated)
	EndingFailed    Ending = Ending(flowv1alpha1.PhaseFailed)

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
// task that reached it be reported even after the flow is gone — flow may be
// nil for exactly that reason, and still get the right answer for those two.
func EndingOf(flow *flowv1alpha1.TaskFlowSpec, phase flowv1alpha1.Phase) Ending {
	switch {
	case phase == flowv1alpha1.PhaseEscalated:
		return EndingEscalated
	case phase == flowv1alpha1.PhaseFailed:
		return EndingFailed
	case flow == nil, !IsTerminal(flow.Bindings, phase):
		return EndingRunning
	}
	switch flow.Terminals[phase] {
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

// PhaseEnding pairs a place a task can stop with what stopping there means.
type PhaseEnding struct {
	Phase  flowv1alpha1.Phase
	Ending Ending
}

// DeclaredEndings lists every place a task of this flow can stop, paired with
// what stopping there means, in a stable order.
//
// It exists so the endings a flow declares can be reported before any task has
// reached one. A counter's child series is born at its first increment, with
// no zero sample before it, and a range function needs two samples to see a
// rise — so an ending that happens once, long after the process started, rises
// from nothing and is invisible to increase() (ADR-0010). The endings that
// matter most here are exactly the rare ones, so the flow has to say they
// exist before they do.
//
// The stopping places are the destinations no binding claims: every next a
// binding names, minus the phases that are themselves bound. The framework's
// own two are always among them whether or not a flow names them — Escalated
// can happen to any flow that cannot read an answer, and Failed to any flow
// that turns out to be broken — so they are added rather than discovered.
//
// Each phase appears once, with the single ending it means. severity is not a
// dimension a phase varies over: EndingOf reads it from the flow's own
// terminals declaration, so a phase has one meaning and the pairs are a list,
// never the product of the two labels.
func DeclaredEndings(spec *flowv1alpha1.TaskFlowSpec) []PhaseEnding {
	if spec == nil {
		return nil
	}

	seen := make(map[flowv1alpha1.Phase]bool, len(spec.Bindings))
	phases := make([]flowv1alpha1.Phase, 0, len(spec.Bindings))
	for _, binding := range spec.Bindings {
		for dest := range binding.Next {
			if !IsTerminal(spec.Bindings, dest) || seen[dest] {
				continue
			}
			seen[dest] = true
			phases = append(phases, dest)
		}
	}
	for _, reserved := range flowv1alpha1.ReservedPhases {
		if !seen[reserved] {
			seen[reserved] = true
			phases = append(phases, reserved)
		}
	}
	sortPhases(phases)

	endings := make([]PhaseEnding, 0, len(phases))
	for _, phase := range phases {
		endings = append(endings, PhaseEnding{Phase: phase, Ending: EndingOf(spec, phase)})
	}
	return endings
}

// sortPhases keeps DeclaredEndings' order independent of the map hash seed, so
// two identical flows prime the same series in the same order every time.
func sortPhases(phases []flowv1alpha1.Phase) {
	slices.Sort(phases)
}

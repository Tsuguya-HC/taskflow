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

package v1alpha1

import "slices"

// Phase is a status name. It is a free string chosen by whoever writes the
// flow: this framework does not know what the work is, so it has no business
// naming its stages. "調査" and "Planning" are equally valid.
//
// Three names are the framework's rather than the author's. Two of them are
// answers it decides — see ReservedPhases — and the third is PhaseFinally,
// which is not an answer at all but the name a cleanup run is recorded under.
type Phase string

const (
	// PhaseEscalated is where a task goes when no single answer came back:
	// nothing was written, several things were, the run timed out, or the flow
	// no longer explains what did arrive. A flow may also send work here on
	// purpose, by naming it in a phase's next. A human takes it from here
	// either way; the outcome recorded says which of the two happened.
	PhaseEscalated Phase = "Escalated"

	// PhaseFailed is where a task goes when the flow itself is broken —
	// a phase with no binding, an ambiguous mapping. Nothing is repaired,
	// because the fault is in the definition rather than in the work.
	PhaseFailed Phase = "Failed"

	// PhaseFinally is the name the run declared by spec.finally is recorded
	// under: currentRun names it while that run is in flight, and history
	// keeps the line it wrote. It never appears in status.phase, which is the
	// ending the task reached and which the cleanup run does not change
	// (ADR-0009), so it is not in ReservedPhases — a task is not "at" Finally
	// and stopping there is not something IsTerminal is ever asked about.
	//
	// A flow may not use it as a binding key or as an edge's destination
	// (flowcheck refuses both): the cleanup is not a phase, and a phase
	// sharing its name would make one line of history mean two things. A
	// TaskHandler may still declare phase: Finally — spec.phase says what a
	// handler is for, and for this one that is the truth.
	PhaseFinally Phase = "Finally"
)

// ReservedPhases may not be used as a binding key. They are the two outcomes
// the framework owns, and a flow that could bind them could route "no answer"
// onto its own success path — which is the one thing this design will not
// allow to be one line away.
//
// As destinations the two part company. A phase's next may name Escalated,
// and doing so is what gives the run a directory for "I will not decide
// this" — a conclusion, reached and reported, rather than the silence of a
// run that died. Failed may not be named: it says the definition is broken,
// and a definition does not get to conclude that about itself. Both rules
// live in transition.Next.
//
// The CEL rule on TaskHandlerSpec.Phase (taskhandler_types.go) re-encodes
// these two names as a literal, since CEL cannot reference a Go const —
// update it too if this changes.
var ReservedPhases = []Phase{PhaseEscalated, PhaseFailed}

// IsReserved reports whether p is one of the framework's own outcomes.
func (p Phase) IsReserved() bool {
	return slices.Contains(ReservedPhases, p)
}

// IsFinally reports whether p is the cleanup run's reserved name.
func (p Phase) IsFinally() bool {
	return p == PhaseFinally
}

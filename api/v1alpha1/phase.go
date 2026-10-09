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
// One name is the framework's rather than the author's. It is an answer it
// decides — see ReservedPhases — and PhaseFinally, which is not an answer
// at all but the name a cleanup run is recorded under.
type Phase string

const (
	// PhaseTaskFailed is where a task goes when the work did not conclude:
	// nothing was written, several things were, the run timed out, the run
	// limit was reached, or the flow no longer explains what did arrive —
	// and where it goes when the flow itself is broken, a phase with no
	// binding or an ambiguous mapping. Nothing is repaired in the latter
	// case, because the fault is in the definition rather than in the work;
	// what separates the two cases is the outcome recorded, not the name.
	// A flow may also send work here on purpose, by naming it in a phase's
	// next. It is spelled out so it greps as one word: Failed already means
	// a Job's failure, a handler that could not run, and a cleanup that did
	// not happen, and none of those is this.
	PhaseTaskFailed Phase = "TaskFailed"

	// PhaseFinally is the name the run declared by spec.finally is recorded
	// under: currentRuns names it while that run is in flight, and history
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

// ReservedPhases may not be used as a binding key. It is the one answer
// the framework owns, and a flow that could bind it could route "no answer"
// onto its own success path — which is the one thing this design will not
// allow to be one line away.
//
// As a destination it may be named: a phase's next naming TaskFailed is
// what gives the run a directory for "I will not decide
// this" — a conclusion, reached and reported, rather than the silence of a
// run that died.
//
// The CEL rule on TaskHandlerSpec.Phase (taskhandler_types.go) re-encodes
// this name as a literal, since CEL cannot reference a Go const —
// update it too if this changes.
//
// Escalated and Failed are not reserved anymore, but they are still refused
// wherever a name is declared — binding keys, next destinations, terminals
// and handler phases — with a pointer to TaskFailed. Left merely unreserved,
// a flow written for the old names would read as an unbound declared ending
// and pass silently, succeeding a task nobody decided.
var ReservedPhases = []Phase{PhaseTaskFailed}

// IsReserved reports whether p is one of the framework's own outcomes.
func (p Phase) IsReserved() bool {
	return slices.Contains(ReservedPhases, p)
}

// IsFinally reports whether p is the cleanup run's reserved name.
func (p Phase) IsFinally() bool {
	return p == PhaseFinally
}

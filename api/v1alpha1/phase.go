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

// Phase is the fixed vocabulary a task moves through. It is deliberately not
// extensible: CiliumNetworkPolicies and Kyverno policies are written against
// these names, so a flow that could invent a phase could reach a pod nobody
// wrote a policy for. Flows choose the edges between phases, not the phases.
type Phase string

const (
	PhasePending      Phase = "Pending"
	PhaseTriaging     Phase = "Triaging"
	PhasePlanning     Phase = "Planning"
	PhasePlanReview   Phase = "PlanReview"
	PhaseImplementing Phase = "Implementing"
	PhaseReview       Phase = "Review"
	PhaseVerifying    Phase = "Verifying"

	// Terminal.
	PhaseDone      Phase = "Done"
	PhaseEscalated Phase = "Escalated"
	PhaseFailed    Phase = "Failed"
)

// IsTerminal reports whether no further transition leaves this phase.
//
// Escalated is terminal: a human takes over, and today they do so by creating
// a new task rather than resuming this one. Whether an edge back should exist
// is still open (design.md §13).
func (p Phase) IsTerminal() bool {
	switch p {
	case PhaseDone, PhaseEscalated, PhaseFailed:
		return true
	default:
		return false
	}
}

// Verdict is what a handler reports by writing into one of the out/
// directories. The three below are the vocabulary handlers can produce; the
// two after them are produced by the controller and can never be claimed by a
// handler, because a handler that could report "indeterminate" as a pass would
// break the one rule this design will not bend (P6).
type Verdict string

const (
	VerdictPass     Verdict = "pass"
	VerdictRework   Verdict = "rework"
	VerdictEscalate Verdict = "escalate"

	// Controller-produced. Not writable by a handler.
	VerdictIndeterminate Verdict = "indeterminate"
	VerdictTimeout       Verdict = "timeout"
)

// ReservedVerdicts are the tokens a flow may not bind in outcomes. They route
// to Escalated unconditionally: an unknown result is not an approval, and
// making that overridable would put P6 one YAML line away from being off.
var ReservedVerdicts = []Verdict{VerdictIndeterminate, VerdictTimeout}

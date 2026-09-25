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
	"fmt"
	"slices"
	"strings"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
)

// OutcomeCancelled is a branch stopped before it answered, because another
// branch of the same fork had already sent the task to Escalated (ADR-0013
// 決定4). Nothing the stopped branch could have said would have changed that,
// so it is not waited for; the line says it was started and why it was not
// heard from.
const OutcomeCancelled Outcome = "Cancelled"

// Branches is every phase fork may start, in name order: the destinations its
// run can choose, and those its join starts regardless. Escalated is where a
// run goes instead of choosing, and the join is where branches meet rather
// than one of them, so neither is a branch. An always entry that could not be
// one — reserved, the fork itself, its own join — is left out rather than
// guessed at; admission is what says so (flowcheck).
func Branches(bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding, fork flowv1alpha1.Phase) []flowv1alpha1.Phase {
	binding, bound := bindings[fork]
	if !bound || binding.Join == nil {
		return nil
	}
	var out []flowv1alpha1.Phase
	for dest := range binding.Next {
		if !dest.IsReserved() && dest != binding.Join.Phase {
			out = append(out, dest)
		}
	}
	for _, always := range binding.Join.Always {
		if always != "" && !always.IsReserved() && !always.IsFinally() &&
			always != fork && always != binding.Join.Phase && !slices.Contains(out, always) {
			out = append(out, always)
		}
	}
	slices.Sort(out)
	return out
}

// ForkResult is where a fork's run sends the task: the branches to start, or
// the one place it stops instead.
type ForkResult struct {
	// Branches to start, in name order, and empty when the task stops.
	Branches []flowv1alpha1.Phase
	// Next is Escalated or Failed when the task stops at the fork, and empty
	// when the branches start: the task stays at the fork while they run.
	Next flowv1alpha1.Phase
	// Outcome explains the move, as Result.Outcome does.
	Outcome Outcome
	// Detail is the short human-facing reason.
	Detail string
}

// Fork decides what a fork's run starts (ADR-0013 決定1). Its answer is every
// directory it wrote into, joined; each chooses one branch, and the join's
// always list is started beside them. The rules that stop a fork instead are
// the ones that stop any run, applied to the whole answer:
//
//	no answer, or a word outside the words      -> Escalated (NoAnswer)
//	only the declared escalate directory         -> Escalated (Declined)
//	escalate beside a branch                     -> Escalated (NoAnswer): not one answer
//	a branch at its run limit                    -> Escalated (RunLimitReached)
//	two statuses on one directory, Failed named,
//	a branch or the join nobody binds, an edge
//	straight to the join, a limit below one      -> Failed    (Structural)
//
// Nothing is started unless every branch can be: a fork is one decision.
func Fork(in Input) ForkResult {
	binding, bound := in.Bindings[in.Phase]
	if !bound || binding.Join == nil {
		return ForkResult{Next: flowv1alpha1.PhaseFailed, Outcome: OutcomeStructural,
			Detail: "phase " + string(in.Phase) + " is not a fork in this flow"}
	}
	if in.Directory == "" {
		detail := in.NoAnswer
		if detail == "" {
			detail = "the run produced no answer"
		}
		return ForkResult{Next: flowv1alpha1.PhaseEscalated, Outcome: OutcomeNoAnswer, Detail: detail}
	}
	if in.MaxRuns < 1 {
		return ForkResult{Next: flowv1alpha1.PhaseFailed, Outcome: OutcomeStructural,
			Detail: fmt.Sprintf("maxRunsPerPhase is %d, which lets no phase run", in.MaxRuns)}
	}

	var chosen []flowv1alpha1.Phase
	escalating := false
	for _, dir := range strings.Split(in.Directory, contract.DirectorySeparator) {
		var dests []flowv1alpha1.Phase
		for dest, d := range binding.Next {
			if d == dir {
				dests = append(dests, dest)
			}
		}
		switch {
		case len(dests) > 1:
			return ForkResult{Next: flowv1alpha1.PhaseFailed, Outcome: OutcomeStructural,
				Detail: "directory " + dir + " selects more than one status"}
		case len(dests) == 0:
			return ForkResult{Next: flowv1alpha1.PhaseEscalated, Outcome: OutcomeNoAnswer,
				Detail: "no status is declared for directory " + dir}
		}
		switch dest := dests[0]; {
		case dest == flowv1alpha1.PhaseEscalated:
			escalating = true
		case dest == flowv1alpha1.PhaseFailed:
			return ForkResult{Next: flowv1alpha1.PhaseFailed, Outcome: OutcomeStructural,
				Detail: "directory " + dir + " is declared to reach Failed, which is the framework's own"}
		case dest == binding.Join.Phase:
			return ForkResult{Next: flowv1alpha1.PhaseFailed, Outcome: OutcomeStructural,
				Detail: "directory " + dir + " leads straight to where the branches meet, which a fork may not do yet"}
		default:
			chosen = append(chosen, dest)
		}
	}
	if escalating {
		if len(chosen) == 0 {
			return ForkResult{Next: flowv1alpha1.PhaseEscalated, Outcome: OutcomeDeclined,
				Detail: "escalated on purpose, by writing into " + in.Directory}
		}
		return ForkResult{Next: flowv1alpha1.PhaseEscalated, Outcome: OutcomeNoAnswer,
			Detail: "wrote " + in.Directory + ": escalating and choosing branches are not one answer"}
	}

	if _, bound := in.Bindings[binding.Join.Phase]; !bound {
		return ForkResult{Next: flowv1alpha1.PhaseFailed, Outcome: OutcomeStructural,
			Detail: "the branches of " + string(in.Phase) + " meet at " + string(binding.Join.Phase) + ", which nothing binds"}
	}
	branches := slices.Clone(chosen)
	for _, always := range Branches(in.Bindings, in.Phase) {
		if slices.Contains(binding.Join.Always, always) && !slices.Contains(branches, always) {
			branches = append(branches, always)
		}
	}
	slices.Sort(branches)

	rework := false
	for _, b := range branches {
		if _, bound := in.Bindings[b]; !bound {
			return ForkResult{Next: flowv1alpha1.PhaseFailed, Outcome: OutcomeStructural,
				Detail: "branch " + string(b) + " has no binding in this flow"}
		}
		n := in.Runs[b]
		if n >= in.MaxRuns {
			return ForkResult{Next: flowv1alpha1.PhaseEscalated, Outcome: OutcomeRunLimitReached,
				Detail: fmt.Sprintf("branch %s has already run %d of %d times", b, n, in.MaxRuns)}
		}
		rework = rework || n > 0
	}
	outcome := OutcomeDeclared
	if rework {
		outcome = OutcomeRework
	}
	names := make([]string, len(branches))
	for i, b := range branches {
		names[i] = string(b)
	}
	return ForkResult{Branches: branches, Outcome: outcome, Detail: "forked into " + strings.Join(names, ", ")}
}

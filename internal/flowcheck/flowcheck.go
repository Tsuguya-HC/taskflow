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

// Package flowcheck is everything a TaskFlow can be wrong about on its own.
//
// It is the Go half of design.md §5: the rows of that table which close
// inside a single TaskFlow live here, and the rows that a CEL rule can state
// in one line stay on the type (ADR-0006 決定1). What is deliberately absent
// is anything needing a second object — a handler's spec.phase, a profile's
// required phases — because the admission of one object must not be gated on
// another one's existence: a handler that lands after its flow cannot be a
// reason to refuse the flow (ADR-0006 決定4).
//
// Like transition, it is a pure function over values: no client, no context,
// no clock. Admission is where it is called from, not what it is — the
// runtime keeps its own checks, because nothing here may be assumed to have
// run (ADR-0006 決定5).
package flowcheck

import (
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/util/validation/field"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// Check reports everything wrong with spec, as field errors rooted at path.
//
// It returns all of them rather than the first: a flow is written by hand and
// applied by GitOps, so the round trip between a rejection and the next
// attempt is a commit. Every error the author could have been told about at
// once, they are told about at once.
func Check(spec *flowv1alpha1.TaskFlowSpec, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	bindings := path.Child("bindings")

	for _, phase := range sortedPhases(spec.Bindings) {
		key := bindings.Key(string(phase))
		switch {
		case phase == "":
			errs = append(errs, field.Invalid(key, "", "a phase with no name is not a phase"))
		case phase.IsReserved():
			errs = append(errs, field.Forbidden(key,
				fmt.Sprintf("%s is one of the framework's own answers and cannot be bound to a handler", phase)))
		case phase.IsFinally():
			errs = append(errs, field.Forbidden(key,
				"Finally is the name the cleanup run is recorded under, not a phase; the handler that runs "+
					"after the ending goes in spec.finally"))
		}
		if phase != "" && !phase.IsReserved() && !phase.IsFinally() {
			if err := contract.CheckDirectoryName(string(phase)); err != nil {
				errs = append(errs, field.Invalid(key, string(phase),
					"a phase's name is also the directory its answer is shown under to the runs it leads to "+
						"(the inputs view, ADR-0013): "+err.Error()))
			}
		}
		errs = append(errs, checkNext(spec.Bindings[phase], key.Child("next"))...)
		if spec.Bindings[phase].Join != nil {
			errs = append(errs, checkJoin(spec, phase, bindings)...)
		}
	}
	errs = append(errs, checkBranchEntry(spec, bindings)...)

	errs = append(errs, checkFinally(spec.Finally, path.Child("finally"))...)

	// Everything below walks the graph, and a walk needs somewhere to start.
	// A start that binds nothing is reported once, here, rather than again as
	// every phase being unreachable from it.
	if _, bound := spec.Bindings[spec.Start]; !bound {
		return append(errs, field.Invalid(path.Child("start"), string(spec.Start),
			"the phase a task begins at must be one this flow binds"))
	}

	reached, endings := walk(spec)
	for _, phase := range sortedPhases(spec.Bindings) {
		if !reached[phase] {
			errs = append(errs, field.Invalid(bindings.Key(string(phase)), string(phase),
				fmt.Sprintf("no path from %q reaches this phase, so nothing this binding says can ever happen",
					spec.Start)))
		}
	}
	if !endings {
		errs = append(errs, field.Invalid(bindings, nil,
			fmt.Sprintf("no path from %q reaches a phase this flow leaves unbound, so no task of this flow can "+
				"finish on its own terms — Escalated does not count, since it is where the framework puts work "+
				"nobody decided", spec.Start)))
	}
	return errs
}

// checkNext judges one binding's edges: where they may lead, and what the
// directories that select them may be called.
func checkNext(binding flowv1alpha1.PhaseBinding, path *field.Path) field.ErrorList {
	var errs field.ErrorList
	claimed := map[string]flowv1alpha1.Phase{}

	for _, dest := range sortedDestinations(binding.Next) {
		dir := binding.Next[dest]
		key := path.Key(string(dest))

		switch dest {
		case "":
			errs = append(errs, field.Invalid(key, dir, "an edge must say which phase it leads to"))
		case flowv1alpha1.PhaseFailed:
			errs = append(errs, field.Forbidden(key,
				"Failed means the definition is broken, which is not something a run gets to conclude about "+
					"the flow it is running; Escalated is the reserved name an edge may name"))
		case flowv1alpha1.PhaseFinally:
			errs = append(errs, field.Forbidden(key,
				"the cleanup run follows the ending rather than being somewhere a verdict can lead, so no "+
					"edge may name Finally; declare it in spec.finally"))
		}

		if err := contract.CheckDirectoryName(dir); err != nil {
			errs = append(errs, field.Invalid(key, dir, err.Error()))
			continue
		}
		if first, taken := claimed[dir]; taken {
			errs = append(errs, field.Invalid(key, dir,
				fmt.Sprintf("%q already selects %q; two statuses sharing one directory would leave the run's "+
					"destination undecidable", dir, first)))
			continue
		}
		claimed[dir] = dest
	}
	return errs
}

// checkJoin judges one fork: where its branches meet, which branches it has,
// and that each of them is a single phase that goes nowhere but the join or
// Escalated (ADR-0013 決定2 and the v1 limits of 決定3). Whether anything
// outside the fork reaches into a branch is a question about every binding at
// once, and checkBranchEntry asks it.
func checkJoin(spec *flowv1alpha1.TaskFlowSpec, fork flowv1alpha1.Phase, bindings *field.Path) field.ErrorList {
	var errs field.ErrorList
	path := bindings.Key(string(fork)).Child("join")
	join := spec.Bindings[fork].Join
	joinPath := path.Child("phase")

	switch _, bound := spec.Bindings[join.Phase]; {
	case join.Phase == fork:
		errs = append(errs, field.Invalid(joinPath, string(join.Phase),
			"a fork cannot be where its own branches meet"))
	case !bound:
		errs = append(errs, field.Invalid(joinPath, string(join.Phase),
			"branches meet at a phase that runs once they all arrive, so it must be one this flow binds"))
	}

	chosen := 0
	for _, dest := range sortedDestinations(spec.Bindings[fork].Next) {
		switch {
		case dest.IsReserved():
		case dest == join.Phase:
			errs = append(errs, field.Forbidden(bindings.Key(string(fork)).Child("next").Key(string(dest)),
				"an edge from a fork straight to where its branches meet is not supported yet (ADR-0013 決定3)"))
		default:
			chosen++
		}
	}
	if chosen == 0 {
		errs = append(errs, field.Invalid(path, nil,
			"a fork needs at least one destination besides Escalated for its run to choose; with none, "+
				"the only thing its run could ever write is a refusal"))
	}

	next := spec.Bindings[fork].Next
	for i, always := range join.Always {
		key := path.Child("always").Index(i)
		_, destination := next[always]
		switch {
		case always == "" || always.IsReserved() || always.IsFinally():
			errs = append(errs, field.Invalid(key, string(always), "a branch must be a phase someone runs"))
		case always == fork || always == join.Phase:
			errs = append(errs, field.Invalid(key, string(always),
				"a branch is neither the fork it starts from nor the phase it meets at"))
		case destination:
			errs = append(errs, field.Invalid(key, string(always),
				"this phase is already one of the fork's destinations; a branch chosen both ways would have "+
					"two answers to whether it runs"))
		}
	}

	for _, branch := range branches(spec, fork) {
		errs = append(errs, checkBranch(spec, branch, join.Phase, bindings.Key(string(branch)))...)
	}
	return errs
}

// checkBranch judges one branch against the v1 shape: a single phase, bound,
// with no fork of its own, whose every edge leads to the join or Escalated
// and at least one leads to the join.
func checkBranch(
	spec *flowv1alpha1.TaskFlowSpec,
	branch, join flowv1alpha1.Phase,
	path *field.Path,
) field.ErrorList {
	binding, bound := spec.Bindings[branch]
	if !bound {
		return field.ErrorList{field.Invalid(path, string(branch),
			"a fork's branch must be a phase this flow binds; a branch with no handler has no one to run it")}
	}
	var errs field.ErrorList
	if binding.Join != nil {
		errs = append(errs, field.Forbidden(path.Child("join"),
			"a branch that forks again is not supported yet (ADR-0013 決定3)"))
	}
	if branch == spec.Start {
		errs = append(errs, field.Invalid(path, string(branch),
			"a branch is entered only from its fork, and the start is entered from nowhere"))
	}
	meets := false
	for _, dest := range sortedDestinations(binding.Next) {
		switch dest {
		case join:
			meets = true
		case flowv1alpha1.PhaseEscalated:
		default:
			errs = append(errs, field.Invalid(path.Child("next").Key(string(dest)), binding.Next[dest],
				fmt.Sprintf("a branch leaves only to where its branches meet (%q) or to Escalated; anywhere else "+
					"and the fork would wait for a branch that is no longer coming", join)))
		}
	}
	if !meets {
		errs = append(errs, field.Invalid(path.Child("next"), nil,
			fmt.Sprintf("a branch must be able to reach %q, where its fork's branches meet", join)))
	}
	return errs
}

// checkBranchEntry refuses every way into a branch other than its own fork
// (ADR-0013 S4): an edge from some other binding, or a branch claimed by two
// forks. A run that entered a branch any other way would have no fork to wait
// with, and no answer to which join it belongs to.
func checkBranchEntry(spec *flowv1alpha1.TaskFlowSpec, bindings *field.Path) field.ErrorList {
	var errs field.ErrorList
	owner := map[flowv1alpha1.Phase]flowv1alpha1.Phase{}
	for _, fork := range sortedPhases(spec.Bindings) {
		if spec.Bindings[fork].Join == nil {
			continue
		}
		for _, branch := range branches(spec, fork) {
			if first, taken := owner[branch]; taken {
				errs = append(errs, field.Invalid(bindings.Key(string(fork)).Child("join"), string(branch),
					fmt.Sprintf("%q is already a branch of %q; a branch belongs to one fork", branch, first)))
				continue
			}
			owner[branch] = fork
		}
	}
	for _, from := range sortedPhases(spec.Bindings) {
		for _, dest := range sortedDestinations(spec.Bindings[from].Next) {
			if fork, isBranch := owner[dest]; isBranch && fork != from {
				errs = append(errs, field.Invalid(bindings.Key(string(from)).Child("next").Key(string(dest)),
					spec.Bindings[from].Next[dest],
					fmt.Sprintf("%q is a branch of %q and is entered only from there", dest, fork)))
			}
		}
	}
	return errs
}

// branches is every phase a fork may start — the one rule transition uses
// to start them, so what admission checks and what runs are the same set.
func branches(spec *flowv1alpha1.TaskFlowSpec, fork flowv1alpha1.Phase) []flowv1alpha1.Phase {
	return transition.Branches(spec.Bindings, fork)
}

// checkFinally judges the cleanup run's declaration. Only the directory needs
// judging here — the handler's name is a string this flow will look up in its
// own namespace, and whether anything answers to it is not a question
// admission may ask (ADR-0006 決定4) — and it is judged by exactly the rule an
// edge's directory is, from the same function the sidecar re-checks it with.
func checkFinally(finally *flowv1alpha1.FinallySpec, path *field.Path) field.ErrorList {
	if finally == nil {
		return nil
	}
	if err := contract.CheckDirectoryName(finally.Done); err != nil {
		return field.ErrorList{field.Invalid(path.Child("done"), finally.Done, err.Error())}
	}
	return nil
}

// walk follows the flow's edges out of start, and reports which bound phases
// it got to and whether any path leaves the graph at a phase the flow itself
// declares an ending — one it does not bind, and not one of the framework's
// two, which are where a task stops without the flow having finished.
func walk(spec *flowv1alpha1.TaskFlowSpec) (reached map[flowv1alpha1.Phase]bool, endings bool) {
	reached = map[flowv1alpha1.Phase]bool{spec.Start: true}
	queue := []flowv1alpha1.Phase{spec.Start}

	for len(queue) > 0 {
		phase := queue[0]
		queue = queue[1:]
		dests := sortedDestinations(spec.Bindings[phase].Next)
		if join := spec.Bindings[phase].Join; join != nil {
			// A branch listed in always starts without an edge naming it,
			// so it is reached the way a destination is.
			dests = append(dests, join.Always...)
		}
		for _, dest := range dests {
			if _, bound := spec.Bindings[dest]; !bound {
				endings = endings || !dest.IsReserved()
				continue
			}
			if !reached[dest] {
				reached[dest] = true
				queue = append(queue, dest)
			}
		}
	}
	return reached, endings
}

// sortedPhases and sortedDestinations exist so that a flow with several
// mistakes is told about them in the same order every time. Ranging a map
// would make the report depend on the hash seed, which turns one wrong flow
// into an error message that differs between two identical applies.
func sortedPhases(bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding) []flowv1alpha1.Phase {
	phases := make([]flowv1alpha1.Phase, 0, len(bindings))
	for phase := range bindings {
		phases = append(phases, phase)
	}
	sortPhases(phases)
	return phases
}

func sortedDestinations(next map[flowv1alpha1.Phase]string) []flowv1alpha1.Phase {
	dests := make([]flowv1alpha1.Phase, 0, len(next))
	for dest := range next {
		dests = append(dests, dest)
	}
	sortPhases(dests)
	return dests
}

func sortPhases(phases []flowv1alpha1.Phase) {
	slices.Sort(phases)
}

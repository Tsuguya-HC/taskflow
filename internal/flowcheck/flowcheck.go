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
		}
		errs = append(errs, checkNext(spec.Bindings[phase], key.Child("next"))...)
	}

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
		for _, dest := range sortedDestinations(spec.Bindings[phase].Next) {
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

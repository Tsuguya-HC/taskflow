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

package flowcheck

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// Spelled as a literal so this file compiles before the constant exists
// (#217).
const redTerminal = flowv1alpha1.Phase("TaskFailed")

// The two names the single terminal replaces, spelled as literals so this
// file still compiles once their constants are gone (#217).
const (
	redEscalated = flowv1alpha1.Phase("Escalated")
	redFailed    = flowv1alpha1.Phase("Failed")
)

func redRefused(spec *flowv1alpha1.TaskFlowSpec) []string {
	errs := Check(spec, field.NewPath("spec"))
	lines := make([]string, 0, len(errs))
	for _, e := range errs {
		lines = append(lines, e.Field+": "+e.ErrorBody())
	}
	return lines
}

// Catches walk counting the single terminal as a way for a task to finish
// on its own terms, instead of refusing a flow whose only way out is the
// framework's own answer (#217).
func TestRefusesAFlowWhoseOnlyWayOutIsTaskFailed(t *testing.T) {
	spec := sampleFlow()
	delete(spec.Bindings[phaseReport].Next, phaseDone)
	spec.Bindings[phaseReport].Next[redTerminal] = "refuse"
	got := redRefused(spec)
	for _, line := range got {
		if strings.Contains(line, "TaskFailed") {
			return
		}
	}
	t.Fatalf("wanted the TaskFailed-only exit refused with TaskFailed named, got %v", got)
}

// Catches TaskFailed being bindable to a handler (#217).
func TestRefusesTaskFailedBoundToAHandler(t *testing.T) {
	spec := sampleFlow()
	spec.Bindings[redTerminal] = flowv1alpha1.PhaseBinding{
		Handler: "red-owner",
		Next:    map[flowv1alpha1.Phase]string{phaseDone: "red-handled"},
	}
	got := redRefused(spec)
	for _, line := range got {
		if strings.HasPrefix(line, `spec.bindings[TaskFailed]:`) &&
			strings.Contains(line, "framework's own") {
			return
		}
	}
	t.Fatalf("wanted spec.bindings[TaskFailed] refused as the framework's own, got %v", got)
}

// Catches the old terminals staying bindable as reserved names instead of
// being refused with a pointer to the single terminal (#217).
func TestRefusesTheOldTerminalsWithAPointerToTaskFailed(t *testing.T) {
	for _, old := range []flowv1alpha1.Phase{
		redEscalated, redFailed,
	} {
		t.Run(string(old), func(t *testing.T) {
			spec := sampleFlow()
			spec.Bindings[old] = flowv1alpha1.PhaseBinding{
				Handler: "red-owner",
				Next:    map[flowv1alpha1.Phase]string{phaseDone: "red-handled"},
			}
			got := redRefused(spec)
			hit := false
			for _, line := range got {
				if strings.HasPrefix(line, "spec.bindings["+string(old)+"]:") &&
					strings.Contains(line, "TaskFailed") {
					hit = true
				}
			}
			if !hit {
				t.Fatalf("wanted %s refused with TaskFailed named, got %v", old, got)
			}
		})
	}
}

// Catches the old terminals staying declarable as next destinations instead
// of being refused with a pointer to the single terminal (#217).
func TestRefusesTheOldTerminalsAsDestinations(t *testing.T) {
	for _, old := range []flowv1alpha1.Phase{
		redEscalated, redFailed,
	} {
		t.Run(string(old), func(t *testing.T) {
			spec := sampleFlow()
			spec.Bindings[phaseInvestigate].Next[old] = "legacy"
			got := redRefused(spec)
			hit := false
			for _, line := range got {
				if strings.Contains(line, "TaskFailed") {
					hit = true
				}
			}
			if !hit {
				t.Fatalf("wanted %s refused with TaskFailed named, got %v", old, got)
			}
		})
	}
}

// Catches a branch still forced to leave only to Escalated instead of the
// single terminal (#217).
func TestABranchMayLeaveOnlyToTaskFailed(t *testing.T) {
	spec := forkFlow()
	for dest := range spec.Bindings[phaseSecurity].Next {
		if dest == redEscalated {
			delete(spec.Bindings[phaseSecurity].Next, dest)
			spec.Bindings[phaseSecurity].Next[redTerminal] = dirStuck
		}
	}
	if got := redRefused(spec); len(got) != 0 {
		t.Fatalf("a branch leaving to TaskFailed was refused: %v", got)
	}
}

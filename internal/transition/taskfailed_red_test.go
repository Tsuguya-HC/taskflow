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
	"testing"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// The single terminal work stops at once no answer, the run limit, or a
// broken definition is reached (#217). Spelled as a literal so this file
// compiles before the constant exists.
const redTerminal = flowv1alpha1.Phase("TaskFailed")

// The two names the single terminal replaces, spelled as literals so this
// file still compiles once their constants are gone (#217).
const (
	redEscalated = flowv1alpha1.Phase("Escalated")
	redFailed    = flowv1alpha1.Phase("Failed")
)

const (
	redPhaseA flowv1alpha1.Phase = "red-a"
	redPhaseB flowv1alpha1.Phase = "red-b"
	redPhaseC flowv1alpha1.Phase = "red-c"
)

const (
	redRefuseDir = "red-refuse"
	redSilentWhy = "red-silence"
)

func redBindings() map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding {
	return map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		redPhaseA: {
			Handler: "h",
			Next:    map[flowv1alpha1.Phase]string{redPhaseB: "ok"},
		},
		redPhaseB: {
			Handler: "h",
			Next:    map[flowv1alpha1.Phase]string{redPhaseC: "sent"},
		},
	}
}

// Catches IsReserved still answering for the two old names instead of the
// single terminal (#217).
func TestTaskFailedIsTheReservedTerminal(t *testing.T) {
	if !redTerminal.IsReserved() {
		t.Fatalf("%q is not reserved", redTerminal)
	}
	if redEscalated.IsReserved() || redFailed.IsReserved() {
		t.Fatalf("the old terminals are still reserved")
	}
}

// Catches IsTerminal letting a bound TaskFailed run again (#217).
func TestABoundTaskFailedStillStops(t *testing.T) {
	b := redBindings()
	b[redTerminal] = flowv1alpha1.PhaseBinding{
		Handler: "h",
		Next:    map[flowv1alpha1.Phase]string{redPhaseC: "handled"},
	}
	if !IsTerminal(b, redTerminal) {
		t.Fatalf("a bound %q is not terminal", redTerminal)
	}
}

// Catches Next stopping silence at the old Escalated (#217).
func TestSilenceEndsAtTaskFailed(t *testing.T) {
	got := Next(Input{Bindings: redBindings(), Phase: redPhaseA, NoAnswer: redSilentWhy,
		Runs: ranOnce(redPhaseA), MaxRuns: 3})
	if got.Next != redTerminal || got.Outcome != OutcomeNoAnswer {
		t.Fatalf("got %q/%q, want TaskFailed/NoAnswer", got.Next, got.Outcome)
	}
}

// Catches Next stopping a phase at its limit at the old Escalated (#217).
func TestTheRunLimitEndsAtTaskFailed(t *testing.T) {
	b := redBindings()
	b[redPhaseA].Next[redPhaseA] = "more"
	got := Next(Input{Bindings: b, Phase: redPhaseA, Directory: "more",
		Runs: map[flowv1alpha1.Phase]int32{redPhaseA: 2}, MaxRuns: 2})
	if got.Next != redTerminal || got.Outcome != OutcomeRunLimitReached {
		t.Fatalf("got %q/%q, want TaskFailed/RunLimitReached", got.Next, got.Outcome)
	}
}

// Catches Next stopping a broken definition at the old Failed (#217).
func TestABrokenDefinitionEndsAtTaskFailed(t *testing.T) {
	t.Run("a phase with no binding", func(t *testing.T) {
		got := Next(Input{Bindings: redBindings(), Phase: "red-missing", Directory: "ok", MaxRuns: 3})
		if got.Next != redTerminal || got.Outcome != OutcomeStructural {
			t.Fatalf("got %q/%q, want TaskFailed/Structural", got.Next, got.Outcome)
		}
	})
	t.Run("two statuses sharing a directory", func(t *testing.T) {
		b := redBindings()
		b[redPhaseA].Next["red-other"] = "ok"
		got := Next(Input{Bindings: b, Phase: redPhaseA, Directory: "ok", Runs: ranOnce(redPhaseA), MaxRuns: 3})
		if got.Next != redTerminal || got.Outcome != OutcomeStructural {
			t.Fatalf("got %q/%q, want TaskFailed/Structural", got.Next, got.Outcome)
		}
	})
}

// Catches a declared edge to TaskFailed reading as an ordinary edge instead
// of the handler's own refusal (#217).
func TestADeclaredEdgeToTaskFailedIsDeclined(t *testing.T) {
	b := redBindings()
	b[redPhaseA].Next[redTerminal] = redRefuseDir
	got := Next(Input{Bindings: b, Phase: redPhaseA, Directory: redRefuseDir, Runs: ranOnce(redPhaseA), MaxRuns: 3})
	if got.Next != redTerminal || got.Outcome != OutcomeDeclined {
		t.Fatalf("got %q/%q, want TaskFailed/Declined", got.Next, got.Outcome)
	}
}

// Catches EndingOf answering TaskFailed with the flow's silence (#217).
func TestEndingOfTaskFailedIsItself(t *testing.T) {
	spec := &flowv1alpha1.TaskFlowSpec{Bindings: redBindings()}
	if got := EndingOf(spec, redTerminal); got != Ending(redTerminal) {
		t.Fatalf("ending = %q, want TaskFailed", got)
	}
	if got := EndingOf(nil, redTerminal); got != Ending(redTerminal) {
		t.Fatalf("ending without a flow = %q, want TaskFailed", got)
	}
}

// Catches DeclaredEndings priming the two old terminals instead of the
// single one (#217).
func TestDeclaredEndingsNameOnlyTaskFailed(t *testing.T) {
	got := DeclaredEndings(&flowv1alpha1.TaskFlowSpec{Bindings: redBindings()})
	names := map[flowv1alpha1.Phase]int{}
	for _, e := range got {
		names[e.Phase]++
	}
	if names[redTerminal] != 1 {
		t.Fatalf("endings = %v, want TaskFailed exactly once", got)
	}
	if names[redEscalated] != 0 || names[redFailed] != 0 {
		t.Fatalf("endings = %v, want neither old terminal", got)
	}
}

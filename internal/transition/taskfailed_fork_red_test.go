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

func redForkBindings() map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding {
	pick := redPick
	a := redPickBranchA
	sort := flowv1alpha1.Phase("red-sort")
	return map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		pick: {
			Handler: "pick",
			Next: map[flowv1alpha1.Phase]string{
				a:           "a",
				redTerminal: redRefuseDir,
			},
			Join: &flowv1alpha1.JoinSpec{Phase: sort},
		},
		a:    {Handler: "a", Next: map[flowv1alpha1.Phase]string{sort: "done"}},
		sort: {Handler: "sort", Next: map[flowv1alpha1.Phase]string{flowv1alpha1.Phase("red-end"): "ok"}},
	}
}

const redPickBranchA = flowv1alpha1.Phase("red-branch-a")

const redPick = flowv1alpha1.Phase("red-pick")

// Catches a fork's silence stopping at the old Escalated (#217).
func TestAForkWithoutAnAnswerEndsAtTaskFailed(t *testing.T) {
	got := Fork(Input{Bindings: redForkBindings(), Phase: redPick, NoAnswer: redSilentWhy,
		Runs: ranOnce(redPick), MaxRuns: 3})
	if got.Next != redTerminal || got.Outcome != OutcomeNoAnswer {
		t.Fatalf("got %q/%q, want TaskFailed/NoAnswer", got.Next, got.Outcome)
	}
}

// Catches a forked branch at its limit stopping at the old Escalated (#217).
func TestAForkedBranchAtItsLimitEndsAtTaskFailed(t *testing.T) {
	runs := ranOnce(redPick)
	runs[redPickBranchA] = 2
	got := Fork(Input{Bindings: redForkBindings(), Phase: redPick, Directory: "a",
		Runs: runs, MaxRuns: 2})
	if got.Next != redTerminal || got.Outcome != OutcomeRunLimitReached {
		t.Fatalf("got %q/%q, want TaskFailed/RunLimitReached", got.Next, got.Outcome)
	}
}

// Catches a broken fork stopping at the old Failed (#217).
func TestABrokenForkEndsAtTaskFailed(t *testing.T) {
	b := redForkBindings()
	delete(b, "red-sort")
	got := Fork(Input{Bindings: b, Phase: redPick, Directory: "a", Runs: ranOnce(redPick), MaxRuns: 2})
	if got.Next != redTerminal || got.Outcome != OutcomeStructural {
		t.Fatalf("got %q/%q, want TaskFailed/Structural", got.Next, got.Outcome)
	}
}

// Catches a fork's declared refusal reading as an ordinary edge (#217).
func TestAForkDeclaredRefusalIsDeclined(t *testing.T) {
	got := Fork(Input{Bindings: redForkBindings(), Phase: redPick, Directory: redRefuseDir,
		Runs: ranOnce(redPick), MaxRuns: 3})
	if got.Next != redTerminal || got.Outcome != OutcomeDeclined {
		t.Fatalf("got %q/%q, want TaskFailed/Declined", got.Next, got.Outcome)
	}
}

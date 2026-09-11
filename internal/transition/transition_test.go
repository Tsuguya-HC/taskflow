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
	"slices"
	"strings"
	"testing"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// A flow named in the author's own words, to keep honest the claim that the
// framework does not supply the vocabulary.
const (
	phaseInvestigate flowv1alpha1.Phase = "調査"
	phaseReport      flowv1alpha1.Phase = "報告"
	phaseDone        flowv1alpha1.Phase = "おわり"
	phaseGave        flowv1alpha1.Phase = "失敗"
)

func sampleFlow() map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding {
	return map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		phaseInvestigate: {
			Handler: "sample-handler",
			Next: map[flowv1alpha1.Phase]string{
				phaseReport:      dirOK,
				phaseInvestigate: dirMore,
			},
		},
		phaseReport: {
			Handler: "notify",
			Next:    map[flowv1alpha1.Phase]string{phaseDone: dirSent},
		},
	}
}

// The directories the example flow declares.
const (
	dirOK       = "ok"
	dirMore     = "more"
	dirSent     = "sent"
	dirEscalate = "escalate"
)

// withEscalate is the same flow with an escalate directory on 調査: somewhere
// for a run that will not conclude to say so, rather than only being able to
// write nothing.
func withEscalate() map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding {
	b := sampleFlow()
	b[phaseInvestigate].Next[flowv1alpha1.PhaseEscalated] = dirEscalate
	return b
}

func visited(phases ...flowv1alpha1.Phase) map[flowv1alpha1.Phase]bool {
	v := map[flowv1alpha1.Phase]bool{}
	for _, p := range phases {
		v[p] = true
	}
	return v
}

func TestDeclaredEdges(t *testing.T) {
	got := Next(Input{Bindings: sampleFlow(), Phase: phaseInvestigate, Directory: dirOK, Visited: visited(phaseInvestigate), Budget: 2})
	if got.Next != phaseReport || got.Outcome != OutcomeDeclared {
		t.Fatalf("got %q/%q, want 報告/Declared (%s)", got.Next, got.Outcome, got.Detail)
	}
	if got.Budget != 2 {
		t.Fatalf("budget = %d, want it untouched", got.Budget)
	}
}

// A status nobody bound is where the flow ends. Nothing declares it terminal,
// and there is no reserved name for success — "おわり" is just a status with
// nowhere to go.
func TestUnboundStatusIsWhereItStops(t *testing.T) {
	b := sampleFlow()
	if !IsTerminal(b, phaseDone) {
		t.Fatal("a status with no binding is the end of the flow")
	}
	if IsTerminal(b, phaseReport) {
		t.Fatal("a bound status is not the end")
	}
	for _, r := range flowv1alpha1.ReservedPhases {
		if !IsTerminal(b, r) {
			t.Fatalf("%q is always the end", r)
		}
	}
}

// The declaration is also the mount list: these are the only directories that
// will exist, so they are the only answers a handler can give.
func TestDirectoriesComeFromTheDeclaration(t *testing.T) {
	dirs := Directories(sampleFlow(), phaseInvestigate)
	slices.Sort(dirs)
	if !slices.Equal(dirs, []string{dirMore, dirOK}) {
		t.Fatalf("directories = %v, want [more ok]", dirs)
	}
	if Directories(sampleFlow(), "見たことない") != nil {
		t.Fatal("an unbound phase has no directories")
	}
}

func TestNoSingleAnswerEscalates(t *testing.T) {
	for _, why := range []string{"nothing was written", "two directories were written", "the run timed out"} {
		t.Run(why, func(t *testing.T) {
			got := Next(Input{Bindings: sampleFlow(), Phase: phaseInvestigate, NoAnswer: why, Visited: visited(phaseInvestigate), Budget: 2})
			if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeNoAnswer {
				t.Fatalf("got %q/%q, want Escalated/NoAnswer", got.Next, got.Outcome)
			}
			if got.Detail != why {
				t.Fatalf("detail = %q, want the reason carried through", got.Detail)
			}
		})
	}
}

// The fail-closed path (P6) needs a message even when the caller did not
// bother to say why — an empty NoAnswer must not become an empty Detail.
func TestNoSingleAnswerWithoutReasonGetsADefaultMessage(t *testing.T) {
	got := Next(Input{Bindings: sampleFlow(), Phase: phaseInvestigate, Visited: visited(phaseInvestigate), Budget: 2})
	if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeNoAnswer {
		t.Fatalf("got %q/%q, want Escalated/NoAnswer", got.Next, got.Outcome)
	}
	if got.Detail != "the run produced no single answer" {
		t.Fatalf("detail = %q, want the default message", got.Detail)
	}
}

// The handler cannot invent this — the directory would not exist — but a flow
// edited under a running task can leave one behind.
func TestUndeclaredDirectoryEscalates(t *testing.T) {
	got := Next(Input{Bindings: sampleFlow(), Phase: phaseInvestigate, Directory: "looks-fine", Visited: visited(phaseInvestigate), Budget: 2})
	if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeNoAnswer {
		t.Fatalf("got %q/%q, want Escalated/NoAnswer", got.Next, got.Outcome)
	}
}

// Writing into the declared escalate directory and writing nothing at all
// both stop the task at Escalated, and that is the point of separating them:
// the outcome is what tells a human whether there is a report to read or a
// run that died. Run 1 of the first real task escalated on max-turns and was
// indistinguishable in the history from a deliberate hand-off.
func TestDeclaredEscalationIsNotSilence(t *testing.T) {
	got := Next(Input{Bindings: withEscalate(), Phase: phaseInvestigate, Directory: dirEscalate,
		Visited: visited(phaseInvestigate), Budget: 2})
	if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeDeclined {
		t.Fatalf("got %q/%q, want Escalated/Declined (%s)", got.Next, got.Outcome, got.Detail)
	}
	if got.Budget != 2 {
		t.Fatalf("budget = %d, want it untouched — nothing runs after Escalated", got.Budget)
	}
	if !strings.Contains(got.Detail, dirEscalate) {
		t.Fatalf("detail = %q, want the directory named in it", got.Detail)
	}

	silent := Next(Input{Bindings: withEscalate(), Phase: phaseInvestigate, NoAnswer: "the run ran out of turns",
		Visited: visited(phaseInvestigate), Budget: 2})
	if silent.Next != got.Next {
		t.Fatalf("silence went to %q and a declared escalation to %q; both stop the task", silent.Next, got.Next)
	}
	if silent.Outcome == got.Outcome {
		t.Fatalf("both outcomes are %q; the history cannot tell a report from a run that died", got.Outcome)
	}
}

// An exhausted budget is what turns a rework into an escalation, and a
// declared escalation must not be mistaken for one: it is where the flow says
// to go, not the last resort after the flow ran out of room.
func TestDeclaredEscalationDoesNotConsultTheBudget(t *testing.T) {
	got := Next(Input{Bindings: withEscalate(), Phase: phaseInvestigate, Directory: dirEscalate,
		Visited: visited(phaseInvestigate, flowv1alpha1.PhaseEscalated), Budget: 0})
	if got.Outcome != OutcomeDeclined {
		t.Fatalf("outcome = %q, want Declined even with no budget and Escalated marked visited", got.Outcome)
	}
}

// The declaration is also what gets created on disk, so declaring the edge is
// the whole of what gives the run somewhere to write.
func TestTheEscalateDirectoryIsCreated(t *testing.T) {
	dirs := Directories(withEscalate(), phaseInvestigate)
	slices.Sort(dirs)
	if !slices.Equal(dirs, []string{dirEscalate, dirMore, dirOK}) {
		t.Fatalf("directories = %v, want the escalate directory among them", dirs)
	}
}

// What a task's stopping place means is the flow's to say, and the framework
// asks rather than assumes. The two reserved names answer for themselves.
func TestEndingOfReportsWhatStoppingThereMeans(t *testing.T) {
	flow := &flowv1alpha1.TaskFlowSpec{
		Bindings: sampleFlow(),
		Terminals: map[flowv1alpha1.Phase]flowv1alpha1.TerminalSeverity{
			phaseDone: flowv1alpha1.TerminalSuccess,
			phaseGave: flowv1alpha1.TerminalFailure,
		},
	}
	for _, tc := range []struct {
		name  string
		phase flowv1alpha1.Phase
		want  Ending
	}{
		{"a phase still bound to a handler", phaseInvestigate, EndingRunning},
		{"an ending declared a success", phaseDone, EndingSuccess},
		{"an ending declared a failure", phaseGave, EndingFailure},
		{"the framework's own escalation", flowv1alpha1.PhaseEscalated, EndingEscalated},
		{"the framework's own failure", flowv1alpha1.PhaseFailed, EndingFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := EndingOf(flow, tc.phase); got != tc.want {
				t.Fatalf("ending = %q, want %q", got, tc.want)
			}
		})
	}
}

// A flow written before terminals existed declares nothing, and silence is
// not consent: reporting it as Undeclared is what keeps "nobody has said"
// distinguishable from "somebody said this was fine".
func TestAnUndeclaredEndingIsNotASuccess(t *testing.T) {
	if got := EndingOf(&flowv1alpha1.TaskFlowSpec{Bindings: sampleFlow()}, phaseDone); got != EndingUndeclared {
		t.Fatalf("ending = %q, want Undeclared for a flow that never said", got)
	}
	partly := &flowv1alpha1.TaskFlowSpec{
		Bindings:  sampleFlow(),
		Terminals: map[flowv1alpha1.Phase]flowv1alpha1.TerminalSeverity{phaseGave: flowv1alpha1.TerminalFailure},
	}
	if got := EndingOf(partly, phaseDone); got != EndingUndeclared {
		t.Fatalf("ending = %q; declaring one ending must not speak for the others", got)
	}
}

// Escalated answers for itself before the bindings are consulted, which is
// what lets a task that reached it still be reported after its flow is gone.
func TestTheReservedEndingsNeedNoFlow(t *testing.T) {
	if got := EndingOf(nil, flowv1alpha1.PhaseEscalated); got != EndingEscalated {
		t.Fatalf("ending = %q, want Escalated with no flow to read", got)
	}
	if got := EndingOf(nil, flowv1alpha1.PhaseFailed); got != EndingFailed {
		t.Fatalf("ending = %q, want Failed with no flow to read", got)
	}
}

func TestBrokenFlowFails(t *testing.T) {
	t.Run("a phase with no binding", func(t *testing.T) {
		got := Next(Input{Bindings: sampleFlow(), Phase: "存在しない", Directory: dirOK, Budget: 2})
		if got.Next != flowv1alpha1.PhaseFailed || got.Outcome != OutcomeStructural {
			t.Fatalf("got %q/%q, want Failed/Structural", got.Next, got.Outcome)
		}
	})

	// Creation refuses this; a flow edited afterwards can still carry it, and
	// picking one of the two would be worse than stopping.
	t.Run("two statuses sharing a directory", func(t *testing.T) {
		b := sampleFlow()
		b[phaseInvestigate].Next["中止"] = dirOK
		got := Next(Input{Bindings: b, Phase: phaseInvestigate, Directory: dirOK, Visited: visited(phaseInvestigate), Budget: 2})
		if got.Next != flowv1alpha1.PhaseFailed || got.Outcome != OutcomeStructural {
			t.Fatalf("got %q/%q, want Failed/Structural", got.Next, got.Outcome)
		}
	})

	// Escalated may be declared as a destination; Failed may not. It says the
	// definition is broken, and a definition does not get to conclude that
	// about itself — so naming it is the break, and the outcome says so
	// rather than reading like an edge the flow was entitled to declare.
	t.Run("Failed declared as a destination", func(t *testing.T) {
		b := sampleFlow()
		b[phaseInvestigate].Next[flowv1alpha1.PhaseFailed] = "broken"
		got := Next(Input{Bindings: b, Phase: phaseInvestigate, Directory: "broken", Visited: visited(phaseInvestigate), Budget: 2})
		if got.Next != flowv1alpha1.PhaseFailed || got.Outcome != OutcomeStructural {
			t.Fatalf("got %q/%q, want Failed/Structural", got.Next, got.Outcome)
		}
	})
}

func TestReworkSpendsBudget(t *testing.T) {
	got := Next(Input{Bindings: sampleFlow(), Phase: phaseInvestigate, Directory: dirMore, Visited: visited(phaseInvestigate), Budget: 2})
	if got.Next != phaseInvestigate || got.Outcome != OutcomeRework {
		t.Fatalf("got %q/%q, want 調査/Rework", got.Next, got.Outcome)
	}
	if got.Budget != 1 {
		t.Fatalf("budget = %d, want 1", got.Budget)
	}
}

func TestReworkWithoutBudgetEscalates(t *testing.T) {
	got := Next(Input{Bindings: sampleFlow(), Phase: phaseInvestigate, Directory: dirMore, Visited: visited(phaseInvestigate), Budget: 0})
	if got.Next != flowv1alpha1.PhaseEscalated || got.Outcome != OutcomeBudgetExhausted {
		t.Fatalf("got %q/%q, want Escalated/BudgetExhausted", got.Next, got.Outcome)
	}
}

// The first version of this rule decremented unconditionally, which made
// reworkBudget: 0 mean "cannot run at all" instead of "never goes back".
func TestForwardEdgesIgnoreBudget(t *testing.T) {
	got := Next(Input{Bindings: sampleFlow(), Phase: phaseInvestigate, Directory: dirOK, Visited: visited(phaseInvestigate), Budget: 0})
	if got.Next != phaseReport || got.Outcome != OutcomeDeclared {
		t.Fatalf("got %q/%q, want 報告/Declared with no budget", got.Next, got.Outcome)
	}
	if got.Budget != 0 {
		t.Fatalf("budget = %d, want it untouched at 0 — a forward edge must not spend it", got.Budget)
	}
}

// Budget only ever decreases, so a cycle cannot run forever.
func TestCycleTerminates(t *testing.T) {
	budget := int32(2)
	phase := phaseInvestigate
	seen := visited(phaseInvestigate)

	for range 50 {
		got := Next(Input{Bindings: sampleFlow(), Phase: phase, Directory: dirMore, Visited: seen, Budget: budget})
		budget = got.Budget
		phase = got.Next
		seen[phase] = true
		if IsTerminal(sampleFlow(), phase) {
			if phase != flowv1alpha1.PhaseEscalated {
				t.Fatalf("terminated at %q, want Escalated once the budget ran out", phase)
			}
			if budget != 0 {
				t.Fatalf("ended with budget %d, want 0", budget)
			}
			return
		}
	}
	t.Fatal("did not terminate")
}

// The endings a flow declares have to be nameable before any task reaches
// one, because the counter that reports them is born at its first increment
// and a rise from nothing is invisible to a range function (ADR-0010).
func TestDeclaredEndingsAreTheStoppingPlaces(t *testing.T) {
	flow := &flowv1alpha1.TaskFlowSpec{
		Bindings:  sampleFlow(),
		Terminals: map[flowv1alpha1.Phase]flowv1alpha1.TerminalSeverity{phaseDone: flowv1alpha1.TerminalSuccess},
	}
	want := []PhaseEnding{
		{Phase: flowv1alpha1.PhaseEscalated, Ending: EndingEscalated},
		{Phase: flowv1alpha1.PhaseFailed, Ending: EndingFailed},
		{Phase: phaseDone, Ending: EndingSuccess},
	}
	if got := DeclaredEndings(flow); !slices.Equal(got, want) {
		t.Fatalf("endings = %v, want %v", got, want)
	}
}

// A phase still bound to a handler is not a place anything stops, so priming
// it would claim an ending that cannot happen.
func TestDeclaredEndingsLeaveOutBoundPhases(t *testing.T) {
	for _, got := range DeclaredEndings(&flowv1alpha1.TaskFlowSpec{Bindings: sampleFlow()}) {
		if got.Phase == phaseInvestigate || got.Phase == phaseReport {
			t.Fatalf("%q is bound to a handler and is not an ending", got.Phase)
		}
		if got.Ending == EndingRunning {
			t.Fatalf("%q was listed as an ending that means nothing", got.Phase)
		}
	}
}

// The framework's own two can happen to any flow — Escalated whenever an
// answer cannot be read, Failed whenever the flow turns out to be broken — so
// they are reported whether or not the flow names them, and naming one does
// not report it twice.
func TestDeclaredEndingsAlwaysIncludeTheReservedTwo(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding
	}{
		{"a flow that never mentions them", sampleFlow()},
		{"a flow that declares an escalate directory", withEscalate()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := DeclaredEndings(&flowv1alpha1.TaskFlowSpec{Bindings: tc.bindings})
			for _, reserved := range flowv1alpha1.ReservedPhases {
				n := 0
				for _, e := range got {
					if e.Phase == reserved {
						n++
					}
				}
				if n != 1 {
					t.Fatalf("%q appears %d times in %v, want exactly once", reserved, n, got)
				}
			}
		})
	}
}

// Silence stays distinguishable from consent here too: a flow that declared
// nothing primes its ending as Undeclared rather than as a success waiting to
// happen (P8).
func TestDeclaredEndingsKeepUndeclaredUndeclared(t *testing.T) {
	got := DeclaredEndings(&flowv1alpha1.TaskFlowSpec{Bindings: sampleFlow()})
	if !slices.Contains(got, PhaseEnding{Phase: phaseDone, Ending: EndingUndeclared}) {
		t.Fatalf("endings = %v, want おわり reported as Undeclared", got)
	}
}

// Ranging a map would make the primed order depend on the hash seed. The
// series are the same either way, but a test that reads the list must not
// depend on which run it is.
func TestDeclaredEndingsAreOrderedTheSameEveryTime(t *testing.T) {
	first := DeclaredEndings(&flowv1alpha1.TaskFlowSpec{Bindings: withEscalate()})
	for range 20 {
		if got := DeclaredEndings(&flowv1alpha1.TaskFlowSpec{Bindings: withEscalate()}); !slices.Equal(got, first) {
			t.Fatalf("endings = %v, want the same order as %v", got, first)
		}
	}
	if !slices.IsSortedFunc(first, func(a, b PhaseEnding) int { return strings.Compare(string(a.Phase), string(b.Phase)) }) {
		t.Fatalf("endings = %v, want them sorted by phase", first)
	}
}

// fail() reaches Failed with no flow at all, and asking a nil spec what it
// declares must not panic on the way there.
func TestDeclaredEndingsOfNothing(t *testing.T) {
	if got := DeclaredEndings(nil); got != nil {
		t.Fatalf("endings = %v, want none for a flow that is not there", got)
	}
}

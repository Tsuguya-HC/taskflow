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
	"github.com/Tsuguya-HC/taskflow/internal/contract"
)

// The example flow, in the author's own words — the same one design.md §16
// and the transition tests use, so that what admission accepts and what the
// controller can actually walk are visibly the same flow.
const (
	phaseInvestigate flowv1alpha1.Phase = "調査"
	phaseReport      flowv1alpha1.Phase = "報告"
	phaseDone        flowv1alpha1.Phase = "おわり"
	phaseGave        flowv1alpha1.Phase = "失敗"
)

const (
	dirOK       = "ok"
	dirMore     = "more"
	dirSent     = "sent"
	dirEscalate = "escalate"
)

// What the broken flows below are made of: a handler nobody wrote, a
// directory declared to reach Failed, and a name that is a path rather than
// a path element.
const (
	handlerNobody = "nobody"
	dirBroken     = "broken"
	dirNested     = "nested/sent"
)

// The field 報告's edge to おわり is reported under, and the reason a name
// that cannot be a directory is refused. Several cases break that one edge,
// each in a different way.
const (
	fieldReportToDone = `spec.bindings[報告].next[おわり]`
	notAPathElement   = "single path element"
)

// cnpCheck is a flow that should be accepted: 調査 either reports or asks for
// another round, 報告 ends at a phase nothing binds.
func cnpCheck() *flowv1alpha1.TaskFlowSpec {
	return &flowv1alpha1.TaskFlowSpec{
		Profile: flowv1alpha1.ProfileInvestigate,
		Start:   phaseInvestigate,
		Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
			phaseInvestigate: {
				Handler: "cnp-reader",
				Next: map[flowv1alpha1.Phase]string{
					phaseReport:      dirOK,
					phaseInvestigate: dirMore,
				},
			},
			phaseReport: {
				Handler: "discord",
				Next:    map[flowv1alpha1.Phase]string{phaseDone: dirSent},
			},
		},
	}
}

// check runs Check and returns the errors as "field: message" lines.
func check(spec *flowv1alpha1.TaskFlowSpec) []string {
	errs := Check(spec, field.NewPath("spec"))
	lines := make([]string, 0, len(errs))
	for _, e := range errs {
		lines = append(lines, e.Field+": "+e.ErrorBody())
	}
	return lines
}

func TestAcceptsAWellFormedFlow(t *testing.T) {
	if got := check(cnpCheck()); len(got) != 0 {
		t.Fatalf("a flow the design itself uses was refused: %v", got)
	}
}

// A flow may send work to Escalated on purpose — that is a conclusion, not
// silence (transition.OutcomeDeclined) — as long as it also has a way to
// finish.
func TestAcceptsADeclaredEdgeToEscalated(t *testing.T) {
	spec := cnpCheck()
	spec.Bindings[phaseInvestigate].Next[flowv1alpha1.PhaseEscalated] = dirEscalate
	if got := check(spec); len(got) != 0 {
		t.Fatalf("a declared escalation was refused: %v", got)
	}
}

// Two endings, one of them the flow's own bad news. Both are phases nothing
// binds, and declaring what they mean is terminals' business, not this one's.
func TestAcceptsSeveralEndings(t *testing.T) {
	spec := cnpCheck()
	spec.Bindings[phaseReport].Next[phaseGave] = "gave-up"
	spec.Terminals = map[flowv1alpha1.Phase]flowv1alpha1.TerminalSeverity{
		phaseDone: flowv1alpha1.TerminalSuccess,
		phaseGave: flowv1alpha1.TerminalFailure,
	}
	if got := check(spec); len(got) != 0 {
		t.Fatalf("a flow with two declared endings was refused: %v", got)
	}
}

// Each case breaks the accepted flow in exactly one way, and names the field
// the author has to go and look at. The message is matched loosely — what is
// being pinned is which field is blamed and that the reason is recognisable,
// not the wording.
func TestRefuses(t *testing.T) {
	cases := []struct {
		name    string
		break_  func(*flowv1alpha1.TaskFlowSpec)
		field   string
		mention string
	}{
		{
			name: "a start that binds nothing",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Start = "着手"
			},
			field:   "spec.start",
			mention: "must be one this flow binds",
		},
		{
			name: "Escalated bound to a handler",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[flowv1alpha1.PhaseEscalated] = flowv1alpha1.PhaseBinding{
					Handler: "human",
					Next:    map[flowv1alpha1.Phase]string{phaseDone: "handled"},
				}
			},
			field:   `spec.bindings[Escalated]`,
			mention: "framework's own answers",
		},
		{
			name: "Failed bound to a handler",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[flowv1alpha1.PhaseFailed] = flowv1alpha1.PhaseBinding{
					Handler: "human",
					Next:    map[flowv1alpha1.Phase]string{phaseDone: "handled"},
				}
			},
			field:   `spec.bindings[Failed]`,
			mention: "framework's own answers",
		},
		{
			name: "a binding under no name at all",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[""] = flowv1alpha1.PhaseBinding{
					Handler: handlerNobody,
					Next:    map[flowv1alpha1.Phase]string{phaseDone: "done"},
				}
			},
			field:   `spec.bindings[]`,
			mention: "no name",
		},
		{
			name: "an edge to Failed",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phaseInvestigate].Next[flowv1alpha1.PhaseFailed] = dirBroken
			},
			field:   `spec.bindings[調査].next[Failed]`,
			mention: "not something a run gets to conclude",
		},
		{
			name: "an edge leading nowhere named",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phaseReport].Next[""] = "nowhere"
			},
			field:   `spec.bindings[報告].next[]`,
			mention: "which phase it leads to",
		},
		{
			name: "two statuses selected by one directory",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phaseInvestigate].Next[phaseReport] = dirMore
			},
			field:   `spec.bindings[調査].next[調査]`,
			mention: "undecidable",
		},
		{
			name: "a directory that is a path rather than a name",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phaseReport].Next[phaseDone] = dirNested
			},
			field:   fieldReportToDone,
			mention: notAPathElement,
		},
		{
			name: "a directory that climbs out of the run",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phaseReport].Next[phaseDone] = ".."
			},
			field:   fieldReportToDone,
			mention: notAPathElement,
		},
		{
			name: "a directory with no name",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phaseReport].Next[phaseDone] = ""
			},
			field:   fieldReportToDone,
			mention: notAPathElement,
		},
		{
			name: "a directory wearing the pod's mark",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phaseReport].Next[phaseDone] = contract.MarkName
			},
			field:   fieldReportToDone,
			mention: "reserved for the pod's mark",
		},
		{
			name: "a phase nothing can reach",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings["棚上げ"] = flowv1alpha1.PhaseBinding{
					Handler: handlerNobody,
					Next:    map[flowv1alpha1.Phase]string{phaseDone: "shelved"},
				}
			},
			field:   `spec.bindings[棚上げ]`,
			mention: "no path from",
		},
		{
			name: "a flow with no way to finish",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phaseReport].Next[phaseDone] = ""
				delete(s.Bindings[phaseReport].Next, phaseDone)
				s.Bindings[phaseReport].Next[phaseInvestigate] = dirSent
			},
			field:   "spec.bindings",
			mention: "no task of this flow can finish",
		},
		{
			name: "a flow whose only way out is escalation",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				delete(s.Bindings[phaseReport].Next, phaseDone)
				s.Bindings[phaseReport].Next[flowv1alpha1.PhaseEscalated] = dirEscalate
			},
			field:   "spec.bindings",
			mention: "Escalated does not count",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := cnpCheck()
			tc.break_(spec)
			got := check(spec)
			for _, line := range got {
				if strings.HasPrefix(line, tc.field+":") && strings.Contains(line, tc.mention) {
					return
				}
			}
			t.Fatalf("wanted %s to be blamed for %q, got %v", tc.field, tc.mention, got)
		})
	}
}

// A start that binds nothing stops the walk, so the phases it cannot reach
// are not each reported as unreachable on top of it. One mistake, one line.
func TestAnUnboundStartIsReportedOnce(t *testing.T) {
	spec := cnpCheck()
	spec.Start = "着手"
	got := check(spec)
	if len(got) != 1 {
		t.Fatalf("wanted the unbound start alone, got %v", got)
	}
}

// Two mistakes in one flow are both reported: a rejection costs a commit, so
// the author is told everything at once rather than one apply at a time.
func TestReportsEveryMistakeAtOnce(t *testing.T) {
	spec := cnpCheck()
	spec.Bindings[phaseInvestigate].Next[flowv1alpha1.PhaseFailed] = dirBroken
	spec.Bindings[phaseReport].Next[phaseDone] = dirNested
	if got := check(spec); len(got) != 2 {
		t.Fatalf("wanted both mistakes, got %v", got)
	}
}

// The report is the same every time. Ranging a Go map would order it by the
// hash seed, so two identical applies of one wrong flow would disagree about
// what is wrong with it.
func TestTheReportIsStable(t *testing.T) {
	first := ""
	for i := range 32 {
		spec := cnpCheck()
		spec.Bindings[phaseInvestigate].Next[flowv1alpha1.PhaseFailed] = dirBroken
		spec.Bindings[phaseReport].Next[phaseDone] = dirNested
		spec.Bindings["棚上げ"] = flowv1alpha1.PhaseBinding{
			Handler: handlerNobody,
			Next:    map[flowv1alpha1.Phase]string{phaseDone: "shelved"},
		}
		got := strings.Join(check(spec), "\n")
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("the same flow produced two different reports:\n%s\n---\n%s", first, got)
		}
	}
}

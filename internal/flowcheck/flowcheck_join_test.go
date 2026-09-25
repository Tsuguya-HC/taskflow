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

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// The review flow ADR-0013 was written for: pick the perspectives, review
// each one at once, then sort the findings. tests runs whatever was picked.
const (
	phasePick     flowv1alpha1.Phase = "観点出し"
	phaseSecurity flowv1alpha1.Phase = "security"
	phaseLogic    flowv1alpha1.Phase = "logic"
	phaseTests    flowv1alpha1.Phase = "tests"
	phaseSort     flowv1alpha1.Phase = "仕分け"
	phaseFinished flowv1alpha1.Phase = "完了"
)

const (
	dirDone  = "done"
	dirStuck = "stuck"
)

func forkFlow() *flowv1alpha1.TaskFlowSpec {
	toSort := func() map[flowv1alpha1.Phase]string {
		return map[flowv1alpha1.Phase]string{phaseSort: dirDone, flowv1alpha1.PhaseEscalated: dirStuck}
	}
	return &flowv1alpha1.TaskFlowSpec{
		Profile: flowv1alpha1.ProfileInvestigate,
		Start:   phasePick,
		Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
			phasePick: {
				Handler: "pick",
				Next: map[flowv1alpha1.Phase]string{
					phaseSecurity:               "security",
					phaseLogic:                  "logic",
					flowv1alpha1.PhaseEscalated: dirStuck,
				},
				Join: &flowv1alpha1.JoinSpec{Phase: phaseSort, Always: []flowv1alpha1.Phase{phaseTests}},
			},
			phaseSecurity: {Handler: "review-security", Next: toSort()},
			phaseLogic:    {Handler: "review-logic", Next: toSort()},
			phaseTests:    {Handler: "review-tests", Next: toSort()},
			// Sending work back to the fork is an ordinary rework: it enters
			// the fork, not one of its branches.
			phaseSort: {
				Handler: "sort",
				Next:    map[flowv1alpha1.Phase]string{phaseFinished: "ok", phasePick: "again"},
			},
		},
	}
}

// tests is reached only through always — no edge names it — and must not be
// reported as unreachable for that.
func TestAcceptsAFork(t *testing.T) {
	if got := check(forkFlow()); len(got) != 0 {
		t.Fatalf("the fork ADR-0013 describes was refused: %v", got)
	}
}

func TestRefusesAMalformedFork(t *testing.T) {
	cases := []struct {
		name    string
		break_  func(*flowv1alpha1.TaskFlowSpec)
		field   string
		mention string
	}{
		{
			name:    "a join at a phase nothing binds",
			break_:  func(s *flowv1alpha1.TaskFlowSpec) { s.Bindings[phasePick].Join.Phase = "どこか" },
			field:   `spec.bindings[観点出し].join.phase`,
			mention: "must be one this flow binds",
		},
		{
			name:    "a fork that is its own join",
			break_:  func(s *flowv1alpha1.TaskFlowSpec) { s.Bindings[phasePick].Join.Phase = phasePick },
			field:   `spec.bindings[観点出し].join.phase`,
			mention: "its own branches meet",
		},
		{
			name:    "an edge from the fork straight to the join",
			break_:  func(s *flowv1alpha1.TaskFlowSpec) { s.Bindings[phasePick].Next[phaseSort] = "none" },
			field:   `spec.bindings[観点出し].next[仕分け]`,
			mention: "not supported yet",
		},
		{
			name: "a fork whose run can only refuse",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				delete(s.Bindings[phasePick].Next, phaseSecurity)
				delete(s.Bindings[phasePick].Next, phaseLogic)
			},
			field:   `spec.bindings[観点出し].join`,
			mention: "at least one destination",
		},
		{
			name: "always naming one of the fork's destinations",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phasePick].Join.Always = []flowv1alpha1.Phase{phaseSecurity}
			},
			field:   `spec.bindings[観点出し].join.always[0]`,
			mention: "already one of the fork's destinations",
		},
		{
			name: "always naming the join",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phasePick].Join.Always = []flowv1alpha1.Phase{phaseSort}
			},
			field:   `spec.bindings[観点出し].join.always[0]`,
			mention: "neither the fork",
		},
		{
			name: "always naming Escalated",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phasePick].Join.Always = []flowv1alpha1.Phase{flowv1alpha1.PhaseEscalated}
			},
			field:   `spec.bindings[観点出し].join.always[0]`,
			mention: "someone runs",
		},
		{
			name:    "a branch nothing binds",
			break_:  func(s *flowv1alpha1.TaskFlowSpec) { s.Bindings[phasePick].Next["未実装"] = "later" },
			field:   `spec.bindings[未実装]`,
			mention: "must be a phase this flow binds",
		},
		{
			name: "a branch that forks again",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				b := s.Bindings[phaseSecurity]
				b.Join = &flowv1alpha1.JoinSpec{Phase: phaseSort}
				s.Bindings[phaseSecurity] = b
			},
			field:   `spec.bindings[security].join`,
			mention: "forks again",
		},
		{
			name:    "a branch that leaves for somewhere other than the join",
			break_:  func(s *flowv1alpha1.TaskFlowSpec) { s.Bindings[phaseSecurity].Next[phaseFinished] = "skip" },
			field:   `spec.bindings[security].next[完了]`,
			mention: "leaves only",
		},
		{
			name: "a branch that never reaches the join",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				delete(s.Bindings[phaseLogic].Next, phaseSort)
			},
			field:   `spec.bindings[logic].next`,
			mention: "must be able to reach",
		},
		{
			name:    "a branch that is the start",
			break_:  func(s *flowv1alpha1.TaskFlowSpec) { s.Start = phaseSecurity },
			field:   `spec.bindings[security]`,
			mention: "entered from nowhere",
		},
		{
			name:    "an edge from outside into a branch",
			break_:  func(s *flowv1alpha1.TaskFlowSpec) { s.Bindings[phaseSort].Next[phaseSecurity] = "recheck" },
			field:   `spec.bindings[仕分け].next[security]`,
			mention: "entered only from there",
		},
		{
			name: "two forks sharing a branch",
			break_: func(s *flowv1alpha1.TaskFlowSpec) {
				s.Bindings[phaseSort].Next["再点検"] = "recheck"
				s.Bindings["再点検"] = flowv1alpha1.PhaseBinding{
					Handler: "pick",
					Next:    map[flowv1alpha1.Phase]string{phaseSecurity: "security"},
					Join:    &flowv1alpha1.JoinSpec{Phase: phaseSort},
				}
			},
			// Forks are judged in name order, so the second to claim the
			// branch is the one blamed.
			field:   `spec.bindings[観点出し].join`,
			mention: "belongs to one fork",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := forkFlow()
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

// A phase's name becomes a directory under inputs/ for whatever it leads to,
// so a name that is a path rather than a path element is refused for every
// binding — not only for flows that fork.
func TestRefusesAPhaseNameThatIsNotAPathElement(t *testing.T) {
	spec := sampleFlow()
	spec.Bindings[phaseReport].Next["棚/上げ"] = "shelve"
	spec.Bindings["棚/上げ"] = flowv1alpha1.PhaseBinding{
		Handler: handlerNobody,
		Next:    map[flowv1alpha1.Phase]string{phaseDone: "shelved"},
	}
	for _, line := range check(spec) {
		if strings.HasPrefix(line, `spec.bindings[棚/上げ]:`) && strings.Contains(line, "inputs view") {
			return
		}
	}
	t.Fatalf("a phase named with a slash was accepted: %v", check(spec))
}

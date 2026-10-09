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

// Catches the join.always old-name loop going missing: without it a fork
// listing Escalated or Failed as an always branch passes silently (#217).
func TestRefusesOldTerminalsInJoinAlways(t *testing.T) {
	for _, old := range []flowv1alpha1.Phase{redEscalated, redFailed} {
		t.Run(string(old), func(t *testing.T) {
			spec := forkFlow()
			spec.Bindings[phasePick].Join.Always = []flowv1alpha1.Phase{old}
			got := check(spec)
			for _, line := range got {
				if strings.HasPrefix(line, fieldPickAlways+":") && strings.Contains(line, "TaskFailed") {
					return
				}
			}
			t.Fatalf("wanted %s refused at %s with TaskFailed named, got %v", old, fieldPickAlways, got)
		})
	}
}

// Catches the terminals old-name loop going missing: without it a flow
// declaring Escalated or Failed as a terminal passes silently (#217).
func TestRefusesOldTerminalsInTerminals(t *testing.T) {
	for _, old := range []flowv1alpha1.Phase{redEscalated, redFailed} {
		t.Run(string(old), func(t *testing.T) {
			spec := sampleFlow()
			spec.Terminals = map[flowv1alpha1.Phase]flowv1alpha1.TerminalSeverity{
				old: flowv1alpha1.TerminalSuccess,
			}
			got := check(spec)
			for _, line := range got {
				if strings.Contains(line, string(old)) && strings.Contains(line, "TaskFailed") {
					return
				}
			}
			t.Fatalf("wanted %s refused with TaskFailed named, got %v", old, got)
		})
	}
}

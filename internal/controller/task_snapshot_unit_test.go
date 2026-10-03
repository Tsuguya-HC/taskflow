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

package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// snapshotRevisionName is the one piece of the copy the tests pin without a
// cluster: every reconcile derives it, and a collision there decides whether a
// recreated task starts on a stale copy.
func TestSnapshotRevisionName(t *testing.T) {
	task := func(name string, uid types.UID) *flowv1alpha1.Task {
		return &flowv1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid},
		}
	}

	// The UID digest is what keeps a recreated task from taking the deleted
	// task's leftover revision for its own.
	t.Run("names a different revision for a recreated task under the same name", func(t *testing.T) {
		first := snapshotRevisionName(task("task", "uid-1"))
		second := snapshotRevisionName(task("task", "uid-2"))
		if first == second {
			t.Fatalf("snapshotRevisionName = %q for both, want different names", first)
		}
		if !strings.Contains(first, "task") || !strings.Contains(second, "task") {
			t.Fatalf("names = %q, %q; want both to contain the task name", first, second)
		}
	})

	// A name the apiserver would refuse is a reconcile that never reaches the
	// copy, so the cut keeps the readable prefix and drops the rest.
	t.Run("fits the longest task names into the object name limit", func(t *testing.T) {
		if got := snapshotRevisionName(task(strings.Repeat("t", 200), "uid-1")); len(got) != 63 {
			t.Fatalf("snapshotRevisionName length = %d, want 63", len(got))
		}
	})
}

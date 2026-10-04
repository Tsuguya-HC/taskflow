package runner

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

// A task name already at the object-name limit has to be cut to leave room
// for the suffix; the mutant that drops the cut produces a name the apiserver
// refuses, and the one that drops the UID hash makes two generations of the
// same task share one copy.
func TestSnapshotRevisionNameFitsAndKeepsTheUID(t *testing.T) {
	long := strings.Repeat("t", maxNameLength)
	a := SnapshotRevisionName(long, types.UID("uid-a"))
	b := SnapshotRevisionName(long, types.UID("uid-b"))
	if len(a) > maxNameLength {
		t.Fatalf("name %q is %d long, past the %d an object name may be", a, len(a), maxNameLength)
	}
	if a == b {
		t.Fatalf("two UIDs under one long name got the same revision name %q", a)
	}
	if short := SnapshotRevisionName("t", types.UID("uid-a")); !strings.HasPrefix(short, "t-snapshot-") {
		t.Fatalf("a short name is cut anyway: %q", short)
	}
}

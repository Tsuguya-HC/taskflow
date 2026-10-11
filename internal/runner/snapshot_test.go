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

func TestSnapshotRevisionNameCutsAtTheLimit(t *testing.T) {
	uid := types.UID("uid-a")
	suffixLength := len("-snapshot-") + taskHashLength
	fits := maxNameLength - suffixLength

	for _, tc := range []struct {
		name       string
		nameLength int
		wantPrefix int
	}{
		{"exactly at the limit", fits, fits},
		{"one past the limit", fits + 1, fits},
		{"far past the limit", 80, fits},
	} {
		t.Run(tc.name, func(t *testing.T) {
			taskName := strings.Repeat("x", tc.nameLength)
			got := SnapshotRevisionName(taskName, uid)

			wantLength := min(tc.nameLength+suffixLength, maxNameLength)
			if len(got) != wantLength {
				t.Fatalf("len(%q) = %d; want %d", got, len(got), wantLength)
			}
			if prefix := taskName[:tc.wantPrefix] + "-snapshot-"; !strings.HasPrefix(got, prefix) {
				t.Fatalf("%q does not start with the first %d characters of the task name", got, tc.wantPrefix)
			}
			hash := got[tc.wantPrefix+len("-snapshot-"):]
			if len(hash) != taskHashLength || strings.Trim(hash, "0123456789abcdef") != "" {
				t.Fatalf("hash %q is not %d hex digits", hash, taskHashLength)
			}
			if !strings.HasSuffix(SnapshotRevisionName("t", uid), hash) {
				t.Fatalf("hash %q differs from the one the same UID gets under a short name", hash)
			}
		})
	}
}

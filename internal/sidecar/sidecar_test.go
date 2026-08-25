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

package sidecar

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tsuguya/taskflow/internal/collect"
)

var declared = []string{"ok", "more"}

func prepared(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	if err := Prepare(out, declared); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	// The closed out directory cannot be removed by TempDir's cleanup as is.
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })
	return out
}

func write(t *testing.T, out, dir, file string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(out, dir, file), []byte("x"), 0o644); err != nil {
		t.Fatalf("write %s/%s: %v", dir, file, err)
	}
}

func TestPrepareLaysDownExactlyTheDeclaration(t *testing.T) {
	out := prepared(t)

	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
		if !e.IsDir() {
			t.Fatalf("%s is not a directory", e.Name())
		}
	}
	if strings.Join(names, ",") != "more,ok" {
		t.Fatalf("out holds %v, want exactly the declaration", names)
	}
}

func TestPrepareClosesOutAndOpensTheChildren(t *testing.T) {
	out := prepared(t)

	st, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != modeOut {
		t.Fatalf("out mode = %o, want %o so nothing else can be created there", got, modeOut)
	}
	for _, name := range declared {
		st, err := os.Stat(filepath.Join(out, name))
		if err != nil {
			t.Fatal(err)
		}
		if got := st.Mode().Perm(); got != modeDir {
			t.Fatalf("%s mode = %o, want %o so the agent's group can write", name, got, modeDir)
		}
	}
}

// The same uid can always chmod its way back in, so this cannot show the
// cross-uid refusal the design relies on. It shows the half a unit test can:
// with out closed, a create under it is refused for anyone without the owner's
// power to reopen it.
func TestClosedOutRefusesAnUndeclaredDirectory(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory modes")
	}
	out := prepared(t)
	err := os.Mkdir(filepath.Join(out, "escalate"), 0o755)
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("mkdir out/escalate: err = %v, want permission denied", err)
	}
}

func TestPrepareIsRepeatable(t *testing.T) {
	out := prepared(t)
	write(t, out, "ok", "report.md")

	if err := Prepare(out, declared); err != nil {
		t.Fatalf("second Prepare: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out, "ok", "report.md")); err != nil {
		t.Fatalf("a second prepare must not remove what is there: %v", err)
	}
	st, _ := os.Stat(out)
	if st.Mode().Perm() != modeOut {
		t.Fatalf("out was left at %o after the second prepare", st.Mode().Perm())
	}
}

func TestPrepareRefusesANameThatIsNotOnePathElement(t *testing.T) {
	for _, bad := range []string{"", ".", "..", "a/b", "../x", "a\x00b"} {
		err := Prepare(filepath.Join(t.TempDir(), "out"), []string{"ok", bad})
		if !errors.Is(err, ErrBadName) {
			t.Fatalf("Prepare with %q: err = %v, want ErrBadName", bad, err)
		}
	}
}

func TestSealFindsTheOneDirectoryWrittenInto(t *testing.T) {
	out := prepared(t)
	write(t, out, "more", "report.md")

	got := Seal(out, declared)
	if got.Directory != "more" {
		t.Fatalf("directory = %q (%s)", got.Directory, got.Reason)
	}
	if !strings.Contains(got.Reason, "report.md") {
		t.Fatalf("reason = %q; it should say what was written", got.Reason)
	}
}

func TestSealOfNothingWrittenIsNoAnswer(t *testing.T) {
	out := prepared(t)
	got := Seal(out, declared)
	if got.Directory != "" || got.Reason == "" {
		t.Fatalf("got %+v", got)
	}
}

func TestSealOfTwoDirectoriesIsNoAnswer(t *testing.T) {
	out := prepared(t)
	write(t, out, "ok", "a")
	write(t, out, "more", "b")

	got := Seal(out, declared)
	if got.Directory != "" {
		t.Fatalf("directory = %q; two answers are no answer", got.Directory)
	}
	if !strings.Contains(got.Reason, "ok") || !strings.Contains(got.Reason, "more") {
		t.Fatalf("reason = %q; it should name both", got.Reason)
	}
}

// An entry that is itself an empty directory still counts as written into:
// the rule is about existence, and reading deeper would be a parser.
func TestSealCountsAnyEntry(t *testing.T) {
	out := prepared(t)
	if err := os.Mkdir(filepath.Join(out, "ok", "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	if got := Seal(out, declared); got.Directory != "ok" {
		t.Fatalf("directory = %q (%s)", got.Directory, got.Reason)
	}
}

// summarize's own truncation, driven through Seal: one more entry than
// maxListed must name only the first maxListed and say how many more there
// were, rather than listing everything.
func TestSealSummarizesManyEntries(t *testing.T) {
	out := prepared(t)
	for i := range maxListed + 1 {
		write(t, out, "ok", fmt.Sprintf("f%d", i))
	}

	got := Seal(out, declared)
	if got.Directory != "ok" {
		t.Fatalf("directory = %q (%s)", got.Directory, got.Reason)
	}
	for i := range maxListed {
		if !strings.Contains(got.Reason, fmt.Sprintf("f%d", i)) {
			t.Fatalf("reason = %q; want it to name f%d among the first %d entries", got.Reason, i, maxListed)
		}
	}
	if strings.Contains(got.Reason, fmt.Sprintf("f%d", maxListed)) {
		t.Fatalf("reason = %q; the %dth entry must not be named", got.Reason, maxListed+1)
	}
	if !strings.Contains(got.Reason, "and 1 more") {
		t.Fatalf("reason = %q; want it to say how many entries were left out", got.Reason)
	}
}

func TestSealOfAMissingDeclaredDirectoryIsNoAnswer(t *testing.T) {
	out := prepared(t)
	write(t, out, "ok", "a")
	_ = os.Chmod(out, 0o755)
	if err := os.RemoveAll(filepath.Join(out, "more")); err != nil {
		t.Fatal(err)
	}

	got := Seal(out, declared)
	if got.Directory != "" {
		t.Fatalf("directory = %q; a broken vocabulary must not yield an answer", got.Directory)
	}
}

func TestSealWithoutPrepareIsNoAnswer(t *testing.T) {
	got := Seal(filepath.Join(t.TempDir(), "never-made"), declared)
	if got.Directory != "" || got.Reason == "" {
		t.Fatalf("got %+v", got)
	}
}

// A PVC shared across attempts lets a symlink stand in for a declared
// directory before this one's Prepare runs. Following it would hand
// os.Chmod's power to open a directory for the agent's group to whatever the
// link points at.
func TestPrepareRefusesASymlinkStandingInForADeclaredDirectory(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "out")
	elsewhere := filepath.Join(root, "elsewhere")
	if err := os.MkdirAll(elsewhere, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(out, "ok")); err != nil {
		t.Fatal(err)
	}

	err := Prepare(out, declared)
	if !errors.Is(err, ErrNotADirectory) {
		t.Fatalf("Prepare with a symlink in place of %q: err = %v, want ErrNotADirectory", "ok", err)
	}
}

// Seal must not follow a symlink either: swapping one in after Prepare
// closed out is the same attack from the other end of the run, and it must
// come through as no answer rather than as whatever the link points at.
func TestSealTreatsASymlinkDeclaredDirectoryAsNoAnswer(t *testing.T) {
	out := prepared(t)
	elsewhere := t.TempDir()
	if err := os.WriteFile(filepath.Join(elsewhere, "sneaked-in"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	_ = os.Chmod(out, 0o755)
	if err := os.RemoveAll(filepath.Join(out, "ok")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(out, "ok")); err != nil {
		t.Fatal(err)
	}

	got := Seal(out, declared)
	if got.Directory != "" {
		t.Fatalf("directory = %q; a symlink standing in for a declared directory must not count as an answer", got.Directory)
	}
	if !strings.Contains(got.Reason, "is not a directory") {
		t.Fatalf("reason = %q, want it to say ok is not a directory", got.Reason)
	}
}

// The two ends of the channel must agree. Whatever seal writes, collect on
// the controller side must read back to the same directory — and a no-answer
// must come through as no answer, never as a directory.
func TestMessageRoundTripsThroughCollect(t *testing.T) {
	out := prepared(t)
	write(t, out, "ok", "report.md")

	for _, tc := range []struct {
		name string
		ans  Answer
		want string
	}{
		{"answer", Seal(out, declared), "ok"},
		{"no answer", Answer{Reason: "nothing was written into any of ok, more"}, ""},
		{"reason that starts with a name", Answer{Reason: "ok was not written"}, ""},
	} {
		msg := tc.ans.Message()
		got := collect.FromPod(podWith(msg), declared)
		if got.Directory != tc.want {
			t.Fatalf("%s: message %q read back as %q, want %q", tc.name, msg, got.Directory, tc.want)
		}
	}
}

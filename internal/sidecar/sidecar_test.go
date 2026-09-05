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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tsuguya-HC/taskflow/internal/collect"
)

var declared = []string{"ok", "more"}

// prepared is a run directory Prepare has laid the vocabulary down in and
// closed — what the handler sees at the root of its mount.
func prepared(t *testing.T) string {
	t.Helper()
	run := filepath.Join(t.TempDir(), "3")
	if err := Prepare(run, declared); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	// The closed run directory cannot be removed by TempDir's cleanup as is.
	t.Cleanup(func() { _ = os.Chmod(run, 0o755) })
	return run
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
		t.Fatalf("the run directory holds %v, want exactly the declaration", names)
	}
}

func TestPrepareClosesTheRunAndOpensTheChildren(t *testing.T) {
	out := prepared(t)

	st, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := st.Mode().Perm(); got != modeRun {
		t.Fatalf("run mode = %o, want %o so nothing else can be created there", got, modeRun)
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
		t.Fatalf("mkdir escalate/ in the closed run: err = %v, want permission denied", err)
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
	if st.Mode().Perm() != modeRun {
		t.Fatalf("the run was left at %o after the second prepare", st.Mode().Perm())
	}
}

func TestPrepareRefusesANameThatIsNotOnePathElement(t *testing.T) {
	for _, bad := range []string{"", ".", "..", "a/b", "../x", "a\x00b"} {
		err := Prepare(filepath.Join(t.TempDir(), "3"), []string{"ok", bad})
		if !errors.Is(err, ErrBadName) {
			t.Fatalf("Prepare with %q: err = %v, want ErrBadName", bad, err)
		}
	}
}

// A flow declaring markName as one of its own directories would let Prepare's
// Mkdir land on the file Mark already wrote there — ErrNotADirectory on every
// run. checkName has to refuse the name outright, ahead of that collision.
func TestPrepareRefusesTheMarkNameAsADeclaration(t *testing.T) {
	err := Prepare(filepath.Join(t.TempDir(), "3"), []string{"ok", markName})
	if !errors.Is(err, ErrBadName) {
		t.Fatalf("Prepare with %q declared: err = %v, want ErrBadName", markName, err)
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
	out := filepath.Join(root, "3")
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

// What Move shelves is the run exactly as prepare closed it: the declared
// directories at its root, and the directory closed. A closed directory
// cannot be renamed to another parent as is (its own ".." is rewritten,
// which needs write permission on it), so Move has to open it for the
// rename — and what the shelf must then show is a run closed again.
func TestMoveRelocatesTheDirectoryAndKeepsItClosed(t *testing.T) {
	root := t.TempDir()
	from := filepath.Join(root, "work", "3")
	to := filepath.Join(root, "results", "3")
	if err := Prepare(from, declared); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(to, 0o755); _ = os.Chmod(from, 0o755) })
	if err := os.WriteFile(filepath.Join(from, "ok", "report.md"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := Move(from, to); err != nil {
		t.Fatalf("Move: %v", err)
	}
	if _, err := os.Stat(filepath.Join(to, "ok", "report.md")); err != nil {
		t.Fatalf("results/3 does not hold what work/3 wrote: %v", err)
	}
	if _, err := os.Lstat(from); !os.IsNotExist(err) {
		t.Fatalf("work/3 still exists after the move: %v", err)
	}
	st, err := os.Stat(to)
	if err != nil || st.Mode().Perm() != modeRun {
		t.Fatalf("results/3 = %v (%v); the shelved run must be closed, the same %o prepare left it at", st, err, modeRun)
	}
}

// A rename that fails outright — not the existing-destination check above,
// which returns before os.Rename is ever called — must still leave the run
// as it was found: closed, and on work/, rather than opened by the attempt
// and left that way for the next one. work/ itself is closed to force the
// rename to fail there rather than at the Lstat(to) early return.
func TestMoveRenameFailureLeavesTheRunClosed(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	root := t.TempDir()
	work := filepath.Join(root, "work")
	from := filepath.Join(work, "3")
	to := filepath.Join(root, "results", "3")
	if err := Prepare(from, declared); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(work, 0o755); _ = os.Chmod(from, 0o755) })
	// os.Rename needs write permission on the source's parent to remove its
	// entry there; closing work/ itself denies exactly that, ahead of
	// anything Move's own from-side chmod opens.
	if err := os.Chmod(work, 0o555); err != nil {
		t.Fatal(err)
	}

	if err := Move(from, to); err == nil {
		t.Fatal("Move must fail when the rename itself is refused")
	}
	st, err := os.Stat(from)
	if err != nil || st.Mode().Perm() != modeRun {
		t.Fatalf("work/3 = %v (%v); a failed rename must leave the run closed where it was", st, err)
	}
	if _, err := os.Lstat(to); !os.IsNotExist(err) {
		t.Fatalf("results/3 must not exist after a failed rename: %v", err)
	}
}

// A shelf that cannot even be created must fail Move before it ever touches
// the run: results/ has never been made, and closing root itself denies the
// MkdirAll that would make it. from must be left exactly as Prepare closed
// it, since nothing past the shelf's creation was reached.
func TestMoveFailsWhenTheShelfCannotBeCreated(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	root := t.TempDir()
	from := filepath.Join(root, "work", "3")
	to := filepath.Join(root, "results", "3")
	if err := Prepare(from, declared); err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755); _ = os.Chmod(from, 0o755) })
	if err := os.Chmod(root, 0o555); err != nil {
		t.Fatal(err)
	}

	if err := Move(from, to); err == nil {
		t.Fatal("Move must fail when the shelf cannot be created")
	}
	st, err := os.Stat(from)
	if err != nil || st.Mode().Perm() != modeRun {
		t.Fatalf("work/3 = %v (%v); a failed Move must leave the run closed and untouched", st, err)
	}
}

// os.Rename onto an existing *empty* directory succeeds silently on POSIX —
// the one case that matters most, an answer already sealed under this run's
// number, is exactly the one a bare Rename would not refuse. Move has to
// check for it itself.
func TestMoveRefusesAnExistingDestination(t *testing.T) {
	root := t.TempDir()
	from := filepath.Join(root, "work", "3")
	to := filepath.Join(root, "results", "3")
	if err := os.MkdirAll(from, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(to, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := Move(from, to); err == nil {
		t.Fatal("Move over an existing results/3 must be refused")
	}
	if _, err := os.Stat(from); err != nil {
		t.Fatalf("a refused move must leave work/3 in place: %v", err)
	}
}

// A run whose work/<runID> was never made — publish invoked against the
// wrong path, or a prior failure that never got this far — must not succeed
// silently; os.Rename's own ENOENT is what Move is expected to surface.
func TestMoveFailsWhenTheSourceIsMissing(t *testing.T) {
	root := t.TempDir()
	from := filepath.Join(root, "work", "3")
	to := filepath.Join(root, "results", "3")

	if err := Move(from, to); err == nil {
		t.Fatal("Move of a source that was never made must fail")
	}
	if _, err := os.Stat(to); !os.IsNotExist(err) {
		t.Fatalf("a failed move must not create the destination: %v", err)
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

func TestMakeRunCreatesAnOpenDirectory(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "3")
	if err := MakeRun(dir); err != nil {
		t.Fatalf("MakeRun: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Fatalf("stat %s: %v", dir, err)
	}
	if got := info.Mode().Perm(); got != modeOpen {
		t.Fatalf("mode = %o; the run's directory is open for Mark and Prepare to fill in, and Prepare closes it", got)
	}
	if err := MakeRun(dir); err != nil {
		t.Fatalf("MakeRun must tolerate a directory that already exists: %v", err)
	}
}

// A symlink standing in for the run's own directory must not be followed:
// MkdirAll finds it already "exists" (Stat follows the link), so it is
// checkRealDir's Lstat that has to catch it — the same attack
// TestPrepareRefusesASymlinkStandingInForADeclaredDirectory catches from the
// other end, on the run's own children rather than the run directory itself.
func TestMakeRunRefusesASymlinkStandingInForTheRunDirectory(t *testing.T) {
	root := t.TempDir()
	elsewhere := filepath.Join(root, "elsewhere")
	if err := os.MkdirAll(elsewhere, 0o755); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "3")
	if err := os.Symlink(elsewhere, dir); err != nil {
		t.Fatal(err)
	}

	err := MakeRun(dir)
	if !errors.Is(err, ErrNotADirectory) {
		t.Fatalf("MakeRun with a symlink standing in for the run directory: err = %v, want ErrNotADirectory", err)
	}
}

// A run keeps its number across infrastructure retries, so the second
// attempt's prepare finds the first attempt's directory sitting where it is
// about to work. Leftovers from an attempt that reached the agent would
// otherwise be there to be read as this attempt's own.
func TestMakeRunClearsWhatTheLastAttemptLeft(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "3")
	if err := os.MkdirAll(filepath.Join(dir, "ok"), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, "ok", "report.md")
	if err := os.WriteFile(stale, []byte("the last attempt's answer"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := MakeRun(dir); err != nil {
		t.Fatalf("MakeRun over the last attempt's directory: %v", err)
	}

	if _, err := os.Lstat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the last attempt's report survived into this one: err = %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("run directory is not empty: %v", entries)
	}
}

// A dir that exists but cannot be cleared must fail MakeRun outright rather
// than starting work on top of whatever RemoveAll left behind — the same
// bargain Sweep strikes with the abandoned runs, applied to this run's own
// leftovers.
func TestMakeRunFailsWhenTheExistingDirectoryCannotBeRemoved(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	root := t.TempDir()
	dir := filepath.Join(root, "3")
	if err := os.MkdirAll(filepath.Join(dir, "ok"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })

	if err := MakeRun(dir); err == nil {
		t.Fatal("MakeRun must fail when the existing run directory cannot be removed")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("a failed MakeRun must leave the existing directory alone: %v", err)
	}
}

// The mark is laid ahead of the vocabulary and closed in with it: once
// Prepare has run, the agent's uid can neither remove nor rewrite it, the
// same way it cannot touch the declared directories' set — and the mark
// stays out of Seal's way, which reads only the declared names.
func TestMarkIsClosedInWithTheVocabulary(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "3")
	if err := Mark(out, "pod-a"); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	if err := Prepare(out, declared); err != nil {
		t.Fatalf("Prepare over a marked run: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })

	if err := CheckMark(out, "pod-a"); err != nil {
		t.Fatalf("CheckMark with the pod that marked it: %v", err)
	}
	info, err := os.Lstat(filepath.Join(out, markName))
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o022 != 0 {
		t.Fatalf("mark = %v (%v); want a regular file nobody but its owner can write", info, err)
	}
	if got := Seal(out, declared); got.Directory != "" || !strings.Contains(got.Reason, "nothing was written") {
		t.Fatalf("Seal over a marked run = %+v; the mark must not count as an answer", got)
	}
	if os.Getuid() != 0 {
		if err := os.Remove(filepath.Join(out, markName)); err == nil {
			t.Fatal("the mark could be removed through the closed run directory")
		}
	}
}

// A publish whose pod is not the one that prepared the directory has found
// another attempt's run at its path, and must not touch it.
func TestCheckMarkRefusesAnotherPodsRun(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "3")
	if err := Mark(out, "pod-a"); err != nil {
		t.Fatalf("Mark: %v", err)
	}

	err := CheckMark(out, "pod-b")
	if !errors.Is(err, ErrMark) {
		t.Fatalf("CheckMark as another pod: err = %v, want ErrMark", err)
	}
	if !strings.Contains(err.Error(), "pod-a") || !strings.Contains(err.Error(), "pod-b") {
		t.Fatalf("err = %q; a human reading it should see both pods", err)
	}
}

// No mark at all means nothing of this binary's prepared the directory — a
// run laid down under an older sidecar, or a path that is not a run at all.
// Neither is this pod's to shelve.
func TestCheckMarkRefusesAnUnmarkedRun(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "3")
	if err := os.MkdirAll(filepath.Join(out, "ok"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := CheckMark(out, "pod-a"); !errors.Is(err, ErrMark) {
		t.Fatalf("CheckMark over an unmarked run: err = %v, want ErrMark", err)
	}
}

// Mark runs on the directory MakeRun just cleared, so a mark already there
// is a directory that is not the fresh one it was meant to be. Exclusive
// creation is what refuses it — WriteFile would truncate and carry on.
func TestMarkRefusesToOverwrite(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "3")
	if err := Mark(out, "pod-a"); err != nil {
		t.Fatalf("Mark: %v", err)
	}

	if err := Mark(out, "pod-b"); err == nil {
		t.Fatal("Mark over an existing mark must be refused")
	}
	if err := CheckMark(out, "pod-a"); err != nil {
		t.Fatalf("a refused Mark must leave the existing one intact: %v", err)
	}
}

// A symlink standing in for the run would carry Mark's write, and Prepare's
// chmod after it, wherever the link points — the same substitution
// MakeRun and Prepare refuse on their own targets.
func TestMarkRefusesASymlinkStandingInForTheRun(t *testing.T) {
	root := t.TempDir()
	elsewhere := filepath.Join(root, "elsewhere")
	if err := os.MkdirAll(elsewhere, 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(root, "3")
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, out); err != nil {
		t.Fatal(err)
	}

	if err := Mark(out, "pod-a"); !errors.Is(err, ErrNotADirectory) {
		t.Fatalf("Mark through a symlink: err = %v, want ErrNotADirectory", err)
	}
	if _, err := os.Lstat(filepath.Join(elsewhere, markName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the mark landed where the link pointed: err = %v", err)
	}
}

// CheckMark must not follow a symlink standing in for the mark either: Lstat
// catches it ahead of ReadFile, which would otherwise happily read through to
// whatever the link points at — even a file naming the right pod.
func TestCheckMarkRefusesASymlinkStandingInForTheMark(t *testing.T) {
	root := t.TempDir()
	out := filepath.Join(root, "3")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	elsewhere := filepath.Join(root, "elsewhere")
	if err := os.WriteFile(elsewhere, []byte("pod-a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(out, markName)); err != nil {
		t.Fatal(err)
	}

	if err := CheckMark(out, "pod-a"); !errors.Is(err, ErrMark) {
		t.Fatalf("CheckMark with a symlink standing in for the mark: err = %v, want ErrMark", err)
	}
}

func TestSweepRemovesOnlyTheListed(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"1", "2", "3"} {
		if err := os.MkdirAll(filepath.Join(root, d, "ok"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := Sweep(root, []string{"1", "2"}, "3"); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	for _, d := range []string{"1", "2"} {
		if _, err := os.Stat(filepath.Join(root, d)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("run %s survived the sweep", d)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "3")); err != nil {
		t.Fatalf("run 3 is not on the list and must survive: %v", err)
	}
	if err := Sweep(root, []string{"9"}, "3"); err != nil {
		t.Fatalf("a run that never wrote anything has nothing to remove; that is fine: %v", err)
	}
}

// A run that will not go must fail Sweep outright rather than being silently
// skipped: new work must not start on a volume in a disputed state. Removal
// is denied by closing off root itself, the same as
// TestClosedOutRefusesAnUndeclaredDirectory does for a create — root's own
// uid can always chmod its way back in, so this only shows the half a unit
// test can.
func TestSweepFailsWhenARunCannotBeRemoved(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "1", "ok"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(root, 0o755) })

	if err := Sweep(root, []string{"1"}, "2"); err == nil {
		t.Fatal("Sweep must fail when a run's directory cannot be removed")
	}
	if _, err := os.Stat(filepath.Join(root, "1")); err != nil {
		t.Fatalf("a failed sweep must leave the run's directory alone: %v", err)
	}
}

func TestSweepRefusesABadList(t *testing.T) {
	root := t.TempDir()
	for _, ids := range [][]string{{"../results"}, {""}, {"3"}} {
		if err := Sweep(root, ids, "3"); err == nil {
			t.Fatalf("Sweep(%v) accepted a list it should refuse", ids)
		}
	}
}

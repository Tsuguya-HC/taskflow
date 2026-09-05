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

// Package sidecar is the two ends of the verdict channel that live inside the
// pod: prepare makes the run's directory and lays the declared directories
// down in it before the agent runs, and seal reads which one was written
// into after it has finished. When the task's flow brings a workspace,
// publish's closing act is Move — a rename that shelves the now-sealed run
// where a later phase reads completed runs back from, once and only once its
// answer is decided.
//
// The run's directory is the vocabulary: the declared directories sit
// directly in it, and it is what the handler's own containers see at the
// root of their mount (ADR-0005). It knows that layout and nothing else.
// What the directories are called comes from the flow, through the
// environment; where the run's directory is comes from the controller,
// through a flag. There is no store here and no credentials — sealing the
// workspace's contents somewhere durable is a separate concern, and this
// package does not read what the agent wrote.
package sidecar

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

const (
	// modeRun is what the run's directory is left at once its children
	// exist: readable and traversable, not writable. The agent, running as
	// another uid, can then neither add a directory the flow did not declare
	// nor remove one it did. The kernel enforces the vocabulary.
	modeRun fs.FileMode = 0o555
	// modeDir is what each declared directory gets. Writable by anyone,
	// because the agent runs as whatever uid and gid the handler chose and
	// nothing here should oblige it to line up with prepare's. "Anyone" is
	// the containers sharing this pod's volume, which is the same set a
	// group-writable mode would admit once the pod's fsGroup is applied —
	// it just no longer asks the handler to know what group that is.
	modeDir fs.FileMode = 0o777
	// modeOpen is what the run's directory is temporarily at while its
	// children are made.
	modeOpen fs.FileMode = 0o755

	// maxListed bounds how many entries a sealed directory names in its
	// reason. The reason is a line for a human, not a manifest.
	maxListed = 8

	// markName is the file Mark leaves in the run's directory naming the
	// pod whose prepare laid the run down, and which CheckMark reads back
	// before the run is sealed. In the run's directory, beside the declared
	// directories: the directory is closed to the agent once prepared, so
	// the mark is as safe from a stray rm -rf as the vocabulary is. A
	// dotfile, so a plain ls shows the vocabulary and nothing else. It
	// travels with the run onto results/, where it says which pod produced
	// what is there.
	markName = ".prepared-by"
)

// ErrMark reports a run directory whose mark is not this pod's — it was
// prepared by another pod, and is that pod's to shelve.
var ErrMark = errors.New("run directory was prepared by another pod")

// ErrBadName reports a declared directory name that cannot be a single path
// element. Admission is meant to refuse these at creation; the sidecar refuses
// them again because it must not depend on admission having run.
var ErrBadName = errors.New("directory name is not a single path element")

// ErrNotADirectory reports a declared name whose entry already exists but is
// not itself a directory — most concerningly a symlink. Prepare's own uid can
// always chmod its way past a directory it made; a symlink left by a prior
// attempt sharing the same PVC is the one substitution that would carry that
// power somewhere else, so it is refused rather than followed.
var ErrNotADirectory = errors.New("existing entry is not a directory")

// Answer is what seal found. It mirrors collect.Answer on the other side of
// the termination message, and is written there in the same shape: the
// directory on the first line, the reason after it.
type Answer struct {
	// Directory that was written into, when exactly one was.
	Directory string
	// Reason is a line for a human — what the directory held, or why no
	// directory counted.
	Reason string
}

// checkName refuses anything that is not a plain single path element, and
// markName itself: a flow declaring that name would let Prepare's Mkdir land
// on the file Mark already put there, failing every run with
// ErrNotADirectory.
func checkName(name string) error {
	if name == "" || name == "." || name == ".." ||
		strings.ContainsAny(name, "/\x00") || filepath.Base(name) != name {
		return fmt.Errorf("%w: %q", ErrBadName, name)
	}
	if name == markName {
		return fmt.Errorf("%w: %q is reserved for the pod's mark", ErrBadName, name)
	}
	return nil
}

// checkRealDir refuses anything at path that is not itself a directory —
// Lstat rather than Stat, so a symlink is caught rather than followed.
func checkRealDir(path string) error {
	st, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if !st.Mode().IsDir() {
		return fmt.Errorf("%w: %s", ErrNotADirectory, path)
	}
	return nil
}

// Prepare creates run/<name> for each declared name and then closes the
// run's directory itself to writing. It runs before the agent, as a user the
// agent is not.
//
// It is safe to run on a directory that already exists — one from a
// previous, interrupted attempt in the same pod is reopened, filled in, and
// closed again — but it never removes anything; MakeRun, ahead of it, is
// what clears a run.
func Prepare(run string, declared []string) error {
	for _, name := range declared {
		if err := checkName(name); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(run, modeOpen); err != nil {
		return fmt.Errorf("create %s: %w", run, err)
	}
	// MkdirAll leaves an existing directory's mode alone, and it may be the
	// closed 0555 from an earlier attempt; open it for the duration.
	if err := os.Chmod(run, modeOpen); err != nil {
		return fmt.Errorf("open %s for writing: %w", run, err)
	}

	for _, name := range declared {
		dir := filepath.Join(run, name)
		if err := os.Mkdir(dir, modeDir); err != nil {
			if !errors.Is(err, fs.ErrExist) {
				return fmt.Errorf("create %s: %w", dir, err)
			}
			// Reused from a previous attempt in the same pod — but only if
			// it is actually a directory. os.Chmod below follows a symlink,
			// so an entry a concurrent attempt swapped in would otherwise
			// hand it whatever the link points at.
			if err := checkRealDir(dir); err != nil {
				return err
			}
		}
		// Mkdir's mode is subject to the umask; set it outright.
		if err := os.Chmod(dir, modeDir); err != nil {
			return fmt.Errorf("set mode on %s: %w", dir, err)
		}
	}

	if err := os.Chmod(run, modeRun); err != nil {
		return fmt.Errorf("close %s to writing: %w", run, err)
	}
	return nil
}

// Seal reads which declared directory the run wrote into.
//
// One rule, the same one the controller applies to the termination message:
// exactly one declared directory is non-empty, or there is no answer. Nothing
// inside a directory is read — an entry's existence is the whole signal, and
// a stray file is as good as a report for deciding that something was said.
func Seal(run string, declared []string) Answer {
	if _, err := os.Stat(run); err != nil {
		return Answer{Reason: fmt.Sprintf("%s is not there: %v", run, err)}
	}

	type written struct {
		name    string
		entries []string
	}
	var found []written
	for _, name := range slices.Sorted(slices.Values(declared)) {
		if err := checkName(name); err != nil {
			return Answer{Reason: err.Error()}
		}
		dir := filepath.Join(run, name)
		// Lstat rather than Stat: os.ReadDir below would follow a symlink,
		// and a declared name a concurrent attempt sharing the same PVC
		// swapped out for one could make a non-empty directory somewhere
		// else pass for this run's answer.
		st, err := os.Lstat(dir)
		if err != nil {
			// A declared directory that is not there means prepare did not
			// run, or something removed it. Either way the vocabulary is
			// broken, and guessing from what is left would be worse.
			return Answer{Reason: fmt.Sprintf("declared directory %s is missing: %v", name, err)}
		}
		if !st.Mode().IsDir() {
			return Answer{Reason: fmt.Sprintf("declared directory %s is not a directory", name)}
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return Answer{Reason: fmt.Sprintf("declared directory %s is missing: %v", name, err)}
		}
		if len(entries) == 0 {
			continue
		}
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		found = append(found, written{name, names})
	}

	switch len(found) {
	case 1:
		return Answer{Directory: found[0].name, Reason: "wrote " + summarize(found[0].entries)}
	case 0:
		return Answer{Reason: "nothing was written into any of " + strings.Join(declared, ", ")}
	default:
		names := make([]string, 0, len(found))
		for _, w := range found {
			names = append(names, w.name)
		}
		return Answer{Reason: fmt.Sprintf("%d directories were written into (%s); one was expected",
			len(found), strings.Join(names, ", "))}
	}
}

// summarize names a few of a directory's entries for a human.
func summarize(entries []string) string {
	if len(entries) <= maxListed {
		return strings.Join(entries, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(entries[:maxListed], ", "), len(entries)-maxListed)
}

// Move shelves a sealed run: a rename of its directory from where it was
// prepared and written (from, on work/) to where a later phase reads finished
// runs back (to, on results/). One rename within the same volume, so the
// shelf never shows a run halfway.
//
// It refuses a destination that already exists. A results/<runID> already
// there means either this run somehow sealed twice — which the numbering
// says can never have run again — or something else put it there first;
// either way, renaming over it would erase whatever is already sealed
// there. The check runs before MkdirAll below rather than trusting
// os.Rename's own semantics for it, since renaming a directory onto an
// existing *empty* one succeeds silently on POSIX rather than failing — the
// one case that would matter most, an answer already sitting there, is
// exactly the case a bare Rename would not refuse.
//
// The run's directory is closed (0555) by the time it is moved, and a
// directory being moved to a different parent has its own ".." rewritten,
// which Linux permits only with write permission on that directory —
// exactly what the closing withholds, from prepare's own uid as much as
// from the agent's (measured 2026-09-05: EACCES). So the move opens the
// directory to its owner first, and closes it again once it stands on the
// shelf. Owner-only (0755), so nothing running as another uid gains
// anything in between; the answer was read before this is reached, and the
// agent's containers are already stopped. Every step failing is an error:
// a run left open on the shelf is a run whose seal a reader can no longer
// trust from its mode alone.
//
// If closing it back up on the shelf fails, the rename is undone rather than
// left standing open: a run keeps its number across infrastructure retries
// (ADR-0004), so the next attempt at this same runID would otherwise find to
// already there and refuse forever at the Lstat above, with no way for
// anything to shelve this run again.
func Move(from, to string) error {
	if _, err := os.Lstat(to); err == nil {
		return fmt.Errorf("%s already exists; refusing to move %s over it", to, from)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("stat %s: %w", to, err)
	}

	shelf := filepath.Dir(to)
	if err := os.MkdirAll(shelf, modeDir); err != nil {
		return fmt.Errorf("create %s: %w", shelf, err)
	}
	// MkdirAll leaves an existing directory's mode alone, and the shelf may
	// already be there from an earlier run's publish, running as a
	// different uid; either way MkdirAll's own mode is subject to the
	// umask, so it is set outright rather than trusted.
	if err := os.Chmod(shelf, modeDir); err != nil {
		return fmt.Errorf("open %s for writing: %w", shelf, err)
	}

	if err := checkRealDir(from); err != nil {
		return err
	}
	if err := os.Chmod(from, modeOpen); err != nil {
		return fmt.Errorf("open %s for the move: %w", from, err)
	}
	if err := os.Rename(from, to); err != nil {
		// Leave it as it was found: closed, and where it was.
		_ = os.Chmod(from, modeRun)
		return fmt.Errorf("move %s to %s: %w", from, to, err)
	}
	if err := os.Chmod(to, modeRun); err != nil {
		// Undo the rename rather than leave the run open on the shelf: the
		// next attempt at this runID must still find work/<runID> free to
		// move into, not results/<runID> already there and refused forever.
		if rerr := os.Rename(to, from); rerr != nil {
			return fmt.Errorf("close %s on the shelf: %w (and moving it back to %s failed: %v)", to, err, from, rerr)
		}
		_ = os.Chmod(from, modeRun)
		return fmt.Errorf("close %s on the shelf: %w", to, err)
	}
	return nil
}

// Message renders an answer the way the controller reads it: the directory
// on its own first line, the reason after. When there is no directory the
// first line is the reason itself, which by construction is not a declared
// name — so the controller sees no answer, without any second protocol for
// saying so.
func (a Answer) Message() string {
	if a.Directory == "" {
		return a.Reason
	}
	return a.Directory + "\n" + a.Reason
}

// MakeRun creates a run's own directory under work/, open for the moment:
// Mark and Prepare fill it in, and Prepare closes it. prepare makes it,
// rather than leaving it to the kubelet's subPath machinery, so that what is
// closed is prepare's own — a directory the kubelet made is root's, and a
// chmod on it comes back EPERM (measured 2026-08-30). Made this way, the
// directory is the vocabulary itself, and it is what the handler's mount,
// pinned to it, shows at its root (ADR-0005).
//
// Anything already at the path goes first. A run keeps its number across
// infrastructure retries, so on the second attempt this is where the first
// attempt's leftovers are, and starting work on top of them would mix two
// attempts in one directory. The removal is Sweep's bargain applied to this
// run rather than the abandoned ones: a directory that will not go means
// some zombie still holds a file open in it, and the run fails rather than
// starting work on a volume in a disputed state. On the first attempt there
// is nothing there and this costs a syscall.
//
// Only a real directory is cleared. Anything else standing at the path —
// most concerningly a symlink, which RemoveAll would unlink without a word,
// leaving the check below to pass on the fresh directory made in its place —
// is refused instead, the same answer checkRealDir gives afterwards.
func MakeRun(dir string) error {
	switch st, err := os.Lstat(dir); {
	case err == nil && !st.Mode().IsDir():
		return fmt.Errorf("%w: %s", ErrNotADirectory, dir)
	case err == nil:
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("clear %s: %w", dir, err)
		}
	case !errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("stat %s: %w", dir, err)
	}
	if err := os.MkdirAll(dir, modeOpen); err != nil {
		return fmt.Errorf("create %s: %w", dir, err)
	}
	if err := checkRealDir(dir); err != nil {
		return err
	}
	if err := os.Chmod(dir, modeOpen); err != nil {
		return fmt.Errorf("open %s for writing: %w", dir, err)
	}
	return nil
}

// Mark records id in the run's directory as the identity of the pod that
// prepared this run, for CheckMark in the same pod's publish to read back
// before the run is sealed. It runs after MakeRun and ahead of Prepare,
// while the directory is still open — Prepare closes it, and the mark is
// meant to be closed in with the vocabulary.
//
// A run keeps its number across infrastructure retries (ADR-0004), so two
// attempts at one run use the same work/<runID> at different times. A pod
// whose object is gone but whose processes are not — a node that lost the
// apiserver and came back — still gets its SIGTERM eventually, and its
// publish would then seal and shelve whatever is at that path: the attempt
// running there now. Under the old numbering the two never met; with the
// number held still, the mark is what tells a publish whose directory it is
// looking at.
//
// The file is created exclusively rather than written over. MakeRun has
// just cleared the run, so nothing should be there; anything that is means
// the path is not the fresh directory it was meant to be.
func Mark(run, id string) error {
	if err := os.MkdirAll(run, modeOpen); err != nil {
		return fmt.Errorf("create %s: %w", run, err)
	}
	if err := checkRealDir(run); err != nil {
		return err
	}
	path := filepath.Join(run, markName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("mark %s: %w", path, err)
	}
	if _, err := f.WriteString(id); err != nil {
		_ = f.Close()
		return fmt.Errorf("mark %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("mark %s: %w", path, err)
	}
	return nil
}

// CheckMark reads the mark Mark left in the run's directory and refuses
// anything but id:
// a missing mark as much as a foreign one, since a run with no mark was not
// prepared by this binary at all. It is publish's gate on the move — and on
// sealing, which comes first: an answer read out of another pod's directory
// is not this run's answer and should not even reach the log.
//
// There is a window between this check and the rename that follows it, in
// which another attempt's MakeRun could clear and remake the directory. It
// is the width of a few stats against a process that was frozen for
// minutes, and what it costs is a fail-closed run (the moved directory is
// missing where the live attempt looks for it), not a wrong verdict.
func CheckMark(run, id string) error {
	path := filepath.Join(run, markName)
	st, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrMark, path, err)
	}
	if !st.Mode().IsRegular() {
		return fmt.Errorf("%w: %s is not a regular file", ErrMark, path)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if string(got) != id {
		return fmt.Errorf("%w: %s names %q, this pod is %q", ErrMark, path, string(got), id)
	}
	return nil
}

// Sweep removes the work/ leftovers of the runs it is told to — and only
// those. The list is the controller's: which runs are live is only visible
// where the cluster's state is, so what to delete is decided there and merely
// executed here, next to the volume (ADR-0003). Every id has to look like a
// run number and none may name this run itself; both are defenses against a
// mangled invocation, not expected traffic.
//
// A directory already gone is fine — an abandoned run may never have written
// anything, and a sweep may simply be running again. A directory that will
// not go (NFS keeps one alive while some zombie still holds a file in it
// open) is an error, and failing the run over it is deliberate: new work
// does not start on a volume in a disputed state.
func Sweep(root string, ids []string, self string) error {
	for _, id := range ids {
		if id == "" || strings.Trim(id, "0123456789") != "" {
			return fmt.Errorf("sweep id %q is not a run number", id)
		}
		if id == self {
			return fmt.Errorf("sweep id %q names this run itself", id)
		}
		if err := os.RemoveAll(filepath.Join(root, id)); err != nil {
			return fmt.Errorf("sweep run %s: %w", id, err)
		}
	}
	return nil
}

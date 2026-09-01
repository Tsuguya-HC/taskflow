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
// pod: prepare lays the declared directories down before the agent runs, and
// seal reads which one was written into after it has finished. When the
// task's flow brings a workspace, publish's closing act is Move — a rename
// that shelves the now-sealed run where a later phase reads completed runs
// back from, once and only once its answer is decided.
//
// It knows the layout and nothing else. What the directories are called comes
// from the flow, through the environment; where they go comes from whoever
// wrote the pod spec, through a flag. There is no store here and no
// credentials — sealing the workspace's contents somewhere durable is a
// separate concern, and this package does not read what the agent wrote.
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
	// modeOut is what the out directory is left at once its children exist:
	// readable and traversable, not writable. The agent, running as another
	// uid, can then neither add a directory the flow did not declare nor
	// remove one it did. The kernel enforces the vocabulary.
	modeOut fs.FileMode = 0o555
	// modeDir is what each declared directory gets. Writable by anyone,
	// because the agent runs as whatever uid and gid the handler chose and
	// nothing here should oblige it to line up with prepare's. "Anyone" is
	// the containers sharing this pod's volume, which is the same set a
	// group-writable mode would admit once the pod's fsGroup is applied —
	// it just no longer asks the handler to know what group that is.
	modeDir fs.FileMode = 0o777
	// modeOpen is what out is temporarily at while its children are made.
	modeOpen fs.FileMode = 0o755

	// maxListed bounds how many entries a sealed directory names in its
	// reason. The reason is a line for a human, not a manifest.
	maxListed = 8
)

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

// checkName refuses anything that is not a plain single path element.
func checkName(name string) error {
	if name == "" || name == "." || name == ".." ||
		strings.ContainsAny(name, "/\x00") || filepath.Base(name) != name {
		return fmt.Errorf("%w: %q", ErrBadName, name)
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

// Prepare creates out/<name> for each declared name and then closes out
// itself to writing. It runs before the agent, as a user the agent is not.
//
// It is safe to run on an out that already exists — a directory from a
// previous, interrupted attempt in the same pod is reopened, filled in, and
// closed again — but it never removes anything.
func Prepare(out string, declared []string) error {
	for _, name := range declared {
		if err := checkName(name); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(out, modeOpen); err != nil {
		return fmt.Errorf("create %s: %w", out, err)
	}
	// MkdirAll leaves an existing directory's mode alone, and it may be the
	// closed 0555 from an earlier attempt; open it for the duration.
	if err := os.Chmod(out, modeOpen); err != nil {
		return fmt.Errorf("open %s for writing: %w", out, err)
	}

	for _, name := range declared {
		dir := filepath.Join(out, name)
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

	if err := os.Chmod(out, modeOut); err != nil {
		return fmt.Errorf("close %s to writing: %w", out, err)
	}
	return nil
}

// Seal reads which declared directory the run wrote into.
//
// One rule, the same one the controller applies to the termination message:
// exactly one declared directory is non-empty, or there is no answer. Nothing
// inside a directory is read — an entry's existence is the whole signal, and
// a stray file is as good as a report for deciding that something was said.
func Seal(out string, declared []string) Answer {
	if _, err := os.Stat(out); err != nil {
		return Answer{Reason: fmt.Sprintf("%s is not there: %v", out, err)}
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
		dir := filepath.Join(out, name)
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

// Move relocates a run's directory from the work/ shelf it wrote into onto
// the results/ shelf a later phase reads back, once Seal has already decided
// the run's answer. from and to are expected on the same volume, so the
// rename is a single atomic step rather than a copy — the controller only
// ever calls this with paths under one task's own claim.
//
// to must not already exist. results/<runID> being there already means
// either an earlier publish already moved this run and this one should
// never have run again, or something else put it there first; either way,
// renaming over it would erase whatever is already sealed there; the check
// runs before MkdirAll below rather than trusting os.Rename's own semantics
// for it, since renaming a directory onto an existing *empty* one succeeds
// silently on POSIX rather than failing — the one case that would matter
// most, an answer already sitting there, is exactly the case a bare Rename
// would not refuse.
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

	if err := os.Rename(from, to); err != nil {
		return fmt.Errorf("move %s to %s: %w", from, to, err)
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

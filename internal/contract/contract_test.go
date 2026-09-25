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

package contract

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// The rule these exercise is run at both ends — admission when a flow is
// written, the sidecar when a run is prepared (ADR-0006 決定5) — and each end
// has its own tests that a refusal arrives. Those pass for whatever this
// function happens to say, so they are not a test of the rule: they would
// keep passing if the rule started accepting "../..".

// A declared directory is made by name inside the run's own directory, so
// anything that is not a single element under it is refused rather than
// interpreted. Most cases here are refused by one clause of the rule alone,
// so dropping that clause would go unnoticed behind another: "." and ".."
// only by their own literal check, "/" and "o\x00k" only by ContainsAny.
// "" and "ok/sub" are each caught twice over (by the empty/ContainsAny check
// and by filepath.Base(name) != name), and no case here isolates the Base
// check the way the others do — deleting it left every case in this table
// still refused, so this table cannot say that clause is pulling its weight.
func TestRefusesANameThatIsNotOneElementUnderTheRunsDirectory(t *testing.T) {
	for _, name := range []string{
		"",       // nothing to make
		".",      // the run's own directory
		"..",     // its parent, which is work/, not the run's own directory
		"/",      // a path, not a name — caught before Base sees it
		"ok/sub", // a path, not a name
		"o\x00k", // not a name any filesystem would take
	} {
		err := CheckDirectoryName(name)
		if !errors.Is(err, ErrBadDirectoryName) {
			t.Errorf("CheckDirectoryName(%q) = %v, want ErrBadDirectoryName", name, err)
			continue
		}
		// The flow's author sees this message with none of our context: the
		// name has to be in it, or a flow declaring several directories says
		// only that one of them is wrong.
		if !strings.Contains(err.Error(), fmt.Sprintf("%q", name)) {
			t.Errorf("CheckDirectoryName(%q) = %q, which does not say which name", name, err)
		}
	}
}

// The flow's author picks these words, and the rule may not narrow that to
// what a label value or an object name would allow — the whole reason the
// phase is an annotation rather than a label is that these strings are free.
func TestAcceptsTheNamesAFlowsAuthorMayChoose(t *testing.T) {
	for _, name := range []string{
		"ok",
		"調査",        // not a legal label value, and that is the point
		"more work", // a space is why the vocabulary travels as JSON
		".hidden",   // a leading dot is not "." and not ".."
		"...",
	} {
		if err := CheckDirectoryName(name); err != nil {
			t.Errorf("CheckDirectoryName(%q) = %v, want accepted", name, err)
		}
	}
}

// prepare writes its pod's UID into MarkName in the same directory it lays
// the declared directories down in, so a flow that declares this name would
// fail every run. Refusing it is the controller's side of a rule the sidecar
// enforces anyway — and it has to follow MarkName, not a copy of its current
// spelling, or moving the mark would quietly open the collision again.
func TestRefusesTheNameThePodsMarkOccupies(t *testing.T) {
	if err := CheckDirectoryName(MarkName); !errors.Is(err, ErrBadDirectoryName) {
		t.Fatalf("CheckDirectoryName(MarkName=%q) = %v, want ErrBadDirectoryName", MarkName, err)
	}
}

// A fork's answer has one spelling: the same directories read the same
// whichever order they came in, and the input is left as it was.
func TestJoinDirectories(t *testing.T) {
	in := []string{"security", "logic"}
	if got := JoinDirectories(in); got != "logic"+DirectorySeparator+"security" {
		t.Fatalf("JoinDirectories = %q", got)
	}
	if in[0] != "security" {
		t.Fatal("JoinDirectories sorted its caller's slice in place")
	}
	if got := JoinDirectories([]string{"ok"}); got != "ok" {
		t.Fatalf("one directory joins to itself, got %q", got)
	}
}

// The separator is the one character a directory name can never contain, so
// a joined answer splits back into exactly the names that went in.
func TestTheSeparatorIsNeverInAName(t *testing.T) {
	if err := CheckDirectoryName("a" + DirectorySeparator + "b"); err == nil {
		t.Fatal("a name containing the separator was accepted")
	}
}

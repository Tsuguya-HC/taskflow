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

package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Tsuguya/taskflow/internal/contract"
)

// termLog returns the path to a termination-log file that already exists,
// the way the kubelet's bind mount would leave it, and its current contents.
func termLog(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "termination-log")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// runArgs builds the argument list run() takes for one subcommand, so the
// flag names are spelled once rather than at every call site.
func runArgs(cmd, out, log string) []string {
	return []string{cmd, "-out", out, "-termination-log", log}
}

func TestRunWithNoArgsIsAnError(t *testing.T) {
	if err := run(nil); err == nil {
		t.Fatal("run(nil): want an error, got nil")
	}
}

func TestRunWithAnUnknownSubcommandIsAnError(t *testing.T) {
	if err := run([]string{"launch"}); err == nil {
		t.Fatal(`run(["launch"]): want an error, got nil`)
	}
}

func TestPrepareWithoutFlowDirectoriesFailsAndReportsIt(t *testing.T) {
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "out")

	err := run(runArgs(cmdPrepare, out, log))
	if err == nil {
		t.Fatal("want an error when FLOW_DIRECTORIES is unset")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "prepare failed:")
	}
}

func TestPrepareWithInvalidJSONFailsAndReportsIt(t *testing.T) {
	t.Setenv(contract.EnvDirectories, "not json")
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "out")

	err := run(runArgs(cmdPrepare, out, log))
	if err == nil {
		t.Fatal("want an error when FLOW_DIRECTORIES is not a JSON array")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "prepare failed:")
	}
}

func TestPublishWithoutFlowDirectoriesFailsAndReportsIt(t *testing.T) {
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "out")

	err := run(runArgs(cmdPublish, out, log))
	if err == nil {
		t.Fatal("want an error when FLOW_DIRECTORIES is unset")
	}
	if got := readFile(t, log); !strings.Contains(got, "publish failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "publish failed:")
	}
}

func TestPublishWithInvalidJSONFailsAndReportsIt(t *testing.T) {
	t.Setenv(contract.EnvDirectories, "not json")
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "out")

	err := run(runArgs(cmdPublish, out, log))
	if err == nil {
		t.Fatal("want an error when FLOW_DIRECTORIES is not a JSON array")
	}
	if got := readFile(t, log); !strings.Contains(got, "publish failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "publish failed:")
	}
}

func TestPrepareLaysDownTheDeclaredDirectories(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok", "more"]`)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "out")
	// prepare closes out to 0o555 on success; reopen it so TempDir's cleanup
	// can remove what is inside.
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })

	if err := run(runArgs(cmdPrepare, out, log)); err != nil {
		t.Fatalf("run(prepare): %v", err)
	}
	for _, name := range []string{"ok", "more"} {
		if st, err := os.Stat(filepath.Join(out, name)); err != nil || !st.IsDir() {
			t.Fatalf("out/%s was not created: %v", name, err)
		}
	}
}

func TestPrepareWithABadNameFailsAndReportsIt(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["a/b"]`)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "out")

	err := run(runArgs(cmdPrepare, out, log))
	if err == nil {
		t.Fatal("want an error for a declared name that is not a single path element")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "prepare failed:")
	}
}

// A native sidecar is stopped with SIGTERM. This drives run("publish")
// through exactly that: it waits on the process's own signal channel, so
// sending the process a real SIGTERM is what exercises the same path a
// kubelet-stopped container takes, rather than a stand-in for it.
func TestPublishSealsAndReportsOnSIGTERM(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "out")
	// prepare closes out to 0o555 on success; reopen it so TempDir's cleanup
	// can remove what is inside.
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })
	if err := run(runArgs(cmdPrepare, out, log)); err != nil {
		t.Fatalf("run(prepare): %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- run(runArgs(cmdPublish, out, log))
	}()

	// run("publish") only starts waiting after NotifyContext is registered;
	// there is no signal back for "now waiting", so give it a moment before
	// sending SIGTERM.
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("kill: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run(publish): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run(publish) did not return after SIGTERM")
	}

	if got := readFile(t, log); got == "" {
		t.Fatal("termination log is empty; publish must report an answer once stopped")
	}
}

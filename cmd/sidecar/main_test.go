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

	"github.com/Tsuguya-HC/taskflow/internal/contract"
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

// runArgsWithSeal is runArgs plus the two flags a flow-workspace publish
// takes, so a run's directory moves from sealFrom to sealTo once sealed.
func runArgsWithSeal(cmd, out, log, sealFrom, sealTo string) []string {
	return append(runArgs(cmd, out, log), "-seal-from", sealFrom, "-seal-to", sealTo)
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

// One seal flag without the other is refused outright, before publish even
// waits for SIGTERM — a controller-built Job always sets both together
// (runner.injectSidecars), so reaching this at all means something else
// wrote the command line, and silently sealing only would strand the run in
// work/ forever with nothing to say why it never reached results/.
func TestPublishWithOnlyOneSealFlagIsRefused(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "out")

	err := run(append(runArgs(cmdPublish, out, log), "-seal-from", "/somewhere"))
	if err == nil {
		t.Fatal("publish with only -seal-from set must be refused")
	}
	if got := readFile(t, log); !strings.Contains(got, "publish failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "publish failed:")
	}
}

// -seal-from must be -out's parent, the same shape prepare's own -run-dir
// check enforces from the other end: a controller-built Job always sets
// -out under -seal-from (runner.injectSidecars), so a mismatch here is a
// mangled command line, not a run to attempt sealing.
func TestPublishSealFromMustParentOut(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "out")

	err := run(append(runArgs(cmdPublish, out, log), "-seal-from", "/somewhere-else", "-seal-to", "/somewhere-else-2"))
	if err == nil {
		t.Fatal("publish with a -seal-from that is not -out's parent must be refused")
	}
	if got := readFile(t, log); !strings.Contains(got, "publish failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "publish failed:")
	}
}

// The seal flags are publish's alone; prepare never moves anything, so a
// prepare command line naming either is a misconfiguration to fail on, the
// same as any other flag mixup at that end of the run.
func TestPrepareWithSealFlagsIsRefused(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "out")

	err := run(append(runArgs(cmdPrepare, out, log), "-seal-from", "/somewhere"))
	if err == nil {
		t.Fatal("prepare with a seal flag set must be refused")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "prepare failed:")
	}
}

// A flow-workspace publish moves the sealed run from work/<runID> to
// results/<runID> before it reports anything, once -seal-from and -seal-to
// are both given.
func TestPublishMovesTheRunOntoResultsOnSIGTERM(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	log := termLog(t)
	root := t.TempDir()
	sealFrom := filepath.Join(root, "work", "3")
	sealTo := filepath.Join(root, "results", "3")
	out := filepath.Join(sealFrom, "out")
	// prepare closes out to 0o555 on success; a successful move relocates it
	// to sealTo/out, so it is that path, not the original, TempDir's cleanup
	// needs reopened.
	t.Cleanup(func() { _ = os.Chmod(filepath.Join(sealTo, "out"), 0o755) })
	if err := run(runArgs(cmdPrepare, out, log)); err != nil {
		t.Fatalf("run(prepare): %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- run(runArgsWithSeal(cmdPublish, out, log, sealFrom, sealTo))
	}()
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

	if _, err := os.Stat(sealFrom); !os.IsNotExist(err) {
		t.Fatalf("work/3 still exists after publish moved it: %v", err)
	}
	if _, err := os.Stat(filepath.Join(sealTo, "out")); err != nil {
		t.Fatalf("results/3 does not hold the moved out/: %v", err)
	}
	if got := readFile(t, log); got == "" {
		t.Fatal("termination log is empty; a successful move must still report the answer")
	}
}

// A move that cannot complete must not report a verdict at all: an answer
// without the run actually having made it onto the results/ shelf would tell
// a later phase to read a directory that is not there. This has to come
// through as no termination message and a non-zero exit, the same as any
// other infrastructure failure, rather than as an answer collect would trust.
func TestPublishMoveFailureLeavesNoTerminationMessage(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	log := termLog(t)
	root := t.TempDir()
	sealFrom := filepath.Join(root, "work", "3")
	sealTo := filepath.Join(root, "results", "3")
	out := filepath.Join(sealFrom, "out")
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })
	if err := run(runArgs(cmdPrepare, out, log)); err != nil {
		t.Fatalf("run(prepare): %v", err)
	}
	// results/3 already exists, so Move must refuse rather than overwrite it.
	if err := os.MkdirAll(sealTo, 0o755); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- run(runArgsWithSeal(cmdPublish, out, log, sealFrom, sealTo))
	}()
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("kill: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("run(publish) must fail when the move is refused")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run(publish) did not return after SIGTERM")
	}

	if got := readFile(t, log); got != "" {
		t.Fatalf("termination log = %q; a failed move must not report a verdict at all", got)
	}
}

// runArgsWithRun is runArgs plus the flags a flow-workspace prepare takes:
// its own run directory, and optionally the abandoned runs to sweep first.
func runArgsWithRun(cmd, out, log, runDir, sweep string) []string {
	args := append(runArgs(cmd, out, log), "-run-dir", runDir)
	if sweep != "" {
		args = append(args, "-sweep", sweep)
	}
	return args
}

func TestPrepareMakesItsRunAndSweepsTheAbandoned(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	work := t.TempDir()
	for _, d := range []string{"1", "2"} {
		if err := os.MkdirAll(filepath.Join(work, d, "out", "ok"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	log := termLog(t)
	runDir := filepath.Join(work, "3")
	out := filepath.Join(runDir, "out")
	// Prepare closes out/ to 0555; open it back up so TempDir's own cleanup
	// can remove what the test made.
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })

	if err := run(runArgsWithRun(cmdPrepare, out, log, runDir, "1,2")); err != nil {
		t.Fatalf("run: %v", err)
	}

	for _, d := range []string{"1", "2"} {
		if _, err := os.Stat(filepath.Join(work, d)); err == nil {
			t.Fatalf("abandoned run %s survived the sweep", d)
		}
	}
	info, err := os.Stat(runDir)
	if err != nil || info.Mode().Perm() != 0o777 {
		t.Fatalf("run dir = %v (%v); the agent must be able to write beside out/", info, err)
	}
	if _, err := os.Stat(filepath.Join(out, "ok")); err != nil {
		t.Fatalf("the vocabulary was not laid down: %v", err)
	}
}

// Making the run's own directory failing must fail prepare outright, wired
// end to end through run() rather than sidecar.MakeRun in isolation: work is
// closed off before -run-dir is ever created, so MakeRun's own MkdirAll is
// what fails, ahead of anything sweep or Prepare could do.
func TestPrepareMakeRunFailureIsReported(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	work := t.TempDir()
	if err := os.Chmod(work, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(work, 0o755) })

	log := termLog(t)
	runDir := filepath.Join(work, "3") // never created
	out := filepath.Join(runDir, "out")
	err := run(runArgsWithRun(cmdPrepare, out, log, runDir, ""))
	if err == nil {
		t.Fatal("prepare must fail when the run's own directory cannot be created")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "prepare failed:")
	}
}

// A sweep that cannot remove a run's leftovers must fail prepare outright,
// wired end to end through run() rather than sidecar.Sweep in isolation: the
// run's own directory (work/3) is made first and must survive, only the
// abandoned one (work/1) is left behind. work/3 already exists before work
// is closed off, so MakeRun's MkdirAll finds it rather than needing to
// create it — the failure has to come from the sweep, not from making the
// run's own directory.
func TestPrepareSweepFailureIsReported(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	work := t.TempDir()
	if err := os.MkdirAll(filepath.Join(work, "1", "out", "ok"), 0o755); err != nil {
		t.Fatal(err)
	}
	runDir := filepath.Join(work, "3")
	if err := os.Mkdir(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(work, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(work, 0o755) })

	log := termLog(t)
	out := filepath.Join(runDir, "out")
	err := run(runArgsWithRun(cmdPrepare, out, log, runDir, "1"))
	if err == nil {
		t.Fatal("prepare must fail when a swept run cannot be removed")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "prepare failed:")
	}
	if _, err := os.Stat(filepath.Join(work, "1")); err != nil {
		t.Fatalf("a failed sweep must leave the run's directory alone: %v", err)
	}
}

func TestPrepareSweepWithoutARunDirIsRefused(t *testing.T) {
	log := termLog(t)
	if err := run(append(runArgs(cmdPrepare, "/x/3/out", log), "-sweep", "1")); err == nil {
		t.Fatal("a sweep with no run-dir has no work/ to operate under and must be refused")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") {
		t.Fatalf("termination log = %q; startup validation reports its cause", got)
	}
}

func TestPrepareRunDirMustParentOut(t *testing.T) {
	log := termLog(t)
	if err := run(append(runArgs(cmdPrepare, "/x/other/out", log), "-run-dir", "/x/3")); err == nil {
		t.Fatal("a run-dir that is not out's parent is a mangled command line and must be refused")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") {
		t.Fatalf("termination log = %q; startup validation reports its cause", got)
	}
}

func TestPublishWithPrepareFlagsIsRefused(t *testing.T) {
	log := termLog(t)
	if err := run(append(runArgs(cmdPublish, "/x/out", log), "-run-dir", "/x/3")); err == nil {
		t.Fatal("run-dir is prepare's; publish must refuse it")
	}
	if got := readFile(t, log); !strings.Contains(got, "publish failed:") {
		t.Fatalf("termination log = %q; startup validation reports its cause", got)
	}
}

// -sweep alone, with no -run-dir, must be just as refused as -run-dir was
// above: the two are checked with an OR, and a test only ever setting
// -run-dir never touches the -sweep side of it.
func TestPublishWithSweepFlagIsRefused(t *testing.T) {
	log := termLog(t)
	if err := run(append(runArgs(cmdPublish, "/x/out", log), "-sweep", "1")); err == nil {
		t.Fatal("sweep is prepare's; publish must refuse it")
	}
	if got := readFile(t, log); !strings.Contains(got, "publish failed:") {
		t.Fatalf("termination log = %q; startup validation reports its cause", got)
	}
}

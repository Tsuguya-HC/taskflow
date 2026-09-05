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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/sidecar"
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
// flag names are spelled once rather than at every call site. out is the
// run's own directory — what the handler sees at its mount's root.
func runArgs(cmd, out, log string) []string {
	return []string{cmd, "-out", out, "-termination-log", log}
}

// publishArgs is runArgs for publish plus the flag a flow-workspace one
// takes, so the run's directory moves to sealTo once sealed.
func publishArgs(out, log, sealTo string) []string {
	return append(runArgs(cmdPublish, out, log), "-seal-to", sealTo)
}

// prepareArgs is runArgs for prepare plus, optionally, the abandoned runs to
// sweep beside this one first.
func prepareArgs(out, log, sweep string) []string {
	args := runArgs(cmdPrepare, out, log)
	if sweep != "" {
		args = append(args, "-sweep", sweep)
	}
	return args
}

// podA and podB are the identities the tests run as; pod sets the one every
// subcommand needs, prepare to mark the run with and publish to check the
// mark against.
const (
	podA = "pod-a"
	podB = "pod-b"
)

func pod(t *testing.T, uid string) {
	t.Helper()
	t.Setenv(contract.EnvPodUID, uid)
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
	pod(t, podA)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "3")

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
	pod(t, podA)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "3")

	err := run(runArgs(cmdPrepare, out, log))
	if err == nil {
		t.Fatal("want an error when FLOW_DIRECTORIES is not a JSON array")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "prepare failed:")
	}
}

func TestPublishWithoutFlowDirectoriesFailsAndReportsIt(t *testing.T) {
	pod(t, podA)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "3")

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
	pod(t, podA)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "3")

	err := run(runArgs(cmdPublish, out, log))
	if err == nil {
		t.Fatal("want an error when FLOW_DIRECTORIES is not a JSON array")
	}
	if got := readFile(t, log); !strings.Contains(got, "publish failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "publish failed:")
	}
}

// The declared directories land directly in the run's directory — the one
// the handler's mount is pinned to — with nothing between (ADR-0005): what
// prepare makes here is exactly what `ls /workspace` shows the agent.
func TestPrepareLaysDownTheDeclaredDirectories(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok", "more"]`)
	pod(t, podA)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "3")
	// prepare closes the run to 0o555 on success; reopen it so TempDir's
	// cleanup can remove what is inside.
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })

	if err := run(runArgs(cmdPrepare, out, log)); err != nil {
		t.Fatalf("run(prepare): %v", err)
	}
	for _, name := range []string{"ok", "more"} {
		if st, err := os.Stat(filepath.Join(out, name)); err != nil || !st.IsDir() {
			t.Fatalf("%s/ was not created in the run: %v", name, err)
		}
	}
	if st, err := os.Stat(out); err != nil || st.Mode().Perm() != 0o555 {
		t.Fatalf("run = %v (%v); the run's directory itself is what is closed, there is no out/ under it", st, err)
	}
	if got := readFile(t, filepath.Join(out, ".prepared-by")); got != podA {
		t.Fatalf("mark = %q, want this pod's identity closed in with the vocabulary", got)
	}
}

func TestPrepareWithABadNameFailsAndReportsIt(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["a/b"]`)
	pod(t, podA)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "3")

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
	pod(t, podA)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "3")
	// prepare closes the run to 0o555 on success; reopen it so TempDir's
	// cleanup can remove what is inside.
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

// The seal flag is publish's alone; prepare never moves anything, so a
// prepare command line naming it is a misconfiguration to fail on, the
// same as any other flag mixup at that end of the run.
func TestPrepareWithTheSealFlagIsRefused(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	pod(t, podA)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "3")

	err := run(append(runArgs(cmdPrepare, out, log), "-seal-to", "/somewhere"))
	if err == nil {
		t.Fatal("prepare with the seal flag set must be refused")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") {
		t.Fatalf("termination log = %q, want it to contain %q", got, "prepare failed:")
	}
	if _, err := os.Lstat(out); !os.IsNotExist(err) {
		t.Fatalf("the run directory was made before the command line was checked: %v", err)
	}
}

// There is no default run directory to fall back on: a controller-built Job
// always spells -out out, and a default would only hide one built without.
func TestRunWithoutOutIsRefused(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	pod(t, podA)
	for _, cmd := range []string{cmdPrepare, cmdPublish} {
		t.Run(cmd, func(t *testing.T) {
			log := termLog(t)
			if err := run([]string{cmd, "-termination-log", log}); err == nil {
				t.Fatalf("%s without -out must be refused", cmd)
			}
			if got := readFile(t, log); !strings.Contains(got, cmd+" failed:") || !strings.Contains(got, "-out") {
				t.Fatalf("termination log = %q; startup validation names the missing flag", got)
			}
		})
	}
}

// A flow-workspace publish moves the sealed run from work/<runID> to
// results/<runID> before it reports anything, once -seal-to is given. What
// arrives on the shelf is the run's directory as the handler saw it: the
// declared directories at its root, results/<runID>/ok/report.md and no
// layer between (ADR-0005).
func TestPublishMovesTheRunOntoResultsOnSIGTERM(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	pod(t, podA)
	log := termLog(t)
	root := t.TempDir()
	out := filepath.Join(root, "work", "3")
	sealTo := filepath.Join(root, "results", "3")
	// prepare closes the run to 0o555 on success; a successful move relocates
	// it to sealTo, so it is that path, not the original, TempDir's cleanup
	// needs reopened.
	t.Cleanup(func() { _ = os.Chmod(sealTo, 0o755) })
	if err := run(prepareArgs(out, log, "")); err != nil {
		t.Fatalf("run(prepare): %v", err)
	}
	if err := os.WriteFile(filepath.Join(out, "ok", "report.md"), []byte("the answer"), 0o644); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- run(publishArgs(out, log, sealTo))
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

	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("work/3 still exists after publish moved it: %v", err)
	}
	if got := readFile(t, filepath.Join(sealTo, "ok", "report.md")); got != "the answer" {
		t.Fatalf("results/3/ok/report.md = %q; the shelf holds the run exactly as the handler wrote it, ok/ at its root", got)
	}
	if got := readFile(t, log); !strings.HasPrefix(got, "ok\n") {
		t.Fatalf("termination log = %q; a successful move must still report the answer", got)
	}
	if st, err := os.Stat(sealTo); err != nil || st.Mode().Perm() != 0o555 {
		t.Fatalf("results/3 = %v (%v); the shelved run is closed, as the handler saw it", st, err)
	}
	if got := readFile(t, filepath.Join(sealTo, ".prepared-by")); got != podA {
		t.Fatalf("the shelved run's mark = %q, want the pod that prepared it to travel with it", got)
	}
}

// A move that cannot complete must not report a verdict at all: an answer
// without the run actually having made it onto the results/ shelf would tell
// a later phase to read a directory that is not there. This has to come
// through as no termination message and a non-zero exit, the same as any
// other infrastructure failure, rather than as an answer collect would trust.
func TestPublishMoveFailureLeavesNoTerminationMessage(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	pod(t, podA)
	log := termLog(t)
	root := t.TempDir()
	out := filepath.Join(root, "work", "3")
	sealTo := filepath.Join(root, "results", "3")
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })
	if err := run(prepareArgs(out, log, "")); err != nil {
		t.Fatalf("run(prepare): %v", err)
	}
	// results/3 already exists, so Move must refuse rather than overwrite it.
	if err := os.MkdirAll(sealTo, 0o755); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- run(publishArgs(out, log, sealTo))
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

func TestPrepareMakesItsRunAndSweepsTheAbandoned(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	pod(t, podA)
	work := t.TempDir()
	for _, d := range []string{"1", "2"} {
		if err := os.MkdirAll(filepath.Join(work, d, "ok"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	log := termLog(t)
	out := filepath.Join(work, "3")
	// Prepare closes the run to 0555; open it back up so TempDir's own
	// cleanup can remove what the test made.
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })

	if err := run(prepareArgs(out, log, "1,2")); err != nil {
		t.Fatalf("run: %v", err)
	}

	for _, d := range []string{"1", "2"} {
		if _, err := os.Stat(filepath.Join(work, d)); err == nil {
			t.Fatalf("abandoned run %s survived the sweep", d)
		}
	}
	info, err := os.Stat(out)
	if err != nil || info.Mode().Perm() != 0o555 {
		t.Fatalf("run dir = %v (%v); the run's directory is the vocabulary, closed once laid down", info, err)
	}
	if _, err := os.Stat(filepath.Join(out, "ok")); err != nil {
		t.Fatalf("the vocabulary was not laid down: %v", err)
	}
	if got := readFile(t, filepath.Join(out, ".prepared-by")); got != podA {
		t.Fatalf("mark = %q, want this pod's identity closed in with the vocabulary", got)
	}
}

// The identity is needed on both ends, and its absence is a misbuilt Job
// rather than something to carry on without: a run left unmarked could never
// be sealed by its own publish.
func TestPrepareWithNoPodIdentityFailsAndReportsIt(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	work := t.TempDir()
	log := termLog(t)
	out := filepath.Join(work, "3")

	err := run(prepareArgs(out, log, ""))
	if err == nil {
		t.Fatal("prepare with no pod identity must fail")
	}
	if got := readFile(t, log); !strings.Contains(got, "prepare failed:") || !strings.Contains(got, contract.EnvPodUID) {
		t.Fatalf("termination log = %q, want it to name %s", got, contract.EnvPodUID)
	}
	if _, err := os.Lstat(out); !os.IsNotExist(err) {
		t.Fatalf("the run directory was made before the identity was checked: %v", err)
	}
}

func TestPublishWithNoPodIdentityFailsAndReportsIt(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "work", "3")

	err := run(runArgs(cmdPublish, out, log))
	if err == nil {
		t.Fatal("publish with no pod identity must fail")
	}
	if got := readFile(t, log); !strings.Contains(got, "publish failed:") || !strings.Contains(got, contract.EnvPodUID) {
		t.Fatalf("termination log = %q, want it to name %s", got, contract.EnvPodUID)
	}
}

// A publish that outlives its own pod's record still gets its SIGTERM, and by
// then the next attempt at the same runID may be working at the same path.
// It must not seal what it finds there, let alone shelve it: the failure is
// reported the way startup validation's is, and the run stays where the live
// attempt left it.
func TestPublishRefusesAnotherPodsRunWithoutSealing(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	pod(t, podB)
	log := termLog(t)
	root := t.TempDir()
	out := filepath.Join(root, "work", "3")
	sealTo := filepath.Join(root, "results", "3")
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })
	if err := run(prepareArgs(out, log, "")); err != nil {
		t.Fatalf("run(prepare): %v", err)
	}
	// The live attempt has answered; the zombie must not carry that off.
	if err := os.WriteFile(filepath.Join(out, "ok", "report.md"), []byte("the live attempt's"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(log, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	pod(t, podA)
	done := make(chan error, 1)
	go func() {
		done <- run(publishArgs(out, log, sealTo))
	}()
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("kill: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, sidecar.ErrMark) {
			t.Fatalf("run(publish) as another pod: err = %v, want ErrMark", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run(publish) did not return after SIGTERM")
	}

	if got := readFile(t, log); !strings.HasPrefix(got, "publish failed:") || strings.HasPrefix(got, "ok") {
		t.Fatalf("termination log = %q; the refusal must be reported, never the other run's answer", got)
	}
	if _, err := os.Stat(filepath.Join(out, "ok", "report.md")); err != nil {
		t.Fatalf("the live attempt's run was disturbed: %v", err)
	}
	if _, err := os.Lstat(sealTo); !os.IsNotExist(err) {
		t.Fatalf("results/3 appeared: %v", err)
	}
}

// A template volume has no -seal-to and so no shelf, but publish must still
// refuse another pod's run before sealing: CheckMark is called unconditionally
// in publish(), ahead of the -seal-to branch, and this pins that down so a
// regression that made it conditional on sealTo != "" would fail here even
// though TestPublishRefusesAnotherPodsRunWithoutSealing only exercises the
// flow-workspace path.
func TestPublishOnATemplateVolumeStillChecksTheMark(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	pod(t, podB)
	log := termLog(t)
	out := filepath.Join(t.TempDir(), "3")
	t.Cleanup(func() { _ = os.Chmod(out, 0o755) })
	if err := run(prepareArgs(out, log, "")); err != nil {
		t.Fatalf("run(prepare): %v", err)
	}
	// The live attempt has answered; the zombie must not carry that off.
	if err := os.WriteFile(filepath.Join(out, "ok", "report.md"), []byte("the live attempt's"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(log, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	pod(t, podA)
	done := make(chan error, 1)
	go func() {
		done <- run(runArgs(cmdPublish, out, log))
	}()
	time.Sleep(100 * time.Millisecond)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("kill: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, sidecar.ErrMark) {
			t.Fatalf("run(publish) as another pod on a template volume: err = %v, want ErrMark", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run(publish) did not return after SIGTERM")
	}

	if got := readFile(t, log); !strings.HasPrefix(got, "publish failed:") || strings.HasPrefix(got, "ok") {
		t.Fatalf("termination log = %q; the refusal must be reported, never the other run's answer", got)
	}
	if _, err := os.Stat(filepath.Join(out, "ok", "report.md")); err != nil {
		t.Fatalf("the live attempt's run was disturbed: %v", err)
	}
}

// Making the run's own directory failing must fail prepare outright, wired
// end to end through run() rather than sidecar.MakeRun in isolation: work is
// closed off before -out is ever created, so MakeRun's own MkdirAll is what
// fails, ahead of anything sweep or Prepare could do.
func TestPrepareMakeRunFailureIsReported(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	pod(t, podA)
	work := t.TempDir()
	if err := os.Chmod(work, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(work, 0o755) })

	log := termLog(t)
	out := filepath.Join(work, "3") // never created
	err := run(prepareArgs(out, log, ""))
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
// abandoned one (work/1) is left behind. work itself stays writable — closing
// it off would make MakeRun's own RemoveAll of the pre-existing work/3 fail
// first, never reaching the sweep — so it is work/1 that is closed instead,
// refusing the removal of what it holds.
func TestPrepareSweepFailureIsReported(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	pod(t, podA)
	work := t.TempDir()
	if err := os.MkdirAll(filepath.Join(work, "1", "ok"), 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(work, "3")
	if err := os.Mkdir(out, 0o755); err != nil {
		t.Fatal(err)
	}
	abandoned := filepath.Join(work, "1")
	if err := os.Chmod(abandoned, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(abandoned, 0o755) })

	log := termLog(t)
	err := run(prepareArgs(out, log, "1"))
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

// -sweep is prepare's; publish never clears anything, so a publish command
// line naming it is a mangled one to fail on.
func TestPublishWithSweepFlagIsRefused(t *testing.T) {
	t.Setenv(contract.EnvDirectories, `["ok"]`)
	pod(t, podA)
	log := termLog(t)
	if err := run(append(runArgs(cmdPublish, "/x/work/3", log), "-sweep", "1")); err == nil {
		t.Fatal("sweep is prepare's; publish must refuse it")
	}
	if got := readFile(t, log); !strings.Contains(got, "publish failed:") {
		t.Fatalf("termination log = %q; startup validation reports its cause", got)
	}
}

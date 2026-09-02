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

// The sidecar image's entrypoint: one binary, two subcommands, so the code
// that lays the vocabulary down and the code that reads it back are the same
// code.
//
//	sidecar prepare   init container: create the declared directories, close out
//	                  — for a flow workspace, after making this run's own
//	                  directory, sweeping the abandoned ones, and marking the
//	                  run with this pod's identity
//	sidecar publish   native sidecar: wait to be stopped, seal, report — and,
//	                  for a flow workspace, check the run is this pod's and
//	                  move the now-sealed run onto the results/ shelf first
//
// The declared directories arrive in FLOW_DIRECTORIES, which the controller
// sets on every container. Where they live is the pod spec's business and
// comes in as a flag.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/sidecar"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "sidecar:", err)
		os.Exit(1)
	}
}

// cmdPrepare and cmdPublish name the two subcommands. They are the
// contract's own names — internal/contract is what the controller's side
// (runner.BuildJob) reads too, so the two ends cannot drift over a spelling.
const (
	cmdPrepare = contract.SubcommandPrepare
	cmdPublish = contract.SubcommandPublish
)

// options is the parsed command line, the same set for both subcommands:
// each one's flags are declared on the other's flag set too, so a flag given
// to the wrong subcommand fails in checkFlags rather than being silently
// accepted and ignored.
type options struct {
	out     string
	termLog string
	// sealFrom and sealTo are publish-only, and meaningful only together: a
	// flow workspace's run is shelved from the one to the other once sealed.
	sealFrom string
	sealTo   string
	// runDir and sweep are prepare-only, the same arrangement in the other
	// direction.
	runDir string
	sweep  string
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: sidecar prepare|publish [flags]")
	}
	cmd, args := args[0], args[1:]
	if cmd != cmdPrepare && cmd != cmdPublish {
		return fmt.Errorf("unknown subcommand %q: want prepare or publish", cmd)
	}

	var o options
	fs := flag.NewFlagSet("sidecar "+cmd, flag.ContinueOnError)
	fs.StringVar(&o.out, contract.FlagOut, "/workspace/out", "directory the declared directories are created under")
	fs.StringVar(&o.termLog, "termination-log", "/dev/termination-log", "where the answer is written for the controller")
	fs.StringVar(&o.sealFrom, contract.FlagSealFrom, "", "publish only: move the run's directory from here once sealed")
	fs.StringVar(&o.sealTo, contract.FlagSealTo, "", "publish only: move the run's directory to here once sealed")
	fs.StringVar(&o.runDir, contract.FlagRunDir, "",
		"prepare only: this run's own directory, made and opened before the vocabulary is laid down")
	fs.StringVar(&o.sweep, contract.FlagSweep, "",
		"prepare only: comma-separated runIDs whose work/ leftovers are cleared away first")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Everything up to the subcommand's own body is startup validation,
	// ahead of anything that could have produced a verdict yet — a
	// misconfigured command line, same kind of fault as an unparsable flag.
	// Reported the way every other prepare or publish failure ahead of Seal
	// is: cause named, non-zero exit. That is not the same footing publish's
	// Move failure stands on, which happens only after Seal already has a
	// real answer to have moved.
	if cause := checkFlags(cmd, o); cause != nil {
		return reportFailure(o.termLog, cmd, cause)
	}
	// Both subcommands need the same vocabulary — prepare to create it,
	// publish to read it back — so it is read once here rather than
	// duplicated in each.
	declared, err := declaredDirectories()
	if err != nil {
		return reportFailure(o.termLog, cmd, err)
	}
	// A flow workspace's run is marked with the pod that prepared it and
	// checked against the pod that publishes it, so the identity is needed
	// on both ends of that and on neither end of a template-backed one.
	// Read up front for the same reason the vocabulary is: a missing one
	// should fail before anything has been touched.
	podUID := ""
	if o.runDir != "" || o.sealFrom != "" {
		if podUID, err = podIdentity(); err != nil {
			return reportFailure(o.termLog, cmd, err)
		}
	}

	if cmd == cmdPrepare {
		return prepare(o, declared, podUID)
	}
	return publish(o, declared, podUID)
}

// checkFlags refuses the command lines a controller-built Job never writes:
// one subcommand's flags handed to the other, or a flow workspace's flags
// given by halves.
func checkFlags(cmd string, o options) error {
	if cmd == cmdPrepare {
		if o.sealFrom != "" || o.sealTo != "" {
			return fmt.Errorf("-%s/-%s are publish's to set, not prepare's", contract.FlagSealFrom, contract.FlagSealTo)
		}
		if o.sweep != "" && o.runDir == "" {
			return fmt.Errorf("-%s without -%s has no work/ to sweep under", contract.FlagSweep, contract.FlagRunDir)
		}
		if o.runDir != "" && filepath.Dir(o.out) != o.runDir {
			return fmt.Errorf("-%s %q is not the parent of -%s %q", contract.FlagRunDir, o.runDir, contract.FlagOut, o.out)
		}
		return nil
	}
	if (o.sealFrom == "") != (o.sealTo == "") {
		// One without the other is refused, not treated as neither: silently
		// sealing only would leave a flow workspace's run stranded in work/
		// forever, with nothing to say why it never reached the results/
		// shelf a later phase reads back.
		return fmt.Errorf("-%s and -%s must be given together or not at all", contract.FlagSealFrom, contract.FlagSealTo)
	}
	if o.sealFrom != "" && filepath.Dir(o.out) != o.sealFrom {
		// The same shape prepare's own -run-dir/-out check refuses: -out is
		// always sealFrom/out, so a -seal-from naming anything else is a
		// mangled command line (runner.injectSidecars sets both from the
		// same run directory).
		return fmt.Errorf("-%s %q is not the parent of -%s %q", contract.FlagSealFrom, o.sealFrom, contract.FlagOut, o.out)
	}
	if o.runDir != "" || o.sweep != "" {
		return fmt.Errorf("-%s/-%s are prepare's to set, not publish's", contract.FlagRunDir, contract.FlagSweep)
	}
	return nil
}

func prepare(o options, declared []string, podUID string) error {
	if o.runDir != "" {
		// The run's own directory first, the sweep second: a sweep that
		// fails should not also leave this run without even a place it was
		// being prepared in, and neither step touches the other's target —
		// the sweep list never names this run. The mark goes down last of
		// the three, once the directory it belongs in is known to be the
		// fresh one MakeRun made, and ahead of Prepare so it is closed in
		// with the vocabulary.
		if err := sidecar.MakeRun(o.runDir); err != nil {
			return reportFailure(o.termLog, cmdPrepare, err)
		}
		if o.sweep != "" {
			if err := sidecar.Sweep(filepath.Dir(o.runDir), strings.Split(o.sweep, ","), filepath.Base(o.runDir)); err != nil {
				return reportFailure(o.termLog, cmdPrepare, err)
			}
		}
		if err := sidecar.Mark(o.out, podUID); err != nil {
			return reportFailure(o.termLog, cmdPrepare, err)
		}
	}
	if err := sidecar.Prepare(o.out, declared); err != nil {
		return reportFailure(o.termLog, cmdPrepare, err)
	}
	fmt.Printf("prepared %s with %v\n", o.out, declared)
	return nil
}

func publish(o options, declared []string, podUID string) error {
	// A native sidecar is stopped with SIGTERM once the main containers have
	// finished. That signal is the only "the run is over" there is.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	fmt.Println("waiting for the run to finish")
	<-ctx.Done()

	// Before sealing, not just before moving: the directory at -out is this
	// run's only if the pod that prepared it is this one. A run keeps its
	// number across infrastructure retries, so a publish outliving its own
	// pod's record — gone from the apiserver, still running until the node
	// gets around to stopping it — finds the next attempt's directory at
	// the same path, and an answer read out of that is not this run's to
	// report, let alone to shelve. Ahead of Seal, this is startup
	// validation's footing: cause named, non-zero exit, no verdict.
	if o.sealFrom != "" {
		if err := sidecar.CheckMark(o.out, podUID); err != nil {
			return reportFailure(o.termLog, cmdPublish, err)
		}
	}

	ans := sidecar.Seal(o.out, declared)
	// Printed before the move is attempted: a run whose move then fails
	// still sealed to a real answer, and that answer belongs in the pod's
	// own log even though it never reaches the termination message below —
	// otherwise a failed move is the one path through publish that leaves no
	// trace anywhere of what sealing actually found.
	fmt.Println(ans.Message())
	// checkFlags has already ruled out one of these being set without the
	// other; a flow workspace has both, a template-backed one has neither.
	if o.sealFrom != "" {
		if err := sidecar.Move(o.sealFrom, o.sealTo); err != nil {
			// Unlike the startup validation above, this comes after Seal
			// already decided a real answer — and no termination message is
			// written for it: a verdict without the move having actually
			// happened would tell a later phase to read a run that never
			// made it onto the results/ shelf. Coming through as no verdict
			// at all — the same as any other infrastructure failure the
			// controller retries on its own — is safer than an answer
			// collect would otherwise trust.
			return fmt.Errorf("publish: %w", err)
		}
	}
	return report(o.termLog, ans)
}

// declaredDirectories reads the vocabulary the controller put in the
// environment. An unset or unparsable value is an error rather than an empty
// vocabulary: a run with no declared directories can never answer, and that
// should fail loudly at prepare, not quietly at seal.
func declaredDirectories() ([]string, error) {
	raw, ok := os.LookupEnv(contract.EnvDirectories)
	if !ok {
		return nil, fmt.Errorf("%s is not set; is this container in a Job the controller created?", contract.EnvDirectories)
	}
	var dirs []string
	if err := json.Unmarshal([]byte(raw), &dirs); err != nil {
		return nil, fmt.Errorf("%s is not a JSON array of names: %w", contract.EnvDirectories, err)
	}
	return dirs, nil
}

// podIdentity reads the UID of the pod this container runs in, which the
// controller hands the injected containers through the downward API. It is
// what tells one attempt's prepare and publish apart from another's at the
// same runID, so an unset value is an error where it is needed, the same as
// an unset vocabulary.
func podIdentity() (string, error) {
	uid, ok := os.LookupEnv(contract.EnvPodUID)
	if !ok || uid == "" {
		return "", fmt.Errorf("%s is not set; is this container in a Job the controller created?", contract.EnvPodUID)
	}
	return uid, nil
}

// reportFailure says why in the same channel the answer would have used, so
// the controller's record shows a prepare or publish that failed rather than
// a run that said nothing — then returns the original error so the process
// still exits non-zero. If writing the termination log itself fails, that
// failure is wrapped around the cause rather than replacing it, so neither
// is lost.
func reportFailure(termLog, cmd string, cause error) error {
	if err := report(termLog, sidecar.Answer{Reason: cmd + " failed: " + cause.Error()}); err != nil {
		return fmt.Errorf("%w (and writing the termination log failed: %v)", cause, err)
	}
	return cause
}

// report writes the answer where the kubelet will pick it up. The file is
// already there, bind-mounted by the kubelet; it is truncated, not appended,
// so a retry within the container does not stack messages.
func report(path string, ans sidecar.Answer) error {
	if err := os.WriteFile(path, []byte(ans.Message()), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

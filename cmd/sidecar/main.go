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
//	sidecar publish   native sidecar: wait to be stopped, seal, report — and,
//	                  for a flow workspace, move the now-sealed run onto the
//	                  results/ shelf first
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

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: sidecar prepare|publish [flags]")
	}
	cmd, args := args[0], args[1:]
	if cmd != cmdPrepare && cmd != cmdPublish {
		return fmt.Errorf("unknown subcommand %q: want prepare or publish", cmd)
	}

	fs := flag.NewFlagSet("sidecar "+cmd, flag.ContinueOnError)
	out := fs.String(contract.FlagOut, "/workspace/out", "directory the declared directories are created under")
	termLog := fs.String("termination-log", "/dev/termination-log", "where the answer is written for the controller")
	// sealFrom and sealTo are publish-only, and meaningful only together: a
	// flow workspace's run is shelved from the one to the other once sealed.
	// Declared here regardless of cmd so prepare's own flag set can refuse
	// them too, below, rather than accepting and silently ignoring a typo.
	sealFrom := fs.String(contract.FlagSealFrom, "", "publish only: move the run's directory from here once sealed")
	sealTo := fs.String(contract.FlagSealTo, "", "publish only: move the run's directory to here once sealed")
	// runDir and sweep are prepare-only, the same arrangement in the other
	// direction: declared for both subcommands so a typo fails rather than
	// being silently accepted, refused below when publish gets them.
	runDir := fs.String(contract.FlagRunDir, "",
		"prepare only: this run's own directory, made and opened before the vocabulary is laid down")
	sweep := fs.String(contract.FlagSweep, "",
		"prepare only: comma-separated runIDs whose work/ leftovers are cleared away first")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Both of these are startup validation, ahead of anything that could have
	// produced a verdict yet — a misconfigured command line, same kind of
	// fault as an unparsable flag. Reported the way every other prepare or
	// publish failure ahead of Seal is: cause named, non-zero exit. That is
	// not the same footing the Move failure below stands on, which happens
	// only after Seal already has a real answer to have moved.
	if cmd == cmdPrepare && (*sealFrom != "" || *sealTo != "") {
		cause := fmt.Errorf("-%s/-%s are publish's to set, not prepare's", contract.FlagSealFrom, contract.FlagSealTo)
		return reportFailure(*termLog, cmd, cause)
	}
	if cmd == cmdPublish && (*sealFrom == "") != (*sealTo == "") {
		// One without the other is refused, not treated as neither: silently
		// sealing only would leave a flow workspace's run stranded in work/
		// forever, with nothing to say why it never reached the results/
		// shelf a later phase reads back.
		cause := fmt.Errorf("-%s and -%s must be given together or not at all", contract.FlagSealFrom, contract.FlagSealTo)
		return reportFailure(*termLog, cmd, cause)
	}
	if cmd == cmdPublish && *sealFrom != "" && filepath.Dir(*out) != *sealFrom {
		// The same shape prepare's own -run-dir/-out check refuses: -out is
		// always sealFrom/out, so a -seal-from naming anything else is a
		// mangled command line, the kind a controller-built Job never writes
		// (runner.injectSidecars sets both from the same run directory).
		cause := fmt.Errorf("-%s %q is not the parent of -%s %q", contract.FlagSealFrom, *sealFrom, contract.FlagOut, *out)
		return reportFailure(*termLog, cmd, cause)
	}
	if cmd == cmdPublish && (*runDir != "" || *sweep != "") {
		cause := fmt.Errorf("-%s/-%s are prepare's to set, not publish's", contract.FlagRunDir, contract.FlagSweep)
		return reportFailure(*termLog, cmd, cause)
	}
	if cmd == cmdPrepare {
		if *sweep != "" && *runDir == "" {
			cause := fmt.Errorf("-%s without -%s has no work/ to sweep under", contract.FlagSweep, contract.FlagRunDir)
			return reportFailure(*termLog, cmd, cause)
		}
		if *runDir != "" && filepath.Dir(*out) != *runDir {
			cause := fmt.Errorf("-%s %q is not the parent of -%s %q", contract.FlagRunDir, *runDir, contract.FlagOut, *out)
			return reportFailure(*termLog, cmd, cause)
		}
	}

	// Both subcommands need the same vocabulary — prepare to create it,
	// publish to read it back — so it is read once here rather than
	// duplicated in each branch below.
	declared, err := declaredDirectories()
	if err != nil {
		return reportFailure(*termLog, cmd, err)
	}

	if cmd == cmdPrepare {
		if *runDir != "" {
			// The run's own directory first, the sweep second: a sweep
			// that fails should not also leave this run without even a
			// place it was being prepared in, and neither step touches the
			// other's target — the sweep list never names this run.
			if err := sidecar.MakeRun(*runDir); err != nil {
				return reportFailure(*termLog, cmd, err)
			}
			if *sweep != "" {
				if err := sidecar.Sweep(filepath.Dir(*runDir), strings.Split(*sweep, ","), filepath.Base(*runDir)); err != nil {
					return reportFailure(*termLog, cmd, err)
				}
			}
		}
		if err := sidecar.Prepare(*out, declared); err != nil {
			return reportFailure(*termLog, cmd, err)
		}
		fmt.Printf("prepared %s with %v\n", *out, declared)
		return nil
	}

	// cmd == cmdPublish: a native sidecar is stopped with SIGTERM once the
	// main containers have finished. That signal is the only "the run is
	// over" there is.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	fmt.Println("waiting for the run to finish")
	<-ctx.Done()

	ans := sidecar.Seal(*out, declared)
	// Printed before the move is attempted: a run whose move then fails
	// still sealed to a real answer, and that answer belongs in the pod's
	// own log even though it never reaches the termination message below —
	// otherwise a failed move is the one path through publish that leaves no
	// trace anywhere of what sealing actually found.
	fmt.Println(ans.Message())
	// The flag validation above has already ruled out one of these being
	// set without the other; a flow workspace has both, a template-backed
	// one has neither.
	if *sealFrom != "" {
		if err := sidecar.Move(*sealFrom, *sealTo); err != nil {
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
	return report(*termLog, ans)
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

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
//	sidecar publish   native sidecar: wait to be stopped, then seal and report
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
	"syscall"

	"github.com/Tsuguya/taskflow/internal/contract"
	"github.com/Tsuguya/taskflow/internal/sidecar"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "sidecar:", err)
		os.Exit(1)
	}
}

// cmdPrepare and cmdPublish name the two subcommands. Constants rather than
// literals repeated at each switch and error site.
const (
	cmdPrepare = "prepare"
	cmdPublish = "publish"
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
	out := fs.String("out", "/workspace/out", "directory the declared directories are created under")
	termLog := fs.String("termination-log", "/dev/termination-log", "where the answer is written for the controller")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Both subcommands need the same vocabulary — prepare to create it,
	// publish to read it back — so it is read once here rather than
	// duplicated in each branch below.
	declared, err := declaredDirectories()
	if err != nil {
		return reportFailure(*termLog, cmd, err)
	}

	if cmd == cmdPrepare {
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
	fmt.Println(ans.Message())
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

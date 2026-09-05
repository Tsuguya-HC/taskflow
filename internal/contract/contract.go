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

// Package contract is the vocabulary shared between whatever the controller
// puts on a Pod and whatever a binary running inside that Pod reads back —
// the environment variable and annotation names both sides must agree on
// without agreeing on anything else.
//
// This package must not import anything beyond the standard library. It is
// the one place a Pod-side binary (cmd/sidecar, and any handler that wants
// these names) can depend on without pulling in the controller's own
// packages — client-go, the CRD types, anything that talks to the API
// server — none of which a container running as the task's own workload has
// any business linking against.
package contract

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	// LabelTaskUID is the only label the controller sets, and it sets it on
	// the Job and on the pods that Job makes. The controller needs it on the
	// Job to find its own work; it is on the pod so that one task's pods can
	// be pulled up directly — kubectl -l, hubble --label — rather than
	// through whatever name the Job happened to get. It is bookkeeping, not
	// something a policy is meant to select on: what a pod must carry to be
	// allowed to run is the handler's to write.
	//
	// A UID also happens to be legal as a label value, which a status name
	// picked by whoever wrote the flow is not — hence the phase below.
	LabelTaskUID = "flow.tgy.io/task-uid"

	// AnnotationPhase carries the status name. An annotation rather than a
	// label because these are free strings: 調査 is not a legal label value
	// and not a legal object name either.
	AnnotationPhase = "flow.tgy.io/phase"
	// AnnotationRunID is the record of which run this is — the number of the
	// runs the task has decided its way through, which an infrastructure
	// retry does not move (ADR-0004); two attempts at one run are told apart
	// by the Job's name, not by this. It is carried on the pod for whoever
	// wants it — not read by the plumbing itself: prepare and
	// publish get the run's paths as CLI arguments the controller computes
	// at generation time (runner.injectSidecars), never by reading this
	// back. A handler's own author can still pull it into a container of
	// their own via a fieldRef, the way any other annotation would be. The
	// agent has no use for it — its mount is pinned to the run, so it opens
	// ok/ and writes — and there is reason to keep it away from one: a run
	// count suggests how much rope is left, the same way a remaining-rework
	// count would.
	AnnotationRunID = "flow.tgy.io/run-id"
	// AnnotationPrevRunID is absent on the first run.
	AnnotationPrevRunID = "flow.tgy.io/prev-run-id"

	// Prefix is what marks a label or an annotation as the framework's. A
	// template that sets one of these is refused rather than overwritten.
	Prefix = "flow.tgy.io/"

	// EnvTaskUID, EnvPhase and EnvInput are set on every container in the
	// template. Unlike the run number these say what the work is, not how
	// many attempts it has had.
	EnvTaskUID = "FLOW_TASK_UID"
	EnvPhase   = "FLOW_PHASE"
	EnvInput   = "FLOW_INPUT"
	// EnvDirectories is the run's vocabulary: the directories the flow's
	// next declares for this phase, as a JSON array. It goes to every
	// container because it is what the agent may say, not how much rope it
	// has — the sidecar creates exactly these, and the agent can see them
	// on disk anyway.
	EnvDirectories = "FLOW_DIRECTORIES"
	// EnvPodUID is the injected containers' own, not every container's: the
	// UID of the pod they run in, from the downward API. prepare writes it
	// into the run's directory and publish reads it back before it seals,
	// so a publish whose pod is already gone from the apiserver — but still
	// running, and still due its SIGTERM — cannot seal or shelve the
	// directory a later attempt at the same runID is working in (ADR-0004).
	// It is not generation-time information the way the run's paths are, so
	// it is the one thing the sidecars take from the pod rather than from
	// their arguments.
	EnvPodUID = "FLOW_POD_UID"

	// WorkspaceVolume is the name of the volume the controller adds to a Job
	// when the task's flow declares a workspace, backed by that task's own
	// claim. A handler joins the flow's workspace by naming it in
	// spec.workspace.volume and mounting it from its own containers; a
	// template that defines a volume under this name itself is refused, the
	// same way a container wearing an injected container's name is.
	WorkspaceVolume = "flow-workspace"

	// SubcommandPrepare and SubcommandPublish name the sidecar binary's two
	// subcommands. The Job template the controller builds (runner.BuildJob)
	// and the sidecar's own argument parsing (cmd/sidecar) both read these,
	// so the two ends of the verdict protocol cannot drift over a spelling.
	SubcommandPrepare = "prepare"
	SubcommandPublish = "publish"

	// FlagOut is the name of the flag both the injected Args and the
	// sidecar's flag set use for the run's own directory: the one prepare
	// makes, lays the declared directories down in directly, and closes;
	// the one publish seals. It is the same directory the handler's own
	// containers see at their mount's root (ADR-0005) — there is no layer
	// between the mount and the vocabulary. prepare reaches it through a
	// mount of its parent, work/, so the directory is prepare's to create
	// and close rather than whatever the kubelet's subPath machinery would
	// have left there.
	FlagOut = "out"

	// FlagSealTo names the flag publish takes when the task's flow brings a
	// workspace: once sealing has decided the run's answer, publish moves
	// the run's directory from where it is (FlagOut, on the work/ shelf)
	// onto the results/ shelf a later phase reads back, one rename within
	// the same volume (§ADR-0002 決定5). Absent means publish only seals —
	// the template-volume case, where there is no shelf to move onto.
	// prepare refuses it outright — it is publish's alone.
	FlagSealTo = "seal-to"

	// FlagSweep is the comma-separated runIDs whose work/ leftovers prepare
	// clears away before this run starts. The controller computes the list —
	// only it knows which runs are live — and prepare deletes exactly what
	// it is told (ADR-0003). Sealed runs left work/ when their rename moved
	// them, so what this actually removes is the debris of attempts that
	// died before sealing. Only a flow workspace has anything to sweep: a
	// template volume is new with every pod. publish refuses it — it is
	// prepare's alone.
	FlagSweep = "sweep"
)

// MarkName is the file prepare writes its own pod's UID into, in the run's
// directory beside the declared directories, and that publish reads back
// before it seals (ADR-0004). It is here rather than in the sidecar because
// the controller has to know it too: a flow may not declare a judgement
// directory under this name, and refusing that at creation is the
// controller's side of the same rule the sidecar enforces at runtime.
const MarkName = ".prepared-by"

// ErrBadDirectoryName reports a declared judgement directory that cannot be
// made: not a single path element, or MarkName, which prepare would then
// find already occupied by the mark it just wrote.
var ErrBadDirectoryName = errors.New("directory name is not a single path element")

// CheckDirectoryName refuses a name the flow cannot have as a judgement
// directory. Both ends run it: admission so a flow that would fail every run
// is refused when it is written, and the sidecar so that refusal does not
// depend on admission having run (ADR-0006 決定5).
func CheckDirectoryName(name string) error {
	if name == "" || name == "." || name == ".." ||
		strings.ContainsAny(name, "/\x00") || filepath.Base(name) != name {
		return fmt.Errorf("%w: %q", ErrBadDirectoryName, name)
	}
	if name == MarkName {
		return fmt.Errorf("%w: %q is reserved for the pod's mark", ErrBadDirectoryName, name)
	}
	return nil
}

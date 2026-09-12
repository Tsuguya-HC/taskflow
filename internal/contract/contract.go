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

// Package contract is the vocabulary the controller shares with whatever is
// on the other end of a run — the environment variable, label, annotation and
// key names both sides must agree on without agreeing on anything else.
//
// There are two such ends. One is a binary running inside a Pod the
// controller made, reading back what was put on it. The other is whoever
// answers a run the framework does not start (ADR-0011): the controller opens
// a place for that answer, and the names it is reached and written by are as
// much a published contract as the Pod's are.
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
	// LabelTaskUID says whose an object is, and it is the one label the
	// controller puts on a pod. Every object the framework makes carries it;
	// the controller needs it on the Job to find its own work, and it is on
	// the pod so that one task's pods can be pulled up directly — kubectl -l,
	// hubble --label — rather than through whatever name the Job happened to
	// get. It is bookkeeping, not something a policy is meant to select on:
	// what a pod must carry to be allowed to run is the handler's to write.
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
	// LabelRunID is the same fact as AnnotationRunID, spelled where a
	// selector can reach it. The two are not a duplicate that can drift:
	// they are the same string, and which one an object carries follows one
	// rule — the framework's own objects (the Job, the verdict box) wear the
	// labels below so the user side can select them, and the pod wears the
	// annotations, because what a pod carries is the handler's to decide and
	// a fieldRef reads annotations just as well.
	//
	// A run number is legal as a label value, which is what decides the
	// question at all: the same object's phase cannot be a label, because a
	// status name the flow's author chose (調査) is not a legal one.
	LabelRunID = AnnotationRunID
	// AnnotationPrevRunID is absent on the first run.
	AnnotationPrevRunID = "flow.tgy.io/prev-run-id"

	// Prefix is what marks a label or an annotation as the framework's. A
	// template that sets one of these is refused rather than overwritten.
	Prefix = "flow.tgy.io/"

	// LabelManagedBy and ManagedBy mark an object as the framework's own
	// make — one the controller decided the contents of, not one a handler's
	// author wrote (ADR-0011 決定5). The Job, the workspace claim and the
	// verdict box carry it; a pod does not, because what a pod wears is a
	// policy question and policy is the user side's.
	//
	// It is there so the user side can manage them: find them at once, put
	// them in or out of a sweep, and — for objects whose names are generated,
	// where RBAC's resourceNames cannot reach — write an admission policy
	// that selects exactly the framework's own.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	ManagedBy      = "taskflow"

	// AnnotationChoices is the vocabulary a State run may be answered with, on
	// the verdict box the controller opens for it. It is there so that
	// answering does not require reading the flow: the declaration puts the
	// choices in front of whoever answers, the way prepare lays the same
	// names down as directories for a run that has a pod (ADR-0011 決定2). An
	// annotation rather than a label for the reason the phase is one — these
	// are the flow author's strings.
	//
	// Rendered as a JSON array, the same as EnvDirectories below and for the
	// same reason: a directory name is a free string that may itself contain
	// a space, and a delimited format would have to forbid whatever character
	// it delimits on. See runner.BuildVerdictBox for how this is built.
	AnnotationChoices = "flow.tgy.io/choices"

	// KeyVerdict is where the answer goes in that box, and KeyReason the one
	// line a human reads next to it — the same two things a termination
	// message carries in its first line and the rest, named rather than
	// positional because a map has no first line.
	//
	// One key, not a directory each: a verdict box cannot have "exactly one
	// non-empty entry" go wrong the way a run's directories can, because
	// there is only ever one place to write.
	KeyVerdict = "verdict"
	KeyReason  = "reason"

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

	// EnvEnding, EnvEndingPhase and EnvEndingOutcome are the cleanup run's
	// alone, and the only three the framework sets that describe something
	// other than the run they are in: the ending this run follows
	// (ADR-0009). No other run has an ending to be told about, so no other
	// run is given them.
	//
	// EnvEnding is what stopping there meant — Success or Failure as the flow
	// declared it, Escalated or Failed for the framework's own two, Undeclared
	// for a flow that never said. EnvEndingPhase is the status name it stopped
	// at, which EnvPhase cannot carry because that says what this run is, and
	// this run is the cleanup. EnvEndingOutcome is the framework's account of
	// the run that reached the ending, and empty when no run did — a flow
	// broken before anything could be started has an ending with no run behind
	// it, and an empty value is the honest report of that rather than the last
	// unrelated run's verdict (P8).
	//
	// They are values to read, not a control flow to obey: a handler may
	// report differently for a Failure than for a Success, but nothing the
	// framework does depends on which it was.
	EnvEnding        = "FLOW_ENDING"
	EnvEndingPhase   = "FLOW_ENDING_PHASE"
	EnvEndingOutcome = "FLOW_ENDING_OUTCOME"

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
	// between the mount and the vocabulary. Both injected containers mount
	// the volume at its root and are told this path in full, so the
	// directory is prepare's to create and close rather than whatever the
	// kubelet's subPath machinery would have left there, and so the shelf
	// beside it is reachable at all.
	FlagOut = "out"

	// FlagSealTo names the flag publish takes when the task's flow brings a
	// workspace: once sealing has decided the run's answer, publish moves
	// the run's directory from where it is (FlagOut, on the work/ shelf)
	// onto the results/ shelf a later phase reads back, one rename within
	// the same volume (§ADR-0002 決定5). Absent means publish only seals —
	// the template-volume case, where there is no shelf to move onto.
	// prepare refuses it outright — it is publish's alone.
	FlagSealTo = "seal-to"

	// FlagShelve is a run that never had a pod, and the directory it
	// answered with, as the path the two make on the results/ shelf. It may
	// be given more than once, which is what a delimiter would have had to
	// be chosen for: a declared directory name is a free string and nothing
	// stops one containing whatever the delimiter was.
	//
	// A run the framework does not start seals nothing, because there is no
	// pod of its own to seal from (ADR-0011 決定7): its number would leave a
	// hole on the shelf a later phase reads back, and a hole is only
	// readable by whoever already knows it is there (ADR-0004). So the next
	// run that does have a pod lays the empty directory the answer amounts
	// to — an empty declared directory is already what a verdict looks like,
	// so nothing new had to be invented to say it.
	//
	// prepare lays only what is missing: a run with a pod shelves itself,
	// and one of those found already there is left exactly as its own
	// publish sealed it.
	FlagShelve = "shelve"

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

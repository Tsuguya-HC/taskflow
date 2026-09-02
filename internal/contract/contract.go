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
	// agent has no use for it — it opens work/1/ok while the run is in
	// flight and reads — and there is reason to keep it away from one: a run
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
	// into the run's out/ and publish reads it back before it moves the run
	// onto the results/ shelf, so a publish whose pod is already gone from
	// the apiserver — but still running, and still due its SIGTERM — cannot
	// shelve the directory a later attempt at the same runID is working in
	// (ADR-0004). It is not generation-time information the way the run's
	// paths are, so it is the one thing the sidecars take from the pod
	// rather than from their arguments.
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
	// sidecar's flag set use for the directory the declared directories are
	// created under.
	FlagOut = "out"

	// FlagSealFrom and FlagSealTo name the flags publish takes when the
	// task's flow brings a workspace: once sealing has decided the run's
	// answer, publish moves its own directory from the work/ shelf it wrote
	// into onto the results/ shelf a later phase reads back, one rename
	// within the same volume (§ADR-0002 決定5). Both absent means publish
	// only seals — the template-volume case, where there is no shelf to
	// move between. One without the other is refused rather than treated as
	// neither: a run silently left sealing-only in a flow workspace would
	// never reach the results/ shelf a later phase reads, with nothing to
	// say why. prepare refuses both outright — they are publish's alone.
	FlagSealFrom = "seal-from"
	FlagSealTo   = "seal-to"

	// FlagRunDir is prepare's own run directory under a flow workspace —
	// prepare mounts work/ itself and makes this run's directory inside it,
	// so the directory is prepare's to create and open up rather than
	// whatever the kubelet's subPath machinery would have left there.
	// Absent for a template-backed workspace, which has no run layout.
	FlagRunDir = "run-dir"
	// FlagSweep is the comma-separated runIDs whose work/ leftovers prepare
	// clears away before this run starts. The controller computes the list —
	// only it knows which runs are live — and prepare deletes exactly what
	// it is told (ADR-0003). Sealed runs left work/ when their rename moved
	// them, so what this actually removes is the debris of attempts that
	// died before sealing. Meaningful only with FlagRunDir.
	FlagSweep = "sweep"
)

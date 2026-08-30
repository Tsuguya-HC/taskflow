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
	// AnnotationRunID is read by the plumbing that files results under
	// results/<runID>/. The agent has no use for it — it opens
	// results/1/ok and reads — and there is reason to keep it away from
	// one: a run count suggests how much rope is left, the same way a
	// remaining-rework count would.
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
)

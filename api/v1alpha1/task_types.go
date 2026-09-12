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

package v1alpha1

import (
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// TaskSpec is the whole instance. Four fields, three of them optional: whoever
// creates a task — a cron, an event, an agent — does not have to know the
// graph, and the graph can change without them changing.
//
// The spec is immutable after creation. Editing what a running task was asked
// to do would leave its history describing a question nobody asked.
//
// The rule covers input as well, which is not obvious: input is
// x-kubernetes-preserve-unknown-fields, and CEL cannot address what is inside
// it. Comparing the whole spec still works — measured, with the rule removed
// as a control, in api_shapes_test.go.
//
// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="a task's spec is fixed at creation; make a new task rather than changing what this one was asked to do"
type TaskSpec struct {
	// Flow is resolved in this namespace, always. There is no field naming
	// another namespace, so "which flows may I start" collapses into "which
	// namespaces may I create a Task in" — which plain RBAC can express, and
	// field values cannot.
	// +kubebuilder:validation:MinLength=1
	Flow string `json:"flow"`

	// Input is written to in/input.json and exposed to prompts as template
	// variables. Its shape is the flow author's business; the controller only
	// carries it.
	// +kubebuilder:pruning:PreserveUnknownFields
	// +optional
	Input *apiextensionsv1.JSON `json:"input,omitempty"`

	// DedupKey suppresses creation while another task with the same key is
	// still running. Useful when the producer is an event source that may
	// deliver twice.
	// +optional
	DedupKey string `json:"dedupKey,omitempty"`
}

// RunRef identifies one execution of one phase. runID separates the second
// visit to a phase from the first, so a rework does not read what the previous
// run left behind. It does not separate two attempts at starting one run —
// an infrastructure retry comes back with the same number (ADR-0004), and
// infraRetries is what tells those apart.
//
// A task's currentRun, when it has one, ordinarily names the phase the task
// is on. The one exception is the cleanup run a task that has reached its
// ending may carry: there, currentRun.phase is the reserved name Finally
// while status.phase stays at the ending itself (ADR-0009), and
// taskstate.InFinally is the predicate that tells that apart from a task
// mid-flight. A task that has stopped for good, with no cleanup run in
// flight or owed, has no currentRun at all.
type RunRef struct {
	Phase Phase `json:"phase"`
	RunID int32 `json:"runID"`
	// JobName is derived deterministically from the task, phase, runID and
	// the count of infrastructure retries, so a controller restart re-creates
	// the same object instead of a second one.
	//
	// Empty for a run the framework does not start; verdictBox below is what
	// such a run has instead, and exactly one of the two is set once an
	// attempt is under way. Which one says how this run is being driven, and
	// the controller reads it from here rather than from the handler: an
	// attempt already in flight is not re-decided by a definition edited
	// underneath it (ADR-0007).
	//
	// "Once an attempt" rather than "once a run" on purpose: an infrastructure
	// retry (taskstate.RetryInfra) rebuilds this ref with neither field set,
	// so the run it carries goes back to having both empty until the next
	// reconcile reads the handler again — the same moment ensureJob would
	// build a fresh Job for it anyway. The invariant is real, but it resets
	// at every attempt boundary rather than holding across all of a run's.
	//
	// The reset only ever happens to a run with a Job, though: retryInfra is
	// reached from driveJobRun's own reading of a Job's pods, and a run with
	// a verdictBox instead never goes through it — nothing started, so
	// nothing can have failed to start. A run being driven by state is fixed
	// for the run's whole life the moment its box is opened; the "one
	// attempt" this invariant resets at is a distinction that only exists on
	// the Job side.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// VerdictBox names the ConfigMap this run's answer appears in, for a run
	// the framework does not start (ADR-0011). Empty for every run that has
	// a Job.
	//
	// It is written down before the object is created, not after. That
	// ordering is what lets the controller tell its own box from one somebody
	// put there first: on the reconcile that first opens a box this field is
	// empty, so the create is unconditional and an AlreadyExists means the
	// place was taken before the run began. It is also how whoever is meant
	// to answer finds where to write, without being told a naming rule.
	//
	// That does not hold on the repair path, where this field is already set
	// but the box it names is not there and this run has never been dated
	// (ensureVerdictBox): the create there cannot tell a squatter from its own
	// earlier create having landed late, so it is not treated as fencing
	// there — an AlreadyExists is retried, and the next reconcile's Get is
	// what actually decides whether the box is this run's.
	//
	// This is not a pointer to where a run's output went (ADR-0008): the
	// framework decided this name, created the object and reads it back. What
	// it must not become is a second place to say where results live.
	// +optional
	VerdictBox string `json:"verdictBox,omitempty"`
	// VerdictBoxUID is the apiserver's own UID for the object VerdictBox
	// names, stamped the moment this run's Create succeeds (ensureVerdictBox)
	// and empty until then. The name alone is not enough to say a later Get
	// found the same object: a name can be deleted and recreated, and nothing
	// about the new object need be the same one except its name — least of
	// all its ownerReferences, which are free-form metadata their author
	// wrote and which IsControlledBy alone cannot tell forged from genuine.
	// This UID is different: the apiserver assigns it, once, to the object
	// this run's own Create actually made, and no later Create under the same
	// name can produce it again. Once it is stamped, a Get returning an
	// object with a different UID is refused (ensureVerdictBox) whatever its
	// ownerReferences claim — IsControlledBy still runs, and is still what
	// decides the one window before this is stamped: the moment right after a
	// Create whose response this process never saw.
	// +optional
	VerdictBoxUID types.UID `json:"verdictBoxUID,omitempty"`
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`

	// InfraRetries counts attempts lost to something other than the handler's
	// judgement. It is not in the design's status example, but maxInfraRetries
	// cannot be enforced without somewhere to count.
	// +optional
	InfraRetries int32 `json:"infraRetries,omitempty"`
}

// Runner says how r is being driven, when that is something r itself already
// says: RunnerState once VerdictBox is set, RunnerJob once JobName is — the
// same rule in the one place it should live, rather than copied wherever
// something needs to tell the two apart. The zero value means neither field
// is set yet, and is deliberately not one of the two real answers: a run
// about to start has not picked a kind, and a caller with its own idea of
// what an unset ref means (the controller reads the handler; taskstate
// defaults to Job) is the one that gets to decide, not this method.
func (r *RunRef) Runner() RunnerType {
	switch {
	case r.VerdictBox != "":
		return RunnerState
	case r.JobName != "":
		return RunnerJob
	default:
		return ""
	}
}

// HistoryEntry records a completed run. This is the audit trail, and it is
// the whole of what the framework knows: what the run decided and why. Where
// the run's output ended up is not here, because the controller never learns
// it — it does not touch the workspace or any store (ADR-0008).
// HistoryReasonMaxLength is HistoryEntry.Reason's own limit, mirrored here in
// Go because the marker below cannot be read at runtime. The two must stay in
// sync — this is what taskstate.Advance and taskstate.FinishFinally clamp an
// assembled Reason to before it ever reaches the write the CRD would
// otherwise refuse, and a mismatch would mean one of them stopped meaning
// what it says. Counted in runes, the same unit the CRD's maxLength counts in.
const HistoryReasonMaxLength = 2048

type HistoryEntry struct {
	Phase Phase `json:"phase"`
	RunID int32 `json:"runID"`
	// Directory the handler wrote into, empty when the run produced no single
	// answer.
	// +optional
	Directory string `json:"directory,omitempty"`
	// Outcome is why the task moved: the framework's account of the run,
	// recorded even when the handler said nothing.
	Outcome string `json:"outcome"`
	// Runner is how the run was driven. It is here, and not only on the
	// handler, because a run the framework did not start seals nothing: the
	// next run that has a pod lays its directory on the shelf instead
	// (ADR-0011 決定7), and by then the handler may say something else or be
	// gone. It is also the honest answer to what a human reads this line
	// for — whether anything ran at all.
	//
	// Empty reads as Job — but not because Job was ever the only kind a run
	// could have had before this field existed: ADR-0011 決定2〜6 already let
	// a build drive and settle a State run before 決定7 added this field, so
	// a task that reached settle in that window has a history line that is
	// empty for a run that never had a pod, and reading it as Job is wrong
	// for exactly that line. There is no fixing it after the fact — the
	// handler bound to that phase may have changed since, or be gone — so
	// shelfHoles simply does not shelve that run's answer: its number is a
	// gap 決定7 cannot close.
	// +optional
	Runner RunnerType `json:"runner,omitempty"`
	// Reason is the one line a human reads next to Outcome: which edge was
	// followed, or why no answer counted, or what the handler said after
	// naming its directory. The transition never reads it.
	// +optional
	// Kept in sync with HistoryReasonMaxLength above.
	// +kubebuilder:validation:MaxLength=2048
	Reason string `json:"reason,omitempty"`
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
}

// TaskStatus holds control state only. Logs, reports and results are not here
// — etcd has been lost twice here, and a design that puts payloads in it turns
// that from an outage into data loss. Nor are pointers to them: where a run's
// output was put is the deployment's arrangement, and this status is written
// by a controller that has no part in it (ADR-0008).
type TaskStatus struct {
	// +optional
	Phase Phase `json:"phase,omitempty"`

	// RunID of the current or last run. It counts the runs this task has
	// decided its way through, not the attempts it took to start them: an
	// infrastructure retry leaves it alone, so the numbers on the results/
	// shelf run without gaps (ADR-0004).
	// +optional
	RunID int32 `json:"runID,omitempty"`

	// ReworkBudget remaining. Counted down by the controller as reworks are
	// taken, never declared by the flow.
	// +optional
	ReworkBudget int32 `json:"reworkBudget,omitempty"`

	// +optional
	CurrentRun *RunRef `json:"currentRun,omitempty"`

	// ExpiresAt is when the controller deletes this task, set once it stops.
	// The moment is fixed here rather than derived from the flow's ttl each
	// time, so a task at Escalated keeps its date even after the flow that
	// set it is edited or gone — the same reason the reserved phases need no
	// flow to be terminal.
	// +optional
	ExpiresAt *metav1.Time `json:"expiresAt,omitempty"`

	// +optional
	// +listType=atomic
	History []HistoryEntry `json:"history,omitempty"`

	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Flow",type=string,JSONPath=`.spec.flow`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Run",type=integer,JSONPath=`.status.runID`
// +kubebuilder:printcolumn:name="Budget",type=integer,JSONPath=`.status.reworkBudget`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Task is one execution of a flow.
type Task struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TaskSpec   `json:"spec,omitempty"`
	Status TaskStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TaskList contains a list of Task.
type TaskList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Task `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &Task{}, &TaskList{})
		return nil
	})
}

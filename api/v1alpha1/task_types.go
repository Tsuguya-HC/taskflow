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
)

// TaskSpec is the whole instance. Four fields, three of them optional: whoever
// creates a task — a cron, an event, an agent — does not have to know the
// graph, and the graph can change without them changing.
//
// The spec is immutable after creation. Editing what a running task was asked
// to do would leave its history describing a question nobody asked.
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
type RunRef struct {
	Phase Phase `json:"phase"`
	RunID int32 `json:"runID"`
	// JobName is derived deterministically from the task, phase, runID and
	// the count of infrastructure retries, so a controller restart re-creates
	// the same object instead of a second one.
	// +optional
	JobName string `json:"jobName,omitempty"`
	// +optional
	Deadline *metav1.Time `json:"deadline,omitempty"`

	// InfraRetries counts attempts lost to something other than the handler's
	// judgement. It is not in the design's status example, but maxInfraRetries
	// cannot be enforced without somewhere to count.
	// +optional
	InfraRetries int32 `json:"infraRetries,omitempty"`
}

// HistoryEntry records a completed run. This is the audit trail; the artifacts
// it points at live in object storage, never in etcd.
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
	// Reason is the one line a human reads next to Outcome: which edge was
	// followed, or why no answer counted, or what the handler said after
	// naming its directory. The transition never reads it.
	// +optional
	// +kubebuilder:validation:MaxLength=2048
	Reason string `json:"reason,omitempty"`
	// Ref points at the run's output in the store.
	// +optional
	Ref string `json:"ref,omitempty"`
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
}

type ArtifactsRef struct {
	// +optional
	Store string `json:"store,omitempty"`
}

// TaskStatus holds control state only. Logs, reports and results are
// references — etcd has been lost twice here, and a design that puts payloads
// in it turns that from an outage into data loss.
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

	// +optional
	Artifacts *ArtifactsRef `json:"artifacts,omitempty"`

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

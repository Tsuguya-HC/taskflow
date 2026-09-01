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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Profile is a validation schema, not a behaviour switch: it says which phases
// a flow must bind. It is a controller-side enum rather than a third CRD —
// adding one should mean code, tests and a matching network policy, not a line
// of YAML.
// +kubebuilder:validation:Enum=investigate
type Profile string

const (
	ProfileInvestigate Profile = "investigate"
)

// TerminalSeverity is what reaching one of a flow's endings means.
//
// The framework cannot work this out. A status with no binding is where the
// flow stops, and that is all it knows: 失敗 and おわり are equally just names
// its author chose, and neither is spelled in a vocabulary the framework
// owns. So the flow says which of its endings are bad news, and only then can
// the framework raise its voice about one.
// +kubebuilder:validation:Enum=Success;Failure
type TerminalSeverity string

const (
	// TerminalSuccess is an ending that needs nobody.
	TerminalSuccess TerminalSeverity = "Success"
	// TerminalFailure is an ending somebody has to see. The run finished and
	// the handler concluded; what it concluded is that something is wrong.
	// This is not Escalated — nothing is undecided — and not Failed, which
	// is a defect in the flow rather than a finding about the work.
	TerminalFailure TerminalSeverity = "Failure"
)

// PhaseBinding says who fills a phase and where each of their answers leads.
//
// Next is keyed by the status to move to, and its value is the directory a
// handler writes into to choose that status. One declaration does three jobs:
// it is the edge, it is the set of directories the framework creates for the
// run, and — because those are the only directories that exist — it is the
// handler's entire vocabulary. A token outside it cannot be produced rather
// than merely being rejected.
//
// Two statuses must not share a directory; that is refused at creation, since
// at runtime it would leave the destination undecidable.
type PhaseBinding struct {
	// Handler names a TaskHandler in the same namespace. There is deliberately
	// no field for another namespace: keeping resolution local reduces "which
	// flows may I start" to "which namespaces may I create a Task in", which
	// plain RBAC can express.
	// +kubebuilder:validation:MinLength=1
	Handler string `json:"handler"`

	// Next maps a destination status to the directory that selects it.
	// A destination with no binding of its own is where the task stops.
	// +kubebuilder:validation:MinProperties=1
	Next map[Phase]string `json:"next"`
}

// TTLSpec is how long a finished task sticks around. Cleanup is anchored on
// the Task and propagates through ownerReferences; Jobs are never given a TTL
// of their own, because a Job that deletes itself takes the verdict with it.
//
// Both fields default rather than stay optional: a task that never goes away
// is a row in etcd that never goes away, and etcd has been lost here twice
// (§10 "etcd 保護のため必須"). The defaults are the design's own example.
type TTLSpec struct {
	// Succeeded applies to a task that stopped at a phase the flow declared
	// terminal — one with no binding of its own.
	// +kubebuilder:default="1h"
	// +optional
	Succeeded *metav1.Duration `json:"succeeded,omitempty"`
	// Failed applies to a task that stopped at Escalated or Failed, however
	// it got there — the framework forcing a stop or the flow itself
	// declaring the edge with next. A human has to look at those, so they
	// wait longer.
	// +kubebuilder:default="168h"
	// +optional
	Failed *metav1.Duration `json:"failed,omitempty"`
}

// FlowWorkspace gives every task of this flow one volume that lives exactly
// as long as the task, so consecutive phases hand their results to each other
// through it. It sits on the flow, not the handler or the task: what crosses
// phases belongs to the thing that defines the set of phases — a handler is
// one phase and cannot own what spans several, and a task is stamped out by
// whatever submits it, which should not have to know about storage.
type FlowWorkspace struct {
	// VolumeClaimTemplate is the claim's spec, and only its spec — the
	// controller owns the claim's name and ownership, and a whole
	// PersistentVolumeClaim here would let a flow fight it over both (and
	// carries name, merge-key and serialization problems its users have
	// documented). It is the standard type: storageClassName, accessModes,
	// resources, even volumeName for a hand-made PV all mean exactly what
	// they mean on any claim. Left out entirely, it defaults to a small
	// ReadWriteMany scratch claim on the cluster's default StorageClass;
	// written at all, it is taken as written — the default is a whole
	// template, not a base to merge into.
	//
	// Changing it affects tasks created afterwards. Claims that already
	// exist are never rewritten to match.
	// +kubebuilder:default={accessModes: {ReadWriteMany}, resources: {requests: {storage: "1Gi"}}}
	// +optional
	VolumeClaimTemplate *corev1.PersistentVolumeClaimSpec `json:"volumeClaimTemplate,omitempty"`
}

// TaskFlowSpec is the topology. It is validated once, when the TaskFlow is
// created, and not re-derived per task.
//
// +kubebuilder:validation:XValidation:rule="!has(self.terminals) || self.terminals.all(p, !(p in self.bindings))",message="terminals may only name a phase with no binding of its own, since a phase something binds is not where the flow ends"
// +kubebuilder:validation:XValidation:rule="!has(self.terminals) || (!('Escalated' in self.terminals) && !('Failed' in self.terminals))",message="Escalated and Failed are the framework's own endings and their meaning is not the flow's to declare"
type TaskFlowSpec struct {
	Profile Profile `json:"profile"`

	// Start names the phase a task begins at.
	//
	// Stated rather than worked out. It could be inferred — the phase no
	// binding sends anything to — but P8 refuses to guess, and the inference
	// is wrong for any flow whose first phase is also where a rework returns:
	// there, every phase is somebody's destination and the guess finds
	// nothing.
	// +kubebuilder:validation:MinLength=1
	Start Phase `json:"start"`

	// Bindings is keyed by phase name.
	// +kubebuilder:validation:MinProperties=1
	Bindings map[Phase]PhaseBinding `json:"bindings"`

	// Terminals says what this flow's endings mean. It is keyed by a phase
	// nothing binds — one of the places a task stops — and says whether
	// stopping there is good news. A Failure ending is the one thing that
	// makes the framework speak up about a task that finished: a warning
	// event, Ready=False, and the longer ttl, so the record is still there
	// when somebody comes to look.
	//
	// Optional, and leaving it out is not the same as saying Success. A flow
	// that has not declared keeps exactly the silence it had before this
	// field existed, and its endings are reported as Undeclared rather than
	// guessed at (P8). Declaring one ending does not oblige the flow to
	// declare the rest.
	// +optional
	Terminals map[Phase]TerminalSeverity `json:"terminals,omitempty"`

	// ReworkBudget caps how many times this flow may send work back. It is
	// spent at runtime by the controller, never declared per edge — an edge
	// that had to say "and decrement" would be an expression, and expressions
	// cannot be checked without running them (P9).
	// +kubebuilder:validation:Minimum=0
	// +optional
	ReworkBudget int32 `json:"reworkBudget,omitempty"`

	// MaxInFlight caps concurrent tasks of this flow.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxInFlight *int32 `json:"maxInFlight,omitempty"`

	// +kubebuilder:default={}
	// +optional
	TTL *TTLSpec `json:"ttl,omitempty"`

	// Workspace, when set, backs the flow's workspace with one claim per
	// task, created by the controller and deleted with the task. A handler
	// joins by naming the reserved volume flow-workspace; a flow that leaves
	// this out keeps today's arrangement, where each handler's template
	// brings its own volume and nothing crosses a phase.
	// +optional
	Workspace *FlowWorkspace `json:"workspace,omitempty"`
}

// TaskFlowStatus reports whether the flow was accepted. Nothing reconciles a
// TaskFlow on its own; this is set when it is validated.
type TaskFlowStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=tf
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=`.spec.profile`
// +kubebuilder:printcolumn:name="Accepted",type=string,JSONPath=`.status.conditions[?(@.type=="Accepted")].status`,priority=1
// +kubebuilder:printcolumn:name="Budget",type=integer,JSONPath=`.spec.reworkBudget`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TaskFlow is the type of a task: which phases exist, who fills them, and
// where each verdict leads.
type TaskFlow struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TaskFlowSpec   `json:"spec,omitempty"`
	Status TaskFlowStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TaskFlowList contains a list of TaskFlow.
type TaskFlowList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TaskFlow `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &TaskFlow{}, &TaskFlowList{})
		return nil
	})
}

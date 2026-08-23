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
type TTLSpec struct {
	// +optional
	Succeeded *metav1.Duration `json:"succeeded,omitempty"`
	// +optional
	Failed *metav1.Duration `json:"failed,omitempty"`
}

// TaskFlowSpec is the topology. It is validated once, when the TaskFlow is
// created, and not re-derived per task.
type TaskFlowSpec struct {
	Profile Profile `json:"profile"`

	// Bindings is keyed by phase name.
	// +kubebuilder:validation:MinProperties=1
	Bindings map[Phase]PhaseBinding `json:"bindings"`

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

	// +optional
	TTL *TTLSpec `json:"ttl,omitempty"`
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

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

// RunnerType selects how a phase is executed.
//
// Argo Workflows is deliberately absent. Multi-step work fits in one pod with
// shared volumes, so an Argo runner would buy only the reuse of existing
// WorkflowTemplates — not worth the dependency, and a single pod structurally
// avoids the "emptyDir is not shared between steps" trap that cost a day.
// +kubebuilder:validation:Enum=Job;External
type RunnerType string

const (
	// RunnerJob runs the handler as a batch Job. The only runner in Step 1.
	RunnerJob RunnerType = "Job"
	// RunnerExternal means something outside the cluster fills the phase and
	// reports back. A human reviewer is this.
	RunnerExternal RunnerType = "External"
)

// EmbeddedObjectMeta is the part of a pod's metadata a handler may set.
//
// Spelled out rather than embedding metav1.ObjectMeta, because controller-gen
// renders that as an object with no properties and a structural schema drops
// everything inside one. Labels written there vanish on write with nothing
// said — and those are the labels a network policy selects on, so in a
// default-deny namespace the symptom is traffic disappearing rather than an
// error. Measured, not guessed: the server stored template.metadata as {}.
type EmbeddedObjectMeta struct {
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// PodTemplate is the pod a run consists of.
type PodTemplate struct {
	// +optional
	Metadata EmbeddedObjectMeta `json:"metadata,omitzero"`
	Spec     corev1.PodSpec     `json:"spec"`
}

// JobTemplate is what a handler runs.
//
// Deliberately not batchv1.JobTemplateSpec. Most of a JobSpec is reserved —
// backoffLimit, ttlSecondsAfterFinished, activeDeadlineSeconds, completions
// and parallelism all break something the design relies on — and a field that
// does not exist cannot be set wrong. The directories work the same way: a
// token outside the vocabulary is not rejected, it is unwritable.
type JobTemplate struct {
	Template PodTemplate `json:"template"`
}

type RunnerSpec struct {
	// +kubebuilder:default=Job
	Type RunnerType `json:"type"`
}

// TaskHandlerSpec is the box that fills one phase.
//
// There is no field here for a prompt, a model, an API key, a store or a
// notification target, and that absence is the design: the controller's
// vocabulary is phases, Jobs and verdicts, and everything an LLM needs lives
// in the pod spec its author writes. The same shape therefore accepts a
// linter, a test suite or a shell script — from the controller's side there is
// no difference between those and an agent.
type TaskHandlerSpec struct {
	// Phase this handler can fill — a status name from the flow's own
	// vocabulary. The framework owns only two names and refuses those; every
	// other string is the author's to choose.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:XValidation:rule="self != 'Escalated' && self != 'Failed'",message="Escalated and Failed are decided by the controller; a handler cannot fill them"
	Phase Phase `json:"phase"` // CEL above must stay in sync with ReservedPhases (phase.go)

	// +kubebuilder:default={type: Job}
	// +optional
	Runner RunnerSpec `json:"runner,omitzero"`

	// JobTemplate is handed to the runner as written. The controller does not
	// add labels, service accounts or images to it: what a pod must carry to
	// be selected by a network policy is a property of the cluster it runs in,
	// not something a framework may decide. What the framework provides is one
	// place to say it — the same shape CronJob uses — rather than the two
	// locations with differing semantics that Argo requires.
	//
	// Required when runner.type is Job; ignored when it is External.
	// +optional
	JobTemplate *JobTemplate `json:"jobTemplate,omitempty"`

	// Timeout is the single source of truth for how long a run may take. The
	// controller writes it into the Job as well, so the kubelet enforces it
	// too; setting activeDeadlineSeconds yourself is rejected.
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// MaxInfraRetries bounds retries for failures that are not the handler's
	// judgement — an image that would not pull, an evicted pod. A handler that
	// ran and reached no conclusion is not retried here; it is indeterminate,
	// and indeterminate goes to a human.
	// +kubebuilder:validation:Minimum=0
	// +optional
	MaxInfraRetries int32 `json:"maxInfraRetries,omitempty"`
}

type TaskHandlerStatus struct {
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=th
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.spec.phase`
// +kubebuilder:printcolumn:name="Runner",type=string,JSONPath=`.spec.runner.type`
// +kubebuilder:printcolumn:name="Timeout",type=string,JSONPath=`.spec.timeout`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TaskHandler is a reusable box that fills one phase. Flows name it; it does
// not know which flows use it.
type TaskHandler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TaskHandlerSpec   `json:"spec,omitempty"`
	Status TaskHandlerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TaskHandlerList contains a list of TaskHandler.
type TaskHandlerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TaskHandler `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &TaskHandler{}, &TaskHandlerList{})
		return nil
	})
}

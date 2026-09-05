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
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/flowcheck"
)

// SetupTaskFlowWebhookWithManager registers the webhook for TaskFlow in the manager.
func SetupTaskFlowWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &flowv1alpha1.TaskFlow{}).
		WithValidator(&TaskFlowCustomValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-flow-tgy-io-v1alpha1-taskflow,mutating=false,failurePolicy=fail,sideEffects=None,groups=flow.tgy.io,resources=taskflows,verbs=create;update,versions=v1alpha1,name=vtaskflow-v1alpha1.kb.io,admissionReviewVersions=v1

// TaskFlowCustomValidator refuses a TaskFlow that contradicts itself, at the
// moment it is written rather than at the moment a task walks into the
// contradiction (§5「矛盾したら作らせない」).
//
// It holds nothing. Every question it answers is answered from the object in
// front of it — no client, no cache, no cluster state — which is what lets
// the same rules be exercised as a pure function (internal/flowcheck) and
// keeps this file down to translating a field.ErrorList into a rejection.
//
// It is deliberately not registered for delete: a flow that already exists
// was accepted, and refusing to remove it would only strand it.
type TaskFlowCustomValidator struct{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type TaskFlow.
func (v *TaskFlowCustomValidator) ValidateCreate(_ context.Context, obj *flowv1alpha1.TaskFlow) (admission.Warnings, error) {
	return nil, validate(obj)
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type TaskFlow.
//
// The new object is judged on its own, with no reference to the old one. A
// flow is edited by GitOps rolling out a new definition, and what matters is
// whether what lands is coherent — not how far it moved. Tasks already
// running under the previous definition are not this webhook's business
// either: their runs are held to the spec their Jobs froze (ADR-0007), and
// the controller answers for a task whose flow changed underneath it.
func (v *TaskFlowCustomValidator) ValidateUpdate(_ context.Context, _, newObj *flowv1alpha1.TaskFlow) (admission.Warnings, error) {
	return nil, validate(newObj)
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type TaskFlow.
func (v *TaskFlowCustomValidator) ValidateDelete(_ context.Context, _ *flowv1alpha1.TaskFlow) (admission.Warnings, error) {
	return nil, nil
}

// validate turns what flowcheck found into the rejection the API server
// reports. All of it, in one status: apierrors.NewInvalid renders every cause
// with its field path, so an author fixing a flow by hand sees the whole list
// instead of peeling it one apply at a time.
func validate(flow *flowv1alpha1.TaskFlow) error {
	errs := flowcheck.Check(&flow.Spec, field.NewPath("spec"))
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(
		schema.GroupKind{Group: flowv1alpha1.GroupVersion.Group, Kind: "TaskFlow"},
		flow.GetName(), errs)
}

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

// snapshot is what is actually copied: the flow's spec, and the spec of
// every handler the copy names. Status is deliberately absent — what the
// reference read when it was copied is the definition, never its progress.
type snapshot struct {
	Flow     flowv1alpha1.TaskFlowSpec               `json:"flow"`
	Handlers map[string]flowv1alpha1.TaskHandlerSpec `json:"handlers"`
}

// ensureSnapshot copies the flow's spec and the spec of every handler the
// flow's bindings name into one ControllerRevision owned by the task, before
// the task's first run.
//
// Bindings alone decide which handlers the copy needs: a handler nothing
// binds is not part of what any run of this task can reach. The cleanup
// handler rides along when it exists, and is skipped when it does not —
// its absence is recorded where the cleanup run is owed, not here.
//
// Whether the copy fits is left to the apiserver's refusal rather than a
// ceiling of our own: a second ceiling would have to be kept in step with
// whatever limits the storage below it is run with.
func (r *TaskReconciler) ensureSnapshot(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
) error {
	if r.APIReader == nil {
		// Reading back an existing copy through the cache would need list and
		// watch on ControllerRevisions, which the controller does not hold.
		return errors.New("controller: no uncached reader to read a snapshot revision with")
	}
	handlers, err := r.snapshotHandlers(ctx, task, flow)
	if err != nil {
		return err
	}
	data, err := json.Marshal(snapshot{Flow: *flow.Spec.DeepCopy(), Handlers: handlers})
	if err != nil {
		return err
	}
	rev := runner.BuildSnapshotRevision(task, data)
	err = r.Create(ctx, rev)
	switch {
	case err == nil:
		return nil
	case tooLarge(err):
		return brokenFlow{fmt.Sprintf("task %q definitions do not fit in one object", task.Name)}
	case !apierrors.IsAlreadyExists(err):
		return err
	}
	// A copy already there is this task's own from a begin whose status write
	// did not land, or a predecessor's under the same name that has not been
	// collected yet. The latter is not adopted; returning it plain retries
	// until garbage collection frees the name.
	var got appsv1.ControllerRevision
	if err := r.APIReader.Get(ctx, types.NamespacedName{Name: rev.Name, Namespace: task.Namespace}, &got); err != nil {
		return err
	}
	if !metav1.IsControlledBy(&got, task) {
		return notOwnedError("snapshot revision", rev.Name, task, got.OwnerReferences)
	}
	return nil
}

// snapshotHandlers resolves every handler the snapshot must carry: one per
// binding, plus the cleanup handler when it exists. A binding naming a
// handler that does not exist fails the task through handlerFor's brokenFlow;
// a missing cleanup handler is skipped instead.
func (r *TaskReconciler) snapshotHandlers(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
) (map[string]flowv1alpha1.TaskHandlerSpec, error) {
	phases := make([]flowv1alpha1.Phase, 0, len(flow.Spec.Bindings))
	for phase := range flow.Spec.Bindings {
		phases = append(phases, phase)
	}
	slices.Sort(phases)
	handlers := make(map[string]flowv1alpha1.TaskHandlerSpec, len(phases)+1)
	for _, phase := range phases {
		name := flow.Spec.Bindings[phase].Handler
		if _, done := handlers[name]; done {
			continue
		}
		handler, err := r.handlerFor(ctx, task, name, phase)
		if err != nil {
			return nil, err
		}
		handlers[name] = handler.Spec
	}
	if flow.Spec.Finally == nil {
		return handlers, nil
	}
	name := flow.Spec.Finally.Handler
	if _, done := handlers[name]; done {
		return handlers, nil
	}
	handler, err := r.handlerFor(ctx, task, name, flowv1alpha1.PhaseFinally)
	var broken brokenFlow
	switch {
	case err == nil:
		handlers[name] = handler.Spec
	case !errors.As(err, &broken):
		return nil, err
	}
	return handlers, nil
}

// tooLarge reports whether the apiserver refused an object for its size.
// Creating one ControllerRevision against envtest's 1.37 control plane
// (2026-10-04) was refused three ways: at 1.5 MiB a 500 "etcdserver: request
// is too large", at 2 to 2.5 MiB a 500 "rpc error: code = ResourceExhausted
// desc = trying to send message larger than max (...)", and at 4 MiB a 413
// from the apiserver's own body limit. The 500s carry no reason of their own,
// so the message is the only thing that tells them from other internal
// errors. A bare ResourceExhausted is not matched: it is also what overload
// looks like, and that should retry.
func tooLarge(err error) bool {
	if apierrors.IsRequestEntityTooLargeError(err) {
		return true
	}
	if !apierrors.IsInternalError(err) {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "request is too large") || strings.Contains(msg, "trying to send message larger than max")
}

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

// snapshotLimit is the largest payload the copy of a task's definitions may
// carry. etcd refuses a single object past its own ceiling, and the copy is
// one object, so a task whose definitions do not fit cannot start — the
// failure below is that refusal read back, not a size the framework chose.
const snapshotLimit = 3 << 20

// kindTask is the kind the snapshot's ownerReference points at.
const kindTask = "Task"

// snapshot is what is actually copied: the flow's spec, and the spec of
// every handler the copy names. Status is deliberately absent — what the
// reference read when it was copied is the definition, never its progress.
type snapshot struct {
	Flow     flowv1alpha1.TaskFlowSpec               `json:"flow"`
	Handlers map[string]flowv1alpha1.TaskHandlerSpec `json:"handlers"`
}

// snapshotRevisionName derives the revision's name from the task. The UID is
// hashed in for the same reason the workspace claim and the verdict box carry
// it: a task deleted and recreated under the same name is a different task,
// and a bare name would hand the new task its predecessor's copy. Storing the
// name in status instead would have handed every later reconcile a pointer to
// trust; deriving it recomputes the same name and checks the owner each time.
func snapshotRevisionName(taskName string, uid types.UID) string {
	sum := sha256.Sum256([]byte(uid))
	suffix := "-snapshot-" + hex.EncodeToString(sum[:])[:8]
	prefix := taskName
	if len(prefix)+len(suffix) > 63 {
		prefix = taskName[:63-len(suffix)]
	}
	return prefix + suffix
}

// ensureSnapshot copies the flow's spec and the spec of every handler the
// flow's bindings name into one ControllerRevision owned by the task, before
// the task's first run. It returns the revision it created or adopted.
//
// Bindings alone decide which handlers the copy needs: a handler nothing
// binds is not part of what any run of this task can reach. The cleanup
// handler rides along when it exists, and is skipped when it does not —
// its absence is recorded where the cleanup run is owed, not here.
//
// A revision found under the name that names a different UID is not this
// task's, so it is refused rather than adopted: the name alone cannot tell a
// predecessor's leftover from this task's own copy.
func (r *TaskReconciler) ensureSnapshot(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
) (*appsv1.ControllerRevision, error) {
	handlers, err := r.snapshotHandlers(ctx, task, flow)
	if err != nil {
		return nil, err
	}
	rev := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotRevisionName(task.Name, task.UID),
			Namespace: task.Namespace,
			Labels: map[string]string{
				runner.LabelManagedBy: runner.ManagedBy,
				runner.LabelTaskUID:   string(task.UID),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         flowv1alpha1.SchemeGroupVersion.String(),
				Kind:               kindTask,
				Name:               task.Name,
				UID:                task.UID,
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(false),
			}},
		},
		Data: runtime.RawExtension{Raw: mustSnapshotJSON(&flow.Spec, handlers)},
	}
	// What counts as too large is judged on the bytes actually written, not on
	// an estimate made beforehand: the estimate would be a second ceiling to
	// keep in step with the encoding, and the apiserver's refusal already says
	// which side of its own limit the object falls on.
	if len(rev.Data.Raw) > snapshotLimit {
		return nil, brokenFlow{fmt.Sprintf(
			"task %q definitions do not fit in one object (%d bytes)", task.Name, len(rev.Data.Raw))}
	}
	if err := r.Create(ctx, rev); err != nil {
		if apierrors.IsAlreadyExists(err) {
			var got appsv1.ControllerRevision
			if err := r.Get(ctx, types.NamespacedName{Name: rev.Name, Namespace: task.Namespace}, &got); err != nil {
				return nil, err
			}
			if !metav1.IsControlledBy(&got, task) {
				return nil, notOwnedError("snapshot", rev.Name, task, got.OwnerReferences)
			}
			return &got, nil
		}
		if apierrors.IsRequestEntityTooLargeError(err) {
			return nil, brokenFlow{fmt.Sprintf(
				"task %q definitions do not fit in one object", task.Name)}
		}
		return nil, err
	}
	return rev, nil
}

// snapshotHandlers resolves every handler the snapshot must carry: one per
// binding, plus the cleanup handler when it exists. A binding naming a
// handler that does not exist is a definition fault, so it fails the task
// rather than being retried; a missing cleanup handler is skipped instead,
// since its absence is what the cleanup run is owed to record.
func (r *TaskReconciler) snapshotHandlers(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
) (map[string]*flowv1alpha1.TaskHandler, error) {
	names := make([]string, 0, len(flow.Spec.Bindings)+1)
	seen := map[string]bool{}
	for _, binding := range flow.Spec.Bindings {
		if !seen[binding.Handler] {
			seen[binding.Handler] = true
			names = append(names, binding.Handler)
		}
	}
	slices.Sort(names)
	var finally string
	if flow.Spec.Finally != nil {
		finally = flow.Spec.Finally.Handler
	}
	handlers := make(map[string]*flowv1alpha1.TaskHandler, len(names)+1)
	for _, name := range names {
		handler, err := r.handlerForName(ctx, task, name)
		if err != nil {
			return nil, err
		}
		handlers[name] = handler
	}
	if finally != "" && !seen[finally] {
		// A missing cleanup handler is skipped, not failed: handlerForName
		// reports the absence as brokenFlow rather than NotFound, so the
		// skip keys off that. Any other error is transient and retried.
		handler, err := r.handlerForName(ctx, task, finally)
		if err == nil {
			handlers[finally] = handler
		} else {
			var broken brokenFlow
			if !errors.As(err, &broken) {
				return nil, err
			}
		}
	}
	return handlers, nil
}

// handlerForName fetches one handler the snapshot names. A missing one is a
// definition fault carrying which handler is gone, so the task fails the
// same way no matter which binding pointed at it.
func (r *TaskReconciler) handlerForName(
	ctx context.Context,
	task *flowv1alpha1.Task,
	name string,
) (*flowv1alpha1.TaskHandler, error) {
	var handler flowv1alpha1.TaskHandler
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: task.Namespace}, &handler); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, brokenFlow{fmt.Sprintf("handler %q does not exist", name)}
		}
		return nil, err
	}
	return &handler, nil
}

// mustSnapshotJSON renders the copy with handlers in name order, so the same
// definitions always encode to the same bytes.
func mustSnapshotJSON(flow *flowv1alpha1.TaskFlowSpec, handlers map[string]*flowv1alpha1.TaskHandler) []byte {
	names := make([]string, 0, len(handlers))
	for name := range handlers {
		names = append(names, name)
	}
	slices.Sort(names)
	specs := make(map[string]flowv1alpha1.TaskHandlerSpec, len(handlers))
	for _, name := range names {
		specs[name] = *handlers[name].Spec.DeepCopy()
	}
	raw, err := json.Marshal(snapshot{Flow: *flow.DeepCopy(), Handlers: specs})
	if err != nil {
		panic(err)
	}
	return raw
}

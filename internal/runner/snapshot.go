package runner

import (
	"crypto/sha256"
	"encoding/hex"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// SnapshotRevisionName is where one task's copy of its definitions lives. The
// UID is hashed in for the workspace claim's reason: a task deleted and
// recreated under the same name is a different task, and a bare name would
// hand the new one its predecessor's copy. Storing the name in status instead
// was rejected: it would give every later reconcile a pointer to trust, where
// deriving it recomputes the same name and checks the owner each time.
func SnapshotRevisionName(taskName string, uid types.UID) string {
	sum := sha256.Sum256([]byte(uid))
	suffix := "-snapshot-" + hex.EncodeToString(sum[:])[:taskHashLength]
	prefix := taskName
	if len(prefix)+len(suffix) > maxNameLength {
		prefix = taskName[:maxNameLength-len(suffix)]
	}
	return prefix + suffix
}

// BuildSnapshotRevision returns the ControllerRevision that holds one task's
// copy of its definitions. data is the encoded copy as the caller decided it;
// what is the framework's here is the name, the bookkeeping labels and the
// ownership, the same as the workspace claim's.
func BuildSnapshotRevision(task *flowv1alpha1.Task, data []byte) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SnapshotRevisionName(task.Name, task.UID),
			Namespace: task.Namespace,
			Labels:    objectLabels(task.UID, 0),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         flowv1alpha1.SchemeGroupVersion.String(),
				Kind:               kindTask,
				Name:               task.Name,
				UID:                task.UID,
				Controller:         ptr(true),
				BlockOwnerDeletion: ptr(false),
			}},
		},
		Data: runtime.RawExtension{Raw: data},
	}
}

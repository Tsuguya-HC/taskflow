package controller

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// snapshotRevisionName is the one piece of the copy the tests pin without a
// cluster: every reconcile derives it, and a collision there decides whether a
// recreated task starts on a stale copy.
var _ = Describe("deriving the snapshot revision name", func() {
	task := func(name string, uid types.UID) *flowv1alpha1.Task {
		return &flowv1alpha1.Task{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid},
		}
	}

	// The UID digest is what keeps a recreated task from taking the deleted
	// task's leftover revision for its own.
	It("names a different revision for a recreated task under the same name", func() {
		first := snapshotRevisionName(task("task", "uid-1"))
		second := snapshotRevisionName(task("task", "uid-2"))
		Expect(first).NotTo(Equal(second))
		Expect(first).To(ContainSubstring("task"))
		Expect(second).To(ContainSubstring("task"))
	})

	// A name the apiserver would refuse is a reconcile that never reaches the
	// copy, so the cut keeps the readable prefix and drops the rest.
	It("fits the longest task names into the object name limit", func() {
		got := snapshotRevisionName(task(strings.Repeat("t", 200), "uid-1"))
		Expect(got).To(HaveLen(63))
	})
})

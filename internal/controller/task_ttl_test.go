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

package controller

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
)

// The TTL is the one thing a stopped task still owes the cluster (§10). These
// specs drive a task to each kind of stop and check the date it gets, then
// move the clock and check that the controller acts on it.
var _ = Describe("expiring a finished task", func() {
	var fx *fixture
	var clock time.Time

	const (
		succeededTTL = time.Hour
		failedTTL    = 168 * time.Hour
	)

	BeforeEach(func() {
		fx = newFixture()
		clock = time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
		fx.reconciler.Now = func() time.Time { return clock }
	})

	withTTL := func(f *flowv1alpha1.TaskFlow) {
		f.Spec.TTL = &flowv1alpha1.TTLSpec{
			Succeeded: &metav1.Duration{Duration: succeededTTL},
			Failed:    &metav1.Duration{Duration: failedTTL},
		}
	}

	// complete makes the first run finish with the given container message,
	// the way task_finish_test does, and reconciles once to settle it.
	complete := func(message string) {
		fx.reconcile()
		fx.reconcile()
		job := fx.job(1)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      job.Name,
				Namespace: resourceNamespace,
				Labels:    map[string]string{batchv1.ControllerUidLabel: string(job.UID)},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: agentName, Image: agentImage}},
			},
		}
		Expect(k8sClient.Create(fx.ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, pod) })
		pod.Status.Phase = corev1.PodSucceeded
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  agentName,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: message}},
		}}
		Expect(k8sClient.Status().Update(fx.ctx, pod)).To(Succeed())

		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = append(job.Status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		Expect(k8sClient.Status().Update(fx.ctx, job)).To(Succeed())
		fx.reconcile()
	}

	It("dates a task that stopped where the flow said with ttl.succeeded", func() {
		fx.makeFlow(withTTL)
		fx.makeHandler()
		fx.makeTask()
		complete("ok")

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(phaseReport))
		Expect(tk.Status.ExpiresAt).NotTo(BeNil())
		Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(succeededTTL)))
	})

	It("dates an escalated task with ttl.failed", func() {
		fx.makeFlow(withTTL)
		fx.makeHandler()
		fx.makeTask()
		complete("") // exit 0, said nothing

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseEscalated))
		Expect(tk.Status.ExpiresAt).NotTo(BeNil())
		Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
	})

	It("dates a task whose flow is broken with ttl.failed", func() {
		fx.makeFlow(withTTL, func(f *flowv1alpha1.TaskFlow) { f.Spec.Start = "nowhere" })
		fx.makeTask()
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(tk.Status.ExpiresAt).NotTo(BeNil())
		Expect(tk.Status.ExpiresAt.Time).To(BeTemporally("==", clock.Add(failedTTL)))
	})

	It("keeps a task whose flow does not exist, having no ttl to read", func() {
		fx.makeTask()
		fx.reconcile()

		tk := fx.get()
		Expect(tk.Status.Phase).To(Equal(flowv1alpha1.PhaseFailed))
		Expect(tk.Status.ExpiresAt).To(BeNil())
	})

	It("uses the CRD's defaults when the flow says nothing about ttl", func() {
		f := fx.makeFlow()
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: f.Name, Namespace: f.Namespace}, f)).To(Succeed())
		Expect(f.Spec.TTL).NotTo(BeNil(), "the apiserver fills in ttl from the CRD default")
		Expect(f.Spec.TTL.Succeeded.Duration).To(Equal(time.Hour))
		Expect(f.Spec.TTL.Failed.Duration).To(Equal(168 * time.Hour))
	})

	It("waits until the date and then deletes the task", func() {
		fx.makeFlow(withTTL)
		fx.makeHandler()
		fx.makeTask()
		complete("ok")

		// Well before: nothing but a wake-up call for later.
		clock = clock.Add(succeededTTL / 2)
		res := fx.reconcile()
		Expect(res.RequeueAfter).To(Equal(succeededTTL / 2))
		Expect(fx.get().Status.Phase).To(Equal(phaseReport), "untouched")

		// After: gone, and the flow does not need to exist for that.
		Expect(k8sClient.Delete(fx.ctx, &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
		})).To(Succeed())
		clock = clock.Add(succeededTTL / 2)
		res = fx.reconcile()
		Expect(res.RequeueAfter).To(BeZero())

		var tk flowv1alpha1.Task
		err := k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, &tk)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected the task to be deleted, got %v", err)
	})

	It("does not delete a task by the same name created after the one that expired", func() {
		fx.makeFlow(withTTL)
		fx.makeHandler()
		fx.makeTask()
		complete("ok")
		expired := fx.get()

		// Stale reconciler view: the object it read has passed its date, but
		// the apiserver's object under that name is now a different one.
		Expect(k8sClient.Delete(fx.ctx, expired)).To(Succeed())
		fresh := fx.makeTask()
		Expect(fresh.UID).NotTo(Equal(expired.UID))

		clock = clock.Add(2 * succeededTTL)
		err := fx.reconciler.Delete(fx.ctx, expired, client.Preconditions{UID: &expired.UID})
		Expect(apierrors.IsConflict(err) || apierrors.IsNotFound(err)).To(BeTrue(),
			"a UID precondition must refuse the newer object, got %v", err)
		Expect(fx.get().UID).To(Equal(fresh.UID))
	})
})

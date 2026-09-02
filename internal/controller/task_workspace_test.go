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
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

var _ = Describe("a flow with a workspace", func() {
	var fx *fixture
	BeforeEach(func() { fx = newFixture() })

	withWorkspace := func(f *flowv1alpha1.TaskFlow) {
		f.Spec.Workspace = &flowv1alpha1.FlowWorkspace{}
	}
	// onFlowWorkspace moves the handler onto the reserved volume, the way a
	// handler joins the claim its task's flow brings.
	onFlowWorkspace := func(h *flowv1alpha1.TaskHandler) {
		h.Spec.Workspace.Volume = contract.WorkspaceVolume
		spec := &h.Spec.JobTemplate.Template.Spec
		spec.Volumes = nil
		spec.Containers[0].VolumeMounts[0].Name = contract.WorkspaceVolume
	}
	pvcKey := func(tk *flowv1alpha1.Task) types.NamespacedName {
		return types.NamespacedName{Name: runner.WorkspacePVCName(tk.Name, tk.UID), Namespace: resourceNamespace}
	}

	// Claims made here are never cleaned up: envtest runs no garbage
	// collector to honour their ownerReferences, and no protection
	// controller to clear the pvc-protection finalizer a delete would leave
	// them terminating under. Their names carry each task's UID, so one
	// spec's leftover cannot be mistaken for another's claim.

	It("defaults the claim template at admission, where it is visible", func() {
		flow := fx.makeFlow(withWorkspace)

		vct := flow.Spec.Workspace.VolumeClaimTemplate
		Expect(vct).NotTo(BeNil(),
			"the default is written into the stored object, not resolved in some controller's memory")
		Expect(vct.AccessModes).To(ConsistOf(corev1.ReadWriteMany))
		Expect(vct.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("1Gi")))
	})

	It("backs the run with the task's own claim", func() {
		fx.makeFlow(withWorkspace)
		fx.makeHandler(onFlowWorkspace)
		tk := fx.makeTask()

		fx.reconcile() // settles the starting phase
		fx.reconcile() // creates the claim and the Job

		var pvc corev1.PersistentVolumeClaim
		Expect(k8sClient.Get(fx.ctx, pvcKey(tk), &pvc)).To(Succeed())
		Expect(metav1.IsControlledBy(&pvc, tk)).To(BeTrue())
		Expect(pvc.Labels).To(HaveKeyWithValue(runner.LabelTaskUID, string(tk.UID)))
		Expect(pvc.Spec.AccessModes).To(ConsistOf(corev1.ReadWriteMany),
			"what gets made is what admission defaulted onto the flow")

		podSpec := fx.job(1).Spec.Template.Spec
		claimed := ""
		for _, v := range podSpec.Volumes {
			if v.Name == contract.WorkspaceVolume && v.PersistentVolumeClaim != nil {
				claimed = v.PersistentVolumeClaim.ClaimName
			}
		}
		Expect(claimed).To(Equal(pvc.Name), "the reserved volume mounts this task's claim and no other")
		prepare, publish := podSpec.InitContainers[0], podSpec.InitContainers[1]
		Expect(prepare.VolumeMounts[0].SubPath).To(Equal("work"),
			"prepare works one level above its run, where it can make this run's directory and sweep abandoned ones")
		Expect(podSpec.Containers[0].VolumeMounts[0].SubPath).To(Equal("work/1"),
			"the handler's own writable mount is pinned to the run's number in work/ too, not left to the handler to resolve")

		// publish moves this run from work/ to results/ once it seals, so its
		// own mount has to see the claim's root, writably, rather than be
		// pinned to either shelf.
		Expect(publish.VolumeMounts[0].SubPath).To(BeEmpty(),
			"a flow-workspace publish must mount the claim's root to see both shelves")
		Expect(publish.VolumeMounts[0].ReadOnly).To(BeFalse(),
			"publish must be able to write, to move the run onto the results/ shelf")
	})

	It("refuses a foreign claim sitting under the task's name", func() {
		fx.makeFlow(withWorkspace)
		fx.makeHandler(onFlowWorkspace)
		tk := fx.makeTask()
		fx.reconcile()

		squatter := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: pvcKey(tk).Name, Namespace: resourceNamespace},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Mi")},
				},
			},
		}
		Expect(k8sClient.Create(fx.ctx, squatter)).To(Succeed())

		_, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})
		Expect(err).To(HaveOccurred(),
			"whatever sits under the name has to prove it is this task's before being mounted")
	})

	It("adopts its own claim on a call that finds it already made", func() {
		fx.makeFlow(withWorkspace)
		fx.makeHandler(onFlowWorkspace)
		tk := fx.makeTask()
		fx.reconcile() // settles the starting phase

		var flow flowv1alpha1.TaskFlow
		Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, &flow)).To(Succeed())

		name, err := fx.reconciler.ensureWorkspacePVC(fx.ctx, tk, &flow)
		Expect(err).NotTo(HaveOccurred())

		// A controller restart between the claim's creation and the Job's
		// would re-enter here with the claim already made; the second call
		// has to find and reuse it rather than trying to create it again.
		again, err := fx.reconciler.ensureWorkspacePVC(fx.ctx, tk, &flow)
		Expect(err).NotTo(HaveOccurred())
		Expect(again).To(Equal(name), "the second call adopts the claim the first one made")
	})

	It("carries the previous run's sweep list onto the next run's prepare", func() {
		// A rework, not an infrastructure retry: only a run that reached a
		// verdict moves the number on, so a self-loop is what puts a run 1
		// behind run 2 for the sweep list to name.
		fx.makeFlow(withWorkspace, func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings[phaseInvestigate] = flowv1alpha1.PhaseBinding{
				Handler: fx.name,
				Next:    map[flowv1alpha1.Phase]string{phaseReport: "ok", phaseInvestigate: "more"},
			}
		})
		fx.makeHandler(onFlowWorkspace)
		fx.makeTask()

		fx.reconcile() // settles the starting phase
		fx.reconcile() // creates the claim and the first Job
		job := fx.job(1)

		// A pod whose publish container named "more", which is the self-loop
		// edge — the same shape task_finish_test.go's rework spec drives,
		// spelled out here because its helpers are local to that file.
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      job.Name,
				Namespace: resourceNamespace,
				Labels:    map[string]string{batchv1.ControllerUidLabel: string(job.UID)},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: "agent", Image: "example.invalid/agent:v0"}},
			},
		}
		Expect(k8sClient.Create(fx.ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, pod) })
		pod.Status.Phase = corev1.PodSucceeded
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  "agent",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: "more"}},
		}}
		Expect(k8sClient.Status().Update(fx.ctx, pod)).To(Succeed())

		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = append(job.Status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		Expect(k8sClient.Status().Update(fx.ctx, job)).To(Succeed())

		fx.reconcile() // the rework lands on run 2
		Expect(fx.get().Status.RunID).To(BeEquivalentTo(2))

		fx.reconcile() // creates the second Job
		second := fx.job(2)
		prepare := second.Spec.Template.Spec.InitContainers[0]
		Expect(prepare.Args).To(Equal([]string{
			contract.SubcommandPrepare, "--" + contract.FlagOut, "/workspace/2/out",
			"--" + contract.FlagRunDir, "/workspace/2",
			"--" + contract.FlagSweep, "1",
		}), "run 1 sealed, but the sweep list names every run before this one either way")
	})

	// A retry is not a new run, so it comes back to the same directory rather
	// than being handed the last attempt's on a sweep list. Clearing it is
	// MakeRun's, which is what makes a directory some zombie still holds open
	// stop the retry instead of it starting work beside a live writer.
	It("puts an infrastructure retry back on the same run directory", func() {
		fx.makeFlow(withWorkspace)
		fx.makeHandler(onFlowWorkspace, func(h *flowv1alpha1.TaskHandler) { h.Spec.MaxInfraRetries = 1 })
		fx.makeTask()

		fx.reconcile() // settles the starting phase
		fx.reconcile() // creates the claim and the first Job
		job := fx.job(1)

		// The pod exists, but no container in it ever terminated — never
		// pulled, the same infrastructure shape task_finish_test.go's own
		// infra-retry spec uses.
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      job.Name,
				Namespace: resourceNamespace,
				Labels:    map[string]string{batchv1.ControllerUidLabel: string(job.UID)},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: "agent", Image: "example.invalid/agent:v0"}},
			},
		}
		Expect(k8sClient.Create(fx.ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, pod) })

		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.Conditions = append(job.Status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded},
			batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded})
		Expect(k8sClient.Status().Update(fx.ctx, job)).To(Succeed())

		fx.reconcile() // records the infra retry
		Expect(fx.get().Status.RunID).To(BeEquivalentTo(1), "nothing was decided, so no run was spent")

		fx.reconcile() // creates the retry's Job
		second := fx.jobAttempt(1, 1)
		Expect(second.Name).NotTo(Equal(job.Name), "the failed attempt's Job is still there to collide with")
		prepare := second.Spec.Template.Spec.InitContainers[0]
		Expect(prepare.Args).To(Equal([]string{
			contract.SubcommandPrepare, "--" + contract.FlagOut, "/workspace/1/out",
			"--" + contract.FlagRunDir, "/workspace/1",
		}), "the retry returns to run 1's own directory, with nothing before it to sweep")
	})

	It("refuses a stored flow whose workspace has no volumeClaimTemplate", func() {
		// The CRD schema defaults this at admission; nil here means the
		// object predates or otherwise escaped that default. Built by hand
		// rather than through the API, since going through it would let the
		// schema default fill this in.
		flow := &flowv1alpha1.TaskFlow{
			ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
			Spec:       flowv1alpha1.TaskFlowSpec{Workspace: &flowv1alpha1.FlowWorkspace{}},
		}
		tk := &flowv1alpha1.Task{ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace}}

		_, err := fx.reconciler.ensureWorkspacePVC(fx.ctx, tk, flow)

		var broken brokenFlow
		Expect(errors.As(err, &broken)).To(BeTrue(),
			"a workspace with no volumeClaimTemplate is a definition fault, not a transient one")
	})

	It("fails the task when the cluster refuses the claim template", func() {
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Workspace = &flowv1alpha1.FlowWorkspace{
				// Written at all, taken as written: no accessModes, which
				// the apiserver rejects when the claim is made — a
				// definition problem, not a transient one.
				VolumeClaimTemplate: &corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					},
				},
			}
		})
		fx.makeHandler(onFlowWorkspace)
		fx.makeTask()

		fx.reconcile() // settles the starting phase
		fx.reconcile() // tries the claim, finds the definition broken

		Expect(fx.get().Status.Phase).To(Equal(flowv1alpha1.PhaseFailed),
			"a template the cluster rejects is the flow's fault, and retrying it would meet the same rejection forever")
	})
})

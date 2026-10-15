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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

// The wire values of the condition are spelled out, as conditionPinned's are:
// a rename in the package under test should fail these specs.
const (
	conditionArchived = "Archived"
	reasonUploading   = "Uploading"
	reasonUploaded    = "Uploaded"
	reasonWithheld    = "Withheld"
	reasonUploadFail  = "UploadFailed"
)

var _ = Describe("archiving an ended task before it is deleted", func() {
	var fx *fixture
	var clock time.Time
	var expiresAt time.Time

	const (
		secretName = "archive-creds"
		timeout    = 15 * time.Minute
	)

	BeforeEach(func() {
		fx = newFixture()
		clock = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
		expiresAt = clock.Add(time.Hour)
		fx.reconciler.Now = func() time.Time { return clock }
		fx.reconciler.Archive = ArchiveConfig{
			Endpoint: "s3.example:9000", Bucket: "records", Prefix: "taskflow/", Region: "us-east-1",
			CredentialsSecret: secretName, Timeout: timeout,
		}
	})

	// ended makes a task that has stopped with its date already set, which is
	// where the archive step takes over. Ready is stamped so a spec can check
	// the archive leaves it alone.
	ended := func(mut ...func(*flowv1alpha1.TaskFlow)) *flowv1alpha1.Task {
		fx.makeFlow(mut...)
		tk := fx.makeTask()
		tk.Status.Phase = flowv1alpha1.PhaseTaskFailed
		tk.Status.ExpiresAt = &metav1.Time{Time: expiresAt}
		meta.SetStatusCondition(&tk.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: "Stopped", Message: "stopped for the spec",
		})
		Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
		return fx.get()
	}

	archiveJob := func() *batchv1.Job {
		var jobs batchv1.JobList
		Expect(k8sClient.List(fx.ctx, &jobs, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID), contract.LabelRole: contract.RoleArchive})).To(Succeed())
		Expect(jobs.Items).To(HaveLen(1))
		return &jobs.Items[0]
	}
	archiveJobs := func() int {
		var jobs batchv1.JobList
		Expect(k8sClient.List(fx.ctx, &jobs, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID)})).To(Succeed())
		return len(jobs.Items)
	}
	archiveMaps := func() []corev1.ConfigMap {
		var cms corev1.ConfigMapList
		Expect(k8sClient.List(fx.ctx, &cms, client.InNamespace(resourceNamespace),
			client.MatchingLabels{runner.LabelTaskUID: string(fx.taskUID), contract.LabelRole: contract.RoleArchive})).To(Succeed())
		return cms.Items
	}
	archived := func() *metav1.Condition {
		return meta.FindStatusCondition(fx.get().Status.Conditions, conditionArchived)
	}
	gone := func() bool {
		var tk flowv1alpha1.Task
		err := k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, &tk)
		return apierrors.IsNotFound(err)
	}

	// settle plays the cluster: a pod that terminated with message, and the
	// Job complete or failed.
	settle := func(job *batchv1.Job, ok bool, message string) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: job.Name + "-pod", Namespace: resourceNamespace,
				Labels: map[string]string{batchv1.ControllerUidLabel: string(job.UID)},
			},
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyNever,
				Containers:    []corev1.Container{{Name: job.Spec.Template.Spec.Containers[0].Name, Image: sidecarImage}},
			},
		}
		Expect(k8sClient.Create(fx.ctx, pod)).To(Succeed())
		DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, pod) })
		exit := int32(0)
		pod.Status.Phase = corev1.PodSucceeded
		if !ok {
			exit = 1
			pod.Status.Phase = corev1.PodFailed
		}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  job.Spec.Template.Spec.Containers[0].Name,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exit, Message: message}},
		}}
		Expect(k8sClient.Status().Update(fx.ctx, pod)).To(Succeed())

		now := metav1.Now()
		job.Status.StartTime = &now
		if ok {
			job.Status.CompletionTime = &now
			job.Status.Conditions = append(job.Status.Conditions,
				batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
				batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		} else {
			job.Status.Conditions = append(job.Status.Conditions,
				batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded},
				batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded})
		}
		Expect(k8sClient.Status().Update(fx.ctx, job)).To(Succeed())
	}

	Describe("the objects it makes", func() {
		It("makes a ConfigMap of the task and a Job that sends it, both the task's own", func() {
			tk := ended()
			fx.reconcile()

			maps := archiveMaps()
			Expect(maps).To(HaveLen(1))
			cm := maps[0]
			Expect(metav1.IsControlledBy(&cm, tk)).To(BeTrue())
			Expect(cm.Labels).To(HaveKeyWithValue(contract.LabelManagedBy, contract.ManagedBy))
			var sb strings.Builder
			for _, v := range cm.Data {
				sb.WriteString(v)
			}
			carried := sb.String()
			Expect(carried).To(ContainSubstring(`"name":"`+fx.name+`"`), "the record is the task's JSON")

			job := archiveJob()
			Expect(metav1.IsControlledBy(job, tk)).To(BeTrue())
			Expect(job.Labels).To(HaveKeyWithValue(runner.LabelTaskUID, string(tk.UID)))
			Expect(job.Labels).To(HaveKeyWithValue(contract.LabelRole, contract.RoleArchive))
			Expect(job.Spec.BackoffLimit).To(Equal(ptr.To(int32(2))))
			Expect(job.Spec.ActiveDeadlineSeconds).To(Equal(ptr.To(int64(timeout.Seconds()))))

			pod := job.Spec.Template.Spec
			Expect(pod.ServiceAccountName).To(BeElementOf("", "default"))
			Expect(pod.AutomountServiceAccountToken).To(Equal(ptr.To(false)))
			Expect(pod.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
			Expect(pod.SecurityContext).NotTo(BeNil())
			Expect(pod.SecurityContext.RunAsNonRoot).To(Equal(ptr.To(true)))
			Expect(pod.SecurityContext.SeccompProfile).NotTo(BeNil())
			Expect(pod.SecurityContext.SeccompProfile.Type).To(Equal(corev1.SeccompProfileTypeRuntimeDefault))

			Expect(pod.Containers).To(HaveLen(1))
			c := pod.Containers[0]
			Expect(c.Image).To(Equal(sidecarImage))
			Expect(c.SecurityContext).NotTo(BeNil())
			Expect(c.SecurityContext.AllowPrivilegeEscalation).To(Equal(ptr.To(false)))
			Expect(c.SecurityContext.Capabilities).NotTo(BeNil())
			Expect(c.SecurityContext.Capabilities.Drop).To(ConsistOf(corev1.Capability("ALL")))
			Expect(c.EnvFrom).To(HaveLen(1))
			Expect(c.EnvFrom[0].SecretRef).NotTo(BeNil())
			Expect(c.EnvFrom[0].SecretRef.Name).To(Equal(secretName))
			args := strings.Join(append(append([]string{}, c.Command...), c.Args...), " ")
			Expect(args).To(ContainSubstring("archive"))
			Expect(args).To(ContainSubstring("s3.example:9000"))
			Expect(args).To(ContainSubstring("records"))

			var fromMap bool
			for _, v := range pod.Volumes {
				if v.ConfigMap != nil && v.ConfigMap.Name == cm.Name {
					fromMap = true
				}
			}
			Expect(fromMap).To(BeTrue(), "the record reaches the pod through the ConfigMap")
			for _, m := range c.VolumeMounts {
				Expect(m.SubPath).NotTo(Equal("results"), "a flow with no workspace has no shelf to mount")
			}
		})

		It("mounts the flow's workspace, read-only, at the results shelf", func() {
			tk := ended(func(f *flowv1alpha1.TaskFlow) { f.Spec.Workspace = &flowv1alpha1.FlowWorkspace{} })
			var flow flowv1alpha1.TaskFlow
			Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, &flow)).To(Succeed())
			pvc := runner.BuildWorkspacePVC(tk, flow.Spec.Workspace.VolumeClaimTemplate)
			Expect(k8sClient.Create(fx.ctx, pvc)).To(Succeed())
			fx.reconcile()

			pod := archiveJob().Spec.Template.Spec
			var volume string
			for _, v := range pod.Volumes {
				if v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == runner.WorkspacePVCName(tk.Name, tk.UID) {
					volume = v.Name
				}
			}
			Expect(volume).NotTo(BeEmpty(), "the task's own claim is a volume of the Job")
			var mounted *corev1.VolumeMount
			for i, m := range pod.Containers[0].VolumeMounts {
				if m.Name == volume {
					mounted = &pod.Containers[0].VolumeMounts[i]
				}
			}
			Expect(mounted).NotTo(BeNil())
			Expect(mounted.SubPath).To(Equal("results"))
			Expect(mounted.ReadOnly).To(BeTrue())
		})

		It("sets Archived to Unknown/Uploading once it has made them", func() {
			ended()
			fx.reconcile()

			c := archived()
			Expect(c).NotTo(BeNil())
			Expect(c.Status).To(Equal(metav1.ConditionUnknown))
			Expect(c.Reason).To(Equal(reasonUploading))
		})
	})

	Describe("holding the deletion", func() {
		It("keeps a task past its date while the upload is unsettled, and deletes it once it settles", func() {
			ended()
			fx.reconcile()

			clock = expiresAt.Add(time.Minute)
			res := fx.reconcile()
			Expect(gone()).To(BeFalse(), "an upload in flight holds the task")
			Expect(res.RequeueAfter).To(BeNumerically(">", 0), "and the controller comes back to look")
			Expect(archived().Status).To(Equal(metav1.ConditionUnknown))

			settle(archiveJob(), true, `{"uploaded":3,"withheld":0}`)
			fx.reconcile()
			c := archived()
			if c != nil {
				Expect(c.Status).To(Equal(metav1.ConditionTrue))
				Expect(c.Reason).To(Equal(reasonUploaded))
			}
			fx.reconcile()
			Expect(gone()).To(BeTrue(), "settled and past its date")
		})

		It("does not delete a settled task before its date", func() {
			ended()
			fx.reconcile()
			settle(archiveJob(), true, `{"uploaded":3,"withheld":0}`)
			fx.reconcile()

			Expect(archived()).NotTo(BeNil())
			Expect(archived().Status).To(Equal(metav1.ConditionTrue))
			Expect(gone()).To(BeFalse())
		})

		It("says Withheld, with how many, when some files were not sent", func() {
			ended()
			fx.reconcile()
			settle(archiveJob(), true, `{"uploaded":2,"withheld":4}`)
			fx.reconcile()

			c := archived()
			Expect(c).NotTo(BeNil())
			Expect(c.Status).To(Equal(metav1.ConditionTrue))
			Expect(c.Reason).To(Equal(reasonWithheld))
			Expect(c.Message).To(ContainSubstring("4"))
		})
	})

	Describe("a failed upload", func() {
		It("is recorded and announced, and changes neither Ready, the date nor the phase", func() {
			before := ended()
			fx.reconcile()
			settle(archiveJob(), false, "")
			fx.reconcile()

			c := archived()
			Expect(c).NotTo(BeNil())
			Expect(c.Status).To(Equal(metav1.ConditionFalse))
			Expect(c.Reason).To(Equal(reasonUploadFail))
			Expect(fx.announced()).To(ContainElement(And(HavePrefix("Warning"), ContainSubstring(reasonUploadFail))))

			after := fx.get()
			Expect(after.Status.Phase).To(Equal(before.Status.Phase))
			Expect(after.Status.ExpiresAt.Time).To(BeTemporally("==", before.Status.ExpiresAt.Time))
			Expect(meta.FindStatusCondition(after.Status.Conditions, "Ready")).To(Equal(
				meta.FindStatusCondition(before.Status.Conditions, "Ready")))
		})

		It("lets the task go at its date, the failure having settled it", func() {
			ended()
			fx.reconcile()
			settle(archiveJob(), false, "")
			fx.reconcile()

			clock = expiresAt.Add(time.Minute)
			fx.reconcile()
			Expect(gone()).To(BeTrue())
		})
	})

	Describe("a restart", func() {
		It("adopts the Job already there rather than making a second", func() {
			ended()
			fx.reconcile()
			first := archiveJob()

			restarted := *fx.reconciler
			fx.reconciler = &restarted
			fx.reconcile()

			Expect(archiveJob().UID).To(Equal(first.UID))
			Expect(archiveJobs()).To(Equal(1))
			Expect(archiveMaps()).To(HaveLen(1))
		})
	})

	Describe("a name someone else took", func() {
		It("refuses a Job of that name that another owner holds", func() {
			ended()
			stray := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: archiveJobName(fx.name), Namespace: resourceNamespace,
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: flowv1alpha1.SchemeGroupVersion.String(), Kind: archiveTaskKind,
						Name: "someone-elses-task", UID: types.UID("not-" + string(fx.taskUID)),
						Controller: ptr.To(true),
					}},
				},
				Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: agentName, Image: agentImage}},
				}}},
			}
			Expect(k8sClient.Create(fx.ctx, stray)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(fx.ctx, stray, client.PropagationPolicy(metav1.DeletePropagationBackground))
			})

			_, err := fx.reconciler.Reconcile(fx.ctx, reqFor(fx.name))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(stray.Name))
			Expect(err.Error()).To(ContainSubstring("someone-elses-task"))
			Expect(archived()).To(BeNil(), "someone else's Job is not this task's upload")
		})
	})

	Describe("without configuration", func() {
		It("makes nothing, adds no condition, and deletes at the date as before", func() {
			fx.reconciler.Archive = ArchiveConfig{}
			ended()
			fx.reconcile()

			Expect(archiveJobs()).To(BeZero())
			Expect(archiveMaps()).To(BeEmpty())
			Expect(archived()).To(BeNil())

			clock = expiresAt.Add(time.Minute)
			fx.reconcile()
			Expect(gone()).To(BeTrue())
		})
	})
})

func reqFor(name string) reconcile.Request {
	return reconcile.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: resourceNamespace}}
}

const archiveTaskKind = "Task"

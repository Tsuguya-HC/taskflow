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
	"context"
	"errors"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
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
						Name: foreignOwnerName, UID: types.UID("not-" + string(fx.taskUID)),
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
			Expect(err.Error()).To(ContainSubstring(foreignOwnerName))
			Expect(archived()).To(BeNil(), "someone else's Job is not this task's upload")
		})
	})

	// argValue is the value of a --name=value argument of the archive Job.
	argValue := func(job *batchv1.Job, name string) string {
		c := job.Spec.Template.Spec.Containers[0]
		for _, a := range append(append([]string{}, c.Command...), c.Args...) {
			if v, ok := strings.CutPrefix(a, name+"="); ok {
				return v
			}
		}
		return ""
	}

	// completeWithoutPod marks the Job complete with no pod behind it, the
	// shape of a Job whose pod the controller cannot see (yet, or any more).
	completeWithoutPod := func(job *batchv1.Job) {
		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Conditions = append(job.Status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
		Expect(k8sClient.Status().Update(fx.ctx, job)).To(Succeed())
	}

	// strayOwner is an owner that is not the task, as when a name was taken
	// by something else.
	strayOwner := func() []metav1.OwnerReference {
		return []metav1.OwnerReference{{
			APIVersion: flowv1alpha1.SchemeGroupVersion.String(), Kind: archiveTaskKind,
			Name: foreignOwnerName, UID: types.UID("not-" + string(fx.taskUID)),
			Controller: ptr.To(true),
		}}
	}

	Describe("a long task name", func() {
		It("still gets its Job and ConfigMap, the names fitting what a label can hold", func() {
			fx.name = strings.Repeat("a", 58-len(fx.name)) + fx.name
			Expect(fx.name).To(HaveLen(58))
			ended()
			fx.reconcile()

			Expect(len(archiveJob().Name)).To(BeNumerically("<=", 63))
			Expect(archiveMaps()).To(HaveLen(1))
			Expect(archived()).NotTo(BeNil())
		})
	})

	Describe("the key the record is sent under", func() {
		// endedUnder makes an ended task whose name is not its flow's.
		endedUnder := func(flow string) {
			fx.makeFlow()
			fx.name = fx.name + "-run2"
			tk := &flowv1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
				Spec:       flowv1alpha1.TaskSpec{Flow: flow},
			}
			Expect(k8sClient.Create(fx.ctx, tk)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, tk) })
			fx.taskUID = tk.UID
			tk.Status.Phase = flowv1alpha1.PhaseTaskFailed
			tk.Status.ExpiresAt = &metav1.Time{Time: expiresAt}
			Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
		}

		It("is namespace, flow and UID under the prefix, the flow's name not the task's", func() {
			flow := fx.name
			endedUnder(flow)
			fx.reconcile()

			Expect(argValue(archiveJob(), "--manifest-key-prefix")).To(Equal(
				"taskflow/" + resourceNamespace + "/" + flow + "/" + string(fx.taskUID)))
		})

		It("puts _unresolved where the flow would be when the task's flow is not there", func() {
			endedUnder("no-such-flow")
			fx.reconcile()

			Expect(argValue(archiveJob(), "--manifest-key-prefix")).To(Equal(
				"taskflow/" + resourceNamespace + "/_unresolved/" + string(fx.taskUID)))
		})
	})

	Describe("a flow that cannot be read for a reason other than being absent", func() {
		It("fails instead of filing the record under _unresolved", func() {
			tk := ended()
			boom := errors.New("the apiserver said no")
			watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			fx.reconciler.APIReader = interceptor.NewClient(watchClient, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*appsv1.ControllerRevision); ok {
						return boom
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})

			_, err = fx.reconciler.ensureArchiveJob(fx.ctx, tk)

			Expect(err).To(MatchError(ContainSubstring(boom.Error())))
			Expect(archiveJobs()).To(BeZero(), "no record is sent under a key that was guessed")
		})
	})

	Describe("a Job that finished with nothing to read", func() {
		It("is not recorded as uploaded when its pod cannot be found", func() {
			ended()
			fx.reconcile()
			completeWithoutPod(archiveJob())
			fx.reconcile()

			if c := archived(); c != nil {
				Expect(c.Status).NotTo(Equal(metav1.ConditionTrue), "nothing was seen to be sent")
			}
		})

		// The pod is gone for good (garbage collected), not late: the Task
		// must not be held past its date forever.
		It("is given up on as UploadFailed once the pod has been missing long enough", func() {
			ended()
			fx.reconcile()
			completeWithoutPod(archiveJob())
			fx.reconcile()
			Expect(archived()).NotTo(BeNil())
			Expect(archived().Status).To(Equal(metav1.ConditionUnknown), "a pod may still be on its way")

			clock = time.Now().AddDate(10, 0, 0)
			fx.reconcile()

			c := archived()
			Expect(c).NotTo(BeNil())
			Expect(c.Status).To(Equal(metav1.ConditionFalse))
			Expect(c.Reason).To(Equal("UploadFailed"))
		})

		It("is not recorded as uploaded when its termination message is not a result", func() {
			ended()
			fx.reconcile()
			settle(archiveJob(), true, "not json")
			fx.reconcile()

			if c := archived(); c != nil {
				Expect(c.Status).NotTo(Equal(metav1.ConditionTrue), "nothing was seen to be sent")
			}
		})
	})

	Describe("a restart with the archive switched off", func() {
		// The process died between making the Job and writing the condition.
		It("does not delete a task whose Job was made but whose condition was never written", func() {
			tk := ended()
			_, err := fx.reconciler.ensureArchiveJob(fx.ctx, tk)
			Expect(err).NotTo(HaveOccurred())
			Expect(archived()).To(BeNil(), "the crash happened before the condition was written")

			clock = expiresAt.Add(time.Minute)
			fx.reconciler.Archive = ArchiveConfig{}
			fx.reconcile()

			Expect(gone()).To(BeFalse(), "an upload Job exists for this task; it must not be deleted out from under it")
		})

		It("does not delete a task whose upload was still in flight", func() {
			ended()
			fx.reconcile()
			Expect(archived()).NotTo(BeNil())
			Expect(archived().Status).To(Equal(metav1.ConditionUnknown))

			clock = expiresAt.Add(time.Minute)
			fx.reconciler.Archive = ArchiveConfig{}
			fx.reconcile()

			Expect(gone()).To(BeFalse(), "an unsettled upload holds the task whatever the flags say now")
			Expect(archived().Status).To(Equal(metav1.ConditionUnknown))
		})

		// Catches archiveHeld holding on to an Unknown condition whatever the
		// Job and its Pod look like: with the switch off nothing else gives up
		// on a Pod that is gone, and the task would outlive its date forever.
		It("gives up on a task whose pod is gone for good and deletes it", func() {
			ended()
			fx.reconcile()
			completeWithoutPod(archiveJob())
			fx.reconcile()
			Expect(archived()).NotTo(BeNil())
			Expect(archived().Status).To(Equal(metav1.ConditionUnknown))

			fx.reconciler.Archive = ArchiveConfig{}
			clock = time.Now().AddDate(10, 0, 0)
			fx.reconcile()
			fx.reconcile()

			Expect(gone()).To(BeTrue(), "the pod is gone for good; the upload is given up on and the task deleted")
		})
	})

	Describe("the archive switched off and the Job gone or never recorded", func() {
		It("gives up on a task whose pod is gone for good when the condition was never written", func() {
			tk := ended()
			_, err := fx.reconciler.ensureArchiveJob(fx.ctx, tk)
			Expect(err).NotTo(HaveOccurred())
			Expect(archived()).To(BeNil(), "the crash happened before the condition was written")
			completeWithoutPod(archiveJob())

			fx.reconciler.Archive = ArchiveConfig{}
			clock = time.Now().AddDate(10, 0, 0)
			fx.reconcile()
			fx.reconcile()
			fx.reconcile()

			Expect(gone()).To(BeTrue(), "the pod is gone for good; the task must not outlive its date forever")
		})

		It("does not hold a task whose unsettled Job has vanished", func() {
			ended()
			fx.reconcile()
			Expect(archived()).NotTo(BeNil())
			Expect(archived().Status).To(Equal(metav1.ConditionUnknown))
			Expect(k8sClient.Delete(fx.ctx, archiveJob(), client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
			Eventually(archiveJobs).Should(BeZero())

			fx.reconciler.Archive = ArchiveConfig{}
			clock = time.Now().AddDate(10, 0, 0)
			fx.reconcile()
			fx.reconcile()
			fx.reconcile()

			Expect(gone()).To(BeTrue(), "the Job is gone; nothing will ever settle this upload")
		})

		// #280: a task must not be deleted with Archived still Unknown; the
		// vanished Job's upload is settled as failed and announced first.
		It("settles the upload as failed before deleting a task whose unsettled Job has vanished", func() {
			ended()
			fx.reconcile()
			Expect(archived()).NotTo(BeNil())
			Expect(archived().Status).To(Equal(metav1.ConditionUnknown))
			Expect(k8sClient.Delete(fx.ctx, archiveJob(), client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
			Eventually(archiveJobs).Should(BeZero())

			fx.reconciler.Archive = ArchiveConfig{}
			clock = time.Now().AddDate(10, 0, 0)
			fx.reconcile()
			fx.reconcile()
			fx.reconcile()

			Expect(gone()).To(BeTrue())
			Expect(fx.announced()).To(ContainElement(And(HavePrefix("Warning"), ContainSubstring(reasonUploadFail))),
				"deletion must follow a decided outcome, announced")
		})

		// Catches the switched-off guard in driveArchive being dropped: a Job
		// would be made with no destination configured.
		It("makes no Job when the archive is off and none exists", func() {
			ended()
			fx.reconcile()
			Expect(k8sClient.Delete(fx.ctx, archiveJob(), client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
			Eventually(archiveJobs).Should(BeZero())

			fx.reconciler.Archive = ArchiveConfig{}
			_, err := fx.reconciler.Reconcile(fx.ctx, reqFor(fx.name))

			Expect(err).NotTo(HaveOccurred())
			Expect(archiveJobs()).To(BeZero(), "nothing is sent to nowhere")
		})

		// Catches the owner check being dropped from the guard: a stranger's
		// Job of the same name would make every pass fail on ownership.
		It("does not fail on a Job of that name that another owner holds", func() {
			ended()
			fx.reconcile()
			Expect(k8sClient.Delete(fx.ctx, archiveJob(), client.PropagationPolicy(metav1.DeletePropagationBackground))).To(Succeed())
			Eventually(archiveJobs).Should(BeZero())
			stray := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: archiveJobName(fx.name), Namespace: resourceNamespace, OwnerReferences: strayOwner(),
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

			fx.reconciler.Archive = ArchiveConfig{}
			_, err := fx.reconciler.Reconcile(fx.ctx, reqFor(fx.name))

			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("a restart with the archive switched off and the Job unreadable or not ours", func() {
		// Catches archiveHeld swallowing the Get error (the task would be
		// deleted with a send possibly in flight) or ignoring the owner check
		// (a stranger's Job of the same name would hold the task forever).
		It("returns the error instead of deleting when the Job cannot be read", func() {
			tk := ended()
			_, err := fx.reconciler.ensureArchiveJob(fx.ctx, tk)
			Expect(err).NotTo(HaveOccurred())

			boom := errors.New("the apiserver blinked")
			watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			fx.reconciler.Client = interceptor.NewClient(watchClient, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*batchv1.Job); ok {
						return boom
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})

			clock = expiresAt.Add(time.Minute)
			fx.reconciler.Archive = ArchiveConfig{}
			_, err = fx.reconciler.Reconcile(fx.ctx, reqFor(fx.name))

			Expect(err).To(MatchError(ContainSubstring(boom.Error())))
			Expect(gone()).To(BeFalse(), "a Job that could not be read may still be sending")
		})

		It("deletes at the date when the Job of that name belongs to another owner", func() {
			ended()
			stray := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: archiveJobName(fx.name), Namespace: resourceNamespace, OwnerReferences: strayOwner(),
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

			clock = expiresAt.Add(time.Minute)
			fx.reconciler.Archive = ArchiveConfig{}
			fx.reconcile()

			Expect(gone()).To(BeTrue(), "a Job this task does not own is not an upload of its record")
		})
	})

	Describe("looking again before the date", func() {
		It("comes back within the recheck interval while the upload is unsettled", func() {
			ended()
			fx.reconcile()
			res := fx.reconcile()

			Expect(archived().Status).To(Equal(metav1.ConditionUnknown))
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(res.RequeueAfter).To(BeNumerically("<=", 30*time.Second),
				"the date is an hour away but the upload may settle sooner")
		})
	})

	Describe("a ConfigMap or claim of that name that another owner holds", func() {
		It("refuses a ConfigMap of that name", func() {
			ended()
			stray := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
				Name: archiveJobName(fx.name), Namespace: resourceNamespace, OwnerReferences: strayOwner(),
			}}
			Expect(k8sClient.Create(fx.ctx, stray)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, stray) })

			_, err := fx.reconciler.Reconcile(fx.ctx, reqFor(fx.name))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(foreignOwnerName))
			Expect(archiveJobs()).To(BeZero(), "no Job is made on a record that is not this task's")
		})

		It("refuses a workspace claim of the task's claim name that another owner holds", func() {
			tk := ended(func(f *flowv1alpha1.TaskFlow) { f.Spec.Workspace = &flowv1alpha1.FlowWorkspace{} })
			var flow flowv1alpha1.TaskFlow
			Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, &flow)).To(Succeed())
			stray := runner.BuildWorkspacePVC(tk, flow.Spec.Workspace.VolumeClaimTemplate)
			stray.OwnerReferences = strayOwner()
			Expect(k8sClient.Create(fx.ctx, stray)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, stray) })

			_, err := fx.reconciler.Reconcile(fx.ctx, reqFor(fx.name))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(foreignOwnerName))
			Expect(archiveJobs()).To(BeZero(), "no Job mounts a claim that is not this task's")
		})
	})

	// The spec reads status.archive and the counter by their wire names, not
	// through the Go types: the specs must compile before the field and the
	// variable exist, and a rename of either should fail them.
	Describe("where the record went, and how it ended", func() {
		const unresolvedLabel = "<unresolved>"
		outcomes := []string{"Uploaded", "Withheld", "Failed"}

		// archiveStatus is status.archive as the API server stores it.
		archiveStatus := func() map[string]any {
			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(flowv1alpha1.GroupVersion.WithKind("Task"))
			Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, u)).To(Succeed())
			got, _, err := unstructured.NestedMap(u.Object, "status", "archive")
			Expect(err).NotTo(HaveOccurred())
			return got
		}
		// counted reads taskflow_archive_outcomes_total from the registry
		// rather than asking the vector for a child, which would create the
		// series the spec is asking about.
		counted := func(flow, outcome string) (float64, bool) {
			fams, err := ctrlmetrics.Registry.Gather()
			Expect(err).NotTo(HaveOccurred())
			for _, f := range fams {
				if f.GetName() != "taskflow_archive_outcomes_total" {
					continue
				}
				for _, m := range f.GetMetric() {
					labels := map[string]string{}
					for _, l := range m.GetLabel() {
						labels[l.GetName()] = l.GetValue()
					}
					if labels["flow"] == flow && labels["outcome"] == outcome {
						return m.GetCounter().GetValue(), true
					}
				}
			}
			return 0, false
		}

		// total is counted's value alone, for Expect, which refuses a
		// non-zero extra return.
		total := func(flow, outcome string) float64 {
			v, _ := counted(flow, outcome)
			return v
		}

		It("records the location and endpoint when the Job is made", func() {
			tk := ended()
			fx.reconcile()

			Expect(archiveStatus()).To(And(
				HaveKeyWithValue("location", "s3://records/taskflow/"+resourceNamespace+"/"+fx.name+"/"+string(tk.UID)+"/"),
				HaveKeyWithValue("endpoint", "s3.example:9000")))
		})

		It("files the location under _unresolved when the task's flow is not there", func() {
			fx.makeFlow()
			fx.name += "-run2"
			tk := &flowv1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
				Spec:       flowv1alpha1.TaskSpec{Flow: "no-such-flow"},
			}
			Expect(k8sClient.Create(fx.ctx, tk)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, tk) })
			fx.taskUID = tk.UID
			tk.Status.Phase = flowv1alpha1.PhaseTaskFailed
			tk.Status.ExpiresAt = &metav1.Time{Time: expiresAt}
			Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
			fx.reconcile()

			Expect(archiveStatus()).To(HaveKeyWithValue("location",
				"s3://records/taskflow/"+resourceNamespace+"/_unresolved/"+string(tk.UID)+"/"))
			for _, o := range outcomes {
				v, ok := counted(unresolvedLabel, o)
				Expect(ok).To(BeTrue(), "%s is primed under the unresolved label", o)
				Expect(v).To(BeNumerically(">=", 0))
			}
		})

		It("has all three series at zero before anything has ended", func() {
			ended()
			fx.reconcile()

			for _, o := range outcomes {
				v, ok := counted(fx.name, o)
				Expect(ok).To(BeTrue(), "%s exists once the Job is made", o)
				Expect(v).To(BeZero())
			}
		})

		DescribeTable("counts the ending once",
			func(ok bool, message, outcome string) {
				ended()
				fx.reconcile()
				before, _ := counted(fx.name, outcome)
				settle(archiveJob(), ok, message)
				fx.reconcile()

				Expect(total(fx.name, outcome)).To(Equal(before+1), "%s rose by one", outcome)
				for _, other := range outcomes {
					if other != outcome {
						Expect(total(fx.name, other)).To(BeZero(), "%s did not move", other)
					}
				}
				fx.reconcile()
				Expect(total(fx.name, outcome)).To(Equal(before+1), "a later pass does not count it again")
			},
			Entry("sent", true, `{"uploaded":3,"withheld":0}`, "Uploaded"),
			Entry("some withheld", true, `{"uploaded":2,"withheld":4}`, "Withheld"),
			Entry("the Job failed", false, "", "Failed"),
		)

		// The give-up after the read grace is its own branch: its outcome and
		// its Event are not covered by the Job-failed or bad-message paths.
		It("counts and announces a Job whose pod stayed unreadable past the grace", func() {
			ended()
			fx.reconcile()
			completeWithoutPod(archiveJob())
			fx.reconcile()
			before, _ := counted(fx.name, "Failed")

			clock = time.Now().AddDate(10, 0, 0)
			fx.reconcile()

			Expect(total(fx.name, "Failed")).To(Equal(before+1), "Failed rose by one")
			Expect(total(fx.name, "Uploaded")).To(BeZero())
			Expect(total(fx.name, "Withheld")).To(BeZero())
			Expect(fx.announced()).To(ContainElement(And(HavePrefix("Warning"), ContainSubstring(reasonUploadFail))))
		})

		// The counter is raised on the series the Job's creation primed. A
		// settle-time read error or a flow that appeared while the Job ran
		// must not move the count onto a series that was never primed.
		It("does not fold a settle-time read error into the unresolved series", func() {
			ended()
			fx.reconcile()
			for _, o := range outcomes {
				_, ok := counted(fx.name, o)
				Expect(ok).To(BeTrue(), "%s is primed under the flow", o)
			}
			beforeUnresolved := total(unresolvedLabel, "Uploaded")
			watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			fx.reconciler.APIReader = interceptor.NewClient(watchClient, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*appsv1.ControllerRevision); ok {
						return errors.New("the apiserver blinked at settle time")
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			settle(archiveJob(), true, `{"uploaded":3,"withheld":0}`)
			_, _ = fx.reconciler.Reconcile(fx.ctx, reqFor(fx.name))

			Expect(total(unresolvedLabel, "Uploaded")).To(Equal(beforeUnresolved), "the unresolved series did not move")
		})

		// A Job made before the label was recorded on it was still primed under
		// the flow's name; the count belongs to that series.
		It("counts a Job without the recorded label under the flow it was primed for", func() {
			ended()
			fx.reconcile()
			job := archiveJob()
			delete(job.Annotations, annotationFlowLabel)
			Expect(k8sClient.Update(fx.ctx, job)).To(Succeed())
			beforeUnresolved := total(unresolvedLabel, "Uploaded")
			before := total(fx.name, "Uploaded")
			settle(archiveJob(), true, `{"uploaded":3,"withheld":0}`)
			fx.reconcile()

			Expect(total(fx.name, "Uploaded")).To(Equal(before + 1))
			Expect(total(unresolvedLabel, "Uploaded")).To(Equal(beforeUnresolved))
		})

		// The Archived condition's write is not retried once made, so a read
		// that fails around it must not cost the settle its count.
		It("still counts a settle whose fallback label resolve failed", func() {
			ended()
			fx.reconcile()
			job := archiveJob()
			delete(job.Annotations, annotationFlowLabel)
			Expect(k8sClient.Update(fx.ctx, job)).To(Succeed())
			before := total(fx.name, "Uploaded")

			watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			fx.reconciler.APIReader = interceptor.NewClient(watchClient, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*appsv1.ControllerRevision); ok {
						return errors.New("the apiserver blinked at settle time")
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			settle(archiveJob(), true, `{"uploaded":3,"withheld":0}`)
			_, err = fx.reconciler.Reconcile(fx.ctx, reqFor(fx.name))
			Expect(err).To(HaveOccurred(), "the fallback resolve failed")
			Expect(archived().Status).To(Equal(metav1.ConditionUnknown), "the settle waits for the label rather than being written without it")

			fx.reconciler.APIReader = k8sClient
			fx.reconcile()
			Expect(total(fx.name, "Uploaded")).To(Equal(before+1), "a later pass still counts it once")
		})

		// The adopt path resolves the label first and would fail before the
		// settle does; here only the settle's own resolve fails, so the order
		// of that resolve and the condition write is what is held.
		It("does not write the settle's condition when its own label resolve fails", func() {
			ended()
			fx.reconcile()
			job := archiveJob()
			delete(job.Annotations, annotationFlowLabel)
			Expect(k8sClient.Update(fx.ctx, job)).To(Succeed())
			before := total(fx.name, "Uploaded")

			watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			reads := 0
			fx.reconciler.APIReader = interceptor.NewClient(watchClient, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*appsv1.ControllerRevision); ok {
						reads++
						if reads > 1 {
							return errors.New("the apiserver blinked at settle time")
						}
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			settle(archiveJob(), true, `{"uploaded":3,"withheld":0}`)
			_, err = fx.reconciler.Reconcile(fx.ctx, reqFor(fx.name))
			Expect(err).To(HaveOccurred(), "the settle's resolve failed")
			Expect(reads).To(BeNumerically(">", 1), "the adopt path's resolve passed and the settle's did not")
			Expect(archived().Status).To(Equal(metav1.ConditionUnknown))

			fx.reconciler.APIReader = k8sClient
			fx.reconcile()
			Expect(total(fx.name, "Uploaded")).To(Equal(before + 1))
		})

		// A Job this controller did not create (an earlier version's, or one
		// found again after a restart) is adopted; its series must be born at
		// 0 while it runs, not at 1 when it settles.
		It("primes the series of an existing Job it adopts before the Job settles", func() {
			tk := ended()
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: archiveJobName(fx.name), Namespace: resourceNamespace,
					Labels: archiveLabels(tk),
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: flowv1alpha1.SchemeGroupVersion.String(), Kind: archiveTaskKind,
						Name: fx.name, UID: fx.taskUID, Controller: ptr.To(true),
					}},
				},
				Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: archiveContainer, Image: sidecarImage}},
				}}},
			}
			Expect(k8sClient.Create(fx.ctx, job)).To(Succeed())
			_, wasPrimed := counted(fx.name, "Failed")
			Expect(wasPrimed).To(BeFalse(), "nothing has primed this Job's series yet")

			fx.reconcile()

			for _, o := range outcomes {
				v, ok := counted(fx.name, o)
				Expect(ok).To(BeTrue(), "%s is primed once the Job is adopted", o)
				Expect(v).To(BeZero())
			}
		})

		adoptable := func(tk *flowv1alpha1.Task) *batchv1.Job {
			return &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name: archiveJobName(fx.name), Namespace: resourceNamespace,
					Labels: archiveLabels(tk),
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion: flowv1alpha1.SchemeGroupVersion.String(), Kind: archiveTaskKind,
						Name: fx.name, UID: fx.taskUID, Controller: ptr.To(true),
					}},
				},
				Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: archiveContainer, Image: sidecarImage}},
				}}},
			}
		}

		// Adopting is the only way a Job this process did not create gets
		// its status.archive; creation is not the only place the sending begins.
		It("records status.archive when it adopts an existing Job", func() {
			tk := ended()
			Expect(k8sClient.Create(fx.ctx, adoptable(tk))).To(Succeed())

			fx.reconcile()

			Expect(archiveStatus()).To(HaveKeyWithValue("endpoint", "s3.example:9000"))
			Expect(archiveStatus()).To(HaveKey("location"))
		})

		It("records where the adopted Job sends, not where the flow resolves now", func() {
			tk := ended()
			job := adoptable(tk)
			job.Spec.Template.Spec.Containers[0].Args = []string{
				"--endpoint=elsewhere:9000", "--bucket=old-bucket", "--manifest-key-prefix=old/prefix",
			}
			Expect(k8sClient.Create(fx.ctx, job)).To(Succeed())

			fx.reconcile()

			Expect(archiveStatus()).To(HaveKeyWithValue("location", "s3://old-bucket/old/prefix/"))
			Expect(archiveStatus()).To(HaveKeyWithValue("endpoint", "elsewhere:9000"))
		})

		// Catches archiveStatusOf accepting a Job whose arguments name only part
		// of the place: the rest would be filled with nothing instead of the
		// flow's resolution.
		DescribeTable("resolves the flow again when the adopted Job's arguments lack part of the place",
			func(args ...string) {
				tk := ended()
				job := adoptable(tk)
				job.Spec.Template.Spec.Containers[0].Args = args
				Expect(k8sClient.Create(fx.ctx, job)).To(Succeed())

				fx.reconcile()

				Expect(archiveStatus()).To(HaveKeyWithValue("endpoint", "s3.example:9000"))
				Expect(archiveStatus()).To(HaveKeyWithValue("location",
					And(HavePrefix("s3://"), Not(ContainSubstring("old/prefix")), Not(ContainSubstring("old-bucket")))))
			},
			Entry("no bucket", "--endpoint=elsewhere:9000", "--manifest-key-prefix=old/prefix"),
			Entry("no endpoint", "--bucket=old-bucket", "--manifest-key-prefix=old/prefix"),
			Entry("no prefix", "--endpoint=elsewhere:9000", "--bucket=old-bucket"),
		)

		It("keeps the status.archive already recorded when it adopts the Job", func() {
			tk := ended()
			tk.Status.Archive = &flowv1alpha1.ArchiveStatus{Location: "s3://kept/prefix/", Endpoint: "kept:9000"}
			Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
			job := adoptable(tk)
			job.Spec.Template.Spec.Containers[0].Args = []string{"--bucket=other", "--manifest-key-prefix=other/prefix"}
			Expect(k8sClient.Create(fx.ctx, job)).To(Succeed())

			fx.reconcile()

			Expect(archiveStatus()).To(HaveKeyWithValue("location", "s3://kept/prefix/"))
		})

		// Catches the adopt path discarding the label resolve's error: an
		// empty-label series would be primed and the pass would carry on.
		It("fails the pass and primes nothing when the label of an adopted Job cannot be resolved", func() {
			tk := ended()
			Expect(k8sClient.Create(fx.ctx, adoptable(tk))).To(Succeed())

			watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			fx.reconciler.APIReader = interceptor.NewClient(watchClient, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*appsv1.ControllerRevision); ok {
						return errors.New("the apiserver blinked while adopting")
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			_, err = fx.reconciler.Reconcile(fx.ctx, reqFor(fx.name))
			Expect(err).To(HaveOccurred(), "the adopted Job's label could not be resolved")
			for _, o := range outcomes {
				_, ok := counted("", o)
				Expect(ok).To(BeFalse(), "no %s series is born under an empty flow label", o)
			}
		})

		// Catches the adopt path discarding the error of resolving the flow again
		// for a Job whose arguments lack part of the place: the status would be
		// recorded from a flow that did not resolve. The Job carries its label so
		// the label resolve, which has its own guard, is not what fails.
		It("fails the pass when the flow cannot be resolved for an adopted Job lacking part of the place", func() {
			tk := ended()
			job := adoptable(tk)
			job.Annotations = map[string]string{annotationFlowLabel: fx.name}
			job.Spec.Template.Spec.Containers[0].Args = []string{"--bucket=old-bucket"}
			Expect(k8sClient.Create(fx.ctx, job)).To(Succeed())

			watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			fx.reconciler.APIReader = interceptor.NewClient(watchClient, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*appsv1.ControllerRevision); ok {
						return errors.New("the apiserver blinked while resolving the place")
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			_, err = fx.reconciler.ensureArchiveJob(fx.ctx, tk)
			Expect(err).To(HaveOccurred(), "the place could not be resolved")
			Expect(archiveStatus()).NotTo(HaveKey("location"))
		})

		// Catches the adopt path discarding the error of writing status.archive:
		// the pass would carry on with the Job and nothing would write it again.
		It("fails the pass when status.archive of an adopted Job cannot be written", func() {
			tk := ended()
			Expect(k8sClient.Create(fx.ctx, adoptable(tk))).To(Succeed())

			watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			fx.reconciler.Client = interceptor.NewClient(watchClient, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					return errors.New("the apiserver blinked while recording the place")
				},
			})
			_, err = fx.reconciler.ensureArchiveJob(fx.ctx, tk)
			Expect(err).To(HaveOccurred(), "status.archive could not be written")
			Expect(archiveStatus()).NotTo(HaveKey("location"))
		})

		It("does not count on a series the Job's creation never primed", func() {
			fx.makeFlow()
			fx.name += "-run2"
			tk := &flowv1alpha1.Task{
				ObjectMeta: metav1.ObjectMeta{Name: fx.name, Namespace: resourceNamespace},
				Spec:       flowv1alpha1.TaskSpec{Flow: "appears-later"},
			}
			Expect(k8sClient.Create(fx.ctx, tk)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, tk) })
			fx.taskUID = tk.UID
			tk.Status.Phase = flowv1alpha1.PhaseTaskFailed
			tk.Status.ExpiresAt = &metav1.Time{Time: expiresAt}
			Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
			fx.reconcile()
			beforeUnresolved := total(unresolvedLabel, "Uploaded")
			_, wasPrimed := counted("appears-later", "Uploaded")
			Expect(wasPrimed).To(BeFalse())

			later := &flowv1alpha1.TaskFlow{
				ObjectMeta: metav1.ObjectMeta{Name: "appears-later", Namespace: resourceNamespace},
				Spec: flowv1alpha1.TaskFlowSpec{
					Profile: flowv1alpha1.ProfileInvestigate,
					Start:   phaseInvestigate,
					Bindings: map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
						phaseInvestigate: {Handler: fx.name, Next: map[flowv1alpha1.Phase]string{phaseReport: "ok"}},
					},
				},
			}
			Expect(k8sClient.Create(fx.ctx, later)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(fx.ctx, later) })
			settle(archiveJob(), true, `{"uploaded":1,"withheld":0}`)
			fx.reconcile()

			Expect(total(unresolvedLabel, "Uploaded")).To(Equal(beforeUnresolved+1), "the primed series took the count")
			_, nowExists := counted("appears-later", "Uploaded")
			Expect(nowExists).To(BeFalse(), "no series appeared from nowhere")
		})
	})

	Describe("without configuration", func() {
		It("leaves status.archive and the counter's series out", func() {
			fx.reconciler.Archive = ArchiveConfig{}
			ended()
			fx.reconcile()

			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(flowv1alpha1.GroupVersion.WithKind("Task"))
			Expect(k8sClient.Get(fx.ctx, types.NamespacedName{Name: fx.name, Namespace: resourceNamespace}, u)).To(Succeed())
			_, found, err := unstructured.NestedMap(u.Object, "status", "archive")
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse())

			fams, err := ctrlmetrics.Registry.Gather()
			Expect(err).NotTo(HaveOccurred())
			for _, f := range fams {
				if f.GetName() != "taskflow_archive_outcomes_total" {
					continue
				}
				for _, m := range f.GetMetric() {
					for _, l := range m.GetLabel() {
						Expect(l.GetValue()).NotTo(Equal(fx.name), "no series for a flow when nothing is sent")
					}
				}
			}
		})

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

// foreignOwnerName is the owner a pre-existing, not-ours object claims.
const foreignOwnerName = "someone-elses-task"

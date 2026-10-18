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
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/archive"
	"github.com/Tsuguya-HC/taskflow/internal/contract"
	"github.com/Tsuguya-HC/taskflow/internal/metrics"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
)

const (
	archivedType        = "Archived"
	archiveUploading    = "Uploading"
	archiveUploaded     = "Uploaded"
	archiveWithheld     = "Withheld"
	archiveUploadFailed = "UploadFailed"

	actionArchiving           = "Archiving"
	archiveContainer          = "archive"
	archiveUnresolved         = "_unresolved"
	archiveRecheck            = 30 * time.Second
	archiveReadGrace          = 10 * time.Minute
	annotationFlowLabel       = "flow.tgy.io/archive-flow-label"
	archiveUID          int64 = 65532

	flagEndpoint          = "--endpoint"
	flagBucket            = "--bucket"
	flagManifestKeyPrefix = "--manifest-key-prefix"
)

// ArchiveConfig is what the controller is told about sending ended Tasks away.
// The zero value is the feature switched off.
type ArchiveConfig struct {
	Endpoint          string
	Bucket            string
	Prefix            string
	Region            string
	CredentialsSecret string
	Timeout           time.Duration
}

// Enabled reports whether endpoint, bucket and secret are all given.
func (c ArchiveConfig) Enabled() bool {
	return c.Endpoint != "" && c.Bucket != "" && c.CredentialsSecret != ""
}

// Validate refuses a configuration that names some of endpoint, bucket and
// secret but not all of them.
func (c ArchiveConfig) Validate() error {
	given := 0
	for _, v := range []string{c.Endpoint, c.Bucket, c.CredentialsSecret} {
		if v != "" {
			given++
		}
	}
	if given != 0 && given != 3 {
		return errors.New("--archive-endpoint, --archive-bucket and --archive-credentials-secret must be given together")
	}
	return nil
}

// archiveJobName is the name of the Task's archive Job.
func archiveJobName(taskName string) string {
	return runner.ArchiveName(taskName)
}

func (r *TaskReconciler) archiveReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// archiveSettled reports whether the upload has come to an end, one way or the
// other, so that the Task may be deleted.
func archiveSettled(task *flowv1alpha1.Task) bool {
	c := meta.FindStatusCondition(task.Status.Conditions, archivedType)
	return c != nil && c.Status != metav1.ConditionUnknown
}

// archiveHeld reports whether the Task must outlive its date for the upload's
// sake: it is switched on and not settled, or its Job is still there to be
// followed, which a restart with the switch off must not turn into a deletion.
// With the switch off a Job that is gone, or not ours, settles nothing: holding
// the Task for it would keep it from ever being deleted.
func (r *TaskReconciler) archiveHeld(ctx context.Context, task *flowv1alpha1.Task) (bool, error) {
	if archiveSettled(task) {
		return false, nil
	}
	if r.Archive.Enabled() {
		return true, nil
	}
	var job batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: archiveJobName(task.Name), Namespace: task.Namespace}, &job)
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return metav1.IsControlledBy(&job, task), nil
}

// driveArchive makes the archive Job for a Task that has none yet, and records
// how the Job went once it has ended. It does nothing for a settled Task.
// wrote says the condition was changed, which is a reconcile of its own: the
// Task is deleted on a later pass, not in the one that records the outcome.
func (r *TaskReconciler) driveArchive(ctx context.Context, task *flowv1alpha1.Task) (wrote bool, err error) {
	if archiveSettled(task) {
		return false, nil
	}
	if !r.Archive.Enabled() {
		// Switched off: only a Job already ours is followed to its end. Making
		// one with no destination configured would be sending to nowhere.
		var existing batchv1.Job
		err := r.Get(ctx, types.NamespacedName{Name: archiveJobName(task.Name), Namespace: task.Namespace}, &existing)
		if apierrors.IsNotFound(err) || (err == nil && !metav1.IsControlledBy(&existing, task)) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
	}
	job, err := r.ensureArchiveJob(ctx, task)
	if err != nil {
		return false, err
	}
	finished, failure := jobFinished(job)
	switch {
	case !finished:
		if meta.FindStatusCondition(task.Status.Conditions, archivedType) != nil {
			return false, nil
		}
		return true, r.setArchived(ctx, task, metav1.ConditionUnknown, archiveUploading, "the record is being sent")
	case failure != "":
		msg := fmt.Sprintf("the archive Job failed: %s", failure)
		if err := r.settleArchived(ctx, task, job, metav1.ConditionFalse, archiveUploadFailed, msg, metrics.ArchiveFailed); err != nil {
			return false, err
		}
		if r.Recorder != nil {
			r.Recorder.Eventf(task, nil, corev1.EventTypeWarning, archiveUploadFailed, actionArchiving, "%s", msg)
		}
		return true, nil
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(job.Namespace),
		client.MatchingLabels{batchv1.ControllerUidLabel: string(job.UID)}); err != nil {
		return false, err
	}
	var res archive.Result
	looked := 0
	for _, p := range pods.Items {
		for _, cs := range p.Status.ContainerStatuses {
			if t := cs.State.Terminated; t != nil && t.ExitCode == 0 && t.Message != "" {
				looked++
				if err := json.Unmarshal([]byte(t.Message), &res); err != nil {
					msg := fmt.Sprintf("the archive Job's result could not be read: %v", err)
					if err := r.settleArchived(ctx, task, job, metav1.ConditionFalse, archiveUploadFailed, msg, metrics.ArchiveFailed); err != nil {
						return false, err
					}
					if r.Recorder != nil {
						r.Recorder.Eventf(task, nil, corev1.EventTypeWarning, archiveUploadFailed, actionArchiving, "%s", msg)
					}
					return true, nil
				}
			}
		}
	}
	// Nothing to read yet is not an upload of nothing: the Pod may not have
	// reached the cache, and the Task stays held until it has. A Pod that is
	// still unreadable after the grace is gone for good, and holding on would
	// keep the Task from ever being deleted.
	if looked == 0 {
		if r.now().Sub(archiveJobEnded(job)) < archiveReadGrace {
			return false, nil
		}
		msg := "the archive Job's result could not be read: its Pod is gone"
		if err := r.settleArchived(ctx, task, job, metav1.ConditionFalse, archiveUploadFailed, msg, metrics.ArchiveFailed); err != nil {
			return false, err
		}
		if r.Recorder != nil {
			r.Recorder.Eventf(task, nil, corev1.EventTypeWarning, archiveUploadFailed, actionArchiving, "%s", msg)
		}
		return true, nil
	}
	if res.Withheld > 0 {
		return true, r.settleArchived(ctx, task, job, metav1.ConditionTrue, archiveWithheld,
			fmt.Sprintf("%d uploaded, %d withheld", res.Uploaded, res.Withheld), metrics.ArchiveWithheld)
	}
	return true, r.settleArchived(ctx, task, job, metav1.ConditionTrue, archiveUploaded,
		fmt.Sprintf("%d uploaded", res.Uploaded), metrics.ArchiveUploaded)
}

// archiveJobEnded is when the Job completed, falling back to when its
// Complete condition last changed, then to its creation.
func archiveJobEnded(job *batchv1.Job) time.Time {
	if t := job.Status.CompletionTime; t != nil {
		return t.Time
	}
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue && !c.LastTransitionTime.IsZero() {
			return c.LastTransitionTime.Time
		}
	}
	return job.CreationTimestamp.Time
}

// archiveJobLabel is the flow label of the series the Job's outcome is counted
// in. It is resolved before the condition is written: once the condition is
// there nothing calls settleArchived again, so a resolve that failed after it
// would lose the count for good.
func (r *TaskReconciler) archiveJobLabel(ctx context.Context, task *flowv1alpha1.Task, job *batchv1.Job) (string, error) {
	if label, ok := job.Annotations[annotationFlowLabel]; ok {
		return label, nil
	}
	// A Job made before the label was recorded on it: the flow is all there
	// is to go by.
	_, label, err := r.archiveFlow(ctx, task)
	return label, err
}

// settleArchived records how the sending ended and counts it. The count
// follows the write, so a write that failed is tried again and counted once.
// The series is the one primed when the Job was made or found, read back from the Job,
// because resolving the flow again could land on another series or fail; only
// a Job made before the label was recorded on it is resolved here.
func (r *TaskReconciler) settleArchived(
	ctx context.Context, task *flowv1alpha1.Task, job *batchv1.Job, status metav1.ConditionStatus, reason, message, outcome string,
) error {
	label, err := r.archiveJobLabel(ctx, task, job)
	if err != nil {
		return err
	}
	if err := r.setArchived(ctx, task, status, reason, message); err != nil {
		return err
	}
	metrics.ArchiveOutcomes.With(prometheus.Labels{metrics.LabelFlow: label, metrics.LabelOutcome: outcome}).Inc()
	return nil
}

func (r *TaskReconciler) setArchived(
	ctx context.Context, task *flowv1alpha1.Task, status metav1.ConditionStatus, reason, message string,
) error {
	meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type: archivedType, Status: status, Reason: reason, Message: message,
		ObservedGeneration: task.Generation,
	})
	return r.Status().Update(ctx, task)
}

func (r *TaskReconciler) ensureArchiveJob(ctx context.Context, task *flowv1alpha1.Task) (*batchv1.Job, error) {
	name := archiveJobName(task.Name)
	key := types.NamespacedName{Name: name, Namespace: task.Namespace}

	adopt := func(job *batchv1.Job) (*batchv1.Job, error) {
		if !metav1.IsControlledBy(job, task) {
			return nil, notOwnedError("job", name, task, job.OwnerReferences)
		}
		return job, nil
	}
	var existing batchv1.Job
	err := r.Get(ctx, key, &existing)
	if err == nil {
		job, err := adopt(&existing)
		if err != nil {
			return nil, err
		}
		// Priming is idempotent and runs on every sight of the Job: one this
		// process never made (an older controller's, or one found again
		// after a restart) has had no series primed for it.
		label, err := r.archiveJobLabel(ctx, task, job)
		if err != nil {
			return nil, err
		}
		metrics.PrimeArchiveOutcomes(label)
		if task.Status.Archive == nil {
			// Where the Job sends is read from the Job: resolving the flow again could
			// name another place than the one it writes to.
			status, ok := archiveStatusOf(job)
			if !ok {
				flow, _, err := r.archiveFlow(ctx, task)
				if err != nil {
					return nil, err
				}
				status = r.archiveStatusAt(r.archivePrefix(task, flow))
			}
			if err := r.recordArchive(ctx, task, status); err != nil {
				return nil, err
			}
		}
		return job, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	if err := r.ensureArchiveMap(ctx, task); err != nil {
		return nil, err
	}
	pvc, err := r.archiveWorkspace(ctx, task)
	if err != nil {
		return nil, err
	}
	flow, label, err := r.archiveFlow(ctx, task)
	if err != nil {
		return nil, err
	}
	prefix := r.archivePrefix(task, flow)
	if err := r.recordArchive(ctx, task, r.archiveStatusAt(prefix)); err != nil {
		return nil, err
	}
	metrics.PrimeArchiveOutcomes(label)
	job := r.buildArchiveJob(task, pvc, prefix, label)
	if err := controllerutil.SetControllerReference(task, job, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			var got batchv1.Job
			if err := r.Get(ctx, key, &got); err != nil {
				return nil, err
			}
			return adopt(&got)
		}
		return nil, err
	}
	return job, nil
}

// recordArchive writes where the task's archive goes.
func (r *TaskReconciler) recordArchive(ctx context.Context, task *flowv1alpha1.Task, status *flowv1alpha1.ArchiveStatus) error {
	task.Status.Archive = status
	return r.Status().Update(ctx, task)
}

func (r *TaskReconciler) archivePrefix(task *flowv1alpha1.Task, flow string) string {
	return path.Join(r.Archive.Prefix, task.Namespace, flow, string(task.UID))
}

func (r *TaskReconciler) archiveStatusAt(prefix string) *flowv1alpha1.ArchiveStatus {
	return &flowv1alpha1.ArchiveStatus{Location: archiveLocation(r.Archive.Bucket, prefix), Endpoint: r.Archive.Endpoint}
}

func archiveLocation(bucket, prefix string) string {
	return "s3://" + bucket + "/" + prefix + "/"
}

// archiveStatusOf reads the place a Job sends to from its own arguments.
func archiveStatusOf(job *batchv1.Job) (*flowv1alpha1.ArchiveStatus, bool) {
	for _, c := range job.Spec.Template.Spec.Containers {
		if c.Name != archiveContainer {
			continue
		}
		args := map[string]string{}
		for _, a := range c.Args {
			if k, v, ok := strings.Cut(a, "="); ok {
				args[k] = v
			}
		}
		prefix, bucket, endpoint := args[flagManifestKeyPrefix], args[flagBucket], args[flagEndpoint]
		if prefix == "" || bucket == "" || endpoint == "" {
			return nil, false
		}
		return &flowv1alpha1.ArchiveStatus{Location: archiveLocation(bucket, prefix), Endpoint: endpoint}, true
	}
	return nil, false
}

func (r *TaskReconciler) ensureArchiveMap(ctx context.Context, task *flowv1alpha1.Task) error {
	name := archiveJobName(task.Name)
	var existing corev1.ConfigMap
	err := r.archiveReader().Get(ctx, types.NamespacedName{Name: name, Namespace: task.Namespace}, &existing)
	if err == nil {
		if !metav1.IsControlledBy(&existing, task) {
			return notOwnedError("configmap", name, task, existing.OwnerReferences)
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	record := task.DeepCopy()
	record.ManagedFields = nil
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: task.Namespace,
			Labels: archiveLabels(task),
		},
		Data: map[string]string{"task.json": string(body)},
	}
	if err := controllerutil.SetControllerReference(task, cm, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, cm); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return err
		}
		var got corev1.ConfigMap
		if err := r.archiveReader().Get(ctx, client.ObjectKeyFromObject(cm), &got); err != nil {
			return err
		}
		if !metav1.IsControlledBy(&got, task) {
			return notOwnedError("configmap", name, task, got.OwnerReferences)
		}
	}
	return nil
}

// archiveWorkspace is the name of the Task's workspace claim, or "" when it
// has none to send.
func (r *TaskReconciler) archiveWorkspace(ctx context.Context, task *flowv1alpha1.Task) (string, error) {
	name := runner.WorkspacePVCName(task.Name, task.UID)
	var pvc corev1.PersistentVolumeClaim
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: task.Namespace}, &pvc)
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !metav1.IsControlledBy(&pvc, task) {
		return "", notOwnedError("claim", name, task, pvc.OwnerReferences)
	}
	return name, nil
}

func archiveLabels(task *flowv1alpha1.Task) map[string]string {
	return map[string]string{
		contract.LabelManagedBy: contract.ManagedBy,
		runner.LabelTaskUID:     string(task.UID),
		contract.LabelRole:      contract.RoleArchive,
	}
}

// archiveFlow is the flow's place in the manifest key and its value for the
// metric's flow label.
func (r *TaskReconciler) archiveFlow(ctx context.Context, task *flowv1alpha1.Task) (key, label string, err error) {
	if _, _, err := r.resolveFlow(ctx, task); err != nil {
		if apierrors.IsNotFound(err) {
			return archiveUnresolved, metrics.FlowUnresolved, nil
		}
		return "", "", err
	}
	return task.Spec.Flow, task.Spec.Flow, nil
}

func (r *TaskReconciler) buildArchiveJob(task *flowv1alpha1.Task, pvc, prefix, flowLabel string) *batchv1.Job {
	cfg := r.Archive
	const recordDir = "/archive"
	args := []string{
		contract.SubcommandArchive,
		"--task-json=" + recordDir + "/task.json",
		flagEndpoint + "=" + cfg.Endpoint,
		flagBucket + "=" + cfg.Bucket,
		"--region=" + cfg.Region,
		flagManifestKeyPrefix + "=" + prefix,
	}
	mounts := []corev1.VolumeMount{{Name: "record", MountPath: recordDir, ReadOnly: true}}
	volumes := []corev1.Volume{{Name: "record", VolumeSource: corev1.VolumeSource{
		ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: archiveJobName(task.Name)}},
	}}}
	if pvc != "" {
		args = append(args, "--results="+recordDir+"/results")
		mounts = append(mounts, corev1.VolumeMount{
			Name: contract.WorkspaceVolume, MountPath: recordDir + "/results", SubPath: "results", ReadOnly: true,
		})
		volumes = append(volumes, corev1.Volume{Name: contract.WorkspaceVolume, VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc, ReadOnly: true},
		}})
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: archiveJobName(task.Name), Namespace: task.Namespace, Labels: archiveLabels(task),
			Annotations: map[string]string{annotationFlowLabel: flowLabel},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          ptr.To(int32(2)),
			ActiveDeadlineSeconds: ptr.To(int64(cfg.Timeout.Seconds())),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: archiveLabels(task)},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: ptr.To(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   ptr.To(true),
						RunAsUser:      ptr.To(archiveUID),
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Volumes: volumes,
					Containers: []corev1.Container{{
						Name:         archiveContainer,
						Image:        r.SidecarImage,
						Args:         args,
						EnvFrom:      []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: cfg.CredentialsSecret}}}},
						VolumeMounts: mounts,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptr.To(false),
							ReadOnlyRootFilesystem:   ptr.To(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
					}},
				},
			},
		},
	}
}

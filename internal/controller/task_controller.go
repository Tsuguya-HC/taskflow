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
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/Tsuguya/taskflow/internal/collect"
	"github.com/Tsuguya/taskflow/internal/runner"
	"github.com/Tsuguya/taskflow/internal/taskstate"
	"github.com/Tsuguya/taskflow/internal/transition"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
)

// brokenFlow says the definition is wrong rather than the work. It is carried
// as an error so that every path out of the reconcile goes through one place
// that writes Failed, instead of each caller remembering to.
type brokenFlow struct{ reason string }

func (e brokenFlow) Error() string { return e.reason }

// TaskReconciler reconciles a Task object
type TaskReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Now is the clock deadlines are judged against; nil means the wall clock.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=flow.tgy.io,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=flow.tgy.io,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=flow.tgy.io,resources=tasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=flow.tgy.io,resources=taskflows;taskhandlers,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Task object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *TaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var task flowv1alpha1.Task
	if err := r.Get(ctx, req.NamespacedName, &task); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !task.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// A stopped task has one thing left to do, and its date is already on
	// it; nothing below needs consulting, least of all the flow. The Jobs go
	// with it through their ownerReference (§10).
	if task.Status.ExpiresAt != nil {
		remaining := task.Status.ExpiresAt.Sub(r.now())
		if remaining > 0 {
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
		log.Info("task expired", "phase", task.Status.Phase, "expiresAt", task.Status.ExpiresAt)
		return ctrl.Result{}, r.expire(ctx, &task)
	}

	// The framework's own terminal phases are terminal on their own say-so —
	// unlike a phase the flow declared terminal, they need no binding table to
	// tell. Checking that before the flow is resolved means a Task already at
	// Escalated is never at the mercy of a flow that GitOps has since deleted
	// or renamed out from under it.
	if task.Status.Phase.IsReserved() {
		// A Task that reached Escalated or Failed before expiresAt existed
		// has none, and never will on its own: nothing above sees it again,
		// so without this it would sit forever. Backfilling means fetching
		// the flow this once, ahead of where it is normally resolved; a
		// flow already gone leaves nothing to read a ttl from, the same as
		// the nil ttl fail() gets when there is no flow at all.
		var flow flowv1alpha1.TaskFlow
		if err := r.Get(ctx, types.NamespacedName{Name: task.Spec.Flow, Namespace: task.Namespace}, &flow); err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.backfillExpiry(ctx, &task, nil, flow.Spec.TTL)
	}

	// A flow is always resolved in the task's own namespace. There is no field
	// naming another one, which is what reduces "may I start this flow" to
	// "may I create a Task here" — a question plain RBAC can answer.
	var flow flowv1alpha1.TaskFlow
	if err := r.Get(ctx, types.NamespacedName{Name: task.Spec.Flow, Namespace: task.Namespace}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			// The flow was deleted or never existed. Nothing to repair.
			return ctrl.Result{}, r.fail(ctx, &task, nil, fmt.Sprintf("flow %q does not exist in this namespace", task.Spec.Flow))
		}
		return ctrl.Result{}, err
	}

	if task.Status.Phase == "" {
		return ctrl.Result{}, r.begin(ctx, &task, &flow)
	}
	// A phase with no binding is terminal (§5 "束縛の無いステータスが終端") — but
	// which of two things happened is not the same call. CurrentRun tells them
	// apart: begin and Advance never set it without first confirming a
	// binding, so it is nil exactly when the phase was terminal on arrival,
	// and still set only when a run was in flight and the flow was edited out
	// from under it. The latter is a structural fault (§5 "実行時の矛盾は修復せ
	// ず Failed"), not a quiet finish, so it must not be indistinguishable
	// from success.
	if _, bound := flow.Spec.Bindings[task.Status.Phase]; !bound {
		if task.Status.CurrentRun != nil {
			return ctrl.Result{}, r.fail(ctx, &task, flow.Spec.TTL, fmt.Sprintf(
				"phase %q lost its binding in flow %q while a run was in flight", task.Status.Phase, flow.Name))
		}
		// Same backfill as the reserved-phase branch above, for a task that
		// reached a flow-declared terminal phase before expiresAt existed.
		// The flow is already in hand here, so nothing extra needs fetching.
		return ctrl.Result{}, r.backfillExpiry(ctx, &task, flow.Spec.Bindings, flow.Spec.TTL)
	}

	run := task.Status.CurrentRun
	recovering := run == nil
	if recovering {
		// A non-terminal task with nothing in flight means the status was
		// written but the Job never got created — a crash between the two
		// writes. Pick it up rather than stalling.
		run = &flowv1alpha1.RunRef{Phase: task.Status.Phase, RunID: task.Status.RunID}
	}
	prior := run.DeepCopy()

	job, err := r.ensureJob(ctx, &task, &flow, run)
	if err != nil {
		var broken brokenFlow
		if errors.As(err, &broken) {
			return ctrl.Result{}, r.fail(ctx, &task, flow.Spec.TTL, broken.reason)
		}
		return ctrl.Result{}, err
	}

	// ensureJob fills in run.JobName and run.Deadline; a fresh recovery run
	// had neither to begin with, and even an existing run's status object
	// might predate these fields. Persist them so a stuck run can be found by
	// name, and its deadline read, without recomputing either.
	if recovering || !equality.Semantic.DeepEqual(run, prior) {
		task.Status.CurrentRun = run
		if err := r.Status().Update(ctx, &task); err != nil {
			return ctrl.Result{}, err
		}
	}

	finished, failure := jobFinished(job)
	if !finished {
		if run.Deadline == nil {
			log.V(1).Info("run in flight", "phase", run.Phase, "runID", run.RunID, "job", job.Name)
			return ctrl.Result{}, nil
		}
		// The Job carries the same deadline and the kubelet normally enforces
		// it first, so this path only ever fires when that did not end the
		// run — a pod stuck terminating, most likely. Waiting a little past
		// the deadline before stepping in lets the ordinary route report
		// first, and the answer is the same either way.
		remaining := run.Deadline.Sub(r.now()) + deadlineGrace
		if remaining > 0 {
			log.V(1).Info("run in flight", "phase", run.Phase, "runID", run.RunID, "job", job.Name, "deadlineIn", remaining)
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
		return ctrl.Result{}, r.settle(ctx, &task, &flow, run, nil, timedOut(job))
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(job.Namespace),
		client.MatchingLabels{batchv1.ControllerUidLabel: string(job.UID)}); err != nil {
		return ctrl.Result{}, err
	}

	switch {
	case failure == batchv1.JobReasonDeadlineExceeded:
		// A run cut short may well have written a directory before it was
		// killed, but a directory written on the way out is not a conclusion.
		// Whatever it says, the answer is that it did not finish.
		return ctrl.Result{}, r.settle(ctx, &task, &flow, run, nil, timedOut(job))
	case failure != "" && !collect.Ran(pods.Items):
		// The handler never got to run — nothing pulled, nothing scheduled.
		// That is the one kind of failure the controller retries on its own,
		// under a new runID so the retry does not find the last attempt's
		// directories.
		return ctrl.Result{}, r.retryInfra(ctx, &task, &flow, run, failure)
	}

	answer := collect.FromPods(pods.Items, transition.Directories(flow.Spec.Bindings, run.Phase))
	return ctrl.Result{}, r.settle(ctx, &task, &flow, run, &answer, "")
}

// deadlineGrace is how long past a run's deadline the controller waits for the
// Job to report the timeout itself before ruling on it.
const deadlineGrace = time.Minute

// jobFinished reports whether the Job is done, and the reason when it failed.
// A Job that finished with neither condition true is still running.
func jobFinished(job *batchv1.Job) (finished bool, failure string) {
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			return true, ""
		case batchv1.JobFailed:
			// A failure with no reason is still a failure, and an empty
			// string would read as "completed" to the caller.
			if c.Reason == "" {
				return true, "Failed"
			}
			return true, c.Reason
		}
	}
	return false, ""
}

func timedOut(job *batchv1.Job) string {
	if job.Spec.ActiveDeadlineSeconds == nil {
		return "the run timed out"
	}
	return fmt.Sprintf("the run timed out after %s", time.Duration(*job.Spec.ActiveDeadlineSeconds)*time.Second)
}

// settle records a finished run and moves the task on. answer is nil when the
// run is being ruled on without reading it — a timeout — and noAnswer then
// says why.
func (r *TaskReconciler) settle(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
	run *flowv1alpha1.RunRef,
	answer *collect.Answer,
	noAnswer string,
) error {
	in := transition.Input{
		Bindings: flow.Spec.Bindings,
		Phase:    run.Phase,
		NoAnswer: noAnswer,
		Visited:  taskstate.Visited(&task.Status, flow.Spec.Bindings),
		Budget:   task.Status.ReworkBudget,
	}
	if answer != nil {
		in.Directory = answer.Directory
		if answer.Directory == "" {
			in.NoAnswer = answer.Reason
		}
	}
	res := transition.Next(in)
	if answer != nil && answer.Directory != "" && answer.Reason != "" {
		// What the handler said after naming its directory is the most
		// useful line a human will get about this run; keep it next to the
		// framework's own account rather than losing it.
		res.Detail += ": " + answer.Reason
	}

	logf.FromContext(ctx).Info("run finished",
		"phase", run.Phase, "runID", run.RunID, "directory", in.Directory,
		"outcome", res.Outcome, "next", res.Next)
	now := metav1.NewTime(r.now())
	taskstate.Advance(&task.Status, flow.Spec.Bindings, in.Directory, res, "", flow.Spec.TTL, now)
	return r.Status().Update(ctx, task)
}

// retryInfra re-runs a phase the handler never got to run, or escalates when
// the handler's retry allowance is spent. The handler is fetched here rather
// than carried from ensureJob because only this path needs it, and its
// disappearing in between is the same broken-flow fault it would be anywhere.
func (r *TaskReconciler) retryInfra(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
	run *flowv1alpha1.RunRef,
	failure string,
) error {
	binding := flow.Spec.Bindings[run.Phase]
	handler, err := r.handlerFor(ctx, task, binding, run.Phase)
	if err != nil {
		var broken brokenFlow
		if errors.As(err, &broken) {
			return r.fail(ctx, task, flow.Spec.TTL, broken.reason)
		}
		return err
	}

	if taskstate.InfraRetriesExhausted(&task.Status, handler.Spec.MaxInfraRetries) {
		return r.settle(ctx, task, flow, run, nil, fmt.Sprintf(
			"the run never started (%s) and %d infrastructure retries were spent",
			failure, run.InfraRetries))
	}

	logf.FromContext(ctx).Info("retrying a run that never started",
		"phase", run.Phase, "runID", run.RunID, "failure", failure, "retries", run.InfraRetries)
	taskstate.RetryInfra(&task.Status)
	return r.Status().Update(ctx, task)
}

// handlerFor fetches the handler a binding names. Its disappearing is a
// broken flow rather than a transient fault, reported through brokenFlow so
// every caller turns it into a Failed the same way instead of each
// remembering the NotFound check itself.
func (r *TaskReconciler) handlerFor(
	ctx context.Context,
	task *flowv1alpha1.Task,
	binding flowv1alpha1.PhaseBinding,
	phase flowv1alpha1.Phase,
) (*flowv1alpha1.TaskHandler, error) {
	var handler flowv1alpha1.TaskHandler
	if err := r.Get(ctx, types.NamespacedName{Name: binding.Handler, Namespace: task.Namespace}, &handler); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, brokenFlow{fmt.Sprintf("phase %q names handler %q, which does not exist", phase, binding.Handler)}
		}
		return nil, err
	}
	return &handler, nil
}

func (r *TaskReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// begin puts a fresh task on the flow's starting phase. The decision of what
// that does to status lives in taskstate; this is just the fetch-mutate-write
// around it.
func (r *TaskReconciler) begin(ctx context.Context, task *flowv1alpha1.Task, flow *flowv1alpha1.TaskFlow) error {
	if _, bound := flow.Spec.Bindings[flow.Spec.Start]; !bound {
		return r.fail(ctx, task, flow.Spec.TTL, fmt.Sprintf("flow %q starts at %q, which nothing binds", flow.Name, flow.Spec.Start))
	}
	taskstate.Begin(&task.Status, flow.Spec.Start, flow.Spec.ReworkBudget)
	return r.Status().Update(ctx, task)
}

// fail stops a task whose flow is broken. Nothing is retried: the fault is in
// the definition rather than in the work, and guessing at a repair would hide
// it.
//
// ttl is the flow's, or nil when the fault is that there is no flow to read
// one from; such a task stays for as long as the flow stays gone. That is
// deliberate, not a gap: the reserved-phase branch above backfills expiresAt
// from whatever flow the task names on every later reconcile, so a flow
// created under the same name afterward is enough to make the date appear
// and the task eventually clear, with no special path needed for that case.
func (r *TaskReconciler) fail(ctx context.Context, task *flowv1alpha1.Task, ttl *flowv1alpha1.TTLSpec, reason string) error {
	// A dispatched task with nothing in flight has already reached a terminal
	// phase: Advance clears CurrentRun exactly when it lands one there, whether
	// that phase is Failed, Escalated, or one the flow itself declared
	// terminal. Leaving it alone here, not just for Failed specifically, is
	// what keeps a deleted or renamed flow from overwriting a finished task's
	// audit trail.
	if task.Status.Phase != "" && task.Status.CurrentRun == nil {
		return nil
	}
	taskstate.Fail(&task.Status, reason, ttl, metav1.NewTime(r.now()))
	return r.Status().Update(ctx, task)
}

// backfillExpiry stamps expiresAt on a task that reached a terminal phase
// before this field existed to stamp it there — the one gap Advance and Fail
// cannot close themselves, since they only ever run at the moment a task
// lands on a phase, not on every later reconcile of one already there.
// Expire is unconditional here, not guarded on ExpiresAt already being set,
// because Expire's own idempotence covers that: called again on a task that
// already has a date, or one that would get none from ttl, it leaves status
// exactly as it found it, and this returns nil rather than writing back a
// status equal to what is already stored.
func (r *TaskReconciler) backfillExpiry(
	ctx context.Context,
	task *flowv1alpha1.Task,
	bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding,
	ttl *flowv1alpha1.TTLSpec,
) error {
	before := task.Status.ExpiresAt
	taskstate.Expire(&task.Status, bindings, ttl, metav1.NewTime(r.now()))
	if task.Status.ExpiresAt == before {
		return nil
	}
	return r.Status().Update(ctx, task)
}

// expire deletes a task whose date has passed. The UID precondition means a
// stale read — the object this reconcile fetched has since been deleted and
// a different one created under the same name — refuses rather than taking
// the newer object down with it.
func (r *TaskReconciler) expire(ctx context.Context, task *flowv1alpha1.Task) error {
	return client.IgnoreNotFound(r.Delete(ctx, task, client.Preconditions{UID: &task.UID}))
}

// ensureJob creates the Job for a run, or returns the one already there.
//
// The name is derived from the task, phase and run, so a second call after a
// restart collides with the first Job instead of starting a second one.
func (r *TaskReconciler) ensureJob(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
	run *flowv1alpha1.RunRef,
) (*batchv1.Job, error) {
	name := runner.JobName(task.Name, run.Phase, run.RunID)
	// Deterministic the moment it's computed, regardless of which branch below
	// ends up returning: the caller persists this into CurrentRun so a run
	// stuck in flight can be found by name without recomputing the hash.
	run.JobName = name

	var existing batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: task.Namespace}, &existing)
	if err == nil {
		// The name is deterministic, not exclusive: anything with create
		// permission on Jobs could have taken it first. Trusting whatever sits
		// under it without checking who made it would let that Job's outcome
		// pass for this task's.
		if !runner.OwnedByTask(&existing, task.UID) {
			return nil, fmt.Errorf("job %q exists but is not owned by task %s (uid %s): owners = %s",
				name, task.Name, task.UID, ownerSummary(existing.OwnerReferences))
		}
		run.Deadline = deadlineOf(&existing)
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	// Reconcile only ever calls this with a phase it already confirmed is
	// bound — an unbound current phase is either a quiet finish or, with a
	// run in flight, a Failed of its own, and neither reaches here.
	binding := flow.Spec.Bindings[run.Phase]

	handler, err := r.handlerFor(ctx, task, binding, run.Phase)
	if err != nil {
		return nil, err
	}

	job, err := runner.BuildJob(runner.Input{
		Task:        task,
		Handler:     handler,
		Phase:       run.Phase,
		RunID:       run.RunID,
		PrevRunID:   previousRun(task),
		Directories: transition.Directories(flow.Spec.Bindings, run.Phase),
	})
	if err != nil {
		// A template that breaks an invariant is a definition problem, so it
		// fails rather than being retried.
		return nil, brokenFlow{err.Error()}
	}
	if err := r.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Another reconcile got there first, which is what the
			// deterministic name is for — but "another reconcile" needs the
			// same ownership check as the r.Get above, for the same reason.
			var got batchv1.Job
			if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: task.Namespace}, &got); err != nil {
				return nil, err
			}
			if !runner.OwnedByTask(&got, task.UID) {
				return nil, fmt.Errorf("job %q exists but is not owned by task %s (uid %s): owners = %s",
					name, task.Name, task.UID, ownerSummary(got.OwnerReferences))
			}
			run.Deadline = deadlineOf(&got)
			return &got, nil
		}
		return nil, err
	}
	run.Deadline = deadlineOf(job)
	return job, nil
}

// deadlineOf is when the Job's own deadline falls: the timeout the handler
// declared, counted from when the Job was actually created. Read off the Job
// rather than computed from the clock so a controller restart lands on the
// same instant, and nil when the handler declared no timeout.
func deadlineOf(job *batchv1.Job) *metav1.Time {
	if job.Spec.ActiveDeadlineSeconds == nil {
		return nil
	}
	t := metav1.NewTime(job.CreationTimestamp.Add(time.Duration(*job.Spec.ActiveDeadlineSeconds) * time.Second))
	return &t
}

// ownerSummary renders a Job's actual owners for an error message, so
// whoever reads it can see what claimed the name first.
func ownerSummary(refs []metav1.OwnerReference) string {
	if len(refs) == 0 {
		return "none"
	}
	parts := make([]string, len(refs))
	for i, ref := range refs {
		parts[i] = fmt.Sprintf("%s/%s (uid %s)", ref.Kind, ref.Name, ref.UID)
	}
	return strings.Join(parts, ", ")
}

// previousRun is the run before the one in flight, or 0 on the first attempt.
func previousRun(task *flowv1alpha1.Task) int32 {
	if len(task.Status.History) == 0 {
		return 0
	}
	return task.Status.History[len(task.Status.History)-1].RunID
}

// SetupWithManager sets up the controller with the Manager.
func (r *TaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&flowv1alpha1.Task{}).
		// Jobs are owned, so finishing one wakes the task that started it.
		Owns(&batchv1.Job{}).
		Named("task").
		Complete(r)
}

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
	"slices"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/collect"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// driveBranches takes a fork's branches as far as this reconcile can
// (ADR-0013). Each branch is a Job run of its own, started, watched and
// timed like any other; what differs is that none of them moves the task on
// its own. Every branch found finished here is settled together, in one
// status write, by taskstate.SettleBranches — which is what decides whether
// they have all met at the join, or one of them stopped the task and the
// rest are cancelled. A branch still running is waited for; the soonest of
// their deadlines is when this looks again.
func (r *TaskReconciler) driveBranches(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
) (ctrl.Result, error) {
	fork := task.Status.Phase
	if !forks(&flow.Spec, fork) {
		// The runs in flight are branches of a fork the flow no longer has:
		// there is no join to meet at and nothing to judge their answers
		// against, so the task stops on the definition (§5, P8).
		return ctrl.Result{}, r.failBranches(ctx, task, &flow.Spec, fmt.Sprintf(
			"flow %q no longer forks at %q while its branches were running", flow.Name, fork))
	}

	prior := task.Status.DeepCopy()
	// Counted once, before anything settles: every branch here is judged
	// against the same record, so the order they are looked at in cannot
	// change what any of them is allowed to do.
	runs := taskstate.Runs(&task.Status, flow.Spec.Bindings)
	var settled []taskstate.SettledBranch
	var wait time.Duration
	for _, run := range slices.Clone(task.Status.CurrentRuns) {
		branch, after, err := r.observeBranch(ctx, task, flow, &run, runs)
		if err != nil {
			var broken brokenFlow
			if errors.As(err, &broken) {
				return ctrl.Result{}, r.failBranches(ctx, task, &flow.Spec, broken.reason)
			}
			return ctrl.Result{}, err
		}
		if branch != nil {
			settled = append(settled, *branch)
		}
		if after > 0 && (wait == 0 || after < wait) {
			wait = after
		}
	}

	if len(settled) > 0 {
		taskstate.SettleBranches(&task.Status, &flow.Spec, settled, metav1.NewTime(r.now()))
	}
	if equality.Semantic.DeepEqual(prior, &task.Status) {
		return ctrl.Result{RequeueAfter: wait}, nil
	}
	if err := r.Status().Update(ctx, task); err != nil {
		return ctrl.Result{}, err
	}

	if transition.IsTerminal(flow.Spec.Bindings, task.Status.Phase) {
		detail := ""
		if n := len(task.Status.History); n > 0 {
			detail = task.Status.History[n-1].Reason
		}
		r.announce(task, &flow.Spec, task.Status.Phase, detail)
		// The task stopped on a branch's say-so. The branches that were still
		// running are recorded as cancelled; their Jobs are stopped now that
		// the record says so, because nothing they could answer would change
		// where the task went (ADR-0013 決定4). Announced before this runs, so
		// a delete that fails here — and the requeue that follows — never
		// costs the task its metric or its Event.
		return ctrl.Result{}, r.reapCancelledJobs(ctx, task)
	}
	return ctrl.Result{RequeueAfter: wait}, nil
}

// observeBranch looks at one branch: starts its Job if it has none yet, and
// says what it answered once it has finished. It returns the branch settled,
// or how long until it should be looked at again, or neither while it simply
// runs. Anything it learns about the run — its Job's name, its deadline, an
// attempt that never started and is tried again — is written into the
// task's status for the caller's one write.
func (r *TaskReconciler) observeBranch(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
	run *flowv1alpha1.RunRef,
	runs map[flowv1alpha1.Phase]int32,
) (*taskstate.SettledBranch, time.Duration, error) {
	kind, err := r.runnerOf(ctx, task, flow, run)
	if err != nil {
		return nil, 0, err
	}
	if kind != flowv1alpha1.RunnerJob {
		// A branch answered in a box would have to be waited on alongside
		// Jobs by a second mechanism; for now a fork's branches are Jobs
		// (ADR-0013 決定3), and a handler that says otherwise is a flow this
		// controller cannot run as written.
		return nil, 0, brokenFlow{fmt.Sprintf(
			"branch %q is filled by a %s runner; a fork's branches run as Jobs", run.Phase, kind)}
	}

	job, err := r.ensureJob(ctx, task, flow, run)
	if err != nil {
		return nil, 0, err
	}
	taskstate.SetRun(&task.Status, *run)

	finished, failure := jobFinished(job)
	if !finished {
		if run.Deadline == nil {
			return nil, 0, nil
		}
		remaining := run.Deadline.Sub(r.now()) + deadlineGrace
		if remaining > 0 {
			return nil, remaining, nil
		}
		return r.judgeBranch(flow, run, runs, nil, timedOut(job)), 0, nil
	}

	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(job.Namespace),
		client.MatchingLabels{batchv1.ControllerUidLabel: string(job.UID)}); err != nil {
		return nil, 0, err
	}
	switch {
	case failure == batchv1.JobReasonDeadlineExceeded:
		return r.judgeBranch(flow, run, runs, nil, timedOut(job)), 0, nil
	case failure != "" && !collect.Ran(pods.Items):
		handlerName, _, ok := runSpec(&flow.Spec, run.Phase)
		if !ok {
			return nil, 0, brokenFlow{fmt.Sprintf(
				"flow %q no longer says who fills run %d of %q", flow.Name, run.RunID, run.Phase)}
		}
		handler, err := r.handlerFor(ctx, task, handlerName, run.Phase)
		if err != nil {
			return nil, 0, err
		}
		if run.InfraRetries >= handler.Spec.MaxInfraRetries {
			return r.judgeBranch(flow, run, runs, nil, fmt.Sprintf(
				"the run never started (%s) and %d infrastructure retries were spent", failure, run.InfraRetries)), 0, nil
		}
		logf.FromContext(ctx).Info("retrying a branch that never started",
			"phase", run.Phase, "runID", run.RunID, "failure", failure, "retries", run.InfraRetries)
		taskstate.RetryRun(&task.Status, run.Phase)
		return nil, 0, nil
	}

	_, directories, ok := runSpec(&flow.Spec, run.Phase)
	if !ok {
		return nil, 0, brokenFlow{fmt.Sprintf(
			"flow %q no longer says what run %d of %q may answer with", flow.Name, run.RunID, run.Phase)}
	}
	answer := collect.FromPods(pods.Items, directories, false)
	return r.judgeBranch(flow, run, runs, &answer, ""), 0, nil
}

// judgeBranch is where one branch's answer sends it, by the flow's own table —
// the join, or somewhere that stops the task — packaged for SettleBranches.
func (r *TaskReconciler) judgeBranch(
	flow *flowv1alpha1.TaskFlow,
	run *flowv1alpha1.RunRef,
	runs map[flowv1alpha1.Phase]int32,
	answer *collect.Answer,
	noAnswer string,
) *taskstate.SettledBranch {
	in := transition.Input{
		Bindings: flow.Spec.Bindings,
		Phase:    run.Phase,
		NoAnswer: noAnswer,
		Runs:     runs,
		MaxRuns:  flow.Spec.MaxRunsPerPhase,
	}
	if answer != nil {
		in.Directory = answer.Directory
		if answer.Directory == "" {
			in.NoAnswer = answer.Reason
		}
	}
	res := transition.Next(in)
	if answer != nil && answer.Directory != "" && answer.Reason != "" {
		res.Detail += ": " + answer.Reason
	}
	return &taskstate.SettledBranch{Run: *run, Directory: in.Directory, Result: res}
}

// reapCancelledJobs stops the Jobs of every run history records Cancelled,
// for as long as they are still there and have not finished on their own. A
// delete is a request, not a guarantee, and a Cancelled branch's pod may
// still be sealing its publish through its grace period regardless — so
// rather than trusting a delete issued elsewhere to have landed, this asks
// the cluster what is actually there. It reads entirely from history and the
// Job list, not from any run ref a caller might be holding, which is what
// makes it safe to call again on any later reconcile, or after a controller
// restart, and find whatever is still left.
//
// A Job jobFinished already calls done is left alone: it finished on its
// own, cancelled or not, and its pod stays as autopsy material until its own
// TTL (design.md) — this only reaches for the ones a cancellation is still
// owed to.
func (r *TaskReconciler) reapCancelledJobs(ctx context.Context, task *flowv1alpha1.Task) error {
	cancelled := make(map[string]bool)
	for _, h := range task.Status.History {
		if transition.Outcome(h.Outcome) == transition.OutcomeCancelled {
			cancelled[strconv.Itoa(int(h.RunID))] = true
		}
	}
	if len(cancelled) == 0 {
		return nil
	}
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(task.Namespace),
		client.MatchingLabels{runner.LabelTaskUID: string(task.UID)}); err != nil {
		return err
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if !cancelled[job.Labels[runner.LabelRunID]] || job.DeletionTimestamp != nil {
			continue
		}
		if finished, _ := jobFinished(job); finished {
			continue
		}
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// failBranches stops a task whose flow broke while a fork's branches were
// running — the definition itself gave out, not one of the branches, so none
// of them is answered for; every branch still in flight is recorded
// Cancelled, and only then does the task fail.
//
// taskstate.Fail is called directly here rather than through r.fail: by the
// time the branches are cancelled, currentRuns is empty and the task would
// look Idle to the guard r.fail keeps against writing over an
// already-finished task's history. That guard does not apply on this path —
// a task with branches still running has not finished — so it must be
// skipped rather than tripped.
func (r *TaskReconciler) failBranches(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlowSpec,
	reason string,
) error {
	now := metav1.NewTime(r.now())
	taskstate.CancelBranches(&task.Status, reason, now)
	taskstate.Fail(&task.Status, reason, flow, now)
	if err := r.Status().Update(ctx, task); err != nil {
		return err
	}
	r.announce(task, flow, flowv1alpha1.PhaseFailed, reason)
	// Announced before this runs, so a delete that fails here — and the
	// requeue that follows — never costs the task its metric or its Event.
	return r.reapCancelledJobs(ctx, task)
}

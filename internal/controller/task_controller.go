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

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/Tsuguya/taskflow/internal/runner"
	"github.com/Tsuguya/taskflow/internal/transition"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
)

// conditionReady is the single condition a task carries: whether the
// framework can go on with it.
const conditionReady = "Ready"

// brokenFlow says the definition is wrong rather than the work. It is carried
// as an error so that every path out of the reconcile goes through one place
// that writes Failed, instead of each caller remembering to.
type brokenFlow struct{ reason string }

func (e brokenFlow) Error() string { return e.reason }

// TaskReconciler reconciles a Task object
type TaskReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=flow.tgy.io,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=flow.tgy.io,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=flow.tgy.io,resources=tasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=flow.tgy.io,resources=taskflows;taskhandlers,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
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

	// A flow is always resolved in the task's own namespace. There is no field
	// naming another one, which is what reduces "may I start this flow" to
	// "may I create a Task here" — a question plain RBAC can answer.
	var flow flowv1alpha1.TaskFlow
	if err := r.Get(ctx, types.NamespacedName{Name: task.Spec.Flow, Namespace: task.Namespace}, &flow); err != nil {
		if apierrors.IsNotFound(err) {
			// The flow was deleted or never existed. Nothing to repair.
			return ctrl.Result{}, r.fail(ctx, &task, fmt.Sprintf("flow %q does not exist in this namespace", task.Spec.Flow))
		}
		return ctrl.Result{}, err
	}

	if task.Status.Phase == "" {
		return ctrl.Result{}, r.begin(ctx, &task, &flow)
	}
	if transition.IsTerminal(flow.Spec.Bindings, task.Status.Phase) {
		return ctrl.Result{}, nil
	}

	run := task.Status.CurrentRun
	if run == nil {
		// A non-terminal task with nothing in flight means the status was
		// written but the Job never got created — a crash between the two
		// writes. Pick it up rather than stalling.
		run = &flowv1alpha1.RunRef{Phase: task.Status.Phase, RunID: task.Status.RunID}
	}

	job, err := r.ensureJob(ctx, &task, &flow, run)
	if err != nil {
		var broken brokenFlow
		if errors.As(err, &broken) {
			return ctrl.Result{}, r.fail(ctx, &task, broken.reason)
		}
		return ctrl.Result{}, err
	}
	log.V(1).Info("run in flight", "phase", run.Phase, "runID", run.RunID, "job", job.Name)
	return ctrl.Result{}, nil
}

// begin puts a fresh task on the flow's starting phase.
func (r *TaskReconciler) begin(ctx context.Context, task *flowv1alpha1.Task, flow *flowv1alpha1.TaskFlow) error {
	if _, bound := flow.Spec.Bindings[flow.Spec.Start]; !bound {
		return r.fail(ctx, task, fmt.Sprintf("flow %q starts at %q, which nothing binds", flow.Name, flow.Spec.Start))
	}
	task.Status.Phase = flow.Spec.Start
	task.Status.RunID = 1
	task.Status.ReworkBudget = flow.Spec.ReworkBudget
	task.Status.CurrentRun = &flowv1alpha1.RunRef{Phase: flow.Spec.Start, RunID: 1}
	return r.Status().Update(ctx, task)
}

// fail stops a task whose flow is broken. Nothing is retried: the fault is in
// the definition rather than in the work, and guessing at a repair would hide
// it.
func (r *TaskReconciler) fail(ctx context.Context, task *flowv1alpha1.Task, reason string) error {
	if task.Status.Phase == flowv1alpha1.PhaseFailed {
		return nil
	}
	task.Status.Phase = flowv1alpha1.PhaseFailed
	task.Status.CurrentRun = nil
	meta.SetStatusCondition(&task.Status.Conditions, metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "FlowBroken",
		Message: reason,
	})
	return r.Status().Update(ctx, task)
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

	var existing batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: task.Namespace}, &existing)
	if err == nil {
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	binding, bound := flow.Spec.Bindings[run.Phase]
	if !bound {
		return nil, brokenFlow{fmt.Sprintf("phase %q has no binding in flow %q", run.Phase, flow.Name)}
	}

	var handler flowv1alpha1.TaskHandler
	if err := r.Get(ctx, types.NamespacedName{Name: binding.Handler, Namespace: task.Namespace}, &handler); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, brokenFlow{fmt.Sprintf("phase %q names handler %q, which does not exist", run.Phase, binding.Handler)}
		}
		return nil, err
	}

	job, err := runner.BuildJob(runner.Input{
		Task:      task,
		Handler:   &handler,
		Phase:     run.Phase,
		RunID:     run.RunID,
		PrevRunID: previousRun(task),
	})
	if err != nil {
		// A template that breaks an invariant is a definition problem, so it
		// fails rather than being retried.
		return nil, brokenFlow{err.Error()}
	}
	if err := r.Create(ctx, job); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Another reconcile got there first, which is what the
			// deterministic name is for.
			return job, r.Get(ctx, types.NamespacedName{Name: name, Namespace: task.Namespace}, job)
		}
		return nil, err
	}
	return job, nil
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

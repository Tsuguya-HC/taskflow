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
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"

	"github.com/Tsuguya-HC/taskflow/internal/collect"
	"github.com/Tsuguya-HC/taskflow/internal/metrics"
	"github.com/Tsuguya-HC/taskflow/internal/runner"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
	"github.com/Tsuguya-HC/taskflow/internal/transition"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
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
	// Recorder is where a task's ending is announced to whoever is watching
	// events rather than polling status.
	Recorder events.EventRecorder
	// Now is the clock deadlines are judged against; nil means the wall clock.
	Now func() time.Time
	// SidecarImage is what runs prepare and publish in every Job.
	SidecarImage string
}

// +kubebuilder:rbac:groups=flow.tgy.io,resources=tasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=flow.tgy.io,resources=tasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=flow.tgy.io,resources=tasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=flow.tgy.io,resources=taskflows;taskhandlers,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

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
		// the nil ttl fail() gets when there is no flow at all — and nothing
		// to read a cleanup run's declaration from either, which is why a
		// task owed one never gets it once its flow is gone.
		flow, err := r.resolveFlow(ctx, &task)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return ctrl.Result{}, nil
			}
			return ctrl.Result{}, err
		}
		return r.terminal(ctx, &task, flow)
	}

	// A flow is always resolved in the task's own namespace. There is no field
	// naming another one, which is what reduces "may I start this flow" to
	// "may I create a Task here" — a question plain RBAC can answer.
	flow, err := r.resolveFlow(ctx, &task)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// The flow was deleted or never existed. Nothing to repair.
			return ctrl.Result{}, r.fail(ctx, &task, nil, fmt.Sprintf("flow %q does not exist in this namespace", task.Spec.Flow))
		}
		return ctrl.Result{}, err
	}

	if task.Status.Phase == "" {
		return ctrl.Result{}, r.begin(ctx, &task, flow)
	}
	// A phase with no binding is terminal (§5 "束縛の無いステータスが終端") — but
	// which of three things happened is not the same call. CurrentRun tells
	// them apart: begin and Advance never set it to a phase without first
	// confirming a binding, so a ref naming anything but the cleanup run means
	// a run was in flight and the flow was edited out from under it. That is a
	// structural fault (§5 "実行時の矛盾は修復せず Failed"), not a quiet finish,
	// so it must not be indistinguishable from success. A ref naming the
	// cleanup run is the one legitimate way a stopped task still has one
	// (ADR-0009), and no ref at all means the task was terminal on arrival.
	if _, bound := flow.Spec.Bindings[task.Status.Phase]; !bound {
		if task.Status.CurrentRun != nil && !taskstate.InFinally(&task.Status) {
			return ctrl.Result{}, r.fail(ctx, &task, &flow.Spec, fmt.Sprintf(
				"phase %q lost its binding in flow %q while a run was in flight", task.Status.Phase, flow.Name))
		}
		// Same handling as the reserved-phase branch above, for a task that
		// stopped at a phase the flow itself leaves unbound. The flow is
		// already in hand here, so nothing extra needs fetching.
		return r.terminal(ctx, &task, flow)
	}

	run := task.Status.CurrentRun
	recovering := run == nil
	if recovering {
		// A non-terminal task with nothing in flight means the status was
		// written but the Job never got created — a crash between the two
		// writes. Pick it up rather than stalling.
		run = &flowv1alpha1.RunRef{Phase: task.Status.Phase, RunID: task.Status.RunID}
	}
	return r.driveRun(ctx, &task, flow, run, recovering)
}

// resolveFlow is the one route to a task's TaskFlow, and the two must not
// come apart: a caller that fetched one by any other means would get a flow
// whose endings were never primed, which is exactly the gap ADR-0010 closed.
// Folding the Get and the prime into one call is what makes a third call site
// safe by construction rather than by a comment repeated at each one.
//
// A NotFound error is returned as-is rather than interpreted here, because
// what it means differs by caller: the reserved-phase branch has nothing to
// repair, the main path fails the task. That decision stays where the two
// branches already were.
func (r *TaskReconciler) resolveFlow(ctx context.Context, task *flowv1alpha1.Task) (*flowv1alpha1.TaskFlow, error) {
	var flow flowv1alpha1.TaskFlow
	if err := r.Get(ctx, types.NamespacedName{Name: task.Spec.Flow, Namespace: task.Namespace}, &flow); err != nil {
		return nil, err
	}
	// Say what this flow's endings are before any of them happens (ADR-0010).
	primeFlowMetrics(&flow)
	return &flow, nil
}

// terminal is a task that has stopped. At most one thing is left to do: the
// cleanup run its flow declared, if the task is owed one and has not had it
// yet, and otherwise only the deletion date — which a task that was never owed
// a cleanup already has, so for most stopped tasks this reconcile writes
// nothing at all.
//
// Whether a cleanup is owed is read off the task, not off the flow: stop wrote
// the ref when the ending was decided, so a flow that says finally today
// reaches the tasks it starts tomorrow and leaves the ones that already
// stopped alone (ADR-0009 決定7).
func (r *TaskReconciler) terminal(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
) (ctrl.Result, error) {
	if taskstate.InFinally(&task.Status) {
		return r.driveRun(ctx, task, flow, task.Status.CurrentRun, false)
	}
	return ctrl.Result{}, r.backfillExpiry(ctx, task, &flow.Spec)
}

// driveRun takes the run in flight as far as this reconcile can: it makes sure
// the Job exists, then either waits, rules on a deadline, retries an attempt
// that never started, or reads the answer and settles.
//
// It is the same sequence for every run, the cleanup one included — a run is a
// Job with a vocabulary in front of it, and the framework has one way of
// watching that happen. What differs is only where the vocabulary came from
// and what settling means, and both of those are answered by the run's phase
// (runSpec, settleRun), not by a second copy of this loop.
//
// recovering says the ref was rebuilt here rather than read from status, so it
// must be persisted even if ensureJob found nothing new to add to it.
func (r *TaskReconciler) driveRun(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
	run *flowv1alpha1.RunRef,
	recovering bool,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	prior := run.DeepCopy()

	job, err := r.ensureJob(ctx, task, flow, run)
	if err != nil {
		var broken brokenFlow
		if errors.As(err, &broken) {
			return ctrl.Result{}, r.brokeDuringRun(ctx, task, flow, run, broken.reason)
		}
		return ctrl.Result{}, err
	}

	// ensureJob fills in run.JobName and run.Deadline; a fresh recovery run
	// had neither to begin with, and even an existing run's status object
	// might predate these fields. Persist them so a stuck run can be found by
	// name, and its deadline read, without recomputing either.
	if recovering || !equality.Semantic.DeepEqual(run, prior) {
		task.Status.CurrentRun = run
		if err := r.Status().Update(ctx, task); err != nil {
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
		return ctrl.Result{}, r.settleRun(ctx, task, flow, run, nil, timedOut(job))
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
		return ctrl.Result{}, r.settleRun(ctx, task, flow, run, nil, timedOut(job))
	case failure != "" && !collect.Ran(pods.Items):
		// The handler never got to run — nothing pulled, nothing scheduled.
		// That is the one kind of failure the controller retries on its own,
		// under the same runID: nothing was decided, so no run was spent
		// (ADR-0004). Whatever the attempt left behind is prepare's to clear.
		return ctrl.Result{}, r.retryInfra(ctx, task, flow, run, failure)
	}

	_, directories, ok := runSpec(&flow.Spec, run.Phase)
	if !ok {
		// What the run was started from is gone: a phase's binding, or
		// spec.finally, edited away while it ran. There is nothing left to read
		// its answer against, so it is settled as the fault it is rather than
		// judged against a vocabulary reconstructed here.
		return ctrl.Result{}, r.brokeDuringRun(ctx, task, flow, run, fmt.Sprintf(
			"flow %q no longer says what run %d of %q may answer with", flow.Name, run.RunID, run.Phase))
	}
	answer := collect.FromPods(pods.Items, directories)
	return ctrl.Result{}, r.settleRun(ctx, task, flow, run, &answer, "")
}

// runSpec says who fills a run and which directories it may answer with. Both
// come from where the run's phase came from: a phase the flow binds, or
// spec.finally for the cleanup run that follows the ending. It is the only
// place either is looked up, which is what keeps Finally — a name no binding
// may use — from being searched for among the bindings.
func runSpec(flow *flowv1alpha1.TaskFlowSpec, phase flowv1alpha1.Phase) (handler string, directories []string, ok bool) {
	if phase.IsFinally() {
		if flow.Finally == nil {
			return "", nil, false
		}
		return flow.Finally.Handler, []string{flow.Finally.Done}, true
	}
	binding, bound := flow.Bindings[phase]
	if !bound {
		return "", nil, false
	}
	return binding.Handler, transition.Directories(flow.Bindings, phase), true
}

// settleRun writes down a finished run. Which of the two ways depends on what
// the run was: a phase's run is settled by the flow's own table, and the
// cleanup run has no table to consult — it is recorded, and the ending it
// followed stays where it was.
func (r *TaskReconciler) settleRun(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
	run *flowv1alpha1.RunRef,
	answer *collect.Answer,
	noAnswer string,
) error {
	if run.Phase.IsFinally() {
		return r.settleFinally(ctx, task, flow, run, answer, noAnswer)
	}
	return r.settle(ctx, task, flow, run, answer, noAnswer)
}

// brokeDuringRun is what a broken definition does to the run that found it.
//
// For a phase's run the task is Failed: the fault is in the flow, no verdict
// from it can be trusted, and nothing is repaired (§5). For the cleanup run it
// is not, because the ending is already decided and a decided ending does not
// move (ADR-0009 決定2) — the same fault is recorded as a cleanup that did not
// happen, which is exactly what it is, and the task waits for a human with the
// reason on it.
func (r *TaskReconciler) brokeDuringRun(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
	run *flowv1alpha1.RunRef,
	reason string,
) error {
	if run.Phase.IsFinally() {
		return r.settleFinally(ctx, task, flow, run, nil, reason)
	}
	return r.fail(ctx, task, &flow.Spec, reason)
}

// deadlineGrace is how long past a run's deadline the controller waits for the
// Job to report the timeout itself before ruling on it.
const deadlineGrace = time.Minute

// actionFinishing is what the controller was doing when it recorded the
// event: the events API asks for the operation as well as the reason, and
// every event this controller emits comes from settling a finished run.
const actionFinishing = "Finishing"

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
	taskstate.Advance(&task.Status, &flow.Spec, in.Directory, res, now)
	if err := r.Status().Update(ctx, task); err != nil {
		return err
	}
	r.announce(task, &flow.Spec, res.Next, res.Detail)
	return nil
}

// The one ending no flow can prime: a task naming a flow that does not exist
// never resolves one to read endings from. fail() always lands on Failed, so
// that is the only pairing this case can produce, and it is fixed by the code
// rather than by anything in git — which is why it belongs here and not in
// primeFlowMetrics (ADR-0010).
//
// It is done as the package loads rather than when a reconciler is built, so
// that the series is there for the first scrape either way, and so that it is
// not a claim only a running manager makes.
func init() {
	metrics.PrimeOutcome(metrics.FlowUnresolved, string(flowv1alpha1.PhaseFailed), string(transition.EndingFailed))
}

// primeFlowMetrics reports every ending this flow declares at zero, so that
// the rare one reads as a rise when it finally happens rather than as a
// series appearing from nowhere (ADR-0010). The name is plural because it
// crosses two metrics, not one: TaskOutcomes always, and FinallyOutcomes when
// the flow declares a cleanup run (below) — unlike metrics.PrimeOutcome,
// which primes exactly the one series it is asked about.
//
// It runs on the way past rather than from a watch on TaskFlow, because what
// has to be true is only that the zero is there before a task of this flow
// stops — and a task is reconciled on its way to stopping, every time. A watch
// would learn the same thing no sooner, and nothing else about the controller
// needs to know the moment a TaskFlow appears.
//
// The endings of a flow nobody runs are never primed, which is the bargain
// this makes: it says what is true of the flows in use, not what is in git.
//
// A flow that declares finally has FinallyOutcomes' two cleanup outcomes
// primed alongside its own endings, for the identical reason: that counter is
// born at 1 the same way TaskOutcomes is. A flow with no finally can never
// produce either outcome, so priming them there would claim a cleanup that
// cannot happen.
func primeFlowMetrics(flow *flowv1alpha1.TaskFlow) {
	for _, ending := range transition.DeclaredEndings(&flow.Spec) {
		metrics.PrimeOutcome(flow.Name, string(ending.Phase), string(ending.Ending))
	}
	if flow.Spec.Finally != nil {
		metrics.PrimeFinallyOutcome(flow.Name, string(transition.OutcomeDeclared))
		metrics.PrimeFinallyOutcome(flow.Name, string(transition.OutcomeNoAnswer))
	}
}

// announce says what a task's ending means to the two audiences that do not
// read its status: whoever is watching metrics, and whoever is watching
// events. It is the one place every path that lands a task somewhere it stops
// runs through — a run settling on the flow's own table, or fail() stopping a
// task the definition itself broke — so a metric asking "how many tasks
// ended, and how" cannot go quiet just because the ending came from the
// framework's own fault rather than the flow's answer.
//
// It runs after the status write rather than before, so a write that fails
// and gets retried does not count the same ending twice. The cost of that
// ordering is a window, not a retry: a crash between the status write
// succeeding and this running loses that one Event and that one metric
// sample for good, since the next reconcile finds an already-terminal task
// and returns before reaching here again. That is accepted rather than
// closed — status is the record of truth, and Event/metric are signals, not
// a second copy of it.
//
// flow may be nil: fail() reaches Failed with no flow to read at all when the
// flow was deleted or never existed, and that is exactly the case a metric
// meant to surface a broken flow must not drop.
//
// The metric's flow label is task.Spec.Flow when flow resolved to a real
// TaskFlow, and metrics.FlowUnresolved when it did not. Only the unresolved
// case is masked: task.Spec.Flow is free text a task's own author chooses,
// and using the raw value there would let whoever can create Tasks grow this
// metric's cardinality without bound simply by naming a different
// nonexistent flow each time. Which name was missing is not lost by
// collapsing it — it is still in the task's status and in fail()'s reason —
// the metric only has to say how many tasks ended on a broken reference, not
// which one.
//
// Only a Failure ending gets an event. The framework's own two already show
// up as Ready=False with an outcome to read, and a Success or Undeclared
// ending is not news; a Failure is the one case where a task that finished
// perfectly normally is carrying bad news that nothing else would say out
// loud.
func (r *TaskReconciler) announce(
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlowSpec,
	phase flowv1alpha1.Phase,
	detail string,
) {
	ending := transition.EndingOf(flow, phase)
	if ending == transition.EndingRunning {
		return
	}
	flowLabel := metrics.FlowUnresolved
	if flow != nil {
		flowLabel = task.Spec.Flow
	}
	metrics.TaskOutcomes.With(prometheus.Labels{
		metrics.LabelFlow: flowLabel, metrics.LabelPhase: string(phase), metrics.LabelSeverity: string(ending),
	}).Inc()

	if ending != transition.EndingFailure || r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(task, nil, corev1.EventTypeWarning, taskstate.ReasonHandlerFailed, actionFinishing,
		"Task ended at %s, which flow %s declares a failure: %s", phase, task.Spec.Flow, detail)
}

// settleFinally records the cleanup run. Nothing moves: the task stopped
// before this run started, and where it stopped is not this run's to revise
// (ADR-0009 決定2). There is no transition to consult and no next run to start,
// so this is where the task is finished for good — the date it will be deleted
// on goes on here, and until now it had none.
//
// answer is nil when the run is being ruled on without being read — a timeout,
// an attempt that never started, a declaration that went missing — and
// noAnswer then says which. Either way "it did not say it was done" is one
// state, not several: the directory is empty and the reason carries the detail.
func (r *TaskReconciler) settleFinally(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
	run *flowv1alpha1.RunRef,
	answer *collect.Answer,
	noAnswer string,
) error {
	directory, outcome, detail := "", transition.OutcomeNoAnswer, noAnswer
	if answer != nil {
		detail = answer.Reason
		if answer.Directory != "" {
			directory, outcome = answer.Directory, transition.OutcomeDeclared
		}
	}

	logf.FromContext(ctx).Info("cleanup finished",
		"phase", task.Status.Phase, "runID", run.RunID, "directory", directory, "outcome", outcome)
	now := metav1.NewTime(r.now())
	taskstate.FinishFinally(&task.Status, &flow.Spec, directory, outcome, detail, now)
	if err := r.Status().Update(ctx, task); err != nil {
		return err
	}
	r.announceCleanup(task, outcome, detail)
	return nil
}

// announceCleanup says what the cleanup run came to, to the two audiences that
// do not read status. Both are told either way, because a count of cleanups
// that only rises when they fail cannot be read as a rate.
//
// It is a metric of its own and not another severity on the task's, for the
// reason the whole decision turns on: how a task ended and whether it was
// tidied up afterwards are two facts, and a task whose cleanup failed is not a
// task that ended badly. The Event is only for the failure — a cleanup that
// worked is not news, and nothing else would say out loud that one did not.
//
// Ordering and its cost are the same as announce's: after the status write, so
// a retried write cannot count twice, at the price of losing the signal (never
// the record) to a crash in between.
func (r *TaskReconciler) announceCleanup(task *flowv1alpha1.Task, outcome transition.Outcome, detail string) {
	metrics.FinallyOutcomes.With(prometheus.Labels{
		metrics.LabelFlow: task.Spec.Flow, metrics.LabelOutcome: string(outcome),
	}).Inc()

	if outcome == transition.OutcomeDeclared || r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(task, nil, corev1.EventTypeWarning, taskstate.ReasonFinallyFailed, actionFinishing,
		"Task ended at %s, but the cleanup run of flow %s did not report it cleaned up: %s",
		task.Status.Phase, task.Spec.Flow, detail)
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
	handlerName, _, ok := runSpec(&flow.Spec, run.Phase)
	if !ok {
		return r.brokeDuringRun(ctx, task, flow, run, fmt.Sprintf(
			"flow %q no longer says who fills run %d of %q", flow.Name, run.RunID, run.Phase))
	}
	handler, err := r.handlerFor(ctx, task, handlerName, run.Phase)
	if err != nil {
		var broken brokenFlow
		if errors.As(err, &broken) {
			return r.brokeDuringRun(ctx, task, flow, run, broken.reason)
		}
		return err
	}

	if taskstate.InfraRetriesExhausted(&task.Status, handler.Spec.MaxInfraRetries) {
		return r.settleRun(ctx, task, flow, run, nil, fmt.Sprintf(
			"the run never started (%s) and %d infrastructure retries were spent",
			failure, run.InfraRetries))
	}

	logf.FromContext(ctx).Info("retrying a run that never started",
		"phase", run.Phase, "runID", run.RunID, "failure", failure, "retries", run.InfraRetries)
	taskstate.RetryInfra(&task.Status)
	return r.Status().Update(ctx, task)
}

// handlerFor fetches the handler the flow named for this run. Its
// disappearing is a broken flow rather than a transient fault, reported
// through brokenFlow so every caller turns it into the same thing instead of
// each remembering the NotFound check itself.
func (r *TaskReconciler) handlerFor(
	ctx context.Context,
	task *flowv1alpha1.Task,
	name string,
	phase flowv1alpha1.Phase,
) (*flowv1alpha1.TaskHandler, error) {
	var handler flowv1alpha1.TaskHandler
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: task.Namespace}, &handler); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, brokenFlow{fmt.Sprintf("phase %q names handler %q, which does not exist", phase, name)}
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
		return r.fail(ctx, task, &flow.Spec, fmt.Sprintf("flow %q starts at %q, which nothing binds", flow.Name, flow.Spec.Start))
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
func (r *TaskReconciler) fail(ctx context.Context, task *flowv1alpha1.Task, flow *flowv1alpha1.TaskFlowSpec, reason string) error {
	// A dispatched task with nothing in flight has already reached a terminal
	// phase: Advance clears CurrentRun exactly when it lands one there, whether
	// that phase is Failed, Escalated, or one the flow itself declared
	// terminal. Leaving it alone here, not just for Failed specifically, is
	// what keeps a deleted or renamed flow from overwriting a finished task's
	// audit trail. A task waiting on its cleanup run has stopped just as
	// surely — the ending is already decided — so it is left alone too, and a
	// flow deleted while that run was owed cannot turn a task that finished
	// into one that failed.
	if task.Status.Phase != "" && (task.Status.CurrentRun == nil || taskstate.InFinally(&task.Status)) {
		return nil
	}
	taskstate.Fail(&task.Status, reason, flow, metav1.NewTime(r.now()))
	if err := r.Status().Update(ctx, task); err != nil {
		return err
	}
	r.announce(task, flow, flowv1alpha1.PhaseFailed, reason)
	return nil
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
//
// It does not call announce. The task ended before this reconcile, possibly
// long before it; counting it now would tell the metric and whoever reads the
// Event that a task ended today when it did not.
func (r *TaskReconciler) backfillExpiry(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlowSpec,
) error {
	before := task.Status.ExpiresAt
	taskstate.Expire(&task.Status, flow, metav1.NewTime(r.now()))
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
	name := runner.JobName(task.Name, run.Phase, run.RunID, run.InfraRetries)
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
		if !metav1.IsControlledBy(&existing, task) {
			return nil, notOwnedError("job", name, task, existing.OwnerReferences)
		}
		run.Deadline = deadlineOf(&existing)
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	// Reconcile only ever calls this with a phase it already confirmed is
	// bound — an unbound current phase is either a quiet finish or, with a
	// run in flight, a Failed of its own, and neither reaches here — or with
	// the cleanup run, which terminal reaches only for a task whose status
	// says one is owed. Either declaration can still have been edited away
	// between that check and this lookup, which is what !ok is.
	handlerName, directories, ok := runSpec(&flow.Spec, run.Phase)
	if !ok {
		return nil, brokenFlow{fmt.Sprintf(
			"flow %q no longer says who fills run %d of %q", flow.Name, run.RunID, run.Phase)}
	}

	handler, err := r.handlerFor(ctx, task, handlerName, run.Phase)
	if err != nil {
		return nil, err
	}

	workspacePVC, err := r.ensureWorkspacePVC(ctx, task, flow)
	if err != nil {
		return nil, err
	}

	job, err := runner.BuildJob(runner.Input{
		Task:         task,
		Handler:      handler,
		Phase:        run.Phase,
		RunID:        run.RunID,
		Attempt:      run.InfraRetries,
		PrevRunID:    previousRun(task),
		Directories:  directories,
		Ending:       endingFor(task, &flow.Spec, run),
		SidecarImage: r.SidecarImage,
		WorkspacePVC: workspacePVC,
		SweepRuns:    sweepRuns(run.RunID),
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
			if !metav1.IsControlledBy(&got, task) {
				return nil, notOwnedError("job", name, task, got.OwnerReferences)
			}
			run.Deadline = deadlineOf(&got)
			return &got, nil
		}
		return nil, err
	}
	run.Deadline = deadlineOf(job)
	return job, nil
}

// ensureWorkspacePVC creates the claim behind the flow's workspace, or
// returns the one already there; "" with no error means the flow declares no
// workspace and there is nothing to mount. Idempotent the same way ensureJob
// is — the name is deterministic, so a second reconcile collides instead of
// making a second claim, and whatever sits under the name has to prove it is
// this task's before being trusted. That check is IsControlledBy reading an
// ownerReference, and an ownerReference is a garbage-collection hint, not an
// authorization decision: its UID is whatever its author wrote, so it only
// keeps this task's claim safe from squatting if nothing but the controller
// can create PersistentVolumeClaims here (§ADR-0002) — the real backstop is
// RBAC, not this check.
//
// An Invalid on create is the flow's volumeClaimTemplate being unusable as
// written — a definition problem, so it goes through brokenFlow to Failed
// rather than being retried into the same rejection forever. StatefulSet
// left that surfacing to the moment the claim is made too, but with nothing
// watching, an apply that passed turned into pods that never came; here the
// task itself says so.
func (r *TaskReconciler) ensureWorkspacePVC(
	ctx context.Context,
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlow,
) (string, error) {
	if flow.Spec.Workspace == nil {
		return "", nil
	}
	vct := flow.Spec.Workspace.VolumeClaimTemplate
	if vct == nil {
		// The CRD defaults this at admission; nil means the stored flow
		// somehow predates or escaped the schema. Refusing beats guessing
		// at a claim the flow never wrote (P8).
		return "", brokenFlow{fmt.Sprintf("flow %q declares a workspace with no volumeClaimTemplate", flow.Name)}
	}

	pvc := runner.BuildWorkspacePVC(task, vct)
	adopt := func(existing *corev1.PersistentVolumeClaim) (string, error) {
		if !metav1.IsControlledBy(existing, task) {
			return "", notOwnedError("claim", pvc.Name, task, existing.OwnerReferences)
		}
		return pvc.Name, nil
	}

	var existing corev1.PersistentVolumeClaim
	err := r.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, &existing)
	if err == nil {
		return adopt(&existing)
	}
	if !apierrors.IsNotFound(err) {
		return "", err
	}
	if err := r.Create(ctx, pvc); err != nil {
		if apierrors.IsAlreadyExists(err) {
			var got corev1.PersistentVolumeClaim
			if err := r.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: pvc.Namespace}, &got); err != nil {
				return "", err
			}
			return adopt(&got)
		}
		if apierrors.IsInvalid(err) {
			return "", brokenFlow{fmt.Sprintf("flow %q has a volumeClaimTemplate the cluster refuses: %v", flow.Name, err)}
		}
		return "", err
	}
	return pvc.Name, nil
}

// sweepRuns is every run before this one — what prepare may clear out of
// work/. With runs strictly serial, a prior run is either sealed (its work
// directory already renamed onto the shelf, so there is nothing to remove)
// or abandoned. The day runs overlap, this is the one place that learns to
// subtract the live ones; prepare stays a program that deletes what it is
// told (ADR-0003).
func sweepRuns(current int32) []int32 {
	var ids []int32
	for id := int32(1); id < current; id++ {
		ids = append(ids, id)
	}
	return ids
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

// notOwnedError reports that something already sits under a deterministic
// name but was not put there by this task — the one error both idempotent
// create paths (Job, PersistentVolumeClaim) raise when their ownership check
// fails, so the wording does not drift between the two. kind names what
// sits under the name, for the message.
func notOwnedError(kind, name string, task *flowv1alpha1.Task, owners []metav1.OwnerReference) error {
	return fmt.Errorf("%s %q exists but is not owned by task %s (uid %s): owners = %s",
		kind, name, task.Name, task.UID, ownerSummary(owners))
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

// endingFor is what the cleanup run is told about the ending it follows, and
// nil for every other run — a phase's run has no ending yet to be told about.
//
// Meaning is read from flow as it stands at dispatch time, the same as the
// Job's template or image: it is not pinned to whatever flow looked like when
// the task reached its ending. What is pinned from that earlier moment —
// status.phase, the Ready condition's reason, the Event, the metric sample —
// was already written then and is not rewritten here or by anything that
// reads this. Which ttl the cleanup run earns follows the same rule in the
// other direction: taskstate.FinishFinally reads the Ready condition already
// recorded rather than re-deriving it from flow at settle time, for the same
// reason terminals here are not the last word on an ending already reached.
//
// The outcome is looked up by run number rather than read off the end of
// history, because the two differ exactly where it matters. A flow broken
// before its run could settle appends nothing, so history's last line is then
// some earlier run's verdict: true of that run, and not of this ending.
// Reporting nothing is the honest answer there (P8).
func endingFor(
	task *flowv1alpha1.Task,
	flow *flowv1alpha1.TaskFlowSpec,
	run *flowv1alpha1.RunRef,
) *runner.Ending {
	if !run.Phase.IsFinally() {
		return nil
	}
	return &runner.Ending{
		Meaning: string(transition.EndingOf(flow, task.Status.Phase)),
		Phase:   task.Status.Phase,
		Outcome: outcomeOf(task, run.RunID-1),
	}
}

// outcomeOf is what was recorded for one run of this task, and empty when that
// run left no record — it never settled, or never started at all.
func outcomeOf(task *flowv1alpha1.Task, runID int32) string {
	for _, h := range slices.Backward(task.Status.History) {
		if h.RunID == runID {
			return h.Outcome
		}
	}
	return ""
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

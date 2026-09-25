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

// Package taskstate records what happened. Where the transition package
// decides, this writes the decision down: the counters, the history, and the
// pointer to the run in flight.
//
// Like transition it is pure, so the bookkeeping that makes cycles terminate
// can be tested without a cluster.
//
// This package keeps one invariant on status.currentRuns: whenever it is set,
// it names the phase status.phase also names — with two exceptions. The
// cleanup run is named PhaseFinally while status.phase stays at the ending
// the task reached (ADR-0009). And a fork's branches (ADR-0013 決定8) are
// each named for themselves while status.phase stays at the fork they left:
// status.phase names where the task stands, not what is running, for as long
// as more than one thing is. Every writer here keeps the single-run half of
// the rule together — Begin and Advance set both at once, RetryInfra rebuilds
// the ref from the run in flight, and a task that has stopped has either no
// ref at all or the cleanup one — owed the moment stop writes it, before the
// Job behind it exists. Reconcile's recovery path, in task_controller.go, is
// the one writer outside this package, and holds the same rule: finding no
// run in flight, it rebuilds one from status.phase before persisting it —
// which is only safe once currentRuns is confirmed empty, since a fork with
// its branches in flight also has nothing Current can name. Nothing reads a
// flag to know any of this, which is why it is written down here.
//
// The controller leans on it twice over. It decides a task is terminal by
// looking up status.phase and then hands the run in flight to settle, so the two
// naming different phases would settle a run against the wrong binding — which
// is why the cleanup run, the one place they differ on purpose, is dispatched
// by InFinally rather than through that path at all, and never reaches
// transition. And it is the reason transition.Next's "phase has no binding"
// guard cannot be reached in production: Reconcile's check of status.phase is
// also a check of the run's phase for every run that goes through it. Break
// the invariant and that guard is what catches it — with less to say than
// Reconcile's own message, because Reconcile, looking only at status.phase,
// never saw the mismatch. Pinned by TestCurrentRunNamesTheCurrentPhase in this
// package.
package taskstate

import (
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// ConditionReady is the single condition a task carries: whether the
// framework can go on with it.
const ConditionReady = "Ready"

// ReasonHandlerFailed is why Ready goes false for a task that ran to one of
// its flow's Failure endings. The outcome of that move is Declared — the
// handler followed an edge the flow wrote down, and nothing went wrong with
// the machinery — so the outcome is not the thing to put in front of a human
// here. What they need to know is that the answer was bad news.
const ReasonHandlerFailed = "HandlerFailed"

// ReasonFinallyFailed is why Ready goes false for a task whose cleanup run
// never said it was done. It is deliberately not one of the ending's own
// reasons: the work reached whatever conclusion it reached, and this says
// something else was left behind.
const ReasonFinallyFailed = "FinallyFailed"

// Current is the run a task has in flight, or nil when it has none or more
// than one — the same rule legacyMirror keeps for status.currentRun, so the
// two never disagree on when there is a single run to point at. A fork's
// branches (ADR-0013) are the one time there is more than one: nil here does
// not mean nothing is running, only that Current cannot say which. Whether a
// task has no run in flight at all is Idle's question, not this one — a
// caller that means "no run" checks Idle before calling Current, since nil
// here reads that way whether currentRuns is empty or holds a fork's
// branches, and falling back to a recovery path is only correct in the first
// of those.
//
// Until forks run, a task never has more than one, and Current is how every
// caller that means "the run" says so rather than indexing the list. The
// pointer is into status itself, so a caller filling in the run's Job or box
// writes it where the next status update will carry it.
func Current(status *flowv1alpha1.TaskStatus) *flowv1alpha1.RunRef {
	if len(status.CurrentRuns) != 1 {
		return nil
	}
	return &status.CurrentRuns[0]
}

// Idle reports whether a task has no run in flight at all — the question
// Current cannot answer once forks run (ADR-0013), since its nil also covers
// a fork's branches in flight. Idle is what a caller checks before treating
// nil the way this package's history predates forks and treated it: as
// nothing running, and so safe to rebuild a run for or otherwise recover
// from.
func Idle(status *flowv1alpha1.TaskStatus) bool {
	return len(status.CurrentRuns) == 0
}

// SetCurrent makes run the one run in flight, or leaves none when run is nil.
// It also keeps status.currentRun in step, as the one run in flight seen the
// way a controller before ADR-0013 reads it — this is the expand half of the
// migration, and the reason a rollback to that controller keeps working: it
// never reads currentRuns at all, only this mirror. The field goes back to
// nil the moment there is more than one run in flight (ADR-0013 決定7, PR4):
// that shape has no single-run field to hold it in, and a rolled-back
// controller finding none is the same as it finding a task that has not
// started an attempt yet, which is the closer of the two wrong answers.
func SetCurrent(status *flowv1alpha1.TaskStatus, run *flowv1alpha1.RunRef) {
	if run == nil {
		status.CurrentRuns = nil
	} else {
		status.CurrentRuns = []flowv1alpha1.RunRef{*run}
	}
	status.CurrentRun = legacyMirror(status)
}

// legacyMirror is what status.currentRun should hold to mirror currentRuns —
// a copy of the one run in flight, or nil when there is none or more than
// one. Its own object, not a pointer into currentRuns: the two fields are
// meant to be read independently by two controller versions that may run at
// different times, and never through each other.
func legacyMirror(status *flowv1alpha1.TaskStatus) *flowv1alpha1.RunRef {
	if len(status.CurrentRuns) != 1 {
		return nil
	}
	run := status.CurrentRuns[0]
	return &run
}

// AdoptLegacyRun brings status.currentRun and status.currentRuns back into
// the mirrored shape SetCurrent writes going forward, and reports whether it
// changed anything. It exists for a task a controller before this one wrote
// to, or wrote to again after this one did: that controller only ever
// touches the old field, so a disagreement between the two means the old
// field is the newer write and currentRuns is what has fallen behind — this
// controller itself never leaves the two disagreeing, since SetCurrent
// always writes both at once. Reading currentRuns empty and the old field
// set — a task mid-run across the upgrade, never yet written by this
// controller — is one instance of that same disagreement, and adopted the
// same way: currentRuns rebuilt from the old field.
//
// Two runs or more in flight (ADR-0013 決定7, PR4) has no old-field shape to
// read the other way: currentRuns is authoritative there instead, and the
// old field, if a stale write left it set, is cleared to match — this
// controller's own eventual write of a second branch would otherwise look
// like a third writer overwriting fresh currentRuns with a stale ref.
//
// Two fields already agreeing is reported as no change, so a controller
// upgraded for a while, with every write since going through SetCurrent,
// does not pay for this on every reconcile.
//
// The old field is not removed here, nor by any other writer in this
// package: expand keeps writing it so a rollback to the controller before
// this one keeps working, and contract — retiring the field once a version
// exists that no longer needs a single-run shape to fall back to (ADR-0013
// 決定7, PR4) — is the boundary a rollback cannot cross.
func AdoptLegacyRun(status *flowv1alpha1.TaskStatus) bool {
	if len(status.CurrentRuns) >= 2 {
		if status.CurrentRun == nil {
			return false
		}
		status.CurrentRun = nil
		return true
	}
	if equality.Semantic.DeepEqual(status.CurrentRun, legacyMirror(status)) {
		return false
	}
	if status.CurrentRun == nil {
		status.CurrentRuns = nil
	} else {
		status.CurrentRuns = []flowv1alpha1.RunRef{*status.CurrentRun}
	}
	return true
}

// InFinally reports whether the run in flight is the cleanup one — the single
// case where a task that has stopped still has a run in flight. Callers ask this
// instead of comparing phases themselves, because "a stopped task has no run"
// is load-bearing in several places and each of them needs the same exception.
func InFinally(status *flowv1alpha1.TaskStatus) bool {
	run := Current(status)
	return run != nil && run.Phase.IsFinally()
}

// needsAHuman reports whether an ending is one somebody has to come and look
// at. Three of the five are: the framework's own two, and the endings a flow
// declared to be Failure. It is one predicate rather than two because the
// two things it decides — whether Ready goes false, and which of the two ttls
// dates the cleanup — are the same question asked twice, and a task whose
// condition says "come and look" while its ttl deletes it within the hour
// would be answering it both ways.
func needsAHuman(e transition.Ending) bool {
	switch e {
	case transition.EndingEscalated, transition.EndingFailed, transition.EndingFailure:
		return true
	default:
		return false
	}
}

// runnerOf says how the run being recorded was driven, read off the run
// rather than passed in: a run with a place for an answer is one the
// framework did not start (ADR-0011). RunRef.Runner is the one place that
// rule lives — the controller's own runnerOf reads the same method — and
// what lets it be read here at all is the invariant this package keeps —
// the run in status.currentRuns is the one the history line being written is about.
//
// No ref at all, or one Runner cannot yet tell apart, reads as Job — this
// package's own answer for a run about to start, unlike the controller's,
// which reads the handler instead. That is not a claim that Job was ever the
// only kind a run without one could have had: ADR-0011 決定2〜6 already let a
// build drive and settle a State run before 決定7 added this field, so this
// same default also reads an already-settled State run's now-empty history
// line as Job, wrongly, with no way left to tell the two apart
// (HistoryEntry.Runner's own doc carries the same caveat).
func runnerOf(run *flowv1alpha1.RunRef) flowv1alpha1.RunnerType {
	if run != nil {
		if kind := run.Runner(); kind != "" {
			return kind
		}
	}
	return flowv1alpha1.RunnerJob
}

// Runs is how many times each phase of this task has run, derived from
// history rather than stored beside it. Two records of the same fact drift;
// this one cannot disagree with the history a human reads.
func Runs(status *flowv1alpha1.TaskStatus, bindings map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding) map[flowv1alpha1.Phase]int32 {
	runs := make(map[flowv1alpha1.Phase]int32, len(status.History)+len(status.CurrentRuns))
	for _, h := range status.History {
		runs[h.Phase]++
	}
	// A run in flight has run even though it has not been recorded yet,
	// which is what makes a self-loop count as a rework. The runs in flight
	// are counted rather than status.phase, because at a fork the two part:
	// the task stands at the fork while its branches are what run (ADR-0013).
	// With nothing in flight — a status written before its run was, the gap
	// Reconcile's recovery closes — status.phase is the run about to start.
	inFlight := false
	for _, r := range status.CurrentRuns {
		if !r.Phase.IsFinally() {
			runs[r.Phase]++
			inFlight = true
		}
	}
	if !inFlight && status.Phase != "" && !transition.IsTerminal(bindings, status.Phase) {
		runs[status.Phase]++
	}
	return runs
}

// clampReason keeps a Reason within HistoryEntry.Reason's own CRD limit
// (flowv1alpha1.HistoryReasonMaxLength) before Advance or FinishFinally ever
// writes one. This is the one place that limit is actually enforced: what
// reaches here can be a transition's own detail, a controller error folded in
// (ensureVerdictBox's notOwnedError, unbounded in the number of owners it
// lists), or free text a handler wrote — none of it bounded upstream to a
// number that agrees with the CRD's, and a write that walks over it is
// refused for good rather than merely truncated (Round 2's collect.reasonf
// already clamps its own two producers; this is the backstop that also
// covers everything built here in taskstate). Truncated text says so, with an
// ellipsis, so a human reading a Reason that stops mid-sentence knows it was
// cut rather than mistaking it for the whole story.
func clampReason(s string) string {
	const ellipsis = "…"
	runes := []rune(s)
	if len(runes) <= flowv1alpha1.HistoryReasonMaxLength {
		return s
	}
	cut := max(flowv1alpha1.HistoryReasonMaxLength-len([]rune(ellipsis)), 0)
	return string(runes[:cut]) + ellipsis
}

// Advance records the completed run and moves the task to res.Next.
//
// runID rises once for every run that settles, reworks included — it names
// the run's directory and its child objects, and an infrastructure retry,
// which settles nothing, leaves it alone (ADR-0004). What bounds a
// cycle is not written here: it is the history this appends to, counted per
// phase by Runs the next time the task moves.
//
// The whole flow spec is passed rather than the two or three fields this
// needs. They are read together at one instant — the edges say where the
// task goes, terminals say what arriving there means, and ttl dates the
// cleanup — and a caller threading them as loose arguments is a caller who
// can update two and forget the third.
func Advance(
	status *flowv1alpha1.TaskStatus,
	flow *flowv1alpha1.TaskFlowSpec,
	directory string,
	res transition.Result,
	now metav1.Time,
) {
	run := Current(status)
	if run == nil {
		// Only Reconcile's recovery ever settles a run it has not written
		// down yet, and that run is by construction the one status.phase
		// names; recording it under that name is what Advance always did.
		run = &flowv1alpha1.RunRef{Phase: status.Phase, RunID: status.RunID}
	}
	record(status, run, directory, res.Outcome, res.Detail, now)
	move(status, flow, res, now)
}

// record appends run's line to history: what it answered and why the task
// moved as it did. The run is named rather than read off status because a
// fork's branches are several runs at once, and each is recorded as it
// settles (ADR-0013).
func record(
	status *flowv1alpha1.TaskStatus,
	run *flowv1alpha1.RunRef,
	directory string,
	outcome transition.Outcome,
	detail string,
	now metav1.Time,
) {
	status.History = append(status.History, flowv1alpha1.HistoryEntry{
		Phase:      run.Phase,
		RunID:      run.RunID,
		Directory:  directory,
		Outcome:    string(outcome),
		Runner:     runnerOf(run),
		Reason:     clampReason(detail),
		FinishedAt: &now,
	})
}

// move takes the task to res.Next: the next phase's run, or the ending and
// everything an ending says.
func move(status *flowv1alpha1.TaskStatus, flow *flowv1alpha1.TaskFlowSpec, res transition.Result, now metav1.Time) {
	status.Phase = res.Next

	// Three endings need a human: the framework's own two, and an ending the
	// flow declared to be Failure. The first two mean nothing moves forward
	// until somebody looks; the third means the task finished and the news
	// is bad. The history entry says so too, but a condition is where
	// kubectl and anything watching for stuck tasks look first.
	ending := transition.EndingOf(flow, res.Next)
	if needsAHuman(ending) {
		// For the reserved two, res.Outcome is exactly the distinction worth
		// surfacing — NoAnswer from a run that said nothing, Declined from
		// one that said it would not decide.
		reason := string(res.Outcome)
		if ending == transition.EndingFailure {
			reason = ReasonHandlerFailed
		}
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:    ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  reason,
			Message: res.Detail,
		})
	}

	if ending != transition.EndingRunning {
		stop(status, flow, now)
		return
	}
	status.RunID++
	SetCurrent(status, &flowv1alpha1.RunRef{Phase: res.Next, RunID: status.RunID})
}

// stop is what becomes of a task that has reached its ending. Either the flow
// declared a cleanup run and that run takes it from here, or nothing more will
// happen to it and the deletion date can be written now. Advance and Fail
// share this so the two cannot answer the question differently — a task that
// broke its flow on the way out is owed the same cleanup as one that finished.
//
// Without a cleanup run: nothing is in flight, so the ref goes. Leaving a stale
// one would let a late answer look like it belonged to something, and bumping
// runID would leave status.runID naming a run that never happened.
//
// With one: runID does move, because a run really is about to happen, and the
// ref names it. Recording it there is what makes "this task is owed a cleanup"
// a fact about the task rather than a re-reading of its flow — a task that
// stopped before the flow ever said finally has no such ref, and so is never
// handed one afterwards (ADR-0009 決定7). The date waits for that run to
// settle: Expire never moves a date once written, so writing one here would be
// writing the only one, and a task could be deleted out from under its own
// cleanup.
func stop(status *flowv1alpha1.TaskStatus, flow *flowv1alpha1.TaskFlowSpec, now metav1.Time) {
	if flow != nil && flow.Finally != nil {
		status.RunID++
		SetCurrent(status, &flowv1alpha1.RunRef{Phase: flowv1alpha1.PhaseFinally, RunID: status.RunID})
		return
	}
	SetCurrent(status, nil)
	Expire(status, flow, now)
}

// FinishFinally records the cleanup run and closes the task for good.
//
// directory is the one the run wrote, and empty means it never said it was
// done: nothing written, a run cut short, an infrastructure allowance spent, a
// handler that could not be resolved. None of that touches the ending the task
// reached (ADR-0009 決定2) — status.phase stays put, and the severity whoever
// watches metrics already recorded stays true. What a cleanup that did not
// happen changes is who hears about it: Ready goes false with a reason of its
// own, and the task waits out ttl.failed rather than being swept away within
// the hour with the mess still there.
func FinishFinally(
	status *flowv1alpha1.TaskStatus,
	flow *flowv1alpha1.TaskFlowSpec,
	directory string,
	outcome transition.Outcome,
	detail string,
	now metav1.Time,
) {
	// Read before anything below writes to it. Advance and Fail are the only
	// two writers of Ready before this run starts, and each set it false
	// exactly when the ending it recorded needed a human — so this is that
	// answer, already given, rather than a second way of asking the same
	// question. terminals is not: a flow can be edited while its task is
	// still alive (design.md §5, issue #19), and a cleanup run can take
	// minutes to settle, so re-reading terminals here could disagree with
	// the Ready condition Advance or Fail already wrote for this same
	// ending — the exact split needsAHuman exists to rule out.
	endingNeededAHuman := meta.IsStatusConditionFalse(status.Conditions, ConditionReady)

	status.History = append(status.History, flowv1alpha1.HistoryEntry{
		Phase:      flowv1alpha1.PhaseFinally,
		RunID:      status.RunID,
		Directory:  directory,
		Outcome:    string(outcome),
		Runner:     runnerOf(Current(status)),
		Reason:     clampReason(detail),
		FinishedAt: &now,
	})
	SetCurrent(status, nil)

	done := directory != ""
	if !done {
		meta.SetStatusCondition(&status.Conditions, metav1.Condition{
			Type:    ConditionReady,
			Status:  metav1.ConditionFalse,
			Reason:  ReasonFinallyFailed,
			Message: detail,
		})
	}
	// The ending's own answer to "does somebody have to come and look" is still
	// true — a task that escalated is no less escalated for having been tidied
	// up after — so both reasons for the longer ttl are taken together.
	stamp(status, flow, !done || endingNeededAHuman, now)
}

// Expire stamps the deletion time of a task that has stopped, and does
// nothing to one that has not.
//
// Which of the two durations applies follows one rule: an ending somebody
// has to come and look at keeps the task around for ttl.failed, and every
// other ending takes ttl.succeeded. That covers the framework's own two
// however they were reached — including an Escalated edge the flow declared
// with next — and an ending the flow itself marked Failure, which is a task
// that finished with bad news and would otherwise be swept away in an hour
// while nobody was looking. A nil flow, a nil ttl or a nil duration leaves
// the task to be cleaned up by hand.
//
// A date already stamped is never moved: Expire is called both the instant
// a task lands on a terminal phase and, for one that landed there before
// this existed, on a later reconcile that only means to backfill what that
// first call missed. Idempotence here is what lets the second kind of call
// be unconditional rather than needing its own "already has one" guard.
//
// A task whose flow declares a cleanup run is not dated here at all — stop
// hands it to that run instead, and FinishFinally writes the date once the run
// has settled.
func Expire(
	status *flowv1alpha1.TaskStatus,
	flow *flowv1alpha1.TaskFlowSpec,
	now metav1.Time,
) {
	ending := transition.EndingOf(flow, status.Phase)
	if ending == transition.EndingRunning {
		return
	}
	stamp(status, flow, needsAHuman(ending), now)
}

// stamp writes the deletion date, choosing between the flow's two durations.
// Expire picks needsHuman from the ending alone; FinishFinally has a second
// reason to pick the longer one, which is why the choice is a parameter here
// rather than something this works out for itself.
func stamp(
	status *flowv1alpha1.TaskStatus,
	flow *flowv1alpha1.TaskFlowSpec,
	needsHuman bool,
	now metav1.Time,
) {
	if status.ExpiresAt != nil || flow == nil || flow.TTL == nil {
		return
	}
	d := flow.TTL.Succeeded
	if needsHuman {
		d = flow.TTL.Failed
	}
	if d == nil {
		return
	}
	t := metav1.NewTime(now.Add(d.Duration))
	status.ExpiresAt = &t
}

// Begin puts a fresh task on the flow's starting phase.
func Begin(status *flowv1alpha1.TaskStatus, start flowv1alpha1.Phase) {
	status.Phase = start
	status.RunID = 1
	SetCurrent(status, &flowv1alpha1.RunRef{Phase: start, RunID: 1})
}

// Fail stops a task whose flow is broken. Nothing is retried: the fault is in
// the definition rather than in the work, and guessing at a repair would hide
// it.
//
// flow is the one the task names, or nil when the fault is that there is no
// flow to read a ttl from; Expire then leaves the task alone. That is a
// wait, not a dead end: the controller keeps looking for a flow of that name
// on every later reconcile and backfills the date once one appears.
func Fail(status *flowv1alpha1.TaskStatus, reason string, flow *flowv1alpha1.TaskFlowSpec, now metav1.Time) {
	status.Phase = flowv1alpha1.PhaseFailed
	meta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:    ConditionReady,
		Status:  metav1.ConditionFalse,
		Reason:  "FlowBroken",
		Message: reason,
	})
	// Failed is reserved, so it is terminal and needs a human on its own
	// say-so; Expire reaches that without consulting the flow's bindings or
	// terminals, which is what makes a nil flow here mean only "no ttl to
	// read" rather than "cannot tell what this ending was". A flow this broken
	// can still declare a cleanup run, and stop hands the task to it: the
	// definition being wrong is no reason to leave whatever it already made
	// lying around. No flow at all means no cleanup either — there is nothing
	// to read the declaration from.
	stop(status, flow, now)
}

// RetryInfra prepares another attempt at the same phase after a failure that
// was not the handler's judgement — an image that would not pull, an evicted
// pod. It costs no budget, because nothing was decided, and no runID, because
// nothing was run.
//
// The number stays put deliberately. A runID counts the runs a task has
// decided its way through, not the attempts it took to get one started, and
// keeping the two apart is what makes results/ line up with history: one
// sealed directory per run, numbered the way the phases went. A number spent
// on an attempt that never ran would leave a hole in that shelf, and a hole
// is only readable by whoever already knows it is there.
//
// Reusing it also puts the retry back on the same directory, which is where
// the last attempt's leftovers would be. That is MakeRun's to clear, and its
// removal is fail-closed: a directory some zombie still holds open will not
// go, and the retry stops rather than starting work beside a live writer.
// Under the old numbering that zombie was invisible — the retry simply
// started somewhere else and nothing said the volume was contested.
//
// Nothing is appended to history: no verdict was reached, and a history of
// non-events makes the record harder to read, not easier.
// The phase comes from the run being retried rather than from status.phase.
// For every run but one they are the same name; the cleanup run is the
// exception, and reading status.phase there would restart it as the ending the
// task stopped at.
func RetryInfra(status *flowv1alpha1.TaskStatus) {
	phase, retries := status.Phase, int32(0)
	if run := Current(status); run != nil {
		phase = run.Phase
		retries = run.InfraRetries + 1
	}
	SetCurrent(status, &flowv1alpha1.RunRef{
		Phase:        phase,
		RunID:        status.RunID,
		InfraRetries: retries,
	})
}

// InfraRetriesExhausted reports whether another infrastructure retry is
// allowed. When it is not, the task escalates rather than failing: something
// outside the handler kept it from running, and that is for a human to look
// at.
func InfraRetriesExhausted(status *flowv1alpha1.TaskStatus, max int32) bool {
	run := Current(status)
	if run == nil {
		return false
	}
	return run.InfraRetries >= max
}

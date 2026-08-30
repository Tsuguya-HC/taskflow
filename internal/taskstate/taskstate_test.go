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

package taskstate

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	flowv1alpha1 "github.com/Tsuguya/taskflow/api/v1alpha1"
	"github.com/Tsuguya/taskflow/internal/transition"
)

var at = metav1.Time{}

const (
	phaseInvestigate flowv1alpha1.Phase = "調査"
	phaseReport      flowv1alpha1.Phase = "報告"
	phaseDone        flowv1alpha1.Phase = "おわり"
)

// The directories the example flow declares.
const (
	dirOK   = "ok"
	dirMore = "more"
	dirSent = "sent"
)

func flow() map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding {
	return map[flowv1alpha1.Phase]flowv1alpha1.PhaseBinding{
		phaseInvestigate: {Handler: "cnp-reader", Next: map[flowv1alpha1.Phase]string{phaseReport: dirOK, phaseInvestigate: dirMore}},
		phaseReport:      {Handler: "discord", Next: map[flowv1alpha1.Phase]string{phaseDone: dirSent}},
	}
}

func TestVisitedComesFromHistory(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{
		Phase: phaseReport,
		History: []flowv1alpha1.HistoryEntry{
			{Phase: phaseInvestigate, RunID: 1, Directory: dirOK},
		},
	}
	got := Visited(s, flow())
	if !got[phaseInvestigate] {
		t.Fatal("a phase in history must count as visited")
	}
	if !got[phaseReport] {
		t.Fatal("the phase in flight has run, so a self-loop back to it is a rework")
	}
	if got[flowv1alpha1.Phase("見たことない")] {
		t.Fatal("a phase never run must not count as visited")
	}
}

// A status the flow stops at has no handler, so the task never ran there.
func TestVisitedIgnoresWhereItStopped(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{Phase: phaseDone}
	if Visited(s, flow())[phaseDone] {
		t.Fatal("nothing runs at a status with no binding")
	}
	esc := &flowv1alpha1.TaskStatus{Phase: flowv1alpha1.PhaseEscalated}
	if Visited(esc, flow())[flowv1alpha1.PhaseEscalated] {
		t.Fatal("nothing runs at Escalated either")
	}
}

// TaskStatus.Phase is +optional: a task that has not been dispatched yet has
// none set. Visited must not treat that as a phase named "" having run.
func TestVisitedBeforeFirstDispatch(t *testing.T) {
	got := Visited(&flowv1alpha1.TaskStatus{}, flow())
	if len(got) != 0 {
		t.Fatalf("visited = %v, want empty before any run", got)
	}
}

func TestAdvanceRecordsAndMoves(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{
		Phase:        phaseReport,
		RunID:        2,
		ReworkBudget: 2,
		CurrentRun:   &flowv1alpha1.RunRef{Phase: phaseReport, RunID: 2},
	}
	res := transition.Result{Next: phaseInvestigate, Outcome: transition.OutcomeRework, Budget: 1}
	Advance(s, flow(), dirMore, res, "s3://bucket/task/2/", nil, at)

	if len(s.History) != 1 {
		t.Fatalf("history has %d entries, want 1", len(s.History))
	}
	h := s.History[0]
	if h.Phase != phaseReport || h.RunID != 2 || h.Directory != dirMore {
		t.Fatalf("history recorded %+v, want the run that just finished", h)
	}
	if h.Ref != "s3://bucket/task/2/" {
		t.Fatalf("ref = %q, want the store location", h.Ref)
	}
	if s.Phase != phaseInvestigate {
		t.Fatalf("phase = %q, want Planning", s.Phase)
	}
	if s.RunID != 3 {
		t.Fatalf("runID = %d, want 3", s.RunID)
	}
	if s.ReworkBudget != 1 {
		t.Fatalf("budget = %d, want 1", s.ReworkBudget)
	}
	if s.CurrentRun == nil || s.CurrentRun.RunID != 3 || s.CurrentRun.Phase != phaseInvestigate {
		t.Fatalf("currentRun = %+v, want Planning at run 3", s.CurrentRun)
	}
}

func TestAdvanceToTerminalClearsCurrentRun(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{
		Phase:      phaseReport,
		RunID:      1,
		CurrentRun: &flowv1alpha1.RunRef{Phase: phaseReport, RunID: 1},
	}
	Advance(s, flow(), dirSent, transition.Result{Next: phaseDone, Outcome: transition.OutcomeDeclared}, "", nil, at)
	if s.CurrentRun != nil {
		t.Fatal("a finished task has nothing in flight; a stale currentRun would make a late verdict look owned")
	}
	if s.RunID != 1 {
		t.Fatalf("runID = %d, want 1 — a terminal move starts no run, so \"current or last run\" must still name the one that happened", s.RunID)
	}
}

// A structural failure reached through Advance (not Fail) must set the same
// Ready=False condition Fail sets directly, or a watcher keying off the
// condition misses tasks that stopped this way.
func TestAdvanceToFailedSetsReadyCondition(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{
		Phase:      phaseReport,
		RunID:      1,
		CurrentRun: &flowv1alpha1.RunRef{Phase: phaseReport, RunID: 1},
	}
	res := transition.Result{
		Next:    flowv1alpha1.PhaseFailed,
		Outcome: transition.OutcomeStructural,
		Detail:  "directory ok selects more than one status",
	}
	Advance(s, flow(), dirOK, res, "", nil, at)

	if s.Phase != flowv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", s.Phase)
	}
	cond := meta.FindStatusCondition(s.Conditions, ConditionReady)
	if cond == nil {
		t.Fatal("no Ready condition was set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("Ready condition status = %q, want False", cond.Status)
	}
	if cond.Reason != string(transition.OutcomeStructural) {
		t.Fatalf("Ready condition reason = %q, want %q", cond.Reason, transition.OutcomeStructural)
	}
	if cond.Message != res.Detail {
		t.Fatalf("Ready condition message = %q, want %q", cond.Message, res.Detail)
	}
}

func TestBeginPutsAFreshTaskOnTheStartPhase(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{}
	Begin(s, phaseInvestigate, 2)

	if s.Phase != phaseInvestigate {
		t.Fatalf("phase = %q, want %q", s.Phase, phaseInvestigate)
	}
	if s.RunID != 1 {
		t.Fatalf("runID = %d, want 1", s.RunID)
	}
	if s.ReworkBudget != 2 {
		t.Fatalf("reworkBudget = %d, want 2", s.ReworkBudget)
	}
	if s.CurrentRun == nil || s.CurrentRun.Phase != phaseInvestigate || s.CurrentRun.RunID != 1 {
		t.Fatalf("currentRun = %+v, want %q at run 1", s.CurrentRun, phaseInvestigate)
	}
}

func TestFailStopsATaskAndRecordsWhy(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{
		Phase:      phaseReport,
		RunID:      3,
		CurrentRun: &flowv1alpha1.RunRef{Phase: phaseReport, RunID: 3},
	}
	Fail(s, "flow \"cnp-check\" does not exist in this namespace", nil, at)

	if s.Phase != flowv1alpha1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", s.Phase)
	}
	if s.CurrentRun != nil {
		t.Fatal("a failed task has nothing in flight")
	}
	cond := meta.FindStatusCondition(s.Conditions, ConditionReady)
	if cond == nil {
		t.Fatal("no Ready condition was set")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Fatalf("Ready condition status = %q, want False", cond.Status)
	}
	if cond.Reason != "FlowBroken" {
		t.Fatalf("Ready condition reason = %q, want FlowBroken", cond.Reason)
	}
	if cond.Message != "flow \"cnp-check\" does not exist in this namespace" {
		t.Fatalf("Ready condition message = %q", cond.Message)
	}
}

func TestInfraRetryCostsARunButNoBudget(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{
		Phase:        phaseReport,
		RunID:        4,
		ReworkBudget: 1,
		CurrentRun:   &flowv1alpha1.RunRef{Phase: phaseReport, RunID: 4},
	}
	RetryInfra(s)

	if s.RunID != 5 || s.CurrentRun.RunID != 5 {
		t.Fatalf("runID = %d, want 5 so the retry cannot read the last attempt's out/", s.RunID)
	}
	if s.ReworkBudget != 1 {
		t.Fatalf("budget = %d, want 1 — nothing was judged", s.ReworkBudget)
	}
	if s.Phase != phaseReport {
		t.Fatalf("phase = %q, want to stay on Review", s.Phase)
	}
	if len(s.History) != 0 {
		t.Fatal("no verdict was reached, so nothing belongs in history")
	}
	if s.CurrentRun.InfraRetries != 1 {
		t.Fatalf("infraRetries = %d, want 1", s.CurrentRun.InfraRetries)
	}
}

func TestInfraRetriesExhausted(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{CurrentRun: &flowv1alpha1.RunRef{InfraRetries: 2}}
	if !InfraRetriesExhausted(s, 2) {
		t.Fatal("two retries against a maximum of two is exhausted")
	}
	if InfraRetriesExhausted(s, 3) {
		t.Fatal("two retries against a maximum of three is not")
	}
	if InfraRetriesExhausted(&flowv1alpha1.TaskStatus{}, 0) {
		t.Fatal("a task that has not started has not exhausted anything")
	}
}

// The counters exist to bound a cycle, so drive one: review keeps sending
// work back and the budget keeps falling until the task lands on a human.
// runID rises the whole way, which is what keeps each attempt's artifacts
// separate.
func TestCountersBoundACycle(t *testing.T) {
	s := &flowv1alpha1.TaskStatus{Phase: phaseInvestigate, RunID: 1, ReworkBudget: 2}

	for range 20 {
		// 調査 keeps asking for more of itself; the budget is the only thing
		// that stops it.
		dir := dirMore
		if s.Phase == phaseReport {
			dir = dirSent
		}
		res := transition.Next(transition.Input{
			Bindings:  flow(),
			Phase:     s.Phase,
			Directory: dir,
			Visited:   Visited(s, flow()),
			Budget:    s.ReworkBudget,
		})
		Advance(s, flow(), dir, res, "", nil, at)
		if transition.IsTerminal(flow(), s.Phase) {
			if s.Phase != flowv1alpha1.PhaseEscalated {
				t.Fatalf("ended at %q, want Escalated once the budget was spent", s.Phase)
			}
			if s.ReworkBudget != 0 {
				t.Fatalf("budget = %d, want 0", s.ReworkBudget)
			}
			// Every attempt got its own id, so no two runs share a directory.
			ids := map[int32]bool{}
			for _, h := range s.History {
				if ids[h.RunID] {
					t.Fatalf("runID %d reused; attempts would share a directory", h.RunID)
				}
				ids[h.RunID] = true
			}
			return
		}
	}
	t.Fatal("the cycle did not terminate")
}

func ttl(succeeded, failed time.Duration) *flowv1alpha1.TTLSpec {
	return &flowv1alpha1.TTLSpec{
		Succeeded: &metav1.Duration{Duration: succeeded},
		Failed:    &metav1.Duration{Duration: failed},
	}
}

func TestExpireStampsADeclaredTerminalWithSucceeded(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC))
	status := &flowv1alpha1.TaskStatus{Phase: phaseDone}

	Expire(status, flow(), ttl(time.Hour, 168*time.Hour), now)

	if status.ExpiresAt == nil || !status.ExpiresAt.Equal(&metav1.Time{Time: now.Add(time.Hour)}) {
		t.Fatalf("expiresAt = %v, want now+1h", status.ExpiresAt)
	}
}

func TestExpireStampsReservedPhasesWithFailed(t *testing.T) {
	now := metav1.NewTime(time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC))
	for _, phase := range flowv1alpha1.ReservedPhases {
		status := &flowv1alpha1.TaskStatus{Phase: phase}

		Expire(status, flow(), ttl(time.Hour, 168*time.Hour), now)

		if status.ExpiresAt == nil || !status.ExpiresAt.Equal(&metav1.Time{Time: now.Add(168 * time.Hour)}) {
			t.Fatalf("%s: expiresAt = %v, want now+168h", phase, status.ExpiresAt)
		}
	}
}

func TestExpireLeavesARunningTaskAlone(t *testing.T) {
	status := &flowv1alpha1.TaskStatus{Phase: phaseReport, CurrentRun: &flowv1alpha1.RunRef{Phase: phaseReport, RunID: 2}}

	Expire(status, flow(), ttl(time.Hour, time.Hour), at)

	if status.ExpiresAt != nil {
		t.Fatalf("a task with a binding to run is not finished; expiresAt = %v", status.ExpiresAt)
	}
}

// A date already stamped is never moved: this is what lets a later
// reconcile call Expire unconditionally to backfill a task that missed its
// first chance, without first checking whether it needs to.
func TestExpireDoesNotMoveADateAlreadyStamped(t *testing.T) {
	stamped := metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	status := &flowv1alpha1.TaskStatus{Phase: phaseDone, ExpiresAt: &stamped}

	// The values here never get read: an already-stamped date makes Expire
	// return before it looks at ttl at all.
	Expire(status, flow(), ttl(30*time.Minute, 168*time.Hour), metav1.NewTime(time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)))

	if !status.ExpiresAt.Equal(&stamped) {
		t.Fatalf("expiresAt = %v, want the original %v untouched", status.ExpiresAt, stamped)
	}
}

func TestExpireWithoutATTLKeepsTheTask(t *testing.T) {
	for name, spec := range map[string]*flowv1alpha1.TTLSpec{
		"nil ttl":      nil,
		"nil duration": {},
	} {
		status := &flowv1alpha1.TaskStatus{Phase: flowv1alpha1.PhaseFailed}

		Expire(status, nil, spec, at)

		if status.ExpiresAt != nil {
			t.Fatalf("%s: expiresAt = %v, want none", name, status.ExpiresAt)
		}
	}
}

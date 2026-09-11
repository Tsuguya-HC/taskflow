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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
	"github.com/Tsuguya-HC/taskflow/internal/metrics"
	"github.com/Tsuguya-HC/taskflow/internal/taskstate"
	"github.com/Tsuguya-HC/taskflow/internal/transition"
)

// outcome names one series of taskflow_task_outcomes_total.
type outcome struct {
	flow, phase, severity string
}

// collectedOutcomes reads the series that exist, rather than asking for one
// by its labels. TaskOutcomes.With() would create whatever it is asked about,
// so a spec that used it to check priming would be proving its own premise —
// the whole question here is which children exist before anything counted.
func collectedOutcomes() map[outcome]float64 {
	ch := make(chan prometheus.Metric)
	// CounterVec.Collect sends synchronously to ch with nothing draining it on
	// its own; without a concurrent reader here, a series count past
	// whatever buffer this had would hang the whole suite rather than fail
	// one spec.
	go func() {
		defer close(ch)
		metrics.TaskOutcomes.Collect(ch)
	}()

	out := map[outcome]float64{}
	for m := range ch {
		var d dto.Metric
		Expect(m.Write(&d)).To(Succeed())
		var key outcome
		for _, l := range d.GetLabel() {
			switch l.GetName() {
			case metrics.LabelFlow:
				key.flow = l.GetValue()
			case metrics.LabelPhase:
				key.phase = l.GetValue()
			case metrics.LabelSeverity:
				key.severity = l.GetValue()
			}
		}
		out[key] = d.GetCounter().GetValue()
	}
	return out
}

// finallyOutcome names one series of taskflow_finally_outcomes_total.
type finallyOutcome struct {
	flow, outcome string
}

// collectedFinallyOutcomes is collectedOutcomes' counterpart for
// FinallyOutcomes, for the same reason and against the same hazard.
func collectedFinallyOutcomes() map[finallyOutcome]float64 {
	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		metrics.FinallyOutcomes.Collect(ch)
	}()

	out := map[finallyOutcome]float64{}
	for m := range ch {
		var d dto.Metric
		Expect(m.Write(&d)).To(Succeed())
		var key finallyOutcome
		for _, l := range d.GetLabel() {
			switch l.GetName() {
			case metrics.LabelFlow:
				key.flow = l.GetValue()
			case metrics.LabelOutcome:
				key.outcome = l.GetValue()
			}
		}
		out[key] = d.GetCounter().GetValue()
	}
	return out
}

var _ = Describe("What a flow's endings say before they happen", func() {
	// An ending that happens once rises from nothing, and a range function
	// given a single sample returns nothing at all — so the endings that
	// matter most are the ones a watcher would never see. Reconciling a task
	// says they exist first (ADR-0010, issue #125).
	It("reports every ending of the flow at zero, as soon as a task uses it", func() {
		fx := newFixture()
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Bindings[phaseInvestigate].Next[phaseBroken] = "broken"
			f.Spec.Terminals = map[flowv1alpha1.Phase]flowv1alpha1.TerminalSeverity{
				phaseReport: flowv1alpha1.TerminalSuccess,
				phaseBroken: flowv1alpha1.TerminalFailure,
			}
		})
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile()

		got := collectedOutcomes()
		for _, want := range []outcome{
			{fx.name, string(phaseReport), string(transition.EndingSuccess)},
			{fx.name, string(phaseBroken), string(transition.EndingFailure)},
			{fx.name, string(flowv1alpha1.PhaseEscalated), string(transition.EndingEscalated)},
			{fx.name, string(flowv1alpha1.PhaseFailed), string(transition.EndingFailed)},
		} {
			value, ok := got[want]
			Expect(ok).To(BeTrue(), "%v was never reported, so nothing could see it rise", want)
			Expect(value).To(BeZero(), "%v counted something before the task reached any ending", want)
		}
	})

	// The phase the task is working in is not a place it stops. Priming it
	// would report an ending that cannot be reached.
	It("says nothing about a phase still bound to a handler", func() {
		fx := newFixture()
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile()

		for got := range collectedOutcomes() {
			if got.flow == fx.name {
				Expect(got.phase).NotTo(Equal(string(phaseInvestigate)),
					"%q is bound to a handler and is not an ending", phaseInvestigate)
			}
		}
	})

	// A flow that declares finally has FinallyOutcomes' two cleanup outcomes
	// primed the same way its own endings are, for the same reason: that
	// counter is born at 1 the first time a cleanup run settles, and the rare
	// NoAnswer must not rise from nothing (ADR-0010).
	It("reports finally's two outcomes at zero when the flow declares one", func() {
		fx := newFixture()
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: fx.name + "-cleanup", Done: "done"}
		})
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile()

		got := collectedFinallyOutcomes()
		for _, want := range []finallyOutcome{
			{fx.name, string(transition.OutcomeDeclared)},
			{fx.name, string(transition.OutcomeNoAnswer)},
		} {
			value, ok := got[want]
			Expect(ok).To(BeTrue(), "%v was never reported, so nothing could see it rise", want)
			Expect(value).To(BeZero(), "%v counted something before any cleanup run settled", want)
		}
	})

	// A flow with no finally can never produce either outcome. Priming them
	// anyway would claim a cleanup run that this flow will never have.
	It("says nothing about finally when the flow does not declare one", func() {
		fx := newFixture()
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile()

		for got := range collectedFinallyOutcomes() {
			Expect(got.flow).NotTo(Equal(fx.name), "%v declares no finally and must prime nothing", got)
		}
	})

	// A controller restart is the one time a task already sitting at
	// Escalated or Failed and still owed a cleanup run is reconciled: it
	// arrives on the reserved-phase branch, never on the one below it, so
	// that branch has to prime too.
	It("primes finally's outcomes on the reserved-phase path a restarted controller takes", func() {
		fx := newFixture()
		fx.makeFlow(func(f *flowv1alpha1.TaskFlow) {
			f.Spec.Finally = &flowv1alpha1.FinallySpec{Handler: fx.name + "-cleanup", Done: "done"}
		})
		fx.makeHandler(func(h *flowv1alpha1.TaskHandler) {
			h.Name = fx.name + "-cleanup"
			h.Spec.Phase = flowv1alpha1.PhaseFinally
		})
		tk := fx.makeTask()
		// This is what stop() actually writes for a task whose flow declares
		// finally (taskstate.go): the ending stands, and CurrentRun points at
		// the cleanup run still owed. A task at Escalated or Failed with no
		// CurrentRun is the opposite case — one that already had, or never
		// owed, a cleanup — so leaving it out here would test a state stop()
		// never produces for this flow.
		tk.Status.Phase = flowv1alpha1.PhaseEscalated
		tk.Status.CurrentRun = &flowv1alpha1.RunRef{Phase: flowv1alpha1.PhaseFinally, RunID: 1}
		Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())
		Expect(taskstate.InFinally(&tk.Status)).To(BeTrue(), "the task must actually be owed a cleanup run for this path to mean anything")

		// One reconcile only creates the cleanup Job; it does not settle it,
		// so FinallyOutcomes must still read zero here.
		fx.reconcile()

		got := collectedFinallyOutcomes()
		for _, want := range []finallyOutcome{
			{fx.name, string(transition.OutcomeDeclared)},
			{fx.name, string(transition.OutcomeNoAnswer)},
		} {
			value, ok := got[want]
			Expect(ok).To(BeTrue(), "%v was never reported on the reserved-phase path", want)
			Expect(value).To(BeZero(), "%v counted something before any cleanup run settled", want)
		}
	})
})

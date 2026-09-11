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

// Package metrics is what the controller says to whoever is watching rather
// than reading. A task's status is the record; this is the signal, written
// best-effort just after it — a crash in the narrow gap between the two
// loses the signal, not the record, so a watcher of this package must not
// assume it agrees with status down to the last task.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// FlowUnresolved stands in for the flow label when a task names a flow that
// does not exist, so that label is never the raw, attacker-chosen
// Task.spec.flow. It cannot collide with a real flow's name: it is not a
// legal Kubernetes object name.
const FlowUnresolved = "<unresolved>"

// LabelFlow, LabelPhase and LabelSeverity name TaskOutcomes' labels. Callers
// use these rather than the literals so a caller writing With(prometheus.
// Labels{...}) cannot typo a key that then silently drops that label to its
// zero value.
const (
	LabelFlow     = "flow"
	LabelPhase    = "phase"
	LabelSeverity = "severity"
	LabelOutcome  = "outcome"
)

// TaskOutcomes counts tasks by how they ended.
//
// severity is the ending, not the phase: Success and Failure as the flow
// declared them, Escalated and Failed for the framework's own two, and
// Undeclared for a flow that stopped somewhere without ever saying what
// stopping there means. Keeping Undeclared as a value of its own is what
// makes "no flow has declared its endings yet" visible instead of looking
// like a quiet run of successes.
//
// flow is either the name of a TaskFlow that exists or FlowUnresolved, never
// a caller-supplied string that failed to resolve to one — Task.spec.flow has
// no length or pattern limit, and a task's own author decides it, so treating
// it as a label value would let cardinality grow at runtime by whoever can
// create Tasks. Both other labels are the phases and endings a flow's own
// author declared in git, so the cardinality of this metric is the number of
// flows times the number of phases they declare, plus exactly one row for
// FlowUnresolved — a flow fails to resolve only through fail(), which always
// lands on Failed, so the phase and severity paired with it never vary —
// bounded by what is in git, not by anything a task can do at runtime.
var TaskOutcomes = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "taskflow_task_outcomes_total",
		Help: "Tasks that reached a phase they stop at, by what that ending means.",
	},
	[]string{LabelFlow, LabelPhase, LabelSeverity},
)

// FinallyOutcomes counts the cleanup runs that follow an ending, by whether
// they said the task was cleaned up.
//
// It is a metric of its own rather than another severity on TaskOutcomes,
// because a cleanup that failed does not change how the task ended: the same
// task is counted once there for the conclusion its work reached, and once
// here for whether the tidying up happened. Folding the two would make the
// first number wrong, which is the mistake this design is trying not to
// inherit (ADR-0009).
//
// outcome is the framework's own account of the run — Declared for one that
// wrote the directory it was given, NoAnswer for one that did not — so like
// every label here its cardinality is fixed by the code, not by a task.
var FinallyOutcomes = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "taskflow_finally_outcomes_total",
		Help: "Cleanup runs that followed an ending, by whether they reported the task cleaned up.",
	},
	[]string{LabelFlow, LabelOutcome},
)

// PrimeOutcome creates the child series for one ending and leaves it at zero,
// so a later increment reads as a rise rather than as a series appearing from
// nowhere.
//
// A CounterVec's child is born at its first Inc, with no zero sample before
// it, and a range function given a single sample returns nothing at all — so
// an ending that happens once is invisible to increase() no matter how long
// the watcher waits. Calling With and discarding the result is the client
// library's own way of saying "this exists and has not happened yet"
// (ADR-0010). It is safe to call again at any time: a child that already
// exists keeps whatever it has counted.
//
// The caller decides which endings exist, because that is a question about
// flows; this package only knows how to say it.
func PrimeOutcome(flow, phase, severity string) {
	TaskOutcomes.With(prometheus.Labels{
		LabelFlow: flow, LabelPhase: phase, LabelSeverity: severity,
	})
}

// PrimeFinallyOutcome does for FinallyOutcomes what PrimeOutcome does for
// TaskOutcomes, for the same reason (see PrimeOutcome).
func PrimeFinallyOutcome(flow, outcome string) {
	FinallyOutcomes.With(prometheus.Labels{
		LabelFlow: flow, LabelOutcome: outcome,
	})
}

func init() {
	metrics.Registry.MustRegister(TaskOutcomes, FinallyOutcomes)
}

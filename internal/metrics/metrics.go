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

func init() {
	metrics.Registry.MustRegister(TaskOutcomes)
}

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
// than reading. A task's status is the record; this is the signal, and the
// two are written at the same moment so they cannot disagree.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
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
// Both other labels are strings the flow's author chose, so the cardinality
// of this metric is the number of flows times the number of phases they
// declare — bounded by what is in git, not by anything a task can do at
// runtime.
var TaskOutcomes = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "taskflow_task_outcomes_total",
		Help: "Tasks that reached a phase they stop at, by what that ending means.",
	},
	[]string{"flow", "phase", "severity"},
)

func init() {
	metrics.Registry.MustRegister(TaskOutcomes)
}

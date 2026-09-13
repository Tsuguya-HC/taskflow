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

package metrics

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// valueOf reads the series carrying exactly these labels, and reports whether
// it exists at all. Asking the vec for the child would create it, which is
// the one thing these tests must not do: what is being checked is which
// children exist before anything counted.
func valueOf(t *testing.T, c prometheus.Collector, want prometheus.Labels) (float64, bool) {
	t.Helper()

	ch := make(chan prometheus.Metric)
	// Collect sends synchronously with nothing draining it on its own, so the
	// reader has to be here rather than the collection in a goroutine.
	go func() {
		defer close(ch)
		c.Collect(ch)
	}()

	value, found := 0.0, false
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatalf("writing a collected metric: %v", err)
		}
		got := prometheus.Labels{}
		for _, l := range d.GetLabel() {
			got[l.GetName()] = l.GetValue()
		}
		if len(got) != len(want) {
			continue
		}
		match := true
		for k, v := range want {
			if got[k] != v {
				match = false
				break
			}
		}
		if match {
			value, found = d.GetCounter().GetValue(), true
		}
	}
	return value, found
}

// An ending that has never happened has to be visible as zero, because a
// counter's child is born at its first Inc and a range function given one
// sample returns nothing: the ending nobody has seen yet is exactly the one a
// watcher would miss (ADR-0010). Priming is also documented as safe to repeat,
// and a task is reconciled many times — if the second call reset the child,
// priming would be worse than not priming at all.
func TestPrimingSaysAnEndingExistsWithoutCountingIt(t *testing.T) {
	labels := prometheus.Labels{
		LabelFlow: "flow-primed", LabelPhase: "調査", LabelSeverity: "Success",
	}
	// TaskOutcomes is a package-global vec, so the child this test primes
	// would otherwise outlive it and fail the next run's "does not exist yet"
	// assert (seen with -count=2). Delete only this test's own labels, not
	// Reset, which would also erase children other tests rely on.
	t.Cleanup(func() { TaskOutcomes.Delete(labels) })

	if _, found := valueOf(t, TaskOutcomes, labels); found {
		t.Fatalf("%v exists before anything primed it", labels)
	}

	PrimeOutcome("flow-primed", "調査", "Success")
	v, found := valueOf(t, TaskOutcomes, labels)
	if !found {
		t.Fatalf("%v does not exist after priming", labels)
	}
	if v != 0 {
		t.Fatalf("priming counted %v", v)
	}

	TaskOutcomes.With(labels).Inc()
	PrimeOutcome("flow-primed", "調査", "Success")
	if v, _ := valueOf(t, TaskOutcomes, labels); v != 1 {
		t.Fatalf("priming again over a counted ending left %v, want 1", v)
	}
}

// The cleanup run is counted separately so that a cleanup that failed does not
// make the conclusion the task's work reached look different (ADR-0009), and
// it is primed for the same reason the endings are.
func TestPrimingSaysACleanupOutcomeExistsWithoutCountingIt(t *testing.T) {
	labels := prometheus.Labels{LabelFlow: "flow-primed", LabelOutcome: "NoAnswer"}
	// Same reason as TestPrimingSaysAnEndingExistsWithoutCountingIt: this
	// child must not survive into the next -count run.
	t.Cleanup(func() { FinallyOutcomes.Delete(labels) })

	if _, found := valueOf(t, FinallyOutcomes, labels); found {
		t.Fatalf("%v exists before anything primed it", labels)
	}

	PrimeFinallyOutcome("flow-primed", "NoAnswer")
	v, found := valueOf(t, FinallyOutcomes, labels)
	if !found {
		t.Fatalf("%v does not exist after priming", labels)
	}
	if v != 0 {
		t.Fatalf("priming counted %v", v)
	}
}

// The flow label stands in for a name a task's own author chose, so it is
// replaced by this whenever that name resolves to no TaskFlow. It can only
// stand in if no TaskFlow can ever be called it — a flow that could take this
// name would have its endings counted together with every unresolved one.
func TestTheStandInIsANameNoFlowCanHave(t *testing.T) {
	if errs := validation.IsDNS1123Subdomain(FlowUnresolved); len(errs) == 0 {
		t.Fatalf("FlowUnresolved = %q is a legal object name, so a TaskFlow could take it", FlowUnresolved)
	}
}

// Nothing in this package reports anything if the collectors are not in the
// registry the manager serves — and the failure is silence, which is what
// these metrics exist to break. Registering is done in init(), so the only
// way to find it undone is to look.
//
// Looking by Gather rather than by trying to register again would not work: a
// CounterVec with no child series yet reports no family at all, so Gather
// would say whether some other test happened to have counted something, not
// whether init() registered the collector. Registering the same collector a
// second time instead fails in a way that only depends on the first
// registration having happened.
func TestBothCollectorsAreInTheRegistryTheManagerServes(t *testing.T) {
	for _, c := range []prometheus.Collector{TaskOutcomes, FinallyOutcomes} {
		err := ctrlmetrics.Registry.Register(c)
		var already prometheus.AlreadyRegisteredError
		if !errors.As(err, &already) {
			t.Errorf("registering %v again = %v, want AlreadyRegisteredError, which init() registering it once would produce", c, err)
			continue
		}
		if already.ExistingCollector != c {
			t.Errorf("%v is registered under a different collector than the package's own var, so callers using the var would not be counted", c)
		}
	}
}

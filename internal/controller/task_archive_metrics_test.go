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
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"

	"github.com/Tsuguya-HC/taskflow/internal/metrics"
)

// collectedArchiveOutcomes reads the series that exist, keyed by flow and
// outcome. ArchiveOutcomes.With() would create what it is asked about, so a
// spec using it to check priming would prove its own premise.
func collectedArchiveOutcomes() map[[2]string]float64 {
	ch := make(chan prometheus.Metric)
	go func() {
		defer close(ch)
		metrics.ArchiveOutcomes.Collect(ch)
	}()

	out := map[[2]string]float64{}
	for m := range ch {
		var d dto.Metric
		Expect(m.Write(&d)).To(Succeed())
		var key [2]string
		for _, l := range d.GetLabel() {
			switch l.GetName() {
			case metrics.LabelFlow:
				key[0] = l.GetValue()
			case metrics.LabelOutcome:
				key[1] = l.GetValue()
			}
		}
		out[key] = d.GetCounter().GetValue()
	}
	return out
}

var archiveOutcomeNames = []string{metrics.ArchiveUploaded, metrics.ArchiveWithheld, metrics.ArchiveFailed}

func enabledArchive() ArchiveConfig {
	return ArchiveConfig{Endpoint: archiveTestEndpoint, Bucket: "records", CredentialsSecret: "archive-creds"}
}

var _ = Describe("What the archive outcomes say before a record is sent", func() {
	// Created with the Job, the series is born at 1 when the sending settles
	// faster than a scrape, and increase() misses it (ADR-0010). The zero has
	// to exist while the task is still running, before any Job.
	It("reports the three outcomes at zero once a task resolves its flow, before any archive Job", func() {
		fx := newFixture()
		fx.reconciler.Archive = enabledArchive()
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile()

		got := collectedArchiveOutcomes()
		for _, o := range archiveOutcomeNames {
			value, ok := got[[2]string{fx.name, o}]
			Expect(ok).To(BeTrue(), "%s was never reported, so nothing could see it rise", o)
			Expect(value).To(BeZero())
		}
	})

	// Gathered rather than read with With(), which would create the series
	// and turn this into a vacuous pass. The flow name is this spec's own.
	It("creates no series for a flow when archiving is disabled", func() {
		fx := newFixture()
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile()

		for key := range collectedArchiveOutcomes() {
			Expect(key[0]).NotTo(Equal(fx.name), "archiving is off, so resolving a flow must not create %v", key)
		}
	})

	It("reports the unresolved flow's three outcomes at zero from assembly when archiving is enabled", func() {
		metrics.ArchiveOutcomes.DeletePartialMatch(prometheus.Labels{metrics.LabelFlow: metrics.FlowUnresolved})
		(&TaskReconciler{Archive: enabledArchive()}).primeUnresolvedArchive()

		got := collectedArchiveOutcomes()
		for _, o := range archiveOutcomeNames {
			value, ok := got[[2]string{metrics.FlowUnresolved, o}]
			Expect(ok).To(BeTrue(), "%s was not primed for the unresolved flow", o)
			Expect(value).To(BeZero())
		}
	})

	It("creates no unresolved series at assembly when archiving is disabled", func() {
		metrics.ArchiveOutcomes.DeletePartialMatch(prometheus.Labels{metrics.LabelFlow: metrics.FlowUnresolved})
		(&TaskReconciler{}).primeUnresolvedArchive()

		for key := range collectedArchiveOutcomes() {
			Expect(key[0]).NotTo(Equal(metrics.FlowUnresolved))
		}
	})
})

// A flow's zero has to be there again after a reconcile on every path that
// resolves the flow, not only the first reconcile of a new task, which reads
// the live flow. Each spec removes what the first reconcile reported so only
// the path under test can bring it back.
var _ = Describe("What the archive outcomes say on the paths a started task resolves its flow by", func() {
	forgetFlowArchive := func(flow string) {
		metrics.ArchiveOutcomes.DeletePartialMatch(prometheus.Labels{metrics.LabelFlow: flow})
	}
	expectZeroes := func(flow string) {
		got := collectedArchiveOutcomes()
		for _, o := range archiveOutcomeNames {
			value, ok := got[[2]string{flow, o}]
			Expect(ok).To(BeTrue(), "%s was not reported again", o)
			Expect(value).To(BeZero())
		}
	}

	It("reports them for a started task that reads its flow from the copy", func() {
		fx := newFixture()
		fx.reconciler.Archive = enabledArchive()
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile()
		Expect(fx.get().Status.Phase).NotTo(BeEmpty())

		forgetFlowArchive(fx.name)
		fx.reconcile()
		expectZeroes(fx.name)
	})

	It("reports them for a task at a framework phase that reads its flow from the copy", func() {
		fx := newFixture()
		fx.reconciler.Archive = enabledArchive()
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile()

		tk := fx.get()
		tk.Status.Phase = flowv1alpha1.PhaseTaskFailed
		Expect(k8sClient.Status().Update(fx.ctx, tk)).To(Succeed())

		forgetFlowArchive(fx.name)
		fx.reconcile()
		expectZeroes(fx.name)
	})
})

// Reconcile is called directly by every other spec, so nothing else goes
// through the assembly that production uses.
var _ = Describe("What the archive outcomes say once the controller is assembled", func() {
	It("reports the unresolved flow's three outcomes at zero from SetupWithManager", func() {
		metrics.ArchiveOutcomes.DeletePartialMatch(prometheus.Labels{metrics.LabelFlow: metrics.FlowUnresolved})
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:     k8sClient.Scheme(),
			Metrics:    metricsserver.Options{BindAddress: "0"},
			Controller: ctrlconfig.Controller{SkipNameValidation: ptr.To(true)},
		})
		Expect(err).NotTo(HaveOccurred())

		r := &TaskReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Archive: enabledArchive()}
		Expect(r.SetupWithManager(mgr)).To(Succeed())

		got := collectedArchiveOutcomes()
		for _, o := range archiveOutcomeNames {
			_, ok := got[[2]string{metrics.FlowUnresolved, o}]
			Expect(ok).To(BeTrue(), "%s was not primed by SetupWithManager", o)
		}
	})
})

const archiveTestEndpoint = "s3.example:9000"

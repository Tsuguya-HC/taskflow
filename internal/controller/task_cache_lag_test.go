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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// Issue #287: a Job that completed while the controller was restarting
// (leader change) is seen Complete by a controller whose Pod cache has not
// caught up, so listing its pods through the cache returns nothing even though
// the pod exists. The answer is in that pod, so it has to be read from the
// API server instead of being given up as "no pod to read".
var _ = Describe("a finished run whose pods the cache cannot see yet", func() {
	var fx *fixture

	BeforeEach(func() {
		fx = newFixture()
	})

	It("reads the answer from the pod instead of ending the task with no answer", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile() // settles the starting phase
		fx.reconcile() // creates the Job
		finishJobWith(fx, fx.job(1), "ok\nfound it")

		blindToPods(fx)
		fx.reconcile()

		Expect(fx.get().Status.Phase).To(Equal(phaseReport),
			"the pod existed and said ok; a stale cache must not turn that into a failure")
	})
})

// blindToPods makes every Pod list through the reconciler's own client come
// back empty, as a cold informer would, while APIReader still sees them.
func blindToPods(fx *fixture) {
	watchClient, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
	Expect(err).NotTo(HaveOccurred())
	fx.reconciler.Client = interceptor.NewClient(watchClient, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return nil
			}
			return c.List(ctx, list, opts...)
		},
	})
}

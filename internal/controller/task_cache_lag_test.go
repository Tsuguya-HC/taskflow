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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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

	It("returns the cache's empty list when there is no APIReader", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile()
		fx.reconcile()
		finishJobWith(fx, fx.job(1), "ok\nfound it")

		blindToPods(fx)
		fx.reconciler.APIReader = nil
		var pods corev1.PodList
		var err error
		Expect(func() { pods, err = fx.reconciler.podsOf(fx.ctx, fx.job(1)) }).NotTo(Panic())
		Expect(err).NotTo(HaveOccurred())
		Expect(pods.Items).To(BeEmpty())
	})

	It("surfaces the error when the API server read of the pods fails", func() {
		fx.makeFlow()
		fx.makeHandler()
		fx.makeTask()
		fx.reconcile()
		fx.reconcile()
		finishJobWith(fx, fx.job(1), "ok\nfound it")

		blindToPods(fx)
		fx.reconciler.APIReader = failingPodReader{Reader: fx.reconciler.APIReader}
		_, err := fx.reconciler.Reconcile(fx.ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: fx.name, Namespace: resourceNamespace},
		})
		Expect(err).To(HaveOccurred())
	})
})

// failingPodReader fails every Pod list, as an overloaded API server would.
type failingPodReader struct{ client.Reader }

func (f failingPodReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		return errors.New("api server unavailable")
	}
	return f.Reader.List(ctx, list, opts...)
}

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

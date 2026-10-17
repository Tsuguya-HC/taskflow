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
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Every Job the envtest suite completes carries a CompletionTime, so the
// fallbacks are only reachable here.
func TestArchiveJobEnded(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	completed := created.Add(time.Hour)
	transitioned := created.Add(2 * time.Hour)
	ct := metav1.NewTime(completed)

	cond := func(typ batchv1.JobConditionType, s corev1.ConditionStatus, at time.Time) batchv1.JobCondition {
		return batchv1.JobCondition{Type: typ, Status: s, LastTransitionTime: metav1.NewTime(at)}
	}

	cases := []struct {
		name       string
		completion *metav1.Time
		conditions []batchv1.JobCondition
		want       time.Time
	}{
		{"completion time wins", &ct, []batchv1.JobCondition{cond(batchv1.JobComplete, corev1.ConditionTrue, transitioned)}, completed},
		{"Complete condition without completion time", nil, []batchv1.JobCondition{cond(batchv1.JobComplete, corev1.ConditionTrue, transitioned)}, transitioned},
		{"other conditions are skipped", nil, []batchv1.JobCondition{
			cond(batchv1.JobFailed, corev1.ConditionTrue, created.Add(5*time.Hour)),
			cond(batchv1.JobComplete, corev1.ConditionFalse, created.Add(6*time.Hour)),
			cond(batchv1.JobComplete, corev1.ConditionTrue, transitioned),
		}, transitioned},
		{"Complete condition without a transition time", nil, []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}, created},
		{"neither: creation", nil, nil, created},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{CreationTimestamp: metav1.NewTime(created)}}
			job.Status.CompletionTime = tc.completion
			job.Status.Conditions = tc.conditions
			if got := archiveJobEnded(job); !got.Equal(tc.want) {
				t.Fatalf("archiveJobEnded = %v, want %v", got, tc.want)
			}
		})
	}
}

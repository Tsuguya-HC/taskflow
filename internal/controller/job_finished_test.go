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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// jobFinished needs no cluster, so this drives it directly rather than
// through envtest.
func TestJobFinished(t *testing.T) {
	cases := []struct {
		name         string
		conditions   []batchv1.JobCondition
		wantFinished bool
		wantFailure  string
	}{
		{
			name:         "still running",
			conditions:   nil,
			wantFinished: false,
		},
		{
			name: "complete",
			conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			},
			wantFinished: true,
			wantFailure:  "",
		},
		{
			name: "failed with a reason",
			conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: batchv1.JobReasonBackoffLimitExceeded},
			},
			wantFinished: true,
			wantFailure:  batchv1.JobReasonBackoffLimitExceeded,
		},
		{
			// An empty reason must not read as "" the way Complete does — that
			// would look like success to the caller, which only checks
			// failure == "".
			name: "failed with no reason",
			conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: ""},
			},
			wantFinished: true,
			wantFailure:  "Failed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{Status: batchv1.JobStatus{Conditions: tc.conditions}}
			finished, failure := jobFinished(job)
			if finished != tc.wantFinished {
				t.Fatalf("finished = %v, want %v", finished, tc.wantFinished)
			}
			if failure != tc.wantFailure {
				t.Fatalf("failure = %q, want %q", failure, tc.wantFailure)
			}
		})
	}
}

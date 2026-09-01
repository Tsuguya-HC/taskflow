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
	"reflect"
	"testing"
)

// With runs strictly serial, sweepRuns is every run before the current one —
// the set prepare may find abandoned in work/ (a sealed one has already been
// renamed onto the shelf, so sweeping it again is a no-op).
func TestSweepRuns(t *testing.T) {
	cases := []struct {
		current int32
		want    []int32
	}{
		{current: 1, want: nil},
		{current: 2, want: []int32{1}},
		{current: 3, want: []int32{1, 2}},
	}
	for _, c := range cases {
		if got := sweepRuns(c.current); !reflect.DeepEqual(got, c.want) {
			t.Errorf("sweepRuns(%d) = %v, want %v", c.current, got, c.want)
		}
	}
}

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

import "testing"

func TestArchiveConfigSwitch(t *testing.T) {
	full := ArchiveConfig{Endpoint: archiveTestEndpoint, Bucket: "b", CredentialsSecret: "s"}
	cases := []struct {
		name    string
		mut     func(*ArchiveConfig)
		enabled bool
		invalid bool
	}{
		{"none given", func(c *ArchiveConfig) { *c = ArchiveConfig{} }, false, false},
		{"all three given", func(*ArchiveConfig) {}, true, false},
		{"only the optional ones given", func(c *ArchiveConfig) { *c = ArchiveConfig{Prefix: "p", Region: "r"} }, false, false},
		{"endpoint missing", func(c *ArchiveConfig) { c.Endpoint = "" }, false, true},
		{"bucket missing", func(c *ArchiveConfig) { c.Bucket = "" }, false, true},
		{"secret missing", func(c *ArchiveConfig) { c.CredentialsSecret = "" }, false, true},
		{"only the bucket given", func(c *ArchiveConfig) { *c = ArchiveConfig{Bucket: "b"} }, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := full
			tc.mut(&c)
			if got := c.Enabled(); got != tc.enabled {
				t.Errorf("Enabled() = %v, want %v", got, tc.enabled)
			}
			if err := c.Validate(); (err != nil) != tc.invalid {
				t.Errorf("Validate() = %v, want error: %v", err, tc.invalid)
			}
		})
	}
}

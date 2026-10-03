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

// Package snapshot is the shape of the copy a task's start writes.
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"k8s.io/apimachinery/pkg/types"

	flowv1alpha1 "github.com/Tsuguya-HC/taskflow/api/v1alpha1"
)

// Payload is what one start-time ControllerRevision carries.
type Payload struct {
	Flow flowv1alpha1.TaskFlowSpec `json:"flow"`
	// Handlers is keyed by handler name. A finally handler with no object
	// behind it is absent here and reported by FinallyAbsent instead.
	Handlers      map[string]flowv1alpha1.TaskHandlerSpec `json:"handlers,omitempty"`
	FinallyAbsent bool                                    `json:"finallyAbsent,omitempty"`
}

func Decode(raw []byte) (Payload, error) {
	var p Payload
	if err := json.Unmarshal(raw, &p); err != nil {
		return Payload{}, err
	}
	return p, nil
}

// RevisionName is the name of the one revision a task's start writes. The
// task's UID is in it: a task deleted and recreated under the same name is a
// different task, and sharing the old name would rebind the new task to a
// revision that is stale or still terminating.
func RevisionName(taskName string, uid types.UID) string {
	sum := sha256.Sum256([]byte(uid))
	suffix := "-snap-" + hex.EncodeToString(sum[:])[:8]
	prefix := taskName
	if len(prefix)+len(suffix) > 63 {
		prefix = taskName[:63-len(suffix)]
	}
	return prefix + suffix
}

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

// Package collect reads what a finished run answered.
//
// The answer travels in a container's termination message, which Kubernetes
// surfaces on the pod status — so the controller needs no credentials for
// whatever holds the workspace, and no idea what kind of store that is.
//
// There is no parser here, and therefore no parse error. A message answers if
// its first line is exactly one of the directories the flow declared, and does
// not otherwise. That is the same rule the directories themselves follow, for
// the same reason: a vocabulary you cannot spell wrong beats one you validate.
package collect

import (
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// Answer is what a run reported.
type Answer struct {
	// Directory the run chose, empty when it did not choose exactly one.
	Directory string
	// Reason is the rest of the reporting container's message when there is
	// an answer, and why there is none when there is not. Human-facing: the
	// transition never reads it.
	Reason string
	// Container that reported, when exactly one did.
	Container string
}

// FromPod returns the answer a finished pod gave.
//
// Every container is examined — init containers included, since a native
// sidecar is one — because nothing tells the framework which container was
// meant to report. Whoever writes the handler decides that, and the framework
// finds out by looking at what the declared vocabulary allows.
func FromPod(pod *corev1.Pod, declared []string) Answer {
	if pod == nil {
		return Answer{Reason: "no pod to read"}
	}

	type candidate struct{ container, dir, reason string }
	var found []candidate
	looked := 0

	statuses := slices.Concat(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses)
	for _, cs := range statuses {
		if cs.State.Terminated == nil {
			continue
		}
		msg := cs.State.Terminated.Message
		if msg == "" {
			continue
		}
		looked++
		first, rest, _ := strings.Cut(msg, "\n")
		first = strings.TrimSpace(first)
		if !slices.Contains(declared, first) {
			continue
		}
		found = append(found, candidate{cs.Name, first, strings.TrimSpace(rest)})
	}

	switch len(found) {
	case 1:
		return Answer{Directory: found[0].dir, Reason: found[0].reason, Container: found[0].container}
	case 0:
		if looked == 0 {
			// Nothing wrote a message at all: the node died, the pod was
			// OOM-killed, or the sidecar never ran. Fail-closed with no
			// effort, which is the point of using this channel.
			return Answer{Reason: "no container left a termination message"}
		}
		return Answer{Reason: fmt.Sprintf(
			"%d container(s) reported, none naming a declared directory (%s)",
			looked, strings.Join(declared, ", "))}
	default:
		names := make([]string, 0, len(found))
		for _, c := range found {
			names = append(names, fmt.Sprintf("%s→%s", c.container, c.dir))
		}
		return Answer{Reason: "more than one container answered: " + strings.Join(names, ", ")}
	}
}

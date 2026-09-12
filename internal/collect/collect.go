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
// For a run with a pod the answer travels in a container's termination
// message, which Kubernetes surfaces on the pod status — so the controller
// needs no credentials for whatever holds the workspace, and no idea what
// kind of store that is. For a run the framework does not start, it is a key
// in the ConfigMap the controller opened for it (ADR-0011). Two channels,
// one rule.
//
// There is no parser here, and therefore no parse error. An answer counts if
// it is exactly one of the directories the flow declared, and does not
// otherwise. That is the same rule the directories themselves follow, for
// the same reason: a vocabulary you cannot spell wrong beats one you validate.
package collect

import (
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/Tsuguya-HC/taskflow/internal/contract"
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
func FromPod(pod *corev1.Pod, declared []string) Answer {
	if pod == nil {
		return Answer{Reason: "no pod to read"}
	}
	return FromPods([]corev1.Pod{*pod}, declared)
}

// FromPods returns the answer a finished run gave, read across every pod the
// run's Job produced.
//
// Every container is examined — init containers included, since a native
// sidecar is one — because nothing tells the framework which container was
// meant to report. Whoever writes the handler decides that, and the framework
// finds out by looking at what the declared vocabulary allows.
//
// A Job is meant to produce one pod, but the Job controller can replace a pod
// that is still terminating, so there may be two. The rule does not change for
// that: exactly one container across all of them names a declared directory,
// or there is no answer. Two pods that both answered is exactly the case a
// human should look at.
func FromPods(pods []corev1.Pod, declared []string) Answer {
	if len(pods) == 0 {
		return Answer{Reason: "no pod to read"}
	}

	type candidate struct{ container, dir, reason string }
	var found []candidate
	looked := 0

	for _, pod := range pods {
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
			name := cs.Name
			if len(pods) > 1 {
				name = pod.Name + "/" + cs.Name
			}
			found = append(found, candidate{name, first, Sanitize(strings.TrimSpace(rest))})
		}
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
		return Answer{Reason: reasonf(
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

// Ran reports whether the handler got as far as running: some container, in
// some pod, reached a terminated state. It separates a run the handler had a
// hand in from one it never touched — an image that would not pull, a pod that
// never got scheduled — which is the only kind the controller retries on its
// own. A run that started and then said nothing is the handler's silence, and
// silence goes to a human, not back into the queue.
func Ran(pods []corev1.Pod) bool {
	for _, pod := range pods {
		for _, cs := range slices.Concat(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses) {
			if cs.State.Terminated != nil {
				return true
			}
		}
	}
	return false
}

// FromBox returns the answer written into the place a State run is answered
// in, and whether anything was written there at all.
//
// The two are separate questions here, which they are not for a pod: a run
// that produced no directory is over and has said nothing, while an unwritten
// verdict box is a run still being considered. Only the caller's clock can
// tell the second from a run nobody will ever answer, so this reports what it
// sees and leaves the deadline to the caller.
//
// The value is matched by the rule FromPods applies to a termination
// message's first line — exactly one of the declared directories — so a word
// outside the vocabulary is not an error to report but an answer that does
// not count, and ends the same way: no directory, and a reason saying so.
// Surrounding space is trimmed for the same reason it is there, an answer
// typed by hand.
func FromBox(box *corev1.ConfigMap, declared []string) (Answer, bool) {
	if box == nil {
		return Answer{Reason: "no verdict box to read"}, false
	}
	value := strings.TrimSpace(box.Data[contract.KeyVerdict])
	if value == "" {
		// An empty value is not an answer that failed to match: no declared
		// directory can be named by the empty string (contract.CheckDirectoryName
		// refuses it), so writing one is indistinguishable from not having
		// written yet, and the honest reading is that the run is still waiting.
		return Answer{}, false
	}
	reason := Sanitize(strings.TrimSpace(box.Data[contract.KeyReason]))
	if !slices.Contains(declared, value) {
		return Answer{Reason: reasonf(
			"%q is not one of the words this run may be answered with (%s)",
			Sanitize(value), strings.Join(declared, ", "))}, true
	}
	return Answer{Directory: value, Reason: reason}, true
}

// reasonf builds a Reason from prose wrapped around pieces that are each
// bounded on their own — an answer already run through Sanitize, a declared
// directory that admission holds to a single path element — but not as a
// whole: strings.Join has no length of its own, and a flow can declare as
// many directories as it likes. What lands in status still has a hard limit
// of its own (HistoryEntry.Reason, 2048 runes in the CRD), so the assembled
// message is clamped the same way Sanitize clamps a single field, at
// maxReasonRunes — comfortably under that limit even doubled, and simpler
// than deriving the CRD's number here. FromPods and FromBox both build a
// message this shape, and share this rather than each capping it separately.
func reasonf(format string, args ...any) string {
	return Sanitize(fmt.Sprintf(format, args...))
}

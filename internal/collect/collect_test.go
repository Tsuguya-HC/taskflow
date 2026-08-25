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

package collect

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

const dirMore = "more"

var declared = []string{"ok", dirMore}

func pod(containers ...corev1.ContainerStatus) *corev1.Pod {
	return &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: containers}}
}

func terminated(name, msg string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  name,
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Message: msg}},
	}
}

func TestAnswers(t *testing.T) {
	got := FromPod(pod(terminated("publish", "ok")), declared)
	if got.Directory != "ok" {
		t.Fatalf("directory = %q (%s)", got.Directory, got.Reason)
	}
	if got.Container != "publish" {
		t.Fatalf("container = %q", got.Container)
	}
}

// The rest of the message is for a human. It never reaches the transition.
func TestCarriesTheReasonWithoutParsingIt(t *testing.T) {
	got := FromPod(pod(terminated("publish", "more\n3 件のうち 1 件しか確認できていない")), declared)
	if got.Directory != dirMore {
		t.Fatalf("directory = %q", got.Directory)
	}
	if got.Reason != "3 件のうち 1 件しか確認できていない" {
		t.Fatalf("reason = %q", got.Reason)
	}
}

// The reason travels into status a human reads with kubectl or a dashboard.
// An agent is not trusted, so control sequences it writes must not survive
// into that channel.
func TestReasonStripsControlSequences(t *testing.T) {
	got := FromPod(pod(terminated("publish", "more\n\x1b[31mred\x1b[0m\x07 alert")), declared)
	if got.Directory != dirMore {
		t.Fatalf("directory = %q", got.Directory)
	}
	if strings.ContainsAny(got.Reason, "\x1b\x07") {
		t.Fatalf("reason still carries control characters: %q", got.Reason)
	}
	if got.Reason != "[31mred[0m alert" {
		t.Fatalf("reason = %q", got.Reason)
	}
}

// unicode.IsPrint treats every space but ASCII 0x20 as non-printable, so a
// full-width or other Unicode space separator (category Zs) must be kept
// explicitly or it silently vanishes and runs the surrounding words together.
func TestReasonKeepsUnicodeSpaceSeparators(t *testing.T) {
	got := FromPod(pod(terminated("publish", "more\n設定が　正しくない")), declared)
	if got.Directory != dirMore {
		t.Fatalf("directory = %q", got.Directory)
	}
	if got.Reason != "設定が　正しくない" {
		t.Fatalf("reason = %q", got.Reason)
	}
}

// A byte sequence that is not valid UTF-8 must not panic Sanitize, and it
// must not vanish silently either — the replacement character marks that
// something unreadable was there instead of the text just getting shorter.
func TestReasonHandlesInvalidUTF8(t *testing.T) {
	got := FromPod(pod(terminated("publish", "ok\nabc\xffdef")), declared)
	if got.Directory != "ok" {
		t.Fatalf("directory = %q", got.Directory)
	}
	if !strings.Contains(got.Reason, "�") {
		t.Fatalf("reason = %q, want the invalid byte kept as U+FFFD rather than dropped", got.Reason)
	}
}

// A handler could write arbitrarily much after the directory line; the
// reason must not carry an unbounded amount of it into status.
func TestReasonIsTruncated(t *testing.T) {
	long := strings.Repeat("a", maxReasonRunes+500)
	got := FromPod(pod(terminated("publish", "more\n"+long)), declared)
	if got.Directory != dirMore {
		t.Fatalf("directory = %q", got.Directory)
	}
	if len([]rune(got.Reason)) != maxReasonRunes {
		t.Fatalf("reason has %d runes, want %d", len([]rune(got.Reason)), maxReasonRunes)
	}
}

// A native sidecar is an init container, so those have to be read too.
func TestReadsInitContainers(t *testing.T) {
	p := &corev1.Pod{Status: corev1.PodStatus{
		InitContainerStatuses: []corev1.ContainerStatus{terminated("publish", "ok")},
		ContainerStatuses:     []corev1.ContainerStatus{terminated("agent", "")},
	}}
	if got := FromPod(p, declared); got.Directory != "ok" {
		t.Fatalf("directory = %q (%s)", got.Directory, got.Reason)
	}
}

// Nothing wrote anything: the node died, the pod was OOM-killed, the sidecar
// never got to run. Fail-closed with no effort on anyone's part.
func TestSilenceIsNotAnAnswer(t *testing.T) {
	got := FromPod(pod(corev1.ContainerStatus{Name: "agent"}), declared)
	if got.Directory != "" {
		t.Fatalf("directory = %q, want none", got.Directory)
	}
	if !strings.Contains(got.Reason, "termination message") {
		t.Fatalf("reason = %q", got.Reason)
	}
}

// This is how a sidecar reports its own failure: it writes something that is
// not a declared directory. Nothing had to be designed for it.
func TestAnUndeclaredMessageIsNotAnAnswer(t *testing.T) {
	got := FromPod(pod(terminated("publish", "publish failed: 503 from the store")), declared)
	if got.Directory != "" {
		t.Fatalf("directory = %q, want none", got.Directory)
	}
	if !strings.Contains(got.Reason, "none naming a declared directory") {
		t.Fatalf("reason = %q", got.Reason)
	}
}

func TestTwoAnswersAreNoAnswer(t *testing.T) {
	got := FromPod(pod(terminated("publish", "ok"), terminated("agent", dirMore)), declared)
	if got.Directory != "" {
		t.Fatalf("directory = %q, want none", got.Directory)
	}
	if !strings.Contains(got.Reason, "more than one") {
		t.Fatalf("reason = %q", got.Reason)
	}
}

// Even agreeing containers are two answers. Picking one would mean deciding
// which container speaks for the run, which is exactly what this avoids.
func TestTwoAgreeingAnswersAreStillNoAnswer(t *testing.T) {
	got := FromPod(pod(terminated("publish", "ok"), terminated("agent", "ok")), declared)
	if got.Directory != "" {
		t.Fatalf("directory = %q, want none", got.Directory)
	}
}

// The vocabulary comes from the flow, so a name that was not declared for this
// phase is not an answer here even if some other phase declares it.
func TestOnlyThisPhasesVocabularyCounts(t *testing.T) {
	got := FromPod(pod(terminated("publish", "sent")), declared)
	if got.Directory != "" {
		t.Fatalf("directory = %q; sent belongs to another phase", got.Directory)
	}
}

func TestIgnoresSurroundingWhitespace(t *testing.T) {
	if got := FromPod(pod(terminated("publish", "  ok  \nfine")), declared); got.Directory != "ok" {
		t.Fatalf("directory = %q", got.Directory)
	}
}

// A message that merely contains a declared name does not answer — the first
// line has to be the name and nothing else. Otherwise a stray log line
// mentioning ok would decide the task.
func TestSubstringsDoNotCount(t *testing.T) {
	for _, msg := range []string{"looks ok to me", "okay", "not ok"} {
		if got := FromPod(pod(terminated("publish", msg)), declared); got.Directory != "" {
			t.Fatalf("%q was read as %q", msg, got.Directory)
		}
	}
}

func TestNoPod(t *testing.T) {
	if got := FromPod(nil, declared); got.Directory != "" || got.Reason == "" {
		t.Fatalf("got %+v", got)
	}
}

func TestRunningContainersAreNotRead(t *testing.T) {
	p := pod(corev1.ContainerStatus{
		Name:  "publish",
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	})
	if got := FromPod(p, declared); got.Directory != "" {
		t.Fatalf("directory = %q from a container that has not finished", got.Directory)
	}
}

// A Job normally has one pod, but a pod replaced while terminating leaves two
// (KEP-3939). The rule is the same across them: exactly one answer.
func TestReadsAcrossThePodsOfAJob(t *testing.T) {
	a := pod(terminated("publish", "ok"))
	a.Name = "run-a"
	b := pod(terminated("publish", ""))
	b.Name = "run-b"

	got := FromPods([]corev1.Pod{*a, *b}, declared)
	if got.Directory != "ok" {
		t.Fatalf("directory = %q (%s)", got.Directory, got.Reason)
	}
	if got.Container != "run-a/publish" {
		t.Fatalf("container = %q; with several pods the answer must say which one", got.Container)
	}
}

func TestTwoPodsBothAnsweringIsNoAnswer(t *testing.T) {
	a := pod(terminated("publish", "ok"))
	a.Name = "run-a"
	b := pod(terminated("publish", "ok"))
	b.Name = "run-b"

	got := FromPods([]corev1.Pod{*a, *b}, declared)
	if got.Directory != "" {
		t.Fatalf("directory = %q; two pods agreeing is still two answers", got.Directory)
	}
	if !strings.Contains(got.Reason, "run-a/publish") || !strings.Contains(got.Reason, "run-b/publish") {
		t.Fatalf("reason = %q; it must name both", got.Reason)
	}
}

func TestNoPodsIsNoAnswer(t *testing.T) {
	got := FromPods(nil, declared)
	if got.Directory != "" || got.Reason == "" {
		t.Fatalf("got %+v", got)
	}
}

// Ran separates "the handler did something" from "nothing ever started" —
// the latter being the only failure the controller retries by itself.
func TestRan(t *testing.T) {
	waiting := corev1.ContainerStatus{Name: "agent", State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}

	if Ran(nil) {
		t.Fatal("no pods did not run")
	}
	if Ran([]corev1.Pod{*pod(waiting)}) {
		t.Fatal("a container that never left Waiting did not run")
	}
	if !Ran([]corev1.Pod{*pod(terminated("agent", ""))}) {
		t.Fatal("a terminated container ran, whatever it said")
	}
	init := &corev1.Pod{Status: corev1.PodStatus{
		InitContainerStatuses: []corev1.ContainerStatus{terminated("prepare", "")},
		ContainerStatuses:     []corev1.ContainerStatus{waiting},
	}}
	if !Ran([]corev1.Pod{*init}) {
		t.Fatal("an init container that terminated counts: the handler's own code ran")
	}
}

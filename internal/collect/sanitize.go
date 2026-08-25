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
	"unicode"
	"unicode/utf8"
)

// maxReasonRunes bounds how much of an untrusted termination message the
// framework carries into status. It is not a parse limit — the directory
// line is still read whole — only how much of the free-text remainder a
// handler can make a human, or a dashboard, render.
const maxReasonRunes = 1024

// Sanitize prepares free text an agent wrote — the second line onward of a
// termination message — for a status field a person reads with kubectl or a
// dashboard. The agent is not trusted: nothing stops it writing terminal
// escapes or other control sequences into that text, and status is exactly
// the channel that would carry them to an operator's terminal. This does not
// parse or validate the text, only strip what a terminal or renderer would
// act on, and cap its length.
func Sanitize(s string) string {
	var b strings.Builder
	n := 0
	for _, r := range s {
		if n >= maxReasonRunes {
			break
		}
		// \n and \t are kept: the text is meant to read as a line or two of
		// prose. Everything else non-printable — C0/C1 controls, escapes,
		// stray format characters — is dropped. utf8.RuneError marks a byte
		// sequence that was never valid text; leaving it as the replacement
		// character says so instead of silently deleting it. unicode.IsPrint
		// treats every space but ASCII 0x20 as non-printable, which would
		// silently drop U+3000 and other Unicode space separators (category
		// Zs) and run words together; unicode.IsSpace is not the fix, since
		// it also passes C1 controls such as U+0085 (NEL).
		if r != '\n' && r != '\t' && r != utf8.RuneError && !unicode.IsPrint(r) && !unicode.Is(unicode.Zs, r) {
			continue
		}
		b.WriteRune(r)
		n++
	}
	return b.String()
}

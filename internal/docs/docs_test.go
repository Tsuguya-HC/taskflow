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

// Package docs holds the checks on docs/ that a grep can decide (#182): the
// phrases of the design from before a Task ran on a copy of its definitions
// are gone, ADR-0014 is there in the form ADR-0009 set, and the ADRs it
// partly overrides carry the marker line.
package docs

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	adr14File = "0014-pin-definitions-at-start.md"
	markerKey = "- **一部を覆された**: [ADR-0014](" + adr14File + ")"
)

func read(t *testing.T, rel ...string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(append([]string{"..", "..", "docs"}, rel...)...))
	if err != nil {
		t.Fatalf("cannot read the document: %v", err)
	}
	return string(b)
}

func section(t *testing.T, md, start string, stops ...string) string {
	t.Helper()
	lines := strings.Split(md, "\n")
	from := -1
	for i, l := range lines {
		if strings.HasPrefix(l, start) {
			from = i
			break
		}
	}
	if from < 0 {
		t.Fatalf("no line starts with %q", start)
	}
	to := len(lines)
	for i := from + 1; i < len(lines) && to == len(lines); i++ {
		for _, s := range stops {
			if strings.HasPrefix(lines[i], s) {
				to = i
				break
			}
		}
	}
	return strings.Join(lines[from:to], "\n")
}

func headerBullets(t *testing.T, adr string) []string {
	t.Helper()
	lines := strings.Split(adr, "\n")
	for i, l := range lines {
		if !strings.HasPrefix(l, "- **status**") {
			continue
		}
		for j := i; j < len(lines); j++ {
			if lines[j] == "" {
				return lines[i:j]
			}
		}
	}
	t.Fatal("no status line")
	return nil
}

func mustContain(t *testing.T, where, text string, wants ...string) {
	t.Helper()
	for _, w := range wants {
		if !strings.Contains(text, w) {
			t.Errorf("%s does not contain %q", where, w)
		}
	}
}

// These are the sentences of the design in which a running Task reads the
// live definitions on every reconcile.
func TestDesignHasNoPhrasesOfTheUnpinnedDesign(t *testing.T) {
	design := read(t, "design.md")
	for _, p := range []string{
		"毎 reconcile で解決し直す",
		"スナップショットしない",
		"その写しはまだ run に使わない",
		"dispatch 時点の",
		"毎 reconcile で読み直す",
		"`batch/v1` + `core/v1` だけ",
		"`batch/v1` と core/v1 だけ",
		"フェーズの binding 自体が消えた",
		"  conditions: [...]",
	} {
		if strings.Contains(design, p) {
			t.Errorf("docs/design.md still says %q", p)
		}
	}
}

func TestDesignNamesNoFunctionsWhereItWasRewritten(t *testing.T) {
	design := read(t, "design.md")
	for _, name := range []string{"handlerFor", "driveBranches"} {
		if strings.Contains(design, name) {
			t.Errorf("docs/design.md still names the function %q", name)
		}
	}
}

func TestDesignDescribesTheCopy(t *testing.T) {
	design := read(t, "design.md")

	api := section(t, design, "## 4. API", "## 5. ")
	mustContain(t, "§4", api, "ControllerRevision", "DefinitionsPinned", adr14File)

	labels := section(t, design, "### コントローラが作る物には決まったラベルを付ける", "### ", "## ")
	mustContain(t, "§4 labels", labels, "ControllerRevision")

	terminals := section(t, design, "### 終端の意味は flow が宣言する", "### ", "## ")
	mustContain(t, "§5 terminals", terminals, "DefinitionsLost", "FlowBroken")

	finally := section(t, design, "### 終端の後に 1 回だけ走る `finally`", "### ", "## ")
	mustContain(t, "§5 finally", finally, "DefinitionsLost")

	contradictions := section(t, design, "### 実行時の矛盾は修復せず `Failed`", "### ", "## ")
	mustContain(t, "§5 runtime contradictions", contradictions, "DefinitionsLost")

	cleanup := section(t, design, "## 10. 掃除", "### ", "## 11.")
	mustContain(t, "§10 cleanup table", cleanup, "ControllerRevision")
}

// Flipping a sentence's verdict leaves the word DefinitionsLost in its
// section, so the sentences are pinned whole.
func TestDesignSaysWhichDefinitionsTheCopyDecides(t *testing.T) {
	design := read(t, "design.md")

	finally := section(t, design, "### 終端の後に 1 回だけ走る `finally`", "### ", "## ")
	mustContain(t, "§5 finally", finally,
		"写しが消されて `DefinitionsLost` で `Failed` になったときも走らない")

	ttl := section(t, design, "### Task の TTL", "### ", "## ")
	mustContain(t, "§10 Task の TTL", ttl,
		"写しの flow から決まり、live の flow の編集は届かない")
}

// Each option ADR-0014 rejected has its own row in §11, so a row dropped or
// merged into another leaves fewer than four.
func TestDesignRejectedIdeasHaveTheFourRowsOfADR0014(t *testing.T) {
	rejected := section(t, read(t, "design.md"), "## 11. 却下した案と理由", "---", "## 12.")
	n := 0
	for l := range strings.SplitSeq(rejected, "\n") {
		if strings.HasPrefix(l, "|") && strings.Contains(l, "ADR-0014") {
			n++
		}
	}
	if n < 4 {
		t.Errorf("§11 has %d rows that cite ADR-0014, want at least 4", n)
	}
}

func TestADR0014HasTheSectionsOfADR0009(t *testing.T) {
	adr := read(t, "adr", adr14File)

	if !strings.HasPrefix(adr, "# ADR-0014 ") {
		t.Errorf("the title does not start with %q", "# ADR-0014 ")
	}
	mustContain(t, "ADR-0014", adr, "- **status**: accepted（2026-10-02、人間の承認）")

	root := section(t, adr, "- **根拠**", "**決定**")
	mustContain(t, "the 根拠 of ADR-0014", root,
		"#176", "issuecomment-5950628820", "#194", "#197", "#200", "#205", "#210")

	last := -1
	for _, h := range []string{"**決定**:", "**なぜ**:", "**覆したもの**:", "**覆すには**:", "**やらなかったこと**:"} {
		idx := regexp.MustCompile("(?m)^" + regexp.QuoteMeta(h)).FindStringIndex(adr)
		if idx == nil {
			t.Errorf("ADR-0014 has no line starting with %q", h)
			continue
		}
		if idx[0] < last {
			t.Errorf("%q is out of order", h)
		}
		last = idx[0]
	}
	if strings.Contains(adr, "**却下した案**") {
		t.Error("ADR-0014 mixes ADR-0012's form of the rejected options into ADR-0009's")
	}
	if strings.Contains(adr, "一部を覆された") {
		t.Error("ADR-0014 carries the marker line that belongs to the ADRs it overrides")
	}
}

func TestADR0014SaysWhatThePinningIs(t *testing.T) {
	adr := read(t, "adr", adr14File)

	mustContain(t, "ADR-0014", adr,
		"ControllerRevision", "DefinitionsPinned", "DefinitionsLost", "FlowBroken", "ADR-0008")

	decision := section(t, adr, "**決定**:", "**なぜ**:")
	mustContain(t, "the 決定 of ADR-0014", decision, "finally", "ConfigMap", "Secret", "作り直")

	why := section(t, adr, "**なぜ**:", "**覆したもの**:")
	mustContain(t, "the なぜ of ADR-0014", why, "実行の同一性", "P5", "P7",
		"storedWorkflowSpec", "Tekton", "StatefulSet")

	overridden := section(t, adr, "**覆したもの**:", "**覆すには**:")
	mustContain(t, "the 覆したもの of ADR-0014", overridden,
		"ADR-0006", "ADR-0007", "ADR-0009", "ADR-0011")

	rejected := section(t, adr, "**やらなかったこと**:")
	n := 0
	for l := range strings.SplitSeq(rejected, "\n") {
		if strings.HasPrefix(l, "- ") {
			n++
		}
	}
	if n < 4 {
		t.Errorf("やらなかったこと has %d items, want 4", n)
	}
}

// New text names no code and no deployment: AGENTS.md keeps both out of ADRs.
func TestADR0014NamesNoImplementationAndNoDeployment(t *testing.T) {
	adr := read(t, "adr", adr14File)
	for _, name := range []string{
		"ensureSnapshot", "startedDefinitions", "flowFromCopy", "snapshotHandlers",
		"SnapshotRevisionName", "BuildSnapshotRevision", "brokenFlow", "handlerFor",
		"task_snapshot", "task_controller", "internal/", ".go",
		"home-cluster",
	} {
		if strings.Contains(adr, name) {
			t.Errorf("ADR-0014 names %q", name)
		}
	}
	if m := regexp.MustCompile(`(?:\.md|\.go|\b00\d\d):\d+`).FindString(adr); m != "" {
		t.Errorf("ADR-0014 cites a line number: %q", m)
	}
}

func TestADR0014LinksResolve(t *testing.T) {
	adr := read(t, "adr", adr14File)
	for _, m := range regexp.MustCompile(`\]\(([^)#]+\.md)(?:#[^)]*)?\)`).FindAllStringSubmatch(adr, -1) {
		if _, err := os.Stat(filepath.Join("..", "..", "docs", "adr", m[1])); err != nil {
			t.Errorf("ADR-0014 links to %q, which does not resolve", m[1])
		}
	}
}

func TestOverriddenADRsCarryTheMarkerAsTheLastHeaderBullet(t *testing.T) {
	for file, what := range map[string]string{
		"0007-no-resolved-spec-hashes.md":     "決定 3〜5 と未解決。決定 1・2 は残る",
		"0009-finally-after-the-ending.md":    "決定 6 の「dispatch 時点の flow から読む」と、決定 7 の表の 3 行",
		"0011-verdict-from-declared-state.md": "「ADR-0007 との関係」と、「覆すには」の外部依存の列挙",
	} {
		adr := read(t, "adr", file)
		want := markerKey + "（" + what + "）"
		if n := strings.Count(adr, "一部を覆された"); n != 1 {
			t.Errorf("%s has %d marker lines, want 1", file, n)
		}
		lines := headerBullets(t, adr)
		if got := lines[len(lines)-1]; got != want {
			t.Errorf("%s: the last bullet under the title is\n  %q\nwant\n  %q", file, got, want)
		}
		if !strings.Contains(adr, "- **status**: accepted") {
			t.Errorf("%s no longer has status accepted", file)
		}
	}
}

func TestOnlyTheThreeADRsAreMarked(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "..", "docs", "adr", "0*.md"))
	if err != nil {
		t.Fatal(err)
	}
	marked := map[string]bool{
		"0007-no-resolved-spec-hashes.md":     true,
		"0009-finally-after-the-ending.md":    true,
		"0011-verdict-from-declared-state.md": true,
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		name := filepath.Base(f)
		if has := strings.Contains(string(b), "一部を覆された"); has != marked[name] {
			t.Errorf("%s: marker present = %v, want %v", name, has, marked[name])
		}
	}
}

func TestREADMEListsADR0014AndTheMarkerRule(t *testing.T) {
	readme := read(t, "adr", "README.md")
	mustContain(t, "docs/adr/README.md", readme,
		"| [0014]("+adr14File+") | accepted | Task は開始時に写した定義で最後まで走る。新しい定義は作り直した Task から |")

	rules := section(t, readme, "- 決定文", "| #")
	found := false
	for l := range strings.SplitSeq(rules, "\n") {
		if strings.HasPrefix(l, "- ") && strings.Contains(l, "一部を覆された") && strings.Contains(l, "accepted") {
			found = true
		}
	}
	if !found {
		t.Error("docs/adr/README.md has no rule line that says a partly overridden ADR keeps accepted and gets 一部を覆された")
	}
}

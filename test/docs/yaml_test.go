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

// Package docs checks the YAML that docs/ shows against the CRDs the
// controller serves.
package docs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	repoRoot  = "../.."
	namespace = "docs"
)

var (
	k8sClient client.Client
	// group and kinds come from the CRDs the apiserver was given, so a kind
	// added later is checked without anyone listing it here.
	group string
	kinds = map[string]bool{}
)

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join(repoRoot, "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: firstEnvtestBinaryDir(),
	}
	cfg, err := env.Start()
	if err != nil {
		fmt.Fprintln(os.Stderr, "start envtest:", err)
		return 1
	}
	defer func() { _ = env.Stop() }()

	k8sClient, err = client.New(cfg, client.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		return 1
	}
	for _, crd := range env.CRDs {
		group = crd.Spec.Group
		kinds[crd.Spec.Names.Kind] = true
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
	if err := k8sClient.Create(context.Background(), ns); err != nil {
		fmt.Fprintln(os.Stderr, "namespace:", err)
		return 1
	}
	return m.Run()
}

func firstEnvtestBinaryDir() string {
	base := filepath.Join(repoRoot, "bin", "k8s")
	entries, err := os.ReadDir(base)
	if err != nil {
		return ""
	}
	for _, e := range entries {
		if e.IsDir() {
			return filepath.Join(base, e.Name())
		}
	}
	return ""
}

// block is one fenced code block whose language is yaml.
type block struct {
	where  string // file:line of the opening fence
	sketch bool
	body   string
}

var fence = regexp.MustCompile("^( *)(`{3,}|~{3,})(.*)$")

// yamlBlocks returns the yaml blocks of one Markdown file, with each block's
// lines shifted left by the indent of its opening fence (a block inside a
// list item is indented with it).
func yamlBlocks(name, src string) []block {
	var out []block
	lines := strings.Split(src, "\n")
	for i := 0; i < len(lines); i++ {
		m := fence.FindStringSubmatch(lines[i])
		if m == nil {
			continue
		}
		indent, marker, info := len(m[1]), m[2], strings.Fields(m[3])
		start := i
		var body []string
		for i++; i < len(lines); i++ {
			trimmed := strings.TrimSpace(lines[i])
			if strings.HasPrefix(trimmed, marker) && strings.Trim(trimmed, marker[:1]) == "" {
				break
			}
			line := lines[i]
			cut := min(indent, len(line)-len(strings.TrimLeft(line, " ")))
			body = append(body, line[cut:])
		}
		if len(info) == 0 || info[0] != "yaml" {
			continue
		}
		out = append(out, block{
			where:  fmt.Sprintf("%s:%d", name, start+1),
			sketch: slices.Contains(info[1:], "sketch"),
			body:   strings.Join(body, "\n"),
		})
	}
	return out
}

// objects decodes every document in a block and keeps the mappings; a list
// or a scalar is a fragment of something else. A block tagged yaml that is
// not YAML is an error whoever's object it shows.
func objects(b block) ([]map[string]any, error) {
	var out []map[string]any
	dec := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(b.body), 4096)
	for {
		var doc any
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if obj, ok := doc.(map[string]any); ok {
			out = append(out, obj)
		}
	}
}

// ours reports whether a decoded document claims to be one of the CRDs'
// objects: by its kind, or by an apiVersion in the CRDs' group. Either is
// enough, so a wrong apiVersion on one of our kinds and one of our
// apiVersions on a misspelt kind are both checked rather than skipped.
func ours(obj map[string]any) bool {
	if kind, _ := obj["kind"].(string); kinds[kind] {
		return true
	}
	apiVersion, _ := obj["apiVersion"].(string)
	gv, err := schema.ParseGroupVersion(apiVersion)
	return err == nil && gv.Group == group
}

// validate has the apiserver take obj as a create, without storing it.
// Strict field validation is the point: without it the apiserver prunes a
// field the schema does not know and the create succeeds.
func validate(ctx context.Context, where string, obj map[string]any) error {
	if _, ok := obj["apiVersion"].(string); !ok {
		return fmt.Errorf("%s: %v has no apiVersion; a reader cannot apply it as written", where, obj["kind"])
	}
	u := &unstructured.Unstructured{Object: obj}
	// A worked example may leave out the name; what it shows is the spec.
	if u.GetName() == "" && u.GetGenerateName() == "" {
		u.SetGenerateName("doc-")
	}
	u.SetNamespace(namespace)
	strict := client.FieldValidation(metav1.FieldValidationStrict)
	if err := k8sClient.Create(ctx, u, client.DryRunAll, strict); err != nil {
		return fmt.Errorf("%s: %w", where, err)
	}
	return nil
}

// check validates every object of ours in blocks and returns how many it
// validated and what it refused.
func check(ctx context.Context, blocks []block) (int, []error) {
	var checked int
	var errs []error
	for _, b := range blocks {
		if b.sketch {
			continue
		}
		objs, err := objects(b)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: not YAML: %w", b.where, err))
			continue
		}
		for _, obj := range objs {
			if !ours(obj) {
				continue
			}
			checked++
			if err := validate(ctx, b.where, obj); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return checked, errs
}

func docsBlocks(t *testing.T) []block {
	t.Helper()
	var blocks []block
	root := filepath.Join(repoRoot, "docs")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".md" {
			return err
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		blocks = append(blocks, yamlBlocks(filepath.ToSlash(rel), string(src))...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return blocks
}

func TestDocsYAMLIsWhatTheCRDsAccept(t *testing.T) {
	blocks := docsBlocks(t)
	checked, errs := check(t.Context(), blocks)
	for _, err := range errs {
		t.Error(err)
	}
	t.Logf("%d yaml blocks in docs/, %d objects of %s checked", len(blocks), checked, group)
	if checked == 0 {
		t.Fatalf("no %s object found in docs/; a check of nothing passes whatever docs/ says", group)
	}
}

// Each case is a way docs/ has been wrong before (#38) or a way this check
// would pass without checking: if the apiserver stopped refusing unknown
// fields, or the extraction stopped finding indented blocks, a case here
// fails instead of the docs check going quietly green.
func TestTheCheckRefuses(t *testing.T) {
	const flow = `apiVersion: flow.tgy.io/v1alpha1
kind: TaskFlow
spec:
  profile: investigate
  start: 調査
  bindings:
    調査: {handler: h, next: {おわり: ok}}
`
	cases := map[string]string{
		"an apiVersion of another group": "```yaml\n" +
			strings.Replace(flow, "flow.tgy.io/v1alpha1", "agent.tgy.io/v1alpha1", 1) + "```\n",
		"a version the CRDs do not serve": "```yaml\n" +
			strings.Replace(flow, "v1alpha1", "v1", 1) + "```\n",
		"a kind the group does not have": "```yaml\n" +
			strings.Replace(flow, "kind: TaskFlow", "kind: Taskflow", 1) + "```\n",
		"no apiVersion": "```yaml\n" +
			strings.Replace(flow, "apiVersion: flow.tgy.io/v1alpha1\n", "", 1) + "```\n",
		// The field the CRD prunes without an error unless validation is strict.
		"an unknown field": "```yaml\n" + flow + "  publishTo: s3://bucket\n```\n",
		"an unknown field in a binding": "```yaml\n" +
			strings.Replace(flow, "{handler: h,", "{handlers: [h],", 1) + "```\n",
		"a value outside an enum": "```yaml\n" +
			strings.Replace(flow, "investigate", "implement", 1) + "```\n",
		"a missing required field": "```yaml\n" + strings.Replace(flow, "  start: 調査\n", "", 1) + "```\n",
		"a block that is not YAML": "```yaml\nkind: TaskFlow\n  spec: [\n```\n",
		"a block inside a list item": "1. item\n\n   ```yaml\n" +
			indent(flow+"  publishTo: s3://bucket\n", "   ") + "   ```\n",
	}
	for name, md := range cases {
		t.Run(name, func(t *testing.T) {
			_, errs := check(t.Context(), yamlBlocks("case.md", md))
			if len(errs) == 0 {
				t.Fatalf("accepted:\n%s", md)
			}
			t.Log(errs)
		})
	}

	t.Run("nothing it should accept", func(t *testing.T) {
		md := "```yaml\n" + flow + "```\n"
		checked, errs := check(t.Context(), yamlBlocks("case.md", md))
		if checked != 1 || len(errs) != 0 {
			t.Fatalf("checked %d, refused %v; the refusals above mean nothing if this is refused too", checked, errs)
		}
	})
}

func TestTheCheckSkips(t *testing.T) {
	cases := map[string]string{
		"a sketch":             "```yaml sketch\nkind: TaskFlow\nspec: {handlers: [a, b]}\n```\n",
		"another group's kind": "```yaml\napiVersion: v1\nkind: ConfigMap\ndata: {a: b}\n```\n",
		"a fragment":           "```yaml\nnext: {報告: ok}\n```\n",
		"a list":               "```yaml\n- name: publish\n```\n",
		"another language":     "```sh\nkind: TaskFlow\n```\n",
	}
	for name, md := range cases {
		t.Run(name, func(t *testing.T) {
			checked, errs := check(t.Context(), yamlBlocks("case.md", md))
			if checked != 0 || len(errs) != 0 {
				t.Fatalf("checked %d, refused %v", checked, errs)
			}
		})
	}
}

func indent(s, prefix string) string {
	lines := strings.SplitAfter(s, "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = prefix + l
		}
	}
	return strings.Join(lines, "")
}

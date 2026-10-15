package archive_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/Tsuguya-HC/taskflow/internal/archive"
)

// Secret-shaped strings are assembled at run time so this file itself does
// not trip a scanner.
var (
	pemKey = "-----BEGIN " + "RSA PRIVATE KEY-----\nMIIB\n-----END " + "RSA PRIVATE KEY-----"
	akia   = "AKIA" + "IOSFODNN7EXAMPLE"
	jwt    = "eyJhbGciOiJIUzI1NiJ9" + "." + "eyJzdWIiOiIxMjM0NTYifQ" + "." + "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

const ruleAkia = "akia"

var rules = []struct {
	name string
	hit  string
	miss string
}{
	{"pem", pemKey, "-----BEGIN " + "PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----"},
	{ruleAkia, "id=" + akia, "AKIA" + "SHORT"},
	{"asia", "id=ASIA" + "IOSFODNN7EXAMPLE", "ASIA is a region"},
	{"ghp", "ghp_" + strings.Repeat("a", 36), "ghp_short"},
	{"gho", "gho_" + strings.Repeat("a", 36), "gho_short"},
	{"ghu", "ghu_" + strings.Repeat("a", 36), "ghu_short"},
	{"ghs", "ghs_" + strings.Repeat("a", 36), "ghs_short"},
	{"ghr", "ghr_" + strings.Repeat("a", 36), "ghr_short"},
	{"github_pat", "github_pat_" + strings.Repeat("A1", 20), "github_pat_"},
	{"xox", "xoxb-" + "123456789012-abcdefABCDEF", "xoxz-123456789012"},
	{"sk-ant", "sk-ant-" + "api03-" + strings.Repeat("x", 30), "task-ant-colony"},
	{"jwt", "token " + jwt, "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTYifQ"},
}

func TestInspectRules(t *testing.T) {
	for _, r := range rules {
		t.Run(r.name, func(t *testing.T) {
			if got := archive.Inspect([]byte("x\n" + r.hit + "\ny")); got == "" {
				t.Errorf("hit example not matched")
			}
			if got := archive.Inspect([]byte(r.miss)); got != "" {
				t.Errorf("miss example matched rule %q", got)
			}
		})
	}
}

type fakeS3 struct {
	mu     sync.Mutex
	puts   map[string]string
	failOn string
	srv    *httptest.Server
}

func newFakeS3(t *testing.T) *fakeS3 {
	f := &fakeS3{puts: map[string]string{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "only PUT", http.StatusMethodNotAllowed)
			return
		}
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.failOn != "" && strings.HasSuffix(r.URL.Path, f.failOn) {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		f.puts[strings.TrimPrefix(r.URL.Path, "/bkt/")] = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeS3) cfg(dir string) archive.Config {
	return archive.Config{
		Endpoint: strings.TrimPrefix(f.srv.URL, "http://"), Bucket: "bkt", Region: "us-east-1",
		AccessKey: "k", SecretKey: "s",
		TaskJSON: filepath.Join(dir, "task.json"), Results: filepath.Join(dir, "results"),
		KeyPrefix: "pre/ns/flow/uid",
	}
}

type withheldEntry struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

func manifest(t *testing.T, f *fakeS3) []withheldEntry {
	t.Helper()
	raw, ok := f.puts["pre/ns/flow/uid/manifest.json"]
	if !ok {
		t.Fatalf("manifest.json not PUT; puts: %v", keys(f.puts))
	}
	var m struct {
		Withheld []withheldEntry `json:"withheld"`
	}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	return m.Withheld
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func write(t *testing.T, p, s string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRunPutsFilesAndWithholds(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("must run as non-root: chmod 000 does not make a file unreadable for root")
	}
	dir := t.TempDir()
	write(t, filepath.Join(dir, "task.json"), `{"name":"t"}`)
	res := filepath.Join(dir, "results")
	write(t, filepath.Join(res, "run1/out/a.txt"), "hello")
	write(t, filepath.Join(res, "run1/secret.txt"), "key "+akia)
	write(t, filepath.Join(res, "run1/unreadable.txt"), "x")
	if err := os.Chmod(filepath.Join(res, "run1/unreadable.txt"), 0); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/proc/self/environ", filepath.Join(res, "run1/env")); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(filepath.Join(res, "run1/fifo"), 0o644); err != nil {
		t.Fatal(err)
	}

	f := newFakeS3(t)
	got, err := archive.Run(context.Background(), f.cfg(dir))
	if err != nil {
		t.Fatal(err)
	}
	if got != (archive.Result{Uploaded: 2, Withheld: 4}) {
		t.Errorf("result = %+v", got)
	}
	if f.puts["pre/ns/flow/uid/task.json"] != `{"name":"t"}` {
		t.Errorf("task.json put = %q", f.puts["pre/ns/flow/uid/task.json"])
	}
	if f.puts["pre/ns/flow/uid/results/run1/out/a.txt"] != "hello" {
		t.Errorf("a.txt not put; puts: %v", keys(f.puts))
	}
	for k, v := range f.puts {
		if strings.Contains(v, akia) && k != "" {
			t.Errorf("matched secret leaked in %s", k)
		}
		if strings.Contains(k, "run1/env") || strings.Contains(k, "unreadable") || strings.Contains(k, "fifo") {
			t.Errorf("withheld file was put: %s", k)
		}
	}
	reasons := map[string]string{}
	for _, w := range manifest(t, f) {
		reasons[w.Path] = w.Reason
	}
	want := map[string]string{
		"results/run1/env":            "symlink",
		"results/run1/unreadable.txt": "unreadable",
		"results/run1/secret.txt":     ruleAkia,
	}
	for p, r := range want {
		switch r {
		case ruleAkia:
			if got := reasons[p]; got == "" || got == "symlink" || got == "unreadable" {
				t.Errorf("manifest[%s] = %q, want a rule name", p, got)
			}
		default:
			if reasons[p] != r {
				t.Errorf("manifest[%s] = %q, want %q", p, reasons[p], r)
			}
		}
	}
	if reasons["results/run1/fifo"] == "" {
		t.Errorf("non-regular file not in manifest: %v", reasons)
	}
	if strings.Contains(f.puts["pre/ns/flow/uid/manifest.json"], akia) {
		t.Error("manifest carries the matched string")
	}
}

func TestRunWithholdsTaskJSON(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "task.json"), `{"env":"`+akia+`"}`)
	f := newFakeS3(t)
	cfg := f.cfg(dir)
	cfg.Results = ""
	got, err := archive.Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got != (archive.Result{Uploaded: 0, Withheld: 1}) {
		t.Errorf("result = %+v", got)
	}
	if _, ok := f.puts["pre/ns/flow/uid/task.json"]; ok {
		t.Error("task.json put despite matching a rule")
	}
	if m := manifest(t, f); len(m) != 1 || m[0].Path != "task.json" || m[0].Reason == "" {
		t.Errorf("manifest = %+v", m)
	}
}

func TestRunPutFailure(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, "task.json"), `{}`)
	write(t, filepath.Join(dir, "results/a.txt"), "a")
	f := newFakeS3(t)
	f.failOn = "a.txt"
	if _, err := archive.Run(context.Background(), f.cfg(dir)); err == nil {
		t.Fatal("PUT failure returned nil")
	}
}

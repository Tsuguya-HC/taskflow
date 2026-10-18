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
	"time"

	"github.com/Tsuguya-HC/taskflow/internal/archive"
)

const (
	reasonSymlink    = "symlink"
	reasonUnreadable = "unreadable"
)

// Secret-shaped strings are assembled at run time so this file itself does
// not trip a scanner.
var (
	pemKey = "-----BEGIN " + "RSA PRIVATE KEY-----\nMIIB\n-----END " + "RSA PRIVATE KEY-----"
	akia   = "AKIA" + "IOSFODNN7EXAMPLE"
	jwt    = "eyJhbGciOiJIUzI1NiJ9" + "." + "eyJzdWIiOiIxMjM0NTYifQ" + "." + "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

const ruleAkia = "akia"
const taskJSONName = "task.json"

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
		TaskJSON: filepath.Join(dir, taskJSONName), Results: filepath.Join(dir, "results"),
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
	write(t, filepath.Join(dir, taskJSONName), `{"name":"t"}`)
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
		"results/run1/env":            reasonSymlink,
		"results/run1/unreadable.txt": reasonUnreadable,
		"results/run1/secret.txt":     ruleAkia,
	}
	for p, r := range want {
		switch r {
		case ruleAkia:
			if got := reasons[p]; got == "" || got == reasonSymlink || got == reasonUnreadable {
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
	write(t, filepath.Join(dir, taskJSONName), `{"env":"`+akia+`"}`)
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
	if m := manifest(t, f); len(m) != 1 || m[0].Path != taskJSONName || m[0].Reason == "" {
		t.Errorf("manifest = %+v", m)
	}
}

func TestRunPutFailure(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, taskJSONName), `{}`)
	write(t, filepath.Join(dir, "results/a.txt"), "a")
	f := newFakeS3(t)
	f.failOn = "a.txt"
	if _, err := archive.Run(context.Background(), f.cfg(dir)); err == nil {
		t.Fatal("PUT failure returned nil")
	}
}

// The Lstat guard on the shelf has to cover task.json too: a link there
// would carry whatever the sidecar can read (its own env file, say) out as
// task.json. The target is a string no rule matches, so only the link check
// can hold it back.
func TestRunWithholdsTaskJSONSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "elsewhere")
	write(t, target, "not-matched-by-any-rule")
	if err := os.Symlink(target, filepath.Join(dir, taskJSONName)); err != nil {
		t.Fatal(err)
	}
	f := newFakeS3(t)
	cfg := f.cfg(dir)
	cfg.Results = ""
	if _, err := archive.Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if body, ok := f.puts["pre/ns/flow/uid/task.json"]; ok {
		t.Errorf("task.json symlink target was put: %q", body)
	}
	if m := manifest(t, f); len(m) != 1 || m[0].Path != taskJSONName || m[0].Reason != reasonSymlink {
		t.Errorf("manifest = %+v", m)
	}
}

// A link whose target does not exist cannot be resolved, but it is still a
// link pointing out of the directory: held back as a symlink, not a Run error.
func TestRunWithholdsDanglingTaskJSONSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "elsewhere", "nonexistent")
	if err := os.Symlink(target, filepath.Join(dir, taskJSONName)); err != nil {
		t.Fatal(err)
	}
	f := newFakeS3(t)
	cfg := f.cfg(dir)
	cfg.Results = ""
	res, err := archive.Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run returned error for a dangling task.json link: %v", err)
	}
	if res.Uploaded != 0 {
		t.Errorf("uploaded = %d, want 0", res.Uploaded)
	}
	if m := manifest(t, f); len(m) != 1 || m[0].Path != taskJSONName || m[0].Reason != reasonSymlink {
		t.Errorf("manifest = %+v", m)
	}
}

// The kubelet shows a ConfigMap key as <dir>/task.json -> ..data/task.json,
// ..data -> ..<timestamp>; the real file is a regular one inside <dir>. A
// link that stays inside the directory the controller handed over is sent.
func TestRunSendsConfigMapVolumeTaskJSON(t *testing.T) {
	dir := t.TempDir()
	stamp := "..2026_10_18_00_00_00.123456789"
	if err := os.Mkdir(filepath.Join(dir, stamp), 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, filepath.Join(dir, stamp, taskJSONName), `{"kind":"Task"}`)
	if err := os.Symlink(stamp, filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..data", taskJSONName), filepath.Join(dir, taskJSONName)); err != nil {
		t.Fatal(err)
	}
	f := newFakeS3(t)
	cfg := f.cfg(dir)
	cfg.Results = ""
	res, err := archive.Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if body := f.puts["pre/ns/flow/uid/task.json"]; body != `{"kind":"Task"}` {
		t.Errorf("task.json put = %q", body)
	}
	if res.Uploaded != 1 {
		t.Errorf("uploaded = %d", res.Uploaded)
	}
	if m := manifest(t, f); len(m) != 0 {
		t.Errorf("withheld = %+v", m)
	}
}

// Catches dropping the read error of task.json: an empty body would pass
// Inspect and be put as if it were the Task.
func TestRunMissingTaskJSONFails(t *testing.T) {
	dir := t.TempDir()
	f := newFakeS3(t)
	cfg := f.cfg(dir)
	cfg.Results = ""
	if _, err := archive.Run(context.Background(), cfg); err == nil {
		t.Fatal("a missing task.json returned nil")
	}
	if len(f.puts) != 0 {
		t.Errorf("put despite no task.json: %v", keys(f.puts))
	}
}

// Catches ignoring the read error of a task.json that exists but cannot be read.
func TestRunUnreadableTaskJSONFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("must run as non-root: chmod 000 does not make a file unreadable for root")
	}
	dir := t.TempDir()
	write(t, filepath.Join(dir, taskJSONName), `{}`)
	if err := os.Chmod(filepath.Join(dir, taskJSONName), 0); err != nil {
		t.Fatal(err)
	}
	f := newFakeS3(t)
	cfg := f.cfg(dir)
	cfg.Results = ""
	if _, err := archive.Run(context.Background(), cfg); err == nil {
		t.Fatal("an unreadable task.json returned nil")
	}
	if len(f.puts) != 0 {
		t.Errorf("put despite unreadable task.json: %v", keys(f.puts))
	}
}

// Catches ignoring the walk error: a directory that cannot be listed must be
// recorded, not silently skipped.
func TestRunWithholdsUnreadableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("must run as non-root: chmod 000 does not make a directory unreadable for root")
	}
	dir := t.TempDir()
	write(t, filepath.Join(dir, taskJSONName), `{}`)
	write(t, filepath.Join(dir, "results/locked/a.txt"), "a")
	locked := filepath.Join(dir, "results/locked")
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	f := newFakeS3(t)
	if _, err := archive.Run(context.Background(), f.cfg(dir)); err != nil {
		t.Fatal(err)
	}
	m := manifest(t, f)
	if len(m) != 1 || m[0].Path != "results/locked" || m[0].Reason != reasonUnreadable {
		t.Errorf("manifest = %+v", m)
	}
}

// A FIFO at task.json would block os.ReadFile until a writer shows up, which
// would stop the archive Job from ever settling.
func TestRunWithholdsNonRegularTaskJSON(t *testing.T) {
	f := newFakeS3(t)
	dir := t.TempDir()
	cfg := f.cfg(dir)
	cfg.Results = ""
	if err := syscall.Mkfifo(cfg.TaskJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	// Releases a blocked open so a failing run does not leak its goroutine.
	t.Cleanup(func() {
		if fd, err := syscall.Open(cfg.TaskJSON, syscall.O_RDWR|syscall.O_NONBLOCK, 0); err == nil {
			_ = syscall.Close(fd)
		}
	})
	type out struct {
		res archive.Result
		err error
	}
	done := make(chan out, 1)
	go func() {
		res, err := archive.Run(context.Background(), cfg)
		done <- out{res, err}
	}()
	select {
	case o := <-done:
		if o.err != nil {
			t.Fatalf("Run: %v", o.err)
		}
		if o.res.Withheld != 1 || o.res.Uploaded != 0 {
			t.Errorf("result = %+v, want task.json withheld", o.res)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return for a FIFO task.json")
	}
}

// A failing PUT must surface promptly: the failure has to reach the
// termination log within the Pod's grace period, so the client's own retry
// backoff may not stretch it. Measured before the fix: 7.6-8.2s for three
// PUTs against a store that answers 500 to everything.
func TestRunPutFailureIsNotDelayedByRetries(t *testing.T) {
	dir := t.TempDir()
	write(t, filepath.Join(dir, taskJSONName), `{}`)
	write(t, filepath.Join(dir, "results/a.txt"), "a")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	cfg := newFakeS3(t).cfg(dir)
	cfg.Endpoint = strings.TrimPrefix(srv.URL, "http://")

	start := time.Now()
	if _, err := archive.Run(context.Background(), cfg); err == nil {
		t.Fatal("PUT failure returned nil")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("Run took %s on failing PUTs, want under 2s", took)
	}
}

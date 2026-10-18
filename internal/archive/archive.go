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

// Package archive sends an ended Task's record to S3-compatible storage.
package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config is what `sidecar archive` is given.
type Config struct {
	// Endpoint is host:port; Secure selects https.
	Endpoint  string
	Secure    bool
	Bucket    string
	Region    string
	AccessKey string
	SecretKey string
	// TaskJSON is the Task's JSON file. Results is the shelf directory, or
	// empty when the flow has no workspace claim.
	TaskJSON string
	Results  string
	// KeyPrefix is the key under which everything is put.
	KeyPrefix string
}

// Result is the termination message's content.
type Result struct {
	Uploaded int `json:"uploaded"`
	Withheld int `json:"withheld"`
}

var rules = []struct {
	name string
	re   *regexp.Regexp
}{
	{"pem-private-key", regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`)},
	{"aws-key-id", regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`)},
	{"github-token", regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{36,}`)},
	{"github-pat", regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}`)},
	{"slack-token", regexp.MustCompile(`\bxox[abpr]-[0-9A-Za-z-]{10,}`)},
	{"anthropic-key", regexp.MustCompile(`\bsk-ant-[A-Za-z0-9_-]{20,}`)},
	{"jwt", regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+`)},
}

// Inspect returns the name of the first fixed rule content matches, or ""
// when none does.
func Inspect(content []byte) string {
	for _, r := range rules {
		if r.re.Match(content) {
			return r.name
		}
	}
	return ""
}

type withheld struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type putter struct {
	ctx    context.Context
	client *minio.Client
	bucket string
	failed []error
	result Result
}

// put is a single sized PUT; a failure is kept and the rest still goes, so
// one bad key does not hide what else could have been sent.
func (p *putter) put(key string, body []byte) bool {
	_, err := p.client.PutObject(p.ctx, p.bucket, key, bytes.NewReader(body), int64(len(body)),
		minio.PutObjectOptions{ContentType: "application/octet-stream", DisableContentSha256: true})
	if err != nil {
		p.failed = append(p.failed, fmt.Errorf("put %s: %w", key, err))
		return false
	}
	return true
}

// resolveTaskJSON returns the path to open for task.json. A kubelet ConfigMap
// volume shows every key as a symlink (task.json -> ..data/task.json), so a
// symlink is followed only while it stays inside the directory it was handed
// in; anything pointing out of it comes back as the "symlink" reason.
func resolveTaskJSON(p string) (string, string, error) {
	info, err := os.Lstat(p)
	if err != nil {
		return "", "", err
	}
	if info.Mode()&fs.ModeSymlink == 0 {
		return p, "", nil
	}
	dir, err := filepath.EvalSymlinks(filepath.Dir(p))
	if err != nil {
		return "", "", err
	}
	resolved, err := filepath.EvalSymlinks(p)
	if err != nil {
		// A dangling or looping link points at nothing inside the directory.
		return "", "symlink", nil
	}
	rel, err := filepath.Rel(dir, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "symlink", nil
	}
	return resolved, "", nil
}

// Run puts task.json, the shelf's regular files and manifest.json.
func Run(ctx context.Context, cfg Config) (Result, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure:       cfg.Secure,
		Region:       cfg.Region,
		BucketLookup: minio.BucketLookupPath,
		// A failed PUT must reach the exit code and termination message before
		// the pod's grace period ends; the default 10 retries back off for seconds.
		MaxRetries: 1,
	})
	if err != nil {
		return Result{}, err
	}
	p := &putter{ctx: ctx, client: client, bucket: cfg.Bucket}
	var held []withheld
	hold := func(rel, reason string) {
		held = append(held, withheld{Path: rel, Reason: reason})
	}

	// Files are sent whole: the size is known before the PUT, which is what
	// lets a single PUT stand in for multipart.
	taskPath, state, err := resolveTaskJSON(cfg.TaskJSON)
	if err != nil {
		return Result{}, fmt.Errorf("read task json: %w", err)
	}
	if state != "" {
		hold("task.json", state)
	} else {
		// The type is judged on the descriptor that is read, not on the path.
		// O_NONBLOCK keeps a FIFO from blocking the open.
		f, err := os.OpenFile(taskPath, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return Result{}, fmt.Errorf("read task json: %w", err)
		}
		defer func() { _ = f.Close() }()
		info, err := f.Stat()
		if err != nil {
			return Result{}, fmt.Errorf("read task json: %w", err)
		}
		if !info.Mode().IsRegular() {
			hold("task.json", "not-regular")
		} else {
			body, err := io.ReadAll(f)
			if err != nil {
				return Result{}, fmt.Errorf("read task json: %w", err)
			}
			if rule := Inspect(body); rule != "" {
				hold("task.json", rule)
			} else if p.put(path.Join(cfg.KeyPrefix, "task.json"), body) {
				p.result.Uploaded++
			}
		}
	}

	if cfg.Results != "" {
		err := filepath.WalkDir(cfg.Results, func(full string, d fs.DirEntry, werr error) error {
			rel, rerr := filepath.Rel(cfg.Results, full)
			if rerr != nil {
				return rerr
			}
			if rel == "." {
				return werr
			}
			rel = path.Join("results", filepath.ToSlash(rel))
			if werr != nil {
				hold(rel, "unreadable")
				return nil
			}
			if d.IsDir() {
				return nil
			}
			info, ierr := os.Lstat(full)
			switch {
			case ierr != nil:
				hold(rel, "unreadable")
			case info.Mode()&fs.ModeSymlink != 0:
				hold(rel, "symlink")
			case !info.Mode().IsRegular():
				hold(rel, "not-regular")
			default:
				content, rerr := os.ReadFile(full)
				if rerr != nil {
					hold(rel, "unreadable")
				} else if rule := Inspect(content); rule != "" {
					hold(rel, rule)
				} else if p.put(path.Join(cfg.KeyPrefix, rel), content) {
					p.result.Uploaded++
				}
			}
			return nil
		})
		if err != nil {
			return p.result, fmt.Errorf("walk results: %w", err)
		}
	}

	if held == nil {
		held = []withheld{}
	}
	manifest, err := json.Marshal(struct {
		Withheld []withheld `json:"withheld"`
	}{held})
	if err != nil {
		return p.result, err
	}
	p.put(path.Join(cfg.KeyPrefix, "manifest.json"), manifest)
	p.result.Withheld = len(held)
	return p.result, errors.Join(p.failed...)
}

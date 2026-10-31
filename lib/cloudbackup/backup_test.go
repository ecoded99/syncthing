// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package cloudbackup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"golang.org/x/oauth2"
)

const testDevice = "DEVICE"

func fixture(t *testing.T) config.FolderConfiguration {
	t.Helper()
	src := filepath.Join(t.TempDir(), "source")
	os.MkdirAll(filepath.Join(src, ".stfolder"), 0700)
	return config.FolderConfiguration{ID: "fixture", Path: src, FilesystemType: config.FilesystemTypeBasic, CloudBackup: config.CloudBackupConfiguration{Enabled: true}}
}
func write(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}
func run(t *testing.T, d *fakeDrive, f config.FolderConfiguration) Result {
	t.Helper()
	r, err := Backup(context.Background(), d.client(), f, testDevice, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func current(t *testing.T, d *fakeDrive, r Result, rel string) string {
	t.Helper()
	file := d.lookup(r.RootID, "Current/"+rel)
	if file == nil {
		t.Fatalf("%s missing from Drive", rel)
	}
	return string(file.data)
}

func TestOneWayVersionsAndIgnores(t *testing.T) {
	d := newFakeDrive(t)
	f := fixture(t)
	write(t, filepath.Join(f.Path, ".stignore"), "node_modules\n.playwright-cli/traces\n")
	for _, name := range []string{"file", "nested/file", ".hidden", "node_modules/dep"} {
		write(t, filepath.Join(f.Path, name), "original")
	}
	write(t, filepath.Join(f.Path, ".playwright-cli/traces/test"), "excluded")
	stamp := time.Unix(1700000000, 123456700)
	os.Chtimes(filepath.Join(f.Path, "file"), stamp, stamp)
	r := run(t, d, f)
	for _, name := range []string{"file", "nested/file", ".hidden", "node_modules/dep"} {
		if current(t, d, r, name) != "original" {
			t.Fatal(name)
		}
	}
	if d.lookup(r.RootID, "Current/.playwright-cli/traces/test") != nil || d.lookup(r.RootID, "Current/.stfolder") != nil {
		t.Fatal("excluded content uploaded")
	}
	if got := d.lookup(r.RootID, "Current/file").modified(); !got.Equal(stamp.Truncate(time.Millisecond)) {
		t.Fatal("mtime not preserved", got)
	}
	if again := run(t, d, f); again.Files != 0 || again.RootID != r.RootID {
		t.Fatal("unchanged files uploaded or root not reused", again)
	}
	write(t, filepath.Join(f.Path, "file"), "changed")
	os.Remove(filepath.Join(f.Path, "nested/file"))
	run(t, d, f)
	if current(t, d, r, "file") != "changed" || current(t, d, r, "nested/file") != "original" {
		t.Fatal("one-way retention failed")
	}
	if v := d.versions(r.RootID, "file"); len(v) != 1 || v[0] != "original" {
		t.Fatal("previous content not retained in Versions", v)
	}
}

func TestEqualSizeAndTimeChangeIsDetected(t *testing.T) {
	d := newFakeDrive(t)
	f := fixture(t)
	path := filepath.Join(f.Path, "file")
	stamp := time.Unix(1700000000, 0)
	write(t, path, "aaaa")
	os.Chtimes(path, stamp, stamp)
	r := run(t, d, f)
	write(t, path, "bbbb")
	os.Chtimes(path, stamp, stamp)
	if run(t, d, f).Files != 1 || current(t, d, r, "file") != "bbbb" {
		t.Fatal("content change with equal size and mtime was missed")
	}
}

func TestResumableUpload(t *testing.T) {
	oldThreshold, oldChunk := resumableThreshold, resumableChunk
	resumableThreshold, resumableChunk = 1024, 1000
	defer func() { resumableThreshold, resumableChunk = oldThreshold, oldChunk }()
	d := newFakeDrive(t)
	f := fixture(t)
	data := strings.Repeat("0123456789", 350)
	write(t, filepath.Join(f.Path, "large"), data)
	r := run(t, d, f)
	if current(t, d, r, "large") != data {
		t.Fatal("chunked upload corrupted content")
	}
	write(t, filepath.Join(f.Path, "large"), data+"x")
	run(t, d, f)
	if current(t, d, r, "large") != data+"x" || len(d.versions(r.RootID, "large")) != 1 {
		t.Fatal("chunked update failed")
	}
}

func TestRootsAreSeparatedByDeviceAndFolder(t *testing.T) {
	d := newFakeDrive(t)
	f := fixture(t)
	write(t, filepath.Join(f.Path, "file"), "x")
	a := run(t, d, f)
	other, err := Backup(context.Background(), d.client(), f, "OTHER", nil)
	if err != nil {
		t.Fatal(err)
	}
	if other.RootID == a.RootID {
		t.Fatal("different devices share a backup root")
	}
	d.mu.Lock()
	name := d.files[a.RootID].Name
	d.mu.Unlock()
	if name != "Syncthing backup - fixture" {
		t.Fatal("unexpected default folder name", name)
	}
}

func TestTypeConflictRetainsRemote(t *testing.T) {
	d := newFakeDrive(t)
	f := fixture(t)
	write(t, filepath.Join(f.Path, "entry"), "file")
	r := run(t, d, f)
	os.Remove(filepath.Join(f.Path, "entry"))
	write(t, filepath.Join(f.Path, "entry/child"), "nested")
	if _, err := Backup(context.Background(), d.client(), f, testDevice, nil); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatal("type conflict not reported", err)
	}
	if current(t, d, r, "entry") != "file" {
		t.Fatal("conflicting remote file replaced")
	}
}

func TestChecksumMismatchIsReported(t *testing.T) {
	d := newFakeDrive(t)
	d.corrupt = true
	f := fixture(t)
	write(t, filepath.Join(f.Path, "file"), "x")
	if _, err := Backup(context.Background(), d.client(), f, testDevice, nil); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatal("checksum mismatch not reported", err)
	}
}

func TestQuotaIsChecked(t *testing.T) {
	d := newFakeDrive(t)
	d.limit = 4
	f := fixture(t)
	write(t, filepath.Join(f.Path, "file"), "too large")
	if _, err := Backup(context.Background(), d.client(), f, testDevice, nil); err == nil || !strings.Contains(err.Error(), "quota") {
		t.Fatal("quota not enforced", err)
	}
}

func TestSourceValidation(t *testing.T) {
	f := fixture(t)
	os.RemoveAll(filepath.Join(f.Path, ".stfolder"))
	if _, err := ValidateSource(f); err == nil {
		t.Fatal("missing marker accepted")
	}
	f = fixture(t)
	f.Type = config.FolderTypeReceiveEncrypted
	if _, err := ValidateSource(f); err == nil {
		t.Fatal("encrypted folder accepted")
	}
}

func fastPace(t *testing.T) {
	t.Helper()
	oldMin, oldMax := paceMin, paceMax
	paceMin, paceMax = time.Millisecond, 4*time.Millisecond
	t.Cleanup(func() { paceMin, paceMax = oldMin, oldMax })
}

func TestUploadsRetryRateLimits(t *testing.T) {
	fastPace(t)
	d := newFakeDrive(t)
	d.failUploads = 3
	f := fixture(t)
	write(t, filepath.Join(f.Path, "a"), "first")
	write(t, filepath.Join(f.Path, "b"), "second")
	r := run(t, d, f)
	if r.Failed != 0 || current(t, d, r, "a") != "first" || current(t, d, r, "b") != "second" {
		t.Fatal("rate-limited uploads were not retried", r)
	}
}

func TestUploadRetriesAreBounded(t *testing.T) {
	fastPace(t)
	d := newFakeDrive(t)
	d.failUploads = 1000
	f := fixture(t)
	write(t, filepath.Join(f.Path, "file"), "x")
	r, err := Backup(context.Background(), d.client(), f, testDevice, nil)
	if err == nil || r.Failed != 1 || d.uploads != uploadAttempts+1 {
		t.Fatal("unexpected retry behaviour", err, r.Failed, d.uploads)
	}
}

func TestPermanentUploadErrorIsNotRetried(t *testing.T) {
	fastPace(t)
	d := newFakeDrive(t)
	d.failUploads, d.failStatus = 1, http.StatusBadRequest
	f := fixture(t)
	write(t, filepath.Join(f.Path, "file"), "x")
	r, err := Backup(context.Background(), d.client(), f, testDevice, nil)
	if err == nil || r.Failed != 1 || d.uploads != 1 {
		t.Fatal("permanent error retried or not reported", err, r.Failed, d.uploads)
	}
}

func TestFailedCountBeyondReportedErrors(t *testing.T) {
	fastPace(t)
	d := newFakeDrive(t)
	d.failUploads, d.failStatus = 1000, http.StatusBadRequest
	f := fixture(t)
	for i := range 12 {
		write(t, filepath.Join(f.Path, fmt.Sprint("file", i)), "x")
	}
	r, err := Backup(context.Background(), d.client(), f, testDevice, nil)
	if r.Failed != 12 || err == nil || !strings.Contains(err.Error(), "2 more errors") {
		t.Fatal("failure count not reported", r.Failed, err)
	}
}

func TestRetryableClassification(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{&driveError{Status: http.StatusTooManyRequests}, true},
		{&driveError{Status: http.StatusBadGateway}, true},
		{&driveError{Status: http.StatusForbidden, Body: "userRateLimitExceeded"}, true},
		{&driveError{Status: http.StatusForbidden, Body: "insufficientPermissions"}, false},
		{&driveError{Status: http.StatusBadRequest}, false},
		{&url.Error{Op: "Post", URL: "u", Err: io.ErrUnexpectedEOF}, true},
		{&url.Error{Op: "Post", URL: "u", Err: context.Canceled}, false},
		{&url.Error{Op: "Post", URL: "u", Err: errPrimaryBusy}, false},
		{&url.Error{Op: "Post", URL: "u", Err: fmt.Errorf("auth: %w", &oauth2.RetrieveError{})}, false},
		{errors.New("source changed during backup"), false},
	} {
		if got := retryable(c.err); got != c.want {
			t.Errorf("retryable(%v) = %v", c.err, got)
		}
	}
}

func TestPacerBacksOffAndRecovers(t *testing.T) {
	p := &pacer{}
	for range 10 {
		p.failed()
	}
	if p.delay != paceMax {
		t.Fatal("backoff not capped", p.delay)
	}
	for range 7 {
		p.succeeded()
	}
	if p.delay != 0 {
		t.Fatal("pacer did not recover", p.delay)
	}
}

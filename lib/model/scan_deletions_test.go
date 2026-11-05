// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package model

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	stfs "github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
)

type deletionTestFS struct {
	stfs.Filesystem
	lstat func(string) (stfs.FileInfo, error)
}

func (f deletionTestFS) Lstat(name string) (stfs.FileInfo, error) { return f.lstat(name) }

func TestScanDeletionSafety(t *testing.T) {
	filesystem := stfs.NewFilesystem(stfs.FilesystemTypeFake, t.TempDir())
	if err := filesystem.MkdirAll("dir", 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"live", "dir/child", "was-file"} {
		file, err := filesystem.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		file.Close()
	}
	if err := filesystem.CreateSymlink("dir", "link"); err != nil {
		t.Fatal(err)
	}
	errorsFS := deletionTestFS{Filesystem: filesystem, lstat: func(name string) (stfs.FileInfo, error) {
		if name == "denied" {
			return nil, fs.ErrPermission
		}
		if name == "wrong-case" {
			return nil, &stfs.CaseConflictError{Given: name, Real: "Wrong-Case"}
		}
		return filesystem.Lstat(name)
	}}
	matcher := ignore.New(errorsFS)
	if err := matcher.Parse(strings.NewReader("ignored\n"), ".stignore"); err != nil {
		t.Fatal(err)
	}
	files := []protocol.FileInfo{
		{Name: "live"}, {Name: "missing"}, {Name: "link"}, {Name: "link/child"}, {Name: "dir/child"},
		{Name: "was-file/child"}, {Name: "denied"}, {Name: "wrong-case"}, {Name: "ignored"},
		{Name: "old-deletion", Deleted: true}, {Name: "unsupported", LocalFlags: protocol.FlagLocalUnsupported},
		{Name: "previously-ignored", LocalFlags: protocol.FlagLocalIgnored},
	}
	// Cross multiple batch boundaries while preserving the special cases.
	for i := range 3 * scanDeletionBatchSize {
		files = append(files, protocol.FileInfo{Name: fmt.Sprintf("missing-%04d", i)})
	}
	for _, workers := range []int{1, 2, 4, 8} {
		var names []string
		for result, err := range scanDeletionChecks(t.Context(), slices.Values(files), func() error { return nil }, errorsFS, matcher, workers) {
			if err != nil {
				t.Fatal(err)
			}
			names = append(names, result.file.Name)
			if result.check && result.deleted != osutil.IsDeleted(errorsFS, result.file.Name) {
				t.Errorf("incorrect deletion decision: %+v", result)
			}
			if result.file.Name == "denied" && result.deleted {
				t.Fatal("permission error treated as deletion")
			}
			if result.file.Name == "ignored" && (!result.ignored || result.check) {
				t.Fatal("ignored item checked")
			}
			if (result.file.Name == "old-deletion" || result.file.Name == "unsupported") && result.check {
				t.Fatal("inactive item checked")
			}
		}
		for i, name := range names {
			if name != files[i].Name {
				t.Fatal("database order changed")
			}
		}
		if len(names) != len(files) {
			t.Fatal("lost input")
		}
	}
}

func TestScanDeletionBoundedAndCancelled(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("concurrency requires two processors")
	}
	filesystem := stfs.NewFilesystem(stfs.FilesystemTypeFake, t.TempDir())
	entered, release := make(chan struct{}, scanDeletionBatchSize), make(chan struct{})
	var active, peak, produced atomic.Int32
	filesystem = deletionTestFS{Filesystem: filesystem, lstat: func(string) (stfs.FileInfo, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		return nil, fs.ErrNotExist
	}}
	source := iter.Seq[protocol.FileInfo](func(yield func(protocol.FileInfo) bool) {
		for i := range 10000 {
			produced.Add(1)
			if !yield(protocol.FileInfo{Name: fmt.Sprint(i)}) {
				return
			}
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		for _, err := range scanDeletionChecks(ctx, source, func() error { return nil }, filesystem, ignore.New(filesystem), 4) {
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	workers := min(4, runtime.GOMAXPROCS(0))
	for range workers {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			cancel()
			close(release)
			t.Fatal("filesystem reads did not run concurrently")
		}
	}
	if n := produced.Load(); n > scanDeletionBatchSize {
		cancel()
		close(release)
		t.Fatalf("unbounded lookahead: %d", n)
	}
	cancel()
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled scan leaked workers")
	}
	if active.Load() != 0 || peak.Load() > int32(workers) {
		t.Fatalf("worker limit/leak: active=%d peak=%d", active.Load(), peak.Load())
	}
}

func TestScanDeletionIteratorErrorAndEarlyStop(t *testing.T) {
	filesystem := stfs.NewFilesystem(stfs.FilesystemTypeFake, t.TempDir())
	var count int
	sentinel := errors.New("index read failed")
	for _, err := range scanDeletionChecks(t.Context(), slices.Values([]protocol.FileInfo{{Name: "missing"}}), func() error { return sentinel }, filesystem, ignore.New(filesystem), 4) {
		if err != nil {
			if !errors.Is(err, sentinel) {
				t.Fatal(err)
			}
			break
		}
		count++
	}
	if count != 1 {
		t.Fatal("partial index result was lost")
	}
	closed := false
	source := iter.Seq[protocol.FileInfo](func(yield func(protocol.FileInfo) bool) {
		defer func() { closed = true }()
		for i := range 1000 {
			if !yield(protocol.FileInfo{Name: fmt.Sprint(i)}) {
				return
			}
		}
	})
	for _, err := range scanDeletionChecks(t.Context(), source, func() error { return sentinel }, filesystem, ignore.New(filesystem), 4) {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	if !closed {
		t.Fatal("early stop did not close source iterator")
	}
}

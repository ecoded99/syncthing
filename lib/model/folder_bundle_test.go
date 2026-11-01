// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

type timestampBeforeRenameFS struct {
	fs.Filesystem
	t       *testing.T
	want    time.Time
	checked bool
}

func (f *timestampBeforeRenameFS) Rename(from, to string) error {
	info, err := f.Filesystem.Lstat(from)
	must(f.t, err)
	if !info.ModTime().Equal(f.want) {
		f.t.Fatal("live rename would expose temporary timestamp", info.ModTime(), f.want)
	}
	f.checked = true
	return f.Filesystem.Rename(from, to)
}

func TestBundleFinishTimestampBeforeRename(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	file := protocol.FileInfo{Name: "new", Size: 4, Permissions: 0o644, ModifiedS: 1700000000}
	checked := &timestampBeforeRenameFS{Filesystem: f.mtimefs, t: t, want: file.ModTime()}
	f.mtimefs = checked
	s, err := f.stageBundleFile(t.Context(), file, []byte("data"))
	must(t, err)
	_, err = s.finalClose()
	must(t, err)
	must(t, f.performFinish(file, s.curFile, s.hasCurFile, s.tempName, make(chan dbUpdateJob, 1), make(chan string, 1)))
	if !checked.checked {
		t.Fatal("rename checkpoint not exercised")
	}
}

func TestBundleStagingRechecksParent(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	outside := t.TempDir()
	if err := f.mtimefs.CreateSymlink(outside, "parent"); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	s, err := f.stageBundleFile(t.Context(), protocol.FileInfo{Name: filepath.Join("parent", "new"), Size: 4}, []byte("data"))
	if err == nil || s != nil {
		t.Fatal("staged through changed symlink parent")
	}
	entries, err := os.ReadDir(outside)
	must(t, err)
	if len(entries) != 0 {
		t.Fatal("created file outside sync root")
	}
}

func TestBundleStagingPreservesExistingTemp(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	file := protocol.FileInfo{Name: "new", Size: 4, Permissions: 0o644}
	name := fs.TempName(file.Name)
	writeFile(t, f.mtimefs, name, []byte("resumable content"))
	s, err := f.stageBundleFile(t.Context(), file, []byte("data"))
	must(t, err)
	if s.failed() == nil {
		t.Fatal("replaced existing temporary file")
	}
	_, closeErr := s.finalClose()
	if closeErr == nil {
		t.Fatal("temporary collision marked successful")
	}
	fd, err := f.mtimefs.Open(name)
	must(t, err)
	defer fd.Close()
	data, err := io.ReadAll(fd)
	must(t, err)
	if string(data) != "resumable content" {
		t.Fatal("destroyed resumable content")
	}
}

func TestParallelBundleStaging(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			_, f := setupSendReceiveFolder(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if canceled {
				cancel()
			}
			in := make(chan downloadedBundle, 1)
			bundle := downloadedBundle{}
			for i := range protocol.BundleMaxFiles {
				bundle.files = append(bundle.files, protocol.FileInfo{Name: fmt.Sprintf("file-%d", i), Size: 4, Permissions: 0o644, ModifiedS: 1700000000})
				bundle.results = append(bundle.results, protocol.BundleResult{Data: []byte("data")})
			}
			in <- bundle
			close(in)
			copies := make(chan copyBlocksState, protocol.BundleMaxFiles)
			finished := make(chan *sharedPullerState, protocol.BundleMaxFiles)
			f.stageDownloadedBundles(ctx, in, copies, finished)
			if len(copies) != 0 {
				t.Fatal("verified members went through ordinary copier")
			}
			if canceled {
				if len(finished) != 0 {
					t.Fatal("cancellation staged files")
				}
				return
			}
			if len(finished) != protocol.BundleMaxFiles {
				t.Fatal("lost staged members", len(finished))
			}
			db := make(chan dbUpdateJob, protocol.BundleMaxFiles)
			scans := make(chan string, protocol.BundleMaxFiles)
			for range protocol.BundleMaxFiles {
				s := <-finished
				f.model.progressEmitter.Deregister(s)
				if _, err := f.mtimefs.Lstat(s.file.Name); !fs.IsNotExist(err) {
					t.Fatal("staging touched live file", err)
				}
				closed, err := s.finalClose()
				must(t, err)
				if !closed || !s.fsync {
					t.Fatal("durability bypassed")
				}
				must(t, f.performFinish(s.file, s.curFile, s.hasCurFile, s.tempName, db, scans))
				fd, err := f.mtimefs.Open(s.file.Name)
				must(t, err)
				data, err := io.ReadAll(fd)
				fd.Close()
				must(t, err)
				if string(data) != "data" {
					t.Fatal("corrupt parallel staging", string(data))
				}
			}
			if len(db) != protocol.BundleMaxFiles || len(scans) != 0 {
				t.Fatal("incorrect apply/index updates")
			}
		})
	}
}

func TestBundleEligibility(t *testing.T) {
	h := sha256.Sum256([]byte("data"))
	file := protocol.FileInfo{Name: "new", Size: 4, Blocks: []protocol.BlockInfo{{Size: 4, Hash: h[:]}}}
	if !bundleFileEligible(file) {
		t.Fatal("initial single-block file rejected")
	}
	for name, change := range map[string]func(*protocol.FileInfo){
		"deleted":    func(f *protocol.FileInfo) { f.Deleted = true },
		"directory":  func(f *protocol.FileInfo) { f.Type = protocol.FileInfoTypeDirectory },
		"encrypted":  func(f *protocol.FileInfo) { f.Encrypted = []byte{1} },
		"large":      func(f *protocol.FileInfo) { f.Size = protocol.BundleMaxFileSize + 1 },
		"partial":    func(f *protocol.FileInfo) { f.Size++ },
		"multiblock": func(f *protocol.FileInfo) { f.Blocks = append(f.Blocks, f.Blocks[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			f := file
			change(&f)
			if bundleFileEligible(f) {
				t.Fatal("unsafe bundle candidate accepted")
			}
		})
	}
}

func TestBundleStagingPreservesLocalEdits(t *testing.T) {
	for _, localEdit := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial-copy", true: "raced-local-edit"}[localEdit], func(t *testing.T) {
			_, f := setupSendReceiveFolder(t)
			file := protocol.FileInfo{Name: "new", Size: 4, Permissions: 0o644, ModifiedS: 1700000000}
			s, err := f.stageBundleFile(t.Context(), file, []byte("data"))
			must(t, err)
			if _, err := f.mtimefs.Lstat(file.Name); !fs.IsNotExist(err) {
				t.Fatal("staging touched live path", err)
			}
			if localEdit {
				writeFile(t, f.mtimefs, file.Name, []byte("local edit"))
			}
			closed, err := s.finalClose()
			must(t, err)
			if !closed || !s.fsync {
				t.Fatal("staged file did not use durable finalization")
			}
			db := make(chan dbUpdateJob, 1)
			scans := make(chan string, 1)
			err = f.performFinish(file, s.curFile, s.hasCurFile, s.tempName, db, scans)
			want := "data"
			if localEdit {
				want = "local edit"
				if !errors.Is(err, errModified) || len(db) != 0 || len(scans) != 1 {
					t.Fatal("local edit overwritten or not scheduled for scan", err)
				}
			} else {
				must(t, err)
				if len(db) != 1 {
					t.Fatal("missing index update")
				}
			}
			fd, err := f.mtimefs.Open(file.Name)
			must(t, err)
			data, err := io.ReadAll(fd)
			fd.Close()
			must(t, err)
			if string(data) != want {
				t.Fatal(string(data), want)
			}
		})
	}
}

func TestBundleFallbackAndCancellation(t *testing.T) {
	m, f := setupSendReceiveFolder(t)
	h := sha256.Sum256([]byte("data"))
	file := protocol.FileInfo{Name: "new", Size: 4, Blocks: []protocol.BlockInfo{{Size: 4, Hash: h[:]}}}
	// A connection without the extension must fall back into the normal
	// copier, rather than losing queue entries or marking anything complete.
	conn := newFakeConnection(device1, m)
	in := make(chan fileBundle, 1)
	in <- fileBundle{conn: conn, files: []protocol.FileInfo{file}}
	close(in)
	copies := make(chan copyBlocksState, 1)
	finished := make(chan *sharedPullerState, 1)
	f.bundleRoutine(t.Context(), in, copies, finished)
	if len(copies) != 1 || len(finished) != 0 {
		t.Fatal("fallback did not use ordinary copier")
	}
	state := <-copies
	if state.file.Name != file.Name || len(state.blocks) != 1 {
		t.Fatal("fallback changed file manifest")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	in = make(chan fileBundle, 1)
	in <- fileBundle{conn: conn, files: []protocol.FileInfo{file}}
	close(in)
	f.bundleRoutine(ctx, in, copies, finished)
	if len(copies) != 0 || len(finished) != 0 {
		t.Fatal("canceled bundle started disk work")
	}
}

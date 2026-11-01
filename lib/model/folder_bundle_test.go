// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

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

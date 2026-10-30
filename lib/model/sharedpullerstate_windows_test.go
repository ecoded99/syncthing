// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestSharedPullerFastMetadataWindows(t *testing.T) {
	for _, mode := range []uint32{0o644, 0o444} {
		t.Run(fs.FileMode(mode).String(), func(t *testing.T) {
			root := t.TempDir()
			filesystem := fs.NewFilesystem(fs.FilesystemTypeBasic, root)
			payload := bytes.Repeat([]byte("block"), 100)
			stamp := time.Now().Add(-time.Hour).Truncate(100 * time.Nanosecond)
			file := protocol.FileInfo{Name: "file.bin", Type: protocol.FileInfoTypeFile, Size: int64(len(payload)), Permissions: mode, ModifiedS: stamp.Unix(), ModifiedNs: int32(stamp.Nanosecond())}
			state := newSharedPullerState(file, filesystem, "test", "file.tmp", nil, nil, false, false, protocol.FileInfo{}, true, true)
			state.fastMetadata = true
			writer, err := state.tempFile()
			if err != nil {
				t.Fatal(err)
			}
			defer os.Chmod(filepath.Join(root, "file.tmp"), 0o600)
			if _, err = writer.WriteAt(payload, 0); err != nil {
				t.Fatal(err)
			}
			if closed, err := state.finalClose(); !closed || err != nil {
				t.Fatalf("close: %v %v", closed, err)
			}
			if !state.metadataReady {
				t.Fatal("fast metadata was not applied")
			}
			info, err := filesystem.Lstat("file.tmp")
			if err != nil || !info.ModTime().Equal(stamp) || info.Mode()&0o200 != fs.FileMode(mode)&0o200 {
				t.Fatalf("metadata after close: %v %v", info, err)
			}
			if got, err := os.ReadFile(filepath.Join(root, "file.tmp")); err != nil || !bytes.Equal(got, payload) {
				t.Fatal("content mismatch", err)
			}
			if closed, err := state.finalClose(); closed || err != nil {
				t.Fatalf("duplicate close: %v %v", closed, err)
			}
		})
	}
}

func TestSharedPullerFastMetadataFailureWindows(t *testing.T) {
	filesystem := fs.NewFilesystem(fs.FilesystemTypeBasic, t.TempDir())
	state := newSharedPullerState(protocol.FileInfo{Name: "file", Permissions: 0o644}, filesystem, "test", "file.tmp", nil, nil, false, false, protocol.FileInfo{}, false, true)
	state.fastMetadata = true
	if _, err := state.tempFile(); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("block failed integrity verification")
	state.fail(failure)
	if closed, err := state.finalClose(); !closed || !errors.Is(err, failure) {
		t.Fatalf("failed close: %v %v", closed, err)
	}
	if state.metadataReady {
		t.Fatal("failed output must not take fast finalization")
	}
}

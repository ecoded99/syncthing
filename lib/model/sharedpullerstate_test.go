// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/rand"
)

// Test creating temporary file inside read-only directory
func TestReadOnlyDir(t *testing.T) {
	ffs := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32))
	ffs.Mkdir("testdir", 0o555)

	s := sharedPullerState{
		fs:       ffs,
		tempName: "testdir/.temp_name",
	}

	fd, err := s.tempFile()
	if err != nil {
		t.Fatal(err)
	}
	if fd == nil {
		t.Fatal("Unexpected nil fd")
	}

	s.fail(nil)
	s.finalClose()
}

func TestBlocksToSize(t *testing.T) {
	for _, test := range []struct {
		name                     string
		blocks, total, blockSize int
		fileSize, want           int64
	}{
		{"one full block", 1, 1, 128, 128, 128},
		{"aligned partial", 1, 2, 128, 256, 128},
		{"aligned complete", 2, 2, 128, 256, 256},
		{"short final complete", 2, 2, 128, 200, 200},
		{"short final estimated partial", 1, 2, 128, 200, 100},
		{"empty", 0, 0, 128, 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := blocksToSize(test.blocks, test.total, test.blockSize, test.fileSize); got != test.want {
				t.Fatalf("got %d bytes, want %d", got, test.want)
			}
		})
	}
}

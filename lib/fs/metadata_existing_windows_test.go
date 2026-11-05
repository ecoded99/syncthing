// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
//go:build windows

package fs

import (
	"os"
	"syscall"
	"testing"
)

// Read-only opt-in benchmark of the actual filesystem's metadata calls.
func BenchmarkExistingMetadata(b *testing.B) {
	path := os.Getenv("STSCANMETADATA")
	if path == "" {
		b.Skip("set STSCANMETADATA to an existing absolute file/directory path")
	}
	filesystem := newBasicFilesystem(".")
	namep, _ := syscall.UTF16PtrFromString(path)
	info, queryErr := queryFileInformationByName(namep)
	fi, statErr := filesystem.underlyingLstat(path)
	b.Logf("query attrs=%x tag=%x error=%v stat=%T/%v", info.Attributes, info.Tag, queryErr, fi, statErr)
	b.Run("query", func(b *testing.B) {
		for b.Loop() {
			if _, err := queryFileInformationByName(namep); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("lstat", func(b *testing.B) {
		for b.Loop() {
			if _, err := os.Lstat(path); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("legacy", func(b *testing.B) {
		for b.Loop() {
			fi, err := os.Lstat(path)
			if err != nil {
				b.Fatal(err)
			}
			if fi.Mode()&os.ModeIrregular != 0 {
				namep, _ := syscall.UTF16PtrFromString(path)
				if _, err := readReparseTagHandle(namep); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("fresh", func(b *testing.B) {
		for b.Loop() {
			if _, err := filesystem.underlyingLstat(path); err != nil {
				b.Fatal(err)
			}
		}
	})
}

//go:build windows

// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package cloudbackup

import (
	"os"
	"syscall"
)

func isLink(fi os.FileInfo) bool {
	if fi.Mode()&(os.ModeSymlink|os.ModeIrregular) != 0 {
		return true
	}
	if attrs, ok := fi.Sys().(*syscall.Win32FileAttributeData); ok {
		return attrs.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0
	}
	return false
}

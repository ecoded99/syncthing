// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

//go:build !windows

package fs

import "time"

func prepareFileMetadata(_ basicFile, _ *FileMode, _ time.Time) (bool, error) {
	return false, nil
}

func renameNew(_ *BasicFilesystem, _, _ string) (bool, error) {
	return false, nil
}

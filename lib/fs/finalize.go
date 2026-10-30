// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package fs

import (
	"path/filepath"
	"time"
)

// PrepareFileMetadata clears the temporary hidden attribute and applies the
// final timestamp and, when non-nil, permissions through an existing file
// handle. The caller must exclude writes until it closes the file. Unsupported
// files and platforms return false. On failure, metadata can be partially
// applied; the caller must use normal path-based operations in either case.
func PrepareFileMetadata(file File, mode *FileMode, mtime time.Time) (bool, error) {
	for f := file; ; {
		if m, ok := f.(*metricsFile); ok {
			defer m.fs.account("preparemetadata")(-1)
			break
		}
		wrapped, ok := f.(interface{ unwrap() File })
		if !ok {
			break
		}
		f = wrapped.unwrap()
	}
	f, ok := unwrap(file).(basicFile)
	if !ok {
		return false, nil
	}
	return prepareFileMetadata(f, mode, mtime)
}

// RecordMtime records the observed on-disk time after a handle-based timestamp
// update and rename, retaining normal mtime virtualization, coarse precision,
// and case-insensitive mappings. False means no mtime wrapper was found.
func RecordMtime(filesystem Filesystem, name string, desired time.Time) (bool, error) {
	f, ok := unwrapFilesystem[*mtimeFS](filesystem)
	if !ok {
		return false, nil
	}
	if metrics, ok := unwrapFilesystem[*metricsFS](filesystem); ok {
		defer metrics.account("recordmtime")(-1)
	}
	info, err := f.Filesystem.Lstat(name)
	if err != nil {
		return false, err
	}
	f.save(name, info.ModTime(), desired)
	return true, nil
}

// RenameNew atomically installs a same-directory file only if its target does
// not exist. It retains case-conflict validation and cache updates. Unknown
// filesystem wrappers and unsupported platforms decline the optimization.
// Unlike Rename, it must never remove or replace an existing destination.
func RenameNew(filesystem Filesystem, from, to string) (bool, error) {
	if from == to {
		return true, ErrExist
	}
	if filepath.Dir(from) != filepath.Dir(to) {
		return false, nil
	}
	switch f := filesystem.(type) {
	case *caseFilesystem:
		oldcan, err := f.checkCase(from)
		if err != nil {
			return true, err
		}
		newcan, err := f.checkCase(to)
		if err != nil {
			return true, err
		}
		used, err := RenameNew(f.Filesystem, from, to)
		if used && err == nil {
			f.noteRemoved(oldcan)
			f.noteAdded(newcan)
		}
		return used, err
	case *walkFilesystem:
		return RenameNew(f.Filesystem, from, to)
	case *mtimeFS:
		return RenameNew(f.Filesystem, from, to)
	case *metricsFS:
		defer f.account("renamenew")(-1)
		return RenameNew(f.next, from, to)
	case *BasicFilesystem:
		return renameNew(f, from, to)
	default:
		return false, nil
	}
}

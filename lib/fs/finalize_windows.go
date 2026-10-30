// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package fs

import (
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Layout of FILE_BASIC_INFO, including its trailing alignment on Windows.
type fileBasicInfo struct {
	creationTime, lastAccessTime, lastWriteTime, changeTime int64
	attributes                                              uint32
	_                                                       uint32
}

func prepareFileMetadata(file basicFile, mode *FileMode, mtime time.Time) (bool, error) {
	// UnixNano cannot represent dates outside this range. Leave unusual
	// timestamps and reserved Windows timestamp values to the normal path.
	if mtime.Year() < 1678 || mtime.Year() > 2261 {
		return false, nil
	}
	t := windows.NsecToFiletime(mtime.UnixNano())
	stamp := int64(uint64(t.HighDateTime)<<32 | uint64(t.LowDateTime))
	if stamp <= 0 {
		return false, nil
	}
	conn, err := file.SyscallConn()
	if err != nil {
		return false, err
	}
	var opErr error
	var applied bool
	err = conn.Control(func(fd uintptr) {
		h := windows.Handle(fd)
		var original fileBasicInfo
		opErr = windows.GetFileInformationByHandleEx(h, windows.FileBasicInfo, (*byte)(unsafe.Pointer(&original)), uint32(unsafe.Sizeof(original)))
		if opErr != nil || original.attributes&(windows.FILE_ATTRIBUTE_DIRECTORY|windows.FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
			return
		}
		attrs := original.attributes &^ (windows.FILE_ATTRIBUTE_HIDDEN | windows.FILE_ATTRIBUTE_NORMAL)
		if mode != nil {
			if *mode&0o200 == 0 {
				attrs |= windows.FILE_ATTRIBUTE_READONLY
			} else {
				attrs &^= windows.FILE_ATTRIBUTE_READONLY
			}
		}
		if attrs == 0 {
			attrs = windows.FILE_ATTRIBUTE_NORMAL
		}
		// Zero creation/change timestamps leave them unchanged. Preserve all
		// unrelated attributes instead of assuming newly created file flags.
		info := fileBasicInfo{lastAccessTime: stamp, lastWriteTime: stamp, attributes: attrs}
		opErr = windows.SetFileInformationByHandle(h, windows.FileBasicInfo, (*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)))
		applied = opErr == nil
	})
	if err != nil {
		return false, err
	}
	return applied, opErr
}

func renameNew(filesystem *BasicFilesystem, from, to string) (bool, error) {
	source, err := filesystem.rooted(from)
	if err != nil {
		return true, err
	}
	target, err := filesystem.rooted(to)
	if err != nil {
		return true, err
	}
	oldp, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return true, err
	}
	newp, err := windows.UTF16PtrFromString(target)
	if err != nil {
		return true, err
	}
	// No REPLACE_EXISTING or COPY_ALLOWED: a concurrent local creation is
	// preserved and a move cannot silently become a non-atomic copy.
	err = windows.MoveFileEx(oldp, newp, 0)
	return true, err
}

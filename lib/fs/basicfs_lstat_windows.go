// Copyright (C) 2015 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build windows

package fs

import (
	"fmt"
	"go/version"
	"os"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

func readReparseTag(path string) (uint32, error) {
	namep, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, fmt.Errorf("syscall.UTF16PtrFromString failed with: %s", err)
	}
	attrs := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS | syscall.FILE_FLAG_OPEN_REPARSE_POINT)
	h, err := syscall.CreateFile(namep, 0, 0, nil, syscall.OPEN_EXISTING, attrs, 0)
	if err != nil {
		return 0, fmt.Errorf("syscall.CreateFile failed with: %s", err)
	}
	defer syscall.CloseHandle(h)

	// https://docs.microsoft.com/windows/win32/api/winbase/ns-winbase-file_attribute_tag_info
	const fileAttributeTagInfo = 9
	type FILE_ATTRIBUTE_TAG_INFO struct {
		FileAttributes uint32
		ReparseTag     uint32
	}

	var ti FILE_ATTRIBUTE_TAG_INFO
	err = windows.GetFileInformationByHandleEx(windows.Handle(h), fileAttributeTagInfo, (*byte)(unsafe.Pointer(&ti)), uint32(unsafe.Sizeof(ti)))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok && errno == windows.ERROR_INVALID_PARAMETER {
			// It appears calling GetFileInformationByHandleEx with
			// FILE_ATTRIBUTE_TAG_INFO fails on FAT file system with
			// ERROR_INVALID_PARAMETER. Clear ti.ReparseTag in that
			// instance to indicate no symlinks are possible.
			ti.ReparseTag = 0
		} else {
			return 0, fmt.Errorf("windows.GetFileInformationByHandleEx failed with: %s", err)
		}
	}

	return ti.ReparseTag, nil
}

func isDirectoryJunction(reparseTag uint32) bool {
	return reparseTag == windows.IO_REPARSE_TAG_MOUNT_POINT
}

type dirJunctFileInfo struct {
	os.FileInfo
}

func (fi *dirJunctFileInfo) Mode() os.FileMode {
	// Simulate a directory and not a symlink; also set the execute
	// bits so the directory can be traversed Unix-side.
	return fi.FileInfo.Mode()&^junctionPointModeMask | os.ModeDir | 0o111
}

func (fi *dirJunctFileInfo) IsDir() bool {
	return true
}

// cloudFileInfo is a Cloud Files placeholder (as used by OneDrive and by
// Syncthing's own Explorer status) whose data is fully on disk. Go reports
// every reparse point that is not a link as ModeIrregular, which would make
// the scanner skip the file, but such a placeholder reads like any other file.
type cloudFileInfo struct {
	os.FileInfo
}

func (fi *cloudFileInfo) Mode() os.FileMode {
	return fi.FileInfo.Mode() &^ os.ModeIrregular
}

// ioReparseTagCloud is IO_REPARSE_TAG_CLOUD; the filter uses the variants
// IO_REPARSE_TAG_CLOUD_1 to _F, which differ in bits 12 to 15.
const (
	ioReparseTagCloud     = 0x9000001A
	ioReparseTagCloudMask = 0xFFFF0FFF
)

// isLocalCloudFile reports whether fi is a Cloud Files placeholder whose
// contents are on disk. Placeholders that would be fetched from the cloud on
// access stay irregular, so scanning never triggers a download.
func isLocalCloudFile(name string, fi os.FileInfo) bool {
	if fi.Mode()&os.ModeIrregular == 0 {
		return false
	}
	attrs, ok := fi.Sys().(*syscall.Win32FileAttributeData)
	if !ok || attrs.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		return false
	}
	const remote = windows.FILE_ATTRIBUTE_RECALL_ON_OPEN | windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS | windows.FILE_ATTRIBUTE_OFFLINE
	if attrs.FileAttributes&remote != 0 {
		return false
	}
	tag, err := readReparseTag(name)
	return err == nil && tag&ioReparseTagCloudMask == ioReparseTagCloud
}

var junctionPointModeMask os.FileMode

func init() {
	// Per https://tip.golang.org/doc/go1.23#minor_library_changes
	// In go1.22 (and go1.23 when GODEBUG=winsymlink=0) Windows' directory
	// junctions (aka "mount points") always have ModeSymlink set.
	junctionPointModeMask = os.ModeSymlink
	if version.Compare(runtime.Version(), "go1.23") >= 0 {
		// In go1.23 Windows' directory junctions always have ModeIrregular set
		// (unless GODEBUG=winsymlink=0).
		junctionPointModeMask |= os.ModeIrregular
	}
}

func (f *BasicFilesystem) underlyingLstat(name string) (os.FileInfo, error) {
	fi, err := os.Lstat(name)

	// There are cases where files are tagged as symlink, but they end up being
	// something else. Make sure we properly handle those types.
	if err == nil {
		// NTFS directory junctions can be treated as ordinary directories,
		// see https://forum.syncthing.net/t/option-to-follow-directory-junctions-symbolic-links/14750
		if fi.Mode()&junctionPointModeMask != 0 && f.junctionsAsDirs {
			if reparseTag, reparseErr := readReparseTag(name); reparseErr == nil && isDirectoryJunction(reparseTag) {
				return &dirJunctFileInfo{fi}, nil
			}
		}
		if isLocalCloudFile(name, fi) {
			return &cloudFileInfo{fi}, nil
		}
	}

	return fi, err
}

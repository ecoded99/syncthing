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
	"path/filepath"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func readReparseTag(path string) (uint32, error) {
	namep, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, fmt.Errorf("syscall.UTF16PtrFromString failed with: %s", err)
	}
	if tag, err := readReparseTagByName(namep); err == nil {
		return tag, nil
	}
	return readReparseTagHandle(namep)
}

var getFileInformationByName = windows.NewLazySystemDLL("kernelbase.dll").NewProc("GetFileInformationByName")

// Windows 11 can query fresh metadata without opening another file handle.
// Unlike FindFirstFile's directory metadata, this is a file information query.
// Older Windows/filesystems and every error retain the existing handle path.
func readReparseTagByName(name *uint16) (uint32, error) {
	info, err := queryFileInformationByName(name)
	if err != nil {
		return 0, err
	}
	if info.Attributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		return 0, nil
	}
	return info.Tag, nil
}

type namedFileInformation struct {
	FileID, Creation, Access, Write, Change, Allocation, Size     int64
	Attributes, Tag, Links, DeviceType, Characteristics, Reserved uint32
	Volume                                                        int64
	ID128                                                         [16]byte
}

func queryFileInformationByName(name *uint16) (namedFileInformation, error) {
	var info namedFileInformation
	if err := getFileInformationByName.Find(); err != nil {
		return info, err
	}
	// FILE_STAT_BASIC_INFORMATION, FileStatBasicByNameInfo (3).
	// https://learn.microsoft.com/windows/win32/api/winbase/nf-winbase-getfileinformationbyname
	ok, _, err := getFileInformationByName.Call(uintptr(unsafe.Pointer(name)), 3, uintptr(unsafe.Pointer(&info)), unsafe.Sizeof(info))
	if ok == 0 {
		return info, err
	}
	return info, nil
}

const remoteCloudAttributes = windows.FILE_ATTRIBUTE_RECALL_ON_OPEN | windows.FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS | windows.FILE_ATTRIBUTE_OFFLINE

// A fresh by-name snapshot is sufficient for fully local cloud placeholders.
// Every other file type retains os.Lstat, including all links and remote data.
type namedCloudFileInfo struct {
	path string
	info namedFileInformation
}

func (f *namedCloudFileInfo) sameFile(other os.FileInfo) (bool, bool) {
	otherNamed, ok := other.(*namedCloudFileInfo)
	if !ok {
		return false, false
	}
	if f.info.FileID == 0 || otherNamed.info.FileID == 0 {
		return false, false
	}
	return f.info.Volume == otherNamed.info.Volume && f.info.FileID == otherNamed.info.FileID && f.info.ID128 == otherNamed.info.ID128, true
}

func (f *namedCloudFileInfo) Name() string { return filepath.Base(f.path) }
func (f *namedCloudFileInfo) Size() int64  { return f.info.Size }
func (f *namedCloudFileInfo) Mode() os.FileMode {
	mode := os.FileMode(0o666)
	if f.info.Attributes&syscall.FILE_ATTRIBUTE_READONLY != 0 {
		mode = 0o444
	}
	if f.IsDir() {
		mode |= os.ModeDir | 0o111
	}
	return mode
}
func (f *namedCloudFileInfo) IsDir() bool {
	return f.info.Attributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0
}
func (f *namedCloudFileInfo) ModTime() time.Time {
	ft := syscall.Filetime{LowDateTime: uint32(f.info.Write), HighDateTime: uint32(uint64(f.info.Write) >> 32)}
	return time.Unix(0, ft.Nanoseconds())
}
func (f *namedCloudFileInfo) Sys() any {
	ft := func(v int64) syscall.Filetime {
		return syscall.Filetime{LowDateTime: uint32(v), HighDateTime: uint32(uint64(v) >> 32)}
	}
	return &syscall.Win32FileAttributeData{FileAttributes: f.info.Attributes, CreationTime: ft(f.info.Creation), LastAccessTime: ft(f.info.Access), LastWriteTime: ft(f.info.Write), FileSizeHigh: uint32(uint64(f.info.Size) >> 32), FileSizeLow: uint32(f.info.Size)}
}

func readReparseTagHandle(namep *uint16) (uint32, error) {
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
	if getFileInformationByName.Find() == nil {
		if namep, err := syscall.UTF16PtrFromString(name); err == nil {
			// A cheap hint keeps ordinary files on their existing path. It is
			// never used as metadata: either the fresh query or os.Lstat follows.
			if attrs, err := syscall.GetFileAttributes(namep); err == nil && attrs&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
				if info, err := queryFileInformationByName(namep); err == nil && info.Attributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 && info.Attributes&remoteCloudAttributes == 0 && info.Tag&ioReparseTagCloudMask == ioReparseTagCloud {
					return &namedCloudFileInfo{path: name, info: info}, nil
				}
			}
		}
	}
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

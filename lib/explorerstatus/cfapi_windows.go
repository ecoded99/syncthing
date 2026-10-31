// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build windows

package explorerstatus

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	cldapi              = windows.NewLazySystemDLL("cldapi.dll")
	procConnectSyncRoot = cldapi.NewProc("CfConnectSyncRoot")
	procDisconnect      = cldapi.NewProc("CfDisconnectSyncRoot")
	procConvert         = cldapi.NewProc("CfConvertToPlaceholder")
	procSetInSyncState  = cldapi.NewProc("CfSetInSyncState")
	procSetPinState     = cldapi.NewProc("CfSetPinState")
)

const (
	cfConvertFlagMarkInSync = 0x1
	cfConvertFlagAlwaysFull = 0x8

	cfInSyncStateNotInSync = 0
	cfInSyncStateInSync    = 1

	cfPinStateUnspecified = 0
	cfPinStateExcluded    = 3

	cfCallbackTypeNone = 0xffffffff

	ioReparseTagCloud     = 0x9000001A
	ioReparseTagCloudMask = 0xFFFF0FFF
)

var platformOpen Opener = open

// open registers the folder with Explorer and connects to it.
func open(folderID, path, label string) (Root, error) {
	if err := cldapi.Load(); err != nil {
		return nil, fmt.Errorf("cloud files are not supported on this version of Windows: %w", err)
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	id, err := syncRootID(folderID)
	if err != nil {
		return nil, err
	}
	icon := ""
	if exe, err := os.Executable(); err == nil {
		icon = exe + ",0"
	}
	if err := registerSyncRoot(id, path, "Syncthing - "+label, icon); err != nil {
		return nil, err
	}
	pathp, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	// No callbacks: placeholders are always full, so Windows never asks for
	// data or directory contents.
	callbacks := [1]struct {
		Type     uint32
		Callback uintptr
	}{{Type: cfCallbackTypeNone}}
	var key int64
	if err := hresult(procConnectSyncRoot.Call(uintptr(unsafe.Pointer(pathp)), uintptr(unsafe.Pointer(&callbacks[0])), 0, 0, uintptr(unsafe.Pointer(&key)))); err != nil {
		_ = unregisterSyncRoot(id)
		return nil, fmt.Errorf("CfConnectSyncRoot: %w", err)
	}
	return &cfRoot{id: id, path: path, key: key}, nil
}

type cfRoot struct {
	id   string
	path string
	key  int64
}

func (r *cfRoot) Set(name string, st State) error {
	p := filepath.Join(r.path, name)
	pp, err := windows.UTF16PtrFromString(p)
	if err != nil {
		return err
	}
	// Opening with FILE_WRITE_ATTRIBUTES does not change the file's
	// modification time, so the scanner sees nothing new.
	h, err := windows.CreateFile(pp, windows.FILE_READ_ATTRIBUTES|windows.FILE_WRITE_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		if err == windows.ERROR_FILE_NOT_FOUND || err == windows.ERROR_PATH_NOT_FOUND {
			return os.ErrNotExist
		}
		return err
	}
	defer windows.CloseHandle(h)

	var ti struct {
		FileAttributes uint32
		ReparseTag     uint32
	}
	if err := windows.GetFileInformationByHandleEx(h, windows.FileAttributeTagInfo, (*byte)(unsafe.Pointer(&ti)), uint32(unsafe.Sizeof(ti))); err != nil {
		return err
	}
	if ti.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 && ti.ReparseTag&ioReparseTagCloudMask != ioReparseTagCloud {
		// A link or another kind of reparse point; leave it alone.
		return nil
	}
	isPlaceholder := ti.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0

	if !isPlaceholder {
		// Always convert as in sync: a directory converted out of sync shows
		// as not fully on disk. The flag is cleared below when needed.
		flags := uintptr(cfConvertFlagAlwaysFull | cfConvertFlagMarkInSync)
		err := hresult(procConvert.Call(uintptr(h), 0, 0, flags, 0, 0))
		if err == windows.ERROR_INVALID_PARAMETER {
			// Windows 10 does not know CF_CONVERT_FLAG_ALWAYS_FULL.
			err = hresult(procConvert.Call(uintptr(h), 0, 0, flags&^cfConvertFlagAlwaysFull, 0, 0))
		}
		if err != nil {
			return fmt.Errorf("CfConvertToPlaceholder: %w", err)
		}
	}
	if isPlaceholder || st != InSync {
		state := uintptr(cfInSyncStateNotInSync)
		if st == InSync {
			state = cfInSyncStateInSync
		}
		if err := hresult(procSetInSyncState.Call(uintptr(h), state, 0, 0)); err != nil {
			return fmt.Errorf("CfSetInSyncState: %w", err)
		}
	}

	// Excluded shows ignored files as not synced; anything else inherits.
	switch {
	case st == Excluded:
		err = hresult(procSetPinState.Call(uintptr(h), cfPinStateExcluded, 0, 0))
	case isPlaceholder:
		err = hresult(procSetPinState.Call(uintptr(h), cfPinStateUnspecified, 0, 0))
	}
	if err != nil {
		return fmt.Errorf("CfSetPinState: %w", err)
	}
	return nil
}

func (r *cfRoot) Close(remove bool) error {
	_ = hresult(procDisconnect.Call(uintptr(r.key)))
	if remove {
		return unregisterSyncRoot(r.id)
	}
	return nil
}

var unsafeIDChars = regexp.MustCompile(`[^A-Za-z0-9._-]`)

// syncRootID is the id Explorer expects: provider!user SID!account.
func syncRootID(folderID string) (string, error) {
	token := windows.GetCurrentProcessToken()
	user, err := token.GetTokenUser()
	if err != nil {
		return "", err
	}
	return "Syncthing!" + user.User.Sid.String() + "!" + unsafeIDChars.ReplaceAllString(folderID, "_"), nil
}

func hresult(r uintptr, _ uintptr, _ error) error {
	if int32(r) >= 0 {
		return nil
	}
	if r&0xffff0000 == 0x80070000 {
		return syscall.Errno(r & 0xffff)
	}
	return syscall.Errno(r)
}

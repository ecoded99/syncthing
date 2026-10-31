// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build windows

package fs

import (
	"os"
	"path/filepath"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

// TestLstatCloudPlaceholder checks that a Cloud Files placeholder with its
// data on disk is scanned as a regular file instead of being skipped as
// irregular.
func TestLstatCloudPlaceholder(t *testing.T) {
	cldapi := windows.NewLazySystemDLL("cldapi.dll")
	if err := cldapi.Load(); err != nil {
		t.Skip("no Cloud Files API:", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}

	dirp, _ := windows.UTF16PtrFromString(dir)
	name, _ := windows.UTF16PtrFromString("SyncthingTest")
	version, _ := windows.UTF16PtrFromString("1")
	identity := []byte("test")
	registration := struct {
		StructSize             uint32
		ProviderName           *uint16
		ProviderVersion        *uint16
		SyncRootIdentity       uintptr
		SyncRootIdentityLength uint32
		FileIdentity           uintptr
		FileIdentityLength     uint32
		ProviderID             windows.GUID
	}{ProviderName: name, ProviderVersion: version, SyncRootIdentity: uintptr(unsafe.Pointer(&identity[0])), SyncRootIdentityLength: uint32(len(identity))}
	registration.StructSize = uint32(unsafe.Sizeof(registration))
	type policy struct{ Primary, Modifier uint16 }
	policies := struct {
		StructSize            uint32
		Hydration             policy
		Population            policy
		InSync                uint32
		HardLink              uint32
		PlaceholderManagement uint32
	}{Hydration: policy{Primary: 3}, Population: policy{Primary: 3}} // always full
	policies.StructSize = uint32(unsafe.Sizeof(policies))
	if r, _, _ := cldapi.NewProc("CfRegisterSyncRoot").Call(uintptr(unsafe.Pointer(dirp)), uintptr(unsafe.Pointer(&registration)), uintptr(unsafe.Pointer(&policies)), 0); int32(r) < 0 {
		t.Skipf("CfRegisterSyncRoot: 0x%08x", uint32(r))
	}
	defer cldapi.NewProc("CfUnregisterSyncRoot").Call(uintptr(unsafe.Pointer(dirp)))

	for _, n := range []string{"file", "dir"} {
		p, _ := windows.UTF16PtrFromString(filepath.Join(dir, n))
		h, err := windows.CreateFile(p, windows.FILE_READ_ATTRIBUTES|windows.FILE_WRITE_ATTRIBUTES, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
		if err != nil {
			t.Fatal(err)
		}
		const markInSync = 1
		r, _, _ := cldapi.NewProc("CfConvertToPlaceholder").Call(uintptr(h), 0, 0, markInSync, 0, 0)
		windows.CloseHandle(h)
		if int32(r) < 0 {
			t.Fatalf("CfConvertToPlaceholder(%s): 0x%08x", n, uint32(r))
		}
	}

	fs := newBasicFilesystem(dir)
	if fi, err := os.Lstat(filepath.Join(dir, "file")); err != nil || fi.Mode()&os.ModeIrregular == 0 {
		t.Skip("Go does not report this placeholder as irregular; nothing to test")
	}
	fi, err := fs.Lstat("file")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsRegular() || fi.Size() != 4 {
		t.Errorf("placeholder file: regular=%v size=%d mode=%v", fi.IsRegular(), fi.Size(), fi.Mode())
	}
	fi, err = fs.Lstat("dir")
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() || fi.Mode()&ModeType != ModeDirectory {
		t.Errorf("placeholder dir: mode=%v", fi.Mode())
	}
}

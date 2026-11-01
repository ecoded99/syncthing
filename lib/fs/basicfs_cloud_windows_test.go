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
	callbacks := [1]struct {
		Type     uint32
		Callback uintptr
	}{{Type: 0xffffffff}}
	var key int64
	if r, _, _ := cldapi.NewProc("CfConnectSyncRoot").Call(uintptr(unsafe.Pointer(dirp)), uintptr(unsafe.Pointer(&callbacks[0])), 0, 0, uintptr(unsafe.Pointer(&key))); int32(r) < 0 {
		t.Fatalf("CfConnectSyncRoot: 0x%08x", uint32(r))
	}
	defer cldapi.NewProc("CfDisconnectSyncRoot").Call(uintptr(key))

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
	path := filepath.Join(dir, "file")
	tag, err := readReparseTag(path)
	if err != nil {
		t.Fatal(err)
	}
	if tag&ioReparseTagCloudMask != ioReparseTagCloud {
		t.Fatalf("not a Cloud Files placeholder: tag 0x%08x", tag)
	}
	// Some Go versions already recognize cloud placeholders. Still verify
	// the real filesystem result, and exercise our fallback explicitly below.
	raw, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	irregular := irregularCloudInfo{raw}
	if !isLocalCloudFile(path, irregular) {
		t.Fatal("fully local cloud placeholder was not recognized")
	}
	if mode := (&cloudFileInfo{irregular}).Mode(); !mode.IsRegular() {
		t.Errorf("normalized placeholder mode: %v", mode)
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
	if !fi.IsDir() || os.FileMode(fi.Mode())&os.ModeIrregular != 0 {
		t.Errorf("placeholder dir: mode=%v", fi.Mode())
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "data" {
		t.Fatalf("placeholder contents: %q, %v", data, err)
	}
}

type irregularCloudInfo struct{ os.FileInfo }

func (fi irregularCloudInfo) Mode() os.FileMode {
	return fi.FileInfo.Mode() | os.ModeIrregular
}

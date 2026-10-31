// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build windows

package explorerstatus

// Explorer only draws status icons for sync roots registered through the
// WinRT StorageProviderSyncRootManager, which (unlike writing its registry
// keys directly) works without administrator rights. The calls below go
// through the COM vtables; slot numbers follow the interface declarations in
// the Windows SDK's windows.storage.provider.h and windows.storage.h, after
// the six IInspectable methods.

import (
	"errors"
	"fmt"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	combase                    = windows.NewLazySystemDLL("combase.dll")
	procRoInitialize           = combase.NewProc("RoInitialize")
	procRoUninitialize         = combase.NewProc("RoUninitialize")
	procRoActivateInstance     = combase.NewProc("RoActivateInstance")
	procRoGetActivationFactory = combase.NewProc("RoGetActivationFactory")
	procWindowsCreateString    = combase.NewProc("WindowsCreateString")
	procWindowsDeleteString    = combase.NewProc("WindowsDeleteString")
)

var (
	iidSyncRootInfo           = windows.GUID{Data1: 0x7c1305c4, Data2: 0x99f9, Data3: 0x41ac, Data4: [8]byte{0x89, 0x04, 0xab, 0x05, 0x5d, 0x65, 0x49, 0x26}}
	iidSyncRootManagerStatics = windows.GUID{Data1: 0x3e99fbbf, Data2: 0x8fe3, Data3: 0x4b40, Data4: [8]byte{0xab, 0xc7, 0xf6, 0xfc, 0x3d, 0x74, 0xc9, 0x8e}}
	iidStorageFolderStatics   = windows.GUID{Data1: 0x08f327ff, Data2: 0x85d5, Data3: 0x48b9, Data4: [8]byte{0xae, 0xe9, 0x28, 0x51, 0x1e, 0x33, 0x9f, 0x9f}}
	iidAsyncInfo              = windows.GUID{Data1: 0x00000036, Data2: 0x0000, Data3: 0x0000, Data4: [8]byte{0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46}}
)

// Vtable slots.
const (
	slotQueryInterface = 0
	slotRelease        = 2

	slotFolderFromPathAsync = 6 // IStorageFolderStatics
	slotAsyncStatus         = 7 // IAsyncInfo.get_Status
	slotAsyncErrorCode      = 8 // IAsyncInfo.get_ErrorCode
	slotAsyncGetResults     = 8 // IAsyncOperation.GetResults

	slotPutID                  = 7 // IStorageProviderSyncRootInfo
	slotPutPath                = 11
	slotPutDisplayName         = 13
	slotPutIcon                = 15
	slotPutHydrationPolicy     = 17
	slotPutPopulationPolicy    = 21
	slotPutInSyncPolicy        = 23
	slotPutVersion             = 29
	slotPutProtectionMode      = 31
	slotRegisterSyncRoot       = 6 // IStorageProviderSyncRootManagerStatics
	slotUnregisterSyncRoot     = 7
	hydrationPolicyAlwaysFull  = 3
	populationPolicyAlwaysFull = 2
)

type comObject struct{ vtbl *[64]uintptr }

func (o *comObject) call(slot int, args ...uintptr) error {
	r, _, _ := syscall.SyscallN(o.vtbl[slot], append([]uintptr{uintptr(unsafe.Pointer(o))}, args...)...)
	if int32(r) < 0 {
		return fmt.Errorf("HRESULT 0x%08x", uint32(r))
	}
	return nil
}

func (o *comObject) release() {
	syscall.SyscallN(o.vtbl[slotRelease], uintptr(unsafe.Pointer(o)))
}

func (o *comObject) query(iid *windows.GUID) (*comObject, error) {
	var out *comObject
	err := o.call(slotQueryInterface, uintptr(unsafe.Pointer(iid)), uintptr(unsafe.Pointer(&out)))
	return out, err
}

type hstring uintptr

func newHString(s string) (hstring, error) {
	u, err := windows.UTF16FromString(s)
	if err != nil {
		return 0, err
	}
	var h hstring
	r, _, _ := procWindowsCreateString.Call(uintptr(unsafe.Pointer(&u[0])), uintptr(len(u)-1), uintptr(unsafe.Pointer(&h)))
	if int32(r) < 0 {
		return 0, fmt.Errorf("WindowsCreateString: HRESULT 0x%08x", uint32(r))
	}
	return h, nil
}

func (h hstring) free() { procWindowsDeleteString.Call(uintptr(h)) }

func activationFactory(class string, iid *windows.GUID) (*comObject, error) {
	h, err := newHString(class)
	if err != nil {
		return nil, err
	}
	defer h.free()
	var out *comObject
	r, _, _ := procRoGetActivationFactory.Call(uintptr(h), uintptr(unsafe.Pointer(iid)), uintptr(unsafe.Pointer(&out)))
	if int32(r) < 0 {
		return nil, fmt.Errorf("RoGetActivationFactory(%s): HRESULT 0x%08x", class, uint32(r))
	}
	return out, nil
}

// withWinRT runs fn on a locked thread in the multithreaded apartment.
func withWinRT(fn func() error) error {
	if err := combase.Load(); err != nil {
		return err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	const roInitMultithreaded = 1
	if r, _, _ := procRoInitialize.Call(roInitMultithreaded); int32(r) >= 0 {
		defer procRoUninitialize.Call()
	}
	return fn()
}

// await waits for a WinRT async operation to finish.
func await(op *comObject) error {
	info, err := op.query(&iidAsyncInfo)
	if err != nil {
		return err
	}
	defer info.release()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		var status int32
		if err := info.call(slotAsyncStatus, uintptr(unsafe.Pointer(&status))); err != nil {
			return err
		}
		const started, completed = 0, 1
		switch status {
		case started:
			continue
		case completed:
			return nil
		default:
			var code int32
			_ = info.call(slotAsyncErrorCode, uintptr(unsafe.Pointer(&code)))
			return fmt.Errorf("async operation failed: HRESULT 0x%08x", uint32(code))
		}
	}
	return errors.New("async operation timed out")
}

func storageFolder(path string) (*comObject, error) {
	statics, err := activationFactory("Windows.Storage.StorageFolder", &iidStorageFolderStatics)
	if err != nil {
		return nil, err
	}
	defer statics.release()
	hpath, err := newHString(path)
	if err != nil {
		return nil, err
	}
	defer hpath.free()
	var op *comObject
	if err := statics.call(slotFolderFromPathAsync, uintptr(hpath), uintptr(unsafe.Pointer(&op))); err != nil {
		return nil, fmt.Errorf("GetFolderFromPathAsync: %w", err)
	}
	defer op.release()
	if err := await(op); err != nil {
		return nil, fmt.Errorf("GetFolderFromPathAsync: %w", err)
	}
	var folder *comObject
	if err := op.call(slotAsyncGetResults, uintptr(unsafe.Pointer(&folder))); err != nil {
		return nil, fmt.Errorf("GetFolderFromPathAsync: %w", err)
	}
	return folder, nil
}

func registerSyncRoot(id, path, displayName, icon string) error {
	return withWinRT(func() error {
		folder, err := storageFolder(path)
		if err != nil {
			return err
		}
		defer folder.release()

		class, err := newHString("Windows.Storage.Provider.StorageProviderSyncRootInfo")
		if err != nil {
			return err
		}
		defer class.free()
		var inspectable *comObject
		if r, _, _ := procRoActivateInstance.Call(uintptr(class), uintptr(unsafe.Pointer(&inspectable))); int32(r) < 0 {
			return fmt.Errorf("RoActivateInstance: HRESULT 0x%08x", uint32(r))
		}
		defer inspectable.release()
		info, err := inspectable.query(&iidSyncRootInfo)
		if err != nil {
			return err
		}
		defer info.release()

		texts := make(map[int]string)
		texts[slotPutID] = id
		texts[slotPutDisplayName] = displayName
		texts[slotPutVersion] = "1"
		if icon != "" {
			texts[slotPutIcon] = icon
		}
		for slot, s := range texts {
			h, err := newHString(s)
			if err != nil {
				return err
			}
			err = info.call(slot, uintptr(h))
			h.free()
			if err != nil {
				return fmt.Errorf("sync root info slot %d: %w", slot, err)
			}
		}
		for _, v := range []struct {
			slot  int
			value uintptr
		}{
			{slotPutPath, uintptr(unsafe.Pointer(folder))},
			{slotPutHydrationPolicy, hydrationPolicyAlwaysFull},
			{slotPutPopulationPolicy, populationPolicyAlwaysFull},
			{slotPutInSyncPolicy, 0},
			{slotPutProtectionMode, 0},
		} {
			if err := info.call(v.slot, v.value); err != nil {
				return fmt.Errorf("sync root info slot %d: %w", v.slot, err)
			}
		}
		manager, err := activationFactory("Windows.Storage.Provider.StorageProviderSyncRootManager", &iidSyncRootManagerStatics)
		if err != nil {
			return err
		}
		defer manager.release()
		if err := manager.call(slotRegisterSyncRoot, uintptr(unsafe.Pointer(info))); err != nil {
			return fmt.Errorf("registering sync root: %w", err)
		}
		return nil
	})
}

func unregisterSyncRoot(id string) error {
	return withWinRT(func() error {
		manager, err := activationFactory("Windows.Storage.Provider.StorageProviderSyncRootManager", &iidSyncRootManagerStatics)
		if err != nil {
			return err
		}
		defer manager.release()
		h, err := newHString(id)
		if err != nil {
			return err
		}
		defer h.free()
		if err := manager.call(slotUnregisterSyncRoot, uintptr(h)); err != nil {
			return fmt.Errorf("unregistering sync root: %w", err)
		}
		return nil
	})
}

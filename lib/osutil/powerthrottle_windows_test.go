// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package osutil

import (
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestDisablePowerThrottling(t *testing.T) {
	if err := DisablePowerThrottling(); err != nil {
		t.Fatal(err)
	}

	get := windows.NewLazySystemDLL("kernel32.dll").NewProc("GetProcessInformation")
	if get.Find() != nil {
		t.Skip("GetProcessInformation unavailable")
	}
	state := processPowerThrottlingState{Version: processPowerThrottlingCurrentVer}
	r, _, err := get.Call(uintptr(windows.CurrentProcess()), processPowerThrottling, uintptr(unsafe.Pointer(&state)), unsafe.Sizeof(state))
	if r == 0 {
		t.Skip("cannot read power throttling state:", err)
	}
	if state.ControlMask&processPowerThrottlingExecutionSpd == 0 || state.StateMask&processPowerThrottlingExecutionSpd != 0 {
		t.Fatalf("execution speed throttling not disabled: control=%#x state=%#x", state.ControlMask, state.StateMask)
	}
}

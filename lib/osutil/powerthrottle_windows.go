// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package osutil

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

var procSetProcessInformation = windows.NewLazySystemDLL("kernel32.dll").NewProc("SetProcessInformation")

const (
	processPowerThrottling             = 4 // PROCESS_INFORMATION_CLASS ProcessPowerThrottling
	processPowerThrottlingCurrentVer   = 1
	processPowerThrottlingExecutionSpd = 0x1
)

type processPowerThrottlingState struct {
	Version     uint32
	ControlMask uint32
	StateMask   uint32
}

// DisablePowerThrottling opts the process out of Windows execution-speed
// throttling (EcoQoS). Otherwise Windows may move a windowless, low priority
// process to efficiency cores at reduced clock, where threads holding NTFS
// directory locks stall every other file operation in that directory.
func DisablePowerThrottling() error {
	if err := procSetProcessInformation.Find(); err != nil {
		return nil // Older Windows without power throttling.
	}
	state := processPowerThrottlingState{
		Version:     processPowerThrottlingCurrentVer,
		ControlMask: processPowerThrottlingExecutionSpd,
		StateMask:   0, // Control the policy, and turn throttling off.
	}
	r, _, err := procSetProcessInformation.Call(uintptr(windows.CurrentProcess()), processPowerThrottling, uintptr(unsafe.Pointer(&state)), unsafe.Sizeof(state))
	if r == 0 {
		return fmt.Errorf("set process power throttling: %w", err)
	}
	return nil
}

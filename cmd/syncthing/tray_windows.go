// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build windows

package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"log/slog"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/svcutil"
	"github.com/syncthing/syncthing/lib/syncthing"
)

//go:embed trayicon.png
var trayIconPNG []byte

var (
	user32   = windows.NewLazySystemDLL("user32.dll")
	gdi32    = windows.NewLazySystemDLL("gdi32.dll")
	shell32  = windows.NewLazySystemDLL("shell32.dll")
	kernel32 = windows.NewLazySystemDLL("kernel32.dll")

	procRegisterClassExW       = user32.NewProc("RegisterClassExW")
	procCreateWindowExW        = user32.NewProc("CreateWindowExW")
	procDestroyWindow          = user32.NewProc("DestroyWindow")
	procDefWindowProcW         = user32.NewProc("DefWindowProcW")
	procGetMessageW            = user32.NewProc("GetMessageW")
	procTranslateMessage       = user32.NewProc("TranslateMessage")
	procDispatchMessageW       = user32.NewProc("DispatchMessageW")
	procPostMessageW           = user32.NewProc("PostMessageW")
	procPostQuitMessage        = user32.NewProc("PostQuitMessage")
	procRegisterWindowMessageW = user32.NewProc("RegisterWindowMessageW")
	procCreatePopupMenu        = user32.NewProc("CreatePopupMenu")
	procAppendMenuW            = user32.NewProc("AppendMenuW")
	procSetMenuDefaultItem     = user32.NewProc("SetMenuDefaultItem")
	procTrackPopupMenu         = user32.NewProc("TrackPopupMenu")
	procDestroyMenu            = user32.NewProc("DestroyMenu")
	procSetForegroundWindow    = user32.NewProc("SetForegroundWindow")
	procGetCursorPos           = user32.NewProc("GetCursorPos")
	procCreateIconIndirect     = user32.NewProc("CreateIconIndirect")
	procDestroyIcon            = user32.NewProc("DestroyIcon")
	procCreateDIBSection       = gdi32.NewProc("CreateDIBSection")
	procCreateBitmap           = gdi32.NewProc("CreateBitmap")
	procDeleteObject           = gdi32.NewProc("DeleteObject")
	procShellNotifyIconW       = shell32.NewProc("Shell_NotifyIconW")
	procGetModuleHandleW       = kernel32.NewProc("GetModuleHandleW")
)

const (
	wmNull        = 0x0000
	wmDestroy     = 0x0002
	wmClose       = 0x0010
	wmContextMenu = 0x007B
	wmApp         = 0x8000

	wmTrayCallback = wmApp + 1
	wmTrayUpdate   = wmApp + 2

	ninSelect    = 0x0400
	ninKeySelect = 0x0401

	nimAdd        = 0
	nimModify     = 1
	nimDelete     = 2
	nimSetVersion = 4

	nifMessage   = 0x01
	nifIcon      = 0x02
	nifTip       = 0x04
	nifShowTip   = 0x80
	notifyIconV4 = 4

	mfString    = 0x0000
	mfGrayed    = 0x0001
	mfChecked   = 0x0008
	mfPopup     = 0x0010
	mfSeparator = 0x0800

	tpmRightButton = 0x0002
	tpmBottomAlign = 0x0020
	tpmReturnCmd   = 0x0100
)

const (
	trayCmdOpen = iota + 1
	trayCmdRescan
	trayCmdPause
	trayCmdRestart
	trayCmdQuit
	trayCmdFolderBase = 1000
)

type wndClassEx struct {
	Size       uint32
	Style      uint32
	WndProc    uintptr
	ClsExtra   int32
	WndExtra   int32
	Instance   windows.Handle
	Icon       windows.Handle
	Cursor     windows.Handle
	Background windows.Handle
	MenuName   *uint16
	ClassName  *uint16
	IconSm     windows.Handle
}

type winPoint struct {
	X, Y int32
}

type winMsg struct {
	Hwnd    windows.HWND
	Message uint32
	WParam  uintptr
	LParam  uintptr
	Time    uint32
	Pt      winPoint
	Private uint32
}

type notifyIconData struct {
	Size             uint32
	Wnd              windows.HWND
	ID               uint32
	Flags            uint32
	CallbackMessage  uint32
	Icon             windows.Handle
	Tip              [128]uint16
	State            uint32
	StateMask        uint32
	Info             [256]uint16
	TimeoutOrVersion uint32
	InfoTitle        [64]uint16
	InfoFlags        uint32
	GUIDItem         windows.GUID
	BalloonIcon      windows.Handle
}

type iconInfo struct {
	Icon     int32
	XHotspot uint32
	YHotspot uint32
	Mask     windows.Handle
	Color    windows.Handle
}

type bitmapInfoHeader struct {
	Size          uint32
	Width         int32
	Height        int32
	Planes        uint16
	BitCount      uint16
	Compression   uint32
	SizeImage     uint32
	XPelsPerMeter int32
	YPelsPerMeter int32
	ClrUsed       uint32
	ClrImportant  uint32
}

// trayState is the overall state shown by the icon badge, in increasing
// order of importance.
type trayState int

const (
	trayIdle trayState = iota
	trayDisconnected
	trayPaused
	trayScanning
	traySyncing
	trayError
)

var trayBadgeColors = map[trayState]color.NRGBA{
	trayDisconnected: {0xF0, 0x9A, 0x1F, 0xFF},
	trayPaused:       {0x8A, 0x8A, 0x8A, 0xFF},
	trayScanning:     {0x5B, 0xC0, 0xEB, 0xFF},
	traySyncing:      {0x1E, 0x6F, 0xD9, 0xFF},
	trayError:        {0xD9, 0x34, 0x2B, 0xFF},
}

type trayFolder struct {
	label string
	path  string
	text  string
}

type trayDevice struct {
	name      string
	connected bool
	paused    bool
}

type trayStatus struct {
	state     trayState
	summary   string
	folders   []trayFolder
	devices   []trayDevice
	allPaused bool
}

type tray struct {
	app        *syncthing.App
	cfg        config.Wrapper
	canRestart bool

	hwnd           windows.HWND
	taskbarCreated uint32
	icons          map[trayState]windows.Handle

	mut    sync.Mutex
	status trayStatus
	shown  trayState

	stop    chan struct{}
	stopped chan struct{}
}

// The window procedure has no context argument, and there is only ever one
// tray icon per process.
var activeTray *tray

// startTray shows a notification area icon with the current sync status and
// a menu of common actions. The returned function removes the icon again.
func (c *serveCmd) startTray(app *syncthing.App, cfg config.Wrapper) func() {
	if c.NoTray {
		return func() {}
	}
	if isService, err := svc.IsWindowsService(); err == nil && isService {
		// Services have no desktop to show an icon on.
		return func() {}
	}

	t := &tray{
		app:        app,
		cfg:        cfg,
		canRestart: c.InternalInnerProcess,
		stop:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}
	ready := make(chan error, 1)
	go t.run(ready)
	if err := <-ready; err != nil {
		slog.Warn("Failed to create notification area icon", slogutil.Error(err))
		return func() {}
	}
	go t.pollStatus()

	return func() {
		close(t.stop)
		procPostMessageW.Call(uintptr(t.hwnd), wmClose, 0, 0)
		select {
		case <-t.stopped:
		case <-time.After(2 * time.Second):
		}
	}
}

// run owns the window and the icon, and must stay on one OS thread.
func (t *tray) run(ready chan<- error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(t.stopped)

	icons, err := makeTrayIcons()
	if err != nil {
		ready <- err
		return
	}
	t.icons = icons
	defer func() {
		for _, icon := range t.icons {
			procDestroyIcon.Call(uintptr(icon))
		}
	}()

	instance, _, _ := procGetModuleHandleW.Call(0)
	className := windows.StringToUTF16Ptr("SyncthingTray")
	wc := wndClassEx{
		WndProc:   windows.NewCallback(trayWndProc),
		Instance:  windows.Handle(instance),
		ClassName: className,
	}
	wc.Size = uint32(unsafe.Sizeof(wc))
	if r, _, err := procRegisterClassExW.Call(uintptr(unsafe.Pointer(&wc))); r == 0 {
		ready <- fmt.Errorf("RegisterClassEx: %w", err)
		return
	}

	activeTray = t
	// A hidden top-level window rather than a message-only one, so that
	// we receive the TaskbarCreated broadcast when Explorer restarts.
	hwnd, _, err := procCreateWindowExW.Call(0, uintptr(unsafe.Pointer(className)), uintptr(unsafe.Pointer(className)),
		0, 0, 0, 0, 0, 0, 0, instance, 0)
	if hwnd == 0 {
		ready <- fmt.Errorf("CreateWindowEx: %w", err)
		return
	}
	t.hwnd = windows.HWND(hwnd)
	tc, _, _ := procRegisterWindowMessageW.Call(uintptr(unsafe.Pointer(windows.StringToUTF16Ptr("TaskbarCreated"))))
	t.taskbarCreated = uint32(tc)

	t.refreshStatus()
	if err := t.addIcon(); err != nil {
		procDestroyWindow.Call(hwnd)
		ready <- err
		return
	}
	ready <- nil

	var msg winMsg
	for {
		r, _, _ := procGetMessageW.Call(uintptr(unsafe.Pointer(&msg)), 0, 0, 0)
		if int32(r) <= 0 {
			return
		}
		procTranslateMessage.Call(uintptr(unsafe.Pointer(&msg)))
		procDispatchMessageW.Call(uintptr(unsafe.Pointer(&msg)))
	}
}

func trayWndProc(hwnd, msg, wParam, lParam uintptr) uintptr {
	t := activeTray
	switch uint32(msg) {
	case wmTrayCallback:
		switch uint32(lParam) & 0xFFFF {
		case ninSelect, ninKeySelect, wmContextMenu:
			t.showMenu()
		}
		return 0
	case wmTrayUpdate:
		t.updateIcon()
		return 0
	case wmClose:
		t.notifyIcon(nimDelete, 0)
		procDestroyWindow.Call(hwnd)
		return 0
	case wmDestroy:
		procPostQuitMessage.Call(0)
		return 0
	}
	if t != nil && t.taskbarCreated != 0 && uint32(msg) == t.taskbarCreated {
		// Explorer restarted and forgot about our icon.
		_ = t.addIcon()
		return 0
	}
	r, _, _ := procDefWindowProcW.Call(hwnd, msg, wParam, lParam)
	return r
}

func (t *tray) notifyIcon(op uint32, flags uint32) error {
	nid := notifyIconData{
		Wnd:             t.hwnd,
		ID:              1,
		Flags:           flags,
		CallbackMessage: wmTrayCallback,
	}
	nid.Size = uint32(unsafe.Sizeof(nid))
	if flags&nifIcon != 0 || flags&nifTip != 0 {
		t.mut.Lock()
		nid.Icon = t.icons[t.status.state]
		t.shown = t.status.state
		tip := "Syncthing: " + t.status.summary
		t.mut.Unlock()
		copyUTF16(nid.Tip[:], tip)
	}
	if op == nimSetVersion {
		nid.TimeoutOrVersion = notifyIconV4
	}
	if r, _, err := procShellNotifyIconW.Call(uintptr(op), uintptr(unsafe.Pointer(&nid))); r == 0 {
		return fmt.Errorf("Shell_NotifyIcon(%d): %w", op, err)
	}
	return nil
}

func (t *tray) addIcon() error {
	if err := t.notifyIcon(nimAdd, nifMessage|nifIcon|nifTip|nifShowTip); err != nil {
		return err
	}
	return t.notifyIcon(nimSetVersion, 0)
}

func (t *tray) updateIcon() {
	_ = t.notifyIcon(nimModify, nifIcon|nifTip|nifShowTip)
}

func (t *tray) pollStatus() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
			if t.refreshStatus() {
				procPostMessageW.Call(uintptr(t.hwnd), wmTrayUpdate, 0, 0)
			}
		}
	}
}

// refreshStatus recomputes the status and reports whether the icon or
// tooltip needs updating.
func (t *tray) refreshStatus() bool {
	st := t.computeStatus()
	t.mut.Lock()
	defer t.mut.Unlock()
	changed := st.state != t.status.state || st.summary != t.status.summary
	t.status = st
	return changed
}

func (t *tray) computeStatus() trayStatus {
	var st trayStatus
	internals := t.app.Internals
	if internals == nil {
		st.summary = "Starting"
		return st
	}

	folders := t.cfg.FolderList()
	slices.SortFunc(folders, func(a, b config.FolderConfiguration) int {
		return strings.Compare(strings.ToLower(folderLabel(a)), strings.ToLower(folderLabel(b)))
	})
	var syncing, scanning, errored, paused int
	var needBytes, globalBytes int64
	for _, fcfg := range folders {
		f := trayFolder{label: folderLabel(fcfg), path: fcfg.Filesystem().URI()}
		state, _, err := internals.FolderState(fcfg.ID)
		switch {
		case fcfg.Paused:
			paused++
			f.text = "Paused"
		case err != nil:
			errored++
			f.text = "Error: " + err.Error()
		case state == "syncing" || state == "sync-preparing":
			syncing++
			f.text = "Syncing"
			if comp, err := internals.Completion(protocol.LocalDeviceID, fcfg.ID); err == nil {
				needBytes += comp.NeedBytes
				globalBytes += comp.GlobalBytes
				f.text = fmt.Sprintf("Syncing %.0f%%", math.Floor(comp.CompletionPct))
			}
		case state == "scanning" || state == "scan-waiting":
			scanning++
			f.text = "Scanning"
		case state == "sync-waiting":
			f.text = "Waiting to sync"
		case state == "idle":
			f.text = "Up to date"
		case state == "":
			f.text = "Stopped"
		default:
			f.text = capitalize(state)
		}
		st.folders = append(st.folders, f)
	}

	myID := t.cfg.MyID()
	var connected, active int
	for _, dcfg := range t.cfg.DeviceList() {
		if dcfg.DeviceID == myID {
			continue
		}
		d := trayDevice{name: dcfg.Name, connected: internals.IsConnectedTo(dcfg.DeviceID), paused: dcfg.Paused}
		if d.name == "" {
			d.name = dcfg.DeviceID.Short().String()
		}
		if !d.paused {
			active++
		}
		if d.connected {
			connected++
		}
		st.devices = append(st.devices, d)
	}
	slices.SortFunc(st.devices, func(a, b trayDevice) int {
		return strings.Compare(strings.ToLower(a.name), strings.ToLower(b.name))
	})
	st.allPaused = len(folders) > 0 && paused == len(folders) && active == 0

	devices := fmt.Sprintf("%d/%d devices connected", connected, len(st.devices))
	switch {
	case errored > 0:
		st.state = trayError
		st.summary = fmt.Sprintf("%d folder(s) with errors", errored)
	case syncing > 0:
		st.state = traySyncing
		st.summary = "Syncing"
		if globalBytes > 0 {
			pct := 100 * (1 - float64(needBytes)/float64(globalBytes))
			st.summary = fmt.Sprintf("Syncing %.0f%%", math.Floor(pct))
		}
	case scanning > 0:
		st.state = trayScanning
		st.summary = "Scanning"
	case st.allPaused:
		st.state = trayPaused
		st.summary = "Paused"
	case active > 0 && connected == 0:
		st.state = trayDisconnected
		st.summary = "No devices connected"
	default:
		st.summary = "Up to date"
	}
	st.summary += " · " + devices
	return st
}

func (t *tray) showMenu() {
	t.refreshStatus()
	t.mut.Lock()
	st := t.status
	t.mut.Unlock()
	t.updateIcon()

	menu, _, _ := procCreatePopupMenu.Call()
	if menu == 0 {
		return
	}
	defer procDestroyMenu.Call(menu)

	appendMenu(menu, mfString, trayCmdOpen, "Open Syncthing")
	procSetMenuDefaultItem.Call(menu, trayCmdOpen, 0)
	appendMenu(menu, mfSeparator, 0, "")
	appendMenu(menu, mfString|mfGrayed, 0, st.summary)

	if len(st.folders) > 0 {
		appendMenu(menu, mfSeparator, 0, "")
		for i, f := range st.folders {
			appendMenu(menu, mfString, uintptr(trayCmdFolderBase+i), f.label+"\t"+truncate(f.text, 48))
		}
	}
	if len(st.devices) > 0 {
		sub, _, _ := procCreatePopupMenu.Call()
		for _, d := range st.devices {
			text := "Disconnected"
			flags := uintptr(mfString | mfGrayed)
			switch {
			case d.paused:
				text = "Paused"
			case d.connected:
				text = "Connected"
				flags |= mfChecked
			}
			appendMenu(sub, flags, 0, d.name+"\t"+text)
		}
		// The submenu is destroyed along with its parent.
		appendMenu(menu, mfString|mfPopup, sub, "Devices")
	}

	appendMenu(menu, mfSeparator, 0, "")
	appendMenu(menu, mfString, trayCmdRescan, "Rescan all folders")
	if st.allPaused {
		appendMenu(menu, mfString, trayCmdPause, "Resume all")
	} else {
		appendMenu(menu, mfString, trayCmdPause, "Pause all")
	}
	if t.canRestart {
		appendMenu(menu, mfString, trayCmdRestart, "Restart")
	}
	appendMenu(menu, mfString, trayCmdQuit, "Quit Syncthing")

	var pt winPoint
	procGetCursorPos.Call(uintptr(unsafe.Pointer(&pt)))
	// Without this the menu doesn't close when clicking elsewhere.
	procSetForegroundWindow.Call(uintptr(t.hwnd))
	cmd, _, _ := procTrackPopupMenu.Call(menu, tpmReturnCmd|tpmRightButton|tpmBottomAlign,
		uintptr(pt.X), uintptr(pt.Y), 0, uintptr(t.hwnd), 0)
	procPostMessageW.Call(uintptr(t.hwnd), wmNull, 0, 0)

	t.runCommand(int(cmd), st)
}

func (t *tray) runCommand(cmd int, st trayStatus) {
	switch {
	case cmd == trayCmdOpen:
		go func() {
			if err := (browserCmd{}).Run(); err != nil {
				slog.Error("Failed to open browser", slogutil.Error(err))
			}
		}()
	case cmd == trayCmdRescan:
		go t.app.Internals.ScanFolders()
	case cmd == trayCmdPause:
		paused := !st.allPaused
		go func() {
			_, err := t.cfg.Modify(func(cfg *config.Configuration) {
				for i := range cfg.Devices {
					cfg.Devices[i].Paused = paused
				}
				for i := range cfg.Folders {
					cfg.Folders[i].Paused = paused
				}
			})
			if err != nil {
				slog.Error("Cannot adjust paused state", slogutil.Error(err))
			}
			if t.refreshStatus() {
				procPostMessageW.Call(uintptr(t.hwnd), wmTrayUpdate, 0, 0)
			}
		}()
	case cmd == trayCmdRestart:
		go t.app.Stop(svcutil.ExitRestart)
	case cmd == trayCmdQuit:
		go t.app.Stop(svcutil.ExitSuccess)
	case cmd >= trayCmdFolderBase && cmd-trayCmdFolderBase < len(st.folders):
		path := st.folders[cmd-trayCmdFolderBase].path
		go func() {
			if err := openURL(path); err != nil {
				slog.Error("Failed to open folder", slogutil.FilePath(path), slogutil.Error(err))
			}
		}()
	}
}

func appendMenu(menu, flags, id uintptr, text string) {
	var ptr uintptr
	if text != "" {
		// A lone ampersand would underline the next character.
		ptr = uintptr(unsafe.Pointer(windows.StringToUTF16Ptr(strings.ReplaceAll(text, "&", "&&"))))
	}
	procAppendMenuW.Call(menu, flags, id, ptr)
}

// makeTrayIcons renders the logo once per state, with a colored badge in
// the corner for anything other than idle.
func makeTrayIcons() (map[trayState]windows.Handle, error) {
	logo, err := png.Decode(bytes.NewReader(trayIconPNG))
	if err != nil {
		return nil, err
	}
	icons := make(map[trayState]windows.Handle)
	for _, state := range []trayState{trayIdle, trayDisconnected, trayPaused, trayScanning, traySyncing, trayError} {
		img := image.NewNRGBA(logo.Bounds())
		for y := img.Rect.Min.Y; y < img.Rect.Max.Y; y++ {
			for x := img.Rect.Min.X; x < img.Rect.Max.X; x++ {
				img.Set(x, y, logo.At(x, y))
			}
		}
		if c, ok := trayBadgeColors[state]; ok {
			drawBadge(img, c)
		}
		icon, err := createIcon(img)
		if err != nil {
			for _, icon := range icons {
				procDestroyIcon.Call(uintptr(icon))
			}
			return nil, err
		}
		icons[state] = icon
	}
	return icons, nil
}

// drawBadge paints an antialiased dot with a white ring in the bottom
// right corner.
func drawBadge(img *image.NRGBA, c color.NRGBA) {
	size := float64(img.Rect.Dx())
	r := size * 0.24
	ring := size * 0.06
	cx, cy := size-r-ring, size-r-ring
	white := color.NRGBA{0xFF, 0xFF, 0xFF, 0xFF}
	for y := 0; y < img.Rect.Dy(); y++ {
		for x := 0; x < img.Rect.Dx(); x++ {
			d := math.Hypot(float64(x)+0.5-cx, float64(y)+0.5-cy)
			if outer := clamp01(r + ring - d + 0.5); outer > 0 {
				blend(img, x, y, white, outer)
			}
			if inner := clamp01(r - d + 0.5); inner > 0 {
				blend(img, x, y, c, inner)
			}
		}
	}
}

func blend(img *image.NRGBA, x, y int, c color.NRGBA, a float64) {
	dst := img.NRGBAAt(x, y)
	da := float64(dst.A) / 255
	oa := a + da*(1-a)
	if oa == 0 {
		return
	}
	mix := func(s, d uint8) uint8 {
		return uint8((float64(s)*a + float64(d)*da*(1-a)) / oa)
	}
	img.SetNRGBA(x, y, color.NRGBA{mix(c.R, dst.R), mix(c.G, dst.G), mix(c.B, dst.B), uint8(oa * 255)})
}

func createIcon(img *image.NRGBA) (windows.Handle, error) {
	w, h := img.Rect.Dx(), img.Rect.Dy()
	bmi := bitmapInfoHeader{
		Width:    int32(w),
		Height:   -int32(h), // top-down
		Planes:   1,
		BitCount: 32,
	}
	bmi.Size = uint32(unsafe.Sizeof(bmi))
	var bits unsafe.Pointer
	hbmColor, _, err := procCreateDIBSection.Call(0, uintptr(unsafe.Pointer(&bmi)), 0, uintptr(unsafe.Pointer(&bits)), 0, 0)
	if hbmColor == 0 {
		return 0, fmt.Errorf("CreateDIBSection: %w", err)
	}
	defer procDeleteObject.Call(hbmColor)
	px := unsafe.Slice((*byte)(bits), w*h*4)
	for i := 0; i < w*h; i++ {
		px[i*4+0] = img.Pix[i*4+2]
		px[i*4+1] = img.Pix[i*4+1]
		px[i*4+2] = img.Pix[i*4+0]
		px[i*4+3] = img.Pix[i*4+3]
	}

	mask, _, err := procCreateBitmap.Call(uintptr(w), uintptr(h), 1, 1, 0)
	if mask == 0 {
		return 0, fmt.Errorf("CreateBitmap: %w", err)
	}
	defer procDeleteObject.Call(mask)

	ii := iconInfo{Icon: 1, Mask: windows.Handle(mask), Color: windows.Handle(hbmColor)}
	icon, _, err := procCreateIconIndirect.Call(uintptr(unsafe.Pointer(&ii)))
	if icon == 0 {
		return 0, fmt.Errorf("CreateIconIndirect: %w", err)
	}
	return windows.Handle(icon), nil
}

func folderLabel(fcfg config.FolderConfiguration) string {
	if fcfg.Label != "" {
		return fcfg.Label
	}
	return fcfg.ID
}

func copyUTF16(dst []uint16, s string) {
	u, _ := windows.UTF16FromString(truncate(s, len(dst)-1))
	copy(dst[:len(dst)-1], u)
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + strings.ReplaceAll(s[1:], "-", " ")
}

func clamp01(v float64) float64 {
	return math.Max(0, math.Min(1, v))
}

// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	modelmocks "github.com/syncthing/syncthing/lib/model/mocks"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestLinkSpeedAPI(t *testing.T) {
	m := new(modelmocks.Model)
	s := &service{
		id:    protocol.LocalDeviceID,
		cfg:   config.Wrap("", config.Configuration{Devices: []config.DeviceConfiguration{{DeviceID: dev1}}}, protocol.LocalDeviceID, events.NoopLogger),
		model: m,
	}
	call := func(device string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.postLinkSpeedTest(w, httptest.NewRequest(http.MethodPost, "/rest/system/linkspeed?device="+device, nil))
		return w
	}
	if w := call("invalid"); w.Code != http.StatusBadRequest {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(""); w.Code != http.StatusBadRequest {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(dev1.String()); w.Code != http.StatusConflict {
		t.Fatal(w.Code, w.Body.String())
	}
	m.ConnectedToReturns(true)
	s.linkSpeedRunning.Store(true)
	if w := call(dev1.String()); w.Code != http.StatusConflict {
		t.Fatal(w.Code, w.Body.String())
	}
	s.linkSpeedRunning.Store(false)
	m.RequestGlobalReturns(nil, protocol.ErrLinkSpeedUnsupported)
	if w := call(dev1.String()); w.Code != http.StatusNotImplemented {
		t.Fatal(w.Code, w.Body.String())
	}
	if s.linkSpeedRunning.Load() {
		t.Fatal("failed test did not release running guard")
	}
	m.RequestGlobalReturns(nil, context.DeadlineExceeded)
	if w := call(dev1.String()); w.Code != http.StatusGatewayTimeout {
		t.Fatal(w.Code, w.Body.String())
	}
	block := make([]byte, 1<<20)
	m.RequestGlobalCalls(func(_ context.Context, id protocol.DeviceID, folder, name string, _ int, _ int64, size int, _ []byte, _ bool) ([]byte, error) {
		if id != dev1 || folder != "" || size != len(block) {
			t.Error("incorrect test request")
		}
		if name == ".st-link-speed-v1-upload" {
			return block[:32], nil
		}
		return block, nil
	})
	w := call(dev1.String())
	if w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	var result protocol.LinkSpeedResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || result.Download.Bytes == 0 || result.Upload.Bytes == 0 {
		t.Fatal("invalid result", w.Body.String(), err)
	}
}

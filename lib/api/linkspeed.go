// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/syncthing/syncthing/lib/protocol"
)

func (s *service) postLinkSpeedTest(w http.ResponseWriter, r *http.Request) {
	id, err := protocol.DeviceIDFromString(r.URL.Query().Get("device"))
	if err != nil || id == protocol.EmptyDeviceID || id == s.id {
		http.Error(w, "invalid remote device ID", http.StatusBadRequest)
		return
	}
	if _, ok := s.cfg.Device(id); !ok {
		http.Error(w, "no such device", http.StatusNotFound)
		return
	}
	if !s.model.ConnectedTo(id) {
		http.Error(w, "remote device is disconnected", http.StatusConflict)
		return
	}
	if !s.linkSpeedRunning.CompareAndSwap(false, true) {
		http.Error(w, "a link speed test is already running", http.StatusConflict)
		return
	}
	defer s.linkSpeedRunning.Store(false)
	result, err := protocol.TestLinkSpeed(r.Context(), func(ctx context.Context, req *protocol.Request) ([]byte, error) {
		return s.model.RequestGlobal(ctx, id, req.Folder, req.Name, 0, 0, req.Size, nil, false)
	})
	if err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, protocol.ErrLinkSpeedUnsupported) {
			status = http.StatusNotImplemented
		} else if errors.Is(err, context.DeadlineExceeded) {
			status = http.StatusGatewayTimeout
		}
		http.Error(w, err.Error(), status)
		return
	}
	sendJSON(w, result)
}

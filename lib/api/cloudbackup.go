// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package api

import "net/http"

func (s *service) getCloudBackup(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("folder")
	if _, ok := s.cfg.Folder(id); !ok {
		http.Error(w, "no such folder", http.StatusNotFound)
		return
	}
	if s.cloudBackup == nil {
		http.Error(w, "backup service unavailable", http.StatusServiceUnavailable)
		return
	}
	sendJSON(w, s.cloudBackup.Status(id))
}
func (s *service) postCloudBackup(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("folder")
	f, ok := s.cfg.Folder(id)
	if !ok {
		http.Error(w, "no such folder", http.StatusNotFound)
		return
	}
	if !f.CloudBackup.Enabled || f.Paused {
		http.Error(w, "backup disabled or folder paused", http.StatusConflict)
		return
	}
	if s.cloudBackup == nil {
		http.Error(w, "backup service unavailable", http.StatusServiceUnavailable)
		return
	}
	if !s.cloudBackup.Run(id) {
		http.Error(w, "backup queue full", http.StatusTooManyRequests)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/syncthing/syncthing/lib/cloudbackup"
)

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

func (s *service) googleAccount(w http.ResponseWriter) *cloudbackup.Account {
	if s.cloudBackup == nil || s.cloudBackup.Account() == nil {
		http.Error(w, "backup service unavailable", http.StatusServiceUnavailable)
		return nil
	}
	return s.cloudBackup.Account()
}
func (s *service) getGoogleAccount(w http.ResponseWriter, _ *http.Request) {
	if a := s.googleAccount(w); a != nil {
		sendJSON(w, a.Status())
	}
}

// postGoogleConnect starts authorization. The optional body supplies an OAuth
// client for builds without one: {"clientID": "...", "clientSecret": "..."}.
func (s *service) postGoogleConnect(w http.ResponseWriter, r *http.Request) {
	a := s.googleAccount(w)
	if a == nil {
		return
	}
	var req struct {
		ClientID     string `json:"clientID"`
		ClientSecret string `json:"clientSecret"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	authURL, err := a.Connect(req.ClientID, req.ClientSecret)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sendJSON(w, map[string]string{"url": authURL})
}
func (s *service) deleteGoogleAccount(w http.ResponseWriter, r *http.Request) {
	if a := s.googleAccount(w); a != nil {
		if err := a.Disconnect(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

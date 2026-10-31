// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package cloudbackup

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

type memStore struct {
	mu sync.Mutex
	m  map[string]string
}

func (s *memStore) String(k string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok, nil
}
func (s *memStore) PutString(k, v string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[string]string{}
	}
	s.m[k] = v
	return nil
}
func (s *memStore) Delete(k string) error { s.mu.Lock(); delete(s.m, k); s.mu.Unlock(); return nil }

func TestConnectStoresRefreshToken(t *testing.T) {
	d := newFakeDrive(t)
	var mu sync.Mutex
	var verifier, revoked string
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		mu.Lock()
		verifier = r.Form.Get("code_verifier")
		mu.Unlock()
		if r.Form.Get("code") != "code" || r.Form.Get("client_id") != "client" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"access_token": "access", "refresh_token": "refresh", "token_type": "Bearer", "expires_in": 3600})
	})
	mux.HandleFunc("/revoke", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		mu.Lock()
		revoked = r.Form.Get("token")
		mu.Unlock()
	})
	tokenSrv := httptest.NewServer(mux)
	defer tokenSrv.Close()
	t.Setenv("STGOOGLECLIENTID", "")
	store := &memStore{}
	a := NewAccount(store)
	a.endpoint = oauth2.Endpoint{AuthURL: "https://accounts.example/auth", TokenURL: tokenSrv.URL + "/token", AuthStyle: oauth2.AuthStyleInParams}
	a.revokeURL = tokenSrv.URL + "/revoke"
	a.apiURL = d.srv.URL + "/drive/v3"
	if GoogleClientID == "" && a.Status().ClientConfigured {
		t.Fatal("client reported configured without credentials")
	}
	authURL, err := a.Connect("client", "secret")
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(authURL)
	q := u.Query()
	if q.Get("scope") != driveScope || q.Get("access_type") != "offline" || q.Get("code_challenge_method") != "S256" {
		t.Fatal("unexpected authorization request", authURL)
	}
	if !a.Status().Pending {
		t.Fatal("flow not pending")
	}
	// Bad state is rejected without completing the flow.
	if resp, err := http.Get(q.Get("redirect_uri") + "?state=wrong&code=code"); err != nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatal("bad state accepted", err)
	}
	resp, err := http.Get(q.Get("redirect_uri") + "?state=" + q.Get("state") + "&code=code")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	st := a.Status()
	mu.Lock()
	defer mu.Unlock()
	if !st.Connected || st.Email != "user@example.com" || st.Error != "" || verifier == "" {
		t.Fatal("connection not stored", st)
	}
	if tok, _, _ := store.String(keyToken); strings.Contains(tok, "secret") {
		t.Fatal("client secret stored with token")
	}
	client, err := a.drive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if about, err := client.about(context.Background()); err != nil || about.User.EmailAddress == "" {
		t.Fatal("authorized client unusable", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for a.Status().Pending && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if a.Status().Pending {
		t.Fatal("loopback listener not closed after completion")
	}
	mu.Unlock()
	err = a.Disconnect(context.Background())
	mu.Lock()
	if err != nil {
		t.Fatal(err)
	}
	if a.Status().Connected || revoked != "refresh" {
		t.Fatal("disconnect retained or did not revoke token", revoked)
	}
}

func TestDriveRequiresConnection(t *testing.T) {
	if _, err := NewAccount(&memStore{}).drive(context.Background()); err == nil {
		t.Fatal("unconnected account returned a client")
	}
}

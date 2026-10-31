// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package cloudbackup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

// Google OAuth client for the installed-app flow. Google documents that such
// client secrets cannot be kept confidential; PKCE and the loopback redirect
// protect the exchange. Release builds may embed a client with -ldflags -X;
// STGOOGLECLIENTID and STGOOGLECLIENTSECRET override it, and the GUI may store one.
var (
	GoogleClientID     string
	GoogleClientSecret string
)

// The drive.file scope only grants access to files this app creates, never to
// the rest of the account's Drive.
const driveScope = "https://www.googleapis.com/auth/drive.file"

var googleEndpoint = oauth2.Endpoint{
	AuthURL:   "https://accounts.google.com/o/oauth2/auth",
	TokenURL:  "https://oauth2.googleapis.com/token",
	AuthStyle: oauth2.AuthStyleInParams,
}

const (
	keyClient  = "cloudbackup-google-client"
	keyToken   = "cloudbackup-google-token"
	keyAccount = "cloudbackup-google-email"
)

// Store persists credentials outside the exported configuration.
type Store interface {
	String(key string) (string, bool, error)
	PutString(key, val string) error
	Delete(key string) error
}

type clientCredentials struct {
	ID     string `json:"id"`
	Secret string `json:"secret"`
}

// AccountStatus is reported to the GUI. It never contains credentials.
type AccountStatus struct {
	ClientConfigured bool   `json:"clientConfigured"`
	Connected        bool   `json:"connected"`
	Email            string `json:"email,omitempty"`
	Pending          bool   `json:"pending"`
	Error            string `json:"error,omitempty"`
}

// Account holds the single Google account used by all folder backups.
type Account struct {
	store Store
	// Overridable in tests.
	endpoint  oauth2.Endpoint
	apiURL    string
	revokeURL string

	mu      sync.Mutex
	pending *pendingAuth
	lastErr string
}

type pendingAuth struct {
	listener net.Listener
	cancel   context.CancelFunc
}

func NewAccount(store Store) *Account {
	return &Account{store: store, endpoint: googleEndpoint, apiURL: driveAPI, revokeURL: "https://oauth2.googleapis.com/revoke"}
}

func (a *Account) client() (clientCredentials, error) {
	if id := os.Getenv("STGOOGLECLIENTID"); id != "" {
		return clientCredentials{id, os.Getenv("STGOOGLECLIENTSECRET")}, nil
	}
	if a.store != nil {
		if s, ok, err := a.store.String(keyClient); err != nil {
			return clientCredentials{}, err
		} else if ok {
			var c clientCredentials
			if err := json.Unmarshal([]byte(s), &c); err == nil && c.ID != "" {
				return c, nil
			}
		}
	}
	if GoogleClientID != "" {
		return clientCredentials{GoogleClientID, GoogleClientSecret}, nil
	}
	return clientCredentials{}, errors.New("no Google OAuth client configured")
}

func (a *Account) config(redirect string) (*oauth2.Config, error) {
	c, err := a.client()
	if err != nil {
		return nil, err
	}
	return &oauth2.Config{ClientID: c.ID, ClientSecret: c.Secret, Endpoint: a.endpoint, Scopes: []string{driveScope}, RedirectURL: redirect}, nil
}

func (a *Account) Status() AccountStatus {
	_, clientErr := a.client()
	st := AccountStatus{ClientConfigured: clientErr == nil}
	if a.store != nil {
		_, st.Connected, _ = a.store.String(keyToken)
		st.Email, _, _ = a.store.String(keyAccount)
	}
	a.mu.Lock()
	st.Pending = a.pending != nil
	st.Error = a.lastErr
	a.mu.Unlock()
	return st
}

// Connect starts the authorization flow and returns the URL the user opens.
// Google redirects the browser to a temporary loopback listener, so the GUI
// must be used from a browser running on this computer.
func (a *Account) Connect(clientID, clientSecret string) (string, error) {
	if a.store == nil {
		return "", errors.New("no credential store")
	}
	if clientID != "" {
		data, _ := json.Marshal(clientCredentials{strings.TrimSpace(clientID), strings.TrimSpace(clientSecret)})
		if err := a.store.PutString(keyClient, string(data)); err != nil {
			return "", err
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	redirect := "http://" + ln.Addr().String() + "/"
	conf, err := a.config(redirect)
	if err != nil {
		ln.Close()
		return "", err
	}
	state := randomID()
	verifier := oauth2.GenerateVerifier()
	authURL := conf.AuthCodeURL(state, oauth2.AccessTypeOffline, oauth2.ApprovalForce, oauth2.S256ChallengeOption(verifier))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	a.mu.Lock()
	if a.pending != nil {
		a.pending.cancel()
	}
	p := &pendingAuth{listener: ln, cancel: cancel}
	a.pending = p
	a.lastErr = ""
	a.mu.Unlock()

	done := make(chan struct{})
	srv := &http.Server{ReadHeaderTimeout: 10 * time.Second}
	srv.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("state") != state {
			http.Error(w, "invalid state", http.StatusBadRequest)
			return
		}
		err := a.finish(ctx, conf, q, verifier)
		msg := "Google Drive connected. You can close this window and return to Syncthing."
		if err != nil {
			msg = "Connecting Google Drive failed: " + err.Error()
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, "<!doctype html><title>Syncthing</title><p>%s</p>", html.EscapeString(msg))
		select {
		case <-done:
		default:
			close(done)
		}
	})
	go srv.Serve(ln)
	go func() {
		select {
		case <-ctx.Done():
		case <-done:
			// Let the response reach the browser before closing.
			time.Sleep(time.Second)
		}
		srv.Close()
		cancel()
		a.mu.Lock()
		if a.pending == p {
			a.pending = nil
			if ctx.Err() == context.DeadlineExceeded && a.lastErr == "" {
				if _, ok, _ := a.store.String(keyToken); !ok {
					a.lastErr = "authorization timed out"
				}
			}
		}
		a.mu.Unlock()
	}()
	return authURL, nil
}

func (a *Account) finish(ctx context.Context, conf *oauth2.Config, q url.Values, verifier string) (err error) {
	defer func() {
		a.mu.Lock()
		a.lastErr = ""
		if err != nil {
			a.lastErr = err.Error()
		}
		a.mu.Unlock()
	}()
	if e := q.Get("error"); e != "" {
		return errors.New(e)
	}
	tok, err := conf.Exchange(ctx, q.Get("code"), oauth2.VerifierOption(verifier))
	if err != nil {
		return err
	}
	if tok.RefreshToken == "" {
		return errors.New("no refresh token returned by Google")
	}
	if err := a.saveToken(tok); err != nil {
		return err
	}
	d := &drive{client: conf.Client(ctx, tok), api: a.apiURL}
	if about, err := d.about(ctx); err == nil {
		_ = a.store.PutString(keyAccount, about.User.EmailAddress)
	} else {
		slog.Warn("Google Drive account lookup failed", "error", err)
	}
	slog.Info("Google Drive connected for cloud backup")
	return nil
}

func (a *Account) saveToken(tok *oauth2.Token) error {
	data, err := json.Marshal(tok)
	if err != nil {
		return err
	}
	return a.store.PutString(keyToken, string(data))
}

func (a *Account) Disconnect(ctx context.Context) error {
	if a.store == nil {
		return nil
	}
	if s, ok, _ := a.store.String(keyToken); ok {
		var tok oauth2.Token
		if json.Unmarshal([]byte(s), &tok) == nil && tok.RefreshToken != "" {
			// Revocation is best effort; local credentials are removed regardless.
			form := url.Values{"token": {tok.RefreshToken}}
			if req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.revokeURL, strings.NewReader(form.Encode())); err == nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				if resp, err := http.DefaultClient.Do(req); err == nil {
					resp.Body.Close()
				}
			}
		}
	}
	a.mu.Lock()
	a.lastErr = ""
	a.mu.Unlock()
	return errors.Join(a.store.Delete(keyToken), a.store.Delete(keyAccount))
}

// persistingSource stores refreshed tokens so restarts reuse them.
type persistingSource struct {
	account *Account
	src     oauth2.TokenSource
	mu      sync.Mutex
	last    string
}

func (p *persistingSource) Token() (*oauth2.Token, error) {
	tok, err := p.src.Token()
	if err != nil {
		return nil, fmt.Errorf("google authorization failed; reconnect Google Drive: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if tok.AccessToken != p.last {
		p.last = tok.AccessToken
		if err := p.account.saveToken(tok); err != nil {
			slog.Warn("Saving refreshed Google token failed", "error", err)
		}
	}
	return tok, nil
}

// drive returns an authorized client, or an error when no account is connected.
func (a *Account) drive(ctx context.Context) (*drive, error) {
	if a == nil || a.store == nil {
		return nil, errors.New("not connected to Google Drive")
	}
	s, ok, err := a.store.String(keyToken)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("not connected to Google Drive")
	}
	var tok oauth2.Token
	if err := json.Unmarshal([]byte(s), &tok); err != nil {
		return nil, err
	}
	conf, err := a.config("")
	if err != nil {
		return nil, err
	}
	src := &persistingSource{account: a, src: conf.TokenSource(context.Background(), &tok), last: tok.AccessToken}
	return &drive{client: oauth2.NewClient(ctx, src), api: a.apiURL, upload: strings.Replace(a.apiURL, "/drive/v3", "/upload/drive/v3", 1)}, nil
}

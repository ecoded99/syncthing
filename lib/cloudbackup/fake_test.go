// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package cloudbackup

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeDrive implements the subset of the Drive v3 API used by the backup.
type fakeDrive struct {
	t        *testing.T
	srv      *httptest.Server
	mu       sync.Mutex
	files    map[string]*fakeFile
	next     int
	requests int
	sessions map[string]*fakeSession
	// corrupt makes uploads report a wrong checksum.
	corrupt bool
	limit   int64
}

type fakeFile struct {
	driveFile
	data []byte
}

type fakeSession struct {
	id   string
	meta driveFile
	buf  []byte
}

var (
	parentsRe = regexp.MustCompile(`'([^']+)' in parents`)
	ownerRe   = regexp.MustCompile(`value='([^']+)'`)
)

func newFakeDrive(t *testing.T) *fakeDrive {
	f := &fakeDrive{t: t, files: map[string]*fakeFile{}, sessions: map[string]*fakeSession{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeDrive) client() *drive {
	return &drive{client: f.srv.Client(), api: f.srv.URL + "/drive/v3", upload: f.srv.URL + "/upload/drive/v3"}
}

func (f *fakeDrive) id() string { f.next++; return "id" + strconv.Itoa(f.next) }

func (f *fakeDrive) store(id string, meta driveFile, data []byte) driveFile {
	file, ok := f.files[id]
	if !ok {
		file = &fakeFile{driveFile: driveFile{ID: id, Name: meta.Name, Parents: meta.Parents, AppProperties: meta.AppProperties, MimeType: "application/octet-stream"}}
		f.files[id] = file
	}
	if meta.ModifiedTime != "" {
		file.ModifiedTime = meta.ModifiedTime
	}
	file.data = data
	sum := md5.Sum(data)
	file.MD5 = hex.EncodeToString(sum[:])
	if f.corrupt {
		file.MD5 = strings.Repeat("0", 32)
	}
	file.Size = strconv.Itoa(len(data))
	return file.driveFile
}

func (f *fakeDrive) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests++
	reply := func(v any) { json.NewEncoder(w).Encode(v) }
	p := r.URL.Path
	switch {
	case r.Method == http.MethodGet && p == "/drive/v3/about":
		a := driveAbout{}
		a.User.EmailAddress = "user@example.com"
		if f.limit > 0 {
			a.StorageQuota.Limit = strconv.FormatInt(f.limit, 10)
			a.StorageQuota.Usage = "0"
		}
		reply(a)
	case r.Method == http.MethodGet && p == "/drive/v3/files":
		q := r.URL.Query().Get("q")
		var out []driveFile
		parents := map[string]bool{}
		for _, m := range parentsRe.FindAllStringSubmatch(q, -1) {
			parents[m[1]] = true
		}
		owner := ownerRe.FindStringSubmatch(q)
		for _, file := range f.files {
			switch {
			case owner != nil:
				if file.AppProperties[ownerKey] == owner[1] {
					out = append(out, file.driveFile)
				}
			case len(file.Parents) > 0 && parents[file.Parents[0]]:
				out = append(out, file.driveFile)
			}
		}
		reply(map[string]any{"files": out})
	case r.Method == http.MethodPost && p == "/drive/v3/files":
		var meta driveFile
		json.NewDecoder(r.Body).Decode(&meta)
		id := f.id()
		meta.ID = id
		f.files[id] = &fakeFile{driveFile: meta}
		reply(meta)
	case r.Method == http.MethodPost && strings.HasPrefix(p, "/drive/v3/files/") && strings.HasSuffix(p, "/copy"):
		src := f.files[strings.TrimSuffix(strings.TrimPrefix(p, "/drive/v3/files/"), "/copy")]
		var meta driveFile
		json.NewDecoder(r.Body).Decode(&meta)
		meta.ModifiedTime = src.ModifiedTime
		reply(f.store(f.id(), meta, append([]byte(nil), src.data...)))
	case strings.HasPrefix(p, "/upload/drive/v3/files"):
		id := strings.TrimPrefix(strings.TrimPrefix(p, "/upload/drive/v3/files"), "/")
		switch r.URL.Query().Get("uploadType") {
		case "multipart":
			_, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
			mr := multipart.NewReader(r.Body, params["boundary"])
			part, _ := mr.NextPart()
			var meta driveFile
			json.NewDecoder(part).Decode(&meta)
			part, _ = mr.NextPart()
			data, _ := io.ReadAll(part)
			if id == "" {
				id = f.id()
			}
			reply(f.store(id, meta, data))
		case "resumable":
			var meta driveFile
			json.NewDecoder(r.Body).Decode(&meta)
			s := &fakeSession{id: id, meta: meta}
			key := f.id()
			f.sessions[key] = s
			w.Header().Set("Location", f.srv.URL+"/session/"+key)
		}
	case r.Method == http.MethodPut && strings.HasPrefix(p, "/session/"):
		s := f.sessions[strings.TrimPrefix(p, "/session/")]
		data, _ := io.ReadAll(r.Body)
		var start, end, total int64
		fmt.Sscanf(r.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total)
		if start != int64(len(s.buf)) {
			http.Error(w, "bad offset", http.StatusBadRequest)
			return
		}
		s.buf = append(s.buf, data...)
		if int64(len(s.buf)) < total {
			w.WriteHeader(http.StatusPermanentRedirect)
			return
		}
		id := s.id
		if id == "" {
			id = f.id()
		}
		reply(f.store(id, s.meta, s.buf))
	default:
		http.Error(w, "unexpected "+r.Method+" "+p, http.StatusNotImplemented)
	}
}

// lookup resolves a slash-separated path below the folder with the given ID.
func (f *fakeDrive) lookup(parent, rel string) *fakeFile {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, name := range strings.Split(rel, "/") {
		var found *fakeFile
		for _, file := range f.files {
			if file.Name == name && len(file.Parents) > 0 && file.Parents[0] == parent {
				found = file
			}
		}
		if found == nil {
			return nil
		}
		parent = found.ID
	}
	for _, file := range f.files {
		if file.ID == parent {
			return file
		}
	}
	return nil
}

// versions returns the contents of every retained version of rel.
func (f *fakeDrive) versions(rootID, rel string) []string {
	runs := f.lookup(rootID, "Versions")
	if runs == nil {
		return nil
	}
	f.mu.Lock()
	var ids []string
	for _, file := range f.files {
		if len(file.Parents) > 0 && file.Parents[0] == runs.ID {
			ids = append(ids, file.ID)
		}
	}
	f.mu.Unlock()
	var out []string
	for _, id := range ids {
		if v := f.lookup(id, rel); v != nil {
			out = append(out, string(v.data))
		}
	}
	return out
}

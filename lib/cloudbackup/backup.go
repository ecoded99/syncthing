// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

// Package cloudbackup implements a secondary one-way backup into Google Drive.
// It uploads through the Drive API with the drive.file scope, so it only sees
// the folders and files it created itself.
package cloudbackup

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	stfs "github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore"
)

type Status struct {
	State    string    `json:"state"`
	Started  time.Time `json:"started,omitempty"`
	Finished time.Time `json:"finished,omitempty"`
	Files    int64     `json:"files"`
	Bytes    int64     `json:"bytes"`
	Failed   int64     `json:"failed"`
	Error    string    `json:"error,omitempty"`
	// CloudVerified is set when every file in the run was uploaded and Drive
	// reported the same MD5 checksum as the local content.
	CloudVerified  bool   `json:"cloudVerified"`
	DriveFolderURL string `json:"driveFolderURL,omitempty"`
}

type Service struct {
	cfg          config.Wrapper
	account      *Account
	device       string
	mu           sync.RWMutex
	statuses     map[string]Status
	requested    chan string
	activeCancel context.CancelFunc
	primaryBusy  func() bool
}

func New(cfg config.Wrapper, account *Account, device string, primaryBusy ...func() bool) *Service {
	s := &Service{cfg: cfg, account: account, device: device, statuses: make(map[string]Status), requested: make(chan string, 16)}
	if len(primaryBusy) > 0 {
		s.primaryBusy = primaryBusy[0]
	}
	return s
}
func (*Service) String() string { return "cloud backup" }

// Account returns the Google account shared by all folder backups.
func (s *Service) Account() *Account { return s.account }
func (s *Service) Status(id string) Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if st, ok := s.statuses[id]; ok {
		return st
	}
	return Status{State: "idle"}
}
func (s *Service) Run(id string) bool {
	select {
	case s.requested <- id:
		return true
	default:
		return false
	}
}
func (s *Service) set(id string, st Status) { s.mu.Lock(); s.statuses[id] = st; s.mu.Unlock() }
func (s *Service) CommitConfiguration(from, to config.Configuration) bool {
	if !backupInputsChanged(from, to) {
		return true
	}
	s.mu.RLock()
	cancel := s.activeCancel
	s.mu.RUnlock()
	if cancel != nil {
		cancel()
	}
	return true
}
func (s *Service) Serve(ctx context.Context) error {
	s.cfg.Subscribe(s)
	defer s.cfg.Unsubscribe(s)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	run := func(id string, f config.FolderConfiguration) {
		runCtx, cancel := context.WithCancel(ctx)
		s.mu.Lock()
		s.activeCancel = cancel
		s.mu.Unlock()
		defer func() { cancel(); s.mu.Lock(); s.activeCancel = nil; s.mu.Unlock() }()
		st := Status{State: "uploading", Started: time.Now().UTC(), DriveFolderURL: s.Status(id).DriveFolderURL}
		s.set(id, st)
		if s.primaryBusy != nil {
			control := &primaryControl{busy: s.primaryBusy, interval: 250 * time.Millisecond, idleDelay: time.Second}
			control.state = func(waiting bool) {
				st.State = "uploading"
				if waiting {
					st.State = "waiting-for-primary"
				}
				s.set(id, st)
			}
			runCtx = context.WithValue(runCtx, primaryControlKey{}, control)
		}
		var result Result
		d, err := s.account.drive(runCtx)
		if err == nil {
			result, err = Backup(runCtx, d, f, s.device, func(r Result) {
				st.Files, st.Bytes, st.Failed = r.Files, r.Bytes, r.Failed
				if r.RootID != "" {
					st.DriveFolderURL = folderURL(r.RootID)
				}
				s.set(id, st)
			})
		}
		st.Files = result.Files
		st.Bytes = result.Bytes
		st.Failed = result.Failed
		if result.RootID != "" {
			st.DriveFolderURL = folderURL(result.RootID)
		}
		st.Finished = time.Now().UTC()
		st.State = "complete"
		st.CloudVerified = true
		if err != nil {
			st.State = "error"
			st.CloudVerified = false
			st.Error = err.Error()
			slog.Warn("Cloud backup failed", "folder", id, "error", err)
		}
		s.set(id, st)
	}
	for {
		for id, f := range s.cfg.Folders() {
			if !f.CloudBackup.Enabled || f.Paused {
				continue
			}
			interval := f.CloudBackup.IntervalS
			if interval <= 0 {
				interval = 3600
			}
			interval = max(interval, 60)
			st := s.Status(id)
			if st.Finished.IsZero() || time.Since(st.Finished) >= time.Duration(interval)*time.Second {
				run(id, f)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case id := <-s.requested:
			if f, ok := s.cfg.Folder(id); ok && f.CloudBackup.Enabled && !f.Paused {
				run(id, f)
			}
		}
	}
}

func folderURL(id string) string { return "https://drive.google.com/drive/folders/" + id }

type Result struct {
	Files, Bytes int64
	// Failed counts files and directories left for the next run.
	Failed int64
	// RootID is the Drive folder holding Current and Versions.
	RootID string
}

// ValidateSource rejects folders the backup cannot read safely.
func ValidateSource(f config.FolderConfiguration) (string, error) {
	expanded, err := stfs.ExpandTilde(f.Path)
	if err != nil {
		return "", err
	}
	src, err := filepath.Abs(expanded)
	if err != nil {
		return "", err
	}
	if f.FilesystemType != config.FilesystemTypeBasic || f.Type == config.FolderTypeReceiveEncrypted {
		return "", errors.New("backup requires a basic, unencrypted local folder")
	}
	if err := noLinks(src); err != nil {
		return "", err
	}
	marker := f.MarkerName
	if marker == "" {
		marker = config.DefaultMarkerName
	}
	if !filepath.IsLocal(marker) {
		return "", errors.New("invalid source marker")
	}
	if err := noLinks(filepath.Join(src, marker)); err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(src, marker)); err != nil {
		return "", fmt.Errorf("source marker missing: %w", err)
	}
	return src, nil
}

// noLinks checks all existing ancestors, including Windows junctions. Rooted
// operations below additionally prevent a replaced link from escaping its root.
func noLinks(path string) error {
	for {
		fi, err := os.Lstat(path)
		if err == nil && isLink(fi) {
			return fmt.Errorf("redirected path rejected: %s", path)
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return nil
		}
		path = parent
	}
}

// ownerValue identifies the backup root of one folder on one device. Drive
// limits app property keys and values, so the identity is hashed.
func ownerValue(device, folder string) string {
	sum := sha256.Sum256([]byte(device + "\x00" + folder))
	return hex.EncodeToString(sum[:16])
}

const ownerKey = "syncthingBackup"

// remoteTree indexes the Current subtree by slash-separated relative path.
type remoteTree struct {
	d       *drive
	dirs    map[string]string
	entries map[string]driveFile
}

// root finds the backup folder created earlier for this folder and device,
// or creates it. Ownership is recorded in app properties, so a user rename or
// move in Drive is harmless and unrelated folders are never adopted.
func root(ctx context.Context, d *drive, f config.FolderConfiguration, device string) (string, error) {
	owner := ownerValue(device, f.ID)
	found, err := d.list(ctx, fmt.Sprintf("mimeType='%s' and trashed=false and appProperties has { key='%s' and value='%s' }", folderMime, ownerKey, owner))
	if err != nil {
		return "", err
	}
	if len(found) > 0 {
		// A retried create may have produced duplicates; choose deterministically.
		sort.Slice(found, func(i, j int) bool { return found[i].ID < found[j].ID })
		return found[0].ID, nil
	}
	name := f.CloudBackup.DriveFolderName
	if name == "" {
		label := f.Label
		if label == "" {
			label = f.ID
		}
		name = "Syncthing backup - " + label
	}
	created, err := d.createFolder(ctx, name, "", map[string]string{ownerKey: owner})
	return created.ID, err
}

func (d *drive) children(ctx context.Context, parents []string) ([]driveFile, error) {
	var all []driveFile
	for len(parents) > 0 {
		n := min(len(parents), 40)
		clauses := make([]string, n)
		for i, id := range parents[:n] {
			clauses[i] = fmt.Sprintf("'%s' in parents", id)
		}
		files, err := d.list(ctx, "("+strings.Join(clauses, " or ")+") and trashed=false")
		if err != nil {
			return nil, err
		}
		all = append(all, files...)
		parents = parents[n:]
	}
	return all, nil
}

// ensureChild returns the named subfolder of parent, creating it when absent.
func ensureChild(ctx context.Context, d *drive, parent, name string) (string, error) {
	files, err := d.children(ctx, []string{parent})
	if err != nil {
		return "", err
	}
	for _, f := range files {
		if f.Name == name && f.isFolder() {
			return f.ID, nil
		}
	}
	created, err := d.createFolder(ctx, name, parent, nil)
	return created.ID, err
}

func loadTree(ctx context.Context, d *drive, current string) (*remoteTree, error) {
	t := &remoteTree{d: d, dirs: map[string]string{"": current}, entries: map[string]driveFile{}}
	level := map[string]string{current: ""}
	for len(level) > 0 {
		ids := make([]string, 0, len(level))
		for id := range level {
			ids = append(ids, id)
		}
		files, err := d.children(ctx, ids)
		if err != nil {
			return nil, err
		}
		next := map[string]string{}
		for _, f := range files {
			for _, p := range f.Parents {
				dir, ok := level[p]
				if !ok {
					continue
				}
				rel := path.Join(dir, f.Name)
				// Duplicate names can only arise outside this program; prefer
				// the most recently modified entry deterministically.
				if old, ok := t.entries[rel]; ok && (old.isFolder() && !f.isFolder() || old.modified().After(f.modified()) || old.modified().Equal(f.modified()) && old.ID < f.ID) {
					continue
				}
				t.entries[rel] = f
				if f.isFolder() {
					t.dirs[rel] = f.ID
					next[f.ID] = rel
				}
			}
		}
		level = next
	}
	return t, nil
}

func (t *remoteTree) mkdir(ctx context.Context, rel string) (string, error) {
	if id, ok := t.dirs[rel]; ok {
		return id, nil
	}
	if e, ok := t.entries[rel]; ok && !e.isFolder() {
		return "", fmt.Errorf("backup type conflict: %s is a file in Google Drive", rel)
	}
	parent, err := t.mkdir(ctx, path.Dir(strings.TrimSuffix("/"+rel, "/"))[1:])
	if err != nil {
		return "", err
	}
	created, err := t.d.createFolder(ctx, path.Base(rel), parent, nil)
	if err != nil {
		return "", err
	}
	t.dirs[rel] = created.ID
	t.entries[rel] = created
	return created.ID, nil
}

func Backup(ctx context.Context, d *drive, f config.FolderConfiguration, device string, progress func(Result)) (result Result, err error) {
	if err := waitForPrimary(ctx); err != nil {
		return result, err
	}
	src, err := ValidateSource(f)
	if err != nil {
		return result, err
	}
	about, err := d.about(ctx)
	if err != nil {
		return result, err
	}
	available := about.available()
	if result.RootID, err = root(ctx, d, f, device); err != nil {
		return result, err
	}
	if progress != nil {
		progress(result)
	}
	currentID, err := ensureChild(ctx, d, result.RootID, "Current")
	if err != nil {
		return result, err
	}
	versionsID, err := ensureChild(ctx, d, result.RootID, "Versions")
	if err != nil {
		return result, err
	}
	tree, err := loadTree(ctx, d, currentID)
	if err != nil {
		return result, err
	}
	// Versions of this run are created on first use.
	versions := &remoteTree{d: d, dirs: map[string]string{}, entries: map[string]driveFile{}}
	runID := time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + randomID()
	versionDir := func(ctx context.Context, rel string) (string, error) {
		if _, ok := versions.dirs[""]; !ok {
			id, err := d.createFolder(ctx, runID, versionsID, nil)
			if err != nil {
				return "", err
			}
			versions.dirs[""] = id.ID
		}
		return versions.mkdir(ctx, rel)
	}

	source, err := os.OpenRoot(src)
	if err != nil {
		return result, err
	}
	defer source.Close()
	matcher := ignore.New(f.Filesystem())
	// The backup deliberately includes dependencies and files ignored by sync.
	// Only Playwright traces and Syncthing internal state are excluded.
	if err = matcher.Parse(strings.NewReader("/.playwright-cli/traces\n**/.playwright-cli/traces\n"), ".backup-ignore"); err != nil {
		return result, err
	}
	marker := f.MarkerName
	if marker == "" {
		marker = config.DefaultMarkerName
	}
	var fileErrors []error
	recordError := func(err error) {
		result.Failed++
		if len(fileErrors) < 10 {
			fileErrors = append(fileErrors, err)
		}
	}
	pace := &pacer{}
	err = fs.WalkDir(source.FS(), ".", func(name string, de fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			recordError(walkErr)
			return nil
		}
		if err := waitForPrimary(ctx); err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		if name == marker || name == ".stversions" || strings.HasPrefix(de.Name(), ".syncthing.") && strings.HasSuffix(de.Name(), ".tmp") {
			if de.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		match := matcher.Match(name)
		if match.IsIgnored() {
			if de.IsDir() && match.CanSkipDir() {
				return fs.SkipDir
			}
			if !de.IsDir() {
				return nil
			}
		}
		info, err := de.Info()
		if err != nil {
			return err
		}
		if isLink(info) {
			recordError(fmt.Errorf("backup does not follow links or junctions: %s", name))
			if de.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if de.IsDir() {
			if _, err := tree.mkdir(ctx, name); err != nil {
				// A type conflict keeps the remote file; skip the subtree.
				recordError(err)
				return fs.SkipDir
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported backup file: %s", name)
		}
		if available >= 0 && info.Size() > available {
			return errors.New("google drive storage quota exhausted")
		}
		changed, err := uploadWhenIdle(ctx, source, tree, name, versionDir, pace)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			recordError(fmt.Errorf("%s: %w", name, err))
			if progress != nil {
				progress(result)
			}
			return nil
		}
		if changed {
			result.Files++
			result.Bytes += info.Size()
			if available >= 0 {
				available -= info.Size()
			}
		}
		if progress != nil {
			progress(result)
		}
		return nil
	})
	if result.Failed > int64(len(fileErrors)) {
		fileErrors = append(fileErrors, fmt.Errorf("%d more errors", result.Failed-int64(len(fileErrors))))
	}
	return result, errors.Join(append(fileErrors, err)...)
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func md5sum(ctx context.Context, f io.Reader) (string, error) {
	h := md5.New()
	_, err := io.Copy(h, contextReader{ctx, f})
	return hex.EncodeToString(h.Sum(nil)), err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(b []byte) (int, error) {
	if err := checkPrimary(r.ctx); err != nil {
		return 0, err
	}
	return r.r.Read(b)
}

// uploadFile makes the Drive copy of name match the source. versioned reports
// whether the previous Drive content was already preserved by an earlier
// attempt in this run, so a retry after yielding does not copy it twice.
// While the run is backing off it returns errPace before contacting Drive,
// unless the caller has already waited; the caller then waits without
// holding the source open.
func uploadFile(ctx context.Context, src *os.Root, tree *remoteTree, name string, initial os.FileInfo, versionDir func(context.Context, string) (string, error), versioned *bool, pace *pacer, waited bool) (bool, error) {
	if err := checkPrimary(ctx); err != nil {
		return false, err
	}
	local := filepath.FromSlash(name)
	in, err := src.Open(local)
	if err != nil {
		return false, err
	}
	defer in.Close()
	before, err := in.Stat()
	if err != nil {
		return false, err
	}
	if !os.SameFile(initial, before) || before.Size() != initial.Size() || !before.ModTime().Equal(initial.ModTime()) {
		return false, errors.New("source changed before backup")
	}
	remote, exists := tree.entries[name]
	if exists && remote.isFolder() {
		return false, errors.New("backup type conflict: a folder exists in Google Drive")
	}
	if exists && remote.MD5 == "" {
		return false, errors.New("backup conflict: Google Drive holds a converted document")
	}
	// Compare contents too: Drive edits and equal-size, equal-time changes must
	// not silently become the backup. This is intentionally conservative.
	if exists && remote.size() == before.Size() && remote.modified().Equal(before.ModTime().Truncate(time.Millisecond)) {
		sum, err := md5sum(ctx, in)
		if err != nil {
			return false, err
		}
		if sum == remote.MD5 {
			return false, nil
		}
		if _, err = in.Seek(0, io.SeekStart); err != nil {
			return false, err
		}
	}
	if pace.delay > 0 && !waited {
		return false, errPace
	}
	dir := path.Dir(name)
	if dir == "." {
		dir = ""
	}
	parent, err := tree.mkdir(ctx, dir)
	if err != nil {
		return false, err
	}
	if exists && !*versioned {
		vdir, err := versionDir(ctx, dir)
		if err != nil {
			return false, err
		}
		// Preserve the previous content server side before replacing it. A
		// failed replacement leaves Current intact; a retry may version again.
		if err = tree.d.copy(ctx, remote.ID, remote.Name, vdir); err != nil {
			return false, err
		}
		*versioned = true
	}
	h := md5.New()
	meta := driveFile{Name: path.Base(name), Parents: []string{parent}, ModifiedTime: driveTime(before.ModTime())}
	uploaded, err := tree.d.upload(ctx, remote.ID, meta, io.TeeReader(contextReader{ctx, in}, h), before.Size())
	if err != nil {
		return false, err
	}
	tree.entries[name] = uploaded
	after, err := in.Stat()
	if err != nil {
		return false, err
	}
	latest, err := src.Lstat(local)
	if err != nil {
		return false, err
	}
	if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) || !os.SameFile(before, latest) || !latest.ModTime().Equal(before.ModTime()) {
		return false, errors.New("source changed during backup")
	}
	if !strings.EqualFold(uploaded.MD5, hex.EncodeToString(h.Sum(nil))) || uploaded.size() != before.Size() {
		return false, errors.New("google drive checksum mismatch after upload")
	}
	return true, nil
}

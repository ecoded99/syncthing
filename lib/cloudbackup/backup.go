// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

// Package cloudbackup implements a secondary one-way local backup. A separate
// cloud client uploads its destination; this package never claims cloud success.
package cloudbackup

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/syncthing/syncthing/lib/config"
	stfs "github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore"
)

type Status struct {
	State         string    `json:"state"`
	Started       time.Time `json:"started,omitempty"`
	Finished      time.Time `json:"finished,omitempty"`
	Files         int64     `json:"files"`
	Bytes         int64     `json:"bytes"`
	Error         string    `json:"error,omitempty"`
	CloudVerified bool      `json:"cloudVerified"`
}

type Service struct {
	cfg          config.Wrapper
	mu           sync.RWMutex
	statuses     map[string]Status
	requested    chan string
	activeCancel context.CancelFunc
}

func New(cfg config.Wrapper) *Service {
	return &Service{cfg: cfg, statuses: make(map[string]Status), requested: make(chan string, 16)}
}
func (*Service) String() string { return "cloud backup" }
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
func (s *Service) CommitConfiguration(_, _ config.Configuration) bool {
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
		st := Status{State: "copying", Started: time.Now().UTC()}
		s.set(id, st)
		result, err := Backup(runCtx, f, s.cfg.Folders(), func(files, bytes int64) { st.Files = files; st.Bytes = bytes; s.set(id, st) })
		st.Files = result.Files
		st.Bytes = result.Bytes
		st.Finished = time.Now().UTC()
		st.State = "local-copy-complete"
		if err != nil {
			st.State = "error"
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

type Result struct{ Files, Bytes int64 }
type owner struct {
	Folder string `json:"folder"`
	Source string `json:"source"`
}

func normalized(p string) string {
	p = filepath.Clean(p)
	if os.PathSeparator == '\\' {
		p = strings.ToLower(p)
	}
	return p
}
func overlaps(a, b string) bool {
	a = normalized(a)
	b = normalized(b)
	return a == b || strings.HasPrefix(a, strings.TrimRight(b, string(os.PathSeparator))+string(os.PathSeparator)) || strings.HasPrefix(b, strings.TrimRight(a, string(os.PathSeparator))+string(os.PathSeparator))
}

// ValidatePaths rejects aliases and overlap before creating or modifying anything.
func ValidatePaths(f config.FolderConfiguration, folders map[string]config.FolderConfiguration) (string, string, error) {
	expanded, err := stfs.ExpandTilde(f.Path)
	if err != nil {
		return "", "", err
	}
	src, err := filepath.Abs(expanded)
	if err != nil {
		return "", "", err
	}
	if f.CloudBackup.Path == "" || !filepath.IsAbs(f.CloudBackup.Path) {
		return "", "", errors.New("backup path must be absolute")
	}
	dst, err := filepath.Abs(f.CloudBackup.Path)
	if err != nil {
		return "", "", err
	}
	if overlaps(src, dst) {
		return "", "", errors.New("source and backup paths overlap")
	}
	if f.FilesystemType != config.FilesystemTypeBasic || f.Type == config.FolderTypeReceiveEncrypted {
		return "", "", errors.New("backup requires a basic, unencrypted local folder")
	}
	for id, other := range folders {
		otherPath, err := stfs.ExpandTilde(other.Path)
		if err != nil {
			return "", "", err
		}
		path, err := filepath.Abs(otherPath)
		if err != nil {
			return "", "", err
		}
		if overlaps(dst, path) {
			return "", "", fmt.Errorf("backup overlaps synchronized folder %q", id)
		}
		if id != f.ID && other.CloudBackup.Enabled && overlaps(dst, other.CloudBackup.Path) {
			return "", "", errors.New("backup destinations overlap")
		}
	}
	if err := noLinks(src); err != nil {
		return "", "", err
	}
	if err := noLinks(dst); err != nil {
		return "", "", err
	}
	marker := f.MarkerName
	if marker == "" {
		marker = config.DefaultMarkerName
	}
	if !filepath.IsLocal(marker) {
		return "", "", errors.New("invalid source marker")
	}
	if err := noLinks(filepath.Join(src, marker)); err != nil {
		return "", "", err
	}
	if _, err := os.Stat(filepath.Join(src, marker)); err != nil {
		return "", "", fmt.Errorf("source marker missing: %w", err)
	}
	return src, dst, nil
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

func Backup(ctx context.Context, f config.FolderConfiguration, folders map[string]config.FolderConfiguration, progress func(int64, int64)) (result Result, err error) {
	src, dst, err := ValidatePaths(f, folders)
	if err != nil {
		return result, err
	}
	if err = os.MkdirAll(dst, 0700); err != nil {
		return result, err
	}
	if err = noLinks(dst); err != nil {
		return result, err
	}
	usage, err := disk.Usage(dst)
	if err != nil {
		return result, err
	}
	minFree := f.CloudBackup.MinFreeBytes
	if minFree == 0 {
		minFree = 15 << 30
	}
	if usage.Free < minFree {
		return result, errors.New("backup destination free space below reserve")
	}
	root, err := os.OpenRoot(dst)
	if err != nil {
		return result, err
	}
	defer root.Close()
	if err = noLinks(filepath.Join(dst, ".native-backup.lock")); err != nil {
		return result, err
	}
	lock := flock.New(filepath.Join(dst, ".native-backup.lock"))
	locked, err := lock.TryLock()
	if err != nil {
		return result, err
	}
	if !locked {
		return result, errors.New("backup already running")
	}
	defer lock.Unlock()
	if err = claim(root, f, src); err != nil {
		return result, err
	}
	for _, dir := range []string{"Current", "Versions", ".staging"} {
		if err = noLinks(filepath.Join(dst, dir)); err != nil {
			return result, err
		}
		if err = root.MkdirAll(dir, 0700); err != nil {
			return result, err
		}
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
	runID := time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + randomID()
	marker := f.MarkerName
	if marker == "" {
		marker = config.DefaultMarkerName
	}
	var fileErrors []error
	recordError := func(err error) {
		if len(fileErrors) < 10 {
			fileErrors = append(fileErrors, err)
		}
	}
	err = fs.WalkDir(source.FS(), ".", func(name string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			recordError(walkErr)
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		if name == marker || name == ".stversions" || strings.HasPrefix(d.Name(), ".syncthing.") && strings.HasSuffix(d.Name(), ".tmp") {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		match := matcher.Match(name)
		if match.IsIgnored() {
			if d.IsDir() && match.CanSkipDir() {
				return fs.SkipDir
			}
			if !d.IsDir() {
				return nil
			}
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if isLink(info) {
			recordError(fmt.Errorf("backup does not follow links or junctions: %s", name))
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		current := filepath.Join("Current", filepath.FromSlash(name))
		if err = noLinks(filepath.Join(dst, current)); err != nil {
			return err
		}
		if d.IsDir() {
			return root.MkdirAll(current, 0700)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported backup file: %s", name)
		}
		if err = root.MkdirAll(filepath.Dir(current), 0700); err != nil {
			return err
		}
		if usage, err := disk.Usage(dst); err != nil {
			return err
		} else if usage.Free < minFree+uint64(info.Size()) {
			return errors.New("backup destination free space below reserve")
		}
		changed, err := copyFile(ctx, source, root, filepath.FromSlash(name), current, filepath.Join("Versions", runID, filepath.FromSlash(name)), info)
		if err != nil {
			recordError(fmt.Errorf("%s: %w", name, err))
			return nil
		}
		if changed {
			result.Files++
			result.Bytes += info.Size()
		}
		if progress != nil {
			progress(result.Files, result.Bytes)
		}
		return nil
	})
	return result, errors.Join(append(fileErrors, err)...)
}

func claim(root *os.Root, f config.FolderConfiguration, src string) error {
	const manifest = ".native-backup-owner.json"
	if fi, err := root.Lstat(manifest); err == nil && isLink(fi) {
		return errors.New("redirected owner manifest")
	}
	data, err := root.ReadFile(manifest)
	if err == nil {
		var o owner
		if json.Unmarshal(data, &o) != nil || o.Folder != f.ID || normalized(o.Source) != normalized(src) {
			return errors.New("backup ownership mismatch")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	entries, err := root.Open(".")
	if err != nil {
		return err
	}
	names, err := entries.Readdirnames(-1)
	entries.Close()
	if err != nil {
		return err
	}
	nonempty := false
	for _, name := range names {
		if name != ".native-backup.lock" {
			nonempty = true
		}
	}
	if nonempty {
		if f.CloudBackup.OwnerID == "" {
			return errors.New("refusing to adopt nonempty backup without explicit owner ID")
		}
		if fi, err := root.Lstat(".backup-owner.json"); err != nil || isLink(fi) {
			return errors.New("invalid legacy owner manifest")
		}
		data, err := root.ReadFile(".backup-owner.json")
		if err != nil {
			return err
		}
		var legacy struct {
			ID     string `json:"backup_id"`
			Source string `json:"source"`
		}
		if json.Unmarshal(data, &legacy) != nil || legacy.ID != f.CloudBackup.OwnerID || normalized(legacy.Source) != normalized(src) {
			return errors.New("legacy backup ownership mismatch")
		}
	}
	data, _ = json.Marshal(owner{f.ID, src})
	file, err := root.OpenFile(manifest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(data)
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func digest(ctx context.Context, f *os.File) ([32]byte, error) {
	h := sha256.New()
	_, err := io.Copy(h, contextReader{ctx, f})
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(b []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(b)
}

func copyFile(ctx context.Context, src, dst *os.Root, name, current, version string, initial os.FileInfo) (bool, error) {
	in, err := src.Open(name)
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
	// Compare contents too: destination edits and equal-size, equal-time changes
	// must not silently become the backup. This is intentionally conservative.
	old, oldErr := dst.Lstat(current)
	if oldErr != nil && !os.IsNotExist(oldErr) {
		return false, oldErr
	}
	if oldErr == nil && (!old.Mode().IsRegular() || isLink(old)) {
		return false, errors.New("backup file type conflict")
	}
	if oldErr == nil && old.Size() == before.Size() && old.ModTime().Equal(before.ModTime()) {
		sourceHash, err := digest(ctx, in)
		if err != nil {
			return false, err
		}
		backup, err := dst.Open(current)
		if err != nil {
			return false, err
		}
		backupHash, err := digest(ctx, backup)
		backup.Close()
		if err != nil {
			return false, err
		}
		after, err := in.Stat()
		if err != nil {
			return false, err
		}
		if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
			return false, errors.New("source changed during verification")
		}
		if sourceHash == backupHash {
			return false, nil
		}
		if _, err = in.Seek(0, io.SeekStart); err != nil {
			return false, err
		}
	}
	stage := filepath.Join(".staging", randomID())
	out, err := dst.OpenFile(stage, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return false, err
	}
	defer dst.Remove(stage)
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), contextReader{ctx, in})
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	after, err := in.Stat()
	if err != nil {
		return false, err
	}
	latest, err := src.Lstat(name)
	if err != nil {
		return false, err
	}
	if n != before.Size() || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) || !os.SameFile(before, latest) || !latest.ModTime().Equal(before.ModTime()) {
		return false, errors.New("source changed during backup")
	}
	verify, err := dst.Open(stage)
	if err != nil {
		return false, err
	}
	verified, err := digest(ctx, verify)
	verify.Close()
	if err != nil {
		return false, err
	}
	if !strings.EqualFold(hex.EncodeToString(verified[:]), hex.EncodeToString(h.Sum(nil))) {
		return false, errors.New("backup hash mismatch")
	}
	if err = dst.Chtimes(stage, before.ModTime(), before.ModTime()); err != nil {
		return false, err
	}
	if err = dst.Chmod(stage, before.Mode().Perm()); err != nil {
		return false, err
	}
	if oldErr == nil {
		if err = dst.MkdirAll(filepath.Dir(version), 0700); err != nil {
			return false, err
		}
		// Copy the previous version before replacing Current. A failed replacement
		// leaves the old Current intact, and a durable version can be safely retried.
		if err = copyVersion(ctx, dst, current, version, old); err != nil {
			return false, err
		}
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if oldErr == nil && old.Mode().Perm()&0200 == 0 {
		if err = dst.Chmod(current, old.Mode().Perm()|0200); err != nil {
			return false, err
		}
	}
	if err = dst.Rename(stage, current); err != nil {
		if oldErr == nil {
			_ = dst.Chmod(current, old.Mode().Perm())
		}
		return false, err
	}
	return true, nil
}
func copyVersion(ctx context.Context, root *os.Root, from, to string, info os.FileInfo) error {
	in, err := root.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	stage := filepath.Join(".staging", randomID())
	defer root.Remove(stage)
	out, err := root.OpenFile(stage, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, err := io.Copy(out, contextReader{ctx, in})
	if err == nil {
		err = out.Sync()
	}
	closeErr := out.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	after, err := in.Stat()
	if err != nil {
		return err
	}
	if n != info.Size() || after.Size() != info.Size() || !after.ModTime().Equal(info.ModTime()) {
		return errors.New("previous backup changed during versioning")
	}
	if err = root.Chtimes(stage, info.ModTime(), info.ModTime()); err != nil {
		return err
	}
	if err = root.Chmod(stage, info.Mode().Perm()); err != nil {
		return err
	}
	return root.Rename(stage, to)
}

// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

// Package archivebatch moves bounded sets of small files through an untrusted
// ZIP. It only prepares private staged files. The caller must retain its normal
// version, path, conflict, permission, publication and index-update checks.
package archivebatch

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	MaxFiles        = 1024
	MaxPayloadBytes = 16 << 20
	MaxFileBytes    = 64 << 10
	MaxNameBytes    = 4096
	// Includes two copies of each name, bounded extra fields and ZIP headers.
	MaxArchiveBytes = MaxPayloadBytes + MaxFiles*(2*MaxNameBytes+512) + 22
	workers         = 8
)

// Member is metadata supplied by the authenticated caller, never a ZIP manifest.
// Name is an NFC, slash-separated relative path. Modified may be zero for Pack;
// Unpack applies the receiver's value, subject to the destination OS precision.
type Member struct {
	Name     string
	Size     int64
	SHA256   [sha256.Size]byte
	Modified time.Time
}

type OpenFunc func(context.Context, Member) (io.ReadCloser, error)

// Pack verifies and closes one source at a time. It stores data without ZIP
// compression, using at most one small-file buffer. The caller must discard all
// output if Pack fails; previously emitted bytes are not rolled back.
func Pack(ctx context.Context, w io.Writer, members []Member, open OpenFunc) error {
	if _, err := validateMembers(members); err != nil {
		return err
	}
	if open == nil || w == nil {
		return errors.New("archivebatch: missing source or output")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	zw := zip.NewWriter(&boundedWriter{ctx: ctx, w: w, remaining: MaxArchiveBytes})
	for _, member := range members {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := open(ctx, member)
		if err != nil {
			if r != nil {
				err = errors.Join(err, r.Close())
			}
			return fmt.Errorf("archivebatch: open %q: %w", member.Name, err)
		}
		if r == nil {
			return fmt.Errorf("archivebatch: nil source for %q", member.Name)
		}
		data, readErr := readMember(ctx, r, member.Size)
		closeErr := r.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return fmt.Errorf("archivebatch: read %q: %w", member.Name, err)
		}
		if int64(len(data)) != member.Size || sha256.Sum256(data) != member.SHA256 {
			return fmt.Errorf("archivebatch: source size or hash changed for %q", member.Name)
		}
		header := &zip.FileHeader{Name: member.Name, Method: zip.Store}
		header.SetMode(0o600)
		entry, err := zw.CreateHeader(header)
		if err != nil {
			return fmt.Errorf("archivebatch: ZIP header: %w", err)
		}
		if _, err := entry.Write(data); err != nil {
			return fmt.Errorf("archivebatch: ZIP data: %w", err)
		}
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("archivebatch: finish ZIP: %w", err)
	}
	return ctx.Err()
}

// StagedFile has a generated internal path, unrelated to the ZIP entry name.
// ObservedModified records actual filesystem precision; exact virtual mtimes
// remain the responsibility of the normal Syncthing publication path.
type StagedFile struct {
	Member           Member
	Path             string
	ObservedModified time.Time
}

type ownedFile struct {
	info os.FileInfo
}

// Stage owns a fresh private directory and its generated regular files. It may
// be used only while the caller prevents other writers from using that directory.
// Known files may be moved out by the caller before Close. Close refuses unknown
// entries, replaced identities or symlinks, and never recursively deletes paths.
type Stage struct {
	mut    sync.Mutex
	path   string
	info   os.FileInfo
	root   *os.Root
	owned  map[string]ownedFile
	files  []StagedFile
	closed bool
}

func (s *Stage) Files() []StagedFile {
	s.mut.Lock()
	defer s.mut.Unlock()
	return append([]StagedFile(nil), s.files...)
}

// Close is idempotent after success. On a failed safety check it retains the
// directory for the caller to inspect, rather than deleting foreign content.
func (s *Stage) Close() (err error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	defer func() {
		if err != nil && s.root != nil {
			err = errors.Join(err, s.root.Close())
			s.root = nil
		}
	}()
	if s.closed {
		return nil
	}
	current, err := os.Lstat(s.path)
	if err != nil {
		return fmt.Errorf("archivebatch: inspect owned stage: %w", err)
	}
	if !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || !os.SameFile(current, s.info) {
		return errors.New("archivebatch: stage identity changed")
	}
	if s.root == nil {
		s.root, err = os.OpenRoot(s.path)
		if err != nil {
			return err
		}
	}
	dir, err := s.root.Open(".")
	if err != nil {
		return err
	}
	entries, readErr := dir.ReadDir(MaxFiles + 1)
	closeErr := dir.Close()
	if err := errors.Join(ignoreEOF(readErr), closeErr); err != nil {
		return err
	}
	if len(entries) > len(s.owned) {
		return errors.New("archivebatch: unexpected stage entries")
	}
	for _, entry := range entries {
		owned, ok := s.owned[entry.Name()]
		if !ok {
			return errors.New("archivebatch: unknown stage entry")
		}
		info, err := s.root.Lstat(entry.Name())
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || !os.SameFile(info, owned.info) {
			return errors.New("archivebatch: staged file identity changed")
		}
	}
	for _, entry := range entries {
		if err := s.root.Remove(entry.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := s.root.Close(); err != nil {
		return err
	}
	s.root = nil
	// All operations above were anchored to the opened owned directory. Verify
	// its path again before removing the now-empty directory itself.
	current, err = os.Lstat(s.path)
	if err != nil || !os.SameFile(current, s.info) {
		return errors.New("archivebatch: stage identity changed during cleanup")
	}
	if err := os.Remove(s.path); err != nil {
		return err
	}
	s.closed = true
	return nil
}

// Unpack checks the complete bounded ZIP inventory before creating freshStage.
// It then verifies content against expected and writes only generated filenames,
// with at most eight workers. Every file gets the trusted timestamp before Sync
// and Close. No target file is published and no local index is updated.
// On error all workers join and owned partials are cleaned; a cleanup failure is
// returned too. The caller already knows freshStage for private recovery.
func Unpack(ctx context.Context, r io.ReaderAt, archiveSize int64, expected []Member, freshStage string) (*Stage, error) {
	return unpack(ctx, r, archiveSize, expected, freshStage, defaultStageOps())
}

type stageWriter interface {
	io.Writer
	Sync() error
	Close() error
	Stat() (os.FileInfo, error)
	Name() string
}

type stageOps struct {
	createTemp func(string, string) (stageWriter, error)
	chtimes    func(*os.Root, string, time.Time) error
}

func defaultStageOps() stageOps {
	return stageOps{
		createTemp: func(dir, pattern string) (stageWriter, error) { return os.CreateTemp(dir, pattern) },
		chtimes: func(root *os.Root, name string, modified time.Time) error {
			return root.Chtimes(name, modified, modified)
		},
	}
}

func unpack(ctx context.Context, r io.ReaderAt, archiveSize int64, expected []Member, freshStage string, ops stageOps) (*Stage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	byName, err := validateMembers(expected)
	if err != nil {
		return nil, err
	}
	if r == nil || freshStage == "" {
		return nil, errors.New("archivebatch: missing ZIP or fresh stage path")
	}
	reader := contextReaderAt{ctx, r}
	if err := validateZIP(reader, archiveSize, byName); err != nil {
		return nil, err
	}
	zr, err := zip.NewReader(reader, archiveSize)
	if err != nil {
		return nil, fmt.Errorf("archivebatch: open ZIP: %w", err)
	}
	entries := make([]*zip.File, len(expected))
	for _, file := range zr.File {
		idx, ok := byName[file.Name]
		if !ok || entries[idx] != nil || !file.Mode().IsRegular() || file.UncompressedSize64 != uint64(expected[idx].Size) {
			return nil, errors.New("archivebatch: ZIP inventory differs from expected regular files")
		}
		entries[idx] = file
	}
	if len(zr.File) != len(expected) {
		return nil, errors.New("archivebatch: incomplete ZIP inventory")
	}
	stagePath, err := filepath.Abs(freshStage)
	if err != nil {
		return nil, err
	}
	if err := os.Mkdir(stagePath, 0o700); err != nil {
		return nil, fmt.Errorf("archivebatch: create fresh stage: %w", err)
	}
	info, err := os.Lstat(stagePath)
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(stagePath)
	if err != nil {
		return nil, errors.Join(err, os.Remove(stagePath))
	}
	stage := &Stage{path: stagePath, info: info, root: root, owned: make(map[string]ownedFile, len(expected)), files: make([]StagedFile, len(expected))}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var wg sync.WaitGroup
	var errOnce sync.Once
	var workErr error
	for range min(workers, len(expected)) {
		wg.Go(func() {
			buffer := make([]byte, 32<<10)
			for idx := range jobs {
				if err := workCtx.Err(); err != nil {
					continue
				}
				if err := stage.extract(workCtx, entries[idx], expected[idx], idx, buffer, ops); err != nil {
					errOnce.Do(func() { workErr = err; cancel() })
				}
			}
		})
	}
feed:
	for idx := range expected {
		select {
		case jobs <- idx:
		case <-workCtx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	if workErr == nil {
		workErr = ctx.Err()
	}
	if workErr != nil {
		return nil, errors.Join(workErr, stage.Close())
	}
	return stage, nil
}

func (s *Stage) extract(ctx context.Context, entry *zip.File, member Member, idx int, buffer []byte, ops stageOps) (err error) {
	r, err := entry.Open()
	if err != nil {
		return fmt.Errorf("archivebatch: open entry %q: %w", member.Name, err)
	}
	defer func() { err = errors.Join(err, r.Close()) }()
	file, err := ops.createTemp(s.path, fmt.Sprintf("%04d-", idx))
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, file.Close())
		}
	}()
	name := filepath.Base(file.Name())
	if filepath.Dir(file.Name()) != s.path {
		return errors.New("archivebatch: created file escaped owned stage")
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	s.mut.Lock()
	s.owned[name] = ownedFile{info}
	s.mut.Unlock()
	hash := sha256.New()
	n, err := io.CopyBuffer(io.MultiWriter(file, hash), io.LimitReader(contextReader{ctx, r}, member.Size+1), buffer)
	if err != nil {
		return fmt.Errorf("archivebatch: extract %q: %w", member.Name, err)
	}
	if n != member.Size || !equalHash(hash.Sum(nil), member.SHA256) {
		return fmt.Errorf("archivebatch: entry size or SHA256 mismatch for %q", member.Name)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ops.chtimes(s.root, name, member.Modified); err != nil {
		return fmt.Errorf("archivebatch: set trusted mtime: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("archivebatch: sync staged file: %w", err)
	}
	// Close before observing final on-disk metadata. Do not claim timestamps have
	// finer precision than the destination filesystem supports.
	closeErr := file.Close()
	closed = true
	if err := closeErr; err != nil {
		return fmt.Errorf("archivebatch: close staged file: %w", err)
	}
	observed, err := s.root.Stat(name)
	if err != nil {
		return err
	}
	s.files[idx] = StagedFile{Member: member, Path: filepath.Join(s.path, name), ObservedModified: observed.ModTime()}
	return ctx.Err()
}

func validateMembers(members []Member) (map[string]int, error) {
	if len(members) == 0 || len(members) > MaxFiles {
		return nil, errors.New("archivebatch: invalid member count")
	}
	byName := make(map[string]int, len(members))
	folded := make(map[string]bool, len(members))
	var total int64
	for idx, member := range members {
		if err := validName(member.Name); err != nil {
			return nil, err
		}
		key := cases.Fold().String(member.Name)
		if folded[key] {
			return nil, errors.New("archivebatch: duplicate or case-alias member")
		}
		if member.Size < 0 || member.Size > MaxFileBytes || total > MaxPayloadBytes-member.Size {
			return nil, errors.New("archivebatch: expanded payload exceeds limits")
		}
		total += member.Size
		byName[member.Name], folded[key] = idx, true
	}
	for key := range folded {
		for parent := path.Dir(key); parent != "."; parent = path.Dir(parent) {
			if folded[parent] {
				return nil, errors.New("archivebatch: file/directory alias in inventory")
			}
		}
	}
	return byName, nil
}

func validName(name string) error {
	if name == "" || len(name) > MaxNameBytes || !utf8.ValidString(name) || !norm.NFC.IsNormalString(name) || path.Clean(name) != name || name == "." || strings.HasPrefix(name, "/") || strings.ContainsAny(name, "\\:") {
		return errors.New("archivebatch: invalid canonical relative member name")
	}
	for _, part := range strings.Split(name, "/") {
		if part == ".." || strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
			return errors.New("archivebatch: unsafe member component")
		}
	}
	for _, char := range name {
		if char < 32 || char == 127 {
			return errors.New("archivebatch: control character in member name")
		}
	}
	return nil
}

func equalHash(actual []byte, expected [sha256.Size]byte) bool {
	return len(actual) == len(expected) && [sha256.Size]byte(actual) == expected
}

// One extra byte detects a source or decompressor exceeding the trusted size.
// Reading through EOF also allows archive/zip to verify its CRC.
func readMember(ctx context.Context, r io.Reader, size int64) ([]byte, error) {
	buf := make([]byte, int(size)+1)
	n, err := io.ReadFull(contextReader{ctx, r}, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	if int64(n) != size {
		return nil, errors.New("archivebatch: member length differs from trusted size")
	}
	return buf[:n], nil
}

func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type contextReaderAt struct {
	ctx context.Context
	r   io.ReaderAt
}

func (r contextReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.ReadAt(p, off)
}

type boundedWriter struct {
	ctx       context.Context
	w         io.Writer
	remaining int64
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(p)) > w.remaining {
		return 0, errors.New("archivebatch: ZIP exceeds output bound")
	}
	n, err := w.w.Write(p)
	w.remaining -= int64(n)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return n, err
}

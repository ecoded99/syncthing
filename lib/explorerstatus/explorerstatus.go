// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

// Package explorerstatus shows the sync state of folders in Windows Explorer.
//
// An enabled folder is registered as a Cloud Files sync root and every file
// in it becomes a placeholder whose data always stays on disk. Explorer draws
// the Status column from each placeholder's in-sync flag: synced, or pending
// while Syncthing still needs the file, failed to pull it, or has not yet
// scanned a local change (Windows clears the flag itself when a file is
// written). Ignored files are shown as excluded. Turning the option off
// unregisters the root, which turns the placeholders back into ordinary files.
package explorerstatus

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	stfs "github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/model"
	"github.com/syncthing/syncthing/lib/protocol"
)

// State is what Explorer shows for a file.
type State int

const (
	InSync State = iota
	Pending
	Excluded
)

// Root is a folder registered with Explorer.
type Root interface {
	// Set marks name (relative to the root, with native separators as the
	// model uses them), turning it into a placeholder first if needed.
	Set(name string, st State) error
	// Close disconnects from the root. With remove it also unregisters the
	// root, which turns its placeholders back into ordinary files.
	Close(remove bool) error
}

// Opener registers (or re-registers) a folder and connects to it.
type Opener func(folderID, path, label string) (Root, error)

// Model is the part of the model the service reads.
type Model interface {
	NeedFolderFiles(folder string, page, perpage int) ([]protocol.FileInfo, []protocol.FileInfo, []protocol.FileInfo, error)
	FolderErrors(folder string) ([]model.FileError, error)
}

const (
	refreshDelay = 2 * time.Second
	needPageSize = 1000
	// maxNeeded bounds how many needed files are marked pending, so a large
	// initial sync does not mean a huge diff on every refresh.
	maxNeeded = 50000
)

type Service struct {
	cfg      config.Wrapper
	model    Model
	evLogger events.Logger
	open     Opener
	folders  map[string]*folder
}

// New returns the service. It does nothing on platforms without Explorer.
func New(cfg config.Wrapper, m Model, evLogger events.Logger) *Service {
	return newService(cfg, m, evLogger, platformOpen)
}

func newService(cfg config.Wrapper, m Model, evLogger events.Logger, open Opener) *Service {
	return &Service{cfg: cfg, model: m, evLogger: evLogger, open: open, folders: make(map[string]*folder)}
}

func (*Service) String() string { return "explorer status" }

func (s *Service) Serve(ctx context.Context) error {
	if s.open == nil {
		<-ctx.Done()
		return nil
	}
	sub := s.evLogger.Subscribe(events.LocalIndexUpdated | events.RemoteIndexUpdated | events.FolderErrors | events.StateChanged | events.ConfigSaved)
	defer sub.Unsubscribe()
	defer func() {
		for id, f := range s.folders {
			f.stop(false)
			delete(s.folders, id)
		}
	}()

	s.reconcile(ctx)
	refresh := time.NewTimer(time.Hour)
	refresh.Stop()
	dirty := make(map[string]struct{})
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-sub.C():
			if ev.Type == events.ConfigSaved {
				s.reconcile(ctx)
				continue
			}
			data, _ := ev.Data.(map[string]any)
			id, _ := data["folder"].(string)
			f := s.folders[id]
			if f == nil {
				continue
			}
			if ev.Type == events.LocalIndexUpdated {
				names, _ := data["filenames"].([]string)
				f.updated(names)
			}
			if len(dirty) == 0 {
				refresh.Reset(refreshDelay)
			}
			dirty[id] = struct{}{}
		case <-refresh.C:
			for id := range dirty {
				if f := s.folders[id]; f != nil {
					f.refresh(s.model)
				}
				delete(dirty, id)
			}
		}
	}
}

// reconcile registers newly enabled folders and unregisters folders that
// were turned off, removed or moved.
func (s *Service) reconcile(ctx context.Context) {
	want := make(map[string]config.FolderConfiguration)
	for id, fcfg := range s.cfg.Folders() {
		if fcfg.ExplorerStatus && fcfg.FilesystemType == config.FilesystemTypeBasic {
			want[id] = fcfg
		}
	}
	for id, f := range s.folders {
		if fcfg, ok := want[id]; ok && fcfg.Path == f.cfg.Path {
			f.cfg = fcfg
			continue
		}
		f.stop(true)
		delete(s.folders, id)
		slog.Info("Removed folder status from Explorer", f.cfg.LogAttr())
	}
	for id, fcfg := range want {
		if _, ok := s.folders[id]; ok {
			continue
		}
		label := fcfg.Label
		if label == "" {
			label = id
		}
		root, err := s.open(id, fcfg.Path, label)
		if err != nil {
			slog.Warn("Failed to show folder status in Explorer", fcfg.LogAttr(), slogutil.Error(err))
			continue
		}
		f := newFolder(fcfg, root)
		s.folders[id] = f
		f.start(ctx, s.model)
	}
}

type folder struct {
	cfg     config.FolderConfiguration
	root    Root
	ignores *ignore.Matcher

	mut     sync.Mutex
	pending map[string]struct{} // needed or failed files and their parent directories

	cancel context.CancelFunc
	done   chan struct{}
}

func newFolder(cfg config.FolderConfiguration, root Root) *folder {
	return &folder{
		cfg:     cfg,
		root:    root,
		ignores: ignore.New(cfg.Filesystem()),
		pending: make(map[string]struct{}),
	}
}

// start marks every file once in the background, then applies the current
// needs.
func (f *folder) start(ctx context.Context, m Model) {
	ctx, f.cancel = context.WithCancel(ctx)
	f.done = make(chan struct{})
	go func() {
		defer close(f.done)
		if err := f.sweep(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("Failed to mark folder files for Explorer", f.cfg.LogAttr(), slogutil.Error(err))
		}
		if ctx.Err() == nil {
			f.refresh(m)
		}
	}()
}

func (f *folder) stop(remove bool) {
	f.cancel()
	<-f.done
	f.mut.Lock()
	defer f.mut.Unlock()
	if err := f.root.Close(remove); err != nil {
		slog.Warn("Failed to unregister folder from Explorer", f.cfg.LogAttr(), slogutil.Error(err))
	}
}

func (f *folder) sweep(ctx context.Context) error {
	f.loadIgnores()
	return filepath.WalkDir(f.cfg.Path, func(p string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil || p == f.cfg.Path {
			return nil
		}
		name, err := filepath.Rel(f.cfg.Path, p)
		if err != nil {
			return nil
		}
		if internal(name) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		f.mut.Lock()
		f.set(name)
		f.mut.Unlock()
		if d.IsDir() && f.ignores.Match(name).IsIgnored() && f.ignores.Match(name).CanSkipDir() {
			return filepath.SkipDir
		}
		return nil
	})
}

// updated marks files the model just scanned or pulled.
func (f *folder) updated(names []string) {
	f.loadIgnores()
	f.mut.Lock()
	defer f.mut.Unlock()
	for _, name := range names {
		if !internal(name) {
			f.set(name)
		}
	}
}

// refresh marks what the model needs or failed to pull as pending, and what
// it no longer needs as synced.
func (f *folder) refresh(m Model) {
	need := make(map[string]struct{})
	add := func(name string) {
		for ; name != "." && name != string(filepath.Separator) && name != ""; name = filepath.Dir(name) {
			if _, ok := need[name]; ok {
				return
			}
			need[name] = struct{}{}
		}
	}
	for page := 1; len(need) < maxNeeded; page++ {
		progress, queued, rest, err := m.NeedFolderFiles(f.cfg.ID, page, needPageSize)
		if err != nil {
			return
		}
		for _, list := range [][]protocol.FileInfo{progress, queued, rest} {
			for _, fi := range list {
				add(fi.Name)
			}
		}
		if len(progress)+len(queued)+len(rest) < needPageSize {
			break
		}
	}
	if errs, err := m.FolderErrors(f.cfg.ID); err == nil {
		for _, fe := range errs {
			add(fe.Path)
		}
	}

	f.loadIgnores()
	f.mut.Lock()
	defer f.mut.Unlock()
	old := f.pending
	f.pending = need
	for name := range need {
		if _, ok := old[name]; !ok {
			f.set(name)
		}
	}
	for name := range old {
		if _, ok := need[name]; !ok {
			f.set(name)
		}
	}
}

// set applies the state name should have; f.mut must be held.
func (f *folder) set(name string) {
	st := InSync
	if f.ignores.Match(name).IsIgnored() {
		st = Excluded
	} else if _, ok := f.pending[name]; ok {
		st = Pending
	}
	if err := f.root.Set(name, st); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Debug("Failed to set Explorer status", slogutil.FilePath(name), slogutil.Error(err))
	}
}

func (f *folder) loadIgnores() {
	if err := f.ignores.Load(".stignore"); err != nil && !errors.Is(err, fs.ErrNotExist) {
		slog.Debug("Failed to load ignores for Explorer status", f.cfg.LogAttr(), slogutil.Error(err))
	}
}

// internal reports whether name is one of Syncthing's own files, which are
// left alone.
func internal(name string) bool {
	return stfs.IsInternal(name) || stfs.IsTemporary(name)
}

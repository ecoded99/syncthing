// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/archivebatch"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
)

const (
	archiveWindowFiles = 1024
	archiveWindowBytes = 16 << 20
	archiveMemberBytes = 64 << 10
	archiveWorkers     = 8
)

// archivePullWindow is owned by the serial needed-file planner. Only the ZIP
// materialization callbacks run concurrently. Nothing reaches a finisher until
// the complete window has been verified and every temporary writer is closed.
type archivePullWindow struct {
	folder    *sendReceiveFolder
	ctx       context.Context
	dbUpdates chan<- dbUpdateJob
	ordinary  chan<- copyBlocksState
	scans     chan<- string
	states    []copyBlocksState
	bytes     int64
	device    protocol.DeviceID
	disabled  map[protocol.DeviceID]bool
	request   func(context.Context, protocol.DeviceID, []*protocol.Request) ([]byte, error)
}

func (f *sendReceiveFolder) newArchivePullWindow(ctx context.Context, dbUpdates chan<- dbUpdateJob, ordinary chan<- copyBlocksState, scans chan<- string) *archivePullWindow {
	if !f.ExperimentalArchiveTransfers || f.Type == config.FolderTypeReceiveEncrypted || f.mtimefs.Type() != fs.FilesystemTypeBasic || f.DisableFsync {
		return nil
	}
	return &archivePullWindow{
		folder: f, ctx: ctx, dbUpdates: dbUpdates, ordinary: ordinary, scans: scans,
		disabled: make(map[protocol.DeviceID]bool), request: f.model.requestArchive,
	}
}

func archiveFileEligible(file protocol.FileInfo) bool {
	return file.Type == protocol.FileInfoTypeFile && !file.IsDeleted() && !file.IsInvalid() &&
		len(file.Encrypted) == 0 && file.Size > 0 && file.Size <= archiveMemberBytes &&
		len(file.Blocks) == 1 && file.Blocks[0].Size == int(file.Size) &&
		len(file.Blocks[0].Hash) == sha256.Size && !file.Blocks[0].IsEmpty()
}

func (w *archivePullWindow) add(state copyBlocksState) {
	// A reusable temporary block or any non-full-block state keeps the original
	// copier path, including its resume and local-block shortcuts.
	if state.reused != 0 || len(state.blocks) != 1 || !archiveFileEligible(state.file) {
		w.flush()
		w.ordinary <- state
		return
	}
	candidates := w.folder.model.blockAvailability(w.folder.FolderConfiguration, state.file, state.blocks[0])
	available := candidates[:0]
	for _, candidate := range candidates {
		if !candidate.FromTemporary && !w.disabled[candidate.ID] {
			available = append(available, candidate)
		}
	}
	selected := -1
	for i, candidate := range available {
		if len(w.states) > 0 && candidate.ID == w.device {
			selected = i
			break
		}
	}
	if selected < 0 {
		selected = activity.leastBusy(available)
	}
	if selected < 0 {
		w.flush()
		w.ordinary <- state
		return
	}
	device := available[selected].ID
	if len(w.states) > 0 && (device != w.device || len(w.states) == archiveWindowFiles || w.bytes+state.file.Size > archiveWindowBytes) {
		w.flush()
	}
	w.device = device
	w.states = append(w.states, state)
	w.bytes += state.file.Size
	if len(w.states) == archiveWindowFiles || w.bytes == archiveWindowBytes {
		w.flush()
	}
}

func (w *archivePullWindow) flush() {
	if w == nil || len(w.states) == 0 {
		return
	}
	states, device, payloadBytes := w.states, w.device, w.bytes
	w.states, w.bytes = nil, 0

	members := make([]archivebatch.Member, len(states))
	requests := make([]*protocol.Request, len(states))
	byName := make(map[string]*sharedPullerState, len(states))
	for i, state := range states {
		name := osutil.NormalizedFilename(state.file.Name)
		var hash [sha256.Size]byte
		copy(hash[:], state.blocks[0].Hash)
		members[i] = archivebatch.Member{Name: name, Size: state.file.Size, SHA256: hash, Modified: state.file.ModTime()}
		requests[i] = &protocol.Request{Folder: w.folder.folderID, Name: state.file.Name, Size: int(state.file.Size), Hash: bytes.Clone(hash[:])}
		byName[name] = state.sharedPullerState
		w.folder.model.progressEmitter.Register(state.sharedPullerState)
	}
	w.folder.setState(FolderSyncing)
	ctx, cancel := context.WithTimeout(w.ctx, time.Minute)
	defer cancel()
	requestStarted := time.Now()
	archive, err := w.request(ctx, device, requests)
	metricFolderArchivePhaseSeconds.WithLabelValues(w.folder.folderID, metricArchivePhaseRequest).Add(time.Since(requestStarted).Seconds())
	if err == nil {
		err = w.folder.CheckAvailableSpace(uint64(payloadBytes))
	}
	if err != nil {
		// No callback has opened a writer: the unchanged prepared states can
		// safely resume on the ordinary authenticated block-request path.
		w.disabled[device] = true
		w.folder.sl.DebugContext(w.ctx, "Archive request unavailable; using ordinary pulls", "files", len(states), "error", err)
		for _, state := range states {
			w.ordinary <- state
		}
		return
	}

	// processNeeded already checked/resurrected each parent. Callbacks retain
	// a fresh traversal check and ordinary temp-file creation checks; a parent
	// removed meanwhile fails this window and is repaired by a later planner.
	materializeStarted := time.Now()
	err = archivebatch.UnpackTo(ctx, bytes.NewReader(archive), int64(len(archive)), members, func(ctx context.Context, member archivebatch.Member, input io.Reader) error {
		state, ok := byName[member.Name]
		if !ok {
			return fmt.Errorf("unexpected archive member %q", member.Name)
		}
		return w.folder.materializeArchiveMember(ctx, state, input)
	})
	if err != nil {
		// Some callbacks may have written or durably closed normal temp files.
		// Never blindly restart their counters/writers or publish a subset of
		// this failed window. Original finishers close failures and leave need
		// intact; the next iteration can reuse valid ordinary temp blocks.
		w.disabled[device] = true
		for _, state := range states {
			state.fail(fmt.Errorf("archive materialization: %w", err))
		}
	}
	metricFolderArchivePhaseSeconds.WithLabelValues(w.folder.folderID, metricArchivePhaseMaterialize).Add(time.Since(materializeStarted).Seconds())
	w.finish(states)
}

func (w *archivePullWindow) finish(states []copyBlocksState) {
	started := time.Now()
	defer func() {
		metricFolderArchivePhaseSeconds.WithLabelValues(w.folder.folderID, metricArchivePhaseFinish).Add(time.Since(started).Seconds())
	}()
	workers := min(archiveWorkers, w.folder.Copiers, len(states))
	workers = max(1, workers)
	ready := make(chan *sharedPullerState, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() { w.folder.finisherRoutine(w.ctx, ready, w.dbUpdates, w.scans) })
	}
	for _, state := range states {
		ready <- state.sharedPullerState
	}
	close(ready)
	wg.Wait()
}

func (f *sendReceiveFolder) materializeArchiveMember(ctx context.Context, state *sharedPullerState, input io.Reader) error {
	data, err := f.verifyArchiveMemberData(ctx, state, input)
	if err != nil {
		return err
	}
	state.pullStarted()
	openStarted := time.Now()
	writer, err := state.tempFile()
	metricFolderArchiveWorkerSeconds.WithLabelValues(f.folderID, metricArchiveWorkerTempOpen).Add(time.Since(openStarted).Seconds())
	if err != nil {
		return err
	}
	writeStarted := time.Now()
	writeErr := f.limitedWriteAt(ctx, writer, data, 0)
	metricFolderArchiveWorkerSeconds.WithLabelValues(f.folderID, metricArchiveWorkerWrite).Add(time.Since(writeStarted).Seconds())
	closeStarted := time.Now()
	closeErr := state.closeArchiveTemp(writeErr == nil)
	metricFolderArchiveWorkerSeconds.WithLabelValues(f.folderID, metricArchiveWorkerSyncClose).Add(time.Since(closeStarted).Seconds())
	if err := errors.Join(writeErr, closeErr); err != nil {
		return err
	}
	state.pullDone(state.file.Blocks[0])
	return nil
}

func (f *sendReceiveFolder) verifyArchiveMemberData(ctx context.Context, state *sharedPullerState, input io.Reader) ([]byte, error) {
	started := time.Now()
	defer func() {
		metricFolderArchiveWorkerSeconds.WithLabelValues(f.folderID, metricArchiveWorkerVerifyParent).Add(time.Since(started).Seconds())
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(input, state.file.Size+1))
	if err != nil {
		return nil, err
	}
	if err := f.verifyBuffer(data, state.file.Blocks[0]); err != nil {
		return nil, err
	}
	if err := osutil.TraversesSymlink(f.mtimefs, filepath.Dir(state.file.Name)); err != nil {
		return nil, fmt.Errorf("checking archive parent: %w", err)
	}
	return data, nil
}

func (m *model) requestArchive(ctx context.Context, device protocol.DeviceID, requests []*protocol.Request) ([]byte, error) {
	conn, ok := m.requestConnectionForDevice(device)
	if !ok {
		return nil, fmt.Errorf("archive request: no connection to device: %s", device.Short())
	}
	activity.using(Availability{ID: device})
	defer activity.done(Availability{ID: device})
	return protocol.RequestArchive(ctx, conn, requests)
}

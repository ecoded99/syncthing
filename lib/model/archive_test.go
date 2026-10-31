// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/syncthing/syncthing/lib/archivebatch"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/semaphore"
)

func archiveCounterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	var metric dto.Metric
	must(t, counter.Write(&metric))
	return metric.GetCounter().GetValue()
}

func archiveReadFile(t *testing.T, filesystem fs.Filesystem, name string) []byte {
	t.Helper()
	file, err := filesystem.Open(name)
	must(t, err)
	data, err := io.ReadAll(file)
	must(t, errors.Join(err, file.Close()))
	return data
}

func setupArchiveFolder(t *testing.T) (*testModel, *sendReceiveFolder) {
	t.Helper()
	w, cfg := newDefaultCfgWrapper(t)
	cfg.FilesystemType = config.FilesystemTypeBasic
	cfg.Path = filepath.Join(t.TempDir(), "folder")
	cfg.MinDiskFree = config.Size{}
	cfg.ExperimentalArchiveTransfers = true
	cfg.DisableFsync = false
	cfg.Copiers = 8
	must(t, cfg.CreateRoot())
	must(t, cfg.CreateMarker())
	setFolder(t, w, cfg)
	// These tests drive the receiver directly. Register the real folder and
	// SQLite state without starting background folder scanners/watchers.
	m := newModel(t, w, myID, nil)
	must(t, m.initFolders(w.RawCopy()))
	t.Cleanup(m.evCancel)
	runner, ok := m.folderRunners.Get(cfg.ID)
	if !ok {
		t.Fatal("archive folder runner missing")
	}
	f := runner.(*sendReceiveFolder)
	must(t, f.scanSubdirs(t.Context(), nil))
	f.tempPullErrors = make(map[string]string)
	return m, f
}

func archiveTestFile(name string, data []byte, sequence int64) protocol.FileInfo {
	hash := sha256.Sum256(data)
	return protocol.FileInfo{
		Name: name, Size: int64(len(data)), Type: protocol.FileInfoTypeFile,
		Permissions: 0o640, ModifiedS: 1700000000, ModifiedNs: 123456700,
		ModifiedBy: device1.Short(), Sequence: sequence,
		Version: protocol.Vector{}.Update(device1.Short()),
		Blocks:  []protocol.BlockInfo{{Size: len(data), Hash: hash[:]}},
	}
}

func archiveTestPack(t *testing.T, files []protocol.FileInfo, data map[string][]byte) []byte {
	t.Helper()
	members := make([]archivebatch.Member, len(files))
	for i, file := range files {
		var hash [sha256.Size]byte
		copy(hash[:], file.Blocks[0].Hash)
		members[i] = archivebatch.Member{Name: osutil.NormalizedFilename(file.Name), Size: file.Size, SHA256: hash, Modified: file.ModTime()}
	}
	var result bytes.Buffer
	err := archivebatch.Pack(t.Context(), &result, members, func(_ context.Context, member archivebatch.Member) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data[member.Name])), nil
	})
	must(t, err)
	return result.Bytes()
}

func preparedArchiveWindow(t *testing.T, f *sendReceiveFolder, files []protocol.FileInfo, payload []byte) (*archivePullWindow, chan dbUpdateJob, chan copyBlocksState) {
	t.Helper()
	dbUpdates := make(chan dbUpdateJob, 16)
	ordinary := make(chan copyBlocksState, 16)
	w := f.newArchivePullWindow(t.Context(), dbUpdates, ordinary, make(chan string, 16))
	if w == nil {
		t.Fatal("archive route not enabled for real filesystem")
	}
	w.device = device1
	for _, file := range files {
		state, err := f.prepareFile(t.Context(), file)
		must(t, err)
		w.states = append(w.states, state)
		w.bytes += file.Size
	}
	w.request = func(_ context.Context, _ protocol.DeviceID, reqs []*protocol.Request) ([]byte, error) {
		if len(reqs) != len(files) {
			t.Errorf("requested %d members, expected %d", len(reqs), len(files))
		}
		return payload, nil
	}
	return w, dbUpdates, ordinary
}

func TestArchiveMaterializesFreshAndChanged(t *testing.T) {
	m, f := setupArchiveFolder(t)
	must(t, f.mtimefs.Mkdir("nested", 0o755))
	changed := filepath.Join("nested", "changed.bin")
	writeFile(t, f.mtimefs, changed, []byte("original"))
	must(t, f.scanSubdirs(t.Context(), nil))
	cur, ok, err := m.sdb.GetDeviceFile(f.folderID, protocol.LocalDeviceID, changed)
	must(t, err)
	if !ok {
		t.Fatal("existing local file was not scanned")
	}
	data := map[string][]byte{"fresh.bin": []byte("new archive file"), "nested/changed.bin": []byte("changed archive file")}
	files := []protocol.FileInfo{archiveTestFile("fresh.bin", data["fresh.bin"], 1), archiveTestFile(changed, data["nested/changed.bin"], 2)}
	files[1].Version = cur.Version.Update(device1.Short())
	must(t, m.sdb.Update(f.folderID, device1, files))
	w, dbUpdates, ordinary := preparedArchiveWindow(t, f, files, archiveTestPack(t, files, data))
	before := archiveCounterValue(t, metricFolderArchiveFilesTotal.WithLabelValues(f.folderID))
	phaseBefore := make(map[string]float64)
	for _, phase := range []string{metricArchivePhaseRequest, metricArchivePhaseMaterialize, metricArchivePhaseFinish} {
		phaseBefore[phase] = archiveCounterValue(t, metricFolderArchivePhaseSeconds.WithLabelValues(f.folderID, phase))
	}
	w.flush()
	if len(ordinary) != 0 || len(dbUpdates) != 2 {
		t.Fatalf("fallback=%d index updates=%d", len(ordinary), len(dbUpdates))
	}
	if archiveCounterValue(t, metricFolderArchiveFilesTotal.WithLabelValues(f.folderID))-before != 2 {
		t.Fatal("successful archive publication was not observable")
	}
	for phase, before := range phaseBefore {
		after := archiveCounterValue(t, metricFolderArchivePhaseSeconds.WithLabelValues(f.folderID, phase))
		// The mocked request returns a slice immediately and may complete
		// within one Windows clock tick; real materialization/finish do I/O.
		if after < before || (phase != metricArchivePhaseRequest && after == before) {
			t.Fatalf("archive phase was not observed: %s", phase)
		}
	}
	close(dbUpdates)
	if changed := f.dbUpdaterRoutine(dbUpdates); changed != 2 {
		t.Fatalf("committed %d files", changed)
	}
	for _, file := range files {
		got := archiveReadFile(t, f.mtimefs, file.Name)
		if !bytes.Equal(got, data[osutil.NormalizedFilename(file.Name)]) {
			t.Fatalf("wrong published data: %s", file.Name)
		}
		info, err := f.mtimefs.Lstat(file.Name)
		must(t, err)
		if !info.ModTime().Equal(file.ModTime()) {
			t.Fatalf("wrong published mtime: %s", file.Name)
		}
		indexed, ok, err := m.sdb.GetDeviceFile(f.folderID, protocol.LocalDeviceID, file.Name)
		must(t, err)
		if !ok || !indexed.Version.Equal(file.Version) || !indexed.BlocksEqual(file) {
			t.Fatalf("wrong committed index: %s", file.Name)
		}
		if _, err := f.mtimefs.Lstat(fs.TempName(file.Name)); !fs.IsNotExist(err) {
			t.Fatalf("temporary file remains: %s (%v)", file.Name, err)
		}
	}
	need, err := m.sdb.CountNeed(f.folderID, protocol.LocalDeviceID)
	must(t, err)
	if need.TotalItems() != 0 {
		t.Fatalf("needed files remain after archive index commit: %v", need)
	}
}

func TestArchiveCorruptionNeverPublishesSubset(t *testing.T) {
	_, f := setupArchiveFolder(t)
	data := map[string][]byte{"first.bin": []byte("unique first payload"), "second.bin": []byte("unique second payload")}
	files := []protocol.FileInfo{archiveTestFile("first.bin", data["first.bin"], 1), archiveTestFile("second.bin", data["second.bin"], 2)}
	archive := archiveTestPack(t, files, data)
	pos := bytes.Index(archive, data["second.bin"])
	if pos < 0 {
		t.Fatal("stored ZIP payload not found")
	}
	archive[pos] ^= 1
	w, dbUpdates, ordinary := preparedArchiveWindow(t, f, files, archive)
	states := append([]copyBlocksState(nil), w.states...)
	before := archiveCounterValue(t, metricFolderArchiveFilesTotal.WithLabelValues(f.folderID))
	w.flush()
	if len(dbUpdates) != 0 || len(ordinary) != 0 || archiveCounterValue(t, metricFolderArchiveFilesTotal.WithLabelValues(f.folderID)) != before {
		t.Fatal("failed archive published, indexed, or blindly retried a partially written window")
	}
	for _, state := range states {
		if _, err := f.mtimefs.Lstat(state.file.Name); !fs.IsNotExist(err) {
			t.Fatal("failed window changed a final path")
		}
		if !state.closed || state.writer != nil || state.failed() == nil {
			t.Fatal("failed window retained a writer or a publishable state")
		}
	}
}

func TestArchiveUnsupportedKeepsOrdinaryPreparedState(t *testing.T) {
	_, f := setupArchiveFolder(t)
	file := archiveTestFile("fallback.bin", []byte("ordinary fallback"), 1)
	w, dbUpdates, ordinary := preparedArchiveWindow(t, f, []protocol.FileInfo{file}, nil)
	state := w.states[0].sharedPullerState
	w.request = func(context.Context, protocol.DeviceID, []*protocol.Request) ([]byte, error) {
		return nil, protocol.ErrArchiveUnsupported
	}
	w.flush()
	if len(dbUpdates) != 0 || len(ordinary) != 1 {
		t.Fatal("unsupported peer did not fall back to ordinary copying")
	}
	got := <-ordinary
	if got.sharedPullerState != state || got.copyNeeded != 1 || got.pullNeeded != 0 || got.closed || got.writer != nil || got.archiveMaterialized {
		t.Fatal("fallback changed the ordinary state")
	}
	if !w.disabled[device1] {
		t.Fatal("unsupported peer would be retried for every window")
	}
}

func TestArchiveMissingParentRemainsNeeded(t *testing.T) {
	m, f := setupArchiveFolder(t)
	must(t, f.mtimefs.Mkdir("removed-parent", 0o755))
	data := []byte("parent disappears while fetching archive")
	file := archiveTestFile(filepath.Join("removed-parent", "needed.bin"), data, 1)
	must(t, m.sdb.Update(f.folderID, device1, []protocol.FileInfo{file}))
	payload := archiveTestPack(t, []protocol.FileInfo{file}, map[string][]byte{osutil.NormalizedFilename(file.Name): data})
	w, dbUpdates, ordinary := preparedArchiveWindow(t, f, []protocol.FileInfo{file}, payload)
	scans := make(chan string, 2)
	w.scans = scans
	state := w.states[0].sharedPullerState
	w.request = func(context.Context, protocol.DeviceID, []*protocol.Request) ([]byte, error) {
		must(t, f.mtimefs.Remove("removed-parent"))
		return payload, nil
	}
	before := archiveCounterValue(t, metricFolderArchiveFilesTotal.WithLabelValues(f.folderID))
	w.flush()
	if len(dbUpdates) != 0 || len(ordinary) != 0 || len(scans) != 0 || archiveCounterValue(t, metricFolderArchiveFilesTotal.WithLabelValues(f.folderID)) != before {
		t.Fatal("missing parent caused publication, indexing, or an unexpected fallback/resurrection")
	}
	if !state.closed || state.writer != nil || state.archiveMaterialized || state.failed() == nil {
		t.Fatal("missing-parent failure retained a writer or publishable state")
	}
	if _, err := f.mtimefs.Lstat("removed-parent"); !fs.IsNotExist(err) {
		t.Fatal("archive materializer resurrected a parent after planning")
	}
	if _, exists, err := m.sdb.GetDeviceFile(f.folderID, protocol.LocalDeviceID, file.Name); err != nil || exists {
		t.Fatal("missing-parent failure entered the local index", err)
	}
	need, err := m.sdb.CountNeed(f.folderID, protocol.LocalDeviceID)
	must(t, err)
	if need.Files != 1 || need.TotalItems() != 1 || need.Bytes != int64(len(data)) {
		t.Fatalf("failed archive lost needed file: %v", need)
	}
	// The original planner remains responsible for resurrection on retry.
	if !f.checkParent(file.Name, scans) || len(scans) != 1 {
		t.Fatal("ordinary next-iteration parent repair no longer works")
	}
}

func TestArchiveResumeKeepsOriginalCopier(t *testing.T) {
	_, f := setupArchiveFolder(t)
	data := []byte("already downloaded complete block")
	file := archiveTestFile("resume.bin", data, 1)
	writeFile(t, f.mtimefs, fs.TempName(file.Name), data)
	state, err := f.prepareFile(t.Context(), file)
	must(t, err)
	if state.reused != 1 || len(state.blocks) != 0 {
		t.Fatal("test failed to prepare a reusable temporary block")
	}
	ordinary := make(chan copyBlocksState, 1)
	w := f.newArchivePullWindow(t.Context(), make(chan dbUpdateJob, 1), ordinary, make(chan string, 1))
	w.request = func(context.Context, protocol.DeviceID, []*protocol.Request) ([]byte, error) {
		return nil, errors.New("must not request resumed content")
	}
	w.add(state)
	if len(w.states) != 0 || len(ordinary) != 1 || (<-ordinary).sharedPullerState != state.sharedPullerState {
		t.Fatal("archive route consumed a resumable temporary block")
	}
}

func TestArchiveRejectsUnscannedLocalEdit(t *testing.T) {
	m, f := setupArchiveFolder(t)
	name := "edited.bin"
	writeFile(t, f.mtimefs, name, []byte("old indexed content"))
	must(t, f.scanSubdirs(t.Context(), nil))
	cur, ok, err := m.sdb.GetDeviceFile(f.folderID, protocol.LocalDeviceID, name)
	must(t, err)
	if !ok {
		t.Fatal("local index missing")
	}
	data := []byte("incoming archive content")
	file := archiveTestFile(name, data, 1)
	file.Version = cur.Version.Update(device1.Short())
	payload := archiveTestPack(t, []protocol.FileInfo{file}, map[string][]byte{name: data})
	w, dbUpdates, _ := preparedArchiveWindow(t, f, []protocol.FileInfo{file}, payload)
	w.request = func(context.Context, protocol.DeviceID, []*protocol.Request) ([]byte, error) {
		writeFile(t, f.mtimefs, name, []byte("new unscanned local edit must survive"))
		stamp := time.Now().Add(time.Hour)
		must(t, f.mtimefs.Chtimes(name, stamp, stamp))
		return payload, nil
	}
	w.flush()
	got := archiveReadFile(t, f.mtimefs, name)
	if string(got) != "new unscanned local edit must survive" || len(dbUpdates) != 0 {
		t.Fatal("archive path bypassed original local-change protection")
	}
}

func TestArchiveEligibilityAndDefault(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	if f.newArchivePullWindow(t.Context(), nil, nil, nil) != nil {
		t.Fatal("archive route enabled by default")
	}
	file := archiveTestFile("small.bin", []byte("content"), 1)
	if !archiveFileEligible(file) {
		t.Fatal("valid one-block file ineligible")
	}
	for _, mutate := range []func(*protocol.FileInfo){
		func(f *protocol.FileInfo) { f.Deleted = true },
		func(f *protocol.FileInfo) { f.Type = protocol.FileInfoTypeDirectory },
		func(f *protocol.FileInfo) { f.Encrypted = []byte{1} },
		func(f *protocol.FileInfo) { f.Size = archiveMemberBytes + 1 },
		func(f *protocol.FileInfo) { f.Blocks = nil },
		func(f *protocol.FileInfo) { f.Size = 0 },
	} {
		bad := file
		mutate(&bad)
		if archiveFileEligible(bad) {
			t.Fatal("unsupported file accepted")
		}
	}
	if !fs.IsTemporary(fs.TempName(filepath.Join("nested", "small.bin"))) {
		t.Fatal("archive temporary path is not ignored by the scanner convention")
	}
}

type archiveCloseRecorder struct {
	fs.File
	syncs, closes int
	closeError    error
}

func (f *archiveCloseRecorder) Sync() error {
	f.syncs++
	return f.File.Sync()
}

func (f *archiveCloseRecorder) Close() error {
	f.closes++
	return errors.Join(f.File.Close(), f.closeError)
}

func TestArchiveTempSyncCloseOwnership(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "close-error"}[fail], func(t *testing.T) {
			_, f := setupSendReceiveFolder(t)
			file := archiveTestFile("closed.bin", []byte("verified block"), 1)
			state, err := f.prepareFile(t.Context(), file)
			must(t, err)
			state.pullStarted()
			writer, err := state.tempFile()
			must(t, err)
			recorder := &archiveCloseRecorder{File: writer.fd}
			if fail {
				recorder.closeError = errors.New("controlled close failure")
			}
			writer.fd = recorder
			must(t, f.limitedWriteAt(t.Context(), writer, []byte("verified block"), 0))
			err = state.closeArchiveTemp(true)
			if fail && !errors.Is(err, recorder.closeError) {
				t.Fatal("close failure was lost")
			}
			if !fail {
				must(t, err)
				state.pullDone(file.Blocks[0])
			} else {
				state.fail(err)
			}
			if state.closed || state.writer != nil || state.archiveMaterialized == fail {
				t.Fatal("archive callback claimed finisher or retained writer")
			}
			closed, err := state.finalClose()
			if !closed || (fail && !errors.Is(err, recorder.closeError)) {
				t.Fatal("normal finisher did not claim the expected result")
			}
			if recorder.syncs != 1 || recorder.closes != 1 {
				t.Fatalf("Sync/Close repeated: %d/%d", recorder.syncs, recorder.closes)
			}
			if closed, _ := state.finalClose(); closed {
				t.Fatal("state was acknowledged twice")
			}
		})
	}
}

type archiveCancelReader struct {
	*bytes.Reader
	cancel context.CancelFunc
}

func (r archiveCancelReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.cancel()
	}
	return n, err
}

func TestArchiveCancellationClosesOrdinaryTemp(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	data := []byte("cancel after payload read")
	file := archiveTestFile("cancel.bin", data, 1)
	state, err := f.prepareFile(t.Context(), file)
	must(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.writeLimiter = semaphore.New(1)
	f.writeLimiter.Take(1)
	defer f.writeLimiter.Give(1)
	err = f.materializeArchiveMember(ctx, state.sharedPullerState, archiveCancelReader{bytes.NewReader(data), cancel})
	if !errors.Is(err, context.Canceled) || state.writer != nil || state.archiveMaterialized || state.closed {
		t.Fatal("canceled archive retained a writer or publishable state", err)
	}
	state.fail(err)
	if closed, err := state.finalClose(); !closed || !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation did not reach original failed finisher")
	}
}

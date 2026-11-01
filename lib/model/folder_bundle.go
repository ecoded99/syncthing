// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"context"
	"crypto/sha256"
	"sync"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

type fileBundle struct {
	conn  protocol.Connection
	files []protocol.FileInfo
}

// Initial copies only: preserve local block reuse, resumed temporary files and
// all existing-file paths in the ordinary puller. The FileInfo snapshot is the
// local manifest (version, metadata, size and hash) used through finalization.
func bundleFileEligible(file protocol.FileInfo) bool {
	return file.Type == protocol.FileInfoTypeFile && !file.IsDeleted() && !file.IsInvalid() &&
		len(file.Encrypted) == 0 && file.Size > 0 && file.Size <= protocol.BundleMaxFileSize &&
		len(file.Blocks) == 1 && file.Blocks[0].Offset == 0 &&
		int64(file.Blocks[0].Size) == file.Size && len(file.Blocks[0].Hash) == sha256.Size && !file.Blocks[0].IsEmpty()
}

func (f *sendReceiveFolder) bundleConnection(file protocol.FileInfo, preferred protocol.Connection, available []Availability) protocol.Connection {
	if !f.ExperimentalBatchedTransfers || f.Type == config.FolderTypeReceiveEncrypted || !bundleFileEligible(file) {
		return nil
	}
	cur, exists, err := f.model.sdb.GetDeviceFile(f.folderID, protocol.LocalDeviceID, file.Name)
	if err != nil || exists && !cur.IsDeleted() {
		return nil
	}
	// Do not discard resumable data or overwrite an unscanned local edit.
	for _, name := range []string{file.Name, fs.TempName(file.Name)} {
		if _, err := f.mtimefs.Lstat(name); !fs.IsNotExist(err) {
			return nil
		}
	}
	for _, source := range available {
		if preferred != nil && source.ID == preferred.DeviceID() && preferred.Statistics().PeerSupportsBatch {
			return preferred
		}
		if conn, ok := f.model.requestConnectionForDevice(source.ID); ok && conn.Statistics().PeerSupportsBatch {
			return conn
		}
	}
	return nil
}

type downloadedBundle struct {
	fileBundle
	results []protocol.BundleResult
	err     error
}

// Download, staging and durable finalization run independently. Only one bundle
// is downloaded, one queued and one staged at a time (at most 6 MiB of content).
// Staging workers share the folder write limiter and never touch live paths.
func (f *sendReceiveFolder) bundleRoutine(ctx context.Context, in <-chan fileBundle, copyChan chan<- copyBlocksState, out chan<- *sharedPullerState) {
	downloaded := make(chan downloadedBundle, 1)
	go func() {
		defer close(downloaded)
		f.downloadBundles(ctx, in, downloaded)
	}()
	f.stageDownloadedBundles(ctx, downloaded, copyChan, out)
}

func (f *sendReceiveFolder) stageDownloadedBundles(ctx context.Context, downloaded <-chan downloadedBundle, copyChan chan<- copyBlocksState, out chan<- *sharedPullerState) {
	workers := make(chan struct{}, max(1, min(8, f.MaxConcurrentWrites)))
	for bundle := range downloaded {
		var wg sync.WaitGroup
		for i, file := range bundle.files {
			workers <- struct{}{}
			wg.Go(func() {
				defer func() { <-workers }()
				var result protocol.BundleResult
				if bundle.err == nil {
					result = bundle.results[i]
				}
				f.stageBundleMember(ctx, file, result, bundle.err, copyChan, out)
			})
		}
		wg.Wait()
	}
}

func (f *sendReceiveFolder) downloadBundles(ctx context.Context, in <-chan fileBundle, out chan<- downloadedBundle) {
	for bundle := range in {
		f.setState(FolderSyncing)
		requests := make([]protocol.Request, len(bundle.files))
		for i, file := range bundle.files {
			requests[i] = protocol.Request{Folder: f.folderID, Name: file.Name, Size: int(file.Size), Hash: file.Blocks[0].Hash}
		}
		results, err := protocol.RequestBundle(ctx, bundle.conn, requests)
		out <- downloadedBundle{bundle, results, err}
	}
}

func (f *sendReceiveFolder) stageBundleMember(ctx context.Context, file protocol.FileInfo, result protocol.BundleResult, bundleErr error, copyChan chan<- copyBlocksState, out chan<- *sharedPullerState) {
	if ctx.Err() != nil {
		f.queue.Done(file.Name)
		return
	}
	if bundleErr != nil || result.Error != nil {
		// Includes stock/disconnected peers, corrupt frames and individual
		// source errors. Normal requests can select another source.
		if err := f.handleFile(ctx, file, copyChan); err != nil {
			f.newPullError(file.Name, err)
		}
		return
	}
	f.evLogger.Log(events.ItemStarted, map[string]string{"folder": f.folderID, "item": file.Name, "type": "file", "action": "update"})
	s, err := f.stageBundleFile(ctx, file, result.Data)
	if err != nil {
		f.newPullError(file.Name, err)
		f.queue.Done(file.Name)
		return
	}
	f.model.progressEmitter.Register(s)
	out <- s
}

func (f *sendReceiveFolder) stageBundleFile(ctx context.Context, file protocol.FileInfo, data []byte) (*sharedPullerState, error) {
	cur, exists, err := f.model.sdb.GetDeviceFile(f.folderID, protocol.LocalDeviceID, file.Name)
	if err != nil {
		return nil, err
	}
	s := newSharedPullerState(file, f.mtimefs, f.folderID, fs.TempName(file.Name), nil, nil, f.IgnorePerms || file.NoPermissions, exists, cur, !f.DisableSparseFiles, !f.DisableFsync)
	fd, err := s.tempFile() // Exclusive create; never remove a raced-in temp file.
	if err == nil {
		err = f.limitedWriteAt(ctx, fd, data, 0)
	}
	if err != nil {
		s.fail(err)
	} else {
		s.pullTotal = 1
	}
	return s, nil // The normal finisher closes handles even after staging errors.
}

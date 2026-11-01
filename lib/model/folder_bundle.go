// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"context"
	"crypto/sha256"

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

func (f *sendReceiveFolder) bundleConnection(file protocol.FileInfo, preferred protocol.Connection) protocol.Connection {
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
	for _, available := range f.model.fileAvailability(f.FolderConfiguration, file) {
		if preferred != nil && available.ID == preferred.DeviceID() && preferred.Statistics().PeerSupportsBatch {
			return preferred
		}
		if conn, ok := f.model.requestConnectionForDevice(available.ID); ok && conn.Statistics().PeerSupportsBatch {
			return conn
		}
	}
	return nil
}

// One downloading/staging worker and one queued bundle bound memory independently
// of folder size. Ordinary finishers flush and apply verified staged files while
// the next bundle downloads. Nothing is extracted over live paths.
func (f *sendReceiveFolder) bundleRoutine(ctx context.Context, in <-chan fileBundle, copyChan chan<- copyBlocksState, out chan<- *sharedPullerState) {
	for bundle := range in {
		f.setState(FolderSyncing)
		requests := make([]protocol.Request, len(bundle.files))
		for i, file := range bundle.files {
			requests[i] = protocol.Request{Folder: f.folderID, Name: file.Name, Size: int(file.Size), Hash: file.Blocks[0].Hash}
		}
		results, err := protocol.RequestBundle(ctx, bundle.conn, requests)
		for i, file := range bundle.files {
			if ctx.Err() != nil {
				f.queue.Done(file.Name)
				continue
			}
			if err != nil || results[i].Error != nil {
				// Includes stock/disconnected peers, corrupt frames and individual
				// source errors. Normal requests can select another source.
				if err := f.handleFile(ctx, file, copyChan); err != nil {
					f.newPullError(file.Name, err)
				}
				continue
			}
			f.evLogger.Log(events.ItemStarted, map[string]string{"folder": f.folderID, "item": file.Name, "type": "file", "action": "update"})
			s, err := f.stageBundleFile(ctx, file, results[i].Data)
			if err != nil {
				f.newPullError(file.Name, err)
				f.queue.Done(file.Name)
				continue
			}
			f.model.progressEmitter.Register(s)
			out <- s
		}
	}
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

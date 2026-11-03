// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package scanner

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

const scanMetadataBatchSize = 128

// BatchCurrentFiler is optional. Implementations must support concurrent calls
// and return owned metadata (including block size/hash, not block lists).
type BatchCurrentFiler interface {
	CurrentFiles([]string) (map[string]protocol.FileInfo, error)
}

type scanMetadataItem struct {
	path string
	info fs.FileInfo
}

type cachedCurrentFiles map[string]protocol.FileInfo

func (c cachedCurrentFiles) CurrentFile(name string) (protocol.FileInfo, bool) {
	fi, ok := c[name]
	// updateFileInfo increments the returned vector in place. Each lookup must
	// own it, even when overlapping inputs refer to the same cached name.
	fi.Version = fi.Version.Copy()
	return fi, ok
}

// Only accepted regular files enter this stage. Ignore decisions, normalization,
// symlinks and directory traversal remain ordered on the walking goroutine.
func (w *walker) startMetadata(ctx context.Context, toHash chan<- protocol.FileInfo, finished chan<- ScanResult) func() {
	current, ok := w.CurrentFiler.(BatchCurrentFiler)
	if !ok {
		return func() {}
	}
	workers := w.MetadataWorkers
	if workers <= 0 {
		workers = 4
	}
	workers = max(1, min(workers, 8, runtime.GOMAXPROCS(0)))
	items := make(chan scanMetadataItem, scanMetadataBatchSize)
	jobs := make(chan []scanMetadataItem, workers)
	w.regularQueue = items
	var wg sync.WaitGroup
	for range workers {
		worker := *w
		wg.Go(func() {
			for batch := range jobs {
				if ctx.Err() != nil {
					return
				}
				names := make([]string, len(batch))
				for i, item := range batch {
					names[i] = item.path
				}
				files, err := current.CurrentFiles(names)
				if err != nil {
					// Never interpret a failed lookup as a new local file/version.
					for _, item := range batch {
						handleError(ctx, "scan metadata", item.path, err, finished)
					}
					continue
				}
				worker.CurrentFiler = cachedCurrentFiles(files)
				for _, item := range batch {
					if ctx.Err() != nil {
						return
					}
					if err := worker.walkRegular(ctx, item.path, item.info, toHash); err != nil {
						handleError(ctx, "scan", item.path, err, finished)
					}
				}
			}
		})
	}
	wg.Go(func() {
		defer close(jobs)
		batch := make([]scanMetadataItem, 0, scanMetadataBatchSize)
		flush := func() bool {
			select {
			case jobs <- batch:
				batch = make([]scanMetadataItem, 0, scanMetadataBatchSize)
				return true
			case <-ctx.Done():
				return false
			}
		}
		// Flush small batches even if discovery pauses in a slow directory.
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case item, ok := <-items:
				if !ok {
					if len(batch) > 0 {
						flush()
					}
					return
				}
				batch = append(batch, item)
				if len(batch) == scanMetadataBatchSize && !flush() {
					return
				}
			case <-ticker.C:
				if len(batch) > 0 && !flush() {
					return
				}
			case <-ctx.Done():
				return
			}
		}

	})
	return func() {
		close(items)
		wg.Wait()
	}
}

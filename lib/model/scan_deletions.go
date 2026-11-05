// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"context"
	"iter"
	"runtime"
	"sync"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
)

const scanDeletionBatchSize = 128

type scanDeletionResult struct {
	file                    protocol.FileInfo
	ignored, deleted, check bool
}

// Only filesystem reads run in parallel. Results retain database order so
// ignored-parent handling, receive-only reconciliation and index writes remain
// on the caller. At most one batch of metadata is retained; no seen-file cache
// can hide files removed after discovery.
func scanDeletionChecks(ctx context.Context, files iter.Seq[protocol.FileInfo], errFn func() error, filesystem fs.Filesystem, matcher *ignore.Matcher, workers int) iter.Seq2[scanDeletionResult, error] {
	return func(yield func(scanDeletionResult, error) bool) {
		workers = max(1, min(workers, 8, runtime.GOMAXPROCS(0)))
		checkers := make([]*osutil.DeletedChecker, workers)
		for i := range checkers {
			checkers[i] = osutil.NewDeletedChecker(filesystem)
		}
		batch := make([]scanDeletionResult, 0, scanDeletionBatchSize)
		flush := func() bool {
			check := func(worker int) {
				for i := worker; i < len(batch); i += workers {
					if ctx.Err() != nil {
						return
					}
					if batch[i].check {
						batch[i].deleted = checkers[worker].IsDeleted(batch[i].file.Name)
					}
				}
			}
			if workers == 1 {
				check(0)
			} else {
				var wg sync.WaitGroup
				for worker := range workers {
					wg.Go(func() { check(worker) })
				}
				wg.Wait()
			}
			if err := ctx.Err(); err != nil {
				yield(scanDeletionResult{}, err)
				return false
			}
			for _, result := range batch {
				if !yield(result, nil) {
					return false
				}
			}
			clear(batch)
			batch = batch[:0]
			return true
		}
		for file := range files {
			if err := ctx.Err(); err != nil {
				yield(scanDeletionResult{}, err)
				return
			}
			ignored := matcher.Match(file.Name).IsIgnored()
			check := !ignored && (file.IsIgnored() || !file.IsDeleted() && !file.IsUnsupported())
			batch = append(batch, scanDeletionResult{file: file, ignored: ignored, check: check})
			if len(batch) == cap(batch) && !flush() {
				return
			}
		}
		if len(batch) > 0 && !flush() {
			return
		}
		if err := errFn(); err != nil {
			yield(scanDeletionResult{}, err)
		}
	}
}

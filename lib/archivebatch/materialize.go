// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package archivebatch

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
)

// UnpackTo verifies a bounded ZIP and calls materialize for each independently
// verified member, using at most eight workers. Each <=64KiB entry is fully read
// and checked (including ZIP CRC and trusted SHA256) before its callback starts.
// Payload buffering is thus <=8*64KiB, rather than a full batch in memory.
//
// ZIP timestamps and permissions are ignored. The callback owns normal temp-file
// creation, inherited ACLs, Sync/Close and cleanup; it must consume the supplied
// reader and close its writers on every path, including cancellation. All started
// callbacks join before return. On ANY error the caller must fail the whole batch
// and must not publish or acknowledge even the members whose callbacks succeeded.
// Cancellation is checked between bounded reads; supplied I/O must itself avoid
// unbounded blocking. This function never publishes a destination or updates DBs.
func UnpackTo(ctx context.Context, r io.ReaderAt, archiveSize int64, expected []Member, materialize func(context.Context, Member, io.Reader) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	byName, err := validateMembers(expected)
	if err != nil {
		return err
	}
	if r == nil || materialize == nil {
		return errors.New("archivebatch: missing ZIP or materializer")
	}
	reader := contextReaderAt{ctx, r}
	if err := validateZIP(reader, archiveSize, byName); err != nil {
		return err
	}
	zr, err := zip.NewReader(reader, archiveSize)
	if err != nil {
		return err
	}
	entries := make([]*zip.File, len(expected))
	for _, entry := range zr.File {
		idx, ok := byName[entry.Name]
		if !ok || entries[idx] != nil || !entry.Mode().IsRegular() || entry.UncompressedSize64 != uint64(expected[idx].Size) {
			return errors.New("archivebatch: ZIP inventory differs from expected regular files")
		}
		entries[idx] = entry
	}
	if len(zr.File) != len(expected) {
		return errors.New("archivebatch: incomplete ZIP inventory")
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	var wg sync.WaitGroup
	var errorsMut sync.Mutex
	var failures []error
	for range min(workers, len(expected)) {
		wg.Go(func() {
			for idx := range jobs {
				if workCtx.Err() != nil {
					continue
				}
				member := expected[idx]
				err := materializeEntry(workCtx, entries[idx], member, materialize)
				if err != nil {
					errorsMut.Lock()
					failures = append(failures, fmt.Errorf("archivebatch: materialize %q: %w", member.Name, err))
					errorsMut.Unlock()
					cancel()
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
	return errors.Join(append(failures, ctx.Err())...)
}

func materializeEntry(ctx context.Context, entry *zip.File, member Member, materialize func(context.Context, Member, io.Reader) error) error {
	r, err := entry.Open()
	if err != nil {
		return err
	}
	data, readErr := readMember(ctx, r, member.Size)
	closeErr := r.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return err
	}
	if int64(len(data)) != member.Size || sha256.Sum256(data) != member.SHA256 {
		return errors.New("entry size or SHA256 mismatch")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	content := bytes.NewReader(data)
	if err := materialize(ctx, member, content); err != nil {
		return err
	}
	if content.Len() != 0 {
		return errors.New("materializer did not consume complete member")
	}
	return ctx.Err()
}

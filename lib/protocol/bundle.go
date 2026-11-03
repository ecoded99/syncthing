// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package protocol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"sync/atomic"
	"time"

	"golang.org/x/text/unicode/norm"
)

const BundleMaxFiles = batchMaxCount
const BundleMaxFileSize = batchMaxFile

var ErrBundleUnavailable = errors.New("whole-file bundle unavailable")

// BundleResult retains per-member errors. No returned bytes should be applied
// until the caller has checked the corresponding result.
type BundleResult struct {
	Data  []byte
	Error error
}

// RequestBundle explicitly groups whole-file reads rather than depending on
// coincident block requests. It uses the negotiated, bounded STB1/STR1 container;
// no peer-supplied archive paths are ever extracted onto the filesystem.
func RequestBundle(ctx context.Context, conn Connection, requests []Request) ([]BundleResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	c, ok := conn.(wireFormatConnection)
	if !ok || len(requests) < 2 || len(requests) > BundleMaxFiles {
		return nil, ErrBundleUnavailable
	}
	e, ok := c.Connection.(encryptedConnection)
	if !ok || !e.conn.peerBatch.Load() {
		return nil, ErrBundleUnavailable
	}
	reqs := make([]*Request, len(requests))
	for i, req := range requests {
		if _, encrypted := e.folderKeys.get(req.Folder); encrypted {
			return nil, ErrBundleUnavailable
		}
		req.Name = norm.NFC.String(filepath.ToSlash(req.Name))
		if checkFilename(req.Name) != nil || req.Folder == "" || !batchable(&req) {
			return nil, ErrBundleUnavailable
		}
		reqs[i] = &req
	}
	payload, size, err := encodeBatchRequests(reqs)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(payload)
	data, err := e.conn.requestDirect(ctx, &Request{Name: batchName, Size: size, Hash: h[:], batchPayload: payload})
	if err != nil {
		if ctx.Err() == nil {
			e.conn.peerBatch.Store(false)
		}
		return nil, err
	}
	decoded, err := decodeBatchResults(data, reqs)
	if err != nil {
		e.conn.peerBatch.Store(false)
		return nil, err
	}
	workers := 1
	if size >= 256<<10 {
		workers = batchWorkers
	}
	if err := verifyBundleMembers(ctx, decoded, reqs, workers); err != nil {
		if ctx.Err() == nil {
			e.conn.peerBatch.Store(false)
		}
		return nil, err
	}
	results := make([]BundleResult, len(decoded))
	for i, res := range decoded {
		results[i] = BundleResult{res.val, res.err}
	}
	e.conn.batchSent.Add(1)
	return results, nil
}

// No member is exposed until all successful members have been verified.
func verifyBundleMembers(ctx context.Context, decoded []asyncResult, reqs []*Request, workers int) error {
	var mismatch atomic.Bool
	parallelBatchMembers(len(decoded), workers, func(i int) {
		if ctx.Err() != nil || mismatch.Load() || decoded[i].err != nil {
			return
		}
		h := sha256.Sum256(decoded[i].val)
		if !bytes.Equal(h[:], reqs[i].Hash) {
			mismatch.Store(true)
		}
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	if mismatch.Load() {
		return errors.New("bundle member hash mismatch")
	}
	return nil
}

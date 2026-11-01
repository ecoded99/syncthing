// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package protocol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	"github.com/syncthing/syncthing/internal/gen/bep"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// Private, versioned fork extensions. Unknown fields are ignored by stock BEP.
// No extension is sent until the peer explicitly advertises version 1.
const (
	batchCapabilityField protowire.Number = 3000
	batchRequestField    protowire.Number = 3001
	batchMaxCount                         = 32
	batchMaxFile                          = 64 << 10
	batchMaxData                          = batchMaxCount * batchMaxFile
	batchMaxMetadata                      = 512 << 10
	batchName                             = ".stbatch-v1"
)

var errBatchFallback = errors.New("batch unavailable; retry ordinary request")

func setExtension(m proto.Message, field protowire.Number, data []byte) {
	b := m.ProtoReflect().GetUnknown()
	b = protowire.AppendTag(b, field, protowire.BytesType)
	b = protowire.AppendBytes(b, data)
	m.ProtoReflect().SetUnknown(b)
}

func getExtension(m proto.Message, field protowire.Number) []byte {
	b := m.ProtoReflect().GetUnknown()
	var found []byte
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil
		}
		b = b[n:]
		if num == field && typ == protowire.BytesType {
			val, n := protowire.ConsumeBytes(b)
			if n < 0 || found != nil {
				return nil
			}
			found = val
			b = b[n:]
		} else {
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return nil
			}
			b = b[n:]
		}
	}
	return found
}

func bytesEqualVersion(b []byte) bool { return bytes.Equal(b, []byte{1}) }
func batchable(r *Request) bool {
	return r.Offset == 0 && r.BlockNo == 0 && r.Size > 0 && r.Size <= batchMaxFile && len(r.Hash) == sha256.Size && len(r.batchPayload) == 0 && len(r.speedPayload) == 0
}

type batchCall struct {
	ctx    context.Context
	req    Request
	result chan asyncResult
}

func (c *rawConnection) requestBatched(ctx context.Context, req *Request) ([]byte, error) {
	call := &batchCall{ctx: ctx, req: *req, result: make(chan asyncResult, 1)}
	call.req.Hash = bytes.Clone(req.Hash)
	c.batchMut.Lock()
	c.batchPending = append(c.batchPending, call)
	if len(c.batchPending) == 1 {
		c.batchTimer = time.AfterFunc(2*time.Millisecond, c.flushBatch)
	}
	full := len(c.batchPending) >= batchMaxCount
	var group []*batchCall
	if full {
		c.batchTimer.Stop()
		group = c.batchPending
		c.batchPending = nil
	}
	c.batchMut.Unlock()
	if full {
		go c.sendBatch(group)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, ErrClosed
	case res := <-call.result:
		if errors.Is(res.err, errBatchFallback) {
			return c.requestDirect(ctx, req)
		}
		return res.val, res.err
	}
}

func (c *rawConnection) flushBatch() {
	c.batchMut.Lock()
	group := c.batchPending
	c.batchPending = nil
	c.batchMut.Unlock()
	c.sendBatch(group)
}

func (c *rawConnection) sendBatch(group []*batchCall) {
	live := group[:0]
	for _, call := range group {
		if call.ctx.Err() == nil {
			live = append(live, call)
		}
	}
	if len(live) == 0 {
		return
	}
	// This RPC belongs to the connection, not to any one file's context.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-c.closed:
			cancel()
		case <-done:
		}
	}()
	if len(live) == 1 {
		data, err := c.requestDirect(ctx, &live[0].req)
		live[0].result <- asyncResult{data, err}
		return
	}
	reqs := make([]*Request, len(live))
	for i, call := range live {
		reqs[i] = &call.req
	}
	payload, total, err := encodeBatchRequests(reqs)
	if err == nil {
		h := sha256.Sum256(payload)
		var data []byte
		data, err = c.requestDirect(ctx, &Request{Name: batchName, Size: total, Hash: h[:], batchPayload: payload})
		if err == nil {
			var results []asyncResult
			results, err = decodeBatchResults(data, reqs)
			if err == nil {
				c.batchSent.Add(1)
				for i, call := range live {
					call.result <- results[i]
				}
				return
			}
		}
	}
	// Fail closed to the extension and retry using normal authenticated requests.
	c.peerBatch.Store(false)
	for _, call := range live {
		call.result <- asyncResult{err: errBatchFallback}
	}
}

func encodeBatchRequests(reqs []*Request) ([]byte, int, error) {
	if len(reqs) < 2 || len(reqs) > batchMaxCount {
		return nil, 0, errors.New("invalid batch count")
	}
	b := append([]byte("STB1"), byte(len(reqs)))
	total := 0
	for _, req := range reqs {
		if !batchable(req) {
			return nil, 0, errors.New("invalid batch member")
		}
		w := req.toWire()
		w.Id = 0
		data, err := proto.Marshal(w)
		if err != nil {
			return nil, 0, err
		}
		b = binary.AppendUvarint(b, uint64(len(data)))
		b = append(b, data...)
		total += req.Size
	}
	if len(b) > batchMaxMetadata {
		return nil, 0, errors.New("batch metadata limit exceeded")
	}
	return b, total, nil
}

func takeFrame(b []byte, limit int) ([]byte, []byte, error) {
	n, used := binary.Uvarint(b)
	if used <= 0 || n > uint64(limit) || n > uint64(len(b)-used) {
		return nil, nil, errors.New("invalid batch frame length")
	}
	return b[used : used+int(n)], b[used+int(n):], nil
}

func decodeBatchRequests(b []byte) ([]*Request, int, error) {
	if len(b) < 5 || len(b) > batchMaxMetadata || string(b[:4]) != "STB1" || b[4] < 2 || b[4] > batchMaxCount {
		return nil, 0, errors.New("invalid batch header")
	}
	count := int(b[4])
	b = b[5:]
	reqs := make([]*Request, 0, count)
	total := 0
	for range count {
		var frame []byte
		var err error
		frame, b, err = takeFrame(b, batchMaxMetadata)
		if err != nil {
			return nil, 0, err
		}
		w := new(bep.Request)
		if err := proto.Unmarshal(frame, w); err != nil {
			return nil, 0, err
		}
		if err := protovalidate.Validate(w); err != nil {
			return nil, 0, err
		}
		r := requestFromWire(w)
		if !batchable(r) || checkFilename(r.Name) != nil || r.Folder == "" {
			return nil, 0, errors.New("invalid batch member")
		}
		// Nested extensions, even unknown versions, must not be interpreted.
		if len(w.ProtoReflect().GetUnknown()) != 0 {
			return nil, 0, errors.New("extended batch member")
		}
		total += r.Size
		reqs = append(reqs, r)
	}
	if len(b) != 0 || total > batchMaxData {
		return nil, 0, errors.New("invalid batch bounds")
	}
	return reqs, total, nil
}

func (c *rawConnection) serveBatch(outer *Request) (RequestResponse, error) {
	c.batchServeMut.Lock()
	transferred := false
	defer func() {
		if !transferred {
			c.batchServeMut.Unlock()
		}
	}()
	select {
	case <-c.closed:
		return nil, ErrClosed
	default:
	}
	reqs, total, err := decodeBatchRequests(outer.batchPayload)
	hash := sha256.Sum256(outer.batchPayload)
	if err != nil || outer.Name != batchName || outer.Offset != 0 || total != outer.Size || !bytes.Equal(hash[:], outer.Hash) {
		return nil, fmt.Errorf("invalid batch: %w", ErrInvalid)
	}
	b := append([]byte("STR1"), byte(len(reqs)))
	for _, r := range reqs {
		// Ordinary authorization, encryption wrappers, and source consistency checks.
		res, err := c.model.Request(r)
		if _, ok := errors.AsType[protocolError](err); ok {
			return nil, err
		}
		b = append(b, byte(errorToCode(err)))
		if err != nil {
			b = binary.AppendUvarint(b, 0)
			continue
		}
		data := res.Data()
		if len(data) != r.Size {
			res.Close()
			return nil, ErrInvalid
		}
		b = binary.AppendUvarint(b, uint64(len(data)))
		b = append(b, data...)
		// Release the normal request limiter before reading the next member.
		res.Close()
	}
	transferred = true
	return &batchResponse{data: b, done: make(chan struct{}), release: c.batchServeMut.Unlock}, nil
}

func decodeBatchResults(b []byte, reqs []*Request) ([]asyncResult, error) {
	if len(b) < 5 || len(b) > batchMaxData+batchMaxCount*12 || string(b[:4]) != "STR1" || int(b[4]) != len(reqs) {
		return nil, errors.New("invalid batch response")
	}
	b = b[5:]
	results := make([]asyncResult, len(reqs))
	for i, r := range reqs {
		if len(b) == 0 {
			return nil, errors.New("truncated batch result")
		}
		code := ErrorCode(b[0])
		b = b[1:]
		if code < ErrorCodeNoError || code > ErrorCodeInvalidFile {
			return nil, errors.New("invalid batch result code")
		}
		data, rest, err := takeFrame(b, batchMaxFile)
		if err != nil {
			return nil, err
		}
		b = rest
		if code == ErrorCodeNoError && len(data) != r.Size || code != ErrorCodeNoError && len(data) != 0 {
			return nil, errors.New("invalid batch result size")
		}
		results[i] = asyncResult{data[:len(data):len(data)], codeToError(code)}
	}
	if len(b) != 0 {
		return nil, errors.New("trailing batch response")
	}
	return results, nil
}

type batchResponse struct {
	data    []byte
	done    chan struct{}
	once    sync.Once
	release func()
}

func (r *batchResponse) Data() []byte { return r.data }
func (r *batchResponse) Close() {
	r.once.Do(func() {
		close(r.done)
		if r.release != nil {
			r.release()
		}
	})
}
func (r *batchResponse) Wait() { <-r.done }

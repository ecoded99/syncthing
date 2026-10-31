// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package protocol

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"buf.build/go/protovalidate"
	"github.com/syncthing/syncthing/internal/gen/bep"
	"github.com/syncthing/syncthing/lib/archivebatch"
	"golang.org/x/text/unicode/norm"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const (
	archiveCapabilityField protowire.Number = 3010
	archiveRequestField    protowire.Number = 3011
	archiveMaxMetadata                      = 2 << 20
	archiveName                             = ".starchive-v1"
)

// ErrArchiveUnsupported means that ordinary authenticated block requests should
// be used instead. Capability negotiation never changes Connection or its mocks.
var ErrArchiveUnsupported = errors.New("archive transfer unsupported")

type archiveRequester interface {
	requestArchive(context.Context, []*Request) ([]byte, error)
}

// RequestArchive fetches a bounded stored ZIP over the existing authenticated BEP
// connection. Names/hashes refer to the caller's current remote index; the caller
// must validate/stage the complete response against that index before publication.
// Any error leaves all ordinary requests available as the fallback.
func RequestArchive(ctx context.Context, conn Connection, reqs []*Request) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c, ok := conn.(archiveRequester); ok {
		return c.requestArchive(ctx, reqs)
	}
	return nil, ErrArchiveUnsupported
}

// hasExtension distinguishes an invalid/empty/duplicate extension from a normal
// request, so malformed archive frames cannot fall through to ordinary serving.
func hasExtension(m proto.Message, field protowire.Number) bool {
	b := m.ProtoReflect().GetUnknown()
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return false
		}
		b = b[n:]
		if num == field {
			return true
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return false
		}
		b = b[n:]
	}
	return false
}

func archiveMembers(reqs []*Request) ([]archivebatch.Member, int, error) {
	if len(reqs) == 0 || len(reqs) > archivebatch.MaxFiles {
		return nil, 0, ErrInvalid
	}
	members := make([]archivebatch.Member, len(reqs))
	seen := make(map[string]struct{}, len(reqs))
	total := 0
	for i, r := range reqs {
		if r == nil || r.Folder == "" || r.Folder != reqs[0].Folder ||
			r.Offset != 0 || r.BlockNo != 0 || r.FromTemporary ||
			r.Size <= 0 || r.Size > archivebatch.MaxFileBytes || len(r.Hash) != sha256.Size ||
			len(r.batchPayload) != 0 || len(r.archivePayload) != 0 || r.archivePresent ||
			!utf8.ValidString(r.Name) || !norm.NFC.IsNormalString(r.Name) || strings.Contains(r.Name, `\`) || checkFilename(r.Name) != nil {
			return nil, 0, ErrInvalid
		}
		key := strings.ToLower(r.Name)
		if _, ok := seen[key]; ok {
			return nil, 0, ErrInvalid
		}
		seen[key] = struct{}{}
		total += r.Size
		if total > archivebatch.MaxPayloadBytes {
			return nil, 0, ErrInvalid
		}
		members[i] = archivebatch.Member{Name: r.Name, Size: int64(r.Size)}
		copy(members[i].SHA256[:], r.Hash)
	}
	return members, total, nil
}

func encodeArchiveRequests(reqs []*Request) ([]byte, int, error) {
	_, total, err := archiveMembers(reqs)
	if err != nil {
		return nil, 0, err
	}
	b := binary.AppendUvarint([]byte("STA1"), uint64(len(reqs)))
	for _, r := range reqs {
		w := r.toWire()
		w.Id = 0
		if err := protovalidate.Validate(w); err != nil {
			return nil, 0, err
		}
		data, err := proto.Marshal(w)
		if err != nil {
			return nil, 0, err
		}
		if len(b)+binary.MaxVarintLen64+len(data) > archiveMaxMetadata {
			return nil, 0, ErrInvalid
		}
		b = binary.AppendUvarint(b, uint64(len(data)))
		b = append(b, data...)
	}
	return b, total, nil
}

func decodeArchiveRequests(b []byte) ([]*Request, int, error) {
	if len(b) < 5 || len(b) > archiveMaxMetadata || string(b[:4]) != "STA1" {
		return nil, 0, ErrInvalid
	}
	count, n := binary.Uvarint(b[4:])
	if n <= 0 || count == 0 || count > archivebatch.MaxFiles {
		return nil, 0, ErrInvalid
	}
	b = b[4+n:]
	reqs := make([]*Request, 0, int(count))
	for range count {
		frame, rest, err := takeFrame(b, archiveMaxMetadata)
		if err != nil {
			return nil, 0, err
		}
		b = rest
		w := new(bep.Request)
		if err := proto.Unmarshal(frame, w); err != nil {
			return nil, 0, err
		}
		if err := protovalidate.Validate(w); err != nil {
			return nil, 0, err
		}
		if len(w.ProtoReflect().GetUnknown()) != 0 || w.Id != 0 {
			return nil, 0, ErrInvalid
		}
		reqs = append(reqs, requestFromWire(w))
	}
	if len(b) != 0 {
		return nil, 0, ErrInvalid
	}
	_, total, err := archiveMembers(reqs)
	return reqs, total, err
}

func (c *rawConnection) archiveFolderAllowed(folder string) bool {
	// NewConnection always installs encryptedModel here. Unknown raw-model
	// wrappers decline rather than bypassing their encryption/admission policy.
	em, ok := c.model.(encryptedModel)
	if !ok || em.folderKeys == nil {
		return false
	}
	_, encrypted := em.folderKeys.get(folder)
	return !encrypted
}

func (c *rawConnection) requestArchive(ctx context.Context, reqs []*Request) ([]byte, error) {
	if !c.peerArchive.Load() {
		return nil, ErrArchiveUnsupported
	}
	payload, total, err := encodeArchiveRequests(reqs)
	if err != nil {
		return nil, err
	}
	if !c.archiveFolderAllowed(reqs[0].Folder) {
		return nil, ErrArchiveUnsupported
	}
	hash := sha256.Sum256(payload)
	data, err := c.requestDirect(ctx, &Request{Name: archiveName, Size: total, Hash: hash[:], archivePayload: payload})
	if err != nil {
		return nil, err
	}
	if len(data) == 0 || len(data) > archivebatch.MaxArchiveBytes {
		return nil, fmt.Errorf("archive response bound: %w", ErrInvalid)
	}
	if err := validateArchiveResponse(data, reqs); err != nil {
		return nil, fmt.Errorf("archive response framing: %w", ErrInvalid)
	}
	c.archiveRequestsSent.Add(1)
	return data, nil
}

func validateArchiveResponse(data []byte, reqs []*Request) error {
	z, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil || len(z.File) != len(reqs) {
		return ErrInvalid
	}
	expected := make(map[string]*Request, len(reqs))
	for _, req := range reqs {
		expected[req.Name] = req
	}
	for _, file := range z.File {
		req, ok := expected[file.Name]
		if !ok || file.Method != zip.Store || file.Flags&1 != 0 || !file.Mode().IsRegular() || file.UncompressedSize64 != uint64(req.Size) || file.CompressedSize64 != uint64(req.Size) {
			return ErrInvalid
		}
		delete(expected, file.Name)
	}
	if len(expected) != 0 {
		return ErrInvalid
	}
	return nil
}

type archiveBoundedBuffer struct{ bytes.Buffer }

func (b *archiveBoundedBuffer) Write(p []byte) (int, error) {
	if len(p) > archivebatch.MaxArchiveBytes-b.Len() {
		return 0, fmt.Errorf("archive output bound: %w", ErrInvalid)
	}
	return b.Buffer.Write(p)
}

func (c *rawConnection) serveArchive(outer *Request) (RequestResponse, error) {
	if !c.peerArchive.Load() || !c.archiveServeMut.TryLock() {
		return nil, ErrGeneric
	}
	transferred := false
	defer func() {
		if !transferred {
			c.archiveServeMut.Unlock()
		}
	}()
	reqs, total, err := decodeArchiveRequests(outer.archivePayload)
	hash := sha256.Sum256(outer.archivePayload)
	if err != nil || outer.archiveMixed || len(outer.batchPayload) != 0 || outer.Name != archiveName || outer.Folder != "" || outer.Offset != 0 || outer.BlockNo != 0 || outer.FromTemporary || total != outer.Size || !bytes.Equal(hash[:], outer.Hash) {
		return nil, ErrInvalid
	}
	if !c.archiveFolderAllowed(reqs[0].Folder) {
		return nil, ErrInvalid
	}
	members, _, err := archiveMembers(reqs)
	if err != nil {
		return nil, err
	}
	requests := make(map[string]*Request, len(reqs))
	for _, r := range reqs {
		requests[r.Name] = r
	}
	ctx, cancel := context.WithCancel(context.Background())
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
	var output archiveBoundedBuffer
	err = archivebatch.Pack(ctx, &output, members, func(ctx context.Context, member archivebatch.Member) (io.ReadCloser, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		original := requests[member.Name]
		if original == nil {
			return nil, ErrInvalid
		}
		r := *original
		r.Hash = bytes.Clone(original.Hash)
		// Preserve ordinary source authorization, path/encryption wrappers,
		// source-change checks and normal byte/IO permits for every member.
		response, err := c.model.Request(&r)
		if err != nil {
			if response != nil {
				response.Close()
			}
			return nil, err
		}
		if response == nil {
			return nil, ErrInvalid
		}
		source := response.Data()
		if len(source) != original.Size {
			response.Close()
			return nil, ErrInvalid
		}
		data := bytes.Clone(source)
		response.Close() // Release permits BEFORE asking for another member.
		return io.NopCloser(bytes.NewReader(data)), nil
	})
	if err != nil {
		return nil, err
	}
	c.archiveResponsesSent.Add(1)
	transferred = true
	return &batchResponse{data: output.Bytes(), done: make(chan struct{}), release: c.archiveServeMut.Unlock}, nil
}

func (c wireFormatConnection) requestArchive(ctx context.Context, reqs []*Request) ([]byte, error) {
	converted := make([]*Request, len(reqs))
	for i, req := range reqs {
		if req == nil {
			return nil, ErrInvalid
		}
		r := *req
		r.Hash = bytes.Clone(req.Hash)
		r.Name = norm.NFC.String(filepath.ToSlash(r.Name))
		converted[i] = &r
	}
	return RequestArchive(ctx, c.Connection, converted)
}

func (e encryptedConnection) requestArchive(ctx context.Context, reqs []*Request) ([]byte, error) {
	for _, req := range reqs {
		if req == nil {
			return nil, ErrInvalid
		}
		if _, encrypted := e.folderKeys.get(req.Folder); encrypted {
			return nil, ErrArchiveUnsupported
		}
	}
	return e.conn.requestArchive(ctx, reqs)
}

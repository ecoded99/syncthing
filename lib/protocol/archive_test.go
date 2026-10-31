// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package protocol

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/gen/bep"
	"github.com/syncthing/syncthing/lib/testutil"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func archiveFixture(name string) *Request {
	h := sha256.Sum256([]byte("data"))
	return &Request{Folder: "fixture", Name: name, Size: 4, Hash: h[:]}
}

func TestArchiveRequestFraming(t *testing.T) {
	reqs := []*Request{archiveFixture("a"), archiveFixture("nested/b")}
	b, total, err := encodeArchiveRequests(reqs)
	if err != nil || total != 8 {
		t.Fatal(total, err)
	}
	got, total, err := decodeArchiveRequests(b)
	if err != nil || total != 8 || len(got) != 2 || got[1].Name != "nested/b" {
		t.Fatal(got, total, err)
	}
	for i := range len(b) {
		if _, _, err := decodeArchiveRequests(b[:i]); err == nil {
			t.Fatalf("accepted prefix %d", i)
		}
	}
	if _, _, err := decodeArchiveRequests(append(bytes.Clone(b), 0)); err == nil {
		t.Fatal("trailing frame accepted")
	}
	for _, name := range []string{"../escape", "/root", "a/../b", "", `a\b`, "a//b", "a/./b", "."} {
		if _, _, err := encodeArchiveRequests([]*Request{archiveFixture(name)}); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	for _, mutate := range []func(*Request){
		func(r *Request) { r.Folder = "other" }, func(r *Request) { r.Offset = 1 }, func(r *Request) { r.BlockNo = 1 },
		func(r *Request) { r.FromTemporary = true }, func(r *Request) { r.Size = 0 }, func(r *Request) { r.Size = 65537 },
		func(r *Request) { r.Hash = make([]byte, 48) }, func(r *Request) { r.batchPayload = []byte{1} },
	} {
		bad := archiveFixture("bad")
		mutate(bad)
		if _, _, err := encodeArchiveRequests([]*Request{archiveFixture("a"), bad}); err == nil {
			t.Fatal("invalid member accepted")
		}
	}
	if _, _, err := encodeArchiveRequests([]*Request{archiveFixture("A"), archiveFixture("a")}); err == nil {
		t.Fatal("case alias accepted")
	}
	if _, _, err := encodeArchiveRequests([]*Request{nil}); err == nil {
		t.Fatal("nil member accepted")
	}
	large := make([]*Request, 1024)
	for i := range large {
		large[i] = archiveFixture(fmt.Sprint(i))
		large[i].Size = 16384
	}
	if _, total, err := encodeArchiveRequests(large); err != nil || total != 16<<20 {
		t.Fatal(total, err)
	}
	large[0].Size++
	if _, _, err := encodeArchiveRequests(large); err == nil {
		t.Fatal("payload bound ignored")
	}
	large = append(large, archiveFixture("extra"))
	if _, _, err := encodeArchiveRequests(large); err == nil {
		t.Fatal("count bound ignored")
	}
}

func TestArchiveExtensionRouting(t *testing.T) {
	w := (&ClusterConfig{}).toWire()
	if !bytesEqualVersion(getExtension(w, archiveCapabilityField)) || !bytesEqualVersion(getExtension(w, batchCapabilityField)) {
		t.Fatal("independent capabilities missing")
	}
	if getExtension(new(bep.ClusterConfig), archiveCapabilityField) != nil {
		t.Fatal("stock capability")
	}
	setExtension(w, archiveCapabilityField, []byte{1})
	if bytesEqualVersion(getExtension(w, archiveCapabilityField)) {
		t.Fatal("duplicate capability accepted")
	}
	for _, extension := range [][]byte{
		protowire.AppendBytes(protowire.AppendTag(nil, archiveRequestField, protowire.BytesType), nil),
		protowire.AppendVarint(protowire.AppendTag(nil, archiveRequestField, protowire.VarintType), 1),
	} {
		r := archiveFixture(archiveName).toWire()
		r.ProtoReflect().SetUnknown(extension)
		if !requestFromWire(r).archivePresent {
			t.Fatal("malformed extension fell through")
		}
	}
	b, _, _ := encodeArchiveRequests([]*Request{archiveFixture("a")})
	r := archiveFixture(archiveName).toWire()
	setExtension(r, archiveRequestField, b)
	setExtension(r, batchRequestField, []byte{1})
	if !requestFromWire(r).archiveMixed {
		t.Fatal("mixed extensions not identified")
	}
	member := archiveFixture("a").toWire()
	setExtension(member, archiveRequestField, []byte{1})
	frame, _ := proto.Marshal(member)
	bad := append([]byte{'S', 'T', 'A', '1', 1}, byte(len(frame)))
	bad = append(bad, frame...)
	if _, _, err := decodeArchiveRequests(bad); err == nil {
		t.Fatal("nested extension accepted")
	}
}

type archiveTestModel struct {
	*TestModel
	calls  atomic.Int64
	active atomic.Int64
	closed atomic.Int64
	bad    atomic.Bool
}

type archiveTestResponse struct {
	data  []byte
	model *archiveTestModel
	once  sync.Once
}

func (r *archiveTestResponse) Data() []byte { return r.data }
func (r *archiveTestResponse) Wait()        {}
func (r *archiveTestResponse) Close() {
	r.once.Do(func() { r.model.active.Add(-1); r.model.closed.Add(1); clear(r.data) })
}
func (m *archiveTestModel) Request(_ Connection, r *Request) (RequestResponse, error) {
	m.calls.Add(1)
	if m.active.Add(1) != 1 {
		m.active.Add(-1)
		return nil, errors.New("previous normal response permit retained")
	}
	response := &archiveTestResponse{data: []byte("data"), model: m}
	if r.Folder != "fixture" || filepath.ToSlash(r.Name) == "missing" {
		return response, ErrNoSuchFile
	}
	if m.bad.Swap(false) {
		response.data = []byte("evil")
	}
	return response, nil
}

func TestArchiveTransportAndImmediateClose(t *testing.T) {
	ar, aw := io.Pipe()
	br, bw := io.Pipe()
	model := &archiveTestModel{TestModel: newTestModel()}
	c0 := NewConnection(c0ID, ar, bw, testutil.NoopCloser{}, newTestModel(), new(mockedConnectionInfo), CompressionAlways, testKeyGen)
	c1 := NewConnection(c1ID, br, aw, testutil.NoopCloser{}, model, new(mockedConnectionInfo), CompressionAlways, testKeyGen)
	c0.Start()
	c1.Start()
	defer closeAndWait(c0, ar, bw)
	defer closeAndWait(c1, br, aw)
	c0.ClusterConfig(&ClusterConfig{}, nil)
	c1.ClusterConfig(&ClusterConfig{}, nil)
	raw0, raw1 := getRawConnection(c0), getRawConnection(c1)
	deadline := time.Now().Add(2 * time.Second)
	for (!raw0.peerArchive.Load() || !raw1.peerArchive.Load()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !raw0.peerArchive.Load() || !raw1.peerArchive.Load() {
		t.Fatal("capability negotiation failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := RequestArchive(ctx, c0, []*Request{archiveFixture("a"), archiveFixture(filepath.FromSlash("nested/b"))})
	if err != nil {
		t.Fatal(err)
	}
	z, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	contents := 0
	for _, file := range z.File {
		reader, e := file.Open()
		if e != nil {
			t.Fatal(e)
		}
		b, e := io.ReadAll(reader)
		reader.Close()
		if e != nil {
			t.Fatal(e)
		}
		if bytes.Equal(b, []byte("data")) {
			contents++
		}
	}
	if contents != 2 || model.calls.Load() != 2 || model.closed.Load() != 2 || model.active.Load() != 0 {
		t.Fatal("source checks/close/copy failed", contents, model.calls.Load(), model.closed.Load(), model.active.Load())
	}
	if raw0.Statistics().ArchiveRequestsSent != 1 || raw1.Statistics().ArchiveResponsesSent != 1 || !raw0.Statistics().PeerSupportsArchive {
		t.Fatal("archive counters missing")
	}
	before := model.calls.Load()
	raw0.peerArchive.Store(false)
	if _, err := RequestArchive(ctx, c0, []*Request{archiveFixture("unsupported")}); !errors.Is(err, ErrArchiveUnsupported) {
		t.Fatal(err)
	}
	if model.calls.Load() != before || raw0.Statistics().ArchiveRequestsSent != 1 {
		t.Fatal("unsupported sent request")
	}
	raw0.peerArchive.Store(true)
	if _, err := RequestArchive(ctx, c0, []*Request{archiveFixture("missing"), archiveFixture("never")}); err == nil {
		t.Fatal("member failure accepted")
	}
	if model.calls.Load() != before+1 || model.active.Load() != 0 {
		t.Fatal("failed archive leaked permit/continued")
	}
	model.bad.Store(true)
	if _, err := RequestArchive(ctx, c0, []*Request{archiveFixture("corrupt")}); err == nil {
		t.Fatal("source hash mismatch accepted")
	}
	if raw0.Statistics().ArchiveRequestsSent != 1 || raw1.Statistics().ArchiveResponsesSent != 1 {
		t.Fatal("failure incremented success count")
	}
	// The extension is distinguished by field presence, never by its name.
	// An existing user file with this name keeps ordinary request semantics.
	for _, capable := range []bool{false, true} {
		raw0.peerArchive.Store(capable)
		raw1.peerArchive.Store(capable)
		normalData, err := c0.Request(ctx, archiveFixture(archiveName))
		if err != nil || string(normalData) != "data" {
			t.Fatalf("reserved-looking ordinary filename changed behavior (capable=%v): %q %v", capable, normalData, err)
		}
	}
	if raw0.Statistics().ArchiveRequestsSent != 1 || raw1.Statistics().ArchiveResponsesSent != 1 {
		t.Fatal("ordinary request incremented archive counters")
	}
	// Client-side and serving-side folder-key rejection are both enforced.
	raw0.model.(encryptedModel).folderKeys.setPasswords(testKeyGen, map[string]string{"fixture": "secret"})
	if _, err := RequestArchive(ctx, c0, []*Request{archiveFixture("encrypted")}); !errors.Is(err, ErrArchiveUnsupported) {
		t.Fatal(err)
	}
	raw0.model.(encryptedModel).folderKeys.setPasswords(testKeyGen, nil)
	raw1.model.(encryptedModel).folderKeys.setPasswords(testKeyGen, map[string]string{"fixture": "secret"})
	before = model.calls.Load()
	if _, err := RequestArchive(ctx, c0, []*Request{archiveFixture("encrypted")}); err == nil {
		t.Fatal("sender accepted encrypted folder")
	}
	if model.calls.Load() != before {
		t.Fatal("encrypted archive reached model")
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err := RequestArchive(cancelled, c0, []*Request{archiveFixture("cancelled")}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestArchiveServerAdmission(t *testing.T) {
	raw := getRawConnection(NewConnection(c0ID, nil, nil, testutil.NoopCloser{}, newTestModel(), new(mockedConnectionInfo), CompressionNever, testKeyGen))
	raw.peerArchive.Store(true)
	b, total, _ := encodeArchiveRequests([]*Request{archiveFixture("a")})
	hash := sha256.Sum256(b)
	outer := &Request{Name: archiveName, Size: total, Hash: hash[:], archivePayload: b, archivePresent: true}
	raw.archiveServeMut.Lock()
	done := make(chan error, 1)
	go func() { _, err := raw.serveArchive(outer); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("busy accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("busy sender queued unbounded")
	}
	raw.archiveServeMut.Unlock()
	for _, mutate := range []func(*Request){func(r *Request) { r.archivePayload = nil }, func(r *Request) { r.Size++ }, func(r *Request) { r.Hash = []byte("bad") }, func(r *Request) { r.Name = "other" }, func(r *Request) { r.FromTemporary = true }, func(r *Request) { r.archiveMixed = true }} {
		r := *outer
		mutate(&r)
		if response, err := raw.serveArchive(&r); err == nil {
			response.Close()
			t.Fatal("malformed outer request accepted")
		}
	}
}

func FuzzArchiveRequestDecoder(f *testing.F) {
	b, _, _ := encodeArchiveRequests([]*Request{archiveFixture("a"), archiveFixture("b")})
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) {
		reqs, total, err := decodeArchiveRequests(b)
		if err == nil && (len(reqs) == 0 || len(reqs) > 1024 || total > 16<<20) {
			t.Fatal("decoder bounds")
		}
	})
}

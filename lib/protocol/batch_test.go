// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package protocol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/syncthing/syncthing/internal/gen/bep"
	"github.com/syncthing/syncthing/lib/testutil"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func batchFixture(name string) *Request {
	h := sha256.Sum256([]byte("data"))
	return &Request{Folder: "fixture", Name: name, Size: 4, Hash: h[:], BatchEligible: true}
}

func TestBatchFraming(t *testing.T) {
	reqs := []*Request{batchFixture("a"), batchFixture("nested/b")}
	b, total, err := encodeBatchRequests(reqs)
	if err != nil || total != 8 {
		t.Fatal(total, err)
	}
	got, n, err := decodeBatchRequests(b)
	if err != nil || n != 8 || len(got) != 2 || got[1].Name != "nested/b" {
		t.Fatal(got, n, err)
	}
	for i := 0; i < len(b); i++ {
		if _, _, err := decodeBatchRequests(b[:i]); err == nil {
			t.Fatalf("accepted truncated frame %d", i)
		}
	}
	if _, _, err := decodeBatchRequests(append(b, 0)); err == nil {
		t.Fatal("accepted trailing bytes")
	}
	for _, name := range []string{"../escape", "/absolute", "a/../b", ""} {
		reqs[0] = batchFixture(name)
		b, _, _ := encodeBatchRequests(reqs)
		if _, _, err := decodeBatchRequests(b); err == nil {
			t.Fatalf("accepted %q", name)
		}
	}
	tooMany := make([]*Request, batchMaxCount+1)
	for i := range tooMany {
		tooMany[i] = batchFixture("a")
	}
	if _, _, err := encodeBatchRequests(tooMany); err == nil {
		t.Fatal("accepted oversized batch")
	}
	encrypted := batchFixture("a")
	encrypted.Hash = make([]byte, 48)
	if batchable(encrypted) {
		t.Fatal("encrypted request batchable")
	}
}

func TestBatchResponsesRejectCorruption(t *testing.T) {
	reqs := []*Request{batchFixture("a"), batchFixture("b")}
	good := []byte{'S', 'T', 'R', '1', 2, 0, 4, 'd', 'a', 't', 'a', 0, 4, 'd', 'a', 't', 'a'}
	if _, err := decodeBatchResults(good, reqs); err != nil {
		t.Fatal(err)
	}
	for i := range good {
		if _, err := decodeBatchResults(good[:i], reqs); err == nil {
			t.Fatalf("accepted truncated response %d", i)
		}
	}
	for _, bad := range [][]byte{append(bytes.Clone(good), 0), {'S', 'T', 'R', '1', 2, 99, 0}, {'S', 'T', 'R', '1', 2, 0, 3, 'a', 'b', 'c'}} {
		if _, err := decodeBatchResults(bad, reqs); err == nil {
			t.Fatal("accepted invalid response")
		}
	}
}

func TestBatchNegotiation(t *testing.T) {
	if getExtension(&bep.ClusterConfig{}, batchCapabilityField) != nil {
		t.Fatal("stock peer advertises batching")
	}
	if !bytesEqualVersion(getExtension((&ClusterConfig{}).toWire(), batchCapabilityField)) {
		t.Fatal("fork capability missing")
	}
	if bytesEqualVersion([]byte{2}) {
		t.Fatal("unknown version accepted")
	}
}

type batchTestModel struct {
	*TestModel
	calls   atomic.Int64
	badOnce atomic.Bool
}

func (m *batchTestModel) Request(_ Connection, r *Request) (RequestResponse, error) {
	m.calls.Add(1)
	if m.badOnce.Swap(false) {
		return &fakeRequestResponse{[]byte("bad")}, nil
	}
	if r.Name == "missing" {
		return nil, ErrNoSuchFile
	}
	if r.Folder != "fixture" {
		return nil, ErrInvalid
	}
	return &fakeRequestResponse{[]byte("data")}, nil
}

func TestBatchTransport(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			ar, aw := io.Pipe()
			br, bw := io.Pipe()
			m := &batchTestModel{TestModel: newTestModel()}
			c0 := getRawConnection(NewConnection(c0ID, ar, bw, testutil.NoopCloser{}, newTestModel(), new(mockedConnectionInfo), CompressionAlways, testKeyGen))
			c1 := getRawConnection(NewConnection(c1ID, br, aw, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionAlways, testKeyGen))
			c0.Start()
			c1.Start()
			defer closeAndWait(c0, ar, bw)
			defer closeAndWait(c1, br, aw)
			c0.ClusterConfig(&ClusterConfig{}, nil)
			c1.ClusterConfig(&ClusterConfig{}, nil)
			deadline := time.Now().Add(time.Second)
			for !c0.peerBatch.Load() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !c0.peerBatch.Load() {
				t.Fatal("negotiation failed")
			}
			c0.peerBatch.Store(enabled)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var wg sync.WaitGroup
			for i := range 64 {
				wg.Go(func() {
					name := fmt.Sprint(i)
					if i == 7 {
						name = "missing"
					}
					data, err := c0.Request(ctx, batchFixture(name))
					if name == "missing" {
						if !errors.Is(err, ErrNoSuchFile) {
							t.Errorf("lost per-file error: %v", err)
						}
					} else if err != nil || string(data) != "data" {
						t.Errorf("bad result: %s %v", data, err)
					}
				})
			}
			wg.Wait()
			if enabled {
				m.badOnce.Store(true)

				for range 32 {
					wg.Go(func() {
						data, err := c0.Request(ctx, batchFixture("fallback"))
						if err != nil || string(data) != "data" {
							t.Errorf("fallback failed: %q %v", data, err)
						}
					})
				}
				wg.Wait()
				if c0.peerBatch.Load() {
					t.Fatal("failed batch extension not disabled")
				}
			}
			expectedCalls := int64(64)
			if enabled {
				expectedCalls += 33
			}
			if m.calls.Load() != expectedCalls {
				t.Fatal("not all members authorized", m.calls.Load())
			}
			if enabled && c0.batchSent.Load() == 0 {
				t.Fatal("no batches sent")
			}
			if !enabled && c0.batchSent.Load() != 0 {
				t.Fatal("batched stock peer")
			}
			cancelled, stop := context.WithCancel(context.Background())
			stop()
			if _, err := c0.Request(cancelled, batchFixture("cancel")); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

func FuzzBatchRequestDecoder(f *testing.F) {
	b, _, _ := encodeBatchRequests([]*Request{batchFixture("a"), batchFixture("b")})
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) {
		reqs, total, err := decodeBatchRequests(b)
		if err == nil && (len(reqs) > batchMaxCount || total > batchMaxData) {
			t.Fatal("bounds violated")
		}
	})
}

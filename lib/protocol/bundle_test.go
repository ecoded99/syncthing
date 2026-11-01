// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package protocol

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/testutil"
)

type corruptBundleModel struct{ *batchTestModel }

func (m corruptBundleModel) Request(c Connection, r *Request) (RequestResponse, error) {
	if r.Name == "corrupt" {
		return &fakeRequestResponse{[]byte("xxxx")}, nil
	}
	return m.batchTestModel.Request(c, r)
}

func TestExplicitBundle(t *testing.T) {
	ar, aw := io.Pipe()
	br, bw := io.Pipe()
	m := corruptBundleModel{&batchTestModel{TestModel: newTestModel()}}
	c0 := NewConnection(c0ID, ar, bw, testutil.NoopCloser{}, newTestModel(), new(mockedConnectionInfo), CompressionAlways, testKeyGen)
	c1 := NewConnection(c1ID, br, aw, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionAlways, testKeyGen)
	r0, r1 := getRawConnection(c0), getRawConnection(c1)
	c0.Start()
	c1.Start()
	defer closeAndWait(r0, ar, bw)
	defer closeAndWait(r1, br, aw)
	requests := []Request{*batchFixture("a"), *batchFixture("nested/b"), *batchFixture("missing")}
	if _, err := RequestBundle(t.Context(), c0, requests); !errors.Is(err, ErrBundleUnavailable) {
		t.Fatal("unnegotiated peer accepted", err)
	}
	c0.ClusterConfig(&ClusterConfig{}, nil)
	c1.ClusterConfig(&ClusterConfig{}, nil)
	deadline := time.Now().Add(5 * time.Second)
	for !r0.peerBatch.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	results, err := RequestBundle(t.Context(), c0, requests)
	if err != nil || len(results) != 3 {
		t.Fatal(results, err)
	}
	if string(results[0].Data) != "data" || string(results[1].Data) != "data" || !errors.Is(results[2].Error, ErrNoSuchFile) || r0.batchSent.Load() != 1 {
		t.Fatal("lost bundle result or failed to send one explicit frame", results)
	}
	requests[1] = *batchFixture("corrupt")
	if results, err := RequestBundle(t.Context(), c0, requests); err == nil || results != nil {
		t.Fatal("exposed unverified bundle", results, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := RequestBundle(ctx, c0, requests); !errors.Is(err, context.Canceled) {
		t.Fatal("ignored cancellation")
	}
	// Folder encryption must never be bypassed by the explicit fast path.
	r0.peerBatch.Store(true)
	c0.ClusterConfig(&ClusterConfig{}, map[string]string{"fixture": "secret"})
	if _, err := RequestBundle(t.Context(), c0, requests); !errors.Is(err, ErrBundleUnavailable) {
		t.Fatal("encrypted folder bundled", err)
	}
}

func FuzzBatchResponseDecoder(f *testing.F) {
	f.Add([]byte{'S', 'T', 'R', '1', 2, 0, 4, 'd', 'a', 't', 'a', 0, 4, 'd', 'a', 't', 'a'})
	f.Fuzz(func(t *testing.T, data []byte) {
		requests := []*Request{batchFixture("a"), batchFixture("b")}
		results, err := decodeBatchResults(data, requests)
		if err != nil {
			return
		}
		if len(results) != len(requests) {
			t.Fatal("result count mismatch")
		}
		for i, result := range results {
			if result.err == nil && len(result.val) != requests[i].Size || result.err != nil && len(result.val) != 0 {
				t.Fatal("invalid successful decode")
			}
		}
	})
}

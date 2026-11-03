// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package protocol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type parallelReadModel struct {
	rawModel
	slots   chan struct{}
	entered chan struct{}
	gate    chan struct{}
	active  atomic.Int32
	peak    atomic.Int32
	closed  atomic.Int32
}

func (m *parallelReadModel) Request(r *Request) (RequestResponse, error) {
	m.slots <- struct{}{}
	active := m.active.Add(1)
	for peak := m.peak.Load(); active > peak; peak = m.peak.Load() {
		if m.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	m.entered <- struct{}{}
	<-m.gate
	if r.Name == "missing" {
		m.active.Add(-1)
		<-m.slots
		return nil, ErrNoSuchFile
	}
	return &recycledBatchResponse{data: []byte(r.Name), model: m}, nil
}

type recycledBatchResponse struct {
	data  []byte
	model *parallelReadModel
}

func (r *recycledBatchResponse) Data() []byte { return r.data }
func (r *recycledBatchResponse) Wait()        {}
func (r *recycledBatchResponse) Close() {
	clear(r.data) // Simulate immediate reuse of a pooled response buffer.
	r.model.closed.Add(1)
	r.model.active.Add(-1)
	<-r.model.slots
}

func TestParallelBundleReads(t *testing.T) {
	for _, capacity := range []int{1, batchWorkers} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			m := &parallelReadModel{slots: make(chan struct{}, capacity), entered: make(chan struct{}, batchMaxCount), gate: make(chan struct{})}
			c := &rawConnection{model: m, closed: make(chan struct{})}
			reqs := make([]*Request, batchMaxCount)
			for i := range reqs {
				name := fmt.Sprintf("%04d", i)
				reqs[i] = &Request{Name: name, Size: len(name)}
			}
			reqs[7].Name = "missing"
			done := make(chan []asyncResult, 1)
			go func() { done <- c.readBatchMembers(reqs, batchWorkers) }()
			// Require real overlap when scheduler and limiter capacity allow it.
			expected := min(capacity, runtime.GOMAXPROCS(0))
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			for range expected {
				select {
				case <-m.entered:
				case <-timer.C:
					close(m.gate)
					t.Fatal("workers did not overlap")
				}
			}
			close(m.gate)
			var results []asyncResult
			select {
			case results = <-done:
			case <-timer.C:
				t.Fatal("response limiter deadlock")
			}
			for i, result := range results {
				if i == 7 {
					if !errors.Is(result.err, ErrNoSuchFile) || len(result.val) != 0 {
						t.Fatal("lost member error", result)
					}
				} else if result.err != nil || string(result.val) != reqs[i].Name {
					t.Fatalf("out of order or recycled data at %d: %q %v", i, result.val, result.err)
				}
			}
			if m.active.Load() != 0 || m.closed.Load() != batchMaxCount-1 || m.peak.Load() > batchWorkers {
				t.Fatal("response leak or worker limit exceeded", m.active.Load(), m.closed.Load(), m.peak.Load())
			}
		})
	}
}

func bundleVerifyFixture(size int) ([]asyncResult, []*Request) {
	decoded := make([]asyncResult, batchMaxCount)
	reqs := make([]*Request, batchMaxCount)
	for i := range decoded {
		data := bytes.Repeat([]byte{byte(i)}, size)
		hash := sha256.Sum256(data)
		decoded[i].val = data
		reqs[i] = &Request{Size: size, Hash: hash[:]}
	}
	return decoded, reqs
}

func TestParallelBundleVerification(t *testing.T) {
	for _, workers := range []int{1, batchWorkers} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			decoded, reqs := bundleVerifyFixture(batchMaxFile)
			decoded[3] = asyncResult{err: ErrNoSuchFile}
			if err := verifyBundleMembers(t.Context(), decoded, reqs, workers); err != nil {
				t.Fatal(err)
			}
			for _, i := range []int{0, batchMaxCount / 2, batchMaxCount - 1} {
				decoded[i].val[0] ^= 1
				if err := verifyBundleMembers(t.Context(), decoded, reqs, workers); err == nil {
					t.Fatal("accepted corrupt member", i)
				}
				decoded[i].val[0] ^= 1
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := verifyBundleMembers(ctx, decoded, reqs, workers); !errors.Is(err, context.Canceled) {
				t.Fatal("ignored cancellation", err)
			}
		})
	}
}

func BenchmarkBundleVerification(b *testing.B) {
	for _, size := range []int{4, 4096, 16384, batchMaxFile} {
		decoded, reqs := bundleVerifyFixture(size)
		for _, workers := range []int{1, 2, batchWorkers} {
			b.Run(fmt.Sprintf("bytes=%d/workers=%d", size, workers), func(b *testing.B) {
				b.SetBytes(int64(size * len(reqs)))
				b.ReportAllocs()
				for b.Loop() {
					if err := verifyBundleMembers(b.Context(), decoded, reqs, workers); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

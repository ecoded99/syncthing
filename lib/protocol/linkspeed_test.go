// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package protocol

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/gen/bep"
	"github.com/syncthing/syncthing/lib/testutil"
)

func TestLinkSpeedNegotiationAndValidation(t *testing.T) {
	if getExtension(&bep.ClusterConfig{}, speedCapabilityField) != nil || !bytesEqualVersion(getExtension((&ClusterConfig{}).toWire(), speedCapabilityField)) {
		t.Fatal("invalid speed capability negotiation")
	}
	c := &rawConnection{}
	req := &Request{Name: speedDownloadName, Size: speedBlockSize}
	if err := c.prepareSpeedRequest(req); !errors.Is(err, ErrLinkSpeedUnsupported) {
		t.Fatal(err)
	}
	c.peerSpeed.Store(true)
	if err := c.prepareSpeedRequest(req); err != nil {
		t.Fatal(err)
	}
	req = requestFromWire(req.toWire())
	res, err := c.serveSpeedRequest(req)
	if err != nil || len(res.Data()) != speedBlockSize {
		t.Fatal("download failed", err)
	}
	res.Close()
	for _, mutate := range []func(*Request){
		func(r *Request) { r.Folder = "shared-folder" },
		func(r *Request) { r.Size++ },
		func(r *Request) { r.Offset = 1 },
		func(r *Request) { r.Hash[0] ^= 1 },
		func(r *Request) { r.speedPayload = nil },
		func(r *Request) { r.Name = "unexpected" },
		func(r *Request) { r.batchPayload = []byte{1} },
	} {
		bad := requestFromWire(req.toWire())
		bad.Hash = append([]byte(nil), bad.Hash...)
		mutate(bad)
		if _, err := c.serveSpeedRequest(bad); !errors.Is(err, ErrInvalid) {
			t.Fatal("accepted invalid speed request", err)
		}
	}
}

func TestLinkSpeedTransport(t *testing.T) {
	ar, aw := io.Pipe()
	br, bw := io.Pipe()
	c0 := NewConnection(c0ID, ar, bw, testutil.NoopCloser{}, newTestModel(), new(mockedConnectionInfo), CompressionAlways, testKeyGen)
	c1 := NewConnection(c1ID, br, aw, testutil.NoopCloser{}, newTestModel(), new(mockedConnectionInfo), CompressionAlways, testKeyGen)
	c0.Start()
	c1.Start()
	defer closeAndWait(c0, ar, bw)
	defer closeAndWait(c1, br, aw)
	c0.ClusterConfig(&ClusterConfig{}, nil)
	c1.ClusterConfig(&ClusterConfig{}, nil)
	deadline := time.Now().Add(time.Second)
	for (!getRawConnection(c0).peerSpeed.Load() || !getRawConnection(c1).peerSpeed.Load()) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	result, err := TestLinkSpeed(context.Background(), c0.Request)
	if err != nil {
		t.Fatal(err)
	}
	for _, measurement := range []LinkSpeedMeasurement{result.Download, result.Upload} {
		if measurement.Bytes <= 0 || measurement.Bytes > speedBlockSize*speedMaxBlocks || measurement.Seconds <= 0 || measurement.BytesPerSecond <= 0 {
			t.Fatal("invalid measurement", measurement)
		}
	}
	stats := c0.Statistics()
	// io.Pipe can unblock the response reader before countingWriter has
	// accounted for the final Write return. Wait for that bookkeeping; still
	// require every payload byte to have crossed the uncompressed transport.
	deadline = time.Now().Add(time.Second)
	for (stats.InBytesTotal < result.Download.Bytes || stats.OutBytesTotal < result.Upload.Bytes) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		stats = c0.Statistics()
	}
	if stats.InBytesTotal < result.Download.Bytes || stats.OutBytesTotal < result.Upload.Bytes {
		t.Fatal("compression inflated the measured throughput", stats, result)
	}
	getRawConnection(c0).peerSpeed.Store(false)
	if _, err := TestLinkSpeed(context.Background(), c0.Request); !errors.Is(err, ErrLinkSpeedUnsupported) {
		t.Fatal("stock peer must not receive speed requests", err)
	}
	getRawConnection(c0).peerSpeed.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := TestLinkSpeed(ctx, c0.Request); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
	getRawConnection(c0).awaitingMut.Lock()
	defer getRawConnection(c0).awaitingMut.Unlock()
	if len(getRawConnection(c0).awaiting) != 0 {
		t.Fatal("requests leaked after cancellation")
	}
}

func TestLinkSpeedRejectsShortResponse(t *testing.T) {
	_, err := TestLinkSpeed(context.Background(), func(context.Context, *Request) ([]byte, error) {
		return make([]byte, sha256.Size), nil
	})
	if err == nil {
		t.Fatal("accepted short download")
	}
}

func TestLinkSpeedPendingCancellation(t *testing.T) {
	c := &rawConnection{
		outbox:   make(chan asyncMessage, 4),
		closed:   make(chan struct{}),
		awaiting: make(map[int]chan asyncResult),
	}
	c.peerSpeed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := TestLinkSpeed(ctx, c.Request); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("pending requests did not time out", err)
	}
	if len(c.awaiting) != 0 {
		t.Fatal("pending requests leaked after timeout")
	}
	close(c.closed)
	if _, err := TestLinkSpeed(context.Background(), c.Request); !errors.Is(err, ErrClosed) {
		t.Fatal("disconnection ignored", err)
	}
}

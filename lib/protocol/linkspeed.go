// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package protocol

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

const (
	speedCapabilityField protowire.Number = 3002
	speedRequestField    protowire.Number = 3003
	speedDownloadName                     = ".st-link-speed-v1-download"
	speedUploadName                       = ".st-link-speed-v1-upload"
	speedBlockSize                        = 1 << 20
	speedMaxBlocks                        = 64
)

var ErrLinkSpeedUnsupported = errors.New("remote device does not support link speed tests; update both devices")

// Random data avoids reporting inflated speeds when BEP compression is enabled.
var speedData = sync.OnceValue(func() []byte {
	b := make([]byte, speedBlockSize)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return b
})

type LinkSpeedMeasurement struct {
	Bytes          int64   `json:"bytes"`
	Seconds        float64 `json:"seconds"`
	BytesPerSecond float64 `json:"bytesPerSecond"`
}

type LinkSpeedResult struct {
	Download LinkSpeedMeasurement `json:"download"`
	Upload   LinkSpeedMeasurement `json:"upload"`
}

// TestLinkSpeed measures application throughput over an existing authenticated
// connection. request must pass requests through without enabling batching.
// An empty folder and versioned names identify these local control requests;
// only the negotiated extension causes the receiver to treat them as a test.
// Each direction starts requests for up to five seconds or 64 MiB. In-flight
// requests may take longer; the complete test has a twenty-second timeout.
func TestLinkSpeed(ctx context.Context, request func(context.Context, *Request) ([]byte, error)) (LinkSpeedResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var result LinkSpeedResult
	var err error
	result.Download, err = measureLinkSpeed(ctx, request, speedDownloadName)
	if err != nil {
		return result, err
	}
	result.Upload, err = measureLinkSpeed(ctx, request, speedUploadName)
	return result, err
}

func measureLinkSpeed(ctx context.Context, request func(context.Context, *Request) ([]byte, error), name string) (LinkSpeedMeasurement, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var blocks, total atomic.Int64
	var firstErr error
	var once sync.Once
	start := time.Now()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for time.Since(start) < 5*time.Second && blocks.Add(1) <= speedMaxBlocks {
				data, err := request(ctx, &Request{Name: name, Size: speedBlockSize})
				expected := speedBlockSize
				if name == speedUploadName {
					expected = sha256.Size
				}
				if err == nil && len(data) != expected {
					err = errors.New("invalid link speed test response")
				}
				if err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
				total.Add(speedBlockSize)
			}
		})
	}
	wg.Wait()
	if firstErr != nil {
		return LinkSpeedMeasurement{}, firstErr
	}
	seconds := max(time.Since(start), time.Nanosecond).Seconds()
	return LinkSpeedMeasurement{total.Load(), seconds, float64(total.Load()) / seconds}, nil
}

func (c *rawConnection) prepareSpeedRequest(req *Request) error {
	if !c.peerSpeed.Load() {
		return ErrLinkSpeedUnsupported
	}
	if req.Size != speedBlockSize {
		return ErrInvalid
	}
	req.speedPayload = []byte{1}
	if req.Name == speedUploadName {
		req.speedPayload = append(req.speedPayload, speedData()...)
	}
	hash := sha256.Sum256(req.speedPayload)
	req.Hash = hash[:]
	return nil
}

func (c *rawConnection) serveSpeedRequest(req *Request) (RequestResponse, error) {
	hash := sha256.Sum256(req.speedPayload)
	if !c.peerSpeed.Load() || req.Folder != "" || req.Size != speedBlockSize || req.Offset != 0 || req.BlockNo != 0 || req.FromTemporary || len(req.batchPayload) != 0 || len(req.speedPayload) == 0 || !bytes.Equal(req.Hash, hash[:]) || req.speedPayload[0] != 1 {
		return nil, fmt.Errorf("invalid link speed test: %w", ErrInvalid)
	}
	switch {
	case req.Name == speedDownloadName && len(req.speedPayload) == 1:
		return &batchResponse{data: speedData(), done: make(chan struct{})}, nil
	case req.Name == speedUploadName && len(req.speedPayload) == 1+speedBlockSize:
		return &batchResponse{data: hash[:], done: make(chan struct{})}, nil
	default:
		return nil, ErrInvalid
	}
}

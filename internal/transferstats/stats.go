// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

// Package transferstats records process-lifetime payload bytes at pipeline
// boundaries. Polling clients compute live bytes/second from counter deltas,
// using one wall-clock interval for all stages (never summed worker times).
package transferstats

import "sync/atomic"

var Prepared, Decoded, Applied atomic.Int64

type Snapshot struct {
	PreparedBytes int64 `json:"preparedBytesTotal"`
	DecodedBytes  int64 `json:"decodedBytesTotal"`
	AppliedBytes  int64 `json:"appliedBytesTotal"`
}

func Current() Snapshot {
	return Snapshot{Prepared.Load(), Decoded.Load(), Applied.Load()}
}

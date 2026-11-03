// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package scanner

import (
	"testing"
	"time"
)

func TestByteCounterRecentRate(t *testing.T) {
	start := time.Unix(100, 0)
	c := &byteCounter{lastTime: start}
	c.Update(100 << 20)
	if rate := c.rateAt(start.Add(time.Second)); rate != 100<<20 {
		t.Fatal("first-second throughput lost", rate)
	}
	if rate := c.rateAt(start.Add(2 * time.Second)); rate != 0 {
		t.Fatal("idle interval retained stale throughput", rate)
	}
	c.Update(50 << 20)
	if rate := c.rateAt(start.Add(2500 * time.Millisecond)); rate != 100<<20 {
		t.Fatal("fractional interval measured incorrectly", rate)
	}
	if rate := c.rateAt(start); rate != 0 {
		t.Fatal("negative interval", rate)
	}
	if c.Total() != 150<<20 {
		t.Fatal("total changed", c.Total())
	}
}

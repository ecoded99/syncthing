// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package model

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBundleTunerGrowthAndCeiling(t *testing.T) {
	c := newBundleTuner(32)
	if c.limit != 16 {
		t.Fatal(c)
	}
	rate := 100.0
	for range 20 {
		c.observe(true, false, true, rate, 0.01)
		if c.trialRate > 0 {
			rate *= 1.2
		}
		if c.limit > 32 || c.limit < 16 {
			t.Fatal(c)
		}
	}
	if c.limit != 32 {
		t.Fatal(c)
	}
}
func TestBundleTunerPlateauPressureAndUnavailable(t *testing.T) {
	c := newBundleTuner(32)
	c.observe(true, false, true, 100, 0.01)
	c.observe(true, false, true, 100, 0.01)
	if c.limit != 20 {
		t.Fatal(c)
	}
	c.observe(true, false, true, 102, 0.01)
	if c.limit != 16 || c.cooldown == 0 {
		t.Fatal(c)
	}
	c.limit = 28
	c.observe(true, true, true, 100, 0.01)
	if c.limit != 24 {
		t.Fatal(c)
	}
	c.observe(false, false, true, 100, 0.01)
	if c.limit != 16 {
		t.Fatal(c)
	}
	c.limit = 24
	c.priorRate = 100
	c.priorLatency = 0.01
	c.observe(true, false, true, 50, 0.04)
	if c.limit != 20 {
		t.Fatal("filesystem latency regression ignored", c)
	}
}
func TestBundleTunerIdleDoesNotProbe(t *testing.T) {
	c := newBundleTuner(32)
	for range 30 {
		c.observe(true, false, false, 100, 0.01)
	}
	if c.limit != 16 {
		t.Fatal(c)
	}
}
func TestBundleAdmissionCancellationAndSingleRelease(t *testing.T) {
	a := newBundleAdmission(1)
	done, err := a.take(t.Context(), 123)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = a.take(ctx, 456); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	done(true)
	done(true)
	a.wait()
	if a.active != 0 || a.completed != 1 || a.bytes != 123 {
		t.Fatal(a)
	}
}
func TestBundleAdmissionWaitsForCompletion(t *testing.T) {
	a := newBundleAdmission(1)
	release, err := a.take(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan func(bool), 1)
	go func() {
		done, err := a.take(t.Context(), 1)
		if err == nil {
			acquired <- done
		}
	}()
	select {
	case <-acquired:
		t.Fatal("exceeded admission")
	case <-time.After(10 * time.Millisecond):
	}
	release(true)
	select {
	case done := <-acquired:
		done(true)
	case <-time.After(time.Second):
		t.Fatal("waiter stuck")
	}
	a.wait()
}

// Failed files release admission but must not look like successful throughput.
func TestBundleAdmissionFailureDoesNotCountBytes(t *testing.T) {
	a := newBundleAdmission(32)
	done, err := a.take(t.Context(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	done(false)
	a.wait()
	if a.bytes != 0 || a.active != 0 {
		t.Fatal(a)
	}
}

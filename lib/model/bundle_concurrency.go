// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
)

// bundleTuner probes four additional files at a time. Measurements cover the
// full create/write/flush/close/publish path, not just time spent writing bytes.
// CPU/memory are system-wide signals; completion latency is a filesystem-pressure
// proxy, not a measurement of interactive UI responsiveness.
type bundleTuner struct {
	limit, floor, ceiling   int
	healthy                 int
	trialRate, trialLatency float64
	priorRate, priorLatency float64
	cooldown                int
}

func newBundleTuner(maximum int) bundleTuner {
	maximum = max(1, min(32, maximum))
	return bundleTuner{limit: min(16, maximum), floor: min(16, maximum), ceiling: maximum}
}
func (t *bundleTuner) observe(valid, pressure, busy bool, rate, latency float64) {
	if valid && busy && rate > 0 {
		pressure = pressure || (t.priorLatency > 0 && latency > max(0.005, t.priorLatency*2) && rate < t.priorRate*0.9)
		if !pressure {
			if t.priorRate == 0 {
				t.priorRate, t.priorLatency = rate, latency
			} else {
				t.priorRate = 0.8*t.priorRate + 0.2*rate
				t.priorLatency = 0.8*t.priorLatency + 0.2*latency
			}
		}
	}
	if !valid {
		t.limit = t.floor
	}
	if pressure {
		t.limit = max(t.floor, t.limit-4)
		t.trialRate = 0
		t.healthy = 0
		t.cooldown = 15
		return
	}
	if !valid || !busy || rate <= 0 {
		// Never extrapolate a trial across idle time or failed telemetry.
		if t.trialRate > 0 {
			t.limit = max(t.floor, t.limit-4)
		}
		t.trialRate = 0
		t.healthy = 0
		return
	}
	if t.trialRate > 0 {
		if rate < t.trialRate*1.05 || latency > t.trialLatency*1.5 {
			t.limit = max(t.floor, t.limit-4)
			t.cooldown = 15
		}
		t.trialRate = 0
		t.healthy = 0
		return
	}
	if t.cooldown > 0 {
		t.cooldown--
		return
	}
	t.healthy++
	if t.healthy >= 2 && t.limit < t.ceiling {
		t.trialRate, t.trialLatency = rate, latency
		t.limit = min(t.ceiling, t.limit+4)
		t.healthy = 0
	}
}

type bundleAdmission struct {
	mut              sync.Mutex
	changed          chan struct{}
	active, peak     int
	bytes, completed int64
	latency          time.Duration
	tuner            bundleTuner
}

func newBundleAdmission(maximum int) *bundleAdmission {
	return &bundleAdmission{changed: make(chan struct{}), tuner: newBundleTuner(maximum)}
}
func (a *bundleAdmission) signal() { close(a.changed); a.changed = make(chan struct{}) }
func (a *bundleAdmission) take(ctx context.Context, size int64) (func(bool), error) {
	for {
		a.mut.Lock()
		if err := ctx.Err(); err != nil {
			a.mut.Unlock()
			return nil, err
		}
		if a.active < a.tuner.limit {
			a.active++
			a.peak = max(a.peak, a.active)
			a.mut.Unlock()
			start := time.Now()
			var once sync.Once
			return func(success bool) {
				once.Do(func() {
					a.mut.Lock()
					defer a.mut.Unlock()
					a.active--
					if success {
						a.bytes += size
					}
					a.completed++
					a.latency += time.Since(start)
					a.signal()
				})
			}, nil
		}
		changed := a.changed
		a.mut.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}
func (a *bundleAdmission) wait() {
	for {
		a.mut.Lock()
		active, changed := a.active, a.changed
		a.mut.Unlock()
		if active == 0 {
			return
		}
		<-changed
	}
}
func (a *bundleAdmission) monitor(ctx context.Context) {
	ticks := time.NewTicker(2 * time.Second)
	defer ticks.Stop()
	previous, err := cpu.TimesWithContext(ctx, false)
	validPrevious := err == nil && len(previous) == 1
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks.C:
		}
		current, errCPU := cpu.TimesWithContext(ctx, false)
		memory, errMemory := mem.VirtualMemoryWithContext(ctx)
		valid := validPrevious && errCPU == nil && len(current) == 1 && errMemory == nil
		pressure := false
		if valid {
			elapsed := current[0].Total() - previous[0].Total()
			idle := current[0].Idle - previous[0].Idle
			valid = elapsed > 0 && idle >= 0
			pressure = valid && (1-idle/elapsed >= 0.85 || memory.UsedPercent >= 90 || memory.Available < 512*1024*1024)
		}
		previous = current
		validPrevious = errCPU == nil && len(current) == 1
		a.mut.Lock()
		latency := 0.0
		if a.completed > 0 {
			latency = a.latency.Seconds() / float64(a.completed)
		}
		oldLimit := a.tuner.limit
		a.tuner.observe(valid, pressure, a.peak >= a.tuner.limit, float64(a.bytes)/2, latency)
		if a.tuner.limit != oldLimit {
			slog.InfoContext(ctx, "Adjusted bundled file concurrency", "workers", a.tuner.limit, "previousWorkers", oldLimit, "telemetryValid", valid, "systemPressure", pressure)
		}
		a.bytes, a.completed, a.latency = 0, 0, 0
		a.peak = a.active
		a.signal()
		a.mut.Unlock()
	}
}

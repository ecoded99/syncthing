// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"maps"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/events"
)

// Serializes scanner and model progress so a late hasher event cannot move the
// UI backwards from deletion checks to discovery, or leak into a later scan.
type scanProgressLogger struct {
	events.Logger
	mut                sync.Mutex
	data               map[string]any
	active             bool
	lastDeletionReport time.Time
}

func newScanProgressLogger(logger events.Logger, folder, reason string, paths int) *scanProgressLogger {
	if logger == nil {
		logger = events.NoopLogger
	}
	scope := "paths"
	if paths == 0 {
		scope = "full"
	}
	return &scanProgressLogger{Logger: logger, active: true, data: map[string]any{
		"folder": folder, "reason": reason, "scope": scope, "pathCount": paths,
		"phase": "discovering", "startedAt": time.Now(), "entriesVisited": int64(0),
		"filesToHash": int64(0), "deletionChecks": int64(0), "changes": 0,
	}}
}

func (p *scanProgressLogger) Log(t events.EventType, data any) {
	if t != events.FolderScanProgress {
		p.Logger.Log(t, data)
		return
	}
	p.mut.Lock()
	defer p.mut.Unlock()
	if !p.active {
		return
	}
	if phase := p.data["phase"]; phase != "discovering" && phase != "hashing" {
		return
	}
	for k, v := range data.(map[string]any) {
		p.data[k] = v
	}
	if complete, _ := p.data["discoveryComplete"].(bool); complete {
		p.data["phase"] = "hashing"
	}
	p.emitLocked()
}

func (p *scanProgressLogger) setPhase(phase string, changes int) {
	p.mut.Lock()
	defer p.mut.Unlock()
	p.data["phase"], p.data["changes"] = phase, changes
	if p.active {
		p.emitLocked()
	}
}

func (p *scanProgressLogger) deletionProgress(checked int64, force bool) {
	p.mut.Lock()
	defer p.mut.Unlock()
	if !p.active {
		return
	}
	now := time.Now()
	if !force && now.Sub(p.lastDeletionReport) < time.Second {
		return
	}
	p.lastDeletionReport = now
	p.data["deletionChecks"] = checked
	p.emitLocked()
}

func (p *scanProgressLogger) emitLocked() {
	p.Logger.Log(events.FolderScanProgress, maps.Clone(p.data))
}

func (p *scanProgressLogger) close() {
	p.mut.Lock()
	p.active = false
	p.mut.Unlock()
}

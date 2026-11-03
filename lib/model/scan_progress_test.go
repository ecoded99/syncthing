// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/syncthing/syncthing/internal/transferstats"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/semaphore"
)

type capturedScanLogger struct {
	events.Logger
	mut      sync.Mutex
	progress []map[string]any
}

func (l *capturedScanLogger) Log(t events.EventType, data any) {
	if t != events.FolderScanProgress {
		return
	}
	l.mut.Lock()
	l.progress = append(l.progress, data.(map[string]any))
	l.mut.Unlock()
}

func TestScanProgressPhases(t *testing.T) {
	l := &capturedScanLogger{Logger: events.NoopLogger}
	p := newScanProgressLogger(l, "folder", "watcher", 2)
	p.setPhase("discovering", 0)
	p.Log(events.FolderScanProgress, map[string]any{"entriesVisited": int64(123), "discoveryComplete": false})
	p.Log(events.FolderScanProgress, map[string]any{"discoveryComplete": true})
	p.setPhase("deletions", 3)
	p.deletionProgress(456, true)
	before := len(l.progress)
	// A late scanner callback must not replace a later model phase.
	p.Log(events.FolderScanProgress, map[string]any{"discoveryComplete": false})
	p.close()
	p.Log(events.FolderScanProgress, map[string]any{"entriesVisited": int64(999)})
	if len(l.progress) != before {
		t.Fatal("late progress leaked")
	}
	if l.progress[0]["phase"] != "discovering" || l.progress[0]["entriesVisited"] != int64(0) {
		t.Fatal("previous event was mutated", l.progress[0])
	}
	if l.progress[2]["phase"] != "hashing" {
		t.Fatal(l.progress[2])
	}
	last := l.progress[len(l.progress)-1]
	if last["scope"] != "paths" || last["reason"] != "watcher" || last["pathCount"] != 2 || last["deletionChecks"] != int64(456) || last["changes"] != 3 {
		t.Fatal(last)
	}
	if newScanProgressLogger(nil, "folder", "periodic", 0).data["scope"] != "full" {
		t.Fatal("full scan hidden")
	}
}

type partialScanWriter struct {
	n   int
	err error
}

func (w partialScanWriter) WriteAt([]byte, int64) (int, error) { return w.n, w.err }

func TestFileWriteStatsCountPartialWrites(t *testing.T) {
	f := &sendReceiveFolder{writeLimiter: semaphore.New(1)}
	for _, tc := range []struct {
		n         int
		err, want error
	}{
		{8, nil, nil}, {3, io.ErrUnexpectedEOF, io.ErrUnexpectedEOF}, {2, nil, io.ErrShortWrite}, {0, io.ErrClosedPipe, io.ErrClosedPipe},
	} {
		before := transferstats.Written.Load()
		err := f.limitedWriteAt(t.Context(), partialScanWriter{tc.n, tc.err}, make([]byte, 8), 0)
		if !errors.Is(err, tc.want) || transferstats.Written.Load()-before != int64(tc.n) {
			t.Fatal(tc, err, transferstats.Written.Load()-before)
		}
	}
}

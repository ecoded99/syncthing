// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package cloudbackup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
)

func TestPrimaryYieldClosesHandlesAndRetriesFreshSource(t *testing.T) {
	d := newFakeDrive(t)
	f := fixture(t)
	source := filepath.Join(f.Path, "file")
	write(t, source, "previous")
	r := run(t, d, f)
	write(t, source, strings.Repeat("incoming", 32768))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	oldThreshold, oldChunk := resumableThreshold, resumableChunk
	resumableThreshold, resumableChunk = 1024, 4096
	defer func() { resumableThreshold, resumableChunk = oldThreshold, oldChunk }()
	// Become busy once Drive has received part of the new content.
	var triggered, busy atomic.Bool
	paused := make(chan struct{}, 1)
	control := &primaryControl{interval: time.Nanosecond}
	control.busy = func() bool {
		if !triggered.Load() {
			d.mu.Lock()
			for _, s := range d.sessions {
				if len(s.buf) > 0 {
					triggered.Store(true)
					busy.Store(true)
				}
			}
			d.mu.Unlock()
		}
		return busy.Load()
	}
	control.state = func(waiting bool) {
		if waiting {
			select {
			case paused <- struct{}{}:
			default:
			}
		}
	}
	ctx = context.WithValue(ctx, primaryControlKey{}, control)
	done := make(chan error, 1)
	go func() { _, err := Backup(ctx, d.client(), f, testDevice, nil); done <- err }()
	select {
	case <-paused:
	case err := <-done:
		t.Fatalf("backup did not yield mid-upload: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if current(t, d, r, "file") != "previous" {
		t.Fatal("partial incoming file replaced the previous backup")
	}
	// On Windows these operations fail if the yielded backup still holds
	// source handles without delete sharing. Simulate a primary replacement.
	if err := os.Rename(source, source+".old"); err != nil {
		t.Fatal(err)
	}
	write(t, source, "primary replacement")
	if err := os.Remove(source + ".old"); err != nil {
		t.Fatal(err)
	}
	busy.Store(false)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if current(t, d, r, "file") != "primary replacement" {
		t.Fatal("retry did not use fresh source contents")
	}
	if v := d.versions(r.RootID, "file"); len(v) != 1 || v[0] != "previous" {
		t.Fatal("yield damaged version retention", v)
	}
}

func TestPrimaryWaitCancellationDoesNotContactDrive(t *testing.T) {
	d := newFakeDrive(t)
	f := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	ctx = context.WithValue(ctx, primaryControlKey{}, &primaryControl{busy: func() bool { return true }, interval: time.Millisecond})
	if _, err := Backup(ctx, d.client(), f, testDevice, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("paused backup did not honor cancellation", err)
	}
	if d.requests != 0 {
		t.Fatal("busy primary caused Drive requests", d.requests)
	}
}

func TestBackupConfigurationCancellationScope(t *testing.T) {
	f := fixture(t)
	from := config.Configuration{Folders: []config.FolderConfiguration{f}}
	to := from.Copy()
	to.Options.SetLowPriority = !from.Options.SetLowPriority
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(nil, nil, testDevice)
	s.activeCancel = cancel
	s.CommitConfiguration(from, to)
	if ctx.Err() != nil {
		t.Fatal("unrelated priority option cancelled backup")
	}
	to.Folders[0].CloudBackup.DriveFolderName = "changed"
	s.CommitConfiguration(from, to)
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("backup setting change did not cancel old run")
	}
}

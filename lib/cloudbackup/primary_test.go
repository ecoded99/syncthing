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
	f, all := fixture(t)
	source := filepath.Join(f.Path, "file")
	current := filepath.Join(f.CloudBackup.Path, "Current/file")
	write(t, source, "previous")
	run(t, f, all)
	write(t, source, strings.Repeat("incoming", 32768))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var triggered, busy atomic.Bool
	paused := make(chan struct{}, 1)
	control := &primaryControl{interval: time.Millisecond}
	control.busy = func() bool {
		if !triggered.Load() {
			stages, _ := filepath.Glob(filepath.Join(f.CloudBackup.Path, ".staging/*"))
			for _, stage := range stages {
				if info, err := os.Stat(stage); err == nil && info.Size() > 0 {
					triggered.Store(true)
					busy.Store(true)
				}
			}
		}
		return busy.Load()
	}
	// Always inspect the stage at read boundaries to make the interruption
	// deterministic even on fast disks. Waiting still sleeps one millisecond.
	control.interval = time.Nanosecond
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
	go func() { _, err := Backup(ctx, f, all, nil); done <- err }()
	select {
	case <-paused:
	case err := <-done:
		t.Fatalf("backup did not yield mid-copy: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if read(t, current) != "previous" {
		t.Fatal("partial incoming file replaced the previous backup")
	}
	stages, _ := filepath.Glob(filepath.Join(f.CloudBackup.Path, ".staging/*"))
	if len(stages) != 0 {
		t.Fatal("staging file retained while paused", stages)
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
	if read(t, current) != "primary replacement" {
		t.Fatal("retry did not use fresh source contents")
	}
	versions, _ := filepath.Glob(filepath.Join(f.CloudBackup.Path, "Versions/*/file"))
	if len(versions) != 1 || read(t, versions[0]) != "previous" {
		t.Fatal("yield damaged version retention", versions)
	}
}

func TestPrimaryWaitCancellationDoesNotCreateBackup(t *testing.T) {
	f, all := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	ctx = context.WithValue(ctx, primaryControlKey{}, &primaryControl{busy: func() bool { return true }, interval: time.Millisecond})
	if _, err := Backup(ctx, f, all, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("paused backup did not honor cancellation", err)
	}
	if _, err := os.Stat(f.CloudBackup.Path); !os.IsNotExist(err) {
		t.Fatal("busy primary caused backup filesystem work", err)
	}
}

func TestBackupConfigurationCancellationScope(t *testing.T) {
	f, _ := fixture(t)
	from := config.Configuration{Folders: []config.FolderConfiguration{f}}
	to := from.Copy()
	to.Options.SetLowPriority = !from.Options.SetLowPriority
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := New(nil)
	s.activeCancel = cancel
	s.CommitConfiguration(from, to)
	if ctx.Err() != nil {
		t.Fatal("unrelated priority option cancelled backup")
	}
	to.Folders[0].CloudBackup.Path += "-changed"
	s.CommitConfiguration(from, to)
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("destination change did not cancel old run")
	}
}

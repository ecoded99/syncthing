// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package cloudbackup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/syncthing/syncthing/lib/config"
)

var errPrimaryBusy = errors.New("primary synchronization is active")

type primaryControlKey struct{}

// A control belongs to one serial backup run. In-flight reads return promptly
// when primary work begins; the caller closes its file handles before waiting.
// Holding a source handle while asleep could prevent Windows sync renames.
type primaryControl struct {
	busy      func() bool
	state     func(bool)
	interval  time.Duration
	idleDelay time.Duration
	nextCheck time.Time
	lastBusy  time.Time
	paused    bool
	waiting   bool
}

func (c *primaryControl) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	if !now.Before(c.nextCheck) {
		c.nextCheck = now.Add(c.interval)
		if c.busy() {
			c.lastBusy = now
			c.paused = true
		} else {
			c.paused = !c.lastBusy.IsZero() && now.Sub(c.lastBusy) < c.idleDelay
		}
	}
	if c.paused {
		return errPrimaryBusy
	}
	return nil
}

func (c *primaryControl) wait(ctx context.Context) error {
	for {
		err := c.check(ctx)
		waiting := errors.Is(err, errPrimaryBusy)
		if waiting != c.waiting {
			c.waiting = waiting
			if c.state != nil {
				c.state(waiting)
			}
		}
		if !waiting {
			return err
		}
		timer := time.NewTimer(c.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func waitForPrimary(ctx context.Context) error {
	if c, ok := ctx.Value(primaryControlKey{}).(*primaryControl); ok {
		return c.wait(ctx)
	}
	return ctx.Err()
}

func checkPrimary(ctx context.Context) error {
	if c, ok := ctx.Value(primaryControlKey{}).(*primaryControl); ok {
		return c.check(ctx)
	}
	return ctx.Err()
}

func uploadWhenIdle(ctx context.Context, src *os.Root, tree *remoteTree, name string, versionDir func(context.Context, string) (string, error)) (bool, error) {
	versioned := false
	for {
		if err := waitForPrimary(ctx); err != nil {
			return false, err
		}
		// Directory enumeration may predate a pause. Re-read identity and
		// metadata after resuming instead of retrying stale source metadata.
		initial, err := src.Lstat(filepath.FromSlash(name))
		if err != nil {
			return false, err
		}
		if isLink(initial) || !initial.Mode().IsRegular() {
			return false, fmt.Errorf("source file type changed while backup yielded: %s", name)
		}
		changed, err := uploadFile(ctx, src, tree, name, initial, versionDir, &versioned)
		if !errors.Is(err, errPrimaryBusy) {
			return changed, err
		}
		// uploadFile has returned: the source handle is closed and the aborted
		// request left the existing Drive copy unchanged.
	}
}

// Changing unrelated options must not cancel a running backup and postpone it
// for a full interval. Changes to paths, ownership or backup settings still do.
func backupInputsChanged(from, to config.Configuration) bool {
	if len(from.Folders) != len(to.Folders) {
		return true
	}
	previous := make(map[string]config.FolderConfiguration, len(from.Folders))
	for _, f := range from.Folders {
		previous[f.ID] = f
	}
	for _, f := range to.Folders {
		old, ok := previous[f.ID]
		if !ok || old.Path != f.Path || old.FilesystemType != f.FilesystemType || old.MarkerName != f.MarkerName || old.Paused != f.Paused || old.CloudBackup != f.CloudBackup {
			return true
		}
	}
	return false
}

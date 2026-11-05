// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

// Opt-in read-only filesystem measurement against an existing folder and an
// independent, consistent SQLite backup. No service, peer, watcher or Explorer
// provider is started; only the backup database is opened for writing.
func BenchmarkExistingFolderScan(b *testing.B) {
	root := os.Getenv("STSCANBENCH")
	if root == "" {
		b.Skip("requires STSCANBENCH with snapshot.marker, folder.json and index-v2 backup")
	}
	if data, err := os.ReadFile(filepath.Join(root, "snapshot.marker")); err != nil || string(data) != "sqlite-backup" {
		b.Fatal("requires independent SQLite backup")
	}
	data, err := os.ReadFile(filepath.Join(root, "folder.json"))
	if err != nil {
		b.Fatal(err)
	}
	var folder config.FolderConfiguration
	if err := json.Unmarshal(data, &folder); err != nil {
		b.Fatal(err)
	}
	database, err := sqlite.Open(filepath.Join(root, "index-v2"))
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()
	filesystem := folder.Filesystem(fs.NewMtimeOption(database, folder.ID))
	matcher := ignore.New(filesystem)
	if err := matcher.Load(".stignore"); err != nil && !fs.IsNotExist(err) {
		b.Fatal(err)
	}
	cfg := scanner.Config{Folder: folder.ID, Filesystem: filesystem, CurrentFiler: cFiler{db: database, folder: folder.ID}, Matcher: matcher,
		IgnorePerms: folder.IgnorePerms, ModTimeWindow: folder.ModTimeWindow(),
		TempLifetime: time.Duration(1<<63 - 1), AutoNormalize: false, Hashers: 1, ProgressTickIntervalS: -1, EventLogger: events.NoopLogger}
	for b.Loop() {
		start := time.Now()
		for result := range scanner.WalkWithoutHashing(b.Context(), cfg) {
			b.Fatalf("folder changed during measurement: %s %v", result.Path, result.Err)
		}
		discovery := time.Since(start)
		files, errFn := database.AllLocalFilesWithPrefix(folder.ID, protocol.LocalDeviceID, "")
		var count int64
		for result, err := range scanDeletionChecks(b.Context(), files, errFn, filesystem, matcher, 4) {
			if err != nil {
				b.Fatal(err)
			}
			if result.check && result.deleted {
				b.Fatal("unexpected deleted file", result.file.Name)
			}
			count++
		}
		b.ReportMetric(discovery.Seconds(), "discovery-s")
		b.ReportMetric(time.Since(start).Seconds()-discovery.Seconds(), "deletion-s")
		b.ReportMetric(float64(count), "indexed-entries")
	}
}

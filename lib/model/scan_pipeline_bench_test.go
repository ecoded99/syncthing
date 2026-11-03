// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package model

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

type scanPointReference struct{ cf cFiler }

func (r scanPointReference) CurrentFile(name string) (protocol.FileInfo, bool) {
	return r.cf.CurrentFile(name)
}

// Includes native filesystem traversal and a real SQLite index, excludes only
// the model's subsequent deletion pass. Benchmarks each stage configuration
// against the same unchanged fixture; completion must emit no updates.
func BenchmarkScanMetadataPipeline(b *testing.B) {
	const count = 4096
	root := b.TempDir()
	database, err := sqlite.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer database.Close()
	for i := range count {
		dir := filepath.Join(root, fmt.Sprintf("dir-%03d", i/64))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("file-%05d.bin", i)), []byte("scan fixture"), 0o644); err != nil {
			b.Fatal(err)
		}
	}
	filesystem := fs.NewFilesystem(fs.FilesystemTypeBasic, root)
	cfg := scanner.Config{Folder: "scan-bench", Filesystem: filesystem, Hashers: 8, EventLogger: events.NoopLogger}
	var files []protocol.FileInfo
	for res := range scanner.Walk(context.Background(), cfg) {
		if res.Err != nil {
			b.Fatal(res.Err)
		}
		files = append(files, res.File)
	}
	if err := database.Update("scan-bench", protocol.LocalDeviceID, files); err != nil {
		b.Fatal(err)
	}
	cf := cFiler{db: database, folder: "scan-bench"}
	for _, workers := range []int{0, 1, 4, 8} {
		b.Run(fmt.Sprint(workers), func(b *testing.B) {
			cfg := cfg
			if workers == 0 {
				cfg.CurrentFiler = scanPointReference{cf}
			} else {
				cfg.CurrentFiler = cf
				cfg.MetadataWorkers = workers
			}
			for b.Loop() {
				for res := range scanner.Walk(b.Context(), cfg) {
					b.Fatalf("unchanged scan produced update: %v", res)
				}
			}
			b.ReportMetric(float64(count)*float64(b.N)/b.Elapsed().Seconds(), "files/s")
		})
	}
}

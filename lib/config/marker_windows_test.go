// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateMarkerReleasesRootDirectory(t *testing.T) {
	root := filepath.Join(t.TempDir(), "folder")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	folder := FolderConfiguration{ID: "marker-close", Path: root, FilesystemType: FilesystemTypeBasic, MarkerName: DefaultMarkerName}
	if err := folder.CreateMarker(); err != nil {
		t.Fatal(err)
	}
	if err := folder.RemoveMarker(); err != nil {
		t.Fatal(err)
	}
	// Windows refuses this while CreateMarker retains its read-directory handle.
	// No GC or cleanup retry should be required to release that handle.
	if err := os.Remove(root); err != nil {
		t.Fatalf("marker creation retained the root directory: %v", err)
	}
}

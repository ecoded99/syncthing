// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package model

import (
	"path/filepath"
	"testing"

	"github.com/syncthing/syncthing/lib/config"
)

func TestFolderPathChangeRequiresPause(t *testing.T) {
	for _, tc := range []struct {
		name       string
		fromPaused bool
		toPaused   bool
		changePath bool
		wantError  bool
	}{
		{"running relocation", false, false, true, true},
		{"resume and relocate", true, false, true, true},
		{"pause and relocate", false, true, true, false},
		{"paused relocation", true, true, true, false},
		{"resume same path", true, false, false, false},
		{"unchanged running", false, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			from := config.Configuration{Folders: []config.FolderConfiguration{{ID: "shared", Path: "old", Paused: tc.fromPaused}}}
			to := from.Copy()
			to.Folders[0].Paused = tc.toPaused
			if tc.changePath {
				to.Folders[0].Path = "new"
			}
			err := new(model).VerifyConfiguration(from, to)
			if (err != nil) != tc.wantError {
				t.Fatalf("VerifyConfiguration() = %v, want error: %v", err, tc.wantError)
			}
		})
	}
}

func TestFolderPathNormalizationDoesNotRequirePause(t *testing.T) {
	root := t.TempDir()
	from := config.Configuration{Folders: []config.FolderConfiguration{{ID: "shared", Path: root + string(filepath.Separator)}}}
	to := from.Copy()
	to.Folders[0].Path = root
	if err := new(model).VerifyConfiguration(from, to); err != nil {
		t.Fatal(err)
	}
}

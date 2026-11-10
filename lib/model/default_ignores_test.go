// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/syncthing/syncthing/lib/config"
)

func TestApplyDefaultIgnores(t *testing.T) {
	for _, tc := range []struct {
		name      string
		existing  *string
		lines     []string
		encrypted bool
		want      string
		wantFile  bool
	}{
		{name: "new folder", lines: []string{"(?d)**/node_modules", "(?d)**/build"}, want: "(?d)**/node_modules\n(?d)**/build\n", wantFile: true},
		{name: "existing ignores", existing: new("custom\n"), lines: []string{"default"}, want: "custom\n", wantFile: true},
		{name: "empty existing ignores", existing: new(""), lines: []string{"default"}, wantFile: true},
		{name: "no defaults"},
		{name: "encrypted", lines: []string{"default"}, encrypted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "folder")
			if tc.existing != nil {
				if err := os.Mkdir(root, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, ".stignore"), []byte(*tc.existing), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg := config.FolderConfiguration{Path: root, FilesystemType: config.FilesystemTypeBasic}
			if tc.encrypted {
				cfg.Type = config.FolderTypeReceiveEncrypted
			}
			if err := applyDefaultIgnores(cfg, tc.lines); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(root, ".stignore"))
			if !tc.wantFile && os.IsNotExist(err) {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !tc.wantFile {
				t.Fatal("unexpected ignore file")
			}
			if got := strings.ReplaceAll(string(data), "\r\n", "\n"); got != tc.want {
				t.Fatalf("ignore file = %q, want %q", got, tc.want)
			}
		})
	}
}

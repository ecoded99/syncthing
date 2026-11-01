// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package cloudbackup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMacOSSourceAliases(t *testing.T) {
	for _, alias := range []string{"/var", "/tmp", "/etc"} {
		t.Run(alias, func(t *testing.T) {
			if _, err := os.Readlink(alias); err != nil {
				t.Skip("system alias unavailable:", err)
			}
			got := canonicalSourcePath(alias + "/syncthing-source")
			if want := "/private" + alias + "/syncthing-source"; got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
}

func TestMacOSSourceRejectsUserLinks(t *testing.T) {
	f := fixture(t)
	physical, err := ValidateSource(f)
	if err != nil {
		t.Fatal("system temporary directory must be accepted:", err)
	}
	if strings.HasPrefix(physical, "/var/") {
		t.Fatal("source was not canonicalized:", physical)
	}
	alias := filepath.Join(t.TempDir(), "source-link")
	if err := os.Symlink(physical, alias); err != nil {
		t.Fatal(err)
	}
	f.Path = alias
	if _, err := ValidateSource(f); err == nil {
		t.Fatal("user-created source symlink accepted")
	}
	f.Path = filepath.Join(alias, "child")
	if _, err := ValidateSource(f); err == nil {
		t.Fatal("user-created ancestor symlink accepted")
	}
	f.Path = physical
	marker := filepath.Join(physical, ".stfolder")
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), marker); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateSource(f); err == nil {
		t.Fatal("source marker symlink accepted")
	}
}

// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package cloudbackup

import (
	"context"
	"encoding/json"
	"github.com/syncthing/syncthing/lib/config"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func fixture(t *testing.T) (config.FolderConfiguration, map[string]config.FolderConfiguration) {
	t.Helper()
	base := t.TempDir()
	src := filepath.Join(base, "source")
	os.MkdirAll(filepath.Join(src, ".stfolder"), 0700)
	f := config.FolderConfiguration{ID: "fixture", Path: src, FilesystemType: config.FilesystemTypeBasic, CloudBackup: config.CloudBackupConfiguration{Enabled: true, Path: filepath.Join(base, "backup")}}
	return f, map[string]config.FolderConfiguration{f.ID: f}
}
func write(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}
func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
func run(t *testing.T, f config.FolderConfiguration, all map[string]config.FolderConfiguration) Result {
	t.Helper()
	r, err := Backup(context.Background(), f, all, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestOneWayVersionsAndIgnores(t *testing.T) {
	f, all := fixture(t)
	dst := f.CloudBackup.Path
	write(t, filepath.Join(f.Path, ".stignore"), "node_modules\n.playwright-cli/traces\n")
	for _, name := range []string{"file", "nested/file", ".hidden", "node_modules/dep"} {
		write(t, filepath.Join(f.Path, name), "original")
	}
	write(t, filepath.Join(f.Path, ".playwright-cli/traces/test"), "excluded")
	stamp := time.Unix(1700000000, 123456700)
	os.Chtimes(filepath.Join(f.Path, "file"), stamp, stamp)
	run(t, f, all)
	for _, name := range []string{"file", "nested/file", ".hidden", "node_modules/dep"} {
		if read(t, filepath.Join(dst, "Current", name)) != "original" {
			t.Fatal(name)
		}
	}
	if _, err := os.Stat(filepath.Join(dst, "Current/.playwright-cli/traces/test")); !os.IsNotExist(err) {
		t.Fatal("trace copied", err)
	}
	info, _ := os.Stat(filepath.Join(dst, "Current/file"))
	if !info.ModTime().Equal(stamp) {
		t.Fatal("mtime not preserved", info.ModTime())
	}
	if r := run(t, f, all); r.Files != 0 {
		t.Fatal("unchanged files copied", r)
	}
	write(t, filepath.Join(f.Path, "file"), "changed")
	os.Remove(filepath.Join(f.Path, "nested/file"))
	run(t, f, all)
	if read(t, filepath.Join(dst, "Current/file")) != "changed" || read(t, filepath.Join(dst, "Current/nested/file")) != "original" {
		t.Fatal("one-way retention failed")
	}
	versions, err := filepath.Glob(filepath.Join(dst, "Versions/*/file"))
	if err != nil || len(versions) != 1 || read(t, versions[0]) != "original" {
		t.Fatal("old content not versioned", versions, err)
	}
	write(t, filepath.Join(dst, "Current/file"), "tampered")
	run(t, f, all)
	if read(t, filepath.Join(f.Path, "file")) != "changed" || read(t, filepath.Join(dst, "Current/file")) != "changed" {
		t.Fatal("backup wrote back to source")
	}
}
func TestBackupGuardsAndCancellation(t *testing.T) {
	f, all := fixture(t)
	write(t, filepath.Join(f.Path, "file"), "original")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Backup(ctx, f, all, nil); err == nil {
		t.Fatal("cancelled run succeeded")
	}
	if _, err := os.Stat(filepath.Join(f.CloudBackup.Path, "Current/file")); !os.IsNotExist(err) {
		t.Fatal("cancelled run published file")
	}
	original := f.CloudBackup.Path
	f.CloudBackup.Path = filepath.Join(f.Path, "nested")
	if _, _, err := ValidatePaths(f, all); err == nil {
		t.Fatal("overlap allowed")
	}
	f.CloudBackup.Path = original
	other := f
	other.ID = "other"
	other.Path = filepath.Dir(original)
	all[other.ID] = other
	if _, _, err := ValidatePaths(f, all); err == nil {
		t.Fatal("another sync folder overlap allowed")
	}
	delete(all, other.ID)
	os.RemoveAll(filepath.Join(f.Path, ".stfolder"))
	if _, _, err := ValidatePaths(f, all); err == nil {
		t.Fatal("missing marker allowed")
	}
}
func TestOwnershipMigration(t *testing.T) {
	f, all := fixture(t)
	write(t, filepath.Join(f.Path, "file"), "original")
	write(t, filepath.Join(f.CloudBackup.Path, "Current/retained"), "keep")
	if _, err := Backup(context.Background(), f, all, nil); err == nil {
		t.Fatal("unowned nonempty destination adopted")
	}
	legacy, _ := json.Marshal(map[string]string{"backup_id": "legacy-id", "source": f.Path})
	write(t, filepath.Join(f.CloudBackup.Path, ".backup-owner.json"), string(legacy))
	f.CloudBackup.OwnerID = "wrong"
	if _, err := Backup(context.Background(), f, all, nil); err == nil {
		t.Fatal("wrong owner adopted")
	}
	f.CloudBackup.OwnerID = "legacy-id"
	run(t, f, all)
	if read(t, filepath.Join(f.CloudBackup.Path, "Current/retained")) != "keep" {
		t.Fatal("migration deleted old file")
	}
	f.ID = "different-folder"
	if _, err := Backup(context.Background(), f, all, nil); err == nil {
		t.Fatal("ownership stolen")
	}
}
func TestBackupRejectsLinks(t *testing.T) {
	f, all := fixture(t)
	outside := filepath.Join(t.TempDir(), "sentinel")
	write(t, outside, "safe")
	if err := os.Symlink(outside, filepath.Join(f.Path, "link")); err != nil {
		t.Skip("symlink unavailable", err)
	}
	if _, err := Backup(context.Background(), f, all, nil); err == nil {
		t.Fatal("followed source symlink")
	}
	if read(t, outside) != "safe" {
		t.Fatal("outside file modified")
	}
}

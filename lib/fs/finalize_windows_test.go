// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package fs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func TestPrepareFileMetadataWindows(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode FileMode
		keep bool
	}{
		{"writable", 0o644, false},
		{"readonly", 0o444, false},
		{"ignoredPermissions", 0o444, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			db := make(mapStore)
			fsys := NewFilesystem(FilesystemTypeBasic, root, NewMtimeOption(db, "test", WithCaseInsensitivity(true)), &OptionDetectCaseConflicts{})
			file, err := fsys.OpenFile("output.tmp", OptCreate|OptExclusive|OptReadWrite, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			defer os.Chmod(filepath.Join(root, "output.tmp"), 0o600)
			data := bytes.Repeat([]byte("verified bytes"), 2000)
			if _, err = file.Write(data); err != nil {
				t.Fatal(err)
			}
			path, _ := windows.UTF16PtrFromString(filepath.Join(root, "output.tmp"))
			attrs := uint32(windows.FILE_ATTRIBUTE_HIDDEN | windows.FILE_ATTRIBUTE_SYSTEM | windows.FILE_ATTRIBUTE_ARCHIVE)
			if tc.keep {
				attrs |= windows.FILE_ATTRIBUTE_READONLY
			}
			if err = windows.SetFileAttributes(path, attrs); err != nil {
				t.Fatal(err)
			}
			stamp := time.Unix(1234567890, 123456789)
			mode := &tc.mode
			if tc.keep {
				mode = nil
			}
			ok, err := PrepareFileMetadata(file, mode, stamp)
			if err != nil || !ok {
				t.Fatalf("prepare: %v %v", ok, err)
			}
			if err = file.Sync(); err != nil {
				t.Fatal(err)
			}
			if err = file.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := windows.GetFileAttributes(path)
			if err != nil {
				t.Fatal(err)
			}
			wantReadonly := tc.keep || tc.mode&0o200 == 0
			if got&windows.FILE_ATTRIBUTE_HIDDEN != 0 || (got&windows.FILE_ATTRIBUTE_READONLY != 0) != wantReadonly || got&(windows.FILE_ATTRIBUTE_SYSTEM|windows.FILE_ATTRIBUTE_ARCHIVE) != windows.FILE_ATTRIBUTE_SYSTEM|windows.FILE_ATTRIBUTE_ARCHIVE {
				t.Fatalf("wrong attributes: %#x", got)
			}
			actual, err := os.Stat(filepath.Join(root, "output.tmp"))
			if err != nil {
				t.Fatal(err)
			}
			if !actual.ModTime().Equal(stamp.Truncate(100 * time.Nanosecond)) {
				t.Fatalf("physical mtime after flush/close: %v", actual.ModTime())
			}
			if ok, err = RecordMtime(fsys, "output.tmp", stamp); err != nil || !ok {
				t.Fatalf("record: %v %v", ok, err)
			}
			info, err := fsys.Lstat("output.tmp")
			if err != nil || !info.ModTime().Equal(stamp) {
				t.Fatalf("virtual mtime: %v %v", info, err)
			}
			if got, err := os.ReadFile(filepath.Join(root, "output.tmp")); err != nil || !bytes.Equal(got, data) {
				t.Fatal("content changed", err)
			}
		})
	}
}

func TestPrepareFileMetadataUnsupportedWindows(t *testing.T) {
	fsys := NewFilesystem(FilesystemTypeBasic, t.TempDir())
	f, err := fsys.Create("file")
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Now()
	if ok, err := PrepareFileMetadata(f, nil, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)); ok || err != nil {
		t.Fatalf("unsupported timestamp: %v %v", ok, err)
	}
	f.Close()
	if ok, err := PrepareFileMetadata(f, nil, stamp); ok || err == nil {
		t.Fatalf("closed file: %v %v", ok, err)
	}
	d, err := fsys.Open(".")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if ok, err := PrepareFileMetadata(d, nil, stamp); ok || err != nil {
		t.Fatalf("directory: %v %v", ok, err)
	}
	if unsafe.Sizeof(fileBasicInfo{}) != 40 {
		t.Fatal("incorrect Windows structure layout")
	}
}

func TestRecordMtimeMissingAndUnwrapped(t *testing.T) {
	plain := NewFilesystem(FilesystemTypeBasic, t.TempDir())
	if ok, err := RecordMtime(plain, "missing", time.Now()); ok || err != nil {
		t.Fatalf("unwrapped: %v %v", ok, err)
	}
	wrapped := NewFilesystem(FilesystemTypeBasic, t.TempDir(), NewMtimeOption(make(mapStore), "test"))
	if ok, err := RecordMtime(wrapped, "missing", time.Now()); ok || !IsNotExist(err) {
		t.Fatalf("missing: %v %v", ok, err)
	}
}

func TestRenameNewWindows(t *testing.T) {
	for _, detectCase := range []bool{false, true} {
		t.Run(fmt.Sprint(detectCase), func(t *testing.T) {
			root := t.TempDir()
			opts := []Option{NewMtimeOption(make(mapStore), "test")}
			if detectCase {
				opts = append(opts, &OptionDetectCaseConflicts{})
			}
			filesystem := NewFilesystem(FilesystemTypeBasic, root, opts...)
			if err := WriteFile(filesystem, "source.tmp", []byte("incoming"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := WriteFile(filesystem, "target.bin", []byte("local edit"), 0o644); err != nil {
				t.Fatal(err)
			}
			if used, err := RenameNew(filesystem, "source.tmp", "target.bin"); !used || err == nil {
				t.Fatalf("existing target: %v %v", used, err)
			}
			if got, err := os.ReadFile(filepath.Join(root, "target.bin")); err != nil || string(got) != "local edit" {
				t.Fatal("local edit overwritten", err)
			}
			if got, err := os.ReadFile(filepath.Join(root, "source.tmp")); err != nil || string(got) != "incoming" {
				t.Fatal("source lost", err)
			}
			if used, err := RenameNew(filesystem, "source.tmp", "TARGET.BIN"); !used || err == nil {
				t.Fatalf("case collision: %v %v", used, err)
			}
			if used, err := RenameNew(filesystem, "source.tmp", "source.tmp"); !used || err == nil {
				t.Fatalf("same name: %v %v", used, err)
			}
			if used, err := RenameNew(filesystem, "source.tmp", "new.bin"); !used || err != nil {
				t.Fatalf("new target: %v %v", used, err)
			}
			if _, err := filesystem.Lstat("source.tmp"); !IsNotExist(err) {
				t.Fatal("old name retained", err)
			}
			if got, err := os.ReadFile(filepath.Join(root, "new.bin")); err != nil || string(got) != "incoming" {
				t.Fatal("new content lost", err)
			}
			if _, err := filesystem.Lstat("new.bin"); err != nil {
				t.Fatal("case cache not updated", err)
			}
			if used, err := RenameNew(filesystem, "../outside.tmp", "../outside.bin"); !used || err == nil {
				t.Fatalf("traversal accepted: %v %v", used, err)
			}
		})
	}
}

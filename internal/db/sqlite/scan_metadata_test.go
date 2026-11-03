// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package sqlite

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestScanMetadataBatch(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	files := make([]protocol.FileInfo, 256)
	names := make([]string, len(files))
	for i := range files {
		names[i] = fmt.Sprintf("nested/file-%03d_%%.bin", i)
		files[i] = genFile(names[i], 2, 0)
	}
	files[17].SetDeleted(1)
	if err := s.Update(folderID, protocol.LocalDeviceID, files); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(folderID, protocol.DeviceID{42}, []protocol.FileInfo{genFile("remote-only", 1, 0)}); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetDeviceFilesForScan(folderID, protocol.LocalDeviceID, names)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(files) {
		t.Fatal(len(got))
	}
	for _, name := range names {
		want, ok, err := s.GetDeviceFile(folderID, protocol.LocalDeviceID, name)
		if err != nil || !ok {
			t.Fatal(name, err)
		}
		want.Blocks = nil
		if !reflect.DeepEqual(got[osutil.NativeFilename(name)], want) {
			t.Fatalf("metadata changed for %s", name)
		}
	}
	got, err = s.GetDeviceFilesForScan(folderID, protocol.LocalDeviceID, []string{"missing", "remote-only", names[0], names[0]})
	if err != nil || len(got) != 1 {
		t.Fatal(got, err)
	}
	for _, names := range [][]string{nil, {}} {
		got, err = s.GetDeviceFilesForScan(folderID, protocol.LocalDeviceID, names)
		if err != nil || len(got) != 0 {
			t.Fatal(got, err)
		}
	}
	if _, err := s.GetDeviceFilesForScan(folderID, protocol.LocalDeviceID, make([]string, 257)); err == nil {
		t.Fatal("accepted unbounded batch")
	}
	got, err = s.GetDeviceFilesForScan("absent", protocol.LocalDeviceID, []string{"x"})
	if err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
}

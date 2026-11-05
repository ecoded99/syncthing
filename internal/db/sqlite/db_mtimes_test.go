// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMtimePairs(t *testing.T) {
	t.Parallel()

	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatal()
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t0 := time.Now().Truncate(time.Second)
	t1 := t0.Add(1234567890)

	// Set a pair
	if err := db.PutMtime("foo", "bar", t0, t1); err != nil {
		t.Fatal(err)
	}

	// Check it
	gt0, gt1 := db.GetMtime("foo", "bar")
	if !gt0.Equal(t0) || !gt1.Equal(t1) {
		t.Log(t0, gt0)
		t.Log(t1, gt1)
		t.Log("bad times")
	}

	// Delete it
	if err := db.DeleteMtime("foo", "bar"); err != nil {
		t.Fatal(err)
	}

	// Check it
	gt0, gt1 = db.GetMtime("foo", "bar")
	if !gt0.IsZero() || !gt1.IsZero() {
		t.Log(gt0, gt1)
		t.Log("bad times")
	}
}

func TestMtimeShortcutPersistence(t *testing.T) {
	path := t.TempDir()
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	folder, err := database.getFolderDB("mtime-test", true)
	if err != nil {
		t.Fatal(err)
	}
	if !folder.mtimesAbsent.Load() {
		t.Fatal("new folder has no timestamp overrides")
	}
	ondisk, virtual := time.Unix(1700000000, 0), time.Unix(1700000000, 12345)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Go(func() {
			for item := range 32 {
				name := fmt.Sprintf("%d-%d", worker, item)
				database.GetMtime("mtime-test", name)
				if err := database.PutMtime("mtime-test", name, ondisk, virtual); err != nil {
					t.Error(err)
					return
				}
				gotDisk, gotVirtual := database.GetMtime("mtime-test", name)
				if !gotDisk.Equal(ondisk) || !gotVirtual.Equal(virtual) {
					t.Error("completed override write was hidden")
				}
			}
		})
	}
	wg.Wait()
	if folder.mtimesAbsent.Load() {
		t.Fatal("override write did not invalidate shortcut")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	gotDisk, gotVirtual := database.GetMtime("mtime-test", "0-0")
	if !gotDisk.Equal(ondisk) || !gotVirtual.Equal(virtual) {
		t.Fatal("reopened overrides were hidden")
	}
	if err := database.DeleteMtime("mtime-test", "0-0"); err != nil {
		t.Fatal(err)
	}
	if a, b := database.GetMtime("mtime-test", "0-0"); !a.IsZero() || !b.IsZero() {
		t.Fatal("deleted override retained")
	}
}

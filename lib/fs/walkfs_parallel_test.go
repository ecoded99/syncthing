// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package fs

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

type observedWalkFS struct {
	Filesystem
	stat func(string) (FileInfo, error)
}

type orderedWalkFS struct{ Filesystem }

func (f orderedWalkFS) DirNames(name string) ([]string, error) {
	names, err := f.Filesystem.DirNames(name)
	slices.Sort(names)
	return names, err
}

func (f observedWalkFS) Lstat(name string) (FileInfo, error) { return f.stat(name) }

func TestWalkParallelBoundedAndEarlyStop(t *testing.T) {
	workers := min(4, runtime.GOMAXPROCS(0))
	if workers < 2 {
		t.Skip("requires two processors")
	}
	base := NewFilesystem(FilesystemTypeBasic, t.TempDir())
	for i := 0; i < 16; i++ {
		if err := WriteFile(base, fmt.Sprintf("file-%02d", i), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	var active, peak, calls atomic.Int32
	observer := observedWalkFS{Filesystem: base, stat: func(name string) (FileInfo, error) {
		if name != "." {
			calls.Add(1)
			n := active.Add(1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			defer active.Add(-1)
		}
		return base.Lstat(name)
	}}
	stop := errors.New("stop walking")
	done := make(chan error, 1)
	go func() {
		done <- WalkWithLstatWorkers(NewWalkFilesystem(observer), ".", func(name string, _ FileInfo, _ error) error {
			if name != "." {
				return stop
			}
			return nil
		}, 4)
	}()
	for range workers {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("metadata workers did not run concurrently")
		}
	}
	if calls.Load() != int32(workers) {
		close(release)
		t.Fatal("unbounded lookahead", calls.Load())
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, stop) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("walk did not join workers")
	}
	if active.Load() != 0 || peak.Load() > int32(workers) || calls.Load() != int32(workers) {
		t.Fatal("worker leak or excess reads", active.Load(), peak.Load(), calls.Load())
	}
}

func TestWalkParallelOrderAndFreshSiblings(t *testing.T) {
	for _, workers := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			filesystem := NewWalkFilesystem(orderedWalkFS{NewFilesystem(FilesystemTypeBasic, t.TempDir())})
			if err := filesystem.MkdirAll("a", 0o700); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{filepath.Join("a", "leaf"), "b", "c", "d"} {
				if err := WriteFile(filesystem, name, []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			var order []string
			err := WalkWithLstatWorkers(filesystem, ".", func(name string, info FileInfo, err error) error {
				order = append(order, name)
				if name == filepath.Join("a", "leaf") {
					if err := WriteFile(filesystem, "b", make([]byte, 77), 0o600); err != nil {
						t.Fatal(err)
					}
					if err := filesystem.Remove("c"); err != nil {
						t.Fatal(err)
					}
				}
				if name == "b" && (err != nil || info.Size() != 77) {
					t.Fatal("stale sibling metadata", info, err)
				}
				if name == "c" {
					if !IsNotExist(err) {
						t.Fatal("deleted sibling not rechecked", err)
					}
					return nil
				}
				return err
			}, workers)
			if err != nil {
				t.Fatal(err)
			}
			if want := []string{".", "a", filepath.Join("a", "leaf"), "b", "c", "d"}; !slices.Equal(order, want) {
				t.Fatal("callback order", order)
			}
		})
	}
}

func TestWalkParallelSkipAndSymlink(t *testing.T) {
	filesystem := NewFilesystem(FilesystemTypeFake, t.Name())
	if err := filesystem.MkdirAll("skip/sub", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := filesystem.CreateSymlink("skip", "link"); err != nil {
		t.Fatal(err)
	}
	var serial, parallel []string
	walk := func(out *[]string) WalkFunc {
		return func(name string, info FileInfo, err error) error {
			if err != nil {
				return err
			}
			*out = append(*out, name)
			if name == "skip" {
				return SkipDir
			}
			return nil
		}
	}
	if err := filesystem.Walk(".", walk(&serial)); err != nil {
		t.Fatal(err)
	}
	if err := WalkWithLstatWorkers(filesystem, ".", walk(&parallel), 4); err != nil {
		t.Fatal(err)
	}
	// The fake filesystem returns map-order directory listings.
	slices.Sort(serial)
	slices.Sort(parallel)
	if !slices.Equal(serial, parallel) {
		t.Fatal(serial, parallel)
	}
}

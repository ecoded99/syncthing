// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package scanner

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/d4l3k/messagediff"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
)

type batchTestCurrent struct {
	files fakeCurrentFiler
	calls atomic.Int32
	fail  bool
}

func (c *batchTestCurrent) CurrentFile(name string) (protocol.FileInfo, bool) {
	fi, ok := c.files[name]
	fi.Version = fi.Version.Copy()
	return fi, ok
}
func (c *batchTestCurrent) CurrentFiles(names []string) (map[string]protocol.FileInfo, error) {
	c.calls.Add(1)
	if c.fail {
		return nil, errors.New("lookup failed")
	}
	result := make(map[string]protocol.FileInfo, len(names))
	for _, name := range names {
		if fi, ok := c.CurrentFile(name); ok {
			result[name] = fi
		}
	}
	return result, nil
}

func TestMetadataScanEquivalent(t *testing.T) {
	filesystem := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16))
	for i := range 600 {
		name := fmt.Sprintf("file-%04d", i)
		fd, err := filesystem.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		fd.Write([]byte("old"))
		fd.Close()
	}
	current := &batchTestCurrent{files: make(fakeCurrentFiler)}
	for _, fi := range walkDir(filesystem, ".", nil, nil, 0) {
		current.files[fi.Name] = fi
	}
	for _, workers := range []int{1, 4, 8} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			cfg, cancel := testConfig()
			defer cancel()
			cfg.Filesystem = filesystem
			cfg.CurrentFiler = current
			cfg.MetadataWorkers = workers
			for res := range Walk(t.Context(), cfg) {
				t.Fatalf("unchanged file returned: %v", res)
			}
		})
	}
	if current.calls.Load() == 0 {
		t.Fatal("batch path unused")
	}
	fd, _ := filesystem.Create("file-0007")
	fd.Write([]byte("changed-content"))
	fd.Close()
	fd, _ = filesystem.Create("new-file")
	fd.Write([]byte("new"))
	fd.Close()
	filesystem.Remove("file-0022")
	matcher := ignore.New(filesystem)
	matcher.Parse(strings.NewReader("file-0008\n"), "test")
	// Hide the batch method for the serial reference, and copy mutable versions.
	reference := pointTestCurrent{current}
	want := walkDir(filesystem, ".", reference, matcher, 0)
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Filesystem = filesystem
	cfg.CurrentFiler = current
	cfg.Matcher = matcher
	cfg.ScanOwnership = true
	var got []protocol.FileInfo
	for res := range Walk(t.Context(), cfg) {
		if res.Err != nil {
			t.Fatal(res.Err)
		}
		got = append(got, res.File)
	}
	slices.SortFunc(got, compareByName)
	// Version counters use the current second; compare content/metadata and
	// conflict ancestry independently of the time spent between scans.
	for i := range want {
		for j := range want[i].Version.Counters {
			want[i].Version.Counters[j].Value = 0
		}
	}
	for i := range got {
		for j := range got[i].Version.Counters {
			got[i].Version.Counters[j].Value = 0
		}
	}
	if diff, equal := messagediff.PrettyDiff(want, got); !equal {
		t.Fatal(diff)
	}
}

type pointTestCurrent struct{ c *batchTestCurrent }

func (c pointTestCurrent) CurrentFile(name string) (protocol.FileInfo, bool) {
	return c.c.CurrentFile(name)
}

func TestMetadataLookupFailure(t *testing.T) {
	filesystem := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16))
	fd, _ := filesystem.Create("file")
	fd.Close()
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Filesystem = filesystem
	cfg.CurrentFiler = &batchTestCurrent{fail: true}
	count := 0
	for res := range Walk(t.Context(), cfg) {
		if res.Err == nil || res.Path != "file" {
			t.Fatal("lookup failure became a local update", res)
		}
		count++
	}
	if count != 1 {
		t.Fatal(count)
	}
}

type discoveryGateFS struct {
	fs.Filesystem
	gate    chan struct{}
	entered chan struct{}
}

func (f discoveryGateFS) Walk(root string, fn fs.WalkFunc) error {
	err := f.Filesystem.Walk(root, fn)
	close(f.entered)
	<-f.gate
	return err
}
func TestHashingOverlapsDiscovery(t *testing.T) {
	filesystem := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16))
	fd, _ := filesystem.Create(filepath.Join("first"))
	fd.Write([]byte("data"))
	fd.Close()
	gate := make(chan struct{})
	entered := make(chan struct{})
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Filesystem = discoveryGateFS{filesystem, gate, entered}
	cfg.CurrentFiler = &batchTestCurrent{}
	cfg.ProgressTickIntervalS = 1
	sub := cfg.EventLogger.Subscribe(events.FolderScanProgress)
	defer sub.Unsubscribe()
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	out := Walk(ctx, cfg)
	released := false
	defer func() {
		if !released {
			close(gate)
		}
	}()
	select {
	case res := <-out:
		if res.Err != nil || res.File.Name != "first" || len(res.File.Blocks) == 0 {
			t.Fatal(res)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hashing waited for discovery to finish")
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("discovery did not reach gate")
	}
	event, err := sub.Poll(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if event.Data.(map[string]any)["discoveryComplete"] != false {
		t.Fatal("reported premature completion", event.Data)
	}
	close(gate)
	released = true
	for range out {
	}
}

func TestMetadataScanCancellation(t *testing.T) {
	filesystem := fs.NewWalkFilesystem(&infiniteFS{fs.NewFilesystem(fs.FilesystemType("error"), "."), 100, 100, 1e6})
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Filesystem = filesystem
	cfg.CurrentFiler = &batchTestCurrent{}
	cfg.MetadataWorkers = 4
	ctx, stop := context.WithCancel(t.Context())
	out := Walk(ctx, cfg)
	<-out
	stop()
	done := make(chan struct{})
	go func() {
		for range out {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata pipeline failed to stop")
	}
}

type boundedMetadataCurrent struct {
	entered chan struct{}
	gate    chan struct{}
	active  atomic.Int32
	peak    atomic.Int32
}

func (c *boundedMetadataCurrent) CurrentFile(string) (protocol.FileInfo, bool) {
	return protocol.FileInfo{}, false
}
func (c *boundedMetadataCurrent) CurrentFiles(names []string) (map[string]protocol.FileInfo, error) {
	active := c.active.Add(1)
	for peak := c.peak.Load(); active > peak; peak = c.peak.Load() {
		if c.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	c.entered <- struct{}{}
	<-c.gate
	c.active.Add(-1)
	return nil, nil
}

type countedMetadataFS struct {
	fs.Filesystem
	visited *atomic.Int32
}

func (f countedMetadataFS) Walk(root string, fn fs.WalkFunc) error {
	return f.Filesystem.Walk(root, func(path string, info fs.FileInfo, err error) error { f.visited.Add(1); return fn(path, info, err) })
}
func TestMetadataWorkersBounded(t *testing.T) {
	filesystem := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16))
	for i := range 4096 {
		fd, err := filesystem.Create(fmt.Sprint(i))
		if err != nil {
			t.Fatal(err)
		}
		fd.Close()
	}
	current := &boundedMetadataCurrent{entered: make(chan struct{}, 64), gate: make(chan struct{})}
	var visited atomic.Int32
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Filesystem = countedMetadataFS{filesystem, &visited}
	cfg.CurrentFiler = current
	cfg.MetadataWorkers = 4
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	out := Walk(ctx, cfg)
	workers := min(4, runtime.GOMAXPROCS(0))
	released := false
	defer func() {
		if !released {
			close(current.gate)
		}
	}()
	for range workers {
		select {
		case <-current.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("metadata workers did not overlap")
		}
	}
	// Even with all consumers blocked, discovery must stop at bounded queues.
	if n := visited.Load(); n > int32((2*workers+3)*scanMetadataBatchSize+1) {
		t.Fatal("unbounded metadata discovery", n)
	}
	stop()
	close(current.gate)
	released = true
	done := make(chan struct{})
	go func() {
		for range out {
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked metadata workers did not drain")
	}
	if current.active.Load() != 0 || current.peak.Load() > int32(workers) {
		t.Fatal("metadata worker leak", current.active.Load(), current.peak.Load())
	}
}

func TestMetadataCacheOwnsVersions(t *testing.T) {
	cache := cachedCurrentFiles{"same": {Version: protocol.Vector{Counters: []protocol.Counter{{ID: 1, Value: 7}}}}}
	first, _ := cache.CurrentFile("same")
	first.Version = first.Version.Update(1)
	again, _ := cache.CurrentFile("same")
	if again.Version.Counters[0].Value != 7 {
		t.Fatal("scan mutated cached version", again.Version)
	}
}

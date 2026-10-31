// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package archivebatch

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testMember(name string, data []byte) Member {
	return Member{Name: name, Size: int64(len(data)), SHA256: sha256.Sum256(data), Modified: time.Unix(1712345678, 123456700).UTC()}
}

type trackedSource struct {
	io.Reader
	closed   *atomic.Int64
	closeErr error
}

func (s trackedSource) Close() error { s.closed.Add(1); return s.closeErr }
func packed(t *testing.T, members []Member, data map[string][]byte) []byte {
	t.Helper()
	var output bytes.Buffer
	var closed atomic.Int64
	err := Pack(t.Context(), &output, members, func(_ context.Context, m Member) (io.ReadCloser, error) {
		return trackedSource{bytes.NewReader(data[m.Name]), &closed, nil}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if closed.Load() != int64(len(members)) {
		t.Fatal("source not closed", closed.Load())
	}
	return output.Bytes()
}
func newStagePath(t *testing.T) string { t.Helper(); return filepath.Join(t.TempDir(), "fresh-stage") }
func absent(t *testing.T, name string) {
	t.Helper()
	if _, err := os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected absent %q: %v", name, err)
	}
}

func TestPackUnpackAndAdoption(t *testing.T) {
	data := map[string][]byte{"nested/source.go": []byte("package example\n"), "empty": {}, "large": bytes.Repeat([]byte{0x71}, MaxFileBytes)}
	members := []Member{testMember("nested/source.go", data["nested/source.go"]), testMember("empty", nil), testMember("large", data["large"])}
	archive := packed(t, members, data)
	stagePath := newStagePath(t)
	stage, err := Unpack(t.Context(), bytes.NewReader(archive), int64(len(archive)), members, stagePath)
	if err != nil {
		t.Fatal(err)
	}
	files := stage.Files()
	if len(files) != 3 {
		t.Fatal("missing staged files")
	}
	for i, file := range files {
		if file.Member != members[i] || filepath.Dir(file.Path) != stagePath || strings.Contains(filepath.Base(file.Path), "source.go") {
			t.Fatal("untrusted path used", file)
		}
		got, e := os.ReadFile(file.Path)
		if e != nil || !bytes.Equal(got, data[file.Member.Name]) {
			t.Fatal("bad staged contents", e)
		}
		info, e := os.Stat(file.Path)
		if e != nil {
			t.Fatal(e)
		}
		if !info.ModTime().Equal(file.ObservedModified) || absDuration(info.ModTime().Sub(file.Member.Modified)) > 2*time.Second {
			t.Fatal("trusted timestamp not applied/observed", info.ModTime(), file)
		}
	}
	files[0].Member.Name = "changed"
	if stage.Files()[0].Member.Name != members[0].Name {
		t.Fatal("Files exposes mutable state")
	}
	adopted := filepath.Join(t.TempDir(), "adopted")
	if err := os.Rename(stage.Files()[0].Path, adopted); err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	absent(t, stagePath)
	if err := stage.Close(); err != nil {
		t.Fatal("close not idempotent", err)
	}
	if _, err := os.Stat(adopted); err != nil {
		t.Fatal("adopted file removed", err)
	}
}

func TestPackRejectsChangedSourcesAndCloses(t *testing.T) {
	want := testMember("file", []byte("good"))
	for _, value := range []string{"bad", "evil", "toolong"} {
		t.Run(value, func(t *testing.T) {
			var closed atomic.Int64
			var output bytes.Buffer
			err := Pack(t.Context(), &output, []Member{want}, func(context.Context, Member) (io.ReadCloser, error) {
				return trackedSource{strings.NewReader(value), &closed, nil}, nil
			})
			if err == nil || closed.Load() != 1 {
				t.Fatal("changed source accepted or leaked", err, closed.Load())
			}
		})
	}
	t.Run("close-error", func(t *testing.T) {
		var closed atomic.Int64
		sentinel := errors.New("source close failed")
		err := Pack(t.Context(), io.Discard, []Member{want}, func(context.Context, Member) (io.ReadCloser, error) {
			return trackedSource{strings.NewReader("good"), &closed, sentinel}, nil
		})
		if !errors.Is(err, sentinel) || closed.Load() != 1 {
			t.Fatal(err)
		}
	})
	t.Run("open-error-with-handle", func(t *testing.T) {
		var closed atomic.Int64
		sentinel := errors.New("open failed")
		err := Pack(t.Context(), io.Discard, []Member{want}, func(context.Context, Member) (io.ReadCloser, error) {
			return trackedSource{strings.NewReader("good"), &closed, nil}, sentinel
		})
		if !errors.Is(err, sentinel) || closed.Load() != 1 {
			t.Fatal(err)
		}
	})
}

func TestMemberLimitsAndAliases(t *testing.T) {
	for _, name := range []string{"", ".", "../a", "a/../b", "a//b", "/root", "a\\b", "C:/x", "a.", "a ", "a/..", "a\x00b", "e\u0301", strings.Repeat("a", MaxNameBytes+1)} {
		t.Run(fmt.Sprintf("name-%q", name), func(t *testing.T) {
			if _, err := validateMembers([]Member{testMember(name, nil)}); err == nil {
				t.Fatal("unsafe name accepted")
			}
		})
	}
	for _, names := range [][]string{{"same", "same"}, {"Foo", "foo"}, {"É", "é"}, {"Straße", "STRASSE"}, {"a", "A/file"}} {
		members := []Member{testMember(names[0], nil), testMember(names[1], nil)}
		if _, err := validateMembers(members); err == nil {
			t.Fatal("aliases accepted", names)
		}
	}
	if _, err := validateMembers(make([]Member, MaxFiles+1)); err == nil {
		t.Fatal("member overflow")
	}
	large := testMember("large", nil)
	large.Size = MaxFileBytes + 1
	if _, err := validateMembers([]Member{large}); err == nil {
		t.Fatal("file overflow")
	}
	large.Size = -1
	if _, err := validateMembers([]Member{large}); err == nil {
		t.Fatal("negative size")
	}
	var members []Member
	for i := 0; i < MaxPayloadBytes/MaxFileBytes; i++ {
		m := large
		m.Name = fmt.Sprintf("file-%04d", i)
		m.Size = MaxFileBytes
		members = append(members, m)
	}
	if _, err := validateMembers(members); err != nil {
		t.Fatal("exact payload limit rejected", err)
	}
	m := large
	m.Name = "overflow"
	m.Size = 1
	members = append(members, m)
	if _, err := validateMembers(members); err == nil {
		t.Fatal("total expanded overflow")
	}
}

type zipEntry struct {
	name   string
	data   []byte
	mode   os.FileMode
	method uint16
}

func craftedZIP(t *testing.T, entries ...zipEntry) []byte {
	t.Helper()
	var output bytes.Buffer
	zw := zip.NewWriter(&output)
	for _, e := range entries {
		h := &zip.FileHeader{Name: e.name, Method: e.method, Modified: time.Unix(1000000000, 0)}
		h.SetMode(e.mode)
		w, err := zw.CreateHeader(h)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(e.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}
func TestInventoryRejectedBeforeAnyMaterialization(t *testing.T) {
	member := testMember("safe", []byte("data"))
	base := craftedZIP(t, zipEntry{"safe", []byte("data"), 0600, zip.Store})
	cases := map[string][]byte{
		"duplicate":         craftedZIP(t, zipEntry{"safe", []byte("data"), 0600, zip.Store}, zipEntry{"safe", []byte("data"), 0600, zip.Store}),
		"traversal":         craftedZIP(t, zipEntry{"../safe", []byte("data"), 0600, zip.Store}),
		"case-alias":        craftedZIP(t, zipEntry{"SAFE", []byte("data"), 0600, zip.Store}),
		"symlink":           craftedZIP(t, zipEntry{"safe", []byte("data"), os.ModeSymlink | 0777, zip.Store}),
		"expanded-mismatch": craftedZIP(t, zipEntry{"safe", []byte("longer"), 0600, zip.Store}),
		"truncated":         append([]byte(nil), base[:len(base)-1]...),
	}
	localMismatch := append([]byte(nil), base...)
	localMismatch[30] = 'X'
	cases["local-central-name"] = localMismatch
	hugeCount := append([]byte(nil), base...)
	binary.LittleEndian.PutUint16(hugeCount[len(hugeCount)-12:], 65535)
	cases["central-count"] = hugeCount
	encrypted := append([]byte(nil), base...)
	central := bytes.Index(encrypted, []byte{'P', 'K', 1, 2})
	encrypted[central+8] |= 1
	encrypted[6] |= 1
	cases["encryption"] = encrypted
	for name, archive := range cases {
		t.Run(name, func(t *testing.T) {
			stagePath := newStagePath(t)
			if stage, err := Unpack(t.Context(), bytes.NewReader(archive), int64(len(archive)), []Member{member}, stagePath); err == nil || stage != nil {
				t.Fatal("inventory accepted")
			}
			absent(t, stagePath)
			var calls atomic.Int64
			err := UnpackTo(t.Context(), bytes.NewReader(archive), int64(len(archive)), []Member{member}, func(context.Context, Member, io.Reader) error { calls.Add(1); return nil })
			if err == nil || calls.Load() != 0 {
				t.Fatal("callback before complete inventory validation", err, calls.Load())
			}
		})
	}
}

func TestContentCorruptionNeverReachesCallback(t *testing.T) {
	member := testMember("file", []byte("correct"))
	good := packed(t, []Member{member}, map[string][]byte{"file": []byte("correct")})
	zr, err := zip.NewReader(bytes.NewReader(good), int64(len(good)))
	if err != nil {
		t.Fatal(err)
	}
	offset, err := zr.File[0].DataOffset()
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), good...)
	corrupt[offset] ^= 0x20
	wrongHash := member
	wrongHash.SHA256 = sha256.Sum256([]byte("changed"))
	for _, tc := range []struct {
		name    string
		archive []byte
		member  Member
	}{{"crc", corrupt, member}, {"sha256", good, wrongHash}} {
		t.Run(tc.name, func(t *testing.T) {
			var called atomic.Int64
			err := UnpackTo(t.Context(), bytes.NewReader(tc.archive), int64(len(tc.archive)), []Member{tc.member}, func(context.Context, Member, io.Reader) error { called.Add(1); return nil })
			if err == nil || called.Load() != 0 {
				t.Fatal("corrupt content reached callback", err)
			}
			stagePath := newStagePath(t)
			if stage, err := Unpack(t.Context(), bytes.NewReader(tc.archive), int64(len(tc.archive)), []Member{tc.member}, stagePath); err == nil || stage != nil {
				t.Fatal("corrupt staged content accepted")
			}
			absent(t, stagePath)
		})
	}
}

func TestDeflateAndFalseExpandedSize(t *testing.T) {
	data := bytes.Repeat([]byte("x"), MaxFileBytes)
	member := testMember("compressed", data)
	archive := craftedZIP(t, zipEntry{"compressed", data, 0600, zip.Deflate})
	var seen int
	err := UnpackTo(t.Context(), bytes.NewReader(archive), int64(len(archive)), []Member{member}, func(_ context.Context, _ Member, r io.Reader) error { b, e := io.ReadAll(r); seen = len(b); return e })
	if err != nil || seen != len(data) {
		t.Fatal("deflate rejected", err, seen)
	}
	central := bytes.Index(archive, []byte{'P', 'K', 1, 2})
	descriptor := bytes.LastIndex(archive[:central], []byte{'P', 'K', 7, 8})
	if central < 0 || descriptor < 0 {
		t.Fatal("bad test ZIP")
	}
	binary.LittleEndian.PutUint32(archive[central+24:], 4)
	binary.LittleEndian.PutUint32(archive[descriptor+12:], 4)
	member.Size = 4
	member.SHA256 = sha256.Sum256([]byte("xxxx"))
	seen = 0
	err = UnpackTo(t.Context(), bytes.NewReader(archive), int64(len(archive)), []Member{member}, func(context.Context, Member, io.Reader) error { seen++; return nil })
	if err == nil || seen != 0 {
		t.Fatal("expanded stream exceeded trusted bound", err, seen)
	}
}

func TestUnpackExistingStageAndUnknownCleanup(t *testing.T) {
	member := testMember("file", []byte("data"))
	archive := packed(t, []Member{member}, map[string][]byte{"file": []byte("data")})
	stagePath := newStagePath(t)
	if err := os.Mkdir(stagePath, 0700); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(stagePath, "foreign")
	if err := os.WriteFile(foreign, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Unpack(t.Context(), bytes.NewReader(archive), int64(len(archive)), []Member{member}, stagePath); !errors.Is(err, os.ErrExist) {
		t.Fatal("existing stage accepted", err)
	}
	got, err := os.ReadFile(foreign)
	if err != nil || string(got) != "keep" {
		t.Fatal("existing contents altered", err)
	}
	stagePath = newStagePath(t)
	stage, err := Unpack(t.Context(), bytes.NewReader(archive), int64(len(archive)), []Member{member}, stagePath)
	if err != nil {
		t.Fatal(err)
	}
	foreign = filepath.Join(stagePath, "foreign")
	if err := os.WriteFile(foreign, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err == nil {
		t.Fatal("unknown file removed")
	}
	if _, err := os.Stat(stage.Files()[0].Path); err != nil {
		t.Fatal("owned files changed on unsafe cleanup", err)
	}
	if err := os.Remove(foreign); err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStageCloseRejectsReplacedIdentity(t *testing.T) {
	member := testMember("file", []byte("data"))
	archive := packed(t, []Member{member}, map[string][]byte{"file": []byte("data")})
	stagePath := newStagePath(t)
	stage, err := Unpack(t.Context(), bytes.NewReader(archive), int64(len(archive)), []Member{member}, stagePath)
	if err != nil {
		t.Fatal(err)
	}
	file := stage.Files()[0].Path
	moved := filepath.Join(t.TempDir(), "moved")
	if err := os.Rename(file, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err == nil {
		t.Fatal("replaced file removed")
	}
	got, err := os.ReadFile(file)
	if err != nil || string(got) != "foreign" {
		t.Fatal("foreign identity altered", err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
}

type injectedFile struct {
	*os.File
	syncErr, closeErr error
	syncs, closes     *atomic.Int64
	beforeSync        func()
}

func (f injectedFile) Sync() error {
	f.syncs.Add(1)
	if f.beforeSync != nil {
		f.beforeSync()
	}
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.File.Sync()
}
func (f injectedFile) Close() error { f.closes.Add(1); return errors.Join(f.File.Close(), f.closeErr) }
func TestStageSyncCloseAndTimestampFailureCleanup(t *testing.T) {
	member := testMember("file", []byte("data"))
	archive := packed(t, []Member{member}, map[string][]byte{"file": []byte("data")})
	for _, mode := range []string{"sync", "close", "mtime"} {
		t.Run(mode, func(t *testing.T) {
			var syncs, closes atomic.Int64
			sentinel := errors.New("injected " + mode)
			ops := defaultStageOps()
			timesSet := false
			originalTimes := ops.chtimes
			ops.chtimes = func(root *os.Root, name string, stamp time.Time) error {
				if mode == "mtime" {
					return sentinel
				}
				err := originalTimes(root, name, stamp)
				timesSet = err == nil
				return err
			}
			ops.createTemp = func(dir, pattern string) (stageWriter, error) {
				f, err := os.CreateTemp(dir, pattern)
				if err != nil {
					return nil, err
				}
				wrapped := injectedFile{File: f, syncs: &syncs, closes: &closes, beforeSync: func() {
					if !timesSet {
						t.Error("Sync preceded trusted timestamp")
					}
				}}
				if mode == "sync" {
					wrapped.syncErr = sentinel
				}
				if mode == "close" {
					wrapped.closeErr = sentinel
				}
				return wrapped, nil
			}
			stagePath := newStagePath(t)
			stage, err := unpack(t.Context(), bytes.NewReader(archive), int64(len(archive)), []Member{member}, stagePath, ops)
			if !errors.Is(err, sentinel) || stage != nil || closes.Load() != 1 {
				t.Fatal("injected failure accepted or handle not closed once", err, closes.Load())
			}
			absent(t, stagePath)
			if mode != "mtime" && syncs.Load() != 1 {
				t.Fatal("missing sync")
			}
		})
	}
}

func TestUnpackToWorkerBoundAndJoin(t *testing.T) {
	var members []Member
	data := make(map[string][]byte)
	for i := 0; i < 24; i++ {
		name := fmt.Sprintf("file-%02d", i)
		data[name] = []byte("data")
		members = append(members, testMember(name, data[name]))
	}
	archive := packed(t, members, data)
	started := make(chan struct{}, 24)
	release := make(chan struct{})
	done := make(chan error, 1)
	var active, peak, completed atomic.Int64
	var once sync.Once
	defer once.Do(func() { close(release) })
	go func() {
		done <- UnpackTo(t.Context(), bytes.NewReader(archive), int64(len(archive)), members, func(_ context.Context, _ Member, r io.Reader) error {
			_, err := io.Copy(io.Discard, r)
			if err != nil {
				return err
			}
			n := active.Add(1)
			defer active.Add(-1)
			for {
				old := peak.Load()
				if n <= old || peak.CompareAndSwap(old, n) {
					break
				}
			}
			started <- struct{}{}
			<-release
			completed.Add(1)
			return nil
		})
	}()
	for range 8 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("workers did not start")
		}
	}
	if active.Load() != 8 || peak.Load() != 8 {
		t.Fatal("unexpected worker concurrency", active.Load(), peak.Load())
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker join timeout")
	}
	if active.Load() != 0 || peak.Load() > 8 || completed.Load() != 24 {
		t.Fatal("work not joined or worker bound exceeded", active.Load(), peak.Load(), completed.Load())
	}
}

func TestUnpackToCancellationAndCallbackFailureJoin(t *testing.T) {
	member := testMember("file", []byte("data"))
	archive := packed(t, []Member{member}, map[string][]byte{"file": []byte("data")})
	t.Run("callback-error", func(t *testing.T) {
		sentinel := errors.New("callback failure")
		err := UnpackTo(t.Context(), bytes.NewReader(archive), int64(len(archive)), []Member{member}, func(_ context.Context, _ Member, r io.Reader) error { _, _ = io.Copy(io.Discard, r); return sentinel })
		if !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
	})
	t.Run("incomplete-consumption", func(t *testing.T) {
		if err := UnpackTo(t.Context(), bytes.NewReader(archive), int64(len(archive)), []Member{member}, func(context.Context, Member, io.Reader) error { return nil }); err == nil {
			t.Fatal("incomplete materializer accepted")
		}
	})
	t.Run("cancel-in-callback", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		active := false
		err := UnpackTo(ctx, bytes.NewReader(archive), int64(len(archive)), []Member{member}, func(ctx context.Context, _ Member, r io.Reader) error {
			active = true
			defer func() { active = false }()
			_, _ = io.Copy(io.Discard, r)
			cancel()
			<-ctx.Done()
			return ctx.Err()
		})
		if !errors.Is(err, context.Canceled) || active {
			t.Fatal("callback did not join", err, active)
		}
	})
	t.Run("cancel-before-inventory", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		stagePath := newStagePath(t)
		if _, err := Unpack(ctx, bytes.NewReader(archive), int64(len(archive)), []Member{member}, stagePath); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		absent(t, stagePath)
	})
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

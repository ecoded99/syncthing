// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package explorerstatus

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/model"
	"github.com/syncthing/syncthing/lib/protocol"
)

type fakeRoot struct {
	states map[string]State
}

func (r *fakeRoot) Set(name string, st State) error {
	r.states[filepath.ToSlash(name)] = st
	return nil
}

func (*fakeRoot) Close(bool) error { return nil }

type fakeModel struct {
	need   []string
	errors []string
}

func (m *fakeModel) NeedFolderFiles(_ string, page, perpage int) ([]protocol.FileInfo, []protocol.FileInfo, []protocol.FileInfo, error) {
	var rest []protocol.FileInfo
	for i := (page - 1) * perpage; i < len(m.need) && i < page*perpage; i++ {
		rest = append(rest, protocol.FileInfo{Name: filepath.FromSlash(m.need[i])})
	}
	return nil, nil, rest, nil
}

func (m *fakeModel) FolderErrors(string) ([]model.FileError, error) {
	var errs []model.FileError
	for _, name := range m.errors {
		errs = append(errs, model.FileError{Path: filepath.FromSlash(name), Err: "failed"})
	}
	return errs, nil
}

func setup(t *testing.T) (*folder, *fakeRoot) {
	t.Helper()
	dir := t.TempDir()
	for name, data := range map[string]string{
		"a.txt":                        "a",
		"sub/b.txt":                    "b",
		"sub/deep/c.txt":               "c",
		"skip.log":                     "ignored",
		".stignore":                    "*.log\n",
		".stfolder/marker":             "",
		"sub/~syncthing~b.txt.tmp":     "temp",
		".stversions/old~20260101.txt": "old",
	} {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	root := &fakeRoot{states: make(map[string]State)}
	cfg := config.FolderConfiguration{ID: "f", Path: dir, FilesystemType: config.FilesystemTypeBasic}
	return newFolder(cfg, root), root
}

func expect(t *testing.T, root *fakeRoot, want map[string]State) {
	t.Helper()
	for name, st := range want {
		if got, ok := root.states[name]; !ok || got != st {
			t.Errorf("%s: got %v (set: %v), want %v", name, got, ok, st)
		}
	}
}

func TestSweep(t *testing.T) {
	f, root := setup(t)
	if err := f.sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	expect(t, root, map[string]State{
		"a.txt":          InSync,
		"sub":            InSync,
		"sub/b.txt":      InSync,
		"sub/deep":       InSync,
		"sub/deep/c.txt": InSync,
		"skip.log":       Excluded,
	})
	for name := range root.states {
		if internal(filepath.FromSlash(name)) {
			t.Errorf("internal file %s was marked", name)
		}
	}
}

func TestRefreshMarksNeedsAndParents(t *testing.T) {
	f, root := setup(t)
	m := &fakeModel{need: []string{"sub/deep/c.txt"}, errors: []string{"a.txt"}}
	f.refresh(m)
	expect(t, root, map[string]State{
		"sub/deep/c.txt": Pending,
		"sub/deep":       Pending,
		"sub":            Pending,
		"a.txt":          Pending,
	})
	if _, ok := root.states["sub/b.txt"]; ok {
		t.Error("unrelated file was touched by refresh")
	}

	// A pulled file stays pending until the model stops needing it.
	f.updated([]string{filepath.FromSlash("sub/deep/c.txt")})
	expect(t, root, map[string]State{"sub/deep/c.txt": Pending})

	m.need, m.errors = nil, nil
	f.refresh(m)
	expect(t, root, map[string]State{
		"sub/deep/c.txt": InSync,
		"sub/deep":       InSync,
		"sub":            InSync,
		"a.txt":          InSync,
	})
}

func TestRefreshPages(t *testing.T) {
	f, root := setup(t)
	m := &fakeModel{}
	for i := 0; i < needPageSize+5; i++ {
		m.need = append(m.need, "many/"+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	m.need = append(m.need, "a.txt")
	f.refresh(m)
	expect(t, root, map[string]State{"a.txt": Pending, "many": Pending})
}

func TestUpdatedSkipsInternal(t *testing.T) {
	f, root := setup(t)
	f.updated([]string{".stfolder", filepath.FromSlash("sub/~syncthing~b.txt.tmp"), "a.txt"})
	if len(root.states) != 1 {
		t.Errorf("got %v, want only a.txt", root.states)
	}
}

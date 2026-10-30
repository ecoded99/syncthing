//go:build !noupgrade

// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
//
package upgrade

import (
	"archive/zip"
	"bytes"
	"github.com/syncthing/syncthing/lib/signature"
	"io"
	"os"
	"strings"
	"testing"
)

func TestForkSignedArchive(t *testing.T) {
	private, public, err := signature.GenerateKeys()
	if err != nil {
		t.Fatal(err)
	}
	previous := SigningKey
	SigningKey = public
	t.Cleanup(func() { SigningKey = previous })
	name := "syncthing-windows-amd64-v2.1.7-ecoded.1.zip"
	binary := []byte("disposable executable fixture")
	sig, err := signature.Sign(private, io.MultiReader(strings.NewReader(name+"\n"), bytes.NewReader(binary)))
	if err != nil {
		t.Fatal(err)
	}
	archive := func(data []byte) []byte {
		var b bytes.Buffer
		w := zip.NewWriter(&b)
		f, _ := w.Create("syncthing.exe")
		f.Write(data)
		f, _ = w.Create("release.sig")
		f.Write(sig)
		w.Close()
		return b.Bytes()
	}
	dir := t.TempDir()
	path, err := readZip(name, dir, bytes.NewReader(archive(binary)))
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, binary) {
		t.Fatal("wrong executable", err)
	}
	if _, err := readZip(name, dir, bytes.NewReader(archive([]byte("tampered executable")))); err == nil {
		t.Fatal("tampered binary accepted")
	}
	if _, err := readZip("syncthing-windows-amd64-v2.1.7-ecoded.2.zip", dir, bytes.NewReader(archive(binary))); err == nil {
		t.Fatal("renamed release accepted")
	}
}

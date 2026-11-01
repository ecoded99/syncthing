//go:build !noupgrade

// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
//
package upgrade

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/syncthing/syncthing/lib/signature"
)

func TestForkSignedArchive(t *testing.T) {
	private, public, err := signature.GenerateKeys()
	if err != nil {
		t.Fatal(err)
	}
	previous := SigningKey
	SigningKey = public
	t.Cleanup(func() { SigningKey = previous })
	for _, platform := range []string{"windows-amd64", "macos-arm64", "macos-amd64"} {
		t.Run(platform, func(t *testing.T) {
			testForkSignedArchive(t, private, platform)
		})
	}
}

func testForkSignedArchive(t *testing.T, private []byte, platform string) {
	t.Helper()
	name := fmt.Sprintf("syncthing-%s-v2.1.7-ecoded.1.zip", platform)
	binaryName := "syncthing"
	if strings.HasPrefix(platform, "windows-") {
		binaryName += ".exe"
	}
	binary := []byte("disposable executable fixture")
	sig, err := signature.Sign(private, io.MultiReader(strings.NewReader(name+"\n"), bytes.NewReader(binary)))
	if err != nil {
		t.Fatal(err)
	}
	archive := func(data []byte) []byte {
		var b bytes.Buffer
		w := zip.NewWriter(&b)
		f, _ := w.Create(binaryName)
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
	for _, renamed := range []string{
		strings.Replace(name, "ecoded.1", "ecoded.2", 1),
		strings.Replace(name, platform, "linux-amd64", 1),
	} {
		if _, err := readZip(renamed, dir, bytes.NewReader(archive(binary))); err == nil {
			t.Fatalf("renamed release accepted: %s", renamed)
		}
	}
}

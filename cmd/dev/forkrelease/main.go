// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
// forkrelease creates a signed archive accepted by Syncthing's native updater.
package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"github.com/syncthing/syncthing/lib/signature"
	"github.com/syncthing/syncthing/lib/upgrade"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	archive := flag.String("archive", "", "output archive (its base name is signed)")
	binary := flag.String("binary", "", "Syncthing executable")
	keyPath := flag.String("key", "", "private key file; otherwise STFORK_SIGNING_KEY environment variable")
	verify := flag.Bool("verify", false, "verify archive without a private key")
	flag.Parse()
	if *archive == "" {
		log.Fatal("archive required")
	}
	if *verify {
		if err := verifyArchive(*archive); err != nil {
			log.Fatal(err)
		}
		fmt.Println("signature verified")
		return
	}
	if *binary == "" {
		log.Fatal("binary required")
	}
	key := []byte(os.Getenv("STFORK_SIGNING_KEY"))
	if *keyPath != "" {
		var err error
		key, err = os.ReadFile(*keyPath)
		if err != nil {
			log.Fatal(err)
		}
	}
	data, err := os.ReadFile(*binary)
	if err != nil {
		log.Fatal(err)
	}
	if len(data) > 64<<20 {
		log.Fatal("binary exceeds updater limit")
	}
	signed := io.MultiReader(strings.NewReader(filepath.Base(*archive)+"\n"), bytes.NewReader(data))
	sig, err := signature.Sign(key, signed)
	if err != nil {
		log.Fatal("signing failed: ", err)
	}
	if err = signature.Verify(upgrade.SigningKey, sig, io.MultiReader(strings.NewReader(filepath.Base(*archive)+"\n"), bytes.NewReader(data))); err != nil {
		log.Fatal("signing key does not match embedded updater key")
	}
	out, err := os.OpenFile(*archive, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	z := zip.NewWriter(out)
	for _, item := range []struct {
		name string
		data []byte
	}{{filepath.Base(*binary), data}, {"release.sig", sig}} {
		h := &zip.FileHeader{Name: item.name, Method: zip.Deflate}
		h.SetMode(0755)
		w, err := z.CreateHeader(h)
		if err != nil {
			log.Fatal(err)
		}
		if _, err = w.Write(item.data); err != nil {
			log.Fatal(err)
		}
	}
	for _, name := range []string{"LICENSE", "FORK.md"} {
		b, err := os.ReadFile(name)
		if err != nil {
			log.Fatal(err)
		}
		w, err := z.Create(name)
		if err != nil {
			log.Fatal(err)
		}
		if _, err = w.Write(b); err != nil {
			log.Fatal(err)
		}
	}
	if err = z.Close(); err != nil {
		log.Fatal(err)
	}
	if err = out.Sync(); err != nil {
		log.Fatal(err)
	}
	if err = out.Close(); err != nil {
		log.Fatal(err)
	}
	if err = verifyArchive(*archive); err != nil {
		log.Fatal(err)
	}
	fmt.Println(filepath.Base(*archive), "signed and verified")
}
func verifyArchive(path string) error {
	r, err := zip.OpenReader(path)
	if err != nil {
		return err
	}
	defer r.Close()
	var data, sig []byte
	for _, f := range r.File {
		if f.Name != "syncthing" && f.Name != "syncthing.exe" && f.Name != "release.sig" {
			continue
		}
		if f.UncompressedSize64 > 64<<20 {
			return fmt.Errorf("oversized member")
		}
		in, err := f.Open()
		if err != nil {
			return err
		}
		b, err := io.ReadAll(io.LimitReader(in, 64<<20))
		in.Close()
		if err != nil {
			return err
		}
		if f.Name == "release.sig" {
			sig = b
		} else {
			data = b
		}
	}
	if len(data) == 0 || len(sig) == 0 {
		return fmt.Errorf("binary or signature missing")
	}
	return signature.Verify(upgrade.SigningKey, sig, io.MultiReader(strings.NewReader(filepath.Base(path)+"\n"), bytes.NewReader(data)))
}

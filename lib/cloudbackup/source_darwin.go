// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package cloudbackup

import (
	"os"
	"path/filepath"
	"strings"
)

// macOS exposes these system directories through symlinks. Resolve only those
// exact aliases to their expected targets, then validate every physical ancestor
// as usual. User-created links inside or above the source remain forbidden.
func canonicalSourcePath(src string) string {
	for _, alias := range []string{"/var", "/tmp", "/etc"} {
		if src != alias && !strings.HasPrefix(src, alias+"/") {
			continue
		}
		target, err := os.Readlink(alias)
		if err != nil {
			return src
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(alias), target)
		}
		expected := "/private" + alias
		if filepath.Clean(target) != expected {
			return src
		}
		return expected + strings.TrimPrefix(src, alias)
	}
	return src
}

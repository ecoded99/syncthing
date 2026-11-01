// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package syncthing

import (
	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/config"
)

// modernThemeDBKey records that existing installs have been moved to the
// modern theme once. It is a separate key rather than a config version
// bump so that it never collides with upstream config migrations.
const modernThemeDBKey = "forkModernThemeApplied"

// applyModernTheme switches an install still using the stock "default"
// theme over to "modern", once. A user who picks another theme afterwards
// keeps it.
func applyModernTheme(kv db.KV, cfg config.Wrapper) error {
	miscDB := db.NewMiscDB(kv)
	if _, done, err := miscDB.String(modernThemeDBKey); err != nil || done {
		return err
	}

	if cfg.GUI().Theme == config.DefaultTheme {
		if _, err := cfg.Modify(func(c *config.Configuration) {
			c.GUI.Theme = "modern"
		}); err != nil {
			return err
		}
	}

	return miscDB.PutString(modernThemeDBKey, "1")
}

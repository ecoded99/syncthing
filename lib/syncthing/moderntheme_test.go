// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package syncthing

import (
	"context"
	"os"
	"testing"

	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestNewConfigDefaultsToModernTheme(t *testing.T) {
	if theme := config.New(protocol.LocalDeviceID).GUI.Theme; theme != "modern" {
		t.Fatalf("default theme is %q, want modern", theme)
	}
}

func TestApplyModernTheme(t *testing.T) {
	cases := []struct {
		name, before, want string
	}{
		{"stock default", config.DefaultTheme, "modern"},
		{"user choice kept", "dark", "dark"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Wrap(tempCfgFilename(t), config.Configuration{
				GUI: config.GUIConfiguration{Theme: tc.before},
			}, protocol.LocalDeviceID, events.NoopLogger)
			defer os.Remove(cfg.ConfigPath())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go cfg.Serve(ctx)

			sdb, err := sqlite.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer sdb.Close()

			if err := applyModernTheme(sdb, cfg); err != nil {
				t.Fatal(err)
			}
			if got := cfg.GUI().Theme; got != tc.want {
				t.Fatalf("theme %q, want %q", got, tc.want)
			}

			// Once applied, a later switch back to default sticks.
			if _, err := cfg.Modify(func(c *config.Configuration) {
				c.GUI.Theme = config.DefaultTheme
			}); err != nil {
				t.Fatal(err)
			}
			if err := applyModernTheme(sdb, cfg); err != nil {
				t.Fatal(err)
			}
			if got := cfg.GUI().Theme; got != config.DefaultTheme {
				t.Fatalf("theme changed again to %q", got)
			}
		})
	}
}

//go:build !darwin

// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package cloudbackup

func canonicalSourcePath(src string) string { return src }

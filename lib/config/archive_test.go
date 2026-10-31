// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package config

import (
	"encoding/json"
	"encoding/xml"
	"testing"
)

func TestArchiveTransfersRequireExplicitOptIn(t *testing.T) {
	var old FolderConfiguration
	if err := xml.Unmarshal([]byte(`<folder id="old"><experimentalBatchedTransfers>true</experimentalBatchedTransfers></folder>`), &old); err != nil {
		t.Fatal(err)
	}
	if old.ExperimentalArchiveTransfers || !old.ExperimentalBatchedTransfers {
		t.Fatal("existing batching configuration enabled archive materialization")
	}
	for _, enabled := range []bool{false, true} {
		want := FolderConfiguration{ID: "archive", ExperimentalArchiveTransfers: enabled}
		for name, codec := range map[string]struct {
			marshal   func(any) ([]byte, error)
			unmarshal func([]byte, any) error
		}{"json": {json.Marshal, json.Unmarshal}, "xml": {xml.Marshal, xml.Unmarshal}} {
			data, err := codec.marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			var got FolderConfiguration
			if err := codec.unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			if got.ExperimentalArchiveTransfers != enabled || got.ExperimentalBatchedTransfers {
				t.Fatalf("%s round trip changed independent archive opt-in", name)
			}
		}
	}
}

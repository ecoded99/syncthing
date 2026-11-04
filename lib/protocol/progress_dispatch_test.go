// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package protocol

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/gen/bep"
	"github.com/syncthing/syncthing/lib/testutil"
)

type gatedProgressModel struct {
	*TestModel
	indexStarted chan struct{}
	indexGate    chan struct{}
	configured   chan struct{}
	progress     chan *DownloadProgress
	indexOnce    sync.Once
	configFn     func() error
	progressErr  error
}

func (m *gatedProgressModel) ClusterConfig(_ Connection, _ *ClusterConfig) error {
	if m.configFn != nil {
		if err := m.configFn(); err != nil {
			return err
		}
	}
	select {
	case m.configured <- struct{}{}:
	default:
	}
	return nil
}
func (m *gatedProgressModel) Index(_ Connection, _ *Index) error {
	m.indexOnce.Do(func() { close(m.indexStarted) })
	<-m.indexGate
	return nil
}
func (m *gatedProgressModel) DownloadProgress(_ Connection, p *DownloadProgress) error {
	m.progress <- p
	return m.progressErr
}

func progressTestPair(t *testing.T, m *gatedProgressModel) (Connection, Connection) {
	t.Helper()
	ar, aw := io.Pipe()
	br, bw := io.Pipe()
	sender := NewConnection(c0ID, ar, bw, testutil.NoopCloser{}, newTestModel(), new(mockedConnectionInfo), CompressionNever, testKeyGen)
	receiver := NewConnection(c1ID, br, aw, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionNever, testKeyGen)
	sender.Start()
	receiver.Start()
	t.Cleanup(func() {
		close(m.indexGate)
		closeAndWait(sender, ar, aw, br, bw)
		closeAndWait(receiver)
	})
	return sender, receiver
}

func newGatedProgressModel() *gatedProgressModel {
	return &gatedProgressModel{TestModel: newTestModel(), indexStarted: make(chan struct{}),
		indexGate: make(chan struct{}), configured: make(chan struct{}, 1), progress: make(chan *DownloadProgress, 8)}
}

func TestDownloadProgressDuringIndexUpdate(t *testing.T) {
	m := newGatedProgressModel()
	sender, receiver := progressTestPair(t, m)
	sender.ClusterConfig(&ClusterConfig{}, nil)
	receiver.ClusterConfig(&ClusterConfig{}, nil)
	select {
	case <-m.configured:
	case <-time.After(5 * time.Second):
		t.Fatal("handshake")
	}
	if err := sender.Index(t.Context(), &Index{Folder: "test"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.indexStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("index not started")
	}
	sender.DownloadProgress(t.Context(), &DownloadProgress{Folder: "test"})
	select {
	case progress := <-m.progress:
		if progress.Folder != "test" {
			t.Fatal(progress)
		}
	case <-time.After(time.Second):
		t.Fatal("live progress blocked behind database index processing")
	}
}

func TestDownloadProgressWaitsForConfiguration(t *testing.T) {
	m := newGatedProgressModel()
	entered := make(chan struct{}, 1)
	gate := make(chan struct{})
	defer close(gate)
	m.configFn = func() error {
		entered <- struct{}{}
		<-gate
		return nil
	}
	sender, receiver := progressTestPair(t, m)
	receiver.ClusterConfig(&ClusterConfig{}, nil)
	for i := 0; i < 3; i++ {
		sender.ClusterConfig(&ClusterConfig{}, nil)
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("configuration not received")
		}
		sender.DownloadProgress(t.Context(), &DownloadProgress{Folder: "first"})
		sender.DownloadProgress(t.Context(), &DownloadProgress{Folder: "second"})
		select {
		case <-m.progress:
			t.Fatal("progress bypassed pending configuration")
		case <-time.After(20 * time.Millisecond):
		}
		gate <- struct{}{}
		for _, folder := range []string{"first", "second"} {
			select {
			case p := <-m.progress:
				if p.Folder != folder {
					t.Fatalf("progress order: got %q, want %q", p.Folder, folder)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("progress not delivered after configuration")
			}
		}
	}
}

func TestDownloadProgressHandlerError(t *testing.T) {
	m := newGatedProgressModel()
	m.progressErr = errors.New("progress rejected")
	sender, receiver := progressTestPair(t, m)
	sender.ClusterConfig(&ClusterConfig{}, nil)
	receiver.ClusterConfig(&ClusterConfig{}, nil)
	sender.DownloadProgress(t.Context(), &DownloadProgress{Folder: "test"})
	select {
	case <-getRawConnection(receiver).closed:
	case <-time.After(5 * time.Second):
		t.Fatal("progress handler error did not close the connection")
	}
}

func TestDownloadProgressBeforeConfiguration(t *testing.T) {
	m := newGatedProgressModel()
	sender, receiver := progressTestPair(t, m)
	// Bypass the sender's handshake gate to exercise an invalid peer.
	if err := getRawConnection(sender).writeMessage(&bep.DownloadProgress{Folder: "test"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-getRawConnection(receiver).closed:
	case <-time.After(5 * time.Second):
		t.Fatal("progress before configuration was not rejected")
	}
	select {
	case <-m.progress:
		t.Fatal("unconfigured progress reached the model")
	default:
	}
}

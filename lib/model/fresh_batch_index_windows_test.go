//go:build windows && amd64

// Private generated-fixture comparison only. No production method is changed.
package model

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/scanner"
)

const matchedFilesize = 16384
const matchedWaves = 4

// Opt-in on-disk fresh-file experiment. Writer intentionally omits the live
// receiver's conflict checks; only use with test-created empty directories.
func TestFreshBatchIndexComparison(t *testing.T) {
	mode := os.Getenv("ST_FRESH_BATCH_COMPARISON")
	if mode == "" {
		t.Skip("opt-in disk comparison")
	}
	if mode != "writer" && mode != "writer-overlap" && mode != "receiver" {
		t.Fatal(mode)
	}
	const count = 512
	w, cfg := newDefaultCfgWrapper(t)
	cfg.FilesystemType = config.FilesystemTypeBasic
	cfg.Path = t.TempDir()
	cfg.Copiers = 8
	cfg.MinDiskFree = config.Size{Value: 1, Unit: "%"}
	cfg.DisableFsync = false
	cfg.FSWatcherEnabled = false
	cfg.RescanIntervalS = 0
	must(t, cfg.CreateRoot())
	must(t, cfg.Filesystem().MkdirAll(cfg.MarkerName, 0755))
	waiter, err := w.Modify(func(c *config.Configuration) { c.SetFolder(cfg) })
	must(t, err)
	waiter.Wait()
	m := setupModel(t, w)
	m.cancel()
	<-m.stopped
	t.Cleanup(func() { cleanupModel(m) })
	runner, ok := m.folderRunners.Get(cfg.ID)
	if !ok {
		t.Fatal("missing folder")
	}
	f := runner.(*sendReceiveFolder)
	if f.Copiers != 8 || f.DisableFsync || f.MinDiskFree != cfg.MinDiskFree {
		t.Fatal("receiver configuration differs")
	}
	conn := addFakeConn(m, device1, cfg.ID)
	t.Cleanup(func() { conn.Close(errors.New("matched diagnostic complete")) })

	conn.RequestCalls(func(_ context.Context, req *protocol.Request) ([]byte, error) {
		data, ok := conn.fileData[req.Name]
		if !ok || req.Offset < 0 || req.Size < 0 || req.Offset+int64(req.Size) > int64(len(data)) {
			return nil, fmt.Errorf("invalid request %q", req.Name)
		}

		return data[req.Offset : req.Offset+int64(req.Size)], nil
	})
	stamp := time.Unix(1712345678, 123456700)
	prevSequence := int64(0)
	for wave := 0; wave < matchedWaves; wave++ {
		first := len(conn.files)
		for offset := 0; offset < count; offset++ {
			ordinal := wave*count + offset
			body := matchedBody(ordinal)
			conn.addFile(fmt.Sprintf("file-%06d.bin", ordinal), 0644, protocol.FileInfoTypeFile, body)
			file := &conn.files[len(conn.files)-1]
			file.ModifiedS, file.ModifiedNs = stamp.Unix(), int32(stamp.Nanosecond())
			file.Sequence = int64(ordinal + 1)
		}
		index := make([]protocol.FileInfo, count)
		for i, file := range conn.files[first:] {
			index[i] = prepareFileInfoForIndex(file)
		}
		lastSequence := int64((wave + 1) * count)
		if wave == 0 {
			must(t, m.Index(conn, &protocol.Index{Folder: cfg.ID, Files: index, LastSequence: lastSequence}))
		} else {
			must(t, m.IndexUpdate(conn, &protocol.IndexUpdate{Folder: cfg.ID, Files: index, PrevSequence: prevSequence, LastSequence: lastSequence}))
		}
		sequence, err := m.sdb.GetDeviceSequence(cfg.ID, device1)
		must(t, err)
		if sequence != lastSequence {
			t.Fatal("remote index sequence differs")
		}
		need, err := m.sdb.CountNeed(cfg.ID, protocol.LocalDeviceID)
		must(t, err)
		if need.Files != count || need.TotalItems() != count || need.Bytes != int64(count*matchedFilesize) {
			t.Fatal("preflight needed state differs", need)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 150*time.Second)
		scanChan := make(chan string, count)

		started := time.Now()
		var changed int
		var pullErr error
		var ioSeconds, indexSeconds float64
		if mode == "receiver" {
			changed, pullErr = f.pullerIteration(ctx, scanChan)
		} else {
			jobs := make(chan dbUpdateJob, count)
			indexed := make(chan int, 1)
			runIndex := func() { indexed <- f.dbUpdaterRoutine(jobs) }
			if mode == "writer-overlap" {
				go runIndex()
			}
			ioStart := time.Now()
			pullErr = matchedWriterWave(ctx, f, conn.files[first:], conn.fileData, jobs)
			ioSeconds = time.Since(ioStart).Seconds()
			indexStart := time.Now()
			close(jobs)
			if mode == "writer" {
				runIndex()
			}
			changed = <-indexed
			indexSeconds = time.Since(indexStart).Seconds()
		}
		// Include the final actual SQLite need-zero query in BOTH elapsed clocks.
		need, err = m.sdb.CountNeed(cfg.ID, protocol.LocalDeviceID)
		ended := time.Now()
		cancel()
		must(t, pullErr)
		must(t, err)
		if changed != count || need.TotalItems() != 0 || len(scanChan) != 0 || len(f.tempPullErrors) != 0 {
			t.Fatalf("wave failed: changed=%d need=%v scans=%d errors=%v", changed, need, len(scanChan), f.tempPullErrors)
		}
		matchedVerifyRawInventory(t, cfg.Path, f, conn.files, stamp)
		for _, file := range conn.files {
			data, err := os.ReadFile(filepath.Join(cfg.Path, file.Name))
			must(t, err)
			h := sha256.Sum256(data)
			if string(h[:]) != string(file.Blocks[0].Hash) {
				t.Fatal(file.Name)
			}
		}
		t.Logf("mode=%s wave=%d verified=%d MB/s=%.3f elapsed_s=%.6f write_s=%.6f index_drain_s=%.6f", mode, wave+1, len(conn.files), float64(count*matchedFilesize)/ended.Sub(started).Seconds()/1e6, ended.Sub(started).Seconds(), ioSeconds, indexSeconds)
		prevSequence = lastSequence
	}
}

func matchedBody(ordinal int) []byte {
	body := make([]byte, matchedFilesize)
	for p := 0; p < len(body); p += sha256.Size {
		var seed [16]byte
		binary.LittleEndian.PutUint64(seed[:8], uint64(ordinal))
		binary.LittleEndian.PutUint64(seed[8:], uint64(p))
		h := sha256.Sum256(seed[:])
		copy(body[p:], h[:])
	}
	return body
}

func matchedWriterWave(ctx context.Context, f *sendReceiveFolder, files []protocol.FileInfo, data map[string][]byte, jobs chan<- dbUpdateJob) error {
	var wg sync.WaitGroup
	errs := make([]error, 8)
	gate := make(chan struct{})
	for lane := 0; lane < 8; lane++ {
		wg.Add(1)
		go func(lane int) {
			defer wg.Done()
			<-gate
			for i := len(files) * lane / 8; i < len(files)*(lane+1)/8; i++ {
				if err := ctx.Err(); err != nil {
					errs[lane] = err
					return
				}
				if err := matchedWriterFile(ctx, f, files[i], data[files[i].Name]); err != nil {
					errs[lane] = err
					return
				}
				jobs <- dbUpdateJob{files[i], dbUpdateHandleFile}
			}
		}(lane)
	}
	close(gate)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func matchedWriterFile(ctx context.Context, f *sendReceiveFolder, file protocol.FileInfo, body []byte) (err error) {
	temp := fs.TempName(file.Name)
	if _, err := scanner.HashFile(ctx, f.ID, f.mtimefs, temp, 128*1024, nil); !fs.IsNotExist(err) {
		return fmt.Errorf("expected absent temp: %v", err)
	}
	if err := f.mtimefs.Remove(temp); !fs.IsNotExist(err) {
		return fmt.Errorf("expected absent temp removal: %v", err)
	}
	parent, err := f.mtimefs.Stat(filepath.Dir(temp))
	if err != nil {
		return err
	}
	if !parent.IsDir() || parent.Mode()&0200 == 0 {
		return errors.New("nonwritable fixture parent")
	}
	fd, err := f.mtimefs.OpenFile(temp, fs.OptReadWrite|fs.OptCreate|fs.OptExclusive, 0666)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = fd.Close()
		}
	}()
	if err = f.mtimefs.Hide(temp); err != nil {
		return err
	}
	if err = fd.Truncate(file.Size); err != nil {
		return err
	}
	h := sha256.Sum256(body)
	if len(body) != matchedFilesize || len(file.Blocks) != 1 || !bytes.Equal(h[:], file.Blocks[0].Hash) {
		return errors.New("input hash mismatch")
	}
	n, err := fd.WriteAt(body, 0)
	if err != nil {
		return err
	}
	if n != len(body) {
		return io.ErrShortWrite
	}
	if err = fd.Sync(); err != nil {
		return err
	}
	closed = true
	if err = fd.Close(); err != nil {
		return err
	}
	if err = f.mtimefs.Unhide(temp); err != nil {
		return err
	}
	if err = f.mtimefs.Chmod(temp, fs.FileMode(file.Permissions)); err != nil {
		return err
	}
	if _, err = f.mtimefs.Lstat(file.Name); !fs.IsNotExist(err) {
		return fmt.Errorf("expected absent destination: %v", err)
	}
	if err = osutil.RenameOrCopy(f.CopyRangeMethod.ToFS(), f.mtimefs, f.mtimefs, temp, file.Name); err != nil {
		return err
	}
	return f.mtimefs.Chtimes(file.Name, file.ModTime(), file.ModTime())
}

func matchedVerifyRawInventory(t testing.TB, root string, f *sendReceiveFolder, files []protocol.FileInfo, stamp time.Time) {
	t.Helper()
	expected := map[string]bool{}
	for _, file := range files {
		expected[file.Name] = true
		indexed, ok, err := f.db.GetDeviceFile(f.ID, protocol.LocalDeviceID, file.Name)
		must(t, err)
		if !ok || !indexed.Version.Equal(file.Version) || !indexed.BlocksEqual(file) || !indexed.ModTime().Equal(stamp) {
			t.Fatal("local index blocks or timestamp differ", file.Name)
		}
		raw, err := os.Lstat(filepath.Join(root, file.Name))
		must(t, err)
		if !raw.Mode().IsRegular() || raw.Size() != file.Size || !raw.ModTime().Equal(stamp) {
			t.Fatal("raw filesystem metadata mismatch", file.Name)
		}
		if _, err := f.mtimefs.Lstat(fs.TempName(file.Name)); !fs.IsNotExist(err) {
			t.Fatal("temporary file remains", file.Name, err)
		}
	}
	seen := 0
	must(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == ".stfolder" {
			return filepath.SkipDir
		}
		if entry.IsDir() || !expected[rel] {
			return fmt.Errorf("unexpected fixture entry %s", rel)
		}
		seen++
		return nil
	}))
	if seen != len(files) {
		t.Fatal("inventory count mismatch", seen, len(files))
	}
}

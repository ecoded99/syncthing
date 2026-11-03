# Bounded scan pipeline

The regular-file comparison stage now fetches SQLite metadata in batches of up
to 128 names rather than loading a complete block list with one query per file.
Four workers compare batches concurrently. The internal scanner configuration
can select 1–8 workers, capped by GOMAXPROCS; this is not a new user setting.
Eight workers were slower and more variable than four in the local benchmark.

Directory enumeration, ignore handling, Unicode normalization, directories and
symlinks retain their ordered traversal. Only accepted regular files enter the
parallel stage. The SQLite query retains the existing version, platform metadata,
block size and block hash, but omits unused block lists. Query failures produce
scan errors and must never become new-file updates. Returned version vectors
are copied before mutation. SQL rows close before comparison or indexing.

Hashing starts during discovery, including when a partial metadata batch has
waited 10 ms. Backpressure bounds pending metadata to active worker batches,
one queued batch per worker, an assembling batch and a 128-item input channel.
The hash queue is bounded to twice the hasher count, at most 64 entries. The
previous progress-enabled path retained every changed file until discovery ended.
Cancellation stops discovery, drains producers and waits for metadata workers
before closing downstream channels.

`FolderScanProgress` adds `discoveryComplete`. While false, `total` describes
bytes discovered so far; the GUI hides percentage and ETA. Rate remains visible.
Once true, the existing percentage and ETA apply. The GUI also accepts older
backends that omit the field. No database migration or transfer protocol changes
are involved.

## Scan visibility and throughput

The folder status distinguishes checking local files, hashing changed files,
checking deletions and updating the local index. Progress includes entries
visited, changed files queued for hashing, bytes hashed and indexed entries
checked for deletion. It reports whether the scan covers the entire folder or
affected paths, and why it ran: startup, periodic check, filesystem watcher,
watcher recovery, requested check or verification. Progress-disabled folders
retain their disabled setting. Scan percentage and ETA describe hashing only;
they are hidden during discovery, deletion checks and index updates.

Watcher events already scan affected paths, potentially widening to an ancestor
for previously unknown paths. Startup, periodic checks and watcher recovery
can scan the whole folder's metadata. A metadata check does not imply rehashing
unchanged file contents or downloading files. A scan may go directly to Up to
Date when no transfer is required. In a disposable 512-file Windows check,
editing one known file caused a watcher scan of one entry; an unchanged full
scan visited 522 entries (including directories/internal entries), queued zero
files for hashing and preserved the index sequence.

Network and processing rates now refresh every second, using a monotonic clock
and actual elapsed time, with no overlapping requests. Counter resets and
connection replacements cannot become negative rates or lifetime-total spikes.
Scan hashing reports the recent sample interval instead of a one-minute EWMA
that remained zero until its first five-second tick.

The processing counters distinguish small-file bundle prepare/decode (ordinary
block transfers excluded), bytes accepted by file writes (including partial
files and local copies), and logical bytes/files successfully finalized. File
writes include filesystem caching and are not physical disk throughput.
Finalization includes reused blocks and updates at file completion, so its
rate can differ from network traffic. All totals are process-lifetime counters.
The GUI labels these boundaries and adds finalized files per second, which is
useful for small or empty files. `node script/test_gui_stats.cjs` checks polling,
rate intervals, resets, event wiring, scan phases and older backend behavior.

## Validation and measurements

Tests cover metadata equivalence with single-file lookups, omitted block lists,
missing/deleted files, device isolation, batch limits, ignored paths, unchanged
scans, changed files, lookup failures, version ownership, cancellation, bounded
worker concurrency and hashing before discovery completes. Scanner and SQLite
tests run with Go's race detector. CI and release checks include the scanner.

`BenchmarkScanMetadataPipeline` uses 4,096 real Windows files and SQLite. Three
runs gave median comparison-stage times of 864 ms for individual lookups, 455 ms
for one batched worker, 421 ms for four and 464 ms for eight. These are stage
measurements, not total scan speedups.

`script/benchmark_scans.py` compares two binaries using disposable, isolated
peers and generated 16 KiB files, with watchers and periodic rescans disabled.
It measures initial scan and indexing, three unchanged full scans and a scan
after modifying 20 files, deleting 10 and adding one. It checks file counts,
85 indexed content hashes, all ten deletions and unchanged index sequences.
It never uses a production home or folder. Example from the repository root:

```sh
python script/benchmark_scans.py --binary /path/to/candidate \
  --baseline-binary /path/to/released-binary --work /new/empty-result-path \
  --files 16384 --trials 2
```

The final executable's 16,384-file Windows comparison against
v2.1.7-ecoded.10 measured:

| Complete operation | Released | Candidate | Time reduction |
| --- | ---: | ---: | ---: |
| Initial scan and indexing | 26.13 s | 21.56 s | 17% |
| Unchanged full scan (median of three) | 6.14 s | 5.39 s | 12% |
| Modified/deleted-file scan | 6.51 s | 5.35 s | 18% |

These are warm local synthetic fixtures, not measurements of the live 509,000-file
folder. Filesystem enumeration/stat calls and the model's deletion pass remain
serial and limit whole-scan improvements. The comparison-stage gain must not be
presented as a twofold whole-scan improvement.

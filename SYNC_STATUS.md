# Sync status and estimates

Folder cards describe **this device**. “Up to Date Locally” can coexist with
another device receiving files. Expand a remote device to see each shared
folder's reported activity, data percentage, remaining items and estimate.

The GUI distinguishes checking status, receiving data, applying file changes,
waiting for progress, stale reports, paused folders, unaccepted shares and
disconnected devices. Unknown status is not shown as syncing at zero percent.
Disconnected completion is labeled “Last Known”. Zero-byte files, directories,
links and deletions remain pending work even when no data bytes remain. They do
not get an invented 95% data percentage. A data percentage of 100 does not imply
that all file operations and index updates have finished.

Estimates use the reduction in reported work for that particular device and
folder, including locally reused data. They never divide a folder's size by
whole-connection network traffic. A bounded window holds up to 64 reports, preferring the last five seconds and retaining at least three reports for slower-reporting peers. At least three reports spanning four seconds are required.
Estimates reset when the queue grows, the global byte/item scope changes, a new
sync starts or the peer reconnects. They remain stable between reports, disappear
after 15 seconds without measured progress, and show stale status after 20
seconds without a report. These labels describe the available evidence; they do
not diagnose a stalled peer or its cause. Completed idle folders need no heartbeat.

While bytes remain, “Data remaining” estimates data completion; finalization and
index updates may take longer. Once only item operations remain, an item-rate
estimate is used if enough item progress has been measured. Paused, offline,
unshared and stale folders have no ETA. Scan estimates similarly hide during
discovery/deletion/index phases, after stale reports, and when hashing is done.

API folder summaries and completions include an additive `updatedAt` timestamp
for the start of calculation. The GUI discards older snapshots and preserves
newer state-change events. Database counts and active pullers are sampled
separately, so sequential requests may differ during finalization; these are
observations rather than one atomic cross-device snapshot. Remote temporary
block byte counts are approximate, and progress frequency depends on the peer.
The legacy REST `completion` field retains its non-byte deletion compatibility
behavior; the GUI displays data progress and pending operations separately.

Backend fixes include local completion accounting for active pullers, correct
byte estimates for files aligned to the block size, and concurrent initialization
of remote per-folder progress without losing updates. Completed pullers remain
accounted for until their database batch commits; incoming peer indexes likewise
retain temporary progress until committed. Requests for recently promoted
temporary files can fall back to the final regular file with hash validation.

## Local validation

```console
node script/test_gui_stats.cjs
go test ./lib/model ./lib/api ./lib/protocol ./lib/syncthing
go test -race ./lib/model -run 'Test.*Progress|Test.*Completion|Test.*DeviceDownloadState|TestBlocksToSize'
python script/smoke_sync_status.py --binary syncthing.exe --work NEW_DIRECTORY --transport quic
```

Repeat with `--transport tcp` and with/without `--bundles`. The script creates two
isolated peers, caps sending at 1 MiB/s, verifies all 142 destination file hashes,
checks local and remote partial reports, pauses/resumes the receiver and verifies
142 deletions with zero global bytes before converging on both sides. It records
API snapshots in `results.json` and `samples.json`, and only shuts down processes
it created. Production files and configuration are not opened.

Pass one or more `results.json` paths to `test_gui_stats.cjs` to replay the exact
ETA calculation against measured completion. Four Windows loopback runs across
TCP/QUIC and ordinary/bundled transfers produced 42–44 usable remote ETA samples
per run, with median relative errors of approximately 15–19% against observed
completion (remaining intervals below five seconds use a five-second denominator).
These are capped local measurements, not a prediction of WAN accuracy. GUI checks
cover startup, counters, stale and stopped work, changing scope, empty files,
deletions, pause/disconnect states and reordered snapshots. Browser inspection
confirmed the live per-folder estimate and no horizontal overflow at 375px and
812px landscape.

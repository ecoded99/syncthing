# Transfer pipeline statistics

The **This Device** table shows these rates and cumulative byte totals:

| Reading | Meaning |
| --- | --- |
| Network Receive | Existing inbound protocol traffic, including protocol overhead, across all connections. |
| Bundle Prepare (Send) | Successful bundle payload bytes read and assembled by this device. |
| Bundle Decode (Receive) | Payload bytes parsed from structurally valid incoming bundle responses, before content hash verification. |
| File Writes (Receive) | Bytes accepted by file writes, including partial files and local block copies. Includes filesystem caching; not physical disk throughput. |
| Files Finalized (Receive) | Whole-file bytes successfully flushed and finalized, then handed to the sync index, plus completed files/s and file count. Database commit can finish later. Includes ordinary transfers, bundles and reused local blocks. |

These are aggregate throughput readings, refreshed every second over the actual GUI polling interval,
across all folders. They are not isolated CPU processing benchmarks. Work overlaps,
so do not add the rates or infer stage duration from them. Completed files cause
the processing counter to advance in steps; short operations may appear as a
single polling spike. Empty files have no byte throughput but contribute to files/s.

The bundle format is an uncompressed framed stream. Preparation and decoding are
the relevant equivalents of packing and unpacking; no ZIP compression,
decompression, or repacking happens. Bundle counters stay zero for ordinary
transfers. File processing excludes metadata-only shortcuts and deletions.

`GET /rest/system/connections` exposes the existing network totals plus a
`processing` object containing `preparedBytesTotal`, `decodedBytesTotal`, and
`appliedBytesTotal`, `writtenBytesTotal`, and `appliedFilesTotal`. These counters reset when the process restarts.
The GUI calculates counter deltas divided by monotonic elapsed time, displays
zero on the first sample or a reset, and hides the new rows with older backends.
Polling requests cannot overlap. Scan hashing likewise uses a recent interval
rate instead of the former one-minute EWMA and initial five-second delay.

Validation: targeted protocol/model race tests passed. Two isolated local QUIC
peers transferred and verified 512 files (8 MiB) in both ordinary and bundle
modes. Ordinary applied bytes matched the fixture; bundled sender preparation,
receiver decoding, and receiver applied bytes all matched 8 MiB. GUI rate
calculations were checked for initial samples, counter deltas, resets, and zero
elapsed intervals. This does not establish desktop-to-laptop throughput.

## Bounded stage parallelism

Bundle source reads now use up to four workers per connection, capped by
GOMAXPROCS. Each response is copied and closed before that worker reads its next
member, preserving the ordinary request-memory limiter. All reads drain before
returning; member ordering and per-file errors are preserved. At most one batch
is served per connection. Owned member copies and the final frame are each
bounded to 2 MiB of payload, in addition to up to four normal response buffers
subject to the existing limiter.

Explicit bundle checksum verification uses up to four workers for bundles of
at least 256 KiB. Smaller bundles retain sequential hashing to avoid scheduling
overhead. No member is returned until verification succeeds for every successful
member. Hash corruption still disables the extension; cancellation does not.

Receiving, file writing, finalization, and index work already overlap. Index
processing remains a significant part of the local small-file measurements;
increasing concurrency alone will not remove that cost. Maintain ordering and
version consistency when changing index batching. Additional network connections
have not been enabled by this change.

## Remote folder progress

Download-progress callbacks can run while the preceding index message is being
applied to the database. Previously both used the same dispatcher, allowing a
slow index application to hold the displayed remote percentage at zero while
other connections continued transferring. Progress remains ordered on the
connection reader and waits for all preceding cluster configurations to finish.
Index messages retain their existing order; no additional index queue is added.

A deterministic protocol regression blocks index application and requires live
progress to arrive before releasing it. Additional tests cover configuration
gating, progress ordering, invalid pre-handshake messages and callback errors.
Messages still cannot overtake unread messages on the wire, and the percentage
still depends on the peer reporting progress and the completion query finishing.

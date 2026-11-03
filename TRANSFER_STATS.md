# Transfer pipeline statistics

The **This Device** table shows these rates and cumulative byte totals:

| Reading | Meaning |
| --- | --- |
| Network Receive | Existing inbound protocol traffic, including protocol overhead, across all connections. |
| Bundle Prepare (Send) | Successful bundle payload bytes read and assembled by this device. |
| Bundle Decode (Receive) | Payload bytes parsed from structurally valid incoming bundle responses, before content hash verification. |
| File Processing (Receive) | Whole-file bytes successfully flushed and finalized, then handed to the sync index. Database commit can finish later. Includes ordinary transfers and bundles, and can include reused local blocks. |

These are aggregate throughput readings, measured over the GUI polling interval,
across all folders. They are not isolated CPU processing benchmarks. Work overlaps,
so do not add the rates or infer stage duration from them. Completed files cause
the processing counter to advance in steps; short operations may appear as a
single polling spike. Empty files have no byte throughput.

The bundle format is an uncompressed framed stream. Preparation and decoding are
the relevant equivalents of packing and unpacking; no ZIP compression,
decompression, or repacking happens. Bundle counters stay zero for ordinary
transfers. File processing excludes metadata-only shortcuts and deletions.

`GET /rest/system/connections` exposes the existing network totals plus a
`processing` object containing `preparedBytesTotal`, `decodedBytesTotal`, and
`appliedBytesTotal`. These payload counters reset when the process restarts.
The GUI calculates counter deltas divided by elapsed wall-clock time, displays
zero on the first sample or a reset, and hides the new rows with older backends.

Validation: targeted protocol/model race tests passed. Two isolated local QUIC
peers transferred and verified 512 files (8 MiB) in both ordinary and bundle
modes. Ordinary applied bytes matched the fixture; bundled sender preparation,
receiver decoding, and receiver applied bytes all matched 8 MiB. GUI rate
calculations were checked for initial samples, counter deltas, resets, and zero
elapsed intervals. This does not establish desktop-to-laptop throughput.

## Further parallelism candidates

Source member reads in `serveBatch` and bundle content checksum verification are
currently sequential within a bundle. Bounded concurrency may help if profiling
shows those stages constrain throughput. Source reads must preserve authorization,
source consistency, response ordering, and the request memory limiter; parallel
reads that hold every response open can exhaust that limiter. Checksum work must
still finish before accepting data.

Receiving, file writing, finalization, and index work already overlap. Index
processing remains a significant part of the local small-file measurements;
increasing network streams alone will not remove that cost. Measure the new stage
rates on the real link before choosing the next optimization. Maintain ordering
and version consistency when changing index batching.

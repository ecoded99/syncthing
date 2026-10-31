# Experimental small-file archive receiver

This path is disabled by default. A folder may opt in with
`experimentalArchiveTransfers`; this is separate from the earlier
`experimentalBatchedTransfers` option. It is an experimental transport and
receiver implementation, not a claim of faster synchronization.

The ordinary needed-file planner identifies changed, valid regular files with
one complete block, up to 64 KiB each. It groups at most 1,024 files and 16 MiB
from the same available peer. The sender produces a bounded stored ZIP over the
existing authenticated BEP connection. Unsupported peers use ordinary requests.
Encrypted folders, non-basic filesystems, folders with fsync disabled, and
reusable temporary blocks keep the ordinary path.

The receiver validates the ZIP inventory and each member's length, CRC and
indexed SHA256. Up to eight callbacks write directly into the original
Syncthing temporary files in their original parent directories. Their ordinary
creation, permissions, sparse-file handling and bandwidth limiter remain in
effect. Every writer is SyncClosed before any member enters finalization.
Temporary files use the existing ignored naming convention; there is no extra
archive directory inside the synchronized folder.

The original finishers retain local-change detection, conflict handling,
versioning, permissions, publication and database updates. Archive timestamps
never override the receiver's trusted index metadata. A failed request before
materialization falls back with unchanged prepared states. Any materialization
failure marks the entire window failed before finalization and publishes no
member of that window. Valid ordinary temp blocks remain available for a later
attempt. Final publication itself retains ordinary per-file success/failure
semantics; this is not a filesystem transaction across all files.

`syncthing_model_archive_files_total{folder}` counts successful original
finalizations and index enqueue, not final database commit. Completion still
requires ordinary folder state and need counts. Three aggregate
`syncthing_model_archive_phase_seconds_total{folder,phase}` counters measure
request (sender packing plus transfer), materialize (including parent checks,
validation, writes and SyncClose), and joined finishers. Ordinary planning and
the final database commit wait are outside these counters. Transport request
counters alone do not establish successful publication.

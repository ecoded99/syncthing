# Experimental Windows receiver finalization

This change reduces filesystem work per small file rather than only packing
network messages. It is opt-in (`experimentalFastFinalization`), defaults to
false, and is independent of `experimentalBatchedTransfers`.

## Behavior

For an eligible plain file of at most 64 KiB, the receiver updates permissions,
visibility, and timestamps using its existing output handle before the normal
durable flush and close. It preserves unrelated Windows attributes. Physical
time is read after rename and recorded through the existing virtual-mtime
mapping, retaining precision on filesystems with coarser timestamps.

New files use a same-directory move without replacement or cross-volume copying.
Case validation and case-cache updates remain active. If a destination appears
concurrently, the move fails while retaining both that destination and the
incoming temporary file. The ordinary retry/scan path can reconcile the change.
Existing destinations retain the original conflict/version/deletion handling
and replacement operation. Unsupported filesystem wrappers use ordinary rename.

Encrypted files, ownership/xattr synchronization, unsupported handles and
non-Windows platforms retain ordinary metadata operations. A metadata API failure
also falls back to the ordinary path. Hash verification, per-file durable flush,
directory sync, and index publication are not removed or deferred into a journal.
No antivirus exclusion or protection change is part of this feature.

## Local validation, 30 October 2026

All workloads transfer 8,192 random 16 KiB files (128 MiB) to a fresh destination.
Rates are decimal MB/s, from receiver startup through completed synchronization;
later SHA-256 and exact physical-mtime verification is outside the timed section.
Senders finish initial indexing before receivers start. Both instances use the
same build, and the feature is toggled on the receiver. Network batching is off.
No compiler or competing benchmark ran during the timed transfers. Normal
background applications, including backups and antivirus, remained active.

The metadata-only iteration passed three ordered matched comparisons: controls
36.812/36.328/35.234 seconds, candidates 33.890/32.906/32.953 seconds. Aggregate
throughput improved about 8.6%. That intermediate implementation is superseded
by the combined metadata and new-file installation path.

With the combined implementation and **eight workers in both cases**, ordered
off/on/on/off measurements were 26.469/22.734/23.359/27.218 seconds. Aggregate
throughput was approximately **5.00 MB/s off versus 5.82 MB/s on**, a **16.5%**
gain. Four combined-feature eight-worker trials across the two local matrices
ranged from 5.49 to 5.90 MB/s. This does not establish 10 MB/s for this workload.

Default-worker controls in the preceding matrix varied substantially, so they
are not used to attribute all gains from higher concurrency to this code. All
16 local timed transfers, totaling **131,072 files**, passed SHA-256 and exact
modification-time checks. This is synthetic local evidence, not a guarantee for
an actual project's changed files or a WAN result.

Filesystem counters confirmed the fast path: for the 8,192-file workload,
path-based Lstat counts dropped from approximately 116,000 to 75,000. There were
8,192 handle-metadata operations, new-name moves, and mtime records. Counter
durations overlap across workers and must not be summed as transfer wall time.

Full filesystem, model, configuration and OS utility suites passed on Windows.
Focused race checks passed. New tests cover readonly permissions, unrelated
attributes, physical and virtual time precision, failed/ineligible files,
existing/case-colliding destinations, source retention, traversal rejection and
case-cache updates. A two-instance smoke test checked initial synchronization,
concurrent-edit convergence with the losing edit retained, native backup and
version retention, and forcible receiver-process termination followed by restart
and hash/mtime verification of 1,024 newly added files. It does not simulate a
physical power failure.

Actual receiving-PC/WAN comparisons are pending; do not infer a remote gain from
the local result or enable higher concurrency on an untested device by default.

## API references

- [Windows FILE_BASIC_INFO](https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_basic_info)
- [Windows MoveFileExW flags](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)

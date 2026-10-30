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

## Receiving-laptop validation, 30 October 2026

The same synthetic workload was tested over the existing Tailscale route to the
actual Windows receiving laptop, using fresh isolated instances and destinations.
The binary was identical in every condition. Network batching was off, file and
directory flushes remained enabled, and antivirus protection was unchanged. The
source completed its initial scan before receiver timing began. Every received
file was subsequently checked by SHA-256 and exact physical modification time;
that verification time is excluded from the transfer rate.

With **eight workers in both conditions**, the ordered on/off/off/on transfer
times were 135.860/141.608/143.029/140.357 seconds. Aggregate throughput was
**0.943 MB/s off versus 0.972 MB/s on**, approximately **3.0% higher**. These two
trials per condition do not establish a meaningful real-world improvement or
justify deploying higher concurrency. Bracketing ordinary two-worker controls
took 134.548 and 135.482 seconds, aggregating **0.994 MB/s**. The proposed
eight-worker combination therefore did not beat the existing default behavior.
All six small-file runs, totaling **49,152 files**, passed hash and exact-mtime
verification.

A subsequent ordinary two-worker control transferred the same 128 MiB as one
file on the same route in **7.543 seconds (17.79 decimal MB/s)**, including
receiver startup. Its hash and exact mtime also passed. This single control
shows that the route can exceed 10 MB/s under the observed conditions; the
small-file slowdown is not explained by a fixed 1 MB/s connection cap. It is
not a sustained link-capacity measurement.

The real laptop had its initial native backup running, and a process sample
showed active Google Drive I/O. These measurements reflect that workload;
they do not isolate a particular storage driver, background process, or antivirus
as the cause. They must not be presented as an unloaded hardware limit.

Filesystem counters confirmed that the new path ran on the receiving laptop.
In the first enabled trial, open/create calls averaged about 25 ms and new-name
moves about 49 ms. Operations overlap across workers, and wrapper measurements
can include nested calls, so their durations cannot be summed into elapsed-time
percentages. The evidence still points to substantial per-file receiver work
remaining after metadata calls are combined. At 16 KiB per file, 10 decimal MB/s
would require about 610 files per second; these enabled runs achieved about
59 files per second.

The **10 MB/s goal remains unmet**. Keep this feature experimental and disabled
by default. It has not been released through OTA or enabled on live folders.

## API references

- [Windows FILE_BASIC_INFO](https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_basic_info)
- [Windows MoveFileExW flags](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)

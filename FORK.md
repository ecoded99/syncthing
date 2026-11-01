# ecoded Syncthing

This experimental fork keeps Syncthing as the primary sync engine. It adds a
secondary, browsable, one-way backup into Google Drive through the Drive API,
and an optional batched small-file transport between compatible peers.

The transport collects up to 32 whole-file reads, each at most 64 KiB, into one
bounded archive-like frame over the existing authenticated BEP connection. Every
member is authorized by the normal sender and returned to the normal receiver,
which still checks its hash and applies the usual conflict, metadata, and
per-file durability rules. Encrypted folders and stock peers use ordinary reads.
There is no direct unpacking over live folders. Batching is disabled by default.

## Link speed tests

Expand a connected device under Remote Devices and click **Test Link Speed**
beside Pause and Edit. Both devices must run `v2.1.7-ecoded.8` or later.
The results show download from and upload to that peer, in Mbps, from the
perspective of the device whose GUI is open.

The test sends random data over the existing authenticated sync connections;
it does not create or change synced files. It sends at most 64 MiB per direction
and times out after 20 seconds. One test can run at a time from each GUI server.
Active sync traffic, relay connections, and configured bandwidth limits affect
the measured application throughput. Older peers report that tests are unsupported.

## Validation

On a Windows loopback fixture of 8,192 random 16 KiB files (128 MiB), three
ordinary runs measured 39.56, 47.80, and 42.14 seconds. Three batched runs measured
44.75, 42.67, and 42.06 seconds. All 49,152 received files passed SHA-256 and
modification-time verification. The ranges overlap: batching is **not a validated
speed improvement**. This is not a WAN benchmark. Some runs overlapped development
activity; these figures reject an improvement claim, rather than establish a
precise performance difference. Reducing messages does not eliminate the
receiving filesystem's per-file work.

### Experimental whole-file bundle pipeline

With the per-folder `experimentalBatchedTransfers` setting enabled, initial
copies of small files now use an explicit model-level bundle pipeline rather
than relying only on coincident block requests. It collects up to 32 regular
files of at most 64 KiB each (2 MiB of content), pins a compatible connection,
and requests them in one bounded STB1/STR1 frame. The local index snapshot is
the manifest of names, versions, metadata, sizes and hashes. Successful members
are all hash-verified before any member is staged. The sender authorizes and
reads each member through its ordinary request handler.

Verified content is written into exclusively created Syncthing temporary files.
The normal finisher performs fsync, local-change/conflict checks, replacement,
metadata application and batched database updates. While enabled, finishing
concurrency can increase to eight, bounded by `maxConcurrentWrites`; an existing
higher `copiers` setting is retained. Per-file durability is not disabled. Bundle
payloads and staging/application queues are bounded independently of tree size.
No general-purpose archive extractor runs against live folders.

This path targets initial copies: existing local files, existing temporary files,
large/multiblock files, empty/all-zero files and encrypted folders retain their
ordinary path. Stock peers retain ordinary requests. Failed bundles or member
reads retry through the ordinary copier/puller, which can select another source.
Interrupted staged files retain the ordinary temporary-file recovery behavior;
an interrupted in-memory bundle is requested again. There is no persistent
archive checkpoint or whole-bundle atomic commit. The option remains off by default.

The optimized receiver separates downloading, staging and durable finalization.
It prefetches one verified bundle while up to eight bounded staging workers
create temporary files, and uses bundle-sized queues between finishing and index
updates. At most three bundle payloads are active (6 MiB of file content, excluding
protocol overhead), independently of folder size. The already-computed source
availability is reused instead of querying the database twice per candidate.
Hash verification, fsync and conflict checks remain enabled.

A receiver profile of the first implementation showed filesystem calls dominating
the samples, with some finishers waiting while the sole staging worker created a
temporary file. Alternating before/after trials against commit `ef4a56808` with
bundles enabled in both builds measured the following for 4,096 random 16 KiB files:

| Trial | Previous bundle pipeline | Optimized pipeline |
| --- | ---: | ---: |
| 1 | 16.85 s | 13.52 s |
| 2 | 15.99 s | 13.60 s |
| 3 | 15.98 s | 13.13 s |
| Median | 15.99 s | 13.52 s |

The median elapsed time decreased by 15.4%. All 24,576 received file instances
passed SHA-256 and timestamp verification; all runs reported bundle usage.
This is a same-drive Windows loopback measurement with background applications
running, not a prediction for WAN transfers. Timings include startup, scanning
and index exchange; verification is outside the timed interval.

To compare two bundle builds with the reusable harness, pass
`--baseline-binary /path/to/previous/syncthing` alongside `--binary`. This enables
bundles for both builds and alternates their run order.

Three Windows same-drive loopback trials of 2,048 random 16 KiB files measured
11.43, 10.21 and 10.10 seconds with ordinary sync, versus 10.62, 14.40 and 9.69
seconds with bundles. All 12,288 received files passed SHA-256 and timestamp
verification; receiver counters confirmed bundles were used (52, 64 and 50
frames). Times include startup, scans and index exchange. Production sync and
other applications remained running and caches were not cleared. These ranges
overlap: the durable pipeline is **not a validated speed improvement**, and
the result is not a WAN measurement. It must not be confused with a plain TAR
copy that omits normal sync finalization.

A final smoke test of 8,192 random 16 KiB files measured 65.69 seconds with
ordinary sync and 32.79 seconds with the complete bundle pipeline. It used
exactly 256 bundle frames; all 16,384 received file instances passed content
and timestamp verification. This single larger pair is promising but does not
establish a repeatable speedup given the mixed smaller trials and background
disk activity.

Reproduce against an isolated executable and a new disposable directory:

```console
python script/benchmark_bundles.py --binary /path/to/syncthing --work /new/benchmark-dir --files 2048 --trials 3
```

The harness configures only loopback peers, leaves production sync running,
keeps timings and bundle counters in `results.json`, and verifies contents and
timestamps outside the timed interval. It starts/stops only its own instances.

Production hardening adds bounded cancellation, a parent-traversal recheck before
staging, and timestamp application before the live rename. A forced receiver stop
exposed the old rename/timestamp window; the corrected build recovered all 2,048
files with their SHA-256 hashes and timestamps intact. A separate sender restart
also recovered all 2,048 files, cleared transient pull errors, and converged.
Both scenarios then passed existing-file edits, disabling bundles with continued
ordinary transfers, and deletion propagation. The full core suite passed locally.
These checks are enforced by Windows CI and before Windows release signing.
See `RELEASE-READINESS.md` for rollout gates and rollback instructions; the first
deployment remains an opt-in canary, with batching disabled by default.

```console
python script/smoke_bundle_recovery.py --binary /path/to/syncthing --work /new/recovery-dir --files 2048
```

Native backup, protocol, configuration, upgrade, API, Syncthing service, and model
unit suites passed locally. Protocol and backup race checks passed. Transport
tests cover negotiation, stock peers, bounds, truncated/corrupt framing,
per-file errors, cancellation, and retry through ordinary requests after a
failed extension. The release workflow reruns core suites before signing.

An isolated two-instance smoke test of the earlier local-folder backup also
verified version retention, concurrent-edit conflict convergence with losing content
retained, and an interrupted receiving process followed by successful resumption
and SHA-256 verification of 1,024 newly added files. Signed-archive tests reject
both a modified executable and a renamed version/platform archive.

## Google Drive backup

The Drive API backup is covered by unit tests against an in-process fake of the
Drive and OAuth endpoints. It has not yet been validated against a real Google
account.

In Edit Folder > Advanced, connect a Google account, enable backup, and save.
Each backed-up folder gets its own Drive folder, by default
`Syncthing backup - <folder label>`. Inside it `Current` is browsable and
`Versions/<run>` retains overwritten content indefinitely. Source deletions do
not remove backup files. Syncthing remains the primary sync engine; this is a
one-way upload and nothing in Drive is ever written back to the source.

Syncthing uses the Drive API with the `drive.file` scope. It can only see and
change the folders and files it created itself, never the rest of the Drive.
The Drive folder is found again through private app properties keyed by device
and folder ID, so it may be renamed or moved in Drive. Two devices backing up
the same folder ID get separate Drive folders.

### Connecting an account

Google requires an OAuth client. Release builds may embed one with
`-ldflags "-X github.com/syncthing/syncthing/lib/cloudbackup.GoogleClientID=...
-X github.com/syncthing/syncthing/lib/cloudbackup.GoogleClientSecret=..."`;
`STGOOGLECLIENTID` and `STGOOGLECLIENTSECRET` override it. Otherwise the GUI asks
for one:

1. In Google Cloud Console, create a project and enable the Google Drive API.
2. Configure the OAuth consent screen, add the `drive.file` scope, and publish
   the app (`In production`). Apps left in `Testing` receive refresh tokens
   that expire after seven days. `drive.file` is not a restricted scope, so
   publishing does not require Google verification.
3. Create an OAuth client of type `Desktop app` and enter its ID and secret.

Connect Google Drive opens Google's consent page. Google then redirects the
browser to a temporary listener on `127.0.0.1`, so authorize from a browser on
the computer running Syncthing. The refresh token, the account address and a
GUI-entered client are stored in Syncthing's database, not in `config.xml`.
Disconnect revokes the token and deletes it locally.

### Behavior

Uploads are verified: Drive's MD5 checksum must match the content read locally,
and the status reports `complete` with `cloudVerified` only when every file in
the run verified. Unchanged files are detected by size, modification time and
MD5, so a run still reads every file locally. Overwritten files are first
copied server-side into `Versions`. Files of 5 MiB or more use resumable
uploads. Free Drive quota is checked before each upload.

Rate limits, server errors and dropped connections slow the whole run down
rather than skipping files: the delay before each upload doubles from one
second up to 64 seconds and halves again after each success. A file is retried
up to eight times, without holding the source open while waiting, before it is
reported and left for the next run. Other errors, such as a revoked
authorization, are not retried. The status counts files left for the next run
(`failed`), and lists the first ten errors.

Backups include dependencies and files ignored by primary sync. Only Playwright
traces and internal Syncthing state are excluded. Links and junctions are
reported as errors and never followed. File modification times are preserved;
permissions and directory metadata are not. A changing or locked file leaves
its previous Drive copy intact and reports an error for retry. A file/folder
type change reports a conflict instead of replacing the Drive entry. This is a
per-file backup, not an application-consistent snapshot. The Drive folder
contains everything included from the source, potentially including sensitive
files: choose its sharing accordingly. Drive's per-user write rate means
folders with many small files take a long first run.

On macOS, the standard system aliases `/var`, `/tmp`, and `/etc` are resolved
to their expected `/private` paths before source validation. Other symlinks,
including links within the source folder, remain excluded.

Authenticated endpoints:

- `GET /rest/folder/cloudbackup?folder=ID`: current status and Drive folder link.
- `POST /rest/folder/cloudbackup?folder=ID`: queue a run using saved settings.
- `GET /rest/cloudbackup/google`: account status (never credentials).
- `POST /rest/cloudbackup/google/connect`: start authorization; optional body
  `{"clientID": "...", "clientSecret": "..."}`; returns `{"url": "..."}`.
- `DELETE /rest/cloudbackup/google`: revoke and forget the account.

Backup yields while any enabled primary folder is starting, scanning, syncing,
or cleaning. Its status becomes `waiting-for-primary`, then returns to
`uploading` after primary work has been idle for a second. An in-flight upload
is abandoned and its source handle closed before waiting; Drive keeps the
previous copy, and the file is retried using fresh source metadata. This also
applies to manually queued backups.

Changes to unrelated settings do not cancel an active backup. Changing backup
settings, folder paths, or paused state still cancels the old run.

Earlier fork versions copied into a local folder for Google Drive for desktop.
That mode is removed; its `path`, `minFreeBytes` and `ownerID` settings are
ignored, and existing copies are left untouched. The first Drive API run
uploads everything again.

## Windows Explorer sync status

On Windows, Edit Folder > Advanced > Show Sync Status in Windows Explorer
enables status for an individual folder. It is off by default. Explorer's
Status column shows synced files, pending downloads or pull errors, and
excluded files matched by `.stignore`. A pending file also marks its parent
directories pending. Windows marks a local edit pending until Syncthing scans
it; updates may take a few seconds.

This uses Windows Cloud Files registration rather than overlay-icon slots.
All file contents remain on disk; this feature does not provide online-only
files. It requires Windows Cloud Files support and a compatible local
filesystem. Registration failures are recorded in Syncthing's log. The
registration belongs to the Windows account running Syncthing.

Turning the option off removes the registration and returns placeholders to
ordinary files. Exiting Syncthing preserves the registration for its next
start. Syncthing's internal files and symbolic links are left alone. Existing
online-only cloud files remain excluded from the scanner's placeholder fix.

The service tests cover status selection, parent propagation, paging, and
internal-file exclusions. A Windows integration test creates real Cloud Files
placeholders and checks scanning and readable content. The initial two-instance
smoke test also checked blocked downloads, edits, restart, disabling the option,
and unchanged index sequence after enabling it and rescanning. Explorer states
were checked through shell properties; the icons were not visually inspected.

## Modern theme

The web UI uses the `modern` theme by default. New configurations get it
directly; on first start after updating, an install still on the stock
`default` theme is switched to `modern` once (recorded in the database as
`forkModernThemeApplied`). A theme picked in Settings afterwards, including
`default`, is kept.

## Signed automatic updates

The feed is `https://raw.githubusercontent.com/ecoded99/syncthing/update-feed/releases.json`.
It is refreshed after each signed release and served without GitHub REST API
quotas. Archive URLs point directly to public release downloads; the native
updater still verifies their signatures against its embedded public key.
Existing installations using the GitHub API feed can set Releases URL under
Actions > Advanced > Options to this address without restarting.
Enable
Automatic Upgrades in Syncthing settings and allow prereleases on canary PCs.
Versions named `vX.Y.Z-ecoded.N` are built and signed by the fork workflow.
Stable-channel PCs wait until a release is promoted out of prerelease. The release
workflow builds Windows amd64, macOS Apple Silicon (arm64), and macOS Intel
(amd64), and runs the fork's tests natively on each platform before publishing.

Windows archives contain `syncthing.exe`; macOS archives contain `syncthing`
with executable permissions. Both include `release.sig`, `LICENSE`, and
`FORK.md`. macOS ZIPs are named `syncthing-macos-arm64-vX.Y.Z-ecoded.N.zip`
or `syncthing-macos-amd64-vX.Y.Z-ecoded.N.zip` so the native updater can select
the correct build. `SHA256SUMS.txt` covers all three release archives.
The signature covers the archive filename, a newline, and the executable bytes
using the native updater's
verification format. The fork public key is embedded in the binary; its PEM
SHA-256 is `afdf6df1b1d5584f46499243b94c067571fecb44b1e814b1ab5a49ca723e831c`.
The private key is never included in source or release artifacts.

On macOS, extract the matching archive and run `./syncthing` from Terminal;
the browser GUI provides sync, Drive backup, and manual update checks. The tray
icon, Explorer status integration, and power-throttling opt-out are Windows-only.
These macOS releases are command-line executables, not app bundles, and are not
signed with an Apple Developer ID or notarized. The fork's updater signature is
separate from Apple's code signing.

The native updater saves the previous executable with an `.old` suffix. This is
not an automatic health-check rollback. Preserve config and database backups
before initial migration; restore the previous executable if startup fails.

No project data, credentials, device configuration, backups, or raw diagnostic
logs belong in this repository. Upstream infrastructure workflows are retained
with `.upstream-disabled` suffixes; only fork checks and signed releases execute.

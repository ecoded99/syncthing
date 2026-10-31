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

## Validation

On a Windows loopback fixture of 8,192 random 16 KiB files (128 MiB), three
ordinary runs measured 39.56, 47.80, and 42.14 seconds. Three batched runs measured
44.75, 42.67, and 42.06 seconds. All 49,152 received files passed SHA-256 and
modification-time verification. The ranges overlap: batching is **not a validated
speed improvement**. This is not a WAN benchmark. Some runs overlapped development
activity; these figures reject an improvement claim, rather than establish a
precise performance difference. Reducing messages does not eliminate the
receiving filesystem's per-file work.

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

## Modern theme

The web UI uses the `modern` theme by default. New configurations get it
directly; on first start after updating, an install still on the stock
`default` theme is switched to `modern` once (recorded in the database as
`forkModernThemeApplied`). A theme picked in Settings afterwards, including
`default`, is kept.

## Signed automatic updates

The feed is `https://api.github.com/repos/ecoded99/syncthing/releases`. Enable
Automatic Upgrades in Syncthing settings and allow prereleases on canary PCs.
Versions named `vX.Y.Z-ecoded.N` are built and signed by the fork workflow.
Stable-channel PCs wait until a release is promoted out of prerelease. Windows
amd64 is the initial supported release platform.

Archives contain `syncthing.exe` and `release.sig`. The signature covers the
archive filename, a newline, and the executable bytes using the native updater's
verification format. The fork public key is embedded in the binary; its PEM
SHA-256 is `afdf6df1b1d5584f46499243b94c067571fecb44b1e814b1ab5a49ca723e831c`.
The private key is never included in source or release artifacts.

The native updater saves the previous executable with an `.old` suffix. This is
not an automatic health-check rollback. Preserve config and database backups
before initial migration; restore the previous executable if startup fails.

No project data, credentials, device configuration, backups, or raw diagnostic
logs belong in this repository. Upstream infrastructure workflows are retained
with `.upstream-disabled` suffixes; only fork checks and signed releases execute.

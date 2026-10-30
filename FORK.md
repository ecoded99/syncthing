# ecoded Syncthing

This experimental fork keeps Syncthing as the primary sync engine. It adds a
secondary, browsable, one-way backup into a folder managed by Google Drive for
desktop, and an optional batched small-file transport between compatible peers.

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

## Secondary backup

In Edit Folder > Advanced, choose a separate backup destination already managed
by Google Drive for desktop, enable backup, and save. `Current` is browsable;
`Versions` retains overwritten content indefinitely. Source deletions do not
remove backup files. Syncthing remains the primary sync engine.

The source marker must exist. Destinations must not overlap synchronized folders
or other backup targets. The default interval is hourly and the default free
space reserve is 15 GiB. A nonempty legacy backup requires an explicitly matching
`cloudBackup.ownerID`; stop its previous worker before enabling the native one.
Ownership metadata prevents accidentally adopting unrelated directories.

Backups include dependencies and files ignored by primary sync. Only Playwright
traces and internal Syncthing state are excluded. Links and junctions are
reported as errors and never followed. Contents and file modification times are
preserved; directory metadata and ACL preservation are not guaranteed. A changing
or locked file leaves its previous backup intact and reports an error for retry.
A file/directory type change reports a conflict instead of deleting retained
content. This is a per-file backup, not an application-consistent snapshot.

Google Drive for desktop uploads the backup destination. Local completion does
not confirm cloud upload. The destination contains everything included from the
source, potentially including sensitive files: choose its sharing accordingly.
No cloud credentials are needed by Syncthing for this local-mirror integration.

Authenticated endpoints:

- `GET /rest/folder/cloudbackup?folder=ID`: current status.
- `POST /rest/folder/cloudbackup?folder=ID`: queue a run using saved settings.

Status counts changed files copied in the current run, not cloud uploads. The
GUI exposes configuration, status refresh, and a manual run control.

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

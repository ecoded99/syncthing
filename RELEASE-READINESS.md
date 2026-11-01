# Whole-file bundle release candidate

This change adds an opt-in initial-copy pipeline on top of the existing bounded
STB1/STR1 transport. It does not change the database schema or wire capability
version. Both the older fork and newer fork can serve bundles; stock peers use
ordinary requests. The setting remains `experimentalBatchedTransfers=false`
by default, with the existing per-folder checkbox as the rollout control.

## Release gates

- Build and run the fork's full core suites on Windows, Linux, macOS arm64 and
  macOS amd64 through `Fork checks` before merging.
- Run protocol and model bundle tests under the race detector on non-Windows
  CI runners. Release builds repeat these tests on both macOS architectures.
- Fuzz both request and response framing decoders on Linux. They must reject
  malformed frames and preserve the existing member/data bounds.
- Verify the candidate with an isolated interrupted receiver and sender
  disconnect/reconnect, checking every received file's SHA-256 and timestamp.
- Keep the first signed release a prerelease. Tag creation invokes the existing
  signing workflow and publishes to the updater feed; creating a PR does not.

## Candidate behavior

Groups contain at most 32 files of at most 64 KiB each. All successful members
are hash-verified before staging. At most three bundle payloads are active;
staging and finishing workers and their queues remain bounded. Source reads
retain ordinary per-member authorization. Staging creates temporary files
exclusively, rechecks parent traversal immediately before opening, and preserves
raced-in temporary files. Normal finalization retains fsync, local-edit/conflict
checks, metadata application and database updates.

Cancellation stops new staging without invalidating the remote capability just
because a local folder stopped. Existing local files, existing temporary files,
large/multiblock files, encrypted folders and incompatible peers retain ordinary
transfer behavior. Failed bundles retry through the ordinary puller.

## Canary and promotion

1. Publish a signed prerelease only after the branch checks pass. The signing
   key stays in the existing GitHub Actions secret; local test binaries are not
   signed updater releases.
2. Update two canary devices and enable the folder checkbox on the receiver.
   Confirm `batchRequestsSent` increases in `/rest/system/connections`, both
   directions converge, and normal edits, conflicts and restarts behave correctly.
   Small-file counts and disk latency matter more than raw link bandwidth.
3. Check pull errors, source/receiver hashes and durable restart recovery before
   promoting the release. Compare performance on a representative real folder;
   the recorded loopback gains do not predict WAN performance.
4. Disable the folder checkbox to return to the ordinary pipeline. Existing
   staged files are ordinary Syncthing temporary files and remain resumable.
   If reverting the executable, use the previous signed release and retain the
   normal config/database backups. No automatic health-check rollback is added.

## Reproducible benchmarks

See `FORK.md` for measured timings. To compare the current executable against
the previous bundle implementation with bundles enabled in both builds:

```console
python script/benchmark_bundles.py --binary CURRENT --baseline-binary PREVIOUS --work NEW_DIRECTORY --files 4096 --trials 3
```

The benchmark starts only isolated loopback peers, verifies contents/timestamps
outside the timed interval and saves timings plus bundle counters in
`results.json`. Keep production activity and cache conditions in the report.

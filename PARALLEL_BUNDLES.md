# Bounded bundle stage parallelism

Source reads and explicit bundle checksum verification now use up to four workers, capped by GOMAXPROCS. Hashing stays sequential below 256 KiB. Source responses are copied and closed immediately to release normal request limiters; frames are assembled in request order into a preallocated buffer. No extra network connections were enabled.

## Local QUIC comparison

Baseline: commit `42b78db` (`batch-stats-experiment.exe`). Candidate: `batch-parallel-experiment.exe`. Both enable the existing experimental bundle path and adaptive 16..32 file admission. Disposable peers on this desktop, 16 KiB random files, durable writes, three alternating before/after trials. Each result waits for receiver index completion and then verifies file contents and modification times.

| Files | Before (s), trials | After (s), trials | Median before | Median after | Time reduction |
| --- | --- | --- | --- | --- | --- |
| 512 | 3.805, 3.790, 3.823 | 3.788, 3.575, 4.562 | 3.805 s | 3.788 s | 0.4% |
| 2048 | 7.566, 8.062, 7.865 | 6.717, 6.843, 6.384 | 7.865 s | 6.717 s | 14.6% |

The smaller fixture shows no meaningful gain. The larger fixture improved in all three comparisons, but this small local sample does not establish performance on the real desktop-to-laptop link or isolate the contribution of each code change. File creation, durable finalization, and index work still contribute to total time.

## Checksum microbenchmark

Three runs per size/worker count. Median verification time for a 32-member bundle:

| Member size | 1 worker | 4 workers | Speed ratio |
| --- | --- | --- | --- |
| 4 bytes | 4.769 us | 8.030 us | 0.59x |
| 4 KiB | 85.245 us | 51.593 us | 1.65x |
| 16 KiB | 330.563 us | 128.277 us | 2.58x |
| 64 KiB | 1309.737 us | 381.339 us | 3.43x |

This measures checksum verification only. The conservative 256 KiB bundle threshold avoids parallel scheduling on tiny inputs. Faster checksumming does not imply the same end-to-end speed ratio.

## Verification

- Targeted protocol/model race tests passed three repetitions. The full protocol race suite also passed.
- New tests check concurrent overlap, four-worker limits, one-response limiter progress, response release and pooled-buffer reuse, result ordering, per-member errors, corruption at different positions, and cancellation.
- All 12 comparison transfers verified every file and timestamp.
- Receiver crash recovery and sender reconnect each converged with 2,048 verified files. Existing-file edits, disabling bundles, ordinary transfers after disabling, and deletions passed in both scenarios.

Raw measurements are in `C:/Users/eiad_/Documents/ChatGPT/Robot Vac/work/parallel-bundles-da6a6025`.

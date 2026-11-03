# Adaptive bundled-file application

When experimentalBatchedTransfers is enabled, initial-copy bundle reception
starts with 16 admitted files per folder and can grow to 32. These are file
lifetimes, not 32 network connections. Existing-file changes retain their normal
conflict-aware receiver path.

Admission covers staging, per-file flushing and close, publication and index
handoff. The existing maxConcurrentWrites setting independently limits writes.
No durability, conflict, timestamp or index validation is skipped.

Every two seconds, a background sampler checks system CPU and memory pressure,
successful completed bytes, and file completion latency. Two busy healthy
intervals allow a four-worker trial. The trial is retained only if throughput
improves by at least 5% without a completion-latency increase over 50%.
High CPU (85%), memory use (90% or under 512 MiB available), or doubled
completion latency accompanied by falling throughput reduces admission by four,
with a 30-second cooldown. The floor remains 16. Unavailable telemetry restores
16. Idle periods do not trigger growth. Adjustments appear in the normal log.

Completion latency is a filesystem-pressure proxy. This does not directly
measure disk hardware latency or interactive UI responsiveness. Each folder has
its own controller; it is not a machine-wide concurrency budget.

Source-only change: applying it to an installed copy requires deploying this
build and enabling the experimental bundle feature. Existing folder settings
and running installations are not changed by building or testing this code.

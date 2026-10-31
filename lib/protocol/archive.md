# Private authenticated archive request extension

`RequestArchive(ctx, conn, requests)` is optional. It does not change the public
`Connection` interface or generated mocks. Known wire-format and encryption
wrappers delegate it; unknown wrappers, stock peers and encrypted folders return
`ErrArchiveUnsupported`. The caller retains ordinary block-request fallback.

ClusterConfig unknown field 3010 advertises version 1. Request unknown field
3011 carries `STA1`, a bounded varint count and length-delimited ordinary Request
protobufs. This is independent of STB1 fields 3000/3001. Both peers must advertise
support. The outer request binds the frame with SHA-256 and declares the sum of
member bytes. Its name is `.starchive-v1`; field presence selects this service.
A normal user file with that name still uses ordinary Request handling. Empty,
duplicate, wrong-type, nested and mixed archive extensions fail closed.

Members must be unique canonical NFC slash-relative names in one folder, with
offset/block zero, no temporary-source requests and SHA-256 hashes. Limits are
1024 members, 64 KiB per member, 16 MiB aggregate payload and 2 MiB request
metadata. The stored ZIP is limited by archivebatch.MaxArchiveBytes, currently
less than the existing 32 MiB MaxRequestSize. Ordinary BEP's 500 MB message limit
is unchanged; archive-specific bounds apply after BEP decoding. This does not
claim that an authenticated hostile peer cannot exercise the larger existing
BEP allocation limit.

The sender admits one archive at a time with TryLock, never a waiting archive
queue. It calls the normal wrapped model Request for each member, copies the
data and immediately closes that response before opening another member. Thus
ordinary authorization, source consistency and permit accounting remain active.
Pack checks exact source length/hash. Any member failure discards the entire ZIP.
The response lease is released when normal BEP response handling closes it.

The client checks total ZIP size, framing, exact member names/counts, stored-file
type and declared member sizes. The model must still fully validate/stage the
ZIP against its own expected hashes, modification times and index versions,
then retain its ordinary conflict, permission, fsync, publication and indexing
rules. Sender ZIP timestamps are not trusted metadata.

Statistics.ArchiveRequestsSent counts successful receipt after those structural
checks. ArchiveResponsesSent counts successful bounded ZIP production. Neither
counter means that receiver staging/publication succeeded. Unsupported requests
and failed archives do not increment these counters.

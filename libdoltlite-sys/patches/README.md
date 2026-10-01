# Local DoltLite patches

Files under `../doltlite/` are vendored, unmodified upstream artifacts. Do not
make rusqdoltlite changes there: `upgrade_git.sh` replaces them on every
upstream refresh.

The bundled build copies `doltlite.c` into Cargo's `OUT_DIR`, applies the
numbered standard Git patches with `git apply`, and compiles that generated
copy. This keeps the upstream source replaceable while making local behavior
explicit and reviewable. Git is therefore required when compiling the bundled
DoltLite source.

To update DoltLite:

1. Set `DOLTLITE_GIT_REF` in `../upgrade_git.sh` to the upstream release tag.
2. Run `../upgrade_git.sh`. It builds the pristine amalgamation from a fresh
   clone of that exact ref, vendors matching remote sidecars, regenerates
   bindings, and runs the validation workflow.
3. If a patch no longer applies, check whether upstream incorporated that
   behavior. Remove the obsolete hunk or refresh only that patch; never edit
   `../doltlite/doltlite.c`.
4. Commit the upstream artifact update separately from any patch adjustment
   when possible.

`git apply` deliberately fails when the standard patch context no longer
matches. A failed build is an upgrade-review signal, not permission to silently
skip a local change.

Keep each patch scoped to one source tree. A behavior may have coordinated
parts in the DoltLite and CBS series; apply each series directly to its own
staged source tree and document any dependency between them. The numbered core
patches apply to `doltlite.c`; remote-server sidecar patches apply to the
staged `doltlite_remotesrv.c` and `doltlite_remotesrv.h` files before the
server is appended to the amalgamation. Patch `0010` also applies to the staged
`doltlite_tls.c` sidecar so its opaque connection allocation matches the
amalgamation's `DoltliteConn` layout. Currently, `0001` and `0002` add
`dolt_remote('set-url', ...)`, `0003` adds the CBS upload lock hooks, and
`0004-remote-server-vfs.patch` lets each native remote server select an
optional named SQLite VFS while preserving the process default when omitted.
`0005-doltlite-no-seed.patch` adds an internal open flag that lets the CBS
checkpoint connection skip DoltLite's initial graph seed without changing
ordinary application opens. `0006-http-noop-push-commit.patch` finalizes a
successful no-op HTTP push through `/commit`, allowing capability-scoped remote
sessions to publish their accepted refs check. `0007-bcvfs-empty-store-initialization.patch`
adds a NO_SEED-only native helper that commits an empty refs table for a new
CBS DoltLite store without a SQL write that would create an unborn zero-tip
branch. `0008-http-upload-batch-16mb.patch` reduces the maximum buffered HTTP
upload batch from 32 MiB to 16 MiB.
`0009-remote-server-chunk-limit.patch` keeps the public 64 MiB chunk and
128 MiB request limits. A session-owned, URI-backed IPv4 loopback RemoteServer
may receive larger push bodies up to the native signed-`int` request bound
(2,147,483,647 bytes); the largest single Prolly chunk is 24 bytes less to
account for its hash and length framing. `0010-http-remote-idle-timeout.patch`
makes the HTTP client timeout a 30-second inactivity limit, refreshed after
successful reads and writes. `0011-http-idle-timeout-url.patch` adds the
request-local `http_idle_timeout_ms` URL option, capped at five minutes.
Positive URL values take precedence over `DOLTLITE_HTTP_TIMEOUT_MS`, which
remains the default idle-timeout override. `0013-loopback-unbounded-idle-timeout.patch`
allows the explicit value `0` only for clear HTTP URLs whose host is a numeric
IPv4 address in 127/8. `RemoteServer::database_url` uses that value only for
URI-backed session loopback servers, so their Dolt requests can wait while the
same-process server performs cloud writes. Ordinary remotes and TLS session
URLs retain their configured inactivity limits. TCP connect and TLS handshake
remain bounded at 30 seconds. The staged server TLS struct change keeps socket
descriptors aligned with the patched amalgamation. `0014-dolt-push-transfer-progress.patch`
adds an opt-in, per-connection Rust callback for logical Dolt chunk transfer
progress without changing `dolt_push` SQL or HTTP behavior. It plans the fixed
set of reachable destination-missing chunks before upload and retains only
hash/length metadata. Synchronous remote writes count after success; HTTP writes
count only after the complete chunk batch receives a successful response. These
logical chunk totals are separate from CBS block progress, and the callback
registration is scoped by a Rust guard.

`0015-http-get-chunks-adaptive-batch.patch` retries oversized `/get-chunks`
requests using smaller hash ranges when the peer returns HTTP 413 or the native
HTTP reader reaches its fixed 128 MiB response limit. It keeps each response
within the public server limit and preserves result ordering; a single chunk
that still exceeds the limit remains an error.
`0016-http-chunked-response.patch` adds strict HTTP/1.1 chunked response
decoding for remotes that stream large reads without a Content-Length. It
validates bounded chunk extensions and trailers, caps encoded and decoded bodies
at 128 MiB, and recognizes the final chunk on a keepalive connection without
waiting for EOF.
`0012-http-transport-error-context.patch` preserves the HTTP transport phase
when a socket write/read or response parse fails. It reports the configured
idle timeout as context without claiming every socket error was a timeout. It
does not include the URL, headers, credentials, or provider response body, and
preserves existing SQLite return codes, retry decisions, and timeout behavior.
Patches under `blockcachevfs/` apply only to the staged CBS sources; patches in
this directory apply only to DoltLite sources.

Keep new behavior isolated the same way so an upstreamed fix can be removed
without rebasing unrelated changes.

`0003-support-blockcachevfs-upload-lock.patch` exports the DoltLite graph-lock
hooks used by the CBS upload integration. The upload-side patch holds that
lock across checkpointing and manifest installation so an upload observes a
stable block set; a database without a DoltLite store reports `SQLITE_NOTFOUND`
and retains CBS's stock behavior.

The blockcache VFS patch series is applied separately to the staged CBS
sources by `libdoltlite-sys/build.rs`, after the vendored VFS files have been
copied to `OUT_DIR`. `0013-streaming-and-serverless.patch` combines the bounded
staged-cache and serverless-upload-session changes. It gives
`cachefile.bcv` a block-aligned hard payload-slot limit and reuses those slots
instead of growing the file or allocating one local payload per evicted block.
On restart, persisted clean-slot mappings are discarded before the cache file
is reconciled: a clean-only tail may be truncated for a smaller limit, while
a retained dirty or unpublished high-water slot makes that reduction fail
rather than silently discarding local data. There is no general online cache
compaction.
At the cache watermark, complete immutable block objects may be PUT to the
provider and recorded in staged metadata, but those objects remain
unpublished until the existing explicit `sqlite3_bcvfs_upload` operation
successfully installs a conditional manifest. Database `xSync` forwards to the
cachefile's `xSync` and syncs local payload bytes only; it neither stages data
nor publishes a manifest. Dirty bytes written before `xSync` are outside the
committed local-durability guarantee if power is lost. The patch also
invalidates persisted clean-cache slot mappings at startup, forcing published
blocks to be fetched again rather than treating a clean slot as durable.

The patch records the logical container/database/block generation and object
ID, retains staged mappings across slot reuse and restart, and keeps them for
retry when a block PUT fails or manifest publication is lost to a CAS conflict
or an ambiguous response. Rewrites and truncates tombstone superseded staged
generations in `staged_gc`; their remote object IDs remain tracked for delayed
garbage collection instead of being deleted immediately. The next eligible
manifest publication adds those IDs to the manifest's existing delayed-GC
list, while a failed publication leaves the tombstones available for retry. A
pending-publication marker and sequence counter allow startup to reconcile a
remote manifest with local staged generations. The publication path only
attempts cleanup after a successful conditional
manifest operation, while retained mappings remain available if cleanup is
interrupted. Metadata initialization uses `synchronous=FULL` for this bounded
path because `synchronous=OFF` cannot provide the ordering needed before a
slot is reused. The implementation's crash and power-loss behavior is still
defined by the corresponding fault-injection tests; this patch does not claim
secure erasure or a bound on
all local disk bytes. Staging metadata and retryable remote objects can grow
independently of the bounded block-payload file.

The CBS test-only changes live in the separate
`blockcachevfs-tests/0014-streaming-and-serverless-tests.patch`; the emulator
runner applies it after the production patch so `build.rs` never stages test
sources into the library build.

`blockcachevfs/0014-cbs-create-upload.patch` lets CBS create and upload a new
local database before remote blocks exist. The separate DoltLite patch above
lets the CBS checkpoint-only open skip the initial seed commit; ordinary
application opens keep their seed behavior. Each patch series is applied
directly to its own staged source tree.

`blockcachevfs/0016-session-publication-after-ordinary-upload.patch` lets a
fresh session replace an older terminal `COMMITTED` publication record after
an ordinary, non-session upload advances the manifest. It still requires the
live manifest ETag to equal the fresh session's captured base, and the
conditional manifest write preserves stale-base rejection. Pending
`PUBLISHING` records remain fenced.

`blockcachevfs/0019-gcs-crc32c-integrity.patch` sends CRC32C on GCS JSON media
uploads, checks the server checksum before CBS consumes full-object downloads,
and rejects short blocks before cache writes.

`blockcachevfs/0023-storage-failure-diagnostics.patch` records the first
terminal storage failure observed by a session-owned URI VFS as a safe phase
and an explicitly typed HTTP status or SQLite result code. It covers block
transfers, local cache I/O, session protection, checkpoint and accepted-head
records, and publication writes. It omits URLs, credentials, and provider
response text; transient retries, a missing cleanup guard, expected head CAS
conflicts, and verified immutable-object reuse are not reported as failures.

`blockcachevfs/0024-upload-plan-progress.patch` extends the 0022 progress
callback with a fixed block-work plan. After the explicit final checkpoint has
quiesced the WAL, CBS counts the target container's remaining unpinned dirty
blocks and reports that remainder alongside already completed upload/reuse
counts. The plan includes newly uploaded and verified-reused blocks; bytes are
full block payload sizes, not predicted network traffic, and exclude WAL or
publication metadata. No plan is reported while the producer is active or
when dirty entries are blocked.

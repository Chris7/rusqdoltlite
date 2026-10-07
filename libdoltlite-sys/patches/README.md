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
server is appended to the amalgamation. Patch `0009` also applies to the staged
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
`0009-http-session-transfer-and-progress.patch` is one regenerated net patch
for the former DoltLite patches 0009-0016. Its retention checklist is also in
the patch header so an upstream refresh keeps every behavior. It keeps the
public RemoteServer limits at 64 MiB per chunk and 128 MiB per request, while a
session-owned numeric IPv4 loopback server may accept a request up to the native
signed-int bound (with 24 bytes reserved for Prolly hash and length framing).
HTTP operation timeouts are inactivity limits refreshed on successful reads and
writes; connect and TLS handshake remain bounded at 30 seconds. The request URL
option `http_idle_timeout_ms` is capped at five minutes and takes precedence
over `DOLTLITE_HTTP_TIMEOUT_MS`; zero is accepted only for clear HTTP URLs with
a numeric address in 127/8, as used by URI-backed session loopback servers.
Transport diagnostics retain fixed phase and configured timeout context without
including URLs, headers, credentials, provider bodies, or false timeout claims.

The same patch provides an opt-in per-connection Rust callback for logical Dolt
chunk progress without changing public SQL or HTTP behavior. It plans reachable,
destination-missing chunks while retaining hashes and lengths only, counts
synchronous writes after success and HTTP writes after a complete successful
batch, retries oversized `/get-chunks` reads with smaller ranges after HTTP 413
or the fixed 128 MiB response limit, and decodes strict HTTP/1.1 chunked bodies
with bounded extensions, trailers, and encoded/decoded sizes. A single chunk
that exceeds the limit remains an error, and the final chunk is recognized on a
keepalive connection without waiting for EOF.
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

The consolidated CBS production patch is
`blockcachevfs/0017-session-upload-safety-and-parallel-staging.patch`. Its patch
header lists every former 0017-0025 intent as an upstream-retention checklist.
It stores payload blocks under `blocks/<block-id>.bcv` with no flat-key fallback,
requires create-only session writes and exact-byte verification for immutable
object reuse, and fences each attempt marker with before/after snapshots of the
server-owned cleanup guard and its idle epoch. Google JSON media writes include
CRC32C and full-object downloads are checked before CBS consumes them; short
blocks are rejected before cache writes. The Cargo static-curl build preserves
explicit CA settings, honors `SSL_CERT_FILE` and `SSL_CERT_DIR`, and selects
readable conventional CA locations without weakening TLS verification.

For session-owned Google JSON connections, CBS refreshes Bearer credentials at
the request dispatch boundary and after one 401/403, replaying the same body and
generation conditions once. A live upload retry accepts an exact same-byte replay
of an immutable block, while a fresh process rehydrates only an accepted durable
checkpoint/head; unaccepted overlay mappings are rebuilt from Gen's durable graph,
and attempt markers do not promise bandwidth-free resume for previously staged
blocks. It reports completed physical block uploads and
verified reuse separately from DoltLite's logical chunk progress. The URI-VFS
callback runs synchronously, and Rust callback panics are contained without
changing storage or publication results. After the explicit final checkpoint,
it reports remaining unpinned dirty blocks as a fixed plan; totals count full payload blocks rather than network traffic or WAL
and publication metadata. The first terminal storage error retains a safe phase
and explicitly typed HTTP or SQLite cause without URLs, credentials, or provider
response text.

The patch also batches dirty blocks for watermark staging and the final session
drain. Session markers remain fenced serially before payload PUTs are queued;
effective concurrency is bounded by `upload_concurrency`, 64 blocks,
cache capacity, and 64 MiB of copied payload, with a larger single block staged
alone. Create-only conflicts still require an exact-byte GET. Every queued
request drains, successful siblings are persisted independently, and completed
progress remains visible if another PUT fails. `Config::UploadConcurrency`
accepts values from 1 through `INT_MAX` and is only an upper bound; 1 retains
serial staging. The watermark measures dirty payload blocks so later writes
continue to form batches. DoltLite's 0009 patch
provides the separate logical chunk plan and callback; these CBS edits apply
only to build.rs's OUT_DIR copy and remain separate from the pristine vendored
CBS tree.

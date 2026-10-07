# Cloud Backed SQLite patches

These numbered patches apply only to the pinned CBS source staged by
`build.rs`. They are kept separate from the pristine vendored checkout under
`../../cloudsqlite`.
The Google storage module accepts an HTTP or HTTPS base URL through
`endpoint=<base-url>` while retaining the default
`https://storage.googleapis.com` endpoint and the `maxresults` option.
The default Google module protocol is its existing XML-compatible API;
`api=json` selects the Google Cloud Storage JSON API. JSON mode uses the
standard `/storage/v1` and `/upload/storage/v1` resources, sends the supplied
secret as a Bearer token, percent-encodes bucket/object names and list
continuation tokens, and uses `ifGenerationMatch` for upload/delete
preconditions. A JSON container may be written as `bucket/prefix`; the bucket
is created/addressed separately and the prefix is applied to object names and
removed from list results. Conditional downloads use the JSON API's
`ifGenerationNotMatch` generation query. For example, a local GCS-compatible
service can be selected with
`google?api=json&endpoint=http://127.0.0.1:4443`.

`0003-s3-module.patch` adds a built-in `s3` module. Use
`s3?endpoint=http://127.0.0.1:9000&region=us-east-1&maxresults=1000` for an
S3-compatible service; without `endpoint`, requests use the AWS virtual-hosted
endpoint for ordinary bucket names and `us-east-1`. Dotted bucket names use the
regional AWS path-style endpoint so TLS certificate validation succeeds. The
container is `bucket/prefix`, and a configured
prefix confines object and list operations; destroying such a container is
rejected rather than deleting the bucket. S3 requests use SigV4 with the
actual SHA-256 payload hash. Temporary credentials are passed as
`secret\nsession-token`; the session token is signed and sent as
`x-amz-security-token`. The `s3` endpoint option is path-style (including a
custom endpoint path). Bucket destroy is supported for empty buckets; the
module refuses to delete a configured-prefix container and does not
recursively delete objects. The upstream `util_destroy1` non-empty-container
case is intentionally excluded until a provider-specific object-delete
workflow is added.

`0012-s3-tls-and-verbose-logging.patch` selects the TLS-valid AWS path-style
endpoint for dotted bucket names and filters verbose libcurl output. When
`CurlVerbose` is enabled, only non-header libcurl diagnostic text remains on
stderr; raw HTTP headers and payloads are omitted. This keeps authorization
and temporary-session credentials out of the raw verbose HTTP output while
preserving useful connection information.

Google JSON bucket destroy has the same empty-bucket restriction and refuses a
configured prefix. The legacy Google XML destroy behavior is unchanged.

`0004-create-if-not-exists.patch` adds the safe bootstrap primitive used by
the Rust API. It creates `manifest.bcv` with provider conditional-create
semantics (`ifGenerationMatch=0` for Google JSON, generation `0` for Google
XML, and `If-None-Match: *` for S3) and falls back to provider bucket creation
after a missing-manifest/indeterminate (5xx) preflight or failed first
request. Existing manifests therefore fail without being
overwritten, including when two initializers race.

`0005-vfs-access.patch` makes the VFS `xAccess` callback report attached
container and manifest database paths accurately. This is required by
DoltLite's normal database-open sequence, which probes the container path
before opening the remote database.

`0006-manifest-file-size.patch` reports the finite block-rounded extent of a
database instead of CBS's large compatibility sentinel. This keeps clients
that scan file extents, including DoltLite's chunk store, within the attached
manifest's blocks.

`0008-safe-wal-header-read.patch` bounds CBS's WAL-header byte adjustments to
the requested read buffer. This avoids writing beyond short SQLite header
reads used by clients such as DoltLite; the final patch below removes the
header mutation entirely.

`0009-doltlite-lock-sidecar.patch` recognizes DoltLite's `.<db>-lock`
sidecar as an internal local lock file when the inner database exists in the
attached manifest. It keeps the container client reference and delegates
locking to the underlying local VFS without adding the sidecar to the main
database file list, writing a marker, or opening a cache file.

`0010-doltlite-truncate-noop.patch` permits a block-rounded no-op truncate
when the manifest database has no dirty-block allocation. Actual shrinking
still requires the allocation and uses the existing undirty path.

`0011-doltlite-upload-lock.patch` calls DoltLite's upload-lock hook around the
checkpoint for each uploaded database and keeps the resulting graph lock until
cleanup after the upload. `SQLITE_NOTFOUND` means the database is not a
DoltLite store and preserves CBS's stock checkpoint path; other hook errors
abort the upload. The Rust build enables this integration with the
`BCV_DOLTLITE_INTEGRATION` define; standalone CBS builds therefore retain the
stock path without requiring DoltLite symbols.

`0014-cbs-create-upload.patch` lets SQLite `CREATE` initialize a new local
database and supports checkpointing and uploading it before remote blocks exist.
The Rust build applies it after the preceding CBS patches. The CBS integration
passes DoltLite's private no-seed flag for its checkpoint-only open; the flag is
defined by `../0005-doltlite-no-seed.patch`, and normal application opens retain
their existing seed behavior. Callers add schema and data before upload.

`0015-preserve-database-header.patch` removes CBS's legacy byte-18/19 WAL-mode
rewrite from both database and proxy reads. CBS now returns stored database
header bytes unchanged; DoltLite seals its chunk-store header, so changing those
bytes invalidates the file. Uploads retain the `SQLITE_CHECKPOINT_TRUNCATE`
path and its held checkpointer snapshot lock for WAL databases. SQLite may
report `(-1, -1)` when a successful checkpoint finds no WAL present; the upload
path accepts that result and uploads any already-dirty main-file blocks without
requiring a `-wal` file. This does not imply that attached rollback-journal
writes are supported.

`0017-session-upload-safety-and-parallel-staging.patch` consolidates the
former 0017-0025 production patches as one regenerated net diff. Its patch
header is the change-retention checklist for future CBS upgrades; regenerate it
from the pinned pristine source after applying 0001-0016, and keep
`../../cloudsqlite` untouched.

The retained behavior includes the `blocks/<block-id>.bcv` immutable payload
namespace with no flat-key fallback. This path lets a client CAB grant access to
immutable payloads without granting mutable session-control objects. Session
writes are create-only, and an already-present object requires exact-byte
verification. A staged block PUT does not publish the database; only the explicit
`sqlite3_bcvfs_upload` operation can install the conditional manifest. Session
block writes
are protected by an attempt marker fenced between two reads of the server-owned
cleanup guard. Both observations must show the same idle epoch, or both must
show no guard; cleanup changes the idle epoch when it releases a sweep, and
clients retry a transition for up to 12 seconds.

Ordinary unscoped writers retain one shared writer-guard CAS before any
payload PUT in a staging batch. If cleanup owns SWEEP, the batch queues no
payload and leaves dirty local blocks for retry after the guard returns to
IDLE. Session-owned CAB VFS connections cannot write the shared guard; they
continue to use the read-idle, attempt-marker, read-epoch fence above. The
`bcvfs_session_gc.test` regression keeps the zero-PUT-under-SWEEP and
idle-transition recovery checks.

Google JSON media uploads include CRC32C. CBS verifies the matching checksum
before handing full-object downloads to cache consumers, rejects short blocks,
and preserves conditional 304 behavior. The Cargo-bundled static curl build
keeps `CLOUDSQLITE_CAINFO`, honors `SSL_CERT_FILE` and `SSL_CERT_DIR`, and uses
readable conventional CA paths only when neither environment setting is
present. It does not weaken peer or hostname verification, and standalone CBS
keeps its upstream curl configuration.

Session-owned Google JSON requests obtain a current Bearer token immediately
before dispatch, then refresh and replay the same request once after a 401/403.
The replay preserves its body and generation preconditions; a second denial is
terminal. Static auth and Google XML/S3 retain their existing behavior, and
token-bearing headers stay out of verbose output. A live upload retry accepts an
exact same-byte replay of an immutable block. A fresh process rehydrates only an
accepted durable checkpoint/head; unaccepted overlay-to-block mappings are
disposable and rebuilt from Gen's durable local graph. Attempt markers do not
reconstruct those mappings, so CBS does not promise bandwidth-free resume for
every block staged before acceptance. The optional URI-VFS progress
callback reports completed block uploads and exact-byte-verified reuse during
streaming staging, checkpoint flushes, and ordinary uploads. After the explicit
final checkpoint has quiesced the WAL, it reports the remaining unpinned dirty
blocks as a fixed plan. Plan bytes count full payloads, not predicted network
traffic, WAL, or publication metadata; no plan is emitted during production or
when dirty blocks are blocked. Callback code runs synchronously on the upload
thread, so callers must keep it quick and must not re-enter the same VFS. Rust
callback panics are contained and cannot change storage or publication results.

A session-owned URI VFS also retains its first terminal storage failure as a
safe phase and explicitly typed HTTP-status or SQLite-code cause. It covers
block transfers, local cache I/O, session protection, checkpoints, accepted
heads, and publication writes. It excludes transient retries, missing guards,
expected head CAS conflicts, verified immutable-object reuse, URLs, credentials,
and provider response text.

Dirty payload blocks are staged in bounded parallel batches both at the
watermark and during the final session drain. Attempt markers remain fenced
serially before payload requests are queued. `SQLITE_BCV_NREQUEST` accepts
values from 1 through `INT_MAX` and supplies an upper bound on concurrency; 1
retains serial staging. Effective batch size is
also capped at 64 blocks, cache capacity, and 64 MiB of copied payload;
a single larger block stages alone. Exact-byte checks remain mandatory for
create-only conflicts. All requests drain before buffers, pins, or the dispatcher
are released, and each successful PUT or verified reuse is recorded and cleaned
independently. Completed sibling progress remains visible when another request
fails. The watermark measures dirty payload blocks so later writes continue to
form batches; hard cache-capacity staging remains in place.

Ordinary final uploads continue scanning past each durable staged block so an
initial request chain can still dispatch later dirty blocks. The regression in
`blockcachevfs-tests/bcvfs_staging_fault.test` verifies a staged prefix larger
than the request count followed by a dirty tail, then checks a cold read after
publication. Keep this test enabled alongside the `remote_server` and
`remote_progress` Rust suites, `blockcachevfs_security`, focused
`blockcachevfs_emulator` tests, and the shared CBS Tcl emulator runner when
refreshing the patch series.

The CBS block callback and DoltLite logical chunk plan are paired by the Rust
integration but remain separate native interfaces. Standalone CBS builds do not
require DoltLite. This patch is applied only to staged CBS source; the separate
`blockcachevfs-tests/` series carries CBS test changes and the emulator runner
applies those tests after the production patch.

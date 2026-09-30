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

`0017-session-block-prefix.patch` places every new block read and write under
`blocks/<block-id>.bcv`. Session writes use create-only PUTs; when an immutable
object already exists, CBS verifies its exact bytes before accepting it. The
layout has no flat-key fallback.

`0018-stage-guard-fence.patch` keeps the mutable cleanup guard server-owned.
Before a session block PUT, the client reads the guard ETag, writes its own
session-scoped attempt marker, then reads the guard again. It proceeds only
when both snapshots show the same idle generation (or both show that no guard
exists). It retries guard transitions for up to 12 seconds. Cleanup changes the
idle epoch when releasing its sweep, so a sweep that starts and ends between
the two reads is still detected on stores whose ETags are content-derived.

`0019-gcs-crc32c-integrity.patch` sends CRC32C for every Google JSON media
upload and requires the matching `X-Goog-Hash` checksum on full-object
downloads before CBS consumes the body. Block reads also require the configured
full block size before writing into cache. Conditional 304 responses retain
the existing not-modified behavior. The same JSON media transport covers block
objects, session metadata, and the manifest.

`0020-bundled-curl-ca-fallback.patch` applies only to Rust's Cargo-bundled
static curl build. It preserves `CLOUDSQLITE_CAINFO`, honors `SSL_CERT_FILE`
and `SSL_CERT_DIR`, and otherwise selects readable conventional CA file and
directory locations without weakening peer or hostname verification. The
standalone CBS build keeps its upstream curl configuration.

`0021-cloud-auth-refresh-callback.patch` adds request-boundary Bearer-token
refresh for session-owned Google JSON connections. CBS asks for a current
token immediately before dispatching each HTTP request, then after a 401 or
403 asks for a forced replacement and replays that same request once. The
replay preserves its body and generation preconditions; a second denial is
terminal. Callback errors become a generic SQLite authorization I/O error,
and token-bearing headers are not included in CBS verbose HTTP output. The
static authentication callback and Google XML/S3 paths keep their existing
behavior.

This keeps a live `upload()` retry safe when it resumes from local dirty
manifest state: immutable block objects already accepted by the server remain
valid, and an exact same-byte create-only replay is accepted. A fresh process
can rehydrate only an accepted durable session checkpoint/head. Unaccepted
local overlay-to-block mappings are disposable and rebuilt by Gen from its
durable local graph; session attempt markers do not reconstruct that mapping,
so the VFS does not promise bandwidth-free resume for every block staged
before acceptance.

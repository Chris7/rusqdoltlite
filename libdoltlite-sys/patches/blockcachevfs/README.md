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
endpoint and `us-east-1`. The container is `bucket/prefix`, and a configured
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
reads used by clients such as DoltLite.

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

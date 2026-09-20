# Cloud Backed SQLite patches

These numbered patches apply only to the checksum-pinned CBS source staged by
`build.rs`. They are kept separate from the pristine fetched checkout. The
Google storage module accepts an HTTP or HTTPS base URL through
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

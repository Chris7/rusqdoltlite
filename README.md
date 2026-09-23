# RusqDoltLite

This project is a fork of [`Rusqlite`](https://github.com/rusqlite/rusqlite) using [`DoltLite`](https://github.com/dolthub/doltlite) as the SQLite backend. It should be possible to use this library just as you would use rusqlite.

## Cloud Backed SQLite block-cache VFS

The optional `blockcachevfs` feature builds Cloud Backed SQLite's VFS into the
bundled DoltLite archive. The complete pinned Cloud Backed SQLite checkout is
vendored under `libdoltlite-sys/cloudsqlite` and is included in the
`libdoltlite-sys` crate package, so builds do not download native sources or
require a source-directory environment variable:

```sh
cargo build --features blockcachevfs
```

The vendored checkout is from Fossil check-in
`50e099ad7bf1d12d747f59b0af973d12809887480463fc9893846b0d6ee22e94`; the
corresponding upstream source archive has SHA-256
`b322811e8ec4224753f2d9309ed0f011d81c9f2540ce81bd7b5a6a9550d7d03a`.
The target still needs libcurl and OpenSSL development headers and libraries.
When pkg-config cannot locate them, set
`BLOCKCACHEVFS_CURL_INCLUDE_DIR` and `BLOCKCACHEVFS_OPENSSL_INCLUDE_DIR`.

Google storage keeps its default endpoint exactly
`https://storage.googleapis.com`. For a Google-compatible test service, pass
an HTTP or HTTPS base endpoint; it must not contain `&` because it is encoded
in the CBS module selector, and CBS trims trailing slashes before adding the
bucket. The XML API remains the default; select the JSON API explicitly with
`AttachSpec::google_json(...)` or
`AttachSpec::google_json_with_endpoint(...)`. The auth callback's bearer token
is sent to this endpoint, so use test credentials with emulators:

```rust,no_run
use rusqlite::blockcachevfs::AttachSpec;

let attach = AttachSpec::google_with_endpoint(
    "test-project",
    "bucket",
    "http://127.0.0.1:4443/",
);

let json_attach = AttachSpec::google_json_with_endpoint(
    "test-project",
    "bucket/tenant-a",
    "http://127.0.0.1:14091",
);
```

The XML selector `google?endpoint=<base-url>` requires a compatible Google XML
API endpoint. `fake-gcs-server` is exercised by the opt-in JSON emulator tests
with `-scheme http -port 4443`; select the JSON API explicitly because the
existing XML default remains unchanged. Production Google storage keeps the
hard-coded `storage.googleapis.com` base.

S3 uses AWS Signature Version 4 for every request, including custom
S3-compatible endpoints. The default constructor uses virtual-hosted AWS
addressing for ordinary bucket names and path-style addressing through the
regional AWS endpoint for dotted bucket names, which keeps TLS certificate
validation valid. Custom endpoints use path-style addressing. `bucket/prefix`
containers are isolated to that prefix, and the native module uses ETags for
conditional fetch, upload, and delete operations:

```rust,no_run
use rusqlite::blockcachevfs::{s3_secret_with_session_token, AttachSpec};

let attach = AttachSpec::s3("ACCESS_KEY", "bucket/tenant-a", "us-east-1")
    .alias("tenant-a");
let local_s3 = AttachSpec::s3_with_endpoint(
    "test",
    "bucket/tenant-a",
    "us-east-1",
    "http://127.0.0.1:14567",
);

// Return this value from the VFS auth callback for temporary credentials.
let secret = s3_secret_with_session_token("SECRET_ACCESS_KEY", "SESSION_TOKEN")
    .expect("valid S3 credentials");
```

To bootstrap a new remote database, initialize a new container or prefix and
upload a valid local SQLite file before attaching it:

```rust,no_run
use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Storage};

# fn main() -> rusqlite::Result<()> {
let vfs = BlockCacheVfs::builder("cache")?
    .auth_callback(|_, _, _| Ok("test-token".to_owned()))
    .init()?;
let storage = Storage::google_json("test-project", "bucket/tenant-a");
vfs.initialize_container(&storage)?; // fails if the storage/prefix exists
vfs.create_database(&storage, "seed.sqlite", "database.sqlite")?;
vfs.attach(&AttachSpec::new(storage).alias("tenant-a"))?;
# Ok(()) }
```

`initialize_container` is a non-destructive create-if-absent operation: an
existing manifest causes an error and remains unchanged. `create_database`
requires a non-empty, valid SQLite file; after attachment, use `upload` to
flush changes to that existing remote database.

The S3 authentication callback returns the secret access key. For temporary
credentials, return `secret-access-key\nsession-token` using the helper above;
the session token is signed as `x-amz-security-token` and is never put in the
module selector or URL. Enabling `Config::CurlVerbose(true)` leaves only
non-header libcurl diagnostic text on stderr; raw HTTP headers and payloads are
omitted, excluding raw credential-bearing HTTP headers from verbose output.
The strict Floci emulator at the endpoint above is a convenient local SigV4
test service.

To reuse CBS's own emulator tests, run the shared runner once per backend. It
uses the same vendored checkout, applies this repository's numbered patches to
a disposable copy, and runs the upstream `util_api1.test` and
`util_upload2.test` plus `bcvfs_poll1.test`; it does not duplicate their test
logic, and rewrites only that test's hard-coded container names to unique names
in the disposable copy:

```sh
BLOCKCACHEVFS_S3_ENDPOINT=http://127.0.0.1:14567 \
  tools/run_blockcachevfs_cbs_tests
BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT=http://127.0.0.1:14091 \
  tools/run_blockcachevfs_cbs_tests
```

The runner requires Tcl, libcurl, and OpenSSL development headers to build
CBS's Tcl test binary. It exits with status 77 when an endpoint is not
configured.

For a multi-tenant endpoint, pass the remote container as `bucket/prefix` and
choose a slash-free local alias. Attach, read, write, and upload paths then
remain under that prefix:

```rust,no_run
use rusqlite::blockcachevfs::AttachSpec;

let attach = AttachSpec::google("test-project", "bucket/tenants/acme")
    .alias("acme");
```

Orphan cleanup and object listing are separate privileged maintenance
operations; the current low-level list URL is bucket-root oriented.

```rust,no_run
use rusqlite::blockcachevfs::{AttachSpec, AuthError, BlockCacheVfs, Config};

# fn main() -> rusqlite::Result<()> {
let vfs = BlockCacheVfs::builder("cache")?
    .auth_callback(|storage, _project, _bucket| {
        std::env::var("GOOGLE_ACCESS_TOKEN")
            .map_err(|error| AuthError(format!("{storage} auth: {error}")))
    })
    .config(Config::CacheSize(256 * 1024 * 1024))
    .init()?;
vfs.attach(&AttachSpec::google("my-project", "my-bucket").alias("data"))?;
let db = vfs.open("/data/example.db")?;
vfs.poll("data")?;
vfs.upload("data")?;
# drop(db);
# Ok(()) }
```

## In-process remote server

Enable the `remote` feature to embed DoltLite's HTTP remote server:

```toml
[dependencies]
rusqlite = { package = "rusqdoltlite", version = "0.40.21", features = ["remote"] }
```

```rust,no_run
use rusqlite::{params, Connection, RemoteServer};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all("remotes")?;
    let server = RemoteServer::start("remotes")?;
    let remote_url = server.database_url("origin.db");

    let db = Connection::open("local.db")?;
    let _: i64 = db.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![remote_url],
        |row| row.get(0),
    )?;

    // The background server remains active until `server` is dropped.
    Ok(())
}
```

### Authentication

For a single server, DoltLite can terminate TLS and authenticate clients against
a directory of public JWK files:

```rust,no_run
use std::time::Duration;
use rusqlite::{RemoteServer, RemoteServerOptions};

fn main() -> rusqlite::Result<()> {
    let options = RemoteServerOptions::new()
        .bind_address("0.0.0.0")
        .port(443)
        .tls("server.crt", "server.key")
        .authentication("authorized-keys", "remotes.example.com")
        .request_timeout(Duration::from_secs(30));
    let server = RemoteServer::start_with_options("remotes", &options)?;

    // Keep `server` alive while serving requests.
    Ok(())
}
```

This native mode is a server-wide allowlist: every key in `authorized-keys`
can access every database and operation on that listener. It is suitable for a
single-tenant server, but it is not a distributed authorization system.

The DoltLite HTTP client loads a private credential from `DOLTLITE_CREDS_DIR`
(default `~/.doltlite/creds`) and attaches a freshly signed, 30-second bearer
JWT to each HTTPS request. The audience defaults to the remote hostname, so a
generic host at `https://remotes.example.com/<db>` should validate
`remotes.example.com`. `DOLTLITE_CREDS_KID` selects a credential and
`DOLT_OVERRIDE_GRPC_JWT_AUDIENCE` overrides the audience. Credentials are never
sent on plain HTTP remotes.

For a multi-tenant or serverless host, terminate HTTPS and authenticate at the
public gateway. Treat the token's `kid` as an untrusted lookup hint, fetch the
public JWK from the host's database, KV store, or identity service, verify the
Ed25519 signature and [Dolt JWT claims](https://github.com/dolthub/doltlite/blob/master/AUTH.md),
and then authorize the resulting user for the requested database and operation.
Route the request to a loopback-only
`RemoteServer` owned by the stateful repository shard; do not configure
`authKeysDir` on that internal listener. A short-lived per-instance public-key
cache can reduce lookups, provided revocation invalidates or versions the cache.
The host's registration endpoint stores only the public JWK and maps its
derived `kid` to a user; the client's private seed remains on the client. Set
`DOLTLITE_LOGIN_URL` to that registration page to customize the instructions
printed by `dolt_creds_new()`; DoltLite does not perform the registration
request itself.

The gateway must preserve the request method, path, binary body, status, and
`Content-Length`. The protocol endpoints are:

| Access | Requests |
| --- | --- |
| Read | `GET /<db>/root`, `GET /<db>/chunk/<hash>`, `GET /<db>/refs`, `POST /<db>/has-chunks`, `POST /<db>/get-chunks` |
| Write | `POST /<db>/chunks`, `POST /<db>/commit`, `PUT /<db>/refs`, `PUT /<db>/refs-if` |

Authentication does not make the data plane stateless: the native server opens
database files beneath its configured directory. A serverless control plane
therefore still needs repository affinity to a persistent shard, container, or
volume. A purely stateless function cannot host the native remote server by
itself.

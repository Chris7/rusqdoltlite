# RusqDoltLite

This project is a fork of [`Rusqlite`](https://github.com/rusqlite/rusqlite) using [`DoltLite`](https://github.com/dolthub/doltlite) as the SQLite backend. It should be possible to use this library just as you would use rusqlite.

## In-process remote server

Enable the `remote` feature to embed DoltLite's HTTP remote server:

```toml
[dependencies]
rusqlite = { package = "rusqdoltlite", version = "0.40.14", features = ["remote"] }
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

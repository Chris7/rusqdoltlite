# RusqDoltLite

This project is a fork of [`Rusqlite`](https://github.com/rusqlite/rusqlite) using [`DoltLite`](https://github.com/dolthub/doltlite) as the SQLite backend. It should be possible to use this library just as you would use rusqlite.

## In-process remote server

Enable the `remote` feature to embed DoltLite's HTTP remote server:

```toml
[dependencies]
rusqlite = { package = "rusqdoltlite", version = "0.40.7", features = ["remote"] }
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

#![cfg(all(feature = "remote", not(target_arch = "wasm32")))]

use std::{
    io::{Read as _, Write as _},
    net::TcpListener,
    sync::{Arc, Barrier},
    thread,
};

use rusqlite::{params, Connection, Error, RemoteServer, Result};

#[test]
fn in_process_remote_server_supports_push_and_clone() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");

    let server = RemoteServer::start(&server_root)?;
    assert!(server.port() > 0);
    let remote_url = server.database_url("origin.db");

    let source = Connection::open(temp.path().join("source.db"))?;
    source.execute_batch(
        "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO widgets VALUES(1, 'remote value');",
    )?;
    let _: i64 = source.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String = source.query_row("SELECT dolt_commit('-m', 'seed')", [], |row| row.get(0))?;
    let _: i64 = source.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![remote_url],
        |row| row.get(0),
    )?;
    let _: i64 = source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;

    let clone = Connection::open(temp.path().join("clone.db"))?;
    let _: i64 = clone.query_row(
        "SELECT dolt_clone(?1)",
        params![server.database_url("origin.db")],
        |row| row.get(0),
    )?;
    let value: String = clone.query_row("SELECT name FROM widgets WHERE id = 1", [], |row| {
        row.get(0)
    })?;
    let branch_hash: String =
        clone.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    let tracking_hash: String =
        clone.query_row("SELECT dolt_hashof('origin/main')", [], |row| row.get(0))?;

    assert_eq!(value, "remote value");
    assert_eq!(tracking_hash, branch_hash);
    assert!(server_root.join("origin.db").exists());

    Ok(())
}

#[test]
fn remote_server_persists_across_restarts() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");
    let source_path = temp.path().join("source.db");

    {
        let server = RemoteServer::start(&server_root)?;
        let source = Connection::open(&source_path)?;
        source.execute_batch(
            "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
             INSERT INTO widgets VALUES(1, 'persisted');",
        )?;
        let _: String = source.query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })?;
        let _: i64 = source.query_row(
            "SELECT dolt_remote('add', 'origin', ?1)",
            params![server.database_url("origin.db")],
            |row| row.get(0),
        )?;
        let _: i64 =
            source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;
    }

    let restarted = RemoteServer::start(&server_root)?;
    let clone = Connection::open(temp.path().join("clone.db"))?;
    let _: i64 = clone.query_row(
        "SELECT dolt_clone(?1)",
        params![restarted.database_url("origin.db")],
        |row| row.get(0),
    )?;
    let value: String = clone.query_row("SELECT name FROM widgets WHERE id = 1", [], |row| {
        row.get(0)
    })?;

    assert_eq!(value, "persisted");
    Ok(())
}

#[test]
fn set_url_updates_existing_remote() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let first_root = temp.path().join("first-server");
    let second_root = temp.path().join("second-server");
    std::fs::create_dir(&first_root).expect("first server directory");
    std::fs::create_dir(&second_root).expect("second server directory");

    let first_server = RemoteServer::start(&first_root)?;
    let second_server = RemoteServer::start(&second_root)?;
    let connection = Connection::open(temp.path().join("local.db"))?;
    connection.execute_batch(
        "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO widgets VALUES(1, 'local value');",
    )?;
    let _: i64 = connection.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String =
        connection.query_row("SELECT dolt_commit('-m', 'seed')", [], |row| row.get(0))?;
    let _: i64 = connection.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![first_server.database_url("origin.db")],
        |row| row.get(0),
    )?;
    let _: i64 =
        connection.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;
    let _: i64 =
        connection.query_row("SELECT dolt_fetch('origin', 'main')", [], |row| row.get(0))?;
    let tracking_before: String =
        connection.query_row("SELECT dolt_hashof('origin/main')", [], |row| row.get(0))?;

    let second_url = second_server.database_url("origin.db");
    let _: i64 = connection.query_row(
        "SELECT dolt_remote('set-url', 'origin', ?1)",
        params![second_url],
        |row| row.get(0),
    )?;
    let stored_url: String = connection.query_row(
        "SELECT url FROM dolt_remotes WHERE name = 'origin'",
        [],
        |row| row.get(0),
    )?;
    let remote_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM dolt_remotes WHERE name = 'origin'",
        [],
        |row| row.get(0),
    )?;

    assert_eq!(stored_url, second_url);
    assert_eq!(remote_count, 1);
    let tracking_after: String =
        connection.query_row("SELECT dolt_hashof('origin/main')", [], |row| row.get(0))?;
    assert_eq!(tracking_after, tracking_before);

    Ok(())
}

#[test]
fn pull_persists_a_new_local_branch() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let source_path = temp.path().join("source.db");
    let remote_path = temp.path().join("remote.db");
    let clone_path = temp.path().join("clone.db");
    let remote_url = format!("file://{}", remote_path.display());

    let source = Connection::open(&source_path)?;
    source.execute_batch(
        "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO widgets VALUES(1, 'main');",
    )?;
    let _: i64 = source.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String = source.query_row("SELECT dolt_commit('-m', 'main')", [], |row| row.get(0))?;
    let _: i64 = source.query_row("SELECT dolt_branch('feature')", [], |row| row.get(0))?;
    let _: i64 = source.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![remote_url],
        |row| row.get(0),
    )?;
    let _: i64 = source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;
    let _: i64 = source.query_row("SELECT dolt_push('origin', 'feature')", [], |row| {
        row.get(0)
    })?;

    let clone = Connection::open(&clone_path)?;
    let _: i64 = clone.query_row(
        "SELECT dolt_clone(?1)",
        params![format!("file://{}", remote_path.display())],
        |row| row.get(0),
    )?;
    let _: i64 = clone.query_row("SELECT dolt_branch('-d', 'feature')", [], |row| row.get(0))?;
    let _: i64 = clone.query_row("SELECT dolt_pull('origin', 'feature')", [], |row| {
        row.get(0)
    })?;
    drop(clone);

    let reopened = Connection::open(&clone_path)?;
    let branch_exists: bool = reopened.query_row(
        "SELECT EXISTS(SELECT 1 FROM dolt_branches WHERE name = 'feature')",
        [],
        |row| row.get(0),
    )?;
    assert!(branch_exists);

    Ok(())
}

fn assert_http_status_maps_to_sqlite(status: u16, reason: &str, expected_code: i32) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("listener address");
    let response = format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\n\r\n");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).expect("read request");
        stream
            .write_all(response.as_bytes())
            .expect("write response");
    });

    let connection = Connection::open_in_memory()?;
    let _: i64 = connection.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![format!("http://{address}/remote.db")],
        |row| row.get(0),
    )?;
    let error = connection
        .query_row::<i64, _, _>("SELECT dolt_fetch('origin', 'main')", [], |row| row.get(0))
        .expect_err("fetch should be unauthorized");
    server.join().expect("server thread");

    assert!(matches!(
        error,
        Error::SqliteFailure(code, _) if code.extended_code == expected_code
    ));
    Ok(())
}

#[test]
fn http_authorization_statuses_map_to_sqlite_auth() -> Result<()> {
    assert_http_status_maps_to_sqlite(401, "Unauthorized", rusqlite::ffi::SQLITE_AUTH)?;
    assert_http_status_maps_to_sqlite(403, "Forbidden", rusqlite::ffi::SQLITE_AUTH)
}

#[test]
fn http_conflict_maps_to_sqlite_busy() -> Result<()> {
    assert_http_status_maps_to_sqlite(409, "Conflict", rusqlite::ffi::SQLITE_BUSY)
}

#[test]
fn http_payload_too_large_maps_to_sqlite_toobig() -> Result<()> {
    assert_http_status_maps_to_sqlite(413, "Payload Too Large", rusqlite::ffi::SQLITE_TOOBIG)
}

#[test]
fn concurrent_pushes_reject_one_stale_ref_update() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");
    let server = RemoteServer::start(&server_root)?;
    let remote_url = server.database_url("origin.db");
    let seed_path = temp.path().join("seed.db");

    let seed = Connection::open(&seed_path)?;
    seed.execute_batch(
        "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO widgets VALUES(1, 'seed');",
    )?;
    let _: String = seed.query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
        row.get(0)
    })?;
    let _: i64 = seed.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![remote_url],
        |row| row.get(0),
    )?;
    let _: i64 = seed.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;

    let first_path = temp.path().join("first.db");
    let second_path = temp.path().join("second.db");
    for (path, name) in [(&first_path, "first"), (&second_path, "second")] {
        let connection = Connection::open(path)?;
        let _: i64 = connection.query_row(
            "SELECT dolt_clone(?1)",
            params![server.database_url("origin.db")],
            |row| row.get(0),
        )?;
        connection.execute("UPDATE widgets SET name = ?1 WHERE id = 1", [name])?;
        let _: String =
            connection.query_row("SELECT dolt_commit('-A', '-m', ?1)", [name], |row| {
                row.get(0)
            })?;
    }

    let barrier = Arc::new(Barrier::new(3));
    let handles = [first_path, second_path].map(|path| {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            let connection = Connection::open(path).expect("open concurrent clone");
            barrier.wait();
            connection
                .query_row::<i64, _, _>("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))
        })
    });
    barrier.wait();
    let results = handles.map(|handle| handle.join().expect("concurrent push thread"));
    let successes = results.iter().filter(|result| result.is_ok()).count();

    assert_eq!(
        successes, 1,
        "exactly one concurrent push should advance the remote ref"
    );
    assert!(results.iter().any(|result| {
        matches!(
            result,
            Err(Error::SqliteFailure(code, _))
                if matches!(code.extended_code, rusqlite::ffi::SQLITE_BUSY | rusqlite::ffi::SQLITE_ERROR)
        )
    }));
    Ok(())
}

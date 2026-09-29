#![cfg(feature = "blockcachevfs")]

use std::ffi::CString;

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
use rusqlite::params;
use rusqlite::{ffi, Connection, OpenFlags, Result};

const SQLITE_OPEN_DOLTLITE_NO_SEED: i32 = 0x0080_0000;

fn no_seed_flags(create: bool) -> OpenFlags {
    let mut flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if create {
        flags |= OpenFlags::SQLITE_OPEN_CREATE;
    }
    flags | OpenFlags::from_bits_retain(SQLITE_OPEN_DOLTLITE_NO_SEED)
}

fn initialize_empty_store(connection: &Connection) -> i32 {
    let schema = CString::new("main").expect("static schema name has no NUL bytes");
    // SAFETY: `connection` is live for the call, and `schema` is a valid
    // NUL-terminated schema name that remains alive until the native call ends.
    let db = unsafe { connection.handle() };
    // SAFETY: the helper is called with a live SQLite handle and a valid schema.
    unsafe {
        ffi::blockcachevfs::sqlite3_doltlite_bcvfs_initialize_empty_store(db, schema.as_ptr())
    }
}

fn branch_count(connection: &Connection) -> Result<i64> {
    connection.query_row("SELECT count(*) FROM dolt_branches", [], |row| row.get(0))
}

fn seed_committed_database(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO items VALUES(1, 'preserved');",
    )?;
    let _: i64 = connection.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String = connection.query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
        row.get(0)
    })?;
    Ok(())
}

fn committed_state(connection: &Connection) -> Result<(String, String)> {
    let hash = connection.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    let value =
        connection.query_row("SELECT value FROM items WHERE id = 1", [], |row| row.get(0))?;
    Ok((hash, value))
}

#[test]
fn initializes_and_persists_a_store_without_a_default_branch() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("empty.db");
    let connection = Connection::open_with_flags(&path, no_seed_flags(true))?;

    assert_eq!(initialize_empty_store(&connection), ffi::SQLITE_OK);
    assert_eq!(branch_count(&connection)?, 0);
    connection.close().map_err(|(_, error)| error)?;

    let reopened = Connection::open_with_flags(&path, no_seed_flags(false))?;
    assert_eq!(branch_count(&reopened)?, 0);
    Ok(())
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
fn initialized_empty_store_accepts_its_first_remote_ref() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let remote_path = temp.path().join("remote.db");
    let empty_remote = Connection::open_with_flags(&remote_path, no_seed_flags(true))?;
    assert_eq!(initialize_empty_store(&empty_remote), ffi::SQLITE_OK);
    assert_eq!(branch_count(&empty_remote)?, 0);
    empty_remote.close().map_err(|(_, error)| error)?;

    let source = Connection::open(temp.path().join("source.db"))?;
    seed_committed_database(&source)?;
    let remote_url = format!("file://{}", remote_path.display());
    let _: i64 = source.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![remote_url],
        |row| row.get(0),
    )?;
    let _: i64 = source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;

    let source_hash: String =
        source.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    let published = Connection::open_with_flags(&remote_path, no_seed_flags(false))?;
    let remote_hash: String =
        published.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    let remote_value: String =
        published.query_row("SELECT value FROM items WHERE id = 1", [], |row| row.get(0))?;

    assert_eq!(remote_hash, source_hash);
    assert_eq!(remote_value, "preserved");
    Ok(())
}

#[test]
fn refuses_existing_stores_and_regular_handles_without_changing_data() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("committed.db");
    let initial = Connection::open(&path)?;
    seed_committed_database(&initial)?;
    let expected = committed_state(&initial)?;
    initial.close().map_err(|(_, error)| error)?;

    let no_seed = Connection::open_with_flags(&path, no_seed_flags(false))?;
    assert_eq!(
        initialize_empty_store(&no_seed),
        ffi::SQLITE_CONSTRAINT,
        "the helper must reject an existing committed store"
    );
    assert_eq!(committed_state(&no_seed)?, expected);
    no_seed.close().map_err(|(_, error)| error)?;

    let ordinary = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    assert_eq!(
        initialize_empty_store(&ordinary),
        ffi::SQLITE_MISUSE,
        "the helper must reject a handle opened without NO_SEED"
    );
    assert_eq!(committed_state(&ordinary)?, expected);
    Ok(())
}

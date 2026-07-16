#![cfg(feature = "bundled")]

use std::ffi::CStr;
use std::fs;

use rusqlite::{ffi, Connection, Result};

#[test]
fn bundled_backend_reports_doltlite_source_id() {
    let source_id = unsafe { CStr::from_ptr(ffi::sqlite3_sourceid()) }
        .to_str()
        .expect("sqlite3_sourceid must be valid UTF-8");

    assert!(
        source_id.ends_with("alt1"),
        "expected DoltLite source id to end in alt1, got {source_id:?}"
    );
}

#[test]
fn bundled_backend_registers_doltlite_engine_function() -> Result<()> {
    let db = Connection::open_in_memory()?;
    db.execute_batch("CREATE TABLE backend_probe(id INTEGER PRIMARY KEY);")?;

    let engine: String = db.query_row("SELECT doltlite_engine()", [], |row| row.get(0))?;

    assert!(
        matches!(engine.as_str(), "prolly" | "orig"),
        "unexpected DoltLite engine {engine:?}"
    );

    Ok(())
}

#[test]
fn bundled_backend_persists_in_gen_directory() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let gen_dir = temp.path().join(".gen");
    fs::create_dir(&gen_dir).expect("create .gen directory");
    let path = gen_dir.join("default.db");

    {
        let db = Connection::open(&path)?;
        db.execute_batch(
            "CREATE TABLE defaults (id INTEGER PRIMARY KEY); INSERT INTO defaults VALUES (1);",
        )?;
    }

    let db = Connection::open(path)?;
    assert_eq!(
        1,
        db.query_row("SELECT id FROM defaults", [], |row| row.get::<_, i64>(0))?
    );
    Ok(())
}

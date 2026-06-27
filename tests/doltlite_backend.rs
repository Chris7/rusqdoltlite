#![cfg(feature = "bundled")]

use std::ffi::CStr;

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
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let db = Connection::open(temp_dir.path().join("backend-probe.db"))?;
    db.execute_batch("CREATE TABLE backend_probe(id INTEGER PRIMARY KEY);")?;

    let engine: String = db.query_row("SELECT doltlite_engine()", [], |row| row.get(0))?;

    assert!(
        matches!(engine.as_str(), "prolly" | "orig"),
        "unexpected DoltLite engine {engine:?}"
    );

    Ok(())
}

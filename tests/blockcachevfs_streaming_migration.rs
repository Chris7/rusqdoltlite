#![cfg(feature = "blockcachevfs")]

use std::path::Path;

use rusqlite::blockcachevfs::{BlockCacheVfs, Config};
use rusqlite::{Connection, OpenFlags};

fn create_old_metadata(cache: &Path, staged_row: bool, pending_row: bool) {
    std::fs::create_dir_all(cache).expect("create cache directory");
    let metadata =
        Connection::open(cache.join("blocksdb.bcv")).expect("create cache metadata database");
    metadata
        .execute_batch(
            "CREATE TABLE staged(
                 container TEXT NOT NULL,
                 db INTEGER NOT NULL,
                 dbpos INTEGER NOT NULL,
                 generation BLOB NOT NULL,
                 blockid BLOB NOT NULL,
                 PRIMARY KEY(container, db, dbpos, generation)
             );
             CREATE TABLE pending_publish(
                 container TEXT PRIMARY KEY,
                 manifest BLOB NOT NULL,
                 maxrowid INTEGER NOT NULL
             );",
        )
        .expect("create pre-sequence metadata tables");

    if staged_row {
        metadata
            .execute(
                "INSERT INTO staged(container, db, dbpos, generation, blockid)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    "old-container",
                    1_i64,
                    0_i64,
                    vec![0x11_u8; 32],
                    vec![0x22_u8; 32],
                ],
            )
            .expect("insert pre-sequence staged row");
    }
    if pending_row {
        metadata
            .execute(
                "INSERT INTO pending_publish(container, manifest, maxrowid)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params!["old-container", vec![0x33_u8; 32], 7_i64],
            )
            .expect("insert pre-sequence pending marker");
    }
}

fn init(cache: &Path) -> rusqlite::Result<&'static BlockCacheVfs> {
    BlockCacheVfs::builder(cache)
        .expect("block-cache VFS builder")
        .auth_callback(|_, _, _| Ok("test-token".into()))
        .config(Config::CacheSize(8 * 1024 * 1024))
        .init()
}

fn assert_corrupt(error: rusqlite::Error) {
    let description = format!("{error:?}").to_ascii_lowercase();
    assert!(
        description.contains("corrupt"),
        "pre-sequence metadata must fail closed as corrupt, got {error:?}"
    );
}

fn assert_rejected(cache: &Path, message: &str) {
    match init(cache) {
        Ok(_) => panic!("{message}"),
        Err(error) => assert_corrupt(error),
    }
}

fn assert_column(cache: &Path, table: &str, column: &str) {
    let metadata = Connection::open_with_flags(
        cache.join("blocksdb.bcv"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("reopen migrated cache metadata");
    let mut statement = metadata
        .prepare(&format!("PRAGMA table_info({table})"))
        .expect("inspect migrated metadata table");
    let found = statement
        .query_map([], |row| row.get::<_, String>(1))
        .expect("read migrated metadata columns")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("collect migrated metadata columns")
        .into_iter()
        .any(|name| name == column);
    assert!(
        found,
        "migrated metadata table {table} is missing column {column}"
    );
}

#[test]
fn pre_sequence_unpublished_metadata_fails_closed_and_empty_tables_migrate() {
    let staged_cache = tempfile::tempdir().expect("staged metadata temporary directory");
    create_old_metadata(staged_cache.path(), true, false);
    assert_rejected(
        staged_cache.path(),
        "nonempty pre-sequence staged metadata must not be replayed",
    );

    let pending_cache = tempfile::tempdir().expect("pending metadata temporary directory");
    create_old_metadata(pending_cache.path(), false, true);
    assert_rejected(
        pending_cache.path(),
        "nonempty old pending publication metadata must not be replayed",
    );

    let empty_cache = tempfile::tempdir().expect("empty metadata temporary directory");
    create_old_metadata(empty_cache.path(), false, false);
    init(empty_cache.path()).expect("empty pre-sequence metadata should migrate");
    assert_column(empty_cache.path(), "staged", "sequence");
    assert_column(empty_cache.path(), "pending_publish", "maxseq");
}

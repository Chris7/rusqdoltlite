#![cfg(feature = "blockcachevfs")]

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{c_char, CStr, CString};
use std::fs;
use std::os::raw::c_int;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Config, Storage};
use rusqlite::ffi::{bcvutil as raw_util, blockcachevfs as raw_vfs};
use rusqlite::{params, Connection};

unsafe extern "C" {
    fn sqlite3_bcv_cleanup(handle: *mut raw_util::sqlite3_bcv, n_second: c_int) -> c_int;
    fn sqlite3_bcvfs_revert(
        fs: *mut raw_vfs::sqlite3_bcvfs,
        container: *const c_char,
        error: *mut *mut c_char,
    ) -> c_int;
}

const CACHE_BYTES: i64 = 2 * 4 * 1024 * 1024;
const BODY_BYTES: usize = 2048;
const ROWS: i64 = 7_000;
const UPDATE_IDS: [i64; 5] = [0, 1_500, 3_000, 4_500, 6_000];

#[derive(Debug, Clone)]
struct StagedRow {
    db: i64,
    dbpos: i64,
    blockid: String,
    sequence: i64,
}

#[derive(Debug)]
struct ManifestBlocks {
    live: BTreeSet<String>,
    delete: BTreeSet<String>,
}

fn unique_suffix() -> String {
    format!(
        "rust-streaming-gc-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_nanos()
    )
}

fn ensure_google_bucket(endpoint: &str, bucket: &str) {
    let payload = format!(r#"{{"name":"{bucket}"}}"#);
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code}",
            "-X",
            "POST",
            "-H",
            "content-type: application/json",
            "--data",
            &payload,
            &format!("{endpoint}/storage/v1/b"),
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(response.status.success(), "GCS bucket request failed");
    let code = String::from_utf8_lossy(&response.stdout);
    assert!(
        code == "200" || code == "201" || code == "409",
        "GCS bucket creation returned {code}"
    );
}

fn encode_component(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                char::from(byte).to_string()
            }
            byte => format!("%{byte:02X}"),
        })
        .collect()
}

fn encode_path(value: &str) -> String {
    value
        .split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/")
}

fn manifest_url(backend: &str, endpoint: &str, container: &str) -> String {
    let (bucket, prefix) = container
        .split_once('/')
        .expect("GC test container must contain a prefix");
    let object = format!("{prefix}/manifest.bcv");
    let endpoint = endpoint.trim_end_matches('/');
    match backend {
        "google" => format!(
            "{endpoint}/download/storage/v1/b/{}/o/{}?alt=media",
            encode_component(bucket),
            encode_component(&object)
        ),
        "s3" => format!(
            "{endpoint}/{}/{}",
            encode_component(bucket),
            encode_path(&object)
        ),
        _ => unreachable!("unknown GC backend {backend}"),
    }
}

fn list_remote_keys(backend: &str, endpoint: &str, container: &str) -> BTreeSet<String> {
    let (bucket, prefix) = container
        .split_once('/')
        .expect("GC test container must contain a prefix");
    let endpoint = endpoint.trim_end_matches('/');
    let prefix = format!("{prefix}/");
    let url = match backend {
        "google" => format!(
            "{endpoint}/storage/v1/b/{}/o?prefix={}&maxResults=5000",
            encode_component(bucket),
            encode_component(&prefix)
        ),
        "s3" => format!(
            "{endpoint}/{}/?list-type=2&prefix={}&max-keys=5000",
            encode_component(bucket),
            encode_component(&prefix)
        ),
        _ => unreachable!("unknown GC backend {backend}"),
    };
    let body = tempfile::NamedTempFile::new().expect("object-list temporary file");
    let mut command = Command::new("curl");
    command.args([
        "--silent",
        "--show-error",
        "--output",
        body.path().to_str().expect("object-list path is UTF-8"),
        "--write-out",
        "%{http_code}",
    ]);
    if backend == "s3" {
        command.args(["--aws-sigv4", "aws:amz:us-east-1:s3", "--user", "test:test"]);
    }
    let response = command
        .arg(&url)
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(
        response.status.success(),
        "object-list request failed: {url}"
    );
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "object-list request returned {code}: {url}");
    let text = fs::read_to_string(body.path()).expect("read object-list response");

    match backend {
        "google" => text
            .split("\"name\":\"")
            .skip(1)
            .filter_map(|entry| entry.split('"').next())
            .map(|key| key.to_ascii_lowercase())
            .collect(),
        "s3" => text
            .split("<Key>")
            .skip(1)
            .filter_map(|entry| entry.split("</Key>").next())
            .map(|key| key.to_ascii_lowercase())
            .collect(),
        _ => unreachable!("unknown GC backend {backend}"),
    }
}

fn fetch_manifest(backend: &str, url: &str) -> Vec<u8> {
    let body = tempfile::NamedTempFile::new().expect("manifest body temporary file");
    let mut command = Command::new("curl");
    command.args([
        "--silent",
        "--show-error",
        "--output",
        body.path().to_str().expect("manifest path is UTF-8"),
        "--write-out",
        "%{http_code}",
    ]);
    if backend == "s3" {
        command.args(["--aws-sigv4", "aws:amz:us-east-1:s3", "--user", "test:test"]);
    }
    let response = command
        .arg(url)
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(response.status.success(), "manifest request failed: {url}");
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "manifest request returned {code}: {url}");
    fs::read(body.path()).expect("read manifest body")
}

fn get_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("manifest integer is in bounds"),
    )
}

fn block_id(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_manifest(bytes: &[u8]) -> ManifestBlocks {
    assert!(bytes.len() >= 24, "manifest is shorter than its header");
    assert_eq!(get_u32(bytes, 0), 4, "unexpected manifest version");
    let n_db = get_u32(bytes, 8) as usize;
    let n_delete = get_u32(bytes, 12) as usize;
    let name_size = get_u32(bytes, 16) as usize;
    let db_header_size = 6 * 4 + 128;
    let delete_start = 24 + n_db * db_header_size;
    let delete_entry_size = name_size + 8;
    let delete_end = delete_start + n_delete * delete_entry_size;
    assert!(
        delete_end <= bytes.len(),
        "manifest delete list is truncated"
    );

    let mut delete = BTreeSet::new();
    for index in 0..n_delete {
        let start = delete_start + index * delete_entry_size;
        delete.insert(block_id(&bytes[start..start + name_size]));
    }

    let mut live = BTreeSet::new();
    for index in 0..n_db {
        let header = 24 + index * db_header_size;
        let parent_id = get_u32(bytes, header + 4);
        let n_blocks = (get_u32(bytes, header + 16) & 0x7fff_ffff) as usize;
        let n_entries = get_u32(bytes, header + 20) as usize;
        let mut offset = get_u32(bytes, header + 12) as usize;
        if parent_id == 0 {
            assert!(
                offset + n_entries * name_size <= bytes.len(),
                "manifest block list is truncated"
            );
            for _ in 0..n_blocks {
                live.insert(block_id(&bytes[offset..offset + name_size]));
                offset += name_size;
            }
        } else {
            assert!(
                offset + n_entries * (4 + name_size) <= bytes.len(),
                "manifest delta block list is truncated"
            );
            for _ in 0..n_entries {
                offset += 4;
                live.insert(block_id(&bytes[offset..offset + name_size]));
                offset += name_size;
            }
        }
    }

    ManifestBlocks { live, delete }
}

fn staged_rows(metadata: &Connection, container: &str) -> Vec<StagedRow> {
    let mut statement = metadata
        .prepare(
            "SELECT db, dbpos, hex(blockid), sequence FROM staged \
             WHERE container = ?1 ORDER BY sequence",
        )
        .expect("prepare staged-object query");
    statement
        .query_map([container], |row| {
            Ok(StagedRow {
                db: row.get(0)?,
                dbpos: row.get(1)?,
                blockid: row.get::<_, String>(2)?.to_ascii_lowercase(),
                sequence: row.get(3)?,
            })
        })
        .expect("query staged objects")
        .map(|row| row.expect("read staged object row"))
        .collect()
}

fn staged_gc_rows(metadata: &Connection, container: &str) -> Vec<StagedRow> {
    let mut statement = metadata
        .prepare(
            "SELECT db, dbpos, hex(blockid), sequence FROM staged_gc \
             WHERE container = ?1 ORDER BY sequence",
        )
        .expect("prepare staged-GC query");
    statement
        .query_map([container], |row| {
            Ok(StagedRow {
                db: row.get(0)?,
                dbpos: row.get(1)?,
                blockid: row.get::<_, String>(2)?.to_ascii_lowercase(),
                sequence: row.get(3)?,
            })
        })
        .expect("query staged-GC rows")
        .map(|row| row.expect("read staged-GC row"))
        .collect()
}

fn raw_vfs_handle(vfs: &'static BlockCacheVfs) -> *mut raw_vfs::sqlite3_bcvfs {
    // BlockCacheVfs deliberately keeps the native handle private.  The C
    // object begins with sqlite3_vfs, so resolve the registered VFS by name
    // rather than relying on the Rust wrapper's repr(Rust) field layout.
    let name = CString::new(vfs.name()).expect("VFS name is NUL-free");
    let base = unsafe { rusqlite::ffi::sqlite3_vfs_find(name.as_ptr()) };
    assert!(
        !base.is_null(),
        "registered VFS must be discoverable by name"
    );
    base.cast()
}

fn row_body(generation: usize, id: i64) -> String {
    let prefix = format!("row-{id:08}:");
    format!(
        "{prefix}{}{}",
        char::from(b'0' + (generation % 10) as u8),
        "x".repeat(BODY_BYTES - prefix.len() - 1)
    )
}

fn initial_row_body(id: i64) -> String {
    let prefix = format!("row-{id:08}:");
    format!("{prefix}{}", "x".repeat(BODY_BYTES - prefix.len()))
}

fn write_generation(db: &Connection, generation: usize, initial: bool) {
    db.execute_batch("BEGIN IMMEDIATE;")
        .unwrap_or_else(|error| panic!("begin generation {generation} update: {error:?}"));
    if initial {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare initial generation");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(generation, id)])
                .expect("write initial generation");
        }
    } else {
        let mut update = db
            .prepare("UPDATE payload SET body = ?1 WHERE id = ?2")
            .expect("prepare generation update");
        for id in UPDATE_IDS {
            update
                .execute(params![row_body(generation, id), id])
                .expect("write generation update");
        }
    }
    db.execute_batch("COMMIT;")
        .expect("commit generation update");
}

fn cleanup_storage(backend: &str, endpoint: &str, container: &str) {
    let module = match backend {
        "google" => format!("google?api=json&endpoint={endpoint}"),
        "s3" => format!("s3?region=us-east-1&endpoint={endpoint}"),
        _ => unreachable!("unknown GC backend {backend}"),
    };
    let account = if backend == "google" {
        "test-project"
    } else {
        "test"
    };
    let auth = if backend == "google" {
        "test-token"
    } else {
        "test"
    };
    let module = CString::new(module).expect("cleanup module is NUL-free");
    let account = CString::new(account).expect("cleanup account is NUL-free");
    let auth = CString::new(auth).expect("cleanup auth is NUL-free");
    let container = CString::new(container).expect("cleanup container is NUL-free");
    let mut handle = std::ptr::null_mut();
    let open_rc = unsafe {
        raw_util::sqlite3_bcv_open(
            module.as_ptr(),
            account.as_ptr(),
            auth.as_ptr(),
            container.as_ptr(),
            &mut handle,
        )
    };
    assert_eq!(open_rc, 0, "open cleanup handle returned {open_rc}");
    assert!(!handle.is_null(), "cleanup handle must be non-null");
    let cleanup_rc = unsafe { sqlite3_bcv_cleanup(handle, 0) };
    unsafe { raw_util::sqlite3_bcv_close(handle) };
    assert_eq!(cleanup_rc, 0, "cleanup returned {cleanup_rc}");
}

fn setup(backend: &str, vfs: &'static BlockCacheVfs) -> (String, String, Storage) {
    let suffix = unique_suffix();
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4443".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown GC backend {backend}"),
    };
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rust-streaming-gc-{suffix}")
    };
    let container = format!("{bucket}/{suffix}/cbs");
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    }
    let storage = if backend == "google" {
        Storage::google_json_with_endpoint("test-project", &container, &endpoint)
    } else {
        Storage::s3_with_endpoint("test", &container, "us-east-1", &endpoint)
    };
    vfs.initialize_container(&storage)
        .expect("initialize GC test container");

    let local_dir = tempfile::tempdir().expect("local seed directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local seed database");
    local
        .execute_batch(
            "PRAGMA page_size = 4096;
             CREATE TABLE payload(id INTEGER PRIMARY KEY, body TEXT NOT NULL);",
        )
        .expect("create seed schema");
    {
        let mut insert = local
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare seed rows");
        for id in 0..ROWS {
            insert
                .execute(params![id, initial_row_body(id)])
                .expect("write seed row");
        }
    }
    let _: String = local
        .query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })
        .expect("commit seed database");
    local.close().expect("close seed database");
    vfs.create_database(&storage, &local_path, "streaming.sqlite")
        .expect("upload seed database");
    (endpoint, container, storage)
}

fn run_backend(backend: &str, cache: &Path, vfs: &'static BlockCacheVfs) {
    let (endpoint, container, storage) = setup(backend, vfs);
    let alias = format!("streaming_{}", std::process::id());
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&alias))
        .expect("attach GC test database");
    let manifest = manifest_url(backend, &endpoint, &container);
    let before = fetch_manifest(backend, &manifest);
    let before_keys = list_remote_keys(backend, &endpoint, &container);
    let db_path = format!("/{alias}/streaming.sqlite");
    let control = vfs
        .open(format!("/{alias}"))
        .expect("open control connection before appended generation");
    let nblock_baseline: i64 = control
        .query_row(
            "SELECT nblock FROM bcv_database WHERE container = ?1 AND database = 'streaming.sqlite'",
            [alias.as_str()],
            |row| row.get(0),
        )
        .expect("read baseline block count");
    drop(control);

    // Append one deterministic generation and retain every staged mapping it
    // produced. Replaying this exact append after native revert should produce
    // the same content-derived IDs with newer staged sequence numbers.
    let metadata_path = cache.join("blocksdb.bcv");
    let db = vfs
        .open(&db_path)
        .expect("open for first appended generation");
    db.execute_batch("BEGIN IMMEDIATE;")
        .expect("begin first appended generation");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare first appended generation");
        for id in ROWS..(ROWS + 7_000) {
            insert
                .execute(params![id, row_body(2, id)])
                .expect("write first appended generation");
        }
    }
    db.execute_batch("COMMIT")
        .expect("commit first appended generation");

    let metadata = Connection::open(&metadata_path).expect("open first-generation metadata");
    let same_content_candidates = staged_rows(&metadata, &alias);
    assert!(
        !same_content_candidates.is_empty(),
        "first appended generation must stage blocks: baseline_blocks={nblock_baseline}"
    );
    let first_generation_max_position = same_content_candidates
        .iter()
        .map(|row| row.dbpos)
        .max()
        .expect("first appended generation must have a maximum block position");
    drop(metadata);

    // Append a second generation so native revert has a later block to
    // tombstone separately from the generation that will be replayed.
    db.execute_batch("BEGIN IMMEDIATE;")
        .expect("begin second appended generation");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare second appended generation");
        for id in (ROWS + 7_000)..(ROWS + 14_000) {
            insert
                .execute(params![id, row_body(2, id)])
                .expect("write second appended generation");
        }
    }
    db.execute_batch("COMMIT")
        .expect("commit second appended generation");

    let metadata = Connection::open(&metadata_path).expect("open local cache metadata");
    let staged_before_revert = staged_rows(&metadata, &alias);
    let rollback_target = staged_before_revert
        .iter()
        .find(|row| row.dbpos > first_generation_max_position)
        .cloned()
        .unwrap_or_else(|| {
            panic!(
                "second appended generation must stage a block after the first: first_max={first_generation_max_position}, staged_count={}, max_position={:?}",
                staged_before_revert.len(),
                staged_before_revert.iter().map(|row| row.dbpos).max()
            )
        });
    drop(metadata);

    drop(db);

    // The native revert API is the VFS-level truncation/revert path.  It
    // restores the manifest's old block count and must move every already PUT
    // object it discards to staged_gc rather than dropping its only tracking
    // row.  The Rust wrapper intentionally does not expose this maintenance
    // operation, so this test calls the C entry point directly.
    let native_alias = CString::new(alias.as_str()).expect("native alias is NUL-free");
    let native_fs = raw_vfs_handle(vfs);
    assert_eq!(
        unsafe { raw_vfs::sqlite3_bcvfs_isdaemon(native_fs) != 0 },
        vfs.is_daemon(),
        "test-only native handle extraction disagrees with the Rust wrapper"
    );
    let mut revert_error = std::ptr::null_mut();
    let revert_rc =
        unsafe { sqlite3_bcvfs_revert(native_fs, native_alias.as_ptr(), &mut revert_error) };
    let revert_message = if revert_error.is_null() {
        String::new()
    } else {
        let message = unsafe { CStr::from_ptr(revert_error) }
            .to_string_lossy()
            .into_owned();
        unsafe { rusqlite::ffi::sqlite3_free(revert_error.cast()) };
        message
    };
    assert_eq!(
        revert_rc, 0,
        "native revert failed with {revert_rc}: {revert_message}"
    );

    let control = vfs
        .open(format!("/{alias}"))
        .expect("open control connection after native revert");
    let nblock_after_revert: i64 = control
        .query_row(
            "SELECT nblock FROM bcv_database WHERE container = ?1 AND database = 'streaming.sqlite'",
            [alias.as_str()],
            |row| row.get(0),
        )
        .expect("read block count after native revert");
    assert_eq!(
        nblock_after_revert, nblock_baseline,
        "native revert must restore the baseline logical block count"
    );
    drop(control);

    let metadata = Connection::open(&metadata_path).expect("open local cache metadata");
    let staged_gc_after_revert = staged_gc_rows(&metadata, &alias);
    assert!(
        staged_gc_after_revert.iter().any(|row| {
            row.db == rollback_target.db
                && row.dbpos == rollback_target.dbpos
                && row.blockid == rollback_target.blockid
        }),
        "native revert dropped the staged mapping before publication: target={rollback_target:?}, staged_gc={staged_gc_after_revert:?}"
    );
    let staged_gc_max_before_upload = staged_gc_after_revert
        .iter()
        .map(|row| row.sequence)
        .max()
        .expect("native revert must create a staged-GC row");
    drop(metadata);

    // The reverted append is gone from the local manifest. Recreate it so the
    // current manifest has more logical blocks than the two-slot cache; at
    // least one current staged mapping must therefore be nonresident.
    let db = vfs.open(&db_path).expect("reopen after native revert");
    db.execute_batch("BEGIN IMMEDIATE;")
        .expect("begin same-generation restage");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare same-generation restage");
        for id in ROWS..(ROWS + 7_000) {
            insert
                .execute(params![id, row_body(2, id)])
                .expect("write same-generation restage");
        }
    }
    db.execute_batch("COMMIT")
        .expect("commit same-generation restage");
    let restored_rows: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("read reverted row count");
    assert_eq!(
        restored_rows,
        ROWS + 7_000,
        "same-generation restage did not retain the appended rows"
    );
    write_generation(&db, 3, false);
    write_generation(&db, 4, false);
    drop(db);

    let metadata = Connection::open(&metadata_path).expect("reopen local cache metadata");
    let staged = staged_rows(&metadata, &alias);
    assert!(
        !staged.is_empty(),
        "rollback rewrite must retain staged objects"
    );
    assert!(
        staged
            .windows(2)
            .all(|rows| rows[0].sequence < rows[1].sequence),
        "staged rows must have increasing durable sequence numbers: {staged:?}"
    );
    let mut by_position: BTreeMap<(i64, i64), Vec<&StagedRow>> = BTreeMap::new();
    for row in &staged {
        by_position
            .entry((row.db, row.dbpos))
            .or_default()
            .push(row);
    }
    let superseded = by_position
        .values()
        .find(|rows| rows.len() >= 2)
        .unwrap_or_else(|| {
            panic!("expected multiple staged generations for one logical block; rows={staged:?}")
        });
    let mut superseded_ids: BTreeSet<String> =
        superseded.iter().map(|row| row.blockid.clone()).collect();
    superseded_ids.insert(rollback_target.blockid.clone());
    assert!(
        superseded_ids.len() >= 2,
        "staged generations must have distinct object IDs: {superseded:?}"
    );

    // A staged row can be recreated with the same logical generation after a
    // revert.  The old staged-GC predicate matched only the generation and
    // therefore treated an older tombstone as hiding this newer row.  Make a
    // tombstone with the same generation and an older sequence, then exercise
    // the exact metadata lookup predicate.  The metadata fixture is
    // deliberate: replaying SQL writes is not guaranteed to recreate
    // identical content-addressed page IDs after a rollback.
    let same_content_restaged = staged
        .iter()
        .rev()
        .find(|row| {
            row.sequence > staged_gc_max_before_upload
                && !staged.iter().any(|newer| {
                    newer.db == row.db && newer.dbpos == row.dbpos && newer.sequence > row.sequence
                })
                && metadata
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM block \
                         WHERE lower(hex(blockid)) = ?1)",
                        [row.blockid.as_str()],
                        |result| result.get::<_, i64>(0),
                    )
                    .expect("check same-generation restage residency")
                    == 0
        })
        .cloned()
        .unwrap_or_else(|| {
            panic!("same-generation restage needs an evicted current mapping: staged={staged:?}")
        });
    assert!(
        same_content_restaged.sequence > 0,
        "same-generation restage needs a sequence predecessor"
    );
    let inserted_tombstones = metadata
        .execute(
            "INSERT INTO staged_gc \
             (container, db, dbpos, generation, blockid, sequence) \
             SELECT container, db, dbpos, generation, blockid, sequence - 1 \
             FROM staged \
             WHERE container = ?1 AND db = ?2 AND dbpos = ?3 AND sequence = ?4",
            params![
                alias.as_str(),
                same_content_restaged.db,
                same_content_restaged.dbpos,
                same_content_restaged.sequence,
            ],
        )
        .expect("insert same-generation staged-GC tombstone");
    assert_eq!(
        inserted_tombstones, 1,
        "same-generation staged-GC tombstone insertion must change one row"
    );
    let same_content_tombstone = StagedRow {
        sequence: same_content_restaged.sequence - 1,
        ..same_content_restaged.clone()
    };
    assert!(
        staged_gc_rows(&metadata, &alias)
            .iter()
            .any(|row| row.db == same_content_tombstone.db
                && row.dbpos == same_content_tombstone.dbpos
                && row.blockid == same_content_tombstone.blockid
                && row.sequence == same_content_tombstone.sequence),
        "same-generation staged-GC tombstone was not persisted"
    );
    let lookup_blockid: String = metadata
        .query_row(
            "SELECT lower(hex(s.blockid)) FROM staged AS s \
             WHERE s.container = ?1 AND s.db = ?2 AND s.dbpos = ?3 \
               AND s.sequence = ?4 \
               AND NOT EXISTS ( \
                 SELECT 1 FROM staged_gc AS g \
                 WHERE g.container = s.container AND g.db = s.db \
                   AND g.dbpos = s.dbpos AND g.generation = s.generation \
                   AND g.sequence = s.sequence \
               )",
            params![
                alias.as_str(),
                same_content_restaged.db,
                same_content_restaged.dbpos,
                same_content_restaged.sequence,
            ],
            |row| row.get(0),
        )
        .expect("same-generation staged lookup");
    assert_eq!(
        lookup_blockid, same_content_restaged.blockid,
        "an older staged-GC tombstone hid the newer same-generation mapping"
    );
    let staged_keys: BTreeSet<String> = staged
        .iter()
        .map(|row| {
            let (bucket, prefix) = container.split_once('/').expect("container prefix");
            let _ = bucket;
            format!("{prefix}/{}.bcv", row.blockid)
        })
        .collect();
    let (bucket, prefix) = container.split_once('/').expect("container prefix");
    let _ = bucket;
    let rollback_target_key = format!("{prefix}/{}.bcv", rollback_target.blockid);
    let after_stage_keys = list_remote_keys(backend, &endpoint, &container);
    assert!(
        after_stage_keys.contains(&rollback_target_key),
        "reverted staged object was deleted before publication: target={rollback_target:?}"
    );
    for key in &staged_keys {
        assert!(
            after_stage_keys.contains(key),
            "staged object {key} is missing before publication; remote keys={after_stage_keys:?}; staged rows={staged:?}"
        );
    }
    assert!(
        after_stage_keys.len() >= before_keys.len() + staged_keys.len().saturating_sub(1),
        "staging did not create the expected remote objects: before={before_keys:?}, after={after_stage_keys:?}, staged={staged_keys:?}"
    );
    assert_eq!(
        before,
        fetch_manifest(backend, &manifest),
        "staging must not publish a new manifest"
    );

    vfs.upload(&alias).expect("publish final manifest");
    let db = vfs
        .open(&db_path)
        .expect("reopen after same-generation restage publication");
    let readable_rows: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("read database after same-generation restage publication");
    assert_eq!(
        readable_rows,
        ROWS + 7_000,
        "an older staged-GC tombstone hid the current database mapping"
    );
    drop(db);
    let after_upload_keys = list_remote_keys(backend, &endpoint, &container);
    assert!(
        after_upload_keys.contains(&rollback_target_key),
        "reverted staged object was deleted at the publication boundary"
    );
    for key in &staged_keys {
        assert!(
            after_upload_keys.contains(key),
            "staged object {key} was deleted before the manifest publication boundary"
        );
    }
    let final_manifest = parse_manifest(&fetch_manifest(backend, &manifest));
    assert!(
        final_manifest.delete.contains(&rollback_target.blockid),
        "revert target must be in the final manifest delayed-GC list: target={rollback_target:?}, delete={:?}",
        final_manifest.delete
    );
    assert!(
        !final_manifest.live.contains(&rollback_target.blockid),
        "revert target must not be live in the final manifest: target={rollback_target:?}, live={:?}",
        final_manifest.live
    );
    assert!(
        final_manifest
            .live
            .contains(&same_content_restaged.blockid),
        "same-content restage is missing from the final manifest live set: restaged={same_content_restaged:?}, live={:?}",
        final_manifest.live
    );
    assert!(
        !final_manifest
            .delete
            .contains(&same_content_restaged.blockid),
        "same-content tombstone incorrectly delayed-GC tracked the current mapping: restaged={same_content_restaged:?}, delete={:?}",
        final_manifest.delete
    );
    assert!(
        superseded_ids
            .iter()
            .any(|id| final_manifest.delete.contains(id)),
        "at least one superseded staged object must be in the final manifest delete list: ids={superseded_ids:?}, delete={:?}, live={:?}",
        final_manifest.delete,
        final_manifest.live
    );
    for id in &superseded_ids {
        assert!(
            final_manifest.live.contains(id) || final_manifest.delete.contains(id),
            "staged object {id} is neither live nor delayed-GC tracked"
        );
    }

    let metadata = Connection::open(&metadata_path).expect("reopen metadata after publication");
    let staged_gc_after_upload = staged_gc_rows(&metadata, &alias);
    assert!(
        staged_gc_after_upload
            .iter()
            .all(|row| row.sequence > staged_gc_max_before_upload),
        "published staged-GC rows were not cleaned through the publication high-water mark: before={staged_gc_max_before_upload}, after={staged_gc_after_upload:?}"
    );
    drop(metadata);

    cleanup_storage(backend, &endpoint, &container);
    let after_cleanup_keys = list_remote_keys(backend, &endpoint, &container);
    for id in &final_manifest.live {
        let (bucket, prefix) = container.split_once('/').expect("container prefix");
        let _ = bucket;
        let key = format!("{prefix}/{id}.bcv");
        assert!(
            after_cleanup_keys.contains(&key),
            "cleanup deleted live/pinned/shared block {id}"
        );
    }
    let removed_superseded = superseded_ids.iter().any(|id| {
        let (_, prefix) = container.split_once('/').expect("container prefix");
        !after_cleanup_keys.contains(&format!("{prefix}/{id}.bcv"))
    });
    assert!(
        removed_superseded,
        "cleanup did not remove any delayed-GC superseded object: before={after_upload_keys:?}, after={after_cleanup_keys:?}, delete={:?}",
        final_manifest.delete
    );
    assert!(
        !after_cleanup_keys.contains(&rollback_target_key),
        "cleanup did not remove the exact reverted staged object: target={rollback_target:?}, keys={after_cleanup_keys:?}"
    );
    let same_content_key = format!("{prefix}/{}.bcv", same_content_restaged.blockid);
    assert!(
        after_cleanup_keys.contains(&same_content_key),
        "cleanup deleted the live same-content restage: restaged={same_content_restaged:?}, keys={after_cleanup_keys:?}"
    );

    vfs.detach(&alias).expect("detach GC test database");
}

#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_and_s3_staged_objects_are_gc_safe() {
    let cache = tempfile::tempdir().expect("cache directory");
    let vfs = BlockCacheVfs::builder(cache.path())
        .expect("VFS builder")
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .config(Config::CacheSize(CACHE_BYTES))
        .init()
        .expect("initialize block-cache VFS");
    run_backend("google", cache.path(), vfs);
    run_backend("s3", cache.path(), vfs);
}

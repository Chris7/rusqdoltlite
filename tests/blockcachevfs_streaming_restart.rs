#![cfg(feature = "blockcachevfs")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Config, Storage};
use rusqlite::{params, Connection, OpenFlags};

const CACHE_BYTES: u64 = 2 * 4 * 1024 * 1024;
const BODY_BYTES: usize = 2048;
const ROWS: i64 = 7_000;
const DELETED_ROWS: i64 = 100;
const SLOT_REWRITE_ROWS: i64 = 1_024;
const CACHE_ENV: &str = "BCV_STREAMING_RESTART_CACHE";
const STATE_ENV: &str = "BCV_STREAMING_RESTART_STATE";
const ENDPOINT_ENV: &str = "BCV_STREAMING_RESTART_ENDPOINT";
const CONTAINER_ENV: &str = "BCV_STREAMING_RESTART_CONTAINER";
const MARKER_ENV: &str = "BCV_STREAMING_RESTART_MARKER";

fn unique_suffix() -> String {
    format!(
        "rust-streaming-restart-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_nanos()
    )
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for restart phase"))
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

fn manifest_url(endpoint: &str, container: &str) -> String {
    let (bucket, prefix) = container
        .split_once('/')
        .expect("restart container must contain a prefix");
    let object = format!("{prefix}/manifest.bcv");
    format!(
        "{}/download/storage/v1/b/{}/o/{}?alt=media",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_component(&object)
    )
}

fn fetch_manifest(endpoint: &str, container: &str) -> Vec<u8> {
    let body = tempfile::NamedTempFile::new().expect("manifest temporary file");
    let url = manifest_url(endpoint, container);
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--output",
            body.path().to_str().expect("manifest path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(response.status.success(), "manifest request failed: {url}");
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "manifest request returned {code}: {url}");
    fs::read(body.path()).expect("read manifest body")
}

fn list_remote_objects(endpoint: &str, container: &str) -> usize {
    let (bucket, prefix) = container
        .split_once('/')
        .expect("restart container must contain a prefix");
    let body = tempfile::NamedTempFile::new().expect("object-list temporary file");
    let url = format!(
        "{}/storage/v1/b/{}/o?prefix={}&maxResults=1000",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_component(&format!("{prefix}/"))
    );
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--output",
            body.path().to_str().expect("object-list path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(
        response.status.success(),
        "object-list request failed: {url}"
    );
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "object-list request returned {code}: {url}");
    fs::read_to_string(body.path())
        .expect("read object-list response")
        .matches("\"name\"")
        .count()
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

fn row_body(id: i64) -> String {
    let prefix = format!("row-{id:08}:");
    format!("{prefix}{}", "x".repeat(BODY_BYTES - prefix.len()))
}

fn rewritten_body() -> String {
    let prefix = "rewritten:";
    format!("{prefix}{}", "y".repeat(BODY_BYTES - prefix.len()))
}

fn second_body(id: i64) -> String {
    let prefix = format!("second-{id:08}:");
    format!("{prefix}{}", "q".repeat(BODY_BYTES - prefix.len()))
}

fn objects_before_path(state: &Path) -> PathBuf {
    state.with_extension("objects-before")
}

fn slot_map_path(state: &Path, label: &str) -> PathBuf {
    state.with_extension(format!("slots-{label}"))
}

fn assert_cache_bound(cache: &Path, context: &str) {
    // The public bcv_block virtual table exposes logical block numbers and
    // cache residency, but not the physical cachefile slot. This test
    // therefore infers slot reuse from writing more than the configured
    // payload capacity while the on-disk cache remains bounded.
    let size = fs::metadata(cache.join("cachefile.bcv")).map_or(0, |metadata| metadata.len());
    assert!(
        size <= CACHE_BYTES,
        "cachefile.bcv grew beyond {CACHE_BYTES} bytes ({size}) during {context}"
    );
}

fn inspect_cache_slot_map(cache: &Path) -> Vec<(i64, String)> {
    let metadata_path = cache.join("blocksdb.bcv");
    let metadata = Connection::open_with_flags(
        &metadata_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap_or_else(|error| panic!("open cache metadata {}: {error}", metadata_path.display()));
    let mut query = metadata
        .prepare(
            "SELECT cachefilepos, hex(blockid) FROM block \
             WHERE blockid IS NOT NULL ORDER BY cachefilepos",
        )
        .expect("prepare cache-slot metadata query");
    let slots = query
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .expect("query cache-slot metadata")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("read cache-slot metadata");
    let slot_count = CACHE_BYTES / (4 * 1024 * 1024);
    assert!(
        slots
            .iter()
            .all(|(position, _)| *position >= 0 && *position < slot_count as i64),
        "cache metadata contains an out-of-range physical slot: {slots:?}"
    );
    assert!(
        slots.len() <= slot_count as usize,
        "cache metadata has {} positions for {slot_count} slots: {slots:?}",
        slots.len()
    );
    slots
}

fn write_slot_map(path: &Path, slots: &[(i64, String)]) {
    let contents = slots
        .iter()
        .map(|(position, blockid)| format!("{position}:{blockid}\n"))
        .collect::<String>();
    fs::write(path, contents).expect("write cache-slot metadata snapshot");
}

fn read_slot_map(path: &Path) -> Vec<(i64, String)> {
    fs::read_to_string(path)
        .unwrap_or_else(|error| {
            panic!(
                "read cache-slot metadata snapshot {}: {error}",
                path.display()
            )
        })
        .lines()
        .map(|line| {
            let (position, blockid) = line
                .split_once(':')
                .unwrap_or_else(|| panic!("malformed cache-slot metadata snapshot line {line:?}"));
            (
                position
                    .parse::<i64>()
                    .unwrap_or_else(|error| panic!("invalid cache slot {position:?}: {error}")),
                blockid.to_owned(),
            )
        })
        .collect()
}

fn cache_metadata_summary(cache: &Path) -> String {
    let metadata_path = cache.join("blocksdb.bcv");
    let Ok(metadata) = Connection::open_with_flags(
        &metadata_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) else {
        return format!("unable to open {}", metadata_path.display());
    };
    let block_count = metadata
        .query_row("SELECT count(*) FROM block", [], |row| row.get::<_, i64>(0))
        .map_or_else(
            |error| format!("block_count=<error:{error}>"),
            |count| count.to_string(),
        );
    let staged = metadata
        .query_row(
            "SELECT count(*), count(DISTINCT dbpos) FROM staged",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .map_or_else(
            |error| format!("staged=<error:{error}>"),
            |(rows, positions)| format!("staged_rows={rows},staged_positions={positions}"),
        );
    let pending = metadata
        .query_row("SELECT count(*) FROM pending_publish", [], |row| {
            row.get::<_, i64>(0)
        })
        .map_or_else(
            |error| format!("pending_publish=<error:{error}>"),
            |count| format!("pending_publish={count}"),
        );
    format!("block_count={block_count},{staged},{pending}")
}

fn phase_inputs() -> Option<(PathBuf, PathBuf, String, String)> {
    std::env::var_os(CACHE_ENV)?;
    Some((
        PathBuf::from(required_env(CACHE_ENV)),
        PathBuf::from(required_env(STATE_ENV)),
        required_env(ENDPOINT_ENV),
        required_env(CONTAINER_ENV),
    ))
}

fn new_vfs(cache: &Path) -> &'static BlockCacheVfs {
    BlockCacheVfs::builder(cache)
        .expect("VFS builder")
        .auth_callback(|_, _, _| Ok("test-token".into()))
        .config(Config::CacheSize(CACHE_BYTES as i64))
        .init()
        .expect("initialize block-cache VFS")
}

fn restart_storage(endpoint: &str, container: &str) -> Storage {
    Storage::google_json_with_endpoint("test-project", container, endpoint)
}

#[test]
#[ignore = "child phase for staged-cache restart test"]
fn restart_phase_one() {
    let Some((cache, state, endpoint, container)) = phase_inputs() else {
        return;
    };
    let storage = restart_storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    vfs.initialize_container(&storage)
        .expect("initialize restart-test container");

    let local_dir = tempfile::tempdir().expect("local database directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local seed database");
    local
        .execute_batch(
            "PRAGMA page_size = 4096;
             CREATE TABLE payload(id INTEGER PRIMARY KEY, body TEXT NOT NULL);",
        )
        .expect("create seed schema");
    let _: String = local
        .query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })
        .expect("commit DoltLite seed database");
    local.close().expect("close local seed database");
    vfs.create_database(&storage, &local_path, "streaming.sqlite")
        .expect("upload restart-test seed database");

    let alias = "restart_staged";
    vfs.attach(&AttachSpec::new(storage).alias(alias))
        .expect("attach restart-test database");
    let before = fetch_manifest(&endpoint, &container);
    let objects_before = list_remote_objects(&endpoint, &container);
    assert!(
        ROWS * BODY_BYTES as i64 > CACHE_BYTES as i64,
        "restart workload must exceed the cache payload capacity"
    );
    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("open restart-test database");
    db.execute_batch("PRAGMA cache_size = 16; BEGIN IMMEDIATE;")
        .expect("begin restart-test update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare restart-test insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert restart-test row");
            if id % 256 == 0 {
                assert_cache_bound(&cache, "restart phase one write");
            }
        }
    }
    db.execute(
        "UPDATE payload SET body = ?1 WHERE id = 0",
        [rewritten_body()],
    )
    .expect("rewrite restart-test row");
    db.execute("DELETE FROM payload WHERE id >= ?1", [ROWS - DELETED_ROWS])
        .expect("truncate restart-test tail");
    db.execute_batch("COMMIT")
        .expect("commit restart-test update");
    drop(db);

    let first_slots = inspect_cache_slot_map(&cache);
    assert!(
        !first_slots.is_empty(),
        "restart phase one must leave resident cache slots to compare"
    );
    write_slot_map(&slot_map_path(&state, "first"), &first_slots);

    // A second update rewrites nearly every surviving row. It must reuse the
    // bounded physical slots rather than append another cachefile extent.
    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("reopen restart-test database for slot reuse");
    db.execute_batch("BEGIN IMMEDIATE")
        .expect("begin restart-test slot-reuse update");
    {
        let mut update = db
            .prepare("UPDATE payload SET body = ?1 WHERE id = ?2")
            .expect("prepare restart-test slot-reuse update");
        for id in 1..SLOT_REWRITE_ROWS {
            update
                .execute(params![second_body(id), id])
                .expect("rewrite restart-test slot-reuse row");
            if id % 256 == 0 {
                assert_cache_bound(&cache, "restart phase one slot-reuse write");
            }
        }
    }
    db.execute_batch("COMMIT")
        .expect("commit restart-test slot-reuse update");
    drop(db);

    let second_slots = inspect_cache_slot_map(&cache);
    write_slot_map(&slot_map_path(&state, "second"), &second_slots);

    assert_cache_bound(&cache, "restart phase one");
    assert_eq!(
        before,
        fetch_manifest(&endpoint, &container),
        "restart phase one must not publish the manifest"
    );
    assert!(
        list_remote_objects(&endpoint, &container) > objects_before,
        "restart phase one must stage at least one remote block object"
    );
    fs::write(&state, before).expect("save restart-test manifest snapshot");
    fs::write(
        objects_before_path(&state),
        objects_before.to_string().as_bytes(),
    )
    .expect("save restart-test object count");
}

#[test]
#[ignore = "child phase for staged-cache restart test"]
fn restart_phase_two() {
    let Some((cache, state, endpoint, container)) = phase_inputs() else {
        return;
    };
    let storage = restart_storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    let alias = "restart_staged";
    vfs.attach(&AttachSpec::new(storage).alias(alias).if_not(true))
        .expect("reattach restart-test database");
    assert_eq!(
        fs::read(state).expect("read restart-test manifest snapshot"),
        fetch_manifest(&endpoint, &container),
        "restart phase two must see the unpublished manifest"
    );

    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("reopen restart-test database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .unwrap_or_else(|error| {
            panic!(
                "count rows after restart: {error}; cache metadata: {}",
                cache_metadata_summary(&cache)
            )
        });
    assert_eq!(
        count,
        ROWS - DELETED_ROWS,
        "row count after restart differs; cache metadata: {}",
        cache_metadata_summary(&cache)
    );
    let rewritten: String = db
        .query_row("SELECT body FROM payload WHERE id = 0", [], |row| {
            row.get(0)
        })
        .unwrap_or_else(|error| {
            panic!(
                "read rewritten row after restart: {error}; row_count={count}; cache metadata: {}",
                cache_metadata_summary(&cache)
            )
        });
    assert!(rewritten.starts_with("rewritten"));
    let second: String = db
        .query_row(
            "SELECT body FROM payload WHERE id = ?1",
            [SLOT_REWRITE_ROWS / 2],
            |row| row.get(0),
        )
        .expect("read second slot-reuse update after restart");
    assert!(second.starts_with("second-"));
    for id in [ROWS / 2, ROWS - DELETED_ROWS - 1] {
        let length: i64 = db
            .query_row(
                "SELECT length(body) FROM payload WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .expect("read appended row after restart");
        assert_eq!(length, BODY_BYTES as i64);
    }
    drop(db);

    vfs.upload(alias).expect("publish restart-test manifest");
    vfs.detach(alias).expect("detach restart-test database");
}

#[test]
#[ignore = "child phase for staged-cache restart test"]
fn restart_phase_three() {
    let Some((cache, _state, endpoint, container)) = phase_inputs() else {
        return;
    };
    let storage = restart_storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    let alias = "restart_fresh";
    vfs.attach(&AttachSpec::new(storage).alias(alias))
        .expect("fresh-attach restart-test database");
    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("open fresh restart-test database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("count rows after fresh restart");
    assert_eq!(count, ROWS - DELETED_ROWS);
    let rewritten: String = db
        .query_row("SELECT body FROM payload WHERE id = 0", [], |row| {
            row.get(0)
        })
        .expect("read rewritten row after fresh restart");
    assert!(rewritten.starts_with("rewritten"));
    let second: String = db
        .query_row(
            "SELECT body FROM payload WHERE id = ?1",
            [SLOT_REWRITE_ROWS / 2],
            |row| row.get(0),
        )
        .expect("read second slot-reuse update after fresh restart");
    assert!(second.starts_with("second-"));
    drop(db);
    vfs.detach(alias)
        .expect("detach fresh restart-test database");
}

#[test]
#[ignore = "child phase for abrupt-termination restart test"]
fn crash_phase_one() {
    let Some((cache, state, endpoint, container)) = phase_inputs() else {
        return;
    };
    let marker = required_env(MARKER_ENV);
    let storage = restart_storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    vfs.initialize_container(&storage)
        .expect("initialize abrupt-termination container");

    let local_dir = tempfile::tempdir().expect("local database directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local seed database");
    local
        .execute_batch(
            "PRAGMA page_size = 4096;
             CREATE TABLE payload(id INTEGER PRIMARY KEY, body TEXT NOT NULL);",
        )
        .expect("create seed schema");
    let _: String = local
        .query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })
        .expect("commit DoltLite seed database");
    local.close().expect("close local seed database");
    vfs.create_database(&storage, &local_path, "streaming.sqlite")
        .expect("upload abrupt-termination seed database");

    let alias = "crash_staged";
    vfs.attach(&AttachSpec::new(storage).alias(alias))
        .expect("attach abrupt-termination database");
    let before = fetch_manifest(&endpoint, &container);
    let objects_before = list_remote_objects(&endpoint, &container);
    assert!(
        ROWS * BODY_BYTES as i64 > CACHE_BYTES as i64,
        "abrupt-termination workload must exceed the cache payload capacity"
    );
    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("open abrupt-termination database");
    db.execute_batch("PRAGMA cache_size = 16; BEGIN IMMEDIATE;")
        .expect("begin abrupt-termination update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare abrupt-termination insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert abrupt-termination row");
            if id % 256 == 0 {
                assert_cache_bound(&cache, "abrupt-termination write");
            }
        }
    }
    db.execute(
        "UPDATE payload SET body = ?1 WHERE id = 0",
        [rewritten_body()],
    )
    .expect("rewrite abrupt-termination row");
    db.execute("DELETE FROM payload WHERE id >= ?1", [ROWS - DELETED_ROWS])
        .expect("truncate abrupt-termination tail");
    db.execute_batch("COMMIT")
        .expect("commit abrupt-termination update");
    drop(db);

    assert_cache_bound(&cache, "before abrupt termination");
    assert_eq!(
        before,
        fetch_manifest(&endpoint, &container),
        "abrupt termination must occur before manifest publication"
    );
    assert!(
        list_remote_objects(&endpoint, &container) > objects_before,
        "abrupt-termination workload must stage remote block objects"
    );
    fs::write(&state, before).expect("save abrupt-termination manifest snapshot");
    fs::write(marker, b"ready").expect("write abrupt-termination marker");

    // This intentionally bypasses Rust and SQLite cleanup. SIGKILL avoids the
    // core dump that abort() may create in a developer checkout. It complements
    // fault-injection tests for unsynced metadata; it does not claim that
    // arbitrary in-flight writes survive a crash.
    let pid = std::process::id().to_string();
    let _ = Command::new("kill").args(["-KILL", &pid]).status();
    std::process::exit(137);
}

#[test]
#[ignore = "child phase for abrupt-termination restart test"]
fn crash_phase_two() {
    let Some((cache, state, endpoint, container)) = phase_inputs() else {
        return;
    };
    let storage = restart_storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    let alias = "crash_staged";
    vfs.attach(&AttachSpec::new(storage).alias(alias).if_not(true))
        .expect("recover abrupt-termination database");
    assert_eq!(
        fs::read(state).expect("read abrupt-termination manifest snapshot"),
        fetch_manifest(&endpoint, &container),
        "recovery must see the unpublished manifest"
    );

    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("open recovered database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .unwrap_or_else(|error| {
            panic!(
                "count recovered rows: {error}; cache metadata: {}",
                cache_metadata_summary(&cache)
            )
        });
    assert_eq!(
        count,
        ROWS - DELETED_ROWS,
        "recovered row count differs; cache metadata: {}",
        cache_metadata_summary(&cache)
    );
    let rewritten: String = db
        .query_row("SELECT body FROM payload WHERE id = 0", [], |row| {
            row.get(0)
        })
        .unwrap_or_else(|error| {
            panic!(
                "read recovered rewritten row: {error}; row_count={count}; cache metadata: {}",
                cache_metadata_summary(&cache)
            )
        });
    assert!(rewritten.starts_with("rewritten"));
    drop(db);

    vfs.upload(alias)
        .expect("publish recovered abrupt-termination update");
    vfs.detach(alias)
        .expect("detach recovered abrupt-termination database");
}

fn run_phase(executable: &Path, name: &str, values: &[(&str, &str)]) {
    let status = spawn_phase(executable, name, values);
    assert!(status.success(), "restart phase {name} failed: {status}");
}

fn spawn_phase(executable: &Path, name: &str, values: &[(&str, &str)]) -> std::process::ExitStatus {
    Command::new(executable)
        .args(["--ignored", "--exact", name, "--nocapture"])
        .envs(values.iter().copied())
        .status()
        .unwrap_or_else(|error| panic!("spawn {name}: {error}"))
}

#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn staged_cache_survives_process_restart_and_slot_reuse() {
    let cache = tempfile::tempdir().expect("restart-test cache directory");
    let state = tempfile::tempdir().expect("restart-test state directory");
    let state_file = state.path().join("manifest-before.bin");
    let endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".into());
    let bucket = "app_storage";
    let container = format!("{bucket}/{}/cbs", unique_suffix());
    ensure_google_bucket(&endpoint, bucket);

    let cache_path = cache.path().to_str().expect("cache path is UTF-8");
    let state_path = state_file.to_str().expect("state path is UTF-8");
    let values = [
        (CACHE_ENV, cache_path),
        (STATE_ENV, state_path),
        (ENDPOINT_ENV, endpoint.as_str()),
        (CONTAINER_ENV, container.as_str()),
    ];
    let executable = std::env::current_exe().expect("locate restart-test executable");
    run_phase(&executable, "restart_phase_one", &values);
    let objects_before: usize = fs::read_to_string(objects_before_path(&state_file))
        .expect("read restart-test object count")
        .parse()
        .expect("parse restart-test object count");
    let objects_after = list_remote_objects(&endpoint, &container);
    let first_slots = read_slot_map(&slot_map_path(&state_file, "first"));
    let second_slots = read_slot_map(&slot_map_path(&state_file, "second"));
    let reused_slot = first_slots.iter().any(|(position, blockid)| {
        second_slots.iter().any(|(other_position, other_blockid)| {
            other_position == position && other_blockid != blockid
        })
    });
    assert!(
        reused_slot,
        "no physical slot changed block IDs across the two updates: first={first_slots:?}, second={second_slots:?}"
    );
    assert!(
        objects_after > objects_before + second_slots.len(),
        "remote object growth does not demonstrate slot reuse: before={objects_before}, \
         after={objects_after}, first_slots={first_slots:?}, second_slots={second_slots:?}"
    );
    run_phase(&executable, "restart_phase_two", &values);
    run_phase(&executable, "restart_phase_three", &values);
}

#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn staged_cache_recovers_after_abrupt_termination() {
    let cache = tempfile::tempdir().expect("abrupt-test cache directory");
    let state = tempfile::tempdir().expect("abrupt-test state directory");
    let marker = tempfile::tempdir().expect("abrupt-test marker directory");
    let endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".into());
    let bucket = "app_storage";
    let container = format!("{bucket}/{}/cbs", unique_suffix());
    ensure_google_bucket(&endpoint, bucket);

    let cache_path = cache.path().to_str().expect("cache path is UTF-8");
    let state_path = state.path().join("manifest-before.bin");
    let state_path = state_path.to_str().expect("state path is UTF-8");
    let marker_path = marker.path().join("ready");
    let marker_path = marker_path.to_str().expect("marker path is UTF-8");
    let values = [
        (CACHE_ENV, cache_path),
        (STATE_ENV, state_path),
        (ENDPOINT_ENV, endpoint.as_str()),
        (CONTAINER_ENV, container.as_str()),
        (MARKER_ENV, marker_path),
    ];
    let executable = std::env::current_exe().expect("locate restart-test executable");
    let status = spawn_phase(&executable, "crash_phase_one", &values);
    assert!(
        !status.success(),
        "abrupt-termination phase unexpectedly exited cleanly: {status}"
    );
    assert!(
        fs::metadata(marker_path).is_ok(),
        "abrupt-termination child did not reach its kill point"
    );
    run_phase(&executable, "crash_phase_two", &values);
}

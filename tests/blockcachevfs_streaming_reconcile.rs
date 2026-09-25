#![cfg(feature = "blockcachevfs")]

use std::fs::{self, File, OpenOptions};
use std::io::{Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Config, Storage};
use rusqlite::{params, Connection, OpenFlags};

const BLOCK_BYTES: u64 = 4 * 1024 * 1024;
const CACHE_BYTES: i64 = 2 * BLOCK_BYTES as i64;
const DIRTY_CACHE_BYTES: i64 = 8 * BLOCK_BYTES as i64;
const DOWNSIZED_CACHE_BYTES: i64 = 2 * BLOCK_BYTES as i64;
const BODY_BYTES: usize = 2048;
const ROWS: i64 = 7_000;

const PHASE_ENV: &str = "BCV_STREAMING_RECONCILE_PHASE";
const CACHE_ENV: &str = "BCV_STREAMING_RECONCILE_CACHE";
const CONTAINER_ENV: &str = "BCV_STREAMING_RECONCILE_CONTAINER";

fn unique_suffix() -> String {
    format!(
        "rust-streaming-reconcile-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_nanos()
    )
}

fn endpoint() -> String {
    std::env::var("BLOCKCACHEVFS_S3_EMULATOR").unwrap_or_else(|_| "http://127.0.0.1:4566".into())
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for a child phase"))
}

fn phase_inputs() -> Option<(PathBuf, String, String)> {
    std::env::var_os(PHASE_ENV)?;
    Some((
        PathBuf::from(required_env(CACHE_ENV)),
        required_env(CONTAINER_ENV),
        endpoint(),
    ))
}

fn storage(endpoint: &str, container: &str) -> Storage {
    Storage::s3_with_endpoint("test", container, "us-east-1", endpoint)
}

fn new_vfs(cache: &Path, cache_bytes: i64) -> rusqlite::Result<&'static BlockCacheVfs> {
    BlockCacheVfs::builder(cache)
        .expect("VFS builder")
        .auth_callback(|_, _, _| Ok("test".into()))
        .config(Config::CacheSize(cache_bytes))
        .init()
}

fn new_dirty_fixture_vfs(cache: &Path) -> rusqlite::Result<&'static BlockCacheVfs> {
    BlockCacheVfs::builder(cache)
        .expect("VFS builder")
        .auth_callback(|_, _, _| Ok("test".into()))
        .config(Config::CacheSize(DIRTY_CACHE_BYTES))
        .config(Config::StageWatermark(100))
        .init()
}

fn row_body(id: i64) -> String {
    let prefix = format!("row-{id:08}:");
    format!("{prefix}{}", "x".repeat(BODY_BYTES - prefix.len()))
}

fn seed_database(vfs: &BlockCacheVfs, storage: &Storage) {
    let local_dir = tempfile::tempdir().expect("local database directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create seed database");
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
        .expect("commit seed database");
    local.close().expect("close seed database");
    vfs.create_database(storage, &local_path, "streaming.sqlite")
        .expect("upload seed database");
}

fn create_published_populated_database(vfs: &BlockCacheVfs, storage: &Storage) {
    let local_dir = tempfile::tempdir().expect("published local database directory");
    let local_path = local_dir.path().join("published.sqlite");
    let local = Connection::open(&local_path).expect("create published seed database");
    local
        .execute_batch(
            "PRAGMA page_size = 4096;
             CREATE TABLE payload(id INTEGER PRIMARY KEY, body TEXT NOT NULL);
             BEGIN;",
        )
        .expect("create published seed schema");
    {
        let mut insert = local
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare published seed insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert published seed row");
        }
    }
    local
        .execute_batch("COMMIT;")
        .expect("commit published seed rows");
    let _: String = local
        .query_row(
            "SELECT dolt_commit('-A', '-m', 'published seed')",
            [],
            |row| row.get(0),
        )
        .expect("commit published seed database");
    local.close().expect("close published seed database");
    vfs.create_database(storage, &local_path, "streaming.sqlite")
        .expect("publish populated seed database");
}

fn stage_large_update(vfs: &BlockCacheVfs, storage: &Storage, alias: &str) {
    vfs.initialize_container(storage)
        .expect("initialize reconcile-test container");
    seed_database(vfs, storage);
    vfs.attach(&AttachSpec::new(storage.clone()).alias(alias))
        .expect("attach reconcile-test database");
    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("open reconcile-test database");
    db.execute_batch("PRAGMA cache_size = 16; BEGIN IMMEDIATE;")
        .expect("begin large reconcile-test update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare large reconcile-test insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert large reconcile-test row");
        }
    }
    db.execute_batch("COMMIT")
        .expect("commit large reconcile-test update");
    drop(db);
}

fn cache_slots(cache: &Path) -> Vec<i64> {
    let metadata = Connection::open_with_flags(
        cache.join("blocksdb.bcv"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("open block-cache metadata");
    let mut statement = metadata
        .prepare("SELECT cachefilepos FROM block ORDER BY cachefilepos")
        .expect("prepare cache-slot query");
    statement
        .query_map([], |row| row.get::<_, i64>(0))
        .expect("query cache slots")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("read cache slots")
}

fn dirty_cache_slots(cache: &Path) -> Vec<i64> {
    let metadata = Connection::open_with_flags(
        cache.join("blocksdb.bcv"),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .expect("open block-cache metadata for dirty slots");
    let mut statement = metadata
        .prepare("SELECT cachefilepos FROM block WHERE blockid IS NULL ORDER BY cachefilepos")
        .expect("prepare dirty cache-slot query");
    statement
        .query_map([], |row| row.get::<_, i64>(0))
        .expect("query dirty cache slots")
        .collect::<rusqlite::Result<Vec<_>>>()
        .expect("read dirty cache slots")
}

fn assert_slots_are_well_formed(cache: &Path, expected_capacity: u64) -> Vec<i64> {
    let slots = cache_slots(cache);
    let slot_count = expected_capacity / BLOCK_BYTES;
    let mut sorted = slots.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        slots.len(),
        "cache metadata maps a slot twice"
    );
    assert!(
        slots
            .iter()
            .all(|slot| *slot >= 0 && (*slot as u64) < slot_count),
        "cache metadata contains a slot outside the configured cache: {slots:?}"
    );
    slots
}

fn spawn_phase(executable: &Path, phase: &str, cache: &Path, container: &str) -> ExitStatus {
    Command::new(executable)
        .args(["--ignored", "--exact", phase, "--nocapture"])
        .env(PHASE_ENV, phase)
        .env(CACHE_ENV, cache)
        .env(CONTAINER_ENV, container)
        .status()
        .unwrap_or_else(|error| panic!("spawn {phase}: {error}"))
}

fn run_phase(executable: &Path, phase: &str, cache: &Path, container: &str) {
    let status = spawn_phase(executable, phase, cache, container);
    assert!(status.success(), "child phase {phase} failed: {status}");
}

fn assert_error_mentions(error: &rusqlite::Error, expected: &str) {
    let text = format!("{error:?}").to_ascii_lowercase();
    assert!(
        text.contains(&expected.to_ascii_lowercase()),
        "expected SQLite error to mention {expected}, got {error:?}"
    );
}

fn run_orphan_stage_phase() {
    let Some((cache, container, endpoint)) = phase_inputs() else {
        return;
    };
    let vfs = new_vfs(&cache, CACHE_BYTES).expect("initialize orphan-tail VFS");
    let remote = storage(&endpoint, &container);
    stage_large_update(vfs, &remote, "orphan_tail");
}

fn run_orphan_recover_phase() {
    let Some((cache, container, endpoint)) = phase_inputs() else {
        return;
    };
    let vfs = new_vfs(&cache, CACHE_BYTES).expect("reinitialize orphan-tail VFS");
    vfs.attach(
        &AttachSpec::new(storage(&endpoint, &container))
            .alias("orphan_tail")
            .if_not(true),
    )
    .expect("reattach after orphan-tail truncation");
    let db = vfs
        .open("/orphan_tail/streaming.sqlite")
        .expect("open recovered orphan-tail database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("read recovered orphan-tail row count");
    assert_eq!(count, ROWS, "tail reconciliation lost committed rows");
    let body: String = db
        .query_row(
            "SELECT body FROM payload WHERE id = ?1",
            [ROWS - 1],
            |row| row.get(0),
        )
        .expect("read recovered orphan-tail row");
    assert_eq!(body, row_body(ROWS - 1));
}

fn run_short_reject_phase() {
    let Some((cache, container, endpoint)) = phase_inputs() else {
        return;
    };
    let result = new_vfs(&cache, CACHE_BYTES);
    match result {
        Err(error) => assert_error_mentions(&error, "corrupt"),
        Ok(vfs) => {
            let error = vfs
                .attach(
                    &AttachSpec::new(storage(&endpoint, &container))
                        .alias("dirty_cache")
                        .if_not(true),
                )
                .expect_err("short cachefile must fail closed before opening data");
            assert_error_mentions(&error, "corrupt");
        }
    }
}

fn run_resize_stage_phase() {
    let Some((cache, container, endpoint)) = phase_inputs() else {
        return;
    };
    let vfs = new_vfs(&cache, CACHE_BYTES).expect("initialize resize VFS");
    stage_large_update(vfs, &storage(&endpoint, &container), "resize_cache");
}

fn run_resize_small_phase() {
    let Some((cache, container, endpoint)) = phase_inputs() else {
        return;
    };
    let result = new_vfs(&cache, DOWNSIZED_CACHE_BYTES);
    match result {
        Err(error) => assert_error_mentions(&error, "full"),
        Ok(vfs) => {
            let error = vfs
                .attach(
                    &AttachSpec::new(storage(&endpoint, &container))
                        .alias("resize_cache")
                        .if_not(true),
                )
                .expect_err("downsizing below high-water must be rejected");
            assert_error_mentions(&error, "full");
        }
    }
}

fn run_resize_recover_phase() {
    let Some((cache, container, endpoint)) = phase_inputs() else {
        return;
    };
    let vfs = new_vfs(&cache, DIRTY_CACHE_BYTES).expect("reinitialize resize VFS");
    vfs.attach(
        &AttachSpec::new(storage(&endpoint, &container))
            .alias("resize_cache")
            .if_not(true),
    )
    .expect("reattach after rejected downsizing");
    let db = vfs
        .open("/resize_cache/streaming.sqlite")
        .expect("open database after rejected downsizing");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("read row count after rejected downsizing");
    assert_eq!(count, ROWS, "rejected downsizing lost dirty cache data");
}

fn run_clean_cache_stage_phase() {
    let Some((cache, container, endpoint)) = phase_inputs() else {
        return;
    };
    let vfs = new_vfs(&cache, CACHE_BYTES).expect("initialize clean-cache VFS");
    let remote = storage(&endpoint, &container);
    vfs.initialize_container(&remote)
        .expect("initialize clean-cache container");
    create_published_populated_database(vfs, &remote);
    vfs.attach(&AttachSpec::new(remote).alias("clean_cache"))
        .expect("attach published clean-cache database");
    let db = vfs
        .open("/clean_cache/streaming.sqlite")
        .expect("open published clean-cache database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("read published clean-cache row count");
    assert_eq!(count, ROWS);
    let body: String = db
        .query_row(
            "SELECT body FROM payload WHERE id = ?1",
            [ROWS - 1],
            |row| row.get(0),
        )
        .expect("read published clean-cache row");
    assert_eq!(body, row_body(ROWS - 1));
}

fn run_clean_cache_recover_phase() {
    let Some((cache, container, endpoint)) = phase_inputs() else {
        return;
    };
    let vfs = new_vfs(&cache, CACHE_BYTES).expect("reinitialize clean-cache VFS");
    vfs.attach(
        &AttachSpec::new(storage(&endpoint, &container))
            .alias("clean_cache")
            .if_not(true),
    )
    .expect("reattach published clean-cache database");
    let db = vfs
        .open("/clean_cache/streaming.sqlite")
        .expect("open clean-cache database after payload corruption");
    let (count, total): (i64, i64) = db
        .query_row(
            "SELECT count(*), coalesce(sum(length(body)), 0) FROM payload",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read clean-cache database after restart");
    assert_eq!(count, ROWS, "restart trusted corrupted clean cache bytes");
    assert_eq!(
        total,
        (ROWS as usize * BODY_BYTES) as i64,
        "restart returned corrupted remote payload"
    );
    let body: String = db
        .query_row(
            "SELECT body FROM payload WHERE id = ?1",
            [ROWS - 1],
            |row| row.get(0),
        )
        .expect("read remote row after clean-cache recovery");
    assert_eq!(body, row_body(ROWS - 1));
}

fn run_dirty_cache_stage_phase(alias: &str) {
    let Some((cache, container, endpoint)) = phase_inputs() else {
        return;
    };
    let vfs = new_dirty_fixture_vfs(&cache).expect("initialize dirty-cache VFS");
    let remote = storage(&endpoint, &container);
    vfs.initialize_container(&remote)
        .expect("initialize dirty-cache container");
    create_published_populated_database(vfs, &remote);
    vfs.attach(&AttachSpec::new(remote).alias(alias))
        .expect("attach published dirty-cache database");
    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("open published dirty-cache database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("read dirty-cache source rows");
    assert_eq!(count, ROWS);
    db.execute(
        "UPDATE payload SET body = ?1",
        [format!("dirty:{}", "z".repeat(BODY_BYTES - 6))],
    )
    .expect("rewrite all rows into dirty cache mappings");
    drop(db);
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn orphan_cache_tail_is_truncated_and_data_survives_restart() {
    let cache = tempfile::tempdir().expect("orphan-tail cache directory");
    let container = format!("{}/{}", unique_suffix(), "cbs");
    let executable = std::env::current_exe().expect("locate reconcile-test executable");
    run_phase(&executable, "orphan_stage_phase", cache.path(), &container);

    let cachefile = cache.path().join("cachefile.bcv");
    let original_len = fs::metadata(&cachefile)
        .expect("cachefile after orphan-tail staging")
        .len();
    assert!(original_len > 0, "staging did not create a cachefile");
    let slots = assert_slots_are_well_formed(cache.path(), CACHE_BYTES as u64);
    assert!(
        !slots.is_empty(),
        "staging did not persist a mapped cache slot"
    );

    let mut file = OpenOptions::new()
        .append(true)
        .open(&cachefile)
        .expect("open cachefile for orphan-tail injection");
    file.write_all(&vec![0xa5; (2 * BLOCK_BYTES) as usize])
        .expect("append orphan cachefile tail");
    file.sync_all().expect("sync orphan cachefile tail");
    let tailed_len = fs::metadata(&cachefile)
        .expect("stat cachefile after orphan-tail injection")
        .len();
    assert!(tailed_len > original_len);

    run_phase(
        &executable,
        "orphan_recover_phase",
        cache.path(),
        &container,
    );
    let recovered_len = fs::metadata(&cachefile)
        .expect("cachefile after orphan-tail recovery")
        .len();
    assert_eq!(
        recovered_len, original_len,
        "restart did not truncate bytes beyond the durable slot range"
    );
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn short_cachefile_fails_closed_without_serving_mapped_data() {
    let cache = tempfile::tempdir().expect("short-cache cache directory");
    let container = format!("{}/{}", unique_suffix(), "cbs");
    let executable = std::env::current_exe().expect("locate reconcile-test executable");
    run_phase(
        &executable,
        "dirty_short_stage_phase",
        cache.path(),
        &container,
    );

    let cachefile = cache.path().join("cachefile.bcv");
    let original_len = fs::metadata(&cachefile)
        .expect("cachefile after short-cache staging")
        .len();
    assert!(original_len > 0, "staging did not create a cachefile");
    let slots = assert_slots_are_well_formed(cache.path(), DIRTY_CACHE_BYTES as u64);
    assert!(
        !slots.is_empty(),
        "short-cache test needs a mapped cache slot"
    );
    let dirty_slots = dirty_cache_slots(cache.path());
    assert!(
        !dirty_slots.is_empty() && dirty_slots.iter().any(|slot| *slot >= 2),
        "short-cache fixture did not retain enough dirty high-water mappings: {dirty_slots:?}"
    );
    let expected_len = (dirty_slots
        .iter()
        .max()
        .expect("dirty-cache fixture must have a high slot")
        + 1) as u64
        * BLOCK_BYTES;
    assert!(
        original_len >= expected_len,
        "dirty mapping lies beyond cachefile length: slots={dirty_slots:?}, len={original_len}"
    );
    File::options()
        .write(true)
        .open(&cachefile)
        .expect("open cachefile for short-file injection")
        .set_len(expected_len - 1)
        .expect("truncate cachefile below durable slot range");

    run_phase(&executable, "short_reject_phase", cache.path(), &container);
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn cache_size_downsize_rejects_existing_high_water() {
    let cache = tempfile::tempdir().expect("resize cache directory");
    let container = format!("{}/{}", unique_suffix(), "cbs");
    let executable = std::env::current_exe().expect("locate reconcile-test executable");
    run_phase(
        &executable,
        "dirty_resize_stage_phase",
        cache.path(),
        &container,
    );

    let cachefile_len = fs::metadata(cache.path().join("cachefile.bcv"))
        .expect("cachefile after resize staging")
        .len();
    assert!(
        cachefile_len >= 3 * BLOCK_BYTES,
        "resize workload did not reach the dirty high-water mark: {cachefile_len}"
    );
    let slots = assert_slots_are_well_formed(cache.path(), DIRTY_CACHE_BYTES as u64);
    assert!(
        slots.iter().any(|slot| *slot >= 2),
        "resize workload did not persist a slot above the smaller cache capacity: {slots:?}"
    );
    let dirty_slots = dirty_cache_slots(cache.path());
    assert!(
        !dirty_slots.is_empty() && dirty_slots.iter().any(|slot| *slot >= 2),
        "resize fixture did not retain enough dirty high-water mappings: {dirty_slots:?}"
    );

    run_phase(&executable, "resize_small_phase", cache.path(), &container);
    run_phase(
        &executable,
        "resize_recover_phase",
        cache.path(),
        &container,
    );
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn clean_cache_payload_corruption_is_refetched_after_restart() {
    let cache = tempfile::tempdir().expect("clean-cache directory");
    let container = format!("{}/{}", unique_suffix(), "cbs");
    let executable = std::env::current_exe().expect("locate reconcile-test executable");
    run_phase(
        &executable,
        "clean_cache_stage_phase",
        cache.path(),
        &container,
    );

    let cachefile = cache.path().join("cachefile.bcv");
    let original_len = fs::metadata(&cachefile)
        .expect("cachefile after clean-cache staging")
        .len();
    let slots = assert_slots_are_well_formed(cache.path(), CACHE_BYTES as u64);
    let slot = *slots
        .first()
        .expect("published read must leave a clean mapped cache slot");
    let offset = slot as u64 * BLOCK_BYTES;
    assert!(
        offset + BLOCK_BYTES <= original_len,
        "mapped clean slot is outside the cachefile: slot={slot}, len={original_len}"
    );

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&cachefile)
        .expect("open cachefile for clean-slot corruption");
    file.seek(SeekFrom::Start(offset))
        .expect("seek to clean cache slot");
    file.write_all(&vec![0xa5; BLOCK_BYTES as usize])
        .expect("overwrite clean cache slot");
    file.sync_all().expect("sync clean cache corruption");
    assert_eq!(
        fs::metadata(&cachefile)
            .expect("stat cachefile after clean-slot corruption")
            .len(),
        original_len,
        "clean-slot corruption unexpectedly changed cachefile length"
    );

    run_phase(
        &executable,
        "clean_cache_recover_phase",
        cache.path(),
        &container,
    );
}

#[test]
#[ignore = "child phase for reconciliation integration tests"]
fn orphan_stage_phase() {
    if std::env::var_os(PHASE_ENV).is_some() {
        run_orphan_stage_phase();
    }
}

#[test]
#[ignore = "child phase for reconciliation integration tests"]
fn orphan_recover_phase() {
    if std::env::var_os(PHASE_ENV).is_some() {
        run_orphan_recover_phase();
    }
}

#[test]
#[ignore = "child phase for reconciliation integration tests"]
fn short_reject_phase() {
    if std::env::var_os(PHASE_ENV).is_some() {
        run_short_reject_phase();
    }
}

#[test]
#[ignore = "child phase for reconciliation integration tests"]
fn resize_stage_phase() {
    if std::env::var_os(PHASE_ENV).is_some() {
        run_resize_stage_phase();
    }
}

#[test]
#[ignore = "child phase for reconciliation integration tests"]
fn resize_small_phase() {
    if std::env::var_os(PHASE_ENV).is_some() {
        run_resize_small_phase();
    }
}

#[test]
#[ignore = "child phase for reconciliation integration tests"]
fn resize_recover_phase() {
    if std::env::var_os(PHASE_ENV).is_some() {
        run_resize_recover_phase();
    }
}

#[test]
#[ignore = "child phase for reconciliation integration tests"]
fn clean_cache_stage_phase() {
    if std::env::var_os(PHASE_ENV).is_some() {
        run_clean_cache_stage_phase();
    }
}

#[test]
#[ignore = "child phase for reconciliation integration tests"]
fn clean_cache_recover_phase() {
    if std::env::var_os(PHASE_ENV).is_some() {
        run_clean_cache_recover_phase();
    }
}

#[test]
#[ignore = "child phase for reconciliation integration tests"]
fn dirty_short_stage_phase() {
    if std::env::var_os(PHASE_ENV).is_some() {
        run_dirty_cache_stage_phase("dirty_cache");
    }
}

#[test]
#[ignore = "child phase for reconciliation integration tests"]
fn dirty_resize_stage_phase() {
    if std::env::var_os(PHASE_ENV).is_some() {
        run_dirty_cache_stage_phase("resize_cache");
    }
}

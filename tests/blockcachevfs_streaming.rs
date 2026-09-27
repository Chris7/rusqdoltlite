#![cfg(feature = "blockcachevfs")]

use std::collections::BTreeSet;
use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Config, Storage};
#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
use rusqlite::RemoteServer;
use rusqlite::{params, Connection};

const CACHE_BYTES: i64 = 2 * 4 * 1024 * 1024;
const BODY_BYTES: usize = 2048;
const ROWS: i64 = 7_000;
const DELETED_ROWS: i64 = 100;

struct RemoteManifest {
    bytes: Vec<u8>,
    etag: Option<String>,
}

fn unique_suffix() -> String {
    format!(
        "rust-streaming-{}-{}",
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
        .expect("streaming emulator container must contain a prefix");
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
        _ => unreachable!("unknown streaming backend {backend}"),
    }
}

fn list_remote_objects(backend: &str, endpoint: &str, container: &str) -> usize {
    let (bucket, prefix) = container
        .split_once('/')
        .expect("streaming emulator container must contain a prefix");
    let endpoint = endpoint.trim_end_matches('/');
    let prefix = format!("{prefix}/");
    let url = match backend {
        "google" => format!(
            "{endpoint}/storage/v1/b/{}/o?prefix={}&maxResults=1000",
            encode_component(bucket),
            encode_component(&prefix)
        ),
        "s3" => format!(
            "{endpoint}/{}?list-type=2&prefix={}&max-keys=1000",
            encode_component(bucket),
            encode_component(&prefix)
        ),
        _ => unreachable!("unknown streaming backend {backend}"),
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
    let body = fs::read_to_string(body.path()).expect("read object-list response");
    match backend {
        "google" => body.matches("\"name\"").count(),
        "s3" => body.matches("<Key>").count(),
        _ => unreachable!("unknown streaming backend {backend}"),
    }
}

fn fetch_manifest(backend: &str, url: &str) -> RemoteManifest {
    let body = tempfile::NamedTempFile::new().expect("manifest body temporary file");
    let headers = tempfile::NamedTempFile::new().expect("manifest headers temporary file");
    let mut command = Command::new("curl");
    command.args([
        "--silent",
        "--show-error",
        "--dump-header",
        headers.path().to_str().expect("header path is UTF-8"),
        "--output",
        body.path().to_str().expect("body path is UTF-8"),
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

    let header_text = fs::read_to_string(headers.path()).expect("read manifest headers");
    let etag = header_text.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("etag")
            .then(|| value.trim().to_owned())
    });
    RemoteManifest {
        bytes: fs::read(body.path()).expect("read manifest body"),
        etag,
    }
}

fn assert_cache_bound(cache: &std::path::Path, context: &str) {
    let cache_file = cache.join("cachefile.bcv");
    let size = fs::metadata(&cache_file).map_or(0, |metadata| metadata.len());
    assert!(
        size <= CACHE_BYTES as u64,
        "cachefile.bcv grew beyond {CACHE_BYTES} bytes ({size}) during {context}"
    );

    // The payload cache is one reusable file. A block eviction must not leave
    // one local file per evicted block behind it. The public bcv_block table
    // does not expose physical slot numbers, so slot reuse is inferred from
    // the workload exceeding CACHE_BYTES while this file remains bounded.
    let allowed = BTreeSet::from([
        ".blocksdb.bcv-lock",
        "blocksdb.bcv",
        "cachefile.bcv",
        "portnumber.bcv",
    ]);
    for entry in fs::read_dir(cache).expect("read cache directory") {
        let entry = entry.expect("read cache directory entry");
        if entry.file_type().expect("inspect cache entry").is_file() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            assert!(
                allowed.contains(name.as_ref())
                    || name.starts_with("blocksdb.bcv-")
                    || name.starts_with("cachefile.bcv-"),
                "unexpected per-block cache payload {name:?} during {context}"
            );
        }
    }
}

fn row_body(id: i64) -> String {
    let prefix = format!("row-{id:08}:");
    format!("{prefix}{}", "x".repeat(BODY_BYTES - prefix.len()))
}

fn rewritten_body() -> String {
    let prefix = "rewritten:";
    format!("{prefix}{}", "y".repeat(BODY_BYTES - prefix.len()))
}

fn pragma_i64(db: &Connection, pragma: &str) -> i64 {
    db.query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))
        .unwrap_or_else(|error| panic!("read PRAGMA {pragma}: {error:?}"))
}

fn setup(backend: &str, vfs: &'static BlockCacheVfs) -> (String, String, Storage) {
    let suffix = unique_suffix();
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4443".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown streaming backend {backend}"),
    };
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rust-streaming-{suffix}")
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
        .expect("initialize remote CBS container");

    let local_dir = tempfile::tempdir().expect("local database directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local SQLite database");
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
    local.close().expect("close seed database");
    vfs.create_database(&storage, &local_path, "streaming.sqlite")
        .expect("upload seed database");

    (endpoint, container, storage)
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
fn push_large_native_remote(db: &Connection) {
    let server_dir = tempfile::tempdir().expect("remote server directory");
    let server_root = server_dir.path().join("server");
    fs::create_dir(&server_root).expect("create remote server directory");
    let server = RemoteServer::start(&server_root).expect("start native remote server");
    let remote_url = server.database_url("streaming-remote.db");

    let _: String = db
        .query_row("SELECT dolt_commit('-A', '-m', 'streaming')", [], |row| {
            row.get(0)
        })
        .unwrap_or_else(|error| panic!("commit large native DoltLite update: {error:?}"));
    let _: i64 = db
        .query_row(
            "SELECT dolt_remote('add', 'origin', ?1)",
            [remote_url],
            |row| row.get(0),
        )
        .unwrap_or_else(|error| panic!("configure native remote: {error:?}"));
    let _: i64 = db
        .query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))
        .unwrap_or_else(|error| panic!("push large native DoltLite update: {error:?}"));
}

fn run_streaming(backend: &str, cache: &std::path::Path, vfs: &'static BlockCacheVfs) {
    let (endpoint, container, storage) = setup(backend, vfs);
    let alias = format!("streaming_{}", std::process::id());
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&alias))
        .expect("attach streaming database");
    let url = manifest_url(backend, &endpoint, &container);
    let before = fetch_manifest(backend, &url);
    let objects_before = list_remote_objects(backend, &endpoint, &container);
    assert!(
        ROWS * BODY_BYTES as i64 > CACHE_BYTES,
        "streaming workload must exceed the cache payload capacity"
    );
    let path = format!("/{alias}/streaming.sqlite");
    let db = vfs.open(&path).expect("open streaming database");
    db.execute_batch("PRAGMA cache_size = 16; BEGIN IMMEDIATE;")
        .expect("begin large update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare large insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert large update row");
            if id % 256 == 0 {
                assert_cache_bound(cache, "large write");
            }
        }
    }
    db.execute(
        "UPDATE payload SET body = ?1 WHERE id = 0",
        [rewritten_body()],
    )
    .expect("rewrite staged row");
    db.execute("DELETE FROM payload WHERE id >= ?1", [ROWS - DELETED_ROWS])
        .expect("truncate staged tail");
    db.execute_batch("COMMIT").expect("commit large update");

    // Deleting rows leaves the logical table smaller but does not require
    // SQLite to call the VFS xTruncate callback. Exercise the physical
    // rollback route separately: grow the database in a transaction, then
    // roll it back and require the page count to return to its pre-write
    // extent. This catches a truncate implementation that leaves an appended
    // tail mapped to stale cache slots while keeping the published contents
    // untouched.
    let pages_before_rollback = pragma_i64(&db, "page_count");
    db.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE truncate_probe(id INTEGER PRIMARY KEY, body BLOB NOT NULL);",
    )
    .expect("begin physical-truncate probe");
    {
        let mut insert = db
            .prepare("INSERT INTO truncate_probe(id, body) VALUES (?1, ?2)")
            .expect("prepare physical-truncate probe");
        let body = vec![b'z'; BODY_BYTES];
        for id in 0..2_048_i64 {
            insert
                .execute(params![id, &body])
                .expect("grow physical-truncate probe");
        }
    }
    let pages_during_rollback = pragma_i64(&db, "page_count");
    assert!(
        pages_during_rollback > pages_before_rollback,
        "physical-truncate probe did not extend the database: before={pages_before_rollback}, during={pages_during_rollback}"
    );
    db.execute_batch("ROLLBACK")
        .expect("rollback physical-truncate probe");
    let pages_after_rollback = pragma_i64(&db, "page_count");
    assert_eq!(
        pages_after_rollback, pages_before_rollback,
        "rollback did not physically truncate the appended database tail: before={pages_before_rollback}, after={pages_after_rollback}"
    );
    let probe_tables: i64 = db
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='truncate_probe'",
            [],
            |row| row.get(0),
        )
        .expect("check rolled-back physical-truncate probe");
    assert_eq!(probe_tables, 0, "rolled-back table remains visible");
    #[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
    push_large_native_remote(&db);
    assert_cache_bound(cache, "post-write commit");

    let after_write = fetch_manifest(backend, &url);
    let objects_after_write = list_remote_objects(backend, &endpoint, &container);
    assert_eq!(
        before.bytes, after_write.bytes,
        "remote manifest changed before explicit upload"
    );
    assert!(
        objects_after_write > objects_before,
        "large write did not stage any additional remote block objects: before={objects_before}, after={objects_after_write}"
    );
    if let (Some(before_etag), Some(after_etag)) = (&before.etag, &after_write.etag) {
        assert_eq!(
            before_etag, after_etag,
            "remote manifest ETag changed before explicit upload"
        );
    }

    // Closing the SQLite connection drops its page cache. Reopening through
    // the same VFS forces reads to recover current staged blocks, including
    // appended blocks whose local slots may already have been reused.
    drop(db);
    let db = vfs.open(&path).expect("reopen staged database");
    let rewritten: String = db
        .query_row("SELECT body FROM payload WHERE id = 0", [], |row| {
            row.get(0)
        })
        .expect("read rewritten staged row");
    assert_eq!(rewritten.len(), BODY_BYTES);
    assert!(rewritten.starts_with("rewritten"));
    for id in [ROWS / 2, ROWS - DELETED_ROWS - 1] {
        let length: i64 = db
            .query_row(
                "SELECT length(body) FROM payload WHERE id = ?1",
                [id],
                |row| row.get(0),
            )
            .expect("read appended staged row");
        assert_eq!(length, BODY_BYTES as i64);
    }
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("count staged rows");
    assert_eq!(count, ROWS - DELETED_ROWS);
    let expected_bytes = (ROWS - DELETED_ROWS) * BODY_BYTES as i64;
    let total: i64 = db
        .query_row("SELECT sum(length(body)) FROM payload", [], |row| {
            row.get(0)
        })
        .expect("scan staged rows");
    assert_eq!(total, expected_bytes);
    drop(db);
    assert_cache_bound(cache, "first full scan");

    let db = vfs.open(&path).expect("reopen for repeated staged scan");
    let repeated_total: i64 = db
        .query_row("SELECT sum(length(body)) FROM payload", [], |row| {
            row.get(0)
        })
        .expect("repeat scan staged rows");
    assert_eq!(repeated_total, expected_bytes);
    drop(db);
    assert_cache_bound(cache, "repeated full scan");

    vfs.upload(&alias).expect("publish final manifest");
    let after_upload = fetch_manifest(backend, &url);
    assert_ne!(
        before.bytes, after_upload.bytes,
        "explicit upload did not publish the final manifest"
    );
    vfs.detach(&alias).expect("detach uploaded database");

    // A second attachment reads only the newly published manifest. This is a
    // fresh attachment check; it deliberately reuses the process-lifetime VFS
    // and cache because the Rust API owns one VFS singleton per process.
    let fresh_alias = format!("{alias}_fresh");
    vfs.attach(&AttachSpec::new(storage).alias(&fresh_alias))
        .expect("fresh-attach uploaded database");
    let fresh_path = format!("/{fresh_alias}/streaming.sqlite");
    let fresh = vfs.open(&fresh_path).expect("open fresh attached database");
    let fresh_count: i64 = fresh
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("read fresh attached row count");
    assert_eq!(fresh_count, ROWS - DELETED_ROWS);
    let fresh_total: i64 = fresh
        .query_row("SELECT sum(length(body)) FROM payload", [], |row| {
            row.get(0)
        })
        .expect("read fresh attached scan");
    assert_eq!(fresh_total, expected_bytes);
    let fresh_rewrite: String = fresh
        .query_row("SELECT body FROM payload WHERE id = 0", [], |row| {
            row.get(0)
        })
        .expect("read fresh rewritten row");
    assert!(fresh_rewrite.starts_with("rewritten"));
    assert_cache_bound(cache, "fresh attachment");
    drop(fresh);
    vfs.detach(&fresh_alias)
        .expect("detach fresh attached database");
}

#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_and_s3_emulator_streaming_write() {
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
    run_streaming("google", cache.path(), vfs);
    run_streaming("s3", cache.path(), vfs);
}

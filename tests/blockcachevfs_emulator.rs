#![cfg(feature = "blockcachevfs")]

use std::ffi::{CStr, CString};
#[cfg(feature = "remote")]
use std::fs;
#[cfg(feature = "remote")]
use std::io::{self, Read as _, Write as _};
#[cfg(feature = "remote")]
use std::net::{Shutdown, TcpListener, TcpStream};
#[cfg(all(feature = "remote", unix))]
use std::os::unix::process::ExitStatusExt as _;
#[cfg(feature = "remote")]
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
#[cfg(feature = "remote")]
use std::path::PathBuf;
use std::process::Command;
use std::ptr;
#[cfg(feature = "remote")]
use std::sync::{
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    Mutex,
};
use std::sync::{mpsc, Arc, Barrier, OnceLock};
use std::thread;
use std::time::Duration;
#[cfg(feature = "remote")]
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{
    AttachSpec, BlockCacheVfs, Config, SessionAttachment, SessionOperationId, Storage,
    SESSION_OPERATION_ID_BYTES,
};
#[cfg(feature = "remote")]
use rusqlite::blockcachevfs::{
    AuthError, AuthRefreshReason, StorageFailureCause, StorageFailurePhase, UploadProgress,
};
use rusqlite::ffi::{self, blockcachevfs as raw_bcv};
#[cfg(feature = "remote")]
use rusqlite::SessionOperationStatus;
use rusqlite::{params, Connection, OpenFlags};
#[cfg(feature = "remote")]
use rusqlite::{BlockCacheSessionOptions, RemoteServer, RemoteServerOptions, SessionScope};
use uuid::Uuid;

fn unique_suffix() -> String {
    format!(
        "rust-bootstrap-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_nanos()
    )
}

fn no_create_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX
}

fn uri_for_container(backend: &str, endpoint: &str, bucket: &str, prefix: &str) -> String {
    match backend {
        "google" => format!(
            "gcs://{bucket}/{prefix}?vfs=blockcachevfs&project=test-project&access_token=test-token&endpoint={endpoint}"
        ),
        "s3" => format!(
            "s3://{bucket}/{prefix}?vfs=blockcachevfs&region=us-east-1&access_id=test&secret_access_key=test&endpoint={endpoint}"
        ),
        _ => unreachable!(),
    }
}

fn assert_empty_sqlite_database(database: &Connection) {
    let schema_entries: i64 = database
        .query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))
        .expect("read empty SQLite schema");
    let user_version: i64 = database
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read empty SQLite user_version");

    assert_eq!(schema_entries, 0);
    assert_eq!(user_version, 0);
}

fn attached_database_state(vfs: &BlockCacheVfs, alias: &str, database: &str) -> (i64, String) {
    let control = vfs
        .open(format!("/{alias}"))
        .expect("open CBS control connection");
    let state = control
        .query_row(
            "SELECT nblock, state FROM bcv_database WHERE container = ?1 AND database = ?2",
            params![alias, database],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read CBS database metadata");
    control.close().expect("close CBS control connection");
    state
}

fn run_uri_auto_create(backend: &str) {
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4443".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!(),
    };
    let suffix = unique_suffix();
    let bucket = if backend == "google" {
        ensure_google_bucket(&endpoint, "app_storage");
        "app_storage".to_owned()
    } else {
        format!("rust-create-{suffix}")
    };
    let prefix = format!("{suffix}/uri/create/");
    let uri = uri_for_container(backend, &endpoint, &bucket, &prefix);

    let empty_prefix = format!("{suffix}/uri/read-only/");
    let empty_container = format!("{bucket}/{empty_prefix}");
    let empty_storage = if backend == "google" {
        Storage::google_json_with_endpoint("test-project", &empty_container, &endpoint)
    } else {
        Storage::s3_with_endpoint("test", &empty_container, "us-east-1", &endpoint)
    };
    let cache = tempfile::tempdir().expect("empty database VFS cache directory");
    let vfs = BlockCacheVfs::builder(cache.path())
        .expect("empty database VFS builder")
        .auth_callback(|provider, _, _| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init_owned()
        .expect("initialize empty database VFS");
    vfs.initialize_container(&empty_storage)
        .expect("initialize empty database container");
    let empty_alias = format!("empty_{}", std::process::id());
    vfs.attach(&AttachSpec::new(empty_storage.clone()).alias(&empty_alias))
        .expect("attach empty database container");
    let empty_path = format!("/{empty_alias}/empty.sqlite");
    let empty = vfs
        .open_with_flags(
            &empty_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .expect("create an empty SQLite database");
    empty.close().expect("close created empty database");
    assert_eq!(
        attached_database_state(&vfs, &empty_alias, "empty.sqlite"),
        (1, "copied".to_owned()),
        "SQLite CREATE should write its standard header page without a schema"
    );

    let read_only = vfs
        .open_with_flags(
            &empty_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .expect("open empty database read-only before upload");
    read_only
        .close()
        .expect("close pre-upload read-only database");
    assert_eq!(
        attached_database_state(&vfs, &empty_alias, "empty.sqlite"),
        (1, "copied".to_owned()),
        "read-only open without CREATE must find the pending empty database"
    );

    vfs.upload(&empty_alias)
        .expect("persist empty database metadata");
    assert_eq!(
        attached_database_state(&vfs, &empty_alias, "empty.sqlite"),
        (1, String::new()),
        "upload should persist the empty SQLite database"
    );
    vfs.detach(&empty_alias)
        .expect("detach uploaded empty database");
    vfs.attach(&AttachSpec::new(empty_storage).alias(&empty_alias))
        .expect("reattach uploaded empty database");
    assert_eq!(
        attached_database_state(&vfs, &empty_alias, "empty.sqlite"),
        (1, String::new()),
        "the CBS manifest should retain the empty database after reattach"
    );
    let read_only = vfs
        .open_with_flags(
            &empty_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .expect("open uploaded empty database read-only without CREATE");
    assert_empty_sqlite_database(&read_only);
    read_only
        .close()
        .expect("close post-upload read-only database");
    vfs.detach(&empty_alias)
        .expect("detach re-opened empty database");

    let no_sql_uri = uri_for_container(
        backend,
        &endpoint,
        &bucket,
        &format!("{suffix}/uri/create-no-sql/"),
    );
    let no_sql = Connection::open(&no_sql_uri)
        .expect("CREATE should open an empty database without caller SQL");
    no_sql
        .upload()
        .expect("upload an empty database without caller SQL");
    no_sql.close().expect("close no-SQL empty database");
    let no_sql = Connection::open_with_flags(&no_sql_uri, no_create_flags())
        .expect("reopen no-SQL upload without CREATE");
    assert_empty_sqlite_database(&no_sql);
    no_sql.close().expect("close reopened no-SQL database");

    let database = Connection::open(&uri).expect("CREATE should initialize an empty CBS database");
    let journal_mode: String = database
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("CBS should report its forced WAL journal mode");
    assert_eq!(journal_mode, "wal");
    let journal_mode: String = database
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .expect("requesting the already-active WAL mode should be a no-op");
    assert_eq!(journal_mode, "wal");
    let error = database
        .query_row::<String, _, _>("PRAGMA journal_mode = DELETE", [], |row| row.get(0))
        .expect_err("CBS must reject unsupported journal mode changes");
    assert!(error
        .to_string()
        .contains("blockcachevfs supports only journal_mode=WAL"));
    let schema_entries: i64 = database
        .query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))
        .expect("new CBS database should be a valid empty SQLite database");
    assert_eq!(schema_entries, 0);
    let user_version: i64 = database
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read the empty database user_version");
    assert_eq!(user_version, 0);
    database.upload().expect("upload the empty SQLite database");
    database.close().expect("close uploaded empty database");

    let database = Connection::open_with_flags(&uri, no_create_flags())
        .expect("reopen uploaded empty database without CREATE");
    let schema_entries: i64 = database
        .query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))
        .expect("reopened database should remain empty");
    assert_eq!(schema_entries, 0);
    let user_version: i64 = database
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read the persisted empty database user_version");
    assert_eq!(user_version, 0);
    database.close().expect("close re-opened empty database");

    let database = Connection::open_with_flags(&uri, no_create_flags())
        .expect("reopen empty database before adding application data");
    database
        .execute_batch(
            "CREATE TABLE mutation(value TEXT NOT NULL); INSERT INTO mutation VALUES ('saved');",
        )
        .expect("write application data to the new SQLite database");
    database.upload().expect("upload the new SQLite database");
    database.close().expect("close uploaded database");

    let reopened = Connection::open_with_flags(&uri, no_create_flags())
        .expect("reopen uploaded SQLite database without CREATE");
    let value: String = reopened
        .query_row("SELECT value FROM mutation", [], |row| row.get(0))
        .expect("read uploaded application data");
    assert_eq!(value, "saved");
    reopened.close().expect("close re-opened database");

    let race_prefix = format!("{suffix}/uri/create-race/");
    let race_uri = uri_for_container(backend, &endpoint, &bucket, &race_prefix);
    let start = Arc::new(Barrier::new(3));
    let (opened_tx, opened_rx) = mpsc::channel();
    let mut release_senders = Vec::new();
    let writers = ["left", "right"].map(|value| {
        let uri = race_uri.clone();
        let start = Arc::clone(&start);
        let opened_tx = opened_tx.clone();
        let (release_tx, release_rx) = mpsc::channel();
        release_senders.push(release_tx);
        std::thread::spawn(move || {
            start.wait();
            let database = match Connection::open(&uri) {
                Ok(database) => database,
                Err(_) => {
                    let _ = opened_tx.send(false);
                    return false;
                }
            };
            let _ = opened_tx.send(true);
            if release_rx.recv_timeout(Duration::from_secs(15)).is_err() {
                return false;
            }
            database
                .execute_batch("CREATE TABLE race(value TEXT NOT NULL);")
                .expect("create race table in each local database");
            database
                .execute("INSERT INTO race VALUES (?1)", [value])
                .expect("write independent local race value");
            let uploaded = database.upload().is_ok();
            if let Err((database, _error)) = database.close() {
                drop(database);
            }
            uploaded
        })
    });
    drop(opened_tx);
    start.wait();
    for _ in 0..writers.len() {
        if opened_rx.recv_timeout(Duration::from_secs(15)).is_err() {
            break;
        }
    }
    for release in release_senders {
        let _ = release.send(());
    }
    let outcomes = writers.map(|writer| writer.join().expect("join concurrent CBS writer"));
    assert_eq!(
        outcomes.into_iter().filter(|uploaded| *uploaded).count(),
        1,
        "conditional manifest update must accept one creator and reject the other"
    );

    let winner = Connection::open_with_flags(&race_uri, no_create_flags())
        .expect("open the winning concurrent database without CREATE");
    let value: String = winner
        .query_row("SELECT value FROM race", [], |row| row.get(0))
        .expect("read the winner's persisted row");
    assert!(value == "left" || value == "right");
    winner.close().expect("close the winning database");
}

fn encode_query_value(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

fn encode_first_byte(value: &str) -> String {
    let Some((first, rest)) = value.as_bytes().split_first() else {
        return String::new();
    };
    format!(
        "%{first:02X}{}",
        encode_query_value(std::str::from_utf8(rest).unwrap())
    )
}

fn ensure_google_bucket(endpoint: &str, bucket: &str) {
    let payload = format!(r#"{{"name":"{bucket}"}}"#);
    let status = Command::new("curl")
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
    assert!(status.status.success(), "GCS bucket request failed");
    let code = String::from_utf8_lossy(&status.stdout);
    assert!(
        code == "200" || code == "201" || code == "409",
        "GCS bucket creation returned {code}"
    );
}

#[cfg(feature = "remote")]
fn ensure_s3_bucket(endpoint: &str, bucket: &str) {
    let url = format!(
        "{}/{}",
        endpoint.trim_end_matches('/'),
        encode_component(bucket)
    );
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--aws-sigv4",
            "aws:amz:us-east-1:s3",
            "--user",
            "test:test",
            "--output",
            "/dev/null",
            "--write-out",
            "%{http_code}",
            "-X",
            "PUT",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(response.status.success(), "S3 bucket request failed: {url}");
    let code = String::from_utf8_lossy(&response.stdout);
    assert!(
        matches!(code.as_ref(), "200" | "201" | "204" | "409"),
        "S3 bucket creation returned {code}: {url}"
    );
}

#[cfg(feature = "remote")]
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

#[cfg(feature = "remote")]
fn fetch_google_object(endpoint: &str, bucket: &str, object: &str) -> Vec<u8> {
    let body = tempfile::NamedTempFile::new().expect("Google object temporary file");
    let url = format!(
        "{}/download/storage/v1/b/{}/o/{}?alt=media",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_component(object)
    );
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--output",
            body.path().to_str().expect("Google object path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(
        response.status.success(),
        "Google object request failed: {url}"
    );
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "Google object request returned {code}: {url}");
    fs::read(body.path()).expect("read Google object body")
}

#[cfg(feature = "remote")]
fn encode_path(value: &str) -> String {
    value
        .split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(feature = "remote")]
fn fetch_s3_object(endpoint: &str, bucket: &str, object: &str) -> Vec<u8> {
    let body = tempfile::NamedTempFile::new().expect("S3 object temporary file");
    let url = format!(
        "{}/{}/{}",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_path(object)
    );
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--aws-sigv4",
            "aws:amz:us-east-1:s3",
            "--user",
            "test:test",
            "--output",
            body.path().to_str().expect("S3 object path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(response.status.success(), "S3 object request failed: {url}");
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "S3 object request returned {code}: {url}");
    fs::read(body.path()).expect("read S3 object body")
}

#[cfg(feature = "remote")]
fn fetch_session_object(backend: &str, endpoint: &str, bucket: &str, object: &str) -> Vec<u8> {
    match backend {
        "google" => fetch_google_object(endpoint, bucket, object),
        "s3" => fetch_s3_object(endpoint, bucket, object),
        _ => unreachable!("unknown session backend {backend}"),
    }
}

#[cfg(feature = "remote")]
fn list_google_objects(endpoint: &str, bucket: &str, prefix: &str) -> String {
    let body = tempfile::NamedTempFile::new().expect("Google object-list temporary file");
    let url = format!(
        "{}/storage/v1/b/{}/o?prefix={}&maxResults=1000",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_component(prefix)
    );
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--output",
            body.path()
                .to_str()
                .expect("Google object-list path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(
        response.status.success(),
        "Google object-list request failed: {url}"
    );
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(
        code, "200",
        "Google object-list request returned {code}: {url}"
    );
    fs::read_to_string(body.path()).expect("read Google object-list response")
}

#[cfg(feature = "remote")]
fn list_s3_objects(endpoint: &str, bucket: &str, prefix: &str) -> String {
    let body = tempfile::NamedTempFile::new().expect("S3 object-list temporary file");
    let url = format!(
        "{}/{}?list-type=2&prefix={}&max-keys=1000",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_component(prefix)
    );
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--aws-sigv4",
            "aws:amz:us-east-1:s3",
            "--user",
            "test:test",
            "--output",
            body.path().to_str().expect("S3 object-list path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(
        response.status.success(),
        "S3 object-list request failed: {url}"
    );
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "S3 object-list request returned {code}: {url}");
    fs::read_to_string(body.path()).expect("read S3 object-list response")
}

#[cfg(feature = "remote")]
fn list_session_objects(backend: &str, endpoint: &str, bucket: &str, prefix: &str) -> String {
    match backend {
        "google" => list_google_objects(endpoint, bucket, prefix),
        "s3" => list_s3_objects(endpoint, bucket, prefix),
        _ => unreachable!("unknown session backend {backend}"),
    }
}

#[cfg(feature = "remote")]
fn first_google_object_name(listing: &str, prefix: &str) -> Option<String> {
    let marker = format!(r#""name":"{prefix}"#);
    let start = listing.find(&marker)? + r#""name":""#.len();
    let tail = &listing[start..];
    Some(tail[..tail.find('"')?].to_owned())
}

#[cfg(feature = "remote")]
fn google_object_names(listing: &str) -> Vec<String> {
    let marker = r#""name":""#;
    let mut names = Vec::new();
    let mut remaining = listing;
    while let Some(marker_start) = remaining.find(marker) {
        let name_start = marker_start + marker.len();
        let name_and_rest = &remaining[name_start..];
        let Some(name_end) = name_and_rest.find('"') else {
            break;
        };
        names.push(name_and_rest[..name_end].to_owned());
        remaining = &name_and_rest[name_end + 1..];
    }
    names
}

#[cfg(feature = "remote")]
fn be_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("checkpoint integer is in bounds"),
    )
}

#[cfg(feature = "remote")]
fn be_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checkpoint integer is in bounds"),
    )
}

#[cfg(feature = "remote")]
fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(feature = "remote")]
fn hex_bytes_upper(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

#[cfg(feature = "remote")]
fn deterministic_payload(id: i64, length: usize) -> String {
    const ALPHABET: &[u8; 64] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";
    let mut state = (id as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut payload = format!("row-{id:08}-");
    while payload.len() < length {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(ALPHABET[(state & 63) as usize] as char);
    }
    payload.truncate(length);
    payload
}

/// Reassemble the database named by an immutable checkpoint manifest for
/// content assertions. Native session rehydration from an accepted checkpoint
/// is exercised separately by the fresh-process phase-two flows; this helper
/// inspects the manifest's block mapping and logical file size directly.
#[cfg(feature = "remote")]
fn reassemble_checkpoint_database(
    backend: &str,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    checkpoint_record: &[u8],
    database: &str,
) -> Vec<u8> {
    const RECORD_HEADER_BYTES: usize = 168;
    const RECORD_TRAILER_BYTES: usize = 32;
    const CHECKPOINT_BODY_HEADER_BYTES: usize = 24;
    const MANIFEST_HEADER_BYTES: usize = 24;
    const MANIFEST_DATABASE_HEADER_BYTES: usize = 152;

    assert!(
        checkpoint_record.len() >= RECORD_HEADER_BYTES + RECORD_TRAILER_BYTES,
        "checkpoint record is truncated"
    );
    let body_length = be_u32(checkpoint_record, 160) as usize;
    assert_eq!(
        checkpoint_record.len(),
        RECORD_HEADER_BYTES + body_length + RECORD_TRAILER_BYTES,
        "checkpoint record length is inconsistent"
    );
    let body = &checkpoint_record[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + body_length];
    assert!(
        body.starts_with(b"BCVCP01\0"),
        "checkpoint body has an unexpected format"
    );
    assert!(body.len() >= CHECKPOINT_BODY_HEADER_BYTES);
    let file_size = be_u64(body, 8) as usize;
    let manifest_length = be_u32(body, 16) as usize;
    let etag_length = be_u32(body, 20) as usize;
    assert_eq!(
        body.len(),
        CHECKPOINT_BODY_HEADER_BYTES + etag_length + manifest_length,
        "checkpoint body length is inconsistent"
    );
    let manifest_start = CHECKPOINT_BODY_HEADER_BYTES + etag_length;
    let manifest = &body[manifest_start..manifest_start + manifest_length];
    assert!(manifest.len() >= MANIFEST_HEADER_BYTES);
    assert_eq!(
        be_u32(manifest, 0),
        4,
        "unexpected checkpoint manifest version"
    );
    let block_size = be_u32(manifest, 4) as usize;
    let database_count = be_u32(manifest, 8) as usize;
    let name_size = be_u32(manifest, 16) as usize;
    assert!(block_size > 0);
    assert!((12..=32).contains(&name_size));

    for database_index in 0..database_count {
        let header = MANIFEST_HEADER_BYTES + database_index * MANIFEST_DATABASE_HEADER_BYTES;
        assert!(
            header + MANIFEST_DATABASE_HEADER_BYTES <= manifest.len(),
            "checkpoint database header is truncated"
        );
        let parent_id = be_u32(manifest, header + 4);
        let block_offset = be_u32(manifest, header + 12) as usize;
        let block_count = (be_u32(manifest, header + 16) & 0x7fff_ffff) as usize;
        let entry_count = be_u32(manifest, header + 20) as usize;
        let name_end = header + 24 + name_size.min(128);
        let name = manifest[header + 24..name_end]
            .split(|byte| *byte == 0)
            .next()
            .expect("checkpoint database name")
            .to_vec();
        if name != database.as_bytes() {
            continue;
        }
        assert_eq!(
            parent_id, 0,
            "the session test fixture should use a complete database block list"
        );
        assert_eq!(
            entry_count, 0,
            "a complete database block list must not contain delta entries"
        );
        assert!(
            block_offset
                .checked_add(block_count.checked_mul(name_size).expect("block list size"))
                .is_some_and(|end| end <= manifest.len()),
            "checkpoint database block list is truncated"
        );
        let mut database_bytes = Vec::with_capacity(block_count * block_size);
        for block_index in 0..block_count {
            let start = block_offset + block_index * name_size;
            // CBS block object names are emitted by bcvBlockidToText(), which
            // uses uppercase hexadecimal.  DoltLite chunk routes use their
            // own lowercase hash spelling, so keep that separate in
            // hex_bytes() rather than normalizing all test paths together.
            let block_id = hex_bytes_upper(&manifest[start..start + name_size]);
            let block = fetch_session_object(
                backend,
                endpoint,
                bucket,
                &format!("{prefix}/{block_id}.bcv"),
            );
            assert_eq!(
                block.len(),
                block_size,
                "checkpoint block has an unexpected size"
            );
            database_bytes.extend_from_slice(&block);
        }
        assert!(
            file_size <= database_bytes.len(),
            "checkpoint logical file size exceeds its block mapping"
        );
        database_bytes.truncate(file_size);
        return database_bytes;
    }
    panic!("checkpoint manifest does not contain database {database}");
}

fn shared_cache() -> &'static Path {
    static CACHE: OnceLock<tempfile::TempDir> = OnceLock::new();
    CACHE
        .get_or_init(|| tempfile::tempdir().expect("shared cache directory"))
        .path()
}

#[cfg(feature = "remote")]
fn session_scope(database: &str) -> SessionScope {
    SessionScope::new("emulator-principal", database, "read,write")
        .expect("valid emulator session scope")
}

fn session_operation_id(value: u8) -> SessionOperationId {
    let mut operation_id = [0_u8; SESSION_OPERATION_ID_BYTES];
    operation_id[0] = value;
    SessionOperationId::new(operation_id).expect("valid emulator operation ID")
}

fn session_storage(backend: &str, endpoint: &str, bucket: &str, prefix: &str) -> Storage {
    let container = format!("{bucket}/{prefix}");
    match backend {
        "google" => Storage::google_json_with_endpoint("test-project", container, endpoint),
        "s3" => Storage::s3_with_endpoint("test", container, "us-east-1", endpoint),
        _ => unreachable!("unknown session backend {backend}"),
    }
}

fn attach_scoped_for_test(
    vfs: &'static BlockCacheVfs,
    spec: &AttachSpec,
    session_id: &str,
    operation: u8,
) -> rusqlite::Result<SessionAttachment> {
    let operation_id = session_operation_id(operation);
    vfs.attach_session_scoped(
        spec,
        session_id,
        "emulator-principal",
        "session.sqlite",
        "read,write",
        &operation_id,
    )
}

#[cfg(feature = "remote")]
const HTTP_TEST_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(feature = "remote")]
const SESSION_HTTP_TEST_TIMEOUT: Duration = Duration::from_secs(120);

#[cfg(feature = "remote")]
fn send_http_request(port: u16, request: &[u8]) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to remote server");
    stream
        .set_read_timeout(Some(HTTP_TEST_TIMEOUT))
        .expect("read timeout");
    stream.write_all(request).expect("write request");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    String::from_utf8_lossy(&response).into_owned()
}

#[cfg(feature = "remote")]
struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

#[cfg(feature = "remote")]
fn send_http_request_bytes(port: u16, method: &str, path: &str, body: &[u8]) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to remote server");
    stream
        .set_read_timeout(Some(HTTP_TEST_TIMEOUT))
        .expect("read timeout");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .expect("write request headers");
    stream.write_all(body).expect("write request body");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP response headers");
    let status = response[..header_end]
        .split(|byte| *byte == b' ')
        .nth(1)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|text| text.parse().ok())
        .expect("HTTP response status");
    HttpResponse {
        status,
        body: response[header_end + 4..].to_vec(),
    }
}

#[cfg(feature = "remote")]
fn send_http_request_raw(port: u16, request: &[u8]) -> Vec<u8> {
    send_http_request_raw_with_timeout(port, request, HTTP_TEST_TIMEOUT)
}

#[cfg(feature = "remote")]
fn send_http_request_raw_with_timeout(port: u16, request: &[u8], timeout: Duration) -> Vec<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to remote server");
    stream
        .set_read_timeout(Some(timeout))
        .expect("read timeout");
    stream.write_all(request).expect("write request");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    response
}

#[cfg(feature = "remote")]
fn http_status(response: &[u8]) -> u16 {
    response
        .split(|byte| *byte == b' ')
        .nth(1)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|text| text.parse().ok())
        .expect("HTTP response status")
}

#[cfg(feature = "remote")]
fn read_http_request(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        if stream.read_exact(&mut byte).is_err() {
            return None;
        }
        request.push(byte[0]);
        if request.ends_with(b"\r\n\r\n") {
            break;
        }
        assert!(
            request.len() <= 128 * 1024,
            "HTTP request headers are bounded"
        );
    }
    let header_end = request.len();
    let header_text = String::from_utf8_lossy(&request[..header_end]);
    let content_length = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
        })
        .flatten()
        .unwrap_or(0);
    if content_length > 16 * 1024 * 1024 {
        return None;
    }
    let mut body = vec![0_u8; content_length];
    if stream.read_exact(&mut body).is_err() {
        return None;
    }
    request.extend_from_slice(&body);
    Some(request)
}

#[cfg(feature = "remote")]
fn request_parts(request: &[u8]) -> (String, String, Vec<u8>) {
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP request headers");
    let header = String::from_utf8_lossy(&request[..header_end]);
    let mut request_line = header
        .lines()
        .next()
        .expect("HTTP request line")
        .split_whitespace();
    let method = request_line.next().expect("HTTP request method").to_owned();
    let path = request_line.next().expect("HTTP request path").to_owned();
    (method, path, request[header_end + 4..].to_vec())
}

#[cfg(feature = "remote")]
fn request_route(path: &str) -> &str {
    path.trim_start_matches('/')
        .split_once('/')
        .and_then(|(_, endpoint)| endpoint.split('/').next())
        .and_then(|endpoint| endpoint.split('?').next())
        .unwrap_or("")
}

#[cfg(feature = "remote")]
fn session_request_path(path: &str) -> Option<(&str, String)> {
    let suffix = path.strip_prefix("/capability/")?;
    let (session_id, native_path) = suffix.split_once('/')?;
    if session_id.is_empty() || !native_path.contains('/') {
        return None;
    }
    Some((session_id, format!("/{native_path}")))
}

#[cfg(feature = "remote")]
fn request_with_path(request: &[u8], path: &str) -> Option<Vec<u8>> {
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")?;
    let header = String::from_utf8_lossy(&request[..header_end]);
    let request_line_end = header.find("\r\n")?;
    let mut parts = header[..request_line_end].split_ascii_whitespace();
    let method = parts.next()?;
    let _original_path = parts.next()?;
    let version = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let mut rewritten = Vec::with_capacity(request.len());
    rewritten.extend_from_slice(format!("{method} {path} {version}\r\n").as_bytes());
    rewritten.extend_from_slice(header[request_line_end + 2..].as_bytes());
    rewritten.extend_from_slice(b"\r\n\r\n");
    rewritten.extend_from_slice(&request[header_end + 4..]);
    Some(rewritten)
}

#[cfg(feature = "remote")]
fn chunk_batch_stats(body: &[u8]) -> String {
    let mut offset = 0;
    let mut count = 0_usize;
    let mut largest = 0_usize;
    while offset < body.len() {
        if body.len() - offset < 24 {
            return format!("malformed_at={offset} chunks={count}");
        }
        let size = u32::from_le_bytes(
            body[offset + 20..offset + 24]
                .try_into()
                .expect("chunk length field"),
        ) as usize;
        offset += 24;
        let Some(end) = offset.checked_add(size) else {
            return format!("length_overflow_at={offset} chunks={count}");
        };
        if end > body.len() {
            return format!("truncated_at={offset} size={size} chunks={count}");
        }
        largest = largest.max(size);
        count += 1;
        offset = end;
    }
    format!("chunks={count} largest_chunk_bytes={largest}")
}

#[cfg(feature = "remote")]
fn scoped_session_request_path(path: &str, session_token: &str, database: &str) -> Option<String> {
    let (path_only, query) = path
        .split_once('?')
        .map_or((path, None), |(path, query)| (path, Some(query)));
    let prefix = format!("/s/{session_token}/{database}");
    let suffix = path_only.strip_prefix(&prefix)?;
    if !suffix.is_empty() && !suffix.starts_with('/') {
        return None;
    }
    let mut native_path = format!("/{database}{suffix}");
    if let Some(query) = query {
        native_path.push('?');
        native_path.push_str(query);
    }
    Some(native_path)
}

#[cfg(feature = "remote")]
fn rewrite_http_request_path(request: &[u8], path: &str) -> Vec<u8> {
    let line_end = request
        .windows(2)
        .position(|window| window == b"\r\n")
        .expect("HTTP request line terminator");
    let line = &request[..line_end];
    let first_space = line.iter().position(|byte| *byte == b' ');
    let first_space = first_space.expect("HTTP request method separator");
    let path_end = line[first_space + 1..]
        .iter()
        .position(|byte| *byte == b' ')
        .map(|offset| first_space + 1 + offset)
        .expect("HTTP request path separator");
    let mut rewritten = Vec::with_capacity(request.len() + path.len());
    rewritten.extend_from_slice(&request[..first_space + 1]);
    rewritten.extend_from_slice(path.as_bytes());
    rewritten.extend_from_slice(&request[path_end..]);
    rewritten
}

#[cfg(feature = "remote")]
#[test]
fn session_proxy_requires_and_strips_opaque_token() {
    assert_eq!(
        scoped_session_request_path(
            "/s/opaque-token/session.sqlite/commit?retry=1",
            "opaque-token",
            "session.sqlite",
        ),
        Some("/session.sqlite/commit?retry=1".to_owned())
    );
    assert_eq!(
        scoped_session_request_path("/session.sqlite/commit", "opaque-token", "session.sqlite",),
        None,
        "a URL without the session token must not attach"
    );
    assert_eq!(
        scoped_session_request_path(
            "/s/other-token/session.sqlite/commit",
            "opaque-token",
            "session.sqlite",
        ),
        None,
        "a URL with a foreign session token must not attach"
    );
    assert_eq!(
        scoped_session_request_path(
            "/s/opaque-token/other.sqlite/commit",
            "opaque-token",
            "session.sqlite",
        ),
        None,
        "a URL for another database must not attach"
    );
    let rewritten = rewrite_http_request_path(
        b"POST /s/opaque-token/session.sqlite/commit HTTP/1.1\r\nHost: test\r\n\r\nbody",
        "/session.sqlite/commit",
    );
    assert!(rewritten.starts_with(b"POST /session.sqlite/commit HTTP/1.1\r\n"));
    assert!(rewritten.ends_with(b"\r\n\r\nbody"));
}

#[cfg(feature = "remote")]
struct ReferenceResponseAdapter<'a> {
    server: &'a mut RemoteServer,
    response: Option<Vec<u8>>,
}

#[cfg(feature = "remote")]
fn should_publish_reference_response(route: &str, status: u16) -> bool {
    route == "/commit" && (200..300).contains(&status)
}

#[cfg(feature = "remote")]
fn should_invoke_mutating_handler(status: SessionOperationStatus) -> bool {
    matches!(status, SessionOperationStatus::New)
}

#[cfg(feature = "remote")]
impl<'a> ReferenceResponseAdapter<'a> {
    fn new(server: &'a mut RemoteServer) -> Self {
        Self {
            server,
            response: None,
        }
    }

    fn buffer(&mut self, response: impl Into<Vec<u8>>) {
        assert!(
            self.response.replace(response.into()).is_none(),
            "reference adapter accepts one complete response"
        );
    }

    fn release_after_completion_staged(
        mut self,
        route: &str,
        status: u16,
    ) -> Result<Vec<u8>, (&'static str, rusqlite::Error)> {
        let response = self.response.take().ok_or_else(|| {
            (
                "buffer",
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                    Some("reference adapter has no buffered response".to_owned()),
                ),
            )
        })?;
        if !(200..300).contains(&status) {
            // A failed application handler must not create a checkpoint or
            // advance the accepted operation head. Stop the native workers
            // and let session attachment recovery discard the unpublished
            // overlay on the next request.
            self.server.quiesce().map_err(|error| ("quiesce", error))?;
            return Ok(response);
        }
        if should_publish_reference_response(route, status) {
            // `/commit` is an application close-out decision. Upload owns
            // quiescing, the final checkpoint, and publication; the adapter
            // must not publish an intermediate complete_request first.
            self.server.upload().map_err(|error| ("upload", error))?;
        } else {
            // Other successful responses only make their accepted checkpoint
            // available to the next invocation; they never publish HEAD.
            self.server
                .complete_request()
                .map_err(|error| ("complete_request", error))?;
        }
        Ok(response)
    }

    fn release_after_completion(self, route: &str, status: u16) -> rusqlite::Result<Vec<u8>> {
        self.release_after_completion_staged(route, status)
            .map_err(|(_, error)| error)
    }
}

#[cfg(feature = "remote")]
#[test]
fn reference_adapter_route_selection_is_application_only() {
    assert!(should_publish_reference_response("/commit", 200));
    assert!(should_publish_reference_response("/commit", 204));
    assert!(!should_publish_reference_response("/commit", 500));
    assert!(!should_publish_reference_response("/chunks", 200));
    assert!(!should_publish_reference_response("/refs-if", 204));
    assert!(!should_invoke_mutating_handler(
        SessionOperationStatus::Accepted
    ));
    assert!(!should_invoke_mutating_handler(
        SessionOperationStatus::Committed
    ));
    assert!(should_invoke_mutating_handler(SessionOperationStatus::New));
    assert!(!should_invoke_mutating_handler(
        SessionOperationStatus::Failed
    ));
    assert!(!should_invoke_mutating_handler(
        SessionOperationStatus::Conflict
    ));
    assert_eq!(
        proxy_operation_id(1, "POST", "/session.sqlite/commit", b"commit"),
        proxy_operation_id(2, "POST", "/session.sqlite/commit", b"commit"),
        "mutating retries must retain one operation fence"
    );
    assert_ne!(
        proxy_operation_id(1, "GET", "/session.sqlite/root", &[]),
        proxy_operation_id(2, "GET", "/session.sqlite/root", &[]),
        "independent read requests need distinct application operation IDs"
    );
}

// Run the full create, attach, write, upload, and replacement lifecycle against
// the selected emulator. Return its endpoint and bucket so the same CI entry
// point can exercise the backend-independent session checkpoint regression.
fn run_bootstrap(backend: &str) -> (String, String) {
    let suffix = unique_suffix();
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4443".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!(),
    };
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rust-bootstrap-{suffix}")
    };
    let prefix = format!("{suffix}/cbs");
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    }
    let container = format!("{bucket}/{prefix}");
    let storage = if backend == "google" {
        Storage::google_json_with_endpoint("test-project", &container, &endpoint)
    } else {
        Storage::s3_with_endpoint("test", &container, "us-east-1", &endpoint)
    };

    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("VFS builder")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(move |provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize block-cache VFS");

    vfs.initialize_container(&storage)
        .expect("initialize remote CBS container");
    vfs.cleanup(&storage, std::time::Duration::from_secs(300))
        .expect("scheduled cleanup should use the independent CBS management API");
    let local_dir = tempfile::tempdir().expect("local database directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local SQLite database");
    local
        .execute_batch("CREATE TABLE bootstrap(value TEXT); INSERT INTO bootstrap VALUES ('ok');")
        .expect("seed local SQLite database");
    local.close().expect("close local SQLite database");
    let local_header = std::fs::read(&local_path).expect("read local SQLite database");
    assert!(
        local_header.len() >= 20,
        "local database header is truncated"
    );
    assert_eq!(
        &local_header[18..20],
        &[0, 0],
        "the DoltLite seed should retain its sealed chunk-store header"
    );
    vfs.create_database(&storage, &local_path, "bootstrap.sqlite")
        .expect("upload initial database");
    assert_reserved_remote_database_names_rejected(&vfs, &storage, &local_path);
    vfs.initialize_container(&storage)
        .expect_err("existing CBS manifest must not be replaced");
    let alias = format!("bootstrap_{backend}_{}", std::process::id());
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&alias))
        .expect("attach created database");
    assert_reserved_bcv_copy_targets_rejected(&vfs, &alias, "bootstrap.sqlite");
    let control = vfs
        .open(format!("/{alias}"))
        .expect("open attached container control connection");
    let nblock: i64 = control
        .query_row(
            "SELECT nblock FROM bcv_database WHERE container = ?1 AND database = ?2",
            params![alias, "bootstrap.sqlite"],
            |row| row.get(0),
        )
        .expect("inspect attached database manifest");
    assert_eq!(nblock, 1, "attached manifest must expose one local block");
    drop(control);
    let path = format!("/{alias}/bootstrap.sqlite");
    let db = vfs.open(&path).expect("open created database");
    let value: String = db
        .query_row("SELECT value FROM bootstrap", [], |row| row.get(0))
        .expect("read created database");
    assert_eq!(value, "ok");
    assert_vfs_header_passthrough(&db, &local_header[..20]);
    db.execute("INSERT INTO bootstrap VALUES ('updated')", [])
        .expect("write created database");
    drop(db);
    vfs.upload(&alias).expect("upload modified database");
    vfs.detach(&alias).expect("detach created database");

    let second_alias = format!("{alias}_again");
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&second_alias))
        .expect("re-attach created database");
    let path = format!("/{second_alias}/bootstrap.sqlite");
    let db = vfs.open(&path).expect("re-open created database");
    assert_vfs_header_passthrough(&db, &local_header[..20]);
    let count: i64 = db
        .query_row("SELECT count(*) FROM bootstrap", [], |row| row.get(0))
        .expect("read uploaded database");
    assert_eq!(count, 2);
    drop(db);

    vfs.delete_database(&second_alias, "bootstrap.sqlite")
        .expect("delete database locally");
    vfs.upload(&second_alias).expect("upload database deletion");
    vfs.detach(&second_alias)
        .expect("detach after database deletion");

    vfs.create_database(&storage, &local_path, "bootstrap.sqlite")
        .expect("re-create deleted database");
    let third_alias = format!("{alias}_replacement");
    vfs.attach(&AttachSpec::new(storage).alias(&third_alias))
        .expect("attach replacement database");
    let path = format!("/{third_alias}/bootstrap.sqlite");
    let db = vfs.open(&path).expect("open replacement database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM bootstrap", [], |row| row.get(0))
        .expect("read replacement database");
    assert_eq!(count, 1, "replacement database must not retain old rows");
    let value: String = db
        .query_row("SELECT value FROM bootstrap", [], |row| row.get(0))
        .expect("read replacement database value");
    assert_eq!(value, "ok");
    drop(db);
    vfs.detach(&third_alias)
        .expect("detach replacement database");
    (endpoint, bucket)
}

fn assert_vfs_header_passthrough(database: &Connection, expected: &[u8]) {
    assert_eq!(expected.len(), 20);
    assert_eq!(read_vfs_header(database).as_slice(), expected);
}

fn read_vfs_header(database: &Connection) -> [u8; 20] {
    let database_name = CString::new("main").expect("database name is NUL-free");
    let mut file: *mut ffi::sqlite3_file = ptr::null_mut();
    let rc = unsafe {
        ffi::sqlite3_file_control(
            database.handle(),
            database_name.as_ptr(),
            ffi::SQLITE_FCNTL_FILE_POINTER,
            (&mut file as *mut *mut ffi::sqlite3_file).cast(),
        )
    };
    assert_eq!(rc, ffi::SQLITE_OK, "obtain the CBS main-file handle");
    assert!(!file.is_null(), "CBS main-file handle is null");

    let methods = unsafe { &*(*file).pMethods };
    let x_read = methods.xRead.expect("CBS VFS implements xRead");
    let mut actual = [0u8; 20];
    let rc = unsafe { x_read(file, actual.as_mut_ptr().cast(), actual.len() as i32, 0) };
    assert_eq!(rc, ffi::SQLITE_OK, "read the CBS database header directly");
    actual
}

// A user can pass these names to create_database or directly to the scoped
// attach API. Exact SQLite sidecar suffixes can collide with SQLite's
// interpretation of `<database>-wal`, `-shm`, or `-journal`; `.repo.db-lock`
// is especially dangerous because it is DoltLite's local-only lock path for
// `repo.db`. Publishing that remote name can shadow the coordination file and
// block or corrupt later writes. Exercise both API boundaries against
// run_bootstrap's initialized unique container and valid local seed.
fn assert_reserved_remote_database_names_rejected(
    vfs: &'static BlockCacheVfs,
    storage: &Storage,
    local_path: &Path,
) {
    for (index, remote_name) in [
        "repo.db-wal",
        "repo.db-shm",
        "repo.db-journal",
        ".repo.db-lock",
    ]
    .into_iter()
    .enumerate()
    {
        let create_error = vfs
            .create_database(storage, local_path, remote_name)
            .expect_err("create_database must reject a reserved sidecar or local lock name");
        assert!(
            matches!(
                &create_error,
                rusqlite::Error::SqliteFailure(code, _)
                    if code.extended_code == rusqlite::ffi::SQLITE_ERROR
            ),
            "expected native database-name validation for {remote_name:?}, got {create_error:?}"
        );

        // Direct callers of attach_session_scoped must get the same boundary
        // as callers that first construct SessionScope.
        let session_id = Uuid::new_v4().to_string();
        let operation_id = session_operation_id(index as u8 + 1);
        let attach_error = vfs
            .attach_session_scoped(
                &AttachSpec::new(storage.clone()),
                &session_id,
                "emulator-principal",
                remote_name,
                "read,write",
                &operation_id,
            )
            .expect_err("attach_session_scoped must reject a reserved target database");
        assert!(
            matches!(
                &attach_error,
                rusqlite::Error::SqliteFailure(code, _)
                    if code.extended_code == rusqlite::ffi::SQLITE_MISUSE
            ),
            "expected native scope validation for {remote_name:?}, got {attach_error:?}"
        );
    }

    // Similar-looking but distinct names must remain uploadable. In
    // particular, only the exact hidden `.<stem>-lock` shape is local-only;
    // the non-hidden `repo.db-lock`, a nonterminal `.repo.db-lock-old`, and
    // an uppercase SQLite suffix are valid remote database names.
    for remote_name in ["repo.db-lock", ".repo.db-lock-old", "repo.db-WAL"] {
        vfs.create_database(storage, local_path, remote_name)
            .unwrap_or_else(|error| panic!("upload near-miss {remote_name:?}: {error:?}"));
    }
}

// A caller can bypass create_database and ask native sqlite3_bcvfs_copy to add
// a manifest entry named like another database's SQLite sidecar or DoltLite's
// local `.<database>-lock` file. Copy from the attached valid source and check
// the reserved-name error so missing-source/provider failures cannot pass.
fn assert_reserved_bcv_copy_targets_rejected(
    vfs: &'static BlockCacheVfs,
    alias: &str,
    source_database: &str,
) {
    let vfs_name = CString::new(vfs.name()).expect("VFS name is NUL-free");
    let native_vfs = unsafe { ffi::sqlite3_vfs_find(vfs_name.as_ptr()) };
    assert!(
        !native_vfs.is_null(),
        "registered VFS must be discoverable by name"
    );
    let alias = CString::new(alias).expect("alias is NUL-free");
    let source_database = CString::new(source_database).expect("source name is NUL-free");

    // The first names can shadow SQLite/DoltLite sidecars. The controls
    // resemble them but do not have the exact reserved suffix/path shape.
    for (destination_database, expected_rc) in [
        ("repo.db-wal", ffi::SQLITE_ERROR),
        ("repo.db-shm", ffi::SQLITE_ERROR),
        ("repo.db-journal", ffi::SQLITE_ERROR),
        (".repo.db-lock", ffi::SQLITE_ERROR),
        ("copy.repo.db-lock", ffi::SQLITE_OK),
        (".copy.repo.db-lock-old", ffi::SQLITE_OK),
        ("copy.repo.db-WAL", ffi::SQLITE_OK),
    ] {
        let destination_database =
            CString::new(destination_database).expect("destination name is NUL-free");
        let mut error = ptr::null_mut();
        let rc = unsafe {
            raw_bcv::sqlite3_bcvfs_copy(
                native_vfs.cast(),
                alias.as_ptr(),
                source_database.as_ptr(),
                destination_database.as_ptr(),
                &mut error,
            )
        };
        let message = if error.is_null() {
            None
        } else {
            let message = unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned();
            unsafe { ffi::sqlite3_free(error.cast()) };
            Some(message)
        };
        assert_eq!(
            rc, expected_rc,
            "native copy returned an unexpected result for {destination_database:?}: {message:?}"
        );
        if expected_rc != ffi::SQLITE_OK {
            assert!(
                message.as_deref().is_some_and(|message| {
                    message.contains("reserved SQLite sidecar or local lock name")
                }),
                "native copy should report reserved-name validation, got {message:?}"
            );
        }
    }
}

fn run_session_ownership() {
    let suffix = unique_suffix();
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    let prefix = format!("{suffix}/session");
    ensure_google_bucket(&endpoint, bucket);
    let container = format!("{bucket}/{prefix}");
    let storage = Storage::google_json_with_endpoint("test-project", &container, &endpoint);

    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("VFS builder")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize block-cache VFS");

    vfs.initialize_container(&storage)
        .expect("initialize remote CBS container");
    let local_dir = tempfile::tempdir().expect("local database directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local SQLite database");
    local
        .execute_batch(
            "CREATE TABLE session_data(value TEXT); INSERT INTO session_data VALUES ('ok');",
        )
        .expect("seed local SQLite database");
    local.close().expect("close local SQLite database");
    vfs.create_database(&storage, &local_path, "session.sqlite")
        .expect("upload initial database");
    #[cfg(feature = "remote")]
    let baseline_manifest =
        fetch_google_object(&endpoint, bucket, &format!("{prefix}/manifest.bcv"));

    let spec = AttachSpec::new(storage);
    let first = attach_scoped_for_test(vfs, &spec, "550e8400-e29b-41d4-a716-446655440010", 10)
        .expect("attach first session owner");
    let alias = first.alias().to_owned();
    assert_eq!(alias, "session-550e8400-e29b-41d4-a716-446655440010");

    let active_client_spec = spec.clone().alias("active-client-session-alias");
    let active_client_owner = attach_scoped_for_test(
        vfs,
        &active_client_spec,
        "550e8400-e29b-41d4-a716-446655440014",
        20,
    )
    .expect("attach active-client session owner");
    let active_client_db = vfs
        .open("/active-client-session-alias/session.sqlite")
        .expect("open active-client session database");
    // The owner release loses the detach race while the SQLite client is
    // still open. A second constructor must not reuse or clear that alias.
    drop(active_client_owner);
    let active_client_reuse = attach_scoped_for_test(
        vfs,
        &active_client_spec,
        "550e8400-e29b-41d4-a716-446655440014",
        21,
    )
    .expect_err("an active native client must block same-session reuse");
    assert!(matches!(
        active_client_reuse,
        rusqlite::Error::SqliteFailure(error, _)
            if error.extended_code == rusqlite::ffi::SQLITE_BUSY
    ));
    drop(active_client_db);
    let active_client_recovered = attach_scoped_for_test(
        vfs,
        &active_client_spec,
        "550e8400-e29b-41d4-a716-446655440014",
        22,
    )
    .expect("idle session alias can be reused after its client closes");
    drop(active_client_recovered);

    let explicit_spec = spec.clone().alias("shared-session-alias");
    let explicit_owner = attach_scoped_for_test(
        vfs,
        &explicit_spec,
        "550e8400-e29b-41d4-a716-446655440010",
        11,
    )
    .expect("attach explicit session alias");
    let explicit_db = vfs
        .open("/shared-session-alias/session.sqlite")
        .expect("open explicit session database");
    explicit_db
        .execute_batch(
            "PRAGMA synchronous=OFF; INSERT INTO session_data VALUES ('explicit-dirty');",
        )
        .expect("write explicit session database");
    // This real DoltLite commit exercises the VFS's internal local lock
    // sidecar open. Reserving remote `.<database>-lock` names must preserve it.
    let _: String = explicit_db
        .query_row(
            "SELECT dolt_commit('-A', '-m', 'explicit-dirty')",
            [],
            |row| row.get(0),
        )
        .expect("commit explicit session database update with DoltLite");
    let explicit_dirty_rows: i64 = explicit_db
        .query_row(
            "SELECT COUNT(*) FROM session_data WHERE value = 'explicit-dirty'",
            [],
            |row| row.get(0),
        )
        .expect("query explicit session database update");
    assert_eq!(explicit_dirty_rows, 1);
    drop(explicit_db);
    #[cfg(feature = "remote")]
    assert_eq!(
        baseline_manifest,
        fetch_google_object(&endpoint, bucket, &format!("{prefix}/manifest.bcv")),
        "the DoltLite commit must leave the ordinary manifest unpublished"
    );
    // Preserve alias metadata with a zero active owner. The DoltLite commit
    // leaves the local overlay unaccepted, so the following attempts exercise
    // persisted identity mismatch, not merely live-owner exclusion.
    drop(explicit_owner);
    let wrong_session = attach_scoped_for_test(
        vfs,
        &explicit_spec,
        "550e8400-e29b-41d4-a716-446655440011",
        12,
    )
    .expect_err("a different session must not reuse an explicit alias");
    assert!(matches!(
        wrong_session,
        rusqlite::Error::SqliteFailure(error, _)
            if error.extended_code == rusqlite::ffi::SQLITE_BUSY
    ));
    let wrong_storage = Storage::google_json_with_endpoint(
        "test-project",
        format!("{bucket}/{prefix}-different"),
        &endpoint,
    );
    let wrong_scope = attach_scoped_for_test(
        vfs,
        &AttachSpec::new(wrong_storage).alias("shared-session-alias"),
        "550e8400-e29b-41d4-a716-446655440010",
        13,
    )
    .expect_err("a different storage scope must not reuse an explicit alias");
    assert!(matches!(
        wrong_scope,
        rusqlite::Error::SqliteFailure(error, _)
            if error.extended_code == rusqlite::ffi::SQLITE_BUSY
    ));
    let rotated_account = Storage::google_json_with_endpoint(
        "rotated-project",
        format!("{bucket}/{prefix}"),
        &endpoint,
    );
    let rotated_scope = attach_scoped_for_test(
        vfs,
        &AttachSpec::new(rotated_account).alias("shared-session-alias"),
        "550e8400-e29b-41d4-a716-446655440010",
        14,
    )
    .expect_err("a changed provider account must not reuse a persisted attachment identity");
    assert!(matches!(
        rotated_scope,
        rusqlite::Error::SqliteFailure(error, _)
            if error.extended_code == rusqlite::ffi::SQLITE_BUSY
    ));
    // Rehydration replaces the detached cache's unaccepted generations with
    // the accepted remote base. The explicit-dirty row must not survive this
    // same-session recovery.
    let explicit_recovery = attach_scoped_for_test(
        vfs,
        &explicit_spec,
        "550e8400-e29b-41d4-a716-446655440010",
        15,
    )
    .expect("same-session reuse rehydrates the detached alias");
    let recovered_db = vfs
        .open("/shared-session-alias/session.sqlite")
        .expect("open rehydrated session database");
    let dirty_rows: i64 = recovered_db
        .query_row(
            "SELECT COUNT(*) FROM session_data WHERE value = 'explicit-dirty'",
            [],
            |row| row.get(0),
        )
        .expect("query rehydrated session database");
    assert_eq!(dirty_rows, 0, "unaccepted local writes must be discarded");
    drop(recovered_db);
    drop(explicit_recovery);
    #[cfg(feature = "remote")]
    {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve server port");
        let port = listener.local_addr().expect("reserved port address").port();
        let failed_session = BlockCacheSessionOptions::new(
            vfs,
            spec.clone(),
            "550e8400-e29b-41d4-a716-446655440011",
            session_scope("session.sqlite"),
            session_operation_id(1),
        )
        .expect("construct failed-start session payload");
        let failed_start = RemoteServer::start_with_options(
            "/session-550e8400-e29b-41d4-a716-446655440011",
            &RemoteServerOptions::new()
                .bind_address("127.0.0.1")
                .port(port)
                .blockcache_session(failed_session),
        );
        assert!(failed_start.is_err(), "reserved port must fail startup");
        let cleaned_attachment =
            attach_scoped_for_test(vfs, &spec, "550e8400-e29b-41d4-a716-446655440011", 16)
                .expect("failed startup must release its attachment");
        drop(cleaned_attachment);
        drop(listener);
    }

    let live_conflict =
        attach_scoped_for_test(vfs, &spec, "550e8400-e29b-41d4-a716-446655440010", 17)
            .expect_err("a second live owner must be rejected");
    assert!(matches!(
        live_conflict,
        rusqlite::Error::SqliteFailure(error, _)
            if error.extended_code == rusqlite::ffi::SQLITE_BUSY
    ));

    let db = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("open session database");
    db.execute_batch("PRAGMA synchronous=OFF; INSERT INTO session_data VALUES ('dirty');")
        .expect("write session database");
    drop(db);
    // Drop cannot detach a dirty container. The native session release leaves
    // a zero-owner metadata record; the next same-session attach rehydrates
    // the accepted remote base and discards the unpublished overlay.
    drop(first);

    let recovered = attach_scoped_for_test(vfs, &spec, "550e8400-e29b-41d4-a716-446655440010", 18)
        .expect("same-session recovery rehydrates the detached alias");
    let recovered_db = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("open rehydrated default session database");
    let dirty_rows: i64 = recovered_db
        .query_row(
            "SELECT COUNT(*) FROM session_data WHERE value = 'dirty'",
            [],
            |row| row.get(0),
        )
        .expect("query rehydrated default session database");
    assert_eq!(dirty_rows, 0, "unaccepted local writes must be discarded");
    drop(recovered_db);
    drop(recovered);

    let independent =
        attach_scoped_for_test(vfs, &spec, "550e8400-e29b-41d4-a716-446655440012", 19)
            .expect("different session gets an isolated alias");
    assert_ne!(alias, independent.alias());
    drop(independent);
}

#[cfg(feature = "remote")]
fn run_session_server() {
    let suffix = unique_suffix();
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    let prefix = format!("{suffix}/server");
    ensure_google_bucket(&endpoint, bucket);
    let container = format!("{bucket}/{prefix}");
    let storage = Storage::google_json_with_endpoint("test-project", &container, &endpoint);

    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("VFS builder")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize block-cache VFS");
    vfs.initialize_container(&storage)
        .expect("initialize remote CBS container");
    let local_dir = tempfile::tempdir().expect("local database directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local SQLite database");
    local
        .execute_batch(
            "CREATE TABLE session_server(value TEXT); INSERT INTO session_server VALUES ('ok');",
        )
        .expect("seed local SQLite database");
    local.close().expect("close local SQLite database");
    vfs.create_database(&storage, &local_path, "session.sqlite")
        .expect("upload initial database");
    let baseline_manifest =
        fetch_google_object(&endpoint, bucket, &format!("{prefix}/manifest.bcv"));

    let alias = "session-server-alias";
    let session_id = "550e8400-e29b-41d4-a716-446655440013";
    let session = BlockCacheSessionOptions::new(
        vfs,
        AttachSpec::new(storage).alias(alias),
        session_id,
        session_scope("session.sqlite"),
        session_operation_id(2),
    )
    .expect("construct session server payload");
    let mut server = RemoteServer::start_with_options(
        format!("/{alias}"),
        &RemoteServerOptions::new().blockcache_session(session),
    )
    .expect("start session-owned remote server");

    let response = send_http_request(
        server.port(),
        b"GET /session.sqlite/root HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
    );
    assert!(
        response.starts_with("HTTP/1.1 200 OK\r\n"),
        "native request should resolve the attached alias: {response}"
    );
    // The native server intentionally retains an idle read ChunkStore in its
    // per-server cache.  This fixture performs the no-xSync write through the
    // owning VFS after the read response, so close that request cache before
    // opening the writer.  A real application request would write through
    // the same native handler rather than mix the two access paths.
    server
        .quiesce()
        .expect("quiesce completed read before direct VFS fixture write");
    // Add a local write with synchronous=OFF after the handler response. This
    // is the generic storage-level fixture; the HTTP mutation path is covered
    // separately by run_session_http_flow. The close-out operation must still
    // perform the final checkpoint. Use DoltLite's own commit path (rather
    // than a generic SQLite transaction).
    // The row write is made with synchronous=OFF, then DoltLite commits the
    // resulting working tree with its normal durability settings; close-out
    // itself must not depend on another xSync callback.
    let no_sync_db = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("open session database for no-xSync write");
    // DoltLite's block-cache VFS deliberately rejects journal-mode changes;
    // in particular, a request cannot silently switch this fixture to WAL.
    // Keep this assertion explicit instead of treating the rejection as a
    // reason to weaken the no-WAL coverage.
    let wal_error = no_sync_db
        .query_row::<String, _, _>("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .expect_err("bcvfs must reject a WAL journal-mode request");
    assert!(
        matches!(
            &wal_error,
            rusqlite::Error::SqliteFailure(_, Some(message))
                if message.contains("cannot use \"PRAGMA journal_mode\" with bcvfs")
        ),
        "unexpected journal-mode result: {wal_error:?}"
    );
    no_sync_db
        .execute_batch(
            "PRAGMA synchronous=OFF;
             INSERT INTO session_server VALUES ('no-xsync');",
        )
        .expect("write session database without xSync");
    let _: String = no_sync_db
        .query_row("SELECT dolt_commit('-A', '-m', 'no-xsync')", [], |row| {
            row.get(0)
        })
        .expect("commit DoltLite no-xSync update");
    let changed_rows: i64 = no_sync_db
        .query_row(
            "SELECT COUNT(*) FROM session_server WHERE value = 'no-xsync'",
            [],
            |row| row.get(0),
        )
        .expect("query DoltLite no-xSync update");
    assert_eq!(
        changed_rows, 1,
        "the DoltLite update must be visible locally"
    );
    drop(no_sync_db);
    let reopened_db = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("reopen session database after no-xSync write");
    let reopened_rows: i64 = reopened_db
        .query_row(
            "SELECT COUNT(*) FROM session_server WHERE value = 'no-xsync'",
            [],
            |row| row.get(0),
        )
        .expect("query reopened session database after no-xSync write");
    assert_eq!(
        reopened_rows, 1,
        "the no-xSync row must survive reopening through the VFS"
    );
    drop(reopened_db);
    let mut adapter = ReferenceResponseAdapter::new(&mut server);
    adapter.buffer(response.into_bytes());
    // The native request above is a valid read fixture. The first application
    // response completes an intermediate request but intentionally does not
    // publish the ordinary manifest. No native DoltLite route is renamed or
    // intercepted by this reference adapter.
    adapter
        .release_after_completion("/chunks", 200)
        .expect("successful no-xSync intermediate completion");
    let after_checkpoint_manifest =
        fetch_google_object(&endpoint, bucket, &format!("{prefix}/manifest.bcv"));
    assert_eq!(
        baseline_manifest, after_checkpoint_manifest,
        "intermediate completion must not publish the ordinary manifest"
    );
    drop(server);

    // A fresh server instance reconciles the same opaque operation before the
    // application's final close-out decision. The no-argument upload is the
    // only publication call.
    let mut server =
        start_http_session_server(vfs, &endpoint, bucket, &prefix, alias, session_id, 2);
    assert_eq!(
        server.operation_status().expect("accepted no-xSync status"),
        SessionOperationStatus::Accepted
    );
    let mut final_adapter = ReferenceResponseAdapter::new(&mut server);
    final_adapter.buffer(b"commit response");
    final_adapter
        .release_after_completion("/commit", 200)
        .expect("successful no-xSync close-out must checkpoint and publish");
    let checkpoint_prefix = format!("{prefix}/bcv-session/v1/checkpoint/{session_id}/");
    let checkpoint_listing = list_google_objects(&endpoint, bucket, &checkpoint_prefix);
    let checkpoint_name = first_google_object_name(&checkpoint_listing, &checkpoint_prefix)
        .expect("successful no-xSync completion must persist a checkpoint object");
    let checkpoint_record = fetch_google_object(&endpoint, bucket, &checkpoint_name);
    assert!(
        !checkpoint_record.is_empty(),
        "the durable checkpoint record must be readable"
    );
    let checkpoint_database = reassemble_checkpoint_database(
        "google",
        &endpoint,
        bucket,
        &prefix,
        &checkpoint_record,
        "session.sqlite",
    );
    let checkpoint_file =
        tempfile::NamedTempFile::new().expect("checkpoint database temporary file");
    let mut checkpoint_file_handle = checkpoint_file
        .reopen()
        .expect("reopen checkpoint database temporary file");
    checkpoint_file_handle
        .write_all(&checkpoint_database)
        .expect("write rehydrated checkpoint database");
    checkpoint_file_handle
        .flush()
        .expect("flush rehydrated checkpoint database");
    let rehydrated = Connection::open(checkpoint_file.path())
        .expect("open database rehydrated from checkpoint blocks");
    let rehydrated_rows: i64 = rehydrated
        .query_row(
            "SELECT COUNT(*) FROM session_server WHERE value = 'no-xsync'",
            [],
            |row| row.get(0),
        )
        .expect("query database rehydrated from checkpoint blocks");
    assert_eq!(
        rehydrated_rows, 1,
        "checkpoint rehydration must contain the DoltLite no-xSync row"
    );
    drop(rehydrated);
    let after_manifest = fetch_google_object(&endpoint, bucket, &format!("{prefix}/manifest.bcv"));
    assert_ne!(
        baseline_manifest, after_manifest,
        "explicit upload must publish the no-xSync manifest"
    );
    drop(server);
    let committed_retry =
        start_http_session_server(vfs, &endpoint, bucket, &prefix, alias, session_id, 2);
    let committed_status = committed_retry
        .operation_status()
        .expect("committed operation status");
    assert_eq!(committed_status, SessionOperationStatus::Committed);
    assert!(!should_invoke_mutating_handler(committed_status));
    drop(committed_retry);
    let stale_storage =
        Storage::google_json_with_endpoint("test-project", format!("{bucket}/{prefix}"), &endpoint);
    let stale_result = try_start_http_session_server_with_storage(
        vfs,
        stale_storage,
        alias,
        session_id,
        session_operation_id(1),
    );
    match stale_result {
        Ok(stale) => {
            let status = stale.operation_status().expect("stale operation status");
            assert_eq!(status, SessionOperationStatus::Conflict);
            assert!(!should_invoke_mutating_handler(status));
            panic!("stale operation should be rejected during server startup");
        }
        Err(rusqlite::Error::SqliteFailure(error, _)) => assert_eq!(
            error.extended_code,
            rusqlite::ffi::SQLITE_CONSTRAINT,
            "stale operation should fail with a startup constraint"
        ),
        Err(error) => panic!("stale operation returned an unexpected error: {error:?}"),
    }
}

#[cfg(feature = "remote")]
fn start_http_session_server(
    vfs: &'static BlockCacheVfs,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    alias: &str,
    session_id: &str,
    operation: u8,
) -> RemoteServer {
    start_http_session_server_with_id(
        vfs,
        endpoint,
        bucket,
        prefix,
        alias,
        session_id,
        session_operation_id(operation),
    )
}

#[cfg(feature = "remote")]
fn start_http_session_server_with_id(
    vfs: &'static BlockCacheVfs,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    alias: &str,
    session_id: &str,
    operation_id: SessionOperationId,
) -> RemoteServer {
    let storage =
        Storage::google_json_with_endpoint("test-project", format!("{bucket}/{prefix}"), endpoint);
    start_http_session_server_with_storage(vfs, storage, alias, session_id, operation_id)
}

#[cfg(feature = "remote")]
fn start_http_session_server_with_storage(
    vfs: &'static BlockCacheVfs,
    storage: Storage,
    alias: &str,
    session_id: &str,
    operation_id: SessionOperationId,
) -> RemoteServer {
    try_start_http_session_server_with_storage(vfs, storage, alias, session_id, operation_id)
        .expect("start HTTP session server")
}

#[cfg(feature = "remote")]
fn try_start_http_session_server_with_storage(
    vfs: &'static BlockCacheVfs,
    storage: Storage,
    alias: &str,
    session_id: &str,
    operation_id: SessionOperationId,
) -> rusqlite::Result<RemoteServer> {
    let session = BlockCacheSessionOptions::new(
        vfs,
        AttachSpec::new(storage).alias(alias),
        session_id,
        session_scope("session.sqlite"),
        operation_id,
    )?;
    RemoteServer::start_with_options(
        format!("/{alias}"),
        &RemoteServerOptions::new()
            .request_timeout(SESSION_HTTP_TEST_TIMEOUT)
            .blockcache_session(session),
    )
}

#[cfg(feature = "remote")]
fn proxy_operation_id(sequence: u64, method: &str, path: &str, body: &[u8]) -> SessionOperationId {
    // This deterministic mixer is a test-adapter surrogate only. It is not a
    // cryptographic digest and must not be used as a production idempotency
    // token; a production adapter should derive a collision-resistant ID from
    // the exact request bytes (or another stable app-side equivalent) and
    // preserve that ID across retries.
    // Read-only protocol calls are separate application invocations in this
    // reference proxy and therefore include the deterministic request
    // sequence. Mutating calls intentionally omit it: an exact retry of
    // /chunks, /refs-if, or /commit must recover the same operation head.
    let route = request_route(path);
    let read_only = matches!(
        route,
        "root" | "refs" | "chunk" | "has-chunks" | "get-chunks"
    );
    let operation_seed = if read_only { sequence } else { 0 };
    let mut lanes = [
        0x243f_6a88_85a3_08d3_u64 ^ operation_seed,
        0x1319_8a2e_0370_7344_u64.wrapping_add(operation_seed.rotate_left(7)),
        0xa409_3822_299f_31d0_u64 ^ operation_seed.rotate_left(17),
        0x082e_fa98_ec4e_6c89_u64.wrapping_add(operation_seed.rotate_left(31)),
    ];
    for (index, byte) in method
        .bytes()
        .chain(std::iter::once(0))
        .chain(path.bytes())
        .chain(std::iter::once(0))
        .chain(body.iter().copied())
        .enumerate()
    {
        let lane = index % lanes.len();
        lanes[lane] = lanes[lane]
            .wrapping_mul(0x1000_0000_01b3)
            .wrapping_add(u64::from(byte) + 0x9e37_79b9 + index as u64);
    }
    let mut bytes = [0_u8; SESSION_OPERATION_ID_BYTES];
    for (index, lane) in lanes.into_iter().enumerate() {
        bytes[index * 8..(index + 1) * 8].copy_from_slice(&lane.to_le_bytes());
    }
    SessionOperationId::new(bytes).expect("proxy operation ID is non-zero")
}

#[cfg(feature = "remote")]
fn empty_http_response(status: u16) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        409 => "Conflict",
        _ => "Error",
    };
    format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .into_bytes()
}

#[cfg(feature = "remote")]
struct SessionHttpProxy {
    port: u16,
    session_token: String,
    stop: Arc<AtomicBool>,
    last_commit_request: Arc<Mutex<Option<Vec<u8>>>>,
    failure: Arc<Mutex<Option<String>>>,
    last_native_error: Arc<Mutex<Option<String>>>,
    request_log: Arc<Mutex<Vec<String>>>,
    join: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "remote")]
impl SessionHttpProxy {
    fn start(
        vfs: &'static BlockCacheVfs,
        storage: Storage,
        database: &str,
        alias: &str,
        session_id: &str,
    ) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind stable test proxy");
        listener
            .set_nonblocking(true)
            .expect("configure stable test proxy");
        let port = listener.local_addr().expect("stable proxy address").port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let storage = storage.clone();
        let database = database.to_owned();
        let alias = alias.to_owned();
        let session_id = session_id.to_owned();
        let session_token = format!("token-{}", unique_suffix());
        let session_token_thread = session_token.clone();
        let last_commit_request = Arc::new(Mutex::new(None));
        let last_commit_request_thread = Arc::clone(&last_commit_request);
        let failure = Arc::new(Mutex::new(None));
        let failure_thread = Arc::clone(&failure);
        let last_native_error = Arc::new(Mutex::new(None));
        let last_native_error_thread = Arc::clone(&last_native_error);
        let request_log = Arc::new(Mutex::new(Vec::new()));
        let request_log_thread = Arc::clone(&request_log);
        let join = thread::spawn(move || {
            let mut sequence = 1_u64;
            while !stop_thread.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _peer)) => {
                        stream
                            .set_read_timeout(Some(HTTP_TEST_TIMEOUT))
                            .expect("proxy request timeout");
                        let request = match read_http_request(&mut stream) {
                            Some(request) => request,
                            None => continue,
                        };
                        let (method, path, body) = request_parts(&request);
                        let Some(native_path) =
                            scoped_session_request_path(&path, &session_token_thread, &database)
                        else {
                            // Database selection is an application-level routing
                            // boundary. Do not attach the session or let the native
                            // server resolve a request with a missing or foreign
                            // session token.
                            stream
                                .write_all(&empty_http_response(404))
                                .expect("write out-of-scope session response");
                            continue;
                        };
                        if request_route(&native_path) == "commit" {
                            *last_commit_request_thread
                                .lock()
                                .expect("lock last commit request") = Some(request.clone());
                        }
                        let operation_id =
                            proxy_operation_id(sequence, &method, &native_path, &body);
                        sequence = sequence.wrapping_add(1).max(1);
                        let mut server = match try_start_http_session_server_with_storage(
                            vfs,
                            storage.clone(),
                            &alias,
                            &session_id,
                            operation_id,
                        ) {
                            Ok(server) => server,
                            Err(error) => {
                                let message = format!(
                                    "start request route={} sequence={} failed: {error:?}",
                                    request_route(&native_path),
                                    sequence - 1
                                );
                                if let Ok(mut failure) = failure_thread.lock() {
                                    if failure.is_none() {
                                        *failure = Some(message);
                                    }
                                }
                                stream
                                    .write_all(&empty_http_response(500))
                                    .expect("write proxy start failure response");
                                continue;
                            }
                        };
                        let status = match server.operation_status() {
                            Ok(status) => status,
                            Err(error) => {
                                let message = format!(
                                    "status request route={} sequence={} failed: {error:?}",
                                    request_route(&native_path),
                                    sequence - 1
                                );
                                if let Ok(mut failure) = failure_thread.lock() {
                                    if failure.is_none() {
                                        *failure = Some(message);
                                    }
                                }
                                let _ = server.quiesce();
                                stream
                                    .write_all(&empty_http_response(500))
                                    .expect("write proxy status failure response");
                                continue;
                            }
                        };
                        let route = format!("/{}", request_route(&native_path));
                        if let Ok(mut log) = request_log_thread.lock() {
                            log.push(format!(
                                "sequence={} method={method} route={route} body_bytes={}{}",
                                sequence - 1,
                                body.len(),
                                if route == "/chunks" {
                                    format!(" {}", chunk_batch_stats(&body))
                                } else {
                                    String::new()
                                }
                            ));
                        }
                        let response = if should_invoke_mutating_handler(status) {
                            // A New operation is the only status that may invoke the
                            // native mutating handler. The application adapter owns
                            // the route decision and performs completion before the
                            // response is released to the client.
                            let native_request = rewrite_http_request_path(&request, &native_path);
                            let response = send_http_request_raw_with_timeout(
                                server.port(),
                                &native_request,
                                SESSION_HTTP_TEST_TIMEOUT,
                            );
                            let response_status = http_status(&response);
                            if response_status >= 400 {
                                let response_text = String::from_utf8_lossy(&response);
                                let diagnostic = format!(
                                    "route={route} sequence={} request_body_bytes={} status={response_status} response={}",
                                    sequence - 1,
                                    body.len(),
                                    response_text.chars().take(1024).collect::<String>()
                                );
                                if let Ok(mut last_error) = last_native_error_thread.lock() {
                                    *last_error = Some(diagnostic);
                                }
                            }
                            let mut adapter = ReferenceResponseAdapter::new(&mut server);
                            adapter.buffer(response);
                            match adapter.release_after_completion_staged(&route, response_status) {
                                Ok(response) => response,
                                Err((stage, error)) => {
                                    let message = format!(
                                        "{stage} failed for route={route} sequence={} operation_status={status:?} response_status={response_status}: {error:?}",
                                        sequence - 1
                                    );
                                    if let Ok(mut failure) = failure_thread.lock() {
                                        if failure.is_none() {
                                            *failure = Some(message);
                                        }
                                    }
                                    empty_http_response(500)
                                }
                            }
                        } else if status == SessionOperationStatus::Committed && route == "/commit"
                        {
                            // An exact retry after the response was lost has already
                            // committed this operation. Reconcile it without replaying
                            // the native mutating route or publishing a second head.
                            server.quiesce().expect("quiesce committed retry");
                            empty_http_response(200)
                        } else if status == SessionOperationStatus::Accepted && route == "/commit" {
                            // The prior invocation accepted/checkpointed its writes,
                            // but the application may have lost the final response
                            // before publication. The application adapter can safely
                            // finish the explicit /commit close-out without replaying
                            // the mutating handler.
                            let mut adapter = ReferenceResponseAdapter::new(&mut server);
                            adapter.buffer(empty_http_response(200));
                            match adapter.release_after_completion_staged("/commit", 200) {
                                Ok(response) => response,
                                Err((stage, error)) => {
                                    let message = format!(
                                        "{stage} failed while reconciling accepted /commit sequence={}: {error:?}",
                                        sequence - 1
                                    );
                                    if let Ok(mut failure) = failure_thread.lock() {
                                        if failure.is_none() {
                                            *failure = Some(message);
                                        }
                                    }
                                    empty_http_response(500)
                                }
                            }
                        } else {
                            // Failed, stale, or already accepted non-final operations
                            // fail closed. In particular, do not blindly replay a
                            // rejected handler merely because the request arrived at
                            // a fresh server instance.
                            server.quiesce().expect("quiesce rejected operation");
                            empty_http_response(409)
                        };
                        stream.write_all(&response).expect("write proxied response");
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("stable test proxy accept failed: {error}"),
                }
            }
        });
        Self {
            port,
            session_token,
            stop,
            last_commit_request,
            failure,
            last_native_error,
            request_log,
            join: Some(join),
        }
    }

    fn database_url(&self, database: &str) -> String {
        format!(
            "http://127.0.0.1:{}/s/{}/{}",
            self.port, self.session_token, database
        )
    }

    fn request_path(&self, path: &str) -> String {
        format!("/s/{}/{}", self.session_token, path.trim_start_matches('/'))
    }

    fn last_commit_request(&self) -> Vec<u8> {
        self.last_commit_request
            .lock()
            .expect("lock last commit request")
            .clone()
            .expect("push must send a /commit request")
    }

    fn failure(&self) -> Option<String> {
        self.failure.lock().expect("lock proxy failure").clone()
    }

    fn last_native_error(&self) -> Option<String> {
        self.last_native_error
            .lock()
            .expect("lock last native response error")
            .clone()
    }

    fn request_log(&self) -> Vec<String> {
        self.request_log
            .lock()
            .expect("lock proxy request log")
            .clone()
    }
}

#[cfg(feature = "remote")]
impl Drop for SessionHttpProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(join) = self.join.take() {
            if let Err(panic) = join.join() {
                if let Ok(mut failure) = self.failure.lock() {
                    if failure.is_none() {
                        *failure = Some(format!("proxy thread panic: {panic:?}"));
                    }
                }
            }
        }
    }
}

/// Storage-level fault seam used by the session failure matrix below.
///
/// The native CBS Tcl socket controls are part of the standalone Tcl test
/// binary and are not linked into rusqlite.  This proxy therefore injects the
/// same failures at the actual libcurl HTTP boundary used by the Rust VFS. It
/// matches object names after URL decoding, so the test exercises both the
/// Google JSON and S3 providers rather than a mocked storage implementation.
#[cfg(feature = "remote")]
fn test_crc32c_base64(data: &[u8]) -> String {
    const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut crc = 0xFFFF_FFFF_u32;
    for byte in data {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0_u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82F6_3B78 & mask);
        }
    }
    let bytes = (!crc).to_be_bytes();
    let indexes = [
        bytes[0] >> 2,
        ((bytes[0] & 0x03) << 4) | (bytes[1] >> 4),
        ((bytes[1] & 0x0F) << 2) | (bytes[2] >> 6),
        bytes[2] & 0x3F,
        bytes[3] >> 2,
        (bytes[3] & 0x03) << 4,
    ];
    let mut encoded = String::with_capacity(8);
    for index in indexes {
        encoded.push(char::from(BASE64[usize::from(index)]));
    }
    encoded.push_str("==");
    encoded
}

#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageFaultTarget {
    BlockPut,
    BlockGet,
    XSyncCheckpoint,
    ResponseGateCheckpoint,
    SessionHeadCas,
    PublicationHeadCas,
    ManifestPut,
    FinalHeadCas,
}

#[cfg(feature = "remote")]
impl StorageFaultTarget {
    fn code(self) -> u8 {
        match self {
            Self::BlockPut => 1,
            Self::BlockGet => 8,
            Self::XSyncCheckpoint => 2,
            Self::ResponseGateCheckpoint => 3,
            Self::SessionHeadCas => 4,
            Self::PublicationHeadCas => 5,
            Self::ManifestPut => 6,
            Self::FinalHeadCas => 7,
        }
    }
}

#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageFaultAction {
    Reject,
    DropResponse,
    KillProcess,
    BadUploadChecksum,
    CorruptDownloadBody,
    MissingDownloadChecksum,
    MalformedDownloadChecksum,
    ShortDownloadWithValidChecksum,
    Unauthorized,
    Forbidden,
    AlwaysUnauthorized,
    AlwaysForbidden,
    CreateThenReject,
    CreateCorruptedThenReject,
}

#[cfg(feature = "remote")]
impl StorageFaultAction {
    fn code(self) -> u8 {
        match self {
            Self::Reject => 1,
            Self::DropResponse => 2,
            Self::KillProcess => 3,
            Self::BadUploadChecksum => 4,
            Self::CorruptDownloadBody => 5,
            Self::MissingDownloadChecksum => 6,
            Self::MalformedDownloadChecksum => 7,
            Self::ShortDownloadWithValidChecksum => 8,
            Self::Unauthorized => 9,
            Self::Forbidden => 10,
            Self::AlwaysUnauthorized => 11,
            Self::AlwaysForbidden => 12,
            Self::CreateThenReject => 13,
            Self::CreateCorruptedThenReject => 14,
        }
    }
}

#[cfg(feature = "remote")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct BlockPutObservation {
    object: String,
    if_generation_match: Option<String>,
    payload_bytes: usize,
    payload_crc32c: String,
    google_crc32c: Option<String>,
    authorization: Option<String>,
    injected_status: Option<u16>,
}

#[cfg(feature = "remote")]
struct StorageFaultProxy {
    port: u16,
    stop: Arc<AtomicBool>,
    armed: Arc<AtomicBool>,
    target: Arc<AtomicU8>,
    action: Arc<AtomicU8>,
    matched: Arc<AtomicUsize>,
    faulted: Arc<AtomicUsize>,
    kill_pid: Arc<AtomicUsize>,
    upload_checksums_valid: Arc<Mutex<Vec<bool>>>,
    block_put_observations: Arc<Mutex<Vec<BlockPutObservation>>>,
    thread: Option<thread::JoinHandle<()>>,
    url: String,
}

#[cfg(feature = "remote")]
impl StorageFaultProxy {
    fn start(endpoint: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind storage fault proxy");
        listener
            .set_nonblocking(true)
            .expect("configure storage fault proxy");
        let address = listener.local_addr().expect("storage fault proxy address");
        let url = format!("http://{address}");
        let authority = endpoint
            .trim_end_matches('/')
            .strip_prefix("http://")
            .or_else(|| endpoint.trim_end_matches('/').strip_prefix("https://"))
            .and_then(|value| value.split('/').next())
            .expect("storage endpoint must have an HTTP authority")
            .to_owned();
        let stop = Arc::new(AtomicBool::new(false));
        let armed = Arc::new(AtomicBool::new(false));
        let target = Arc::new(AtomicU8::new(0));
        let action = Arc::new(AtomicU8::new(0));
        let matched = Arc::new(AtomicUsize::new(0));
        let faulted = Arc::new(AtomicUsize::new(0));
        let kill_pid = Arc::new(AtomicUsize::new(0));
        let upload_checksums_valid = Arc::new(Mutex::new(Vec::new()));
        let upload_checksums_valid_thread = Arc::clone(&upload_checksums_valid);
        let block_put_observations = Arc::new(Mutex::new(Vec::new()));
        let block_put_observations_thread = Arc::clone(&block_put_observations);
        let thread_stop = Arc::clone(&stop);
        let thread_armed = Arc::clone(&armed);
        let thread_target = Arc::clone(&target);
        let thread_action = Arc::clone(&action);
        let thread_matched = Arc::clone(&matched);
        let thread_faulted = Arc::clone(&faulted);
        let thread_kill_pid = Arc::clone(&kill_pid);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                let result = handle_storage_fault_connection(
                    &mut stream,
                    &authority,
                    StorageFaultControls {
                        armed: &thread_armed,
                        target: &thread_target,
                        action: &thread_action,
                        matched: &thread_matched,
                        faulted: &thread_faulted,
                        kill_pid: &thread_kill_pid,
                        upload_checksums_valid: &upload_checksums_valid_thread,
                        block_put_observations: &block_put_observations_thread,
                    },
                );
                if let Err(error) = result {
                    let body = format!("storage fault proxy error: {error}");
                    let response = format!(
                        "HTTP/1.1 502 Bad Gateway\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(response.as_bytes());
                }
            }
        });
        Self {
            port: address.port(),
            stop,
            armed,
            target,
            action,
            matched,
            faulted,
            kill_pid,
            upload_checksums_valid,
            block_put_observations,
            thread: Some(thread),
            url,
        }
    }

    fn configure(&self, target: StorageFaultTarget, action: StorageFaultAction) {
        self.armed.store(false, Ordering::Release);
        self.target.store(target.code(), Ordering::Release);
        self.action.store(action.code(), Ordering::Release);
        self.matched.store(0, Ordering::Release);
        self.faulted.store(0, Ordering::Release);
        self.kill_pid.store(0, Ordering::Release);
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
        self.kill_pid.store(0, Ordering::Release);
    }

    fn arm_kill_process(&self, target: StorageFaultTarget, pid: u32) {
        self.configure(target, StorageFaultAction::KillProcess);
        self.kill_pid.store(pid as usize, Ordering::Release);
        self.arm();
    }

    fn matched(&self) -> usize {
        self.matched.load(Ordering::Acquire)
    }

    fn faulted(&self) -> usize {
        self.faulted.load(Ordering::Acquire)
    }

    fn upload_checksums_valid(&self) -> Vec<bool> {
        self.upload_checksums_valid
            .lock()
            .expect("lock recorded GCS checksums")
            .clone()
    }

    fn block_put_observations(&self) -> Vec<BlockPutObservation> {
        self.block_put_observations
            .lock()
            .expect("lock recorded GCS block PUTs")
            .clone()
    }

    fn clear_block_put_observations(&self) {
        self.block_put_observations
            .lock()
            .expect("lock recorded GCS block PUTs")
            .clear();
    }
}

#[cfg(feature = "remote")]
impl Drop for StorageFaultProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(feature = "remote")]
struct StorageFaultControls<'a> {
    armed: &'a AtomicBool,
    target: &'a AtomicU8,
    action: &'a AtomicU8,
    matched: &'a AtomicUsize,
    faulted: &'a AtomicUsize,
    kill_pid: &'a AtomicUsize,
    upload_checksums_valid: &'a Mutex<Vec<bool>>,
    block_put_observations: &'a Mutex<Vec<BlockPutObservation>>,
}

#[cfg(feature = "remote")]
fn handle_storage_fault_connection(
    stream: &mut TcpStream,
    authority: &str,
    controls: StorageFaultControls<'_>,
) -> io::Result<()> {
    stream.set_read_timeout(Some(HTTP_TEST_TIMEOUT))?;
    stream.set_write_timeout(Some(HTTP_TEST_TIMEOUT))?;
    let (method, request_target, headers, body) = read_storage_proxy_request(stream)?;
    let media_upload = method.eq_ignore_ascii_case("POST")
        && request_target.contains("/upload/storage/v1/")
        && request_target.contains("uploadType=media");
    if media_upload {
        let expected = format!("crc32c={}", test_crc32c_base64(&body));
        let valid = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-goog-hash"))
            .is_some_and(|(_, value)| value == &expected);
        controls
            .upload_checksums_valid
            .lock()
            .expect("lock recorded GCS checksums")
            .push(valid);
    }
    let request_fault = storage_fault_target(&method, &request_target, &body);
    let configured_target = controls.target.load(Ordering::Acquire);
    let should_fault = request_fault.is_some_and(|fault| {
        let target_matches = fault.code() == configured_target
            || (fault == StorageFaultTarget::XSyncCheckpoint
                && configured_target == StorageFaultTarget::ResponseGateCheckpoint.code());
        target_matches && controls.armed.load(Ordering::Acquire)
    });
    if should_fault {
        controls.matched.fetch_add(1, Ordering::AcqRel);
    }
    let action = controls.action.load(Ordering::Acquire);
    let always_reject = matches!(
        action,
        value if value == StorageFaultAction::AlwaysUnauthorized.code()
            || value == StorageFaultAction::AlwaysForbidden.code()
    );
    let fault_now = should_fault
        && (always_reject
            || controls
                .faulted
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok());
    if fault_now && always_reject {
        controls.faulted.fetch_add(1, Ordering::AcqRel);
    }
    let injected_status = if fault_now {
        match action {
            value
                if value == StorageFaultAction::Unauthorized.code()
                    || value == StorageFaultAction::AlwaysUnauthorized.code() =>
            {
                Some(401)
            }
            value
                if value == StorageFaultAction::Forbidden.code()
                    || value == StorageFaultAction::AlwaysForbidden.code() =>
            {
                Some(403)
            }
            value
                if value == StorageFaultAction::Reject.code()
                    || value == StorageFaultAction::CreateThenReject.code()
                    || value == StorageFaultAction::CreateCorruptedThenReject.code() =>
            {
                Some(412)
            }
            _ => None,
        }
    } else {
        None
    };
    if request_fault == Some(StorageFaultTarget::BlockPut) {
        controls
            .block_put_observations
            .lock()
            .expect("lock recorded GCS block PUTs")
            .push(BlockPutObservation {
                object: google_upload_object(&request_target)
                    .unwrap_or_else(|| storage_request_path(&request_target)),
                if_generation_match: google_upload_generation_match(&request_target),
                payload_bytes: body.len(),
                payload_crc32c: test_crc32c_base64(&body),
                google_crc32c: headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("x-goog-hash"))
                    .map(|(_, value)| value.clone()),
                authorization: headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                    .map(|(_, value)| value.clone()),
                injected_status,
            });
    }
    if fault_now && matches!(injected_status, Some(401 | 403)) {
        let status = injected_status.expect("matched injected authentication status");
        write!(
            stream,
            "HTTP/1.1 {status} {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            if status == 401 {
                "Unauthorized"
            } else {
                "Forbidden"
            }
        )?;
        return Ok(());
    }
    if fault_now
        && controls.action.load(Ordering::Acquire) == StorageFaultAction::KillProcess.code()
    {
        forward_storage_proxy_request(authority, &method, &request_target, &headers, &body)?;
        let pid = controls.kill_pid.load(Ordering::Acquire);
        if pid > 0 {
            let pid = pid.to_string();
            let _ = Command::new("kill").args(["-KILL", &pid]).status();
        }
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(());
    }
    if fault_now
        && controls.action.load(Ordering::Acquire) == StorageFaultAction::CreateThenReject.code()
    {
        let created =
            forward_storage_proxy_request(authority, &method, &request_target, &headers, &body)?;
        let created_status = http_status(&created);
        if !(200..300).contains(&created_status) {
            return Err(io::Error::other(
                "test proxy could not pre-create the immutable block",
            ));
        }
        stream.write_all(
            b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        return Ok(());
    }
    if fault_now
        && controls.action.load(Ordering::Acquire)
            == StorageFaultAction::CreateCorruptedThenReject.code()
    {
        let Some(first) = body.first() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "test proxy cannot corrupt an empty immutable block",
            ));
        };
        let mut corrupted_body = body.clone();
        corrupted_body[0] = *first ^ 0x01;
        let checksum = format!("crc32c={}", test_crc32c_base64(&corrupted_body));
        let mut corrupted_headers = headers.clone();
        set_test_header(&mut corrupted_headers, "x-goog-hash", &checksum);
        let created = forward_storage_proxy_request(
            authority,
            &method,
            &request_target,
            &corrupted_headers,
            &corrupted_body,
        )?;
        let created_status = http_status(&created);
        if !(200..300).contains(&created_status) {
            return Err(io::Error::other(
                "test proxy could not pre-create the corrupted immutable block",
            ));
        }
        stream.write_all(
            b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        return Ok(());
    }
    if fault_now && controls.action.load(Ordering::Acquire) == StorageFaultAction::Reject.code() {
        stream.write_all(
            b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        return Ok(());
    }

    if fault_now && action == StorageFaultAction::BadUploadChecksum.code() {
        let expected = format!("crc32c={}", test_crc32c_base64(&body));
        let original_checksum = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-goog-hash"))
            .map(|(_, value)| value.as_str());
        if original_checksum != Some(expected.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "test proxy received a bad client CRC32C before fault injection",
            ));
        }
        let mut wrong = expected.as_bytes().to_vec();
        let checksum_index = wrong.len() - 3;
        wrong[checksum_index] = if wrong[checksum_index] == b'A' {
            b'B'
        } else {
            b'A'
        };
        let wrong = String::from_utf8(wrong).expect("CRC32C checksum is ASCII");
        let mut rejected_headers = headers.clone();
        set_test_header(&mut rejected_headers, "x-goog-hash", &wrong);
        let rejected_checksum = rejected_headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("x-goog-hash"))
            .map(|(_, value)| value.as_str());
        if wrong == expected || rejected_checksum != Some(wrong.as_str()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "test proxy failed to create a mismatched CRC32C",
            ));
        }
        let body = r#"{"error":{"code":400,"message":"CRC32C mismatch"}}"#;
        write!(
            stream,
            "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )?;
        stream.write_all(body.as_bytes())?;
        return Ok(());
    }
    let mut response =
        forward_storage_proxy_request(authority, &method, &request_target, &headers, &body)?;
    if fault_now && request_fault == Some(StorageFaultTarget::BlockGet) {
        response = mutate_gcs_download_response(&response, action)?;
    }
    if !(fault_now && action == StorageFaultAction::DropResponse.code()) {
        stream.write_all(&response)?;
    }
    if fault_now {
        let _ = stream.shutdown(Shutdown::Both);
    }
    Ok(())
}

#[cfg(feature = "remote")]
type StorageProxyRequest = (String, String, Vec<(String, String)>, Vec<u8>);

#[cfg(feature = "remote")]
fn read_storage_proxy_request(stream: &mut TcpStream) -> io::Result<StorageProxyRequest> {
    let mut request = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 8192];
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "storage proxy client closed before request headers",
            ));
        }
        request.extend_from_slice(&chunk[..count]);
        if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        if request.len() > 128 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "storage proxy request headers are too large",
            ));
        }
    };
    let header_text = String::from_utf8_lossy(&request[..header_end]);
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "storage proxy request line"))?;
    let mut request_line = request_line.split_whitespace();
    let method = request_line
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "storage proxy method"))?
        .to_owned();
    let target = request_line
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "storage proxy target"))?
        .to_owned();
    let version = request_line
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "storage proxy HTTP version"))?;
    if version != "HTTP/1.1" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "storage proxy only supports HTTP/1.1",
        ));
    }
    let mut headers = Vec::new();
    let mut content_length = 0_usize;
    let mut expect_continue = false;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "storage proxy malformed header")
        })?;
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "storage proxy invalid content length",
                )
            })?;
        }
        if name.eq_ignore_ascii_case("expect") && value.trim().eq_ignore_ascii_case("100-continue")
        {
            expect_continue = true;
        }
        headers.push((name.to_owned(), value.trim().to_owned()));
    }
    if content_length > 32 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "storage proxy request body is too large",
        ));
    }
    if expect_continue {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
    }
    let mut body = request[header_end..].to_vec();
    while body.len() < content_length {
        let old_len = body.len();
        let mut chunk = [0_u8; 16 * 1024];
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "storage proxy client closed before request body",
            ));
        }
        body.extend_from_slice(&chunk[..count]);
        if body.len() == old_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "storage proxy made no progress reading request body",
            ));
        }
    }
    body.truncate(content_length);
    Ok((method, target, headers, body))
}

#[cfg(feature = "remote")]
fn forward_storage_proxy_request(
    authority: &str,
    method: &str,
    target: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> io::Result<Vec<u8>> {
    let origin_target = if let Some(rest) = target.strip_prefix("http://") {
        rest.find('/').map_or("/", |position| &rest[position..])
    } else if let Some(rest) = target.strip_prefix("https://") {
        rest.find('/').map_or("/", |position| &rest[position..])
    } else {
        target
    };
    let mut upstream = TcpStream::connect(authority)?;
    upstream.set_read_timeout(Some(HTTP_TEST_TIMEOUT))?;
    upstream.set_write_timeout(Some(HTTP_TEST_TIMEOUT))?;
    write!(upstream, "{method} {origin_target} HTTP/1.1\r\n")?;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("proxy-connection")
            || name.eq_ignore_ascii_case("connection")
            || name.eq_ignore_ascii_case("expect")
        {
            continue;
        }
        write!(upstream, "{name}: {value}\r\n")?;
    }
    upstream.write_all(b"Connection: close\r\n\r\n")?;
    upstream.write_all(body)?;
    let mut response = Vec::new();
    upstream.read_to_end(&mut response)?;
    Ok(response)
}

#[cfg(feature = "remote")]
fn storage_request_path(target: &str) -> String {
    let path = target
        .split_once("://")
        .and_then(|(_, rest)| rest.find('/').map(|index| &rest[index..]))
        .unwrap_or(target);
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    percent_decode_path(path)
}

#[cfg(feature = "remote")]
fn google_upload_object(target: &str) -> Option<String> {
    let query = target.split_once('?')?.1;
    query.split('&').find_map(|parameter| {
        let (name, value) = parameter.split_once('=')?;
        name.eq_ignore_ascii_case("name")
            .then(|| percent_decode_path(value))
    })
}

#[cfg(feature = "remote")]
fn google_upload_generation_match(target: &str) -> Option<String> {
    let query = target.split_once('?')?.1;
    query.split('&').find_map(|parameter| {
        let (name, value) = parameter.split_once('=')?;
        name.eq_ignore_ascii_case("ifGenerationMatch")
            .then(|| percent_decode_path(value))
    })
}

#[cfg(feature = "remote")]
fn percent_decode_path(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut decoded = String::with_capacity(path.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let high = char::from(bytes[index + 1]).to_digit(16);
            let low = char::from(bytes[index + 2]).to_digit(16);
            if let (Some(high), Some(low)) = (high, low) {
                decoded.push(char::from((high * 16 + low) as u8));
                index += 3;
                continue;
            }
        }
        decoded.push(bytes[index] as char);
        index += 1;
    }
    decoded
}

#[cfg(feature = "remote")]
fn set_test_header(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    if let Some((_, old_value)) = headers
        .iter_mut()
        .find(|(old_name, _)| old_name.eq_ignore_ascii_case(name))
    {
        *old_value = value.to_owned();
    } else {
        headers.push((name.to_owned(), value.to_owned()));
    }
}

#[cfg(feature = "remote")]
fn mutate_gcs_download_response(response: &[u8], action: u8) -> io::Result<Vec<u8>> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing GCS headers"))?;
    let head = String::from_utf8_lossy(&response[..header_end - 4]);
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing GCS status"))?;
    if !status.contains(" 2") {
        return Ok(response.to_vec());
    }
    let mut headers = Vec::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.to_owned(), value.trim().to_owned()));
        }
    }
    if headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("transfer-encoding")
            && value.to_ascii_lowercase().contains("chunked")
    }) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected chunked GCS response in checksum test",
        ));
    }
    let mut body = response[header_end..].to_vec();
    match action {
        value if value == StorageFaultAction::CorruptDownloadBody.code() => {
            let Some(first) = body.first_mut() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty GCS block",
                ));
            };
            *first ^= 0x80;
        }
        value if value == StorageFaultAction::MissingDownloadChecksum.code() => {
            headers.retain(|(name, _)| !name.eq_ignore_ascii_case("x-goog-hash"));
        }
        value if value == StorageFaultAction::MalformedDownloadChecksum.code() => {
            set_test_header(&mut headers, "x-goog-hash", "crc32c=not-base64");
        }
        value if value == StorageFaultAction::ShortDownloadWithValidChecksum.code() => {
            if body.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty GCS block",
                ));
            }
            body.pop();
            let checksum = format!("crc32c={}", test_crc32c_base64(&body));
            set_test_header(&mut headers, "x-goog-hash", &checksum);
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown GCS download checksum fault",
            ));
        }
    }
    let mut output = Vec::new();
    write!(output, "{status}\r\n")?;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("transfer-encoding")
            || name.eq_ignore_ascii_case("connection")
        {
            continue;
        }
        write!(output, "{name}: {value}\r\n")?;
    }
    write!(
        output,
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    output.extend_from_slice(&body);
    Ok(output)
}

#[cfg(feature = "remote")]
fn storage_fault_target(method: &str, path: &str, body: &[u8]) -> Option<StorageFaultTarget> {
    let request_path = storage_request_path(path);
    if method.eq_ignore_ascii_case("GET")
        && request_path.contains("/download/storage/v1/")
        && request_path.contains("/blocks/")
    {
        return Some(StorageFaultTarget::BlockGet);
    }
    let object_path =
        if method.eq_ignore_ascii_case("POST") && request_path.contains("/upload/storage/v1/") {
            google_upload_object(path)?
        } else if method.eq_ignore_ascii_case("PUT") {
            request_path
        } else {
            return None;
        };
    if object_path.ends_with("/manifest.bcv") || object_path == "manifest.bcv" {
        return Some(StorageFaultTarget::ManifestPut);
    }
    if object_path.contains("/bcv-session/v1/checkpoint/") {
        return Some(StorageFaultTarget::XSyncCheckpoint);
    }
    if object_path.contains("/bcv-session/v1/head/") {
        return match body.get(11).copied() {
            Some(4) => Some(StorageFaultTarget::FinalHeadCas),
            _ => Some(StorageFaultTarget::SessionHeadCas),
        };
    }
    if object_path.contains("/bcv-session/v1/publication/") {
        return Some(StorageFaultTarget::PublicationHeadCas);
    }
    if object_path.contains("/bcv-session/v1/") {
        return None;
    }
    object_path
        .ends_with(".bcv")
        .then_some(StorageFaultTarget::BlockPut)
}

#[cfg(feature = "remote")]
#[test]
fn storage_fault_target_classifies_google_and_s3_uploads() {
    let google_checkpoint = "http://proxy/upload/storage/v1/b/bucket/o?uploadType=media&name=prefix%2Fbcv-session%2Fv1%2Fcheckpoint%2Fsession%2Fhash.bcv";
    assert_eq!(
        storage_fault_target("POST", google_checkpoint, &[]),
        Some(StorageFaultTarget::XSyncCheckpoint)
    );
    let google_manifest =
        "/upload/storage/v1/b/bucket/o?uploadType=media&name=prefix%2Fmanifest.bcv";
    assert_eq!(
        storage_fault_target("POST", google_manifest, &[]),
        Some(StorageFaultTarget::ManifestPut)
    );

    let mut final_head = vec![0_u8; 12];
    final_head[11] = 4;
    assert_eq!(
        storage_fault_target(
            "PUT",
            "/bucket/prefix/bcv-session/v1/head/session.bcv",
            &final_head
        ),
        Some(StorageFaultTarget::FinalHeadCas)
    );
    assert_eq!(
        storage_fault_target("PUT", "/bucket/prefix/ABCD.bcv", &[]),
        Some(StorageFaultTarget::BlockPut)
    );
}

#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug)]
enum SessionFaultWindow {
    BlockPut,
    XSyncCheckpoint,
    ResponseGateCheckpoint,
    SessionHeadCas,
    PublicationHeadCas,
    ManifestPut,
    FinalHeadCas,
    AmbiguousBlockPut,
    AmbiguousManifestPut,
}

#[cfg(feature = "remote")]
struct SessionFaultContext<'a> {
    backend: &'a str,
    endpoint: &'a str,
    storage_endpoint: &'a str,
    bucket: &'a str,
    base_prefix: &'a str,
    vfs: &'static BlockCacheVfs,
    proxy: &'a StorageFaultProxy,
}

#[cfg(feature = "remote")]
impl SessionFaultWindow {
    fn target(self) -> StorageFaultTarget {
        match self {
            Self::BlockPut | Self::AmbiguousBlockPut => StorageFaultTarget::BlockPut,
            Self::XSyncCheckpoint => StorageFaultTarget::XSyncCheckpoint,
            Self::ResponseGateCheckpoint => StorageFaultTarget::ResponseGateCheckpoint,
            Self::SessionHeadCas => StorageFaultTarget::SessionHeadCas,
            Self::PublicationHeadCas => StorageFaultTarget::PublicationHeadCas,
            Self::ManifestPut => StorageFaultTarget::ManifestPut,
            Self::FinalHeadCas => StorageFaultTarget::FinalHeadCas,
            Self::AmbiguousManifestPut => StorageFaultTarget::ManifestPut,
        }
    }

    fn action(self) -> StorageFaultAction {
        match self {
            Self::AmbiguousBlockPut | Self::AmbiguousManifestPut => {
                StorageFaultAction::DropResponse
            }
            _ => StorageFaultAction::Reject,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::BlockPut => "block-put-before-head",
            Self::XSyncCheckpoint => "xsync-checkpoint",
            Self::ResponseGateCheckpoint => "response-gate-checkpoint",
            Self::SessionHeadCas => "session-head-cas",
            Self::PublicationHeadCas => "publication-head-cas",
            Self::ManifestPut => "manifest-put",
            Self::FinalHeadCas => "final-head-cas",
            Self::AmbiguousBlockPut => "ambiguous-block-put",
            Self::AmbiguousManifestPut => "ambiguous-manifest-put",
        }
    }
}

#[cfg(feature = "remote")]
fn stage_session_update(database: &Connection) -> rusqlite::Result<()> {
    database.execute_batch(
        "PRAGMA synchronous=OFF;
         PRAGMA cache_size=16;
         BEGIN IMMEDIATE;",
    )?;
    let result = (|| {
        let mut insert = database.prepare("INSERT INTO fault_data(id, value) VALUES (?1, ?2)")?;
        for id in 2..=5_500_i64 {
            insert.execute(params![id, deterministic_payload(id, 2_048)])?;
        }
        database.execute_batch("COMMIT")
    })();
    if result.is_err() {
        let _ = database.execute_batch("ROLLBACK");
    }
    result
}

#[cfg(feature = "remote")]
fn stage_session_small_update(database: &Connection, synchronous: &str) -> rusqlite::Result<()> {
    database.execute_batch(&format!(
        "PRAGMA synchronous={synchronous};
         BEGIN IMMEDIATE;
         INSERT INTO fault_data(id, value) VALUES (2, 'xsync-window');"
    ))?;
    Ok(())
}

#[cfg(feature = "remote")]
fn run_session_fault_case(
    context: &SessionFaultContext<'_>,
    window: SessionFaultWindow,
    index: usize,
) {
    let SessionFaultContext {
        backend,
        endpoint,
        storage_endpoint,
        bucket,
        base_prefix,
        vfs,
        proxy,
    } = context;
    let prefix = format!("{base_prefix}/{}", window.label());
    let storage = session_storage(backend, storage_endpoint, bucket, &prefix);
    vfs.initialize_container(&storage)
        .unwrap_or_else(|error| panic!("initialize {} storage: {error:?}", window.label()));

    let seed_dir = tempfile::tempdir().expect("fault-window seed directory");
    let seed_path = seed_dir.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create fault-window seed database");
    seed.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE fault_data(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO fault_data VALUES (1, 'seed');",
    )
    .expect("seed fault-window database");
    seed.close().expect("close fault-window seed database");
    vfs.create_database(&storage, &seed_path, "session.sqlite")
        .unwrap_or_else(|error| panic!("upload {} seed database: {error:?}", window.label()));
    let baseline =
        fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"));

    let run_id = context
        .base_prefix
        .split('/')
        .next()
        .and_then(|prefix| prefix.rsplit('-').next())
        .unwrap_or("run");
    let alias = format!(
        "fault_{}_{}_{}_{}",
        context.backend,
        window.label().replace('-', "_"),
        index,
        run_id
    );
    let session_id = format!("550e8400-e29b-41d4-a716-{:012x}", index + 1);
    let operation = session_operation_id((index + 1) as u8);
    let mut server =
        start_http_session_server_with_storage(vfs, storage, &alias, &session_id, operation);
    proxy.configure(window.target(), window.action());

    match window {
        SessionFaultWindow::BlockPut | SessionFaultWindow::AmbiguousBlockPut => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open block fault-window database");
            proxy.arm();
            let result = stage_session_update(&database);
            assert!(
                proxy.faulted() > 0,
                "{} did not intercept a block PUT (matched={})",
                window.label(),
                proxy.matched()
            );
            if matches!(window, SessionFaultWindow::BlockPut) {
                assert!(result.is_err(), "a rejected block PUT must reach SQLite");
            }
            drop(database);
            server
                .quiesce()
                .unwrap_or_else(|error| panic!("quiesce {} server: {error:?}", window.label()));
            assert_eq!(
                baseline,
                fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv")),
                "{} must not publish before an accepted session head",
                window.label()
            );
        }
        SessionFaultWindow::XSyncCheckpoint => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open xSync fault-window database");
            stage_session_small_update(&database, "FULL")
                .expect("stage xSync fault-window transaction");
            proxy.arm();
            let result = database.execute_batch("COMMIT");
            assert!(
                result.is_err(),
                "a rejected xSync checkpoint must fail the committing writer"
            );
            assert!(
                proxy.faulted() > 0,
                "xSync checkpoint PUT was not intercepted (matched={})",
                proxy.matched()
            );
            let _ = database.execute_batch("ROLLBACK");
            drop(database);
            server.quiesce().expect("quiesce xSync fault server");
            assert_eq!(
                baseline,
                fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"))
            );
        }
        SessionFaultWindow::ResponseGateCheckpoint => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open response-gate fault-window database");
            stage_session_small_update(&database, "OFF")
                .expect("stage response-gate fault-window transaction");
            database
                .execute_batch("COMMIT")
                .expect("synchronous-off transaction must finish before response gate");
            drop(database);
            proxy.arm();
            let result = server.complete_request();
            assert!(
                result.is_err(),
                "response-gate checkpoint rejection must surface"
            );
            assert!(
                proxy.faulted() > 0,
                "response-gate checkpoint PUT was not intercepted (matched={})",
                proxy.matched()
            );
            assert_eq!(
                baseline,
                fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"))
            );
        }
        SessionFaultWindow::SessionHeadCas => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open session-head fault-window database");
            stage_session_small_update(&database, "OFF")
                .expect("stage session-head fault-window transaction");
            database
                .execute_batch("COMMIT")
                .expect("session-head transaction must finish before CAS");
            drop(database);
            proxy.arm();
            let result = server.complete_request();
            assert!(result.is_err(), "session-head CAS rejection must surface");
            assert!(
                proxy.faulted() > 0,
                "session-head CAS PUT was not intercepted (matched={})",
                proxy.matched()
            );
            assert_eq!(
                baseline,
                fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"))
            );
        }
        SessionFaultWindow::PublicationHeadCas
        | SessionFaultWindow::ManifestPut
        | SessionFaultWindow::FinalHeadCas
        | SessionFaultWindow::AmbiguousManifestPut => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open publication fault-window database");
            stage_session_small_update(&database, "OFF")
                .expect("stage publication fault-window transaction");
            database
                .execute_batch("COMMIT")
                .expect("publication transaction must finish before upload");
            drop(database);
            proxy.arm();
            // Model the application's `/commit` close-out. The public
            // upload operation owns its final checkpoint and publication;
            // an intermediate complete_request would change the failure
            // window and incorrectly accept an unpublished head first.
            let first = server.upload();
            assert!(
                proxy.faulted() > 0,
                "{} fault was not intercepted (matched={})",
                window.label(),
                proxy.matched()
            );
            match window {
                SessionFaultWindow::AmbiguousManifestPut => {
                    if first.is_err() {
                        server.upload().unwrap_or_else(|error| {
                            panic!("retry ambiguous manifest upload: {error:?}")
                        });
                    }
                    let after = fetch_session_object(
                        backend,
                        endpoint,
                        bucket,
                        &format!("{prefix}/manifest.bcv"),
                    );
                    assert_ne!(
                        baseline, after,
                        "ambiguous manifest PUT must be recoverable"
                    );
                }
                SessionFaultWindow::FinalHeadCas => {
                    assert!(first.is_err(), "rejected final head CAS must surface");
                    server
                        .upload()
                        .unwrap_or_else(|error| panic!("retry final head CAS: {error:?}"));
                    let after = fetch_session_object(
                        backend,
                        endpoint,
                        bucket,
                        &format!("{prefix}/manifest.bcv"),
                    );
                    assert_ne!(baseline, after, "final-head retry must retain publication");
                    let head = fetch_session_object(
                        backend,
                        endpoint,
                        bucket,
                        &format!("{prefix}/bcv-session/v1/head/{session_id}.bcv"),
                    );
                    assert_eq!(head.get(11).copied(), Some(4));
                }
                _ => {
                    assert!(first.is_err(), "{} rejection must surface", window.label());
                    assert_eq!(
                        baseline,
                        fetch_session_object(
                            backend,
                            endpoint,
                            bucket,
                            &format!("{prefix}/manifest.bcv")
                        ),
                        "{} must not publish before its fault is retried",
                        window.label()
                    );
                    server.upload().unwrap_or_else(|error| {
                        panic!("retry {} publication: {error:?}", window.label())
                    });
                    assert_ne!(
                        baseline,
                        fetch_session_object(
                            backend,
                            endpoint,
                            bucket,
                            &format!("{prefix}/manifest.bcv")
                        )
                    );
                }
            }
        }
    }
}

#[cfg(feature = "remote")]
fn run_session_fault_matrix(backend: &str) {
    let suffix = unique_suffix();
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
            .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
            .unwrap_or_else(|_| "http://127.0.0.1:19025".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown session backend {backend}"),
    };
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rust-session-fault-{suffix}")
    };
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    } else {
        ensure_s3_bucket(&endpoint, &bucket);
    }
    let proxy = StorageFaultProxy::start(&endpoint);
    let vfs_name = format!("rusqdoltlite-fault-{suffix}");
    // Builder::init returns a process-global VFS. Keep its cache root alive
    // across the GCS and S3 matrices, which run sequentially in this process.
    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("fault VFS builder")
        .name(&vfs_name)
        .expect("fault VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .config(Config::RequestCount(1))
        .config(Config::StageWatermark(50))
        .config(Config::HttpTimeout(3))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize fault VFS");
    let windows = [
        SessionFaultWindow::BlockPut,
        SessionFaultWindow::XSyncCheckpoint,
        SessionFaultWindow::ResponseGateCheckpoint,
        SessionFaultWindow::SessionHeadCas,
        SessionFaultWindow::PublicationHeadCas,
        SessionFaultWindow::ManifestPut,
        SessionFaultWindow::FinalHeadCas,
        SessionFaultWindow::AmbiguousBlockPut,
        SessionFaultWindow::AmbiguousManifestPut,
    ];
    let matrix_prefix = format!("{suffix}/fault-matrix");
    let context = SessionFaultContext {
        backend,
        endpoint: &endpoint,
        storage_endpoint: &proxy.url,
        bucket: &bucket,
        base_prefix: &matrix_prefix,
        vfs,
        proxy: &proxy,
    };
    for (index, window) in windows.into_iter().enumerate() {
        run_session_fault_case(&context, window, index);
    }
}

#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug)]
enum SessionCrashWindow {
    BeforeAcceptedHead,
    AfterAcceptedHead,
    ManifestPut,
    FinalHeadCas,
}

#[cfg(feature = "remote")]
impl SessionCrashWindow {
    fn label(self) -> &'static str {
        match self {
            Self::BeforeAcceptedHead => "crash-before-accepted-head",
            Self::AfterAcceptedHead => "crash-after-accepted-head",
            Self::ManifestPut => "crash-manifest-put",
            Self::FinalHeadCas => "crash-final-head-cas",
        }
    }

    fn target(self) -> StorageFaultTarget {
        match self {
            Self::BeforeAcceptedHead => StorageFaultTarget::XSyncCheckpoint,
            Self::AfterAcceptedHead => StorageFaultTarget::SessionHeadCas,
            Self::ManifestPut => StorageFaultTarget::ManifestPut,
            Self::FinalHeadCas => StorageFaultTarget::FinalHeadCas,
        }
    }
}

#[cfg(feature = "remote")]
fn session_object_exists(backend: &str, endpoint: &str, bucket: &str, object: &str) -> bool {
    let listing = list_session_objects(backend, endpoint, bucket, object);
    match backend {
        "google" => {
            listing.contains(&format!(r#""name":"{object}""#))
                || listing.contains(&format!(r#""name": "{object}""#))
        }
        "s3" => listing.contains(&format!("<Key>{object}</Key>")),
        _ => unreachable!("unknown session backend {backend}"),
    }
}

#[cfg(feature = "remote")]
fn session_listing_contains_object_prefix(backend: &str, listing: &str, prefix: &str) -> bool {
    match backend {
        "google" => first_google_object_name(listing, prefix).is_some(),
        "s3" => listing.contains(&format!("<Key>{prefix}")),
        _ => unreachable!("unknown session backend {backend}"),
    }
}

#[cfg(feature = "remote")]
fn session_head_state(
    backend: &str,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    session_id: &str,
) -> Option<u8> {
    let object = format!("{prefix}/bcv-session/v1/head/{session_id}.bcv");
    if !session_object_exists(backend, endpoint, bucket, &object) {
        return None;
    }
    fetch_session_object(backend, endpoint, bucket, &object)
        .get(11)
        .copied()
}

#[cfg(feature = "remote")]
struct CrashChildInputs {
    backend: String,
    endpoint: String,
    bucket: String,
    prefix: String,
    alias: String,
    cache: PathBuf,
    ready: PathBuf,
    go: PathBuf,
}

#[cfg(feature = "remote")]
fn crash_child_inputs() -> Option<CrashChildInputs> {
    Some(CrashChildInputs {
        backend: std::env::var("BCV_CRASH_BACKEND").ok()?,
        endpoint: std::env::var("BCV_CRASH_ENDPOINT").ok()?,
        bucket: std::env::var("BCV_CRASH_BUCKET").ok()?,
        prefix: std::env::var("BCV_CRASH_PREFIX").ok()?,
        alias: std::env::var("BCV_CRASH_ALIAS").ok()?,
        cache: PathBuf::from(std::env::var_os("BCV_CRASH_CACHE")?),
        ready: PathBuf::from(std::env::var_os("BCV_CRASH_READY")?),
        go: PathBuf::from(std::env::var_os("BCV_CRASH_GO")?),
    })
}

#[cfg(feature = "remote")]
fn run_session_crash_child() {
    let Some(CrashChildInputs {
        backend,
        endpoint,
        bucket,
        prefix,
        alias,
        cache,
        ready,
        go,
    }) = crash_child_inputs()
    else {
        return;
    };
    let session_id = std::env::var("BCV_CRASH_SESSION_ID").expect("crash session ID");
    let operation = std::env::var("BCV_CRASH_OPERATION")
        .expect("crash operation")
        .parse::<u8>()
        .expect("crash operation number");
    let window = match std::env::var("BCV_CRASH_WINDOW")
        .expect("crash window")
        .as_str()
    {
        "before-accepted-head" => SessionCrashWindow::BeforeAcceptedHead,
        "after-accepted-head" => SessionCrashWindow::AfterAcceptedHead,
        "manifest-put" => SessionCrashWindow::ManifestPut,
        "final-head-cas" => SessionCrashWindow::FinalHeadCas,
        value => panic!("unknown crash window {value}"),
    };
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let vfs_name = format!("rusqdoltlite-crash-child-{}", std::process::id());
    let vfs = BlockCacheVfs::builder(&cache)
        .expect("crash child VFS builder")
        .name(&vfs_name)
        .expect("crash child VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .config(Config::RequestCount(1))
        .config(Config::StageWatermark(50))
        .config(Config::HttpTimeout(5))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize crash child VFS");
    let mut server = start_http_session_server_with_storage(
        vfs,
        storage,
        &alias,
        &session_id,
        session_operation_id(operation),
    );
    assert_eq!(
        server
            .operation_status()
            .expect("crash child operation status"),
        SessionOperationStatus::New,
        "the crash child must invoke a new operation"
    );
    fs::write(&ready, b"ready").expect("write crash child ready marker");
    wait_for_race_file(&go);

    match window {
        SessionCrashWindow::BeforeAcceptedHead => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open pre-head crash database");
            stage_session_small_update(&database, "FULL")
                .expect("stage pre-head crash transaction");
            database
                .execute_batch("COMMIT")
                .expect("commit pre-head crash transaction");
            drop(database);
        }
        SessionCrashWindow::AfterAcceptedHead => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open post-head crash database");
            stage_session_small_update(&database, "OFF")
                .expect("stage post-head crash transaction");
            database
                .execute_batch("COMMIT")
                .expect("commit post-head crash transaction");
            drop(database);
            server
                .complete_request()
                .expect("accept post-head crash operation");
        }
        SessionCrashWindow::ManifestPut | SessionCrashWindow::FinalHeadCas => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open publication crash database");
            stage_session_small_update(&database, "OFF")
                .expect("stage publication crash transaction");
            database
                .execute_batch("COMMIT")
                .expect("commit publication crash transaction");
            drop(database);
            server
                .upload()
                .expect("publish publication crash operation");
        }
    }
    panic!("crash child reached the end without being killed: {window:?}");
}

#[cfg(feature = "remote")]
fn run_session_crash_recovery_child() {
    let Some(CrashChildInputs {
        backend,
        endpoint,
        bucket,
        prefix,
        alias,
        cache,
        ready: _ready,
        go: _go,
    }) = crash_child_inputs()
    else {
        return;
    };
    let session_id = std::env::var("BCV_CRASH_SESSION_ID").expect("recovery session ID");
    let operation = std::env::var("BCV_CRASH_OPERATION")
        .expect("recovery operation")
        .parse::<u8>()
        .expect("recovery operation number");
    let window = std::env::var("BCV_CRASH_WINDOW").expect("recovery crash window");
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let vfs_name = format!("rusqdoltlite-crash-recovery-{}", std::process::id());
    let vfs = BlockCacheVfs::builder(&cache)
        .expect("recovery VFS builder")
        .name(&vfs_name)
        .expect("recovery VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .config(Config::RequestCount(1))
        .config(Config::StageWatermark(50))
        .config(Config::HttpTimeout(5))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize recovery VFS");
    let mut server = start_http_session_server_with_storage(
        vfs,
        storage,
        &alias,
        &session_id,
        session_operation_id(operation),
    );
    let status = server
        .operation_status()
        .expect("inspect recovered operation status");
    match window.as_str() {
        "before-accepted-head" => {
            assert_eq!(status, SessionOperationStatus::New);
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open pre-head recovery database");
            stage_session_small_update(&database, "OFF")
                .expect("stage pre-head recovery transaction");
            database
                .execute_batch("COMMIT")
                .expect("commit pre-head recovery transaction");
            drop(database);
            server.upload().expect("publish pre-head recovery");
        }
        "after-accepted-head" | "manifest-put" => {
            assert_eq!(status, SessionOperationStatus::Accepted);
            server.upload().expect("publish accepted recovery");
        }
        "final-head-cas" => {
            assert_eq!(status, SessionOperationStatus::Committed);
            server
                .quiesce()
                .expect("quiesce committed recovery without replay");
        }
        value => panic!("unknown recovery crash window {value}"),
    }
}

#[cfg(feature = "remote")]
fn run_session_crash_case(backend: &str, endpoint: &str, bucket: &str, window: SessionCrashWindow) {
    let suffix = unique_suffix();
    let prefix = format!("{suffix}/crash/{}", window.label());
    let direct_storage = session_storage(backend, endpoint, bucket, &prefix);
    let parent_vfs_name = format!("rusqdoltlite-crash-parent-{suffix}");
    let parent_vfs = BlockCacheVfs::builder(shared_cache())
        .expect("crash parent VFS builder")
        .name(&parent_vfs_name)
        .expect("crash parent VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .config(Config::RequestCount(1))
        .config(Config::StageWatermark(50))
        .config(Config::HttpTimeout(5))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize crash parent VFS");
    parent_vfs
        .initialize_container(&direct_storage)
        .unwrap_or_else(|error| panic!("initialize {} storage: {error:?}", window.label()));
    let seed_dir = tempfile::tempdir().expect("crash seed directory");
    let seed_path = seed_dir.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create crash seed database");
    seed.execute_batch(
        "CREATE TABLE fault_data(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO fault_data VALUES (1, 'seed');",
    )
    .expect("seed crash database");
    seed.close().expect("close crash seed database");
    parent_vfs
        .create_database(&direct_storage, &seed_path, "session.sqlite")
        .unwrap_or_else(|error| panic!("upload {} seed database: {error:?}", window.label()));
    let baseline =
        fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"));

    let proxy = StorageFaultProxy::start(endpoint);
    let root = tempfile::tempdir().expect("crash process directory");
    let cache = root.path().join("crash-cache");
    fs::create_dir(&cache).expect("create crash child cache directory");
    let ready = root.path().join("ready");
    let go = root.path().join("go");
    let alias = format!("crash_{}", window.label().replace('-', "_"));
    let session_id = format!("550e8400-e29b-41d4-a716-{:012x}", 0x100 + suffix.len());
    let operation_value = 0x40_u8.wrapping_add(suffix.len() as u8);
    let executable = std::env::current_exe().expect("locate crash test executable");
    let child = Command::new(&executable)
        .args(["--ignored", "--exact", "session_crash_child", "--nocapture"])
        .env("BCV_CRASH_BACKEND", backend)
        .env("BCV_CRASH_ENDPOINT", &proxy.url)
        .env("BCV_CRASH_BUCKET", bucket)
        .env("BCV_CRASH_PREFIX", &prefix)
        .env("BCV_CRASH_ALIAS", &alias)
        .env("BCV_CRASH_SESSION_ID", &session_id)
        .env("BCV_CRASH_OPERATION", operation_value.to_string())
        .env(
            "BCV_CRASH_WINDOW",
            window.label().strip_prefix("crash-").unwrap(),
        )
        .env("BCV_CRASH_CACHE", &cache)
        .env("BCV_CRASH_READY", &ready)
        .env("BCV_CRASH_GO", &go)
        .spawn()
        .unwrap_or_else(|error| panic!("spawn {} crash child: {error}", window.label()));
    wait_for_race_file(&ready);
    proxy.arm_kill_process(window.target(), child.id());
    fs::write(&go, b"go").expect("release crash child");
    let status = child.wait_with_output().expect("wait for crash child");
    assert!(
        !status.status.success(),
        "{} child unexpectedly completed: stdout={} stderr={}",
        window.label(),
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr)
    );
    #[cfg(unix)]
    assert_eq!(
        status.status.signal(),
        Some(9),
        "{} child must terminate with SIGKILL, not merely fail: stdout={} stderr={}",
        window.label(),
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr)
    );
    assert_eq!(
        proxy.faulted(),
        1,
        "{} fault was not triggered",
        window.label()
    );

    let after_crash =
        fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"));
    let head_state = session_head_state(backend, endpoint, bucket, &prefix, &session_id);
    match window {
        SessionCrashWindow::BeforeAcceptedHead => {
            assert_eq!(after_crash, baseline, "checkpoint crash must not publish");
            assert_eq!(head_state, None, "checkpoint crash must not create HEAD");
        }
        SessionCrashWindow::AfterAcceptedHead => {
            assert_eq!(
                after_crash, baseline,
                "accepted-head crash must not publish"
            );
            assert_ne!(head_state, Some(4), "accepted-head crash must not commit");
        }
        SessionCrashWindow::ManifestPut => {
            assert_ne!(after_crash, baseline, "manifest PUT must be durable");
            assert_ne!(
                head_state,
                Some(4),
                "manifest crash must precede final HEAD"
            );
        }
        SessionCrashWindow::FinalHeadCas => {
            assert_ne!(
                after_crash, baseline,
                "final HEAD crash must retain publication"
            );
            assert_eq!(
                head_state,
                Some(4),
                "final HEAD crash must leave COMMITTED state"
            );
        }
    }

    // The recovery invocation must use the same scoped storage endpoint as
    // the killed invocation. The proxy is now transparent, so the provider
    // identity used to derive the session scope remains unchanged without
    // allowing a second fault.
    proxy.disarm();

    let recovery_cache = root.path().join("recovery-cache");
    fs::create_dir(&recovery_cache).expect("create recovery cache directory");
    let recovery_status = Command::new(&executable)
        .args([
            "--ignored",
            "--exact",
            "session_crash_recovery_child",
            "--nocapture",
        ])
        .env("BCV_CRASH_BACKEND", backend)
        .env("BCV_CRASH_ENDPOINT", &proxy.url)
        .env("BCV_CRASH_BUCKET", bucket)
        .env("BCV_CRASH_PREFIX", &prefix)
        .env("BCV_CRASH_ALIAS", &alias)
        .env("BCV_CRASH_SESSION_ID", &session_id)
        .env("BCV_CRASH_OPERATION", operation_value.to_string())
        .env(
            "BCV_CRASH_WINDOW",
            window.label().strip_prefix("crash-").unwrap(),
        )
        .env("BCV_CRASH_CACHE", &recovery_cache)
        .env("BCV_CRASH_READY", root.path().join("recovery-ready"))
        .env("BCV_CRASH_GO", root.path().join("recovery-go"))
        .status()
        .unwrap_or_else(|error| panic!("spawn {} recovery child: {error}", window.label()));
    assert!(
        recovery_status.success(),
        "{} recovery child failed: {recovery_status}",
        window.label()
    );
    let after_recovery =
        fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"));
    assert_ne!(
        after_recovery,
        baseline,
        "{} recovery must publish the changed database",
        window.label()
    );
    assert_eq!(
        session_head_state(backend, endpoint, bucket, &prefix, &session_id),
        Some(4),
        "{} recovery must leave a committed session head",
        window.label()
    );
}

#[cfg(feature = "remote")]
fn run_session_crash_matrix(backend: &str) {
    let suffix = unique_suffix();
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
            .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
            .unwrap_or_else(|_| "http://127.0.0.1:19025".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown crash backend {backend}"),
    };
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rust-session-crash-{suffix}")
    };
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    } else {
        ensure_s3_bucket(&endpoint, &bucket);
    }
    for window in [
        SessionCrashWindow::BeforeAcceptedHead,
        SessionCrashWindow::AfterAcceptedHead,
        SessionCrashWindow::ManifestPut,
        SessionCrashWindow::FinalHeadCas,
    ] {
        run_session_crash_case(backend, &endpoint, &bucket, window);
    }
}

#[cfg(feature = "remote")]
#[derive(Debug)]
struct ConcurrentSessionResult {
    operation_status: Option<SessionOperationStatus>,
    completed: bool,
    start_error_code: Option<i32>,
    start_error: Option<String>,
}

#[cfg(feature = "remote")]
struct ConcurrentSessionRequest {
    vfs: &'static BlockCacheVfs,
    storage: Storage,
    alias: String,
    session_id: String,
    operation_id: SessionOperationId,
    write_value: String,
}

#[cfg(feature = "remote")]
struct ConcurrentSessionBarriers {
    start: Arc<Barrier>,
    complete: Arc<Barrier>,
}

#[cfg(feature = "remote")]
fn concurrent_failure_result(
    phase: &mut u8,
    start_barrier: &Barrier,
    complete_barrier: &Barrier,
    error_code: Option<i32>,
    error: String,
) -> ConcurrentSessionResult {
    if *phase < 1 {
        *phase = 1;
        start_barrier.wait();
    }
    if *phase < 2 {
        *phase = 2;
        complete_barrier.wait();
    }
    ConcurrentSessionResult {
        operation_status: None,
        completed: false,
        start_error_code: error_code,
        start_error: Some(error),
    }
}

#[cfg(feature = "remote")]
fn spawn_concurrent_session_request(
    request: ConcurrentSessionRequest,
    barriers: ConcurrentSessionBarriers,
) -> thread::JoinHandle<ConcurrentSessionResult> {
    thread::spawn(move || {
        let ConcurrentSessionRequest {
            vfs,
            storage,
            alias,
            session_id,
            operation_id,
            write_value,
        } = request;
        let ConcurrentSessionBarriers { start, complete } = barriers;
        let mut phase = 0_u8;
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let mut server = match try_start_http_session_server_with_storage(
                vfs,
                storage,
                &alias,
                &session_id,
                operation_id,
            ) {
                Ok(server) => server,
                Err(error) => {
                    // A same-session owner race may reject one attach before
                    // the handler runs. Keep both workers synchronized so that
                    // this expected loser cannot turn the test into a
                    // scheduling-dependent deadlock.
                    return concurrent_failure_result(
                        &mut phase,
                        &start,
                        &complete,
                        match &error {
                            rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code),
                            _ => None,
                        },
                        format!("{error:?}"),
                    );
                }
            };
            let database = match vfs.open(format!("/{alias}/session.sqlite")) {
                Ok(database) => database,
                Err(error) => {
                    let error_code = match &error {
                        rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code),
                        _ => None,
                    };
                    let _ = server.quiesce();
                    return concurrent_failure_result(
                        &mut phase,
                        &start,
                        &complete,
                        error_code,
                        format!("open concurrent session database: {error:?}"),
                    );
                }
            };
            if let Err(error) = database.execute(
                "UPDATE race_data SET value = ?1 WHERE id = 1",
                [&write_value],
            ) {
                let error_code = match &error {
                    rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code),
                    _ => None,
                };
                drop(database);
                let _ = server.quiesce();
                return concurrent_failure_result(
                    &mut phase,
                    &start,
                    &complete,
                    error_code,
                    format!("write concurrent session database: {error:?}"),
                );
            }
            drop(database);
            let operation_status = server
                .operation_status()
                .expect("inspect concurrent session operation status");
            phase = 1;
            start.wait();
            debug_assert_eq!(phase, 1);
            let _response_status = if should_invoke_mutating_handler(operation_status) {
                send_http_request_bytes(server.port(), "GET", "/session.sqlite/root", &[]).status
            } else {
                server
                    .quiesce()
                    .expect("quiesce rejected concurrent operation");
                409
            };
            phase = 2;
            complete.wait();
            let completed = if should_invoke_mutating_handler(operation_status) {
                server.complete_request().is_ok()
            } else {
                false
            };
            ConcurrentSessionResult {
                operation_status: Some(operation_status),
                completed,
                start_error_code: None,
                start_error: None,
            }
        }));
        match outcome {
            Ok(result) => result,
            Err(panic) => {
                // Any assertion or native error before a rendezvous must still
                // release the other worker. The phase is set before each wait,
                // so recovery never waits twice on a barrier already crossed.
                concurrent_failure_result(
                    &mut phase,
                    &start,
                    &complete,
                    None,
                    format!("worker panic: {panic:?}"),
                )
            }
        }
    })
}

#[cfg(feature = "remote")]
fn run_concurrent_session_requests(
    first: ConcurrentSessionRequest,
    second: ConcurrentSessionRequest,
) -> [ConcurrentSessionResult; 2] {
    let start_barrier = Arc::new(Barrier::new(2));
    let complete_barrier = Arc::new(Barrier::new(2));
    let first = spawn_concurrent_session_request(
        first,
        ConcurrentSessionBarriers {
            start: Arc::clone(&start_barrier),
            complete: Arc::clone(&complete_barrier),
        },
    );
    let second = spawn_concurrent_session_request(
        second,
        ConcurrentSessionBarriers {
            start: start_barrier,
            complete: complete_barrier,
        },
    );
    [
        first.join().expect("first concurrent session worker"),
        second.join().expect("second concurrent session worker"),
    ]
}

#[cfg(feature = "remote")]
fn run_session_concurrency_matrix() {
    let suffix = unique_suffix();
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    let prefix = format!("{suffix}/session-races");
    ensure_google_bucket(&endpoint, bucket);
    let storage = session_storage("google", &endpoint, bucket, &prefix);
    let vfs_name = format!("rusqdoltlite-race-{suffix}");
    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("race VFS builder")
        .name(&vfs_name)
        .expect("race VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize race VFS");
    vfs.initialize_container(&storage)
        .expect("initialize race storage");
    let seed_dir = tempfile::tempdir().expect("race seed directory");
    let seed_path = seed_dir.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create race seed database");
    seed.execute_batch(
        "CREATE TABLE race_data(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO race_data VALUES (1, 'seed');",
    )
    .expect("seed race database");
    seed.close().expect("close race seed database");
    vfs.create_database(&storage, &seed_path, "session.sqlite")
        .expect("upload race seed database");
    // These are distinct RemoteServer instances sharing this process-global
    // VFS/cache. This checks in-process server arbitration, not independent
    // serverless invocations. The separate-process companion below gives
    // each worker its own process and scratch cache root.
    let same_session = run_concurrent_session_requests(
        ConcurrentSessionRequest {
            vfs,
            storage: storage.clone(),
            alias: "race-one".into(),
            session_id: "550e8400-e29b-41d4-a716-446655440021".into(),
            operation_id: session_operation_id(1),
            write_value: "same-session-one".into(),
        },
        ConcurrentSessionRequest {
            vfs,
            storage: storage.clone(),
            alias: "race-one".into(),
            session_id: "550e8400-e29b-41d4-a716-446655440021".into(),
            operation_id: session_operation_id(2),
            write_value: "same-session-two".into(),
        },
    );
    let same_session_completions = same_session
        .iter()
        .filter(|result| result.completed)
        .count();
    assert_eq!(
        same_session_completions, 1,
        "same-session concurrent requests must have one accepted head: {same_session:?}"
    );
    assert!(
        same_session.iter().all(|result| {
            result.start_error.is_none()
                || matches!(
                    result.start_error_code,
                    Some(rusqlite::ffi::SQLITE_BUSY)
                        | Some(rusqlite::ffi::SQLITE_CONSTRAINT)
                        | Some(rusqlite::ffi::SQLITE_AUTH)
                )
        }),
        "same-session race returned an uncontrolled startup error: {same_session:?}"
    );
    assert!(
        same_session.iter().all(|result| {
            result.operation_status.is_none()
                || matches!(
                    result.operation_status,
                    Some(SessionOperationStatus::New) | Some(SessionOperationStatus::Conflict)
                )
        }),
        "same-session race returned an unexpected operation status: {same_session:?}"
    );

    // Prepare two distinct client sessions from the same immutable base in
    // separate processes. The native VFS cannot safely run two local writes in
    // one process (the second can report SQLITE_FULL before reaching CAS), so
    // the publication race is isolated to fresh server invocations below.
    run_publication_process_race(&endpoint, bucket, &prefix, vfs, &storage, &suffix);

    run_same_session_process_race(&endpoint, bucket, &prefix);
}

#[cfg(feature = "remote")]
fn wait_for_race_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for session race barrier {path:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(feature = "remote")]
fn run_same_session_process_child() {
    let Some(cache) = std::env::var_os("BCV_RACE_CACHE").map(PathBuf::from) else {
        return;
    };
    let endpoint = std::env::var("BCV_RACE_ENDPOINT").expect("same-session race endpoint");
    let bucket = std::env::var("BCV_RACE_BUCKET").expect("same-session race bucket");
    let prefix = std::env::var("BCV_RACE_PREFIX").expect("same-session race prefix");
    let barrier = PathBuf::from(
        std::env::var_os("BCV_RACE_BARRIER").expect("same-session race barrier directory"),
    );
    let result_path =
        PathBuf::from(std::env::var_os("BCV_RACE_RESULT").expect("same-session race result path"));
    let slot = std::env::var("BCV_RACE_SLOT").expect("same-session race slot");
    let other_slot = if slot == "one" { "two" } else { "one" };
    let session_id = std::env::var("BCV_RACE_SESSION_ID").expect("same-session race session ID");
    let alias = std::env::var("BCV_RACE_ALIAS").expect("same-session race alias");
    let operation = std::env::var("BCV_RACE_OPERATION")
        .expect("same-session race operation")
        .parse::<u8>()
        .expect("same-session race operation number");
    let value = std::env::var("BCV_RACE_VALUE").expect("same-session race value");
    let ready = barrier.join(format!("{slot}.ready"));
    let other_ready = barrier.join(format!("{other_slot}.ready"));
    let complete = barrier.join(format!("{slot}.complete"));
    let other_complete = barrier.join(format!("{other_slot}.complete"));
    let write_result = |contents: &str| {
        fs::write(&result_path, contents).expect("write same-session race result");
    };
    let vfs_name = format!("rusqdoltlite-race-child-{slot}");
    let storage = session_storage("google", &endpoint, &bucket, &prefix);
    let vfs = BlockCacheVfs::builder(&cache)
        .expect("same-session child VFS builder")
        .name(&vfs_name)
        .expect("same-session child VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize same-session child VFS");
    let mut server = match try_start_http_session_server_with_storage(
        vfs,
        storage,
        &alias,
        &session_id,
        session_operation_id(operation),
    ) {
        Ok(server) => server,
        Err(error) => {
            write_result(&format!(
                "start=0\nerror_code={}\nerror={error:?}\n",
                match error {
                    rusqlite::Error::SqliteFailure(code, _) => code.extended_code,
                    _ => -1,
                }
            ));
            fs::write(&ready, b"ready").expect("write failed-start ready marker");
            wait_for_race_file(&other_ready);
            fs::write(&complete, b"complete").expect("write failed-start complete marker");
            wait_for_race_file(&other_complete);
            return;
        }
    };
    let operation_status = server
        .operation_status()
        .expect("inspect same-session child status");
    let database = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("open same-session child database");
    database
        .execute("UPDATE race_data SET value = ?1 WHERE id = 1", [&value])
        .expect("write same-session child database");
    drop(database);
    fs::write(&ready, b"ready").expect("write same-session child ready marker");
    wait_for_race_file(&other_ready);
    let response_status = if operation_status == SessionOperationStatus::New {
        send_http_request_bytes(server.port(), "GET", "/session.sqlite/root", &[]).status
    } else {
        server
            .quiesce()
            .expect("quiesce non-new same-session child");
        409
    };
    fs::write(&complete, b"complete").expect("write same-session child complete marker");
    wait_for_race_file(&other_complete);
    let completion = if operation_status == SessionOperationStatus::New {
        server.complete_request()
    } else {
        Ok(())
    };
    let completion_code = completion.as_ref().err().and_then(|error| match error {
        rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code),
        _ => None,
    });
    write_result(&format!(
        "start=1\nnew={}\nresponse={}\ncomplete={}\ncomplete_code={}\n",
        u8::from(operation_status == SessionOperationStatus::New),
        response_status,
        u8::from(completion.is_ok()),
        completion_code.unwrap_or(0),
    ));
}

#[cfg(feature = "remote")]
fn run_same_session_process_race(endpoint: &str, bucket: &str, prefix: &str) {
    let root = tempfile::tempdir().expect("same-session race process directory");
    let barrier = root.path().join("barrier");
    fs::create_dir(&barrier).expect("create same-session race barrier");
    let executable = std::env::current_exe().expect("locate same-session race executable");
    let session_id = "550e8400-e29b-41d4-a716-446655440024";
    let alias = "race-process";
    let mut children = Vec::new();
    for (slot, operation, value) in [("one", 5_u8, "process-one"), ("two", 6_u8, "process-two")] {
        let cache = root.path().join(format!("cache-{slot}"));
        let result = root.path().join(format!("result-{slot}.txt"));
        fs::create_dir(&cache).expect("create same-session child cache directory");
        children.push(
            Command::new(&executable)
                .args([
                    "--ignored",
                    "--exact",
                    "same_session_process_race_child",
                    "--nocapture",
                ])
                .env("BCV_RACE_CACHE", &cache)
                .env("BCV_RACE_ENDPOINT", endpoint)
                .env("BCV_RACE_BUCKET", bucket)
                .env("BCV_RACE_PREFIX", prefix)
                .env("BCV_RACE_BARRIER", &barrier)
                .env("BCV_RACE_RESULT", &result)
                .env("BCV_RACE_SLOT", slot)
                .env("BCV_RACE_SESSION_ID", session_id)
                .env("BCV_RACE_ALIAS", alias)
                .env("BCV_RACE_OPERATION", operation.to_string())
                .env("BCV_RACE_VALUE", value)
                .spawn()
                .unwrap_or_else(|error| panic!("spawn same-session race child {slot}: {error}")),
        );
    }
    for child in children {
        let status = child
            .wait_with_output()
            .expect("wait for same-session race child");
        assert!(
            status.status.success(),
            "same-session race child failed: {}\nstdout={}\nstderr={}",
            status.status,
            String::from_utf8_lossy(&status.stdout),
            String::from_utf8_lossy(&status.stderr)
        );
    }
    let results = ["one", "two"].map(|slot| {
        let path = root.path().join(format!("result-{slot}.txt"));
        fs::read_to_string(path).expect("read same-session race result")
    });
    assert!(
        results
            .iter()
            .all(|result| result.contains("start=1\nnew=1\n")),
        "both independent processes must attach the same predecessor before CAS: {results:?}"
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| result.contains("complete=1"))
            .count(),
        1,
        "same-session independent processes must have one accepted head: {results:?}"
    );
    assert!(
        results.iter().any(|result| {
            result.contains("complete=0\ncomplete_code=5\n")
                || result.contains("complete=0\ncomplete_code=19\n")
                || result.contains("complete=0\ncomplete_code=23\n")
                || result.contains("complete=0\ncomplete_code=412\n")
        }),
        "same-session loser must report a fenced CAS conflict: {results:?}"
    );
}

#[cfg(feature = "remote")]
fn publication_child_vfs(cache: &Path, slot: &str) -> &'static BlockCacheVfs {
    let name = format!("rusqdoltlite-publication-child-{slot}");
    BlockCacheVfs::builder(cache)
        .expect("publication child VFS builder")
        .name(&name)
        .expect("publication child VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize publication child VFS")
}

#[cfg(feature = "remote")]
fn run_publication_prepare_child() {
    let Some(cache) = std::env::var_os("BCV_PUBLICATION_CACHE").map(PathBuf::from) else {
        return;
    };
    let endpoint = std::env::var("BCV_PUBLICATION_ENDPOINT").expect("publication endpoint");
    let bucket = std::env::var("BCV_PUBLICATION_BUCKET").expect("publication bucket");
    let prefix = std::env::var("BCV_PUBLICATION_PREFIX").expect("publication prefix");
    let result_path =
        PathBuf::from(std::env::var_os("BCV_PUBLICATION_RESULT").expect("publication result path"));
    let slot = std::env::var("BCV_PUBLICATION_SLOT").expect("publication slot");
    let alias = std::env::var("BCV_PUBLICATION_ALIAS").expect("publication alias");
    let session_id = std::env::var("BCV_PUBLICATION_SESSION_ID").expect("publication session ID");
    let operation = std::env::var("BCV_PUBLICATION_OPERATION")
        .expect("publication operation")
        .parse::<u8>()
        .expect("publication operation number");
    let value = std::env::var("BCV_PUBLICATION_VALUE").expect("publication value");
    let write_result = |contents: &str| {
        fs::write(&result_path, contents).expect("write publication result");
    };
    let storage = session_storage("google", &endpoint, &bucket, &prefix);
    let vfs = publication_child_vfs(&cache, &slot);
    let mut server = match try_start_http_session_server_with_storage(
        vfs,
        storage,
        &alias,
        &session_id,
        session_operation_id(operation),
    ) {
        Ok(server) => server,
        Err(error) => {
            write_result(&format!(
                "start=0\nerror_code={}\nerror={error:?}\n",
                match error {
                    rusqlite::Error::SqliteFailure(code, _) => code.extended_code,
                    _ => -1,
                }
            ));
            return;
        }
    };
    let operation_status = match server.operation_status() {
        Ok(status) => status,
        Err(error) => {
            let _ = server.quiesce();
            write_result(&format!("start=1\nstatus_error={error:?}\n"));
            return;
        }
    };
    if operation_status != SessionOperationStatus::New {
        let _ = server.quiesce();
        write_result(&format!("start=1\nnew=0\nstatus={operation_status:?}\n"));
        return;
    }
    let database = match vfs.open(format!("/{alias}/session.sqlite")) {
        Ok(database) => database,
        Err(error) => {
            let _ = server.quiesce();
            write_result(&format!("start=1\nnew=1\nopen_error={error:?}\n"));
            return;
        }
    };
    if let Err(error) = database.execute("UPDATE race_data SET value = ?1 WHERE id = 1", [&value]) {
        drop(database);
        let _ = server.quiesce();
        write_result(&format!("start=1\nnew=1\nwrite_error={error:?}\n"));
        return;
    }
    drop(database);
    let response_status =
        send_http_request_bytes(server.port(), "GET", "/session.sqlite/root", &[]).status;
    let completion = if response_status == 200 {
        server.complete_request()
    } else {
        let _ = server.quiesce();
        Err(rusqlite::Error::InvalidQuery)
    };
    let completion_code = completion.as_ref().err().and_then(|error| match error {
        rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code),
        _ => None,
    });
    write_result(&format!(
        "start=1\nnew=1\nresponse={response_status}\ncomplete={}\ncomplete_code={}\n",
        u8::from(completion.is_ok()),
        completion_code.unwrap_or(0),
    ));
}

#[cfg(feature = "remote")]
fn run_publication_upload_child() {
    let Some(cache) = std::env::var_os("BCV_PUBLICATION_CACHE").map(PathBuf::from) else {
        return;
    };
    let endpoint = std::env::var("BCV_PUBLICATION_ENDPOINT").expect("publication endpoint");
    let bucket = std::env::var("BCV_PUBLICATION_BUCKET").expect("publication bucket");
    let prefix = std::env::var("BCV_PUBLICATION_PREFIX").expect("publication prefix");
    let result_path =
        PathBuf::from(std::env::var_os("BCV_PUBLICATION_RESULT").expect("publication result path"));
    let slot = std::env::var("BCV_PUBLICATION_SLOT").expect("publication slot");
    let alias = std::env::var("BCV_PUBLICATION_ALIAS").expect("publication alias");
    let session_id = std::env::var("BCV_PUBLICATION_SESSION_ID").expect("publication session ID");
    let operation = std::env::var("BCV_PUBLICATION_OPERATION")
        .expect("publication operation")
        .parse::<u8>()
        .expect("publication operation number");
    let write_result = |contents: &str| {
        fs::write(&result_path, contents).expect("write publication upload result");
    };
    let storage = session_storage("google", &endpoint, &bucket, &prefix);
    let vfs = publication_child_vfs(&cache, &slot);
    let mut server = match try_start_http_session_server_with_storage(
        vfs,
        storage,
        &alias,
        &session_id,
        session_operation_id(operation),
    ) {
        Ok(server) => server,
        Err(error) => {
            write_result(&format!(
                "start=0\nerror_code={}\nerror={error:?}\n",
                match error {
                    rusqlite::Error::SqliteFailure(code, _) => code.extended_code,
                    _ => -1,
                }
            ));
            return;
        }
    };
    let operation_status = match server.operation_status() {
        Ok(status) => status,
        Err(error) => {
            let _ = server.quiesce();
            write_result(&format!("start=1\nstatus_error={error:?}\n"));
            return;
        }
    };
    let _ = server.quiesce();
    let (uploaded, upload_code, upload_error) = match operation_status {
        SessionOperationStatus::Accepted | SessionOperationStatus::Committed => {
            match server.upload() {
                Ok(()) => (true, 0, None),
                Err(error) => {
                    let code = match &error {
                        rusqlite::Error::SqliteFailure(code, _) => code.extended_code,
                        _ => -1,
                    };
                    (false, code, Some(format!("{error:?}")))
                }
            }
        }
        _ => (false, 0, None),
    };
    write_result(&format!(
        "start=1\nstatus={operation_status:?}\nupload={}\nupload_code={upload_code}\nerror={}\n",
        u8::from(uploaded),
        upload_error.unwrap_or_default(),
    ));
}

#[cfg(feature = "remote")]
fn run_publication_process_race(
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    vfs: &'static BlockCacheVfs,
    storage: &Storage,
    suffix: &str,
) {
    let root = tempfile::tempdir().expect("publication race process directory");
    let executable = std::env::current_exe().expect("locate publication race executable");
    let sessions = [
        (
            "one",
            "publication-one",
            "550e8400-e29b-41d4-a716-446655440022",
            3_u8,
            "session-two",
        ),
        (
            "two",
            "publication-two",
            "550e8400-e29b-41d4-a716-446655440023",
            4_u8,
            "session-three",
        ),
    ];
    let baseline_manifest =
        fetch_google_object(endpoint, bucket, &format!("{prefix}/manifest.bcv"));

    // Complete each private session before starting the publication race. This
    // isolates the required publication CAS from the unsupported same-process
    // local-cache write race while both sessions still share one base tip.
    for (slot, alias, session_id, operation, value) in sessions {
        let cache = root.path().join(format!("prepare-cache-{slot}"));
        let result = root.path().join(format!("prepare-result-{slot}.txt"));
        fs::create_dir(&cache).expect("create publication prepare cache");
        let status = Command::new(&executable)
            .args([
                "--ignored",
                "--exact",
                "independent_session_publication_prepare_child",
                "--nocapture",
            ])
            .env("BCV_PUBLICATION_CACHE", &cache)
            .env("BCV_PUBLICATION_ENDPOINT", endpoint)
            .env("BCV_PUBLICATION_BUCKET", bucket)
            .env("BCV_PUBLICATION_PREFIX", prefix)
            .env("BCV_PUBLICATION_RESULT", &result)
            .env("BCV_PUBLICATION_SLOT", slot)
            .env("BCV_PUBLICATION_ALIAS", alias)
            .env("BCV_PUBLICATION_SESSION_ID", session_id)
            .env("BCV_PUBLICATION_OPERATION", operation.to_string())
            .env("BCV_PUBLICATION_VALUE", value)
            .status()
            .unwrap_or_else(|error| panic!("spawn publication prepare child {slot}: {error}"));
        assert!(
            status.success(),
            "publication prepare child {slot} failed: {status}"
        );
        let contents = fs::read_to_string(&result).expect("read publication prepare result");
        assert!(
            contents.contains("start=1\nnew=1\nresponse=200\ncomplete=1\n"),
            "publication prepare child {slot} did not accept its private head: {contents}"
        );
    }

    let mut children = Vec::new();
    for (slot, alias, session_id, operation, _value) in sessions {
        let cache = root.path().join(format!("upload-cache-{slot}"));
        let result = root.path().join(format!("upload-result-{slot}.txt"));
        fs::create_dir(&cache).expect("create publication upload cache");
        children.push(
            Command::new(&executable)
                .args([
                    "--ignored",
                    "--exact",
                    "independent_session_publication_upload_child",
                    "--nocapture",
                ])
                .env("BCV_PUBLICATION_CACHE", &cache)
                .env("BCV_PUBLICATION_ENDPOINT", endpoint)
                .env("BCV_PUBLICATION_BUCKET", bucket)
                .env("BCV_PUBLICATION_PREFIX", prefix)
                .env("BCV_PUBLICATION_RESULT", &result)
                .env("BCV_PUBLICATION_SLOT", slot)
                .env("BCV_PUBLICATION_ALIAS", alias)
                .env("BCV_PUBLICATION_SESSION_ID", session_id)
                .env("BCV_PUBLICATION_OPERATION", operation.to_string())
                .spawn()
                .unwrap_or_else(|error| panic!("spawn publication upload child {slot}: {error}")),
        );
    }
    for child in children {
        let status = child
            .wait_with_output()
            .expect("wait for publication upload child");
        assert!(
            status.status.success(),
            "publication upload child failed: {}\nstdout={}\nstderr={}",
            status.status,
            String::from_utf8_lossy(&status.stdout),
            String::from_utf8_lossy(&status.stderr)
        );
    }
    let results = ["one", "two"].map(|slot| {
        let path = root.path().join(format!("upload-result-{slot}.txt"));
        fs::read_to_string(path).expect("read publication upload result")
    });
    assert_eq!(
        results
            .iter()
            .filter(|result| result.contains("upload=1"))
            .count(),
        1,
        "exactly one concurrent publication may win the manifest CAS: {results:?}"
    );
    assert!(
        results.iter().all(|result| {
            result.contains("upload=1")
                || result.contains("upload_code=5")
                || result.contains("upload_code=19")
                || result.contains("upload_code=23")
        }),
        "the losing publication must report a fenced conflict: {results:?}"
    );
    let published_alias = format!("race-published-{suffix}");
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&published_alias))
        .expect("attach publication-race winner");
    let published_db = vfs
        .open(format!("/{published_alias}/session.sqlite"))
        .expect("open publication-race winner");
    let published_value: String = published_db
        .query_row("SELECT value FROM race_data WHERE id = 1", [], |row| {
            row.get(0)
        })
        .expect("read publication-race winner");
    let expected_value = if results[0].contains("upload=1") {
        "session-two"
    } else {
        "session-three"
    };
    assert_eq!(
        published_value, expected_value,
        "published manifest must match the winning upload"
    );
    drop(published_db);
    vfs.detach(&published_alias)
        .expect("detach publication-race winner");
    let published_manifest =
        fetch_google_object(endpoint, bucket, &format!("{prefix}/manifest.bcv"));
    assert_ne!(
        baseline_manifest, published_manifest,
        "one concurrent publication must advance the manifest"
    );
}

#[cfg(feature = "remote")]
fn run_session_http_flow(backend: &str) {
    let executable = std::env::current_exe().expect("locate session HTTP flow test executable");
    let status = Command::new(executable)
        .args([
            "--ignored",
            "--exact",
            "session_http_flow_child",
            "--nocapture",
        ])
        .env("BCV_SESSION_HTTP_BACKEND", backend)
        .env(
            "DOLTLITE_HTTP_TIMEOUT_MS",
            SESSION_HTTP_TEST_TIMEOUT.as_millis().to_string(),
        )
        .status()
        .unwrap_or_else(|error| panic!("spawn {backend} session HTTP flow child: {error}"));
    assert!(
        status.success(),
        "{backend} session HTTP flow child failed: {status}"
    );
}

#[cfg(feature = "remote")]
fn run_session_http_flow_in_process(backend: &str) {
    let suffix = unique_suffix();
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
            .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
            .unwrap_or_else(|_| "http://127.0.0.1:19025".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown session backend {backend}"),
    };
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rust-session-http-{suffix}")
    };
    let prefix = format!("{suffix}/session-http");
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    } else {
        ensure_s3_bucket(&endpoint, &bucket);
    }
    let storage = session_storage(backend, &endpoint, &bucket, &prefix);

    const SESSION_CACHE_BYTES: i64 = 4 * 1024 * 1024;
    let cache_dir = tempfile::tempdir().expect("session HTTP cache directory");
    let vfs_name = format!("rusqdoltlite-session-{suffix}");
    let vfs = BlockCacheVfs::builder(cache_dir.path())
        .expect("VFS builder")
        .name(&vfs_name)
        .expect("session HTTP VFS name")
        .config(Config::CacheSize(SESSION_CACHE_BYTES))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize block-cache VFS");
    vfs.initialize_container(&storage)
        .expect("initialize remote CBS container");

    // The HTTP server exposes a DoltLite repository, so seed the object
    // store from a real DoltLite commit rather than a plain SQLite fixture.
    let seed_dir = tempfile::tempdir().expect("seed database directory");
    let seed_path = seed_dir.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create DoltLite seed database");
    seed.execute_batch(
        "CREATE TABLE session_http(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO session_http VALUES (1, 'seed');",
    )
    .expect("seed DoltLite database");
    let _: String = seed
        .query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })
        .expect("commit DoltLite seed database");
    seed.close().expect("close DoltLite seed database");
    vfs.create_database(&storage, &seed_path, "session.sqlite")
        .expect("upload DoltLite seed database");

    let baseline_manifest = fetch_session_object(
        backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    let alias = "session-http-alias";
    let session_id = "550e8400-e29b-41d4-a716-446655440015";

    // A failed application handler is quiesced without checkpointing or
    // accepting its operation. A fresh invocation with the same id therefore
    // remains New and is allowed to retry the handler.
    let mut failed = start_http_session_server_with_storage(
        vfs,
        storage.clone(),
        alias,
        session_id,
        session_operation_id(9),
    );
    let mut failed_adapter = ReferenceResponseAdapter::new(&mut failed);
    failed_adapter.buffer(b"failed response");
    assert_eq!(
        failed_adapter
            .release_after_completion("/chunks", 500)
            .expect("return failed application response"),
        b"failed response"
    );
    drop(failed);
    let failed_retry = start_http_session_server_with_storage(
        vfs,
        storage.clone(),
        alias,
        session_id,
        session_operation_id(9),
    );
    assert_eq!(
        failed_retry
            .operation_status()
            .expect("failed operation status"),
        SessionOperationStatus::New
    );
    drop(failed_retry);

    // The stable test proxy creates a fresh RemoteServer for every incoming
    // HTTP request, with the same session ID and a new deterministic test
    // operation ID. The mixer is only a stand-in for a collision-resistant
    // app-side request identity; raw requests below cover the protocol read
    // routes, while the DoltLite clone and push drive the same proxy path
    // naturally.
    let proxy = SessionHttpProxy::start(vfs, storage.clone(), "session.sqlite", alias, session_id);
    let missing_token = send_http_request_bytes(proxy.port, "GET", "/session.sqlite/root", &[]);
    assert_eq!(
        missing_token.status, 404,
        "the application proxy must reject a URL without a session token"
    );
    let wrong_token =
        send_http_request_bytes(proxy.port, "GET", "/s/wrong-token/session.sqlite/root", &[]);
    assert_eq!(
        wrong_token.status, 404,
        "the application proxy must reject a URL with a foreign session token"
    );
    let out_of_scope = send_http_request_bytes(
        proxy.port,
        "GET",
        &proxy.request_path("other.sqlite/root"),
        &[],
    );
    assert_eq!(
        out_of_scope.status, 404,
        "the application proxy must reject a database outside its session scope"
    );
    let root = send_http_request_bytes(
        proxy.port,
        "GET",
        &proxy.request_path("session.sqlite/root"),
        &[],
    );
    assert_eq!(root.status, 200, "GET /root must succeed");
    assert_eq!(
        root.body.len(),
        20,
        "GET /root returns one 20-byte DoltLite prolly hash"
    );

    let refs = send_http_request_bytes(
        proxy.port,
        "GET",
        &proxy.request_path("session.sqlite/refs"),
        &[],
    );
    assert_eq!(refs.status, 200, "GET /refs must succeed");
    assert!(
        !refs.body.is_empty(),
        "GET /refs returns the seed refs blob"
    );

    let has_chunks = send_http_request_bytes(
        proxy.port,
        "POST",
        &proxy.request_path("session.sqlite/has-chunks"),
        &root.body,
    );
    assert_eq!(has_chunks.status, 200, "POST /has-chunks must succeed");
    assert_eq!(
        has_chunks.body.as_slice(),
        &[1],
        "the seed root chunk must be present"
    );

    let get_chunks = send_http_request_bytes(
        proxy.port,
        "POST",
        &proxy.request_path("session.sqlite/get-chunks"),
        &root.body,
    );
    assert_eq!(get_chunks.status, 200, "POST /get-chunks must succeed");
    assert!(
        get_chunks.body.len() >= 4,
        "chunk response has a length prefix"
    );
    let root_chunk_len = u32::from_be_bytes(
        get_chunks.body[..4]
            .try_into()
            .expect("chunk response length prefix"),
    ) as usize;
    assert_ne!(root_chunk_len, u32::MAX as usize);
    assert_eq!(get_chunks.body.len(), 4 + root_chunk_len);

    let root_path = proxy.request_path(&format!("session.sqlite/chunk/{}", hex_bytes(&root.body)));
    let chunk = send_http_request_bytes(proxy.port, "GET", &root_path, &[]);
    assert_eq!(chunk.status, 200, "GET /chunk/<hash> must succeed");
    assert_eq!(chunk.body, get_chunks.body[4..]);

    let clone_dir = tempfile::tempdir().expect("clone database directory");
    let clone_path = clone_dir.path().join("clone.sqlite");
    let clone = Connection::open(&clone_path).expect("create clone database");
    let _: i64 = clone
        .query_row(
            "SELECT dolt_clone(?1)",
            params![proxy.database_url("session.sqlite")],
            |row| row.get(0),
        )
        .expect("clone through session HTTP server");
    let seed_value: String = clone
        .query_row("SELECT value FROM session_http WHERE id = 1", [], |row| {
            row.get(0)
        })
        .expect("read cloned seed row");
    assert_eq!(seed_value, "seed");
    drop(clone);

    // Every proxy read was checkpointed and accepted by its own fresh server,
    // but none selected the application's final /commit publication route.
    let after_read_manifest = fetch_session_object(
        backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(
        baseline_manifest, after_read_manifest,
        "accepted read request must not publish the ordinary manifest"
    );

    // A real DoltLite push now sends /has-chunks, /chunks, /refs-if, and
    // /commit through fresh session-server instances. The proxy's
    // application adapter publishes only after the successful /commit.
    let writer = Connection::open(&clone_path).expect("reopen clone database");
    let _: i64 = writer
        .query_row(
            "SELECT dolt_remote('set-url', 'origin', ?1)",
            params![proxy.database_url("session.sqlite")],
            |row| row.get(0),
        )
        .expect("point clone at fresh session server");
    writer
        .execute("UPDATE session_http SET value = 'updated' WHERE id = 1", [])
        .expect("update cloned row");
    writer
        .execute_batch("BEGIN IMMEDIATE;")
        .expect("begin large session update");
    {
        let mut insert = writer
            .prepare("INSERT INTO session_http(id, value) VALUES (?1, ?2)")
            .expect("prepare large session update");
        for id in 2..=5_500_i64 {
            let body = deterministic_payload(id, 2_048);
            insert
                .execute(params![id, &body])
                .expect("insert large session update row");
        }
    }
    writer
        .execute_batch("COMMIT;")
        .expect("commit large session update");
    let _: String = writer
        .query_row("SELECT dolt_commit('-A', '-m', 'update')", [], |row| {
            row.get(0)
        })
        .expect("commit cloned update");
    let push_result: rusqlite::Result<i64> =
        writer.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0));
    if let Err(error) = push_result {
        panic!(
            "push cloned update through session HTTP server: {error:?}; proxy failure: {:?}; last native response error: {:?}; requests: {:?}",
            proxy.failure(),
            proxy.last_native_error(),
            proxy.request_log()
        );
    }
    drop(writer);
    let local_database_bytes = fs::metadata(&clone_path)
        .expect("inspect physical cloned database size")
        .len();
    assert!(
        local_database_bytes > SESSION_CACHE_BYTES as u64,
        "physical cloned database must exceed the dedicated cache capacity: {local_database_bytes}"
    );
    let cache_file_size = fs::metadata(cache_dir.path().join("cachefile.bcv"))
        .expect("inspect session HTTP cache file")
        .len();
    assert!(
        cache_file_size <= SESSION_CACHE_BYTES as u64,
        "session HTTP cache file exceeded its configured capacity: {cache_file_size}"
    );
    let published_manifest = fetch_session_object(
        backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    let checkpoint_prefix = format!("{prefix}/bcv-session/v1/checkpoint/{session_id}/");
    let checkpoint_listing = list_session_objects(backend, &endpoint, &bucket, &checkpoint_prefix);
    let head_name = format!("{prefix}/bcv-session/v1/head/{session_id}.bcv");
    let head_record = fetch_session_object(backend, &endpoint, &bucket, &head_name);
    const SESSION_RECORD_HEADER_BYTES: usize = 168;
    const SESSION_RECORD_TRAILER_BYTES: usize = 32;
    assert!(
        head_record.len() >= SESSION_RECORD_HEADER_BYTES + SESSION_RECORD_TRAILER_BYTES,
        "committed session head record is truncated"
    );
    assert_eq!(
        head_record[10], 2,
        "session head object must contain a HEAD record"
    );
    assert_eq!(
        head_record[11], 4,
        "session head must be durably committed before selecting its checkpoint"
    );
    let head_body_length = be_u32(&head_record, 160) as usize;
    assert_eq!(
        head_record.len(),
        SESSION_RECORD_HEADER_BYTES + head_body_length + SESSION_RECORD_TRAILER_BYTES,
        "committed session head record length is inconsistent"
    );
    let head_body =
        &head_record[SESSION_RECORD_HEADER_BYTES..SESSION_RECORD_HEADER_BYTES + head_body_length];
    assert!(
        head_body.starts_with(b"BCVHD02\0"),
        "committed session head must use the version 2 body format"
    );
    assert!(
        head_body.len() >= 116,
        "committed session head body is truncated"
    );
    assert_eq!(
        be_u32(head_body, 8),
        0,
        "version 2 session head reserved field must be zero"
    );
    let history_count = be_u32(head_body, 108) as usize;
    let history_bytes = history_count
        .checked_mul(SESSION_RECORD_TRAILER_BYTES)
        .expect("session head history length overflow");
    let etag_start = 116usize
        .checked_add(history_bytes)
        .expect("session head ETag offset overflow");
    assert!(
        etag_start <= head_body.len(),
        "version 2 session head history exceeds its body"
    );
    let etag_length = be_u32(head_body, 112) as usize;
    assert_eq!(
        head_body.len(),
        etag_start + etag_length,
        "version 2 session head body length is inconsistent"
    );
    for history_index in 0..history_count {
        let history_start = 116 + history_index * SESSION_RECORD_TRAILER_BYTES;
        let operation_id = &head_body[history_start..history_start + SESSION_RECORD_TRAILER_BYTES];
        assert!(
            operation_id.iter().any(|byte| *byte != 0),
            "version 2 session head history contains an empty operation ID"
        );
    }
    let checkpoint_hash = &head_body[12..44];
    assert!(
        checkpoint_hash.iter().any(|byte| *byte != 0),
        "committed session head must reference a checkpoint"
    );
    let candidate_hash = &head_body[44..76];
    assert!(
        candidate_hash.iter().any(|byte| *byte != 0),
        "committed session head must reference a candidate manifest"
    );
    let checkpoint_name = format!("{checkpoint_prefix}{}.bcv", hex_bytes(checkpoint_hash));
    assert!(
        checkpoint_listing.contains(&checkpoint_name),
        "committed head checkpoint is absent from the immutable checkpoint listing: {checkpoint_name}"
    );
    assert_ne!(
        baseline_manifest, published_manifest,
        "the explicit final upload must publish the changed manifest"
    );
    let checkpoint_record = fetch_session_object(backend, &endpoint, &bucket, &checkpoint_name);
    let checkpoint_database = reassemble_checkpoint_database(
        backend,
        &endpoint,
        &bucket,
        &prefix,
        &checkpoint_record,
        "session.sqlite",
    );
    assert!(
        checkpoint_database.len() > SESSION_CACHE_BYTES as usize,
        "published checkpoint block payload must exceed the dedicated cache capacity: {}",
        checkpoint_database.len()
    );

    // Replaying the exact application request after a lost response must use
    // the same opaque operation ID. The proxy observes Committed and returns
    // success without invoking /commit or writing another manifest.
    let duplicate_commit = send_http_request_raw(proxy.port, &proxy.last_commit_request());
    assert_eq!(
        http_status(&duplicate_commit),
        200,
        "an exact /commit retry must reconcile as success"
    );
    let after_duplicate_commit = fetch_session_object(
        backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(
        published_manifest, after_duplicate_commit,
        "an exact /commit retry must not publish a second manifest"
    );

    // A fresh independent client uses a new session scope. A committed
    // session is terminal and must not accept post-publication read requests
    // under the original session ID.
    drop(proxy);
    let final_session_id = "550e8400-e29b-41d4-a716-446655440017";
    let final_alias = "session-http-final-alias";
    let final_proxy = SessionHttpProxy::start(
        vfs,
        storage.clone(),
        "session.sqlite",
        final_alias,
        final_session_id,
    );
    let final_dir = tempfile::tempdir().expect("final clone database directory");
    let final_path = final_dir.path().join("final.sqlite");
    let final_clone = Connection::open(&final_path).expect("create final clone database");
    let _: i64 = final_clone
        .query_row(
            "SELECT dolt_clone(?1)",
            params![final_proxy.database_url("session.sqlite")],
            |row| row.get(0),
        )
        .expect("read published update through fresh session server");
    let final_value: String = final_clone
        .query_row("SELECT value FROM session_http WHERE id = 1", [], |row| {
            row.get(0)
        })
        .expect("read published row");
    assert_eq!(final_value, "updated");
    let final_count: i64 = final_clone
        .query_row("SELECT count(*) FROM session_http", [], |row| row.get(0))
        .expect("count published rows");
    assert_eq!(final_count, 5_500);
    drop(final_clone);
    let after_final_read_manifest = fetch_session_object(
        backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(published_manifest, after_final_read_manifest);

    // The old operation ID is now stale and must not be replayed against the
    // newer accepted/committed head.
    drop(final_proxy);
    let stale = try_start_http_session_server_with_storage(
        vfs,
        storage,
        alias,
        session_id,
        session_operation_id(1),
    );
    match stale {
        Ok(mut stale) => {
            let stale_status = stale.operation_status().expect("stale operation status");
            assert_eq!(stale_status, SessionOperationStatus::Conflict);
            assert!(!should_invoke_mutating_handler(stale_status));
            stale.quiesce().expect("quiesce stale operation");
        }
        Err(rusqlite::Error::SqliteFailure(error, _)) => {
            assert!(
                matches!(
                    error.extended_code,
                    rusqlite::ffi::SQLITE_CONSTRAINT | rusqlite::ffi::SQLITE_AUTH | 412
                ),
                "stale operation returned an unexpected code: {error:?}"
            );
        }
        Err(error) => panic!("stale operation returned an unexpected error: {error:?}"),
    }
}

#[cfg(feature = "remote")]
const GENERIC_SESSION_ID: &str = "550e8400-e29b-41d4-a716-446655440016";

#[cfg(feature = "remote")]
const GENERIC_SESSION_ALIAS: &str = "session-generic-alias";

#[cfg(feature = "remote")]
const GENERIC_READER_SESSION_ID: &str = "550e8400-e29b-41d4-a716-446655440017";

#[cfg(feature = "remote")]
const GENERIC_READER_SESSION_ALIAS: &str = "session-generic-reader-alias";

#[cfg(feature = "remote")]
const GENERIC_SESSION_OPERATION: u8 = 7;

#[cfg(feature = "remote")]
fn generic_phase_inputs() -> Option<(String, PathBuf, String, String, String, PathBuf)> {
    Some((
        std::env::var("BCV_GENERIC_BACKEND").ok()?,
        PathBuf::from(std::env::var_os("BCV_GENERIC_CACHE")?),
        std::env::var("BCV_GENERIC_ENDPOINT").ok()?,
        std::env::var("BCV_GENERIC_BUCKET").ok()?,
        std::env::var("BCV_GENERIC_PREFIX").ok()?,
        PathBuf::from(std::env::var_os("BCV_GENERIC_STATE")?),
    ))
}

#[cfg(feature = "remote")]
fn generic_vfs(cache: &Path) -> &'static BlockCacheVfs {
    BlockCacheVfs::builder(cache)
        .expect("generic VFS builder")
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize generic block-cache VFS")
}

#[cfg(feature = "remote")]
fn run_generic_phase_one() {
    let Some((backend, cache, endpoint, bucket, prefix, state)) = generic_phase_inputs() else {
        return;
    };
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let vfs = generic_vfs(&cache);
    vfs.initialize_container(&storage)
        .expect("initialize generic session container");
    let seed_dir = tempfile::tempdir().expect("generic seed directory");
    let seed_path = seed_dir.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create generic seed database");
    seed.execute_batch(
        "CREATE TABLE generic_session(value TEXT NOT NULL);
         INSERT INTO generic_session VALUES ('seed');",
    )
    .expect("seed generic database");
    seed.close().expect("close generic seed database");
    vfs.create_database(&storage, &seed_path, "session.sqlite")
        .expect("upload generic seed database");
    let before = fetch_session_object(
        &backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    let checkpoint_prefix = format!("{prefix}/bcv-session/v1/checkpoint/{GENERIC_SESSION_ID}/");

    let mut server = start_http_session_server_with_storage(
        vfs,
        storage.clone(),
        GENERIC_SESSION_ALIAS,
        GENERIC_SESSION_ID,
        session_operation_id(GENERIC_SESSION_OPERATION),
    );
    let database = vfs
        .open(format!("/{GENERIC_SESSION_ALIAS}/session.sqlite"))
        .expect("open generic session database");
    database
        .execute_batch(
            "PRAGMA synchronous=OFF;
             INSERT INTO generic_session VALUES ('no-xsync');",
        )
        .expect("write generic session without xSync");
    drop(database);
    let checkpoints_before_close_out =
        list_session_objects(&backend, &endpoint, &bucket, &checkpoint_prefix);
    assert!(
        !session_listing_contains_object_prefix(
            &backend,
            &checkpoints_before_close_out,
            &checkpoint_prefix,
        ),
        "synchronous=OFF write and connection close must not create an xSync checkpoint: {checkpoints_before_close_out}"
    );
    server
        .complete_request()
        .expect("checkpoint and accept generic request");
    assert_eq!(
        server
            .operation_status()
            .expect("generic accepted request status"),
        SessionOperationStatus::Accepted
    );
    let checkpoints_after_close_out =
        list_session_objects(&backend, &endpoint, &bucket, &checkpoint_prefix);
    assert!(
        session_listing_contains_object_prefix(
            &backend,
            &checkpoints_after_close_out,
            &checkpoint_prefix,
        ),
        "complete_request must create an accepted checkpoint object: {checkpoints_after_close_out}"
    );
    let after_checkpoint = fetch_session_object(
        &backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(before, after_checkpoint);
    fs::write(&state, before).expect("persist generic pre-publication manifest");
}

#[cfg(feature = "remote")]
fn run_generic_phase_two() {
    let Some((backend, cache, endpoint, bucket, prefix, state)) = generic_phase_inputs() else {
        return;
    };
    let before = fs::read(&state).expect("read generic pre-publication manifest");
    let vfs = generic_vfs(&cache);
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let mut server = start_http_session_server_with_storage(
        vfs,
        storage.clone(),
        GENERIC_SESSION_ALIAS,
        GENERIC_SESSION_ID,
        session_operation_id(GENERIC_SESSION_OPERATION.wrapping_add(1)),
    );
    assert_eq!(
        server.operation_status().expect("generic fresh status"),
        SessionOperationStatus::New
    );
    let database = vfs
        .open(format!("/{GENERIC_SESSION_ALIAS}/session.sqlite"))
        .expect("rehydrate generic session database");
    let count: i64 = database
        .query_row(
            "SELECT count(*) FROM generic_session WHERE value = 'no-xsync'",
            [],
            |row| row.get(0),
        )
        .expect("read generic no-xSync row");
    assert_eq!(count, 1);
    drop(database);
    let before_upload = fetch_session_object(
        &backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(before, before_upload);
    server
        .upload()
        .expect("publish generic final request directly");
    let published = fetch_session_object(
        &backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_ne!(before, published);
    fs::write(state.with_extension("published"), published)
        .expect("persist generic published manifest");
}

#[cfg(feature = "remote")]
fn run_generic_phase_three() {
    let Some((backend, cache, endpoint, bucket, prefix, state)) = generic_phase_inputs() else {
        return;
    };
    let expected =
        fs::read(state.with_extension("published")).expect("read generic published manifest");
    let actual = fetch_session_object(
        &backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(expected, actual);
    let vfs = generic_vfs(&cache);
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let mut server = start_http_session_server_with_storage(
        vfs,
        storage,
        GENERIC_READER_SESSION_ALIAS,
        GENERIC_READER_SESSION_ID,
        session_operation_id(GENERIC_SESSION_OPERATION.wrapping_add(2)),
    );
    assert_eq!(
        server.operation_status().expect("generic fresh status"),
        SessionOperationStatus::New
    );
    let database = vfs
        .open(format!("/{GENERIC_READER_SESSION_ALIAS}/session.sqlite"))
        .expect("open generic published database");
    let count: i64 = database
        .query_row(
            "SELECT count(*) FROM generic_session WHERE value = 'no-xsync'",
            [],
            |row| row.get(0),
        )
        .expect("read generic published row");
    assert_eq!(count, 1);
    drop(database);
    server.quiesce().expect("quiesce generic read server");
}

#[cfg(feature = "remote")]
fn run_generic_process_flow(backend: &str) {
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
            .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
            .unwrap_or_else(|_| "http://127.0.0.1:19025".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown generic session backend {backend}"),
    };
    let suffix = unique_suffix();
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rs-{}", suffix.trim_start_matches("rust-bootstrap-"))
    };
    let prefix = format!("{suffix}/session-generic");
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    } else {
        ensure_s3_bucket(&endpoint, &bucket);
    }
    let root = tempfile::tempdir().expect("generic process-flow directory");
    let cache_one = root.path().join("cache-one");
    let cache_two = root.path().join("cache-two");
    let cache_three = root.path().join("cache-three");
    let state = root.path().join("state.bin");
    let executable = std::env::current_exe().expect("locate emulator test executable");
    for (phase, cache) in [
        ("generic_session_phase_one", &cache_one),
        ("generic_session_phase_two", &cache_two),
        ("generic_session_phase_three", &cache_three),
    ] {
        fs::create_dir(cache).expect("create generic child cache directory");
        let status = Command::new(&executable)
            .args(["--ignored", "--exact", phase, "--nocapture"])
            .env("BCV_GENERIC_BACKEND", backend)
            .env("BCV_GENERIC_CACHE", cache)
            .env("BCV_GENERIC_ENDPOINT", &endpoint)
            .env("BCV_GENERIC_BUCKET", &bucket)
            .env("BCV_GENERIC_PREFIX", &prefix)
            .env("BCV_GENERIC_STATE", &state)
            .status()
            .unwrap_or_else(|error| panic!("spawn {phase}: {error}"));
        assert!(
            status.success(),
            "generic child phase {phase} failed: {status}"
        );
    }
}

// Serverless SQLite callers may issue xSync repeatedly during a request; invoke
// the real main-file VFS callback directly so each checkpoint is explicit and
// independent of pager scheduling or unrelated sidecar synchronization.
fn session_chain_sync_main_file(database: &Connection) {
    let database_name = CString::new("main").expect("database name is NUL-free");
    let mut file: *mut ffi::sqlite3_file = ptr::null_mut();
    let rc = unsafe {
        ffi::sqlite3_file_control(
            database.handle(),
            database_name.as_ptr(),
            ffi::SQLITE_FCNTL_FILE_POINTER,
            (&mut file as *mut *mut ffi::sqlite3_file).cast(),
        )
    };
    assert_eq!(rc, ffi::SQLITE_OK, "obtain the main database VFS file");
    assert!(!file.is_null(), "main database VFS file pointer is null");
    let methods = unsafe { &*(*file).pMethods };
    let x_sync = methods.xSync.expect("VFS implements xSync");
    let rc = unsafe { x_sync(file, ffi::SQLITE_SYNC_FULL) };
    assert_eq!(rc, ffi::SQLITE_OK, "VFS xSync failed");
}

// Force deterministic checkpoint counts even though serverless updates can
// trigger pager syncs; only this test suppresses them before explicit xSyncs.
fn session_chain_prepare_database(database: &Connection) {
    // The seed is uploaded in DELETE mode because bcvfs rejects journal_mode
    // PRAGMAs. Serverless updates may trigger pager syncs, so test-only
    // synchronous=OFF leaves each intended checkpoint to one explicit xSync.
    database
        .execute_batch("PRAGMA synchronous=OFF;")
        .expect("disable automatic sync for deterministic checkpoints");
}

// Change real database content before each explicit xSync, so checkpoint
// recovery proves the sequence's latest state rather than only chain metadata.
fn session_chain_write_state(database: &Connection, value: i64) {
    database
        .execute("UPDATE checkpoint_state SET value = ?1", [value])
        .expect("update checkpoint test state");
}

// Read the durable test value after a new request attaches; this catches a
// chain that rehydrates structurally but exposes stale checkpoint contents.
fn session_chain_read_state(database: &Connection) -> i64 {
    database
        .query_row("SELECT value FROM checkpoint_state", [], |row| row.get(0))
        .expect("read checkpoint test state")
}

// The production accept wrapper is feature-gated behind `remote`; call the
// native accept boundary directly so blockcachevfs-only tests isolate this
// exact accepted-head transition without involving an HTTP server.
fn session_chain_accept(vfs: &'static BlockCacheVfs, alias: &str, session_id: &str) {
    let vfs_name = CString::new(vfs.name()).expect("VFS name is NUL-free");
    let native_vfs = unsafe { ffi::sqlite3_vfs_find(vfs_name.as_ptr()) };
    assert!(
        !native_vfs.is_null(),
        "registered VFS must be discoverable by name"
    );
    let alias = CString::new(alias).expect("alias is NUL-free");
    let session_id = CString::new(session_id).expect("session ID is NUL-free");
    let mut etag = ptr::null_mut();
    let mut error = ptr::null_mut();
    let rc = unsafe {
        raw_bcv::sqlite3_bcvfs_session_accept(
            native_vfs.cast(),
            alias.as_ptr(),
            session_id.as_ptr(),
            &mut etag,
            &mut error,
        )
    };
    let message = if error.is_null() {
        None
    } else {
        let message = unsafe { CStr::from_ptr(error) }
            .to_string_lossy()
            .into_owned();
        unsafe { ffi::sqlite3_free(error.cast()) };
        Some(message)
    };
    if !etag.is_null() {
        unsafe { ffi::sqlite3_free(etag.cast()) };
    }
    assert_eq!(rc, ffi::SQLITE_OK, "session acceptance failed: {message:?}");
}

// The public Rust finalize wrapper is likewise remote-gated. This raw call
// exercises NEW-operation finalization itself, distinct from accepting a head
// and later uploading it through the already-accepted SessionAttachment path.
fn session_chain_finalize(vfs: &'static BlockCacheVfs, alias: &str, session_id: &str) {
    let vfs_name = CString::new(vfs.name()).expect("VFS name is NUL-free");
    let native_vfs = unsafe { ffi::sqlite3_vfs_find(vfs_name.as_ptr()) };
    assert!(
        !native_vfs.is_null(),
        "registered VFS must be discoverable by name"
    );
    let alias = CString::new(alias).expect("alias is NUL-free");
    let session_id = CString::new(session_id).expect("session ID is NUL-free");
    let mut error = ptr::null_mut();
    let rc = unsafe {
        raw_bcv::sqlite3_bcvfs_session_finalize(
            native_vfs.cast(),
            alias.as_ptr(),
            session_id.as_ptr(),
            &mut error,
        )
    };
    let message = if error.is_null() {
        None
    } else {
        let message = unsafe { CStr::from_ptr(error) }
            .to_string_lossy()
            .into_owned();
        unsafe { ffi::sqlite3_free(error.cast()) };
        Some(message)
    };
    assert_eq!(
        rc,
        ffi::SQLITE_OK,
        "session finalization failed: {message:?}"
    );
}

// Model successive stateless serverless requests: each gets a fresh scoped
// attachment and operation ID while continuing the same logical session.
fn attach_session_chain_request(
    vfs: &'static BlockCacheVfs,
    storage: &Storage,
    alias: &str,
    session_id: &str,
    operation: u8,
) -> SessionAttachment {
    let spec = AttachSpec::new(storage.clone()).alias(alias);
    attach_scoped_for_test(vfs, &spec, session_id, operation)
        .unwrap_or_else(|error| panic!("attach session operation {operation}: {error:?}"))
}

// Serverless requests accumulate xSync checkpoints across fresh attachments:
// 65 used to exceed recovery, while >256 in one attachment could evade
// compaction. The 257-checkpoint genesis case checks the 256 write watermark;
// content is then rehydrated/finalized and verified from a separate cache.
fn run_session_checkpoint_chain_limits(backend: &str, endpoint: &str, bucket: &str) {
    let suffix = unique_suffix();
    assert!(
        matches!(backend, "google" | "s3"),
        "unknown backend {backend}"
    );
    let prefix = format!("{suffix}/session-chain");
    let storage = session_storage(backend, endpoint, bucket, &prefix);
    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("VFS builder")
        .config(Config::CacheSize(8 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize block-cache VFS");
    vfs.initialize_container(&storage)
        .expect("initialize remote CBS container");

    let seed_directory = tempfile::tempdir().expect("seed database directory");
    let seed_path = seed_directory.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create seed SQLite database");
    seed.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE payload(value TEXT NOT NULL);
         INSERT INTO payload VALUES ('session-chain');
         CREATE TABLE checkpoint_state(value INTEGER NOT NULL);
         INSERT INTO checkpoint_state VALUES (0);",
    )
    .expect("seed database");
    seed.close().expect("close seed database");
    vfs.create_database(&storage, &seed_path, "session.sqlite")
        .expect("upload seed database");

    let repeat_session_id = Uuid::new_v4().to_string();
    let repeat_alias = format!("session-chain-repeat-{}", suffix.replace('-', "_"));
    let first = attach_session_chain_request(vfs, &storage, &repeat_alias, &repeat_session_id, 1);
    let database = vfs
        .open(format!("/{repeat_alias}/session.sqlite"))
        .expect("open database for 64 real xSync checkpoints");
    for _ in 0..64 {
        session_chain_sync_main_file(&database);
    }
    drop(database);
    session_chain_accept(vfs, &repeat_alias, &repeat_session_id);
    drop(first);

    // The next request first rehydrates an exactly-64-checkpoint accepted
    // chain, then adds checkpoint 65. A fresh request must rehydrate that
    // longer chain as well.
    let second = attach_session_chain_request(vfs, &storage, &repeat_alias, &repeat_session_id, 2);
    let database = vfs
        .open(format!("/{repeat_alias}/session.sqlite"))
        .expect("open database for checkpoint 65");
    session_chain_sync_main_file(&database);
    drop(database);
    session_chain_accept(vfs, &repeat_alias, &repeat_session_id);
    drop(second);
    let third = attach_session_chain_request(vfs, &storage, &repeat_alias, &repeat_session_id, 3);
    drop(third);

    // A separate genesis session crosses the production watermark in one
    // attachment. Its compacted checkpoint has the all-zero captured
    // predecessor; a subsequent request must rehydrate it, compact against
    // that accepted nonzero checkpoint, and publish through new-operation
    // finalization.
    let compact_session_id = Uuid::new_v4().to_string();
    let compact_alias = format!("session-chain-compact-{}", suffix.replace('-', "_"));
    let first = attach_session_chain_request(vfs, &storage, &compact_alias, &compact_session_id, 1);
    let database = vfs
        .open(format!("/{compact_alias}/session.sqlite"))
        .expect("open database for over-256 xSync checkpoint stress");
    session_chain_prepare_database(&database);
    for value in 1..=257 {
        session_chain_write_state(&database, value);
        session_chain_sync_main_file(&database);
    }
    drop(database);
    session_chain_accept(vfs, &compact_alias, &compact_session_id);
    drop(first);

    let second =
        attach_session_chain_request(vfs, &storage, &compact_alias, &compact_session_id, 2);
    let database = vfs
        .open(format!("/{compact_alias}/session.sqlite"))
        .expect("reattached database after genesis compaction");
    session_chain_prepare_database(&database);
    assert_eq!(
        session_chain_read_state(&database),
        257,
        "fresh request must recover the last genesis checkpoint"
    );
    session_chain_write_state(&database, 258);
    session_chain_sync_main_file(&database);
    drop(database);
    session_chain_finalize(vfs, &compact_alias, &compact_session_id);
    drop(second);

    let fresh_cache_root = tempfile::tempdir().expect("fresh read cache directory");
    let fresh_cache = fresh_cache_root.path().join("cache");
    std::fs::create_dir(&fresh_cache).expect("create fresh read cache");
    let status = Command::new(std::env::current_exe().expect("locate emulator test executable"))
        .args([
            "--ignored",
            "--exact",
            "session_chain_fresh_cache_child",
            "--nocapture",
        ])
        .env("BCV_SESSION_CHAIN_BACKEND", backend)
        .env("BCV_SESSION_CHAIN_ENDPOINT", endpoint)
        .env("BCV_SESSION_CHAIN_BUCKET", bucket)
        .env("BCV_SESSION_CHAIN_PREFIX", &prefix)
        .env("BCV_SESSION_CHAIN_CACHE", &fresh_cache)
        .env(
            "BCV_SESSION_CHAIN_ALIAS",
            format!("session-chain-fresh-{suffix}"),
        )
        .status()
        .expect("spawn fresh-cache session reader");
    assert!(
        status.success(),
        "fresh-cache session reader failed: {status}"
    );
}

// Serverless finalization must publish the compacted checkpoint, not merely
// leave readable state in the writer's cache. This child starts a fresh VFS
// and cache and verifies both final value and original seed data from storage.
#[test]
#[ignore = "child phase for session checkpoint chain regression"]
fn session_chain_fresh_cache_child() {
    let (Ok(backend), Ok(endpoint), Ok(bucket), Ok(prefix), Ok(cache), Ok(alias)) = (
        std::env::var("BCV_SESSION_CHAIN_BACKEND"),
        std::env::var("BCV_SESSION_CHAIN_ENDPOINT"),
        std::env::var("BCV_SESSION_CHAIN_BUCKET"),
        std::env::var("BCV_SESSION_CHAIN_PREFIX"),
        std::env::var("BCV_SESSION_CHAIN_CACHE"),
        std::env::var("BCV_SESSION_CHAIN_ALIAS"),
    ) else {
        return;
    };
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let vfs = BlockCacheVfs::builder(&cache)
        .expect("fresh read VFS builder")
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize fresh read VFS");
    vfs.attach(&AttachSpec::new(storage).alias(&alias))
        .expect("attach published container through fresh VFS");
    let database = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("open database through fresh VFS/cache");
    assert_eq!(
        session_chain_read_state(&database),
        258,
        "published manifest must expose the finalized state"
    );
    let payload: String = database
        .query_row("SELECT value FROM payload", [], |row| row.get(0))
        .expect("read original seed row through fresh VFS/cache");
    assert_eq!(payload, "session-chain");
    drop(database);
    vfs.detach(&alias).expect("detach fresh read VFS container");
}

// Share Google-emulator endpoint, bucket, auth, and VFS setup across standalone
// finding tests so they all cross the same public Rust API boundary.
fn google_review_vfs() -> (&'static BlockCacheVfs, String, String) {
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage".to_owned();
    ensure_google_bucket(&endpoint, &bucket);
    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("Google review VFS builder")
        .config(Config::CacheSize(8 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize Google review VFS");
    (vfs, endpoint, bucket)
}

// Build a known-valid SQLite input for public API tests; its payload lets a
// fresh attachment prove it read the intended uploaded database.
fn create_review_seed_database(path: &Path) {
    let seed = Connection::open(path).expect("create review seed database");
    seed.execute_batch(
        "CREATE TABLE payload(value TEXT NOT NULL);
         INSERT INTO payload VALUES ('public-api-review-seed');",
    )
    .expect("write review seed database");
    seed.close().expect("close review seed database");
}

// Probe publication through a separate process/cache: read-only flags prevent
// SQLite from creating an empty replacement when a remote name cannot reopen.
fn fresh_vfs_database_name_probe(
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    remote_name: &str,
    alias: &str,
    expect_open: bool,
) -> Option<String> {
    let cache_root = tempfile::tempdir().expect("fresh database-name probe directory");
    let cache = cache_root.path().join("cache");
    std::fs::create_dir(&cache).expect("create fresh database-name probe cache");
    let output = Command::new(std::env::current_exe().expect("locate emulator test executable"))
        .args([
            "--ignored",
            "--exact",
            "database_name_fresh_vfs_child",
            "--nocapture",
        ])
        .env("BCV_DATABASE_NAME_ENDPOINT", endpoint)
        .env("BCV_DATABASE_NAME_BUCKET", bucket)
        .env("BCV_DATABASE_NAME_PREFIX", prefix)
        .env("BCV_DATABASE_NAME_REMOTE", remote_name)
        .env("BCV_DATABASE_NAME_ALIAS", alias)
        .env("BCV_DATABASE_NAME_CACHE", &cache)
        .env("BCV_DATABASE_NAME_EXPECT_OPEN", expect_open.to_string())
        .output()
        .expect("spawn fresh database-name probe");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "fresh database-name probe failed for {remote_name:?}: status={}, stdout={stdout:?}, stderr={stderr:?}",
        output.status
    );
    if expect_open {
        assert!(
            stdout.contains("DATABASE_NAME_OPEN_OK"),
            "fresh database-name probe did not confirm open: stdout={stdout:?}, stderr={stderr:?}"
        );
        None
    } else {
        Some(
            stdout
                .lines()
                .find_map(|line| line.strip_prefix("DATABASE_NAME_PROBE:"))
                .unwrap_or_else(|| {
                    panic!(
                        "fresh database-name probe did not report its read-only probe result: stdout={stdout:?}, stderr={stderr:?}"
                    )
                })
                .to_owned(),
        )
    }
}

// Child half of the create_database regression: a new VFS must decode the
// published name from storage, and read-only opening distinguishes a real
// database from an empty local file accidentally created by CREATE flags.
#[test]
#[ignore = "child phase for database-name review regression"]
fn database_name_fresh_vfs_child() {
    let (
        Ok(endpoint),
        Ok(bucket),
        Ok(prefix),
        Ok(remote_name),
        Ok(alias),
        Ok(cache),
        Ok(expect_open),
    ) = (
        std::env::var("BCV_DATABASE_NAME_ENDPOINT"),
        std::env::var("BCV_DATABASE_NAME_BUCKET"),
        std::env::var("BCV_DATABASE_NAME_PREFIX"),
        std::env::var("BCV_DATABASE_NAME_REMOTE"),
        std::env::var("BCV_DATABASE_NAME_ALIAS"),
        std::env::var("BCV_DATABASE_NAME_CACHE"),
        std::env::var("BCV_DATABASE_NAME_EXPECT_OPEN"),
    )
    else {
        return;
    };
    let expect_open = expect_open == "true";
    let storage =
        Storage::google_json_with_endpoint("test-project", format!("{bucket}/{prefix}"), &endpoint);
    let vfs = BlockCacheVfs::builder(&cache)
        .expect("fresh database-name VFS builder")
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize fresh database-name VFS");
    vfs.attach(&AttachSpec::new(storage).alias(&alias))
        .expect("attach database-name container through fresh VFS");
    let path = format!("/{alias}/{remote_name}");
    let probe_result = match vfs.open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Err(error) => Err(format!("read-only open failed: {error:?}")),
        Ok(database) => match database.query_row("SELECT value FROM payload", [], |row| {
            row.get::<_, String>(0)
        }) {
            Err(error) => Err(format!("payload query failed: {error:?}")),
            Ok(payload) if payload == "public-api-review-seed" => Ok(()),
            Ok(payload) => Err(format!(
                "payload mismatch: expected public-api-review-seed, got {payload:?}"
            )),
        },
    };
    match (expect_open, probe_result) {
        (true, Ok(())) => println!("DATABASE_NAME_OPEN_OK"),
        (true, Err(error)) => {
            panic!("fresh VFS could not read valid remote name {remote_name:?} read-only: {error}")
        }
        (false, Ok(())) => {
            println!("DATABASE_NAME_PROBE:read-only open succeeded and seed content matched");
        }
        (false, Err(error)) => println!("DATABASE_NAME_PROBE:{error}"),
    }
    vfs.detach(&alias)
        .expect("detach database-name container from fresh VFS");
}

// A library caller can reasonably choose public attach_session and receive
// Ok, then find that no database opens because the required scope was never
// bound. Scoped attach on the same seed is the positive control for usability.
#[test]
#[ignore = "reproduces validated review finding"]
fn google_json_emulator_public_attach_session_database_scope() {
    let (vfs, endpoint, bucket) = google_review_vfs();
    let suffix = unique_suffix();
    let prefix = format!("{suffix}/public-attach-session");
    let storage =
        Storage::google_json_with_endpoint("test-project", format!("{bucket}/{prefix}"), &endpoint);
    vfs.initialize_container(&storage)
        .expect("initialize public attach_session test container");
    let seed_directory = tempfile::tempdir().expect("public attach_session seed directory");
    let seed_path = seed_directory.path().join("seed.sqlite");
    create_review_seed_database(&seed_path);
    vfs.create_database(&storage, &seed_path, "session.sqlite")
        .expect("upload public attach_session seed database");

    let scoped_alias = format!("scoped-review-{}", suffix.replace('-', "_"));
    let scoped_session_id = Uuid::new_v4().to_string();
    let scoped_spec = AttachSpec::new(storage.clone()).alias(&scoped_alias);
    let scoped = attach_scoped_for_test(vfs, &scoped_spec, &scoped_session_id, 1)
        .expect("scoped positive-control attachment");
    let scoped_database = vfs
        .open(format!("/{scoped_alias}/session.sqlite"))
        .expect("open database through scoped positive-control attachment");
    let scoped_payload: String = scoped_database
        .query_row("SELECT value FROM payload", [], |row| row.get(0))
        .expect("read seed through scoped positive-control attachment");
    assert_eq!(scoped_payload, "public-api-review-seed");
    drop(scoped_database);
    drop(scoped);

    let direct_alias = format!("direct-review-{}", suffix.replace('-', "_"));
    let direct_session_id = Uuid::new_v4().to_string();
    let direct_spec = AttachSpec::new(storage).alias(&direct_alias);
    let direct = vfs
        .attach_session(&direct_spec, &direct_session_id)
        .expect("public lower-level attach_session should return an attachment");
    assert_eq!(direct.session_id(), direct_session_id);
    let direct_database = match vfs.open(format!("/{direct_alias}/session.sqlite")) {
        Ok(database) => database,
        Err(error) => panic!(
            "desired behavior: public attach_session attachment should open its session.sqlite database; actual open error: {error:?}"
        ),
    };
    let direct_payload: String = direct_database
        .query_row("SELECT value FROM payload", [], |row| row.get(0))
        .expect("read seed through public attach_session attachment");
    assert_eq!(direct_payload, "public-api-review-seed");
    drop(direct_database);
    drop(direct);
}

// A caller-supplied slash or backslash name may be published but cannot be
// addressed by a fresh VFS because those characters delimit VFS paths. The
// valid one-component name is the control for this reopenability regression.
#[test]
#[ignore = "reproduces validated review finding"]
fn google_json_emulator_create_database_rejects_path_separators() {
    let (vfs, endpoint, bucket) = google_review_vfs();
    let suffix = unique_suffix();
    let seed_directory = tempfile::tempdir().expect("database-name seed directory");
    let seed_path = seed_directory.path().join("seed.sqlite");
    create_review_seed_database(&seed_path);

    let valid_prefix = format!("{suffix}/database-name-valid");
    let valid_storage = Storage::google_json_with_endpoint(
        "test-project",
        format!("{bucket}/{valid_prefix}"),
        &endpoint,
    );
    vfs.initialize_container(&valid_storage)
        .expect("initialize valid database-name control container");
    vfs.create_database(&valid_storage, &seed_path, "valid.sqlite")
        .expect("upload valid one-component control database name");
    let valid_alias = format!("name-valid-{}", suffix.replace('-', "_"));
    assert_eq!(
        fresh_vfs_database_name_probe(
            &endpoint,
            &bucket,
            &valid_prefix,
            "valid.sqlite",
            &valid_alias,
            true,
        ),
        None,
        "fresh cache must open the valid one-component control name"
    );

    let mut accepted_inaccessible_names = Vec::new();
    for (index, remote_name) in ["nested/name.sqlite", r"nested\name.sqlite"]
        .into_iter()
        .enumerate()
    {
        let prefix = format!("{suffix}/database-name-separator-{index}");
        let storage = Storage::google_json_with_endpoint(
            "test-project",
            format!("{bucket}/{prefix}"),
            &endpoint,
        );
        vfs.initialize_container(&storage)
            .unwrap_or_else(|error| panic!("initialize separator case {remote_name:?}: {error:?}"));
        if vfs
            .create_database(&storage, &seed_path, remote_name)
            .is_ok()
        {
            let open_result = fresh_vfs_database_name_probe(
                &endpoint,
                &bucket,
                &prefix,
                remote_name,
                &format!("name-invalid-{index}-{}", suffix.replace('-', "_")),
                false,
            )
            .expect("fresh VFS probe returns its read-only result");
            accepted_inaccessible_names.push(format!(
                "{remote_name:?}: create_database returned Ok/publication succeeded; fresh VFS/cache probe: {open_result}"
            ));
        }
    }
    assert!(
        accepted_inaccessible_names.is_empty(),
        "desired behavior: create_database should reject slash- and backslash-containing names before upload; accepted names and their fresh read-only probe results: {}",
        accepted_inaccessible_names.join("; ")
    );
}

#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_connection_uploads_and_isolates_vfs_lifetimes() {
    let endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".into());
    let bucket = "app_storage";
    let suffix = unique_suffix();
    let prefix = format!("{suffix}/uri/cbs/");
    ensure_google_bucket(&endpoint, bucket);

    let storage =
        Storage::google_json_with_endpoint("test-project", format!("{bucket}/{prefix}"), &endpoint);
    let vfs_cache = tempfile::tempdir().expect("CBS bootstrap VFS cache directory");
    let bootstrap_vfs = BlockCacheVfs::builder(vfs_cache.path())
        .expect("CBS bootstrap VFS builder")
        .auth_callback(|_, _, _| Ok("bootstrap-token".to_owned()))
        .init_owned()
        .expect("initialize an owned CBS VFS");
    bootstrap_vfs
        .initialize_container(&storage)
        .expect("initialize the prefixed CBS container");

    let local_dir = tempfile::tempdir().expect("local seed directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local seed database");
    local
        .execute_batch(
            "CREATE TABLE mutation(value TEXT NOT NULL); INSERT INTO mutation VALUES ('seed');",
        )
        .expect("seed local database");
    let _: i64 = local
        .query_row("SELECT dolt_add('-A')", [], |row| row.get(0))
        .expect("stage seed data");
    let _: String = local
        .query_row("SELECT dolt_commit('-m', 'URI seed')", [], |row| row.get(0))
        .expect("commit seed data");
    local.close().expect("close local seed database");
    bootstrap_vfs
        .create_database(&storage, &local_path, "default.db")
        .expect("upload seeded default.db");
    drop(bootstrap_vfs);

    let uri = format!(
        "gcs://{bucket}/{prefix}?vfs=blockcachevfs&project=test-project&access_token=first-token&endpoint={endpoint}"
    );
    let first = Connection::open(&uri).expect("open existing CBS database from one URI");
    let second_uri = uri.replace("first-token", "second-token");
    let second =
        Connection::open(&second_uri).expect("open a concurrent connection with its token");
    assert_ne!(
        first.blockcachevfs_name(),
        second.blockcachevfs_name(),
        "concurrent connection credentials must use separate VFS instances"
    );
    assert_ne!(
        first.blockcachevfs_path(),
        second.blockcachevfs_path(),
        "each connection receives its own attached alias"
    );
    assert!(!format!("{first:?}").contains("first-token"));
    second.close().expect("close second CBS connection");

    #[cfg(feature = "remote")]
    {
        let directory = first
            .blockcachevfs_directory()
            .expect("CBS attached directory");
        let vfs_name = first.blockcachevfs_name().expect("CBS VFS name");
        let options = RemoteServerOptions::new().vfs_name(vfs_name);
        let server = RemoteServer::start_with_options(directory, &options)
            .expect("start remote server on the connection-owned VFS");
        let mut stream = TcpStream::connect(("127.0.0.1", server.port()))
            .expect("connect to CBS-backed remote server");
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(3)))
            .expect("set remote server read timeout");
        stream
            .write_all(
                b"GET /default.db/refs HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
            )
            .expect("request CBS database refs");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .expect("read CBS-backed remote server response");
        assert!(
            response.starts_with(b"HTTP/1.1 200 OK\r\n"),
            "selected CBS VFS should resolve the database: {}",
            String::from_utf8_lossy(&response[..response.len().min(256)])
        );
        drop(server);
    }

    first
        .execute("INSERT INTO mutation VALUES ('pushed')", [])
        .expect("write through URI connection");
    first
        .upload()
        .expect("explicitly upload from URI connection");
    first
        .execute("INSERT INTO mutation VALUES ('pending-close')", [])
        .expect("write a change before close");
    let (first, close_error) = first
        .close()
        .expect_err("close must surface unuploaded local CBS changes");
    assert_eq!(
        close_error.sqlite_error_code(),
        Some(rusqlite::ffi::ErrorCode::DatabaseBusy)
    );
    first
        .upload()
        .expect("upload remains available after SQLite close");
    first
        .close()
        .expect("close after uploading the pending CBS changes");

    let verify = Connection::open(&uri).expect("re-open after dropping the first VFS");
    let count: i64 = verify
        .query_row("SELECT count(*) FROM mutation", [], |row| row.get(0))
        .expect("read uploaded row from a fresh VFS");
    assert_eq!(count, 3);
    verify.close().expect("close re-opened CBS connection");

    let missing_uri = format!(
        "gcs://{bucket}/{prefix}?vfs=blockcachevfs&project=test-project&access_token=private-token&endpoint={endpoint}&database=missing.db"
    );
    let error = Connection::open_with_flags(&missing_uri, no_create_flags())
        .expect_err("missing database without CREATE must not be created");
    assert_eq!(
        error.sqlite_extended_error_code(),
        Some(rusqlite::ffi::SQLITE_NOTFOUND)
    );
    assert!(!error.to_string().contains("private-token"));

    let missing_container_uri = format!(
        "gcs://{bucket}/{suffix}/missing-container?vfs=blockcachevfs&project=test-project&access_token=container-secret&endpoint={endpoint}"
    );
    let error = Connection::open_with_flags(&missing_container_uri, no_create_flags())
        .expect_err("missing CBS manifest without CREATE must not be initialized");
    assert_eq!(
        error.sqlite_extended_error_code(),
        Some(rusqlite::ffi::SQLITE_NOTFOUND)
    );
    assert!(!error.to_string().contains("container-secret"));
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn s3_uri_connection_uploads_existing_prefixed_database() {
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let suffix = unique_suffix();
    let bucket = format!("rust-uri-{suffix}");
    let prefix = format!("{suffix}/uri/cbs/");
    let container = format!("{bucket}/{prefix}");
    let access_id = "test";
    let secret_access_key = "test";
    let session_token = "session/+token=";
    let storage = Storage::s3_with_endpoint(access_id, &container, "us-east-1", &endpoint);

    let vfs_cache = tempfile::tempdir().expect("CBS bootstrap VFS cache directory");
    let bootstrap_vfs = BlockCacheVfs::builder(vfs_cache.path())
        .expect("CBS bootstrap VFS builder")
        .auth_callback(move |_, _, _| {
            rusqlite::blockcachevfs::s3_secret_with_session_token(secret_access_key, session_token)
        })
        .init_owned()
        .expect("initialize an owned CBS VFS");
    bootstrap_vfs
        .initialize_container(&storage)
        .expect("initialize the trailing-slash S3 prefix");

    let local_dir = tempfile::tempdir().expect("local seed directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local seed database");
    local
        .execute_batch(
            "CREATE TABLE mutation(value TEXT NOT NULL); INSERT INTO mutation VALUES ('seed');",
        )
        .expect("seed local database");
    local.close().expect("close local seed database");
    bootstrap_vfs
        .create_database(&storage, &local_path, "default.db")
        .expect("upload seeded default.db");
    drop(bootstrap_vfs);

    let uri = format!(
        "s3://{bucket}/{prefix}?vfs=blockcachevfs&region=us-east-1&access_id={}&secret_access_key={}&session_token={}&endpoint={endpoint}",
        encode_first_byte(access_id),
        encode_first_byte(secret_access_key),
        encode_query_value(session_token),
    );
    let connection = Connection::open(&uri).expect("open existing prefixed database from one URI");
    assert_eq!(
        connection
            .query_row("SELECT value FROM mutation", [], |row| row
                .get::<_, String>(0))
            .expect("read seeded row"),
        "seed"
    );
    assert!(!format!("{connection:?}").contains(access_id));
    assert!(!format!("{connection:?}").contains(secret_access_key));
    assert!(!format!("{connection:?}").contains(session_token));

    connection
        .execute("INSERT INTO mutation VALUES ('uploaded')", [])
        .expect("write through S3 URI connection");
    connection.upload().expect("explicitly upload S3 changes");
    connection.close().expect("close uploaded S3 connection");

    let verify = Connection::open(&uri).expect("re-open database through a fresh S3 VFS");
    let values: Vec<String> = verify
        .prepare("SELECT value FROM mutation ORDER BY rowid")
        .expect("prepare readback")
        .query_map([], |row| row.get(0))
        .expect("query readback")
        .collect::<rusqlite::Result<_>>()
        .expect("collect uploaded rows");
    assert_eq!(values, ["seed", "uploaded"]);
    verify.close().expect("close verification connection");

    let missing_database_uri = format!("{uri}&database=missing.db");
    let error = Connection::open_with_flags(missing_database_uri, no_create_flags())
        .expect_err("open without CREATE must not create a missing S3 database");
    assert_eq!(
        error.sqlite_extended_error_code(),
        Some(rusqlite::ffi::SQLITE_NOTFOUND)
    );
    for credential in [access_id, secret_access_key, session_token] {
        assert!(!error.to_string().contains(credential));
        assert!(!format!("{error:?}").contains(credential));
    }

    let missing_container_uri = format!(
        "s3://{bucket}/{suffix}/uri/missing?vfs=blockcachevfs&region=us-east-1&access_id={}&secret_access_key={}&session_token={}&endpoint={endpoint}",
        encode_first_byte(access_id),
        encode_first_byte(secret_access_key),
        encode_query_value(session_token),
    );
    let error = Connection::open_with_flags(missing_container_uri, no_create_flags())
        .expect_err("open without CREATE must not initialize a missing S3 CBS prefix");
    assert_eq!(
        error.sqlite_extended_error_code(),
        Some(rusqlite::ffi::SQLITE_NOTFOUND)
    );
    for credential in [access_id, secret_access_key, session_token] {
        assert!(!error.to_string().contains(credential));
        assert!(!format!("{error:?}").contains(credential));
    }
}

#[cfg(feature = "remote")]
fn assert_gcs_database_value(storage: &Storage, database_name: &str, expected_value: &str) {
    let cache = tempfile::tempdir().expect("fresh GCS verification cache");
    let vfs = BlockCacheVfs::builder(cache.path())
        .expect("GCS verification VFS builder")
        .config(Config::CacheSize(8 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init_owned()
        .expect("initialize GCS verification VFS");
    let alias = format!("verify_{}", std::process::id());
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&alias))
        .expect("attach GCS verification container");
    let path = format!("/{alias}/{database_name}");
    let database = vfs.open(&path).expect("open uploaded GCS database");
    let value: String = database
        .query_row("SELECT value FROM payload", [], |row| row.get(0))
        .expect("read the uploaded value from GCS");
    assert_eq!(value, expected_value);
    database.close().expect("close GCS verification database");
    vfs.detach(&alias)
        .expect("detach GCS verification container");
}

#[cfg(feature = "remote")]
fn assert_gcs_download_fault_does_not_poison_cache(
    proxy: &StorageFaultProxy,
    storage: &Storage,
    database_name: &str,
    fault: StorageFaultAction,
) {
    let cache = tempfile::tempdir().expect("fresh GCS integrity reader cache");
    let vfs = BlockCacheVfs::builder(cache.path())
        .expect("GCS integrity reader VFS builder")
        .config(Config::CacheSize(8 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init_owned()
        .expect("initialize GCS integrity reader VFS");
    let alias = format!("integrity_{}", std::process::id());
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&alias))
        .expect("attach GCS integrity test container");

    proxy.configure(StorageFaultTarget::BlockGet, fault);
    proxy.arm();
    let path = format!("/{alias}/{database_name}");
    let database = vfs
        .open(&path)
        .expect("open succeeds after rejecting the faulted response and retrying");
    let value: String = database
        .query_row("SELECT value FROM payload", [], |row| row.get(0))
        .expect("read the original value after the integrity retry");
    proxy.disarm();
    assert_eq!(
        proxy.faulted(),
        1,
        "the proxy must inject one corrupted, missing, malformed, or short block response"
    );
    assert!(
        proxy.matched() >= 2,
        "the integrity failure should trigger a fresh block download"
    );
    assert_eq!(value, "crc32c-seed");
    database
        .close()
        .expect("close GCS integrity reader database");

    let cached = vfs
        .open(&path)
        .expect("re-open GCS database after the integrity retry");
    let cached_value: String = cached
        .query_row("SELECT value FROM payload", [], |row| row.get(0))
        .expect("read the verified block from the cache");
    assert_eq!(cached_value, "crc32c-seed");
    cached.close().expect("close cached GCS reader database");
    vfs.detach(&alias)
        .expect("detach GCS integrity reader container");
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_json_emulator_crc32c_guards_media_integrity() {
    assert_eq!(test_crc32c_base64(b"123456789"), "4waSgw==");
    let endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let proxy = StorageFaultProxy::start(&endpoint);
    let prefix = format!("{}/integrity/good", unique_suffix());
    let storage = Storage::google_json_with_endpoint(
        "test-project",
        format!("{bucket}/{prefix}"),
        &proxy.url,
    );
    let cache = tempfile::tempdir().expect("GCS integrity writer cache");
    let vfs = BlockCacheVfs::builder(cache.path())
        .expect("GCS integrity writer VFS builder")
        .config(Config::CacheSize(8 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init_owned()
        .expect("initialize GCS integrity writer VFS");
    vfs.initialize_container(&storage)
        .expect("initialize GCS integrity test container");

    let seed_directory = tempfile::tempdir().expect("GCS integrity seed directory");
    let seed_path = seed_directory.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create GCS integrity seed database");
    seed.execute_batch(
        "CREATE TABLE payload(value TEXT NOT NULL);
         INSERT INTO payload VALUES ('crc32c-seed');",
    )
    .expect("write GCS integrity seed database");
    seed.close().expect("close GCS integrity seed database");
    vfs.create_database(&storage, &seed_path, "integrity.sqlite")
        .expect("upload GCS integrity seed database");
    let valid_uploads = proxy.upload_checksums_valid();
    assert!(
        valid_uploads.len() >= 2,
        "manifest and block media uploads were observed"
    );
    assert!(
        valid_uploads.iter().all(|valid| *valid),
        "every GCS media upload must carry CRC32C for its exact request body"
    );

    let bad_prefix = format!("{}/integrity/bad-upload", unique_suffix());
    let bad_storage = Storage::google_json_with_endpoint(
        "test-project",
        format!("{bucket}/{bad_prefix}"),
        &proxy.url,
    );
    vfs.initialize_container(&bad_storage)
        .expect("initialize the bad-checksum upload container");
    proxy.configure(
        StorageFaultTarget::BlockPut,
        StorageFaultAction::BadUploadChecksum,
    );
    proxy.arm();
    vfs.create_database(&bad_storage, &seed_path, "integrity.sqlite")
        .expect("retry the block after a checksum-mismatch response");
    assert_eq!(
        proxy.faulted(),
        1,
        "the proxy must return one checksum-mismatch rejection"
    );
    assert!(
        proxy.matched() >= 2,
        "the rejected block should be retried through the GCS transport"
    );
    assert!(
        proxy.upload_checksums_valid().last() == Some(&true),
        "the client must send a correct checksum on the upload request"
    );
    proxy.disarm();
    assert_gcs_database_value(&bad_storage, "integrity.sqlite", "crc32c-seed");

    for fault in [
        StorageFaultAction::CorruptDownloadBody,
        StorageFaultAction::MissingDownloadChecksum,
        StorageFaultAction::MalformedDownloadChecksum,
        StorageFaultAction::ShortDownloadWithValidChecksum,
    ] {
        assert_gcs_download_fault_does_not_poison_cache(
            &proxy,
            &storage,
            "integrity.sqlite",
            fault,
        );
    }
}

#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_bootstrap() {
    let (endpoint, bucket) = run_bootstrap("google");
    run_session_checkpoint_chain_limits("google", &endpoint, &bucket);
}

#[test]
#[ignore = "requires the pinned local emulator containers"]
fn s3_emulator_bootstrap() {
    let (endpoint, bucket) = run_bootstrap("s3");
    run_session_checkpoint_chain_limits("s3", &endpoint, &bucket);
}

#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_ownership() {
    run_session_ownership();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_server() {
    run_session_server();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_http_flow() {
    run_session_http_flow("google");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn s3_emulator_session_http_flow() {
    run_session_http_flow("s3");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_failure_windows() {
    run_session_fault_matrix("google");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn s3_emulator_session_failure_windows() {
    run_session_fault_matrix("s3");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_crash_windows() {
    run_session_crash_matrix("google");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn s3_emulator_session_crash_windows() {
    run_session_crash_matrix("s3");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for session crash-window test"]
fn session_crash_child() {
    run_session_crash_child();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child recovery phase for session crash-window test"]
fn session_crash_recovery_child() {
    run_session_crash_recovery_child();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_concurrency_matrix() {
    run_session_concurrency_matrix();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for same-session process race test"]
fn same_session_process_race_child() {
    run_same_session_process_child();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for independent publication race test"]
fn independent_session_publication_prepare_child() {
    run_publication_prepare_child();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for independent publication race test"]
fn independent_session_publication_upload_child() {
    run_publication_upload_child();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for the slow session HTTP flow test"]
fn session_http_flow_child() {
    let Some(backend) = std::env::var_os("BCV_SESSION_HTTP_BACKEND") else {
        return;
    };
    let backend = backend
        .into_string()
        .expect("session HTTP flow backend must be valid UTF-8");
    std::env::var("DOLTLITE_HTTP_TIMEOUT_MS")
        .expect("session HTTP flow child requires its configured client timeout");
    run_session_http_flow_in_process(&backend);
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for generic session process-flow test"]
fn generic_session_phase_one() {
    run_generic_phase_one();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for generic session process-flow test"]
fn generic_session_phase_two() {
    run_generic_phase_two();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for generic session process-flow test"]
fn generic_session_phase_three() {
    run_generic_phase_three();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_generic_process_flow() {
    run_generic_process_flow("google");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn s3_emulator_session_generic_process_flow() {
    run_generic_process_flow("s3");
}

#[cfg(feature = "remote")]
fn remote_request(port: u16, method: &str, path: &str, body: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to remote server");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("set remote server read timeout");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("write remote request headers");
    stream.write_all(body).expect("write remote request body");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read remote server response");
    response
}

#[cfg(feature = "remote")]
fn remote_request_large_chunk(port: u16, chunk_bytes: usize) -> Vec<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to remote server");
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .expect("set large-request read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(120)))
        .expect("set large-request write timeout");
    let request_body_bytes = chunk_bytes + 24;
    write!(
        stream,
        "POST /default.db/chunks HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {request_body_bytes}\r\nConnection: close\r\n\r\n"
    )
    .expect("write large chunk request headers");
    stream
        .write_all(&[0_u8; 20])
        .expect("write invalid Prolly hash");
    stream
        .write_all(
            &u32::try_from(chunk_bytes)
                .expect("test chunk fits u32")
                .to_le_bytes(),
        )
        .expect("write chunk length");

    let block = [0_u8; 64 * 1024];
    let mut remaining = chunk_bytes;
    while remaining > 0 {
        let count = remaining.min(block.len());
        if stream.write_all(&block[..count]).is_err() {
            break;
        }
        remaining -= count;
    }

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read large chunk response");
    response
}

#[cfg(feature = "remote")]
fn remote_server_database_uri(backend: &str, endpoint: &str, bucket: &str, prefix: &str) -> String {
    match backend {
        "google" => format!(
            "gcs://{bucket}/{prefix}?vfs=blockcachevfs&project=test-project&access_token=remote-server-token&endpoint={endpoint}&database=remote.db"
        ),
        "s3" => format!(
            "s3://{bucket}/{prefix}?vfs=blockcachevfs&region=us-east-1&access_id=remote-server-access&secret_access_key=remote-server-secret&endpoint={endpoint}&database=remote.db"
        ),
        _ => unreachable!("unknown remote server backend {backend}"),
    }
}

#[cfg(feature = "remote")]
fn run_uri_remote_server_push_workflow(backend: &str) {
    let suffix = unique_suffix();
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4443".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown remote server backend {backend}"),
    };
    let bucket = match backend {
        "google" => "app_storage".to_owned(),
        "s3" => format!("rust-remoteserver-{suffix}"),
        _ => unreachable!("unknown remote server backend {backend}"),
    };
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    } else {
        ensure_s3_bucket(&endpoint, &bucket);
    }

    let prefix = format!("{suffix}/uri/remote-server/");
    for (name, flags) in [
        (
            "readonly",
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ),
        (
            "readwrite",
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ),
    ] {
        let missing_prefix = format!("{prefix}missing-{name}/");
        let missing_uri = remote_server_database_uri(backend, &endpoint, &bucket, &missing_prefix);
        let options = RemoteServerOptions::new().database_open_flags(flags);
        let error = RemoteServer::start_with_options(&missing_uri, &options)
            .expect_err("non-creating URI flags must leave a missing database absent");
        assert_eq!(
            error.sqlite_extended_error_code(),
            Some(ffi::SQLITE_NOTFOUND),
            "a missing remote database should preserve SQLITE_NOTFOUND: {error:?}"
        );
        for credential in [
            "remote-server-token",
            "remote-server-access",
            "remote-server-secret",
        ] {
            assert!(!error.to_string().contains(credential));
            assert!(!format!("{error:?}").contains(credential));
        }
        let listing = list_session_objects(backend, &endpoint, &bucket, &missing_prefix);
        assert!(
            !session_listing_contains_object_prefix(backend, &listing, &missing_prefix),
            "a failed non-creating open must not create cloud objects"
        );
    }

    let uri = remote_server_database_uri(backend, &endpoint, &bucket, &prefix);
    let writable_flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let writable_options = RemoteServerOptions::new().database_open_flags(writable_flags);
    let temp = tempfile::tempdir().expect("remote server workflow tempdir");
    let source = Connection::open(temp.path().join("source.db"))
        .expect("create local DoltLite source database");
    source
        .execute_batch(
            "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
             INSERT INTO widgets VALUES(1, 'first value');",
        )
        .expect("write first source commit");
    let _: i64 = source
        .query_row("SELECT dolt_add('-A')", [], |row| row.get(0))
        .expect("stage first source commit");
    let _: String = source
        .query_row("SELECT dolt_commit('-m', 'first')", [], |row| row.get(0))
        .expect("commit first source changes");

    let first_server = RemoteServer::start_with_options(&uri, &writable_options)
        .expect("start URI-owned remote server");
    for credential in [
        "remote-server-token",
        "remote-server-access",
        "remote-server-secret",
    ] {
        assert!(!format!("{first_server:?}").contains(credential));
    }
    let empty_refs = remote_request(first_server.port(), "GET", "/remote.db/refs", &[]);
    assert!(
        empty_refs.starts_with(b"HTTP/1.1 404 "),
        "opening a new cloud URI must not seed a synthetic main ref: {}",
        String::from_utf8_lossy(&empty_refs[..empty_refs.len().min(200)])
    );
    let remote_url = first_server.database_url("remote.db");
    let _: i64 = source
        .query_row(
            "SELECT dolt_remote('add', 'origin', ?1)",
            params![remote_url],
            |row| row.get(0),
        )
        .expect("add first cloud-backed remote");
    let _: i64 = source
        .query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))
        .expect("push first commit to the cloud-backed server");
    let first_hash: String = source
        .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))
        .expect("read first source branch hash");
    let mut first_server = first_server;
    first_server
        .upload()
        .expect("publish first remote protocol push");
    first_server
        .close()
        .expect("close the URI-owned server after publication");

    let readonly_flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let verify_first = Connection::open_with_flags(&uri, readonly_flags)
        .expect("reopen the published database through a fresh SQL connection");
    let first_count: i64 = verify_first
        .query_row("SELECT count(*) FROM widgets", [], |row| row.get(0))
        .expect("read first published row");
    let published_first_hash: String = verify_first
        .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))
        .expect("read first published branch hash");
    assert_eq!(first_count, 1);
    assert_eq!(published_first_hash, first_hash);
    verify_first.close().expect("close fresh SQL reader");

    source
        .execute("INSERT INTO widgets VALUES(2, 'second value')", [])
        .expect("write second source change");
    let _: String = source
        .query_row("SELECT dolt_commit('-A', '-m', 'second')", [], |row| {
            row.get(0)
        })
        .expect("commit second source changes");
    let second_server = RemoteServer::start_with_options(&uri, &writable_options)
        .expect("reopen URI-owned server for the second push");
    let second_remote_url = second_server.database_url("remote.db");
    let _: i64 = source
        .query_row(
            "SELECT dolt_remote('set-url', 'origin', ?1)",
            params![second_remote_url],
            |row| row.get(0),
        )
        .expect("point source remote at the second server");
    let _: i64 = source
        .query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))
        .expect("push second commit using the published refs CAS state");
    let second_hash: String = source
        .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))
        .expect("read second source branch hash");
    let mut second_server = second_server;
    second_server
        .upload()
        .expect("publish the second remote protocol push");
    second_server
        .close()
        .expect("close the URI-owned server after its second publication");

    let verify_second = Connection::open_with_flags(&uri, readonly_flags)
        .expect("open a fresh SQL reader after the second publication");
    let second_count: i64 = verify_second
        .query_row("SELECT count(*) FROM widgets", [], |row| row.get(0))
        .expect("read both published rows");
    let published_second_hash: String = verify_second
        .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))
        .expect("read second published branch hash");
    assert_eq!(second_count, 2);
    assert_eq!(published_second_hash, second_hash);
    verify_second
        .close()
        .expect("close second fresh SQL reader");

    let readonly_options = RemoteServerOptions::new().database_open_flags(readonly_flags);
    let read_server = RemoteServer::start_with_options(&uri, &readonly_options)
        .expect("start a read-only URI-owned server for clone");
    let clone = Connection::open(temp.path().join("clone.db")).expect("create clone database");
    let _: i64 = clone
        .query_row(
            "SELECT dolt_clone(?1)",
            params![read_server.database_url("remote.db")],
            |row| row.get(0),
        )
        .expect("clone from the published cloud-backed remote");
    let clone_count: i64 = clone
        .query_row("SELECT count(*) FROM widgets", [], |row| row.get(0))
        .expect("read cloned rows");
    assert_eq!(clone_count, 2);
    clone.close().expect("close clone database");
    read_server
        .close()
        .expect("close read-only URI-owned server");
    source.close().expect("close source database");
}

#[cfg(feature = "remote")]
fn start_uri_session_server(
    uri: &str,
    session_id: &str,
    operation_id: SessionOperationId,
) -> rusqlite::Result<RemoteServer> {
    let scope = SessionScope::new("emulator-transfer", "remote.db", "push,read")?;
    let session = BlockCacheSessionOptions::for_uri(session_id, scope, operation_id)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    RemoteServer::start_with_options(
        uri,
        &RemoteServerOptions::new()
            .database_open_flags(flags)
            .blockcache_session(session),
    )
}

#[cfg(feature = "remote")]
fn start_graph_uri_session_server(
    uri: &str,
    session_id: &str,
    operation_id: SessionOperationId,
) -> rusqlite::Result<RemoteServer> {
    let scope = SessionScope::new("graph-transfer", "default.db", "push,read")?;
    let session = BlockCacheSessionOptions::for_uri(session_id, scope, operation_id)?;
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    RemoteServer::start_with_options(
        uri,
        &RemoteServerOptions::new()
            .database_open_flags(flags)
            .blockcache_session(session),
    )
}

#[cfg(feature = "remote")]
fn start_uri_session_server_with_progress<F>(
    uri: &str,
    session_id: &str,
    operation_id: SessionOperationId,
    callback: F,
) -> rusqlite::Result<RemoteServer>
where
    F: Fn(UploadProgress) + Send + Sync + 'static,
{
    let scope = SessionScope::new("emulator-transfer", "remote.db", "push,read")?;
    let session = BlockCacheSessionOptions::for_uri(session_id, scope, operation_id)?
        .upload_progress_callback(callback);
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    RemoteServer::start_with_options(
        uri,
        &RemoteServerOptions::new()
            .database_open_flags(flags)
            .blockcache_session(session),
    )
}

#[cfg(feature = "remote")]
fn start_uri_session_server_with_auth<F>(
    uri: &str,
    session_id: &str,
    operation_id: SessionOperationId,
    callback: F,
) -> rusqlite::Result<RemoteServer>
where
    F: Fn(&str, &str, &str, AuthRefreshReason) -> Result<String, AuthError> + Send + Sync + 'static,
{
    let scope = SessionScope::new("emulator-transfer", "remote.db", "push,read")?;
    let session =
        BlockCacheSessionOptions::for_uri(session_id, scope, operation_id)?.auth_callback(callback);
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    RemoteServer::start_with_options(
        uri,
        &RemoteServerOptions::new()
            .database_open_flags(flags)
            .blockcache_session(session),
    )
}

#[cfg(feature = "remote")]
fn seed_uri_session_fault_data(database: &Connection) -> rusqlite::Result<()> {
    database.execute_batch(
        "CREATE TABLE fault_data(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO fault_data VALUES (1, 'seed');",
    )
}

#[cfg(feature = "remote")]
fn configure_uri_session_test_cache(database: &Connection) {
    configure_uri_session_test_cache_with(database, 8 * 1024 * 1024, 50);
}

#[cfg(feature = "remote")]
fn configure_uri_session_test_cache_with(
    database: &Connection,
    cache_bytes: i32,
    stage_watermark: i32,
) {
    let vfs_name = CString::new(
        database
            .blockcachevfs_name()
            .expect("URI session exposes its CBS VFS name"),
    )
    .expect("CBS VFS name is NUL-free");
    let native_vfs = unsafe { ffi::sqlite3_vfs_find(vfs_name.as_ptr()) };
    assert!(!native_vfs.is_null(), "URI session VFS must be registered");
    for (option, value) in [
        (raw_bcv::SQLITE_BCV_CACHESIZE, cache_bytes),
        (raw_bcv::SQLITE_BCV_STAGEWATERMARK, stage_watermark),
    ] {
        let rc = unsafe { raw_bcv::sqlite3_bcvfs_config(native_vfs.cast(), option, value.into()) };
        assert_eq!(rc, ffi::SQLITE_OK, "configure the URI test VFS cache");
    }
}

#[cfg(feature = "remote")]
struct UriSessionHttpProxy {
    port: u16,
    stop: Arc<AtomicBool>,
    reject_commits: Arc<AtomicBool>,
    rejected_commits: Arc<AtomicUsize>,
    completed_refs_if: Arc<AtomicUsize>,
    published_commits: Arc<AtomicUsize>,
    failure: Arc<Mutex<Option<String>>>,
    join: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "remote")]
impl UriSessionHttpProxy {
    fn start(uri: &str) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind URI session proxy");
        listener
            .set_nonblocking(true)
            .expect("configure URI session proxy");
        let port = listener
            .local_addr()
            .expect("URI session proxy address")
            .port();
        let uri = uri.to_owned();
        let stop = Arc::new(AtomicBool::new(false));
        let reject_commits = Arc::new(AtomicBool::new(true));
        let rejected_commits = Arc::new(AtomicUsize::new(0));
        let completed_refs_if = Arc::new(AtomicUsize::new(0));
        let published_commits = Arc::new(AtomicUsize::new(0));
        let failure = Arc::new(Mutex::new(None));

        let thread_stop = Arc::clone(&stop);
        let thread_reject_commits = Arc::clone(&reject_commits);
        let thread_rejected_commits = Arc::clone(&rejected_commits);
        let thread_completed_refs_if = Arc::clone(&completed_refs_if);
        let thread_published_commits = Arc::clone(&published_commits);
        let thread_failure = Arc::clone(&failure);
        let join = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => {
                        if let Ok(mut failure) = thread_failure.lock() {
                            *failure = Some(format!("accept failed: {error}"));
                        }
                        break;
                    }
                };
                if let Err(error) = stream.set_read_timeout(Some(HTTP_TEST_TIMEOUT)) {
                    if let Ok(mut failure) = thread_failure.lock() {
                        *failure = Some(format!("set request timeout failed: {error}"));
                    }
                    break;
                }
                let Some(request) = read_http_request(&mut stream) else {
                    continue;
                };
                let (method, path, body) = request_parts(&request);
                let Some((session_id, native_path)) = session_request_path(&path) else {
                    if let Ok(mut failure) = thread_failure.lock() {
                        *failure = Some(format!(
                            "request path={path} has no capability session prefix"
                        ));
                    }
                    let _ = stream.write_all(&empty_http_response(400));
                    continue;
                };
                let route = request_route(&native_path);
                if request_header(&request, "Idempotency-Token").is_some() {
                    if let Ok(mut failure) = thread_failure.lock() {
                        *failure = Some(format!(
                            "native request route={route} unexpectedly carried Idempotency-Token"
                        ));
                    }
                    let _ = stream.write_all(&empty_http_response(400));
                    continue;
                }
                let Some(native_request) = request_with_path(&request, &native_path) else {
                    if let Ok(mut failure) = thread_failure.lock() {
                        *failure = Some(format!("could not rewrite request path for {route}"));
                    }
                    let _ = stream.write_all(&empty_http_response(400));
                    continue;
                };
                if route == "commit" && thread_reject_commits.load(Ordering::Acquire) {
                    thread_rejected_commits.fetch_add(1, Ordering::AcqRel);
                    let _ = stream.write_all(&empty_http_response(503));
                    continue;
                }

                let mutation = matches!(route, "chunks" | "refs-if" | "commit");
                let operation_id = if mutation {
                    match SessionOperationId::from_request(&method, &native_path, &body) {
                        Ok(operation_id) => operation_id,
                        Err(error) => {
                            if let Ok(mut failure) = thread_failure.lock() {
                                *failure = Some(format!("derive mutation operation ID: {error}"));
                            }
                            let _ = stream.write_all(&empty_http_response(400));
                            continue;
                        }
                    }
                } else {
                    SessionOperationId::random()
                };
                let mut server = match start_uri_session_server(&uri, session_id, operation_id) {
                    Ok(server) => server,
                    Err(error) => {
                        if let Ok(mut failure) = thread_failure.lock() {
                            *failure =
                                Some(format!("start URI session server for {route}: {error}"));
                        }
                        let _ = stream.write_all(&empty_http_response(500));
                        continue;
                    }
                };
                let response = if !mutation {
                    let native_request = send_http_request_raw_with_timeout(
                        server.port(),
                        &native_request,
                        SESSION_HTTP_TEST_TIMEOUT,
                    );
                    if let Err(error) = server.quiesce().and_then(|()| server.close()) {
                        if let Ok(mut failure) = thread_failure.lock() {
                            *failure = Some(format!("close read request {route}: {error}"));
                        }
                        empty_http_response(500)
                    } else {
                        native_request
                    }
                } else {
                    match server.operation_status() {
                        Err(error) => {
                            let _ = server.quiesce();
                            let _ = server.close();
                            if let Ok(mut failure) = thread_failure.lock() {
                                *failure =
                                    Some(format!("read operation status for {route}: {error}"));
                            }
                            empty_http_response(500)
                        }
                        Ok(SessionOperationStatus::New) => {
                            let native_request = send_http_request_raw_with_timeout(
                                server.port(),
                                &native_request,
                                SESSION_HTTP_TEST_TIMEOUT,
                            );
                            let status = http_status(&native_request);
                            if status >= 400 {
                                if let Ok(mut failure) = thread_failure.lock() {
                                    *failure = Some(format!(
                                        "native route={route} method={method} returned {status}: {}",
                                        String::from_utf8_lossy(&native_request)
                                            .chars()
                                            .take(512)
                                            .collect::<String>()
                                    ));
                                }
                                let _ = server.quiesce();
                                let _ = server.close();
                                native_request
                            } else {
                                let completion = if route == "commit" {
                                    server.upload()
                                } else {
                                    server.complete_request()
                                };
                                let result = completion.and_then(|()| server.close());
                                if let Err(error) = result {
                                    if let Ok(mut failure) = thread_failure.lock() {
                                        *failure =
                                            Some(format!("complete mutation {route}: {error}"));
                                    }
                                    empty_http_response(500)
                                } else {
                                    if route == "refs-if" {
                                        thread_completed_refs_if.fetch_add(1, Ordering::AcqRel);
                                    }
                                    if route == "commit" {
                                        thread_published_commits.fetch_add(1, Ordering::AcqRel);
                                    }
                                    native_request
                                }
                            }
                        }
                        Ok(SessionOperationStatus::Accepted) if route == "commit" => {
                            let result = server.upload().and_then(|()| server.close());
                            if let Err(error) = result {
                                if let Ok(mut failure) = thread_failure.lock() {
                                    *failure = Some(format!("publish accepted /commit: {error}"));
                                }
                                empty_http_response(500)
                            } else {
                                thread_published_commits.fetch_add(1, Ordering::AcqRel);
                                empty_http_response(200)
                            }
                        }
                        Ok(
                            SessionOperationStatus::Accepted | SessionOperationStatus::Committed,
                        ) => {
                            let result = server.quiesce().and_then(|()| server.close());
                            if let Err(error) = result {
                                if let Ok(mut failure) = thread_failure.lock() {
                                    *failure =
                                        Some(format!("close accepted retry {route}: {error}"));
                                }
                                empty_http_response(500)
                            } else {
                                empty_http_response(200)
                            }
                        }
                        Ok(SessionOperationStatus::Failed | SessionOperationStatus::Conflict) => {
                            let _ = server.quiesce();
                            let _ = server.close();
                            empty_http_response(409)
                        }
                    }
                };
                if let Err(error) = stream.write_all(&response) {
                    if let Ok(mut failure) = thread_failure.lock() {
                        *failure = Some(format!("write response for {route}: {error}"));
                    }
                }
            }
        });

        Self {
            port,
            stop,
            reject_commits,
            rejected_commits,
            completed_refs_if,
            published_commits,
            failure,
            join: Some(join),
        }
    }

    fn allow_commits(&self) {
        self.reject_commits.store(false, Ordering::Release);
    }

    fn database_url(&self, session_id: &str, database: &str) -> String {
        format!(
            "http://127.0.0.1:{}/capability/{session_id}/{database}",
            self.port
        )
    }

    fn failure(&self) -> Option<String> {
        self.failure.lock().expect("lock URI proxy failure").clone()
    }

    fn rejected_commit_count(&self) -> usize {
        self.rejected_commits.load(Ordering::Acquire)
    }

    fn completed_refs_if_count(&self) -> usize {
        self.completed_refs_if.load(Ordering::Acquire)
    }

    fn published_commit_count(&self) -> usize {
        self.published_commits.load(Ordering::Acquire)
    }
}

#[cfg(feature = "remote")]
impl Drop for UriSessionHttpProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(join) = self.join.take() {
            if let Err(panic) = join.join() {
                if let Ok(mut failure) = self.failure.lock() {
                    if failure.is_none() {
                        *failure = Some(format!("URI proxy thread panic: {panic:?}"));
                    }
                }
            }
        }
    }
}

#[cfg(feature = "remote")]
fn request_header(request: &[u8], wanted: &str) -> Option<String> {
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")?;
    let header = String::from_utf8_lossy(&request[..header_end]);
    header.lines().skip(1).find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case(wanted)
            .then(|| value.trim().to_owned())
    })
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_session_publication_recovers_after_ordinary_upload() {
    let suffix = unique_suffix();
    let endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let prefix = format!("{suffix}/uri/session-publication-interoperability/");
    let uri = remote_server_database_uri("google", &endpoint, bucket, &prefix);
    let write_flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let read_flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;

    let initial = Connection::open_with_flags(&uri, write_flags)
        .expect("create URI database before session publication");
    initial
        .execute_batch("PRAGMA user_version = 1;")
        .expect("write initial ordinary URI database version");
    initial
        .upload()
        .expect("publish initial ordinary URI database");
    initial
        .close()
        .expect("close initial ordinary URI database");

    let mut first =
        start_uri_session_server(&uri, &Uuid::new_v4().to_string(), session_operation_id(1))
            .expect("start first session publication");
    first
        .complete_request()
        .expect("accept first session publication");
    first.upload().expect("publish first session head");
    first.close().expect("close first session publisher");

    // An ordinary URI upload legitimately advances the manifest after the
    // terminal session record was committed. A fresh session must be able to
    // publish against this newly captured base.
    let ordinary = Connection::open_with_flags(&uri, write_flags)
        .expect("open ordinary URI writer after session publication");
    ordinary
        .execute_batch("PRAGMA user_version = 2;")
        .expect("write ordinary URI database version");
    ordinary
        .upload()
        .expect("publish ordinary URI update after session publication");
    ordinary
        .close()
        .expect("close ordinary URI writer after session publication");

    let mut fresh =
        start_uri_session_server(&uri, &Uuid::new_v4().to_string(), session_operation_id(2))
            .expect("start fresh session after ordinary upload");
    fresh
        .complete_request()
        .expect("accept fresh session after ordinary upload");
    fresh
        .upload()
        .expect("publish fresh session after ordinary upload");
    fresh.close().expect("close fresh session publisher");
    let positive_reader =
        Connection::open_with_flags(&uri, read_flags).expect("read the fresh session publication");
    let published_version: i64 = positive_reader
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read ordinary update published by the fresh session");
    assert_eq!(published_version, 2);
    positive_reader
        .close()
        .expect("close fresh session publication reader");

    // Once a session has accepted its candidate, a later ordinary upload
    // changes its captured base. That stale candidate must still be rejected.
    let mut stale =
        start_uri_session_server(&uri, &Uuid::new_v4().to_string(), session_operation_id(3))
            .expect("start session for stale-base check");
    stale
        .complete_request()
        .expect("accept candidate for stale-base check");
    let external = Connection::open_with_flags(&uri, write_flags)
        .expect("open external URI writer after candidate acceptance");
    external
        .execute_batch("PRAGMA user_version = 3;")
        .expect("write external URI update after candidate acceptance");
    external
        .upload()
        .expect("publish external update after candidate acceptance");
    external
        .close()
        .expect("close external URI writer after candidate acceptance");
    let error = stale
        .upload()
        .expect_err("reject a session candidate whose base manifest is stale");
    assert_eq!(
        error.sqlite_extended_error_code(),
        Some(ffi::SQLITE_CONSTRAINT),
        "stale base should remain a session conflict: {error:?}"
    );
    stale.close().expect("close stale session publisher");

    let reader = Connection::open_with_flags(&uri, read_flags)
        .expect("read the external manifest after stale-base rejection");
    let user_version: i64 = reader
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("read external manifest version");
    assert_eq!(user_version, 3);
    reader.close().expect("close external manifest reader");
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_session_handoff_keeps_accepted_graph_private_until_upload() {
    let suffix = unique_suffix();
    let endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let prefix = format!("{suffix}/uri/session-handoff/");
    let uri = remote_server_database_uri("google", &endpoint, bucket, &prefix);
    let read_only_flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let temp = tempfile::tempdir().expect("URI session workflow tempdir");
    let source = Connection::open(temp.path().join("source.db"))
        .expect("create URI session source database");
    source
        .execute_batch(
            "CREATE TABLE widgets(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO widgets VALUES(1, 'accepted');",
        )
        .expect("create source row");
    let _: String = source
        .query_row("SELECT dolt_commit('-A', '-m', 'accepted')", [], |row| {
            row.get(0)
        })
        .expect("commit source row");

    let session_id = Uuid::new_v4().to_string();
    let proxy = UriSessionHttpProxy::start(&uri);
    let _: i64 = source
        .query_row(
            "SELECT dolt_remote('add', 'origin', ?1)",
            params![proxy.database_url(&session_id, "remote.db")],
            |row| row.get(0),
        )
        .expect("add URI session proxy remote");
    let first_push: rusqlite::Result<i64> =
        source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0));
    assert!(
        first_push.is_err(),
        "the injected first /commit must fail: {first_push:?}"
    );
    assert!(
        proxy.rejected_commit_count() > 0,
        "first push error={first_push:?}; refs-if completions={}; proxy failure={:?}",
        proxy.completed_refs_if_count(),
        proxy.failure()
    );
    assert!(
        proxy.completed_refs_if_count() > 0,
        "the refs-if request must be durably accepted before the injected /commit failure"
    );
    assert_eq!(proxy.published_commit_count(), 0);
    assert!(
        proxy.failure().is_none(),
        "proxy failure: {:?}",
        proxy.failure()
    );

    let unpublished = Connection::open_with_flags(&uri, read_only_flags)
        .expect("read the fresh published database after accepted refs-if");
    let unpublished_branches: i64 = unpublished
        .query_row("SELECT count(*) FROM dolt_branches", [], |row| row.get(0))
        .expect("read published branch list before commit");
    assert_eq!(unpublished_branches, 0);
    unpublished
        .close()
        .expect("close SQL reader before publication");

    proxy.allow_commits();
    let retry: i64 = source
        .query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))
        .expect("retry the push with the same capability URL");
    let _ = retry;
    assert!(proxy.published_commit_count() > 0);
    assert!(
        proxy.failure().is_none(),
        "proxy failure: {:?}",
        proxy.failure()
    );

    let published = Connection::open_with_flags(&uri, read_only_flags)
        .expect("open ordinary reader after terminal publication");
    let published_branches: i64 = published
        .query_row("SELECT count(*) FROM dolt_branches", [], |row| row.get(0))
        .expect("read published branch list");
    let published_rows: i64 = published
        .query_row("SELECT count(*) FROM widgets", [], |row| row.get(0))
        .expect("read graph rows from published push");
    assert_eq!(published_branches, 1);
    assert_eq!(published_rows, 1);
    published
        .close()
        .expect("close ordinary reader after publication");
    source.close().expect("close URI session source database");
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_staged_graph_is_private_until_server_publication() {
    let suffix = unique_suffix();
    let endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let prefix = format!("{suffix}/uri/direct-session-publish/");
    let uri = format!(
        "gcs://{bucket}/{prefix}?vfs=blockcachevfs&project=test-project&access_token=remote-server-token&endpoint={endpoint}&database=default.db"
    );
    let read_only_flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let session_id = Uuid::new_v4().to_string();
    let operation_id = || {
        SessionOperationId::from_request("POST", "/default.db/commit", b"")
            .expect("derive stable graph publication operation ID")
    };
    let temp = tempfile::tempdir().expect("staged graph tempdir");
    let source =
        Connection::open(temp.path().join("source.db")).expect("create direct GCS push source");
    source
        .execute_batch(
            "CREATE TABLE widgets(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO widgets VALUES(1, 'staged');",
        )
        .expect("write source graph row");
    let _: String = source
        .query_row(
            "SELECT dolt_commit('-A', '-m', 'staged graph')",
            [],
            |row| row.get(0),
        )
        .expect("commit source graph");

    let mut client_server = start_graph_uri_session_server(&uri, &session_id, operation_id())
        .expect("start client-owned session server");
    let manifest_name = format!("{prefix}manifest.bcv");
    let initial_manifest = fetch_google_object(&endpoint, bucket, &manifest_name);
    let remote_url = client_server.database_url("default.db");
    let _: i64 = source
        .query_row(
            "SELECT dolt_remote('add', 'origin', ?1)",
            params![remote_url],
            |row| row.get(0),
        )
        .expect("add direct GCS session server as source remote");
    let _: i64 = source
        .query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))
        .expect("push committed graph to the client-owned session server");
    let pushed_hash: String = source
        .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))
        .expect("read source branch hash");
    let client_database = client_server
        .database_connection()
        .expect("running URI server exposes its database for inspection");
    let staged_hash: String = client_database
        .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))
        .expect("read accepted branch before staging");
    assert_eq!(staged_hash, pushed_hash);

    client_server
        .stage_request()
        .expect("checkpoint and accept the graph without publication");
    assert!(
        client_server.database_connection().is_none(),
        "the SQL accessor must be unavailable after the server is quiesced"
    );
    assert_eq!(
        client_server
            .operation_status()
            .expect("read accepted operation status"),
        SessionOperationStatus::Accepted
    );
    client_server
        .close()
        .expect("close staged client server without publication");

    assert_eq!(
        fetch_google_object(&endpoint, bucket, &manifest_name),
        initial_manifest,
        "staging must leave the published manifest byte-for-byte unchanged"
    );
    let listing = list_google_objects(&endpoint, bucket, &prefix);
    let blocks_prefix = format!("{prefix}blocks/");
    let attempt_prefix = format!("{prefix}bcv-session/v1/attempt/{session_id}/");
    let checkpoint_prefix = format!("{prefix}bcv-session/v1/checkpoint/{session_id}/");
    let head_object = format!("{prefix}bcv-session/v1/head/{session_id}.bcv");
    let guard_object = format!("{prefix}bcv-session/v1/guard.bcv");
    assert!(
        !session_object_exists("google", &endpoint, bucket, &guard_object),
        "client staging must not create or mutate the global cleanup guard"
    );
    assert!(
        session_listing_contains_object_prefix("google", &listing, &blocks_prefix),
        "staged graph blocks must use the CAB-separable blocks/ prefix: {listing}"
    );
    assert!(
        session_listing_contains_object_prefix("google", &listing, &attempt_prefix),
        "stage must persist an attempt marker below this session ID: {listing}"
    );
    assert!(
        session_listing_contains_object_prefix("google", &listing, &checkpoint_prefix),
        "stage must persist an accepted checkpoint below this session ID: {listing}"
    );
    assert!(
        session_object_exists("google", &endpoint, bucket, &head_object),
        "stage must persist this session's accepted head"
    );
    assert!(
        !session_listing_contains_object_prefix(
            "google",
            &listing,
            &format!("{prefix}bcv-session/v1/candidate/")
        ),
        "client staging must not create server publication candidates: {listing}"
    );
    assert!(
        !session_listing_contains_object_prefix(
            "google",
            &listing,
            &format!("{prefix}bcv-session/v1/publication/")
        ),
        "client staging must not create server publication records: {listing}"
    );

    let unpublished = Connection::open_with_flags(&uri, read_only_flags)
        .expect("read published graph before final server upload");
    let unpublished_branches: i64 = unpublished
        .query_row("SELECT count(*) FROM dolt_branches", [], |row| row.get(0))
        .expect("read published refs before final upload");
    assert_eq!(unpublished_branches, 0);
    unpublished
        .close()
        .expect("close reader before final server upload");

    let mut server = start_graph_uri_session_server(&uri, &session_id, operation_id())
        .expect("reattach the accepted operation as the server owner");
    assert_eq!(
        server
            .operation_status()
            .expect("read reattached accepted operation status"),
        SessionOperationStatus::Accepted
    );
    let server_database = server
        .database_connection()
        .expect("reattached server can inspect the accepted graph before publication");
    let accepted_hash: String = server_database
        .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))
        .expect("inspect accepted graph before final upload");
    assert_eq!(accepted_hash, pushed_hash);
    let accepted_rows: i64 = server_database
        .query_row("SELECT count(*) FROM widgets", [], |row| row.get(0))
        .expect("inspect accepted graph rows before final upload");
    assert_eq!(accepted_rows, 1);

    server
        .upload()
        .expect("server publishes the accepted manifest");
    assert!(
        server.database_connection().is_none(),
        "the SQL accessor must be unavailable after upload closes the URI handle"
    );
    server.close().expect("close final server publisher");

    let published = Connection::open_with_flags(&uri, read_only_flags)
        .expect("open published graph after final server upload");
    let published_hash: String = published
        .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))
        .expect("read published branch hash");
    let published_rows: i64 = published
        .query_row("SELECT count(*) FROM widgets", [], |row| row.get(0))
        .expect("read published graph row");
    assert_eq!(published_hash, pushed_hash);
    assert_eq!(published_rows, 1);
    published.close().expect("close published graph reader");
    source.close().expect("close direct GCS push source");
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_cleanup_deletes_prefixed_blocks() {
    let suffix = unique_suffix();
    let endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let prefix = format!("{suffix}/uri/block-cleanup");
    let container = format!("{bucket}/{prefix}");
    let storage = Storage::google_json_with_endpoint("test-project", container, &endpoint);
    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("VFS builder")
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize block-cache VFS");
    vfs.initialize_container(&storage)
        .expect("initialize empty container manifest");
    let temp = tempfile::tempdir().expect("cleanup seed tempdir");
    let seed_path = temp.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create cleanup seed database");
    seed.execute_batch("CREATE TABLE keep(value TEXT); INSERT INTO keep VALUES ('live');")
        .expect("write cleanup seed database");
    seed.close().expect("close cleanup seed database");
    vfs.create_database(&storage, &seed_path, "seed.sqlite")
        .expect("publish a valid database before orphan cleanup");

    let alias = format!("cleanup_{}", std::process::id());
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&alias))
        .expect("attach database before scheduling block cleanup");
    let control = vfs
        .open(&format!("/{alias}"))
        .expect("open attached container control connection");
    let block_id: Vec<u8> = control
        .query_row(
            "SELECT blockid FROM bcv_block \
             WHERE container = ?1 AND database = 'seed.sqlite' \
               AND blockid IS NOT NULL LIMIT 1",
            [&alias],
            |row| row.get(0),
        )
        .expect("read an existing block ID from the manifest");
    drop(control);
    assert_eq!(block_id.len(), 16, "test fixture uses a 16-byte block ID");
    let block_id = block_id
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<String>();
    let flat_object = format!("{prefix}/{block_id}.bcv");
    let blocks_object = format!("{prefix}/blocks/{block_id}.bcv");
    assert!(
        session_object_exists("google", &endpoint, bucket, &blocks_object),
        "new database blocks should live beneath the blocks/ namespace"
    );
    assert!(!session_object_exists(
        "google",
        &endpoint,
        bucket,
        &flat_object
    ));

    vfs.delete_database(&alias, "seed.sqlite")
        .expect("schedule deletion of manifest-referenced blocks");
    vfs.upload(&alias)
        .expect("publish database deletion and its garbage-collection list");
    vfs.detach(&alias)
        .expect("detach after publishing database deletion");
    vfs.cleanup(&storage, Duration::ZERO)
        .expect("remove zero-age deleted blocks from the blocks/ namespace");
    let remaining = list_google_objects(&endpoint, bucket, &prefix);
    assert!(
        !session_object_exists("google", &endpoint, bucket, &blocks_object),
        "cleanup must delete blocks/ prefixed block keys: {remaining}"
    );

    let guard_object = format!("{prefix}/bcv-session/v1/guard.bcv");
    let first_idle_epoch = fetch_google_object(&endpoint, bucket, &guard_object);
    vfs.cleanup(&storage, Duration::ZERO)
        .expect("run another sweep with no remaining blocks");
    let second_idle_epoch = fetch_google_object(&endpoint, bucket, &guard_object);
    assert_ne!(
        first_idle_epoch, second_idle_epoch,
        "each sweep must rotate the idle guard epoch, including when the ETag is content-derived"
    );
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_remote_server_publishes_repeated_pushes() {
    run_uri_remote_server_push_workflow("google");
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn s3_uri_remote_server_publishes_repeated_pushes() {
    run_uri_remote_server_push_workflow("s3");
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_upload_of_direct_remote_chunks_does_not_create_refs() {
    const EMPTY_PROLLY_HASH_HEX: &str = "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9";
    // DoltLite's ProllyHash stores the first 20 bytes of BLAKE3(empty).
    const EMPTY_PROLLY_HASH: [u8; 20] = [
        0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, 0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9,
        0x49, 0x9b, 0xcb, 0x25, 0xc9,
    ];

    let endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".into());
    let bucket = "app_storage";
    let prefix = format!("{}/uri/remote-upload/", unique_suffix());
    ensure_google_bucket(&endpoint, bucket);
    let storage =
        Storage::google_json_with_endpoint("test-project", format!("{bucket}/{prefix}"), &endpoint);

    let cache = tempfile::tempdir().expect("CBS cache directory");
    let vfs = BlockCacheVfs::builder(cache.path())
        .expect("CBS VFS builder")
        .auth_callback(|_, _, _| Ok("test-token".to_owned()))
        .init_owned()
        .expect("initialize CBS VFS");
    vfs.initialize_container(&storage)
        .expect("initialize remote CBS container");
    let alias = format!("remote_{}", std::process::id());
    vfs.attach(&AttachSpec::new(storage).alias(&alias))
        .expect("attach remote CBS container");
    let directory = format!("/{alias}");
    let options = RemoteServerOptions::new().vfs_name(vfs.name());

    let server = RemoteServer::start_with_options(&directory, &options)
        .expect("start CBS-backed remote server");
    let response = remote_request(server.port(), "GET", "/default.db/refs", &[]);
    assert!(
        response.starts_with(b"HTTP/1.1 404 "),
        "uninitialized graph refs should be absent before upload: {}",
        String::from_utf8_lossy(&response[..response.len().min(160)])
    );

    let mut chunk_body = EMPTY_PROLLY_HASH.to_vec();
    chunk_body.extend_from_slice(&0_u32.to_le_bytes());
    let response = remote_request(server.port(), "POST", "/default.db/chunks", &chunk_body);
    assert!(
        response.starts_with(b"HTTP/1.1 200 "),
        "direct remote chunk upload should succeed: {}",
        String::from_utf8_lossy(&response[..response.len().min(160)])
    );
    let chunk_path = format!("/default.db/chunk/{EMPTY_PROLLY_HASH_HEX}");
    let response = remote_request(server.port(), "GET", &chunk_path, &[]);
    assert!(
        response.starts_with(b"HTTP/1.1 200 "),
        "the direct upload should make its chunk readable: {}",
        String::from_utf8_lossy(&response[..response.len().min(160)])
    );
    let response = remote_request(server.port(), "GET", "/default.db/refs", &[]);
    assert!(
        response.starts_with(b"HTTP/1.1 404 "),
        "chunk upload should not create refs before CBS checkpoint: {}",
        String::from_utf8_lossy(&response[..response.len().min(160)])
    );
    drop(server);

    vfs.upload(&alias)
        .expect("upload the remotely received chunk through CBS");

    let server = RemoteServer::start_with_options(&directory, &options)
        .expect("restart CBS-backed remote server after upload");
    let response = remote_request(server.port(), "GET", &chunk_path, &[]);
    assert!(
        response.starts_with(b"HTTP/1.1 200 "),
        "CBS upload should persist the received chunk: {}",
        String::from_utf8_lossy(&response[..response.len().min(160)])
    );
    let response = remote_request(server.port(), "GET", "/default.db/refs", &[]);
    assert!(
        response.starts_with(b"HTTP/1.1 404 "),
        "CBS checkpoint must not create refs before refs-if: {}",
        String::from_utf8_lossy(&response[..response.len().min(160)])
    );
    drop(server);
    vfs.detach(&alias).expect("detach uploaded CBS container");
}

#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_connection_creates_empty_database_and_resolves_races() {
    run_uri_auto_create("google");
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn s3_uri_connection_creates_empty_database_and_resolves_races() {
    run_uri_auto_create("s3");
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_session_remote_server_accepts_chunk_over_public_request_limit() {
    const CHUNK_BYTES: usize = 128 * 1024 * 1024 + 1;

    let endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".into());
    let bucket = "app_storage";
    let prefix = format!("{}/uri/session-large-chunk/", unique_suffix());
    ensure_google_bucket(&endpoint, bucket);
    let uri = format!(
        "gcs://{bucket}/{prefix}?vfs=blockcachevfs&project=test-project&access_token=test-token&endpoint={endpoint}&database=default.db"
    );
    let session_id = Uuid::new_v4().to_string();
    let server = start_graph_uri_session_server(&uri, &session_id, session_operation_id(253))
        .expect("start URI-session remote server");

    let response = remote_request_large_chunk(server.port(), CHUNK_BYTES);
    assert!(
        response.starts_with(b"HTTP/1.1 400 Bad Request\r\n"),
        "the session loopback server should parse a 128MiB+ chunk and reject only its invalid hash, not its size: {}",
        String::from_utf8_lossy(&response[..response.len().min(200)])
    );
    server.close().expect("close URI-session remote server");
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_session_reports_complete_block_upload_progress() {
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let proxy = StorageFaultProxy::start(&endpoint);

    let prefix = format!("{}/uri/upload-progress/", unique_suffix());
    let uri = remote_server_database_uri("google", &proxy.url, bucket, &prefix);
    let progress_events = Arc::new(Mutex::new(Vec::<UploadProgress>::new()));
    let callback_events = Arc::clone(&progress_events);
    let panic_once = Arc::new(AtomicBool::new(true));
    let callback_panic_once = Arc::clone(&panic_once);
    let session_id = Uuid::new_v4().to_string();
    let operation_id =
        SessionOperationId::from_request("POST", "/remote.db/commit", session_id.as_bytes())
            .expect("derive upload-progress operation ID");
    let mut server =
        start_uri_session_server_with_progress(&uri, &session_id, operation_id, move |progress| {
            callback_events
                .lock()
                .expect("lock upload progress events")
                .push(progress);
            if callback_panic_once.swap(false, Ordering::AcqRel) {
                panic!("test that upload progress panics are contained");
            }
        })
        .expect("start upload-progress URI session");
    let database = server
        .database_connection()
        .expect("URI session exposes its SQLite anchor");
    configure_uri_session_test_cache(database);
    seed_uri_session_fault_data(database).expect("seed upload-progress database");
    let baseline = progress_events
        .lock()
        .expect("lock initial upload progress")
        .last()
        .copied()
        .unwrap_or_default();
    progress_events
        .lock()
        .expect("lock upload progress events")
        .clear();
    proxy.configure(
        StorageFaultTarget::BlockPut,
        StorageFaultAction::CreateThenReject,
    );
    proxy.clear_block_put_observations();
    proxy.arm();
    let stage_result = stage_session_update(database);
    proxy.disarm();
    assert!(
        stage_result.is_ok(),
        "write enough data to produce multiple blocks: {stage_result:?}; matched={}; observations={:?}; progress={:?}",
        proxy.matched(),
        proxy.block_put_observations(),
        progress_events
            .lock()
            .expect("lock upload progress after staging error")
            .as_slice()
    );
    let producer_events = progress_events
        .lock()
        .expect("lock producer upload progress events")
        .clone();
    assert!(
        producer_events
            .iter()
            .all(|progress| progress.expected.is_none()),
        "the block-work total must stay unknown while DoltLite is producing data: {producer_events:?}"
    );
    server
        .complete_request()
        .expect("checkpoint blocks and accept a verified immutable reuse");
    assert!(
        server.first_storage_error().is_none(),
        "a verified create-only reuse and missing first-use guard are not terminal storage failures"
    );

    let observations = proxy.block_put_observations();
    let reused = observations
        .iter()
        .find(|observation| observation.injected_status == Some(412))
        .expect("proxy should create one block then return a precondition failure");
    let created_puts = observations
        .iter()
        .filter(|observation| observation.injected_status.is_none())
        .collect::<Vec<_>>();
    assert!(
        created_puts.len() >= 2,
        "expected multi-block PUTs: {observations:?}"
    );
    let events = progress_events
        .lock()
        .expect("lock upload progress events")
        .clone();
    assert!(
        !events.is_empty(),
        "successful blocks should report progress"
    );
    assert!(
        events.windows(2).all(|pair| {
            pair[0].uploaded_blocks <= pair[1].uploaded_blocks
                && pair[0].uploaded_bytes <= pair[1].uploaded_bytes
                && pair[0].reused_blocks <= pair[1].reused_blocks
                && pair[0].reused_bytes <= pair[1].reused_bytes
        }),
        "progress snapshots should be cumulative: {events:?}"
    );
    let plan_index = events
        .iter()
        .position(|progress| progress.expected.is_some())
        .expect("the explicit final checkpoint should report a fixed block-work plan");
    assert!(
        events[..plan_index]
            .iter()
            .all(|progress| progress.expected.is_none()),
        "no denominator should appear during producer and proactive staging: {events:?}"
    );
    let plan = events[plan_index]
        .expected
        .expect("the final checkpoint plan should be present");
    assert!(
        events[plan_index..]
            .iter()
            .all(|progress| progress.expected == Some(plan)),
        "the final checkpoint denominator should remain fixed while remaining blocks are handled: {events:?}"
    );
    assert!(
        events[plan_index..].iter().all(|progress| {
            progress.uploaded_blocks + progress.reused_blocks <= plan.blocks
                && progress.uploaded_bytes + progress.reused_bytes <= plan.bytes
        }),
        "block and payload progress must not exceed the fixed plan: {events:?}"
    );
    let final_progress = *events.last().expect("at least one progress event");
    assert_eq!(
        final_progress.uploaded_blocks - baseline.uploaded_blocks,
        created_puts.len() as u64,
        "only complete successful block PUTs should count as uploaded"
    );
    assert_eq!(
        final_progress.uploaded_bytes - baseline.uploaded_bytes,
        created_puts
            .iter()
            .map(|put| put.payload_bytes as u64)
            .sum::<u64>()
    );
    assert_eq!(final_progress.reused_blocks - baseline.reused_blocks, 1);
    assert_eq!(
        final_progress.reused_bytes - baseline.reused_bytes,
        reused.payload_bytes as u64,
        "a 412 should count as reuse only after reading and matching the complete object"
    );
    assert_eq!(
        plan.blocks,
        final_progress.uploaded_blocks + final_progress.reused_blocks,
        "the final plan should equal all completed uploads and verified reuses"
    );
    assert_eq!(
        plan.bytes,
        final_progress.uploaded_bytes + final_progress.reused_bytes,
        "the plan byte count includes complete block payloads, including reuse"
    );
    server.close().expect("close upload-progress URI session");

    let rejected_prefix = format!("{}/uri/upload-progress-rejected/", unique_suffix());
    let rejected_uri = remote_server_database_uri("google", &proxy.url, bucket, &rejected_prefix);
    let rejected_events = Arc::new(Mutex::new(Vec::<UploadProgress>::new()));
    let callback_rejected_events = Arc::clone(&rejected_events);
    let rejected_session_id = Uuid::new_v4().to_string();
    let rejected_operation = SessionOperationId::from_request(
        "POST",
        "/remote.db/commit",
        rejected_session_id.as_bytes(),
    )
    .expect("derive rejected-progress operation ID");
    let rejected_server = start_uri_session_server_with_progress(
        &rejected_uri,
        &rejected_session_id,
        rejected_operation,
        move |progress| {
            callback_rejected_events
                .lock()
                .expect("lock rejected upload progress events")
                .push(progress);
        },
    )
    .expect("start rejected-upload URI session");
    let database = rejected_server
        .database_connection()
        .expect("rejected URI session exposes its SQLite anchor");
    seed_uri_session_fault_data(database).expect("seed rejected-upload database");
    configure_uri_session_test_cache(database);
    rejected_events
        .lock()
        .expect("lock pre-rejection upload progress events")
        .clear();
    proxy.configure(StorageFaultTarget::BlockPut, StorageFaultAction::Reject);
    proxy.clear_block_put_observations();
    proxy.arm();
    let rejected_result =
        stage_session_small_update(database, "FULL").and_then(|_| database.execute_batch("COMMIT"));
    proxy.disarm();
    assert!(
        rejected_result.is_err(),
        "a rejected block with no matching object must fail; matched={}; observations={:?}",
        proxy.matched(),
        proxy.block_put_observations()
    );
    assert!(
        proxy
            .block_put_observations()
            .iter()
            .any(|observation| observation.injected_status == Some(412)),
        "proxy should reject the block PUT"
    );
    assert!(
        rejected_events
            .lock()
            .expect("lock rejected upload progress events")
            .is_empty(),
        "failed PUTs and missing-object verification must not advance progress"
    );
    assert!(
        rejected_events
            .lock()
            .expect("lock rejected upload progress events")
            .iter()
            .all(|progress| progress.expected.is_none()),
        "a failed producer stage must not report a final denominator"
    );
    assert!(
        matches!(
            rejected_server.first_storage_error(),
            Some(failure)
                if failure.phase == StorageFailurePhase::VerifyExistingBlock
                    && failure.cause == StorageFailureCause::HttpStatus(404)
        ),
        "a 412 followed by a missing immutable object should report terminal verification failure: {:?}",
        rejected_server.first_storage_error()
    );
    drop(rejected_server);

    let corrupt_prefix = format!("{}/uri/upload-progress-corrupt/", unique_suffix());
    let corrupt_uri = remote_server_database_uri("google", &proxy.url, bucket, &corrupt_prefix);
    let corrupt_events = Arc::new(Mutex::new(Vec::<UploadProgress>::new()));
    let callback_corrupt_events = Arc::clone(&corrupt_events);
    let corrupt_session_id = Uuid::new_v4().to_string();
    let corrupt_operation = SessionOperationId::from_request(
        "POST",
        "/remote.db/commit",
        corrupt_session_id.as_bytes(),
    )
    .expect("derive corrupt-progress operation ID");
    let corrupt_server = start_uri_session_server_with_progress(
        &corrupt_uri,
        &corrupt_session_id,
        corrupt_operation,
        move |progress| {
            callback_corrupt_events
                .lock()
                .expect("lock corrupt upload progress events")
                .push(progress);
        },
    )
    .expect("start corrupt-upload URI session");
    let database = corrupt_server
        .database_connection()
        .expect("corrupt URI session exposes its SQLite anchor");
    configure_uri_session_test_cache(database);
    seed_uri_session_fault_data(database).expect("seed corrupt-upload database");
    let baseline = corrupt_events
        .lock()
        .expect("lock initial corrupt upload progress")
        .last()
        .copied()
        .unwrap_or_default();
    corrupt_events
        .lock()
        .expect("lock corrupt upload progress events")
        .clear();
    proxy.configure(
        StorageFaultTarget::BlockPut,
        StorageFaultAction::CreateCorruptedThenReject,
    );
    proxy.clear_block_put_observations();
    proxy.arm();
    let corrupt_result =
        stage_session_small_update(database, "FULL").and_then(|_| database.execute_batch("COMMIT"));
    proxy.disarm();
    let corrupt_error = corrupt_result.expect_err("mismatched existing bytes must fail staging");
    assert_eq!(
        corrupt_error.sqlite_error_code(),
        Some(ffi::ErrorCode::DatabaseCorrupt),
        "a valid CRC32C cannot make bytes with the wrong content ID reusable"
    );
    assert!(
        matches!(
            corrupt_server.first_storage_error(),
            Some(failure)
                if failure.phase == StorageFailurePhase::VerifyExistingBlock
                    && failure.cause
                        == StorageFailureCause::SqliteCode(ffi::SQLITE_CORRUPT)
        ),
        "a corrupt existing immutable object should report its terminal verification failure: {:?}",
        corrupt_server.first_storage_error()
    );
    assert!(
        proxy
            .block_put_observations()
            .iter()
            .any(|observation| observation.injected_status == Some(412)),
        "proxy should create a mismatching object then return a precondition failure"
    );
    let corrupt_progress = corrupt_events
        .lock()
        .expect("lock corrupt upload progress events");
    assert!(
        corrupt_progress.iter().all(|progress| {
            progress.reused_blocks == baseline.reused_blocks
                && progress.reused_bytes == baseline.reused_bytes
        }),
        "wrong existing bytes must not count as reuse: {corrupt_progress:?}"
    );
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_session_plan_stays_incomplete_after_final_checkpoint_403() {
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let proxy = StorageFaultProxy::start(&endpoint);
    let prefix = format!("{}/uri/upload-plan-denied/", unique_suffix());
    let uri = remote_server_database_uri("google", &proxy.url, bucket, &prefix);
    let events = Arc::new(Mutex::new(Vec::<UploadProgress>::new()));
    let callback_events = Arc::clone(&events);
    let session_id = Uuid::new_v4().to_string();
    let operation_id =
        SessionOperationId::from_request("POST", "/remote.db/commit", session_id.as_bytes())
            .expect("derive denied-plan operation ID");
    let mut server =
        start_uri_session_server_with_progress(&uri, &session_id, operation_id, move |progress| {
            callback_events
                .lock()
                .expect("lock denied-plan upload progress events")
                .push(progress);
        })
        .expect("start denied-plan URI session");
    let database = server
        .database_connection()
        .expect("denied-plan session exposes its SQLite anchor");
    configure_uri_session_test_cache_with(database, 64 * 1024 * 1024, 90);
    database
        .execute_batch("PRAGMA synchronous=OFF;")
        .expect("disable automatic sync for the one-block fault fixture");
    seed_uri_session_fault_data(database).expect("seed denied-plan database");
    events.lock().expect("clear seed progress events").clear();
    stage_session_small_update(database, "OFF").expect("stage one small dirty block");
    database
        .execute_batch("COMMIT")
        .expect("finish the update without a producer sync");
    let producer_events = events
        .lock()
        .expect("lock producer upload progress events")
        .clone();
    assert!(
        producer_events
            .iter()
            .all(|progress| progress.expected.is_none()),
        "the total must stay unknown before the final checkpoint: {producer_events:?}"
    );
    events
        .lock()
        .expect("clear producer progress events")
        .clear();

    proxy.configure(StorageFaultTarget::BlockPut, StorageFaultAction::Forbidden);
    proxy.clear_block_put_observations();
    proxy.arm();
    let result = server.complete_request();
    proxy.disarm();
    let observations = proxy.block_put_observations();
    let progress = events
        .lock()
        .expect("lock final-checkpoint upload progress events")
        .clone();
    assert!(
        result.is_err(),
        "a denied final-checkpoint block PUT must fail; matched={}; observations={observations:?}; progress={progress:?}",
        proxy.matched()
    );
    assert!(
        observations
            .iter()
            .any(|observation| observation.injected_status == Some(403)),
        "the final checkpoint should attempt a block PUT after publishing its plan: {observations:?}"
    );
    let final_progress = *progress
        .last()
        .expect("the explicit checkpoint should report a plan before staging");
    let plan = final_progress
        .expected
        .expect("the explicit checkpoint should provide a fixed total");
    assert!(
        plan.blocks > 0,
        "the one-block fixture needs a nonempty plan"
    );
    assert!(
        final_progress.uploaded_blocks + final_progress.reused_blocks < plan.blocks,
        "a failed block transfer must remain below the fixed total: {final_progress:?}"
    );
    assert!(
        matches!(
            server.first_storage_error(),
            Some(failure)
                if failure.phase == StorageFailurePhase::StageBlockPut
                    && failure.cause == StorageFailureCause::HttpStatus(403)
        ),
        "the terminal block denial should preserve its phase and status: {:?}",
        server.first_storage_error()
    );
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_session_records_terminal_checkpoint_and_accept_failures() {
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let proxy = StorageFaultProxy::start(&endpoint);

    for (label, target, expected_phase) in [
        (
            "checkpoint",
            StorageFaultTarget::XSyncCheckpoint,
            StorageFailurePhase::SessionCheckpoint,
        ),
        (
            "accept",
            StorageFaultTarget::SessionHeadCas,
            StorageFailurePhase::SessionAccept,
        ),
    ] {
        let prefix = format!("{}/uri/storage-diagnostics-{label}/", unique_suffix());
        let uri = remote_server_database_uri("google", &proxy.url, bucket, &prefix);
        let session_id = Uuid::new_v4().to_string();
        let operation_id =
            SessionOperationId::from_request("POST", "/remote.db/commit", session_id.as_bytes())
                .expect("derive storage-diagnostics operation ID");
        let mut server = start_uri_session_server(&uri, &session_id, operation_id)
            .unwrap_or_else(|error| panic!("start {label} diagnostic session: {error:?}"));
        let database = server
            .database_connection()
            .unwrap_or_else(|| panic!("{label} diagnostic session has no SQLite anchor"));
        configure_uri_session_test_cache_with(database, 64 * 1024 * 1024, 90);
        database
            .execute_batch("PRAGMA synchronous=OFF;")
            .unwrap_or_else(|error| panic!("disable automatic {label} checkpoint: {error:?}"));
        seed_uri_session_fault_data(database)
            .unwrap_or_else(|error| panic!("seed {label} diagnostic database: {error:?}"));
        stage_session_small_update(database, "OFF")
            .unwrap_or_else(|error| panic!("stage {label} diagnostic data: {error:?}"));
        database
            .execute_batch("COMMIT")
            .unwrap_or_else(|error| panic!("commit {label} diagnostic update: {error:?}"));

        proxy.configure(target, StorageFaultAction::Forbidden);
        proxy.arm();
        let result = server.complete_request();
        proxy.disarm();
        assert!(
            result.is_err(),
            "the injected {label} storage failure should fail completion"
        );
        assert_eq!(
            proxy.faulted(),
            1,
            "the proxy should inject one {label} failure"
        );
        assert!(
            matches!(
                server.first_storage_error(),
                Some(failure)
                    if failure.phase == expected_phase
                        && failure.cause == StorageFailureCause::HttpStatus(403)
            ),
            "the {label} failure should retain its typed storage phase and HTTP status: {:?}",
            server.first_storage_error()
        );
        let _ = server.close();
    }
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_session_rotates_credentials_during_one_checkpoint_batch() {
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let proxy = StorageFaultProxy::start(&endpoint);
    let prefix = format!("{}/uri/credential-rotation/", unique_suffix());
    let uri = remote_server_database_uri("google", &proxy.url, bucket, &prefix);
    let request_tokens = Arc::new(AtomicUsize::new(0));
    let callback_tokens = Arc::clone(&request_tokens);
    let session_id = Uuid::new_v4().to_string();
    let operation_id =
        SessionOperationId::from_request("POST", "/remote.db/commit", session_id.as_bytes())
            .expect("derive credential-rotation operation ID");
    let mut server = start_uri_session_server_with_auth(
        &uri,
        &session_id,
        operation_id,
        move |_, _, _, reason| match reason {
            AuthRefreshReason::Request => Ok(format!(
                "rotating-token-{}",
                callback_tokens.fetch_add(1, Ordering::AcqRel)
            )),
            AuthRefreshReason::Unauthorized => Err(AuthError(
                "unexpected emulator rejection during proactive token rotation".into(),
            )),
        },
    )
    .expect("start credential-rotation URI session");
    let database = server
        .database_connection()
        .expect("URI session exposes its SQLite anchor");
    seed_uri_session_fault_data(database).expect("seed credential-rotation database");
    stage_session_update(database).expect("stage several block uploads in one checkpoint");

    proxy.clear_block_put_observations();
    server
        .complete_request()
        .expect("checkpoint data while request tokens rotate");
    let observations = proxy.block_put_observations();
    assert!(
        observations.len() >= 2,
        "large checkpoint should dispatch multiple block uploads: {observations:?}"
    );
    let tokens = observations
        .iter()
        .filter_map(|observation| observation.authorization.as_deref())
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        tokens.len() >= 2,
        "multiple block PUTs in one open VFS should use refreshed bearer values: {observations:?}"
    );
    assert!(
        observations.iter().all(|observation| {
            observation.google_crc32c.as_deref()
                == Some(format!("crc32c={}", observation.payload_crc32c).as_str())
        }),
        "each retried storage request should carry a checksum for its full block body"
    );
    let blocks_prefix = format!("{prefix}blocks/");
    let accepted_blocks =
        google_object_names(&list_google_objects(&endpoint, bucket, &blocks_prefix));
    assert!(
        accepted_blocks.len() >= 2,
        "the checkpoint should map several complete block objects: {accepted_blocks:?}"
    );

    server
        .close()
        .expect("close after the accepted checkpoint without publishing it");
    proxy.disarm();
    let resumed_callback_tokens = Arc::clone(&request_tokens);
    let resumed = start_uri_session_server_with_auth(
        &uri,
        &session_id,
        operation_id,
        move |_, _, _, reason| match reason {
            AuthRefreshReason::Request => Ok(format!(
                "resumed-token-{}",
                resumed_callback_tokens.fetch_add(1, Ordering::AcqRel)
            )),
            AuthRefreshReason::Unauthorized => Err(AuthError(
                "unexpected emulator rejection while resuming accepted checkpoint".into(),
            )),
        },
    )
    .expect("reattach the accepted checkpoint with a renewed provider");
    assert_eq!(
        resumed
            .operation_status()
            .expect("read resumed operation status"),
        SessionOperationStatus::Accepted
    );
    let resumed_database = resumed
        .database_connection()
        .expect("resumed server exposes accepted database");
    let accepted_rows: i64 = resumed_database
        .query_row("SELECT count(*) FROM fault_data", [], |row| row.get(0))
        .expect("read accepted checkpoint before final publication");
    assert_eq!(accepted_rows, 5_500);
    assert_eq!(
        accepted_blocks,
        google_object_names(&list_google_objects(&endpoint, bucket, &blocks_prefix)),
        "reattaching an accepted session must reuse its complete immutable blocks"
    );
    let mut resumed = resumed;
    resumed
        .upload()
        .expect("publish the resumed accepted checkpoint");
    resumed
        .close()
        .expect("close the resumed published URI session");
    let readonly_flags = OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let published = Connection::open_with_flags(&uri, readonly_flags)
        .expect("reopen the database after the live credential changes");
    let row_count: i64 = published
        .query_row("SELECT count(*) FROM fault_data", [], |row| row.get(0))
        .expect("read all checkpointed rows after publication");
    assert_eq!(row_count, 5_500);
    published
        .close()
        .expect("close the published credential-rotation reader");
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_session_retries_the_same_block_after_401_and_403() {
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let proxy = StorageFaultProxy::start(&endpoint);

    for (status, action) in [
        (401, StorageFaultAction::Unauthorized),
        (403, StorageFaultAction::Forbidden),
    ] {
        let prefix = format!("{}/uri/credential-renewal-{status}/", unique_suffix());
        let uri = remote_server_database_uri("google", &proxy.url, bucket, &prefix);
        let was_refreshed = Arc::new(AtomicBool::new(false));
        let callback_was_refreshed = Arc::clone(&was_refreshed);
        let unauthorized_calls = Arc::new(AtomicUsize::new(0));
        let callback_unauthorized_calls = Arc::clone(&unauthorized_calls);
        let session_id = Uuid::new_v4().to_string();
        let mut server = start_uri_session_server_with_auth(
            &uri,
            &session_id,
            SessionOperationId::from_request("POST", "/remote.db/commit", session_id.as_bytes())
                .expect("derive credential-renewal operation ID"),
            move |_, _, _, reason| match reason {
                AuthRefreshReason::Request => {
                    Ok(if callback_was_refreshed.load(Ordering::Acquire) {
                        format!("fresh-token-{status}")
                    } else {
                        format!("expired-token-{status}")
                    })
                }
                AuthRefreshReason::Unauthorized => {
                    callback_unauthorized_calls.fetch_add(1, Ordering::AcqRel);
                    callback_was_refreshed.store(true, Ordering::Release);
                    Ok(format!("fresh-token-{status}"))
                }
            },
        )
        .unwrap_or_else(|error| panic!("start URI session for HTTP {status}: {error:?}"));
        let database = server
            .database_connection()
            .expect("URI session exposes its SQLite anchor");
        seed_uri_session_fault_data(database)
            .unwrap_or_else(|error| panic!("seed HTTP {status} retry database: {error:?}"));
        stage_session_small_update(database, "FULL")
            .unwrap_or_else(|error| panic!("stage HTTP {status} retry data: {error:?}"));
        database
            .execute_batch("COMMIT")
            .unwrap_or_else(|error| panic!("commit HTTP {status} retry data: {error:?}"));

        proxy.configure(StorageFaultTarget::BlockPut, action);
        proxy.clear_block_put_observations();
        proxy.arm();
        server
            .complete_request()
            .unwrap_or_else(|error| panic!("renew after HTTP {status} and checkpoint: {error:?}"));
        proxy.disarm();
        let observations = proxy.block_put_observations();
        let rejected = observations
            .iter()
            .find(|observation| observation.injected_status == Some(status))
            .unwrap_or_else(|| panic!("proxy did not inject HTTP {status}: {observations:?}"));
        let retry = observations
            .iter()
            .find(|observation| {
                observation.injected_status.is_none()
                    && observation.object == rejected.object
                    && observation.if_generation_match == rejected.if_generation_match
                    && observation.payload_bytes == rejected.payload_bytes
                    && observation.payload_crc32c == rejected.payload_crc32c
                    && observation.google_crc32c == rejected.google_crc32c
            })
            .unwrap_or_else(|| {
                panic!("HTTP {status} retry did not resend the same full block: {observations:?}")
            });
        assert_ne!(
            rejected.authorization, retry.authorization,
            "the repeated object PUT must carry the renewed bearer token"
        );
        assert_eq!(
            rejected.if_generation_match.as_deref(),
            Some("0"),
            "the retried block must retain its create-only generation precondition"
        );
        assert_eq!(
            unauthorized_calls.load(Ordering::Acquire),
            1,
            "one injected HTTP {status} should request one forced renewal"
        );

        server
            .upload()
            .unwrap_or_else(|error| panic!("publish HTTP {status} accepted checkpoint: {error:?}"));
        server
            .close()
            .unwrap_or_else(|error| panic!("close HTTP {status} URI session: {error:?}"));
        let readonly_flags = OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        let published = Connection::open_with_flags(&uri, readonly_flags)
            .unwrap_or_else(|error| panic!("reopen HTTP {status} database: {error:?}"));
        let row_count: i64 = published
            .query_row("SELECT count(*) FROM fault_data", [], |row| row.get(0))
            .unwrap_or_else(|error| panic!("read HTTP {status} published rows: {error:?}"));
        assert_eq!(row_count, 2);
        published
            .close()
            .unwrap_or_else(|error| panic!("close HTTP {status} database reader: {error:?}"));
    }
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn google_uri_session_bounds_retries_when_credentials_are_permanently_denied() {
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    ensure_google_bucket(&endpoint, bucket);
    let proxy = StorageFaultProxy::start(&endpoint);
    let prefix = format!("{}/uri/credential-denied/", unique_suffix());
    let uri = remote_server_database_uri("google", &proxy.url, bucket, &prefix);
    let request_calls = Arc::new(AtomicUsize::new(0));
    let callback_request_calls = Arc::clone(&request_calls);
    let unauthorized_calls = Arc::new(AtomicUsize::new(0));
    let callback_unauthorized_calls = Arc::clone(&unauthorized_calls);
    let session_id = Uuid::new_v4().to_string();
    let mut server = start_uri_session_server_with_auth(
        &uri,
        &session_id,
        SessionOperationId::from_request("POST", "/remote.db/commit", session_id.as_bytes())
            .expect("derive denied-credentials operation ID"),
        move |_, _, _, reason| match reason {
            AuthRefreshReason::Request => Ok(format!(
                "still-denied-request-token-{}",
                callback_request_calls.fetch_add(1, Ordering::AcqRel)
            )),
            AuthRefreshReason::Unauthorized => {
                let refresh = callback_unauthorized_calls.fetch_add(1, Ordering::AcqRel);
                Ok(format!("still-denied-refresh-token-{refresh}"))
            }
        },
    )
    .expect("start permanently denied URI session");
    let database = server
        .database_connection()
        .expect("URI session exposes its SQLite anchor");
    seed_uri_session_fault_data(database).expect("seed permanently denied database");
    stage_session_small_update(database, "FULL").expect("stage permanently denied update");
    database
        .execute_batch("COMMIT")
        .expect("commit permanently denied update");

    proxy.configure(
        StorageFaultTarget::BlockPut,
        StorageFaultAction::AlwaysForbidden,
    );
    proxy.clear_block_put_observations();
    proxy.arm();
    let error = server
        .complete_request()
        .expect_err("a permanently denied GCS block upload must fail");
    let observations = proxy.block_put_observations();
    assert!(!observations.is_empty(), "no block PUT reached the proxy");
    assert!(
        observations
            .iter()
            .all(|observation| observation.injected_status == Some(403)),
        "the proxy should deny every matching block PUT: {observations:?}"
    );
    let refresh_count = unauthorized_calls.load(Ordering::Acquire);
    let request_attempts = observations
        .iter()
        .filter(|observation| {
            observation
                .authorization
                .as_deref()
                .is_some_and(|token| token.starts_with("Bearer still-denied-request-token-"))
        })
        .count();
    let renewed_attempts = observations
        .iter()
        .filter(|observation| {
            observation
                .authorization
                .as_deref()
                .is_some_and(|token| token.starts_with("Bearer still-denied-refresh-token-"))
        })
        .count();
    assert!(
        request_attempts == refresh_count && renewed_attempts == refresh_count,
        "each denied request should get one forced refresh and one replay only: refreshes={refresh_count} observations={observations:?}"
    );
    assert!(
        observations.len() == refresh_count * 2,
        "persistent denial should stop after one replay per cloud request: refreshes={refresh_count} observations={observations:?}"
    );
    assert!(!error.to_string().contains("still-denied-request-token"));
    assert!(!format!("{error:?}").contains("still-denied-request-token"));
    assert!(!error.to_string().contains("still-denied-refresh-token"));
    assert!(!format!("{error:?}").contains("still-denied-refresh-token"));
}

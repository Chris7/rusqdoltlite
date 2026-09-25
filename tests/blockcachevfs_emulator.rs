#![cfg(feature = "blockcachevfs")]

use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Storage};
use rusqlite::{params, Connection};

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

fn shared_cache() -> &'static Path {
    static CACHE: OnceLock<tempfile::TempDir> = OnceLock::new();
    CACHE
        .get_or_init(|| tempfile::tempdir().expect("shared cache directory"))
        .path()
}

fn run_bootstrap(backend: &str) {
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
    let local_dir = tempfile::tempdir().expect("local database directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local SQLite database");
    local
        .execute_batch("CREATE TABLE bootstrap(value TEXT); INSERT INTO bootstrap VALUES ('ok');")
        .expect("seed local SQLite database");
    local.close().expect("close local SQLite database");
    vfs.create_database(&storage, &local_path, "bootstrap.sqlite")
        .expect("upload initial database");
    vfs.initialize_container(&storage)
        .expect_err("existing CBS manifest must not be replaced");
    let alias = format!("bootstrap_{backend}_{}", std::process::id());
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&alias))
        .expect("attach created database");
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
}

#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_bootstrap() {
    run_bootstrap("google");
}

#[test]
#[ignore = "requires the pinned local emulator containers"]
fn s3_emulator_bootstrap() {
    run_bootstrap("s3");
}

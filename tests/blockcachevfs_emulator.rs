#![cfg(feature = "blockcachevfs")]

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "remote")]
use std::io::{Read as _, Write as _};
#[cfg(feature = "remote")]
use std::net::TcpStream;

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Storage};
use rusqlite::{params, Connection};
#[cfg(feature = "remote")]
use rusqlite::{RemoteServer, RemoteServerOptions};

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

    let cache = tempfile::tempdir().expect("cache directory");
    let vfs = BlockCacheVfs::builder(cache.path())
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
    let alias = format!("bootstrap_{}", std::process::id());
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
    vfs.attach(&AttachSpec::new(storage).alias(&second_alias))
        .expect("re-attach created database");
    let path = format!("/{second_alias}/bootstrap.sqlite");
    let db = vfs.open(&path).expect("re-open created database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM bootstrap", [], |row| row.get(0))
        .expect("read uploaded database");
    assert_eq!(count, 2);
    drop(db);
    vfs.detach(&second_alias)
        .expect("detach re-opened database");
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
    let error = Connection::open(&missing_uri).expect_err("missing database must not be created");
    assert_eq!(
        error.sqlite_extended_error_code(),
        Some(rusqlite::ffi::SQLITE_NOTFOUND)
    );
    assert!(!error.to_string().contains("private-token"));

    let missing_container_uri = format!(
        "gcs://{bucket}/{suffix}/missing-container?vfs=blockcachevfs&project=test-project&access_token=container-secret&endpoint={endpoint}"
    );
    let error = Connection::open(&missing_container_uri)
        .expect_err("missing CBS manifest must not be initialized by open");
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
    let error = Connection::open(missing_database_uri)
        .expect_err("open must not create a missing S3 database");
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
    let error = Connection::open(missing_container_uri)
        .expect_err("open must not initialize a missing S3 CBS prefix");
    assert_eq!(
        error.sqlite_extended_error_code(),
        Some(rusqlite::ffi::SQLITE_NOTFOUND)
    );
    for credential in [access_id, secret_access_key, session_token] {
        assert!(!error.to_string().contains(credential));
        assert!(!format!("{error:?}").contains(credential));
    }
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

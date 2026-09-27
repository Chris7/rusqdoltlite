#![cfg(feature = "blockcachevfs")]

use std::ffi::OsString;
use std::fs;
use std::io::{self, Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Config, Storage};
use rusqlite::{params, Connection};

const CACHE_BYTES: i64 = 2 * 4 * 1024 * 1024;
const BODY_BYTES: usize = 2048;
const ROWS: i64 = 7_000;
const FAILURE_PHASE_ENV: &str = "BCV_STREAMING_FAILURE_PHASE";
const FAILURE_CACHE_ENV: &str = "BCV_STREAMING_FAILURE_CACHE";
const FAILURE_CONTAINER_ENV: &str = "BCV_STREAMING_FAILURE_CONTAINER";
const FAILURE_MANIFEST_ENV: &str = "BCV_STREAMING_FAILURE_MANIFEST";

fn unique_suffix() -> String {
    format!(
        "rust-streaming-failure-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_nanos()
    )
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

fn fetch_manifest(endpoint: &str, container: &str) -> Vec<u8> {
    let (bucket, prefix) = container
        .split_once('/')
        .expect("failure-test container must contain a prefix");
    let url = format!(
        "{}/{}/{}",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        prefix
            .split('/')
            .map(encode_component)
            .chain(std::iter::once("manifest.bcv".to_owned()))
            .collect::<Vec<_>>()
            .join("/")
    );
    let body = tempfile::NamedTempFile::new().expect("manifest temporary file");
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
        .expect("failure-test container must contain a prefix");
    let prefix = encode_component(&format!("{prefix}/"));
    let url = format!(
        "{}/{bucket}?list-type=2&prefix={prefix}&max-keys=1000",
        endpoint.trim_end_matches('/'),
    );
    let body = tempfile::NamedTempFile::new().expect("object-list temporary file");
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
        .matches("<Key>")
        .count()
}

fn row_body(id: i64) -> String {
    let prefix = format!("row-{id:08}:");
    format!("{prefix}{}", "x".repeat(BODY_BYTES - prefix.len()))
}

fn restore_env(snapshot: Vec<(&'static str, Option<OsString>)>) {
    for (name, value) in snapshot {
        if let Some(value) = value {
            std::env::set_var(name, value);
        } else {
            std::env::remove_var(name);
        }
    }
}

struct ForwardingProxy {
    stop: Arc<AtomicBool>,
    requests: Arc<AtomicUsize>,
    put_requests: Arc<AtomicUsize>,
    rejected_puts: Arc<AtomicUsize>,
    reject_puts: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    url: String,
}

impl ForwardingProxy {
    fn start(endpoint: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind forwarding proxy");
        listener
            .set_nonblocking(true)
            .expect("set forwarding proxy nonblocking");
        let url = format!(
            "http://{}",
            listener.local_addr().expect("forwarding proxy address")
        );
        let authority = endpoint
            .trim_end_matches('/')
            .strip_prefix("http://")
            .or_else(|| endpoint.trim_end_matches('/').strip_prefix("https://"))
            .and_then(|value| value.split('/').next())
            .expect("emulator endpoint must have an HTTP authority")
            .to_owned();
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(AtomicUsize::new(0));
        let put_requests = Arc::new(AtomicUsize::new(0));
        let rejected_puts = Arc::new(AtomicUsize::new(0));
        let reject_puts = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread_requests = Arc::clone(&requests);
        let thread_put_requests = Arc::clone(&put_requests);
        let thread_rejected_puts = Arc::clone(&rejected_puts);
        let thread_reject_puts = Arc::clone(&reject_puts);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                let Ok((mut stream, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                thread_requests.fetch_add(1, Ordering::Relaxed);
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let result = proxy_request(
                    &mut stream,
                    &authority,
                    &thread_put_requests,
                    &thread_rejected_puts,
                    &thread_reject_puts,
                );
                if let Err(error) = result {
                    let body = format!("proxy error: {error}");
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
            stop,
            requests,
            put_requests,
            rejected_puts,
            reject_puts,
            thread: Some(thread),
            url,
        }
    }

    fn requests(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }

    fn put_requests(&self) -> usize {
        self.put_requests.load(Ordering::Relaxed)
    }

    fn rejected_puts(&self) -> usize {
        self.rejected_puts.load(Ordering::Relaxed)
    }

    fn set_reject_puts(&self, reject: bool) {
        self.reject_puts.store(reject, Ordering::Release);
    }
}

impl Drop for ForwardingProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join forwarding proxy");
        }
    }
}

fn proxy_request(
    stream: &mut TcpStream,
    authority: &str,
    put_requests: &AtomicUsize,
    rejected_puts: &AtomicUsize,
    reject_puts: &AtomicBool,
) -> io::Result<()> {
    let mut request = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 4096];
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy client closed before request headers",
            ));
        }
        request.extend_from_slice(&chunk[..count]);
        if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        if request.len() > 128 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy request headers are too large",
            ));
        }
    };

    let header_text = String::from_utf8_lossy(&request[..header_end]);
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy request has no request line",
        )
    })?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy request has no method"))?
        .to_owned();
    let target = request_parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "proxy request has no target"))?
        .to_owned();
    let version = request_parts.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy request has no HTTP version",
        )
    })?;
    if version != "HTTP/1.1" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "proxy only supports HTTP/1.1 requests",
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
            io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy request has malformed header",
            )
        })?;
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "proxy request has invalid content length",
                )
            })?;
        }
        if name.eq_ignore_ascii_case("expect") && value.trim().eq_ignore_ascii_case("100-continue")
        {
            expect_continue = true;
        }
        headers.push((name.to_owned(), value.trim().to_owned()));
    }

    let mut body = request[header_end..].to_vec();
    if expect_continue {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
    }
    while body.len() < content_length {
        let old_len = body.len();
        let mut chunk = [0_u8; 16 * 1024];
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy client closed before request body",
            ));
        }
        body.extend_from_slice(&chunk[..count]);
        if body.len() == old_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy made no progress reading request body",
            ));
        }
    }
    body.truncate(content_length);

    if method.eq_ignore_ascii_case("PUT") {
        put_requests.fetch_add(1, Ordering::Relaxed);
        if reject_puts.load(Ordering::Acquire) {
            rejected_puts.fetch_add(1, Ordering::Relaxed);
            stream.write_all(
                b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )?;
            return Ok(());
        }
    }

    let origin_target = if let Some(rest) = target.strip_prefix("http://") {
        rest.find('/').map_or("/", |position| &rest[position..])
    } else if let Some(rest) = target.strip_prefix("https://") {
        rest.find('/').map_or("/", |position| &rest[position..])
    } else {
        &target
    };
    let mut upstream = TcpStream::connect(authority)?;
    upstream.set_read_timeout(Some(Duration::from_secs(5)))?;
    upstream.set_write_timeout(Some(Duration::from_secs(5)))?;
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
    upstream.write_all(&body)?;
    let mut response = [0_u8; 16 * 1024];
    loop {
        let count = upstream.read(&mut response)?;
        if count == 0 {
            break;
        }
        stream.write_all(&response[..count])?;
    }
    Ok(())
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn failed_staged_block_put_preserves_manifest_and_returns_error() {
    if let Some(phase) = std::env::var_os(FAILURE_PHASE_ENV) {
        match phase.to_str() {
            Some("one") => failure_phase_one(),
            Some("two") => failure_phase_two(),
            Some(other) => panic!("unknown failure-test phase {other}"),
            None => panic!("failure-test phase is not UTF-8"),
        }
        return;
    }

    let executable = std::env::current_exe().expect("locate failure-test executable");
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let suffix = unique_suffix();
    let bucket = format!("rust-streaming-failure-{suffix}");
    let container = format!("{bucket}/{suffix}/cbs");
    let cache = tempfile::tempdir().expect("cache directory");
    let manifest = tempfile::tempdir().expect("manifest state directory");
    let cache_path = cache.path().to_str().expect("cache path is UTF-8");
    let manifest_path = manifest.path().join("before.bin");
    let manifest_path = manifest_path.to_str().expect("manifest path is UTF-8");
    for (phase, label) in [("one", "failure phase one"), ("two", "failure phase two")] {
        let status = Command::new(&executable)
            .args([
                "--ignored",
                "--exact",
                "failed_staged_block_put_preserves_manifest_and_returns_error",
                "--nocapture",
            ])
            .env(FAILURE_PHASE_ENV, phase)
            .env(FAILURE_CACHE_ENV, cache_path)
            .env(FAILURE_CONTAINER_ENV, &container)
            .env(FAILURE_MANIFEST_ENV, manifest_path)
            .env("BLOCKCACHEVFS_S3_EMULATOR", &endpoint)
            .status()
            .unwrap_or_else(|error| panic!("spawn {label}: {error}"));
        assert!(status.success(), "{label} failed: {status}");
    }
}

fn failure_phase_one() {
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let cache = PathBuf::from(
        std::env::var(FAILURE_CACHE_ENV).expect("failure-test cache path must be set"),
    );
    let container =
        std::env::var(FAILURE_CONTAINER_ENV).expect("failure-test container must be set");
    let manifest_path = PathBuf::from(
        std::env::var(FAILURE_MANIFEST_ENV).expect("failure-test manifest path must be set"),
    );
    let storage = Storage::s3_with_endpoint("test", &container, "us-east-1", &endpoint);
    let proxy_names = [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ];
    let no_proxy_names = ["NO_PROXY", "no_proxy"];
    let saved_env = proxy_names
        .iter()
        .chain(no_proxy_names.iter())
        .map(|name| (*name, std::env::var_os(name)))
        .collect();
    let proxy = ForwardingProxy::start(&endpoint);
    // Configure the forwarding proxy before CBS creates its libcurl handles.
    // It forwards normal requests to Floci, and can reject only PUT requests
    // after the first committed update has proven that block staging works.
    for name in proxy_names {
        std::env::set_var(name, &proxy.url);
    }
    for name in no_proxy_names {
        std::env::set_var(name, "");
    }
    let vfs = BlockCacheVfs::builder(&cache)
        .expect("VFS builder")
        .auth_callback(|_, _, _| Ok("test".into()))
        .config(Config::CacheSize(CACHE_BYTES))
        .config(Config::RequestCount(1))
        .config(Config::HttpTimeout(1))
        .init()
        .expect("initialize block-cache VFS");
    if let Err(error) = vfs.initialize_container(&storage) {
        panic!(
            "initialize remote CBS container through proxy (requests={}, puts={}): {error:?}",
            proxy.requests(),
            proxy.put_requests()
        );
    }

    let local_dir = tempfile::tempdir().expect("local database directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create seed database");
    local
        .execute_batch("CREATE TABLE payload(id INTEGER PRIMARY KEY, body TEXT NOT NULL);")
        .expect("create seed schema");
    let _: String = local
        .query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })
        .expect("commit DoltLite seed database");
    local.close().expect("close seed database");
    vfs.create_database(&storage, &local_path, "streaming.sqlite")
        .expect("upload seed database");

    let alias = "streaming_failure";
    vfs.attach(&AttachSpec::new(storage).alias(alias))
        .expect("attach failure-test database");
    let before = fetch_manifest(&endpoint, &container);
    let objects_before = list_remote_objects(&endpoint, &container);
    let puts_before = proxy.put_requests();
    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("open failure-test database");

    // First commit a larger-than-cache update through the forwarding proxy.
    // This proves that at least one real block PUT is staged before failure
    // injection is enabled, rather than treating a rollback as a PUT seam.
    db.execute_batch("PRAGMA cache_size = 16; BEGIN IMMEDIATE")
        .expect("begin initial staging update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare initial staging insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert initial staging row");
        }
    }
    db.execute_batch("COMMIT")
        .expect("initial staging update must commit before PUT rejection");
    let objects_after_initial = list_remote_objects(&endpoint, &container);
    assert!(
        objects_after_initial > objects_before,
        "initial committed update did not stage any remote blocks: before={objects_before}, \
         after={objects_after_initial}"
    );
    assert!(
        proxy.put_requests() > puts_before,
        "initial committed update did not issue a block PUT through the forwarding proxy"
    );
    assert_eq!(
        before,
        fetch_manifest(&endpoint, &container),
        "initial staging update must not publish a manifest"
    );

    proxy.set_reject_puts(true);
    let mut failed = None;
    db.execute_batch("BEGIN IMMEDIATE")
        .expect("begin failing staging update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare failing staging insert");
        for id in ROWS..(2 * ROWS) {
            match insert.execute(params![id, row_body(id)]) {
                Ok(_) => {}
                Err(error) => {
                    failed = Some(error);
                    break;
                }
            }
        }
    }
    if failed.is_none() {
        if let Err(error) = db.execute_batch("COMMIT") {
            failed = Some(error);
        }
    }
    let _ = db.execute_batch("ROLLBACK");
    assert!(
        proxy.rejected_puts() > 0,
        "failing update did not reach a rejected block PUT (proxy requests={}, PUTs={})",
        proxy.requests(),
        proxy.put_requests()
    );

    let error = failed.expect("failed block PUT must surface as a write error");
    assert!(
        matches!(
            error,
            rusqlite::Error::SqliteFailure(code, _)
                if code.extended_code >= rusqlite::ffi::SQLITE_IOERR
        ),
        "unexpected failed block PUT error: {error:?}"
    );
    let after = fetch_manifest(&endpoint, &container);
    assert_eq!(
        before, after,
        "failed block PUT must not publish a new manifest"
    );

    // The failed transaction must not hide or corrupt the previously
    // committed rows, even though the cache now contains an unpublished dirty
    // generation from the rejected PUT.
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("count committed rows after failed PUT");
    assert_eq!(count, ROWS, "failed PUT changed committed row count");
    for id in [0, ROWS / 2, ROWS - 1] {
        let body: String = db
            .query_row("SELECT body FROM payload WHERE id=?1", [id], |row| {
                row.get(0)
            })
            .expect("read committed row after failed PUT");
        assert_eq!(
            body,
            row_body(id),
            "committed row {id} changed after failed PUT"
        );
    }
    fs::write(&manifest_path, before).expect("save pre-failure manifest");
    proxy.set_reject_puts(false);
    drop(db);
    restore_env(saved_env);
    let _ = vfs.detach(alias);
    drop(proxy);
}

fn failure_phase_two() {
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let cache = PathBuf::from(
        std::env::var(FAILURE_CACHE_ENV).expect("failure-test cache path must be set"),
    );
    let container =
        std::env::var(FAILURE_CONTAINER_ENV).expect("failure-test container must be set");
    let manifest_path = PathBuf::from(
        std::env::var(FAILURE_MANIFEST_ENV).expect("failure-test manifest path must be set"),
    );
    let storage = Storage::s3_with_endpoint("test", &container, "us-east-1", &endpoint);
    let vfs = BlockCacheVfs::builder(&cache)
        .expect("VFS builder")
        .auth_callback(|_, _, _| Ok("test".into()))
        .config(Config::CacheSize(CACHE_BYTES))
        .config(Config::RequestCount(1))
        .config(Config::HttpTimeout(1))
        .init()
        .expect("initialize block-cache VFS for restart");
    let alias = "streaming_failure";
    vfs.attach(&AttachSpec::new(storage).alias(alias).if_not(true))
        .expect("reattach failure-test database");
    assert_eq!(
        fs::read(&manifest_path).expect("read pre-failure manifest"),
        fetch_manifest(&endpoint, &container),
        "failed PUT must not publish a manifest before restart"
    );
    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("reopen failure-test database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("count committed rows after restart");
    assert_eq!(
        count, ROWS,
        "restart after failed PUT changed committed row count"
    );
    for id in [0, ROWS / 2, ROWS - 1] {
        let body: String = db
            .query_row("SELECT body FROM payload WHERE id=?1", [id], |row| {
                row.get(0)
            })
            .expect("read committed row after restart");
        assert_eq!(
            body,
            row_body(id),
            "committed row {id} changed after restart"
        );
    }
}

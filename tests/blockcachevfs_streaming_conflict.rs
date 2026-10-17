#![cfg(feature = "blockcachevfs")]

//! Failure-injection coverage for the explicit-manifest publication boundary.
//!
//! The child phases are deliberately separate test-process invocations.  The
//! Rust wrapper owns one process-global CBS VFS, while the cache metadata must
//! survive the process boundary being exercised here.

use std::fs;
use std::io::{Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Config, Storage};
use rusqlite::{Connection, params};

const CACHE_BYTES: u64 = 2 * 4 * 1024 * 1024;
const BODY_BYTES: usize = 2048;
const ROWS: i64 = 7_000;

const CACHE_ENV: &str = "BCV_STREAMING_CONFLICT_CACHE";
const FRESH_CACHE_ENV: &str = "BCV_STREAMING_CONFLICT_FRESH_CACHE";
const STATE_ENV: &str = "BCV_STREAMING_CONFLICT_STATE";
const ENDPOINT_ENV: &str = "BCV_STREAMING_CONFLICT_ENDPOINT";
const PROXY_CONTROL_ENV: &str = "BCV_STREAMING_CONFLICT_PROXY_CONTROL";
const CONTAINER_ENV: &str = "BCV_STREAMING_CONFLICT_CONTAINER";
const STAGED_ALIAS: &str = "streaming_conflict";

fn unique_suffix() -> String {
    format!(
        "rust-streaming-conflict-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_nanos()
    )
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for conflict phase"))
}

fn phase_inputs() -> (PathBuf, PathBuf, PathBuf, String, String) {
    (
        PathBuf::from(required_env(CACHE_ENV)),
        PathBuf::from(required_env(FRESH_CACHE_ENV)),
        PathBuf::from(required_env(STATE_ENV)),
        required_env(ENDPOINT_ENV),
        required_env(CONTAINER_ENV),
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

fn encode_path(value: &str) -> String {
    value
        .split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/")
}

fn manifest_url(endpoint: &str, container: &str) -> String {
    let (bucket, prefix) = container
        .split_once('/')
        .expect("conflict-test container must contain a prefix");
    format!(
        "{}/{}/{}",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_path(&format!("{prefix}/manifest.bcv"))
    )
}

fn fetch_manifest(endpoint: &str, container: &str) -> Vec<u8> {
    let body = tempfile::NamedTempFile::new().expect("manifest temporary file");
    let url = manifest_url(endpoint, container);
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
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
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
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
        .expect("conflict-test container must contain a prefix");
    let body = tempfile::NamedTempFile::new().expect("object-list temporary file");
    let url = format!(
        "{}/{}/?list-type=2&prefix={}&max-keys=1000",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_component(&format!("{prefix}/"))
    );
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
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
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
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

fn newer_body(id: i64) -> String {
    let prefix = format!("newer-{id:08}:");
    format!("{prefix}{}", "n".repeat(BODY_BYTES - prefix.len()))
}

fn rewrite_body(id: i64) -> String {
    let prefix = format!("rewrite-{id:08}:");
    format!("{prefix}{}", "r".repeat(BODY_BYTES - prefix.len()))
}

fn assert_cache_bound(cache: &Path, context: &str) {
    let size = fs::metadata(cache.join("cachefile.bcv")).map_or(0, |metadata| metadata.len());
    assert!(
        size <= CACHE_BYTES,
        "cachefile.bcv grew beyond {CACHE_BYTES} bytes ({size}) during {context}"
    );
}

fn new_vfs(cache: &Path) -> &'static BlockCacheVfs {
    BlockCacheVfs::builder(cache)
        .expect("VFS builder")
        .auth_callback(|_, _, _| Ok("test".into()))
        .config(Config::CacheSize(CACHE_BYTES as i64))
        .config(Config::HttpTimeout(5))
        .init()
        .expect("initialize block-cache VFS")
}

fn storage(endpoint: &str, container: &str) -> Storage {
    Storage::s3_with_endpoint("test", container, "us-east-1", endpoint)
}

fn stage_large_update(cache: &Path, endpoint: &str, container: &str, state: &Path) {
    let storage = storage(endpoint, container);
    let vfs = new_vfs(cache);
    vfs.initialize_container(&storage, None)
        .expect("initialize conflict-test container");

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
        .expect("commit seed database");
    local.close().expect("close seed database");
    vfs.create_database(&storage, &local_path, "streaming.sqlite")
        .expect("upload seed database");

    vfs.attach(&AttachSpec::new(storage).alias(STAGED_ALIAS))
        .expect("attach conflict-test database");
    let before = fetch_manifest(endpoint, container);
    let objects_before = list_remote_objects(endpoint, container);
    let db = vfs
        .open(format!("/{STAGED_ALIAS}/streaming.sqlite"))
        .expect("open conflict-test database");
    db.execute_batch("PRAGMA cache_size = 16; BEGIN IMMEDIATE;")
        .expect("begin large conflict-test update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare large conflict-test insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert large conflict-test row");
        }
    }
    db.execute_batch("COMMIT")
        .expect("commit large conflict-test update");
    drop(db);

    assert_eq!(
        before,
        fetch_manifest(endpoint, container),
        "staging must not publish the manifest before upload"
    );
    assert!(
        list_remote_objects(endpoint, container) > objects_before,
        "large update must upload staged objects before publication"
    );
    fs::write(state, before).expect("save pre-upload manifest");
}

fn assert_final_rows(vfs: &'static BlockCacheVfs, alias: &str) {
    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("open final conflict-test database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("count final rows");
    assert_eq!(
        count, ROWS,
        "final manifest must reference every inserted row"
    );
    let total: i64 = db
        .query_row("SELECT sum(length(body)) FROM payload", [], |row| {
            row.get(0)
        })
        .expect("sum final rows");
    assert_eq!(total, ROWS * BODY_BYTES as i64);
    let body: String = db
        .query_row(
            "SELECT body FROM payload WHERE id = ?1",
            [ROWS - 1],
            |row| row.get(0),
        )
        .expect("read final row");
    assert_eq!(body, row_body(ROWS - 1));
    drop(db);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FaultMode {
    ForwardOnly,
    RejectManifestOnce,
    DropManifestResponseOnce,
}

struct FaultProxy {
    stop: Arc<AtomicBool>,
    reject_manifest_once: Arc<AtomicBool>,
    drop_manifest_response_once: Arc<AtomicBool>,
    metrics: Arc<ProxyMetrics>,
    handlers: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    thread: Option<thread::JoinHandle<()>>,
    url: String,
}

#[derive(Default)]
struct ProxyMetrics {
    manifest_requests: AtomicUsize,
    rejected_manifests: AtomicUsize,
    forwarded_manifests: AtomicUsize,
    dropped_responses: AtomicUsize,
}

impl FaultProxy {
    fn start(endpoint: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fault proxy");
        listener
            .set_nonblocking(true)
            .expect("set fault proxy nonblocking");
        let address = listener.local_addr().expect("fault proxy address");
        let url = format!("http://{address}");
        let upstream = endpoint
            .strip_prefix("http://")
            .or_else(|| endpoint.strip_prefix("https://"))
            .and_then(|value| value.split('/').next())
            .expect("fault proxy requires an HTTP emulator endpoint")
            .to_owned();
        let stop = Arc::new(AtomicBool::new(false));
        let reject_manifest_once = Arc::new(AtomicBool::new(false));
        let drop_manifest_response_once = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(ProxyMetrics::default());
        let thread_stop = Arc::clone(&stop);
        let thread_reject = Arc::clone(&reject_manifest_once);
        let thread_drop = Arc::clone(&drop_manifest_response_once);
        let thread_metrics = Arc::clone(&metrics);
        let handlers = Arc::new(Mutex::new(Vec::new()));
        let thread_handlers = Arc::clone(&handlers);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                let (stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(_) => break,
                };
                let handler_upstream = upstream.clone();
                let handler_reject = Arc::clone(&thread_reject);
                let handler_drop = Arc::clone(&thread_drop);
                let handler_metrics = Arc::clone(&thread_metrics);
                let handler = thread::spawn(move || {
                    let _ = handle_proxy_connection(
                        stream,
                        &handler_upstream,
                        &handler_reject,
                        &handler_drop,
                        &handler_metrics,
                    );
                });
                thread_handlers
                    .lock()
                    .expect("lock fault-proxy handlers")
                    .push(handler);
            }
        });
        Self {
            stop,
            reject_manifest_once,
            drop_manifest_response_once,
            metrics,
            handlers,
            thread: Some(thread),
            url,
        }
    }

    fn arm(&self, mode: FaultMode) {
        self.reject_manifest_once.store(false, Ordering::Release);
        self.drop_manifest_response_once
            .store(false, Ordering::Release);
        match mode {
            FaultMode::ForwardOnly => {}
            FaultMode::RejectManifestOnce => {
                self.reject_manifest_once.store(true, Ordering::Release);
            }
            FaultMode::DropManifestResponseOnce => self
                .drop_manifest_response_once
                .store(true, Ordering::Release),
        }
    }

    fn reset_counts(&self) {
        self.metrics.manifest_requests.store(0, Ordering::Release);
        self.metrics.rejected_manifests.store(0, Ordering::Release);
        self.metrics.forwarded_manifests.store(0, Ordering::Release);
        self.metrics.dropped_responses.store(0, Ordering::Release);
    }

    fn manifest_requests(&self) -> usize {
        self.metrics.manifest_requests.load(Ordering::Relaxed)
    }

    fn rejected_manifests(&self) -> usize {
        self.metrics.rejected_manifests.load(Ordering::Relaxed)
    }

    fn forwarded_manifests(&self) -> usize {
        self.metrics.forwarded_manifests.load(Ordering::Relaxed)
    }

    fn dropped_responses(&self) -> usize {
        self.metrics.dropped_responses.load(Ordering::Relaxed)
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join fault proxy");
        }
        let handlers = std::mem::take(
            &mut *self
                .handlers
                .lock()
                .expect("lock fault-proxy handlers for join"),
        );
        for handler in handlers {
            handler.join().expect("join fault-proxy connection");
        }
    }
}

fn handle_proxy_connection(
    mut client: TcpStream,
    upstream_address: &str,
    reject_manifest_once: &AtomicBool,
    drop_manifest_response_once: &AtomicBool,
    metrics: &ProxyMetrics,
) -> std::io::Result<()> {
    client.set_read_timeout(Some(Duration::from_secs(20)))?;
    let request = read_http_request(&mut client)?;
    let (method, target) = request_line(&request);
    if method.eq_ignore_ascii_case("GET") && target == "/__control/manifest-requests" {
        let body = metrics
            .manifest_requests
            .load(Ordering::Relaxed)
            .to_string();
        write!(
            client,
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )?;
        return client.shutdown(Shutdown::Both);
    }
    let manifest = method == "PUT" && request_path(target).ends_with("/manifest.bcv");
    if !manifest {
        return forward_request(client, upstream_address, request, false);
    }

    metrics.manifest_requests.fetch_add(1, Ordering::Relaxed);
    if reject_manifest_once.swap(false, Ordering::AcqRel) {
        metrics.rejected_manifests.fetch_add(1, Ordering::Relaxed);
        client.write_all(b"HTTP/1.1 412 Precondition Failed\r\n")?;
        client.write_all(b"Content-Length: 0\r\nConnection: close\r\n\r\n")?;
        client.shutdown(Shutdown::Both)
    } else if drop_manifest_response_once.swap(false, Ordering::AcqRel) {
        metrics.forwarded_manifests.fetch_add(1, Ordering::Relaxed);
        metrics.dropped_responses.fetch_add(1, Ordering::Relaxed);
        // The request is sent to the emulator and its complete response
        // is consumed, proving that the manifest PUT reached the remote
        // store before the client-side response is made ambiguous.
        let _ = forward_request(client, upstream_address, request, true);
        Ok(())
    } else {
        metrics.forwarded_manifests.fetch_add(1, Ordering::Relaxed);
        forward_request(client, upstream_address, request, false)
    }
}

fn request_line(request: &[u8]) -> (&str, &str) {
    let end = request
        .windows(2)
        .position(|window| window == b"\r\n")
        .expect("HTTP request line terminator");
    let line = std::str::from_utf8(&request[..end]).expect("HTTP request line is UTF-8");
    let mut fields = line.split_whitespace();
    (
        fields.next().expect("HTTP method"),
        fields.next().expect("HTTP request target"),
    )
}

fn request_path(target: &str) -> &str {
    target
        .split_once("://")
        .and_then(|(_, rest)| rest.find('/').map(|index| &rest[index..]))
        .unwrap_or(target)
}

fn read_http_request(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    let header_end = loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "proxy client closed before HTTP headers",
            ));
        }
        request.extend_from_slice(&buffer[..read]);
        if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        if request.len() > 128 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "proxy request headers are too large",
            ));
        }
    };
    let headers = &request[..header_end];
    let content_length = headers
        .split(|byte| *byte == b'\n')
        .find_map(|line| {
            let line = std::str::from_utf8(line).ok()?;
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    if headers
        .windows(b"expect: 100-continue".len())
        .any(|window| window.eq_ignore_ascii_case(b"expect: 100-continue"))
    {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
    }
    while request.len() < header_end + content_length {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "proxy client closed before HTTP body",
            ));
        }
        request.extend_from_slice(&buffer[..read]);
    }
    request.truncate(header_end + content_length);
    Ok(request)
}

fn forward_request(
    mut client: TcpStream,
    upstream_address: &str,
    request: Vec<u8>,
    discard_response: bool,
) -> std::io::Result<()> {
    let mut upstream = TcpStream::connect(upstream_address)?;
    upstream.set_read_timeout(Some(Duration::from_secs(20)))?;
    let request = with_connection_close(origin_form_request(request));
    upstream.write_all(&request)?;
    let response = read_http_response(&mut upstream)?;
    if !discard_response {
        client.write_all(&response)?;
    }
    let _ = client.shutdown(Shutdown::Both);
    Ok(())
}

fn origin_form_request(request: Vec<u8>) -> Vec<u8> {
    let Some(line_end) = request.windows(2).position(|window| window == b"\r\n") else {
        return request;
    };
    let line = String::from_utf8_lossy(&request[..line_end]);
    let mut fields = line.split_whitespace();
    let Some(method) = fields.next() else {
        return request;
    };
    let Some(target) = fields.next() else {
        return request;
    };
    let Some(version) = fields.next() else {
        return request;
    };
    let path = request_path(target);
    if path == target {
        return request;
    }
    let mut converted = format!("{method} {path} {version}\r\n").into_bytes();
    converted.extend_from_slice(&request[line_end + 2..]);
    converted
}

fn with_connection_close(mut request: Vec<u8>) -> Vec<u8> {
    if let Some(line_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
        request.splice(
            line_end + 2..line_end + 2,
            b"Connection: close\r\n".iter().copied(),
        );
    }
    request
}

fn read_http_response(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    loop {
        let mut response = Vec::new();
        let mut buffer = [0_u8; 16 * 1024];
        let header_end = loop {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "upstream closed before an HTTP response",
                ));
            }
            response.extend_from_slice(&buffer[..read]);
            if let Some(position) = response.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let status = std::str::from_utf8(&response[..header_end])
            .ok()
            .and_then(|headers| headers.lines().next())
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .unwrap_or(0);
        let content_length = response[..header_end]
            .split(|byte| *byte == b'\n')
            .find_map(|line| {
                let line = std::str::from_utf8(line).ok()?;
                let (name, value) = line.split_once(':')?;
                name.trim()
                    .eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            });
        let chunked = response[..header_end]
            .windows(b"transfer-encoding: chunked".len())
            .any(|window| window.eq_ignore_ascii_case(b"transfer-encoding: chunked"));
        if (100..200).contains(&status) || status == 204 || status == 304 {
            if (100..200).contains(&status) {
                continue;
            }
            return Ok(response);
        }
        if let Some(content_length) = content_length {
            while response.len() < header_end + content_length {
                let read = stream.read(&mut buffer)?;
                if read == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "upstream closed before the HTTP response body",
                    ));
                }
                response.extend_from_slice(&buffer[..read]);
            }
            response.truncate(header_end + content_length);
            return Ok(response);
        }
        if chunked {
            // The local S3 emulator normally sends Content-Length, but keep
            // chunked responses bounded by consuming through its terminating
            // zero chunk rather than waiting for a keep-alive close.
            loop {
                let read = stream.read(&mut buffer)?;
                if read == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "upstream closed inside a chunked response",
                    ));
                }
                response.extend_from_slice(&buffer[..read]);
                if response.windows(5).any(|window| window == b"0\r\n\r\n") {
                    return Ok(response);
                }
            }
        }
        let mut tail = Vec::new();
        stream.read_to_end(&mut tail)?;
        response.extend(tail);
        return Ok(response);
    }
}

fn run_phase(
    executable: &Path,
    name: &str,
    values: &[(&str, &str)],
    proxy: &FaultProxy,
) -> ExitStatus {
    Command::new(executable)
        .args(["--ignored", "--exact", name, "--nocapture"])
        .envs(values.iter().copied())
        .env("HTTP_PROXY", &proxy.url)
        .env("http_proxy", &proxy.url)
        .env("ALL_PROXY", &proxy.url)
        .env("all_proxy", &proxy.url)
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .env(PROXY_CONTROL_ENV, &proxy.url)
        .status()
        .unwrap_or_else(|error| panic!("spawn {name}: {error}"))
}

fn proxy_manifest_requests() -> usize {
    let proxy_url = required_env(PROXY_CONTROL_ENV);
    let address = proxy_url
        .strip_prefix("http://")
        .expect("conflict proxy control URL must use HTTP");
    let mut stream = TcpStream::connect(address).expect("connect to parent conflict proxy");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set conflict proxy control read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .expect("set conflict proxy control write timeout");
    write!(
        stream,
        "GET /__control/manifest-requests HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
    )
    .expect("request conflict proxy manifest count");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read conflict proxy manifest count");
    assert!(
        response.starts_with(b"HTTP/1.1 200 OK\r\n"),
        "conflict proxy rejected metric request: {}",
        String::from_utf8_lossy(&response)
    );
    let body_start = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .expect("conflict proxy metric response headers");
    std::str::from_utf8(&response[body_start..])
        .expect("conflict proxy metric response is UTF-8")
        .parse()
        .expect("conflict proxy manifest count is numeric")
}

fn run_child_phases(
    cache: &Path,
    fresh_cache: &Path,
    state: &Path,
    endpoint: &str,
    container: &str,
    phases: &[&str],
    proxy: &FaultProxy,
) {
    let executable = std::env::current_exe().expect("locate conflict-test executable");
    let cache = cache.to_str().expect("cache path is UTF-8");
    let fresh_cache = fresh_cache.to_str().expect("fresh cache path is UTF-8");
    let state = state.to_str().expect("state path is UTF-8");
    let values = [
        (CACHE_ENV, cache),
        (FRESH_CACHE_ENV, fresh_cache),
        (STATE_ENV, state),
        (ENDPOINT_ENV, endpoint),
        (CONTAINER_ENV, container),
    ];
    for phase in phases {
        proxy.reset_counts();
        match *phase {
            "cas_conflict_phase_two" => proxy.arm(FaultMode::RejectManifestOnce),
            "ambiguous_phase_two" => proxy.arm(FaultMode::DropManifestResponseOnce),
            _ => proxy.arm(FaultMode::ForwardOnly),
        }
        let status = run_phase(&executable, phase, &values, proxy);
        assert!(status.success(), "conflict phase {phase} failed: {status}");
        match *phase {
            "cas_conflict_phase_two" => assert_eq!(
                proxy.rejected_manifests(),
                1,
                "the proxy must reject exactly one manifest PUT"
            ),
            "ambiguous_phase_two" => {
                let manifest_requests = proxy.manifest_requests();
                assert!(
                    manifest_requests >= 2,
                    "the client must retry after the first ambiguous manifest PUT"
                );
                assert_eq!(
                    proxy.forwarded_manifests(),
                    manifest_requests,
                    "every manifest PUT, including the retry, must reach the remote emulator"
                );
                assert_eq!(
                    proxy.dropped_responses(),
                    1,
                    "the proxy must drop exactly the first manifest response"
                );
            }
            _ => {}
        }
    }
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn manifest_cas_conflict_retains_staged_objects_for_retry() {
    let cache = tempfile::tempdir().expect("CAS cache directory");
    let fresh_cache = tempfile::tempdir().expect("CAS fresh cache directory");
    let state = tempfile::tempdir().expect("CAS state directory");
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let container = format!("{}/cbs", unique_suffix());
    let state_path = state.path().join("manifest-before.bin");
    let proxy = FaultProxy::start(&endpoint);
    run_child_phases(
        cache.path(),
        fresh_cache.path(),
        &state_path,
        &endpoint,
        &container,
        &[
            "cas_conflict_phase_one",
            "cas_conflict_phase_two",
            "cas_conflict_phase_three",
        ],
        &proxy,
    );
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn ambiguous_manifest_response_is_recoverable_after_restart() {
    let cache = tempfile::tempdir().expect("ambiguous cache directory");
    let fresh_cache = tempfile::tempdir().expect("ambiguous fresh cache directory");
    let state = tempfile::tempdir().expect("ambiguous state directory");
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let container = format!("{}/cbs", unique_suffix());
    let state_path = state.path().join("manifest-before.bin");
    let proxy = FaultProxy::start(&endpoint);
    run_child_phases(
        cache.path(),
        fresh_cache.path(),
        &state_path,
        &endpoint,
        &container,
        &[
            "ambiguous_phase_one",
            "ambiguous_phase_two",
            "ambiguous_phase_three",
            "ambiguous_phase_four",
        ],
        &proxy,
    );
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn ambiguous_publication_keeps_a_newer_local_write_conflicted() {
    let cache = tempfile::tempdir().expect("ambiguous conflict cache directory");
    let fresh_cache = tempfile::tempdir().expect("ambiguous conflict fresh cache directory");
    let state = tempfile::tempdir().expect("ambiguous conflict state directory");
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let container = format!("{}/cbs", unique_suffix());
    let state_path = state.path().join("manifest-before.bin");
    let proxy = FaultProxy::start(&endpoint);
    run_child_phases(
        cache.path(),
        fresh_cache.path(),
        &state_path,
        &endpoint,
        &container,
        &[
            "ambiguous_phase_one",
            "ambiguous_phase_two",
            "ambiguous_newer_phase_three",
        ],
        &proxy,
    );
}

#[test]
#[ignore = "child phase for CAS conflict test"]
fn cas_conflict_phase_one() {
    if std::env::var_os(CACHE_ENV).is_none() {
        return;
    }
    let (cache, _fresh, state, endpoint, container) = phase_inputs();
    stage_large_update(&cache, &endpoint, &container, &state);
}

#[test]
#[ignore = "child phase for CAS conflict test"]
fn cas_conflict_phase_two() {
    if std::env::var_os(CACHE_ENV).is_none() {
        return;
    }
    let (cache, _fresh, state, endpoint, container) = phase_inputs();
    let storage = storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    vfs.attach(&AttachSpec::new(storage).alias(STAGED_ALIAS).if_not(true))
        .expect("reattach CAS conflict database");
    let before = fs::read(&state).expect("read pre-CAS manifest");
    assert_eq!(before, fetch_manifest(&endpoint, &container));
    let objects_before = list_remote_objects(&endpoint, &container);

    let first = vfs.upload(STAGED_ALIAS);
    assert!(
        first.is_err(),
        "a 412 manifest CAS conflict must surface as an error"
    );
    assert_eq!(
        proxy_manifest_requests(),
        1,
        "the failed upload must issue exactly one manifest PUT before the explicit retry"
    );
    assert_eq!(before, fetch_manifest(&endpoint, &container));
    assert!(
        list_remote_objects(&endpoint, &container) >= objects_before,
        "a failed manifest CAS must not delete staged block objects"
    );

    vfs.upload(STAGED_ALIAS)
        .expect("retry after a manifest CAS conflict must succeed");
    assert_ne!(before, fetch_manifest(&endpoint, &container));
}

#[test]
#[ignore = "child phase for CAS conflict test"]
fn cas_conflict_phase_three() {
    if std::env::var_os(CACHE_ENV).is_none() {
        return;
    }
    let (_cache, fresh, _state, endpoint, container) = phase_inputs();
    let storage = storage(&endpoint, &container);
    let vfs = new_vfs(&fresh);
    let alias = "cas_fresh";
    vfs.attach(&AttachSpec::new(storage).alias(alias))
        .expect("attach fresh CAS database");
    assert_final_rows(vfs, alias);
    vfs.detach(alias).expect("detach fresh CAS database");
}

#[test]
#[ignore = "child phase for ambiguous-manifest test"]
fn ambiguous_phase_one() {
    if std::env::var_os(CACHE_ENV).is_none() {
        return;
    }
    let (cache, _fresh, state, endpoint, container) = phase_inputs();
    stage_large_update(&cache, &endpoint, &container, &state);
}

#[test]
#[ignore = "child phase for ambiguous-manifest test"]
fn ambiguous_phase_two() {
    if std::env::var_os(CACHE_ENV).is_none() {
        return;
    }
    let (cache, _fresh, state, endpoint, container) = phase_inputs();
    let storage = storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    vfs.attach(&AttachSpec::new(storage).alias(STAGED_ALIAS).if_not(true))
        .expect("reattach ambiguous-response database");
    let before = fs::read(&state).expect("read pre-ambiguous manifest");
    assert_eq!(before, fetch_manifest(&endpoint, &container));

    let result = vfs.upload(STAGED_ALIAS);
    assert!(
        result.is_err(),
        "a dropped manifest response must remain ambiguous to the caller"
    );
    assert_ne!(
        before,
        fetch_manifest(&endpoint, &container),
        "the remote manifest must be committed before the response is dropped"
    );
}

#[test]
#[ignore = "child phase for ambiguous-manifest test"]
fn ambiguous_phase_three() {
    if std::env::var_os(CACHE_ENV).is_none() {
        return;
    }
    let (cache, _fresh, _state, endpoint, container) = phase_inputs();
    let storage = storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    vfs.attach(&AttachSpec::new(storage).alias(STAGED_ALIAS).if_not(true))
        .expect("restart after ambiguous manifest response");
    assert_final_rows(vfs, STAGED_ALIAS);
    let metadata = Connection::open(cache.join("blocksdb.bcv"))
        .expect("open ambiguous-publication cache metadata");
    let remote_manifest = fetch_manifest(&endpoint, &container);
    let pending: Vec<u8> = metadata
        .query_row("SELECT manifest FROM pending_publish LIMIT 1", [], |row| {
            row.get(0)
        })
        .expect("read durable pending publication manifest");
    assert_eq!(
        pending, remote_manifest,
        "recovery must match the exact durable candidate manifest"
    );
    vfs.poll(STAGED_ALIAS)
        .expect("poll must reconcile the remotely installed manifest");

    let pending_count: i64 = metadata
        .query_row("SELECT count(*) FROM pending_publish", [], |row| row.get(0))
        .expect("count pending publications after recovery");
    assert_eq!(pending_count, 0, "recovery must clear the durable marker");
    let staged_count: i64 = metadata
        .query_row("SELECT count(*) FROM staged", [], |row| row.get(0))
        .expect("count staged rows after recovery");
    assert_eq!(staged_count, 0, "recovery must clear uploaded staged rows");
    let dirty_count: i64 = metadata
        .query_row(
            "SELECT count(*) FROM block WHERE blockid IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("count dirty cache mappings after recovery");
    assert_eq!(
        dirty_count, 0,
        "recovery must persist dirty mappings as clean"
    );
    let clean_count: i64 = metadata
        .query_row(
            "SELECT count(*) FROM block WHERE blockid IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .expect("count clean cache mappings after recovery");
    assert!(
        clean_count > 0,
        "recovery must retain clean resident mappings"
    );

    vfs.upload(STAGED_ALIAS)
        .expect("retry publication after the ambiguous manifest response");
    assert_final_rows(vfs, STAGED_ALIAS);

    // A rewrite larger than cache capacity exercises LRU eligibility of the
    // recovered clean entries. Keep it local so phase four still verifies the
    // remotely published candidate from a fresh cache.
    let db = vfs
        .open(format!("/{STAGED_ALIAS}/streaming.sqlite"))
        .expect("open recovered database for bounded slot reuse");
    db.execute_batch("BEGIN IMMEDIATE")
        .expect("begin recovered database rewrite");
    {
        let mut update = db
            .prepare("UPDATE payload SET body = ?1 WHERE id = ?2")
            .expect("prepare recovered database rewrite");
        for id in 0..ROWS {
            update
                .execute(params![rewrite_body(id), id])
                .expect("rewrite recovered row");
            if id % 256 == 0 {
                assert_cache_bound(&cache, "post-recovery rewrite");
            }
        }
    }
    db.execute_batch("COMMIT")
        .expect("commit recovered database rewrite");
    assert_cache_bound(&cache, "post-recovery rewrite");
}

#[test]
#[ignore = "child phase for newer-local-write ambiguity test"]
fn ambiguous_newer_phase_three() {
    if std::env::var_os(CACHE_ENV).is_none() {
        return;
    }
    let (cache, _fresh, _state, endpoint, container) = phase_inputs();
    let storage = storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    vfs.attach(&AttachSpec::new(storage).alias(STAGED_ALIAS).if_not(true))
        .expect("restart after ambiguous manifest response");
    assert_final_rows(vfs, STAGED_ALIAS);

    let metadata =
        Connection::open(cache.join("blocksdb.bcv")).expect("open newer-write cache metadata");
    let candidate = fetch_manifest(&endpoint, &container);
    let (pending, max_sequence): (Vec<u8>, i64) = metadata
        .query_row(
            "SELECT manifest, maxseq FROM pending_publish LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read pending manifest before newer local write");
    assert_eq!(
        pending, candidate,
        "the ambiguity marker must identify the current remote candidate"
    );

    let db = vfs
        .open(format!("/{STAGED_ALIAS}/streaming.sqlite"))
        .expect("open database for newer local write");
    db.execute_batch("BEGIN IMMEDIATE")
        .expect("begin newer local write");
    db.execute("UPDATE payload SET body = ?1 WHERE id = 0", [newer_body(0)])
        .expect("update a row after ambiguous publication");
    db.execute_batch("COMMIT")
        .expect("commit newer local write");
    let body: String = db
        .query_row("SELECT body FROM payload WHERE id = 0", [], |row| {
            row.get(0)
        })
        .expect("read newer local write");
    assert_eq!(body, newer_body(0));
    drop(db);

    let newer_staged: i64 = metadata
        .query_row(
            "SELECT count(*) FROM staged WHERE sequence > ?1",
            [max_sequence],
            |row| row.get(0),
        )
        .expect("count staged rows after newer local write");
    assert_eq!(
        newer_staged, 0,
        "the regression must exercise dirty-byte verification, not the newer-stage guard"
    );

    let error = vfs
        .poll(STAGED_ALIAS)
        .expect_err("poll must reject a candidate with a newer local write");
    assert!(
        error.to_string().contains("write collision"),
        "poll must preserve ordinary manifest collision behavior: {error}"
    );
    assert_eq!(
        candidate,
        fetch_manifest(&endpoint, &container),
        "a newer local write must not replace the remotely published candidate"
    );
    let pending_after: Vec<u8> = metadata
        .query_row("SELECT manifest FROM pending_publish LIMIT 1", [], |row| {
            row.get(0)
        })
        .expect("retain pending marker after local-write collision");
    assert_eq!(pending_after, candidate);
    let body_after: String = vfs
        .open(format!("/{STAGED_ALIAS}/streaming.sqlite"))
        .expect("reopen database after write collision")
        .query_row("SELECT body FROM payload WHERE id = 0", [], |row| {
            row.get(0)
        })
        .expect("read newer local write after collision");
    assert_eq!(body_after, newer_body(0));
}

#[test]
#[ignore = "child phase for ambiguous-manifest test"]
fn ambiguous_phase_four() {
    if std::env::var_os(CACHE_ENV).is_none() {
        return;
    }
    let (_cache, fresh, _state, endpoint, container) = phase_inputs();
    let storage = storage(&endpoint, &container);
    let vfs = new_vfs(&fresh);
    let alias = "ambiguous_fresh";
    vfs.attach(&AttachSpec::new(storage).alias(alias))
        .expect("attach fresh database after ambiguous response");
    assert_final_rows(vfs, alias);
    vfs.detach(alias)
        .expect("detach fresh ambiguous-response database");
}

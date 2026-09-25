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
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Config, Storage};
use rusqlite::{params, Connection};

const CACHE_BYTES: u64 = 2 * 4 * 1024 * 1024;
const BODY_BYTES: usize = 2048;
const ROWS: i64 = 7_000;

const CACHE_ENV: &str = "BCV_STREAMING_CONFLICT_CACHE";
const FRESH_CACHE_ENV: &str = "BCV_STREAMING_CONFLICT_FRESH_CACHE";
const STATE_ENV: &str = "BCV_STREAMING_CONFLICT_STATE";
const ENDPOINT_ENV: &str = "BCV_STREAMING_CONFLICT_ENDPOINT";
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
    vfs.initialize_container(&storage)
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

fn set_proxy(proxy: &str) {
    for name in ["HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"] {
        // The test endpoint is HTTP; setting both proxy spellings prevents
        // libcurl/environment differences from bypassing the fault point.
        std::env::set_var(name, proxy);
    }
    for name in ["NO_PROXY", "no_proxy"] {
        std::env::set_var(name, "");
    }
}

fn clear_proxy() {
    for name in ["HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"] {
        std::env::remove_var(name);
    }
    for name in ["NO_PROXY", "no_proxy"] {
        std::env::set_var(name, "127.0.0.1,localhost");
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FaultMode {
    RejectManifestOnce,
    DropManifestResponseOnce,
}

struct FaultProxy {
    stop: Arc<AtomicBool>,
    manifest_requests: Arc<AtomicUsize>,
    forwarded_manifests: Arc<AtomicUsize>,
    dropped_responses: Arc<AtomicUsize>,
    handlers: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    thread: Option<thread::JoinHandle<()>>,
    url: String,
}

impl FaultProxy {
    fn start(endpoint: &str, mode: FaultMode) -> Self {
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
        let manifest_requests = Arc::new(AtomicUsize::new(0));
        let forwarded_manifests = Arc::new(AtomicUsize::new(0));
        let dropped_responses = Arc::new(AtomicUsize::new(0));
        let thread_stop = Arc::clone(&stop);
        let thread_requests = Arc::clone(&manifest_requests);
        let thread_forwarded = Arc::clone(&forwarded_manifests);
        let thread_dropped = Arc::clone(&dropped_responses);
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
                let handler_requests = Arc::clone(&thread_requests);
                let handler_forwarded = Arc::clone(&thread_forwarded);
                let handler_dropped = Arc::clone(&thread_dropped);
                let handler = thread::spawn(move || {
                    let _ = handle_proxy_connection(
                        stream,
                        &handler_upstream,
                        mode,
                        &handler_requests,
                        &handler_forwarded,
                        &handler_dropped,
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
            manifest_requests,
            forwarded_manifests,
            dropped_responses,
            handlers,
            thread: Some(thread),
            url,
        }
    }

    fn manifest_requests(&self) -> usize {
        self.manifest_requests.load(Ordering::Relaxed)
    }

    fn forwarded_manifests(&self) -> usize {
        self.forwarded_manifests.load(Ordering::Relaxed)
    }

    fn dropped_responses(&self) -> usize {
        self.dropped_responses.load(Ordering::Relaxed)
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
    mode: FaultMode,
    manifest_requests: &AtomicUsize,
    forwarded_manifests: &AtomicUsize,
    dropped_responses: &AtomicUsize,
) -> std::io::Result<()> {
    client.set_read_timeout(Some(Duration::from_secs(20)))?;
    let request = read_http_request(&mut client)?;
    let (method, target) = request_line(&request);
    let manifest = method == "PUT" && request_path(target).ends_with("/manifest.bcv");
    if !manifest {
        return forward_request(client, upstream_address, request, false);
    }

    let request_number = manifest_requests.fetch_add(1, Ordering::Relaxed);
    match mode {
        FaultMode::RejectManifestOnce if request_number == 0 => {
            client.write_all(b"HTTP/1.1 412 Precondition Failed\r\n")?;
            client.write_all(b"Content-Length: 0\r\nConnection: close\r\n\r\n")?;
            client.shutdown(Shutdown::Both)
        }
        FaultMode::DropManifestResponseOnce if request_number == 0 => {
            forwarded_manifests.fetch_add(1, Ordering::Relaxed);
            dropped_responses.fetch_add(1, Ordering::Relaxed);
            // The request is sent to the emulator and its complete response
            // is consumed, proving that the manifest PUT reached the remote
            // store before the client-side response is made ambiguous.
            let _ = forward_request(client, upstream_address, request, true);
            Ok(())
        }
        _ => {
            forwarded_manifests.fetch_add(1, Ordering::Relaxed);
            forward_request(client, upstream_address, request, false)
        }
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

fn run_phase(executable: &Path, name: &str, values: &[(&str, &str)]) -> ExitStatus {
    Command::new(executable)
        .args(["--ignored", "--exact", name, "--nocapture"])
        .envs(values.iter().copied())
        .status()
        .unwrap_or_else(|error| panic!("spawn {name}: {error}"))
}

fn run_child_phases(
    cache: &Path,
    fresh_cache: &Path,
    state: &Path,
    endpoint: &str,
    container: &str,
    phases: &[&str],
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
        let status = run_phase(&executable, phase, &values);
        assert!(status.success(), "conflict phase {phase} failed: {status}");
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

    let proxy = FaultProxy::start(&endpoint, FaultMode::RejectManifestOnce);
    set_proxy(&proxy.url);
    let first = vfs.upload(STAGED_ALIAS);
    clear_proxy();
    let requests = proxy.manifest_requests();
    drop(proxy);
    assert!(
        first.is_err(),
        "a 412 manifest CAS conflict must surface as an error"
    );
    assert_eq!(
        requests, 1,
        "the proxy must reject exactly one manifest PUT"
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

    let proxy = FaultProxy::start(&endpoint, FaultMode::DropManifestResponseOnce);
    set_proxy(&proxy.url);
    let result = vfs.upload(STAGED_ALIAS);
    clear_proxy();
    let manifest_requests = proxy.manifest_requests();
    let forwarded_manifests = proxy.forwarded_manifests();
    let dropped_responses = proxy.dropped_responses();
    drop(proxy);
    assert!(
        result.is_err(),
        "a dropped manifest response must remain ambiguous to the caller"
    );
    assert!(
        manifest_requests >= 2,
        "the client must retry after the first ambiguous manifest PUT"
    );
    assert_eq!(
        forwarded_manifests, manifest_requests,
        "every manifest PUT, including the retry, must reach the remote emulator"
    );
    assert_eq!(
        dropped_responses, 1,
        "the proxy must drop exactly the first manifest response"
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
    vfs.poll(STAGED_ALIAS)
        .expect("poll must reconcile the remotely installed manifest");
    vfs.upload(STAGED_ALIAS)
        .expect("restart/retry must reconcile an already-installed manifest");
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

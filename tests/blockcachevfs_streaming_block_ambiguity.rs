#![cfg(feature = "blockcachevfs")]

//! Exercise the failure window where a staged block PUT reaches the object
//! store but its response is lost before CBS records the successful PUT.
//!
//! Publication remains separate from staging: the remote manifest must stay
//! unchanged until the explicit upload call. The phases are separate
//! processes so cache metadata and staged mappings cross a real restart.

use std::fs;
use std::io::{self, Read as _, Write as _};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Config, Storage};
use rusqlite::{params, Connection};

const CACHE_BYTES: i64 = 2 * 4 * 1024 * 1024;
const BODY_BYTES: usize = 2048;
const ROWS: i64 = 7_000;
const PHASE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_PHASE";
const CACHE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_CACHE";
const FRESH_CACHE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_FRESH_CACHE";
const STATE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_STATE";
const ENDPOINT_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_ENDPOINT";
const CONTAINER_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_CONTAINER";
const ALIAS: &str = "streaming_block_ambiguity";
const FAILURE_PHASE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_FAILURE_PHASE";
const FAILURE_CACHE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_FAILURE_CACHE";
const FAILURE_STATE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_FAILURE_STATE";
const FAILURE_ENDPOINT_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_FAILURE_ENDPOINT";
const FAILURE_CONTAINER_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_FAILURE_CONTAINER";
const FAILURE_ALIAS: &str = "streaming_block_ambiguity_failure";
const OPEN_UPLOAD_PHASE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_OPEN_UPLOAD_PHASE";
const OPEN_UPLOAD_CACHE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_OPEN_UPLOAD_CACHE";
const OPEN_UPLOAD_ENDPOINT_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_OPEN_UPLOAD_ENDPOINT";
const OPEN_UPLOAD_CONTAINER_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_OPEN_UPLOAD_CONTAINER";
const OPEN_UPLOAD_ALIAS: &str = "streaming_block_ambiguity_open_upload";
const COMBINED_PHASE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_COMBINED_PHASE";
const COMBINED_CACHE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_COMBINED_CACHE";
const COMBINED_STATE_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_COMBINED_STATE";
const COMBINED_ENDPOINT_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_COMBINED_ENDPOINT";
const COMBINED_CONTAINER_ENV: &str = "BCV_STREAMING_BLOCK_AMBIGUITY_COMBINED_CONTAINER";
const COMBINED_ALIAS: &str = "streaming_block_ambiguity_combined";

fn unique_suffix() -> String {
    format!(
        "rust-streaming-block-ambiguity-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_nanos()
    )
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for block ambiguity phase"))
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
        .expect("block ambiguity container must contain a prefix");
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
        .expect("block ambiguity container must contain a prefix");
    let body = tempfile::NamedTempFile::new().expect("object-list temporary file");
    let url = format!(
        "{}/{}/?list-type=2&prefix={}&max-keys=1000",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_component(&format!("{prefix}/")),
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

fn storage(endpoint: &str, container: &str) -> Storage {
    Storage::s3_with_endpoint("test", container, "us-east-1", endpoint)
}

fn new_vfs(cache: &Path) -> &'static BlockCacheVfs {
    BlockCacheVfs::builder(cache)
        .expect("VFS builder")
        .auth_callback(|_, _, _| Ok("test".into()))
        .config(Config::CacheSize(CACHE_BYTES))
        .config(Config::RequestCount(1))
        .config(Config::HttpTimeout(2))
        .init()
        .expect("initialize block-cache VFS")
}

fn set_proxy(proxy: &str) {
    for name in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        std::env::set_var(name, proxy);
    }
    for name in ["NO_PROXY", "no_proxy"] {
        std::env::set_var(name, "");
    }
}

fn clear_proxy() {
    for name in [
        "HTTP_PROXY",
        "http_proxy",
        "HTTPS_PROXY",
        "https_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        std::env::remove_var(name);
    }
    for name in ["NO_PROXY", "no_proxy"] {
        std::env::set_var(name, "127.0.0.1,localhost");
    }
}

struct BlockPutAmbiguityProxy {
    stop: Arc<AtomicBool>,
    block_puts: Arc<AtomicUsize>,
    dropped_responses: Arc<AtomicUsize>,
    drop_next_block: Arc<AtomicBool>,
    drop_all_blocks: Arc<AtomicBool>,
    handlers: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    thread: Option<thread::JoinHandle<()>>,
    url: String,
}

impl BlockPutAmbiguityProxy {
    fn start(endpoint: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind block ambiguity proxy");
        listener
            .set_nonblocking(true)
            .expect("set block ambiguity proxy nonblocking");
        let address = listener
            .local_addr()
            .expect("block ambiguity proxy address");
        let url = format!("http://{address}");
        let upstream = endpoint
            .strip_prefix("http://")
            .or_else(|| endpoint.strip_prefix("https://"))
            .and_then(|value| value.split('/').next())
            .expect("block ambiguity proxy requires an HTTP endpoint")
            .to_owned();
        let stop = Arc::new(AtomicBool::new(false));
        let block_puts = Arc::new(AtomicUsize::new(0));
        let dropped_responses = Arc::new(AtomicUsize::new(0));
        let drop_next_block = Arc::new(AtomicBool::new(false));
        let drop_all_blocks = Arc::new(AtomicBool::new(false));
        let handlers = Arc::new(Mutex::new(Vec::new()));
        let thread_stop = Arc::clone(&stop);
        let thread_block_puts = Arc::clone(&block_puts);
        let thread_dropped = Arc::clone(&dropped_responses);
        let thread_drop_next = Arc::clone(&drop_next_block);
        let thread_drop_all = Arc::clone(&drop_all_blocks);
        let thread_handlers = Arc::clone(&handlers);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                let (stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(_) => break,
                };
                let handler_upstream = upstream.clone();
                let handler_block_puts = Arc::clone(&thread_block_puts);
                let handler_dropped = Arc::clone(&thread_dropped);
                let handler_drop_next = Arc::clone(&thread_drop_next);
                let handler_drop_all = Arc::clone(&thread_drop_all);
                let handler = thread::spawn(move || {
                    let _ = handle_proxy_connection(
                        stream,
                        &handler_upstream,
                        &handler_block_puts,
                        &handler_dropped,
                        &handler_drop_next,
                        &handler_drop_all,
                    );
                });
                thread_handlers
                    .lock()
                    .expect("lock block ambiguity proxy handlers")
                    .push(handler);
            }
        });
        Self {
            stop,
            block_puts,
            dropped_responses,
            drop_next_block,
            drop_all_blocks,
            handlers,
            thread: Some(thread),
            url,
        }
    }

    fn drop_next_block_response(&self) {
        self.drop_next_block.store(true, Ordering::Release);
    }

    fn set_drop_all_block_responses(&self, drop: bool) {
        self.drop_all_blocks.store(drop, Ordering::Release);
    }

    fn block_puts(&self) -> usize {
        self.block_puts.load(Ordering::Acquire)
    }

    fn dropped_responses(&self) -> usize {
        self.dropped_responses.load(Ordering::Acquire)
    }
}

impl Drop for BlockPutAmbiguityProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join block ambiguity proxy");
        }
        let handlers = std::mem::take(
            &mut *self
                .handlers
                .lock()
                .expect("lock block ambiguity handlers for join"),
        );
        for handler in handlers {
            handler.join().expect("join block ambiguity connection");
        }
    }
}

fn handle_proxy_connection(
    mut client: TcpStream,
    upstream_address: &str,
    block_puts: &AtomicUsize,
    dropped_responses: &AtomicUsize,
    drop_next_block: &AtomicBool,
    drop_all_blocks: &AtomicBool,
) -> io::Result<()> {
    client.set_read_timeout(Some(Duration::from_secs(20)))?;
    let request = read_http_request(&mut client)?;
    let (method, target) = request_line(&request);
    let block_put = method == "PUT" && !request_path(target).ends_with("/manifest.bcv");
    if block_put {
        block_puts.fetch_add(1, Ordering::Relaxed);
    }
    let discard_response = block_put
        && (drop_all_blocks.load(Ordering::Acquire)
            || drop_next_block.swap(false, Ordering::AcqRel));
    if discard_response {
        dropped_responses.fetch_add(1, Ordering::Relaxed);
    }
    forward_request(client, upstream_address, request, discard_response)
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

fn read_http_request(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 16 * 1024];
    let header_end = loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy client closed before HTTP headers",
            ));
        }
        request.extend_from_slice(&buffer[..read]);
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
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
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
) -> io::Result<()> {
    let mut upstream = TcpStream::connect(upstream_address)?;
    upstream.set_read_timeout(Some(Duration::from_secs(20)))?;
    upstream.write_all(&with_connection_close(origin_form_request(request)))?;
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

fn read_http_response(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    loop {
        let mut response = Vec::new();
        let mut buffer = [0_u8; 16 * 1024];
        let header_end = loop {
            let read = stream.read(&mut buffer)?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
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
        if (100..200).contains(&status) {
            continue;
        }
        if status == 204 || status == 304 {
            return Ok(response);
        }
        if let Some(content_length) = content_length {
            while response.len() < header_end + content_length {
                let read = stream.read(&mut buffer)?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "upstream closed before the HTTP response body",
                    ));
                }
                response.extend_from_slice(&buffer[..read]);
            }
            response.truncate(header_end + content_length);
            return Ok(response);
        }
        if chunked {
            loop {
                let read = stream.read(&mut buffer)?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
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
    test_name: &str,
    phase_env: &str,
    phase: &str,
    values: &[(&str, &str)],
) {
    let status = Command::new(executable)
        .args(["--ignored", "--exact", test_name, "--nocapture"])
        .env(phase_env, phase)
        .envs(values.iter().copied())
        .status()
        .unwrap_or_else(|error| panic!("spawn block ambiguity phase {phase}: {error}"));
    assert!(
        status.success(),
        "block ambiguity phase {phase} failed: {status}"
    );
}

fn phase_one() {
    let endpoint = required_env(ENDPOINT_ENV);
    let cache = PathBuf::from(required_env(CACHE_ENV));
    let state = PathBuf::from(required_env(STATE_ENV));
    let container = required_env(CONTAINER_ENV);
    let storage = storage(&endpoint, &container);
    let proxy = BlockPutAmbiguityProxy::start(&endpoint);
    set_proxy(&proxy.url);
    let vfs = new_vfs(&cache);
    vfs.initialize_container(&storage)
        .expect("initialize block ambiguity container through proxy");

    let local_dir = tempfile::tempdir().expect("local seed database directory");
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

    vfs.attach(&AttachSpec::new(storage).alias(ALIAS))
        .expect("attach block ambiguity database");
    let before = fetch_manifest(&endpoint, &container);
    let objects_before = list_remote_objects(&endpoint, &container);
    proxy.drop_next_block_response();
    let db = vfs
        .open(format!("/{ALIAS}/streaming.sqlite"))
        .expect("open block ambiguity database");
    db.execute_batch("PRAGMA cache_size = 16; BEGIN IMMEDIATE;")
        .expect("begin block ambiguity update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare block ambiguity insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert block ambiguity row");
        }
    }
    db.execute_batch("COMMIT")
        .expect("ambiguous block PUT must be resolved by an immutable-object retry");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("count locally committed rows after ambiguous PUT");
    assert_eq!(count, ROWS);
    drop(db);
    clear_proxy();

    assert_eq!(
        proxy.dropped_responses(),
        1,
        "the proxy must drop exactly one staged block PUT response"
    );
    assert!(
        proxy.block_puts() > 0,
        "the update must issue at least one staged block PUT"
    );
    assert_eq!(
        before,
        fetch_manifest(&endpoint, &container),
        "staging a block must not publish the manifest"
    );
    assert!(
        list_remote_objects(&endpoint, &container) > objects_before,
        "the object store must contain the staged block despite the lost response"
    );
    fs::write(&state, before).expect("save pre-publication manifest");
    drop(proxy);
}

fn assert_rows(vfs: &'static BlockCacheVfs, alias: &str) {
    let db = vfs
        .open(format!("/{alias}/streaming.sqlite"))
        .expect("open recovered block ambiguity database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM payload", [], |row| row.get(0))
        .expect("count recovered rows");
    assert_eq!(
        count, ROWS,
        "restart must retain all locally committed rows"
    );
    for id in [0, ROWS / 2, ROWS - 1] {
        let body: String = db
            .query_row("SELECT body FROM payload WHERE id=?1", [id], |row| {
                row.get(0)
            })
            .expect("read recovered row");
        assert_eq!(body, row_body(id), "recovered row {id} has wrong data");
    }
    drop(db);
}

fn phase_two() {
    let endpoint = required_env(ENDPOINT_ENV);
    let cache = PathBuf::from(required_env(CACHE_ENV));
    let state = PathBuf::from(required_env(STATE_ENV));
    let container = required_env(CONTAINER_ENV);
    let storage = storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    vfs.attach(&AttachSpec::new(storage).alias(ALIAS).if_not(true))
        .expect("reattach block ambiguity database after restart");
    assert_eq!(
        fs::read(&state).expect("read pre-publication manifest"),
        fetch_manifest(&endpoint, &container),
        "restart must observe the unchanged manifest before upload"
    );
    assert_rows(vfs, ALIAS);
    vfs.upload(ALIAS)
        .expect("explicit upload must publish the recovered staged mapping");
    assert_ne!(
        fs::read(&state).expect("read pre-publication manifest"),
        fetch_manifest(&endpoint, &container),
        "explicit upload must publish a new manifest"
    );
}

fn phase_three() {
    let endpoint = required_env(ENDPOINT_ENV);
    let fresh_cache = PathBuf::from(required_env(FRESH_CACHE_ENV));
    let container = required_env(CONTAINER_ENV);
    let storage = storage(&endpoint, &container);
    let vfs = new_vfs(&fresh_cache);
    let alias = "streaming_block_ambiguity_fresh";
    vfs.attach(&AttachSpec::new(storage).alias(alias))
        .expect("attach fresh cache after publication");
    assert_rows(vfs, alias);
}

fn failure_phase_one() {
    let endpoint = required_env(FAILURE_ENDPOINT_ENV);
    let cache = PathBuf::from(required_env(FAILURE_CACHE_ENV));
    let state = PathBuf::from(required_env(FAILURE_STATE_ENV));
    let container = required_env(FAILURE_CONTAINER_ENV);
    let storage = storage(&endpoint, &container);
    let proxy = BlockPutAmbiguityProxy::start(&endpoint);
    set_proxy(&proxy.url);
    let vfs = new_vfs(&cache);
    vfs.initialize_container(&storage)
        .expect("initialize block ambiguity failure container through proxy");

    let local_dir = tempfile::tempdir().expect("local failure seed directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local failure seed database");
    local
        .execute_batch(
            "PRAGMA page_size = 4096;
             CREATE TABLE payload(id INTEGER PRIMARY KEY, body TEXT NOT NULL);",
        )
        .expect("create failure seed schema");
    let _: String = local
        .query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })
        .expect("commit failure seed database");
    local.close().expect("close failure seed database");
    vfs.create_database(&storage, &local_path, "streaming.sqlite")
        .expect("upload failure seed database");

    vfs.attach(&AttachSpec::new(storage).alias(FAILURE_ALIAS))
        .expect("attach block ambiguity failure database");
    let db = vfs
        .open(format!("/{FAILURE_ALIAS}/streaming.sqlite"))
        .expect("open block ambiguity failure database");
    db.execute_batch("PRAGMA cache_size = 16; BEGIN IMMEDIATE;")
        .expect("begin baseline committed update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare baseline insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert baseline row");
        }
    }
    db.execute_batch("COMMIT")
        .expect("commit baseline update before failure injection");
    drop(db);
    vfs.upload(FAILURE_ALIAS)
        .expect("publish baseline before ambiguous failure");
    let before = fetch_manifest(&endpoint, &container);
    let objects_before = list_remote_objects(&endpoint, &container);

    proxy.set_drop_all_block_responses(true);
    let db = vfs
        .open(format!("/{FAILURE_ALIAS}/streaming.sqlite"))
        .expect("reopen database for ambiguous failure update");
    let mut failed = None;
    db.execute_batch("BEGIN IMMEDIATE")
        .expect("begin ambiguous failure update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare ambiguous failure insert");
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
    clear_proxy();

    let error = failed.expect("dropping every block PUT response must surface an error");
    assert!(
        matches!(error, rusqlite::Error::SqliteFailure(_, _)),
        "unexpected ambiguous block PUT error: {error:?}"
    );
    assert!(
        proxy.dropped_responses() > 0,
        "the failure update must drop at least one staged block response"
    );
    assert!(
        proxy.block_puts() > 0,
        "the failure update must issue a staged block PUT"
    );
    assert_eq!(
        before,
        fetch_manifest(&endpoint, &container),
        "an ambiguous staged block PUT must not publish the manifest"
    );
    assert!(
        list_remote_objects(&endpoint, &container) > objects_before,
        "the emulator must retain an object even though every response was dropped"
    );

    // SQLite rolls back the interrupted transaction before this process
    // exits. Therefore the recoverability boundary here is the last
    // committed baseline, not the uncommitted rows from the failed update.
    drop(db);
    assert_rows(vfs, FAILURE_ALIAS);
    fs::write(&state, before).expect("save baseline manifest");
    drop(proxy);
}

fn failure_phase_two() {
    let endpoint = required_env(FAILURE_ENDPOINT_ENV);
    let cache = PathBuf::from(required_env(FAILURE_CACHE_ENV));
    let state = PathBuf::from(required_env(FAILURE_STATE_ENV));
    let container = required_env(FAILURE_CONTAINER_ENV);
    let storage = storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    vfs.attach(&AttachSpec::new(storage).alias(FAILURE_ALIAS).if_not(true))
        .expect("reattach block ambiguity failure database after restart");
    assert_eq!(
        fs::read(&state).expect("read baseline manifest"),
        fetch_manifest(&endpoint, &container),
        "restart after an ambiguous block PUT must retain the baseline manifest"
    );
    assert_rows(vfs, FAILURE_ALIAS);
}

fn open_upload_phase() {
    let endpoint = required_env(OPEN_UPLOAD_ENDPOINT_ENV);
    let cache = PathBuf::from(required_env(OPEN_UPLOAD_CACHE_ENV));
    let container = required_env(OPEN_UPLOAD_CONTAINER_ENV);
    let storage = storage(&endpoint, &container);
    let proxy = BlockPutAmbiguityProxy::start(&endpoint);
    set_proxy(&proxy.url);
    let vfs = new_vfs(&cache);
    vfs.initialize_container(&storage)
        .expect("initialize open-upload container");

    let local_dir = tempfile::tempdir().expect("local open-upload seed directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local open-upload seed database");
    local
        .execute_batch(
            "PRAGMA page_size = 4096;
             CREATE TABLE payload(id INTEGER PRIMARY KEY, body TEXT NOT NULL);",
        )
        .expect("create open-upload seed schema");
    let _: String = local
        .query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })
        .expect("commit open-upload seed database");
    local.close().expect("close open-upload seed database");
    vfs.create_database(&storage, &local_path, "streaming.sqlite")
        .expect("upload open-upload seed database");

    vfs.attach(&AttachSpec::new(storage).alias(OPEN_UPLOAD_ALIAS))
        .expect("attach open-upload database");
    let before = fetch_manifest(&endpoint, &container);
    let db = vfs
        .open(format!("/{OPEN_UPLOAD_ALIAS}/streaming.sqlite"))
        .expect("open open-upload database");
    db.execute_batch("PRAGMA cache_size = 16; BEGIN IMMEDIATE;")
        .expect("begin open-upload update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare open-upload insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert open-upload row");
        }
    }
    db.execute_batch("COMMIT")
        .expect("commit open-upload update");

    // The public API documentation demonstrates upload before dropping the
    // connection. Keep that contract covered independently of the safer
    // close-before-upload lifecycle used by the restart tests. A native
    // crash cannot be converted into a Result: the parent phase runner will
    // observe a signal and fail this test.
    let result = vfs.upload(OPEN_UPLOAD_ALIAS);
    clear_proxy();
    match result {
        Ok(()) => assert_ne!(
            before,
            fetch_manifest(&endpoint, &container),
            "successful open-connection upload must publish the manifest"
        ),
        Err(error) => {
            assert!(
                matches!(error, rusqlite::Error::SqliteFailure(_, _)),
                "open-connection upload returned an uncontrolled error: {error:?}"
            );
            assert_eq!(
                before,
                fetch_manifest(&endpoint, &container),
                "failed open-connection upload must not publish a manifest"
            );
        }
    }
    drop(db);
    drop(proxy);
}

fn combined_phase_one() {
    let endpoint = required_env(COMBINED_ENDPOINT_ENV);
    let cache = PathBuf::from(required_env(COMBINED_CACHE_ENV));
    let state = PathBuf::from(required_env(COMBINED_STATE_ENV));
    let container = required_env(COMBINED_CONTAINER_ENV);
    let storage = storage(&endpoint, &container);
    let proxy = BlockPutAmbiguityProxy::start(&endpoint);
    set_proxy(&proxy.url);
    let vfs = new_vfs(&cache);
    vfs.initialize_container(&storage)
        .expect("initialize combined open-upload container");

    let local_dir = tempfile::tempdir().expect("local combined seed directory");
    let local_path = local_dir.path().join("seed.sqlite");
    let local = Connection::open(&local_path).expect("create local combined seed database");
    local
        .execute_batch(
            "PRAGMA page_size = 4096;
             CREATE TABLE payload(id INTEGER PRIMARY KEY, body TEXT NOT NULL);",
        )
        .expect("create combined seed schema");
    let _: String = local
        .query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })
        .expect("commit combined seed database");
    local.close().expect("close combined seed database");
    vfs.create_database(&storage, &local_path, "streaming.sqlite")
        .expect("upload combined seed database");

    vfs.attach(&AttachSpec::new(storage).alias(COMBINED_ALIAS))
        .expect("attach combined open-upload database");
    let db = vfs
        .open(format!("/{COMBINED_ALIAS}/streaming.sqlite"))
        .expect("open combined open-upload database");
    db.execute_batch("PRAGMA cache_size = 16; BEGIN IMMEDIATE;")
        .expect("begin combined baseline update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare combined baseline insert");
        for id in 0..ROWS {
            insert
                .execute(params![id, row_body(id)])
                .expect("insert combined baseline row");
        }
    }
    db.execute_batch("COMMIT")
        .expect("commit combined baseline update");

    // Preserve the original sequence: publish while this writer connection
    // is still open, then close it before the injected follow-up update.
    let baseline_upload = vfs.upload(COMBINED_ALIAS);
    assert!(
        baseline_upload.is_ok(),
        "open-connection baseline upload must succeed: {baseline_upload:?}"
    );
    let baseline_manifest = fetch_manifest(&endpoint, &container);
    drop(db);
    let objects_before = list_remote_objects(&endpoint, &container);

    proxy.set_drop_all_block_responses(true);
    let db = vfs
        .open(format!("/{COMBINED_ALIAS}/streaming.sqlite"))
        .expect("reopen combined database for ambiguous update");
    let mut failed = None;
    db.execute_batch("BEGIN IMMEDIATE")
        .expect("begin combined ambiguous update");
    {
        let mut insert = db
            .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
            .expect("prepare combined ambiguous insert");
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
    drop(db);
    clear_proxy();

    let error = failed.expect("combined ambiguous update must return an error");
    assert!(
        matches!(error, rusqlite::Error::SqliteFailure(_, _)),
        "unexpected combined ambiguous update error: {error:?}"
    );
    assert!(
        proxy.dropped_responses() > 0,
        "combined ambiguous update must drop a staged block response"
    );
    assert_eq!(
        baseline_manifest,
        fetch_manifest(&endpoint, &container),
        "combined failed update must not change the published baseline manifest"
    );
    assert!(
        list_remote_objects(&endpoint, &container) > objects_before,
        "combined ambiguous update must leave a stored remote block object"
    );
    fs::write(&state, baseline_manifest).expect("save combined baseline manifest");
    drop(proxy);
}

fn combined_phase_two() {
    let endpoint = required_env(COMBINED_ENDPOINT_ENV);
    let cache = PathBuf::from(required_env(COMBINED_CACHE_ENV));
    let state = PathBuf::from(required_env(COMBINED_STATE_ENV));
    let container = required_env(COMBINED_CONTAINER_ENV);
    let storage = storage(&endpoint, &container);
    let vfs = new_vfs(&cache);
    vfs.attach(&AttachSpec::new(storage).alias(COMBINED_ALIAS).if_not(true))
        .expect("reattach combined database after ambiguous update");
    assert_eq!(
        fs::read(&state).expect("read combined baseline manifest"),
        fetch_manifest(&endpoint, &container),
        "combined restart must retain the published baseline manifest"
    );
    assert_rows(vfs, COMBINED_ALIAS);
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn ambiguous_staged_block_put_failure_preserves_committed_data() {
    if let Some(phase) = std::env::var_os(FAILURE_PHASE_ENV) {
        match phase.to_str() {
            Some("one") => failure_phase_one(),
            Some("two") => failure_phase_two(),
            Some(other) => panic!("unknown block ambiguity failure phase {other}"),
            None => panic!("block ambiguity failure phase is not UTF-8"),
        }
        return;
    }

    let executable = std::env::current_exe().expect("locate block ambiguity executable");
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let cache = tempfile::tempdir().expect("block ambiguity failure cache directory");
    let state = tempfile::tempdir().expect("block ambiguity failure state directory");
    let container = format!("{}/cbs", unique_suffix());
    let state_path = state.path().join("manifest-before.bin");
    let values = [
        (
            FAILURE_CACHE_ENV,
            cache.path().to_str().expect("failure cache path is UTF-8"),
        ),
        (
            FAILURE_STATE_ENV,
            state_path.to_str().expect("failure state path is UTF-8"),
        ),
        (FAILURE_ENDPOINT_ENV, endpoint.as_str()),
        (FAILURE_CONTAINER_ENV, container.as_str()),
    ];
    for phase in ["one", "two"] {
        run_phase(
            &executable,
            "ambiguous_staged_block_put_failure_preserves_committed_data",
            FAILURE_PHASE_ENV,
            phase,
            &values,
        );
    }
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn open_connection_upload_returns_controlled_result() {
    if let Some(phase) = std::env::var_os(OPEN_UPLOAD_PHASE_ENV) {
        match phase.to_str() {
            Some("one") => open_upload_phase(),
            Some(other) => panic!("unknown open-upload phase {other}"),
            None => panic!("open-upload phase is not UTF-8"),
        }
        return;
    }

    let executable = std::env::current_exe().expect("locate open-upload executable");
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let cache = tempfile::tempdir().expect("open-upload cache directory");
    let container = format!("{}/cbs", unique_suffix());
    let values = [
        (
            OPEN_UPLOAD_CACHE_ENV,
            cache
                .path()
                .to_str()
                .expect("open-upload cache path is UTF-8"),
        ),
        (OPEN_UPLOAD_ENDPOINT_ENV, endpoint.as_str()),
        (OPEN_UPLOAD_CONTAINER_ENV, container.as_str()),
    ];
    run_phase(
        &executable,
        "open_connection_upload_returns_controlled_result",
        OPEN_UPLOAD_PHASE_ENV,
        "one",
        &values,
    );
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn open_upload_then_ambiguous_update_preserves_baseline() {
    if let Some(phase) = std::env::var_os(COMBINED_PHASE_ENV) {
        match phase.to_str() {
            Some("one") => combined_phase_one(),
            Some("two") => combined_phase_two(),
            Some(other) => panic!("unknown combined ambiguity phase {other}"),
            None => panic!("combined ambiguity phase is not UTF-8"),
        }
        return;
    }

    let executable = std::env::current_exe().expect("locate combined ambiguity executable");
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let cache = tempfile::tempdir().expect("combined ambiguity cache directory");
    let state = tempfile::tempdir().expect("combined ambiguity state directory");
    let container = format!("{}/cbs", unique_suffix());
    let state_path = state.path().join("manifest-before.bin");
    let values = [
        (
            COMBINED_CACHE_ENV,
            cache.path().to_str().expect("combined cache path is UTF-8"),
        ),
        (
            COMBINED_STATE_ENV,
            state_path.to_str().expect("combined state path is UTF-8"),
        ),
        (COMBINED_ENDPOINT_ENV, endpoint.as_str()),
        (COMBINED_CONTAINER_ENV, container.as_str()),
    ];
    for phase in ["one", "two"] {
        run_phase(
            &executable,
            "open_upload_then_ambiguous_update_preserves_baseline",
            COMBINED_PHASE_ENV,
            phase,
            &values,
        );
    }
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn ambiguous_staged_block_put_is_recoverable_after_restart() {
    if let Some(phase) = std::env::var_os(PHASE_ENV) {
        match phase.to_str() {
            Some("one") => phase_one(),
            Some("two") => phase_two(),
            Some("three") => phase_three(),
            Some(other) => panic!("unknown block ambiguity phase {other}"),
            None => panic!("block ambiguity phase is not UTF-8"),
        }
        return;
    }

    let executable = std::env::current_exe().expect("locate block ambiguity executable");
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let cache = tempfile::tempdir().expect("block ambiguity cache directory");
    let fresh_cache = tempfile::tempdir().expect("block ambiguity fresh cache directory");
    let state = tempfile::tempdir().expect("block ambiguity state directory");
    let container = format!("{}/cbs", unique_suffix());
    let state_path = state.path().join("manifest-before.bin");
    let values = [
        (
            CACHE_ENV,
            cache.path().to_str().expect("cache path is UTF-8"),
        ),
        (
            FRESH_CACHE_ENV,
            fresh_cache
                .path()
                .to_str()
                .expect("fresh cache path is UTF-8"),
        ),
        (STATE_ENV, state_path.to_str().expect("state path is UTF-8")),
        (ENDPOINT_ENV, endpoint.as_str()),
        (CONTAINER_ENV, container.as_str()),
    ];
    for phase in ["one", "two", "three"] {
        run_phase(
            &executable,
            "ambiguous_staged_block_put_is_recoverable_after_restart",
            PHASE_ENV,
            phase,
            &values,
        );
    }
}

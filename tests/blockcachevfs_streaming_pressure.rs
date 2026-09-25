#![cfg(feature = "blockcachevfs")]

//! Pressure tests for the bounded block-cache implementation.
//!
//! These tests deliberately use one VFS cache for two logical databases. The
//! cache is only two 4 MiB slots, while the writer produces several times that
//! amount of data and a separate reader repeatedly scans the other database.
//! The test is ignored because it requires the pinned local Floci container;
//! it never contacts a production object store.

use std::fs;
use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{AttachSpec, BlockCacheVfs, Config, Storage};
use rusqlite::{params, Connection, Error, ErrorCode};

const BLOCK_BYTES: u64 = 4 * 1024 * 1024;
const CACHE_BYTES: i64 = (2 * BLOCK_BYTES) as i64;
const BODY_BYTES: usize = 1536;
const SEED_ROWS: i64 = 64;
const SCAN_ROWS: i64 = 2_048;
const WRITE_ROWS: i64 = 7_000;

fn unique_suffix() -> String {
    format!(
        "rust-streaming-pressure-{}-{}",
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
        .expect("pressure-test container must contain a prefix");
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
        .expect("pressure-test container must contain a prefix");
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

fn assert_cache_bound(cache: &Path, context: &str) {
    let cache_file = cache.join("cachefile.bcv");
    let size = fs::metadata(&cache_file).map_or(0, |metadata| metadata.len());
    assert!(
        size <= CACHE_BYTES as u64,
        "cachefile.bcv grew beyond {CACHE_BYTES} bytes ({size}) during {context}"
    );

    // A bounded cache reuses this payload file. Eviction must not create one
    // local payload file per logical block.
    let allowed = [
        ".blocksdb.bcv-lock",
        "blocksdb.bcv",
        "cachefile.bcv",
        "portnumber.bcv",
    ];
    for entry in fs::read_dir(cache).expect("read cache directory") {
        let entry = entry.expect("read cache directory entry");
        if entry.file_type().expect("inspect cache entry").is_file() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            assert!(
                allowed.iter().any(|allowed| *allowed == name)
                    || name.starts_with("blocksdb.bcv-")
                    || name.starts_with("cachefile.bcv-"),
                "unexpected per-block cache payload {name:?} during {context}"
            );
        }
    }
}

fn create_seed(path: &Path, rows: i64) {
    let db = Connection::open(path).expect("create local seed database");
    db.execute_batch(
        "PRAGMA page_size = 4096;
         CREATE TABLE payload(id INTEGER PRIMARY KEY, body TEXT NOT NULL);",
    )
    .expect("create seed schema");
    let mut insert = db
        .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
        .expect("prepare seed insert");
    for id in 0..rows {
        insert
            .execute(params![id, row_body(id)])
            .expect("insert seed row");
    }
    drop(insert);
    let _: String = db
        .query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })
        .expect("commit seed database");
    db.close().expect("close local seed database");
}

fn new_vfs(cache: &Path) -> &'static BlockCacheVfs {
    BlockCacheVfs::builder(cache)
        .expect("VFS builder")
        .auth_callback(|_, _, _| Ok("test".into()))
        .config(Config::CacheSize(CACHE_BYTES))
        .config(Config::RequestCount(4))
        .config(Config::HttpTimeout(5))
        .init()
        .expect("initialize block-cache VFS")
}

fn direct_storage(endpoint: &str, container: &str) -> Storage {
    Storage::s3_with_endpoint("test", container, "us-east-1", endpoint)
}

fn setup_two_databases(
    vfs: &'static BlockCacheVfs,
    endpoint: &str,
    cache: &Path,
) -> (Storage, String, String) {
    let suffix = unique_suffix();
    let bucket = format!("rust-streaming-pressure-{suffix}");
    let container = format!("{bucket}/{suffix}/cbs");
    let storage = direct_storage(endpoint, &container);
    vfs.initialize_container(&storage)
        .expect("initialize pressure-test container");

    let local_dir = tempfile::tempdir().expect("local seed directory");
    let first = local_dir.path().join("first.sqlite");
    let second = local_dir.path().join("second.sqlite");
    create_seed(&first, SEED_ROWS);
    create_seed(&second, SCAN_ROWS);
    vfs.create_database(&storage, &first, "first.sqlite")
        .expect("upload first seed database");
    vfs.create_database(&storage, &second, "second.sqlite")
        .expect("upload second seed database");
    assert_cache_bound(cache, "database bootstrap");
    (storage, bucket, container)
}

fn setup_three_databases(
    vfs: &'static BlockCacheVfs,
    endpoint: &str,
    cache: &Path,
) -> (Storage, String) {
    let suffix = unique_suffix();
    let bucket = format!("rust-streaming-pressure-pinned-{suffix}");
    let container = format!("{bucket}/{suffix}/cbs");
    let storage = direct_storage(endpoint, &container);
    vfs.initialize_container(&storage)
        .expect("initialize pinned-pressure container");

    let local_dir = tempfile::tempdir().expect("pinned-pressure seed directory");
    for name in ["first.sqlite", "second.sqlite", "third.sqlite"] {
        let local_path = local_dir.path().join(name);
        create_seed(&local_path, SEED_ROWS);
        vfs.create_database(&storage, &local_path, name)
            .unwrap_or_else(|error| panic!("upload pinned-pressure seed {name}: {error:?}"));
    }
    assert_cache_bound(cache, "pinned-pressure bootstrap");
    (storage, container)
}

#[derive(Debug)]
enum WriterOutcome {
    Committed,
    Failed(Error),
}

fn write_under_pressure(
    vfs: &'static BlockCacheVfs,
    path: String,
    cache: PathBuf,
) -> WriterOutcome {
    let db = match vfs.open(path) {
        Ok(db) => db,
        Err(error) => return WriterOutcome::Failed(error),
    };
    if let Err(error) = db.execute_batch("PRAGMA cache_size = 8; BEGIN IMMEDIATE;") {
        return WriterOutcome::Failed(error);
    }

    let mut insert = match db.prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)") {
        Ok(insert) => insert,
        Err(error) => return WriterOutcome::Failed(error),
    };
    for id in SEED_ROWS..(SEED_ROWS + WRITE_ROWS) {
        if let Err(error) = insert.execute(params![id, row_body(id)]) {
            drop(insert);
            let _ = db.execute_batch("ROLLBACK");
            return WriterOutcome::Failed(error);
        }
        if id % 128 == 0 {
            assert_cache_bound(&cache, "concurrent writer");
        }
    }
    drop(insert);
    match db.execute_batch("COMMIT") {
        Ok(()) => WriterOutcome::Committed,
        Err(error) => {
            let _ = db.execute_batch("ROLLBACK");
            WriterOutcome::Failed(error)
        }
    }
}

fn scan_under_pressure(
    vfs: &'static BlockCacheVfs,
    path: String,
    cache: PathBuf,
    stop: Arc<AtomicBool>,
) -> Result<usize, String> {
    let db = vfs
        .open(path)
        .map_err(|error| format!("open reader: {error:?}"))?;
    db.execute_batch("PRAGMA cache_size = 8; BEGIN;")
        .map_err(|error| format!("begin reader snapshot: {error:?}"))?;

    let mut scans = 0;
    for _ in 0..24 {
        let (count, total): (i64, i64) = db
            .query_row(
                "SELECT count(*), coalesce(sum(length(body)), 0) FROM payload",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|error| format!("full reader scan: {error:?}"))?;
        if count != SCAN_ROWS || total != SCAN_ROWS * BODY_BYTES as i64 {
            let _ = db.execute_batch("ROLLBACK");
            return Err(format!(
                "reader observed count={count}, total={total}, expected count={SCAN_ROWS}, total={}",
                SCAN_ROWS * BODY_BYTES as i64
            ));
        }
        scans += 1;
        assert_cache_bound(&cache, "repeated full scan");
        thread::yield_now();
    }
    db.execute_batch("COMMIT")
        .map_err(|error| format!("commit reader snapshot: {error:?}"))?;
    stop.store(true, Ordering::Release);
    Ok(scans)
}

fn is_controlled_pressure_error(error: &Error) -> bool {
    matches!(
        error,
        Error::SqliteFailure(code, _)
            if matches!(
                code.code,
                ErrorCode::SystemIoFailure
                    | ErrorCode::DiskFull
                    | ErrorCode::DatabaseBusy
                    | ErrorCode::DatabaseLocked
            )
    )
}

struct RejectingProxy {
    stop: Arc<AtomicBool>,
    put_requests: Arc<AtomicUsize>,
    rejected_puts: Arc<AtomicUsize>,
    thread: Option<thread::JoinHandle<()>>,
    url: String,
}

impl RejectingProxy {
    fn start(upstream: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind rejecting proxy");
        listener
            .set_nonblocking(true)
            .expect("set rejecting proxy nonblocking");
        let address = listener.local_addr().expect("rejecting proxy address");
        let upstream = upstream
            .trim_end_matches('/')
            .strip_prefix("http://")
            .or_else(|| upstream.trim_end_matches('/').strip_prefix("https://"))
            .and_then(|value| value.split('/').next())
            .expect("S3 emulator endpoint must have an HTTP authority")
            .to_owned();
        let stop = Arc::new(AtomicBool::new(false));
        let put_requests = Arc::new(AtomicUsize::new(0));
        let rejected_puts = Arc::new(AtomicUsize::new(0));
        let thread_stop = Arc::clone(&stop);
        let thread_puts = Arc::clone(&put_requests);
        let thread_rejected = Arc::clone(&rejected_puts);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                let Ok((mut client, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(2));
                    continue;
                };
                let _ = client.set_read_timeout(Some(Duration::from_secs(10)));
                let _ = client.set_write_timeout(Some(Duration::from_secs(10)));
                let _ =
                    handle_proxy_request(&mut client, &upstream, &thread_puts, &thread_rejected);
            }
        });
        Self {
            stop,
            put_requests,
            rejected_puts,
            thread: Some(thread),
            url: format!("http://{address}"),
        }
    }

    fn put_requests(&self) -> usize {
        self.put_requests.load(Ordering::Acquire)
    }

    fn rejected_puts(&self) -> usize {
        self.rejected_puts.load(Ordering::Acquire)
    }
}

impl Drop for RejectingProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join rejecting proxy");
        }
    }
}

struct HoldingProxy {
    stop: Arc<AtomicBool>,
    release: Arc<AtomicBool>,
    block_gets: Arc<AtomicUsize>,
    handlers: Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
    thread: Option<thread::JoinHandle<()>>,
    url: String,
}

impl HoldingProxy {
    fn start(upstream: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind holding proxy");
        listener
            .set_nonblocking(true)
            .expect("set holding proxy nonblocking");
        let address = listener.local_addr().expect("holding proxy address");
        let upstream = upstream
            .trim_end_matches('/')
            .strip_prefix("http://")
            .or_else(|| upstream.trim_end_matches('/').strip_prefix("https://"))
            .and_then(|value| value.split('/').next())
            .expect("S3 emulator endpoint must have an HTTP authority")
            .to_owned();
        let stop = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let block_gets = Arc::new(AtomicUsize::new(0));
        let handlers = Arc::new(Mutex::new(Vec::new()));
        let thread_stop = Arc::clone(&stop);
        let thread_release = Arc::clone(&release);
        let thread_gets = Arc::clone(&block_gets);
        let thread_handlers = Arc::clone(&handlers);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                let (client, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                let handler_upstream = upstream.clone();
                let handler_release = Arc::clone(&thread_release);
                let handler_gets = Arc::clone(&thread_gets);
                let handler = thread::spawn(move || {
                    let mut client = client;
                    let _ = client.set_read_timeout(Some(Duration::from_secs(10)));
                    let _ = client.set_write_timeout(Some(Duration::from_secs(10)));
                    let _ = handle_holding_proxy_request(
                        &mut client,
                        &handler_upstream,
                        &handler_release,
                        &handler_gets,
                    );
                });
                thread_handlers
                    .lock()
                    .expect("lock holding-proxy handlers")
                    .push(handler);
            }
        });
        Self {
            stop,
            release,
            block_gets,
            handlers,
            thread: Some(thread),
            url: format!("http://{address}"),
        }
    }

    fn block_gets(&self) -> usize {
        self.block_gets.load(Ordering::Acquire)
    }

    fn release(&self) {
        self.release.store(true, Ordering::Release);
    }
}

impl Drop for HoldingProxy {
    fn drop(&mut self) {
        self.release();
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join holding proxy");
        }
        let handlers = std::mem::take(
            &mut *self
                .handlers
                .lock()
                .expect("lock holding-proxy handlers for join"),
        );
        for handler in handlers {
            handler.join().expect("join holding-proxy handler");
        }
    }
}

fn read_http_request(stream: &mut TcpStream) -> std::io::Result<(String, String, Vec<u8>)> {
    let mut request = Vec::new();
    let header_end = loop {
        let mut buffer = [0_u8; 16 * 1024];
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed before proxy request headers",
            ));
        }
        request.extend_from_slice(&buffer[..count]);
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

    let header_text = String::from_utf8_lossy(&request[..header_end]);
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "proxy request has no request line",
        )
    })?;
    let mut fields = request_line.split_whitespace();
    let method = fields
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing method"))?
        .to_owned();
    let target = fields
        .next()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "missing target"))?
        .to_owned();
    let mut content_length = 0_usize;
    let mut expect_continue = false;
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "invalid proxy content length",
                    )
                })?;
            }
            if name.eq_ignore_ascii_case("expect")
                && value.trim().eq_ignore_ascii_case("100-continue")
            {
                expect_continue = true;
            }
        }
    }

    let mut body = request[header_end..].to_vec();
    if expect_continue {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
    }
    while body.len() < content_length {
        let mut buffer = [0_u8; 16 * 1024];
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "client closed before proxy request body",
            ));
        }
        body.extend_from_slice(&buffer[..count]);
    }
    body.truncate(content_length);
    Ok((
        method,
        target,
        [request[..header_end].to_vec(), body].concat(),
    ))
}

fn origin_target(target: &str) -> &str {
    if let Some(rest) = target.strip_prefix("http://") {
        rest.find('/').map_or("/", |index| &rest[index..])
    } else if let Some(rest) = target.strip_prefix("https://") {
        rest.find('/').map_or("/", |index| &rest[index..])
    } else {
        target
    }
}

fn handle_proxy_request(
    client: &mut TcpStream,
    upstream_address: &str,
    put_requests: &AtomicUsize,
    rejected_puts: &AtomicUsize,
) -> std::io::Result<()> {
    let (method, target, request) = read_http_request(client)?;
    if method.eq_ignore_ascii_case("PUT") {
        put_requests.fetch_add(1, Ordering::Release);
        rejected_puts.fetch_add(1, Ordering::Release);
        client.write_all(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        return Ok(());
    }

    forward_http_request(client, upstream_address, &method, &target, &request)
}

fn handle_holding_proxy_request(
    client: &mut TcpStream,
    upstream_address: &str,
    release: &AtomicBool,
    block_gets: &AtomicUsize,
) -> std::io::Result<()> {
    let (method, target, request) = read_http_request(client)?;
    let path = origin_target(&target)
        .split_once('?')
        .map_or(origin_target(&target), |(path, _)| path);
    if method.eq_ignore_ascii_case("GET") && !path.ends_with("/manifest.bcv") {
        let request_number = block_gets.fetch_add(1, Ordering::AcqRel);
        if request_number < 2 {
            while !release.load(Ordering::Acquire) {
                thread::sleep(Duration::from_millis(2));
            }
        }
    }
    forward_http_request(client, upstream_address, &method, &target, &request)
}

fn forward_http_request(
    client: &mut TcpStream,
    upstream_address: &str,
    method: &str,
    target: &str,
    request: &[u8],
) -> std::io::Result<()> {
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("proxy request has headers")
        + 4;
    let body = &request[header_end..];
    let header_text = String::from_utf8_lossy(&request[..header_end]);
    let mut lines = header_text.split("\r\n");
    let _request_line = lines.next().expect("proxy request line");
    let mut upstream = TcpStream::connect(upstream_address)?;
    upstream.set_read_timeout(Some(Duration::from_secs(10)))?;
    upstream.set_write_timeout(Some(Duration::from_secs(10)))?;
    write!(
        upstream,
        "{} {} HTTP/1.1\r\n",
        method,
        origin_target(target)
    )?;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("connection")
            || name.eq_ignore_ascii_case("proxy-connection")
            || name.eq_ignore_ascii_case("expect")
        {
            continue;
        }
        write!(upstream, "{name}: {}\r\n", value.trim())?;
    }
    upstream.write_all(b"Connection: close\r\n\r\n")?;
    upstream.write_all(body)?;
    std::io::copy(&mut upstream, client)?;
    Ok(())
}

fn run_pressure(vfs: &'static BlockCacheVfs, endpoint: &str, cache: &Path) {
    let (storage, _bucket, container) = setup_two_databases(vfs, endpoint, cache);
    let alias = format!("pressure_{}", std::process::id());
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&alias))
        .expect("attach pressure-test container");
    let before = fetch_manifest(endpoint, &container);
    let objects_before = list_remote_objects(endpoint, &container);
    let stop = Arc::new(AtomicBool::new(false));

    let writer_path = format!("/{alias}/first.sqlite");
    let reader_path = format!("/{alias}/second.sqlite");
    let writer_cache = cache.to_owned();
    let reader_cache = cache.to_owned();
    let writer_vfs = vfs;
    let reader_vfs = vfs;
    let reader_stop = Arc::clone(&stop);
    let writer = thread::spawn(move || write_under_pressure(writer_vfs, writer_path, writer_cache));
    let reader = thread::spawn(move || {
        scan_under_pressure(reader_vfs, reader_path, reader_cache, reader_stop)
    });

    let reader_result = reader.join().expect("reader thread must not panic");
    let writer_result = writer.join().expect("writer thread must not panic");
    assert!(
        reader_result.is_ok(),
        "reader failed under cache pressure: {reader_result:?}"
    );
    assert_cache_bound(cache, "after concurrent reader and writer");

    match &writer_result {
        WriterOutcome::Committed => {}
        WriterOutcome::Failed(error) => assert!(
            is_controlled_pressure_error(error),
            "writer returned an uncontrolled error under cache pressure: {error:?}"
        ),
    }

    let second = vfs
        .open(format!("/{alias}/second.sqlite"))
        .expect("reopen second database for repeated full scan");
    for _ in 0..8 {
        let (count, total): (i64, i64) = second
            .query_row(
                "SELECT count(*), coalesce(sum(length(body)), 0) FROM payload",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .expect("repeated full scan after pressure");
        assert_eq!(count, SCAN_ROWS);
        assert_eq!(total, SCAN_ROWS * BODY_BYTES as i64);
        assert_cache_bound(cache, "post-pressure repeated scan");
    }
    drop(second);

    if matches!(writer_result, WriterOutcome::Committed) {
        vfs.upload(&alias).expect("publish pressure-test update");
        assert_ne!(before, fetch_manifest(endpoint, &container));
        assert!(
            list_remote_objects(endpoint, &container) >= objects_before,
            "remote object listing regressed after pressure update"
        );
    }
    vfs.detach(&alias)
        .expect("detach pressure-test container after pressure update");
    assert_cache_bound(cache, "after pressure-test detach");
}

fn read_seed_database(vfs: &'static BlockCacheVfs, path: String) -> Result<(), Error> {
    let db = vfs.open(path)?;
    let count: i64 = db.query_row("SELECT count(*) FROM payload", [], |row| row.get(0))?;
    assert_eq!(count, SEED_ROWS);
    Ok(())
}

fn run_pinned_exhaustion(vfs: &'static BlockCacheVfs, endpoint: &str, cache: &Path) {
    let (_storage, container) = setup_three_databases(vfs, endpoint, cache);
    let proxy = HoldingProxy::start(endpoint);
    let proxied = Storage::s3_with_endpoint("test", &container, "us-east-1", &proxy.url);
    let alias = format!("pressure_pinned_{}", std::process::id());
    vfs.attach(&AttachSpec::new(proxied).alias(&alias))
        .expect("attach pinned-pressure container through holding proxy");

    let first_path = format!("/{alias}/first.sqlite");
    let second_path = format!("/{alias}/second.sqlite");
    let first = thread::spawn(move || read_seed_database(vfs, first_path));
    let second = thread::spawn(move || read_seed_database(vfs, second_path));
    let deadline = Instant::now() + Duration::from_secs(10);
    while proxy.block_gets() < 2 && Instant::now() < deadline {
        assert_cache_bound(cache, "all-slots-pinned wait");
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(
        proxy.block_gets(),
        2,
        "two distinct block reads did not become simultaneously held"
    );
    assert_cache_bound(cache, "all slots pinned");

    // The third database is not resident. Both configured slots are held by
    // the two blocked remote reads, so opening/reading it must return a
    // bounded, controlled error rather than allocating a third slot.
    let third_path = format!("/{alias}/third.sqlite");
    let third = vfs.open(third_path).and_then(|db| {
        db.query_row("SELECT count(*) FROM payload", [], |row| {
            row.get::<_, i64>(0)
        })
        .map(|_| ())
    });
    let error = third.expect_err("all pinned cache slots must reject a third block");
    assert!(
        is_controlled_pressure_error(&error),
        "all-slots-pinned exhaustion returned an uncontrolled error: {error:?}"
    );
    assert_cache_bound(cache, "all-slots-pinned rejection");

    proxy.release();
    assert!(
        first
            .join()
            .expect("first pinned reader must not panic")
            .is_ok(),
        "first pinned reader failed after releasing held block"
    );
    assert!(
        second
            .join()
            .expect("second pinned reader must not panic")
            .is_ok(),
        "second pinned reader failed after releasing held block"
    );
    vfs.detach(&alias)
        .expect("detach all-slots-pinned pressure container");
    assert_cache_bound(cache, "after all-slots-pinned test");
}

fn run_failed_put(vfs: &'static BlockCacheVfs, endpoint: &str, cache: &Path) {
    let suffix = unique_suffix();
    let bucket = format!("rust-streaming-pressure-failure-{suffix}");
    let container = format!("{bucket}/{suffix}/cbs");
    let direct = direct_storage(endpoint, &container);
    vfs.initialize_container(&direct)
        .expect("initialize failed-put container");
    let local_dir = tempfile::tempdir().expect("failed-put seed directory");
    let local_path = local_dir.path().join("seed.sqlite");
    create_seed(&local_path, SEED_ROWS);
    vfs.create_database(&direct, &local_path, "first.sqlite")
        .expect("upload failed-put seed database");
    let before = fetch_manifest(endpoint, &container);

    let proxy = RejectingProxy::start(endpoint);
    let proxied = Storage::s3_with_endpoint("test", &container, "us-east-1", &proxy.url);
    let alias = format!("pressure_failure_{}", std::process::id());
    vfs.attach(&AttachSpec::new(proxied).alias(&alias))
        .expect("attach failed-put container through proxy");
    let db = vfs
        .open(format!("/{alias}/first.sqlite"))
        .expect("open failed-put database");
    db.execute_batch("PRAGMA cache_size = 8; BEGIN IMMEDIATE;")
        .expect("begin failed-put update");
    let mut insert = db
        .prepare("INSERT INTO payload(id, body) VALUES (?1, ?2)")
        .expect("prepare failed-put insert");
    let mut failure = None;
    for id in SEED_ROWS..(SEED_ROWS + WRITE_ROWS) {
        match insert.execute(params![id, row_body(id)]) {
            Ok(_) => {
                if id % 128 == 0 {
                    assert_cache_bound(cache, "rejected block PUT");
                }
            }
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    drop(insert);
    if failure.is_none() {
        if let Err(error) = db.execute_batch("COMMIT") {
            failure = Some(error);
        }
    } else {
        let _ = db.execute_batch("ROLLBACK");
    }
    if failure.is_none() {
        failure = vfs.upload(&alias).err();
    }
    assert!(
        proxy.put_requests() > 0,
        "pressure test did not issue a block PUT"
    );
    assert!(
        proxy.rejected_puts() > 0,
        "pressure test did not observe a rejected block PUT"
    );
    let error = failure.expect("a rejected PUT must surface as a controlled error");
    assert!(
        is_controlled_pressure_error(&error),
        "rejected PUT surfaced an unexpected error: {error:?}"
    );
    assert_eq!(
        before,
        fetch_manifest(endpoint, &container),
        "failed staging must not publish a manifest"
    );
    assert_cache_bound(cache, "after rejected block PUT");
    drop(db);
    let _ = vfs.detach(&alias);
}

#[test]
#[ignore = "requires the pinned local S3 emulator container"]
fn bounded_cache_handles_shared_database_pressure_and_failed_puts() {
    let endpoint = std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4566".into());
    let cache = tempfile::tempdir().expect("pressure-test cache directory");
    let vfs = new_vfs(cache.path());
    run_pinned_exhaustion(vfs, &endpoint, cache.path());
    run_pressure(vfs, &endpoint, cache.path());
    run_failed_put(vfs, &endpoint, cache.path());
}

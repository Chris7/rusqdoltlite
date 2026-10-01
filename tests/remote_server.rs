#![cfg(all(feature = "remote", not(target_arch = "wasm32")))]

use std::{
    ffi::{c_char, c_int, c_void, CString},
    io::{Read as _, Write as _},
    net::{TcpListener, TcpStream},
    process::Command,
    ptr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, Mutex,
    },
    thread,
    time::Duration,
};

use rusqlite::{
    ffi, params, Connection, Error, RemoteAuthenticator, RemoteServer, RemoteServerOptions, Result,
};

#[cfg(feature = "blockcachevfs")]
use rusqlite::blockcachevfs::SessionOperationId;
#[cfg(feature = "blockcachevfs")]
use rusqlite::{BlockCacheSessionOptions, OpenFlags, SessionOperationStatus, SessionScope};
#[cfg(feature = "blockcachevfs")]
use std::time::Instant;

unsafe extern "C" {
    #[link_name = "doltliteHttpRemoteOpen"]
    fn doltlite_http_remote_open_for_test(url: *const c_char) -> *mut c_void;
}

struct CountingVfsState {
    underlying: *mut ffi::sqlite3_vfs,
    accesses: AtomicUsize,
    opens: AtomicUsize,
}

struct CountingVfs {
    raw: Box<ffi::sqlite3_vfs>,
    state: Box<CountingVfsState>,
    name: CString,
}

unsafe extern "C" fn counting_vfs_access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    result: *mut c_int,
) -> c_int {
    let state = &*((*vfs).pAppData.cast::<CountingVfsState>());
    state.accesses.fetch_add(1, Ordering::Relaxed);
    ((*state.underlying)
        .xAccess
        .expect("default VFS has xAccess"))(state.underlying, name, flags, result)
}

unsafe extern "C" fn counting_vfs_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    output_flags: *mut c_int,
) -> c_int {
    let state = &*((*vfs).pAppData.cast::<CountingVfsState>());
    state.opens.fetch_add(1, Ordering::Relaxed);
    ((*state.underlying).xOpen.expect("default VFS has xOpen"))(
        state.underlying,
        name,
        file,
        flags,
        output_flags,
    )
}

impl CountingVfs {
    fn new() -> Self {
        let underlying = unsafe { ffi::sqlite3_vfs_find(ptr::null()) };
        assert!(!underlying.is_null(), "DoltLite must provide a default VFS");
        let name = CString::new("rusqdoltlite-counting-vfs").expect("static VFS name");
        let state = Box::new(CountingVfsState {
            underlying,
            accesses: AtomicUsize::new(0),
            opens: AtomicUsize::new(0),
        });
        let mut raw = Box::new(unsafe { *underlying });
        raw.zName = name.as_ptr();
        raw.pAppData = (&*state as *const CountingVfsState)
            .cast_mut()
            .cast::<c_void>();
        raw.pNext = ptr::null_mut();
        raw.xAccess = Some(counting_vfs_access);
        raw.xOpen = Some(counting_vfs_open);
        let rc = unsafe { ffi::sqlite3_vfs_register(raw.as_mut(), 0) };
        assert_eq!(rc, ffi::SQLITE_OK, "register counting VFS");
        Self { raw, state, name }
    }

    fn name(&self) -> &str {
        self.name.to_str().expect("static VFS name")
    }

    fn accesses(&self) -> usize {
        self.state.accesses.load(Ordering::Relaxed)
    }

    fn opens(&self) -> usize {
        self.state.opens.load(Ordering::Relaxed)
    }
}

impl Drop for CountingVfs {
    fn drop(&mut self) {
        let rc = unsafe { ffi::sqlite3_vfs_unregister(self.raw.as_mut()) };
        assert_eq!(rc, ffi::SQLITE_OK, "unregister counting VFS");
    }
}

fn send_http_request(port: u16, request: &[u8]) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to remote server");
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .expect("read timeout");
    stream.write_all(request).expect("write request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    response
}

fn post_chunk_batch(port: u16, body: &[u8]) -> String {
    let header = format!(
        "POST /large.db/chunks HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to remote server");
    stream
        .set_read_timeout(Some(Duration::from_secs(15)))
        .expect("read timeout");
    stream.write_all(header.as_bytes()).expect("write headers");
    stream.write_all(body).expect("write chunk batch");
    let mut response = Vec::new();
    loop {
        let mut buffer = [0_u8; 1024];
        let count = stream.read(&mut buffer).expect("read response headers");
        if count == 0 {
            break;
        }
        response.extend_from_slice(&buffer[..count]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(response).expect("HTTP response headers are UTF-8")
}

#[cfg(feature = "blockcachevfs")]
struct SlowGcsProxy {
    address: String,
    stopped: Arc<AtomicUsize>,
    client_returned: Arc<AtomicUsize>,
    bytes_after_client_return: Arc<AtomicUsize>,
    media_uploads: Arc<AtomicUsize>,
    completed_media_uploads: Arc<AtomicUsize>,
    slow_next_media_upload: Arc<AtomicUsize>,
    failure: Arc<Mutex<Option<String>>>,
    thread: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "blockcachevfs")]
impl SlowGcsProxy {
    fn start(emulator_endpoint: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind slow GCS proxy");
        listener
            .set_nonblocking(true)
            .expect("make slow GCS proxy stoppable");
        let address = listener.local_addr().expect("slow GCS proxy address");
        let authority = emulator_endpoint
            .trim_end_matches('/')
            .strip_prefix("http://")
            .or_else(|| {
                emulator_endpoint
                    .trim_end_matches('/')
                    .strip_prefix("https://")
            })
            .and_then(|endpoint| endpoint.split('/').next())
            .expect("GCS emulator endpoint has an HTTP authority")
            .to_owned();
        let stopped = Arc::new(AtomicUsize::new(0));
        let client_returned = Arc::new(AtomicUsize::new(0));
        let bytes_after_client_return = Arc::new(AtomicUsize::new(0));
        let media_uploads = Arc::new(AtomicUsize::new(0));
        let completed_media_uploads = Arc::new(AtomicUsize::new(0));
        let slow_next_media_upload = Arc::new(AtomicUsize::new(0));
        let failure = Arc::new(Mutex::new(None));

        let thread_stopped = Arc::clone(&stopped);
        let thread_client_returned = Arc::clone(&client_returned);
        let thread_bytes_after_return = Arc::clone(&bytes_after_client_return);
        let thread_media_uploads = Arc::clone(&media_uploads);
        let thread_completed_uploads = Arc::clone(&completed_media_uploads);
        let thread_slow_next = Arc::clone(&slow_next_media_upload);
        let thread_failure = Arc::clone(&failure);
        let join = thread::spawn(move || {
            while thread_stopped.load(Ordering::Acquire) == 0 {
                let (mut client, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
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
                if thread_stopped.load(Ordering::Acquire) != 0 {
                    break;
                }
                let result = relay_gcs_request(
                    &mut client,
                    &authority,
                    &thread_client_returned,
                    &thread_bytes_after_return,
                    &thread_media_uploads,
                    &thread_completed_uploads,
                    &thread_slow_next,
                );
                if let Err(error) = result {
                    if let Ok(mut failure) = thread_failure.lock() {
                        if failure.is_none() {
                            *failure = Some(format!("GCS proxy relay failed: {error}"));
                        }
                    }
                }
            }
        });

        Self {
            address: format!("http://{address}"),
            stopped,
            client_returned,
            bytes_after_client_return,
            media_uploads,
            completed_media_uploads,
            slow_next_media_upload,
            failure,
            thread: Some(join),
        }
    }

    fn arm_slow_media_upload(&self) {
        self.slow_next_media_upload.store(1, Ordering::Release);
    }

    fn mark_client_returned(&self) {
        self.client_returned.store(1, Ordering::Release);
    }

    fn bytes_after_client_return(&self) -> usize {
        self.bytes_after_client_return.load(Ordering::Acquire)
    }

    fn media_uploads(&self) -> usize {
        self.media_uploads.load(Ordering::Acquire)
    }

    fn completed_media_uploads(&self) -> usize {
        self.completed_media_uploads.load(Ordering::Acquire)
    }

    fn failure(&self) -> Option<String> {
        self.failure
            .lock()
            .expect("lock slow GCS proxy failure")
            .clone()
    }
}

#[cfg(feature = "blockcachevfs")]
impl Drop for SlowGcsProxy {
    fn drop(&mut self) {
        self.stopped.store(1, Ordering::Release);
        let _ = TcpStream::connect(self.address.trim_start_matches("http://"));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(feature = "blockcachevfs")]
fn relay_gcs_request(
    client: &mut TcpStream,
    authority: &str,
    client_returned: &AtomicUsize,
    bytes_after_client_return: &AtomicUsize,
    media_uploads: &AtomicUsize,
    completed_media_uploads: &AtomicUsize,
    slow_next_media_upload: &AtomicUsize,
) -> std::io::Result<()> {
    client.set_read_timeout(Some(Duration::from_secs(30)))?;
    client.set_write_timeout(Some(Duration::from_secs(30)))?;
    let mut request = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 8192];
        let count = client.read(&mut chunk)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "storage client closed before request headers",
            ));
        }
        request.extend_from_slice(&chunk[..count]);
        if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        if request.len() > 128 * 1024 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "storage request headers exceed test proxy limit",
            ));
        }
    };
    let header_text = std::str::from_utf8(&request[..header_end])
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()))?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing request line")
    })?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing request method")
    })?;
    let target = parts.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing request target")
    })?;
    let version = parts.next().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "missing HTTP version")
    })?;
    if version != "HTTP/1.1" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "test proxy only supports HTTP/1.1",
        ));
    }
    let mut content_length = 0_usize;
    let mut expect_continue = false;
    let mut transfer_encoded = false;
    let mut forwarded_headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed HTTP header")
        })?;
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid content length")
            })?;
        } else if name.eq_ignore_ascii_case("expect")
            && value.trim().eq_ignore_ascii_case("100-continue")
        {
            expect_continue = true;
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            transfer_encoded = true;
        }
        if !name.eq_ignore_ascii_case("host")
            && !name.eq_ignore_ascii_case("connection")
            && !name.eq_ignore_ascii_case("proxy-connection")
            && !name.eq_ignore_ascii_case("expect")
        {
            forwarded_headers.push((name.to_owned(), value.trim().to_owned()));
        }
    }
    if transfer_encoded {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "chunked GCS storage requests are unsupported by the test proxy",
        ));
    }
    let origin_target = if let Some(rest) = target.strip_prefix("http://") {
        rest.find('/').map_or("/", |index| &rest[index..])
    } else if let Some(rest) = target.strip_prefix("https://") {
        rest.find('/').map_or("/", |index| &rest[index..])
    } else {
        target
    };
    let mut upstream = TcpStream::connect(authority)?;
    upstream.set_read_timeout(Some(Duration::from_secs(30)))?;
    upstream.set_write_timeout(Some(Duration::from_secs(30)))?;
    write!(upstream, "{method} {origin_target} HTTP/1.1\r\n")?;
    for (name, value) in forwarded_headers {
        write!(upstream, "{name}: {value}\r\n")?;
    }
    write!(upstream, "Host: {authority}\r\nConnection: close\r\n\r\n")?;
    if expect_continue {
        client.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
    }

    let media_upload = target.contains("uploadType=media");
    let slow_upload = media_upload && slow_next_media_upload.swap(0, Ordering::AcqRel) != 0;
    let mut slow_chunks_left = if slow_upload { 12 } else { 0 };
    if media_upload {
        media_uploads.fetch_add(1, Ordering::AcqRel);
    }
    let initial_body = &request[header_end..];
    let initial_body = &initial_body[..initial_body.len().min(content_length)];
    relay_gcs_body_chunk(
        &mut upstream,
        initial_body,
        media_upload,
        &mut slow_chunks_left,
        client_returned,
        bytes_after_client_return,
    )?;
    let mut body_read = initial_body.len();
    while body_read < content_length {
        let mut chunk = [0_u8; 1024];
        let count = client.read(&mut chunk)?;
        if count == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "storage client closed before request body completed",
            ));
        }
        let count = count.min(content_length - body_read);
        relay_gcs_body_chunk(
            &mut upstream,
            &chunk[..count],
            media_upload,
            &mut slow_chunks_left,
            client_returned,
            bytes_after_client_return,
        )?;
        body_read += count;
    }
    upstream.shutdown(std::net::Shutdown::Write)?;
    let mut response = Vec::new();
    upstream.read_to_end(&mut response)?;
    if !response.is_empty() {
        let _ = client.write_all(&response);
    }
    if media_upload {
        completed_media_uploads.fetch_add(1, Ordering::AcqRel);
    }
    Ok(())
}

#[cfg(feature = "blockcachevfs")]
fn relay_gcs_body_chunk(
    upstream: &mut TcpStream,
    bytes: &[u8],
    media_upload: bool,
    slow_chunks_left: &mut usize,
    client_returned: &AtomicUsize,
    bytes_after_client_return: &AtomicUsize,
) -> std::io::Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    upstream.write_all(bytes)?;
    if media_upload && client_returned.load(Ordering::Acquire) != 0 {
        bytes_after_client_return.fetch_add(bytes.len(), Ordering::AcqRel);
    }
    if media_upload && *slow_chunks_left > 0 {
        *slow_chunks_left -= 1;
        thread::sleep(Duration::from_millis(30));
    }
    Ok(())
}

#[cfg(feature = "blockcachevfs")]
fn encode_gcs_component(value: &str) -> String {
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

#[cfg(feature = "blockcachevfs")]
fn fetch_gcs_object(endpoint: &str, bucket: &str, object: &str) -> Vec<u8> {
    let body = tempfile::NamedTempFile::new().expect("GCS manifest temporary file");
    let url = format!(
        "{}/download/storage/v1/b/{}/o/{}?alt=media",
        endpoint.trim_end_matches('/'),
        encode_gcs_component(bucket),
        encode_gcs_component(object)
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
                .expect("GCS temporary file path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for GCS emulator tests");
    assert!(response.status.success(), "GCS object request failed");
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "GCS object request returned {code}");
    std::fs::read(body.path()).expect("read GCS object body")
}

#[test]
fn in_process_remote_server_supports_push_and_clone() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");

    let server = RemoteServer::start(&server_root)?;
    assert!(server.port() > 0);
    let remote_url = server.database_url("origin.db");

    let source = Connection::open(temp.path().join("source.db"))?;
    source.execute_batch(
        "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO widgets VALUES(1, 'remote value');",
    )?;
    let _: i64 = source.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String = source.query_row("SELECT dolt_commit('-m', 'seed')", [], |row| row.get(0))?;
    let _: i64 = source.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![remote_url],
        |row| row.get(0),
    )?;
    let _: i64 = source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;

    let clone = Connection::open(temp.path().join("clone.db"))?;
    let _: i64 = clone.query_row(
        "SELECT dolt_clone(?1)",
        params![server.database_url("origin.db")],
        |row| row.get(0),
    )?;
    let value: String = clone.query_row("SELECT name FROM widgets WHERE id = 1", [], |row| {
        row.get(0)
    })?;
    let branch_hash: String =
        clone.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    let tracking_hash: String =
        clone.query_row("SELECT dolt_hashof('origin/main')", [], |row| row.get(0))?;

    assert_eq!(value, "remote value");
    assert_eq!(tracking_hash, branch_hash);
    assert!(server_root.join("origin.db").exists());

    server.close()?;

    Ok(())
}

#[test]
fn fresh_clone_splits_aggregate_get_chunks_413_and_recovers_partial_failure() -> Result<()> {
    const ROWS: i64 = 1_024;
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");
    let server = RemoteServer::start(&server_root)?;
    let source = Connection::open(temp.path().join("source.db"))?;
    source.execute_batch(
        "CREATE TABLE batch_values(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         BEGIN;",
    )?;
    {
        let mut insert = source.prepare("INSERT INTO batch_values VALUES(?1, ?2)")?;
        for id in 1..=ROWS {
            insert.execute(params![id, chunk_batch_test_value(id)])?;
        }
    }
    source.execute_batch("COMMIT")?;
    let _: String = source.query_row(
        "SELECT dolt_commit('-A', '-m', 'aggregate chunk fixture')",
        [],
        |row| row.get(0),
    )?;
    let source_hash: String =
        source.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    let _: i64 = source.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![server.database_url("origin.db")],
        |row| row.get(0),
    )?;
    let _: i64 = source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;

    let probe = GetChunksProxy::start(server.port(), None, GetChunksProxyFailure::None);
    let probe_clone = Connection::open(temp.path().join("probe-clone.db"))?;
    let _: i64 = probe_clone.query_row(
        "SELECT dolt_clone(?1)",
        params![probe.database_url("origin.db")],
        |row| row.get(0),
    )?;
    let probe_stats = probe.stats();
    assert!(
        probe_stats.proxy_errors.is_empty(),
        "proxy errors: {probe_stats:?}"
    );
    assert!(
        probe_stats.max_hashes_per_request > 1,
        "no multi-hash reply: {probe_stats:?}"
    );
    assert!(
        probe_stats.max_multi_response_bytes > probe_stats.max_single_record_bytes,
        "fixture must produce a batch larger than its largest individual chunk: {probe_stats:?}"
    );
    assert!(probe_stats.max_single_record_bytes > 4);
    drop(probe_clone);
    drop(probe);

    let threshold_only = GetChunksProxy::start(
        server.port(),
        Some(probe_stats.max_single_record_bytes),
        GetChunksProxyFailure::None,
    );
    let first_attempt_clone = Connection::open(temp.path().join("first-attempt-clone.db"))?;
    let _: i64 = first_attempt_clone.query_row(
        "SELECT dolt_clone(?1)",
        params![threshold_only.database_url("origin.db")],
        |row| row.get(0),
    )?;
    assert_batch_clone_contents(&first_attempt_clone, &source_hash, ROWS)?;
    let threshold_stats = threshold_only.stats();
    assert!(
        threshold_stats.aggregate_413_responses > 0,
        "the threshold-only proxy should reject aggregate replies: {threshold_stats:?}"
    );
    assert!(
        threshold_stats.successful_split_singleton_responses > 0,
        "the client should split aggregate responses into successful smaller requests: {threshold_stats:?}"
    );
    drop(first_attempt_clone);
    drop(threshold_only);

    let limited = GetChunksProxy::start(
        server.port(),
        Some(probe_stats.max_single_record_bytes),
        GetChunksProxyFailure::FailAfterFirstSplitSingletonOnce,
    );
    let clone = Connection::open(temp.path().join("adaptive-clone.db"))?;
    let remote_url = limited.database_url("origin.db");
    let first_attempt =
        clone.query_row::<i64, _, _>("SELECT dolt_clone(?1)", params![remote_url], |row| {
            row.get(0)
        });
    let partial_stats = limited.stats();
    let first_error = match first_attempt {
        Ok(value) => panic!(
            "should fail the first clone attempt after a split chunk was received: {partial_stats:?}; result={value}"
        ),
        Err(error) => error,
    };
    assert!(
        matches!(first_error, Error::SqliteFailure(code, _) if code.extended_code == ffi::SQLITE_TOOBIG),
        "should preserve a terminal chunk 413 as SQLITE_TOOBIG: {first_error:?}"
    );
    assert!(
        partial_stats.proxy_errors.is_empty(),
        "proxy errors: {partial_stats:?}"
    );
    assert!(
        partial_stats.aggregate_413_responses > 0,
        "the proxy must reject an aggregate get-chunks response: {partial_stats:?}"
    );
    assert_eq!(partial_stats.injected_partial_413_responses, 1);
    assert!(
        partial_stats.successful_split_singleton_responses > 0,
        "at least one split singleton must complete before the injected failure: {partial_stats:?}"
    );
    assert_eq!(partial_stats.singleton_413_responses, 1);
    let usable: i64 = clone.query_row("SELECT 1", [], |row| row.get(0))?;
    assert_eq!(
        usable, 1,
        "failed clone must leave its SQLite handle usable"
    );

    clone.query_row::<i64, _, _>("SELECT dolt_clone(?1)", params![remote_url], |row| {
        row.get(0)
    })?;
    assert_batch_clone_contents(&clone, &source_hash, ROWS)?;
    let completed_stats = limited.stats();
    assert!(
        completed_stats.proxy_errors.is_empty(),
        "proxy errors: {completed_stats:?}"
    );
    assert!(completed_stats.aggregate_413_responses > partial_stats.aggregate_413_responses);
    assert!(
        completed_stats.successful_singleton_responses
            > partial_stats.successful_singleton_responses
    );
    drop(limited);

    let declared = GetChunksProxy::start(
        server.port(),
        None,
        GetChunksProxyFailure::OversizedContentLengthOnce,
    );
    let declared_clone = Connection::open(temp.path().join("declared-length-clone.db"))?;
    let _: i64 = declared_clone.query_row(
        "SELECT dolt_clone(?1)",
        params![declared.database_url("origin.db")],
        |row| row.get(0),
    )?;
    assert_batch_clone_contents(&declared_clone, &source_hash, ROWS)?;
    let declared_stats = declared.stats();
    assert!(
        declared_stats.proxy_errors.is_empty(),
        "proxy errors: {declared_stats:?}"
    );
    assert_eq!(declared_stats.declared_oversized_responses, 1);
    assert!(
        declared_stats.smaller_responses_after_declared_oversize > 0,
        "the client should retry the oversized response with smaller hash ranges: {declared_stats:?}"
    );
    drop(declared);

    let terminal = GetChunksProxy::start(
        server.port(),
        Some(0),
        GetChunksProxyFailure::FailOversizedSingletonOnce,
    );
    let terminal_clone = Connection::open(temp.path().join("terminal-clone.db"))?;
    let terminal_error = terminal_clone
        .query_row::<i64, _, _>(
            "SELECT dolt_clone(?1)",
            params![terminal.database_url("origin.db")],
            |row| row.get(0),
        )
        .expect_err("should reject a single oversized chunk without retrying it");
    assert!(
        matches!(terminal_error, Error::SqliteFailure(code, _) if code.extended_code == ffi::SQLITE_TOOBIG),
        "should map a single chunk 413 to SQLITE_TOOBIG: {terminal_error:?}"
    );
    let terminal_stats = terminal.stats();
    assert!(
        terminal_stats.proxy_errors.is_empty(),
        "proxy errors: {terminal_stats:?}"
    );
    assert_eq!(terminal_stats.singleton_413_responses, 1);
    assert_eq!(terminal_stats.retried_terminal_singleton_responses, 0);
    assert!(
        terminal_stats.requests < 32,
        "terminal 413 should not retry a singleton indefinitely: {terminal_stats:?}"
    );

    Ok(())
}

fn assert_batch_clone_contents(
    connection: &Connection,
    expected_hash: &str,
    expected_rows: i64,
) -> Result<()> {
    let clone_hash: String =
        connection.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    assert_eq!(clone_hash, expected_hash);

    let mut statement = connection.prepare("SELECT id, value FROM batch_values ORDER BY id")?;
    let mut rows = statement.query([])?;
    for expected_id in 1..=expected_rows {
        let row = rows.next()?.expect("should contain each fixture row");
        let actual_id: i64 = row.get(0)?;
        let actual_value: String = row.get(1)?;
        assert_eq!(actual_id, expected_id);
        assert_eq!(
            actual_value,
            chunk_batch_test_value(expected_id),
            "row {expected_id} changed during adaptive retry"
        );
    }
    assert!(
        rows.next()?.is_none(),
        "should not contain extra fixture rows"
    );
    Ok(())
}

#[test]
fn remote_server_keeps_the_public_chunk_limit() -> Result<()> {
    const LARGE_CHUNK_BYTES: u32 = 64 * 1024 * 1024 + 1;
    const EMPTY_PROLLY_HASH: [u8; 20] = [
        0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6, 0xa0, 0x40, 0x4d, 0xea, 0x36, 0xdc, 0xc9,
        0x49, 0x9b, 0xcb, 0x25, 0xc9,
    ];

    let temp = tempfile::tempdir().expect("tempdir");
    let default_root = temp.path().join("default");
    std::fs::create_dir(&default_root).expect("default server directory");
    let default = RemoteServer::start(&default_root)?;

    let mut empty_chunk = EMPTY_PROLLY_HASH.to_vec();
    empty_chunk.extend_from_slice(&0_u32.to_le_bytes());
    let response = post_chunk_batch(default.port(), &empty_chunk);
    assert!(
        response.starts_with("HTTP/1.1 200 OK\r\n"),
        "empty chunk should initialize the test database: {response}"
    );

    let mut large_chunk = vec![0_u8; 24];
    large_chunk[20..24].copy_from_slice(&LARGE_CHUNK_BYTES.to_le_bytes());
    let default_response = post_chunk_batch(default.port(), &large_chunk);
    assert!(
        default_response.starts_with("HTTP/1.1 413 Payload Too Large\r\n"),
        "the default 64 MiB chunk cap should remain unchanged: {default_response}"
    );

    Ok(())
}

#[cfg(not(feature = "blockcachevfs"))]
#[test]
fn remote_server_rejects_cloud_uris_without_blockcachevfs() {
    let token = "private-token";
    let uri = format!("gcs://bucket/prefix?vfs=blockcachevfs&project=test&access_token={token}");
    let error = RemoteServer::start(uri).expect_err("cloud server URIs require blockcachevfs");
    assert!(matches!(
        error,
        Error::SqliteFailure(code, _)
            if code.extended_code == ffi::SQLITE_MISUSE
    ));
    assert!(!format!("{error:?}").contains(token));
    assert!(!error.to_string().contains(token));
}

#[test]
fn remote_server_rejects_unknown_vfs() {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");

    let result = RemoteServer::start_with_options(
        &server_root,
        &RemoteServerOptions::new().vfs_name("rusqdoltlite-vfs-does-not-exist"),
    );
    assert!(matches!(
        result,
        Err(Error::SqliteFailure(code, _)) if code.extended_code == rusqlite::ffi::SQLITE_ERROR
    ));
}

#[test]
fn remote_server_upload_requires_a_session_owned_attachment() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");

    let mut server = RemoteServer::start(&server_root)?;
    let error = server
        .upload()
        .expect_err("a server without a session cannot publish CBS state");
    assert!(matches!(
        error,
        Error::SqliteFailure(code, _)
            if code.extended_code == rusqlite::ffi::SQLITE_MISUSE
    ));
    Ok(())
}

#[test]
fn remote_server_uses_named_vfs_for_access_and_open() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");
    let counting_vfs = CountingVfs::new();
    let vfs_name = counting_vfs.name().to_owned();
    let options = RemoteServerOptions::new().vfs_name(vfs_name);
    let server = RemoteServer::start_with_options(&server_root, &options)?;

    let missing = send_http_request(
        server.port(),
        b"GET /missing.db/refs HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
    );
    assert!(missing.starts_with("HTTP/1.1 404 Not Found\r\n"));
    assert!(
        counting_vfs.accesses() > 0,
        "the configured VFS must handle remote database existence checks"
    );

    std::fs::write(server_root.join("existing.db"), b"not a chunk store")
        .expect("seed existing remote file");
    let _ = send_http_request(
        server.port(),
        b"GET /existing.db/refs HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
    );
    assert!(
        counting_vfs.opens() > 0,
        "the configured VFS must handle remote ChunkStore opens"
    );

    Ok(())
}

#[test]
fn remote_server_options_enable_authentication_and_timeout() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    let authorized_keys = temp.path().join("authorized-keys");
    std::fs::create_dir(&server_root).expect("server directory");
    std::fs::create_dir(&authorized_keys).expect("authorized keys directory");

    let options = RemoteServerOptions::new()
        .authentication(&authorized_keys, "remote.example.test")
        .request_timeout(Duration::from_secs(1));
    let server = RemoteServer::start_with_options(&server_root, &options)?;
    assert!(server.database_url("repo.db").starts_with("http://"));

    let mut stream = TcpStream::connect(("127.0.0.1", server.port())).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("read timeout");
    stream
        .write_all(
            b"GET /repo.db/refs HTTP/1.1\r\nHost: remote.example.test\r\nConnection: close\r\n\r\n",
        )
        .expect("write request");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read response");
    assert!(response.starts_with("HTTP/1.1 401 Unauthorized\r\n"));

    let authenticator = RemoteAuthenticator::new(&authorized_keys, "remote.example.test")?;
    let error = authenticator
        .authenticate("Bearer invalid")
        .expect_err("invalid bearer token");
    assert!(matches!(
        error,
        Error::SqliteFailure(code, _) if code.extended_code == rusqlite::ffi::SQLITE_AUTH
    ));

    Ok(())
}

#[test]
fn remote_server_persists_across_restarts() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");
    let source_path = temp.path().join("source.db");

    {
        let server = RemoteServer::start(&server_root)?;
        let source = Connection::open(&source_path)?;
        source.execute_batch(
            "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
             INSERT INTO widgets VALUES(1, 'persisted');",
        )?;
        let _: String = source.query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })?;
        let _: i64 = source.query_row(
            "SELECT dolt_remote('add', 'origin', ?1)",
            params![server.database_url("origin.db")],
            |row| row.get(0),
        )?;
        let _: i64 =
            source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;
    }

    let restarted = RemoteServer::start(&server_root)?;
    let clone = Connection::open(temp.path().join("clone.db"))?;
    let _: i64 = clone.query_row(
        "SELECT dolt_clone(?1)",
        params![restarted.database_url("origin.db")],
        |row| row.get(0),
    )?;
    let value: String = clone.query_row("SELECT name FROM widgets WHERE id = 1", [], |row| {
        row.get(0)
    })?;

    assert_eq!(value, "persisted");
    Ok(())
}

#[test]
fn set_url_updates_existing_remote() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let first_root = temp.path().join("first-server");
    let second_root = temp.path().join("second-server");
    std::fs::create_dir(&first_root).expect("first server directory");
    std::fs::create_dir(&second_root).expect("second server directory");

    let first_server = RemoteServer::start(&first_root)?;
    let second_server = RemoteServer::start(&second_root)?;
    let connection = Connection::open(temp.path().join("local.db"))?;
    connection.execute_batch(
        "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO widgets VALUES(1, 'local value');",
    )?;
    let _: i64 = connection.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String =
        connection.query_row("SELECT dolt_commit('-m', 'seed')", [], |row| row.get(0))?;
    let _: i64 = connection.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![first_server.database_url("origin.db")],
        |row| row.get(0),
    )?;
    let _: i64 =
        connection.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;
    let _: i64 =
        connection.query_row("SELECT dolt_fetch('origin', 'main')", [], |row| row.get(0))?;
    let tracking_before: String =
        connection.query_row("SELECT dolt_hashof('origin/main')", [], |row| row.get(0))?;

    let second_url = second_server.database_url("origin.db");
    let _: i64 = connection.query_row(
        "SELECT dolt_remote('set-url', 'origin', ?1)",
        params![second_url],
        |row| row.get(0),
    )?;
    let stored_url: String = connection.query_row(
        "SELECT url FROM dolt_remotes WHERE name = 'origin'",
        [],
        |row| row.get(0),
    )?;
    let remote_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM dolt_remotes WHERE name = 'origin'",
        [],
        |row| row.get(0),
    )?;

    assert_eq!(stored_url, second_url);
    assert_eq!(remote_count, 1);
    let tracking_after: String =
        connection.query_row("SELECT dolt_hashof('origin/main')", [], |row| row.get(0))?;
    assert_eq!(tracking_after, tracking_before);

    Ok(())
}

#[test]
fn pull_persists_remote_branch_to_current_local_branch() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let source_path = temp.path().join("source.db");
    let remote_path = temp.path().join("remote.db");
    let clone_path = temp.path().join("clone.db");
    let remote_url = format!("file://{}", remote_path.display());

    let source = Connection::open(&source_path)?;
    source.execute_batch(
        "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO widgets VALUES(1, 'main');",
    )?;
    let _: i64 = source.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String = source.query_row("SELECT dolt_commit('-m', 'main')", [], |row| row.get(0))?;
    let _: i64 = source.query_row("SELECT dolt_branch('feature')", [], |row| row.get(0))?;
    let _: i64 = source.query_row("SELECT dolt_checkout('feature')", [], |row| row.get(0))?;
    source.execute("INSERT INTO widgets VALUES(2, 'feature')", [])?;
    let _: i64 = source.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String =
        source.query_row("SELECT dolt_commit('-m', 'feature')", [], |row| row.get(0))?;
    let _: i64 = source.query_row("SELECT dolt_checkout('main')", [], |row| row.get(0))?;
    let _: i64 = source.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![remote_url],
        |row| row.get(0),
    )?;
    let _: i64 = source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;
    let _: i64 = source.query_row("SELECT dolt_push('origin', 'feature')", [], |row| {
        row.get(0)
    })?;

    let clone = Connection::open(&clone_path)?;
    let _: i64 = clone.query_row(
        "SELECT dolt_clone(?1)",
        params![format!("file://{}", remote_path.display())],
        |row| row.get(0),
    )?;
    let _: i64 = clone.query_row("SELECT dolt_branch('-D', 'feature')", [], |row| row.get(0))?;
    let _: i64 = clone.query_row("SELECT dolt_pull('origin', 'feature')", [], |row| {
        row.get(0)
    })?;
    drop(clone);

    let reopened = Connection::open(&clone_path)?;
    let feature_exists: bool = reopened.query_row(
        "SELECT EXISTS(SELECT 1 FROM dolt_branches WHERE name = 'feature')",
        [],
        |row| row.get(0),
    )?;
    let active_branch: String =
        reopened.query_row("SELECT active_branch()", [], |row| row.get(0))?;
    let local_hash: String =
        reopened.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    let tracking_hash: String =
        reopened.query_row("SELECT dolt_hashof('origin/feature')", [], |row| row.get(0))?;
    let pulled_name: String =
        reopened.query_row("SELECT name FROM widgets WHERE id = 2", [], |row| {
            row.get(0)
        })?;

    assert!(!feature_exists);
    assert_eq!(active_branch, "main");
    assert_eq!(local_hash, tracking_hash);
    assert_eq!(pulled_name, "feature");

    Ok(())
}

fn assert_http_status_maps_to_sqlite(status: u16, reason: &str, expected_code: i32) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("listener address");
    let response = format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\n\r\n");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).expect("read request");
        stream
            .write_all(response.as_bytes())
            .expect("write response");
    });

    let connection = Connection::open_in_memory()?;
    let _: i64 = connection.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![format!("http://{address}/remote.db")],
        |row| row.get(0),
    )?;
    let error = connection
        .query_row::<i64, _, _>("SELECT dolt_fetch('origin', 'main')", [], |row| row.get(0))
        .expect_err("fetch should be unauthorized");
    server.join().expect("server thread");

    assert!(matches!(
        error,
        Error::SqliteFailure(code, _) if code.extended_code == expected_code
    ));
    Ok(())
}

fn spawn_delayed_http_404(delay: Duration) -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("request");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut request = Vec::new();
        loop {
            let mut buffer = [0_u8; 1024];
            let count = stream.read(&mut buffer).expect("read request");
            if count == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..count]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let request_line = String::from_utf8_lossy(&request)
            .lines()
            .next()
            .unwrap_or_default()
            .to_owned();
        thread::sleep(delay);
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        request_line
    });
    (format!("http://{address}/default.db"), server)
}

fn spawn_progressing_http_404(
    interval: Duration,
    body_bytes: usize,
) -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("listener address");
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("request");
        stream.set_nodelay(true).expect("disable Nagle buffering");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut request = Vec::new();
        loop {
            let mut buffer = [0_u8; 1024];
            let count = stream.read(&mut buffer).expect("read request");
            if count == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..count]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let request_line = String::from_utf8_lossy(&request)
            .lines()
            .next()
            .unwrap_or_default()
            .to_owned();
        let response = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Length: {body_bytes}\r\nConnection: close\r\n\r\n"
        );
        let _ = stream.write_all(response.as_bytes());
        for _ in 0..body_bytes {
            thread::sleep(interval);
            if stream.write_all(b"x").is_err() {
                break;
            }
        }
        request_line
    });
    (format!("http://{address}/default.db"), server)
}

fn read_http_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut request_end = None;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set proxy request timeout");
    while request_end.is_none_or(|end| request.len() < end) {
        let mut buffer = [0_u8; 8192];
        let count = stream.read(&mut buffer).expect("read proxy request");
        assert_ne!(count, 0, "client closed an incomplete proxy request");
        request.extend_from_slice(&buffer[..count]);
        if request_end.is_none() {
            if let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..header_end]);
                let content_length = headers
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find_map(|(name, value)| {
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().expect("content length"))
                    })
                    .unwrap_or(0);
                request_end = Some(header_end + 4 + content_length);
            }
        }
    }
    request
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum GetChunksProxyFailure {
    None,
    FailAfterFirstSplitSingletonOnce,
    FailOversizedSingletonOnce,
    OversizedContentLengthOnce,
}

#[derive(Clone, Debug, Default)]
struct GetChunksProxyStats {
    requests: usize,
    max_hashes_per_request: usize,
    max_single_record_bytes: usize,
    max_multi_response_bytes: usize,
    aggregate_413_responses: usize,
    singleton_413_responses: usize,
    injected_partial_413_responses: usize,
    declared_oversized_responses: usize,
    successful_singleton_responses: usize,
    successful_split_singleton_responses: usize,
    smaller_responses_after_declared_oversize: usize,
    retried_terminal_singleton_responses: usize,
    proxy_errors: Vec<String>,
}

struct GetChunksProxyBehavior {
    threshold_bytes: Option<usize>,
    failure: GetChunksProxyFailure,
    failure_injected: bool,
    saw_aggregate_rejection: bool,
    oversized_batch_hash_count: Option<usize>,
}

struct GetChunksProxy {
    address: String,
    port: u16,
    stopped: Arc<AtomicUsize>,
    stats: Arc<Mutex<GetChunksProxyStats>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl GetChunksProxy {
    fn start(
        backend_port: u16,
        threshold_bytes: Option<usize>,
        failure: GetChunksProxyFailure,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind get-chunks proxy");
        listener
            .set_nonblocking(true)
            .expect("make get-chunks proxy stoppable");
        let address = listener.local_addr().expect("get-chunks proxy address");
        let stopped = Arc::new(AtomicUsize::new(0));
        let stats = Arc::new(Mutex::new(GetChunksProxyStats::default()));
        let thread_stopped = Arc::clone(&stopped);
        let thread_stats = Arc::clone(&stats);
        let thread = thread::spawn(move || {
            let mut behavior = GetChunksProxyBehavior {
                threshold_bytes,
                failure,
                failure_injected: false,
                saw_aggregate_rejection: false,
                oversized_batch_hash_count: None,
            };
            while thread_stopped.load(Ordering::Acquire) == 0 {
                let (mut client, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("accept get-chunks proxy request: {error}"),
                };
                if thread_stopped.load(Ordering::Acquire) != 0 {
                    break;
                }
                if let Err(error) = handle_get_chunks_proxy_request(
                    &mut client,
                    backend_port,
                    &mut behavior,
                    &thread_stats,
                ) {
                    thread_stats
                        .lock()
                        .expect("lock get-chunks proxy error stats")
                        .proxy_errors
                        .push(error.to_string());
                    break;
                }
            }
        });
        Self {
            address: format!("http://{address}"),
            port: address.port(),
            stopped,
            stats,
            thread: Some(thread),
        }
    }

    fn database_url(&self, database: &str) -> String {
        format!("{}/{database}", self.address)
    }

    fn stats(&self) -> GetChunksProxyStats {
        self.stats
            .lock()
            .expect("lock get-chunks proxy stats")
            .clone()
    }
}

impl Drop for GetChunksProxy {
    fn drop(&mut self) {
        self.stopped.store(1, Ordering::Release);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join get-chunks proxy");
        }
    }
}

fn handle_get_chunks_proxy_request(
    client: &mut TcpStream,
    backend_port: u16,
    behavior: &mut GetChunksProxyBehavior,
    stats: &Mutex<GetChunksProxyStats>,
) -> std::io::Result<()> {
    let request = read_http_request(client);
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "proxy request has no headers",
            )
        })?
        + 4;
    let headers = std::str::from_utf8(&request[..header_end])
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()))?;
    let request_line = headers.lines().next().unwrap_or_default();
    let mut request_fields = request_line.split_whitespace();
    let method = request_fields.next().unwrap_or_default();
    let target = request_fields.next().unwrap_or_default();
    let path = target.split('?').next().unwrap_or_default();
    let get_chunks = method.eq_ignore_ascii_case("POST") && path.ends_with("/get-chunks");
    let request_body = &request[header_end..];
    let hashes = if get_chunks {
        let (hashes, remainder) = request_body.as_chunks::<20>();
        if !remainder.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "get-chunks request is not a sequence of 20-byte hashes",
            ));
        }
        hashes.to_vec()
    } else {
        Vec::new()
    };

    let mut upstream = TcpStream::connect(("127.0.0.1", backend_port))?;
    upstream.set_read_timeout(Some(Duration::from_secs(30)))?;
    upstream.set_write_timeout(Some(Duration::from_secs(30)))?;
    upstream.write_all(&request)?;
    let mut response = Vec::new();
    upstream.read_to_end(&mut response)?;
    if !get_chunks {
        return client.write_all(&response);
    }

    let (status, response_body) = http_response_status_and_body(&response)?;
    if status != 200 || hashes.is_empty() {
        return client.write_all(&response);
    }
    let largest_record = largest_chunk_record_bytes(response_body, hashes.len())?;
    let too_large = behavior
        .threshold_bytes
        .is_some_and(|threshold| response_body.len() > threshold);
    let mut reject = false;
    let mut declared_oversize = false;
    {
        let mut stats = stats.lock().expect("lock get-chunks proxy stats");
        stats.requests += 1;
        stats.max_hashes_per_request = stats.max_hashes_per_request.max(hashes.len());
        stats.max_single_record_bytes = stats.max_single_record_bytes.max(largest_record);
        if hashes.len() > 1 {
            stats.max_multi_response_bytes =
                stats.max_multi_response_bytes.max(response_body.len());
        }

        if behavior.failure == GetChunksProxyFailure::OversizedContentLengthOnce
            && !behavior.failure_injected
            && hashes.len() > 1
        {
            behavior.failure_injected = true;
            behavior.oversized_batch_hash_count = Some(hashes.len());
            stats.declared_oversized_responses += 1;
            declared_oversize = true;
        } else if too_large && hashes.len() > 1 {
            reject = true;
            behavior.saw_aggregate_rejection = true;
            stats.aggregate_413_responses += 1;
        }

        if !reject
            && !declared_oversize
            && behavior.failure == GetChunksProxyFailure::FailAfterFirstSplitSingletonOnce
            && behavior.saw_aggregate_rejection
            && !behavior.failure_injected
            && hashes.len() == 1
            && stats.successful_split_singleton_responses > 0
        {
            reject = true;
            behavior.failure_injected = true;
            stats.singleton_413_responses += 1;
            stats.injected_partial_413_responses += 1;
        } else if too_large && hashes.len() == 1 {
            if behavior.failure == GetChunksProxyFailure::FailOversizedSingletonOnce
                && !behavior.failure_injected
            {
                reject = true;
                behavior.failure_injected = true;
                stats.singleton_413_responses += 1;
            } else if behavior.failure == GetChunksProxyFailure::FailOversizedSingletonOnce {
                stats.retried_terminal_singleton_responses += 1;
            } else {
                reject = true;
                stats.singleton_413_responses += 1;
            }
        }

        if !reject && !declared_oversize {
            if hashes.len() == 1 {
                stats.successful_singleton_responses += 1;
            }
            if behavior.saw_aggregate_rejection && hashes.len() == 1 {
                stats.successful_split_singleton_responses += 1;
            }
            if behavior
                .oversized_batch_hash_count
                .is_some_and(|count| hashes.len() < count)
            {
                stats.smaller_responses_after_declared_oversize += 1;
            }
        }
    }

    if declared_oversize {
        return client.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Length: 134217729\r\nConnection: close\r\n\r\n",
        );
    }
    if reject {
        client.write_all(
            b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
    } else {
        client.write_all(&response)
    }
}

fn http_response_status_and_body(response: &[u8]) -> std::io::Result<(u16, &[u8])> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "response has no headers")
        })?;
    let headers = std::str::from_utf8(&response[..header_end])
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string()))?;
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "response has invalid status",
            )
        })?;
    Ok((status, &response[header_end + 4..]))
}

fn largest_chunk_record_bytes(body: &[u8], hashes: usize) -> std::io::Result<usize> {
    let mut offset = 0;
    let mut largest = 0;
    for _ in 0..hashes {
        let length_bytes = body.get(offset..offset + 4).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "truncated chunk length")
        })?;
        let length = u32::from_be_bytes(
            length_bytes
                .try_into()
                .expect("four-byte chunk length was selected"),
        );
        offset += 4;
        let chunk_bytes = if length == u32::MAX {
            0
        } else {
            usize::try_from(length).map_err(|error| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
            })?
        };
        let end = offset.checked_add(chunk_bytes).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "chunk length overflow")
        })?;
        if end > body.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "chunk response is truncated",
            ));
        }
        largest = largest.max(4 + chunk_bytes);
        offset = end;
    }
    if offset != body.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "chunk response contains trailing bytes",
        ));
    }
    Ok(largest)
}

fn chunk_batch_test_value(id: i64) -> String {
    let mut state = (id as u32)
        .wrapping_mul(0x9E37_79B9)
        .wrapping_add(0xA5A5_1F3D);
    let mut bytes = Vec::with_capacity(8 * 1024);
    for _ in 0..8 * 1024 {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        bytes.push(b'a' + (state % 26) as u8);
    }
    String::from_utf8(bytes).expect("generated payload uses ASCII")
}

fn spawn_chunk_stalling_http_proxy(backend_port: u16) -> (String, thread::JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind transport fault proxy");
    let address = listener.local_addr().expect("proxy address");
    listener
        .set_nonblocking(true)
        .expect("make transport proxy stoppable");
    let server = thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "push did not send a chunk request to the transport proxy"
            );
            let (mut client, _) = match listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                }
                Err(error) => panic!("accept proxied HTTP request: {error}"),
            };
            let request = read_http_request(&mut client);
            let header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .expect("proxy request headers")
                + 4;
            let headers =
                String::from_utf8(request[..header_end].to_vec()).expect("HTTP headers are UTF-8");
            let request_line = headers
                .lines()
                .next()
                .expect("HTTP request line")
                .to_owned();
            let request_path = request_line
                .split_whitespace()
                .nth(1)
                .expect("HTTP request target");
            if request_path.ends_with("/chunks") {
                thread::sleep(Duration::from_millis(750));
                return request_line;
            }

            let forwarded_headers = headers.replace("/private-token/", "/");
            let mut upstream = TcpStream::connect(("127.0.0.1", backend_port))
                .expect("connect to backend remote server");
            upstream
                .write_all(forwarded_headers.as_bytes())
                .expect("forward request headers");
            upstream
                .write_all(&request[header_end..])
                .expect("forward request body");
            upstream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set backend response timeout");
            let mut response = Vec::new();
            upstream
                .read_to_end(&mut response)
                .expect("read backend response");
            client
                .write_all(&response)
                .expect("forward backend response");
        }
    });
    (address.to_string(), server)
}

fn test_http_idle_timeout_requires_progress() -> Result<()> {
    let connection = Connection::open_in_memory()?;
    let (progressing_url, progressing_server) =
        spawn_progressing_http_404(Duration::from_millis(100), 8);
    let progressing_url = format!("{progressing_url}?http_idle_timeout_ms=500");
    let _: i64 = connection.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![progressing_url],
        |row| row.get(0),
    )?;
    let progress_started = std::time::Instant::now();
    let progressing_error = connection
        .query_row::<i64, _, _>("SELECT dolt_fetch('origin', 'main')", [], |row| row.get(0))
        .expect_err("a 404 response should fail the fetch");
    assert!(
        matches!(
            progressing_error,
            Error::SqliteFailure(code, _) if code.extended_code != ffi::SQLITE_IOERR
        ),
        "active response progress must keep the request alive: {progressing_error:?}"
    );
    assert!(progress_started.elapsed() >= Duration::from_millis(650));
    assert_eq!(
        progressing_server
            .join()
            .expect("progressing-response server thread"),
        "GET /default.db/refs HTTP/1.1"
    );

    let (configured_stall_url, configured_stall_server) =
        spawn_delayed_http_404(Duration::from_millis(750));
    let configured_stall_url = format!(
        "{}?http_idle_timeout_ms=500",
        configured_stall_url.replace("/default.db", "/default.db/private-token")
    );
    let _: i64 = connection.query_row(
        "SELECT dolt_remote('set-url', 'origin', ?1)",
        params![configured_stall_url],
        |row| row.get(0),
    )?;
    let configured_stall_error = connection
        .query_row::<i64, _, _>("SELECT dolt_fetch('origin', 'main')", [], |row| row.get(0))
        .expect_err("an idle server should exceed the URL-configured timeout");
    let diagnostic = match &configured_stall_error {
        Error::SqliteFailure(code, Some(message)) => {
            assert_eq!(code.extended_code, ffi::SQLITE_IOERR);
            message
        }
        other => panic!(
            "a stalled URL-configured request should still fail with an SQLite I/O error: {other:?}"
        ),
    };
    assert!(
        diagnostic.contains("reading remote HTTP response failed"),
        "the diagnostic should name the failed transport phase: {diagnostic}"
    );
    assert!(
        diagnostic.contains("configured idle timeout 500 ms"),
        "the diagnostic should include the configured idle limit: {diagnostic}"
    );
    assert!(
        !diagnostic.contains("private-token") && !diagnostic.contains("default.db"),
        "the diagnostic must not expose the remote URL: {diagnostic}"
    );
    assert_eq!(
        configured_stall_server
            .join()
            .expect("URL-configured stalled-response server thread"),
        "GET /default.db/private-token/refs HTTP/1.1"
    );

    let (stalled_url, stalled_server) = spawn_delayed_http_404(Duration::from_millis(750));
    let _: i64 = connection.query_row(
        "SELECT dolt_remote('set-url', 'origin', ?1)",
        params![stalled_url],
        |row| row.get(0),
    )?;
    let env_stalled_error = connection
        .query_row::<i64, _, _>("SELECT dolt_fetch('origin', 'main')", [], |row| row.get(0))
        .expect_err("an idle server should exceed the environment default");
    assert!(
        matches!(
            env_stalled_error,
            Error::SqliteFailure(code, _) if code.extended_code == ffi::SQLITE_IOERR
        ),
        "a stalled request should still time out from the environment default: {env_stalled_error:?}"
    );
    assert_eq!(
        stalled_server
            .join()
            .expect("environment-configured stalled-response server thread"),
        "GET /default.db/refs HTTP/1.1"
    );

    Ok(())
}

#[test]
fn http_idle_timeout_requires_progress() -> Result<()> {
    const CHILD_MARKER: &str = "DOLTLITE_HTTP_IDLE_TIMEOUT_TEST_CHILD";
    if std::env::var_os(CHILD_MARKER).is_some() {
        return test_http_idle_timeout_requires_progress();
    }

    let output = Command::new(std::env::current_exe().expect("test executable path"))
        .args([
            "--exact",
            "http_idle_timeout_requires_progress",
            "--nocapture",
        ])
        .env(CHILD_MARKER, "1")
        .env("DOLTLITE_HTTP_TIMEOUT_MS", "100")
        .output()
        .expect("run isolated idle-timeout test process");
    assert!(
        output.status.success(),
        "idle-timeout subprocess failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
fn zero_http_idle_timeout_requires_numeric_http_loopback() -> Result<()> {
    for url in [
        "http://localhost:8000/default.db?http_idle_timeout_ms=0",
        "http://192.0.2.1:8000/default.db?http_idle_timeout_ms=0",
        "http://127.0.0.1.example:8000/default.db?http_idle_timeout_ms=0",
        "http://127.0.0.256:8000/default.db?http_idle_timeout_ms=0",
        "https://127.0.0.1:8000/default.db?http_idle_timeout_ms=0",
    ] {
        let url = CString::new(url).expect("test URL contains no NUL byte");
        let remote = unsafe { doltlite_http_remote_open_for_test(url.as_ptr()) };
        assert!(
            remote.is_null(),
            "unbounded idle timeout should reject {url:?}"
        );
    }
    Ok(())
}

#[test]
fn http_push_read_failure_includes_safe_transport_context() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");
    let server = RemoteServer::start(&server_root)?;
    let source = Connection::open(temp.path().join("source.db"))?;
    source.execute_batch(
        "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO widgets VALUES(1, 'before');",
    )?;
    let _: String = source.query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
        row.get(0)
    })?;
    let _: i64 = source.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![server.database_url("origin.db")],
        |row| row.get(0),
    )?;
    let _: i64 = source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;
    source.execute("UPDATE widgets SET name = 'after' WHERE id = 1", [])?;
    let _: String = source.query_row("SELECT dolt_commit('-A', '-m', 'update')", [], |row| {
        row.get(0)
    })?;

    let (proxy_address, proxy) = spawn_chunk_stalling_http_proxy(server.port());
    let proxy_url =
        format!("http://{proxy_address}/origin.db/private-token?http_idle_timeout_ms=500");
    let _: i64 = source.query_row(
        "SELECT dolt_remote('set-url', 'origin', ?1)",
        params![proxy_url],
        |row| row.get(0),
    )?;
    let error = source
        .query_row::<i64, _, _>("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))
        .expect_err("the stalled chunk response should fail the push");
    let diagnostic = match &error {
        Error::SqliteFailure(code, Some(message)) => {
            assert_eq!(code.extended_code, ffi::SQLITE_IOERR);
            message
        }
        other => panic!("expected a contextual SQLite I/O error: {other:?}"),
    };
    assert!(
        diagnostic.contains("reading remote HTTP response failed"),
        "the push error should name the failed transport phase: {diagnostic}"
    );
    assert!(
        diagnostic.contains("configured idle timeout 500 ms"),
        "the push error should include its configured idle limit: {diagnostic}"
    );
    assert!(
        !diagnostic.contains("private-token") && !diagnostic.contains("origin.db"),
        "the push error must not expose its remote URL: {diagnostic}"
    );
    assert_eq!(
        proxy.join().expect("transport proxy thread"),
        "POST /origin.db/private-token/chunks HTTP/1.1"
    );
    Ok(())
}

#[cfg(feature = "blockcachevfs")]
#[test]
#[ignore = "requires the pinned local GCS emulator container"]
fn uri_session_push_can_outlive_loopback_response_idle_timeout() {
    const IDLE_TIMEOUT_MS: &str = "250";
    const CLIENT_ERROR_WAIT: Duration = Duration::from_secs(15);

    let emulator_endpoint = std::env::var("BLOCKCACHEVFS_GCS_EMULATOR")
        .unwrap_or_else(|_| "http://127.0.0.1:4443".to_owned());
    let bucket = "app_storage";
    let bucket_payload = format!(r#"{{"name":"{bucket}"}}"#);
    let bucket_response = Command::new("curl")
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
            &bucket_payload,
            &format!("{}/storage/v1/b", emulator_endpoint.trim_end_matches('/')),
        ])
        .output()
        .expect("curl must be installed for GCS emulator tests");
    assert!(
        bucket_response.status.success(),
        "GCS bucket request failed"
    );
    let status = String::from_utf8_lossy(&bucket_response.stdout);
    assert!(
        status == "200" || status == "201" || status == "409",
        "GCS bucket creation returned {status}"
    );

    let suffix = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock is before Unix epoch")
            .as_nanos()
    );
    let prefix = format!("loopback-idle/{suffix}/");
    let storage_proxy = SlowGcsProxy::start(&emulator_endpoint);
    let database_uri = format!(
        "gcs://{bucket}/{prefix}?vfs=blockcachevfs&project=test-project&access_token=test-token&endpoint={}&database=default.db",
        storage_proxy.address
    );
    let session = BlockCacheSessionOptions::for_uri(
        "550e8400-e29b-41d4-a716-446655440099",
        SessionScope::new("loopback-idle-test", "default.db", "push,read")
            .expect("valid URI session scope"),
        SessionOperationId::from_request("POST", "/default.db/commit", b"")
            .expect("derive the session operation ID"),
    )
    .expect("valid URI session options");
    let database_flags = OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_CREATE
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let mut server = RemoteServer::start_with_options(
        &database_uri,
        &RemoteServerOptions::new()
            .database_open_flags(database_flags)
            .blockcache_session(session),
    )
    .expect("start a GCS-backed URI session server");
    let manifest_name = format!("{prefix}manifest.bcv");
    let initial_manifest = fetch_gcs_object(&emulator_endpoint, bucket, &manifest_name);

    let temp = tempfile::tempdir().expect("create push source directory");
    let source =
        Connection::open(temp.path().join("source.db")).expect("create local Dolt push source");
    source
        .execute_batch("CREATE TABLE sequences(id INTEGER PRIMARY KEY, sequence TEXT NOT NULL);")
        .expect("create large source table");
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    let sequence: String = (0..4 * 1024 * 1024)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            alphabet[((state >> 32) as usize) % alphabet.len()] as char
        })
        .collect();
    source
        .execute(
            "INSERT INTO sequences(id, sequence) VALUES (1, ?1)",
            params![sequence],
        )
        .expect("write large source row");
    let _: i64 = source
        .query_row("SELECT dolt_add('-A')", [], |row| row.get(0))
        .expect("stage source row");
    let _: String = source
        .query_row(
            "SELECT dolt_commit('-m', 'loopback storage timeout fixture')",
            [],
            |row| row.get(0),
        )
        .expect("commit source row");

    let generated_url = server.database_url("default.db");
    assert!(
        generated_url.ends_with("?http_idle_timeout_ms=0"),
        "cleartext URI session loopback URL should explicitly disable its idle timeout"
    );
    let remote_url = generated_url.replace(
        "http_idle_timeout_ms=0",
        &format!("http_idle_timeout_ms={IDLE_TIMEOUT_MS}"),
    );
    assert!(
        remote_url.contains(&format!("http_idle_timeout_ms={IDLE_TIMEOUT_MS}")),
        "URI session loopback URL should carry the shortened test timeout"
    );
    let _: i64 = source
        .query_row(
            "SELECT dolt_remote('add', 'origin', ?1)",
            params![remote_url],
            |row| row.get(0),
        )
        .expect("add the GCS-backed session server as a remote");
    storage_proxy.arm_slow_media_upload();
    let push =
        source.query_row::<i64, _, _>("SELECT dolt_push('origin', 'main')", [], |row| row.get(0));
    storage_proxy.mark_client_returned();
    let error = push.expect_err("short loopback response timeout should fail the push");
    let diagnostic = match &error {
        Error::SqliteFailure(code, Some(message)) => {
            assert_eq!(code.extended_code, ffi::SQLITE_IOERR);
            message
        }
        other => panic!("expected a contextual SQLite I/O error: {other:?}"),
    };
    assert!(
        diagnostic.contains("reading remote HTTP response failed")
            && diagnostic.contains(&format!("configured idle timeout {IDLE_TIMEOUT_MS} ms")),
        "the client error should identify the response wait and its configured limit: {diagnostic}"
    );
    assert!(
        !diagnostic.contains("test-token") && !diagnostic.contains(&prefix),
        "transport context must not leak storage credentials or path: {diagnostic}"
    );

    let deadline = Instant::now() + CLIENT_ERROR_WAIT;
    while storage_proxy.bytes_after_client_return() == 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        storage_proxy.bytes_after_client_return() > 0,
        "GCS media upload bytes should continue flowing after Dolt push returned its idle-timeout error; media uploads={}",
        storage_proxy.media_uploads()
    );
    assert!(
        storage_proxy.completed_media_uploads() > 0,
        "the storage proxy should complete at least one GCS media upload after the client error"
    );
    assert!(
        storage_proxy.failure().is_none(),
        "storage proxy reported a failure: {:?}",
        storage_proxy.failure()
    );

    let mut retry_state = 0x9e37_79b9_7f4a_7c15_u64;
    let retry_suffix: String = (0..1024 * 1024)
        .map(|_| {
            retry_state = retry_state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            alphabet[((retry_state >> 32) as usize) % alphabet.len()] as char
        })
        .collect();
    let retry_sequence = format!("{sequence}{retry_suffix}");
    source
        .execute(
            "UPDATE sequences SET sequence = ?1 WHERE id = 1",
            params![retry_sequence],
        )
        .expect("change source graph after the timed-out attempt");
    let _: String = source
        .query_row(
            "SELECT dolt_commit('-A', '-m', 'retry after loopback wait')",
            [],
            |row| row.get(0),
        )
        .expect("commit retry fixture");
    let _: i64 = source
        .query_row(
            "SELECT dolt_remote('set-url', 'origin', ?1)",
            params![generated_url],
            |row| row.get(0),
        )
        .expect("restore the generated no-idle-timeout URL");
    let uploads_before_retry = storage_proxy.media_uploads();
    storage_proxy.arm_slow_media_upload();
    let retry_started = Instant::now();
    let _: i64 = source
        .query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))
        .expect("generated session URL should wait through slow GCS storage work");
    assert!(
        retry_started.elapsed() > Duration::from_millis(IDLE_TIMEOUT_MS.parse().unwrap()),
        "the client should stay connected beyond the former timeout while the server writes GCS blocks"
    );
    assert!(
        storage_proxy.media_uploads() > uploads_before_retry,
        "the successful push should perform another GCS media upload"
    );
    let local_hash: String = source
        .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))
        .expect("read retried local graph hash");
    let remote_hash: String = server
        .database_connection()
        .expect("running URI server exposes its database")
        .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))
        .expect("read accepted graph hash from URI server");
    assert_eq!(remote_hash, local_hash);
    server
        .stage_request()
        .expect("checkpoint and accept the successful retry");
    assert_eq!(
        server
            .operation_status()
            .expect("read staged session operation status"),
        SessionOperationStatus::Accepted
    );
    assert!(
        server.first_storage_error().is_none(),
        "the storage-backed server should complete its GCS writes despite the loopback client's timeout"
    );
    assert_eq!(
        fetch_gcs_object(&emulator_endpoint, bucket, &manifest_name),
        initial_manifest,
        "staging accepted graph blocks must not publish the cloud manifest"
    );
    server.close().expect("close staged URI session server");
    assert_eq!(
        fetch_gcs_object(&emulator_endpoint, bucket, &manifest_name),
        initial_manifest,
        "closing the staged session must leave manifest publication to the server owner"
    );
    source.close().expect("close local Dolt push source");
}

#[test]
fn http_authorization_statuses_map_to_sqlite_auth() -> Result<()> {
    assert_http_status_maps_to_sqlite(401, "Unauthorized", rusqlite::ffi::SQLITE_AUTH)?;
    assert_http_status_maps_to_sqlite(403, "Forbidden", rusqlite::ffi::SQLITE_AUTH)
}

#[test]
fn http_conflict_maps_to_sqlite_busy_snapshot() -> Result<()> {
    assert_http_status_maps_to_sqlite(409, "Conflict", rusqlite::ffi::SQLITE_BUSY_SNAPSHOT)
}

#[test]
fn http_payload_too_large_maps_to_sqlite_toobig() -> Result<()> {
    assert_http_status_maps_to_sqlite(413, "Payload Too Large", rusqlite::ffi::SQLITE_TOOBIG)
}

#[test]
fn concurrent_pushes_reject_one_stale_ref_update() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");
    let server = RemoteServer::start(&server_root)?;
    let remote_url = server.database_url("origin.db");
    let seed_path = temp.path().join("seed.db");

    let seed = Connection::open(&seed_path)?;
    seed.execute_batch(
        "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO widgets VALUES(1, 'seed');",
    )?;
    let _: String = seed.query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
        row.get(0)
    })?;
    let _: i64 = seed.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![remote_url],
        |row| row.get(0),
    )?;
    let _: i64 = seed.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;

    let first_path = temp.path().join("first.db");
    let second_path = temp.path().join("second.db");
    for (path, name) in [(&first_path, "first"), (&second_path, "second")] {
        let connection = Connection::open(path)?;
        let _: i64 = connection.query_row(
            "SELECT dolt_clone(?1)",
            params![server.database_url("origin.db")],
            |row| row.get(0),
        )?;
        connection.execute("UPDATE widgets SET name = ?1 WHERE id = 1", [name])?;
        let _: String =
            connection.query_row("SELECT dolt_commit('-A', '-m', ?1)", [name], |row| {
                row.get(0)
            })?;
    }

    let barrier = Arc::new(Barrier::new(3));
    let handles = [first_path, second_path].map(|path| {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            let connection = Connection::open(path).expect("open concurrent clone");
            barrier.wait();
            connection
                .query_row::<i64, _, _>("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))
        })
    });
    barrier.wait();
    let results = handles.map(|handle| handle.join().expect("concurrent push thread"));
    let successes = results.iter().filter(|result| result.is_ok()).count();

    assert_eq!(
        successes, 1,
        "exactly one concurrent push should advance the remote ref"
    );
    assert!(results.iter().any(|result| {
        matches!(
            result,
            Err(Error::SqliteFailure(code, _))
                if matches!(code.extended_code, rusqlite::ffi::SQLITE_BUSY | rusqlite::ffi::SQLITE_BUSY_SNAPSHOT | rusqlite::ffi::SQLITE_ERROR)
        )
    }));
    Ok(())
}

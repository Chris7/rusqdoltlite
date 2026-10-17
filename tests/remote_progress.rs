#![cfg(all(feature = "remote", not(target_arch = "wasm32")))]

use std::{
    collections::HashSet,
    io::{self, Read as _, Write as _},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use rusqlite::{
    Connection, DoltCloneProgressEvent, DoltPushProgressEvent, Error, RemoteServer, Result, params,
};

type Trace = Arc<Mutex<Vec<TraceEntry>>>;
type SourceOperationSignature = (String, String, Vec<u8>, Option<u16>, Vec<u8>);

#[derive(Clone, Debug)]
enum TraceEntry {
    Progress(DoltPushProgressEvent),
    CloneProgress(DoltCloneProgressEvent),
    Request(CapturedRequest),
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    method: String,
    path: String,
    body: Vec<u8>,
    status: Option<u16>,
    response_body: Vec<u8>,
}

#[repr(usize)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloneProxyFault {
    None,
    RejectGetChunks,
    CorruptGetChunks,
    TruncateGetChunks,
}

struct RecordingProxy {
    address: String,
    stopped: Arc<AtomicBool>,
    reject_chunk_request: Arc<AtomicUsize>,
    get_chunks_requests: Arc<AtomicUsize>,
    clone_fault: Arc<AtomicUsize>,
    error: Arc<Mutex<Option<String>>>,
    worker: Option<JoinHandle<()>>,
}

impl RecordingProxy {
    fn start(upstream_port: u16, trace: Trace) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("should bind progress proxy");
        listener
            .set_nonblocking(true)
            .expect("should make progress proxy stoppable");
        let address = listener
            .local_addr()
            .expect("should get progress proxy address");
        let stopped = Arc::new(AtomicBool::new(false));
        let chunk_requests = Arc::new(AtomicUsize::new(0));
        let reject_chunk_request = Arc::new(AtomicUsize::new(0));
        let get_chunks_requests = Arc::new(AtomicUsize::new(0));
        let clone_fault = Arc::new(AtomicUsize::new(CloneProxyFault::None as usize));
        let error = Arc::new(Mutex::new(None));

        let worker_stopped = Arc::clone(&stopped);
        let worker_chunk_requests = Arc::clone(&chunk_requests);
        let worker_get_chunks_requests = Arc::clone(&get_chunks_requests);
        let worker_reject_chunk_request = Arc::clone(&reject_chunk_request);
        let worker_clone_fault = Arc::clone(&clone_fault);
        let worker_error = Arc::clone(&error);
        let worker = thread::spawn(move || {
            while !worker_stopped.load(Ordering::Acquire) {
                let (mut client, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => {
                        save_proxy_error(&worker_error, format!("accept failed: {error}"));
                        break;
                    }
                };
                if worker_stopped.load(Ordering::Acquire) {
                    break;
                }
                let _ = client.set_read_timeout(Some(Duration::from_secs(60)));
                let _ = client.set_write_timeout(Some(Duration::from_secs(60)));
                if let Err(error) = handle_proxy_request(
                    &mut client,
                    upstream_port,
                    &trace,
                    &worker_chunk_requests,
                    &worker_get_chunks_requests,
                    &worker_reject_chunk_request,
                    &worker_clone_fault,
                ) {
                    save_proxy_error(&worker_error, format!("request relay failed: {error}"));
                }
            }
        });

        Self {
            address: format!("http://{address}"),
            stopped,
            reject_chunk_request,
            get_chunks_requests,
            clone_fault,
            error,
            worker: Some(worker),
        }
    }

    fn database_url(&self) -> String {
        format!("{}/origin.db", self.address)
    }

    fn reject_nth_chunk_request(&self, request_number: usize) {
        self.reject_chunk_request
            .store(request_number, Ordering::Release);
    }

    fn set_get_chunks_fault(&self, fault: CloneProxyFault) {
        self.get_chunks_requests.store(0, Ordering::Release);
        self.clone_fault.store(fault as usize, Ordering::Release);
    }

    fn error(&self) -> Option<String> {
        self.error.lock().expect("should lock proxy error").clone()
    }
}

impl Drop for RecordingProxy {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        let _ = TcpStream::connect(self.address.trim_start_matches("http://"));
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct TestContext {
    _temporary: tempfile::TempDir,
    server: RemoteServer,
    proxy: RecordingProxy,
    source: Connection,
    trace: Trace,
}

impl TestContext {
    fn new(rows: usize, payload_size: usize) -> Result<Self> {
        let temporary = tempfile::tempdir().expect("should create remote progress tempdir");
        let server_root = temporary.path().join("server");
        std::fs::create_dir(&server_root).expect("should create remote progress server directory");
        let server = RemoteServer::start(&server_root)?;
        let trace = Arc::new(Mutex::new(Vec::new()));
        let proxy = RecordingProxy::start(server.port(), Arc::clone(&trace));
        let source = Connection::open(temporary.path().join("source.db"))?;
        source.execute_batch(
            "CREATE TABLE payloads(id INTEGER PRIMARY KEY, content BLOB NOT NULL);",
        )?;
        for id in 0..rows {
            let content = deterministic_payload(payload_size, id as u32 + 1);
            source.execute(
                "INSERT INTO payloads(id, content) VALUES (?1, ?2)",
                params![id as i64 + 1, &content[..]],
            )?;
        }
        let _: i64 = source.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
        let _: String = source.query_row(
            "SELECT dolt_commit('-m', 'progress test seed')",
            [],
            |row| row.get(0),
        )?;
        let _: i64 = source.query_row(
            "SELECT dolt_remote('add', 'origin', ?1)",
            params![proxy.database_url()],
            |row| row.get(0),
        )?;
        Ok(Self {
            _temporary: temporary,
            server,
            proxy,
            source,
            trace,
        })
    }
}

fn deterministic_payload(length: usize, seed: u32) -> Vec<u8> {
    let mut value = seed | 1;
    let mut bytes = Vec::with_capacity(length);
    for _ in 0..length {
        value ^= value << 13;
        value ^= value >> 17;
        value ^= value << 5;
        bytes.push(value as u8);
    }
    bytes
}

fn save_proxy_error(destination: &Mutex<Option<String>>, message: String) {
    if let Ok(mut current) = destination.lock() {
        if current.is_none() {
            *current = Some(message);
        }
    }
}

fn handle_proxy_request(
    client: &mut TcpStream,
    upstream_port: u16,
    trace: &Trace,
    chunk_requests: &AtomicUsize,
    get_chunks_requests: &AtomicUsize,
    reject_chunk_request: &AtomicUsize,
    clone_fault: &AtomicUsize,
) -> io::Result<()> {
    let request = read_http_request(client)?;
    let header_end = find_header_end(&request)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request headers"))?;
    let header = std::str::from_utf8(&request[..header_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "request headers are not UTF-8"))?;
    let mut request_line = header
        .lines()
        .next()
        .unwrap_or_default()
        .split_ascii_whitespace();
    let method = request_line.next().unwrap_or_default().to_owned();
    let path = request_line.next().unwrap_or_default().to_owned();
    let body = if path.ends_with("/chunks") || path.ends_with("/get-chunks") {
        request[header_end + 4..].to_vec()
    } else {
        Vec::new()
    };
    let request_index = {
        let mut records = trace.lock().expect("should lock proxy trace");
        records.push(TraceEntry::Request(CapturedRequest {
            method: method.clone(),
            path: path.clone(),
            body,
            status: None,
            response_body: Vec::new(),
        }));
        records.len() - 1
    };

    let is_chunk_request = method == "POST" && path.ends_with("/chunks");
    let is_get_chunks = method == "POST" && path.ends_with("/get-chunks");
    let chunk_number = if is_chunk_request {
        chunk_requests.fetch_add(1, Ordering::AcqRel) + 1
    } else {
        0
    };
    let get_chunks_number = if is_get_chunks {
        get_chunks_requests.fetch_add(1, Ordering::AcqRel) + 1
    } else {
        0
    };
    let fault = clone_fault.load(Ordering::Acquire);
    let should_reject =
        is_chunk_request && chunk_number == reject_chunk_request.load(Ordering::Acquire);
    let should_reject_clone_get = is_get_chunks
        && get_chunks_number == 1
        && fault == CloneProxyFault::RejectGetChunks as usize;
    let mut response = if should_reject_clone_get {
        let body = br#"{"code":"injected_failure","message":"reject this clone batch"}"#;
        format!(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            std::str::from_utf8(body).expect("should parse static JSON response as UTF-8")
        )
        .into_bytes()
    } else if should_reject {
        let body = br#"{"code":"injected_failure","message":"reject this chunk batch"}"#;
        format!(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            std::str::from_utf8(body).expect("should parse static JSON response as UTF-8")
        )
        .into_bytes()
    } else {
        let rewritten = with_connection_close(&request)?;
        let mut upstream = TcpStream::connect(("127.0.0.1", upstream_port))?;
        upstream.set_read_timeout(Some(Duration::from_secs(60)))?;
        upstream.set_write_timeout(Some(Duration::from_secs(60)))?;
        upstream.write_all(&rewritten)?;
        read_http_response(&mut upstream)?
    };

    if is_get_chunks && get_chunks_number == 1 {
        match fault {
            value if value == CloneProxyFault::CorruptGetChunks as usize => {
                corrupt_first_get_chunk_payload(&mut response)?;
            }
            value if value == CloneProxyFault::TruncateGetChunks as usize => {
                response.truncate(response.len().saturating_sub(1));
            }
            _ => {}
        }
    }
    let status = response_status(&response)?;
    if let Some(TraceEntry::Request(captured)) = trace
        .lock()
        .expect("should lock proxy trace")
        .get_mut(request_index)
    {
        captured.status = Some(status);
        captured.response_body = http_response_body(&response).unwrap_or_default().to_vec();
    }
    client.write_all(&response)?;
    client.flush()
}

fn find_header_end(message: &[u8]) -> Option<usize> {
    message.windows(4).position(|window| window == b"\r\n\r\n")
}

fn header_value<'a>(header: &'a str, wanted: &str) -> Option<&'a str> {
    header.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.trim()
            .eq_ignore_ascii_case(wanted)
            .then_some(value.trim())
    })
}

fn content_length(header: &str) -> io::Result<Option<usize>> {
    header_value(header, "Content-Length")
        .map(|value| {
            value.parse().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid HTTP content length")
            })
        })
        .transpose()
}

fn http_response_body(response: &[u8]) -> io::Result<&[u8]> {
    let header_end = find_header_end(response)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing response headers"))?;
    Ok(&response[header_end + 4..])
}

fn corrupt_first_get_chunk_payload(response: &mut [u8]) -> io::Result<()> {
    let header_end = find_header_end(response).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "missing chunk response headers")
    })?;
    let body_start = header_end + 4;
    if response.len() < body_start + 4 {
        return Ok(());
    }
    let mut offset = body_start;
    while offset + 4 <= response.len() {
        let payload_len = u32::from_be_bytes(
            response[offset..offset + 4]
                .try_into()
                .expect("should have a four-byte chunk payload length"),
        );
        if payload_len == u32::MAX {
            offset += 4;
            continue;
        }
        let payload_len = payload_len as usize;
        if payload_len > 0 && payload_len <= response.len().saturating_sub(offset + 4) {
            response[offset + 4] ^= 0xff;
            return Ok(());
        }
        if payload_len == 0 {
            offset += 4;
            continue;
        }
        break;
    }
    if response.len() > body_start {
        response[body_start] ^= 0xff;
    }
    Ok(())
}

fn read_http_request(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut message = Vec::new();
    let mut buffer = [0_u8; 8192];
    let header_end = loop {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed before request headers",
            ));
        }
        message.extend_from_slice(&buffer[..count]);
        if let Some(end) = find_header_end(&message) {
            break end;
        }
        if message.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request headers exceeded 64 KiB",
            ));
        }
    };
    let header = std::str::from_utf8(&message[..header_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "request headers are not UTF-8"))?;
    let total_length = header_end + 4 + content_length(header)?.unwrap_or(0);
    while message.len() < total_length {
        let read_length = (total_length - message.len()).min(buffer.len());
        let count = stream.read(&mut buffer[..read_length])?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed before request body",
            ));
        }
        message.extend_from_slice(&buffer[..count]);
    }
    message.truncate(total_length);
    Ok(message)
}

fn read_http_response(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut message = Vec::new();
    let mut buffer = [0_u8; 8192];
    let header_end = loop {
        let count = stream.read(&mut buffer)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "upstream closed before response headers",
            ));
        }
        message.extend_from_slice(&buffer[..count]);
        if let Some(end) = find_header_end(&message) {
            break end;
        }
        if message.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "response headers exceeded 64 KiB",
            ));
        }
    };
    let header = std::str::from_utf8(&message[..header_end]).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "response headers are not UTF-8")
    })?;
    if let Some(body_length) = content_length(header)? {
        let total_length = header_end + 4 + body_length;
        while message.len() < total_length {
            let read_length = (total_length - message.len()).min(buffer.len());
            let count = stream.read(&mut buffer[..read_length])?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "upstream closed before response body",
                ));
            }
            message.extend_from_slice(&buffer[..count]);
        }
        message.truncate(total_length);
    } else {
        stream.read_to_end(&mut message)?;
    }
    Ok(message)
}

fn with_connection_close(message: &[u8]) -> io::Result<Vec<u8>> {
    let header_end = find_header_end(message)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request header end"))?;
    let header = std::str::from_utf8(&message[..header_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "request headers are not UTF-8"))?;
    let mut output = Vec::with_capacity(message.len() + 24);
    for line in header.split("\r\n") {
        if line
            .split_once(':')
            .is_some_and(|(name, _)| name.trim().eq_ignore_ascii_case("Connection"))
        {
            continue;
        }
        output.extend_from_slice(line.as_bytes());
        output.extend_from_slice(b"\r\n");
    }
    output.extend_from_slice(b"Connection: close\r\n\r\n");
    output.extend_from_slice(&message[header_end + 4..]);
    Ok(output)
}

fn response_status(response: &[u8]) -> io::Result<u16> {
    let header_end = find_header_end(response)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing response headers"))?;
    let header = std::str::from_utf8(&response[..header_end]).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "response headers are not UTF-8")
    })?;
    header
        .lines()
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing response status"))?
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid response status"))
}

fn chunk_records(body: &[u8]) -> Vec<([u8; 20], u64)> {
    let mut records = Vec::new();
    let mut offset = 0;
    while offset < body.len() {
        assert!(
            body.len() - offset >= 24,
            "chunk batch has a complete record header"
        );
        let mut hash = [0_u8; 20];
        hash.copy_from_slice(&body[offset..offset + 20]);
        let length = u32::from_le_bytes(
            body[offset + 20..offset + 24]
                .try_into()
                .expect("should have a four-byte chunk record length"),
        ) as usize;
        offset += 24;
        assert!(
            length <= body.len() - offset,
            "chunk payload fits its batch"
        );
        records.push((hash, length as u64));
        offset += length;
    }
    records
}

fn trace_snapshot(trace: &Trace) -> Vec<TraceEntry> {
    trace.lock().expect("should lock progress trace").clone()
}

fn progress_events(entries: &[TraceEntry]) -> Vec<DoltPushProgressEvent> {
    entries
        .iter()
        .filter_map(|entry| match entry {
            TraceEntry::Progress(event) => Some(*event),
            TraceEntry::CloneProgress(_) | TraceEntry::Request(_) => None,
        })
        .collect()
}

fn clone_progress_events(entries: &[TraceEntry]) -> Vec<DoltCloneProgressEvent> {
    entries
        .iter()
        .filter_map(|entry| match entry {
            TraceEntry::CloneProgress(event) => Some(*event),
            TraceEntry::Progress(_) | TraceEntry::Request(_) => None,
        })
        .collect()
}

fn chunk_requests(entries: &[TraceEntry]) -> Vec<(usize, CapturedRequest)> {
    entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| match entry {
            TraceEntry::Request(request)
                if request.method == "POST" && request.path.ends_with("/chunks") =>
            {
                Some((index, request.clone()))
            }
            _ => None,
        })
        .collect()
}

fn get_chunks_requests(entries: &[TraceEntry]) -> Vec<(usize, CapturedRequest)> {
    entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| match entry {
            TraceEntry::Request(request)
                if request.method == "POST" && request.path.ends_with("/get-chunks") =>
            {
                Some((index, request.clone()))
            }
            _ => None,
        })
        .collect()
}

fn source_operation_signature(entries: &[TraceEntry]) -> Vec<SourceOperationSignature> {
    let mut requests = entries
        .iter()
        .filter_map(|entry| match entry {
            TraceEntry::Request(request) => Some((
                request.method.clone(),
                request.path.clone(),
                request.body.clone(),
                request.status,
                request.response_body.clone(),
            )),
            TraceEntry::CloneProgress(_) | TraceEntry::Progress(_) => None,
        })
        .collect::<Vec<_>>();
    requests.sort();
    requests
}

fn get_chunk_payload_lengths(request: &CapturedRequest) -> Vec<u64> {
    assert_eq!(
        request.body.len() % 20,
        0,
        "get-chunks request hashes are complete"
    );
    let hash_count = request.body.len() / 20;
    let mut payload_lengths = Vec::with_capacity(hash_count);
    let mut offset = 0;
    for _ in 0..hash_count {
        let length = u32::from_be_bytes(
            request.response_body[offset..offset + 4]
                .try_into()
                .expect("get-chunks response has each four-byte length"),
        );
        assert_ne!(length, u32::MAX, "all reachable chunks exist on the source");
        let payload_length = length as usize;
        offset += 4 + payload_length;
        assert!(
            offset <= request.response_body.len(),
            "chunk payload fits response"
        );
        payload_lengths.push(u64::from(length));
    }
    assert_eq!(
        offset,
        request.response_body.len(),
        "chunk response has no trailing bytes"
    );
    payload_lengths
}

fn push(connection: &Connection, branch: &str) -> Result<()> {
    let _: i64 =
        connection.query_row("SELECT dolt_push('origin', ?1)", params![branch], |row| {
            row.get(0)
        })?;
    Ok(())
}

fn add_and_commit(connection: &Connection) -> Result<()> {
    connection.execute(
        "INSERT INTO payloads(id, content) VALUES (?1, ?2)",
        params![999_i64, b"after the first push".as_slice()],
    )?;
    let _: i64 = connection.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String =
        connection.query_row("SELECT dolt_commit('-m', 'follow-up commit')", [], |row| {
            row.get(0)
        })?;
    Ok(())
}

#[test]
fn http_progress_plans_first_and_counts_only_acknowledged_batches_across_resume() -> Result<()> {
    const ROWS: usize = 4;
    const PAYLOAD_PER_ROW: usize = 6 * 1024 * 1024;
    let context = TestContext::new(ROWS, PAYLOAD_PER_ROW)?;
    context.proxy.reject_nth_chunk_request(2);

    let callback_trace = Arc::clone(&context.trace);
    let _guard = context.source.dolt_push_progress_callback(move |event| {
        callback_trace
            .lock()
            .expect("should lock progress callback trace")
            .push(TraceEntry::Progress(event));
    })?;

    let failed = push(&context.source, "main");
    assert!(failed.is_err(), "the injected second chunk batch must fail");
    let first_attempt = trace_snapshot(&context.trace);
    assert!(
        context.proxy.error().is_none(),
        "proxy error: {:?}",
        context.proxy.error()
    );
    let first_chunk_requests = chunk_requests(&first_attempt);
    assert!(
        first_chunk_requests.len() >= 2,
        "large fixture should span multiple chunk batches, got {}",
        first_chunk_requests.len()
    );
    assert_eq!(first_chunk_requests[0].1.status, Some(200));
    assert_eq!(first_chunk_requests[1].1.status, Some(500));
    let first_events = progress_events(&first_attempt);
    let first_plan = first_events
        .iter()
        .find_map(|event| match event {
            DoltPushProgressEvent::Plan {
                missing_chunk_count,
                missing_payload_bytes,
            } => Some((*missing_chunk_count, *missing_payload_bytes)),
            DoltPushProgressEvent::Uploaded { .. } => None,
        })
        .expect("should report the first push transfer plan");
    let first_plan_position = first_attempt
        .iter()
        .position(|entry| {
            matches!(
                entry,
                TraceEntry::Progress(DoltPushProgressEvent::Plan { .. })
            )
        })
        .expect("should find the first plan in the trace");
    assert!(
        first_plan_position < first_chunk_requests[0].0,
        "the plan must arrive before the first chunk upload"
    );

    let first_batch_records = chunk_records(&first_chunk_requests[0].1.body);
    let rejected_batch_records = chunk_records(&first_chunk_requests[1].1.body);
    assert!(!rejected_batch_records.is_empty());
    let first_acknowledged_count = first_batch_records.len() as u64;
    let first_acknowledged_bytes = first_batch_records
        .iter()
        .map(|(_, size)| size)
        .sum::<u64>();
    assert!(first_acknowledged_count > 0);
    assert!(first_acknowledged_count < first_plan.0);
    assert!(first_acknowledged_bytes < first_plan.1);
    assert_eq!(
        first_events
            .iter()
            .filter_map(|event| match event {
                DoltPushProgressEvent::Uploaded {
                    acknowledged_chunk_count,
                    acknowledged_payload_bytes,
                } => Some((*acknowledged_chunk_count, *acknowledged_payload_bytes)),
                DoltPushProgressEvent::Plan { .. } => None,
            })
            .collect::<Vec<_>>(),
        vec![(first_acknowledged_count, first_acknowledged_bytes)],
        "only the acknowledged first batch contributes progress"
    );
    assert!(
        !first_attempt.iter().any(|entry| matches!(entry,
            TraceEntry::Request(request) if request.path.ends_with("/refs-if") || request.path.ends_with("/commit")
        )),
        "a failed chunk transfer must stop before publishing refs"
    );

    let retry_trace_start = first_attempt.len();
    push(&context.source, "main")?;
    let complete_trace = trace_snapshot(&context.trace);
    let retry_attempt = &complete_trace[retry_trace_start..];
    let retry_requests = chunk_requests(retry_attempt);
    assert!(
        !retry_requests.is_empty(),
        "retry should upload previously missing chunks"
    );
    assert!(
        retry_requests
            .iter()
            .all(|(_, request)| request.status == Some(200) || request.status == Some(204))
    );
    let retry_events = progress_events(retry_attempt);
    let retry_plan = retry_events
        .iter()
        .find_map(|event| match event {
            DoltPushProgressEvent::Plan {
                missing_chunk_count,
                missing_payload_bytes,
            } => Some((*missing_chunk_count, *missing_payload_bytes)),
            DoltPushProgressEvent::Uploaded { .. } => None,
        })
        .expect("should report a new retry transfer plan");
    let retry_records = retry_requests
        .iter()
        .flat_map(|(_, request)| chunk_records(&request.body))
        .collect::<Vec<_>>();
    let retry_count = retry_records.len() as u64;
    let retry_bytes = retry_records.iter().map(|(_, size)| size).sum::<u64>();
    assert_eq!(retry_plan, (retry_count, retry_bytes));
    let retry_uploaded = retry_events
        .iter()
        .filter_map(|event| match event {
            DoltPushProgressEvent::Uploaded {
                acknowledged_chunk_count,
                acknowledged_payload_bytes,
            } => Some((*acknowledged_chunk_count, *acknowledged_payload_bytes)),
            DoltPushProgressEvent::Plan { .. } => None,
        })
        .next_back()
        .expect("should report retry acknowledgements");
    assert_eq!(retry_uploaded, retry_plan);

    let first_hashes = first_batch_records
        .iter()
        .map(|(hash, _)| *hash)
        .collect::<HashSet<_>>();
    assert!(
        retry_records
            .iter()
            .all(|(hash, _)| !first_hashes.contains(hash)),
        "a retry must not resend chunks accepted by the first batch"
    );
    let union_count = first_batch_records.len() + retry_records.len();
    let union_bytes = first_batch_records
        .iter()
        .chain(retry_records.iter())
        .map(|(_, size)| size)
        .sum::<u64>();
    assert_eq!(union_count as u64, first_plan.0);
    assert_eq!(union_bytes, first_plan.1);
    assert!(
        retry_attempt.iter().any(|entry| matches!(entry,
            TraceEntry::Request(request) if request.path.ends_with("/refs-if")
        )),
        "complete retry publishes the branch refs"
    );

    let clone = Connection::open(context._temporary.path().join("clone.db"))?;
    let _: i64 = clone.query_row(
        "SELECT dolt_clone(?1)",
        params![context.server.database_url("origin.db")],
        |row| row.get(0),
    )?;
    let remote_hash: String =
        clone.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    let source_hash: String =
        context
            .source
            .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    assert_eq!(remote_hash, source_hash);
    Ok(())
}

#[test]
fn progress_guard_is_scoped_and_reports_noop_and_delete_plans() -> Result<()> {
    let context = TestContext::new(1, 64)?;
    let callback_trace = Arc::clone(&context.trace);
    let guard = context.source.dolt_push_progress_callback(move |event| {
        callback_trace
            .lock()
            .expect("should lock progress callback trace")
            .push(TraceEntry::Progress(event));
    })?;
    let nested = context.source.dolt_push_progress_callback(|_| {});
    assert!(matches!(
        nested,
        Err(Error::SqliteFailure(code, _)) if code.extended_code == rusqlite::ffi::SQLITE_MISUSE
    ));

    push(&context.source, "main")?;
    let first_events = progress_events(&trace_snapshot(&context.trace));
    assert!(
        matches!(first_events.first(), Some(DoltPushProgressEvent::Plan { missing_chunk_count, .. }) if *missing_chunk_count > 0)
    );

    push(&context.source, "main")?;
    let all_events = progress_events(&trace_snapshot(&context.trace));
    assert!(matches!(
        all_events.last(),
        Some(DoltPushProgressEvent::Plan {
            missing_chunk_count: 0,
            missing_payload_bytes: 0
        })
    ));

    let _: i64 = context
        .source
        .query_row("SELECT dolt_branch('feature')", [], |row| row.get(0))?;
    push(&context.source, "feature")?;
    let _: i64 = context
        .source
        .query_row("SELECT dolt_branch('-D', 'feature')", [], |row| row.get(0))?;
    push(&context.source, ":feature")?;
    let all_events = progress_events(&trace_snapshot(&context.trace));
    assert!(matches!(
        all_events.last(),
        Some(DoltPushProgressEvent::Plan {
            missing_chunk_count: 0,
            missing_payload_bytes: 0
        })
    ));

    drop(guard);
    add_and_commit(&context.source)?;
    let before_unobserved_push = progress_events(&trace_snapshot(&context.trace)).len();
    push(&context.source, "main")?;
    assert_eq!(
        progress_events(&trace_snapshot(&context.trace)).len(),
        before_unobserved_push,
        "dropping the guard removes the callback"
    );
    assert!(
        context.proxy.error().is_none(),
        "proxy error: {:?}",
        context.proxy.error()
    );
    Ok(())
}

#[test]
fn interrupted_plan_and_final_acknowledgement_do_not_publish_refs() -> Result<()> {
    let context = TestContext::new(1, 64)?;
    let plan_interrupt = context.source.get_interrupt_handle();
    let plan_trace = Arc::clone(&context.trace);
    let plan_guard = context.source.dolt_push_progress_callback(move |event| {
        plan_trace
            .lock()
            .expect("should lock progress callback trace")
            .push(TraceEntry::Progress(event));
        if matches!(event, DoltPushProgressEvent::Plan { .. }) {
            plan_interrupt.interrupt();
        }
    })?;

    let plan_result = push(&context.source, "main");
    assert!(matches!(
        plan_result,
        Err(Error::SqliteFailure(code, _)) if code.extended_code == rusqlite::ffi::SQLITE_INTERRUPT
    ));
    let after_plan_interrupt = trace_snapshot(&context.trace);
    assert!(matches!(
        progress_events(&after_plan_interrupt).as_slice(),
        [DoltPushProgressEvent::Plan { missing_chunk_count, .. }] if *missing_chunk_count > 0
    ));
    assert!(chunk_requests(&after_plan_interrupt).is_empty());
    assert!(!after_plan_interrupt.iter().any(|entry| matches!(entry,
        TraceEntry::Request(request) if request.path.ends_with("/refs-if") || request.path.ends_with("/commit")
    )));

    drop(plan_guard);
    let ack_interrupt = context.source.get_interrupt_handle();
    let ack_trace = Arc::clone(&context.trace);
    let _ack_guard = context.source.dolt_push_progress_callback(move |event| {
        ack_trace
            .lock()
            .expect("should lock progress callback trace")
            .push(TraceEntry::Progress(event));
        if let DoltPushProgressEvent::Uploaded {
            acknowledged_chunk_count: 1..,
            ..
        } = event
        {
            ack_interrupt.interrupt();
        }
    })?;
    let ack_result = push(&context.source, "main");
    assert!(matches!(
        ack_result,
        Err(Error::SqliteFailure(code, _)) if code.extended_code == rusqlite::ffi::SQLITE_INTERRUPT
    ));
    let after_ack_interrupt = trace_snapshot(&context.trace);
    let ack_events = progress_events(&after_ack_interrupt);
    let plan = ack_events
        .iter()
        .filter_map(|event| match event {
            DoltPushProgressEvent::Plan {
                missing_chunk_count,
                missing_payload_bytes,
            } => Some((*missing_chunk_count, *missing_payload_bytes)),
            DoltPushProgressEvent::Uploaded { .. } => None,
        })
        .next_back()
        .expect("should report the second push plan");
    let acknowledged = ack_events
        .into_iter()
        .filter_map(|event| match event {
            DoltPushProgressEvent::Uploaded {
                acknowledged_chunk_count,
                acknowledged_payload_bytes,
            } => Some((acknowledged_chunk_count, acknowledged_payload_bytes)),
            DoltPushProgressEvent::Plan { .. } => None,
        })
        .next_back()
        .expect("should retain accepted chunk counts after interruption");
    assert!(acknowledged.0 > 0);
    assert!(acknowledged.1 > 0);
    assert_eq!(
        acknowledged, plan,
        "the interrupted callback ran on the final ACK"
    );
    assert!(!after_ack_interrupt.iter().any(|entry| matches!(entry,
        TraceEntry::Request(request) if request.path.ends_with("/refs-if") || request.path.ends_with("/commit")
    )), "interruption after the final acknowledgement must stop ref publication");
    assert!(
        context.proxy.error().is_none(),
        "proxy error: {:?}",
        context.proxy.error()
    );
    Ok(())
}

#[test]
fn callback_panics_are_contained_and_sql_push_arity_is_unchanged() -> Result<()> {
    let context = TestContext::new(1, 32)?;
    let invocations = Arc::new(AtomicUsize::new(0));
    let callback_invocations = Arc::clone(&invocations);
    let _guard = context.source.dolt_push_progress_callback(move |_| {
        callback_invocations.fetch_add(1, Ordering::Relaxed);
        panic!("the progress callback panic must not unwind through C");
    })?;

    // This uses the existing two-argument SQL API while the private callback
    // is registered; progress observation does not add a SQL function/arity.
    push(&context.source, "main")?;
    assert!(invocations.load(Ordering::Relaxed) > 0);
    assert!(
        context.proxy.error().is_none(),
        "proxy error: {:?}",
        context.proxy.error()
    );
    Ok(())
}

#[test]
fn observed_http_clone_reports_actual_transfers_without_extra_source_io() -> Result<()> {
    let context = TestContext::new(3, 256)?;
    push(&context.source, "main")?;

    let template_path = context._temporary.path().join("clone-template.db");
    let template = Connection::open(&template_path)?;
    let _: String = template.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    drop(template);
    let unobserved_path = context._temporary.path().join("unobserved-clone.db");
    let observed_path = context._temporary.path().join("observed-clone.db");
    std::fs::copy(&template_path, &unobserved_path)
        .expect("should copy initialized clone template for ordinary clone");
    std::fs::copy(&template_path, &observed_path)
        .expect("should copy initialized clone template for observed clone");
    let unobserved = Connection::open(unobserved_path)?;
    context.trace.lock().expect("should clear trace").clear();
    let _: i64 = unobserved.query_row(
        "SELECT dolt_clone(?1)",
        params![context.proxy.database_url()],
        |row| row.get(0),
    )?;
    let unobserved_entries = trace_snapshot(&context.trace);
    let unobserved_signature = source_operation_signature(&unobserved_entries);
    let unobserved_fetches = get_chunks_requests(&unobserved_entries);
    assert!(
        !unobserved_fetches.is_empty(),
        "ordinary clone fetches graph chunks"
    );
    assert!(
        unobserved_fetches
            .iter()
            .all(|(_, request)| request.status == Some(200))
    );

    let clone = Connection::open(observed_path)?;
    context.trace.lock().expect("should clear trace").clear();
    let active = Arc::new(AtomicBool::new(false));
    let callback_active = Arc::new(Mutex::new(Vec::new()));
    let callback_trace = Arc::clone(&context.trace);
    let callback_active_copy = Arc::clone(&callback_active);
    let active_copy = Arc::clone(&active);
    let _guard = clone.dolt_clone_progress_callback(move |event| {
        callback_trace
            .lock()
            .expect("should lock clone progress trace")
            .push(TraceEntry::CloneProgress(event));
        callback_active_copy
            .lock()
            .expect("should lock callback activity trace")
            .push(active_copy.load(Ordering::Acquire));
    })?;

    active.store(true, Ordering::Release);
    let clone_result = clone.query_row::<i64, _, _>(
        "SELECT dolt_clone(?1)",
        params![context.proxy.database_url()],
        |row| row.get(0),
    );
    active.store(false, Ordering::Release);
    clone_result?;

    let entries = trace_snapshot(&context.trace);
    let events = clone_progress_events(&entries);
    assert_eq!(events.first(), Some(&DoltCloneProgressEvent::Started));
    assert_eq!(events.last(), Some(&DoltCloneProgressEvent::Completed));
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == DoltCloneProgressEvent::Started)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == DoltCloneProgressEvent::Finalizing)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| **event == DoltCloneProgressEvent::Completed)
            .count(),
        1
    );
    let finalizing_position = events
        .iter()
        .position(|event| *event == DoltCloneProgressEvent::Finalizing)
        .expect("clone finalizes after chunk transfer");
    let progress_snapshots = events
        .iter()
        .enumerate()
        .filter_map(|(position, event)| match event {
            DoltCloneProgressEvent::Progress {
                downloaded_chunk_count,
                downloaded_payload_bytes,
                reused_chunk_count,
                reused_payload_bytes,
            } => Some((
                position,
                *downloaded_chunk_count,
                *downloaded_payload_bytes,
                *reused_chunk_count,
                *reused_payload_bytes,
            )),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(!progress_snapshots.is_empty());
    assert!(
        progress_snapshots
            .iter()
            .all(|(position, ..)| 0 < *position && *position < finalizing_position)
    );
    for pair in progress_snapshots.windows(2) {
        let previous_count = pair[0].1 + pair[0].3;
        let current_count = pair[1].1 + pair[1].3;
        let previous_bytes = pair[0].2 + pair[0].4;
        let current_bytes = pair[1].2 + pair[1].4;
        assert!(current_count > previous_count);
        assert!(current_bytes >= previous_bytes);
    }
    assert!(
        callback_active
            .lock()
            .expect("should read callback activity trace")
            .iter()
            .all(|was_active| *was_active),
        "callbacks run while SQL clone is active"
    );

    let start_position = entries
        .iter()
        .position(|entry| {
            matches!(
                entry,
                TraceEntry::CloneProgress(DoltCloneProgressEvent::Started)
            )
        })
        .expect("should find start notification");
    let refs_position = entries
        .iter()
        .position(|entry| {
            matches!(entry, TraceEntry::Request(request) if request.path.ends_with("/refs"))
        })
        .expect("clone reads source refs");
    assert!(
        start_position < refs_position,
        "Started precedes source reads"
    );

    let observed_fetches = get_chunks_requests(&entries);
    assert!(
        !observed_fetches.is_empty(),
        "observed clone fetches graph chunks"
    );
    assert!(
        observed_fetches
            .iter()
            .all(|(_, request)| request.status == Some(200))
    );
    let fetched_payload_lengths = observed_fetches
        .iter()
        .flat_map(|(_, request)| get_chunk_payload_lengths(request))
        .collect::<Vec<_>>();
    let progress = progress_snapshots
        .last()
        .expect("clone reports transferred chunks");
    assert_eq!(progress.1, fetched_payload_lengths.len() as u64);
    assert_eq!(progress.2, fetched_payload_lengths.iter().sum::<u64>());

    assert_eq!(
        source_operation_signature(&entries),
        unobserved_signature,
        "progress adds no source requests or payload bytes"
    );
    let (row_count, content_bytes): (i64, i64) = clone.query_row(
        "SELECT count(*), sum(length(content)) FROM payloads",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(row_count, 3);
    assert_eq!(content_bytes, 3 * 256);
    let remote_hash: String =
        clone.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    let source_hash: String =
        context
            .source
            .query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    assert_eq!(remote_hash, source_hash);
    Ok(())
}

#[test]
fn observed_lazy_clone_is_rejected() -> Result<()> {
    let context = TestContext::new(1, 128)?;
    push(&context.source, "main")?;
    context.trace.lock().expect("should clear trace").clear();

    let clone = Connection::open(context._temporary.path().join("observed-lazy-clone.db"))?;
    let events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&events);
    let _guard = clone.dolt_clone_progress_callback(move |event| {
        callback_events
            .lock()
            .expect("should lock lazy clone progress events")
            .push(event);
    })?;
    let result = clone.query_row::<i64, _, _>(
        "SELECT dolt_clone('--lazy', ?1)",
        params![context.proxy.database_url()],
        |row| row.get(0),
    );
    assert!(result.is_err(), "observed lazy clone is unsupported");
    assert!(
        !events
            .lock()
            .expect("should read lazy clone progress events")
            .contains(&DoltCloneProgressEvent::Completed)
    );
    assert!(
        get_chunks_requests(&trace_snapshot(&context.trace)).is_empty(),
        "lazy clone does not acquire graph chunks"
    );
    Ok(())
}

#[test]
fn failed_get_chunks_never_acknowledges_and_same_guard_can_retry() -> Result<()> {
    let context = TestContext::new(1, 256)?;
    push(&context.source, "main")?;

    for (index, fault) in [
        CloneProxyFault::RejectGetChunks,
        CloneProxyFault::CorruptGetChunks,
        CloneProxyFault::TruncateGetChunks,
    ]
    .into_iter()
    .enumerate()
    {
        context.proxy.set_get_chunks_fault(fault);
        context.trace.lock().expect("should lock trace").clear();
        let clone = Connection::open(
            context
                ._temporary
                .path()
                .join(format!("failed-fetch-{index}.db")),
        )?;
        let callback_trace = Arc::clone(&context.trace);
        let _guard = clone.dolt_clone_progress_callback(move |event| {
            callback_trace
                .lock()
                .expect("should lock clone progress trace")
                .push(TraceEntry::CloneProgress(event));
        })?;
        let failed = clone.query_row::<i64, _, _>(
            "SELECT dolt_clone(?1)",
            params![context.proxy.database_url()],
            |row| row.get(0),
        );
        let failed_trace = trace_snapshot(&context.trace);
        let get_request_summaries = get_chunks_requests(&failed_trace)
            .iter()
            .map(|(_, request)| {
                (
                    request.status,
                    request.body.len(),
                    request.response_body.len(),
                    request.response_body.get(..8).unwrap_or_default().to_vec(),
                )
            })
            .collect::<Vec<_>>();
        assert!(
            failed.is_err(),
            "must reject get-chunks fault {fault:?}; requests={get_request_summaries:?}"
        );
        let failed_events = clone_progress_events(&failed_trace);
        assert!(!failed_events.contains(&DoltCloneProgressEvent::Completed));
        assert!(
            failed_events.iter().all(|event| !matches!(
                event,
                DoltCloneProgressEvent::Progress {
                    downloaded_chunk_count: 1..,
                    downloaded_payload_bytes: 1..,
                    ..
                }
            )),
            "a failed batch is never acknowledged"
        );

        let retry_start = failed_trace.len();
        context.proxy.set_get_chunks_fault(CloneProxyFault::None);
        clone.query_row::<i64, _, _>(
            "SELECT dolt_clone(?1)",
            params![context.proxy.database_url()],
            |row| row.get(0),
        )?;
        let retried_trace = trace_snapshot(&context.trace);
        let retry_events = clone_progress_events(&retried_trace[retry_start..]);
        assert_eq!(retry_events.first(), Some(&DoltCloneProgressEvent::Started));
        assert_eq!(
            retry_events.last(),
            Some(&DoltCloneProgressEvent::Completed)
        );
        assert!(
            context.proxy.error().is_none(),
            "proxy error: {:?}",
            context.proxy.error()
        );
    }
    Ok(())
}

#[test]
fn clone_guards_are_per_connection_scoped_and_panic_contained() -> Result<()> {
    let context = TestContext::new(1, 64)?;
    push(&context.source, "main")?;
    let first = Connection::open(context._temporary.path().join("first-guard.db"))?;
    let second = Connection::open(context._temporary.path().join("second-guard.db"))?;
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let first_calls_copy = Arc::clone(&first_calls);
    let _first_guard = first.dolt_clone_progress_callback(move |_| {
        first_calls_copy.fetch_add(1, Ordering::Relaxed);
    })?;
    let nested = first.dolt_clone_progress_callback(|_| {});
    assert!(matches!(
        nested,
        Err(Error::SqliteFailure(code, _)) if code.extended_code == rusqlite::ffi::SQLITE_MISUSE
    ));
    let second_calls_copy = Arc::clone(&second_calls);
    let _second_guard = second.dolt_clone_progress_callback(move |_| {
        second_calls_copy.fetch_add(1, Ordering::Relaxed);
    })?;
    let _push_guard = first.dolt_push_progress_callback(|_| {})?;

    let _: i64 = first.query_row(
        "SELECT dolt_clone(?1)",
        params![context.proxy.database_url()],
        |row| row.get(0),
    )?;
    assert!(first_calls.load(Ordering::Relaxed) > 0);
    assert_eq!(second_calls.load(Ordering::Relaxed), 0);
    let _: i64 = second.query_row(
        "SELECT dolt_clone(?1)",
        params![context.proxy.database_url()],
        |row| row.get(0),
    )?;
    assert!(second_calls.load(Ordering::Relaxed) > 0);

    drop(_first_guard);
    let dropped = Connection::open(context._temporary.path().join("dropped-guard.db"))?;
    let dropped_calls = Arc::new(AtomicUsize::new(0));
    let dropped_calls_copy = Arc::clone(&dropped_calls);
    let guard = dropped.dolt_clone_progress_callback(move |_| {
        dropped_calls_copy.fetch_add(1, Ordering::Relaxed);
    })?;
    drop(guard);
    let _: i64 = dropped.query_row(
        "SELECT dolt_clone(?1)",
        params![context.proxy.database_url()],
        |row| row.get(0),
    )?;
    assert_eq!(dropped_calls.load(Ordering::Relaxed), 0);

    let panicking = Connection::open(context._temporary.path().join("panicking-guard.db"))?;
    let panic_calls = Arc::new(AtomicUsize::new(0));
    let panic_calls_copy = Arc::clone(&panic_calls);
    let _panic_guard = panicking.dolt_clone_progress_callback(move |_| {
        panic_calls_copy.fetch_add(1, Ordering::Relaxed);
        panic!("clone progress callback panic must not unwind through C");
    })?;
    let _: i64 = panicking.query_row(
        "SELECT dolt_clone(?1)",
        params![context.proxy.database_url()],
        |row| row.get(0),
    )?;
    assert!(panic_calls.load(Ordering::Relaxed) > 0);
    Ok(())
}

#[test]
fn observed_file_clone_reports_actual_counters() -> Result<()> {
    let temporary = tempfile::tempdir().expect("should create file clone progress tempdir");
    let source = Connection::open(temporary.path().join("file-source.db"))?;
    source.execute_batch(
        "CREATE TABLE values_for_clone(id INTEGER PRIMARY KEY, value BLOB NOT NULL);
         INSERT INTO values_for_clone VALUES(1, zeroblob(1024));",
    )?;
    let _: i64 = source.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String = source.query_row("SELECT dolt_commit('-m', 'file clone seed')", [], |row| {
        row.get(0)
    })?;
    let remote_path = temporary.path().join("file-remote.db");
    let remote_url = format!("file://{}", remote_path.display());
    let _: i64 = source.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![remote_url],
        |row| row.get(0),
    )?;
    let _: i64 = source.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0))?;

    let clone = Connection::open(temporary.path().join("file-clone.db"))?;
    let events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&events);
    let _guard = clone.dolt_clone_progress_callback(move |event| {
        callback_events
            .lock()
            .expect("should lock file clone progress events")
            .push(event);
    })?;
    let _: i64 = clone.query_row("SELECT dolt_clone(?1)", params![remote_url], |row| {
        row.get(0)
    })?;
    let events = events
        .lock()
        .expect("should read file clone progress events")
        .clone();
    assert_eq!(events.first(), Some(&DoltCloneProgressEvent::Started));
    assert_eq!(events.last(), Some(&DoltCloneProgressEvent::Completed));
    assert!(events.contains(&DoltCloneProgressEvent::Finalizing));
    let progress = events
        .iter()
        .filter_map(|event| match event {
            DoltCloneProgressEvent::Progress {
                downloaded_chunk_count,
                downloaded_payload_bytes,
                reused_chunk_count,
                reused_payload_bytes,
            } => Some((
                *downloaded_chunk_count,
                *downloaded_payload_bytes,
                *reused_chunk_count,
                *reused_payload_bytes,
            )),
            _ => None,
        })
        .next_back()
        .expect("file clone reports transferred chunks");
    assert!(progress.0 > 0);
    assert!(progress.1 > 0);
    let value_length: i64 = clone.query_row(
        "SELECT length(value) FROM values_for_clone WHERE id = 1",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(value_length, 1024);
    Ok(())
}

#[test]
fn finalizing_interrupt_does_not_complete_and_same_guard_retry_reuses_chunks() -> Result<()> {
    let context = TestContext::new(4, 1024)?;
    push(&context.source, "main")?;
    let clone = Connection::open(context._temporary.path().join("finalizing-clone.db"))?;
    let interrupt = clone.get_interrupt_handle();
    let interrupt_once = Arc::new(AtomicBool::new(false));
    let callback_interrupt_once = Arc::clone(&interrupt_once);
    let events = Arc::new(Mutex::new(Vec::new()));
    let callback_events = Arc::clone(&events);
    let _guard = clone.dolt_clone_progress_callback(move |event| {
        callback_events
            .lock()
            .expect("should lock finalization progress events")
            .push(event);
        if event == DoltCloneProgressEvent::Finalizing
            && !callback_interrupt_once.swap(true, Ordering::AcqRel)
        {
            interrupt.interrupt();
        }
    })?;

    let interrupted = clone.query_row::<i64, _, _>(
        "SELECT dolt_clone(?1)",
        params![context.proxy.database_url()],
        |row| row.get(0),
    );
    assert!(
        matches!(
            &interrupted,
            Err(Error::SqliteFailure(code, _)) if code.extended_code == rusqlite::ffi::SQLITE_INTERRUPT
        ),
        "finalizing interruption should fail the clone, got {interrupted:?}"
    );
    let interrupted_events = events
        .lock()
        .expect("should read interrupted clone progress events")
        .clone();
    assert_eq!(
        interrupted_events.first(),
        Some(&DoltCloneProgressEvent::Started)
    );
    assert_eq!(
        interrupted_events.last(),
        Some(&DoltCloneProgressEvent::Finalizing)
    );
    assert!(!interrupted_events.contains(&DoltCloneProgressEvent::Completed));
    let first_progress = interrupted_events
        .iter()
        .filter_map(|event| match event {
            DoltCloneProgressEvent::Progress {
                downloaded_chunk_count,
                downloaded_payload_bytes,
                reused_chunk_count,
                reused_payload_bytes,
            } => Some((
                *downloaded_chunk_count,
                *downloaded_payload_bytes,
                *reused_chunk_count,
                *reused_payload_bytes,
            )),
            _ => None,
        })
        .next_back()
        .expect("interrupted clone reports completed chunk acquisition");
    assert!(first_progress.0 > 0);
    assert!(first_progress.1 > 0);

    let first_download_requests = get_chunks_requests(&trace_snapshot(&context.trace)).len();
    assert!(
        first_download_requests > 0,
        "first attempt fetches graph chunks"
    );
    events
        .lock()
        .expect("should clear interrupted clone progress events")
        .clear();

    clone.query_row::<i64, _, _>(
        "SELECT dolt_clone(?1)",
        params![context.proxy.database_url()],
        |row| row.get(0),
    )?;
    let retry_events = events
        .lock()
        .expect("should read retry clone progress events")
        .clone();
    assert_eq!(retry_events.first(), Some(&DoltCloneProgressEvent::Started));
    assert_eq!(
        retry_events.last(),
        Some(&DoltCloneProgressEvent::Completed)
    );
    assert!(retry_events.contains(&DoltCloneProgressEvent::Finalizing));
    let retry_progress = retry_events
        .iter()
        .filter_map(|event| match event {
            DoltCloneProgressEvent::Progress {
                downloaded_chunk_count,
                downloaded_payload_bytes,
                reused_chunk_count,
                reused_payload_bytes,
            } => Some((
                *downloaded_chunk_count,
                *downloaded_payload_bytes,
                *reused_chunk_count,
                *reused_payload_bytes,
            )),
            _ => None,
        })
        .next_back()
        .expect("retry reports acquired chunks");
    assert_eq!(retry_progress.0, 0, "retry reuses all stored chunks");
    assert_eq!(retry_progress.1, 0);
    assert_eq!(
        retry_progress.2,
        first_progress.0 + first_progress.2,
        "retry reuses every chunk acquired before finalization"
    );
    assert_eq!(
        retry_progress.3,
        first_progress.1 + first_progress.3,
        "retry reuses every payload byte acquired before finalization"
    );
    assert_eq!(
        get_chunks_requests(&trace_snapshot(&context.trace)).len(),
        first_download_requests,
        "retry avoids downloading chunks already committed before finalization"
    );

    let cloned_rows: i64 =
        clone.query_row("SELECT count(*) FROM payloads", [], |row| row.get(0))?;
    assert_eq!(cloned_rows, 4);
    assert!(
        context.proxy.error().is_none(),
        "proxy error: {:?}",
        context.proxy.error()
    );
    Ok(())
}

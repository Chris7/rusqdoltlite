#![cfg(all(feature = "remote", not(target_arch = "wasm32")))]

use std::{
    collections::HashSet,
    io::{self, Read as _, Write as _},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use rusqlite::{params, Connection, DoltPushProgressEvent, Error, RemoteServer, Result};

type Trace = Arc<Mutex<Vec<TraceEntry>>>;

#[derive(Clone, Debug)]
enum TraceEntry {
    Progress(DoltPushProgressEvent),
    Request(CapturedRequest),
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    method: String,
    path: String,
    body: Vec<u8>,
    status: Option<u16>,
}

struct RecordingProxy {
    address: String,
    stopped: Arc<AtomicBool>,
    reject_chunk_request: Arc<AtomicUsize>,
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
        let error = Arc::new(Mutex::new(None));

        let worker_stopped = Arc::clone(&stopped);
        let worker_chunk_requests = Arc::clone(&chunk_requests);
        let worker_reject_chunk_request = Arc::clone(&reject_chunk_request);
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
                    &worker_reject_chunk_request,
                ) {
                    save_proxy_error(&worker_error, format!("request relay failed: {error}"));
                }
            }
        });

        Self {
            address: format!("http://{address}"),
            stopped,
            reject_chunk_request,
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
    reject_chunk_request: &AtomicUsize,
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
    let body = if path.ends_with("/chunks") {
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
        }));
        records.len() - 1
    };

    let is_chunk_request = method == "POST" && path.ends_with("/chunks");
    let chunk_number = if is_chunk_request {
        chunk_requests.fetch_add(1, Ordering::AcqRel) + 1
    } else {
        0
    };
    let should_reject =
        is_chunk_request && chunk_number == reject_chunk_request.load(Ordering::Acquire);
    let response = if should_reject {
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
    let status = response_status(&response)?;
    if let Some(TraceEntry::Request(captured)) = trace
        .lock()
        .expect("should lock proxy trace")
        .get_mut(request_index)
    {
        captured.status = Some(status);
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
            TraceEntry::Request(_) => None,
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
    assert!(retry_requests
        .iter()
        .all(|(_, request)| request.status == Some(200) || request.status == Some(204)));
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

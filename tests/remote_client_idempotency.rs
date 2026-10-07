#![cfg(all(feature = "remote", not(target_arch = "wasm32")))]

use std::{
    io::{self, Read as _, Write as _},
    net::{TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use rusqlite::{params, Connection, RemoteServer, Result};

const CAPABILITY_PREFIX: &str = "/api/repositories/test/push/opaque-capability";

#[derive(Clone, Debug)]
struct CapturedRequest {
    method: String,
    path: String,
    idempotency_token: Option<String>,
    body: Vec<u8>,
}

struct RecordingProxy {
    port: u16,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    stopped: Arc<AtomicBool>,
    fail_refs_if_once: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl RecordingProxy {
    fn start(upstream_port: u16) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind recording proxy");
        listener
            .set_nonblocking(true)
            .expect("set proxy listener nonblocking");
        let port = listener.local_addr().expect("proxy local address").port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stopped = Arc::new(AtomicBool::new(false));
        let busy_refs_if_once = Arc::new(AtomicBool::new(true));
        let fail_refs_if_once = Arc::new(AtomicBool::new(false));
        let thread_requests = Arc::clone(&requests);
        let thread_stopped = Arc::clone(&stopped);
        let thread_busy_refs_if_once = Arc::clone(&busy_refs_if_once);
        let thread_fail_refs_if_once = Arc::clone(&fail_refs_if_once);
        let worker = thread::spawn(move || {
            while !thread_stopped.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut client, _)) => {
                        let _ = client.set_read_timeout(Some(Duration::from_secs(60)));
                        let _ = client.set_write_timeout(Some(Duration::from_secs(60)));
                        let _ = proxy_request(
                            &mut client,
                            upstream_port,
                            &thread_requests,
                            &thread_busy_refs_if_once,
                            &thread_fail_refs_if_once,
                        );
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            port,
            requests,
            stopped,
            fail_refs_if_once,
            worker: Some(worker),
        }
    }

    fn database_url(&self, database: &str) -> String {
        format!(
            "http://127.0.0.1:{}{CAPABILITY_PREFIX}/{database}",
            self.port
        )
    }

    fn snapshot(&self) -> Vec<CapturedRequest> {
        self.requests.lock().expect("proxy request lock").clone()
    }

    fn fail_next_refs_if(&self) {
        self.fail_refs_if_once.store(true, Ordering::Release);
    }
}

impl Drop for RecordingProxy {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn proxy_request(
    client: &mut TcpStream,
    upstream_port: u16,
    captured: &Mutex<Vec<CapturedRequest>>,
    busy_refs_if_once: &AtomicBool,
    fail_refs_if_once: &AtomicBool,
) -> io::Result<()> {
    let request = read_http_request(client)?;
    let header_end = find_header_end(&request).expect("request has HTTP headers");
    let header = std::str::from_utf8(&request[..header_end]).expect("ASCII HTTP headers");
    let mut lines = header.split("\r\n");
    let mut request_line = lines
        .next()
        .expect("HTTP request line")
        .split_ascii_whitespace();
    let method = request_line.next().unwrap_or_default().to_owned();
    let path = request_line.next().unwrap_or_default().to_owned();
    let idempotency_token = header_value(header, "Idempotency-Token").map(str::to_owned);
    captured
        .lock()
        .expect("proxy request lock")
        .push(CapturedRequest {
            method,
            path: path.clone(),
            idempotency_token,
            body: request[header_end + 4..].to_vec(),
        });

    if path.ends_with("/refs-if") && busy_refs_if_once.swap(false, Ordering::AcqRel) {
        let body = br#"{"code":"busy","message":"retry the request"}"#;
        let response = format!(
            "HTTP/1.1 409 Conflict\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            std::str::from_utf8(body).expect("test response is UTF-8")
        );
        client.write_all(&with_connection_close(response.as_bytes())?)?;
        client.flush()?;
        return Ok(());
    }
    if path.ends_with("/refs-if") && fail_refs_if_once.swap(false, Ordering::AcqRel) {
        let body = br#"{"code":"injected_failure","message":"stop before commit"}"#;
        let response = format!(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            std::str::from_utf8(body).expect("test response is UTF-8")
        );
        client.write_all(&with_connection_close(response.as_bytes())?)?;
        client.flush()?;
        return Ok(());
    }

    let upstream_request = rewrite_capability_path(&request)?;
    let mut upstream = TcpStream::connect(("127.0.0.1", upstream_port))?;
    upstream.set_read_timeout(Some(Duration::from_secs(60)))?;
    upstream.set_write_timeout(Some(Duration::from_secs(60)))?;
    upstream.write_all(&with_connection_close(&upstream_request)?)?;
    let response = read_http_response(&mut upstream)?;
    client.write_all(&with_connection_close(&response)?)?;
    client.flush()?;
    Ok(())
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

fn rewrite_capability_path(request: &[u8]) -> io::Result<Vec<u8>> {
    let header_end = find_header_end(request)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP header end"))?;
    let header = std::str::from_utf8(&request[..header_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "HTTP headers are not ASCII"))?;
    let request_line_end = header
        .find("\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request line"))?;
    let request_line = &header[..request_line_end];
    let mut parts = request_line.split_ascii_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP method"))?;
    let path = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing request path"))?;
    let version = parts
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP version"))?;
    let upstream_path = path
        .strip_prefix(CAPABILITY_PREFIX)
        .filter(|suffix| suffix.starts_with('/'))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing capability prefix"))?;

    let mut rewritten = Vec::with_capacity(request.len());
    rewritten.extend_from_slice(format!("{method} {upstream_path} {version}\r\n").as_bytes());
    rewritten.extend_from_slice(header[request_line_end + 2..].as_bytes());
    rewritten.extend_from_slice(b"\r\n\r\n");
    rewritten.extend_from_slice(&request[header_end + 4..]);
    Ok(rewritten)
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
    let mut chunk = [0; 8192];
    let header_end = loop {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed before HTTP request headers",
            ));
        }
        message.extend_from_slice(&chunk[..count]);
        if let Some(end) = find_header_end(&message) {
            break end;
        }
        if message.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP request headers too large",
            ));
        }
    };
    let header = std::str::from_utf8(&message[..header_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "HTTP headers are not ASCII"))?;
    let body_length = content_length(header)?.unwrap_or(0);
    let total_length = header_end + 4 + body_length;
    while message.len() < total_length {
        let remaining = total_length - message.len();
        let read_length = remaining.min(chunk.len());
        let count = stream.read(&mut chunk[..read_length])?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "client closed before HTTP request body",
            ));
        }
        message.extend_from_slice(&chunk[..count]);
    }
    message.truncate(total_length);
    Ok(message)
}

fn read_http_response(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut message = Vec::new();
    let mut chunk = [0; 8192];
    let header_end = loop {
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "upstream closed before HTTP response headers",
            ));
        }
        message.extend_from_slice(&chunk[..count]);
        if let Some(end) = find_header_end(&message) {
            break end;
        }
        if message.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP response headers too large",
            ));
        }
    };
    let header = std::str::from_utf8(&message[..header_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "HTTP headers are not ASCII"))?;
    let body_length = content_length(header)?;
    if let Some(body_length) = body_length {
        let total_length = header_end + 4 + body_length;
        while message.len() < total_length {
            let remaining = total_length - message.len();
            let read_length = remaining.min(chunk.len());
            let count = stream.read(&mut chunk[..read_length])?;
            if count == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "upstream closed before HTTP response body",
                ));
            }
            message.extend_from_slice(&chunk[..count]);
        }
        message.truncate(total_length);
    } else {
        stream.read_to_end(&mut message)?;
    }
    Ok(message)
}

fn with_connection_close(message: &[u8]) -> io::Result<Vec<u8>> {
    let header_end = find_header_end(message)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing HTTP header end"))?;
    let header = std::str::from_utf8(&message[..header_end])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "HTTP headers are not ASCII"))?;
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

fn add_remote(connection: &Connection, url: &str) -> Result<()> {
    let _: i64 = connection.query_row(
        "SELECT dolt_remote('add', 'origin', ?1)",
        params![url],
        |row| row.get(0),
    )?;
    Ok(())
}

fn seed_database(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE widgets(id INTEGER PRIMARY KEY, name TEXT NOT NULL);
         INSERT INTO widgets VALUES(1, 'first');",
    )?;
    let _: i64 = connection.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String =
        connection.query_row("SELECT dolt_commit('-m', 'initial graph')", [], |row| {
            row.get(0)
        })?;
    Ok(())
}

fn add_and_commit(connection: &Connection, id: i64, name: &str) -> Result<()> {
    connection.execute(
        "INSERT INTO widgets(id, name) VALUES (?1, ?2)",
        params![id, name],
    )?;
    let _: i64 = connection.query_row("SELECT dolt_add('-A')", [], |row| row.get(0))?;
    let _: String =
        connection.query_row("SELECT dolt_commit('-m', 'update graph')", [], |row| {
            row.get(0)
        })?;
    Ok(())
}

fn push(connection: &Connection, force: bool) -> Result<()> {
    let sql = if force {
        "SELECT dolt_push(?1, ?2, '--force')"
    } else {
        "SELECT dolt_push(?1, ?2)"
    };
    let _: i64 = connection.query_row(sql, params!["origin", "main"], |row| row.get(0))?;
    Ok(())
}

fn assert_capability_requests(requests: &[CapturedRequest]) {
    assert!(!requests.is_empty(), "the push should make HTTP requests");
    assert!(requests
        .iter()
        .all(|request| request.idempotency_token.is_none()));
    assert!(requests
        .iter()
        .all(|request| request.path.starts_with(CAPABILITY_PREFIX)));
}

#[test]
fn test_push_uses_opaque_capability_url_and_finalizes_noop_without_token_header() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let server_root = temp.path().join("server");
    std::fs::create_dir(&server_root).expect("server directory");
    let server = RemoteServer::start(&server_root)?;
    let proxy = RecordingProxy::start(server.port());
    let remote_url = proxy.database_url("origin.db");
    let source = Connection::open(temp.path().join("source.db"))?;
    seed_database(&source)?;
    add_remote(&source, &remote_url)?;

    let start = proxy.snapshot().len();
    push(&source, false)?;
    let first_push = proxy.snapshot()[start..].to_vec();
    assert_capability_requests(&first_push);
    let refs = first_push
        .iter()
        .find(|request| request.path.ends_with("/refs"))
        .expect("push reads remote refs");
    assert_eq!(refs.method, "GET");
    let chunks = first_push
        .iter()
        .find(|request| request.path.ends_with("/chunks"))
        .expect("first push uploads chunks");
    assert_eq!(chunks.method, "POST");
    let refs_if_attempts = first_push
        .iter()
        .filter(|request| request.path.ends_with("/refs-if"))
        .collect::<Vec<_>>();
    assert_eq!(refs_if_attempts.len(), 2, "refs-if retries once after busy");
    assert_eq!(refs_if_attempts[0].method, "PUT");
    assert_eq!(refs_if_attempts[1].method, "PUT");
    assert_eq!(refs_if_attempts[0].body, refs_if_attempts[1].body);
    assert_eq!(refs_if_attempts[0].path, refs_if_attempts[1].path);
    let commit = first_push
        .iter()
        .find(|request| request.path.ends_with("/commit"))
        .expect("push crosses the commit publication boundary");
    assert_eq!(commit.method, "POST");

    // Repeating the push takes the no-op refs path, which still finalizes the
    // capability-scoped session through /commit.
    let start = proxy.snapshot().len();
    push(&source, false)?;
    let no_op_push = proxy.snapshot()[start..].to_vec();
    assert_capability_requests(&no_op_push);
    let no_op_refs_if = no_op_push
        .iter()
        .find(|request| request.path.ends_with("/refs-if"))
        .expect("no-op push checks refs");
    assert_eq!(no_op_refs_if.method, "PUT");
    let no_op_commit = no_op_push
        .iter()
        .find(|request| request.path.ends_with("/commit"))
        .expect("no-op push still publishes with commit");
    assert_eq!(no_op_commit.method, "POST");

    add_and_commit(&source, 2, "forced update")?;
    let start = proxy.snapshot().len();
    push(&source, true)?;
    let forced_push = proxy.snapshot()[start..].to_vec();
    assert_capability_requests(&forced_push);
    let refs_if = forced_push
        .iter()
        .find(|request| request.path.ends_with("/refs-if"))
        .expect("forced push includes refs-if");
    assert_eq!(refs_if.method, "PUT");
    let branch_length = u16::from_le_bytes([refs_if.body[0], refs_if.body[1]]) as usize;
    assert_eq!(&refs_if.body[2 + branch_length..3 + branch_length], &[1]);

    // A failed no-op refs-if must stop before /commit.
    proxy.fail_next_refs_if();
    let start = proxy.snapshot().len();
    let failed_push = source.query_row::<i64, _, _>(
        "SELECT dolt_push(?1, ?2)",
        params!["origin", "main"],
        |row| row.get(0),
    );
    assert!(failed_push.is_err(), "refs-if error must propagate");
    let failed_requests = proxy.snapshot()[start..].to_vec();
    assert_capability_requests(&failed_requests);
    assert!(failed_requests
        .iter()
        .any(|request| request.path.ends_with("/refs-if")));
    assert!(!failed_requests
        .iter()
        .any(|request| request.path.ends_with("/commit")));

    let clone = Connection::open(temp.path().join("clone.db"))?;
    let start = proxy.snapshot().len();
    let _: i64 = clone.query_row("SELECT dolt_clone(?1)", params![remote_url], |row| {
        row.get(0)
    })?;
    let _: i64 = clone.query_row("SELECT dolt_fetch('origin', 'main')", [], |row| row.get(0))?;
    let _: i64 = clone.query_row("SELECT dolt_pull('origin', 'main')", [], |row| row.get(0))?;
    let read_requests = proxy.snapshot()[start..].to_vec();
    assert_capability_requests(&read_requests);

    Ok(())
}

#[test]
fn test_ordinary_push_preserves_file_remote_behavior() -> Result<()> {
    let temp = tempfile::tempdir().expect("tempdir");
    let source = Connection::open(temp.path().join("source.db"))?;
    seed_database(&source)?;
    let remote_path = temp.path().join("file-remote.db");
    let remote_url = format!("file://{}", remote_path.display());
    add_remote(&source, &remote_url)?;

    push(&source, false)?;

    let source_hash: String =
        source.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    let remote = Connection::open(remote_path)?;
    let remote_hash: String =
        remote.query_row("SELECT dolt_hashof('main')", [], |row| row.get(0))?;
    assert_eq!(remote_hash, source_hash);
    Ok(())
}

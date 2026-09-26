#![cfg(feature = "blockcachevfs")]

#[cfg(feature = "remote")]
use std::fs;
#[cfg(feature = "remote")]
use std::io::{self, Read as _, Write as _};
#[cfg(feature = "remote")]
use std::net::{Shutdown, TcpListener, TcpStream};
#[cfg(all(feature = "remote", unix))]
use std::os::unix::process::ExitStatusExt as _;
#[cfg(feature = "remote")]
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::Path;
#[cfg(feature = "remote")]
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;
#[cfg(feature = "remote")]
use std::sync::{
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    Arc, Barrier, Mutex,
};
#[cfg(feature = "remote")]
use std::thread;
#[cfg(feature = "remote")]
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::blockcachevfs::{
    AttachSpec, BlockCacheVfs, Config, SessionAttachment, SessionOperationId, Storage,
    SESSION_OPERATION_ID_BYTES,
};
#[cfg(feature = "remote")]
use rusqlite::SessionOperationStatus;
use rusqlite::{params, Connection};
#[cfg(feature = "remote")]
use rusqlite::{BlockCacheSessionOptions, RemoteServer, RemoteServerOptions, SessionScope};

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

#[cfg(feature = "remote")]
fn ensure_s3_bucket(endpoint: &str, bucket: &str) {
    let url = format!(
        "{}/{}",
        endpoint.trim_end_matches('/'),
        encode_component(bucket)
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
            "/dev/null",
            "--write-out",
            "%{http_code}",
            "-X",
            "PUT",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(response.status.success(), "S3 bucket request failed: {url}");
    let code = String::from_utf8_lossy(&response.stdout);
    assert!(
        matches!(code.as_ref(), "200" | "201" | "204" | "409"),
        "S3 bucket creation returned {code}: {url}"
    );
}

#[cfg(feature = "remote")]
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

#[cfg(feature = "remote")]
fn fetch_google_object(endpoint: &str, bucket: &str, object: &str) -> Vec<u8> {
    let body = tempfile::NamedTempFile::new().expect("Google object temporary file");
    let url = format!(
        "{}/download/storage/v1/b/{}/o/{}?alt=media",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_component(object)
    );
    let response = Command::new("curl")
        .args([
            "--silent",
            "--show-error",
            "--noproxy",
            "*",
            "--output",
            body.path().to_str().expect("Google object path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(
        response.status.success(),
        "Google object request failed: {url}"
    );
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "Google object request returned {code}: {url}");
    fs::read(body.path()).expect("read Google object body")
}

#[cfg(feature = "remote")]
fn encode_path(value: &str) -> String {
    value
        .split('/')
        .map(encode_component)
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(feature = "remote")]
fn fetch_s3_object(endpoint: &str, bucket: &str, object: &str) -> Vec<u8> {
    let body = tempfile::NamedTempFile::new().expect("S3 object temporary file");
    let url = format!(
        "{}/{}/{}",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_path(object)
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
            body.path().to_str().expect("S3 object path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(response.status.success(), "S3 object request failed: {url}");
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "S3 object request returned {code}: {url}");
    fs::read(body.path()).expect("read S3 object body")
}

#[cfg(feature = "remote")]
fn fetch_session_object(backend: &str, endpoint: &str, bucket: &str, object: &str) -> Vec<u8> {
    match backend {
        "google" => fetch_google_object(endpoint, bucket, object),
        "s3" => fetch_s3_object(endpoint, bucket, object),
        _ => unreachable!("unknown session backend {backend}"),
    }
}

#[cfg(feature = "remote")]
fn list_google_objects(endpoint: &str, bucket: &str, prefix: &str) -> String {
    let body = tempfile::NamedTempFile::new().expect("Google object-list temporary file");
    let url = format!(
        "{}/storage/v1/b/{}/o?prefix={}&maxResults=1000",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_component(prefix)
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
                .expect("Google object-list path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(
        response.status.success(),
        "Google object-list request failed: {url}"
    );
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(
        code, "200",
        "Google object-list request returned {code}: {url}"
    );
    fs::read_to_string(body.path()).expect("read Google object-list response")
}

#[cfg(feature = "remote")]
fn list_s3_objects(endpoint: &str, bucket: &str, prefix: &str) -> String {
    let body = tempfile::NamedTempFile::new().expect("S3 object-list temporary file");
    let url = format!(
        "{}/{}?list-type=2&prefix={}&max-keys=1000",
        endpoint.trim_end_matches('/'),
        encode_component(bucket),
        encode_component(prefix)
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
            body.path().to_str().expect("S3 object-list path is UTF-8"),
            "--write-out",
            "%{http_code}",
            &url,
        ])
        .output()
        .expect("curl must be installed for emulator tests");
    assert!(
        response.status.success(),
        "S3 object-list request failed: {url}"
    );
    let code = String::from_utf8_lossy(&response.stdout);
    assert_eq!(code, "200", "S3 object-list request returned {code}: {url}");
    fs::read_to_string(body.path()).expect("read S3 object-list response")
}

#[cfg(feature = "remote")]
fn list_session_objects(backend: &str, endpoint: &str, bucket: &str, prefix: &str) -> String {
    match backend {
        "google" => list_google_objects(endpoint, bucket, prefix),
        "s3" => list_s3_objects(endpoint, bucket, prefix),
        _ => unreachable!("unknown session backend {backend}"),
    }
}

#[cfg(feature = "remote")]
fn first_google_object_name(listing: &str, prefix: &str) -> Option<String> {
    let marker = format!(r#""name":"{prefix}"#);
    let start = listing.find(&marker)? + r#""name":""#.len();
    let tail = &listing[start..];
    Some(tail[..tail.find('"')?].to_owned())
}

#[cfg(feature = "remote")]
fn be_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("checkpoint integer is in bounds"),
    )
}

#[cfg(feature = "remote")]
fn be_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("checkpoint integer is in bounds"),
    )
}

#[cfg(feature = "remote")]
fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(feature = "remote")]
fn hex_bytes_upper(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

#[cfg(feature = "remote")]
fn deterministic_payload(id: i64, length: usize) -> String {
    const ALPHABET: &[u8; 64] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";
    let mut state = (id as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut payload = format!("row-{id:08}-");
    while payload.len() < length {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(ALPHABET[(state & 63) as usize] as char);
    }
    payload.truncate(length);
    payload
}

/// Reassemble the database named by an immutable checkpoint manifest for
/// content assertions. Native session rehydration from an accepted checkpoint
/// is exercised separately by the fresh-process phase-two flows; this helper
/// inspects the manifest's block mapping and logical file size directly.
#[cfg(feature = "remote")]
fn reassemble_checkpoint_database(
    backend: &str,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    checkpoint_record: &[u8],
    database: &str,
) -> Vec<u8> {
    const RECORD_HEADER_BYTES: usize = 168;
    const RECORD_TRAILER_BYTES: usize = 32;
    const CHECKPOINT_BODY_HEADER_BYTES: usize = 24;
    const MANIFEST_HEADER_BYTES: usize = 24;
    const MANIFEST_DATABASE_HEADER_BYTES: usize = 152;

    assert!(
        checkpoint_record.len() >= RECORD_HEADER_BYTES + RECORD_TRAILER_BYTES,
        "checkpoint record is truncated"
    );
    let body_length = be_u32(checkpoint_record, 160) as usize;
    assert_eq!(
        checkpoint_record.len(),
        RECORD_HEADER_BYTES + body_length + RECORD_TRAILER_BYTES,
        "checkpoint record length is inconsistent"
    );
    let body = &checkpoint_record[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + body_length];
    assert!(
        body.starts_with(b"BCVCP01\0"),
        "checkpoint body has an unexpected format"
    );
    assert!(body.len() >= CHECKPOINT_BODY_HEADER_BYTES);
    let file_size = be_u64(body, 8) as usize;
    let manifest_length = be_u32(body, 16) as usize;
    let etag_length = be_u32(body, 20) as usize;
    assert_eq!(
        body.len(),
        CHECKPOINT_BODY_HEADER_BYTES + etag_length + manifest_length,
        "checkpoint body length is inconsistent"
    );
    let manifest_start = CHECKPOINT_BODY_HEADER_BYTES + etag_length;
    let manifest = &body[manifest_start..manifest_start + manifest_length];
    assert!(manifest.len() >= MANIFEST_HEADER_BYTES);
    assert_eq!(
        be_u32(manifest, 0),
        4,
        "unexpected checkpoint manifest version"
    );
    let block_size = be_u32(manifest, 4) as usize;
    let database_count = be_u32(manifest, 8) as usize;
    let name_size = be_u32(manifest, 16) as usize;
    assert!(block_size > 0);
    assert!((12..=32).contains(&name_size));

    for database_index in 0..database_count {
        let header = MANIFEST_HEADER_BYTES + database_index * MANIFEST_DATABASE_HEADER_BYTES;
        assert!(
            header + MANIFEST_DATABASE_HEADER_BYTES <= manifest.len(),
            "checkpoint database header is truncated"
        );
        let parent_id = be_u32(manifest, header + 4);
        let block_offset = be_u32(manifest, header + 12) as usize;
        let block_count = (be_u32(manifest, header + 16) & 0x7fff_ffff) as usize;
        let entry_count = be_u32(manifest, header + 20) as usize;
        let name_end = header + 24 + name_size.min(128);
        let name = manifest[header + 24..name_end]
            .split(|byte| *byte == 0)
            .next()
            .expect("checkpoint database name")
            .to_vec();
        if name != database.as_bytes() {
            continue;
        }
        assert_eq!(
            parent_id, 0,
            "the session test fixture should use a complete database block list"
        );
        assert_eq!(
            entry_count, 0,
            "a complete database block list must not contain delta entries"
        );
        assert!(
            block_offset
                .checked_add(block_count.checked_mul(name_size).expect("block list size"))
                .is_some_and(|end| end <= manifest.len()),
            "checkpoint database block list is truncated"
        );
        let mut database_bytes = Vec::with_capacity(block_count * block_size);
        for block_index in 0..block_count {
            let start = block_offset + block_index * name_size;
            // CBS block object names are emitted by bcvBlockidToText(), which
            // uses uppercase hexadecimal.  DoltLite chunk routes use their
            // own lowercase hash spelling, so keep that separate in
            // hex_bytes() rather than normalizing all test paths together.
            let block_id = hex_bytes_upper(&manifest[start..start + name_size]);
            let block = fetch_session_object(
                backend,
                endpoint,
                bucket,
                &format!("{prefix}/{block_id}.bcv"),
            );
            assert_eq!(
                block.len(),
                block_size,
                "checkpoint block has an unexpected size"
            );
            database_bytes.extend_from_slice(&block);
        }
        assert!(
            file_size <= database_bytes.len(),
            "checkpoint logical file size exceeds its block mapping"
        );
        database_bytes.truncate(file_size);
        return database_bytes;
    }
    panic!("checkpoint manifest does not contain database {database}");
}

fn shared_cache() -> &'static Path {
    static CACHE: OnceLock<tempfile::TempDir> = OnceLock::new();
    CACHE
        .get_or_init(|| tempfile::tempdir().expect("shared cache directory"))
        .path()
}

#[cfg(feature = "remote")]
fn session_scope(database: &str) -> SessionScope {
    SessionScope::new("emulator-principal", database, "read,write")
        .expect("valid emulator session scope")
}

fn session_operation_id(value: u8) -> SessionOperationId {
    let mut operation_id = [0_u8; SESSION_OPERATION_ID_BYTES];
    operation_id[0] = value;
    SessionOperationId::new(operation_id).expect("valid emulator operation ID")
}

#[cfg(feature = "remote")]
fn session_storage(backend: &str, endpoint: &str, bucket: &str, prefix: &str) -> Storage {
    let container = format!("{bucket}/{prefix}");
    match backend {
        "google" => Storage::google_json_with_endpoint("test-project", container, endpoint),
        "s3" => Storage::s3_with_endpoint("test", container, "us-east-1", endpoint),
        _ => unreachable!("unknown session backend {backend}"),
    }
}

fn attach_scoped_for_test(
    vfs: &'static BlockCacheVfs,
    spec: &AttachSpec,
    session_id: &str,
    operation: u8,
) -> rusqlite::Result<SessionAttachment> {
    let operation_id = session_operation_id(operation);
    vfs.attach_session_scoped(
        spec,
        session_id,
        "emulator-principal",
        "session.sqlite",
        "read,write",
        &operation_id,
    )
}

#[cfg(feature = "remote")]
const HTTP_TEST_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(feature = "remote")]
const SESSION_HTTP_TEST_TIMEOUT: Duration = Duration::from_secs(120);

#[cfg(feature = "remote")]
fn send_http_request(port: u16, request: &[u8]) -> String {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to remote server");
    stream
        .set_read_timeout(Some(HTTP_TEST_TIMEOUT))
        .expect("read timeout");
    stream.write_all(request).expect("write request");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    String::from_utf8_lossy(&response).into_owned()
}

#[cfg(feature = "remote")]
struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

#[cfg(feature = "remote")]
fn send_http_request_bytes(port: u16, method: &str, path: &str, body: &[u8]) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to remote server");
    stream
        .set_read_timeout(Some(HTTP_TEST_TIMEOUT))
        .expect("read timeout");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .expect("write request headers");
    stream.write_all(body).expect("write request body");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP response headers");
    let status = response[..header_end]
        .split(|byte| *byte == b' ')
        .nth(1)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|text| text.parse().ok())
        .expect("HTTP response status");
    HttpResponse {
        status,
        body: response[header_end + 4..].to_vec(),
    }
}

#[cfg(feature = "remote")]
fn send_http_request_raw(port: u16, request: &[u8]) -> Vec<u8> {
    send_http_request_raw_with_timeout(port, request, HTTP_TEST_TIMEOUT)
}

#[cfg(feature = "remote")]
fn send_http_request_raw_with_timeout(port: u16, request: &[u8], timeout: Duration) -> Vec<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to remote server");
    stream
        .set_read_timeout(Some(timeout))
        .expect("read timeout");
    stream.write_all(request).expect("write request");
    let mut response = Vec::new();
    stream.read_to_end(&mut response).expect("read response");
    response
}

#[cfg(feature = "remote")]
fn http_status(response: &[u8]) -> u16 {
    response
        .split(|byte| *byte == b' ')
        .nth(1)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|text| text.parse().ok())
        .expect("HTTP response status")
}

#[cfg(feature = "remote")]
fn read_http_request(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut request = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        if stream.read_exact(&mut byte).is_err() {
            return None;
        }
        request.push(byte[0]);
        if request.ends_with(b"\r\n\r\n") {
            break;
        }
        assert!(
            request.len() <= 128 * 1024,
            "HTTP request headers are bounded"
        );
    }
    let header_end = request.len();
    let header_text = String::from_utf8_lossy(&request[..header_end]);
    let content_length = header_text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
        })
        .flatten()
        .unwrap_or(0);
    if content_length > 16 * 1024 * 1024 {
        return None;
    }
    let mut body = vec![0_u8; content_length];
    if stream.read_exact(&mut body).is_err() {
        return None;
    }
    request.extend_from_slice(&body);
    Some(request)
}

#[cfg(feature = "remote")]
fn request_parts(request: &[u8]) -> (String, String, Vec<u8>) {
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP request headers");
    let header = String::from_utf8_lossy(&request[..header_end]);
    let mut request_line = header
        .lines()
        .next()
        .expect("HTTP request line")
        .split_whitespace();
    let method = request_line.next().expect("HTTP request method").to_owned();
    let path = request_line.next().expect("HTTP request path").to_owned();
    (method, path, request[header_end + 4..].to_vec())
}

#[cfg(feature = "remote")]
fn request_route(path: &str) -> &str {
    path.trim_start_matches('/')
        .split_once('/')
        .and_then(|(_, endpoint)| endpoint.split('/').next())
        .and_then(|endpoint| endpoint.split('?').next())
        .unwrap_or("")
}

#[cfg(feature = "remote")]
fn chunk_batch_stats(body: &[u8]) -> String {
    let mut offset = 0;
    let mut count = 0_usize;
    let mut largest = 0_usize;
    while offset < body.len() {
        if body.len() - offset < 24 {
            return format!("malformed_at={offset} chunks={count}");
        }
        let size = u32::from_le_bytes(
            body[offset + 20..offset + 24]
                .try_into()
                .expect("chunk length field"),
        ) as usize;
        offset += 24;
        let Some(end) = offset.checked_add(size) else {
            return format!("length_overflow_at={offset} chunks={count}");
        };
        if end > body.len() {
            return format!("truncated_at={offset} size={size} chunks={count}");
        }
        largest = largest.max(size);
        count += 1;
        offset = end;
    }
    format!("chunks={count} largest_chunk_bytes={largest}")
}

#[cfg(feature = "remote")]
fn scoped_session_request_path(path: &str, session_token: &str, database: &str) -> Option<String> {
    let (path_only, query) = path
        .split_once('?')
        .map_or((path, None), |(path, query)| (path, Some(query)));
    let prefix = format!("/s/{session_token}/{database}");
    let suffix = path_only.strip_prefix(&prefix)?;
    if !suffix.is_empty() && !suffix.starts_with('/') {
        return None;
    }
    let mut native_path = format!("/{database}{suffix}");
    if let Some(query) = query {
        native_path.push('?');
        native_path.push_str(query);
    }
    Some(native_path)
}

#[cfg(feature = "remote")]
fn rewrite_http_request_path(request: &[u8], path: &str) -> Vec<u8> {
    let line_end = request
        .windows(2)
        .position(|window| window == b"\r\n")
        .expect("HTTP request line terminator");
    let line = &request[..line_end];
    let first_space = line.iter().position(|byte| *byte == b' ');
    let first_space = first_space.expect("HTTP request method separator");
    let path_end = line[first_space + 1..]
        .iter()
        .position(|byte| *byte == b' ')
        .map(|offset| first_space + 1 + offset)
        .expect("HTTP request path separator");
    let mut rewritten = Vec::with_capacity(request.len() + path.len());
    rewritten.extend_from_slice(&request[..first_space + 1]);
    rewritten.extend_from_slice(path.as_bytes());
    rewritten.extend_from_slice(&request[path_end..]);
    rewritten
}

#[cfg(feature = "remote")]
#[test]
fn session_proxy_requires_and_strips_opaque_token() {
    assert_eq!(
        scoped_session_request_path(
            "/s/opaque-token/session.sqlite/commit?retry=1",
            "opaque-token",
            "session.sqlite",
        ),
        Some("/session.sqlite/commit?retry=1".to_owned())
    );
    assert_eq!(
        scoped_session_request_path("/session.sqlite/commit", "opaque-token", "session.sqlite",),
        None,
        "a URL without the session token must not attach"
    );
    assert_eq!(
        scoped_session_request_path(
            "/s/other-token/session.sqlite/commit",
            "opaque-token",
            "session.sqlite",
        ),
        None,
        "a URL with a foreign session token must not attach"
    );
    assert_eq!(
        scoped_session_request_path(
            "/s/opaque-token/other.sqlite/commit",
            "opaque-token",
            "session.sqlite",
        ),
        None,
        "a URL for another database must not attach"
    );
    let rewritten = rewrite_http_request_path(
        b"POST /s/opaque-token/session.sqlite/commit HTTP/1.1\r\nHost: test\r\n\r\nbody",
        "/session.sqlite/commit",
    );
    assert!(rewritten.starts_with(b"POST /session.sqlite/commit HTTP/1.1\r\n"));
    assert!(rewritten.ends_with(b"\r\n\r\nbody"));
}

#[cfg(feature = "remote")]
struct ReferenceResponseAdapter<'a> {
    server: &'a mut RemoteServer,
    response: Option<Vec<u8>>,
}

#[cfg(feature = "remote")]
fn should_publish_reference_response(route: &str, status: u16) -> bool {
    route == "/commit" && (200..300).contains(&status)
}

#[cfg(feature = "remote")]
fn should_invoke_mutating_handler(status: SessionOperationStatus) -> bool {
    matches!(status, SessionOperationStatus::New)
}

#[cfg(feature = "remote")]
impl<'a> ReferenceResponseAdapter<'a> {
    fn new(server: &'a mut RemoteServer) -> Self {
        Self {
            server,
            response: None,
        }
    }

    fn buffer(&mut self, response: impl Into<Vec<u8>>) {
        assert!(
            self.response.replace(response.into()).is_none(),
            "reference adapter accepts one complete response"
        );
    }

    fn release_after_completion_staged(
        mut self,
        route: &str,
        status: u16,
    ) -> Result<Vec<u8>, (&'static str, rusqlite::Error)> {
        let response = self.response.take().ok_or_else(|| {
            (
                "buffer",
                rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                    Some("reference adapter has no buffered response".to_owned()),
                ),
            )
        })?;
        if !(200..300).contains(&status) {
            // A failed application handler must not create a checkpoint or
            // advance the accepted operation head. Stop the native workers
            // and let session attachment recovery discard the unpublished
            // overlay on the next request.
            self.server.quiesce().map_err(|error| ("quiesce", error))?;
            return Ok(response);
        }
        if should_publish_reference_response(route, status) {
            // `/commit` is an application close-out decision. Upload owns
            // quiescing, the final checkpoint, and publication; the adapter
            // must not publish an intermediate complete_request first.
            self.server.upload().map_err(|error| ("upload", error))?;
        } else {
            // Other successful responses only make their accepted checkpoint
            // available to the next invocation; they never publish HEAD.
            self.server
                .complete_request()
                .map_err(|error| ("complete_request", error))?;
        }
        Ok(response)
    }

    fn release_after_completion(self, route: &str, status: u16) -> rusqlite::Result<Vec<u8>> {
        self.release_after_completion_staged(route, status)
            .map_err(|(_, error)| error)
    }
}

#[cfg(feature = "remote")]
#[test]
fn reference_adapter_route_selection_is_application_only() {
    assert!(should_publish_reference_response("/commit", 200));
    assert!(should_publish_reference_response("/commit", 204));
    assert!(!should_publish_reference_response("/commit", 500));
    assert!(!should_publish_reference_response("/chunks", 200));
    assert!(!should_publish_reference_response("/refs-if", 204));
    assert!(!should_invoke_mutating_handler(
        SessionOperationStatus::Accepted
    ));
    assert!(!should_invoke_mutating_handler(
        SessionOperationStatus::Committed
    ));
    assert!(should_invoke_mutating_handler(SessionOperationStatus::New));
    assert!(!should_invoke_mutating_handler(
        SessionOperationStatus::Failed
    ));
    assert!(!should_invoke_mutating_handler(
        SessionOperationStatus::Conflict
    ));
    assert_eq!(
        proxy_operation_id(1, "POST", "/session.sqlite/commit", b"commit"),
        proxy_operation_id(2, "POST", "/session.sqlite/commit", b"commit"),
        "mutating retries must retain one operation fence"
    );
    assert_ne!(
        proxy_operation_id(1, "GET", "/session.sqlite/root", &[]),
        proxy_operation_id(2, "GET", "/session.sqlite/root", &[]),
        "independent read requests need distinct application operation IDs"
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

    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("VFS builder")
        .config(Config::CacheSize(4 * 1024 * 1024))
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
    vfs.cleanup(&storage, std::time::Duration::from_secs(300))
        .expect("scheduled cleanup should use the independent CBS management API");
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
    let alias = format!("bootstrap_{backend}_{}", std::process::id());
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
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&second_alias))
        .expect("re-attach created database");
    let path = format!("/{second_alias}/bootstrap.sqlite");
    let db = vfs.open(&path).expect("re-open created database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM bootstrap", [], |row| row.get(0))
        .expect("read uploaded database");
    assert_eq!(count, 2);
    drop(db);

    vfs.delete_database(&second_alias, "bootstrap.sqlite")
        .expect("delete database locally");
    vfs.upload(&second_alias).expect("upload database deletion");
    vfs.detach(&second_alias)
        .expect("detach after database deletion");

    vfs.create_database(&storage, &local_path, "bootstrap.sqlite")
        .expect("re-create deleted database");
    let third_alias = format!("{alias}_replacement");
    vfs.attach(&AttachSpec::new(storage).alias(&third_alias))
        .expect("attach replacement database");
    let path = format!("/{third_alias}/bootstrap.sqlite");
    let db = vfs.open(&path).expect("open replacement database");
    let count: i64 = db
        .query_row("SELECT count(*) FROM bootstrap", [], |row| row.get(0))
        .expect("read replacement database");
    assert_eq!(count, 1, "replacement database must not retain old rows");
    let value: String = db
        .query_row("SELECT value FROM bootstrap", [], |row| row.get(0))
        .expect("read replacement database value");
    assert_eq!(value, "ok");
    drop(db);
    vfs.detach(&third_alias)
        .expect("detach replacement database");
}

fn run_session_ownership() {
    let suffix = unique_suffix();
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    let prefix = format!("{suffix}/session");
    ensure_google_bucket(&endpoint, bucket);
    let container = format!("{bucket}/{prefix}");
    let storage = Storage::google_json_with_endpoint("test-project", &container, &endpoint);

    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("VFS builder")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
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
        .execute_batch(
            "CREATE TABLE session_data(value TEXT); INSERT INTO session_data VALUES ('ok');",
        )
        .expect("seed local SQLite database");
    local.close().expect("close local SQLite database");
    vfs.create_database(&storage, &local_path, "session.sqlite")
        .expect("upload initial database");
    #[cfg(feature = "remote")]
    let baseline_manifest =
        fetch_google_object(&endpoint, bucket, &format!("{prefix}/manifest.bcv"));

    let spec = AttachSpec::new(storage);
    let first = attach_scoped_for_test(vfs, &spec, "550e8400-e29b-41d4-a716-446655440010", 10)
        .expect("attach first session owner");
    let alias = first.alias().to_owned();
    assert_eq!(alias, "session-550e8400-e29b-41d4-a716-446655440010");

    let active_client_spec = spec.clone().alias("active-client-session-alias");
    let active_client_owner = attach_scoped_for_test(
        vfs,
        &active_client_spec,
        "550e8400-e29b-41d4-a716-446655440014",
        20,
    )
    .expect("attach active-client session owner");
    let active_client_db = vfs
        .open("/active-client-session-alias/session.sqlite")
        .expect("open active-client session database");
    // The owner release loses the detach race while the SQLite client is
    // still open. A second constructor must not reuse or clear that alias.
    drop(active_client_owner);
    let active_client_reuse = attach_scoped_for_test(
        vfs,
        &active_client_spec,
        "550e8400-e29b-41d4-a716-446655440014",
        21,
    )
    .expect_err("an active native client must block same-session reuse");
    assert!(matches!(
        active_client_reuse,
        rusqlite::Error::SqliteFailure(error, _)
            if error.extended_code == rusqlite::ffi::SQLITE_BUSY
    ));
    drop(active_client_db);
    let active_client_recovered = attach_scoped_for_test(
        vfs,
        &active_client_spec,
        "550e8400-e29b-41d4-a716-446655440014",
        22,
    )
    .expect("idle session alias can be reused after its client closes");
    drop(active_client_recovered);

    let explicit_spec = spec.clone().alias("shared-session-alias");
    let explicit_owner = attach_scoped_for_test(
        vfs,
        &explicit_spec,
        "550e8400-e29b-41d4-a716-446655440010",
        11,
    )
    .expect("attach explicit session alias");
    let explicit_db = vfs
        .open("/shared-session-alias/session.sqlite")
        .expect("open explicit session database");
    explicit_db
        .execute_batch(
            "PRAGMA synchronous=OFF; INSERT INTO session_data VALUES ('explicit-dirty');",
        )
        .expect("write explicit session database");
    let _: String = explicit_db
        .query_row(
            "SELECT dolt_commit('-A', '-m', 'explicit-dirty')",
            [],
            |row| row.get(0),
        )
        .expect("commit explicit session database update with DoltLite");
    let explicit_dirty_rows: i64 = explicit_db
        .query_row(
            "SELECT COUNT(*) FROM session_data WHERE value = 'explicit-dirty'",
            [],
            |row| row.get(0),
        )
        .expect("query explicit session database update");
    assert_eq!(explicit_dirty_rows, 1);
    drop(explicit_db);
    #[cfg(feature = "remote")]
    assert_eq!(
        baseline_manifest,
        fetch_google_object(&endpoint, bucket, &format!("{prefix}/manifest.bcv")),
        "the DoltLite commit must leave the ordinary manifest unpublished"
    );
    // Preserve alias metadata with a zero active owner. The DoltLite commit
    // leaves the local overlay unaccepted, so the following attempts exercise
    // persisted identity mismatch, not merely live-owner exclusion.
    drop(explicit_owner);
    let wrong_session = attach_scoped_for_test(
        vfs,
        &explicit_spec,
        "550e8400-e29b-41d4-a716-446655440011",
        12,
    )
    .expect_err("a different session must not reuse an explicit alias");
    assert!(matches!(
        wrong_session,
        rusqlite::Error::SqliteFailure(error, _)
            if error.extended_code == rusqlite::ffi::SQLITE_BUSY
    ));
    let wrong_storage = Storage::google_json_with_endpoint(
        "test-project",
        format!("{bucket}/{prefix}-different"),
        &endpoint,
    );
    let wrong_scope = attach_scoped_for_test(
        vfs,
        &AttachSpec::new(wrong_storage).alias("shared-session-alias"),
        "550e8400-e29b-41d4-a716-446655440010",
        13,
    )
    .expect_err("a different storage scope must not reuse an explicit alias");
    assert!(matches!(
        wrong_scope,
        rusqlite::Error::SqliteFailure(error, _)
            if error.extended_code == rusqlite::ffi::SQLITE_BUSY
    ));
    let rotated_account = Storage::google_json_with_endpoint(
        "rotated-project",
        format!("{bucket}/{prefix}"),
        &endpoint,
    );
    let rotated_scope = attach_scoped_for_test(
        vfs,
        &AttachSpec::new(rotated_account).alias("shared-session-alias"),
        "550e8400-e29b-41d4-a716-446655440010",
        14,
    )
    .expect_err("a changed provider account must not reuse a persisted attachment identity");
    assert!(matches!(
        rotated_scope,
        rusqlite::Error::SqliteFailure(error, _)
            if error.extended_code == rusqlite::ffi::SQLITE_BUSY
    ));
    // Rehydration replaces the detached cache's unaccepted generations with
    // the accepted remote base. The explicit-dirty row must not survive this
    // same-session recovery.
    let explicit_recovery = attach_scoped_for_test(
        vfs,
        &explicit_spec,
        "550e8400-e29b-41d4-a716-446655440010",
        15,
    )
    .expect("same-session reuse rehydrates the detached alias");
    let recovered_db = vfs
        .open("/shared-session-alias/session.sqlite")
        .expect("open rehydrated session database");
    let dirty_rows: i64 = recovered_db
        .query_row(
            "SELECT COUNT(*) FROM session_data WHERE value = 'explicit-dirty'",
            [],
            |row| row.get(0),
        )
        .expect("query rehydrated session database");
    assert_eq!(dirty_rows, 0, "unaccepted local writes must be discarded");
    drop(recovered_db);
    drop(explicit_recovery);
    #[cfg(feature = "remote")]
    {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("reserve server port");
        let port = listener.local_addr().expect("reserved port address").port();
        let failed_session = BlockCacheSessionOptions::new(
            vfs,
            spec.clone(),
            "550e8400-e29b-41d4-a716-446655440011",
            session_scope("session.sqlite"),
            session_operation_id(1),
        )
        .expect("construct failed-start session payload");
        let failed_start = RemoteServer::start_with_options(
            "/session-550e8400-e29b-41d4-a716-446655440011",
            &RemoteServerOptions::new()
                .bind_address("127.0.0.1")
                .port(port)
                .blockcache_session(failed_session),
        );
        assert!(failed_start.is_err(), "reserved port must fail startup");
        let cleaned_attachment =
            attach_scoped_for_test(vfs, &spec, "550e8400-e29b-41d4-a716-446655440011", 16)
                .expect("failed startup must release its attachment");
        drop(cleaned_attachment);
        drop(listener);
    }

    let live_conflict =
        attach_scoped_for_test(vfs, &spec, "550e8400-e29b-41d4-a716-446655440010", 17)
            .expect_err("a second live owner must be rejected");
    assert!(matches!(
        live_conflict,
        rusqlite::Error::SqliteFailure(error, _)
            if error.extended_code == rusqlite::ffi::SQLITE_BUSY
    ));

    let db = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("open session database");
    db.execute_batch("PRAGMA synchronous=OFF; INSERT INTO session_data VALUES ('dirty');")
        .expect("write session database");
    drop(db);
    // Drop cannot detach a dirty container. The native session release leaves
    // a zero-owner metadata record; the next same-session attach rehydrates
    // the accepted remote base and discards the unpublished overlay.
    drop(first);

    let recovered = attach_scoped_for_test(vfs, &spec, "550e8400-e29b-41d4-a716-446655440010", 18)
        .expect("same-session recovery rehydrates the detached alias");
    let recovered_db = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("open rehydrated default session database");
    let dirty_rows: i64 = recovered_db
        .query_row(
            "SELECT COUNT(*) FROM session_data WHERE value = 'dirty'",
            [],
            |row| row.get(0),
        )
        .expect("query rehydrated default session database");
    assert_eq!(dirty_rows, 0, "unaccepted local writes must be discarded");
    drop(recovered_db);
    drop(recovered);

    let independent =
        attach_scoped_for_test(vfs, &spec, "550e8400-e29b-41d4-a716-446655440012", 19)
            .expect("different session gets an isolated alias");
    assert_ne!(alias, independent.alias());
    drop(independent);
}

#[cfg(feature = "remote")]
fn run_session_server() {
    let suffix = unique_suffix();
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    let prefix = format!("{suffix}/server");
    ensure_google_bucket(&endpoint, bucket);
    let container = format!("{bucket}/{prefix}");
    let storage = Storage::google_json_with_endpoint("test-project", &container, &endpoint);

    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("VFS builder")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
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
        .execute_batch(
            "CREATE TABLE session_server(value TEXT); INSERT INTO session_server VALUES ('ok');",
        )
        .expect("seed local SQLite database");
    local.close().expect("close local SQLite database");
    vfs.create_database(&storage, &local_path, "session.sqlite")
        .expect("upload initial database");
    let baseline_manifest =
        fetch_google_object(&endpoint, bucket, &format!("{prefix}/manifest.bcv"));

    let alias = "session-server-alias";
    let session_id = "550e8400-e29b-41d4-a716-446655440013";
    let session = BlockCacheSessionOptions::new(
        vfs,
        AttachSpec::new(storage).alias(alias),
        session_id,
        session_scope("session.sqlite"),
        session_operation_id(2),
    )
    .expect("construct session server payload");
    let mut server = RemoteServer::start_with_options(
        format!("/{alias}"),
        &RemoteServerOptions::new().blockcache_session(session),
    )
    .expect("start session-owned remote server");

    let response = send_http_request(
        server.port(),
        b"GET /session.sqlite/root HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
    );
    assert!(
        response.starts_with("HTTP/1.1 200 OK\r\n"),
        "native request should resolve the attached alias: {response}"
    );
    // The native server intentionally retains an idle read ChunkStore in its
    // per-server cache.  This fixture performs the no-xSync write through the
    // owning VFS after the read response, so close that request cache before
    // opening the writer.  A real application request would write through
    // the same native handler rather than mix the two access paths.
    server
        .quiesce()
        .expect("quiesce completed read before direct VFS fixture write");
    // Add a local write with synchronous=OFF after the handler response. This
    // is the generic storage-level fixture; the HTTP mutation path is covered
    // separately by run_session_http_flow. The close-out operation must still
    // perform the final checkpoint. Use DoltLite's own commit path (rather
    // than a generic SQLite transaction).
    // The row write is made with synchronous=OFF, then DoltLite commits the
    // resulting working tree with its normal durability settings; close-out
    // itself must not depend on another xSync callback.
    let no_sync_db = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("open session database for no-xSync write");
    // DoltLite's block-cache VFS deliberately rejects journal-mode changes;
    // in particular, a request cannot silently switch this fixture to WAL.
    // Keep this assertion explicit instead of treating the rejection as a
    // reason to weaken the no-WAL coverage.
    let wal_error = no_sync_db
        .query_row::<String, _, _>("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .expect_err("bcvfs must reject a WAL journal-mode request");
    assert!(
        matches!(
            &wal_error,
            rusqlite::Error::SqliteFailure(_, Some(message))
                if message.contains("cannot use \"PRAGMA journal_mode\" with bcvfs")
        ),
        "unexpected journal-mode result: {wal_error:?}"
    );
    no_sync_db
        .execute_batch(
            "PRAGMA synchronous=OFF;
             INSERT INTO session_server VALUES ('no-xsync');",
        )
        .expect("write session database without xSync");
    let _: String = no_sync_db
        .query_row("SELECT dolt_commit('-A', '-m', 'no-xsync')", [], |row| {
            row.get(0)
        })
        .expect("commit DoltLite no-xSync update");
    let changed_rows: i64 = no_sync_db
        .query_row(
            "SELECT COUNT(*) FROM session_server WHERE value = 'no-xsync'",
            [],
            |row| row.get(0),
        )
        .expect("query DoltLite no-xSync update");
    assert_eq!(
        changed_rows, 1,
        "the DoltLite update must be visible locally"
    );
    drop(no_sync_db);
    let reopened_db = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("reopen session database after no-xSync write");
    let reopened_rows: i64 = reopened_db
        .query_row(
            "SELECT COUNT(*) FROM session_server WHERE value = 'no-xsync'",
            [],
            |row| row.get(0),
        )
        .expect("query reopened session database after no-xSync write");
    assert_eq!(
        reopened_rows, 1,
        "the no-xSync row must survive reopening through the VFS"
    );
    drop(reopened_db);
    let mut adapter = ReferenceResponseAdapter::new(&mut server);
    adapter.buffer(response.into_bytes());
    // The native request above is a valid read fixture. The first application
    // response completes an intermediate request but intentionally does not
    // publish the ordinary manifest. No native DoltLite route is renamed or
    // intercepted by this reference adapter.
    adapter
        .release_after_completion("/chunks", 200)
        .expect("successful no-xSync intermediate completion");
    let after_checkpoint_manifest =
        fetch_google_object(&endpoint, bucket, &format!("{prefix}/manifest.bcv"));
    assert_eq!(
        baseline_manifest, after_checkpoint_manifest,
        "intermediate completion must not publish the ordinary manifest"
    );
    drop(server);

    // A fresh server instance reconciles the same opaque operation before the
    // application's final close-out decision. The no-argument upload is the
    // only publication call.
    let mut server =
        start_http_session_server(vfs, &endpoint, bucket, &prefix, alias, session_id, 2);
    assert_eq!(
        server.operation_status().expect("accepted no-xSync status"),
        SessionOperationStatus::Accepted
    );
    let mut final_adapter = ReferenceResponseAdapter::new(&mut server);
    final_adapter.buffer(b"commit response");
    final_adapter
        .release_after_completion("/commit", 200)
        .expect("successful no-xSync close-out must checkpoint and publish");
    let checkpoint_prefix = format!("{prefix}/bcv-session/v1/checkpoint/{session_id}/");
    let checkpoint_listing = list_google_objects(&endpoint, bucket, &checkpoint_prefix);
    let checkpoint_name = first_google_object_name(&checkpoint_listing, &checkpoint_prefix)
        .expect("successful no-xSync completion must persist a checkpoint object");
    let checkpoint_record = fetch_google_object(&endpoint, bucket, &checkpoint_name);
    assert!(
        !checkpoint_record.is_empty(),
        "the durable checkpoint record must be readable"
    );
    let checkpoint_database = reassemble_checkpoint_database(
        "google",
        &endpoint,
        bucket,
        &prefix,
        &checkpoint_record,
        "session.sqlite",
    );
    let checkpoint_file =
        tempfile::NamedTempFile::new().expect("checkpoint database temporary file");
    let mut checkpoint_file_handle = checkpoint_file
        .reopen()
        .expect("reopen checkpoint database temporary file");
    checkpoint_file_handle
        .write_all(&checkpoint_database)
        .expect("write rehydrated checkpoint database");
    checkpoint_file_handle
        .flush()
        .expect("flush rehydrated checkpoint database");
    let rehydrated = Connection::open(checkpoint_file.path())
        .expect("open database rehydrated from checkpoint blocks");
    let rehydrated_rows: i64 = rehydrated
        .query_row(
            "SELECT COUNT(*) FROM session_server WHERE value = 'no-xsync'",
            [],
            |row| row.get(0),
        )
        .expect("query database rehydrated from checkpoint blocks");
    assert_eq!(
        rehydrated_rows, 1,
        "checkpoint rehydration must contain the DoltLite no-xSync row"
    );
    drop(rehydrated);
    let after_manifest = fetch_google_object(&endpoint, bucket, &format!("{prefix}/manifest.bcv"));
    assert_ne!(
        baseline_manifest, after_manifest,
        "explicit upload must publish the no-xSync manifest"
    );
    drop(server);
    let committed_retry =
        start_http_session_server(vfs, &endpoint, bucket, &prefix, alias, session_id, 2);
    let committed_status = committed_retry
        .operation_status()
        .expect("committed operation status");
    assert_eq!(committed_status, SessionOperationStatus::Committed);
    assert!(!should_invoke_mutating_handler(committed_status));
    drop(committed_retry);
    let stale_storage =
        Storage::google_json_with_endpoint("test-project", format!("{bucket}/{prefix}"), &endpoint);
    let stale_result = try_start_http_session_server_with_storage(
        vfs,
        stale_storage,
        alias,
        session_id,
        session_operation_id(1),
    );
    match stale_result {
        Ok(stale) => {
            let status = stale.operation_status().expect("stale operation status");
            assert_eq!(status, SessionOperationStatus::Conflict);
            assert!(!should_invoke_mutating_handler(status));
            panic!("stale operation should be rejected during server startup");
        }
        Err(rusqlite::Error::SqliteFailure(error, _)) => assert_eq!(
            error.extended_code,
            rusqlite::ffi::SQLITE_CONSTRAINT,
            "stale operation should fail with a startup constraint"
        ),
        Err(error) => panic!("stale operation returned an unexpected error: {error:?}"),
    }
}

#[cfg(feature = "remote")]
fn start_http_session_server(
    vfs: &'static BlockCacheVfs,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    alias: &str,
    session_id: &str,
    operation: u8,
) -> RemoteServer {
    start_http_session_server_with_id(
        vfs,
        endpoint,
        bucket,
        prefix,
        alias,
        session_id,
        session_operation_id(operation),
    )
}

#[cfg(feature = "remote")]
fn start_http_session_server_with_id(
    vfs: &'static BlockCacheVfs,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    alias: &str,
    session_id: &str,
    operation_id: SessionOperationId,
) -> RemoteServer {
    let storage =
        Storage::google_json_with_endpoint("test-project", format!("{bucket}/{prefix}"), endpoint);
    start_http_session_server_with_storage(vfs, storage, alias, session_id, operation_id)
}

#[cfg(feature = "remote")]
fn start_http_session_server_with_storage(
    vfs: &'static BlockCacheVfs,
    storage: Storage,
    alias: &str,
    session_id: &str,
    operation_id: SessionOperationId,
) -> RemoteServer {
    try_start_http_session_server_with_storage(vfs, storage, alias, session_id, operation_id)
        .expect("start HTTP session server")
}

#[cfg(feature = "remote")]
fn try_start_http_session_server_with_storage(
    vfs: &'static BlockCacheVfs,
    storage: Storage,
    alias: &str,
    session_id: &str,
    operation_id: SessionOperationId,
) -> rusqlite::Result<RemoteServer> {
    let session = BlockCacheSessionOptions::new(
        vfs,
        AttachSpec::new(storage).alias(alias),
        session_id,
        session_scope("session.sqlite"),
        operation_id,
    )?;
    RemoteServer::start_with_options(
        format!("/{alias}"),
        &RemoteServerOptions::new()
            .request_timeout(SESSION_HTTP_TEST_TIMEOUT)
            .blockcache_session(session),
    )
}

#[cfg(feature = "remote")]
fn proxy_operation_id(sequence: u64, method: &str, path: &str, body: &[u8]) -> SessionOperationId {
    // This deterministic mixer is a test-adapter surrogate only. It is not a
    // cryptographic digest and must not be used as a production idempotency
    // token; a production adapter should derive a collision-resistant ID from
    // the exact request bytes (or another stable app-side equivalent) and
    // preserve that ID across retries.
    // Read-only protocol calls are separate application invocations in this
    // reference proxy and therefore include the deterministic request
    // sequence. Mutating calls intentionally omit it: an exact retry of
    // /chunks, /refs-if, or /commit must recover the same operation head.
    let route = request_route(path);
    let read_only = matches!(
        route,
        "root" | "refs" | "chunk" | "has-chunks" | "get-chunks"
    );
    let operation_seed = if read_only { sequence } else { 0 };
    let mut lanes = [
        0x243f_6a88_85a3_08d3_u64 ^ operation_seed,
        0x1319_8a2e_0370_7344_u64.wrapping_add(operation_seed.rotate_left(7)),
        0xa409_3822_299f_31d0_u64 ^ operation_seed.rotate_left(17),
        0x082e_fa98_ec4e_6c89_u64.wrapping_add(operation_seed.rotate_left(31)),
    ];
    for (index, byte) in method
        .bytes()
        .chain(std::iter::once(0))
        .chain(path.bytes())
        .chain(std::iter::once(0))
        .chain(body.iter().copied())
        .enumerate()
    {
        let lane = index % lanes.len();
        lanes[lane] = lanes[lane]
            .wrapping_mul(0x1000_0000_01b3)
            .wrapping_add(u64::from(byte) + 0x9e37_79b9 + index as u64);
    }
    let mut bytes = [0_u8; SESSION_OPERATION_ID_BYTES];
    for (index, lane) in lanes.into_iter().enumerate() {
        bytes[index * 8..(index + 1) * 8].copy_from_slice(&lane.to_le_bytes());
    }
    SessionOperationId::new(bytes).expect("proxy operation ID is non-zero")
}

#[cfg(feature = "remote")]
fn empty_http_response(status: u16) -> Vec<u8> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        409 => "Conflict",
        _ => "Error",
    };
    format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .into_bytes()
}

#[cfg(feature = "remote")]
struct SessionHttpProxy {
    port: u16,
    session_token: String,
    stop: Arc<AtomicBool>,
    last_commit_request: Arc<Mutex<Option<Vec<u8>>>>,
    failure: Arc<Mutex<Option<String>>>,
    last_native_error: Arc<Mutex<Option<String>>>,
    request_log: Arc<Mutex<Vec<String>>>,
    join: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "remote")]
impl SessionHttpProxy {
    fn start(
        vfs: &'static BlockCacheVfs,
        storage: Storage,
        database: &str,
        alias: &str,
        session_id: &str,
    ) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind stable test proxy");
        listener
            .set_nonblocking(true)
            .expect("configure stable test proxy");
        let port = listener.local_addr().expect("stable proxy address").port();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = Arc::clone(&stop);
        let storage = storage.clone();
        let database = database.to_owned();
        let alias = alias.to_owned();
        let session_id = session_id.to_owned();
        let session_token = format!("token-{}", unique_suffix());
        let session_token_thread = session_token.clone();
        let last_commit_request = Arc::new(Mutex::new(None));
        let last_commit_request_thread = Arc::clone(&last_commit_request);
        let failure = Arc::new(Mutex::new(None));
        let failure_thread = Arc::clone(&failure);
        let last_native_error = Arc::new(Mutex::new(None));
        let last_native_error_thread = Arc::clone(&last_native_error);
        let request_log = Arc::new(Mutex::new(Vec::new()));
        let request_log_thread = Arc::clone(&request_log);
        let join = thread::spawn(move || {
            let mut sequence = 1_u64;
            while !stop_thread.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _peer)) => {
                        stream
                            .set_read_timeout(Some(HTTP_TEST_TIMEOUT))
                            .expect("proxy request timeout");
                        let request = match read_http_request(&mut stream) {
                            Some(request) => request,
                            None => continue,
                        };
                        let (method, path, body) = request_parts(&request);
                        let Some(native_path) =
                            scoped_session_request_path(&path, &session_token_thread, &database)
                        else {
                            // Database selection is an application-level routing
                            // boundary. Do not attach the session or let the native
                            // server resolve a request with a missing or foreign
                            // session token.
                            stream
                                .write_all(&empty_http_response(404))
                                .expect("write out-of-scope session response");
                            continue;
                        };
                        if request_route(&native_path) == "commit" {
                            *last_commit_request_thread
                                .lock()
                                .expect("lock last commit request") = Some(request.clone());
                        }
                        let operation_id =
                            proxy_operation_id(sequence, &method, &native_path, &body);
                        sequence = sequence.wrapping_add(1).max(1);
                        let mut server = match try_start_http_session_server_with_storage(
                            vfs,
                            storage.clone(),
                            &alias,
                            &session_id,
                            operation_id,
                        ) {
                            Ok(server) => server,
                            Err(error) => {
                                let message = format!(
                                    "start request route={} sequence={} failed: {error:?}",
                                    request_route(&native_path),
                                    sequence - 1
                                );
                                if let Ok(mut failure) = failure_thread.lock() {
                                    if failure.is_none() {
                                        *failure = Some(message);
                                    }
                                }
                                stream
                                    .write_all(&empty_http_response(500))
                                    .expect("write proxy start failure response");
                                continue;
                            }
                        };
                        let status = match server.operation_status() {
                            Ok(status) => status,
                            Err(error) => {
                                let message = format!(
                                    "status request route={} sequence={} failed: {error:?}",
                                    request_route(&native_path),
                                    sequence - 1
                                );
                                if let Ok(mut failure) = failure_thread.lock() {
                                    if failure.is_none() {
                                        *failure = Some(message);
                                    }
                                }
                                let _ = server.quiesce();
                                stream
                                    .write_all(&empty_http_response(500))
                                    .expect("write proxy status failure response");
                                continue;
                            }
                        };
                        let route = format!("/{}", request_route(&native_path));
                        if let Ok(mut log) = request_log_thread.lock() {
                            log.push(format!(
                                "sequence={} method={method} route={route} body_bytes={}{}",
                                sequence - 1,
                                body.len(),
                                if route == "/chunks" {
                                    format!(" {}", chunk_batch_stats(&body))
                                } else {
                                    String::new()
                                }
                            ));
                        }
                        let response = if should_invoke_mutating_handler(status) {
                            // A New operation is the only status that may invoke the
                            // native mutating handler. The application adapter owns
                            // the route decision and performs completion before the
                            // response is released to the client.
                            let native_request = rewrite_http_request_path(&request, &native_path);
                            let response = send_http_request_raw_with_timeout(
                                server.port(),
                                &native_request,
                                SESSION_HTTP_TEST_TIMEOUT,
                            );
                            let response_status = http_status(&response);
                            if response_status >= 400 {
                                let response_text = String::from_utf8_lossy(&response);
                                let diagnostic = format!(
                                    "route={route} sequence={} request_body_bytes={} status={response_status} response={}",
                                    sequence - 1,
                                    body.len(),
                                    response_text.chars().take(1024).collect::<String>()
                                );
                                if let Ok(mut last_error) = last_native_error_thread.lock() {
                                    *last_error = Some(diagnostic);
                                }
                            }
                            let mut adapter = ReferenceResponseAdapter::new(&mut server);
                            adapter.buffer(response);
                            match adapter.release_after_completion_staged(&route, response_status) {
                                Ok(response) => response,
                                Err((stage, error)) => {
                                    let message = format!(
                                        "{stage} failed for route={route} sequence={} operation_status={status:?} response_status={response_status}: {error:?}",
                                        sequence - 1
                                    );
                                    if let Ok(mut failure) = failure_thread.lock() {
                                        if failure.is_none() {
                                            *failure = Some(message);
                                        }
                                    }
                                    empty_http_response(500)
                                }
                            }
                        } else if status == SessionOperationStatus::Committed && route == "/commit"
                        {
                            // An exact retry after the response was lost has already
                            // committed this operation. Reconcile it without replaying
                            // the native mutating route or publishing a second head.
                            server.quiesce().expect("quiesce committed retry");
                            empty_http_response(200)
                        } else if status == SessionOperationStatus::Accepted && route == "/commit" {
                            // The prior invocation accepted/checkpointed its writes,
                            // but the application may have lost the final response
                            // before publication. The application adapter can safely
                            // finish the explicit /commit close-out without replaying
                            // the mutating handler.
                            let mut adapter = ReferenceResponseAdapter::new(&mut server);
                            adapter.buffer(empty_http_response(200));
                            match adapter.release_after_completion_staged("/commit", 200) {
                                Ok(response) => response,
                                Err((stage, error)) => {
                                    let message = format!(
                                        "{stage} failed while reconciling accepted /commit sequence={}: {error:?}",
                                        sequence - 1
                                    );
                                    if let Ok(mut failure) = failure_thread.lock() {
                                        if failure.is_none() {
                                            *failure = Some(message);
                                        }
                                    }
                                    empty_http_response(500)
                                }
                            }
                        } else {
                            // Failed, stale, or already accepted non-final operations
                            // fail closed. In particular, do not blindly replay a
                            // rejected handler merely because the request arrived at
                            // a fresh server instance.
                            server.quiesce().expect("quiesce rejected operation");
                            empty_http_response(409)
                        };
                        stream.write_all(&response).expect("write proxied response");
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("stable test proxy accept failed: {error}"),
                }
            }
        });
        Self {
            port,
            session_token,
            stop,
            last_commit_request,
            failure,
            last_native_error,
            request_log,
            join: Some(join),
        }
    }

    fn database_url(&self, database: &str) -> String {
        format!(
            "http://127.0.0.1:{}/s/{}/{}",
            self.port, self.session_token, database
        )
    }

    fn request_path(&self, path: &str) -> String {
        format!("/s/{}/{}", self.session_token, path.trim_start_matches('/'))
    }

    fn last_commit_request(&self) -> Vec<u8> {
        self.last_commit_request
            .lock()
            .expect("lock last commit request")
            .clone()
            .expect("push must send a /commit request")
    }

    fn failure(&self) -> Option<String> {
        self.failure.lock().expect("lock proxy failure").clone()
    }

    fn last_native_error(&self) -> Option<String> {
        self.last_native_error
            .lock()
            .expect("lock last native response error")
            .clone()
    }

    fn request_log(&self) -> Vec<String> {
        self.request_log
            .lock()
            .expect("lock proxy request log")
            .clone()
    }
}

#[cfg(feature = "remote")]
impl Drop for SessionHttpProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(join) = self.join.take() {
            if let Err(panic) = join.join() {
                if let Ok(mut failure) = self.failure.lock() {
                    if failure.is_none() {
                        *failure = Some(format!("proxy thread panic: {panic:?}"));
                    }
                }
            }
        }
    }
}

/// Storage-level fault seam used by the session failure matrix below.
///
/// The native CBS Tcl socket controls are part of the standalone Tcl test
/// binary and are not linked into rusqlite.  This proxy therefore injects the
/// same failures at the actual libcurl HTTP boundary used by the Rust VFS. It
/// matches object names after URL decoding, so the test exercises both the
/// Google JSON and S3 providers rather than a mocked storage implementation.
#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageFaultTarget {
    BlockPut,
    XSyncCheckpoint,
    ResponseGateCheckpoint,
    SessionHeadCas,
    PublicationHeadCas,
    ManifestPut,
    FinalHeadCas,
}

#[cfg(feature = "remote")]
impl StorageFaultTarget {
    fn code(self) -> u8 {
        match self {
            Self::BlockPut => 1,
            Self::XSyncCheckpoint => 2,
            Self::ResponseGateCheckpoint => 3,
            Self::SessionHeadCas => 4,
            Self::PublicationHeadCas => 5,
            Self::ManifestPut => 6,
            Self::FinalHeadCas => 7,
        }
    }
}

#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StorageFaultAction {
    Reject,
    DropResponse,
    KillProcess,
}

#[cfg(feature = "remote")]
impl StorageFaultAction {
    fn code(self) -> u8 {
        match self {
            Self::Reject => 1,
            Self::DropResponse => 2,
            Self::KillProcess => 3,
        }
    }
}

#[cfg(feature = "remote")]
struct StorageFaultProxy {
    port: u16,
    stop: Arc<AtomicBool>,
    armed: Arc<AtomicBool>,
    target: Arc<AtomicU8>,
    action: Arc<AtomicU8>,
    matched: Arc<AtomicUsize>,
    faulted: Arc<AtomicUsize>,
    kill_pid: Arc<AtomicUsize>,
    thread: Option<thread::JoinHandle<()>>,
    url: String,
}

#[cfg(feature = "remote")]
impl StorageFaultProxy {
    fn start(endpoint: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind storage fault proxy");
        listener
            .set_nonblocking(true)
            .expect("configure storage fault proxy");
        let address = listener.local_addr().expect("storage fault proxy address");
        let url = format!("http://{address}");
        let authority = endpoint
            .trim_end_matches('/')
            .strip_prefix("http://")
            .or_else(|| endpoint.trim_end_matches('/').strip_prefix("https://"))
            .and_then(|value| value.split('/').next())
            .expect("storage endpoint must have an HTTP authority")
            .to_owned();
        let stop = Arc::new(AtomicBool::new(false));
        let armed = Arc::new(AtomicBool::new(false));
        let target = Arc::new(AtomicU8::new(0));
        let action = Arc::new(AtomicU8::new(0));
        let matched = Arc::new(AtomicUsize::new(0));
        let faulted = Arc::new(AtomicUsize::new(0));
        let kill_pid = Arc::new(AtomicUsize::new(0));
        let thread_stop = Arc::clone(&stop);
        let thread_armed = Arc::clone(&armed);
        let thread_target = Arc::clone(&target);
        let thread_action = Arc::clone(&action);
        let thread_matched = Arc::clone(&matched);
        let thread_faulted = Arc::clone(&faulted);
        let thread_kill_pid = Arc::clone(&kill_pid);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Acquire) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(_) => break,
                };
                let result = handle_storage_fault_connection(
                    &mut stream,
                    &authority,
                    StorageFaultControls {
                        armed: &thread_armed,
                        target: &thread_target,
                        action: &thread_action,
                        matched: &thread_matched,
                        faulted: &thread_faulted,
                        kill_pid: &thread_kill_pid,
                    },
                );
                if let Err(error) = result {
                    let body = format!("storage fault proxy error: {error}");
                    let response = format!(
                        "HTTP/1.1 502 Bad Gateway\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(), body
                    );
                    let _ = stream.write_all(response.as_bytes());
                }
            }
        });
        Self {
            port: address.port(),
            stop,
            armed,
            target,
            action,
            matched,
            faulted,
            kill_pid,
            thread: Some(thread),
            url,
        }
    }

    fn configure(&self, target: StorageFaultTarget, action: StorageFaultAction) {
        self.armed.store(false, Ordering::Release);
        self.target.store(target.code(), Ordering::Release);
        self.action.store(action.code(), Ordering::Release);
        self.matched.store(0, Ordering::Release);
        self.faulted.store(0, Ordering::Release);
        self.kill_pid.store(0, Ordering::Release);
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
        self.kill_pid.store(0, Ordering::Release);
    }

    fn arm_kill_process(&self, target: StorageFaultTarget, pid: u32) {
        self.configure(target, StorageFaultAction::KillProcess);
        self.kill_pid.store(pid as usize, Ordering::Release);
        self.arm();
    }

    fn matched(&self) -> usize {
        self.matched.load(Ordering::Acquire)
    }

    fn faulted(&self) -> usize {
        self.faulted.load(Ordering::Acquire)
    }
}

#[cfg(feature = "remote")]
impl Drop for StorageFaultProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(feature = "remote")]
struct StorageFaultControls<'a> {
    armed: &'a AtomicBool,
    target: &'a AtomicU8,
    action: &'a AtomicU8,
    matched: &'a AtomicUsize,
    faulted: &'a AtomicUsize,
    kill_pid: &'a AtomicUsize,
}

#[cfg(feature = "remote")]
fn handle_storage_fault_connection(
    stream: &mut TcpStream,
    authority: &str,
    controls: StorageFaultControls<'_>,
) -> io::Result<()> {
    stream.set_read_timeout(Some(HTTP_TEST_TIMEOUT))?;
    stream.set_write_timeout(Some(HTTP_TEST_TIMEOUT))?;
    let (method, request_target, headers, body) = read_storage_proxy_request(stream)?;
    let request_fault = storage_fault_target(&method, &request_target, &body);
    let configured_target = controls.target.load(Ordering::Acquire);
    let should_fault = request_fault.is_some_and(|fault| {
        let target_matches = fault.code() == configured_target
            || (fault == StorageFaultTarget::XSyncCheckpoint
                && configured_target == StorageFaultTarget::ResponseGateCheckpoint.code());
        target_matches && controls.armed.load(Ordering::Acquire)
    });
    if should_fault {
        controls.matched.fetch_add(1, Ordering::AcqRel);
    }
    let fault_now = should_fault
        && controls
            .faulted
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
    if fault_now
        && controls.action.load(Ordering::Acquire) == StorageFaultAction::KillProcess.code()
    {
        forward_storage_proxy_request(authority, &method, &request_target, &headers, &body)?;
        let pid = controls.kill_pid.load(Ordering::Acquire);
        if pid > 0 {
            let pid = pid.to_string();
            let _ = Command::new("kill").args(["-KILL", &pid]).status();
        }
        let _ = stream.shutdown(Shutdown::Both);
        return Ok(());
    }
    if fault_now && controls.action.load(Ordering::Acquire) == StorageFaultAction::Reject.code() {
        stream.write_all(
            b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )?;
        return Ok(());
    }

    let response =
        forward_storage_proxy_request(authority, &method, &request_target, &headers, &body)?;
    if !(fault_now
        && controls.action.load(Ordering::Acquire) == StorageFaultAction::DropResponse.code())
    {
        stream.write_all(&response)?;
    }
    if fault_now {
        let _ = stream.shutdown(Shutdown::Both);
    }
    Ok(())
}

#[cfg(feature = "remote")]
type StorageProxyRequest = (String, String, Vec<(String, String)>, Vec<u8>);

#[cfg(feature = "remote")]
fn read_storage_proxy_request(stream: &mut TcpStream) -> io::Result<StorageProxyRequest> {
    let mut request = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 8192];
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "storage proxy client closed before request headers",
            ));
        }
        request.extend_from_slice(&chunk[..count]);
        if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        if request.len() > 128 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "storage proxy request headers are too large",
            ));
        }
    };
    let header_text = String::from_utf8_lossy(&request[..header_end]);
    let mut lines = header_text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "storage proxy request line"))?;
    let mut request_line = request_line.split_whitespace();
    let method = request_line
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "storage proxy method"))?
        .to_owned();
    let target = request_line
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "storage proxy target"))?
        .to_owned();
    let version = request_line
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "storage proxy HTTP version"))?;
    if version != "HTTP/1.1" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "storage proxy only supports HTTP/1.1",
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
            io::Error::new(io::ErrorKind::InvalidData, "storage proxy malformed header")
        })?;
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "storage proxy invalid content length",
                )
            })?;
        }
        if name.eq_ignore_ascii_case("expect") && value.trim().eq_ignore_ascii_case("100-continue")
        {
            expect_continue = true;
        }
        headers.push((name.to_owned(), value.trim().to_owned()));
    }
    if content_length > 32 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "storage proxy request body is too large",
        ));
    }
    if expect_continue {
        stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n")?;
    }
    let mut body = request[header_end..].to_vec();
    while body.len() < content_length {
        let old_len = body.len();
        let mut chunk = [0_u8; 16 * 1024];
        let count = stream.read(&mut chunk)?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "storage proxy client closed before request body",
            ));
        }
        body.extend_from_slice(&chunk[..count]);
        if body.len() == old_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "storage proxy made no progress reading request body",
            ));
        }
    }
    body.truncate(content_length);
    Ok((method, target, headers, body))
}

#[cfg(feature = "remote")]
fn forward_storage_proxy_request(
    authority: &str,
    method: &str,
    target: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> io::Result<Vec<u8>> {
    let origin_target = if let Some(rest) = target.strip_prefix("http://") {
        rest.find('/').map_or("/", |position| &rest[position..])
    } else if let Some(rest) = target.strip_prefix("https://") {
        rest.find('/').map_or("/", |position| &rest[position..])
    } else {
        target
    };
    let mut upstream = TcpStream::connect(authority)?;
    upstream.set_read_timeout(Some(HTTP_TEST_TIMEOUT))?;
    upstream.set_write_timeout(Some(HTTP_TEST_TIMEOUT))?;
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
    upstream.write_all(body)?;
    let mut response = Vec::new();
    upstream.read_to_end(&mut response)?;
    Ok(response)
}

#[cfg(feature = "remote")]
fn storage_request_path(target: &str) -> String {
    let path = target
        .split_once("://")
        .and_then(|(_, rest)| rest.find('/').map(|index| &rest[index..]))
        .unwrap_or(target);
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    percent_decode_path(path)
}

#[cfg(feature = "remote")]
fn google_upload_object(target: &str) -> Option<String> {
    let query = target.split_once('?')?.1;
    query.split('&').find_map(|parameter| {
        let (name, value) = parameter.split_once('=')?;
        name.eq_ignore_ascii_case("name")
            .then(|| percent_decode_path(value))
    })
}

#[cfg(feature = "remote")]
fn percent_decode_path(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut decoded = String::with_capacity(path.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let high = char::from(bytes[index + 1]).to_digit(16);
            let low = char::from(bytes[index + 2]).to_digit(16);
            if let (Some(high), Some(low)) = (high, low) {
                decoded.push(char::from((high * 16 + low) as u8));
                index += 3;
                continue;
            }
        }
        decoded.push(bytes[index] as char);
        index += 1;
    }
    decoded
}

#[cfg(feature = "remote")]
fn storage_fault_target(method: &str, path: &str, body: &[u8]) -> Option<StorageFaultTarget> {
    let request_path = storage_request_path(path);
    let object_path =
        if method.eq_ignore_ascii_case("POST") && request_path.contains("/upload/storage/v1/") {
            google_upload_object(path)?
        } else if method.eq_ignore_ascii_case("PUT") {
            request_path
        } else {
            return None;
        };
    if object_path.ends_with("/manifest.bcv") || object_path == "manifest.bcv" {
        return Some(StorageFaultTarget::ManifestPut);
    }
    if object_path.contains("/bcv-session/v1/checkpoint/") {
        return Some(StorageFaultTarget::XSyncCheckpoint);
    }
    if object_path.contains("/bcv-session/v1/head/") {
        return match body.get(11).copied() {
            Some(4) => Some(StorageFaultTarget::FinalHeadCas),
            _ => Some(StorageFaultTarget::SessionHeadCas),
        };
    }
    if object_path.contains("/bcv-session/v1/publication/") {
        return Some(StorageFaultTarget::PublicationHeadCas);
    }
    if object_path.contains("/bcv-session/v1/") {
        return None;
    }
    object_path
        .ends_with(".bcv")
        .then_some(StorageFaultTarget::BlockPut)
}

#[cfg(feature = "remote")]
#[test]
fn storage_fault_target_classifies_google_and_s3_uploads() {
    let google_checkpoint =
        "http://proxy/upload/storage/v1/b/bucket/o?uploadType=media&name=prefix%2Fbcv-session%2Fv1%2Fcheckpoint%2Fsession%2Fhash.bcv";
    assert_eq!(
        storage_fault_target("POST", google_checkpoint, &[]),
        Some(StorageFaultTarget::XSyncCheckpoint)
    );
    let google_manifest =
        "/upload/storage/v1/b/bucket/o?uploadType=media&name=prefix%2Fmanifest.bcv";
    assert_eq!(
        storage_fault_target("POST", google_manifest, &[]),
        Some(StorageFaultTarget::ManifestPut)
    );

    let mut final_head = vec![0_u8; 12];
    final_head[11] = 4;
    assert_eq!(
        storage_fault_target(
            "PUT",
            "/bucket/prefix/bcv-session/v1/head/session.bcv",
            &final_head
        ),
        Some(StorageFaultTarget::FinalHeadCas)
    );
    assert_eq!(
        storage_fault_target("PUT", "/bucket/prefix/ABCD.bcv", &[]),
        Some(StorageFaultTarget::BlockPut)
    );
}

#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug)]
enum SessionFaultWindow {
    BlockPut,
    XSyncCheckpoint,
    ResponseGateCheckpoint,
    SessionHeadCas,
    PublicationHeadCas,
    ManifestPut,
    FinalHeadCas,
    AmbiguousBlockPut,
    AmbiguousManifestPut,
}

#[cfg(feature = "remote")]
struct SessionFaultContext<'a> {
    backend: &'a str,
    endpoint: &'a str,
    storage_endpoint: &'a str,
    bucket: &'a str,
    base_prefix: &'a str,
    vfs: &'static BlockCacheVfs,
    proxy: &'a StorageFaultProxy,
}

#[cfg(feature = "remote")]
impl SessionFaultWindow {
    fn target(self) -> StorageFaultTarget {
        match self {
            Self::BlockPut | Self::AmbiguousBlockPut => StorageFaultTarget::BlockPut,
            Self::XSyncCheckpoint => StorageFaultTarget::XSyncCheckpoint,
            Self::ResponseGateCheckpoint => StorageFaultTarget::ResponseGateCheckpoint,
            Self::SessionHeadCas => StorageFaultTarget::SessionHeadCas,
            Self::PublicationHeadCas => StorageFaultTarget::PublicationHeadCas,
            Self::ManifestPut => StorageFaultTarget::ManifestPut,
            Self::FinalHeadCas => StorageFaultTarget::FinalHeadCas,
            Self::AmbiguousManifestPut => StorageFaultTarget::ManifestPut,
        }
    }

    fn action(self) -> StorageFaultAction {
        match self {
            Self::AmbiguousBlockPut | Self::AmbiguousManifestPut => {
                StorageFaultAction::DropResponse
            }
            _ => StorageFaultAction::Reject,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::BlockPut => "block-put-before-head",
            Self::XSyncCheckpoint => "xsync-checkpoint",
            Self::ResponseGateCheckpoint => "response-gate-checkpoint",
            Self::SessionHeadCas => "session-head-cas",
            Self::PublicationHeadCas => "publication-head-cas",
            Self::ManifestPut => "manifest-put",
            Self::FinalHeadCas => "final-head-cas",
            Self::AmbiguousBlockPut => "ambiguous-block-put",
            Self::AmbiguousManifestPut => "ambiguous-manifest-put",
        }
    }
}

#[cfg(feature = "remote")]
fn stage_session_update(database: &Connection) -> rusqlite::Result<()> {
    database.execute_batch(
        "PRAGMA synchronous=OFF;
         PRAGMA cache_size=16;
         BEGIN IMMEDIATE;",
    )?;
    let result = (|| {
        let mut insert = database.prepare("INSERT INTO fault_data(id, value) VALUES (?1, ?2)")?;
        for id in 2..=5_500_i64 {
            insert.execute(params![id, deterministic_payload(id, 2_048)])?;
        }
        database.execute_batch("COMMIT")
    })();
    if result.is_err() {
        let _ = database.execute_batch("ROLLBACK");
    }
    result
}

#[cfg(feature = "remote")]
fn stage_session_small_update(database: &Connection, synchronous: &str) -> rusqlite::Result<()> {
    database.execute_batch(&format!(
        "PRAGMA synchronous={synchronous};
         BEGIN IMMEDIATE;
         INSERT INTO fault_data(id, value) VALUES (2, 'xsync-window');"
    ))?;
    Ok(())
}

#[cfg(feature = "remote")]
fn run_session_fault_case(
    context: &SessionFaultContext<'_>,
    window: SessionFaultWindow,
    index: usize,
) {
    let SessionFaultContext {
        backend,
        endpoint,
        storage_endpoint,
        bucket,
        base_prefix,
        vfs,
        proxy,
    } = context;
    let prefix = format!("{base_prefix}/{}", window.label());
    let storage = session_storage(backend, storage_endpoint, bucket, &prefix);
    vfs.initialize_container(&storage)
        .unwrap_or_else(|error| panic!("initialize {} storage: {error:?}", window.label()));

    let seed_dir = tempfile::tempdir().expect("fault-window seed directory");
    let seed_path = seed_dir.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create fault-window seed database");
    seed.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE fault_data(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO fault_data VALUES (1, 'seed');",
    )
    .expect("seed fault-window database");
    seed.close().expect("close fault-window seed database");
    vfs.create_database(&storage, &seed_path, "session.sqlite")
        .unwrap_or_else(|error| panic!("upload {} seed database: {error:?}", window.label()));
    let baseline =
        fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"));

    let run_id = context
        .base_prefix
        .split('/')
        .next()
        .and_then(|prefix| prefix.rsplit('-').next())
        .unwrap_or("run");
    let alias = format!(
        "fault_{}_{}_{}_{}",
        context.backend,
        window.label().replace('-', "_"),
        index,
        run_id
    );
    let session_id = format!("550e8400-e29b-41d4-a716-{:012x}", index + 1);
    let operation = session_operation_id((index + 1) as u8);
    let mut server =
        start_http_session_server_with_storage(vfs, storage, &alias, &session_id, operation);
    proxy.configure(window.target(), window.action());

    match window {
        SessionFaultWindow::BlockPut | SessionFaultWindow::AmbiguousBlockPut => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open block fault-window database");
            proxy.arm();
            let result = stage_session_update(&database);
            assert!(
                proxy.faulted() > 0,
                "{} did not intercept a block PUT (matched={})",
                window.label(),
                proxy.matched()
            );
            if matches!(window, SessionFaultWindow::BlockPut) {
                assert!(result.is_err(), "a rejected block PUT must reach SQLite");
            }
            drop(database);
            server
                .quiesce()
                .unwrap_or_else(|error| panic!("quiesce {} server: {error:?}", window.label()));
            assert_eq!(
                baseline,
                fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv")),
                "{} must not publish before an accepted session head",
                window.label()
            );
        }
        SessionFaultWindow::XSyncCheckpoint => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open xSync fault-window database");
            stage_session_small_update(&database, "FULL")
                .expect("stage xSync fault-window transaction");
            proxy.arm();
            let result = database.execute_batch("COMMIT");
            assert!(
                result.is_err(),
                "a rejected xSync checkpoint must fail the committing writer"
            );
            assert!(
                proxy.faulted() > 0,
                "xSync checkpoint PUT was not intercepted (matched={})",
                proxy.matched()
            );
            let _ = database.execute_batch("ROLLBACK");
            drop(database);
            server.quiesce().expect("quiesce xSync fault server");
            assert_eq!(
                baseline,
                fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"))
            );
        }
        SessionFaultWindow::ResponseGateCheckpoint => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open response-gate fault-window database");
            stage_session_small_update(&database, "OFF")
                .expect("stage response-gate fault-window transaction");
            database
                .execute_batch("COMMIT")
                .expect("synchronous-off transaction must finish before response gate");
            drop(database);
            proxy.arm();
            let result = server.complete_request();
            assert!(
                result.is_err(),
                "response-gate checkpoint rejection must surface"
            );
            assert!(
                proxy.faulted() > 0,
                "response-gate checkpoint PUT was not intercepted (matched={})",
                proxy.matched()
            );
            assert_eq!(
                baseline,
                fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"))
            );
        }
        SessionFaultWindow::SessionHeadCas => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open session-head fault-window database");
            stage_session_small_update(&database, "OFF")
                .expect("stage session-head fault-window transaction");
            database
                .execute_batch("COMMIT")
                .expect("session-head transaction must finish before CAS");
            drop(database);
            proxy.arm();
            let result = server.complete_request();
            assert!(result.is_err(), "session-head CAS rejection must surface");
            assert!(
                proxy.faulted() > 0,
                "session-head CAS PUT was not intercepted (matched={})",
                proxy.matched()
            );
            assert_eq!(
                baseline,
                fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"))
            );
        }
        SessionFaultWindow::PublicationHeadCas
        | SessionFaultWindow::ManifestPut
        | SessionFaultWindow::FinalHeadCas
        | SessionFaultWindow::AmbiguousManifestPut => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open publication fault-window database");
            stage_session_small_update(&database, "OFF")
                .expect("stage publication fault-window transaction");
            database
                .execute_batch("COMMIT")
                .expect("publication transaction must finish before upload");
            drop(database);
            proxy.arm();
            // Model the application's `/commit` close-out. The public
            // upload operation owns its final checkpoint and publication;
            // an intermediate complete_request would change the failure
            // window and incorrectly accept an unpublished head first.
            let first = server.upload();
            assert!(
                proxy.faulted() > 0,
                "{} fault was not intercepted (matched={})",
                window.label(),
                proxy.matched()
            );
            match window {
                SessionFaultWindow::AmbiguousManifestPut => {
                    if first.is_err() {
                        server.upload().unwrap_or_else(|error| {
                            panic!("retry ambiguous manifest upload: {error:?}")
                        });
                    }
                    let after = fetch_session_object(
                        backend,
                        endpoint,
                        bucket,
                        &format!("{prefix}/manifest.bcv"),
                    );
                    assert_ne!(
                        baseline, after,
                        "ambiguous manifest PUT must be recoverable"
                    );
                }
                SessionFaultWindow::FinalHeadCas => {
                    assert!(first.is_err(), "rejected final head CAS must surface");
                    server
                        .upload()
                        .unwrap_or_else(|error| panic!("retry final head CAS: {error:?}"));
                    let after = fetch_session_object(
                        backend,
                        endpoint,
                        bucket,
                        &format!("{prefix}/manifest.bcv"),
                    );
                    assert_ne!(baseline, after, "final-head retry must retain publication");
                    let head = fetch_session_object(
                        backend,
                        endpoint,
                        bucket,
                        &format!("{prefix}/bcv-session/v1/head/{session_id}.bcv"),
                    );
                    assert_eq!(head.get(11).copied(), Some(4));
                }
                _ => {
                    assert!(first.is_err(), "{} rejection must surface", window.label());
                    assert_eq!(
                        baseline,
                        fetch_session_object(
                            backend,
                            endpoint,
                            bucket,
                            &format!("{prefix}/manifest.bcv")
                        ),
                        "{} must not publish before its fault is retried",
                        window.label()
                    );
                    server.upload().unwrap_or_else(|error| {
                        panic!("retry {} publication: {error:?}", window.label())
                    });
                    assert_ne!(
                        baseline,
                        fetch_session_object(
                            backend,
                            endpoint,
                            bucket,
                            &format!("{prefix}/manifest.bcv")
                        )
                    );
                }
            }
        }
    }
}

#[cfg(feature = "remote")]
fn run_session_fault_matrix(backend: &str) {
    let suffix = unique_suffix();
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
            .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
            .unwrap_or_else(|_| "http://127.0.0.1:19025".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown session backend {backend}"),
    };
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rust-session-fault-{suffix}")
    };
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    } else {
        ensure_s3_bucket(&endpoint, &bucket);
    }
    let proxy = StorageFaultProxy::start(&endpoint);
    let vfs_name = format!("rusqdoltlite-fault-{suffix}");
    // Builder::init returns a process-global VFS. Keep its cache root alive
    // across the GCS and S3 matrices, which run sequentially in this process.
    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("fault VFS builder")
        .name(&vfs_name)
        .expect("fault VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .config(Config::RequestCount(1))
        .config(Config::StageWatermark(50))
        .config(Config::HttpTimeout(3))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize fault VFS");
    let windows = [
        SessionFaultWindow::BlockPut,
        SessionFaultWindow::XSyncCheckpoint,
        SessionFaultWindow::ResponseGateCheckpoint,
        SessionFaultWindow::SessionHeadCas,
        SessionFaultWindow::PublicationHeadCas,
        SessionFaultWindow::ManifestPut,
        SessionFaultWindow::FinalHeadCas,
        SessionFaultWindow::AmbiguousBlockPut,
        SessionFaultWindow::AmbiguousManifestPut,
    ];
    let matrix_prefix = format!("{suffix}/fault-matrix");
    let context = SessionFaultContext {
        backend,
        endpoint: &endpoint,
        storage_endpoint: &proxy.url,
        bucket: &bucket,
        base_prefix: &matrix_prefix,
        vfs,
        proxy: &proxy,
    };
    for (index, window) in windows.into_iter().enumerate() {
        run_session_fault_case(&context, window, index);
    }
}

#[cfg(feature = "remote")]
#[derive(Clone, Copy, Debug)]
enum SessionCrashWindow {
    BeforeAcceptedHead,
    AfterAcceptedHead,
    ManifestPut,
    FinalHeadCas,
}

#[cfg(feature = "remote")]
impl SessionCrashWindow {
    fn label(self) -> &'static str {
        match self {
            Self::BeforeAcceptedHead => "crash-before-accepted-head",
            Self::AfterAcceptedHead => "crash-after-accepted-head",
            Self::ManifestPut => "crash-manifest-put",
            Self::FinalHeadCas => "crash-final-head-cas",
        }
    }

    fn target(self) -> StorageFaultTarget {
        match self {
            Self::BeforeAcceptedHead => StorageFaultTarget::XSyncCheckpoint,
            Self::AfterAcceptedHead => StorageFaultTarget::SessionHeadCas,
            Self::ManifestPut => StorageFaultTarget::ManifestPut,
            Self::FinalHeadCas => StorageFaultTarget::FinalHeadCas,
        }
    }
}

#[cfg(feature = "remote")]
fn session_object_exists(backend: &str, endpoint: &str, bucket: &str, object: &str) -> bool {
    let listing = list_session_objects(backend, endpoint, bucket, object);
    match backend {
        "google" => {
            listing.contains(&format!(r#""name":"{object}""#))
                || listing.contains(&format!(r#""name": "{object}""#))
        }
        "s3" => listing.contains(&format!("<Key>{object}</Key>")),
        _ => unreachable!("unknown session backend {backend}"),
    }
}

#[cfg(feature = "remote")]
fn session_listing_contains_object_prefix(backend: &str, listing: &str, prefix: &str) -> bool {
    match backend {
        "google" => first_google_object_name(listing, prefix).is_some(),
        "s3" => listing.contains(&format!("<Key>{prefix}")),
        _ => unreachable!("unknown session backend {backend}"),
    }
}

#[cfg(feature = "remote")]
fn session_head_state(
    backend: &str,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    session_id: &str,
) -> Option<u8> {
    let object = format!("{prefix}/bcv-session/v1/head/{session_id}.bcv");
    if !session_object_exists(backend, endpoint, bucket, &object) {
        return None;
    }
    fetch_session_object(backend, endpoint, bucket, &object)
        .get(11)
        .copied()
}

#[cfg(feature = "remote")]
struct CrashChildInputs {
    backend: String,
    endpoint: String,
    bucket: String,
    prefix: String,
    alias: String,
    cache: PathBuf,
    ready: PathBuf,
    go: PathBuf,
}

#[cfg(feature = "remote")]
fn crash_child_inputs() -> Option<CrashChildInputs> {
    Some(CrashChildInputs {
        backend: std::env::var("BCV_CRASH_BACKEND").ok()?,
        endpoint: std::env::var("BCV_CRASH_ENDPOINT").ok()?,
        bucket: std::env::var("BCV_CRASH_BUCKET").ok()?,
        prefix: std::env::var("BCV_CRASH_PREFIX").ok()?,
        alias: std::env::var("BCV_CRASH_ALIAS").ok()?,
        cache: PathBuf::from(std::env::var_os("BCV_CRASH_CACHE")?),
        ready: PathBuf::from(std::env::var_os("BCV_CRASH_READY")?),
        go: PathBuf::from(std::env::var_os("BCV_CRASH_GO")?),
    })
}

#[cfg(feature = "remote")]
fn run_session_crash_child() {
    let Some(CrashChildInputs {
        backend,
        endpoint,
        bucket,
        prefix,
        alias,
        cache,
        ready,
        go,
    }) = crash_child_inputs()
    else {
        return;
    };
    let session_id = std::env::var("BCV_CRASH_SESSION_ID").expect("crash session ID");
    let operation = std::env::var("BCV_CRASH_OPERATION")
        .expect("crash operation")
        .parse::<u8>()
        .expect("crash operation number");
    let window = match std::env::var("BCV_CRASH_WINDOW")
        .expect("crash window")
        .as_str()
    {
        "before-accepted-head" => SessionCrashWindow::BeforeAcceptedHead,
        "after-accepted-head" => SessionCrashWindow::AfterAcceptedHead,
        "manifest-put" => SessionCrashWindow::ManifestPut,
        "final-head-cas" => SessionCrashWindow::FinalHeadCas,
        value => panic!("unknown crash window {value}"),
    };
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let vfs_name = format!("rusqdoltlite-crash-child-{}", std::process::id());
    let vfs = BlockCacheVfs::builder(&cache)
        .expect("crash child VFS builder")
        .name(&vfs_name)
        .expect("crash child VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .config(Config::RequestCount(1))
        .config(Config::StageWatermark(50))
        .config(Config::HttpTimeout(5))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize crash child VFS");
    let mut server = start_http_session_server_with_storage(
        vfs,
        storage,
        &alias,
        &session_id,
        session_operation_id(operation),
    );
    assert_eq!(
        server
            .operation_status()
            .expect("crash child operation status"),
        SessionOperationStatus::New,
        "the crash child must invoke a new operation"
    );
    fs::write(&ready, b"ready").expect("write crash child ready marker");
    wait_for_race_file(&go);

    match window {
        SessionCrashWindow::BeforeAcceptedHead => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open pre-head crash database");
            stage_session_small_update(&database, "FULL")
                .expect("stage pre-head crash transaction");
            database
                .execute_batch("COMMIT")
                .expect("commit pre-head crash transaction");
            drop(database);
        }
        SessionCrashWindow::AfterAcceptedHead => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open post-head crash database");
            stage_session_small_update(&database, "OFF")
                .expect("stage post-head crash transaction");
            database
                .execute_batch("COMMIT")
                .expect("commit post-head crash transaction");
            drop(database);
            server
                .complete_request()
                .expect("accept post-head crash operation");
        }
        SessionCrashWindow::ManifestPut | SessionCrashWindow::FinalHeadCas => {
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open publication crash database");
            stage_session_small_update(&database, "OFF")
                .expect("stage publication crash transaction");
            database
                .execute_batch("COMMIT")
                .expect("commit publication crash transaction");
            drop(database);
            server
                .upload()
                .expect("publish publication crash operation");
        }
    }
    panic!("crash child reached the end without being killed: {window:?}");
}

#[cfg(feature = "remote")]
fn run_session_crash_recovery_child() {
    let Some(CrashChildInputs {
        backend,
        endpoint,
        bucket,
        prefix,
        alias,
        cache,
        ready: _ready,
        go: _go,
    }) = crash_child_inputs()
    else {
        return;
    };
    let session_id = std::env::var("BCV_CRASH_SESSION_ID").expect("recovery session ID");
    let operation = std::env::var("BCV_CRASH_OPERATION")
        .expect("recovery operation")
        .parse::<u8>()
        .expect("recovery operation number");
    let window = std::env::var("BCV_CRASH_WINDOW").expect("recovery crash window");
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let vfs_name = format!("rusqdoltlite-crash-recovery-{}", std::process::id());
    let vfs = BlockCacheVfs::builder(&cache)
        .expect("recovery VFS builder")
        .name(&vfs_name)
        .expect("recovery VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .config(Config::RequestCount(1))
        .config(Config::StageWatermark(50))
        .config(Config::HttpTimeout(5))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize recovery VFS");
    let mut server = start_http_session_server_with_storage(
        vfs,
        storage,
        &alias,
        &session_id,
        session_operation_id(operation),
    );
    let status = server
        .operation_status()
        .expect("inspect recovered operation status");
    match window.as_str() {
        "before-accepted-head" => {
            assert_eq!(status, SessionOperationStatus::New);
            let database = vfs
                .open(format!("/{alias}/session.sqlite"))
                .expect("open pre-head recovery database");
            stage_session_small_update(&database, "OFF")
                .expect("stage pre-head recovery transaction");
            database
                .execute_batch("COMMIT")
                .expect("commit pre-head recovery transaction");
            drop(database);
            server.upload().expect("publish pre-head recovery");
        }
        "after-accepted-head" | "manifest-put" => {
            assert_eq!(status, SessionOperationStatus::Accepted);
            server.upload().expect("publish accepted recovery");
        }
        "final-head-cas" => {
            assert_eq!(status, SessionOperationStatus::Committed);
            server
                .quiesce()
                .expect("quiesce committed recovery without replay");
        }
        value => panic!("unknown recovery crash window {value}"),
    }
}

#[cfg(feature = "remote")]
fn run_session_crash_case(backend: &str, endpoint: &str, bucket: &str, window: SessionCrashWindow) {
    let suffix = unique_suffix();
    let prefix = format!("{suffix}/crash/{}", window.label());
    let direct_storage = session_storage(backend, endpoint, bucket, &prefix);
    let parent_vfs_name = format!("rusqdoltlite-crash-parent-{suffix}");
    let parent_vfs = BlockCacheVfs::builder(shared_cache())
        .expect("crash parent VFS builder")
        .name(&parent_vfs_name)
        .expect("crash parent VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .config(Config::RequestCount(1))
        .config(Config::StageWatermark(50))
        .config(Config::HttpTimeout(5))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize crash parent VFS");
    parent_vfs
        .initialize_container(&direct_storage)
        .unwrap_or_else(|error| panic!("initialize {} storage: {error:?}", window.label()));
    let seed_dir = tempfile::tempdir().expect("crash seed directory");
    let seed_path = seed_dir.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create crash seed database");
    seed.execute_batch(
        "CREATE TABLE fault_data(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO fault_data VALUES (1, 'seed');",
    )
    .expect("seed crash database");
    seed.close().expect("close crash seed database");
    parent_vfs
        .create_database(&direct_storage, &seed_path, "session.sqlite")
        .unwrap_or_else(|error| panic!("upload {} seed database: {error:?}", window.label()));
    let baseline =
        fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"));

    let proxy = StorageFaultProxy::start(endpoint);
    let root = tempfile::tempdir().expect("crash process directory");
    let cache = root.path().join("crash-cache");
    fs::create_dir(&cache).expect("create crash child cache directory");
    let ready = root.path().join("ready");
    let go = root.path().join("go");
    let alias = format!("crash_{}", window.label().replace('-', "_"));
    let session_id = format!("550e8400-e29b-41d4-a716-{:012x}", 0x100 + suffix.len());
    let operation_value = 0x40_u8.wrapping_add(suffix.len() as u8);
    let executable = std::env::current_exe().expect("locate crash test executable");
    let child = Command::new(&executable)
        .args(["--ignored", "--exact", "session_crash_child", "--nocapture"])
        .env("BCV_CRASH_BACKEND", backend)
        .env("BCV_CRASH_ENDPOINT", &proxy.url)
        .env("BCV_CRASH_BUCKET", bucket)
        .env("BCV_CRASH_PREFIX", &prefix)
        .env("BCV_CRASH_ALIAS", &alias)
        .env("BCV_CRASH_SESSION_ID", &session_id)
        .env("BCV_CRASH_OPERATION", operation_value.to_string())
        .env(
            "BCV_CRASH_WINDOW",
            window.label().strip_prefix("crash-").unwrap(),
        )
        .env("BCV_CRASH_CACHE", &cache)
        .env("BCV_CRASH_READY", &ready)
        .env("BCV_CRASH_GO", &go)
        .spawn()
        .unwrap_or_else(|error| panic!("spawn {} crash child: {error}", window.label()));
    wait_for_race_file(&ready);
    proxy.arm_kill_process(window.target(), child.id());
    fs::write(&go, b"go").expect("release crash child");
    let status = child.wait_with_output().expect("wait for crash child");
    assert!(
        !status.status.success(),
        "{} child unexpectedly completed: stdout={} stderr={}",
        window.label(),
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr)
    );
    #[cfg(unix)]
    assert_eq!(
        status.status.signal(),
        Some(9),
        "{} child must terminate with SIGKILL, not merely fail: stdout={} stderr={}",
        window.label(),
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr)
    );
    assert_eq!(
        proxy.faulted(),
        1,
        "{} fault was not triggered",
        window.label()
    );

    let after_crash =
        fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"));
    let head_state = session_head_state(backend, endpoint, bucket, &prefix, &session_id);
    match window {
        SessionCrashWindow::BeforeAcceptedHead => {
            assert_eq!(after_crash, baseline, "checkpoint crash must not publish");
            assert_eq!(head_state, None, "checkpoint crash must not create HEAD");
        }
        SessionCrashWindow::AfterAcceptedHead => {
            assert_eq!(
                after_crash, baseline,
                "accepted-head crash must not publish"
            );
            assert_ne!(head_state, Some(4), "accepted-head crash must not commit");
        }
        SessionCrashWindow::ManifestPut => {
            assert_ne!(after_crash, baseline, "manifest PUT must be durable");
            assert_ne!(
                head_state,
                Some(4),
                "manifest crash must precede final HEAD"
            );
        }
        SessionCrashWindow::FinalHeadCas => {
            assert_ne!(
                after_crash, baseline,
                "final HEAD crash must retain publication"
            );
            assert_eq!(
                head_state,
                Some(4),
                "final HEAD crash must leave COMMITTED state"
            );
        }
    }

    // The recovery invocation must use the same scoped storage endpoint as
    // the killed invocation. The proxy is now transparent, so the provider
    // identity used to derive the session scope remains unchanged without
    // allowing a second fault.
    proxy.disarm();

    let recovery_cache = root.path().join("recovery-cache");
    fs::create_dir(&recovery_cache).expect("create recovery cache directory");
    let recovery_status = Command::new(&executable)
        .args([
            "--ignored",
            "--exact",
            "session_crash_recovery_child",
            "--nocapture",
        ])
        .env("BCV_CRASH_BACKEND", backend)
        .env("BCV_CRASH_ENDPOINT", &proxy.url)
        .env("BCV_CRASH_BUCKET", bucket)
        .env("BCV_CRASH_PREFIX", &prefix)
        .env("BCV_CRASH_ALIAS", &alias)
        .env("BCV_CRASH_SESSION_ID", &session_id)
        .env("BCV_CRASH_OPERATION", operation_value.to_string())
        .env(
            "BCV_CRASH_WINDOW",
            window.label().strip_prefix("crash-").unwrap(),
        )
        .env("BCV_CRASH_CACHE", &recovery_cache)
        .env("BCV_CRASH_READY", root.path().join("recovery-ready"))
        .env("BCV_CRASH_GO", root.path().join("recovery-go"))
        .status()
        .unwrap_or_else(|error| panic!("spawn {} recovery child: {error}", window.label()));
    assert!(
        recovery_status.success(),
        "{} recovery child failed: {recovery_status}",
        window.label()
    );
    let after_recovery =
        fetch_session_object(backend, endpoint, bucket, &format!("{prefix}/manifest.bcv"));
    assert_ne!(
        after_recovery,
        baseline,
        "{} recovery must publish the changed database",
        window.label()
    );
    assert_eq!(
        session_head_state(backend, endpoint, bucket, &prefix, &session_id),
        Some(4),
        "{} recovery must leave a committed session head",
        window.label()
    );
}

#[cfg(feature = "remote")]
fn run_session_crash_matrix(backend: &str) {
    let suffix = unique_suffix();
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
            .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
            .unwrap_or_else(|_| "http://127.0.0.1:19025".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown crash backend {backend}"),
    };
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rust-session-crash-{suffix}")
    };
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    } else {
        ensure_s3_bucket(&endpoint, &bucket);
    }
    for window in [
        SessionCrashWindow::BeforeAcceptedHead,
        SessionCrashWindow::AfterAcceptedHead,
        SessionCrashWindow::ManifestPut,
        SessionCrashWindow::FinalHeadCas,
    ] {
        run_session_crash_case(backend, &endpoint, &bucket, window);
    }
}

#[cfg(feature = "remote")]
#[derive(Debug)]
struct ConcurrentSessionResult {
    operation_status: Option<SessionOperationStatus>,
    completed: bool,
    start_error_code: Option<i32>,
    start_error: Option<String>,
}

#[cfg(feature = "remote")]
struct ConcurrentSessionRequest {
    vfs: &'static BlockCacheVfs,
    storage: Storage,
    alias: String,
    session_id: String,
    operation_id: SessionOperationId,
    write_value: String,
}

#[cfg(feature = "remote")]
struct ConcurrentSessionBarriers {
    start: Arc<Barrier>,
    complete: Arc<Barrier>,
}

#[cfg(feature = "remote")]
fn concurrent_failure_result(
    phase: &mut u8,
    start_barrier: &Barrier,
    complete_barrier: &Barrier,
    error_code: Option<i32>,
    error: String,
) -> ConcurrentSessionResult {
    if *phase < 1 {
        *phase = 1;
        start_barrier.wait();
    }
    if *phase < 2 {
        *phase = 2;
        complete_barrier.wait();
    }
    ConcurrentSessionResult {
        operation_status: None,
        completed: false,
        start_error_code: error_code,
        start_error: Some(error),
    }
}

#[cfg(feature = "remote")]
fn spawn_concurrent_session_request(
    request: ConcurrentSessionRequest,
    barriers: ConcurrentSessionBarriers,
) -> thread::JoinHandle<ConcurrentSessionResult> {
    thread::spawn(move || {
        let ConcurrentSessionRequest {
            vfs,
            storage,
            alias,
            session_id,
            operation_id,
            write_value,
        } = request;
        let ConcurrentSessionBarriers { start, complete } = barriers;
        let mut phase = 0_u8;
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let mut server = match try_start_http_session_server_with_storage(
                vfs,
                storage,
                &alias,
                &session_id,
                operation_id,
            ) {
                Ok(server) => server,
                Err(error) => {
                    // A same-session owner race may reject one attach before
                    // the handler runs. Keep both workers synchronized so that
                    // this expected loser cannot turn the test into a
                    // scheduling-dependent deadlock.
                    return concurrent_failure_result(
                        &mut phase,
                        &start,
                        &complete,
                        match &error {
                            rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code),
                            _ => None,
                        },
                        format!("{error:?}"),
                    );
                }
            };
            let database = match vfs.open(format!("/{alias}/session.sqlite")) {
                Ok(database) => database,
                Err(error) => {
                    let error_code = match &error {
                        rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code),
                        _ => None,
                    };
                    let _ = server.quiesce();
                    return concurrent_failure_result(
                        &mut phase,
                        &start,
                        &complete,
                        error_code,
                        format!("open concurrent session database: {error:?}"),
                    );
                }
            };
            if let Err(error) = database.execute(
                "UPDATE race_data SET value = ?1 WHERE id = 1",
                [&write_value],
            ) {
                let error_code = match &error {
                    rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code),
                    _ => None,
                };
                drop(database);
                let _ = server.quiesce();
                return concurrent_failure_result(
                    &mut phase,
                    &start,
                    &complete,
                    error_code,
                    format!("write concurrent session database: {error:?}"),
                );
            }
            drop(database);
            let operation_status = server
                .operation_status()
                .expect("inspect concurrent session operation status");
            phase = 1;
            start.wait();
            debug_assert_eq!(phase, 1);
            let _response_status = if should_invoke_mutating_handler(operation_status) {
                send_http_request_bytes(server.port(), "GET", "/session.sqlite/root", &[]).status
            } else {
                server
                    .quiesce()
                    .expect("quiesce rejected concurrent operation");
                409
            };
            phase = 2;
            complete.wait();
            let completed = if should_invoke_mutating_handler(operation_status) {
                server.complete_request().is_ok()
            } else {
                false
            };
            ConcurrentSessionResult {
                operation_status: Some(operation_status),
                completed,
                start_error_code: None,
                start_error: None,
            }
        }));
        match outcome {
            Ok(result) => result,
            Err(panic) => {
                // Any assertion or native error before a rendezvous must still
                // release the other worker. The phase is set before each wait,
                // so recovery never waits twice on a barrier already crossed.
                concurrent_failure_result(
                    &mut phase,
                    &start,
                    &complete,
                    None,
                    format!("worker panic: {panic:?}"),
                )
            }
        }
    })
}

#[cfg(feature = "remote")]
fn run_concurrent_session_requests(
    first: ConcurrentSessionRequest,
    second: ConcurrentSessionRequest,
) -> [ConcurrentSessionResult; 2] {
    let start_barrier = Arc::new(Barrier::new(2));
    let complete_barrier = Arc::new(Barrier::new(2));
    let first = spawn_concurrent_session_request(
        first,
        ConcurrentSessionBarriers {
            start: Arc::clone(&start_barrier),
            complete: Arc::clone(&complete_barrier),
        },
    );
    let second = spawn_concurrent_session_request(
        second,
        ConcurrentSessionBarriers {
            start: start_barrier,
            complete: complete_barrier,
        },
    );
    [
        first.join().expect("first concurrent session worker"),
        second.join().expect("second concurrent session worker"),
    ]
}

#[cfg(feature = "remote")]
fn run_session_concurrency_matrix() {
    let suffix = unique_suffix();
    let endpoint = std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
        .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
        .unwrap_or_else(|_| "http://127.0.0.1:19025".into());
    let bucket = "app_storage";
    let prefix = format!("{suffix}/session-races");
    ensure_google_bucket(&endpoint, bucket);
    let storage = session_storage("google", &endpoint, bucket, &prefix);
    let vfs_name = format!("rusqdoltlite-race-{suffix}");
    let vfs = BlockCacheVfs::builder(shared_cache())
        .expect("race VFS builder")
        .name(&vfs_name)
        .expect("race VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize race VFS");
    vfs.initialize_container(&storage)
        .expect("initialize race storage");
    let seed_dir = tempfile::tempdir().expect("race seed directory");
    let seed_path = seed_dir.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create race seed database");
    seed.execute_batch(
        "CREATE TABLE race_data(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO race_data VALUES (1, 'seed');",
    )
    .expect("seed race database");
    seed.close().expect("close race seed database");
    vfs.create_database(&storage, &seed_path, "session.sqlite")
        .expect("upload race seed database");
    // These are distinct RemoteServer instances sharing this process-global
    // VFS/cache. This checks in-process server arbitration, not independent
    // serverless invocations. The separate-process companion below gives
    // each worker its own process and scratch cache root.
    let same_session = run_concurrent_session_requests(
        ConcurrentSessionRequest {
            vfs,
            storage: storage.clone(),
            alias: "race-one".into(),
            session_id: "550e8400-e29b-41d4-a716-446655440021".into(),
            operation_id: session_operation_id(1),
            write_value: "same-session-one".into(),
        },
        ConcurrentSessionRequest {
            vfs,
            storage: storage.clone(),
            alias: "race-one".into(),
            session_id: "550e8400-e29b-41d4-a716-446655440021".into(),
            operation_id: session_operation_id(2),
            write_value: "same-session-two".into(),
        },
    );
    let same_session_completions = same_session
        .iter()
        .filter(|result| result.completed)
        .count();
    assert_eq!(
        same_session_completions, 1,
        "same-session concurrent requests must have one accepted head: {same_session:?}"
    );
    assert!(
        same_session.iter().all(|result| {
            result.start_error.is_none()
                || matches!(
                    result.start_error_code,
                    Some(rusqlite::ffi::SQLITE_BUSY)
                        | Some(rusqlite::ffi::SQLITE_CONSTRAINT)
                        | Some(rusqlite::ffi::SQLITE_AUTH)
                )
        }),
        "same-session race returned an uncontrolled startup error: {same_session:?}"
    );
    assert!(
        same_session.iter().all(|result| {
            result.operation_status.is_none()
                || matches!(
                    result.operation_status,
                    Some(SessionOperationStatus::New) | Some(SessionOperationStatus::Conflict)
                )
        }),
        "same-session race returned an unexpected operation status: {same_session:?}"
    );

    // Prepare two distinct client sessions from the same immutable base in
    // separate processes. The native VFS cannot safely run two local writes in
    // one process (the second can report SQLITE_FULL before reaching CAS), so
    // the publication race is isolated to fresh server invocations below.
    run_publication_process_race(&endpoint, bucket, &prefix, vfs, &storage, &suffix);

    run_same_session_process_race(&endpoint, bucket, &prefix);
}

#[cfg(feature = "remote")]
fn wait_for_race_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for session race barrier {path:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(feature = "remote")]
fn run_same_session_process_child() {
    let Some(cache) = std::env::var_os("BCV_RACE_CACHE").map(PathBuf::from) else {
        return;
    };
    let endpoint = std::env::var("BCV_RACE_ENDPOINT").expect("same-session race endpoint");
    let bucket = std::env::var("BCV_RACE_BUCKET").expect("same-session race bucket");
    let prefix = std::env::var("BCV_RACE_PREFIX").expect("same-session race prefix");
    let barrier = PathBuf::from(
        std::env::var_os("BCV_RACE_BARRIER").expect("same-session race barrier directory"),
    );
    let result_path =
        PathBuf::from(std::env::var_os("BCV_RACE_RESULT").expect("same-session race result path"));
    let slot = std::env::var("BCV_RACE_SLOT").expect("same-session race slot");
    let other_slot = if slot == "one" { "two" } else { "one" };
    let session_id = std::env::var("BCV_RACE_SESSION_ID").expect("same-session race session ID");
    let alias = std::env::var("BCV_RACE_ALIAS").expect("same-session race alias");
    let operation = std::env::var("BCV_RACE_OPERATION")
        .expect("same-session race operation")
        .parse::<u8>()
        .expect("same-session race operation number");
    let value = std::env::var("BCV_RACE_VALUE").expect("same-session race value");
    let ready = barrier.join(format!("{slot}.ready"));
    let other_ready = barrier.join(format!("{other_slot}.ready"));
    let complete = barrier.join(format!("{slot}.complete"));
    let other_complete = barrier.join(format!("{other_slot}.complete"));
    let write_result = |contents: &str| {
        fs::write(&result_path, contents).expect("write same-session race result");
    };
    let vfs_name = format!("rusqdoltlite-race-child-{slot}");
    let storage = session_storage("google", &endpoint, &bucket, &prefix);
    let vfs = BlockCacheVfs::builder(&cache)
        .expect("same-session child VFS builder")
        .name(&vfs_name)
        .expect("same-session child VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize same-session child VFS");
    let mut server = match try_start_http_session_server_with_storage(
        vfs,
        storage,
        &alias,
        &session_id,
        session_operation_id(operation),
    ) {
        Ok(server) => server,
        Err(error) => {
            write_result(&format!(
                "start=0\nerror_code={}\nerror={error:?}\n",
                match error {
                    rusqlite::Error::SqliteFailure(code, _) => code.extended_code,
                    _ => -1,
                }
            ));
            fs::write(&ready, b"ready").expect("write failed-start ready marker");
            wait_for_race_file(&other_ready);
            fs::write(&complete, b"complete").expect("write failed-start complete marker");
            wait_for_race_file(&other_complete);
            return;
        }
    };
    let operation_status = server
        .operation_status()
        .expect("inspect same-session child status");
    let database = vfs
        .open(format!("/{alias}/session.sqlite"))
        .expect("open same-session child database");
    database
        .execute("UPDATE race_data SET value = ?1 WHERE id = 1", [&value])
        .expect("write same-session child database");
    drop(database);
    fs::write(&ready, b"ready").expect("write same-session child ready marker");
    wait_for_race_file(&other_ready);
    let response_status = if operation_status == SessionOperationStatus::New {
        send_http_request_bytes(server.port(), "GET", "/session.sqlite/root", &[]).status
    } else {
        server
            .quiesce()
            .expect("quiesce non-new same-session child");
        409
    };
    fs::write(&complete, b"complete").expect("write same-session child complete marker");
    wait_for_race_file(&other_complete);
    let completion = if operation_status == SessionOperationStatus::New {
        server.complete_request()
    } else {
        Ok(())
    };
    let completion_code = completion.as_ref().err().and_then(|error| match error {
        rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code),
        _ => None,
    });
    write_result(&format!(
        "start=1\nnew={}\nresponse={}\ncomplete={}\ncomplete_code={}\n",
        u8::from(operation_status == SessionOperationStatus::New),
        response_status,
        u8::from(completion.is_ok()),
        completion_code.unwrap_or(0),
    ));
}

#[cfg(feature = "remote")]
fn run_same_session_process_race(endpoint: &str, bucket: &str, prefix: &str) {
    let root = tempfile::tempdir().expect("same-session race process directory");
    let barrier = root.path().join("barrier");
    fs::create_dir(&barrier).expect("create same-session race barrier");
    let executable = std::env::current_exe().expect("locate same-session race executable");
    let session_id = "550e8400-e29b-41d4-a716-446655440024";
    let alias = "race-process";
    let mut children = Vec::new();
    for (slot, operation, value) in [("one", 5_u8, "process-one"), ("two", 6_u8, "process-two")] {
        let cache = root.path().join(format!("cache-{slot}"));
        let result = root.path().join(format!("result-{slot}.txt"));
        fs::create_dir(&cache).expect("create same-session child cache directory");
        children.push(
            Command::new(&executable)
                .args([
                    "--ignored",
                    "--exact",
                    "same_session_process_race_child",
                    "--nocapture",
                ])
                .env("BCV_RACE_CACHE", &cache)
                .env("BCV_RACE_ENDPOINT", endpoint)
                .env("BCV_RACE_BUCKET", bucket)
                .env("BCV_RACE_PREFIX", prefix)
                .env("BCV_RACE_BARRIER", &barrier)
                .env("BCV_RACE_RESULT", &result)
                .env("BCV_RACE_SLOT", slot)
                .env("BCV_RACE_SESSION_ID", session_id)
                .env("BCV_RACE_ALIAS", alias)
                .env("BCV_RACE_OPERATION", operation.to_string())
                .env("BCV_RACE_VALUE", value)
                .spawn()
                .unwrap_or_else(|error| panic!("spawn same-session race child {slot}: {error}")),
        );
    }
    for child in children {
        let status = child
            .wait_with_output()
            .expect("wait for same-session race child");
        assert!(
            status.status.success(),
            "same-session race child failed: {}\nstdout={}\nstderr={}",
            status.status,
            String::from_utf8_lossy(&status.stdout),
            String::from_utf8_lossy(&status.stderr)
        );
    }
    let results = ["one", "two"].map(|slot| {
        let path = root.path().join(format!("result-{slot}.txt"));
        fs::read_to_string(path).expect("read same-session race result")
    });
    assert!(
        results
            .iter()
            .all(|result| result.contains("start=1\nnew=1\n")),
        "both independent processes must attach the same predecessor before CAS: {results:?}"
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| result.contains("complete=1"))
            .count(),
        1,
        "same-session independent processes must have one accepted head: {results:?}"
    );
    assert!(
        results.iter().any(|result| {
            result.contains("complete=0\ncomplete_code=5\n")
                || result.contains("complete=0\ncomplete_code=19\n")
                || result.contains("complete=0\ncomplete_code=23\n")
                || result.contains("complete=0\ncomplete_code=412\n")
        }),
        "same-session loser must report a fenced CAS conflict: {results:?}"
    );
}

#[cfg(feature = "remote")]
fn publication_child_vfs(cache: &Path, slot: &str) -> &'static BlockCacheVfs {
    let name = format!("rusqdoltlite-publication-child-{slot}");
    BlockCacheVfs::builder(cache)
        .expect("publication child VFS builder")
        .name(&name)
        .expect("publication child VFS name")
        .config(Config::CacheSize(4 * 1024 * 1024))
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize publication child VFS")
}

#[cfg(feature = "remote")]
fn run_publication_prepare_child() {
    let Some(cache) = std::env::var_os("BCV_PUBLICATION_CACHE").map(PathBuf::from) else {
        return;
    };
    let endpoint = std::env::var("BCV_PUBLICATION_ENDPOINT").expect("publication endpoint");
    let bucket = std::env::var("BCV_PUBLICATION_BUCKET").expect("publication bucket");
    let prefix = std::env::var("BCV_PUBLICATION_PREFIX").expect("publication prefix");
    let result_path =
        PathBuf::from(std::env::var_os("BCV_PUBLICATION_RESULT").expect("publication result path"));
    let slot = std::env::var("BCV_PUBLICATION_SLOT").expect("publication slot");
    let alias = std::env::var("BCV_PUBLICATION_ALIAS").expect("publication alias");
    let session_id = std::env::var("BCV_PUBLICATION_SESSION_ID").expect("publication session ID");
    let operation = std::env::var("BCV_PUBLICATION_OPERATION")
        .expect("publication operation")
        .parse::<u8>()
        .expect("publication operation number");
    let value = std::env::var("BCV_PUBLICATION_VALUE").expect("publication value");
    let write_result = |contents: &str| {
        fs::write(&result_path, contents).expect("write publication result");
    };
    let storage = session_storage("google", &endpoint, &bucket, &prefix);
    let vfs = publication_child_vfs(&cache, &slot);
    let mut server = match try_start_http_session_server_with_storage(
        vfs,
        storage,
        &alias,
        &session_id,
        session_operation_id(operation),
    ) {
        Ok(server) => server,
        Err(error) => {
            write_result(&format!(
                "start=0\nerror_code={}\nerror={error:?}\n",
                match error {
                    rusqlite::Error::SqliteFailure(code, _) => code.extended_code,
                    _ => -1,
                }
            ));
            return;
        }
    };
    let operation_status = match server.operation_status() {
        Ok(status) => status,
        Err(error) => {
            let _ = server.quiesce();
            write_result(&format!("start=1\nstatus_error={error:?}\n"));
            return;
        }
    };
    if operation_status != SessionOperationStatus::New {
        let _ = server.quiesce();
        write_result(&format!("start=1\nnew=0\nstatus={operation_status:?}\n"));
        return;
    }
    let database = match vfs.open(format!("/{alias}/session.sqlite")) {
        Ok(database) => database,
        Err(error) => {
            let _ = server.quiesce();
            write_result(&format!("start=1\nnew=1\nopen_error={error:?}\n"));
            return;
        }
    };
    if let Err(error) = database.execute("UPDATE race_data SET value = ?1 WHERE id = 1", [&value]) {
        drop(database);
        let _ = server.quiesce();
        write_result(&format!("start=1\nnew=1\nwrite_error={error:?}\n"));
        return;
    }
    drop(database);
    let response_status =
        send_http_request_bytes(server.port(), "GET", "/session.sqlite/root", &[]).status;
    let completion = if response_status == 200 {
        server.complete_request()
    } else {
        let _ = server.quiesce();
        Err(rusqlite::Error::InvalidQuery)
    };
    let completion_code = completion.as_ref().err().and_then(|error| match error {
        rusqlite::Error::SqliteFailure(code, _) => Some(code.extended_code),
        _ => None,
    });
    write_result(&format!(
        "start=1\nnew=1\nresponse={response_status}\ncomplete={}\ncomplete_code={}\n",
        u8::from(completion.is_ok()),
        completion_code.unwrap_or(0),
    ));
}

#[cfg(feature = "remote")]
fn run_publication_upload_child() {
    let Some(cache) = std::env::var_os("BCV_PUBLICATION_CACHE").map(PathBuf::from) else {
        return;
    };
    let endpoint = std::env::var("BCV_PUBLICATION_ENDPOINT").expect("publication endpoint");
    let bucket = std::env::var("BCV_PUBLICATION_BUCKET").expect("publication bucket");
    let prefix = std::env::var("BCV_PUBLICATION_PREFIX").expect("publication prefix");
    let result_path =
        PathBuf::from(std::env::var_os("BCV_PUBLICATION_RESULT").expect("publication result path"));
    let slot = std::env::var("BCV_PUBLICATION_SLOT").expect("publication slot");
    let alias = std::env::var("BCV_PUBLICATION_ALIAS").expect("publication alias");
    let session_id = std::env::var("BCV_PUBLICATION_SESSION_ID").expect("publication session ID");
    let operation = std::env::var("BCV_PUBLICATION_OPERATION")
        .expect("publication operation")
        .parse::<u8>()
        .expect("publication operation number");
    let write_result = |contents: &str| {
        fs::write(&result_path, contents).expect("write publication upload result");
    };
    let storage = session_storage("google", &endpoint, &bucket, &prefix);
    let vfs = publication_child_vfs(&cache, &slot);
    let mut server = match try_start_http_session_server_with_storage(
        vfs,
        storage,
        &alias,
        &session_id,
        session_operation_id(operation),
    ) {
        Ok(server) => server,
        Err(error) => {
            write_result(&format!(
                "start=0\nerror_code={}\nerror={error:?}\n",
                match error {
                    rusqlite::Error::SqliteFailure(code, _) => code.extended_code,
                    _ => -1,
                }
            ));
            return;
        }
    };
    let operation_status = match server.operation_status() {
        Ok(status) => status,
        Err(error) => {
            let _ = server.quiesce();
            write_result(&format!("start=1\nstatus_error={error:?}\n"));
            return;
        }
    };
    let _ = server.quiesce();
    let (uploaded, upload_code, upload_error) = match operation_status {
        SessionOperationStatus::Accepted | SessionOperationStatus::Committed => {
            match server.upload() {
                Ok(()) => (true, 0, None),
                Err(error) => {
                    let code = match &error {
                        rusqlite::Error::SqliteFailure(code, _) => code.extended_code,
                        _ => -1,
                    };
                    (false, code, Some(format!("{error:?}")))
                }
            }
        }
        _ => (false, 0, None),
    };
    write_result(&format!(
        "start=1\nstatus={operation_status:?}\nupload={}\nupload_code={upload_code}\nerror={}\n",
        u8::from(uploaded),
        upload_error.unwrap_or_default(),
    ));
}

#[cfg(feature = "remote")]
fn run_publication_process_race(
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    vfs: &'static BlockCacheVfs,
    storage: &Storage,
    suffix: &str,
) {
    let root = tempfile::tempdir().expect("publication race process directory");
    let executable = std::env::current_exe().expect("locate publication race executable");
    let sessions = [
        (
            "one",
            "publication-one",
            "550e8400-e29b-41d4-a716-446655440022",
            3_u8,
            "session-two",
        ),
        (
            "two",
            "publication-two",
            "550e8400-e29b-41d4-a716-446655440023",
            4_u8,
            "session-three",
        ),
    ];
    let baseline_manifest =
        fetch_google_object(endpoint, bucket, &format!("{prefix}/manifest.bcv"));

    // Complete each private session before starting the publication race. This
    // isolates the required publication CAS from the unsupported same-process
    // local-cache write race while both sessions still share one base tip.
    for (slot, alias, session_id, operation, value) in sessions {
        let cache = root.path().join(format!("prepare-cache-{slot}"));
        let result = root.path().join(format!("prepare-result-{slot}.txt"));
        fs::create_dir(&cache).expect("create publication prepare cache");
        let status = Command::new(&executable)
            .args([
                "--ignored",
                "--exact",
                "independent_session_publication_prepare_child",
                "--nocapture",
            ])
            .env("BCV_PUBLICATION_CACHE", &cache)
            .env("BCV_PUBLICATION_ENDPOINT", endpoint)
            .env("BCV_PUBLICATION_BUCKET", bucket)
            .env("BCV_PUBLICATION_PREFIX", prefix)
            .env("BCV_PUBLICATION_RESULT", &result)
            .env("BCV_PUBLICATION_SLOT", slot)
            .env("BCV_PUBLICATION_ALIAS", alias)
            .env("BCV_PUBLICATION_SESSION_ID", session_id)
            .env("BCV_PUBLICATION_OPERATION", operation.to_string())
            .env("BCV_PUBLICATION_VALUE", value)
            .status()
            .unwrap_or_else(|error| panic!("spawn publication prepare child {slot}: {error}"));
        assert!(
            status.success(),
            "publication prepare child {slot} failed: {status}"
        );
        let contents = fs::read_to_string(&result).expect("read publication prepare result");
        assert!(
            contents.contains("start=1\nnew=1\nresponse=200\ncomplete=1\n"),
            "publication prepare child {slot} did not accept its private head: {contents}"
        );
    }

    let mut children = Vec::new();
    for (slot, alias, session_id, operation, _value) in sessions {
        let cache = root.path().join(format!("upload-cache-{slot}"));
        let result = root.path().join(format!("upload-result-{slot}.txt"));
        fs::create_dir(&cache).expect("create publication upload cache");
        children.push(
            Command::new(&executable)
                .args([
                    "--ignored",
                    "--exact",
                    "independent_session_publication_upload_child",
                    "--nocapture",
                ])
                .env("BCV_PUBLICATION_CACHE", &cache)
                .env("BCV_PUBLICATION_ENDPOINT", endpoint)
                .env("BCV_PUBLICATION_BUCKET", bucket)
                .env("BCV_PUBLICATION_PREFIX", prefix)
                .env("BCV_PUBLICATION_RESULT", &result)
                .env("BCV_PUBLICATION_SLOT", slot)
                .env("BCV_PUBLICATION_ALIAS", alias)
                .env("BCV_PUBLICATION_SESSION_ID", session_id)
                .env("BCV_PUBLICATION_OPERATION", operation.to_string())
                .spawn()
                .unwrap_or_else(|error| panic!("spawn publication upload child {slot}: {error}")),
        );
    }
    for child in children {
        let status = child
            .wait_with_output()
            .expect("wait for publication upload child");
        assert!(
            status.status.success(),
            "publication upload child failed: {}\nstdout={}\nstderr={}",
            status.status,
            String::from_utf8_lossy(&status.stdout),
            String::from_utf8_lossy(&status.stderr)
        );
    }
    let results = ["one", "two"].map(|slot| {
        let path = root.path().join(format!("upload-result-{slot}.txt"));
        fs::read_to_string(path).expect("read publication upload result")
    });
    assert_eq!(
        results
            .iter()
            .filter(|result| result.contains("upload=1"))
            .count(),
        1,
        "exactly one concurrent publication may win the manifest CAS: {results:?}"
    );
    assert!(
        results.iter().all(|result| {
            result.contains("upload=1")
                || result.contains("upload_code=5")
                || result.contains("upload_code=19")
                || result.contains("upload_code=23")
        }),
        "the losing publication must report a fenced conflict: {results:?}"
    );
    let published_alias = format!("race-published-{suffix}");
    vfs.attach(&AttachSpec::new(storage.clone()).alias(&published_alias))
        .expect("attach publication-race winner");
    let published_db = vfs
        .open(format!("/{published_alias}/session.sqlite"))
        .expect("open publication-race winner");
    let published_value: String = published_db
        .query_row("SELECT value FROM race_data WHERE id = 1", [], |row| {
            row.get(0)
        })
        .expect("read publication-race winner");
    let expected_value = if results[0].contains("upload=1") {
        "session-two"
    } else {
        "session-three"
    };
    assert_eq!(
        published_value, expected_value,
        "published manifest must match the winning upload"
    );
    drop(published_db);
    vfs.detach(&published_alias)
        .expect("detach publication-race winner");
    let published_manifest =
        fetch_google_object(endpoint, bucket, &format!("{prefix}/manifest.bcv"));
    assert_ne!(
        baseline_manifest, published_manifest,
        "one concurrent publication must advance the manifest"
    );
}

#[cfg(feature = "remote")]
fn run_session_http_flow(backend: &str) {
    let executable = std::env::current_exe().expect("locate session HTTP flow test executable");
    let status = Command::new(executable)
        .args([
            "--ignored",
            "--exact",
            "session_http_flow_child",
            "--nocapture",
        ])
        .env("BCV_SESSION_HTTP_BACKEND", backend)
        .env(
            "DOLTLITE_HTTP_TIMEOUT_MS",
            SESSION_HTTP_TEST_TIMEOUT.as_millis().to_string(),
        )
        .status()
        .unwrap_or_else(|error| panic!("spawn {backend} session HTTP flow child: {error}"));
    assert!(
        status.success(),
        "{backend} session HTTP flow child failed: {status}"
    );
}

#[cfg(feature = "remote")]
fn run_session_http_flow_in_process(backend: &str) {
    let suffix = unique_suffix();
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
            .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
            .unwrap_or_else(|_| "http://127.0.0.1:19025".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown session backend {backend}"),
    };
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rust-session-http-{suffix}")
    };
    let prefix = format!("{suffix}/session-http");
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    } else {
        ensure_s3_bucket(&endpoint, &bucket);
    }
    let storage = session_storage(backend, &endpoint, &bucket, &prefix);

    const SESSION_CACHE_BYTES: i64 = 4 * 1024 * 1024;
    let cache_dir = tempfile::tempdir().expect("session HTTP cache directory");
    let vfs_name = format!("rusqdoltlite-session-{suffix}");
    let vfs = BlockCacheVfs::builder(cache_dir.path())
        .expect("VFS builder")
        .name(&vfs_name)
        .expect("session HTTP VFS name")
        .config(Config::CacheSize(SESSION_CACHE_BYTES))
        .auth_callback(|provider, _account, _container| {
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

    // The HTTP server exposes a DoltLite repository, so seed the object
    // store from a real DoltLite commit rather than a plain SQLite fixture.
    let seed_dir = tempfile::tempdir().expect("seed database directory");
    let seed_path = seed_dir.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create DoltLite seed database");
    seed.execute_batch(
        "CREATE TABLE session_http(id INTEGER PRIMARY KEY, value TEXT NOT NULL);
         INSERT INTO session_http VALUES (1, 'seed');",
    )
    .expect("seed DoltLite database");
    let _: String = seed
        .query_row("SELECT dolt_commit('-A', '-m', 'seed')", [], |row| {
            row.get(0)
        })
        .expect("commit DoltLite seed database");
    seed.close().expect("close DoltLite seed database");
    vfs.create_database(&storage, &seed_path, "session.sqlite")
        .expect("upload DoltLite seed database");

    let baseline_manifest = fetch_session_object(
        backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    let alias = "session-http-alias";
    let session_id = "550e8400-e29b-41d4-a716-446655440015";

    // A failed application handler is quiesced without checkpointing or
    // accepting its operation. A fresh invocation with the same id therefore
    // remains New and is allowed to retry the handler.
    let mut failed = start_http_session_server_with_storage(
        vfs,
        storage.clone(),
        alias,
        session_id,
        session_operation_id(9),
    );
    let mut failed_adapter = ReferenceResponseAdapter::new(&mut failed);
    failed_adapter.buffer(b"failed response");
    assert_eq!(
        failed_adapter
            .release_after_completion("/chunks", 500)
            .expect("return failed application response"),
        b"failed response"
    );
    drop(failed);
    let failed_retry = start_http_session_server_with_storage(
        vfs,
        storage.clone(),
        alias,
        session_id,
        session_operation_id(9),
    );
    assert_eq!(
        failed_retry
            .operation_status()
            .expect("failed operation status"),
        SessionOperationStatus::New
    );
    drop(failed_retry);

    // The stable test proxy creates a fresh RemoteServer for every incoming
    // HTTP request, with the same session ID and a new deterministic test
    // operation ID. The mixer is only a stand-in for a collision-resistant
    // app-side request identity; raw requests below cover the protocol read
    // routes, while the DoltLite clone and push drive the same proxy path
    // naturally.
    let proxy = SessionHttpProxy::start(vfs, storage.clone(), "session.sqlite", alias, session_id);
    let missing_token = send_http_request_bytes(proxy.port, "GET", "/session.sqlite/root", &[]);
    assert_eq!(
        missing_token.status, 404,
        "the application proxy must reject a URL without a session token"
    );
    let wrong_token =
        send_http_request_bytes(proxy.port, "GET", "/s/wrong-token/session.sqlite/root", &[]);
    assert_eq!(
        wrong_token.status, 404,
        "the application proxy must reject a URL with a foreign session token"
    );
    let out_of_scope = send_http_request_bytes(
        proxy.port,
        "GET",
        &proxy.request_path("other.sqlite/root"),
        &[],
    );
    assert_eq!(
        out_of_scope.status, 404,
        "the application proxy must reject a database outside its session scope"
    );
    let root = send_http_request_bytes(
        proxy.port,
        "GET",
        &proxy.request_path("session.sqlite/root"),
        &[],
    );
    assert_eq!(root.status, 200, "GET /root must succeed");
    assert_eq!(
        root.body.len(),
        20,
        "GET /root returns one 20-byte DoltLite prolly hash"
    );

    let refs = send_http_request_bytes(
        proxy.port,
        "GET",
        &proxy.request_path("session.sqlite/refs"),
        &[],
    );
    assert_eq!(refs.status, 200, "GET /refs must succeed");
    assert!(
        !refs.body.is_empty(),
        "GET /refs returns the seed refs blob"
    );

    let has_chunks = send_http_request_bytes(
        proxy.port,
        "POST",
        &proxy.request_path("session.sqlite/has-chunks"),
        &root.body,
    );
    assert_eq!(has_chunks.status, 200, "POST /has-chunks must succeed");
    assert_eq!(
        has_chunks.body.as_slice(),
        &[1],
        "the seed root chunk must be present"
    );

    let get_chunks = send_http_request_bytes(
        proxy.port,
        "POST",
        &proxy.request_path("session.sqlite/get-chunks"),
        &root.body,
    );
    assert_eq!(get_chunks.status, 200, "POST /get-chunks must succeed");
    assert!(
        get_chunks.body.len() >= 4,
        "chunk response has a length prefix"
    );
    let root_chunk_len = u32::from_be_bytes(
        get_chunks.body[..4]
            .try_into()
            .expect("chunk response length prefix"),
    ) as usize;
    assert_ne!(root_chunk_len, u32::MAX as usize);
    assert_eq!(get_chunks.body.len(), 4 + root_chunk_len);

    let root_path = proxy.request_path(&format!("session.sqlite/chunk/{}", hex_bytes(&root.body)));
    let chunk = send_http_request_bytes(proxy.port, "GET", &root_path, &[]);
    assert_eq!(chunk.status, 200, "GET /chunk/<hash> must succeed");
    assert_eq!(chunk.body, get_chunks.body[4..]);

    let clone_dir = tempfile::tempdir().expect("clone database directory");
    let clone_path = clone_dir.path().join("clone.sqlite");
    let clone = Connection::open(&clone_path).expect("create clone database");
    let _: i64 = clone
        .query_row(
            "SELECT dolt_clone(?1)",
            params![proxy.database_url("session.sqlite")],
            |row| row.get(0),
        )
        .expect("clone through session HTTP server");
    let seed_value: String = clone
        .query_row("SELECT value FROM session_http WHERE id = 1", [], |row| {
            row.get(0)
        })
        .expect("read cloned seed row");
    assert_eq!(seed_value, "seed");
    drop(clone);

    // Every proxy read was checkpointed and accepted by its own fresh server,
    // but none selected the application's final /commit publication route.
    let after_read_manifest = fetch_session_object(
        backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(
        baseline_manifest, after_read_manifest,
        "accepted read request must not publish the ordinary manifest"
    );

    // A real DoltLite push now sends /has-chunks, /chunks, /refs-if, and
    // /commit through fresh session-server instances. The proxy's
    // application adapter publishes only after the successful /commit.
    let writer = Connection::open(&clone_path).expect("reopen clone database");
    let _: i64 = writer
        .query_row(
            "SELECT dolt_remote('set-url', 'origin', ?1)",
            params![proxy.database_url("session.sqlite")],
            |row| row.get(0),
        )
        .expect("point clone at fresh session server");
    writer
        .execute("UPDATE session_http SET value = 'updated' WHERE id = 1", [])
        .expect("update cloned row");
    writer
        .execute_batch("BEGIN IMMEDIATE;")
        .expect("begin large session update");
    {
        let mut insert = writer
            .prepare("INSERT INTO session_http(id, value) VALUES (?1, ?2)")
            .expect("prepare large session update");
        for id in 2..=5_500_i64 {
            let body = deterministic_payload(id, 2_048);
            insert
                .execute(params![id, &body])
                .expect("insert large session update row");
        }
    }
    writer
        .execute_batch("COMMIT;")
        .expect("commit large session update");
    let _: String = writer
        .query_row("SELECT dolt_commit('-A', '-m', 'update')", [], |row| {
            row.get(0)
        })
        .expect("commit cloned update");
    let push_result: rusqlite::Result<i64> =
        writer.query_row("SELECT dolt_push('origin', 'main')", [], |row| row.get(0));
    if let Err(error) = push_result {
        panic!(
            "push cloned update through session HTTP server: {error:?}; proxy failure: {:?}; last native response error: {:?}; requests: {:?}",
            proxy.failure(),
            proxy.last_native_error(),
            proxy.request_log()
        );
    }
    drop(writer);
    let local_database_bytes = fs::metadata(&clone_path)
        .expect("inspect physical cloned database size")
        .len();
    assert!(
        local_database_bytes > SESSION_CACHE_BYTES as u64,
        "physical cloned database must exceed the dedicated cache capacity: {local_database_bytes}"
    );
    let cache_file_size = fs::metadata(cache_dir.path().join("cachefile.bcv"))
        .expect("inspect session HTTP cache file")
        .len();
    assert!(
        cache_file_size <= SESSION_CACHE_BYTES as u64,
        "session HTTP cache file exceeded its configured capacity: {cache_file_size}"
    );
    let published_manifest = fetch_session_object(
        backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    let checkpoint_prefix = format!("{prefix}/bcv-session/v1/checkpoint/{session_id}/");
    let checkpoint_listing = list_session_objects(backend, &endpoint, &bucket, &checkpoint_prefix);
    let head_name = format!("{prefix}/bcv-session/v1/head/{session_id}.bcv");
    let head_record = fetch_session_object(backend, &endpoint, &bucket, &head_name);
    const SESSION_RECORD_HEADER_BYTES: usize = 168;
    const SESSION_RECORD_TRAILER_BYTES: usize = 32;
    assert!(
        head_record.len() >= SESSION_RECORD_HEADER_BYTES + SESSION_RECORD_TRAILER_BYTES,
        "committed session head record is truncated"
    );
    assert_eq!(
        head_record[10], 2,
        "session head object must contain a HEAD record"
    );
    assert_eq!(
        head_record[11], 4,
        "session head must be durably committed before selecting its checkpoint"
    );
    let head_body_length = be_u32(&head_record, 160) as usize;
    assert_eq!(
        head_record.len(),
        SESSION_RECORD_HEADER_BYTES + head_body_length + SESSION_RECORD_TRAILER_BYTES,
        "committed session head record length is inconsistent"
    );
    let head_body =
        &head_record[SESSION_RECORD_HEADER_BYTES..SESSION_RECORD_HEADER_BYTES + head_body_length];
    assert!(
        head_body.starts_with(b"BCVHD02\0"),
        "committed session head must use the version 2 body format"
    );
    assert!(
        head_body.len() >= 116,
        "committed session head body is truncated"
    );
    assert_eq!(
        be_u32(head_body, 8),
        0,
        "version 2 session head reserved field must be zero"
    );
    let history_count = be_u32(head_body, 108) as usize;
    let history_bytes = history_count
        .checked_mul(SESSION_RECORD_TRAILER_BYTES)
        .expect("session head history length overflow");
    let etag_start = 116usize
        .checked_add(history_bytes)
        .expect("session head ETag offset overflow");
    assert!(
        etag_start <= head_body.len(),
        "version 2 session head history exceeds its body"
    );
    let etag_length = be_u32(head_body, 112) as usize;
    assert_eq!(
        head_body.len(),
        etag_start + etag_length,
        "version 2 session head body length is inconsistent"
    );
    for history_index in 0..history_count {
        let history_start = 116 + history_index * SESSION_RECORD_TRAILER_BYTES;
        let operation_id = &head_body[history_start..history_start + SESSION_RECORD_TRAILER_BYTES];
        assert!(
            operation_id.iter().any(|byte| *byte != 0),
            "version 2 session head history contains an empty operation ID"
        );
    }
    let checkpoint_hash = &head_body[12..44];
    assert!(
        checkpoint_hash.iter().any(|byte| *byte != 0),
        "committed session head must reference a checkpoint"
    );
    let candidate_hash = &head_body[44..76];
    assert!(
        candidate_hash.iter().any(|byte| *byte != 0),
        "committed session head must reference a candidate manifest"
    );
    let checkpoint_name = format!("{checkpoint_prefix}{}.bcv", hex_bytes(checkpoint_hash));
    assert!(
        checkpoint_listing.contains(&checkpoint_name),
        "committed head checkpoint is absent from the immutable checkpoint listing: {checkpoint_name}"
    );
    assert_ne!(
        baseline_manifest, published_manifest,
        "the explicit final upload must publish the changed manifest"
    );
    let checkpoint_record = fetch_session_object(backend, &endpoint, &bucket, &checkpoint_name);
    let checkpoint_database = reassemble_checkpoint_database(
        backend,
        &endpoint,
        &bucket,
        &prefix,
        &checkpoint_record,
        "session.sqlite",
    );
    assert!(
        checkpoint_database.len() > SESSION_CACHE_BYTES as usize,
        "published checkpoint block payload must exceed the dedicated cache capacity: {}",
        checkpoint_database.len()
    );

    // Replaying the exact application request after a lost response must use
    // the same opaque operation ID. The proxy observes Committed and returns
    // success without invoking /commit or writing another manifest.
    let duplicate_commit = send_http_request_raw(proxy.port, &proxy.last_commit_request());
    assert_eq!(
        http_status(&duplicate_commit),
        200,
        "an exact /commit retry must reconcile as success"
    );
    let after_duplicate_commit = fetch_session_object(
        backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(
        published_manifest, after_duplicate_commit,
        "an exact /commit retry must not publish a second manifest"
    );

    // A fresh independent client uses a new session scope. A committed
    // session is terminal and must not accept post-publication read requests
    // under the original session ID.
    drop(proxy);
    let final_session_id = "550e8400-e29b-41d4-a716-446655440017";
    let final_alias = "session-http-final-alias";
    let final_proxy = SessionHttpProxy::start(
        vfs,
        storage.clone(),
        "session.sqlite",
        final_alias,
        final_session_id,
    );
    let final_dir = tempfile::tempdir().expect("final clone database directory");
    let final_path = final_dir.path().join("final.sqlite");
    let final_clone = Connection::open(&final_path).expect("create final clone database");
    let _: i64 = final_clone
        .query_row(
            "SELECT dolt_clone(?1)",
            params![final_proxy.database_url("session.sqlite")],
            |row| row.get(0),
        )
        .expect("read published update through fresh session server");
    let final_value: String = final_clone
        .query_row("SELECT value FROM session_http WHERE id = 1", [], |row| {
            row.get(0)
        })
        .expect("read published row");
    assert_eq!(final_value, "updated");
    let final_count: i64 = final_clone
        .query_row("SELECT count(*) FROM session_http", [], |row| row.get(0))
        .expect("count published rows");
    assert_eq!(final_count, 5_500);
    drop(final_clone);
    let after_final_read_manifest = fetch_session_object(
        backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(published_manifest, after_final_read_manifest);

    // The old operation ID is now stale and must not be replayed against the
    // newer accepted/committed head.
    drop(final_proxy);
    let stale = try_start_http_session_server_with_storage(
        vfs,
        storage,
        alias,
        session_id,
        session_operation_id(1),
    );
    match stale {
        Ok(mut stale) => {
            let stale_status = stale.operation_status().expect("stale operation status");
            assert_eq!(stale_status, SessionOperationStatus::Conflict);
            assert!(!should_invoke_mutating_handler(stale_status));
            stale.quiesce().expect("quiesce stale operation");
        }
        Err(rusqlite::Error::SqliteFailure(error, _)) => {
            assert!(
                matches!(
                    error.extended_code,
                    rusqlite::ffi::SQLITE_CONSTRAINT | rusqlite::ffi::SQLITE_AUTH | 412
                ),
                "stale operation returned an unexpected code: {error:?}"
            );
        }
        Err(error) => panic!("stale operation returned an unexpected error: {error:?}"),
    }
}

#[cfg(feature = "remote")]
const GENERIC_SESSION_ID: &str = "550e8400-e29b-41d4-a716-446655440016";

#[cfg(feature = "remote")]
const GENERIC_SESSION_ALIAS: &str = "session-generic-alias";

#[cfg(feature = "remote")]
const GENERIC_READER_SESSION_ID: &str = "550e8400-e29b-41d4-a716-446655440017";

#[cfg(feature = "remote")]
const GENERIC_READER_SESSION_ALIAS: &str = "session-generic-reader-alias";

#[cfg(feature = "remote")]
const GENERIC_SESSION_OPERATION: u8 = 7;

#[cfg(feature = "remote")]
fn generic_phase_inputs() -> Option<(String, PathBuf, String, String, String, PathBuf)> {
    Some((
        std::env::var("BCV_GENERIC_BACKEND").ok()?,
        PathBuf::from(std::env::var_os("BCV_GENERIC_CACHE")?),
        std::env::var("BCV_GENERIC_ENDPOINT").ok()?,
        std::env::var("BCV_GENERIC_BUCKET").ok()?,
        std::env::var("BCV_GENERIC_PREFIX").ok()?,
        PathBuf::from(std::env::var_os("BCV_GENERIC_STATE")?),
    ))
}

#[cfg(feature = "remote")]
fn generic_vfs(cache: &Path) -> &'static BlockCacheVfs {
    BlockCacheVfs::builder(cache)
        .expect("generic VFS builder")
        .auth_callback(|provider, _account, _container| {
            if provider.starts_with("google?") {
                Ok("test-token".into())
            } else {
                Ok("test".into())
            }
        })
        .init()
        .expect("initialize generic block-cache VFS")
}

#[cfg(feature = "remote")]
fn run_generic_phase_one() {
    let Some((backend, cache, endpoint, bucket, prefix, state)) = generic_phase_inputs() else {
        return;
    };
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let vfs = generic_vfs(&cache);
    vfs.initialize_container(&storage)
        .expect("initialize generic session container");
    let seed_dir = tempfile::tempdir().expect("generic seed directory");
    let seed_path = seed_dir.path().join("seed.sqlite");
    let seed = Connection::open(&seed_path).expect("create generic seed database");
    seed.execute_batch(
        "CREATE TABLE generic_session(value TEXT NOT NULL);
         INSERT INTO generic_session VALUES ('seed');",
    )
    .expect("seed generic database");
    seed.close().expect("close generic seed database");
    vfs.create_database(&storage, &seed_path, "session.sqlite")
        .expect("upload generic seed database");
    let before = fetch_session_object(
        &backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    let checkpoint_prefix = format!("{prefix}/bcv-session/v1/checkpoint/{GENERIC_SESSION_ID}/");

    let mut server = start_http_session_server_with_storage(
        vfs,
        storage.clone(),
        GENERIC_SESSION_ALIAS,
        GENERIC_SESSION_ID,
        session_operation_id(GENERIC_SESSION_OPERATION),
    );
    let database = vfs
        .open(format!("/{GENERIC_SESSION_ALIAS}/session.sqlite"))
        .expect("open generic session database");
    database
        .execute_batch(
            "PRAGMA synchronous=OFF;
             INSERT INTO generic_session VALUES ('no-xsync');",
        )
        .expect("write generic session without xSync");
    drop(database);
    let checkpoints_before_close_out =
        list_session_objects(&backend, &endpoint, &bucket, &checkpoint_prefix);
    assert!(
        !session_listing_contains_object_prefix(
            &backend,
            &checkpoints_before_close_out,
            &checkpoint_prefix,
        ),
        "synchronous=OFF write and connection close must not create an xSync checkpoint: {checkpoints_before_close_out}"
    );
    server
        .complete_request()
        .expect("checkpoint and accept generic request");
    assert_eq!(
        server
            .operation_status()
            .expect("generic accepted request status"),
        SessionOperationStatus::Accepted
    );
    let checkpoints_after_close_out =
        list_session_objects(&backend, &endpoint, &bucket, &checkpoint_prefix);
    assert!(
        session_listing_contains_object_prefix(
            &backend,
            &checkpoints_after_close_out,
            &checkpoint_prefix,
        ),
        "complete_request must create an accepted checkpoint object: {checkpoints_after_close_out}"
    );
    let after_checkpoint = fetch_session_object(
        &backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(before, after_checkpoint);
    fs::write(&state, before).expect("persist generic pre-publication manifest");
}

#[cfg(feature = "remote")]
fn run_generic_phase_two() {
    let Some((backend, cache, endpoint, bucket, prefix, state)) = generic_phase_inputs() else {
        return;
    };
    let before = fs::read(&state).expect("read generic pre-publication manifest");
    let vfs = generic_vfs(&cache);
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let mut server = start_http_session_server_with_storage(
        vfs,
        storage.clone(),
        GENERIC_SESSION_ALIAS,
        GENERIC_SESSION_ID,
        session_operation_id(GENERIC_SESSION_OPERATION.wrapping_add(1)),
    );
    assert_eq!(
        server.operation_status().expect("generic fresh status"),
        SessionOperationStatus::New
    );
    let database = vfs
        .open(format!("/{GENERIC_SESSION_ALIAS}/session.sqlite"))
        .expect("rehydrate generic session database");
    let count: i64 = database
        .query_row(
            "SELECT count(*) FROM generic_session WHERE value = 'no-xsync'",
            [],
            |row| row.get(0),
        )
        .expect("read generic no-xSync row");
    assert_eq!(count, 1);
    drop(database);
    let before_upload = fetch_session_object(
        &backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(before, before_upload);
    server
        .upload()
        .expect("publish generic final request directly");
    let published = fetch_session_object(
        &backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_ne!(before, published);
    fs::write(state.with_extension("published"), published)
        .expect("persist generic published manifest");
}

#[cfg(feature = "remote")]
fn run_generic_phase_three() {
    let Some((backend, cache, endpoint, bucket, prefix, state)) = generic_phase_inputs() else {
        return;
    };
    let expected =
        fs::read(state.with_extension("published")).expect("read generic published manifest");
    let actual = fetch_session_object(
        &backend,
        &endpoint,
        &bucket,
        &format!("{prefix}/manifest.bcv"),
    );
    assert_eq!(expected, actual);
    let vfs = generic_vfs(&cache);
    let storage = session_storage(&backend, &endpoint, &bucket, &prefix);
    let mut server = start_http_session_server_with_storage(
        vfs,
        storage,
        GENERIC_READER_SESSION_ALIAS,
        GENERIC_READER_SESSION_ID,
        session_operation_id(GENERIC_SESSION_OPERATION.wrapping_add(2)),
    );
    assert_eq!(
        server.operation_status().expect("generic fresh status"),
        SessionOperationStatus::New
    );
    let database = vfs
        .open(format!("/{GENERIC_READER_SESSION_ALIAS}/session.sqlite"))
        .expect("open generic published database");
    let count: i64 = database
        .query_row(
            "SELECT count(*) FROM generic_session WHERE value = 'no-xsync'",
            [],
            |row| row.get(0),
        )
        .expect("read generic published row");
    assert_eq!(count, 1);
    drop(database);
    server.quiesce().expect("quiesce generic read server");
}

#[cfg(feature = "remote")]
fn run_generic_process_flow(backend: &str) {
    let endpoint = match backend {
        "google" => std::env::var("BLOCKCACHEVFS_GOOGLE_JSON_ENDPOINT")
            .or_else(|_| std::env::var("BLOCKCACHEVFS_GCS_EMULATOR"))
            .unwrap_or_else(|_| "http://127.0.0.1:19025".into()),
        "s3" => std::env::var("BLOCKCACHEVFS_S3_EMULATOR")
            .unwrap_or_else(|_| "http://127.0.0.1:4566".into()),
        _ => unreachable!("unknown generic session backend {backend}"),
    };
    let suffix = unique_suffix();
    let bucket = if backend == "google" {
        "app_storage".to_owned()
    } else {
        format!("rs-{}", suffix.trim_start_matches("rust-bootstrap-"))
    };
    let prefix = format!("{suffix}/session-generic");
    if backend == "google" {
        ensure_google_bucket(&endpoint, &bucket);
    } else {
        ensure_s3_bucket(&endpoint, &bucket);
    }
    let root = tempfile::tempdir().expect("generic process-flow directory");
    let cache_one = root.path().join("cache-one");
    let cache_two = root.path().join("cache-two");
    let cache_three = root.path().join("cache-three");
    let state = root.path().join("state.bin");
    let executable = std::env::current_exe().expect("locate emulator test executable");
    for (phase, cache) in [
        ("generic_session_phase_one", &cache_one),
        ("generic_session_phase_two", &cache_two),
        ("generic_session_phase_three", &cache_three),
    ] {
        fs::create_dir(cache).expect("create generic child cache directory");
        let status = Command::new(&executable)
            .args(["--ignored", "--exact", phase, "--nocapture"])
            .env("BCV_GENERIC_BACKEND", backend)
            .env("BCV_GENERIC_CACHE", cache)
            .env("BCV_GENERIC_ENDPOINT", &endpoint)
            .env("BCV_GENERIC_BUCKET", &bucket)
            .env("BCV_GENERIC_PREFIX", &prefix)
            .env("BCV_GENERIC_STATE", &state)
            .status()
            .unwrap_or_else(|error| panic!("spawn {phase}: {error}"));
        assert!(
            status.success(),
            "generic child phase {phase} failed: {status}"
        );
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

#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_ownership() {
    run_session_ownership();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_server() {
    run_session_server();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_http_flow() {
    run_session_http_flow("google");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn s3_emulator_session_http_flow() {
    run_session_http_flow("s3");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_failure_windows() {
    run_session_fault_matrix("google");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn s3_emulator_session_failure_windows() {
    run_session_fault_matrix("s3");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_crash_windows() {
    run_session_crash_matrix("google");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn s3_emulator_session_crash_windows() {
    run_session_crash_matrix("s3");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for session crash-window test"]
fn session_crash_child() {
    run_session_crash_child();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child recovery phase for session crash-window test"]
fn session_crash_recovery_child() {
    run_session_crash_recovery_child();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_concurrency_matrix() {
    run_session_concurrency_matrix();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for same-session process race test"]
fn same_session_process_race_child() {
    run_same_session_process_child();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for independent publication race test"]
fn independent_session_publication_prepare_child() {
    run_publication_prepare_child();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for independent publication race test"]
fn independent_session_publication_upload_child() {
    run_publication_upload_child();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for the slow session HTTP flow test"]
fn session_http_flow_child() {
    let Some(backend) = std::env::var_os("BCV_SESSION_HTTP_BACKEND") else {
        return;
    };
    let backend = backend
        .into_string()
        .expect("session HTTP flow backend must be valid UTF-8");
    std::env::var("DOLTLITE_HTTP_TIMEOUT_MS")
        .expect("session HTTP flow child requires its configured client timeout");
    run_session_http_flow_in_process(&backend);
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for generic session process-flow test"]
fn generic_session_phase_one() {
    run_generic_phase_one();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for generic session process-flow test"]
fn generic_session_phase_two() {
    run_generic_phase_two();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "child phase for generic session process-flow test"]
fn generic_session_phase_three() {
    run_generic_phase_three();
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn google_json_emulator_session_generic_process_flow() {
    run_generic_process_flow("google");
}

#[cfg(feature = "remote")]
#[test]
#[ignore = "requires the pinned local emulator containers"]
fn s3_emulator_session_generic_process_flow() {
    run_generic_process_flow("s3");
}

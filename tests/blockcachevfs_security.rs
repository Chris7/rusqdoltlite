#![cfg(feature = "blockcachevfs")]

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::process::Command;
use std::ptr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use rusqlite::blockcachevfs::{s3_secret_with_session_token, AttachSpec, BlockCacheVfs, Config};
use rusqlite::ffi::bcvutil as raw_util;

unsafe extern "C" {
    fn sqlite3_bcv_config(handle: *mut raw_util::sqlite3_bcv, op: c_int, ...) -> c_int;
}

const CHILD_ENV: &str = "RUSQ_DOLTLITE_VERBOSE_SECURITY_CHILD";
const ENDPOINT_ENV: &str = "RUSQ_DOLTLITE_VERBOSE_SECURITY_ENDPOINT";
const ROUTE_CHILD_ENV: &str = "RUSQ_DOLTLITE_S3_ROUTE_CHILD";
const PROXY_ENV: &str = "RUSQ_DOLTLITE_S3_ROUTE_PROXY";
const ACCESS_KEY: &str = "access-key-canary-verbose";
const SECRET_KEY: &str = "secret-key-canary-verbose";
const SESSION_TOKEN: &str = "session-token-canary-verbose";

unsafe extern "C" fn route_log(ctx: *mut c_void, message: *const c_char) {
    let messages = &mut *ctx.cast::<Vec<String>>();
    if !message.is_null() {
        messages.push(CStr::from_ptr(message).to_string_lossy().into_owned());
    }
}

fn route_child() {
    let _proxy = std::env::var(PROXY_ENV).expect("S3 route proxy");
    for (bucket, expected_uri) in [
        (
            "my.bucket",
            "https://s3.us-west-2.amazonaws.com/my.bucket/manifest.bcv",
        ),
        (
            "ordinary-bucket",
            "https://ordinary-bucket.s3.us-west-2.amazonaws.com/manifest.bcv",
        ),
    ] {
        let module = CString::new("s3?region=us-west-2").expect("module selector");
        let account = CString::new("route-test-access").expect("access key");
        let secret = CString::new("route-test-secret").expect("secret key");
        let bucket = CString::new(bucket).expect("bucket");
        let mut handle = ptr::null_mut();
        let rc = unsafe {
            raw_util::sqlite3_bcv_open(
                module.as_ptr(),
                account.as_ptr(),
                secret.as_ptr(),
                bucket.as_ptr(),
                &mut handle,
            )
        };
        assert_eq!(rc, 0, "S3 handle must open: {bucket:?}");
        assert!(!handle.is_null());

        let mut messages: Vec<String> = Vec::new();
        let rc = unsafe {
            sqlite3_bcv_config(
                handle,
                3,
                (&mut messages as *mut Vec<String>).cast::<c_void>(),
                route_log as unsafe extern "C" fn(*mut c_void, *const c_char),
            )
        };
        assert_eq!(rc, 0, "S3 log callback must configure");
        let _ = unsafe { raw_util::sqlite3_bcv_create_if_not_exists(handle, 0, 0) };
        unsafe { raw_util::sqlite3_bcv_close(handle) };

        assert!(
            messages
                .iter()
                .any(|message| message.contains(expected_uri)),
            "request URI for {bucket:?} was not logged as expected: {messages:?}"
        );
    }
}

fn verbose_child() {
    let endpoint = std::env::var(ENDPOINT_ENV).expect("security test endpoint");
    let cache = tempfile::tempdir().expect("temporary CBS cache directory");
    let vfs = BlockCacheVfs::builder(cache.path())
        .expect("VFS builder")
        .auth_callback(|_, _, _| {
            s3_secret_with_session_token(SECRET_KEY, SESSION_TOKEN)
                .map_err(|error| rusqlite::blockcachevfs::AuthError(error.to_string()))
        })
        .config(Config::CurlVerbose(true))
        .init()
        .expect("initialize block-cache VFS");

    let result = vfs.attach(
        &AttachSpec::s3_with_endpoint(ACCESS_KEY, "bucket", "us-east-1", endpoint)
            .alias("security"),
    );
    assert!(
        result.is_err(),
        "the test server intentionally rejects attach"
    );
}

#[test]
fn curl_verbose_does_not_log_s3_credentials() {
    if std::env::var_os(CHILD_ENV).is_some() {
        verbose_child();
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("local HTTP listener");
    let endpoint = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let server = thread::spawn(move || {
        listener
            .set_nonblocking(true)
            .expect("nonblocking HTTP listener");
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let (mut stream, _) = loop {
            match listener.accept() {
                Ok(connection) => break connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "CBS did not issue a request within 10 seconds"
                    );
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => panic!("security test HTTP request: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let n = stream.read(&mut buffer).expect("HTTP request");
            if n == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..n]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .expect("HTTP response");
        String::from_utf8_lossy(&request).into_owned()
    });

    let output = Command::new(std::env::current_exe().expect("test executable path"))
        .args([
            "--exact",
            "curl_verbose_does_not_log_s3_credentials",
            "--nocapture",
        ])
        .env(CHILD_ENV, "1")
        .env(ENDPOINT_ENV, endpoint)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .output()
        .expect("run isolated verbose child test");
    let request = server.join().expect("HTTP server thread");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "isolated verbose child failed\nstdout:\n{}\nstderr:\n{stderr}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        stderr.contains("* ") || stderr.contains("< "),
        "verbose diagnostics were not emitted:\n{stderr}"
    );
    assert!(request.starts_with("GET /bucket/manifest.bcv"), "{request}");
    assert!(
        request.contains(&format!("x-amz-security-token: {SESSION_TOKEN}")),
        "the request did not carry the session token:\n{request}"
    );
    assert!(
        request.contains(&format!(
            "Authorization: AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/"
        )),
        "the request did not carry SigV4 authorization:\n{request}"
    );
    for canary in [ACCESS_KEY, SECRET_KEY, SESSION_TOKEN] {
        assert!(
            !stderr.contains(canary),
            "verbose diagnostics leaked credential canary {canary:?}:\n{stderr}"
        );
    }
}

#[test]
fn dotted_aws_bucket_uses_tls_valid_path_style_host() {
    if std::env::var_os(ROUTE_CHILD_ENV).is_some() {
        route_child();
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("local HTTPS proxy listener");
    let proxy = format!(
        "http://{}",
        listener.local_addr().expect("proxy listener address")
    );
    let stop = Arc::new(AtomicBool::new(false));
    let server_stop = Arc::clone(&stop);
    let server = thread::spawn(move || {
        listener
            .set_nonblocking(true)
            .expect("nonblocking HTTPS proxy listener");
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        while !server_stop.load(Ordering::Relaxed) {
            if std::time::Instant::now() >= deadline {
                break;
            }
            let Ok((mut stream, _)) = listener.accept() else {
                thread::sleep(Duration::from_millis(10));
                continue;
            };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            loop {
                let Ok(n) = stream.read(&mut buffer) else {
                    break;
                };
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..n]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let _ = stream.write_all(
                b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });

    let output = Command::new(std::env::current_exe().expect("test executable path"))
        .args([
            "--exact",
            "dotted_aws_bucket_uses_tls_valid_path_style_host",
            "--nocapture",
        ])
        .env(ROUTE_CHILD_ENV, "1")
        .env(PROXY_ENV, &proxy)
        .env("HTTPS_PROXY", &proxy)
        .env("https_proxy", &proxy)
        .env("HTTP_PROXY", &proxy)
        .env("http_proxy", &proxy)
        .env("ALL_PROXY", "")
        .env("all_proxy", "")
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .output()
        .expect("run isolated S3 routing child test");
    stop.store(true, Ordering::Relaxed);
    server.join().expect("HTTPS proxy thread");

    assert!(
        output.status.success(),
        "isolated S3 routing child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

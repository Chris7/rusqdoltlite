#![cfg(feature = "blockcachevfs")]

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::fs;
use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::ptr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::blockcachevfs::{s3_secret_with_session_token, AttachSpec, BlockCacheVfs, Config};
use rusqlite::ffi::bcvutil as raw_util;

unsafe extern "C" {
    fn sqlite3_bcv_config(handle: *mut raw_util::sqlite3_bcv, op: c_int, ...) -> c_int;
}

const CHILD_ENV: &str = "RUSQ_DOLTLITE_VERBOSE_SECURITY_CHILD";
const ENDPOINT_ENV: &str = "RUSQ_DOLTLITE_VERBOSE_SECURITY_ENDPOINT";
const ROUTE_CHILD_ENV: &str = "RUSQ_DOLTLITE_S3_ROUTE_CHILD";
const PROXY_ENV: &str = "RUSQ_DOLTLITE_S3_ROUTE_PROXY";
const ROUTE_BUCKET_ENV: &str = "RUSQ_DOLTLITE_S3_ROUTE_BUCKET";
const ACCESS_KEY: &str = "access-key-canary-verbose";
const SECRET_KEY: &str = "secret-key-canary-verbose";
const SESSION_TOKEN: &str = "session-token-canary-verbose";

const TLS_CHILD_ENV: &str = "RUSQ_DOLTLITE_TLS_TRUST_CHILD";
const TLS_ENDPOINT_ENV: &str = "RUSQ_DOLTLITE_TLS_TRUST_ENDPOINT";

const TLS_SERVER_SCRIPT: &str = r#"
import pathlib
import socket
import ssl
import sys

cert, key, ready, seen = sys.argv[1:]
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", 0))
listener.listen(1)
listener.settimeout(12)
pathlib.Path(ready).write_text(str(listener.getsockname()[1]))
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(certfile=cert, keyfile=key)
try:
    raw, _ = listener.accept()
    try:
        with context.wrap_socket(raw, server_side=True) as stream:
            request = stream.recv(4096)
            if request:
                pathlib.Path(seen).write_bytes(request)
                stream.sendall(
                    b"HTTP/1.1 404 Not Found\r\n"
                    b"Content-Length: 0\r\n"
                    b"Connection: close\r\n\r\n"
                )
    except (ssl.SSLError, OSError):
        pass
finally:
    listener.close()
"#;

fn tls_trust_child() {
    let endpoint = std::env::var(TLS_ENDPOINT_ENV).expect("should have a TLS test endpoint");
    let cache = tempfile::tempdir().expect("should create a temporary CBS cache directory");
    let vfs = BlockCacheVfs::builder(cache.path())
        .expect("should create a VFS builder")
        .auth_callback(|_, _, _| Ok("tls-test-secret".to_owned()))
        .init()
        .expect("should initialize the block-cache VFS");
    let result = vfs.attach(
        &AttachSpec::s3_with_endpoint("tls-test-access", "bucket", "us-east-1", endpoint)
            .alias("tls"),
    );
    let error = result.expect_err("should reject the fixture's missing manifest");
    eprintln!("TLS attachment error: {error:?}");
}

fn create_tls_test_certificates(directory: &Path) -> PathBuf {
    let root_key = directory.join("ca.key");
    let root_cert = directory.join("ca.pem");
    let server_key = directory.join("server.key");
    let server_csr = directory.join("server.csr");
    let server_cert = directory.join("server.pem");
    let extensions = directory.join("server.ext");

    let root = Command::new("openssl")
        .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout"])
        .arg(&root_key)
        .args(["-out"])
        .arg(&root_cert)
        .args([
            "-days",
            "3650",
            "-subj",
            "/CN=RusqDoltLite blockcache test CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ])
        .output()
        .expect("openssl must be installed for the local TLS verification test");
    assert!(
        root.status.success(),
        "create test CA: {}",
        String::from_utf8_lossy(&root.stderr)
    );

    let request = Command::new("openssl")
        .args(["req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout"])
        .arg(&server_key)
        .args(["-out"])
        .arg(&server_csr)
        .args(["-subj", "/CN=localhost"])
        .output()
        .expect("openssl must be installed for the local TLS verification test");
    assert!(
        request.status.success(),
        "create test server key: {}",
        String::from_utf8_lossy(&request.stderr)
    );

    fs::write(
        &extensions,
        "subjectAltName=DNS:localhost\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n",
    )
    .expect("should write server certificate extensions");
    let signed = Command::new("openssl")
        .args(["x509", "-req", "-in"])
        .arg(&server_csr)
        .args(["-CA"])
        .arg(&root_cert)
        .args(["-CAkey"])
        .arg(&root_key)
        .args(["-CAcreateserial", "-out"])
        .arg(&server_cert)
        .args(["-days", "3650", "-sha256", "-extfile"])
        .arg(&extensions)
        .output()
        .expect("openssl must be installed for the local TLS verification test");
    assert!(
        signed.status.success(),
        "sign test server certificate: {}",
        String::from_utf8_lossy(&signed.stderr)
    );

    root_cert
}

fn wait_for_process(mut child: Child, description: &str, timeout: Duration) -> Output {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .unwrap_or_else(|error| panic!("wait for {description}: {error}"))
        {
            let output = child
                .wait_with_output()
                .unwrap_or_else(|error| panic!("collect {description} output: {error}"));
            assert_eq!(output.status, status);
            return output;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .unwrap_or_else(|error| panic!("stop timed out {description}: {error}"));
            panic!(
                "{description} exceeded {timeout:?}; stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_tls_server(child: &mut Child, ready: &Path) -> u16 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if ready.is_file() {
            return fs::read_to_string(ready)
                .expect("read TLS test server port")
                .parse()
                .expect("should parse the TLS test server port");
        }
        if let Some(status) = child
            .try_wait()
            .expect("should check TLS test server startup")
        {
            panic!("TLS test server exited during startup with {status}");
        }
        assert!(Instant::now() < deadline, "TLS test server did not start");
        thread::sleep(Duration::from_millis(10));
    }
}

fn verify_tls_case(
    directory: &Path,
    name: &str,
    host: &str,
    ca_file: Option<&Path>,
    expect_request: bool,
) {
    verify_tls_case_with_ca_environment(
        directory,
        name,
        host,
        ca_file.map(|path| ("SSL_CERT_FILE", path)),
        expect_request,
        "bundled_curl_preserves_tls_verification_and_custom_ca",
    );
}

fn verify_tls_case_with_ca_environment(
    directory: &Path,
    name: &str,
    host: &str,
    ca_environment: Option<(&str, &Path)>,
    expect_request: bool,
    child_test_name: &str,
) {
    let ready = directory.join(format!("{name}.port"));
    let seen = directory.join(format!("{name}.request"));
    let mut server = Command::new("python3")
        .args(["-c", TLS_SERVER_SCRIPT])
        .arg(directory.join("server.pem"))
        .arg(directory.join("server.key"))
        .arg(&ready)
        .arg(&seen)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("should have Python 3 installed for the local TLS test server");
    let port = wait_for_tls_server(&mut server, &ready);
    let endpoint = format!("https://{host}:{port}");
    let executable = std::env::current_exe().expect("should locate the TLS child test executable");
    let mut client_command = Command::new(executable);
    client_command
        .args(["--exact", child_test_name, "--nocapture"])
        .env(TLS_CHILD_ENV, "1")
        .env(TLS_ENDPOINT_ENV, endpoint)
        .env("NO_PROXY", "localhost,127.0.0.1")
        .env("no_proxy", "localhost,127.0.0.1")
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy")
        .env_remove("CLOUDSQLITE_CAINFO")
        .env_remove("SSL_CERT_FILE")
        .env_remove("SSL_CERT_DIR");
    if let Some((environment, path)) = ca_environment {
        client_command.env(environment, path);
    }
    let client = client_command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("should spawn the isolated TLS trust child");
    let client_output = wait_for_process(
        client,
        &format!("{name} TLS child"),
        Duration::from_secs(20),
    );
    let server_output = wait_for_process(
        server,
        &format!("{name} TLS server"),
        Duration::from_secs(15),
    );
    assert!(
        client_output.status.success(),
        "{name} TLS child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&client_output.stdout),
        String::from_utf8_lossy(&client_output.stderr)
    );
    assert!(
        server_output.status.success(),
        "{name} TLS server failed: {}",
        String::from_utf8_lossy(&server_output.stderr)
    );
    let request = if seen.is_file() {
        fs::read_to_string(&seen).expect("should read the TLS server request")
    } else {
        String::new()
    };
    assert_eq!(
        seen.is_file(),
        expect_request,
        "{name} TLS case reached the HTTP request phase: {request}"
    );
}

unsafe extern "C" fn route_log(ctx: *mut c_void, message: *const c_char) {
    let _ = ctx;
    if !message.is_null() {
        let message = CStr::from_ptr(message).to_string_lossy();
        let mut stdout = std::io::stdout().lock();
        let _ = writeln!(stdout, "{message}");
        let _ = stdout.flush();
    }
    // The callback runs immediately before CBS hands the request to libcurl.
    // Exit after recording that URI so this test cannot contact AWS or spend
    // time in CBS's retry loop. The parent still places a local proxy in the
    // HTTPS_PROXY slot as a guard against an unexpected transfer.
    std::process::exit(0);
}

fn route_child() {
    let _proxy = std::env::var(PROXY_ENV).expect("S3 route proxy");
    let bucket =
        CString::new(std::env::var(ROUTE_BUCKET_ENV).expect("S3 route bucket")).expect("bucket");
    let module = CString::new("s3?region=us-west-2").expect("module selector");
    let account = CString::new("route-test-access").expect("access key");
    let secret = CString::new("route-test-secret").expect("secret key");
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

    let rc = unsafe {
        sqlite3_bcv_config(
            handle,
            3,
            ptr::null_mut::<c_void>(),
            route_log as unsafe extern "C" fn(*mut c_void, *const c_char),
        )
    };
    assert_eq!(rc, 0, "S3 log callback must configure");
    let _ = unsafe { raw_util::sqlite3_bcv_create_if_not_exists(handle, 0, 0) };
    unreachable!("the log callback must terminate the route child");
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
        let response = format!(
            "HTTP/1.1 404 Not Found\r\n\
             Authorization: {ACCESS_KEY}\r\n\
             x-amz-security-token: {SESSION_TOKEN}\r\n\
             x-secret-key: {SECRET_KEY}\r\n\
             Content-Length: 0\r\n\
             Connection: close\r\n\r\n"
        );
        stream
            .write_all(response.as_bytes())
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
        stderr.contains("* "),
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
fn bundled_curl_preserves_tls_verification_and_custom_ca() {
    if std::env::var_os(TLS_CHILD_ENV).is_some() {
        tls_trust_child();
        return;
    }

    let directory = tempfile::tempdir().expect("should create the TLS test directory");
    let ca_file = create_tls_test_certificates(directory.path());
    for (name, host, trust_file, expect_request) in [
        ("trusted", "localhost", Some(ca_file.as_path()), true),
        ("untrusted", "localhost", None, false),
        ("wrong-host", "127.0.0.1", Some(ca_file.as_path()), false),
    ] {
        verify_tls_case(directory.path(), name, host, trust_file, expect_request);
    }
}

#[cfg(unix)]
#[test]
fn bundled_curl_honors_ssl_cert_dir() {
    if std::env::var_os(TLS_CHILD_ENV).is_some() {
        tls_trust_child();
        return;
    }

    let directory = tempfile::tempdir().expect("should create the TLS test directory");
    let ca_file = create_tls_test_certificates(directory.path());
    let hash = Command::new("openssl")
        .args(["x509", "-hash", "-noout", "-in"])
        .arg(&ca_file)
        .output()
        .expect("should have openssl installed for the local TLS verification test");
    assert!(
        hash.status.success(),
        "calculate the test CA subject hash: {}",
        String::from_utf8_lossy(&hash.stderr)
    );
    let ca_directory = directory.path().join("ca-directory");
    fs::create_dir(&ca_directory).expect("should create the OpenSSL CA directory");
    fs::copy(
        &ca_file,
        ca_directory.join(format!(
            "{}.0",
            String::from_utf8_lossy(&hash.stdout).trim()
        )),
    )
    .expect("should copy the test CA under its OpenSSL subject hash");

    let empty_ca_directory = directory.path().join("empty-ca-directory");
    fs::create_dir(&empty_ca_directory).expect("should create an empty CA directory");

    verify_tls_case_with_ca_environment(
        directory.path(),
        "ssl-cert-dir",
        "localhost",
        Some(("SSL_CERT_DIR", &ca_directory)),
        true,
        "bundled_curl_honors_ssl_cert_dir",
    );
    verify_tls_case_with_ca_environment(
        directory.path(),
        "ssl-cert-dir-untrusted",
        "localhost",
        Some(("SSL_CERT_DIR", &empty_ca_directory)),
        false,
        "bundled_curl_honors_ssl_cert_dir",
    );
    verify_tls_case_with_ca_environment(
        directory.path(),
        "ssl-cert-dir-wrong-host",
        "127.0.0.1",
        Some(("SSL_CERT_DIR", &ca_directory)),
        false,
        "bundled_curl_honors_ssl_cert_dir",
    );
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
    let proxy_seen = Arc::new(AtomicBool::new(false));
    let server_proxy_seen = Arc::clone(&proxy_seen);
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
            server_proxy_seen.store(true, Ordering::Relaxed);
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let mut request = Vec::new();
            let mut buffer = [0u8; 1024];
            while let Ok(n) = stream.read(&mut buffer) {
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

    let executable = std::env::current_exe().expect("test executable path");
    let run_child = |bucket: &str| {
        Command::new(&executable)
            .args([
                "--exact",
                "dotted_aws_bucket_uses_tls_valid_path_style_host",
                "--nocapture",
            ])
            .env(ROUTE_CHILD_ENV, "1")
            .env(ROUTE_BUCKET_ENV, bucket)
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
            .expect("run isolated S3 routing child test")
    };
    let dotted = run_child("my.bucket");
    let ordinary = run_child("ordinary-bucket");
    stop.store(true, Ordering::Relaxed);
    server.join().expect("HTTPS proxy thread");

    assert!(
        dotted.status.success(),
        "isolated dotted S3 routing child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&dotted.stdout),
        String::from_utf8_lossy(&dotted.stderr)
    );
    assert!(
        ordinary.status.success(),
        "isolated ordinary S3 routing child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&ordinary.stdout),
        String::from_utf8_lossy(&ordinary.stderr)
    );
    assert!(
        String::from_utf8_lossy(&dotted.stdout)
            .contains("https://s3.us-west-2.amazonaws.com/my.bucket/manifest.bcv"),
        "dotted bucket URI was not logged:\n{}",
        String::from_utf8_lossy(&dotted.stdout)
    );
    assert!(
        String::from_utf8_lossy(&ordinary.stdout)
            .contains("https://ordinary-bucket.s3.us-west-2.amazonaws.com/manifest.bcv"),
        "ordinary bucket URI was not logged:\n{}",
        String::from_utf8_lossy(&ordinary.stdout)
    );
    assert!(
        !proxy_seen.load(Ordering::Relaxed),
        "route URI test unexpectedly attempted a proxy transfer"
    );
}

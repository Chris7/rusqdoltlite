//! In-process DoltLite HTTP remote server.

use crate::error::error_from_sqlite_code;
use crate::{ffi, path_to_cstring, Result};
use std::ffi::{c_int, c_long, CStr, CString};
use std::fmt;
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Configuration for an in-process DoltLite HTTP remote server.
///
/// The default configuration listens on an operating-system-assigned loopback
/// port without TLS or authentication. Use [`RemoteServerOptions::tls`] and
/// [`RemoteServerOptions::authentication`] before exposing the listener beyond
/// a trusted process or network boundary.
#[derive(Clone, Debug)]
pub struct RemoteServerOptions {
    bind_address: String,
    port: u16,
    certificate_file: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
    authorized_keys_directory: Option<PathBuf>,
    audience: Option<String>,
    request_timeout: Option<Duration>,
}

impl RemoteServerOptions {
    /// Returns the loopback-only default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the IPv4 address on which the server listens.
    #[must_use]
    pub fn bind_address(mut self, bind_address: impl Into<String>) -> Self {
        self.bind_address = bind_address.into();
        self
    }

    /// Sets the TCP port. Port `0` asks the operating system to choose one.
    #[must_use]
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Enables native TLS with a PEM certificate chain and private key.
    #[must_use]
    pub fn tls(
        mut self,
        certificate_file: impl Into<PathBuf>,
        private_key_file: impl Into<PathBuf>,
    ) -> Self {
        self.certificate_file = Some(certificate_file.into());
        self.private_key_file = Some(private_key_file.into());
        self
    }

    /// Requires a Dolt-compatible Ed25519 bearer token on every request.
    ///
    /// `authorized_keys_directory` contains public JWK files named
    /// `<key-id>.jwk`. `audience` must be the public remote hostname, including
    /// when TLS is terminated by a reverse proxy. Every listed key receives
    /// access to every database and operation served by this listener; a
    /// multi-tenant host must enforce finer-grained authorization separately.
    #[must_use]
    pub fn authentication(
        mut self,
        authorized_keys_directory: impl Into<PathBuf>,
        audience: impl Into<String>,
    ) -> Self {
        self.authorized_keys_directory = Some(authorized_keys_directory.into());
        self.audience = Some(audience.into());
        self
    }

    /// Sets the total request-read timeout.
    ///
    /// DoltLite's native default is used when this is not configured.
    #[must_use]
    pub fn request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = Some(request_timeout);
        self
    }
}

impl Default for RemoteServerOptions {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1".to_owned(),
            port: 0,
            certificate_file: None,
            private_key_file: None,
            authorized_keys_directory: None,
            audience: None,
            request_timeout: None,
        }
    }
}

/// Verifies Dolt-compatible remote bearer tokens against a local key directory.
///
/// Successful authentication returns the canonical Ed25519 key ID. This is
/// useful for a single node or for a node whose key directory is reconciled by
/// an external control plane. It is not a shared key store: a distributed or
/// serverless gateway should verify the JWT against its authoritative database,
/// KV store, or identity service and then authorize the requested database and
/// operation. This type performs authentication only; it does not implement
/// repository permissions.
#[derive(Debug)]
pub struct RemoteAuthenticator {
    authorized_keys_directory: CString,
    audience: CString,
}

impl RemoteAuthenticator {
    /// Creates a verifier backed by public JWK files in
    /// `authorized_keys_directory`.
    pub fn new<P: AsRef<Path>>(authorized_keys_directory: P, audience: &str) -> Result<Self> {
        let rc = ffi::initialize_doltlite();
        if rc != ffi::SQLITE_OK {
            return Err(remote_server_error(rc, "failed to initialize DoltLite"));
        }
        if audience.is_empty() {
            return Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "remote authentication requires a non-empty audience",
            ));
        }
        Ok(Self {
            authorized_keys_directory: path_to_cstring(authorized_keys_directory.as_ref())?,
            audience: CString::new(audience)?,
        })
    }

    /// Authenticates an HTTP `Authorization` header and returns its key ID.
    pub fn authenticate(&self, authorization: &str) -> Result<String> {
        let authorization = CString::new(authorization)?;
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| {
                remote_server_error(ffi::SQLITE_ERROR, "system clock is before the Unix epoch")
            })?
            .as_secs();
        let now = c_long::try_from(seconds).map_err(|_| {
            remote_server_error(
                ffi::SQLITE_ERROR,
                "system clock does not fit the native time representation",
            )
        })?;
        let mut key_id = ptr::null_mut();
        let rc = unsafe {
            ffi::doltliteCredsVerifyBearer(
                authorization.as_ptr(),
                self.audience.as_ptr(),
                self.authorized_keys_directory.as_ptr(),
                now,
                &mut key_id,
            )
        };
        if rc != 0 {
            return Err(remote_server_error(
                ffi::SQLITE_AUTH,
                "remote bearer token is missing, invalid, expired, or unauthorized",
            ));
        }
        let key_id = NonNull::new(key_id).ok_or_else(|| {
            remote_server_error(
                ffi::SQLITE_ERROR,
                "DoltLite authenticated the token without returning a key ID",
            )
        })?;
        let result = unsafe { CStr::from_ptr(key_id.as_ptr()) }
            .to_str()
            .map(str::to_owned);
        unsafe { ffi::sqlite3_free(key_id.as_ptr().cast()) };
        Ok(result?)
    }
}

fn remote_server_error(code: c_int, message: &str) -> crate::Error {
    error_from_sqlite_code(code, Some(message.to_owned()))
}

/// A running in-process DoltLite HTTP remote server.
///
/// The server runs on a native background thread. Dropping this value stops
/// that thread, waits for it to exit, and releases its native resources.
#[must_use = "dropping the server immediately stops its background thread"]
pub struct RemoteServer {
    raw: NonNull<ffi::DoltliteServer>,
    scheme: &'static str,
    bind_address: String,
    port: u16,
}

impl RemoteServer {
    /// Starts a loopback-only server on an available operating-system-assigned
    /// port.
    ///
    /// `directory` must already exist. Each database is addressed by its file
    /// name beneath that directory, for example `server.database_url("repo.db")`.
    pub fn start<P: AsRef<Path>>(directory: P) -> Result<Self> {
        Self::start_with_options(directory, &RemoteServerOptions::default())
    }

    /// Starts a server on the requested IPv4 address and port.
    ///
    /// Passing port `0` asks the operating system to select an available port.
    /// Binding a non-loopback address without TLS exposes an unencrypted,
    /// unauthenticated server; prefer [`RemoteServer::start`] unless the server
    /// is protected by a trusted network or reverse proxy.
    pub fn start_on<P: AsRef<Path>>(directory: P, bind_address: &str, port: u16) -> Result<Self> {
        let options = RemoteServerOptions::new()
            .bind_address(bind_address)
            .port(port);
        Self::start_with_options(directory, &options)
    }

    /// Starts a server with explicit TLS, authentication, and timeout options.
    ///
    /// Native authentication is a server-wide, filesystem-backed key allowlist.
    /// For per-database permissions or a distributed key store, bind this server
    /// to loopback and put a host-managed gateway in front of it. The gateway
    /// should authenticate against its own authoritative key store and apply
    /// authorization before proxying the request.
    pub fn start_with_options<P: AsRef<Path>>(
        directory: P,
        options: &RemoteServerOptions,
    ) -> Result<Self> {
        let rc = ffi::initialize_doltlite();
        if rc != ffi::SQLITE_OK {
            return Err(error_from_sqlite_code(
                rc,
                Some("failed to initialize DoltLite".to_owned()),
            ));
        }

        let directory = path_to_cstring(directory.as_ref())?;
        let bind_address = CString::new(options.bind_address.as_str())?;
        let certificate_file = options
            .certificate_file
            .as_deref()
            .map(path_to_cstring)
            .transpose()?;
        let private_key_file = options
            .private_key_file
            .as_deref()
            .map(path_to_cstring)
            .transpose()?;
        let authorized_keys_directory = options
            .authorized_keys_directory
            .as_deref()
            .map(path_to_cstring)
            .transpose()?;
        let audience = options.audience.as_deref().map(CString::new).transpose()?;

        if authorized_keys_directory.is_some()
            && options.audience.as_deref().is_none_or(str::is_empty)
        {
            return Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "remote authentication requires a non-empty audience",
            ));
        }

        let timeout_ms = if let Some(timeout) = options.request_timeout {
            let milliseconds = timeout.as_millis();
            if milliseconds == 0 || milliseconds > c_int::MAX as u128 {
                return Err(remote_server_error(
                    ffi::SQLITE_MISUSE,
                    "remote request timeout must be between 1 ms and i32::MAX ms",
                ));
            }
            milliseconds as c_int
        } else {
            0
        };

        let native_options = ffi::DoltliteServeOpts {
            zDir: directory.as_ptr(),
            port: c_int::from(options.port),
            zBindAddr: bind_address.as_ptr(),
            certFile: certificate_file
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr()),
            keyFile: private_key_file
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr()),
            authKeysDir: authorized_keys_directory
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr()),
            audience: audience
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr()),
            timeoutMs: timeout_ms,
        };
        let raw = unsafe { ffi::doltliteServeAsyncOpts(&native_options) };
        let raw = NonNull::new(raw).ok_or_else(|| {
            error_from_sqlite_code(
                ffi::SQLITE_ERROR,
                Some(format!(
                    "failed to start DoltLite remote server on {}:{}",
                    options.bind_address, options.port
                )),
            )
        })?;

        let actual_port = unsafe { ffi::doltliteServerPort(raw.as_ptr()) };
        let actual_port = u16::try_from(actual_port).map_err(|_| {
            unsafe { ffi::doltliteServerStop(raw.as_ptr()) };
            error_from_sqlite_code(
                ffi::SQLITE_ERROR,
                Some("DoltLite remote server returned an invalid port".to_owned()),
            )
        })?;
        if actual_port == 0 {
            unsafe { ffi::doltliteServerStop(raw.as_ptr()) };
            return Err(error_from_sqlite_code(
                ffi::SQLITE_ERROR,
                Some("DoltLite remote server did not bind a port".to_owned()),
            ));
        }

        Ok(Self {
            raw,
            scheme: if certificate_file.is_some() {
                "https"
            } else {
                "http"
            },
            bind_address: options.bind_address.clone(),
            port: actual_port,
        })
    }

    /// Returns the TCP port on which the server is listening.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns an HTTP remote URL for a database file in the served directory.
    #[must_use]
    pub fn database_url(&self, database: &str) -> String {
        format!(
            "{}://{}:{}/{database}",
            self.scheme, self.bind_address, self.port
        )
    }
}

impl fmt::Debug for RemoteServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteServer")
            .field("scheme", &self.scheme)
            .field("bind_address", &self.bind_address)
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl Drop for RemoteServer {
    fn drop(&mut self) {
        unsafe { ffi::doltliteServerStop(self.raw.as_ptr()) }
    }
}

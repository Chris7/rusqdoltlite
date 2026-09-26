//! In-process DoltLite HTTP remote server.

#[cfg(feature = "blockcachevfs")]
use crate::blockcachevfs::{
    session_alias, AttachSpec, BlockCacheVfs, SessionAttachment, SessionOperationId,
    SessionOperationStatus,
};
use crate::error::error_from_sqlite_code;
use crate::{ffi, path_to_cstring, Result};
use std::ffi::{c_int, c_long, CStr, CString};
use std::fmt;
#[cfg(feature = "blockcachevfs")]
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Maximum UTF-8 bytes accepted for one session-scope field.
///
/// This mirrors the limit used by the staged native scope-hash format. The
/// scope is caller-authorized correlation/access context, not an authentication
/// credential. A scoped native attachment binds these fields into its session
/// records and accepted-head fence; the native HTTP server remains
/// route-agnostic and does not perform per-request database or operation
/// authorization.
#[cfg(feature = "blockcachevfs")]
pub const SESSION_SCOPE_MAX_FIELD_BYTES: usize = 4096;

/// Caller-authorized context bound to one session-owned remote server.
///
/// `principal` identifies the already-authenticated caller, `target_database`
/// is the exact database name this context is for, and `operations` is an
/// opaque permitted-operation scope supplied by the caller. The library does
/// not infer permissions from HTTP methods or route names.
#[cfg(feature = "blockcachevfs")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionScope {
    principal: String,
    target_database: String,
    operations: String,
}

#[cfg(feature = "blockcachevfs")]
impl SessionScope {
    /// Validate and construct a caller-authorized session scope.
    pub fn new(
        principal: impl AsRef<str>,
        target_database: impl AsRef<str>,
        operations: impl AsRef<str>,
    ) -> Result<Self> {
        let target_database = validate_scope_field("target database", target_database)?;
        if !is_native_database_name(&target_database) {
            return Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "session target database must be an ASCII DoltLite database name",
            ));
        }
        Ok(Self {
            principal: validate_scope_field("principal", principal)?,
            target_database,
            operations: validate_scope_field("operation scope", operations)?,
        })
    }

    /// Return the authenticated-principal binding supplied by the caller.
    #[must_use]
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// Return the exact target database name.
    #[must_use]
    pub fn target_database(&self) -> &str {
        &self.target_database
    }

    /// Return the opaque permitted-operation scope.
    #[must_use]
    pub fn operations(&self) -> &str {
        &self.operations
    }

    /// Check an exact database-name match without normalizing or interpreting
    /// the caller's value. This is an adapter authorization check; the native
    /// route-agnostic server does not enforce it by itself.
    #[must_use]
    pub fn matches_database(&self, database: &str) -> bool {
        self.target_database == database
    }
}

#[cfg(feature = "blockcachevfs")]
fn validate_scope_field(name: &str, value: impl AsRef<str>) -> Result<String> {
    let value = value.as_ref();
    let invalid = value.is_empty()
        || value.len() > SESSION_SCOPE_MAX_FIELD_BYTES
        || value.bytes().any(|byte| byte < 0x20 || byte == 0x7f);
    if invalid {
        return Err(remote_server_error(
            ffi::SQLITE_MISUSE,
            &format!(
                "session {name} must be non-empty, control-free, and at most {SESSION_SCOPE_MAX_FIELD_BYTES} bytes"
            ),
        ));
    }
    Ok(value.to_owned())
}

#[cfg(feature = "blockcachevfs")]
fn is_native_database_name(value: &str) -> bool {
    !value.starts_with('.')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

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
    vfs_name: Option<String>,
    certificate_file: Option<PathBuf>,
    private_key_file: Option<PathBuf>,
    authorized_keys_directory: Option<PathBuf>,
    audience: Option<String>,
    request_timeout: Option<Duration>,
    #[cfg(feature = "blockcachevfs")]
    blockcache_session: Option<BlockCacheSessionOptions>,
}

/// One-call construction payload for a session-owned block-cache server.
///
/// The VFS handle, storage attachment, session UUID, and caller-authorized
/// Session scope is supplied together by the authorized application context,
/// along with a non-zero per-request operation ID. The native DoltLite server
/// keeps its existing options ABI; the session remains an owning Rust/VFS
/// context.
/// Storage identity is still the native CBS attachment identity: in the S3
/// constructor, `Storage::account` is the access key. Reusing a persisted
/// attachment after that key rotates is intentionally rejected until a fresh
/// VFS/cache can rehydrate credentials; the scope payload does not change that
/// storage-authentication rule.
/// Native scoped attachment rehydrates the accepted session head internally;
/// when no head exists, it selects the zero initial predecessor and sequence.
/// Callers must not provide or treat a client-supplied expected tip as a
/// substitute.
/// Session construction canonicalizes HTTP endpoint trailing slashes and
/// rejects provider defaults or tuning selectors that could spell the same
/// object namespace more than one way. Use the explicit-region S3 and
/// JSON-API Google constructors for session attachments.
/// The scoped native attachment binds the principal, target database, and
/// operation scope into session records and accepted-head validation. Those
/// fields are not, by themselves, a native HTTP authorization policy: native
/// DoltLite remains route-agnostic and can serve any database reachable under
/// the attached alias. Session servers are therefore loopback-only so the
/// authorized application/proxy can enforce the scope before forwarding each
/// request.
/// If the attachment has no explicit alias, the VFS derives
/// `session-<uuid>`. Database names sent to this server must use that alias
/// (or the explicit alias) as their `/{alias}/{database}` prefix. Native
/// DoltLite currently receives a directory rather than a session ID, so the
/// session server requires that directory to be exactly `/{alias}`. This
/// prevents the native server from resolving requests through another alias
/// or an ordinary local path.
#[cfg(feature = "blockcachevfs")]
#[derive(Clone)]
pub struct BlockCacheSessionOptions {
    vfs: &'static BlockCacheVfs,
    attachment: AttachSpec,
    session_id: crate::blockcachevfs::SessionId,
    scope: SessionScope,
    operation_id: SessionOperationId,
    alias: String,
}

#[cfg(feature = "blockcachevfs")]
impl fmt::Debug for BlockCacheSessionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockCacheSessionOptions")
            .field("vfs", &self.vfs.name())
            .field("attachment", &self.attachment)
            .field("session_id", &self.session_id)
            .field("scope", &self.scope)
            .field("operation_id", &self.operation_id)
            .field("alias", &self.alias)
            .finish()
    }
}

#[cfg(feature = "blockcachevfs")]
impl BlockCacheSessionOptions {
    /// Construct a session payload after validating its required UUID,
    /// caller-authorized scope, and non-zero operation ID.
    pub fn new(
        vfs: &'static BlockCacheVfs,
        attachment: AttachSpec,
        session_id: impl AsRef<str>,
        scope: SessionScope,
        operation_id: SessionOperationId,
    ) -> Result<Self> {
        let mut attachment = attachment;
        let canonical_provider =
            crate::blockcachevfs::canonical_session_storage_provider(&attachment.storage.provider)?;
        attachment.storage.provider = canonical_provider;
        let session_id = crate::blockcachevfs::SessionId::new(session_id)?;
        let alias = session_alias(&attachment, &session_id)?;
        Ok(Self {
            vfs,
            attachment,
            session_id,
            scope,
            operation_id,
            alias,
        })
    }

    /// Return the caller-authorized context carried by this payload.
    #[must_use]
    pub fn scope(&self) -> &SessionScope {
        &self.scope
    }

    /// Return the opaque per-request operation identifier.
    #[must_use]
    pub fn operation_id(&self) -> &SessionOperationId {
        &self.operation_id
    }

    fn vfs_name(&self) -> &str {
        self.vfs.name()
    }

    fn attach(&self) -> Result<SessionAttachment> {
        self.vfs.attach_session_scoped(
            &self.attachment,
            self.session_id.as_str(),
            self.scope.principal(),
            self.scope.target_database(),
            self.scope.operations(),
            &self.operation_id,
        )
    }

    fn expected_directory(&self) -> String {
        format!("/{}", self.alias)
    }
}

impl RemoteServerOptions {
    /// Returns the loopback-only default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the IPv4 address on which the server listens.
    ///
    /// A session-owned server accepts only an IPv4 loopback address. Its
    /// caller/proxy must enforce the session scope before forwarding
    /// requests; the native route-agnostic server does not enforce that
    /// scope itself.
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

    /// Selects the SQLite VFS used by the remote server for database access.
    ///
    /// When this is not configured, the process default VFS is used. The VFS
    /// is resolved when the server starts and an unknown name causes startup
    /// to fail. The selected VFS must remain registered and alive for the
    /// lifetime of the running server.
    #[must_use]
    pub fn vfs_name(mut self, vfs_name: impl Into<String>) -> Self {
        self.vfs_name = Some(vfs_name.into());
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

    /// Supply the complete session-owned block-cache context for this server.
    ///
    /// Construction attaches the storage container exactly once and the
    /// returned [`RemoteServer`] owns that attachment.  The session UUID is
    /// not copied into `DoltliteServeOpts` or inferred from an HTTP route.
    #[cfg(feature = "blockcachevfs")]
    #[must_use]
    pub fn blockcache_session(mut self, session: BlockCacheSessionOptions) -> Self {
        self.blockcache_session = Some(session);
        self
    }
}

impl Default for RemoteServerOptions {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1".to_owned(),
            port: 0,
            vfs_name: None,
            certificate_file: None,
            private_key_file: None,
            authorized_keys_directory: None,
            audience: None,
            request_timeout: None,
            #[cfg(feature = "blockcachevfs")]
            blockcache_session: None,
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

#[cfg(feature = "blockcachevfs")]
fn validate_session_directory(directory: &Path, session: &BlockCacheSessionOptions) -> Result<()> {
    let actual = directory.to_str().ok_or_else(|| {
        remote_server_error(
            ffi::SQLITE_MISUSE,
            "session remote server directory must be valid UTF-8",
        )
    })?;
    let expected = session.expected_directory();
    if actual != expected {
        return Err(remote_server_error(
            ffi::SQLITE_MISUSE,
            &format!("session remote server directory must be exactly {expected}, got {actual}"),
        ));
    }
    Ok(())
}

#[cfg(feature = "blockcachevfs")]
fn validate_session_bind_address(bind_address: &str) -> Result<()> {
    let address = bind_address.parse::<Ipv4Addr>().map_err(|_| {
        remote_server_error(
            ffi::SQLITE_MISUSE,
            "session remote servers must bind to an IPv4 loopback address",
        )
    })?;
    if !address.is_loopback() {
        return Err(remote_server_error(
            ffi::SQLITE_MISUSE,
            "session remote servers must bind to an IPv4 loopback address",
        ));
    }
    Ok(())
}

/// A running in-process DoltLite HTTP remote server.
///
/// The server runs on a native background thread. Dropping this value stops
/// that thread, waits for it to exit, and releases its native resources.
#[must_use = "dropping the server immediately stops its background thread"]
pub struct RemoteServer {
    #[cfg(feature = "blockcachevfs")]
    session: Option<SessionAttachment>,
    raw: Option<NonNull<ffi::DoltliteServer>>,
    scheme: &'static str,
    bind_address: String,
    port: u16,
}

impl RemoteServer {
    /// Starts a loopback-only server on an available operating-system-assigned
    /// port.
    ///
    /// `directory` must already exist for an ordinary local server. Each
    /// database is addressed by its file name beneath that directory, for
    /// example `server.database_url("repo.db")`. Session-owned servers use
    /// [`Self::start_with_options`] and pass the exact CBS alias path instead.
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

    /// Starts a server with explicit TLS, authentication, timeout, and VFS
    /// options.
    ///
    /// Native authentication is a server-wide, filesystem-backed key allowlist.
    /// For per-database permissions or a distributed key store, bind this server
    /// to loopback and put a host-managed gateway in front of it. The gateway
    /// should authenticate against its own authoritative key store and apply
    /// authorization before proxying the request. Configure a registered
    /// database VFS with [RemoteServerOptions::vfs_name] when remote file
    /// access should use something other than the process default. With a
    /// session-owned block-cache payload, `directory` must be exactly the
    /// attachment path `/{alias}`; ordinary local directories remain valid
    /// for unsessioned servers.
    pub fn start_with_options<P: AsRef<Path>>(
        directory: P,
        options: &RemoteServerOptions,
    ) -> Result<Self> {
        let directory_path = directory.as_ref();
        #[cfg(feature = "blockcachevfs")]
        if let Some(session) = options.blockcache_session.as_ref() {
            validate_session_directory(directory_path, session)?;
            validate_session_bind_address(&options.bind_address)?;
        }
        let rc = ffi::initialize_doltlite();
        if rc != ffi::SQLITE_OK {
            return Err(error_from_sqlite_code(
                rc,
                Some("failed to initialize DoltLite".to_owned()),
            ));
        }

        let directory = path_to_cstring(directory_path)?;
        let bind_address = CString::new(options.bind_address.as_str())?;
        #[cfg(feature = "blockcachevfs")]
        if let Some(session) = options.blockcache_session.as_ref() {
            if let Some(configured_vfs) = options.vfs_name.as_deref() {
                if configured_vfs != session.vfs_name() {
                    return Err(remote_server_error(
                        ffi::SQLITE_MISUSE,
                        "remote VFS name conflicts with the session-owned VFS",
                    ));
                }
            }
        }
        let selected_vfs_name = options.vfs_name.as_deref().or({
            #[cfg(feature = "blockcachevfs")]
            {
                options
                    .blockcache_session
                    .as_ref()
                    .map(BlockCacheSessionOptions::vfs_name)
            }
            #[cfg(not(feature = "blockcachevfs"))]
            {
                None
            }
        });
        let vfs_name = selected_vfs_name.map(CString::new).transpose()?;
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
            zVfsName: vfs_name
                .as_ref()
                .map_or(ptr::null(), |value| value.as_ptr()),
        };
        #[cfg(feature = "blockcachevfs")]
        let session = options
            .blockcache_session
            .as_ref()
            .map(BlockCacheSessionOptions::attach)
            .transpose()?;
        #[cfg(feature = "blockcachevfs")]
        if let Some(session) = session.as_ref() {
            if session.operation_status()? == SessionOperationStatus::Conflict {
                return Err(remote_server_error(
                    ffi::SQLITE_CONSTRAINT,
                    "session operation conflicts with the durable accepted head",
                ));
            }
        }
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
            #[cfg(feature = "blockcachevfs")]
            session,
            raw: Some(raw),
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

    /// Stop accepting requests and wait for all native workers to finish.
    ///
    /// A session-owned server retains its attachment after quiescing so the
    /// application can perform its final checkpoint and call [`Self::upload`]
    /// before dropping the server. Calling this more than once is harmless.
    pub fn quiesce(&mut self) -> Result<()> {
        if let Some(raw) = self.raw.take() {
            unsafe { ffi::doltliteServerStop(raw.as_ptr()) };
        }
        Ok(())
    }

    /// Quiesce one request, persist its final session checkpoint, and accept
    /// that checkpoint for the next application-level operation.
    ///
    /// The accepted-head CAS is deliberately separate from publication:
    /// callers choose whether a completed request is the application's final
    /// operation and call [`Self::upload`] explicitly for that case.
    pub fn complete_request(&mut self) -> Result<()> {
        self.quiesce()?;
        #[cfg(feature = "blockcachevfs")]
        {
            let Some(session) = self.session.as_ref() else {
                return Err(remote_server_error(
                    ffi::SQLITE_MISUSE,
                    "request completion requires a session-owned block-cache attachment",
                ));
            };
            match session.operation_status()? {
                SessionOperationStatus::Accepted | SessionOperationStatus::Committed => {
                    return Ok(())
                }
                SessionOperationStatus::Conflict => {
                    return Err(remote_server_error(
                        ffi::SQLITE_CONSTRAINT,
                        "another operation owns the session accepted head",
                    ));
                }
                SessionOperationStatus::Failed => {
                    return Err(remote_server_error(
                        ffi::SQLITE_CONSTRAINT,
                        "the session operation has already failed",
                    ));
                }
                SessionOperationStatus::New => {}
            }
            session.checkpoint()?;
            session.accept()?;
            Ok(())
        }
        #[cfg(not(feature = "blockcachevfs"))]
        {
            Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "request completion requires the blockcachevfs feature",
            ))
        }
    }

    /// Check the owned request operation before invoking or retrying a
    /// mutating handler. This is intentionally separate from
    /// [`Self::complete_request`], which performs the final checkpoint and
    /// accepted-head CAS after a handler has finished. `New` means that this
    /// operation has not been accepted and its captured predecessor is still
    /// current; it is not proof that an ambiguous earlier handler invocation
    /// did not run.
    #[cfg(feature = "blockcachevfs")]
    pub fn operation_status(&self) -> Result<SessionOperationStatus> {
        let Some(session) = self.session.as_ref() else {
            return Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "operation status requires a session-owned block-cache attachment",
            ));
        };
        session.operation_status()
    }

    /// Returns an HTTP remote URL for a database file in the served directory.
    #[must_use]
    pub fn database_url(&self, database: &str) -> String {
        format!(
            "{}://{}:{}/{database}",
            self.scheme, self.bind_address, self.port
        )
    }

    /// Quiesce and publish the current state of this server's session.
    ///
    /// A NEW operation is checkpointed and claims the session head directly
    /// in PUBLISHING state before the fenced manifest publication. This keeps
    /// the final application operation from exposing an ACCEPTED head between
    /// request completion and publication. ACCEPTED and COMMITTED operations
    /// use the ordinary fenced publisher for retry/reconciliation.
    pub fn upload(&mut self) -> Result<()> {
        self.quiesce()?;
        #[cfg(feature = "blockcachevfs")]
        {
            if let Some(session) = self.session.as_ref() {
                match session.operation_status()? {
                    SessionOperationStatus::New => session.finalize(),
                    SessionOperationStatus::Accepted | SessionOperationStatus::Committed => {
                        session.upload()
                    }
                    SessionOperationStatus::Failed => Err(remote_server_error(
                        ffi::SQLITE_CONSTRAINT,
                        "the session operation has already failed",
                    )),
                    SessionOperationStatus::Conflict => Err(remote_server_error(
                        ffi::SQLITE_CONSTRAINT,
                        "another operation owns the session accepted head",
                    )),
                }
            } else {
                Err(remote_server_error(
                    ffi::SQLITE_MISUSE,
                    "remote server has no session-owned block-cache attachment",
                ))
            }
        }
        #[cfg(not(feature = "blockcachevfs"))]
        {
            Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "remote server upload requires the blockcachevfs feature",
            ))
        }
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
        let _ = self.quiesce();
        #[cfg(feature = "blockcachevfs")]
        {
            // Drop after stopping native request handling so no new database
            // opens race the best-effort attachment detach.
            let _ = self.session.take();
        }
    }
}

#[cfg(all(test, feature = "blockcachevfs"))]
mod tests {
    use super::*;
    use crate::blockcachevfs::{AttachSpec, Storage, SESSION_OPERATION_ID_BYTES};

    fn initialized_test_vfs() -> &'static BlockCacheVfs {
        let directory = tempfile::tempdir().expect("create VFS directory");
        let path = directory.path().to_owned();
        // The VFS is process-lifetime state. Keep its backing directory alive
        // for the rest of this test process rather than leaving a singleton
        // pointed at a removed temporary path.
        std::mem::forget(directory);
        BlockCacheVfs::builder(path)
            .expect("build VFS")
            .init()
            .expect("initialize VFS")
    }

    fn test_scope() -> SessionScope {
        SessionScope::new("test-principal", "session.sqlite", "read,write")
            .expect("valid test scope")
    }

    fn test_operation_id(value: u8) -> SessionOperationId {
        let mut operation_id = [0_u8; SESSION_OPERATION_ID_BYTES];
        operation_id[0] = value;
        SessionOperationId::new(operation_id).expect("valid operation ID")
    }

    #[test]
    fn session_scope_rejects_missing_control_and_oversize_fields() {
        assert!(SessionScope::new("", "session.sqlite", "read").is_err());
        assert!(SessionScope::new("principal", "", "read").is_err());
        assert!(SessionScope::new("principal", "session.sqlite", "").is_err());
        assert!(SessionScope::new("principal\n", "session.sqlite", "read").is_err());
        assert!(SessionScope::new("principal", "session\0.sqlite", "read").is_err());
        assert!(SessionScope::new("principal\u{7f}", "session.sqlite", "read").is_err());
        assert!(SessionScope::new("principal", "../session.sqlite", "read").is_err());
        assert!(SessionScope::new("principal", ".hidden", "read").is_err());
        assert!(SessionScope::new("principal", "session/name.sqlite", "read").is_err());
        assert!(SessionScope::new("principal", "séssion.sqlite", "read").is_err());
        assert!(SessionScope::new(
            "x".repeat(SESSION_SCOPE_MAX_FIELD_BYTES + 1),
            "session.sqlite",
            "read",
        )
        .is_err());
        assert!(SessionScope::new(
            "principal",
            "x".repeat(SESSION_SCOPE_MAX_FIELD_BYTES + 1),
            "read",
        )
        .is_err());
        assert!(SessionScope::new(
            "principal",
            "session.sqlite",
            "x".repeat(SESSION_SCOPE_MAX_FIELD_BYTES + 1),
        )
        .is_err());

        let scope =
            SessionScope::new("principal", "session.sqlite", "read,write").expect("valid scope");
        assert_eq!(scope.principal(), "principal");
        assert_eq!(scope.target_database(), "session.sqlite");
        assert_eq!(scope.operations(), "read,write");
        assert!(scope.matches_database("session.sqlite"));
        assert!(!scope.matches_database("other.sqlite"));
    }

    #[test]
    fn session_payload_validates_id_and_vfs_name_before_attach() {
        let vfs = initialized_test_vfs();
        let attachment = AttachSpec::s3("account", "container", "us-east-1").alias("session-alias");
        let unknown_provider = BlockCacheSessionOptions::new(
            vfs,
            AttachSpec::new(Storage::new("unknown", "account", "container")),
            "550e8400-e29b-41d4-a716-446655440000",
            test_scope(),
            test_operation_id(1),
        )
        .expect_err("one-call sessions must reject unknown storage providers");
        assert!(matches!(
            unknown_provider,
            crate::Error::SqliteFailure(code, Some(message))
                if code.extended_code == crate::ffi::SQLITE_MISUSE
                    && message.contains("unknown CBS provider")
        ));
        let default_s3 = BlockCacheSessionOptions::new(
            vfs,
            AttachSpec::new(Storage::new("s3", "account", "container")),
            "550e8400-e29b-41d4-a716-446655440000",
            test_scope(),
            test_operation_id(2),
        )
        .expect_err("session storage must spell out the S3 region");
        assert!(matches!(
            default_s3,
            crate::Error::SqliteFailure(code, Some(message))
                if code.extended_code == crate::ffi::SQLITE_MISUSE
                    && message.contains("explicit region")
        ));
        let google_xml = BlockCacheSessionOptions::new(
            vfs,
            AttachSpec::new(Storage::new("google", "project", "container")),
            "550e8400-e29b-41d4-a716-446655440000",
            test_scope(),
            test_operation_id(3),
        )
        .expect_err("session storage must use the JSON Google selector");
        assert!(matches!(
            google_xml,
            crate::Error::SqliteFailure(code, Some(message))
                if code.extended_code == crate::ffi::SQLITE_MISUSE
                    && message.contains("api=json")
        ));
        let s3_maxresults = BlockCacheSessionOptions::new(
            vfs,
            AttachSpec::new(Storage::new(
                "s3?region=us-east-1&maxresults=100",
                "account",
                "container",
            )),
            "550e8400-e29b-41d4-a716-446655440000",
            test_scope(),
            test_operation_id(4),
        )
        .expect_err("session storage must not vary the S3 selector with maxresults");
        assert!(matches!(
            s3_maxresults,
            crate::Error::SqliteFailure(code, Some(message))
                if code.extended_code == crate::ffi::SQLITE_MISUSE
                    && message.contains("region=<region>")
        ));
        let azure = BlockCacheSessionOptions::new(
            vfs,
            AttachSpec::new(Storage::new("azure?sas=1", "account", "container")),
            "550e8400-e29b-41d4-a716-446655440000",
            test_scope(),
            test_operation_id(5),
        )
        .expect_err("session storage must reject unaudited Azure selector forms");
        assert!(matches!(
            azure,
            crate::Error::SqliteFailure(code, Some(message))
                if code.extended_code == crate::ffi::SQLITE_MISUSE
                    && message.contains("Azure session selectors")
        ));
        assert!(BlockCacheSessionOptions::new(
            vfs,
            attachment.clone(),
            "missing",
            test_scope(),
            test_operation_id(6),
        )
        .is_err());

        let session = BlockCacheSessionOptions::new(
            vfs,
            attachment,
            "550e8400-e29b-41d4-a716-446655440000",
            test_scope(),
            test_operation_id(7),
        )
        .expect("valid session payload");
        assert!(session.scope().matches_database("session.sqlite"));
        assert!(!session.scope().matches_database("other.sqlite"));
        assert_eq!(session.operation_id().as_bytes()[0], 7);
        let canonicalized = BlockCacheSessionOptions::new(
            vfs,
            AttachSpec::google_json_with_endpoint(
                "project",
                "bucket/prefix",
                "http://127.0.0.1:19025/",
            ),
            "550e8400-e29b-41d4-a716-446655440003",
            test_scope(),
            test_operation_id(10),
        )
        .expect("valid Google endpoint session payload");
        assert_eq!(
            canonicalized.attachment.storage.provider,
            "google?api=json&endpoint=http://127.0.0.1:19025"
        );
        let result = RemoteServer::start_with_options(
            "/session-alias",
            &RemoteServerOptions::new()
                .vfs_name("different-vfs")
                .blockcache_session(session),
        );
        assert!(matches!(
            result,
            Err(crate::Error::SqliteFailure(code, _))
            if code.extended_code == crate::ffi::SQLITE_MISUSE
        ));
    }

    #[test]
    fn session_server_directory_must_match_attachment_alias() {
        let vfs = initialized_test_vfs();
        let session = BlockCacheSessionOptions::new(
            vfs,
            AttachSpec::s3("account", "container", "us-east-1").alias("session-alias"),
            "550e8400-e29b-41d4-a716-446655440000",
            test_scope(),
            test_operation_id(11),
        )
        .expect("valid session payload");

        assert!(validate_session_directory(Path::new("/session-alias"), &session).is_ok());
        let error = validate_session_directory(Path::new("/other-alias"), &session)
            .expect_err("different alias must be rejected");
        assert!(matches!(
            error,
            crate::Error::SqliteFailure(code, Some(message))
                if code.extended_code == crate::ffi::SQLITE_MISUSE
                    && message.contains("exactly /session-alias")
        ));

        let start_error = RemoteServer::start_with_options(
            "/other-alias",
            &RemoteServerOptions::new().blockcache_session(session.clone()),
        )
        .expect_err("mismatched directory must fail before attachment");
        assert!(matches!(
            start_error,
            crate::Error::SqliteFailure(code, Some(message))
                if code.extended_code == crate::ffi::SQLITE_MISUSE
                    && message.contains("exactly /session-alias")
        ));

        let bind_error = RemoteServer::start_with_options(
            "/session-alias",
            &RemoteServerOptions::new()
                .bind_address("0.0.0.0")
                .blockcache_session(session.clone()),
        )
        .expect_err("session server must not expose an unauthenticated listener");
        assert!(matches!(
            bind_error,
            crate::Error::SqliteFailure(code, Some(message))
                if code.extended_code == crate::ffi::SQLITE_MISUSE
                    && message.contains("IPv4 loopback")
        ));

        let ipv6_error = RemoteServer::start_with_options(
            "/session-alias",
            &RemoteServerOptions::new()
                .bind_address("::1")
                .blockcache_session(session),
        )
        .expect_err("session server must use the native IPv4 bind contract");
        assert!(matches!(
            ipv6_error,
            crate::Error::SqliteFailure(code, Some(message))
                if code.extended_code == crate::ffi::SQLITE_MISUSE
                    && message.contains("IPv4 loopback")
        ));
    }

    #[test]
    fn independent_default_session_aliases_have_isolated_roots() {
        let vfs = initialized_test_vfs();
        let storage = Storage::s3("account", "container", "us-east-1");
        let first = BlockCacheSessionOptions::new(
            vfs,
            AttachSpec::new(storage.clone()),
            "550e8400-e29b-41d4-a716-446655440000",
            test_scope(),
            test_operation_id(12),
        )
        .expect("first session payload");
        let second = BlockCacheSessionOptions::new(
            vfs,
            AttachSpec::new(storage),
            "550e8400-e29b-41d4-a716-446655440001",
            test_scope(),
            test_operation_id(13),
        )
        .expect("second session payload");

        assert_ne!(first.expected_directory(), second.expected_directory());
        assert!(validate_session_directory(Path::new(&first.expected_directory()), &first).is_ok());
        assert!(
            validate_session_directory(Path::new(&first.expected_directory()), &second).is_err()
        );
    }

    #[test]
    fn quiesce_is_idempotent_for_an_ordinary_server() {
        let directory = tempfile::tempdir().expect("create server directory");
        let server_root = directory.path().join("server");
        std::fs::create_dir(&server_root).expect("create server root");
        let mut server = RemoteServer::start(&server_root).expect("start server");
        server.quiesce().expect("quiesce server");
        server.quiesce().expect("quiesce server again");
    }
}

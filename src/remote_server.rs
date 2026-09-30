//! In-process DoltLite HTTP remote server.

#[cfg(feature = "blockcachevfs")]
use crate::blockcachevfs::{
    session_alias, AttachSpec, AuthError, AuthRefreshCallback, AuthRefreshReason, BlockCacheVfs,
    SessionAttachment, SessionOperationId, SessionOperationStatus, UriSessionContext,
};
use crate::error::error_from_sqlite_code;
use crate::{ffi, path_to_cstring, Result};
#[cfg(feature = "blockcachevfs")]
use crate::{Connection, OpenFlags};
use std::ffi::{c_int, c_long, CStr, CString};
use std::fmt;
#[cfg(feature = "blockcachevfs")]
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::ptr::{self, NonNull};
#[cfg(feature = "blockcachevfs")]
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "blockcachevfs")]
const SQLITE_OPEN_DOLTLITE_NO_SEED: i32 = 0x0080_0000;

const SESSION_LOOPBACK_HTTP_IDLE_TIMEOUT_MS: u32 = 5 * 60 * 1000;

fn cloud_database_uri(path: &Path) -> Option<&str> {
    path.to_str()
        .filter(|path| path.starts_with("gcs://") || path.starts_with("s3://"))
}

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
        // SQLite interprets these exact lowercase endings as sidecars of a
        // shorter database name. Leading-dot names are already excluded by
        // `is_native_database_name`; that also prevents `.repo.db-lock` from
        // colliding with DoltLite's local lock sidecar. Longer suffix
        // near-misses such as `repo.db-wal-old` remain valid database names.
        if ["-wal", "-shm", "-journal"]
            .iter()
            .any(|suffix| target_database.ends_with(*suffix))
        {
            return Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "session target database must not end in a reserved SQLite sidecar suffix",
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
    #[cfg(feature = "blockcachevfs")]
    database_open_flags: OpenFlags,
}

#[cfg(feature = "blockcachevfs")]
#[derive(Clone)]
enum BlockCacheSessionTarget {
    Vfs {
        vfs: &'static BlockCacheVfs,
        attachment: AttachSpec,
        alias: String,
    },
    Uri,
}

/// One-call construction payload for a session-owned block-cache server.
///
/// A payload can attach an existing process-lifetime VFS or own the VFS opened
/// from a GCS/S3 database URI. Both forms bind a client session UUID, an
/// explicitly authorized scope, and one opaque per-request operation ID.
#[cfg(feature = "blockcachevfs")]
#[derive(Clone)]
pub struct BlockCacheSessionOptions {
    target: BlockCacheSessionTarget,
    session_id: crate::blockcachevfs::SessionId,
    scope: SessionScope,
    operation_id: SessionOperationId,
    auth_refresh: Option<Arc<AuthRefreshCallback>>,
}

#[cfg(feature = "blockcachevfs")]
impl fmt::Debug for BlockCacheSessionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("BlockCacheSessionOptions");
        match &self.target {
            BlockCacheSessionTarget::Vfs {
                vfs,
                attachment,
                alias,
            } => {
                debug
                    .field("vfs", &vfs.name())
                    .field("attachment", attachment)
                    .field("alias", alias);
            }
            BlockCacheSessionTarget::Uri => {
                debug.field("target", &"uri");
            }
        }
        debug
            .field("session_id", &self.session_id)
            .field("scope", &self.scope)
            .field("operation_id", &self.operation_id)
            .field("auth_callback_configured", &self.auth_refresh.is_some())
            .finish()
    }
}

#[cfg(feature = "blockcachevfs")]
impl BlockCacheSessionOptions {
    /// Construct a session payload for an existing process-lifetime VFS.
    ///
    /// The session UUID, caller-authorized scope, and per-request operation
    /// ID are validated before the storage attachment is opened.
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
            target: BlockCacheSessionTarget::Vfs {
                vfs,
                attachment,
                alias,
            },
            session_id,
            scope,
            operation_id,
            auth_refresh: None,
        })
    }

    /// Construct a session payload whose VFS is owned by a GCS/S3 URI open.
    ///
    /// `session_id` is the client-stable UUID for one logical transfer.
    /// `operation_id` identifies this individual HTTP request and should be
    /// stable for exact mutating retries. The URI's decoded database option
    /// must match `scope.target_database()`.
    ///
    /// # Arguments
    ///
    /// * `session_id` - Canonical non-nil UUID shared by requests in one transfer.
    /// * `scope` - Caller-authorized principal, exact database, and operation scope.
    /// * `operation_id` - Opaque identifier for this request or exact retry.
    ///
    /// # Errors
    ///
    /// Returns an error if `session_id` is not a canonical non-nil UUID.
    pub fn for_uri(
        session_id: impl AsRef<str>,
        scope: SessionScope,
        operation_id: SessionOperationId,
    ) -> Result<Self> {
        Ok(Self {
            target: BlockCacheSessionTarget::Uri,
            session_id: crate::blockcachevfs::SessionId::new(session_id)?,
            scope,
            operation_id,
            auth_refresh: None,
        })
    }

    /// Supply credentials for a session-owned Google Cloud Storage URI.
    ///
    /// The callback runs once while the URI-owned VFS attaches its storage
    /// container, before each cloud request, and again after an authorization
    /// failure. It should return the current token for the requested storage
    /// identity. This keeps credentials out of the server loop and lets an
    /// open database continue after a short-lived token expires. The callback
    /// is supported only by [`Self::for_uri`] with a GCS URI; using it with a
    /// static VFS session or S3 URI causes server startup to fail.
    ///
    /// # Example
    ///
    /// ```
    /// use rusqlite::blockcachevfs::AuthRefreshReason;
    /// use rusqlite::{BlockCacheSessionOptions, SessionOperationId, SessionScope};
    ///
    /// # fn main() -> rusqlite::Result<()> {
    /// let scope = SessionScope::new("alice", "default.db", "read,write")?;
    /// let operation = SessionOperationId::from_request("POST", "/default.db/commit", b"")?;
    /// let session = BlockCacheSessionOptions::for_uri(
    ///     "9c539f0e-3913-4875-9f93-23627c3c015d",
    ///     scope,
    ///     operation,
    /// )?
    /// .auth_callback(|_storage, _project, _container, reason| match reason {
    ///     AuthRefreshReason::Request | AuthRefreshReason::Unauthorized => {
    ///         Ok("current-access-token".to_owned())
    ///     }
    /// });
    /// let _ = session;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn auth_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(&str, &str, &str, AuthRefreshReason) -> std::result::Result<String, AuthError>
            + Send
            + Sync
            + 'static,
    {
        self.auth_refresh = Some(Arc::new(callback));
        self
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

    fn is_uri(&self) -> bool {
        matches!(self.target, BlockCacheSessionTarget::Uri)
    }

    fn vfs_name(&self) -> Option<&str> {
        match &self.target {
            BlockCacheSessionTarget::Vfs { vfs, .. } => Some(vfs.name()),
            BlockCacheSessionTarget::Uri => None,
        }
    }

    fn attach(&self) -> Result<SessionAttachment> {
        if self.auth_refresh.is_some() {
            return Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "credential refresh callbacks require a session-owned GCS URI",
            ));
        }
        match &self.target {
            BlockCacheSessionTarget::Vfs {
                vfs, attachment, ..
            } => vfs.attach_session_scoped(
                attachment,
                self.session_id.as_str(),
                self.scope.principal(),
                self.scope.target_database(),
                self.scope.operations(),
                &self.operation_id,
            ),
            BlockCacheSessionTarget::Uri => Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "URI-backed sessions are attached while opening the cloud connection",
            )),
        }
    }

    fn expected_directory(&self) -> Option<String> {
        match &self.target {
            BlockCacheSessionTarget::Vfs { alias, .. } => Some(format!("/{alias}")),
            BlockCacheSessionTarget::Uri => None,
        }
    }

    fn uri_context(&self) -> UriSessionContext {
        UriSessionContext {
            session_id: self.session_id.clone(),
            principal: self.scope.principal().to_owned(),
            target_database: self.scope.target_database().to_owned(),
            operations: self.scope.operations().to_owned(),
            operation_id: self.operation_id,
            auth_refresh: self.auth_refresh.as_ref().map(Arc::clone),
        }
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

    /// Sets the flags used to open a GCS- or S3-backed database URI.
    ///
    /// The flags control whether the target database must already exist or
    /// may be created when the server starts. Session-owned URI servers use
    /// the same flags. By default, the URI is
    /// opened with `READ_WRITE | CREATE | URI | NO_MUTEX`.
    #[cfg(feature = "blockcachevfs")]
    #[must_use]
    pub fn database_open_flags(mut self, flags: OpenFlags) -> Self {
        self.database_open_flags = flags;
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
            #[cfg(feature = "blockcachevfs")]
            database_open_flags: OpenFlags::default(),
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

fn remote_server_error_with_context(error: crate::Error, context: &str) -> crate::Error {
    match error {
        crate::Error::SqliteFailure(code, message) => crate::Error::SqliteFailure(
            code,
            Some(match message {
                Some(message) => format!("{context}: {message}"),
                None => context.to_owned(),
            }),
        ),
        error => error,
    }
}

#[cfg(feature = "blockcachevfs")]
fn validate_session_directory(directory: &Path, session: &BlockCacheSessionOptions) -> Result<()> {
    let actual = directory.to_str().ok_or_else(|| {
        remote_server_error(
            ffi::SQLITE_MISUSE,
            "session remote server directory must be valid UTF-8",
        )
    })?;
    let Some(expected) = session.expected_directory() else {
        return Err(remote_server_error(
            ffi::SQLITE_MISUSE,
            "URI session options cannot be used with a local directory",
        ));
    };
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
/// that thread, waits for it to exit, and releases its native resources. A
/// cloud URI owned by the server is not uploaded implicitly; call [`Self::upload`]
/// before [`Self::close`] or dropping the server to publish its changes.
#[must_use = "dropping the server immediately stops its background thread"]
pub struct RemoteServer {
    #[cfg(feature = "blockcachevfs")]
    session: Option<SessionAttachment>,
    #[cfg(feature = "blockcachevfs")]
    database: Option<Connection>,
    raw: Option<NonNull<ffi::DoltliteServer>>,
    scheme: &'static str,
    bind_address: String,
    port: u16,
    session_loopback_transfer: bool,
}

impl RemoteServer {
    /// Starts a loopback-only server on an available operating-system-assigned
    /// port.
    ///
    /// `directory` must already exist for an ordinary local server. Each
    /// database is addressed by its file name beneath that directory, for
    /// example `server.database_url("repo.db")`. With `blockcachevfs`, a
    /// GCS or S3 database URI can be passed instead; the server then owns the
    /// URI connection for its lifetime and can publish it with [`Self::upload`].
    /// Session-owned servers use [`Self::start_with_options`]. A cloud URI
    /// session retains its own CBS attachment; a static-VFS session passes
    /// the exact CBS alias path instead.
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
    /// access should use something other than the process default. With
    /// `blockcachevfs`, a GCS or S3 URI opens and retains its own cloud
    /// connection; the `database_open_flags()` option controls the
    /// URI connection's open flags. Pass URI-backed session options to bind
    /// the attachment to a client session and request operation. Static-VFS
    /// session payloads still require `directory` to be exactly the
    /// attachment path `/{alias}`; ordinary local directories remain valid
    /// for unsessioned servers.
    ///
    /// # Errors
    ///
    /// Returns an error if a cloud URI cannot be opened, its database flags or
    /// session scope are inconsistent, or the native server cannot start.
    /// Cloud credentials are omitted from URI-open diagnostics.
    pub fn start_with_options<P: AsRef<Path>>(
        directory: P,
        options: &RemoteServerOptions,
    ) -> Result<Self> {
        let directory_path = directory.as_ref();
        let cloud_uri = cloud_database_uri(directory_path);
        #[cfg(feature = "blockcachevfs")]
        let session_loopback_transfer = cloud_uri.is_some()
            && options
                .blockcache_session
                .as_ref()
                .is_some_and(BlockCacheSessionOptions::is_uri);
        #[cfg(not(feature = "blockcachevfs"))]
        let session_loopback_transfer = false;
        #[cfg(feature = "blockcachevfs")]
        let (database, uri_directory, uri_vfs_name) = match cloud_uri {
            Some(uri) => {
                if options.vfs_name.is_some() {
                    return Err(remote_server_error(
                        ffi::SQLITE_MISUSE,
                        "cloud database URIs select their own remote VFS",
                    ));
                }
                let session = options.blockcache_session.as_ref();
                if let Some(session) = session {
                    if !session.is_uri() {
                        return Err(remote_server_error(
                            ffi::SQLITE_MISUSE,
                            "cloud database URIs require URI-backed session options",
                        ));
                    }
                    validate_session_bind_address(&options.bind_address)?;
                }
                let flags = OpenFlags::from_bits_retain(
                    options.database_open_flags.bits() | SQLITE_OPEN_DOLTLITE_NO_SEED,
                );
                // DoltLite's private no-seed flag (see
                // libdoltlite-sys/patches/0005-doltlite-no-seed.patch) is
                // used only for this connection-owned URI. The protocol
                // server's refs-if operation creates the first graph refs
                // without a synthetic main ref.
                let database = if let Some(session) = session {
                    crate::blockcachevfs::open_connection_uri_with_session(
                        uri,
                        flags,
                        session.uri_context(),
                    )?
                } else {
                    Connection::open_with_flags(uri, flags)?
                };
                let directory = database
                    .blockcachevfs_directory()
                    .ok_or_else(|| {
                        remote_server_error(
                            ffi::SQLITE_MISUSE,
                            "cloud database URI did not open with blockcachevfs",
                        )
                    })?
                    .to_owned();
                let vfs_name = database
                    .blockcachevfs_name()
                    .ok_or_else(|| {
                        remote_server_error(
                            ffi::SQLITE_MISUSE,
                            "cloud database URI did not register a blockcachevfs VFS",
                        )
                    })?
                    .to_owned();
                (Some(database), Some(directory), Some(vfs_name))
            }
            None => {
                if options
                    .blockcache_session
                    .as_ref()
                    .is_some_and(BlockCacheSessionOptions::is_uri)
                {
                    return Err(remote_server_error(
                        ffi::SQLITE_MISUSE,
                        "URI session options require a GCS or S3 database URI",
                    ));
                }
                (None, None, None)
            }
        };
        #[cfg(not(feature = "blockcachevfs"))]
        if cloud_uri.is_some() {
            return Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "GCS and S3 remote server URIs require the blockcachevfs feature",
            ));
        }
        #[cfg(feature = "blockcachevfs")]
        if let Some(session) = options
            .blockcache_session
            .as_ref()
            .filter(|session| !session.is_uri())
        {
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

        #[cfg(feature = "blockcachevfs")]
        let directory = match uri_directory.as_deref() {
            Some(path) => CString::new(path)?,
            None => path_to_cstring(directory_path)?,
        };
        #[cfg(not(feature = "blockcachevfs"))]
        let directory = path_to_cstring(directory_path)?;
        let bind_address = CString::new(options.bind_address.as_str())?;
        #[cfg(feature = "blockcachevfs")]
        if let Some(session) = options.blockcache_session.as_ref() {
            if let (Some(configured_vfs), Some(session_vfs)) =
                (options.vfs_name.as_deref(), session.vfs_name())
            {
                if configured_vfs != session_vfs {
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
                uri_vfs_name.as_deref().or_else(|| {
                    options
                        .blockcache_session
                        .as_ref()
                        .and_then(BlockCacheSessionOptions::vfs_name)
                })
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
            bSessionLoopbackTransfer: c_int::from(session_loopback_transfer),
        };
        #[cfg(feature = "blockcachevfs")]
        let session = options
            .blockcache_session
            .as_ref()
            .filter(|session| !session.is_uri())
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
            #[cfg(feature = "blockcachevfs")]
            database,
            raw: Some(raw),
            scheme: if certificate_file.is_some() {
                "https"
            } else {
                "http"
            },
            bind_address: options.bind_address.clone(),
            port: actual_port,
            session_loopback_transfer,
        })
    }

    /// Returns the TCP port on which the server is listening.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Returns the connection owned by a cloud-URI server.
    ///
    /// A caller can use this connection to inspect the URI database before
    /// publication. It is available only while the URI connection's SQLite
    /// handle remains open; [`Self::quiesce`], [`Self::stage_request`], and
    /// [`Self::upload`] close that handle. Local-directory and static-VFS
    /// servers return `None`.
    #[cfg(feature = "blockcachevfs")]
    #[must_use]
    pub fn database_connection(&self) -> Option<&Connection> {
        self.raw.as_ref()?;
        self.database.as_ref()
    }

    /// Stop accepting requests and wait for all native workers to finish.
    ///
    /// Session-owned servers retain their attachment after quiescing. URI-
    /// owned servers close their SQLite anchor handle while retaining the VFS
    /// and session owner, so the application can checkpoint or publish after
    /// native workers release their clients. Calling this more than once is
    /// harmless.
    ///
    /// # Errors
    ///
    /// Returns an error if the URI connection's SQLite handle cannot close.
    pub fn quiesce(&mut self) -> Result<()> {
        if let Some(raw) = self.raw.take() {
            unsafe { ffi::doltliteServerStop(raw.as_ptr()) };
        }
        #[cfg(feature = "blockcachevfs")]
        if let Some(database) = self.database.as_mut() {
            database.close_sql_handle()?;
        }
        Ok(())
    }

    #[cfg(feature = "blockcachevfs")]
    fn blockcache_session(&self) -> Option<&SessionAttachment> {
        self.session.as_ref().or_else(|| {
            self.database
                .as_ref()
                .and_then(Connection::blockcache_session)
        })
    }

    /// Quiesce one successful nonterminal mutation, persist its final session
    /// checkpoint, and accept that checkpoint for the next operation.
    ///
    /// The accepted-head CAS is deliberately separate from publication:
    /// callers choose whether a completed request is the application's final
    /// operation and call [`Self::upload`] explicitly for that case.
    ///
    /// # Errors
    ///
    /// Returns an error if this server has no session, the operation conflicts
    /// with another accepted operation, or native checkpoint/accept fails.
    pub fn complete_request(&mut self) -> Result<()> {
        self.quiesce()?;
        #[cfg(feature = "blockcachevfs")]
        {
            let Some(session) = self.blockcache_session() else {
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
            session.checkpoint().map_err(|error| {
                remote_server_error_with_context(error, "session checkpoint failed")
            })?;
            session.accept().map_err(|error| {
                remote_server_error_with_context(error, "session acceptance failed")
            })?;
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

    /// Quiesce and durably accept a URI-backed session operation without
    /// publishing its manifest. A separate server can attach the same URI,
    /// session UUID, scope, and operation ID, inspect the accepted database,
    /// and publish it with [`Self::upload`].
    ///
    /// This method is restricted to session-owned cloud-URI servers. Use
    /// [`Self::complete_request`] for the ordinary per-request boundary on a
    /// local or static-VFS server.
    ///
    /// # Errors
    ///
    /// Returns an error unless the server owns a session-scoped cloud URI, or
    /// if quiescing, checkpointing, or acceptance fails. No manifest is
    /// published by this method.
    pub fn stage_request(&mut self) -> Result<()> {
        #[cfg(feature = "blockcachevfs")]
        {
            let is_uri_session = self
                .database
                .as_ref()
                .is_some_and(|database| database.blockcache_session().is_some());
            if !is_uri_session {
                return Err(remote_server_error(
                    ffi::SQLITE_MISUSE,
                    "staging requires a session-owned cloud URI server",
                ));
            }
            self.complete_request()
        }
        #[cfg(not(feature = "blockcachevfs"))]
        {
            Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "request staging requires the blockcachevfs feature",
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
    ///
    /// # Errors
    ///
    /// Returns an error if this server has no session attachment or the native
    /// session store cannot read the operation status.
    #[cfg(feature = "blockcachevfs")]
    pub fn operation_status(&self) -> Result<SessionOperationStatus> {
        let Some(session) = self.blockcache_session() else {
            return Err(remote_server_error(
                ffi::SQLITE_MISUSE,
                "operation status requires a session-owned block-cache attachment",
            ));
        };
        session.operation_status()
    }

    /// Returns an HTTP remote URL for a database file in the served directory.
    /// URI-backed session loopback URLs carry a five-minute idle timeout for
    /// large object-store commits; other remotes use DoltLite's ordinary
    /// 30-second default, subject to its environment override.
    #[must_use]
    pub fn database_url(&self, database: &str) -> String {
        let url = format!(
            "{}://{}:{}/{database}",
            self.scheme, self.bind_address, self.port
        );
        if self.session_loopback_transfer {
            format!("{url}?http_idle_timeout_ms={SESSION_LOOPBACK_HTTP_IDLE_TIMEOUT_MS}")
        } else {
            url
        }
    }

    /// Quiesce and publish the current state of this server's session or
    /// connection-owned cloud database URI.
    ///
    /// For session-owned servers, a NEW operation is checkpointed and claims
    /// the session head directly in PUBLISHING state before the fenced
    /// manifest publication. This keeps the final application operation from
    /// exposing an ACCEPTED head between request completion and publication.
    /// ACCEPTED and COMMITTED operations use the ordinary fenced publisher
    /// for retry and reconciliation. URI-owned servers publish through
    /// `Connection::upload()` after their native workers stop.
    ///
    /// # Errors
    ///
    /// Returns an error if quiescing, session finalization, or cloud
    /// publication fails.
    pub fn upload(&mut self) -> Result<()> {
        self.quiesce()?;
        #[cfg(feature = "blockcachevfs")]
        {
            if let Some(session) = self.blockcache_session() {
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
            } else if let Some(database) = self.database.as_ref() {
                database.upload()
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

    /// Quiesce the server and close its owned cloud URI connection.
    ///
    /// This does not publish pending changes. Call [`Self::upload`] first when
    /// the server owns a writable GCS or S3 URI. Unlike [`Drop`], this method
    /// reports errors returned while closing the SQLite connection. Because
    /// this method consumes the server, an error also drops its local cache;
    /// pending changes that were not uploaded are discarded.
    ///
    /// # Errors
    ///
    /// Returns an error if quiescing or closing the owned SQLite connection
    /// fails. Call [`Self::upload`] first to publish pending cloud changes.
    pub fn close(mut self) -> Result<()> {
        self.quiesce()?;
        #[cfg(feature = "blockcachevfs")]
        if let Some(database) = self.database.take() {
            database.close().map_err(|(_, error)| error)?;
        }
        Ok(())
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
    fn database_url_extends_idle_timeout_only_for_session_loopback() {
        let make_server = |session_loopback_transfer| RemoteServer {
            session: None,
            database: None,
            raw: None,
            scheme: "http",
            bind_address: "127.0.0.1".to_owned(),
            port: 1234,
            session_loopback_transfer,
        };

        assert_eq!(
            make_server(false).database_url("default.db"),
            "http://127.0.0.1:1234/default.db"
        );
        assert_eq!(
            make_server(true).database_url("default.db"),
            "http://127.0.0.1:1234/default.db?http_idle_timeout_ms=300000"
        );
    }

    #[test]
    fn session_phase_context_preserves_sqlite_error_code_and_native_message() {
        let error = crate::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_IOERR_READ),
            Some("injected native detail".to_owned()),
        );

        let error = remote_server_error_with_context(error, "session checkpoint failed");

        assert!(matches!(
            error,
            crate::Error::SqliteFailure(code, Some(message))
                if code.extended_code == ffi::SQLITE_IOERR_READ
                    && message == "session checkpoint failed: injected native detail"
        ));
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
    fn session_scope_rejects_reserved_sidecar_and_lock_names() {
        // A user can supply `.repo.db-lock` as a session target even though
        // that exact path is DoltLite's local lock sidecar for `repo.db`.
        // Accepting it would let the remote entry collide with the file used
        // to coordinate local writes. The `-wal`/`-shm`/`-journal` cases
        // similarly collide with SQLite sidecars. The non-hidden
        // `repo.db-lock` name remains an ordinary database; leading-dot
        // near-misses remain excluded by the existing name policy above.
        for name in [
            "repo.db-wal",
            "repo.db-shm",
            "repo.db-journal",
            ".repo.db-lock",
        ] {
            assert!(
                SessionScope::new("principal", name, "read").is_err(),
                "reserved sidecar-shaped target should be rejected: {name}"
            );
        }

        for name in [
            "session.sqlite",
            "repo.db-lock",
            "repo.db-wal-old",
            "repo.db-WAL",
        ] {
            assert!(
                SessionScope::new("principal", name, "read").is_ok(),
                "ordinary or near-miss database name should remain valid: {name}"
            );
        }
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
        let BlockCacheSessionTarget::Vfs { attachment, .. } = &canonicalized.target else {
            panic!("should construct a static VFS session target");
        };
        assert_eq!(
            attachment.storage.provider,
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
    fn cloud_uri_session_options_validate_before_open() {
        let vfs = initialized_test_vfs();
        let static_session = BlockCacheSessionOptions::new(
            vfs,
            AttachSpec::s3("account", "container", "us-east-1"),
            "550e8400-e29b-41d4-a716-446655440004",
            test_scope(),
            test_operation_id(12),
        )
        .expect("valid static-VFS session payload");
        let uri = "gcs://bucket/prefix?vfs=blockcachevfs&project=test&access_token=private-token&endpoint=http%3A%2F%2F127.0.0.1%3A1&database=session.sqlite";

        let error = RemoteServer::start_with_options(
            uri,
            &RemoteServerOptions::new().blockcache_session(static_session),
        )
        .expect_err("cloud URI cannot use a static-VFS session payload");
        assert!(matches!(
            error,
            crate::Error::SqliteFailure(code, _)
                if code.extended_code == crate::ffi::SQLITE_MISUSE
        ));
        assert!(!format!("{error:?}").contains("private-token"));

        let uri_session = BlockCacheSessionOptions::for_uri(
            "550e8400-e29b-41d4-a716-446655440005",
            test_scope(),
            test_operation_id(13),
        )
        .expect("valid URI-backed session payload");
        let mismatched_database = uri.replace("database=session.sqlite", "database=other.sqlite");
        let error = RemoteServer::start_with_options(
            &mismatched_database,
            &RemoteServerOptions::new().blockcache_session(uri_session.clone()),
        )
        .expect_err("URI session target mismatch must fail before cloud access");
        assert!(matches!(
            error,
            crate::Error::SqliteFailure(code, _)
                if code.extended_code == crate::ffi::SQLITE_MISUSE
        ));
        assert!(!format!("{error:?}").contains("private-token"));

        let read_only = RemoteServerOptions::new()
            .database_open_flags(OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI)
            .blockcache_session(uri_session);
        let error = RemoteServer::start_with_options(uri, &read_only)
            .expect_err("session URI requires writable open flags before cloud access");
        assert!(matches!(
            error,
            crate::Error::SqliteFailure(code, _)
                if code.extended_code == crate::ffi::SQLITE_MISUSE
        ));
        assert!(!format!("{error:?}").contains("private-token"));

        let error = RemoteServer::start_with_options(
            uri,
            &RemoteServerOptions::new().vfs_name("blockcachevfs"),
        )
        .expect_err("URI selects its own remote VFS");
        assert!(matches!(
            error,
            crate::Error::SqliteFailure(code, _)
                if code.extended_code == crate::ffi::SQLITE_MISUSE
        ));
        assert!(!format!("{error:?}").contains("private-token"));
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

        let first_directory = first
            .expected_directory()
            .expect("should expose a directory for a static VFS session");
        let second_directory = second
            .expected_directory()
            .expect("should expose a directory for a static VFS session");
        assert_ne!(first_directory, second_directory);
        assert!(validate_session_directory(Path::new(&first_directory), &first).is_ok());
        assert!(validate_session_directory(Path::new(&first_directory), &second).is_err());
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

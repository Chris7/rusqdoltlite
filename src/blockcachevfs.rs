//! Safe, process-lifetime access to Cloud Backed SQLite's block-cache VFS.

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::fmt;
use std::path::Path;
use std::ptr;
use std::sync::{Mutex, OnceLock};

use crate::error::{check, Error};
use crate::{Connection, OpenFlags, Result};

use crate::ffi::blockcachevfs as raw;

/// An error returned by an authentication callback.
#[derive(Debug, Clone)]
pub struct AuthError(
    /// Human-readable callback failure.
    pub String,
);

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AuthError {}

/// A callback that supplies a cloud provider authentication token.
pub type AuthCallback =
    dyn Fn(&str, &str, &str) -> std::result::Result<String, AuthError> + Send + Sync + 'static;

/// Cloud storage identity and container.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Storage {
    /// CBS provider name, such as `google` or `azure`.
    pub provider: String,
    /// Provider account or project name.
    pub account: String,
    /// Provider container or bucket name.
    pub container: String,
}

impl Storage {
    /// Construct a provider-specific storage specification.
    pub fn new(
        provider: impl Into<String>,
        account: impl Into<String>,
        container: impl Into<String>,
    ) -> Self {
        Self {
            provider: provider.into(),
            account: account.into(),
            container: container.into(),
        }
    }

    /// Construct a Google Cloud Storage specification.
    ///
    /// `bucket` may be `bucket/prefix` for a multi-tenant container. When
    /// attaching such a container, use a slash-free local alias.
    pub fn google(project: impl Into<String>, bucket: impl Into<String>) -> Self {
        Self::new("google", project, bucket)
    }

    /// Construct a Google-compatible storage specification with a custom HTTP
    /// endpoint. `bucket` may be `bucket/prefix` for a multi-tenant container;
    /// when attaching it, use a slash-free local alias. The endpoint is an
    /// HTTP or HTTPS base URL and is encoded as the CBS module selector
    /// `google?endpoint=<base-url>`; it must not contain `&`, and trailing
    /// slashes are normalized by CBS.
    pub fn google_with_endpoint(
        project: impl Into<String>,
        bucket: impl Into<String>,
        endpoint: impl AsRef<str>,
    ) -> Self {
        Self::new(
            format!("google?endpoint={}", endpoint.as_ref()),
            project,
            bucket,
        )
    }
}

/// A container attachment request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachSpec {
    /// Cloud storage identity.
    pub storage: Storage,
    /// Local VFS alias. Defaults to the remote container name. Use a
    /// slash-free alias when the remote container contains `/`.
    pub alias: Option<String>,
    /// Request secure daemon-mode cache handling.
    pub secure: bool,
    /// Treat an existing alias as success.
    pub if_not: bool,
}

impl AttachSpec {
    /// Construct a generic attachment request.
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            alias: None,
            secure: false,
            if_not: false,
        }
    }

    /// Construct a Google Cloud Storage attachment. `bucket` may be
    /// `bucket/prefix`; use [`Self::alias`] with a slash-free alias in that
    /// case.
    pub fn google(project: impl Into<String>, bucket: impl Into<String>) -> Self {
        Self::new(Storage::google(project, bucket))
    }

    /// Construct a Google-compatible attachment with a custom HTTP endpoint.
    /// `bucket` may be `bucket/prefix`; use [`Self::alias`] with a slash-free
    /// alias in that case. The endpoint is an HTTP or HTTPS base URL without
    /// `&`; its selector is encoded as `google?endpoint=<base-url>`.
    #[must_use]
    pub fn google_with_endpoint(
        project: impl Into<String>,
        bucket: impl Into<String>,
        endpoint: impl AsRef<str>,
    ) -> Self {
        Self::new(Storage::google_with_endpoint(project, bucket, endpoint))
    }

    /// Set the local alias.
    #[must_use]
    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Enable secure daemon-mode cache handling.
    #[must_use]
    pub fn secure(mut self, secure: bool) -> Self {
        self.secure = secure;
        self
    }

    /// Make an existing alias a successful no-op.
    #[must_use]
    pub fn if_not(mut self, if_not: bool) -> Self {
        self.if_not = if_not;
        self
    }
}

/// Integer configuration options accepted by the CBS VFS.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Config {
    /// Maximum local cache size in bytes.
    CacheSize(i64),
    /// Maximum number of simultaneous upload requests.
    RequestCount(i64),
    /// HTTP timeout in seconds.
    HttpTimeout(i64),
    /// Enable verbose libcurl logging.
    CurlVerbose(bool),
    /// HTTP-log entry timeout in seconds.
    HttpLogTimeout(i64),
    /// Maximum HTTP-log entries; negative means unlimited.
    HttpLogEntries(i64),
}

impl Config {
    fn raw(self) -> (c_int, i64) {
        match self {
            Self::CacheSize(v) => (raw::SQLITE_BCV_CACHESIZE, v),
            Self::RequestCount(v) => (raw::SQLITE_BCV_NREQUEST, v),
            Self::HttpTimeout(v) => (raw::SQLITE_BCV_HTTPTIMEOUT, v),
            Self::CurlVerbose(v) => (raw::SQLITE_BCV_CURLVERBOSE, i64::from(v)),
            Self::HttpLogTimeout(v) => (raw::SQLITE_BCV_HTTPLOG_TIMEOUT, v),
            Self::HttpLogEntries(v) => (raw::SQLITE_BCV_HTTPLOG_NENTRY, v),
        }
    }
}

/// Builder for the process-lifetime block-cache VFS.
pub struct Builder {
    directory: std::path::PathBuf,
    name: CString,
    auth: Box<AuthCallback>,
    config: Vec<Config>,
}

struct AuthState {
    callback: Box<AuthCallback>,
}

impl Builder {
    /// Create a builder using `directory` for the local block cache.
    pub fn new(directory: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            directory: directory.as_ref().to_owned(),
            name: CString::new("rusqdoltlite-bcvfs").map_err(Error::NulError)?,
            auth: Box::new(|_, _, _| Err(AuthError("no CBS auth callback configured".into()))),
            config: Vec::new(),
        })
    }

    /// Set the registered VFS name.
    pub fn name(mut self, name: &str) -> Result<Self> {
        self.name = CString::new(name).map_err(Error::NulError)?;
        Ok(self)
    }

    /// Set the cloud authentication callback.
    ///
    /// The returned bearer token is sent to the selected storage endpoint;
    /// use test credentials when targeting an emulator.
    #[must_use]
    pub fn auth_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(&str, &str, &str) -> std::result::Result<String, AuthError> + Send + Sync + 'static,
    {
        self.auth = Box::new(callback);
        self
    }

    /// Add a numeric VFS configuration option.
    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.config.push(config);
        self
    }

    /// Initialize and return the process-global VFS singleton.
    ///
    /// The first successful call owns the native VFS, callback, directory, and
    /// configuration for the remainder of the process. Later calls return the
    /// existing instance and ignore the later builder's settings.
    pub fn init(self) -> Result<&'static BlockCacheVfs> {
        let _guard = INIT_LOCK.lock().expect("CBS VFS init lock was poisoned");
        if let Some(existing) = INSTANCE.get() {
            return Ok(existing);
        }
        check(crate::ffi::initialize_doltlite())?;
        let directory = cstring_path(&self.directory)?;
        let mut fs = ptr::null_mut();
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_create(directory.as_ptr(), self.name.as_ptr(), &mut fs, &mut err)
        };
        result_with_err(rc, &mut err)?;
        if fs.is_null() {
            return Err(Error::SqliteFailure(
                crate::ffi::Error::new(crate::ffi::SQLITE_ERROR),
                None,
            ));
        }

        let mut auth = Box::new(AuthState {
            callback: self.auth,
        });
        let auth_ptr = (&mut *auth) as *mut AuthState as *mut c_void;
        let rc = unsafe { raw::sqlite3_bcvfs_auth_callback(fs, auth_ptr, Some(auth_trampoline)) };
        if let Err(error) = check(rc) {
            return Err(destroy_failed(fs, auth, error));
        }
        for config in &self.config {
            let (op, value) = config.raw();
            if let Err(error) = check(unsafe { raw::sqlite3_bcvfs_config(fs, op, value) }) {
                return Err(destroy_failed(fs, auth, error));
            }
        }
        let vfs = BlockCacheVfs {
            fs,
            name: self.name,
            _auth: auth,
        };
        if let Err(vfs) = INSTANCE.set(vfs) {
            // INIT_LOCK makes this unreachable for ordinary callers, but do
            // not drop a native VFS and its callback context if initialization
            // ever races with another path that sets the OnceLock.
            let _leaked = Box::leak(Box::new(vfs));
            return Ok(INSTANCE
                .get()
                .expect("CBS VFS singleton was initialized concurrently"));
        }
        Ok(INSTANCE
            .get()
            .expect("CBS VFS singleton was just initialized"))
    }
}

/// Process-lifetime CBS VFS handle.
pub struct BlockCacheVfs {
    fs: *mut raw::sqlite3_bcvfs,
    name: CString,
    _auth: Box<AuthState>,
}

unsafe impl Send for BlockCacheVfs {}
unsafe impl Sync for BlockCacheVfs {}

static INSTANCE: OnceLock<BlockCacheVfs> = OnceLock::new();
static INIT_LOCK: Mutex<()> = Mutex::new(());

impl BlockCacheVfs {
    /// Start building a process-lifetime VFS.
    pub fn builder(directory: impl AsRef<Path>) -> Result<Builder> {
        Builder::new(directory)
    }

    /// Return the VFS name used with SQLite open calls.
    pub fn name(&self) -> &str {
        self.name
            .to_str()
            .expect("VFS name was constructed from UTF-8")
    }

    /// Attach a remote storage container.
    pub fn attach(&self, spec: &AttachSpec) -> Result<()> {
        let storage = CString::new(spec.storage.provider.as_str()).map_err(Error::NulError)?;
        let account = CString::new(spec.storage.account.as_str()).map_err(Error::NulError)?;
        let container = CString::new(spec.storage.container.as_str()).map_err(Error::NulError)?;
        let alias = spec
            .alias
            .as_deref()
            .map(CString::new)
            .transpose()
            .map_err(Error::NulError)?;
        let flags = (spec.secure as c_int * raw::SQLITE_BCV_ATTACH_SECURE)
            | (spec.if_not as c_int * raw::SQLITE_BCV_ATTACH_IFNOT);
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_attach(
                self.fs,
                storage.as_ptr(),
                account.as_ptr(),
                container.as_ptr(),
                alias.as_ref().map_or(ptr::null(), |v| v.as_ptr()),
                flags,
                &mut err,
            )
        };
        result_with_err(rc, &mut err)
    }

    /// Detach a container with no open clients or unuploaded changes.
    pub fn detach(&self, alias: &str) -> Result<()> {
        self.with_container(alias, |alias, err| unsafe {
            raw::sqlite3_bcvfs_detach(self.fs, alias.as_ptr(), err)
        })
    }

    /// Return whether this VFS is connected to a CBS daemon.
    pub fn is_daemon(&self) -> bool {
        unsafe { raw::sqlite3_bcvfs_isdaemon(self.fs) != 0 }
    }

    /// Open a database path inside an attached container.
    pub fn open(&self, path: impl AsRef<Path>) -> Result<Connection> {
        self.open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
    }

    /// Open a database path with explicit SQLite flags.
    pub fn open_with_flags(&self, path: impl AsRef<Path>, flags: OpenFlags) -> Result<Connection> {
        let db = Connection::open_with_flags_and_vfs(path, flags, self.name())?;
        check(unsafe { raw::sqlite3_bcvfs_register_vtab(db.handle()) })?;
        Ok(db)
    }

    /// Refresh an attached container's manifest.
    pub fn poll(&self, container: &str) -> Result<()> {
        self.with_container(container, |container, err| unsafe {
            raw::sqlite3_bcvfs_poll(self.fs, container.as_ptr(), err)
        })
    }

    /// Upload local changes in an attached container.
    pub fn upload(&self, container: &str) -> Result<()> {
        self.with_container(container, |container, err| unsafe {
            raw::sqlite3_bcvfs_upload(self.fs, container.as_ptr(), None, ptr::null_mut(), err)
        })
    }

    fn with_container<F>(&self, name: &str, call: F) -> Result<()>
    where
        F: FnOnce(&CStr, *mut *mut c_char) -> c_int,
    {
        let container = CString::new(name).map_err(Error::NulError)?;
        let mut err = ptr::null_mut();
        result_with_err(call(&container, &mut err), &mut err)
    }
}

fn cstring_path(path: &Path) -> Result<CString> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        CString::new(path.as_os_str().as_bytes()).map_err(Error::NulError)
    }
    #[cfg(not(unix))]
    {
        CString::new(path.to_string_lossy().as_bytes()).map_err(Error::NulError)
    }
}

fn result_with_err(rc: c_int, err: &mut *mut c_char) -> Result<()> {
    let message = if (*err).is_null() {
        None
    } else {
        Some(unsafe { CStr::from_ptr(*err).to_string_lossy().into_owned() })
    };
    if !(*err).is_null() {
        unsafe { crate::ffi::sqlite3_free((*err).cast::<c_void>()) };
        *err = ptr::null_mut();
    }
    if rc == crate::ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(Error::SqliteFailure(crate::ffi::Error::new(rc), message))
    }
}

fn destroy_failed(fs: *mut raw::sqlite3_bcvfs, auth: Box<AuthState>, error: Error) -> Error {
    let rc = unsafe { raw::sqlite3_bcvfs_destroy(fs) };
    if rc != crate::ffi::SQLITE_OK {
        // The C object may still call its registered callback if destruction
        // failed; keep the callback context alive for the rest of the process.
        Box::leak(auth);
        return Error::SqliteFailure(crate::ffi::Error::new(rc), None);
    }
    error
}

unsafe extern "C" fn auth_trampoline(
    ctx: *mut c_void,
    storage: *const c_char,
    account: *const c_char,
    container: *const c_char,
    out: *mut *mut c_char,
) -> c_int {
    if out.is_null() || ctx.is_null() {
        return crate::ffi::SQLITE_ERROR;
    }
    *out = ptr::null_mut();
    if storage.is_null() || account.is_null() || container.is_null() {
        return crate::ffi::SQLITE_ERROR;
    }
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let callback = &*(ctx as *const AuthState);
        let storage = CStr::from_ptr(storage).to_str().ok()?;
        let account = CStr::from_ptr(account).to_str().ok()?;
        let container = CStr::from_ptr(container).to_str().ok()?;
        (callback.callback)(storage, account, container).ok()
    }));
    let Some(Some(token)) = result.ok() else {
        return crate::ffi::SQLITE_ERROR;
    };
    if token.as_bytes().contains(&0) {
        return crate::ffi::SQLITE_ERROR;
    }
    let len = match token
        .len()
        .checked_add(1)
        .and_then(|v| c_int::try_from(v).ok())
    {
        Some(len) => len,
        None => return crate::ffi::SQLITE_TOOBIG,
    };
    let ptr = crate::ffi::sqlite3_malloc(len).cast::<u8>();
    if ptr.is_null() {
        return crate::ffi::SQLITE_NOMEM;
    }
    unsafe {
        ptr::copy_nonoverlapping(token.as_ptr(), ptr, token.len());
        *ptr.add(token.len()) = 0;
        *out = ptr.cast();
    }
    crate::ffi::SQLITE_OK
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn google_attachment_uses_builtin_provider() {
        let spec = AttachSpec::google("project", "bucket").alias("data");
        assert_eq!(spec.storage, Storage::new("google", "project", "bucket"));
        assert_eq!(spec.alias.as_deref(), Some("data"));

        let local = AttachSpec::google_with_endpoint("project", "bucket", "http://localhost:4443/");
        assert_eq!(
            local.storage.provider,
            "google?endpoint=http://localhost:4443/"
        );
    }

    #[test]
    fn config_maps_to_cbs_constants() {
        assert_eq!(Config::CacheSize(42).raw(), (raw::SQLITE_BCV_CACHESIZE, 42));
        assert_eq!(
            Config::CurlVerbose(true).raw(),
            (raw::SQLITE_BCV_CURLVERBOSE, 1)
        );
    }

    #[test]
    fn native_vfs_is_registered_without_cloud_access() -> Result<()> {
        use std::io::{Read as _, Write as _};
        use std::net::TcpListener;
        use std::thread;
        use std::time::Duration;

        let directory = tempfile::tempdir().expect("temporary CBS directory");
        let vfs = BlockCacheVfs::builder(directory.path())?
            .auth_callback(|_, _, _| Ok("test-token".to_owned()))
            .init()?;
        let name = CString::new(vfs.name()).expect("VFS name has no NUL");
        let registered = unsafe { crate::ffi::sqlite3_vfs_find(name.as_ptr()) };
        assert!(!registered.is_null());
        assert!(!vfs.is_daemon());

        let listener = TcpListener::bind("127.0.0.1:0").expect("local HTTP listener");
        let address = listener.local_addr().expect("listener address");
        let server = thread::spawn(move || {
            listener
                .set_nonblocking(true)
                .expect("nonblocking HTTP listener");
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let (mut stream, _) = loop {
                match listener.accept() {
                    Ok(connection) => break connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "CBS attach did not connect to the endpoint within 5 seconds"
                        );
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("CBS manifest request: {error}"),
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
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .expect("HTTP response");
            String::from_utf8_lossy(&request).into_owned()
        });
        let endpoint = format!("http://{address}");
        let error = vfs
            .attach(
                &AttachSpec::google_with_endpoint(
                    "project",
                    "my-bucket/tenants/acme/cbs",
                    endpoint,
                )
                .alias("local"),
            )
            .expect_err("controlled manifest failure");
        let request = server.join().expect("HTTP server thread");
        assert!(
            request.starts_with("GET /my-bucket/tenants/acme/cbs/manifest.bcv"),
            "{request}"
        );
        assert!(
            format!("{error:?}").contains("NotFound") || format!("{error:?}").contains("Unknown")
        );
        Ok(())
    }
}

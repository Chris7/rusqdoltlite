//! Safe, process-lifetime access to Cloud Backed SQLite's block-cache VFS.

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::fmt;
use std::path::Path;
use std::ptr;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::error::{check, Error};
use crate::{Connection, OpenFlags, Result};

use crate::ffi::bcvutil as raw_util;
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

/// Encode temporary S3 credentials for the CBS authentication callback.
///
/// CBS receives the access key in [`Storage::s3`] and the value returned by
/// the callback as its secret.  A session token is represented by one newline
/// separator: `secret-access-key\nsession-token`.  The native S3 module
/// signs the token as `x-amz-security-token`; it is not placed in the module
/// selector or URL.  Both values must be non-empty and may not contain a
/// newline.
pub fn s3_secret_with_session_token(
    secret_access_key: impl AsRef<str>,
    session_token: impl AsRef<str>,
) -> std::result::Result<String, AuthError> {
    let secret_access_key = secret_access_key.as_ref();
    let session_token = session_token.as_ref();
    if secret_access_key.is_empty()
        || session_token.is_empty()
        || secret_access_key.contains(['\r', '\n'])
        || session_token.contains(['\r', '\n'])
    {
        return Err(AuthError(
            "S3 secret access keys and session tokens must be non-empty and newline-free".into(),
        ));
    }
    Ok(format!("{secret_access_key}\n{session_token}"))
}

/// Cloud storage identity and container.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Storage {
    /// CBS provider name, such as `google`, `s3`, or `azure`.
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

    /// Construct a Google Cloud Storage specification using the JSON API.
    ///
    /// `bucket` may be `bucket/prefix` for a multi-tenant container.  The
    /// authentication callback must return a Google bearer token.
    pub fn google_json(project: impl Into<String>, bucket: impl Into<String>) -> Self {
        Self::new("google?api=json", project, bucket)
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

    /// Construct a Google-compatible JSON API specification with a custom
    /// HTTP endpoint.  The endpoint is inserted into the CBS selector and
    /// therefore must not contain `&` (or other selector syntax).
    pub fn google_json_with_endpoint(
        project: impl Into<String>,
        bucket: impl Into<String>,
        endpoint: impl AsRef<str>,
    ) -> Self {
        Self::new(
            format!("google?api=json&endpoint={}", endpoint.as_ref()),
            project,
            bucket,
        )
    }

    /// Construct an AWS S3 specification using the standard AWS endpoint.
    ///
    /// `access_key` is passed as the CBS account.  Return the secret access
    /// key (or [`s3_secret_with_session_token`]) from the authentication
    /// callback.  `bucket` may be `bucket/prefix`. AWS uses virtual-hosted
    /// addressing for ordinary bucket names and path-style addressing for
    /// dotted bucket names so HTTPS certificate validation succeeds.
    pub fn s3(
        access_key: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self::new(format!("s3?region={}", region.into()), access_key, bucket)
    }

    /// Construct an S3-compatible specification using a custom HTTP(S)
    /// endpoint.  Custom endpoints use path-style addressing in the native
    /// module, which makes this suitable for local S3 emulators.
    pub fn s3_with_endpoint(
        access_key: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl AsRef<str>,
    ) -> Self {
        Self::new(
            format!("s3?region={}&endpoint={}", region.into(), endpoint.as_ref()),
            access_key,
            bucket,
        )
    }
}

/// Return the selector spelling used for session attachment and scope
/// derivation. CBS normalizes trailing slashes on its HTTP endpoint before
/// making requests, while the native session fence hashes the selector text
/// stored on the container. Normalize that spelling before attachment so
/// equivalent endpoint selectors cannot create distinct fence identities.
pub(crate) fn canonical_storage_provider(provider: &str) -> Result<String> {
    let Some((module, query)) = provider.split_once('?') else {
        return Ok(provider.to_owned());
    };
    let canonical_module = if module.eq_ignore_ascii_case("s3") {
        "s3"
    } else if module.eq_ignore_ascii_case("google") {
        "google"
    } else {
        return Ok(provider.to_owned());
    };

    let mut changed = module != canonical_module;
    let mut parameters = Vec::new();
    for parameter in query.split('&') {
        let Some((key, value)) = parameter.split_once('=') else {
            parameters.push(parameter.to_owned());
            continue;
        };
        let canonical_key = key.to_ascii_lowercase();
        if canonical_key == "endpoint" {
            let canonical_value = value.trim_end_matches('/');
            if canonical_value.is_empty() {
                return Err(Error::SqliteFailure(
                    crate::ffi::Error::new(crate::ffi::SQLITE_MISUSE),
                    Some("storage endpoint must not be empty".to_owned()),
                ));
            }
            if canonical_value != value || canonical_key != key {
                changed = true;
                parameters.push(format!("{canonical_key}={canonical_value}"));
                continue;
            }
        }
        if canonical_key != key {
            changed = true;
            parameters.push(format!("{canonical_key}={value}"));
        } else {
            parameters.push(parameter.to_owned());
        }
    }
    if changed {
        Ok(format!("{canonical_module}?{}", parameters.join("&")))
    } else {
        Ok(provider.to_owned())
    }
}

/// Canonicalize the provider selectors accepted by session attachments.
///
/// Native CBS accepts defaults and tuning parameters that can address the
/// same object namespace while producing different selector strings. Session
/// fences therefore use one spelling: S3 requires an explicit region, and
/// Google requires the JSON API. Max-results, the XML Google selector, and
/// currently un-audited Azure selector forms are intentionally not accepted.
pub(crate) fn canonical_session_storage_provider(provider: &str) -> Result<String> {
    let provider = canonical_storage_provider(provider)?;
    let (module, query) = provider
        .split_once('?')
        .map_or((provider.as_str(), ""), |(module, query)| (module, query));
    match module {
        "s3" => {
            let mut region = None;
            let mut endpoint = None;
            if query.is_empty() {
                return Err(noncanonical_session_selector(
                    "S3 sessions require an explicit region selector",
                ));
            }
            for parameter in query.split('&') {
                let Some((key, value)) = parameter.split_once('=') else {
                    return Err(noncanonical_session_selector(
                        "S3 sessions require region=<region> and may use endpoint=<url>",
                    ));
                };
                match key {
                    "region"
                        if region.is_none()
                            && !value.is_empty()
                            && value == value.to_ascii_lowercase() =>
                    {
                        region = Some(value);
                    }
                    "endpoint" if endpoint.is_none() && !value.is_empty() => {
                        endpoint = Some(value);
                    }
                    _ => {
                        return Err(noncanonical_session_selector(
                            "S3 sessions require region=<region> and may use endpoint=<url>",
                        ));
                    }
                }
            }
            let Some(region) = region else {
                return Err(noncanonical_session_selector(
                    "S3 sessions require an explicit region selector",
                ));
            };
            let mut canonical = format!("s3?region={region}");
            if let Some(endpoint) = endpoint {
                canonical.push_str("&endpoint=");
                canonical.push_str(endpoint);
            }
            Ok(canonical)
        }
        "google" => {
            let mut json_api = false;
            let mut endpoint = None;
            for parameter in query.split('&') {
                let Some((key, value)) = parameter.split_once('=') else {
                    return Err(noncanonical_session_selector(
                        "Google sessions require api=json and may use endpoint=<url>",
                    ));
                };
                match key {
                    "api" if !json_api && value.eq_ignore_ascii_case("json") => {
                        json_api = true;
                    }
                    "endpoint" if endpoint.is_none() && !value.is_empty() => {
                        endpoint = Some(value);
                    }
                    _ => {
                        return Err(noncanonical_session_selector(
                            "Google sessions require api=json and may use endpoint=<url>",
                        ));
                    }
                }
            }
            if !json_api {
                return Err(noncanonical_session_selector(
                    "Google sessions require the api=json selector",
                ));
            }
            let mut canonical = "google?api=json".to_owned();
            if let Some(endpoint) = endpoint {
                canonical.push_str("&endpoint=");
                canonical.push_str(endpoint);
            }
            Ok(canonical)
        }
        "azure" => Err(noncanonical_session_selector(
            "Azure session selectors are unavailable until their canonical form is defined",
        )),
        _ => Err(noncanonical_session_selector(
            "unknown CBS provider selectors are unavailable for session attachments",
        )),
    }
}

fn noncanonical_session_selector(message: &str) -> Error {
    Error::SqliteFailure(
        crate::ffi::Error::new(crate::ffi::SQLITE_MISUSE),
        Some(message.to_owned()),
    )
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

    /// Construct a Google Cloud Storage JSON API attachment. `bucket` may be
    /// `bucket/prefix`; use [`Self::alias`] with a slash-free alias in that
    /// case.
    pub fn google_json(project: impl Into<String>, bucket: impl Into<String>) -> Self {
        Self::new(Storage::google_json(project, bucket))
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

    /// Construct a Google-compatible JSON API attachment with a custom
    /// endpoint. `bucket` may be `bucket/prefix`; use [`Self::alias`] with a
    /// slash-free alias in that case. The endpoint is an HTTP or HTTPS base
    /// URL, must not contain `&`, and is sent the callback's bearer token.
    #[must_use]
    pub fn google_json_with_endpoint(
        project: impl Into<String>,
        bucket: impl Into<String>,
        endpoint: impl AsRef<str>,
    ) -> Self {
        Self::new(Storage::google_json_with_endpoint(
            project, bucket, endpoint,
        ))
    }

    /// Construct an AWS S3 attachment.  The authentication callback should
    /// return the secret access key, optionally encoded with
    /// [`s3_secret_with_session_token`].
    pub fn s3(
        access_key: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self::new(Storage::s3(access_key, bucket, region))
    }

    /// Construct an S3-compatible attachment using a custom endpoint.
    #[must_use]
    pub fn s3_with_endpoint(
        access_key: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
        endpoint: impl AsRef<str>,
    ) -> Self {
        Self::new(Storage::s3_with_endpoint(
            access_key, bucket, region, endpoint,
        ))
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

/// A session identifier bound to one block-cache attachment.
///
/// Session identifiers are carried by the Rust attachment context and copied
/// into the alias-scoped native CBS container metadata. They are never stored
/// in process-global "current session" state; the native value exists only to
/// validate ownership and recovery for this attachment alias.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionId(String);

impl SessionId {
    /// Validate and store a canonical UUID session identifier.
    pub fn new(session_id: impl AsRef<str>) -> Result<Self> {
        let session_id = session_id.as_ref();
        if session_id.len() != 36 {
            return Err(Error::SqliteFailure(
                crate::ffi::Error::new(crate::ffi::SQLITE_MISUSE),
                Some("block-cache session ID must be a canonical non-nil UUID".to_owned()),
            ));
        }
        let session_id = session_id.to_ascii_lowercase();
        if !is_canonical_uuid(&session_id) {
            return Err(Error::SqliteFailure(
                crate::ffi::Error::new(crate::ffi::SQLITE_MISUSE),
                Some("block-cache session ID must be a canonical non-nil UUID".to_owned()),
            ));
        }
        Ok(Self(session_id))
    }

    /// Return the validated UUID text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Number of bytes in a per-request session operation identifier.
pub const SESSION_OPERATION_ID_BYTES: usize = raw::SQLITE_BCVFS_SESSION_HASH_BYTES;

/// Opaque, non-zero identifier for one application request operation.
///
/// This is distinct from the client session UUID. Applications may derive it
/// deterministically from the exact request bytes to make retries idempotent,
/// but the VFS treats it only as an opaque fence key.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionOperationId([u8; SESSION_OPERATION_ID_BYTES]);

impl SessionOperationId {
    /// Validate and store a fixed-size, non-zero operation identifier.
    pub fn new(operation_id: impl AsRef<[u8]>) -> Result<Self> {
        let operation_id = operation_id.as_ref();
        if operation_id.len() != SESSION_OPERATION_ID_BYTES
            || operation_id.iter().all(|byte| *byte == 0)
        {
            return Err(Error::SqliteFailure(
                crate::ffi::Error::new(crate::ffi::SQLITE_MISUSE),
                Some(format!(
                    "session operation ID must be exactly {SESSION_OPERATION_ID_BYTES} non-zero bytes"
                )),
            ));
        }
        let mut value = [0_u8; SESSION_OPERATION_ID_BYTES];
        value.copy_from_slice(operation_id);
        Ok(Self(value))
    }

    /// Return the operation identifier bytes for the native session fence.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; SESSION_OPERATION_ID_BYTES] {
        &self.0
    }
}

/// Result of checking a session operation before invoking or retrying its
/// mutating handler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionOperationStatus {
    /// This operation has not been accepted and its captured predecessor is
    /// still current. This does not prove that an earlier handler invocation
    /// did not run.
    New,
    /// This operation has been accepted but not published.
    Accepted,
    /// This operation has been published and is complete.
    Committed,
    /// The operation was rejected or expired.
    Failed,
    /// A different operation owns the current session head.
    Conflict,
}

impl SessionOperationStatus {
    fn from_native(value: c_int) -> Result<Self> {
        match value {
            raw::SQLITE_BCVFS_SESSION_STATUS_NEW => Ok(Self::New),
            raw::SQLITE_BCVFS_SESSION_STATUS_ACCEPTED => Ok(Self::Accepted),
            raw::SQLITE_BCVFS_SESSION_STATUS_COMMITTED => Ok(Self::Committed),
            raw::SQLITE_BCVFS_SESSION_STATUS_FAILED => Ok(Self::Failed),
            raw::SQLITE_BCVFS_SESSION_STATUS_CONFLICT => Ok(Self::Conflict),
            _ => Err(Error::SqliteFailure(
                crate::ffi::Error::new(crate::ffi::SQLITE_CORRUPT),
                Some("native CBS returned an unknown session operation status".to_owned()),
            )),
        }
    }
}

/// A block-cache attachment owned by one server/application session.
///
/// The attachment stores its session identity locally and never changes a
/// process-wide "current session".  Its alias is attached with `IFNOT`
/// disabled, so an existing alias can never be silently reused by a session
/// with a different storage context. A second live owner is rejected because
/// the current CBS container is a mutable request overlay; same-session reuse
/// is permitted only after the prior owner has released it.
pub struct SessionAttachment {
    vfs: &'static BlockCacheVfs,
    alias: String,
    session_id: SessionId,
    operation_id: Option<SessionOperationId>,
}

impl fmt::Debug for SessionAttachment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionAttachment")
            .field("alias", &self.alias)
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl SessionAttachment {
    /// Return the validated session identifier bound to this attachment.
    #[must_use]
    pub fn session_id(&self) -> &str {
        self.session_id.as_str()
    }

    /// Return the opaque operation identifier bound to this attachment.
    #[must_use]
    pub fn operation_id(&self) -> Option<&SessionOperationId> {
        self.operation_id.as_ref()
    }

    /// Check this operation's durable session state before invoking or
    /// retrying a mutating handler. A conflict means another operation owns
    /// the current session head and must not be retried blindly.
    pub fn operation_status(&self) -> Result<SessionOperationStatus> {
        let alias = CString::new(self.alias.as_str()).map_err(Error::NulError)?;
        let session_id = CString::new(self.session_id.as_str()).map_err(Error::NulError)?;
        let mut status = raw::SQLITE_BCVFS_SESSION_STATUS_NEW;
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_session_operation_status(
                self.vfs.fs,
                alias.as_ptr(),
                session_id.as_ptr(),
                &mut status,
                &mut err,
            )
        };
        result_with_err(rc, &mut err)?;
        SessionOperationStatus::from_native(status)
    }

    /// Return the local alias used by the native VFS.
    #[must_use]
    pub fn alias(&self) -> &str {
        &self.alias
    }

    /// Persist the current logical database state as an immutable session
    /// checkpoint without advancing the accepted or published heads.
    #[cfg(feature = "remote")]
    pub(crate) fn checkpoint(&self) -> Result<()> {
        let alias = CString::new(self.alias.as_str()).map_err(Error::NulError)?;
        let session_id = CString::new(self.session_id.as_str()).map_err(Error::NulError)?;
        let mut checkpoint_hash = [0_u8; raw::SQLITE_BCVFS_SESSION_HASH_BYTES];
        let mut etag = ptr::null_mut();
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_session_checkpoint(
                self.vfs.fs,
                alias.as_ptr(),
                session_id.as_ptr(),
                checkpoint_hash.as_mut_ptr(),
                &mut etag,
                &mut err,
            )
        };
        let result = result_with_err(rc, &mut err);
        if !etag.is_null() {
            unsafe { crate::ffi::sqlite3_free(etag.cast::<c_void>()) };
        }
        result
    }

    /// Advance the accepted request head after the final checkpoint. Native
    /// CBS performs the predecessor ETag CAS and exact duplicate
    /// reconciliation; this does not publish the ordinary manifest.
    #[cfg(feature = "remote")]
    pub(crate) fn accept(&self) -> Result<()> {
        let alias = CString::new(self.alias.as_str()).map_err(Error::NulError)?;
        let session_id = CString::new(self.session_id.as_str()).map_err(Error::NulError)?;
        let mut etag = ptr::null_mut();
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_session_accept(
                self.vfs.fs,
                alias.as_ptr(),
                session_id.as_ptr(),
                &mut etag,
                &mut err,
            )
        };
        let result = result_with_err(rc, &mut err);
        if !etag.is_null() {
            unsafe { crate::ffi::sqlite3_free(etag.cast::<c_void>()) };
        }
        result
    }

    /// Publish the accepted session checkpoint through native CBS's fenced
    /// publication state machine. The native call resolves storage and the
    /// candidate from the owned alias; no caller-supplied tip or container is
    /// accepted.
    pub fn upload(&self) -> Result<()> {
        let alias = CString::new(self.alias.as_str()).map_err(Error::NulError)?;
        let session_id = CString::new(self.session_id.as_str()).map_err(Error::NulError)?;
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_session_upload(
                self.vfs.fs,
                alias.as_ptr(),
                session_id.as_ptr(),
                &mut err,
            )
        };
        result_with_err(rc, &mut err)
    }

    /// Checkpoint and publish this operation while claiming the session head
    /// directly in PUBLISHING state. Unlike [`Self::upload`], this is the
    /// first publication attempt for a still-NEW operation and does not
    /// expose an intermediate ACCEPTED head to a successor request.
    #[cfg(feature = "remote")]
    pub(crate) fn finalize(&self) -> Result<()> {
        let alias = CString::new(self.alias.as_str()).map_err(Error::NulError)?;
        let session_id = CString::new(self.session_id.as_str()).map_err(Error::NulError)?;
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_session_finalize(
                self.vfs.fs,
                alias.as_ptr(),
                session_id.as_ptr(),
                &mut err,
            )
        };
        result_with_err(rc, &mut err)
    }
}

impl Drop for SessionAttachment {
    fn drop(&mut self) {
        // Native detach can legitimately return SQLITE_BUSY while a database
        // or unpublished local changes are still present. The session-aware
        // native release clears the exclusive owner state while preserving
        // durable session metadata for later recovery.
        let Ok(alias) = CString::new(self.alias.as_str()) else {
            return;
        };
        let Ok(session_id) = CString::new(self.session_id.as_str()) else {
            return;
        };
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_detach_session(
                self.vfs.fs,
                alias.as_ptr(),
                session_id.as_ptr(),
                &mut err,
            )
        };
        let _ = result_with_err(rc, &mut err);
    }
}

fn is_canonical_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                *byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
        && bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| !matches!(index, 8 | 13 | 18 | 23) && *byte != b'0')
}

pub(crate) fn session_alias(spec: &AttachSpec, session_id: &SessionId) -> Result<String> {
    let alias = spec
        .alias
        .clone()
        .unwrap_or_else(|| format!("session-{}", session_id.as_str()));
    if alias.is_empty()
        || alias == "."
        || alias == ".."
        || alias.contains('/')
        || alias.contains('\\')
    {
        return Err(Error::SqliteFailure(
            crate::ffi::Error::new(crate::ffi::SQLITE_MISUSE),
            Some("session attachment alias must be one non-empty path component".into()),
        ));
    }
    Ok(alias)
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
    /// Enable verbose libcurl logging. Only non-header diagnostic text is
    /// written to stderr; raw HTTP headers and payloads are omitted, excluding
    /// raw credential-bearing HTTP headers from verbose output.
    CurlVerbose(bool),
    /// HTTP-log entry timeout in seconds.
    HttpLogTimeout(i64),
    /// Maximum HTTP-log entries; negative means unlimited.
    HttpLogEntries(i64),
    /// Cache occupancy percentage at which dirty blocks are proactively
    /// staged. The native VFS accepts values from 1 through 100, inclusive;
    /// its default is 90. Values outside that range make VFS initialization
    /// fail with `SQLITE_MISUSE`.
    StageWatermark(i64),
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
            Self::StageWatermark(v) => (raw::SQLITE_BCV_STAGEWATERMARK, v),
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
    /// The callback owns the provider credential material.  Google returns a
    /// bearer token, while S3 returns the secret access key (or the newline
    /// encoding produced by [`s3_secret_with_session_token`]); the native
    /// module keeps those credentials out of request URLs and logs.  Use test
    /// credentials when targeting an emulator.
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

    /// Attach a container to a validated application session.
    ///
    /// Session attachments intentionally do not honor [`AttachSpec::if_not`]:
    /// accepting an existing alias would make it impossible to prove that the
    /// alias belongs to this session and storage scope. When no alias is
    /// supplied, a session-specific local alias is derived from the UUID so
    /// independent sessions for one remote container do not collide. A second
    /// live owner is rejected; same-session recovery is allowed only after the
    /// prior owner released the attachment. A different session or storage
    /// scope is always rejected. The native storage scope currently includes
    /// [`Storage::account`]. For S3 this is the access key, so rotating that
    /// key requires a fresh VFS/cache instance until rehydration can replace
    /// the local container credentials; changing only the callback's secret or
    /// session token does not change this local identity.
    pub fn attach_session(
        &'static self,
        spec: &AttachSpec,
        session_id: impl AsRef<str>,
    ) -> Result<SessionAttachment> {
        let session_id = SessionId::new(session_id)?;
        if self.is_daemon() {
            return Err(Error::SqliteFailure(
                crate::ffi::Error::new(crate::ffi::SQLITE_MISUSE),
                Some("session-aware attachments are unavailable through the CBS daemon".to_owned()),
            ));
        }
        let mut canonical_spec = spec.clone();
        canonical_spec.storage.provider =
            canonical_session_storage_provider(&canonical_spec.storage.provider)?;
        let alias_text = session_alias(&canonical_spec, &session_id)?;

        let storage =
            CString::new(canonical_spec.storage.provider.as_str()).map_err(Error::NulError)?;
        let account =
            CString::new(canonical_spec.storage.account.as_str()).map_err(Error::NulError)?;
        let container =
            CString::new(canonical_spec.storage.container.as_str()).map_err(Error::NulError)?;
        let alias = CString::new(alias_text.as_str()).map_err(Error::NulError)?;
        let session_id_c = CString::new(session_id.as_str()).map_err(Error::NulError)?;
        let flags = canonical_spec.secure as c_int * raw::SQLITE_BCV_ATTACH_SECURE;
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_attach_session(
                self.fs,
                storage.as_ptr(),
                account.as_ptr(),
                container.as_ptr(),
                alias.as_ptr(),
                session_id_c.as_ptr(),
                flags,
                &mut err,
            )
        };
        result_with_err(rc, &mut err)?;

        Ok(SessionAttachment {
            vfs: self,
            alias: alias_text,
            session_id,
            operation_id: None,
        })
    }

    /// Attach and atomically bind an authorized request context.
    ///
    /// The native operation ID is mandatory and distinct from the client
    /// session UUID. Native CBS rehydrates the accepted head internally after
    /// binding this context; a missing head selects the zero initial
    /// predecessor and sequence. Callers never supply an expected tip.
    pub fn attach_session_scoped(
        &'static self,
        spec: &AttachSpec,
        session_id: impl AsRef<str>,
        principal: &str,
        database: &str,
        operations: &str,
        operation_id: &SessionOperationId,
    ) -> Result<SessionAttachment> {
        let session_id = SessionId::new(session_id)?;
        if self.is_daemon() {
            return Err(Error::SqliteFailure(
                crate::ffi::Error::new(crate::ffi::SQLITE_MISUSE),
                Some("session-aware attachments are unavailable through the CBS daemon".to_owned()),
            ));
        }
        let mut canonical_spec = spec.clone();
        canonical_spec.storage.provider =
            canonical_session_storage_provider(&canonical_spec.storage.provider)?;
        let alias_text = session_alias(&canonical_spec, &session_id)?;

        let storage =
            CString::new(canonical_spec.storage.provider.as_str()).map_err(Error::NulError)?;
        let account =
            CString::new(canonical_spec.storage.account.as_str()).map_err(Error::NulError)?;
        let container =
            CString::new(canonical_spec.storage.container.as_str()).map_err(Error::NulError)?;
        let alias = CString::new(alias_text.as_str()).map_err(Error::NulError)?;
        let session_id_c = CString::new(session_id.as_str()).map_err(Error::NulError)?;
        let principal = CString::new(principal).map_err(Error::NulError)?;
        let database = CString::new(database).map_err(Error::NulError)?;
        let operations = CString::new(operations).map_err(Error::NulError)?;
        let flags = canonical_spec.secure as c_int * raw::SQLITE_BCV_ATTACH_SECURE;
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_attach_session_scoped(
                self.fs,
                storage.as_ptr(),
                account.as_ptr(),
                container.as_ptr(),
                alias.as_ptr(),
                session_id_c.as_ptr(),
                principal.as_ptr(),
                database.as_ptr(),
                operations.as_ptr(),
                operation_id.as_bytes().as_ptr(),
                flags,
                &mut err,
            )
        };
        result_with_err(rc, &mut err)?;

        Ok(SessionAttachment {
            vfs: self,
            alias: alias_text,
            session_id,
            operation_id: Some(*operation_id),
        })
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
            OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
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

    /// Delete a database from an attached container locally.
    ///
    /// The remote database is deleted only after [`Self::upload`] is called
    /// for the same container.
    pub fn delete_database(&self, container: &str, database: &str) -> Result<()> {
        let database = CString::new(database).map_err(Error::NulError)?;
        self.with_container(container, |container, err| unsafe {
            raw::sqlite3_bcvfs_delete(self.fs, container.as_ptr(), database.as_ptr(), err)
        })
    }

    /// Initialize a new CBS container and manifest if it does not exist.
    ///
    /// This operation is non-destructive: an existing manifest is rejected
    /// by a provider conditional-create request and remains unchanged. Only
    /// call it for a new storage container or prefix. CBS also attempts to
    /// create a provider bucket when its backend supports that operation; for
    /// providers where bucket creation is privileged, create it with the
    /// provider's management API first.
    pub fn initialize_container(&self, storage: &Storage) -> Result<()> {
        let handle = self.open_bcv(storage, "initialize_container")?;
        let rc = unsafe { raw_util::sqlite3_bcv_create_if_not_exists(handle.0, 0, 0) };
        bcv_result("initialize_container", rc, &handle)
    }

    /// Remove eligible orphaned blocks from a remote CBS storage target.
    ///
    /// This opens an independent CBS management handle for `storage`; it does
    /// not require an attached database. Applications should call it from
    /// their own scheduled maintenance job. It is never run automatically by
    /// request handling, attachment, `xSync`, or [`Self::upload`].
    ///
    /// `minimum_age` is the minimum age of an orphan before CBS may remove
    /// it. Fractional seconds are rounded up so an object is never treated as
    /// older than the requested duration.
    pub fn cleanup(&self, storage: &Storage, minimum_age: Duration) -> Result<()> {
        let minimum_age_seconds = cleanup_age_seconds(minimum_age)?;
        let handle = self.open_bcv(storage, "cleanup")?;
        let rc = unsafe { raw_util::sqlite3_bcv_cleanup(handle.0, minimum_age_seconds) };
        bcv_result("cleanup", rc, &handle)
    }

    /// Upload a valid, non-empty local SQLite database as a new remote name.
    ///
    /// The storage container must already have been initialized with
    /// [`Self::initialize_container`]. This is the bootstrap operation for a
    /// new remote database; [`Self::upload`] flushes changes to an attached
    /// database and is not a replacement for this method.
    pub fn create_database(
        &self,
        storage: &Storage,
        local_path: impl AsRef<Path>,
        remote_name: &str,
    ) -> Result<()> {
        let local_path = local_path.as_ref();
        let metadata = std::fs::metadata(local_path).map_err(|error| {
            Error::SqliteFailure(
                crate::ffi::Error::new(crate::ffi::SQLITE_CANTOPEN),
                Some(format!(
                    "create_database: cannot inspect local database: {error}"
                )),
            )
        })?;
        if metadata.len() == 0 {
            return Err(Error::SqliteFailure(
                crate::ffi::Error::new(crate::ffi::SQLITE_MISMATCH),
                Some("create_database: local database is empty".into()),
            ));
        }
        let local = cstring_path(local_path)?;
        let remote = CString::new(remote_name).map_err(Error::NulError)?;
        if remote_name.is_empty() {
            return Err(Error::SqliteFailure(
                crate::ffi::Error::new(crate::ffi::SQLITE_MISMATCH),
                Some("create_database: remote name is empty".into()),
            ));
        }

        // Open read-only through rusqlite first so an arbitrary non-empty
        // file cannot be advertised as a database to the native uploader.
        let connection = Connection::open_with_flags(
            local_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let _: i64 = connection.query_row("PRAGMA schema_version", [], |row| row.get(0))?;
        connection.close().map_err(|(_, error)| error)?;

        let handle = self.open_bcv(storage, "create_database")?;
        let rc = unsafe { raw_util::sqlite3_bcv_upload(handle.0, local.as_ptr(), remote.as_ptr()) };
        bcv_result("create_database", rc, &handle)
    }

    fn open_bcv(&self, storage: &Storage, operation: &str) -> Result<BcvHandle> {
        let module = CString::new(storage.provider.as_str()).map_err(Error::NulError)?;
        let account = CString::new(storage.account.as_str()).map_err(Error::NulError)?;
        let container = CString::new(storage.container.as_str()).map_err(Error::NulError)?;
        let auth = self.auth_for(storage)?;
        let mut handle = ptr::null_mut();
        let rc = unsafe {
            raw_util::sqlite3_bcv_open(
                module.as_ptr(),
                account.as_ptr(),
                auth.as_ptr(),
                container.as_ptr(),
                &mut handle,
            )
        };
        let handle = BcvHandle(handle);
        if rc == crate::ffi::SQLITE_OK {
            if handle.0.is_null() {
                return Err(Error::SqliteFailure(
                    crate::ffi::Error::new(crate::ffi::SQLITE_NOMEM),
                    Some(format!("{operation}: native API returned a null handle")),
                ));
            }
            Ok(handle)
        } else {
            Err(bcv_error(operation, rc, &handle))
        }
    }

    fn auth_for(&self, storage: &Storage) -> Result<CString> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (self._auth.callback)(
                storage.provider.as_str(),
                storage.account.as_str(),
                storage.container.as_str(),
            )
        }));
        let token = match result {
            Ok(Ok(token)) => token,
            Ok(Err(error)) => {
                return Err(Error::SqliteFailure(
                    crate::ffi::Error::new(crate::ffi::SQLITE_AUTH),
                    Some(format!("CBS authentication callback failed: {error}")),
                ))
            }
            Err(_) => {
                return Err(Error::SqliteFailure(
                    crate::ffi::Error::new(crate::ffi::SQLITE_ERROR),
                    Some("CBS authentication callback panicked".into()),
                ))
            }
        };
        CString::new(token).map_err(Error::NulError)
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

struct BcvHandle(*mut raw_util::sqlite3_bcv);

impl Drop for BcvHandle {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { raw_util::sqlite3_bcv_close(self.0) };
        }
    }
}

fn bcv_result(operation: &str, rc: c_int, handle: &BcvHandle) -> Result<()> {
    if rc == crate::ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(bcv_error(operation, rc, handle))
    }
}

fn bcv_error(operation: &str, rc: c_int, handle: &BcvHandle) -> Error {
    let detail = if handle.0.is_null() {
        None
    } else {
        unsafe {
            let message = raw_util::sqlite3_bcv_errmsg(handle.0);
            (!message.is_null()).then(|| CStr::from_ptr(message).to_string_lossy().into_owned())
        }
    };
    let message = match detail {
        Some(detail) if !detail.is_empty() => {
            format!("{operation} failed (native/HTTP code {rc}): {detail}")
        }
        _ => format!("{operation} failed (native/HTTP code {rc})"),
    };
    // bcvutil may return HTTP status codes (>= 400), which are not SQLite
    // result codes. Keep the original code in the message and expose a valid
    // SQLite error category to callers.
    let sqlite_code = if rc >= 400 {
        crate::ffi::SQLITE_IOERR
    } else {
        rc
    };
    Error::SqliteFailure(crate::ffi::Error::new(sqlite_code), Some(message))
}

fn cleanup_age_seconds(minimum_age: Duration) -> Result<c_int> {
    let mut rounded_up_seconds = u128::from(minimum_age.as_secs());
    if minimum_age.subsec_nanos() != 0 {
        rounded_up_seconds += 1;
    }
    c_int::try_from(rounded_up_seconds).map_err(|_| {
        Error::SqliteFailure(
            crate::ffi::Error::new(crate::ffi::SQLITE_RANGE),
            Some("cleanup: minimum age exceeds the native seconds limit".into()),
        )
    })
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
    fn cleanup_age_seconds_rounds_up_and_checks_native_range() {
        assert_eq!(cleanup_age_seconds(Duration::ZERO).unwrap(), 0);
        assert_eq!(
            cleanup_age_seconds(Duration::from_secs(7)).unwrap(),
            7,
            "whole seconds should be preserved"
        );
        assert_eq!(
            cleanup_age_seconds(Duration::from_nanos(1)).unwrap(),
            1,
            "a fractional second must round up"
        );
        assert_eq!(
            cleanup_age_seconds(Duration::from_secs(7) + Duration::from_nanos(1)).unwrap(),
            8,
            "rounding up prevents deleting an object before its requested age"
        );
        assert_eq!(
            cleanup_age_seconds(Duration::from_secs(c_int::MAX as u64)).unwrap(),
            c_int::MAX
        );
        assert!(cleanup_age_seconds(
            Duration::from_secs(c_int::MAX as u64) + Duration::from_nanos(1)
        )
        .is_err());
        assert!(cleanup_age_seconds(
            Duration::from_secs(c_int::MAX as u64) + Duration::from_secs(1)
        )
        .is_err());
        assert!(cleanup_age_seconds(Duration::MAX).is_err());
    }

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
    fn google_json_and_s3_selectors_are_exact() {
        assert_eq!(
            Storage::google_json("project", "bucket/prefix").provider,
            "google?api=json"
        );
        assert_eq!(
            Storage::google_json_with_endpoint("project", "bucket", "http://127.0.0.1:14091/")
                .provider,
            "google?api=json&endpoint=http://127.0.0.1:14091/"
        );

        let s3 = AttachSpec::s3("access", "bucket/prefix", "us-west-2").alias("local");
        assert_eq!(s3.storage.provider, "s3?region=us-west-2");
        assert_eq!(s3.storage.account, "access");
        assert_eq!(s3.storage.container, "bucket/prefix");
        assert_eq!(
            AttachSpec::s3_with_endpoint(
                "access",
                "bucket",
                "us-east-1",
                "http://127.0.0.1:14567/"
            )
            .storage
            .provider,
            "s3?region=us-east-1&endpoint=http://127.0.0.1:14567/"
        );
    }

    #[test]
    fn s3_session_credentials_use_one_unambiguous_separator() {
        assert_eq!(
            s3_secret_with_session_token("secret", "session").expect("valid credentials"),
            "secret\nsession"
        );
        assert!(s3_secret_with_session_token("", "session").is_err());
        assert!(s3_secret_with_session_token("secret", "bad\ntoken").is_err());
    }

    #[test]
    fn session_ids_require_canonical_uuid_text() {
        let valid = SessionId::new("550e8400-e29b-41d4-a716-446655440000")
            .expect("canonical UUID should be accepted");
        assert_eq!(valid.as_str(), "550e8400-e29b-41d4-a716-446655440000");
        let uppercase = SessionId::new("550E8400-E29B-41D4-A716-446655440000")
            .expect("UUID hex case should normalize");
        assert_eq!(uppercase.as_str(), valid.as_str());
        assert!(SessionId::new("").is_err());
        assert!(SessionId::new("00000000-0000-0000-0000-000000000000").is_err());
        assert!(SessionId::new("550e8400e29b41d4a716446655440000").is_err());
        assert!(SessionId::new("550e8400-e29b-41d4-a716-44665544000z").is_err());
        assert!(SessionId::new("550e8400-e29b-41d4-a716-446655440000\0").is_err());
    }

    #[test]
    fn session_operation_ids_require_nonzero_fixed_bytes() {
        let mut operation_id = [0_u8; SESSION_OPERATION_ID_BYTES];
        assert!(SessionOperationId::new(operation_id).is_err());
        assert!(SessionOperationId::new([1_u8; SESSION_OPERATION_ID_BYTES - 1]).is_err());
        operation_id[SESSION_OPERATION_ID_BYTES - 1] = 1;
        let operation_id =
            SessionOperationId::new(operation_id).expect("non-zero operation ID should pass");
        assert_eq!(operation_id.as_bytes()[SESSION_OPERATION_ID_BYTES - 1], 1);
    }

    #[test]
    fn session_operation_status_maps_native_states() {
        assert_eq!(
            SessionOperationStatus::from_native(raw::SQLITE_BCVFS_SESSION_STATUS_NEW)
                .expect("new status"),
            SessionOperationStatus::New
        );
        assert_eq!(
            SessionOperationStatus::from_native(raw::SQLITE_BCVFS_SESSION_STATUS_ACCEPTED)
                .expect("accepted status"),
            SessionOperationStatus::Accepted
        );
        assert_eq!(
            SessionOperationStatus::from_native(raw::SQLITE_BCVFS_SESSION_STATUS_COMMITTED)
                .expect("committed status"),
            SessionOperationStatus::Committed
        );
        assert_eq!(
            SessionOperationStatus::from_native(raw::SQLITE_BCVFS_SESSION_STATUS_CONFLICT)
                .expect("conflict status"),
            SessionOperationStatus::Conflict
        );
        assert!(SessionOperationStatus::from_native(99).is_err());
    }

    #[test]
    fn scoped_attach_preserves_native_validation_error_output() {
        let directory = tempfile::tempdir().expect("temporary CBS directory");
        let directory = CString::new(directory.path().to_str().expect("UTF-8 temp path"))
            .expect("path has no NUL");
        let name = CString::new("scoped-attach-abi").expect("name has no NUL");
        let storage = CString::new("s3?region=us-east-1").expect("storage has no NUL");
        let account = CString::new("account").expect("account has no NUL");
        let container = CString::new("bucket").expect("container has no NUL");
        let alias = CString::new("scoped-attach-abi").expect("alias has no NUL");
        let invalid_session =
            CString::new("not-a-canonical-session-id").expect("session has no NUL");
        let principal = CString::new("principal").expect("principal has no NUL");
        let database = CString::new("main").expect("database has no NUL");
        let operations = CString::new("read,write").expect("operations have no NUL");
        let operation_id = [1_u8; SESSION_OPERATION_ID_BYTES];

        let mut fs = ptr::null_mut();
        let mut create_err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_create(directory.as_ptr(), name.as_ptr(), &mut fs, &mut create_err)
        };
        result_with_err(rc, &mut create_err).expect("create test VFS");

        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_attach_session_scoped(
                fs,
                storage.as_ptr(),
                account.as_ptr(),
                container.as_ptr(),
                alias.as_ptr(),
                invalid_session.as_ptr(),
                principal.as_ptr(),
                database.as_ptr(),
                operations.as_ptr(),
                operation_id.as_ptr(),
                0,
                &mut err,
            )
        };
        assert_eq!(rc, crate::ffi::SQLITE_MISUSE);
        let message = unsafe {
            assert!(
                !err.is_null(),
                "native validation error was lost across the FFI"
            );
            CStr::from_ptr(err).to_string_lossy().into_owned()
        };
        assert!(message.contains("canonical non-nil UUID"), "{message}");
        unsafe {
            crate::ffi::sqlite3_free(err.cast::<c_void>());
            assert_eq!(raw::sqlite3_bcvfs_destroy(fs), crate::ffi::SQLITE_OK);
        }
    }

    #[test]
    fn session_attachments_derive_distinct_aliases_when_unspecified() {
        let first = SessionId::new("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let second = SessionId::new("550e8400-e29b-41d4-a716-446655440001").unwrap();
        let spec = AttachSpec::new(Storage::new("s3", "account", "bucket"));
        let first_alias = session_alias(&spec, &first).unwrap();
        let second_alias = session_alias(&spec, &second).unwrap();
        assert_ne!(first_alias, second_alias);
        assert!(first_alias.starts_with("session-"));
        assert!(second_alias.starts_with("session-"));

        let explicit = spec.clone().alias("stable");
        assert_eq!(session_alias(&explicit, &first).unwrap(), "stable");

        assert!(session_alias(&spec.clone().alias("bad/name"), &first).is_err());
        assert!(session_alias(&spec.clone().alias(r"bad\\name"), &first).is_err());
        assert!(session_alias(&spec.clone().alias("."), &first).is_err());
        assert!(session_alias(&spec.clone().alias(".."), &first).is_err());
    }

    #[test]
    fn s3_rejects_dot_segments_in_configured_prefix() {
        let module = CString::new("s3?endpoint=http://127.0.0.1:14567").unwrap();
        let account = CString::new("access").unwrap();
        let auth = CString::new("secret").unwrap();

        for container in [
            "bucket/.",
            "bucket/..",
            "bucket/tenant/../other",
            "bucket/tenant/../../other",
        ] {
            let container = CString::new(container).unwrap();
            let mut handle = ptr::null_mut();
            let rc = unsafe {
                raw_util::sqlite3_bcv_open(
                    module.as_ptr(),
                    account.as_ptr(),
                    auth.as_ptr(),
                    container.as_ptr(),
                    &mut handle,
                )
            };
            assert_ne!(
                rc,
                crate::ffi::SQLITE_OK,
                "dot-segment prefix {container:?} must not be accepted"
            );
            if !handle.is_null() {
                unsafe { raw_util::sqlite3_bcv_close(handle) };
            }
        }
    }

    #[test]
    fn config_maps_to_cbs_constants() {
        assert_eq!(Config::CacheSize(42).raw(), (raw::SQLITE_BCV_CACHESIZE, 42));
        assert_eq!(
            Config::CurlVerbose(true).raw(),
            (raw::SQLITE_BCV_CURLVERBOSE, 1)
        );
        assert_eq!(
            Config::StageWatermark(75).raw(),
            (raw::SQLITE_BCV_STAGEWATERMARK, 75)
        );
    }

    #[test]
    fn stage_watermark_accepts_one_through_one_hundred_only() -> Result<()> {
        let directory = tempfile::tempdir().expect("temporary CBS directory");
        let directory = CString::new(directory.path().to_str().expect("UTF-8 temp path"))
            .expect("path has no NUL");
        let name = CString::new("stage-watermark-test").expect("name has no NUL");

        let mut fs = ptr::null_mut();
        let mut err = ptr::null_mut();
        let rc = unsafe {
            raw::sqlite3_bcvfs_create(directory.as_ptr(), name.as_ptr(), &mut fs, &mut err)
        };
        result_with_err(rc, &mut err)?;
        assert!(!fs.is_null());

        for value in [1, 90, 100] {
            assert_eq!(
                unsafe { raw::sqlite3_bcvfs_config(fs, raw::SQLITE_BCV_STAGEWATERMARK, value) },
                crate::ffi::SQLITE_OK,
                "watermark {value} should be accepted"
            );
        }
        for value in [0, 101, i64::MAX] {
            assert_eq!(
                unsafe { raw::sqlite3_bcvfs_config(fs, raw::SQLITE_BCV_STAGEWATERMARK, value) },
                crate::ffi::SQLITE_MISUSE,
                "watermark {value} should be rejected"
            );
        }

        assert_eq!(
            unsafe { raw::sqlite3_bcvfs_destroy(fs) },
            crate::ffi::SQLITE_OK
        );
        Ok(())
    }

    #[test]
    fn cache_size_validation_rejects_invalid_values_after_block_size_is_known() -> Result<()> {
        const BLOCK_BYTES: i64 = 4 * 1024 * 1024;
        const MANIFEST_VERSION: u32 = 4;
        const NAME_BYTES: u32 = 16;
        const MANIFEST_HEADER_BYTES: usize = 6 * std::mem::size_of::<u32>();
        const DATABASE_HEADER_BYTES: usize = 6 * std::mem::size_of::<u32>() + 128;

        fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
            bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
        }

        // A minimal valid manifest is enough to make bcvfsCacheInit learn the
        // provider block size without contacting a remote backend.  The
        // database has no blocks; this test only exercises configuration.
        let manifest_len = MANIFEST_HEADER_BYTES + DATABASE_HEADER_BYTES;
        let mut manifest = vec![0_u8; manifest_len];
        put_u32(&mut manifest, 0, MANIFEST_VERSION);
        put_u32(&mut manifest, 4, BLOCK_BYTES as u32);
        put_u32(&mut manifest, 8, 1); // one database
        put_u32(&mut manifest, 12, 0); // no delete entries
        put_u32(&mut manifest, 16, NAME_BYTES);
        put_u32(&mut manifest, 20, 1); // largest database id
        let db = MANIFEST_HEADER_BYTES;
        put_u32(&mut manifest, db, 1); // database id
        put_u32(&mut manifest, db + 12, manifest_len as u32);
        let db_name = b"streaming.sqlite";
        manifest[db + 24..db + 24 + db_name.len()].copy_from_slice(db_name);

        let directory = tempfile::tempdir().expect("temporary CBS directory");
        let path = CString::new(directory.path().to_str().expect("UTF-8 temp path"))
            .expect("path has no NUL");
        let name = CString::new(format!("cache-size-boundary-{}", std::process::id()))
            .expect("name has no NUL");
        let mut fs = ptr::null_mut();
        let mut err = ptr::null_mut();
        let rc =
            unsafe { raw::sqlite3_bcvfs_create(path.as_ptr(), name.as_ptr(), &mut fs, &mut err) };
        result_with_err(rc, &mut err)?;
        assert!(!fs.is_null());
        assert_eq!(
            unsafe { raw::sqlite3_bcvfs_destroy(fs) },
            crate::ffi::SQLITE_OK
        );

        let metadata = Connection::open(directory.path().join("blocksdb.bcv"))?;
        metadata.execute(
            "INSERT INTO container(name, storage, user, container, manifest, etag) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            crate::params!["alias", "s3", "account", "bucket/prefix", manifest, "etag"],
        )?;
        drop(metadata);

        fs = ptr::null_mut();
        err = ptr::null_mut();
        let rc =
            unsafe { raw::sqlite3_bcvfs_create(path.as_ptr(), name.as_ptr(), &mut fs, &mut err) };
        result_with_err(rc, &mut err)?;
        assert!(!fs.is_null());

        for value in [0, -1, BLOCK_BYTES - 1, BLOCK_BYTES + 1] {
            assert_eq!(
                unsafe { raw::sqlite3_bcvfs_config(fs, raw::SQLITE_BCV_CACHESIZE, value) },
                crate::ffi::SQLITE_MISUSE,
                "cache size {value} should be rejected"
            );
        }
        for value in [BLOCK_BYTES, 2 * BLOCK_BYTES] {
            assert_eq!(
                unsafe { raw::sqlite3_bcvfs_config(fs, raw::SQLITE_BCV_CACHESIZE, value) },
                crate::ffi::SQLITE_OK,
                "cache size {value} should be accepted"
            );
        }

        assert_eq!(
            unsafe { raw::sqlite3_bcvfs_destroy(fs) },
            crate::ffi::SQLITE_OK
        );
        Ok(())
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

//! In-process DoltLite HTTP remote server.

use crate::error::error_from_sqlite_code;
use crate::{ffi, path_to_cstring, Result};
use std::ffi::{c_int, CString};
use std::fmt;
use std::path::Path;
use std::ptr::NonNull;

/// A running in-process DoltLite HTTP remote server.
///
/// The server runs on a native background thread. Dropping this value stops
/// that thread, waits for it to exit, and releases its native resources.
#[must_use = "dropping the server immediately stops its background thread"]
pub struct RemoteServer {
    raw: NonNull<ffi::DoltliteServer>,
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
        Self::start_on(directory, "127.0.0.1", 0)
    }

    /// Starts a server on the requested IPv4 address and port.
    ///
    /// Passing port `0` asks the operating system to select an available port.
    /// Binding a non-loopback address without TLS exposes an unencrypted,
    /// unauthenticated server; prefer [`RemoteServer::start`] unless the server
    /// is protected by a trusted network or reverse proxy.
    pub fn start_on<P: AsRef<Path>>(directory: P, bind_address: &str, port: u16) -> Result<Self> {
        let rc = ffi::initialize_doltlite();
        if rc != ffi::SQLITE_OK {
            return Err(error_from_sqlite_code(
                rc,
                Some("failed to initialize DoltLite".to_owned()),
            ));
        }

        let directory = path_to_cstring(directory.as_ref())?;
        let bind_address_c = CString::new(bind_address)?;
        let raw = unsafe {
            ffi::doltliteServeAsync(
                directory.as_ptr(),
                c_int::from(port),
                bind_address_c.as_ptr(),
            )
        };
        let raw = NonNull::new(raw).ok_or_else(|| {
            error_from_sqlite_code(
                ffi::SQLITE_ERROR,
                Some(format!(
                    "failed to start DoltLite remote server on {bind_address}:{port}"
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
            bind_address: bind_address.to_owned(),
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
        format!("http://{}:{}/{database}", self.bind_address, self.port)
    }
}

impl fmt::Debug for RemoteServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteServer")
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

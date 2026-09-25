#![expect(non_snake_case, non_camel_case_types)]
#![cfg_attr(not(test), no_std)]
pub use self::error::*;

use core::mem;
#[cfg(not(feature = "loadable_extension"))]
use core::sync::atomic::{AtomicI32, Ordering};

mod error;

#[must_use]
pub fn SQLITE_STATIC() -> sqlite3_destructor_type {
    None
}

#[must_use]
pub fn SQLITE_TRANSIENT() -> sqlite3_destructor_type {
    Some(unsafe { mem::transmute::<isize, unsafe extern "C" fn(*mut core::ffi::c_void)>(-1_isize) })
}

#[allow(dead_code, clippy::all)]
mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindgen.rs"));
}
pub use bindings::*;

/// Raw Cloud Backed SQLite block-cache VFS interface.
#[cfg(feature = "blockcachevfs")]
pub mod blockcachevfs {
    use core::ffi::{c_char, c_int, c_void};

    use super::{sqlite3, sqlite3_int64};

    /// Opaque Cloud Backed SQLite VFS handle.
    #[repr(C)]
    pub struct sqlite3_bcvfs {
        _private: [u8; 0],
    }

    /// Authentication callback used by the CBS VFS.
    pub type sqlite3_bcvfs_auth_callback = unsafe extern "C" fn(
        p_ctx: *mut c_void,
        z_storage: *const c_char,
        z_account: *const c_char,
        z_container: *const c_char,
        pz_auth_token: *mut *mut c_char,
    ) -> c_int;

    /// Busy callback used by an upload checkpoint.
    pub type sqlite3_bcvfs_busy_callback = unsafe extern "C" fn(*mut c_void, c_int) -> c_int;

    pub const SQLITE_BCV_CACHESIZE: c_int = 1;
    pub const SQLITE_BCV_NREQUEST: c_int = 2;
    pub const SQLITE_BCV_HTTPTIMEOUT: c_int = 3;
    pub const SQLITE_BCV_CURLVERBOSE: c_int = 4;
    pub const SQLITE_BCV_HTTPLOG_TIMEOUT: c_int = 5;
    pub const SQLITE_BCV_HTTPLOG_NENTRY: c_int = 6;
    /// Proactively stage dirty blocks once this percentage of the cache is occupied.
    pub const SQLITE_BCV_STAGEWATERMARK: c_int = 7;

    pub const SQLITE_BCV_ATTACH_SECURE: c_int = 0x0001;
    pub const SQLITE_BCV_ATTACH_IFNOT: c_int = 0x0002;

    unsafe extern "C" {
        pub fn sqlite3_bcvfs_create(
            z_dir: *const c_char,
            z_name: *const c_char,
            pp_fs: *mut *mut sqlite3_bcvfs,
            pz_err: *mut *mut c_char,
        ) -> c_int;
        pub fn sqlite3_bcvfs_destroy(fs: *mut sqlite3_bcvfs) -> c_int;
        pub fn sqlite3_bcvfs_isdaemon(fs: *mut sqlite3_bcvfs) -> c_int;
        pub fn sqlite3_bcvfs_register_vtab(db: *mut sqlite3) -> c_int;
        pub fn sqlite3_bcvfs_config(
            fs: *mut sqlite3_bcvfs,
            op: c_int,
            value: sqlite3_int64,
        ) -> c_int;
        pub fn sqlite3_bcvfs_auth_callback(
            fs: *mut sqlite3_bcvfs,
            auth_ctx: *mut c_void,
            auth: Option<sqlite3_bcvfs_auth_callback>,
        ) -> c_int;
        pub fn sqlite3_bcvfs_attach(
            fs: *mut sqlite3_bcvfs,
            z_storage: *const c_char,
            z_account: *const c_char,
            z_container: *const c_char,
            z_alias: *const c_char,
            flags: c_int,
            pz_err: *mut *mut c_char,
        ) -> c_int;
        pub fn sqlite3_bcvfs_detach(
            fs: *mut sqlite3_bcvfs,
            z_alias: *const c_char,
            pz_err: *mut *mut c_char,
        ) -> c_int;
        pub fn sqlite3_bcvfs_poll(
            fs: *mut sqlite3_bcvfs,
            z_container: *const c_char,
            pz_err: *mut *mut c_char,
        ) -> c_int;
        pub fn sqlite3_bcvfs_upload(
            fs: *mut sqlite3_bcvfs,
            z_container: *const c_char,
            busy: Option<sqlite3_bcvfs_busy_callback>,
            busy_ctx: *mut c_void,
            pz_err: *mut *mut c_char,
        ) -> c_int;
        pub fn sqlite3_bcvfs_delete(
            fs: *mut sqlite3_bcvfs,
            z_container: *const c_char,
            z_database: *const c_char,
            pz_err: *mut *mut c_char,
        ) -> c_int;
    }
}

/// Raw Cloud Backed SQLite database-management interface.
#[cfg(feature = "blockcachevfs")]
pub mod bcvutil {
    use core::ffi::{c_char, c_int};

    /// Opaque Cloud Backed SQLite database-management handle.
    #[repr(C)]
    pub struct sqlite3_bcv {
        _private: [u8; 0],
    }

    unsafe extern "C" {
        pub fn sqlite3_bcv_open(
            z_module: *const c_char,
            z_user: *const c_char,
            z_auth: *const c_char,
            z_container: *const c_char,
            pp_out: *mut *mut sqlite3_bcv,
        ) -> c_int;
        pub fn sqlite3_bcv_close(handle: *mut sqlite3_bcv);
        pub fn sqlite3_bcv_errmsg(handle: *mut sqlite3_bcv) -> *const c_char;
        pub fn sqlite3_bcv_upload(
            handle: *mut sqlite3_bcv,
            z_local: *const c_char,
            z_remote: *const c_char,
        ) -> c_int;
        pub fn sqlite3_bcv_create_if_not_exists(
            handle: *mut sqlite3_bcv,
            sz_name: c_int,
            sz_block: c_int,
        ) -> c_int;
    }
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
mod remote {
    use core::ffi::{c_char, c_int, c_long};

    #[repr(C)]
    pub struct DoltliteServer {
        _private: [u8; 0],
    }

    #[repr(C)]
    pub struct DoltliteServeOpts {
        pub zDir: *const c_char,
        pub port: c_int,
        pub zBindAddr: *const c_char,
        pub certFile: *const c_char,
        pub keyFile: *const c_char,
        pub authKeysDir: *const c_char,
        pub audience: *const c_char,
        pub timeoutMs: c_int,
        pub zVfsName: *const c_char,
    }

    unsafe extern "C" {
        pub fn doltliteServe(
            directory: *const c_char,
            port: c_int,
            bind_address: *const c_char,
        ) -> c_int;

        pub fn doltliteServeAsync(
            directory: *const c_char,
            port: c_int,
            bind_address: *const c_char,
        ) -> *mut DoltliteServer;

        pub fn doltliteServeOpts(options: *const DoltliteServeOpts) -> c_int;

        pub fn doltliteServeAsyncOpts(options: *const DoltliteServeOpts) -> *mut DoltliteServer;

        pub fn doltliteServerStop(server: *mut DoltliteServer);

        pub fn doltliteServerPort(server: *mut DoltliteServer) -> c_int;

        pub fn doltliteCredsVerifyBearer(
            authorization: *const c_char,
            expected_audience: *const c_char,
            authorized_keys_directory: *const c_char,
            now: c_long,
            key_id: *mut *mut c_char,
        ) -> c_int;
    }
}

#[cfg(all(feature = "remote", not(target_arch = "wasm32")))]
pub use remote::*;

#[cfg(not(feature = "loadable_extension"))]
unsafe extern "C" {
    fn doltliteInstallAutoExt() -> core::ffi::c_int;
}

#[cfg(not(feature = "loadable_extension"))]
static DOLTLITE_INIT_RESULT: AtomicI32 = AtomicI32::new(i32::MIN);

#[cfg(feature = "loadable_extension")]
pub fn initialize_doltlite() -> core::ffi::c_int {
    // A loadable extension runs inside an already-initialized host process;
    // do not load DoltLite's process-wide auto-extension a second time.
    SQLITE_OK
}

#[cfg(not(feature = "loadable_extension"))]
pub fn initialize_doltlite() -> core::ffi::c_int {
    let existing = DOLTLITE_INIT_RESULT.load(Ordering::Acquire);
    if existing != i32::MIN {
        return existing;
    }

    let result = unsafe { doltliteInstallAutoExt() };
    match DOLTLITE_INIT_RESULT.compare_exchange(
        i32::MIN,
        result,
        Ordering::AcqRel,
        Ordering::Acquire,
    ) {
        Ok(_) => result,
        Err(previous) => previous,
    }
}

impl Default for sqlite3_vtab {
    fn default() -> Self {
        unsafe { mem::zeroed() }
    }
}

impl Default for sqlite3_vtab_cursor {
    fn default() -> Self {
        unsafe { mem::zeroed() }
    }
}

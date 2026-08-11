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

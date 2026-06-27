#![expect(non_snake_case, non_camel_case_types)]
#![cfg_attr(not(test), no_std)]
pub use self::error::*;

use core::mem;
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

unsafe extern "C" {
    fn doltliteInstallAutoExt() -> core::ffi::c_int;
}

static DOLTLITE_INIT_RESULT: AtomicI32 = AtomicI32::new(i32::MIN);

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

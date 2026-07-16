//! Add, remove, or modify a collation
use std::cmp::Ordering;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::panic::catch_unwind;
use std::ptr;
use std::slice;

use crate::ffi;
use crate::util::free_boxed_value;
use crate::{Connection, Error, InnerConnection, Name, Result};

impl Connection {
    /// Add or modify a collation.
    #[inline]
    pub fn create_collation<C, N: Name>(&self, collation_name: N, x_compare: C) -> Result<()>
    where
        C: Fn(&str, &str) -> Ordering + Send + 'static,
    {
        self.db
            .borrow_mut()
            .create_collation(collation_name, x_compare)
    }

    /// Collation needed callback
    #[inline]
    pub fn collation_needed(&self, x_coll_needed: fn(&Self, &str) -> Result<()>) -> Result<()> {
        self.db.borrow_mut().collation_needed(x_coll_needed)
    }

    /// Remove collation.
    #[inline]
    pub fn remove_collation<N: Name>(&self, collation_name: N) -> Result<()> {
        self.db.borrow_mut().remove_collation(collation_name)
    }
}

impl InnerConnection {
    /// ```compile_fail
    /// use rusqlite::{Connection, Result};
    /// fn main() -> Result<()> {
    ///     let db = Connection::open_in_memory()?;
    ///     {
    ///         let mut called = std::sync::atomic::AtomicBool::new(false);
    ///         db.create_collation("foo", |_, _| {
    ///             called.store(true, std::sync::atomic::Ordering::Relaxed);
    ///             std::cmp::Ordering::Equal
    ///         })?;
    ///     }
    ///     let value: String = db.query_row(
    ///         "WITH cte(bar) AS
    ///        (VALUES ('v1'),('v2'),('v3'),('v4'),('v5'))
    ///         SELECT DISTINCT bar COLLATE foo FROM cte;",
    ///         [],
    ///         |row| row.get(0),
    ///     )?;
    ///     assert_eq!(value, "v1");
    ///     Ok(())
    /// }
    /// ```
    fn create_collation<C, N: Name>(&mut self, collation_name: N, x_compare: C) -> Result<()>
    where
        C: Fn(&str, &str) -> Ordering + Send + 'static,
    {
        unsafe extern "C" fn call_boxed_closure<C>(
            arg1: *mut c_void,
            arg2: c_int,
            arg3: *const c_void,
            arg4: c_int,
            arg5: *const c_void,
        ) -> c_int
        where
            C: Fn(&str, &str) -> Ordering,
        {
            let r = catch_unwind(|| {
                let boxed_f: *mut C = arg1.cast::<C>();
                assert!(!boxed_f.is_null(), "Internal error - null function pointer");
                let s1 = {
                    let c_slice = slice::from_raw_parts(arg3.cast::<u8>(), arg2 as usize);
                    String::from_utf8_lossy(c_slice)
                };
                let s2 = {
                    let c_slice = slice::from_raw_parts(arg5.cast::<u8>(), arg4 as usize);
                    String::from_utf8_lossy(c_slice)
                };
                (*boxed_f)(s1.as_ref(), s2.as_ref())
            });
            let t = match r {
                Err(_) => {
                    return -1; // FIXME How ?
                }
                Ok(r) => r,
            };

            match t {
                Ordering::Less => -1,
                Ordering::Equal => 0,
                Ordering::Greater => 1,
            }
        }

        let boxed_f: *mut C = Box::into_raw(Box::new(x_compare));
        let c_name = collation_name.as_cstr()?;
        let flags = ffi::SQLITE_UTF8;
        let r = unsafe {
            ffi::sqlite3_create_collation_v2(
                self.db(),
                c_name.as_ptr(),
                flags,
                boxed_f.cast::<c_void>(),
                Some(call_boxed_closure::<C>),
                Some(free_boxed_value::<C>),
            )
        };
        let res = self.decode_result(r);
        // The xDestroy callback is not called if the sqlite3_create_collation_v2()
        // function fails.
        if res.is_err() {
            drop(unsafe { Box::from_raw(boxed_f) });
        }
        res
    }

    fn collation_needed(
        &mut self,
        x_coll_needed: fn(&Connection, &str) -> Result<()>,
    ) -> Result<()> {
        use std::mem;
        #[expect(clippy::needless_return)]
        unsafe extern "C" fn collation_needed_callback(
            arg1: *mut c_void,
            arg2: *mut ffi::sqlite3,
            e_text_rep: c_int,
            arg3: *const c_char,
        ) {
            use std::str;

            if e_text_rep != ffi::SQLITE_UTF8 {
                // TODO: validate
                return;
            }

            let callback: fn(&Connection, &str) -> Result<()> = mem::transmute(arg1);
            let res = catch_unwind(|| {
                let conn = Connection::from_handle(arg2).unwrap();
                let collation_name = CStr::from_ptr(arg3)
                    .to_str()
                    .expect("illegal collation sequence name");
                callback(&conn, collation_name)
            })
            .unwrap_or_else(|_| Err(Error::UnwindingPanic));
            if let Err(err) = res {
                #[cfg(feature = "modern_sqlite")]
                // 3.51.0
                if let Ok(msg) = std::ffi::CString::new(format!("{}", err)) {
                    let _ = crate::error::set_errmsg(
                        arg2,
                        err.sqlite_extended_error_code()
                            .unwrap_or(ffi::SQLITE_ERROR),
                        Some(&msg),
                    );
                }
                return;
            }
        }

        let r = unsafe {
            ffi::sqlite3_collation_needed(
                self.db(),
                x_coll_needed as *mut c_void,
                Some(collation_needed_callback),
            )
        };
        self.decode_result(r)
    }

    #[inline]
    fn remove_collation<N: Name>(&mut self, collation_name: N) -> Result<()> {
        let c_name = collation_name.as_cstr()?;
        let r = unsafe {
            ffi::sqlite3_create_collation_v2(
                self.db(),
                c_name.as_ptr(),
                ffi::SQLITE_UTF8,
                ptr::null_mut(),
                None,
                None,
            )
        };
        self.decode_result(r)
    }
}

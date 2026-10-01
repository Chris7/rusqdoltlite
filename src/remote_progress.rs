use std::ffi::c_void;
use std::panic::{catch_unwind, AssertUnwindSafe};

use crate::{ffi, Connection, Error, Result};

type Callback = dyn Fn(DoltPushProgressEvent) + Send + Sync + 'static;

/// A logical Dolt chunk progress notification for one push attempt.
///
/// These counts describe reachable Dolt chunks and their payload bytes. They
/// are separate from Cloud Backed SQLite block uploads and exclude HTTP
/// framing and repository metadata.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DoltPushProgressEvent {
    /// The fixed set of destination-missing reachable chunks for this attempt.
    Plan {
        /// Number of logical chunks that need to be sent.
        missing_chunk_count: u64,
        /// Sum of payload bytes for the missing chunks.
        missing_payload_bytes: u64,
    },
    /// Cumulative chunks acknowledged by the destination in this attempt.
    Uploaded {
        /// Number of logical chunks acknowledged so far.
        acknowledged_chunk_count: u64,
        /// Payload bytes acknowledged so far.
        acknowledged_payload_bytes: u64,
    },
}

struct CallbackState {
    callback: Box<Callback>,
}

/// A scoped native `dolt_push` progress callback registration.
///
/// The callback is active only while this guard is alive and only for pushes
/// executed on the same connection. A second registration on that connection
/// fails rather than replacing the existing callback.
#[doc(hidden)]
#[must_use = "dropping the guard removes Dolt push progress notifications"]
pub struct DoltPushProgressGuard<'connection> {
    connection: &'connection Connection,
    context: *mut c_void,
    state: Option<Box<CallbackState>>,
}

impl Drop for DoltPushProgressGuard<'_> {
    fn drop(&mut self) {
        let db = unsafe { self.connection.handle() };
        let rc = unsafe { ffi::sqlite3_doltlite_clear_push_progress_callback(db, self.context) };
        if rc != ffi::SQLITE_OK {
            // Keep the callback state alive if native clearing unexpectedly
            // failed, so SQLite cannot call through a dangling context pointer.
            if let Some(state) = self.state.take() {
                let _ = Box::into_raw(state);
            }
        }
    }
}

impl Connection {
    /// Register progress notifications for native `dolt_push` calls on this connection.
    ///
    /// The callback runs synchronously during the SQL statement. It first gets
    /// one fixed plan for the attempt, then cumulative acknowledgements after
    /// successful destination writes. For HTTP remotes, acknowledgements are
    /// reported only after a complete chunk batch receives a successful
    /// response. A compare-and-swap retry emits a new plan and resets its
    /// counters. Callback panics are contained; keep the callback quick and do
    /// not execute SQL on this connection from it. Dropping the returned guard
    /// removes the registration. A nested registration on the same connection
    /// returns `SQLITE_MISUSE` without replacing the active callback. A
    /// successful no-op or branch deletion reports a zero-sized plan. Each
    /// plan is per native transfer/CAS attempt; a later retry reports a new
    /// plan and resets that attempt's acknowledgement totals.
    ///
    /// # Errors
    ///
    /// Returns the SQLite error if this connection already has a registered
    /// progress callback or native callback registration fails.
    #[doc(hidden)]
    pub fn dolt_push_progress_callback<F>(&self, callback: F) -> Result<DoltPushProgressGuard<'_>>
    where
        F: Fn(DoltPushProgressEvent) + Send + Sync + 'static,
    {
        let mut state = Box::new(CallbackState {
            callback: Box::new(callback),
        });
        let context = (&mut *state as *mut CallbackState).cast::<c_void>();
        let db = unsafe { self.handle() };
        let rc = unsafe {
            ffi::sqlite3_doltlite_set_push_progress_callback(db, context, Some(progress_trampoline))
        };
        if rc != ffi::SQLITE_OK {
            return Err(Error::SqliteFailure(ffi::Error::new(rc), None));
        }
        Ok(DoltPushProgressGuard {
            connection: self,
            context,
            state: Some(state),
        })
    }
}

unsafe extern "C" fn progress_trampoline(
    context: *mut c_void,
    event: std::ffi::c_int,
    count: ffi::sqlite3_int64,
    bytes: ffi::sqlite3_int64,
) {
    if context.is_null() || count < 0 || bytes < 0 {
        return;
    }
    let event = match event {
        ffi::DOLTLITE_PUSH_PROGRESS_PLAN => DoltPushProgressEvent::Plan {
            missing_chunk_count: count as u64,
            missing_payload_bytes: bytes as u64,
        },
        ffi::DOLTLITE_PUSH_PROGRESS_UPLOADED => DoltPushProgressEvent::Uploaded {
            acknowledged_chunk_count: count as u64,
            acknowledged_payload_bytes: bytes as u64,
        },
        _ => return,
    };
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let state = unsafe { &*(context.cast::<CallbackState>()) };
        (state.callback)(event);
    }));
}

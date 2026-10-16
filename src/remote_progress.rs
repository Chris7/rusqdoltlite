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

type CloneCallback = dyn Fn(DoltCloneProgressEvent) + Send + Sync + 'static;

struct CloneCallbackState {
    callback: Box<CloneCallback>,
}

/// Progress reported by an observed native `dolt_clone` operation.
///
/// Counts describe successfully transferred or destination-local Dolt chunks
/// and their payload bytes. They exclude HTTP framing and physical CBS blocks.
/// Events do not include a total or percentage; obtaining one would require an
/// extra source graph scan solely for reporting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DoltCloneProgressEvent {
    /// Clone observation started before refs are read.
    Started,
    /// Cumulative successfully verified chunks downloaded or reused locally.
    Progress {
        /// Number of chunks downloaded, hash-validated, and stored so far.
        downloaded_chunk_count: u64,
        /// Payload bytes downloaded, hash-validated, and stored so far.
        downloaded_payload_bytes: u64,
        /// Number of chunks already present in the destination and reused.
        reused_chunk_count: u64,
        /// Payload bytes represented by destination chunks reused.
        reused_payload_bytes: u64,
    },
    /// Chunk acquisition completed; clone metadata is being finalized.
    Finalizing,
    /// Refs and the cloned working tree were initialized successfully.
    Completed,
}

/// A scoped native `dolt_clone` progress callback registration.
///
/// The callback runs synchronously during SQL clone on the registered
/// connection. It reports cumulative verified downloads and destination-local
/// reuse without a preflight/source scan or source requests solely for
/// reporting. It may read destination chunks already present to discover their
/// children and fetch chunks needed to complete a partial destination.
/// Events do not include a total or percentage. A lazy clone does not acquire
/// the full graph and returns an error when observed. Callback
/// panics are contained; keep the callback quick and do not execute SQL on this
/// connection from it. Dropping this guard removes the registration. A nested
/// clone registration on one connection returns `SQLITE_MISUSE`; clone and
/// push registrations are independent.
#[must_use = "dropping the guard removes Dolt clone progress notifications"]
pub struct DoltCloneProgressGuard<'connection> {
    connection: &'connection Connection,
    context: *mut c_void,
    state: Option<Box<CloneCallbackState>>,
}

impl Drop for DoltCloneProgressGuard<'_> {
    fn drop(&mut self) {
        let db = unsafe { self.connection.handle() };
        let rc = unsafe { ffi::sqlite3_doltlite_clear_clone_progress_callback(db, self.context) };
        if rc != ffi::SQLITE_OK {
            // Keep callback state alive if native clearing unexpectedly fails,
            // so SQLite cannot call through a dangling context pointer.
            if let Some(state) = self.state.take() {
                let _ = Box::into_raw(state);
            }
        }
    }
}

impl Connection {
    /// Register progress notifications for native `dolt_clone` calls on this connection.
    ///
    /// Observation is opt-in and does not change the SQL `dolt_clone(...)`
    /// signature. Events start before refs are read, then report cumulative,
    /// successfully verified downloads and destination-local reuse. Observed
    /// clone does not add a preflight/source scan or source requests solely for
    /// reporting. It may read destination chunks already present to discover
    /// their children and fetch chunks needed to complete a partial destination.
    /// Events do not report a total or percentage.
    /// `Completed` means refs and the working tree were initialized
    /// successfully. Lazy clones reject observation because they do not acquire
    /// the full graph.
    ///
    /// Callback panics are contained. Do not run SQL on this connection from
    /// the callback. A second clone callback on the same connection returns
    /// `SQLITE_MISUSE`, while registrations on separate connections are
    /// independent. The callback is removed when the returned guard is dropped.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use rusqlite::{Connection, Result};
    ///
    /// fn clone_with_progress(connection: &Connection, remote_url: &str) -> Result<()> {
    ///     let _progress = connection.dolt_clone_progress_callback(|event| {
    ///         eprintln!("clone progress: {event:?}");
    ///     })?;
    ///     connection.query_row::<i64, _, _>(
    ///         "SELECT dolt_clone(?1)",
    ///         [remote_url],
    ///         |row| row.get(0),
    ///     )?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the SQLite error if this connection already has a clone
    /// progress callback or native callback registration fails.
    pub fn dolt_clone_progress_callback<F>(&self, callback: F) -> Result<DoltCloneProgressGuard<'_>>
    where
        F: Fn(DoltCloneProgressEvent) + Send + Sync + 'static,
    {
        let mut state = Box::new(CloneCallbackState {
            callback: Box::new(callback),
        });
        let context = (&mut *state as *mut CloneCallbackState).cast::<c_void>();
        let db = unsafe { self.handle() };
        let rc = unsafe {
            ffi::sqlite3_doltlite_set_clone_progress_callback(
                db,
                context,
                Some(clone_progress_trampoline),
            )
        };
        if rc != ffi::SQLITE_OK {
            return Err(Error::SqliteFailure(ffi::Error::new(rc), None));
        }
        Ok(DoltCloneProgressGuard {
            connection: self,
            context,
            state: Some(state),
        })
    }
}

unsafe extern "C" fn clone_progress_trampoline(
    context: *mut c_void,
    event: std::ffi::c_int,
    count: ffi::sqlite3_int64,
    bytes: ffi::sqlite3_int64,
    reused_count: ffi::sqlite3_int64,
    reused_bytes: ffi::sqlite3_int64,
) {
    if context.is_null() || count < 0 || bytes < 0 || reused_count < 0 || reused_bytes < 0 {
        return;
    }
    let event = match event {
        ffi::DOLTLITE_CLONE_PROGRESS_STARTED
            if count == 0 && bytes == 0 && reused_count == 0 && reused_bytes == 0 =>
        {
            DoltCloneProgressEvent::Started
        }
        ffi::DOLTLITE_CLONE_PROGRESS_PROGRESS => DoltCloneProgressEvent::Progress {
            downloaded_chunk_count: count as u64,
            downloaded_payload_bytes: bytes as u64,
            reused_chunk_count: reused_count as u64,
            reused_payload_bytes: reused_bytes as u64,
        },
        ffi::DOLTLITE_CLONE_PROGRESS_FINALIZING
            if count == 0 && bytes == 0 && reused_count == 0 && reused_bytes == 0 =>
        {
            DoltCloneProgressEvent::Finalizing
        }
        ffi::DOLTLITE_CLONE_PROGRESS_COMPLETED
            if count == 0 && bytes == 0 && reused_count == 0 && reused_bytes == 0 =>
        {
            DoltCloneProgressEvent::Completed
        }
        _ => return,
    };
    let _ = catch_unwind(AssertUnwindSafe(|| {
        let state = unsafe { &*(context.cast::<CloneCallbackState>()) };
        (state.callback)(event);
    }));
}

//! Portable hashing entry points used by the Emscripten CBS dispatcher.
//!
//! CBS uses MD5 for its existing block-name format and SHA-256 for session
//! record integrity. These wrappers keep those bytes identical to the native
//! OpenSSL implementation without linking OpenSSL into the browser build.

use core::ffi::{c_int, c_uchar};
use core::mem::{align_of, size_of, MaybeUninit};
use core::ptr;
use core::slice;

use md5::{Digest, Md5};
use sha2::Sha256;

const SHA256_CONTEXT_WORDS: usize = 32;
const SHA256_CONTEXT_BYTES: usize = SHA256_CONTEXT_WORDS * size_of::<u64>();

/// Opaque storage shared with the C CBS hash adapter.
#[repr(C, align(8))]
pub struct BcvBrowserSha256Context {
    _opaque: [u64; SHA256_CONTEXT_WORDS],
}

const _: () = assert!(size_of::<Sha256>() <= SHA256_CONTEXT_BYTES);
const _: () = assert!(align_of::<Sha256>() <= align_of::<BcvBrowserSha256Context>());

/// Initialize the SHA-256 state supplied by CBS.
///
/// # Safety
/// The context must point to live, writable storage aligned for
/// BcvBrowserSha256Context.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bcv_browser_sha256_init(context: *mut BcvBrowserSha256Context) -> c_int {
    if context.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees a writable, aligned context buffer.
    unsafe {
        context
            .cast::<MaybeUninit<Sha256>>()
            .write(MaybeUninit::new(Sha256::new()));
    }
    1
}

/// Add bytes to the SHA-256 state supplied by CBS.
///
/// # Safety
/// The context must point to an initialized SHA-256 context created by
/// bcv_browser_sha256_init and remain exclusively borrowed for this call.
/// If length is nonzero, data must point to length readable bytes that do not
/// overlap context.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bcv_browser_sha256_update(
    context: *mut BcvBrowserSha256Context,
    data: *const c_uchar,
    length: usize,
) -> c_int {
    if context.is_null() || (length > 0 && data.is_null()) || length > isize::MAX as usize {
        return 0;
    }
    let bytes = if length == 0 {
        &[]
    } else {
        // SAFETY: The caller guarantees the readable input region and the
        // length guard satisfies slice::from_raw_parts' isize bound.
        unsafe { slice::from_raw_parts(data, length) }
    };
    // SAFETY: The caller guarantees an initialized, exclusively borrowed state.
    unsafe { &mut *context.cast::<Sha256>() }.update(bytes);
    1
}

/// Finish SHA-256 into the output buffer and clear its temporary state.
///
/// # Safety
/// The context must point to an initialized SHA-256 context created by
/// bcv_browser_sha256_init. Output must point to 32 writable bytes and must
/// not overlap context.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bcv_browser_sha256_final(
    output: *mut c_uchar,
    context: *mut BcvBrowserSha256Context,
) -> c_int {
    if context.is_null() || output.is_null() {
        return 0;
    }
    // SAFETY: The caller guarantees the initialized context and output range.
    let state = unsafe { context.cast::<Sha256>().read() };
    let digest = state.finalize();
    // SAFETY: The caller guarantees a 32-byte output region disjoint from the
    // context. The context was consumed by read and is zeroed before return.
    unsafe {
        ptr::copy_nonoverlapping(digest.as_ptr(), output, digest.len());
        ptr::write_bytes(context, 0, 1);
    }
    1
}

/// Compute one SHA-256 digest for a CBS session record.
///
/// # Safety
/// If length is nonzero, data must point to length readable bytes. Output must
/// point to 32 writable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bcv_browser_sha256(
    data: *const c_uchar,
    length: usize,
    output: *mut c_uchar,
) -> *mut c_uchar {
    if output.is_null() || (length > 0 && data.is_null()) || length > isize::MAX as usize {
        return ptr::null_mut();
    }
    let bytes = if length == 0 {
        &[]
    } else {
        // SAFETY: The caller guarantees the readable input region and the
        // length guard satisfies slice::from_raw_parts' isize bound.
        unsafe { slice::from_raw_parts(data, length) }
    };
    let digest = Sha256::digest(bytes);
    // SAFETY: The caller guarantees a writable 32-byte output region. Digest
    // computation has completed before the output write, so overlap is safe.
    unsafe { ptr::copy_nonoverlapping(digest.as_ptr(), output, digest.len()) };
    output
}

/// Compute the existing CBS MD5 block-name digest.
///
/// # Safety
/// If length is nonzero, data must point to length readable bytes. Output must
/// point to 16 writable bytes. The input may be null only when length is zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bcv_browser_md5(
    data: *const c_uchar,
    length: usize,
    output: *mut c_uchar,
) -> *mut c_uchar {
    if output.is_null() || (length > 0 && data.is_null()) || length > isize::MAX as usize {
        return ptr::null_mut();
    }
    let bytes = if length == 0 {
        &[]
    } else {
        // SAFETY: The caller guarantees the readable input region and the
        // length guard satisfies slice::from_raw_parts' isize bound.
        unsafe { slice::from_raw_parts(data, length) }
    };
    let digest = Md5::digest(bytes);
    // SAFETY: The caller guarantees a writable 16-byte output region. Digest
    // computation has completed before the output write, so overlap is safe.
    unsafe { ptr::copy_nonoverlapping(digest.as_ptr(), output, digest.len()) };
    output
}

#[cfg(test)]
mod tests {
    use super::{
        bcv_browser_md5, bcv_browser_sha256_final, bcv_browser_sha256_init,
        bcv_browser_sha256_update, BcvBrowserSha256Context,
    };
    use core::mem::MaybeUninit;

    #[test]
    fn md5_matches_existing_cbs_block_hash_bytes() {
        let input = b"abc";
        let mut digest = [0_u8; 16];
        // SAFETY: input is readable and digest has the required size.
        let result = unsafe { bcv_browser_md5(input.as_ptr(), input.len(), digest.as_mut_ptr()) };
        assert_eq!(result, digest.as_mut_ptr());
        assert_eq!(
            digest,
            [
                0x90, 0x01, 0x50, 0x98, 0x3c, 0xd2, 0x4f, 0xb0, 0xd6, 0x96, 0x3f, 0x7d, 0x28, 0xe1,
                0x7f, 0x72,
            ]
        );
    }

    #[test]
    fn sha256_accepts_split_session_record_updates() {
        let mut uninitialized = MaybeUninit::<BcvBrowserSha256Context>::uninit();
        let context = uninitialized.as_mut_ptr();
        // SAFETY: The pointer addresses writable storage for one opaque context.
        assert_eq!(unsafe { bcv_browser_sha256_init(context) }, 1);

        for part in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
            // SAFETY: The context was initialized and each input slice is valid.
            assert_eq!(
                unsafe { bcv_browser_sha256_update(context, part.as_ptr(), part.len()) },
                1
            );
        }

        let mut digest = [0_u8; 32];
        // SAFETY: The initialized context is consumed and digest has 32 bytes.
        assert_eq!(
            unsafe { bcv_browser_sha256_final(digest.as_mut_ptr(), context) },
            1
        );
        assert_eq!(
            digest,
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]
        );
    }
}

#[cfg(all(test, target_os = "emscripten", feature = "browser-cloud-tests"))]
mod browser_dispatch_tests {
    use core::ffi::c_int;

    unsafe extern "C" {
        fn bcv_browser_test_auth_replay(
            first_status: c_int,
            second_status: c_int,
            fail_refresh: c_int,
            auth_calls: *mut c_int,
            auth_reason: *mut c_int,
            request_preserved: *mut c_int,
            skip_request_refresh: *mut c_int,
            second_retry: *mut c_int,
            result_code: *mut c_int,
            cause_kind: *mut c_int,
        ) -> c_int;
        fn bcv_browser_test_retry_attempt_count(http_status: c_int) -> c_int;
    }

    #[test]
    fn unauthorized_refresh_replays_body_and_conditional_headers_once() {
        for status in [401, 403] {
            let mut auth_calls = 0;
            let mut auth_reason = -1;
            let mut request_preserved = 0;
            let mut skip_request_refresh = 0;
            let mut second_retry = -1;
            let mut result_code = -1;
            let mut cause_kind = -1;
            // SAFETY: Each output pointer addresses a live c_int for the
            // duration of the synchronous C test harness call.
            let first_retry = unsafe {
                bcv_browser_test_auth_replay(
                    status,
                    status,
                    0,
                    &mut auth_calls,
                    &mut auth_reason,
                    &mut request_preserved,
                    &mut skip_request_refresh,
                    &mut second_retry,
                    &mut result_code,
                    &mut cause_kind,
                )
            };
            assert_eq!(first_retry, 1);
            assert_eq!(auth_calls, 1);
            assert_eq!(auth_reason, 1, "refresh reason must be unauthorized");
            assert_eq!(request_preserved, 1);
            assert_eq!(skip_request_refresh, 1);
            assert_eq!(second_retry, 0, "a second denial must be terminal");
            assert_eq!(result_code, 0);
        }
    }

    #[test]
    fn failed_unauthorized_refresh_is_a_typed_auth_error() {
        let mut auth_calls = 0;
        let mut auth_reason = -1;
        let mut request_preserved = 0;
        let mut skip_request_refresh = 0;
        let mut second_retry = -1;
        let mut result_code = -1;
        let mut cause_kind = -1;
        // SAFETY: Each output pointer addresses a live c_int for the
        // duration of the synchronous C test harness call.
        let first_retry = unsafe {
            bcv_browser_test_auth_replay(
                401,
                401,
                1,
                &mut auth_calls,
                &mut auth_reason,
                &mut request_preserved,
                &mut skip_request_refresh,
                &mut second_retry,
                &mut result_code,
                &mut cause_kind,
            )
        };
        assert_eq!(first_retry, 0);
        assert_eq!(auth_calls, 1);
        assert_eq!(auth_reason, 1);
        assert_eq!(result_code, crate::SQLITE_IOERR_AUTH);
        assert_eq!(cause_kind, 0, "auth callback failures are SQLite errors");
    }

    #[test]
    fn retryable_http_failure_has_two_total_attempts() {
        // SAFETY: The helper has no pointer arguments and keeps its request
        // state local to the call.
        assert_eq!(unsafe { bcv_browser_test_retry_attempt_count(503) }, 2);
    }
}

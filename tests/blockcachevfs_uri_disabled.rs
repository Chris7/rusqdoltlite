#![cfg(not(feature = "blockcachevfs"))]

use rusqlite::{Connection, Error};

#[test]
fn cloud_uris_are_rejected_when_the_feature_is_disabled() {
    let secret = "do-not-leak-this-token";
    for uri in [
        format!(
            "gcs://test-bucket/repository?vfs=blockcachevfs&project=test&access_token={secret}"
        ),
        format!(
            "s3://test-bucket/repository?vfs=blockcachevfs&region=us-east-1&access_id=test&secret_access_key={secret}"
        ),
    ] {
        let error = Connection::open(uri).expect_err("disabled CBS support must reject the URI");
        assert!(matches!(
            error,
            Error::SqliteFailure(code, _) if code.code == rusqlite::ffi::ErrorCode::ApiMisuse
        ));
        assert!(!error.to_string().contains(secret));
    }
}

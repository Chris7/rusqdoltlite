#!/bin/sh -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
echo "$SCRIPT_DIR"
cd "$SCRIPT_DIR" || { echo "fatal error" >&2; exit 1; }
cargo clean -p libdoltlite-sys
TARGET_DIR="$SCRIPT_DIR/../target"
export DOLTLITE_LIB_DIR="$SCRIPT_DIR/doltlite"
export DOLTLITE_INCLUDE_DIR="$SCRIPT_DIR/doltlite"
mkdir -p "$TARGET_DIR" "$DOLTLITE_LIB_DIR"

# Set this to master for the latest upstream source, or to a tag such as
# v0.11.35 for a reproducible Git-based upgrade.
DOLTLITE_GIT_REF=master

# Build pristine upstream artifacts from the configured Git ref. RusqDoltLite
# behavior is maintained separately in patches/ and applied only to Cargo's
# OUT_DIR by the build script. Never apply those patches to doltlite/doltlite.c
# here.
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/rusqdoltlite-doltlite.XXXXXX")
cleanup() {
  if [ -n "${WORK_DIR:-}" ] && [ -d "$WORK_DIR" ]; then
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup 0 1 2 15

SOURCE_DIR="$WORK_DIR/doltlite"
BUILD_DIR="$SOURCE_DIR/build"
git clone --depth 1 --branch "$DOLTLITE_GIT_REF" \
  https://github.com/dolthub/doltlite.git "$SOURCE_DIR"
DOLTLITE_REVISION=$(git -C "$SOURCE_DIR" rev-parse HEAD)
printf 'Building DoltLite ref %s at %s\n' \
  "$DOLTLITE_GIT_REF" "$DOLTLITE_REVISION"

mkdir -p "$BUILD_DIR"
(
  cd "$BUILD_DIR" || exit 1
  ../configure
  make sqlite3.c sqlite3.h sqlite3ext.h
)

cp "$BUILD_DIR/sqlite3.c" "$DOLTLITE_LIB_DIR/doltlite.c"
cp "$BUILD_DIR/sqlite3.h" "$DOLTLITE_LIB_DIR/doltlite.h"
sed 's/#include "sqlite3.h"/#include "doltlite.h"/' \
  "$BUILD_DIR/sqlite3ext.h" > "$DOLTLITE_LIB_DIR/doltliteext.h"

# The single-file amalgamation deliberately omits the credential and TLS
# implementations used by the HTTP remote client and in-process server. Keep
# the matching upstream sidecars next to it so the `remote` feature can compile
# the complete native library without modifying generated doltlite.c.
rm -rf "$DOLTLITE_LIB_DIR/remote"
mkdir -p "$DOLTLITE_LIB_DIR/remote/ed25519" \
  "$DOLTLITE_LIB_DIR/remote/mbedtls"
cp "$SOURCE_DIR/src/doltlite_creds.c" \
  "$SOURCE_DIR/src/doltlite_creds.h" \
  "$SOURCE_DIR/src/doltlite_tls.c" \
  "$SOURCE_DIR/src/doltlite_tls.h" \
  "$SOURCE_DIR/src/doltlite_net.h" \
  "$SOURCE_DIR/src/doltlite_remotesrv.h" \
  "$DOLTLITE_LIB_DIR/remote/"
cp -R "$SOURCE_DIR/ext/ed25519/." "$DOLTLITE_LIB_DIR/remote/ed25519/"
cp "$SOURCE_DIR/ext/mbedtls/LICENSE" "$DOLTLITE_LIB_DIR/remote/mbedtls/"
cp -R "$SOURCE_DIR/ext/mbedtls/include" \
  "$SOURCE_DIR/ext/mbedtls/library" \
  "$DOLTLITE_LIB_DIR/remote/mbedtls/"

# Fail early if upstream master has moved beyond the local patch context. The
# bundled Cargo builds below apply the same patch series to an OUT_DIR copy.
PATCH_CHECK_DIR="$WORK_DIR/patch-check"
mkdir -p "$PATCH_CHECK_DIR"
cp "$DOLTLITE_LIB_DIR/doltlite.c" "$PATCH_CHECK_DIR/doltlite.c"
(
  cd "$PATCH_CHECK_DIR" || exit 1
  git apply --check "$SCRIPT_DIR"/patches/*.patch
)

# Regenerate bindgen file for doltlite.h
rm -f "$DOLTLITE_LIB_DIR/bindgen_bundled_version.rs"
cargo update --quiet
# Just to make sure there is only one bindgen.rs file in target dir
find "$TARGET_DIR" -type f -name bindgen.rs -exec rm {} \;
env LIBDOLTLITE_SYS_BUNDLING=1 cargo build --features "buildtime_bindgen session" --no-default-features
find "$TARGET_DIR" -type f -name bindgen.rs -exec mv {} "$DOLTLITE_LIB_DIR/bindgen_bundled_version.rs" \;

# Regenerate bindgen file for doltliteext.h
# some sqlite3_api_routines fields are function pointers with va_list arg but currently stable Rust doesn't support this type.
# FIXME how to generate portable bindings without :
sed -i.bk -e 's/va_list/void*/' "$DOLTLITE_LIB_DIR/doltliteext.h"
cp "$DOLTLITE_LIB_DIR/doltliteext.h" "$DOLTLITE_LIB_DIR/sqlite3ext.h"
rm -f "$DOLTLITE_LIB_DIR/bindgen_bundled_version_ext.rs"
find "$TARGET_DIR" -type f -name bindgen.rs -exec rm {} \;
env LIBDOLTLITE_SYS_BUNDLING=1 cargo build --features "buildtime_bindgen loadable_extension" --no-default-features
find "$TARGET_DIR" -type f -name bindgen.rs -exec mv {} "$DOLTLITE_LIB_DIR/bindgen_bundled_version_ext.rs" \;
mv "$DOLTLITE_LIB_DIR/doltliteext.h.bk" "$DOLTLITE_LIB_DIR/doltliteext.h"
rm -f "$DOLTLITE_LIB_DIR/sqlite3ext.h"

# Sanity checks
cd "$SCRIPT_DIR/.." || { echo "fatal error" >&2; exit 1; }
cargo update --quiet
cargo test --features "backup blob chrono functions limits load_extension serde_json trace vtab bundled"
printf '    \e[35;1mFinished\e[0m bundled DoltLite tests\n'
cargo test --features "bundled remote" --test remote_server
printf '    \e[35;1mFinished\e[0m bundled DoltLite remote-server tests\n'

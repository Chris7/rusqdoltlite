#!/bin/sh -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
echo "$SCRIPT_DIR"
cd "$SCRIPT_DIR" || { echo "fatal error" >&2; exit 1; }
cargo clean -p libdoltlite-sys
TARGET_DIR="$SCRIPT_DIR/../target"
export DOLTLITE_LIB_DIR="$SCRIPT_DIR/doltlite"
export DOLTLITE_INCLUDE_DIR="$SCRIPT_DIR/doltlite"
mkdir -p "$TARGET_DIR" "$DOLTLITE_LIB_DIR"

# Download and extract pristine upstream artifacts. RusqDoltLite behavior is
# maintained separately in patches/ and applied only to Cargo's OUT_DIR by the
# build script. Never apply those patches to doltlite/doltlite.c here.
DOLTLITE_VERSION=0.11.33
DOLTLITE=doltlite-amalgamation-$DOLTLITE_VERSION
curl -LO "https://github.com/dolthub/doltlite/releases/download/v$DOLTLITE_VERSION/$DOLTLITE.zip"
unzip -p "$DOLTLITE.zip" "$DOLTLITE/doltlite.c" > "$DOLTLITE_LIB_DIR/doltlite.c"
unzip -p "$DOLTLITE.zip" "$DOLTLITE/doltlite.h" > "$DOLTLITE_LIB_DIR/doltlite.h"
unzip -p "$DOLTLITE.zip" "$DOLTLITE/doltliteext.h" > "$DOLTLITE_LIB_DIR/doltliteext.h"
rm -f "$DOLTLITE.zip"

# The single-file amalgamation deliberately omits the credential and TLS
# implementations used by the HTTP remote client and in-process server. Keep
# the matching upstream sidecars next to it so the `remote` feature can compile
# the complete native library without modifying generated doltlite.c.
SOURCE_ARCHIVE=doltlite-source-$DOLTLITE_VERSION.zip
SOURCE_DIR=doltlite-$DOLTLITE_VERSION
curl -L -o "$SOURCE_ARCHIVE" \
  "https://github.com/dolthub/doltlite/archive/refs/tags/v$DOLTLITE_VERSION.zip"
rm -rf "$DOLTLITE_LIB_DIR/remote" "$SOURCE_DIR"
unzip -q "$SOURCE_ARCHIVE"
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
rm -rf "$SOURCE_ARCHIVE" "$SOURCE_DIR"

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

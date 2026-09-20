#!/bin/sh
set -eu

# CBS is not vendored: its upstream checkout has empty COPYING/README files.
CHECKIN=50e099ad7bf1d12d747f59b0af973d12809887480463fc9893846b0d6ee22e94
SHA256=b322811e8ec4224753f2d9309ed0f011d81c9f2540ce81bd7b5a6a9550d7d03a
URL="https://sqlite.org/cloudsqlite/tarball/${CHECKIN}/cloudsqlite.tar.gz"
mode=source
case "${1:-}" in
    --checkout)
        mode=full
        shift
        ;;
    --*)
        echo "usage: $0 [--checkout DEST] [DEST]" >&2
        exit 2
        ;;
esac
if [ "$#" -gt 1 ]; then
    echo "usage: $0 [--checkout DEST] [DEST]" >&2
    exit 2
fi
DEST=${1:-"${PWD}/cloudsqlite-${CHECKIN}"}
ARCHIVE=$(mktemp "${TMPDIR:-/tmp}/cloudsqlite-${CHECKIN}.XXXXXX.tar.gz")
trap 'rm -f "$ARCHIVE"' EXIT HUP INT TERM

if command -v curl >/dev/null 2>&1; then
    curl --fail --location --silent --show-error "$URL" --output "$ARCHIVE"
elif command -v wget >/dev/null 2>&1; then
    wget --quiet --output-document="$ARCHIVE" "$URL"
else
    echo "fetch_blockcachevfs.sh requires curl or wget" >&2
    exit 1
fi

actual=$(sha256sum "$ARCHIVE" | awk '{print $1}')
if [ "$actual" != "$SHA256" ]; then
    echo "CBS archive checksum mismatch: expected $SHA256, got $actual" >&2
    exit 1
fi

mkdir -p "$DEST"
if [ "$(find "$DEST" -mindepth 1 -print -quit)" ]; then
    echo "refusing to extract into non-empty destination: $DEST" >&2
    exit 1
fi
if [ "$mode" = full ]; then
    # The full archive is used by the shared CBS Tcl runner.  Keep the
    # upstream checkout layout (configure, src/, test/, and support files).
    tar -xzf "$ARCHIVE" --strip-components=1 -C "$DEST"
else
    tar -xzf "$ARCHIVE" --strip-components=2 -C "$DEST" \
        "cloudsqlite/src/blockcachevfs.c" \
        "cloudsqlite/src/simplexml.c" \
        "cloudsqlite/src/bcvutil.c" \
        "cloudsqlite/src/bcvmodule.c" \
        "cloudsqlite/src/bcvlog.c" \
        "cloudsqlite/src/bcvencrypt.c" \
        "cloudsqlite/src/blockcachevfs.h" \
        "cloudsqlite/src/bcv_int.h" \
        "cloudsqlite/src/bcvutil.h" \
        "cloudsqlite/src/bcvmodule.h" \
        "cloudsqlite/src/bcvencrypt.h" \
        "cloudsqlite/src/simplexml.h"
fi

echo "$DEST"

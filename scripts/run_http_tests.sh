#!/usr/bin/env bash
# Run the VGI integration suite against the Rust worker over the HTTP transport.
#
# Starts one `vgi-example-worker --http` server per catalog (example, versioned,
# versioned_tables, attach_options, bearer), reads each announced PORT, and
# exports the matching VGI_*_HTTP_WORKER base URLs before running unittest.
#
#   scripts/run_http_tests.sh                 # full in-scope suite over HTTP
#   scripts/run_http_tests.sh http            # one category
#   scripts/run_http_tests.sh "test/sql/integration/http/gzip_fallback.test"
#   VGI_HTTP_DISABLE_ZSTD=1 scripts/run_http_tests.sh http/gzip_fallback.test
#
# Caches under /tmp/vgi-rust-http-cache/.

set -uo pipefail

VGI_RUST="/Users/rusty/Development/vgi-rust"
VGI_EXT="/Users/rusty/Development/vgi"
UNITTEST="$VGI_EXT/build/release/test/unittest"
BIN="$VGI_RUST/target/release/vgi-example-worker"
CACHE="/tmp/vgi-rust-http-cache"
mkdir -p "$CACHE"

BUILD=1
if [[ "${1:-}" == "--no-build" ]]; then BUILD=0; shift; fi
if [[ $BUILD == 1 ]]; then
  echo "[http-harness] building release worker..."
  ( cd "$VGI_RUST" && cargo build --release 2>&1 ) | tail -3
  [[ -x "$BIN" ]] || { echo "build failed"; exit 1; }
fi

PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

# start_worker <logfile> <env=val>...  -> echoes the announced base URL
start_worker() {
  local log="$1"; shift
  : > "$log"
  ( for kv in "$@"; do export "$kv"; done; exec "$BIN" --http ) > "$log" 2>>"$CACHE/worker.log" &
  PIDS+=("$!")
  local port=""
  for _ in $(seq 1 40); do
    port=$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" 2>/dev/null | head -1)
    [[ -n "$port" ]] && break
    sleep 0.25
  done
  [[ -n "$port" ]] || { echo "[http-harness] worker failed to announce PORT ($log)" >&2; cat "$log" >&2; exit 1; }
  echo "http://localhost:$port"
}

: > "$CACHE/worker.log"
ZSTD_ENV="VGI_HTTP_DISABLE_ZSTD=${VGI_HTTP_DISABLE_ZSTD:-}"

W_EXAMPLE=$(start_worker "$CACHE/example.log" "VGI_WORKER_CATALOG_NAME=example" "$ZSTD_ENV" "VGI_BEARER_TOKENS=test-secret-token=test-user")
W_VERSIONED=$(start_worker "$CACHE/versioned.log" "VGI_WORKER_CATALOG_NAME=versioned" "$ZSTD_ENV")
W_VERSIONED_TABLES=$(start_worker "$CACHE/versioned_tables.log" "VGI_WORKER_CATALOG_NAME=versioned_tables" "$ZSTD_ENV")
W_ATTACH_OPTIONS=$(start_worker "$CACHE/attach_options.log" "VGI_WORKER_CATALOG_NAME=attach_options" "$ZSTD_ENV")

echo "[http-harness] example=$W_EXAMPLE versioned=$W_VERSIONED vtables=$W_VERSIONED_TABLES attach_options=$W_ATTACH_OPTIONS"

ARGS=()
if [[ $# -ge 1 ]]; then
  case "$1" in
    test/*) ARGS=("$1");;
    *)      ARGS=("test/sql/integration/$1/*");;
  esac
else
  ARGS=("test/sql/integration/*"
        "~test/sql/integration/writable/*"
        "~test/sql/integration/simple_writable/*"
        "~test/sql/integration/table_in_out/echo/nested_type_combinations.test")
fi

echo "[http-harness] running: ${ARGS[*]}"
env \
  VGI_TEST_WORKER="$W_EXAMPLE" \
  VGI_VERSIONED_HTTP_WORKER="$W_VERSIONED" \
  VGI_VERSIONED_TABLES_HTTP_WORKER="$W_VERSIONED_TABLES" \
  VGI_ATTACH_OPTIONS_HTTP_WORKER="$W_ATTACH_OPTIONS" \
  VGI_TEST_BEARER_TOKEN="test-secret-token" \
  VGI_HTTP_DISABLE_ZSTD="${VGI_HTTP_DISABLE_ZSTD:-}" \
  VGI_WORKER_SUPPORTS_DYNAMIC_CODE=1 \
  "$UNITTEST" "${ARGS[@]}" > "$CACHE/run.log" 2>&1
RC=$?

awk '/unexpectedly|FAILED:|Mismatch on/{print}' "$CACHE/run.log" \
  | grep -oE 'test/sql/integration/[A-Za-z0-9_/]+\.test(_slow)?' | sort -u > "$CACHE/failures" 2>/dev/null
echo "===== TAIL ====="; tail -6 "$CACHE/run.log"
echo "===== HTTP FAILURES ($(wc -l < "$CACHE/failures" | tr -d ' ')) ====="
cat "$CACHE/failures" 2>/dev/null
echo "(log: $CACHE/run.log  worker stderr: $CACHE/worker.log)"
exit $RC

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

# Scratch dir the native-branch fixtures and their .test COPY-TO targets must
# agree on; exported to both the workers and unittest.
BRANCH_DIR="${VGI_TEST_BRANCH_DIR:-$CACHE/branches}"
mkdir -p "$BRANCH_DIR"
export VGI_TEST_BRANCH_DIR="$BRANCH_DIR"

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
  # Run the worker with its cwd set to $VGI_EXT — the directory unittest runs
  # from — so DuckDB's per-test temp dir (__TEST_DIR__ → duckdb_unittest_tempdir/
  # <pid>) and the worker resolve the SAME relative path. Without this the http
  # worker (a separate process) cannot create the COPY ... TO destination the
  # test hands it as a relative path, and every copy_to/copy_from test fails.
  ( cd "$VGI_EXT" || exit 1; for kv in "$@"; do export "$kv"; done; exec "$BIN" --http ) > "$log" 2>>"$CACHE/worker.log" &
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

# Main example server: NO required bearer tokens → anonymous allowed (serves the
# non-auth suite). It does accept two OPTIONAL tokens so the result cache's
# identity-isolation test can attach the same worker as alice and as bob; an
# absent/unknown token still resolves to anonymous, so no other test 401s.
# The bearer_auth tests run separately against W_BEARER below.
W_EXAMPLE=$(start_worker "$CACHE/example.log" "VGI_WORKER_CATALOG_NAME=example" "$ZSTD_ENV" \
  "VGI_OPTIONAL_BEARER_TOKENS=vgi-test-alice=alice,vgi-test-bob=bob")
# Bearer-protected example server (rejects anonymous) for bearer_auth/*.
W_BEARER=$(start_worker "$CACHE/bearer.log" "VGI_WORKER_CATALOG_NAME=example" "$ZSTD_ENV" "VGI_BEARER_TOKENS=test-secret-token=test-principal")
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
  # bearer_auth runs separately (needs the protected server); exclude here.
  ARGS=("test/sql/integration/*"
        "~test/sql/integration/writable/*"
        "~test/sql/integration/simple_writable/*"
        "~test/sql/integration/bearer_auth/*"
        "~test/sql/integration/table_in_out/echo/nested_type_combinations.test")
fi

echo "[http-harness] running: ${ARGS[*]}"
env \
  VGI_TEST_BRANCH_DIR="$BRANCH_DIR" \
  VGI_HTTP_TRANSPORT=1 \
  VGI_TEST_WORKER="$W_EXAMPLE" \
  VGI_VERSIONED_HTTP_WORKER="$W_VERSIONED" \
  VGI_VERSIONED_TABLES_HTTP_WORKER="$W_VERSIONED_TABLES" \
  VGI_ATTACH_OPTIONS_HTTP_WORKER="$W_ATTACH_OPTIONS" \
  VGI_TEST_BEARER_TOKEN="test-secret-token" \
  VGI_HTTP_DISABLE_ZSTD="${VGI_HTTP_DISABLE_ZSTD:-}" \
  "$UNITTEST" "${ARGS[@]}" > "$CACHE/run.log" 2>&1
RC=$?

# bearer_auth/* against the protected server (only in a full run).
if [[ $# -eq 0 ]]; then
  env \
    VGI_TEST_WORKER="$W_BEARER" \
    VGI_TEST_BEARER_TOKEN="test-secret-token" \
    "$UNITTEST" "test/sql/integration/bearer_auth/*" >> "$CACHE/run.log" 2>&1
  RC=$(( RC | $? ))
fi

awk '/unexpectedly|FAILED:|Mismatch on/{print}' "$CACHE/run.log" \
  | grep -oE 'test/sql/integration/[A-Za-z0-9_/]+\.test(_slow)?' | sort -u > "$CACHE/failures" 2>/dev/null
echo "===== TAIL ====="; tail -6 "$CACHE/run.log"
echo "===== HTTP FAILURES ($(wc -l < "$CACHE/failures" | tr -d ' ')) ====="
cat "$CACHE/failures" 2>/dev/null
echo "(log: $CACHE/run.log  worker stderr: $CACHE/worker.log)"
exit $RC

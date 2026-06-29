#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run the canonical Query-farm/vgi integration sqllogictest suite against the
# Rust example worker, using a prebuilt standalone `haybarn-unittest` and the
# signed community vgi extension — no C++ build from source. See ci/README.md.
#
# The single `vgi-example-worker` binary is routed into each catalog by the
# ci/wrappers/* scripts (which set VGI_WORKER_CATALOG_NAME); on Windows, which
# has no AF_UNIX and cannot exec a shell wrapper as a worker LOCATION, only the
# main `example` catalog is exercised (the secondary-catalog tests self-skip via
# require-env).
#
# Required environment:
#   VGI_SRC          path to a Query-farm/vgi checkout (contains test/sql/integration)
#   HAYBARN_UNITTEST path to the haybarn-unittest binary
#   VGI_WORKER_BIN   path to the built vgi-example-worker
# Optional:
#   TRANSPORT        stdio | launch | http   (default stdio)
#   STAGE            scratch dir for the preprocessed test tree (default: mktemp)
set -uo pipefail  # not -e: the suite exit code is managed explicitly (`|| rc=$?`)

: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"
: "${VGI_WORKER_BIN:?path to the built vgi-example-worker}"

HERE="$(cd "$(dirname "$0")" && pwd)"
STAGE="${STAGE:-$(mktemp -d)}"
TRANSPORT="${TRANSPORT:-stdio}"
INTEGRATION="$VGI_SRC/test/sql/integration"
[ -d "$INTEGRATION" ] || { echo "::error::no test/sql/integration under VGI_SRC=$VGI_SRC"; exit 1; }

# Windows (Git Bash) has no AF_UNIX, and the prebuilt runner cannot exec a shell
# catalog wrapper as a subprocess LOCATION, so it runs the main worker only.
WINDOWS=0
case "$(uname -s)" in MINGW* | MSYS* | CYGWIN*) WINDOWS=1 ;; esac

if [ "$TRANSPORT" = "launch" ] && [ "$WINDOWS" = "1" ]; then
  echo "launch transport is unsupported on Windows (no AF_UNIX) — nothing to run."
  exit 0
fi

# ---------------------------------------------------------------------------
# Stage a preprocessed copy of the suite. preprocess-require.awk rewrites each
# `require <ext>` gate into a signed INSTALL+LOAD so the standalone runner can
# run it; on the http lane it also injects `LOAD httpfs` before each ATTACH.
# Out of scope: writable/ + simple_writable/ (write path, deferred read-only
# port); nested_type_combinations.test (segfaults the prebuilt runner);
# expression_filter.test (its EXPLAIN assertion renders the spatial predicate's
# WKT differently under the prebuilt binary's DuckDB/spatial build than the
# locally-built unittest the worker is developed against — a plan-text rendering
# difference, not a worker behaviour difference; covered by the local suite);
# bool_in_union.test (a pre-existing, arch-dependent union-bool bug — its pinned
# expected output matches arm64 but not amd64; dropped on all platforms).
# The http lane drops two files the prebuilt binary can't serve; Windows drops
# the fixtures that read parquet/csv from POSIX /tmp paths.
# ---------------------------------------------------------------------------
AWK_HTTP=0
HTTP_SKIP=()
if [ "$TRANSPORT" = "http" ]; then
  AWK_HTTP=1
  HTTP_SKIP=(-not -name 'projection_pushdown_repro.test' -not -name 'dynamic_filter.test')
fi
WIN_SKIP=()
if [ "$WINDOWS" = "1" ]; then
  # These fixtures stage and read parquet/csv from POSIX `/tmp/...` paths the
  # worker's catalog hard-codes, which don't exist on Windows.
  WIN_SKIP=(-not -name 'multi_branch_heterogeneous.test'
            -not -name 'multi_branch_pushdown_incapable.test'
            -not -name 'multi_branch_reconciliation.test'
            -not -name 'required_field_filter_paths_native.test')
fi

echo "Staging preprocessed tests into $STAGE (transport=$TRANSPORT, windows=$WINDOWS) ..."
mkdir -p "$STAGE/test/sql/integration"
( cd "$INTEGRATION"
  find . -name '*.test' \
       -not -path '*/writable/*' -not -path '*/simple_writable/*' \
       -not -name 'nested_type_combinations.test' \
       -not -name 'expression_filter.test' \
       -not -name 'bool_in_union.test' \
       "${HTTP_SKIP[@]}" "${WIN_SKIP[@]}" | while read -r f; do
    mkdir -p "$STAGE/test/sql/integration/$(dirname "$f")"
    awk -v http="$AWK_HTTP" -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/integration/$f"
  done )

# Background worker processes (http servers) tracked here and killed on exit.
BG_PIDS=()
cleanup() { for p in "${BG_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

# boot_http_worker <executable> [env=val ...] — start it as an HTTP server on an
# ephemeral port; sets the global BOOTED_PORT to the port it announces
# (`PORT:<n>`, the worker's readiness contract). It must NOT be wrapped in $(...):
# a command-substitution subshell reparents the backgrounded worker out of the
# main shell, which is unreliable (the worker may be reaped). Keeping it a direct
# child lets us track it in BG_PIDS, kill it on exit, and keep it alive for the
# whole run. The executable inherits $VGI_WORKER_BIN (wrappers exec it).
BOOTED_PORT=""
boot_http_worker() {
  local exe="$1"; shift
  local log pid port=""
  log="$(mktemp)"
  BOOTED_PORT=""
  # Start the worker with its cwd set to $STAGE — the directory the unittest runs
  # from — so DuckDB's per-test temp dir (__TEST_DIR__ → duckdb_unittest_tempdir/
  # <pid>) and the worker resolve the SAME relative path. Without this the http
  # worker (a separate process started from the repo root) cannot create the
  # COPY ... TO destination the test hands it as a relative path.
  ( cd "$STAGE" || exit 1; for kv in "$@"; do export "$kv"; done; exec "$exe" --http ) >"$log" 2>&1 &
  pid=$!
  BG_PIDS+=("$pid")
  for _ in $(seq 1 80); do
    kill -0 "$pid" 2>/dev/null || { echo "::error::http worker '$exe' exited" >&2; cat "$log" >&2; return 1; }
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$port" ] && break
    sleep 0.25
  done
  [ -n "$port" ] || { echo "::error::http worker '$exe' never announced a port" >&2; cat "$log" >&2; return 1; }
  BOOTED_PORT="$port"
}

export VGI_WORKER_BIN
export VGI_TEST_BEARER_TOKEN="test-secret-token"

WV="$HERE/wrappers/vgi-worker-versioned"
WVT="$HERE/wrappers/vgi-worker-versioned-tables"
WAO="$HERE/wrappers/vgi-worker-attach-options"
WBP="$HERE/wrappers/vgi-worker-bad-protocol"

# Serve the versioned + versioned_tables catalogs over HTTP on every Unix lane:
# attach/versioned_tables_*_http and attach/versioning_http attach an http://
# worker regardless of the main transport.
boot_versioned_http() {
  boot_http_worker "$WVT" && export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
  boot_http_worker "$WV"  && export VGI_VERSIONED_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
}

case "$TRANSPORT" in
  stdio)
    # Subprocess transport (the primary lane). Every query spawns a fresh worker.
    export VGI_TEST_WORKER="$VGI_WORKER_BIN"
    export VGI_TEST_DEDICATED_WORKER="$VGI_WORKER_BIN"
    if [ "$WINDOWS" = "0" ]; then
      export VGI_VERSIONED_WORKER="$WV"
      export VGI_VERSIONED_TABLES_WORKER="$WVT"
      export VGI_ATTACH_OPTIONS_WORKER="$WAO"
      export VGI_BAD_PROTOCOL_WORKER="$WBP"
      boot_versioned_http
    fi
    ;;
  launch)
    # AF_UNIX launcher transport (pooled workers). Unix-only.
    export VGI_TEST_WORKER="launch:${VGI_WORKER_BIN}"
    export VGI_TEST_DEDICATED_WORKER="$VGI_WORKER_BIN"
    export VGI_REQUIRE_LAUNCHER_TRANSPORT=1
    export VGI_VERSIONED_WORKER="launch:${WV}"
    export VGI_VERSIONED_TABLES_WORKER="launch:${WVT}"
    export VGI_ATTACH_OPTIONS_WORKER="launch:${WAO}"
    export VGI_BAD_PROTOCOL_WORKER="launch:${WBP}"
    boot_versioned_http
    ;;
  http)
    # Whole-suite-over-HTTP. Every ATTACH goes over http://, so staging injected
    # `LOAD httpfs`. VGI_REQUIRE_LAUNCHER_TRANSPORT is deliberately unset (the
    # launcher-only tests must skip here). bearer_auth runs separately below.
    boot_http_worker "$VGI_WORKER_BIN" "VGI_WORKER_CATALOG_NAME=example"
    export VGI_TEST_WORKER="http://localhost:${BOOTED_PORT}"
    # Only the *_HTTP_WORKER variants are set: tests read VGI_TEST_WORKER /
    # VGI_*_HTTP_WORKER over http, while the plain VGI_VERSIONED_WORKER etc.
    # remain a subprocess-path contract (unset here, so those subprocess-only
    # checks skip rather than mis-attach an http URL).
    if [ "$WINDOWS" = "0" ]; then
      boot_http_worker "$WV"  && export VGI_VERSIONED_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
      boot_http_worker "$WVT" && export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
      boot_http_worker "$WAO" && export VGI_ATTACH_OPTIONS_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
    fi
    ;;
  *)
    echo "::error::unknown TRANSPORT=$TRANSPORT (expected stdio|launch|http)"; exit 1 ;;
esac

cd "$STAGE"

echo "Warming the extension cache (vgi from community, deps from core) ..."
mkdir -p "$STAGE/test"
cat > "$STAGE/test/_warm.test" <<'EOF'
# name: test/_warm.test
# group: [warm]
statement ok
FORCE INSTALL vgi FROM community;

statement ok
INSTALL httpfs FROM core;

statement ok
INSTALL json FROM core;

statement ok
INSTALL parquet FROM core;

statement ok
INSTALL spatial FROM core;
EOF
"$HAYBARN_UNITTEST" "test/_warm.test" >/dev/null 2>&1 || echo "::warning::extension warm step did not fully succeed"
rm -f "$STAGE/test/_warm.test"

# Run the suite in one invocation, streaming the native sqllogictest report.
# bearer_auth/* runs separately on the http lane against a bearer-protected
# worker; on stdio/launch it runs inline (VGI_TEST_BEARER_TOKEN is set).
echo "Running suite (transport=$TRANSPORT) ..."
rc=0
if [ "$TRANSPORT" = "http" ]; then
  "$HAYBARN_UNITTEST" "test/sql/integration/*" "~test/sql/integration/bearer_auth/*" || rc=$?
  echo "Running bearer_auth/* against a bearer-protected http worker ..."
  boot_http_worker "$VGI_WORKER_BIN" "VGI_WORKER_CATALOG_NAME=example" "VGI_BEARER_TOKENS=test-secret-token=test-principal"
  VGI_TEST_WORKER="http://localhost:${BOOTED_PORT}" \
    "$HAYBARN_UNITTEST" "test/sql/integration/bearer_auth/*" || rc=$?
else
  "$HAYBARN_UNITTEST" "test/sql/integration/*" || rc=$?
fi

exit "$rc"

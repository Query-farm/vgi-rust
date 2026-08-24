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
# difference, not a worker behaviour difference; covered by the local suite).
# The http lane drops one file (see HTTP_SKIP below); Windows drops the fixtures
# that read parquet/csv from POSIX /tmp paths.
#
# bool_in_union.test is NO LONGER excluded here (removed 2026-08-21). It does not
# need a per-SDK exclusion: the file disables itself centrally with `mode skip`
# at test/sql/integration/table_in_out/echo/bool_in_union.test:20, which survives
# preprocess-require.awk, so staging it costs one skipped file and zero
# assertions. (Its own header attributes the pinned output to amd64 and not
# arm64 — the opposite of what this comment used to claim. The central skip makes
# the direction moot here; see the upstream file for the live rationale.)
# ---------------------------------------------------------------------------
AWK_HTTP=0
HTTP_SKIP=()
if [ "$TRANSPORT" = "http" ]; then
  AWK_HTTP=1
  # dynamic_filter.test used to be dropped here too, blamed on the prebuilt
  # binary. That was wrong: this SDK's HTTP server discarded a continuation
  # turn's Arrow custom_metadata, so DuckDB's tightening Top-N filter never
  # reached the worker. Fixed in vgi-rpc-rust 52b702d, which the committed
  # [patch.crates-io] in Cargo.toml picks up. Verified 2026-08-21 against this
  # SDK's own http worker: 52/52 assertions pass. Exclusion removed.
  HTTP_SKIP=(-not -name 'projection_pushdown_repro.test')
fi
# The native-branch fixtures (multi_branch_*, required_filters_native)
# used to stage and read parquet/csv from POSIX `/tmp/...` paths the worker's
# catalog hard-coded, so they had to be skipped on Windows, which has no `/tmp`.
# Both sides now resolve the same $VGI_TEST_BRANCH_DIR (exported below), so they
# run everywhere.
WIN_SKIP=()

echo "Staging preprocessed tests into $STAGE (transport=$TRANSPORT, windows=$WINDOWS) ..."
mkdir -p "$STAGE/test/sql/integration"
( cd "$INTEGRATION"
  find . -name '*.test' \
       -not -path '*/writable/*' -not -path '*/simple_writable/*' \
       -not -name 'nested_type_combinations.test' \
       -not -name 'expression_filter.test' \
       ${HTTP_SKIP[@]+"${HTTP_SKIP[@]}"} ${WIN_SKIP[@]+"${WIN_SKIP[@]}"} | while read -r f; do
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
# NOT exported here. bearer_auth/bearer_token.test gates on this variable
# (`require-env VGI_TEST_BEARER_TOKEN`) and its very first statement is an
# ATTACH carrying the `bearer_token` option — which the client rejects at bind
# on any non-HTTP LOCATION ("bearer_token is only valid for HTTP transport").
# So the file is meaningful on the http lane only, where it is run separately
# against a bearer-PROTECTED server and the variable is exported for that run.
#
# It used to be exported unconditionally and the file still vanished on every
# lane, because its expected errors say "(HTTP 401)" and DuckDB's default
# ignore_error_messages matches "HTTP" as a bare substring. The file now opts
# out of that default upstream, so on stdio/launch the ATTACH stopped being
# swallowed and started failing the lane for real. Letting the require-env gate
# do its job is the fix — an exclusion list would have to be kept in step with
# the suite; this cannot drift.
# Scratch dir shared by the native-branch fixtures (their read_parquet /
# read_csv / iceberg_scan arms) and the .test files' COPY-TO targets. The worker
# reads the same variable; the multi_branch_* and rff_*_native tests `require-env`
# it and skip without it. Not /tmp — Windows has none.
# Forward-slashed with no trailing separator: the .test files substitute this
# value verbatim into their COPY-TO targets, and the worker normalizes the same
# way, so both name a byte-identical path on Windows too.
VGI_TEST_BRANCH_DIR="${VGI_TEST_BRANCH_DIR:-$STAGE/branches}"
VGI_TEST_BRANCH_DIR="${VGI_TEST_BRANCH_DIR//\\//}"
VGI_TEST_BRANCH_DIR="${VGI_TEST_BRANCH_DIR%/}"
export VGI_TEST_BRANCH_DIR
mkdir -p "$VGI_TEST_BRANCH_DIR"

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
    #
    # The two OPTIONAL bearer tokens let the result cache's identity-isolation
    # test attach this same worker as alice and as bob; an absent or unknown
    # token still resolves to anonymous, so no other test on this shared server
    # starts 401ing. (The *required*-token server for bearer_auth/* boots below.)
    boot_http_worker "$VGI_WORKER_BIN" "VGI_WORKER_CATALOG_NAME=example" \
      "VGI_OPTIONAL_BEARER_TOKENS=vgi-test-alice=alice,vgi-test-bob=bob"
    export VGI_TEST_WORKER="http://localhost:${BOOTED_PORT}"
    # Lets HTTP-only tests (bearer/OAuth identity, which subprocess can't carry)
    # gate themselves via `require-env VGI_HTTP_TRANSPORT` instead of skipping.
    export VGI_HTTP_TRANSPORT=1
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
# worker; on stdio/launch it self-skips (no VGI_TEST_BEARER_TOKEN — see above).
echo "Running suite (transport=$TRANSPORT) ..."
rc=0

# Executed-case floor — the collapse detector. haybarn-unittest exits 0 whether
# one test skipped or every test did (a failed require/require-env is a SKIP, not
# an error), so "All tests passed" alone is not evidence anything ran: a dead
# shared worker, an empty stage, or a mis-wired env var all read as green while
# the suite quietly tested nothing. run_unittest accumulates the executed count
# (staged cases minus skips) into TOTAL_EXECUTED across every invocation; the
# check at the end fails the lane if it collapses below MIN_EXECUTED. This is a
# floor, not an equality — the upstream suite grows — so do NOT lower it to make
# a run pass; find what stopped running. (Only the floor is ported from
# vgi-typescript here, not its per-reason skip allowlist: this port's 3-OS matrix
# would need per-OS allowlists to maintain, and the floor alone catches the
# whole-suite collapses that are the real risk. The existing fatal-signal scan in
# run_unittest stays.)
#
# Measured 2026-07-24 against Query-farm/vgi main: stdio 269, launch 270, http
# 262 (Linux/macOS) / 256 (Windows, which runs the main catalog only). Floors sit
# ~15 below, with a lower Windows-http value for its smaller staged set.
TOTAL_EXECUTED=0
case "$TRANSPORT" in
  stdio)  MIN_EXECUTED="${MIN_EXECUTED:-250}" ;;
  launch) MIN_EXECUTED="${MIN_EXECUTED:-255}" ;;
  http)   if [ "$WINDOWS" = "1" ]; then MIN_EXECUTED="${MIN_EXECUTED:-240}"; else MIN_EXECUTED="${MIN_EXECUTED:-245}"; fi ;;
esac

# run_unittest — invoke haybarn-unittest, streaming its output, and additionally
# fail on a fatal-signal report that the process's own exit code cannot express.
#
# Catch2 arms handlers for SIGTERM/SIGINT/SIGSEGV/... for the duration of a test
# case. Those handlers are inherited by any process the extension fork()s, and
# run in the child if a signal lands before it execs. The child then prints a
# full "FAILED: ... due to a fatal error condition: SIGTERM" block plus a run
# summary — the *parent's* accumulated counters, since it's an address-space
# copy — and dies. The parent never sees it, records no failure, and exits 0.
# The only trace is on stdout, so that is what we scan. The fork window itself is
# fixed in Query-farm/vgi (SubProcess now resets signal dispositions in the
# child), but this class of failure can never reach the exit code, so the guard
# is worth keeping regardless of source.
run_unittest() {
  local log unittest_rc=0
  log="$(mktemp)"
  "$HAYBARN_UNITTEST" "$@" 2>&1 | tee "$log"
  # Read PIPESTATUS immediately: any command in between (including `|| true`)
  # overwrites it and would silently swallow every real test failure.
  unittest_rc="${PIPESTATUS[0]}"
  if grep -q 'due to a fatal error condition' "$log"; then
    echo "::error::a forked child ran the test harness's signal handler (see the" \
         "'fatal error condition' block above). The parent exited $unittest_rc and" \
         "would otherwise have passed. This is invisible to the exit code by construction."
    unittest_rc=1
  fi
  # Empty-stage guard: a runner that matched no tests still exits 0.
  if grep -q 'No test cases matched\|No tests ran' "$log"; then
    echo "::error::the runner matched no test cases — the glob or the staging is" \
         "wrong (an empty stage still exits 0). transport=$TRANSPORT"
    unittest_rc=1
  fi
  # Accumulate executed cases (staged total minus skips) for the floor check.
  # total = the N in the last "[i/N] (..%):" progress line; skipped = the sum of
  # the "Skipped tests for the following reasons:" block.
  local total skipped
  total="$(sed -n 's/^\[[0-9]*\/\([0-9]*\)\].*/\1/p' "$log" | tail -1)"
  if [ -n "$total" ]; then
    skipped="$(awk '
      /^Skipped tests for the following reasons:/ { b = 1; next }
      b && /^[[:space:]]*$/                       { b = 0; next }
      b && match($0, /: [0-9]+[[:space:]]*$/)     { n += substr($0, RSTART + 2) }
      END { print n + 0 }' "$log")"
    TOTAL_EXECUTED=$(( TOTAL_EXECUTED + total - skipped ))
  fi
  rm -f "$log"
  return "$unittest_rc"
}

if [ "$TRANSPORT" = "http" ]; then
  run_unittest "test/sql/integration/*" "~test/sql/integration/bearer_auth/*" || rc=$?
  echo "Running bearer_auth/* against a bearer-protected http worker ..."
  boot_http_worker "$VGI_WORKER_BIN" "VGI_WORKER_CATALOG_NAME=example" "VGI_BEARER_TOKENS=test-secret-token=test-principal"
  # Subshell so the override doesn't outlive the call: a `VAR=v func` prefix
  # persists after the function returns in bash, unlike `VAR=v some_binary`.
  ( export VGI_TEST_WORKER="http://localhost:${BOOTED_PORT}"
    export VGI_TEST_BEARER_TOKEN="test-secret-token"
    run_unittest "test/sql/integration/bearer_auth/*" ) || rc=$?
else
  run_unittest "test/sql/integration/*" || rc=$?
fi

# Executed-case floor — reached on every lane that runs the suite (the
# Windows-launch lane exited earlier with nothing to run). A collapse below the
# floor is the signature of a suite-wide silent skip.
if [ "$TOTAL_EXECUTED" -lt "$MIN_EXECUTED" ]; then
  echo "::error::only $TOTAL_EXECUTED test cases executed on the $TRANSPORT lane" \
       "(windows=$WINDOWS), floor is MIN_EXECUTED=$MIN_EXECUTED. This is the" \
       "signature of a suite-wide silent skip (a failed require is a SKIP, not an" \
       "error). Do NOT lower the floor to make this pass — find what stopped running."
  rc=1
else
  echo "Executed $TOTAL_EXECUTED test cases (floor $MIN_EXECUTED) on the $TRANSPORT lane (windows=$WINDOWS)."
fi

exit "$rc"

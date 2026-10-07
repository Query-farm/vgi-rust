#!/usr/bin/env bash
# Run the in-scope VGI integration suite against the Rust worker.
#
# Usage:
#   scripts/run_tests.sh                      # full in-scope suite
#   scripts/run_tests.sh scalar              # one category
#   scripts/run_tests.sh "test/sql/integration/table/sequence.test"   # one file
#   scripts/run_tests.sh --no-build ...      # skip cargo build
#
# Caches output under /tmp/vgi-rust-test-cache/:
#   run.log        full unittest stdout/stderr
#   failures       unique failing .test paths
#   summary        pass/fail totals + context around failures
#   worker.log     captured worker stderr (the C++ binary swallows it)

set -uo pipefail

VGI_RUST="${VGI_RUST:-$(cd "$(dirname "$0")/.." && pwd)}"
VGI_EXT="${VGI_EXT:-$HOME/Development/vgi}"
UNITTEST="${UNITTEST:-$VGI_EXT/build/release/test/unittest}"
BIN="${BIN:-$VGI_RUST/target/release/vgi-example-worker}"
# DuckDB's sqllogictest runner, given no --test-config, turns every error whose
# text contains "HTTP" or "Unable to connect" into a SKIP -- over the HTTP
# transport that is every worker error. The extension ships a config that
# skips on nothing; pass it to every unittest invocation.
TEST_CONFIG="$VGI_EXT/test/configs/no_error_skip.json"
[[ -f "$TEST_CONFIG" ]] || { echo "missing $TEST_CONFIG (vgi extension checkout too old?)" >&2; exit 1; }
CACHE="${CACHE:-/tmp/vgi-rust-test-cache}"
mkdir -p "$CACHE"

# Scratch dir the native-branch fixtures (read_parquet / read_csv / iceberg_scan
# arms) and their .test COPY-TO targets must agree on. The worker reads the same
# env var; the multi_branch_* and rff_*_native tests `require-env` it and skip
# without it.
BRANCH_DIR="${VGI_TEST_BRANCH_DIR:-$CACHE/branches}"
mkdir -p "$BRANCH_DIR"

BUILD=1
if [[ "${1:-}" == "--no-build" ]]; then BUILD=0; shift; fi

if [[ $BUILD == 1 ]]; then
  echo "[harness] building release worker..."
  ( cd "$VGI_RUST" && cargo build --release 2>&1 ) | tail -3
  if [[ ! -x "$BIN" ]]; then echo "[harness] build failed: $BIN missing"; exit 1; fi
fi

# Worker-stderr capture wrapper (the extension swallows fd2).
: > "$CACHE/worker.log"
WRAP="$CACHE/worker-wrap.sh"
cat > "$WRAP" <<EOF
#!/usr/bin/env bash
exec "$BIN" "\$@" 2>>"$CACHE/worker.log"
EOF
chmod +x "$WRAP"

# Per-catalog wrappers (single binary switched by VGI_WORKER_CATALOG_NAME).
mk_wrapper() { # name catalog [extra-env...]
  local f="$CACHE/worker-$1.sh"
  { echo '#!/usr/bin/env bash'
    echo "export VGI_WORKER_CATALOG_NAME=$2"
    shift 2
    for kv in "$@"; do echo "export $kv"; done
    echo "exec \"$BIN\" \"\$@\" 2>>\"$CACHE/worker.log\""
  } > "$f"
  chmod +x "$f"
  echo "$f"
}

W_VERSIONED=$(mk_wrapper versioned versioned)
W_VERSIONED_TABLES=$(mk_wrapper versioned_tables versioned_tables)
W_ATTACH_OPTIONS=$(mk_wrapper attach_options attach_options)
W_BAD_PROTOCOL=$(mk_wrapper bad_protocol example VGI_PROTOCOL_VERSION_OVERRIDE=99.0.0)

# Determine the filter set.
ARGS=()
if [[ $# -ge 1 ]]; then
  case "$1" in
    test/*) ARGS=("$1");;                                   # explicit path
    *)      ARGS=("test/sql/integration/$1/*");;            # category name
  esac
else
  # Full in-scope suite: everything under integration/ except the deferred
  # write path and the known-segfaulting nested_type_combinations test.
  ARGS=("test/sql/integration/*"
        "~test/sql/integration/writable/*"
        "~test/sql/integration/simple_writable/*"
        "~test/sql/integration/table_in_out/echo/nested_type_combinations.test")
fi

# Launcher transport: options_smoke (require-env VGI_REQUIRE_LAUNCHER_TRANSPORT)
# only passes with a `launch:` LOCATION. Run the subprocess suite without that
# env so launcher-only tests skip; pass LAUNCH=1 to test the launcher transport
# (VGI_TEST_WORKER=launch:<wrapper>).
LAUNCHER_ENV=()
if [[ "${LAUNCH:-0}" == "1" ]]; then
  TEST_WORKER="launch:$WRAP"
  LAUNCHER_ENV=(VGI_REQUIRE_LAUNCHER_TRANSPORT=1)
else
  TEST_WORKER="$WRAP"
fi

# VGI_TEST_BEARER_TOKEN is deliberately NOT set: bearer_auth/* gates on it and
# ATTACHes with a bearer_token, which the client rejects at bind on a non-HTTP
# LOCATION. Those tests run on the HTTP lane (scripts/run_http_tests.sh).
echo "[harness] running: ${ARGS[*]}"
env \
  "${LAUNCHER_ENV[@]}" \
  VGI_TEST_BRANCH_DIR="$BRANCH_DIR" \
  VGI_TEST_WORKER="$TEST_WORKER" \
  VGI_VERSIONED_WORKER="$W_VERSIONED" \
  VGI_VERSIONED_TABLES_WORKER="$W_VERSIONED_TABLES" \
  VGI_ATTACH_OPTIONS_WORKER="$W_ATTACH_OPTIONS" \
  VGI_BAD_PROTOCOL_WORKER="$W_BAD_PROTOCOL" \
  "$UNITTEST" --test-config "$TEST_CONFIG" "${ARGS[@]}" > "$CACHE/run.log" 2>&1
RC=$?

grep -E '^\[[0-9]+/[0-9]+\].*test/sql/integration' "$CACHE/run.log" >/dev/null 2>&1
# Extract failures: lines like "test/.../foo.test:NN" appearing in failure blocks.
grep -oE 'test/sql/integration/[A-Za-z0-9_/]+\.test(_slow)?' "$CACHE/run.log" \
  | sort -u > "$CACHE/allmentioned" 2>/dev/null
grep -B1 -A20 -iE 'unexpectedly|FAILED|Mismatch|Worker Exception|Error:' "$CACHE/run.log" \
  > "$CACHE/summary" 2>/dev/null
# A failing test name appears right after "Query unexpectedly" or in a FAILED block path.
awk '/unexpectedly|FAILED:|Mismatch on/{print}' "$CACHE/run.log" | grep -oE 'test/sql/integration/[A-Za-z0-9_/]+\.test(_slow)?' | sort -u > "$CACHE/failures" 2>/dev/null

echo "===== TAIL ====="
tail -6 "$CACHE/run.log"
echo "===== FAILURES ($(wc -l < "$CACHE/failures" | tr -d ' ')) ====="
cat "$CACHE/failures" 2>/dev/null
echo "(full log: $CACHE/run.log  summary: $CACHE/summary  worker stderr: $CACHE/worker.log)"
exit $RC

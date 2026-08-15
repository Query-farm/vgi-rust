#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Runs every documentation example against a real engine and checks its output.
# These examples are embedded verbatim in the vgi-rust documentation, so "it
# compiled" is not the bar — each one has to produce the rows the docs claim.
#
#   HAYBARN=/path/to/haybarn ./verify.sh
#
# Haybarn is DuckDB plus the `vgi` extension. Stock DuckDB cannot INSTALL vgi,
# which is why this is a script rather than a `cargo test`.

set -euo pipefail

HAYBARN="${HAYBARN:-haybarn}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
BIN="$ROOT/target/release"

if ! command -v "$HAYBARN" >/dev/null 2>&1 && [ ! -x "$HAYBARN" ]; then
  echo "haybarn not found. Set HAYBARN=/path/to/haybarn." >&2
  exit 1
fi

echo "building…"
(cd "$ROOT" && cargo build --release -p vgi-docs-examples >/dev/null)

fail=0

# check <name> <catalog> <binary> <sql> <expected-substring>
check() {
  local name="$1" catalog="$2" bin="$3" sql="$4" want="$5"
  local got
  got="$("$HAYBARN" -c "LOAD vgi;
ATTACH '$catalog' (TYPE vgi, LOCATION '$BIN/$bin');
$sql" 2>&1 || true)"
  if grep -qF -- "$want" <<<"$got"; then
    echo "ok   $name"
  else
    echo "FAIL $name — expected to find: $want"
    sed 's/^/     /' <<<"$got"
    fail=1
  fi
}

check scalar calc calcscalar \
  "SELECT calc.double(21) AS answer;" "42"

check table calc calc \
  "SELECT count(*) AS c, sum(n) AS t FROM calc.series(5000);" "12497500"

check constraint calc calc \
  "SELECT * FROM calc.series(-1);" "must be >= 0"

check table-in-out filters filter \
  "SELECT sum(value) AS s FROM filters.filter_positive((SELECT * FROM (VALUES (-2),(5),(0),(9),(-1)) AS t(value)));" "14"

check aggregate agg sum \
  "SELECT agg.vgi_sum(i::BIGINT) AS t FROM range(1000) r(i);" "499500"

check aggregate-nulls agg sum \
  "SELECT agg.vgi_sum(NULL::BIGINT) AS t;" "NULL"

check buffering buffers rowcount \
  "SELECT * FROM buffers.row_count((SELECT * FROM range(250000) r(i)));" "250000"

check catalog cat catalog \
  "SELECT count(*) AS c FROM cat.data.big_cities;" "2"

check caching rates cache \
  "SELECT count(*) FROM rates.rates();
   SELECT count(*) FROM rates.rates();
   SELECT count(*) FROM rates.rates();
   SELECT * FROM rates.upstream_calls();" "1"

exit "$fail"

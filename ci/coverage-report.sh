#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Merge the per-worker .profraw files an instrumented integration run produced
# (see vgi-example-worker's `coverage` feature) and emit a coverage report for
# the vgi SDK + example worker — what the VGI integration suite actually
# exercises. Untested code = a gap in the suite.
#
# Usage: coverage-report.sh <worker-binary> <profraw-dir> [out-dir]
set -uo pipefail

BIN="${1:?worker binary}"
COVDIR="${2:?dir of .profraw files}"
OUT="${3:-coverage}"

TBIN="$(rustc --print sysroot)/lib/rustlib/$(rustc -vV | sed -n 's/host: //p')/bin"
# Report only the workspace crates; drop the instrumented dependency tree and std.
IGNORE='(/\.cargo/|/rustc/|/library/(std|core|alloc)/|vgi-rpc|arrow|tokio)'

mkdir -p "$OUT"
find "$COVDIR" -name '*.profraw' -size +0c > "$COVDIR/list.txt"
echo "Merging $(wc -l < "$COVDIR/list.txt" | tr -d ' ') profraw files ..."
"$TBIN/llvm-profdata" merge -sparse -f "$COVDIR/list.txt" -o "$COVDIR/merged.profdata"

"$TBIN/llvm-cov" export "$BIN" -instr-profile="$COVDIR/merged.profdata" \
  -format=lcov -ignore-filename-regex="$IGNORE" > "$OUT/coverage.lcov"
"$TBIN/llvm-cov" report "$BIN" -instr-profile="$COVDIR/merged.profdata" \
  -ignore-filename-regex="$IGNORE" > "$OUT/report.txt"

# Surface a digest: the vgi SDK files + the TOTAL, sorted by line coverage so the
# least-covered (the gaps) are easy to spot.
{
  echo '### VGI integration-suite coverage of the worker'
  echo
  echo 'What the suite exercises in `vgi/` (the SDK) — least-covered first:'
  echo '```'
  awk 'NR<=2 || /vgi\/src/' "$OUT/report.txt" | sed 's#[^ ]*/vgi-rust/##'
  echo '...'
  grep -E '^TOTAL' "$OUT/report.txt"
  echo '```'
} >> "${GITHUB_STEP_SUMMARY:-/dev/stdout}"

echo "Wrote $OUT/coverage.lcov and $OUT/report.txt"

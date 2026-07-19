#!/usr/bin/env bash
# Build AND run every ```rust worker example in the project's docs, so the
# copy-paste examples can't drift from the API or silently stop working.
#
# Each ```rust fenced block in the doc markdown is expected to be a COMPLETE
# worker program (it has `fn main`). Blocks that are snippets should use a
# different fence (e.g. ```rust,no_run) so they are not picked up here.
#
# Steps:
#   1. extract each ```rust block into its own binary,
#   2. compile them against the LOCAL `vgi` (current code, not a release),
#   3. attach each built worker in Haybarn (Query Farm's DuckDB distribution,
#      which provides the `vgi` extension) and check `upper_case` works.
#
# Requires: cargo, and `uvx` (from https://docs.astral.sh/uv/) for `haybarn-cli`.
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/src/bin"

# The docs whose ```rust blocks are full worker programs.
DOCS=(
  "readme:$REPO/README.md"
  "crate:$REPO/vgi/README.md"
)

names=()
for pair in "${DOCS[@]}"; do
  tag="${pair%%:*}"; file="${pair#*:}"
  [ -f "$file" ] || continue
  # Split the file into ```rust … ``` blocks, writing each to <tag>_<n>.rs.
  while IFS= read -r rs; do names+=("$rs"); done < <(
    awk -v dir="$WORK/src/bin" -v tag="$tag" '
      /^```rust$/      { infence=1; n++; f=dir "/" tag "_" n ".rs"; out[++c]=f; next }
      infence && /^```$/ { infence=0; next }
      infence          { print > f }
      END              { for (i=1;i<=c;i++) print out[i] }
    ' "$file"
  )
done

if [ "${#names[@]}" -eq 0 ]; then
  echo "No \`\`\`rust example blocks found — nothing to check."; exit 0
fi

cat > "$WORK/Cargo.toml" <<EOF
[package]
name = "vgi-doc-examples"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
vgi = { path = "$REPO/vgi" }
# Must track vgi's vgi-rpc dep: a version skew pulls two vgi-rpc copies into
# the example crate, so vgi_rpc error types stop matching vgi's trait bounds.
vgi-rpc = "0.14"
arrow-array = "59"
arrow-schema = "59"

[workspace]
EOF

echo "Compiling ${#names[@]} doc example(s)…"
( cd "$WORK" && cargo build --quiet )
echo "✓ all doc examples compile"

PROBE="vgi_ci_probe"
EXPECTED="VGI_CI_PROBE"
for rs in "${names[@]}"; do
  name="$(basename "$rs" .rs)"
  bin="$WORK/target/debug/$name"
  sql="INSTALL vgi FROM community; LOAD vgi;
       ATTACH 'demo' (TYPE vgi, LOCATION '$bin');
       SELECT demo.main.upper_case('$PROBE') AS r;"
  # Track the LATEST haybarn-cli (no version pin) so doc-examples always
  # validates against the CURRENT community-published `vgi` extension —
  # mirroring the integration suite, which resolves the latest haybarn release
  # at runtime (HAYBARN_RELEASE in .github/workflows/integration.yml). A hard
  # pin goes stale: when the worker's wire schema advances (e.g. a new
  # catalog_attach field) the extension does strict schema-equality, so a pin
  # pointing at an older extension fails ("field count differs"). If
  # `INSTALL vgi FROM community` ever 404s transiently (a DuckDB version bump
  # landing before the extension republishes), it self-heals on the next run.
  out="$(uvx haybarn-cli :memory: -noheader -list -cmd ".bail on" -c "$sql" 2>&1 || true)"
  if ! grep -q "$EXPECTED" <<<"$out"; then
    echo "✗ $name: expected upper_case('$PROBE') = '$EXPECTED', got:"
    echo "$out"
    exit 1
  fi
  echo "✓ $name builds and runs end-to-end (upper_case → $EXPECTED)"
done

echo "All doc examples build and run."

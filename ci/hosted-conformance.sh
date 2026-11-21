#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run vgi's SDK-neutral Orchard hosted-mode conformance harness
# (test/hosted/run_hosted_conformance.py, see its README) against the example
# worker: `--http` in hosted mode at trace log level, and every non-HTTP
# transport (stdio, --unix, --tcp, --iroh-raw-upstream, --http behind the Iroh
# bridge) as a command that must refuse to start. No skips beyond the
# harness's opt-in rules. Needs uv.
#
# Required environment:
#   VGI_SRC          path to a Query-farm/vgi checkout (contains test/hosted)
#   VGI_WORKER_BIN   path to the built vgi-example-worker
# Extra arguments go to the harness (e.g. --streaming-body, --slow).
set -euo pipefail
: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${VGI_WORKER_BIN:?path to the built vgi-example-worker}"
BIN="$(cd "$(dirname "$VGI_WORKER_BIN")" && pwd)/$(basename "$VGI_WORKER_BIN")"
SOCK="${TMPDIR:-/tmp}/vgi-rust-hosted-refuse"
exec "$VGI_SRC/test/hosted/run_hosted_conformance.py" \
  --env VGI_LOG=trace \
  --env RUST_LOG=trace \
  --worker-cmd "$BIN --http --host 127.0.0.1 --port {port}" \
  --refuse-cmd "$BIN" \
  --refuse-cmd "$BIN --unix $SOCK-{port}.sock" \
  --refuse-cmd "$BIN --tcp 127.0.0.1:{port}" \
  --refuse-cmd "$BIN --iroh-raw-upstream 127.0.0.1:{port} --iroh-issuer hosted-refuse" \
  --refuse-cmd "$BIN --http --iroh-issuer hosted-refuse --host 127.0.0.1 --port {port}" \
  "$@"

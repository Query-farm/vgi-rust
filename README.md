# vgi-rust

Rust implementation of **VGI** (Vector Gateway Interface) workers — the worker
side of [Query Farm](https://query.farm)'s DuckDB "Hyperfederation" extension.
A VGI worker serves catalogs, functions, and tables to DuckDB over an
Apache-Arrow-IPC RPC protocol, behind `ATTACH '<name>' (TYPE vgi, LOCATION ...)`.

This is a port of the canonical Python `vgi` worker SDK and is **byte-for-byte
wire-compatible** with it, so a Rust worker drops in for a Python or Go one.
It is built on [`vgi-rpc`](https://github.com/Query-farm/vgi-rpc-rust) (the
transport-agnostic Arrow-IPC RPC framework).

## Workspace layout

| crate | published | summary |
|-------|:---------:|---------|
| [`vgi/`](vgi/) | ✅ [crates.io](https://crates.io/crates/vgi) | The worker SDK. Function models, declarative catalogs, wire dispatch, transports. |
| `vgi-example-worker/` | — | A fixture worker registering every function kind; drives the integration suite. `publish = false`. |

## Transports

A worker selects its transport from argv via `Worker::run`:

- **stdio** (default) — DuckDB spawns the worker per query.
- **Unix socket** (`--unix <path>`) — the launcher contract; one long-lived worker.
- **HTTP** (`--http`) — Arrow-IPC over HTTP with signed stateless stream tokens.

## Testing

Unit-level checks (`cargo fmt` / `clippy` / `build` / `doc`) run in CI. The full
behavioral suite is the canonical **VGI C++ integration suite**
(`test/sql/integration/*` in the `vgi` extension repo), which drives DuckDB's
`unittest` binary against the worker. Run it locally:

```sh
cargo build --release
scripts/run_tests.sh            # subprocess transport, full in-scope suite
LAUNCH=1 scripts/run_tests.sh   # launcher (Unix socket) transport
scripts/run_http_tests.sh       # HTTP transport
```

It passes across all three transports (8176 assertions on subprocess, 7774 on
HTTP, 0 failures).

## Development

`vgi` depends on the published `vgi-rpc` from crates.io. To develop against an
unreleased `vgi-rpc` checkout, add an **uncommitted** patch to the root
`Cargo.toml`:

```toml
[patch.crates-io]
vgi-rpc = { path = "../vgi-rpc-rust/vgi-rpc" }
```

## License

Query Farm Source-Available License v1.0 — see [LICENSE](LICENSE).

Copyright © 2025, 2026 Query Farm LLC.

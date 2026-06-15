# vgi

[![crates.io](https://img.shields.io/crates/v/vgi.svg)](https://crates.io/crates/vgi)
[![docs.rs](https://docs.rs/vgi/badge.svg)](https://docs.rs/vgi)

Rust SDK for writing **VGI** (Vector Gateway Interface) workers — the worker
side of [Query Farm](https://query.farm)'s DuckDB "Hyperfederation" extension.
A VGI worker exposes catalogs, functions, and tables to DuckDB over an
Apache-Arrow-IPC RPC protocol; this crate gives you the typed building blocks
to implement one in Rust.

It is a port of the canonical Python `vgi` worker SDK and is **byte-for-byte
wire-compatible** with it — a Rust worker is a drop-in replacement for a Python
or Go one behind the same DuckDB `ATTACH ... (TYPE vgi)`.

Built on [`vgi-rpc`](https://crates.io/crates/vgi-rpc) (the transport-agnostic
Arrow-IPC RPC framework). Stock `arrow-rs` 58.x, MSRV 1.86.

## What it provides

- **Function models** — register scalar, table (producer), table-in-out
  (exchange), table-buffering (sink → combine → source), and aggregate
  (update / combine / finalize, incl. window/streaming) functions via typed
  traits.
- **Declarative catalogs** — schemas, views, macros, and function-backed
  tables, plus version-shaped catalogs (time travel / `AT`), constraints,
  column statistics, and MetaWorker-style secondary catalogs attachable by
  name.
- **Wire plumbing handled for you** — bind / init / process dispatch, the
  result-envelope boxing, projection & filter pushdown, ORDER BY / TABLESAMPLE
  hints, settings, secrets (two-phase bind), bearer auth, and a cross-process
  state store for buffering / aggregate work.
- **Transports** — stdio (default), Unix-socket launcher (`--unix`), and HTTP
  (`--http`), selected from argv by [`Worker::run`].

## Quick start

```rust
use vgi::Worker;

fn main() {
    let mut worker = Worker::new();
    // worker.register_scalar(MyScalarFn);
    // worker.register_table(MyTableFn);
    // worker.set_catalog(my_catalog());
    worker.run(); // serves stdio / --unix <path> / --http
}
```

See the [`vgi-example-worker`](https://github.com/Query-farm/vgi-rust) crate in
this repository for a complete fixture worker exercising every function kind.

## Status

Verified against the canonical VGI C++ integration suite across all three
transports — subprocess, launcher (Unix socket), and HTTP (8176 / 7774
assertions on subprocess / HTTP respectively, 0 failures).

## License

Query Farm Source-Available License v1.0 — see [LICENSE](../LICENSE).

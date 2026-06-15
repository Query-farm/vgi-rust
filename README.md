# VGI (Vector Gateway Interface) — Rust

<p align="center">
  <img src="docs/vgi-logo.png" alt="VGI Logo" width="480">
</p>

<p align="center">
  <strong>Apache Arrow-based protocol for extending DuckDB using any language.</strong><br/>
  <strong>This is the Rust SDK — build native, single-binary VGI workers with zero DuckDB linking.</strong>
</p>

<p align="center">
  Created by <a href="https://query.farm">Query.Farm</a>
</p>

---

A **VGI worker** is a separate process that DuckDB talks to over Apache Arrow
IPC. It can expose scalar / table / aggregate functions and whole catalogs
(schemas, tables, views) that behave exactly like native DuckDB objects — no C++
extension to compile, no linking against DuckDB, no version coupling.

This crate, [`vgi`](https://crates.io/crates/vgi), is the **Rust** worker SDK. It
is byte-for-byte wire-compatible with the canonical
[Python](https://github.com/Query-farm/vgi-python) and Go implementations, so a
Rust worker is a drop-in replacement behind the same `ATTACH ... (TYPE vgi)`.
Built on [`vgi-rpc`](https://crates.io/crates/vgi-rpc); stock `arrow-rs` 58.x,
MSRV 1.86.

## See It in Action

A worker is a small Rust binary. Add the dependency:

```toml
# Cargo.toml
[dependencies]
vgi = "0.1"
vgi-rpc = "0.2"
arrow-array = "58"
arrow-schema = "58"
```

Define a function and serve it:

```rust
// src/main.rs
use std::sync::Arc;

use arrow_array::{cast::AsArray, ArrayRef, RecordBatch, StringArray};
use arrow_schema::DataType;
use vgi::{ArgSpec, FunctionMetadata, ProcessParams, ScalarFunction, Worker};
use vgi_rpc::{Result, RpcError};

/// `upper_case(s)` — uppercase a string column.
struct UpperCase;

impl ScalarFunction for UpperCase {
    fn name(&self) -> &str {
        "upper_case"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Convert string values to uppercase".into(),
            return_type: Some(DataType::Utf8),
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "varchar", "String to uppercase")]
    }

    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let col = batch.column(0).as_string::<i32>();
        let upper: StringArray = col.iter().map(|v| v.map(str::to_uppercase)).collect();
        let out: ArrayRef = Arc::new(upper);
        RecordBatch::try_new(params.output_schema.clone(), vec![out])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

fn main() {
    let mut worker = Worker::new();
    worker.register_scalar(UpperCase);
    worker.run(); // serves stdio (default), --unix <path>, or --http
}
```

Build it, then call it from any DuckDB-compatible engine:

```sql
-- First time only (stock DuckDB):
INSTALL vgi FROM community;
LOAD vgi;

-- LOCATION is the command DuckDB runs to launch the worker.
ATTACH 'demo' (TYPE vgi, LOCATION './target/release/my-worker');

SELECT demo.main.upper_case(name) FROM (VALUES ('alice'), ('bob')) t(name);
-- ALICE
-- BOB
```

That's it — a native-speed function, shipped as one static binary, callable from
SQL with no extension build.

---

## Why VGI?

VGI extends DuckDB with functions and catalogs that run in a **separate
process**, communicating via Apache Arrow IPC:

| Traditional extension | VGI worker |
|-----------------------|------------|
| C/C++ compilation required | Write it in Rust (or Python / Go / …) |
| Tied to a DuckDB version | Version independent |
| Complex build / release cycle | Ship one executable |
| Runs in-process | Process isolation |
| Single-threaded | Parallel workers |

### Why the Rust SDK specifically?

- **One static binary** — no interpreter, no runtime, trivial to ship and run.
- **Native performance** with zero-copy Arrow throughout.
- **Strong typing** — function arguments, schemas, and catalogs are checked at
  compile time *and* validated at SQL bind time.

**Use cases:** call REST APIs from SQL, run ML inference, expose external data
sources as queryable catalogs, build high-throughput ETL transforms, or ship
domain-specific functions to your team as a single binary.

---

## Function types

Register any mix of these via the typed traits in [`vgi`](https://docs.rs/vgi):

| Type | Trait | SQL pattern | Use case |
|------|-------|-------------|----------|
| **Scalar** | `ScalarFunction` | `SELECT f(col) FROM t` | Per-row transforms (1:1) |
| **Table** | `TableFunction` | `SELECT * FROM f(args)` | Generate / scan data |
| **Table-In-Out** | `TableInOutFunction` | `SELECT * FROM f((SELECT …))` | Streaming transforms |
| **Table-Buffering** | `TableBufferingFunction` | `SELECT * FROM f((SELECT …))` | Aggregate-then-emit (sink → combine → source) |
| **Aggregate** | `AggregateFunction` | `SELECT f(col) … GROUP BY …` | Grouped / window / streaming aggregates |

Each trait is small: a `name`, `metadata`, `argument_specs`, an `on_bind` to
resolve the output schema, and a `process` (or the buffering / aggregate
lifecycle methods). Projection & filter pushdown, ORDER BY / TABLESAMPLE hints,
settings, secrets (two-phase bind), and a cross-process state store are handled
for you.

## Beyond functions: full catalogs

A worker can expose a complete database catalog — schemas, function-backed
**tables**, **views**, and **macros** — via `Worker::set_catalog`, including
constraints, column statistics, time travel (`AT`), and MetaWorker-style
secondary catalogs attachable by name:

```sql
ATTACH 'external_db' (TYPE vgi, LOCATION './my-catalog-worker');

SELECT * FROM external_db.main.users;            -- a function-backed table
SELECT * FROM external_db.analytics.daily_view;  -- a view
SELECT external_db.main.transform(col) FROM t;   -- a function
```

This lets a worker act as a bridge — databases, APIs, file systems — presented
to DuckDB as native catalogs.

## Transports

A worker selects its transport from argv via `Worker::run`:

- **stdio** (default) — DuckDB spawns the worker per query.
- **Unix socket** (`--unix <path>`) — the launcher contract; one long-lived worker.
- **HTTP** (`--http`) — Arrow-IPC over HTTP with AEAD-sealed stateless stream
  tokens, optional bearer auth, and zstd compression.

## Protocol overview

VGI uses [`vgi-rpc`](https://crates.io/crates/vgi-rpc), an Apache-Arrow-IPC RPC
framework, for all client ↔ worker communication:

```
DuckDB (client)                      VGI worker
  │                                      │
  │──── bind(request) ─────────────────▶ │  function name, args, input schema
  │◀─── BindResponse ───────────────────  │  output schema, opaque data
  │                                      │
  │──── init(request) ─────────────────▶ │  start the processing stream
  │◀─── stream header ──────────────────  │  execution_id, max_workers
  │                                      │
  │──── process / exchange(batch) ─────▶ │
  │◀─── output batch ───────────────────  │  your process(batch)
  │            …                         │
  │──── [stream close] ────────────────▶ │
  └──────────────────────────────────────┘
```

---

## Workspace layout

| crate | published | summary |
|-------|:---------:|---------|
| [`vgi/`](vgi/) | ✅ [crates.io](https://crates.io/crates/vgi) · [docs.rs](https://docs.rs/vgi) | The worker SDK: function models, declarative catalogs, wire dispatch, transports. |
| `vgi-example-worker/` | — | A fixture worker registering every function kind; drives the integration suite. `publish = false`. |

## Testing

`cargo fmt` / `clippy` / `build` / `doc` run in CI. The full behavioral suite is
the canonical **VGI C++ integration suite** (`test/sql/integration/*` in the
`vgi` extension repo), which drives DuckDB's `unittest` binary against the
worker. It passes across all three transports (8176 assertions on subprocess,
7774 on HTTP, 0 failures). Run it locally:

```sh
cargo build --release
scripts/run_tests.sh            # subprocess transport, full in-scope suite
LAUNCH=1 scripts/run_tests.sh   # launcher (Unix socket) transport
scripts/run_http_tests.sh       # HTTP transport
```

## Development

`vgi` depends on the published `vgi-rpc` from crates.io. To develop against an
unreleased `vgi-rpc` checkout, add an **uncommitted** patch to the root
`Cargo.toml`:

```toml
[patch.crates-io]
vgi-rpc = { path = "../vgi-rpc-rust/vgi-rpc" }
```

```sh
cargo build --workspace
cargo clippy -p vgi --all-targets --all-features -- -D warnings
cargo fmt --all
```

---

## License

Copyright © 2025, 2026 Query Farm LLC.

Licensed under the **Query Farm Source-Available License, Version 1.0** — see
[LICENSE](LICENSE) for the binding terms. In summary (the LICENSE text governs):

- ✅ **Use, copy, modify, and redistribute** the code freely, **including in
  production and for commercial purposes** — your own internal use, and building
  products and services on top of VGI.
- 🚫 Not permitted **without a separate commercial license**: offering a
  *competing* VGI-equivalent product or service to third parties (hosted,
  embedded, or as-a-service), or operating a commercial marketplace for such
  services.
- ⏳ Each released version converts to the **Apache License, Version 2.0**, ten
  years after its public release.

For a commercial license or any licensing questions, contact
[hello@query.farm](mailto:hello@query.farm).

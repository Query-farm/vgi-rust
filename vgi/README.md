# vgi

[![crates.io](https://badgen.net/crates/v/vgi)](https://crates.io/crates/vgi)
[![docs.rs](https://docs.rs/vgi/badge.svg)](https://docs.rs/vgi)

<p align="center">
  <img src="https://raw.githubusercontent.com/Query-farm/vgi-rust/main/docs/vgi-logo.png" alt="VGI Logo" width="420">
</p>

<p align="center">
  <strong>Add your own functions and tables to DuckDB — written in Rust, shipped as one binary.<br/>
  No C++ extension to compile, no linking against DuckDB, no version coupling.</strong>
</p>

---

A **VGI worker** is a small Rust program that DuckDB talks to over Apache Arrow IPC.
It can expose scalar / table / aggregate functions and whole catalogs (schemas,
tables, views) that behave like native DuckDB objects. DuckDB launches your worker
for you when a query needs it — you never run a server by hand.

`vgi` is the Rust SDK for building those workers. It is byte-for-byte
wire-compatible with the canonical
[Python](https://github.com/Query-farm/vgi-python) SDK, so a Rust worker
drops in behind the same `ATTACH ... (TYPE vgi)`. Built on
[`vgi-rpc`](https://crates.io/crates/vgi-rpc); stock `arrow-rs` 59.x, **MSRV 1.90**.

## Why a worker instead of a C++ extension?

| Traditional DuckDB extension | VGI worker |
|------------------------------|------------|
| Written in C/C++, compiled and linked against DuckDB | Written in Rust, one standalone binary |
| Must be rebuilt for each DuckDB version | Version independent |
| Complex build / signing / release cycle | `cargo build`, ship the binary |
| Runs in-process | Process isolation |

**Reach for it when you want to:** call REST APIs from SQL, run ML inference,
expose an external database / API / filesystem as a queryable catalog, or ship
domain-specific functions to your team as a single binary.

## Your first worker

**1. Create a project and add the dependencies** (these are exactly what the
example below needs):

```toml
# Cargo.toml
[dependencies]
vgi = "0.1"
vgi-rpc = "0.2"
arrow-array = "59"
arrow-schema = "59"
```

**2. Write a function and serve it:**

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

**3. Build it:**

```sh
cargo build --release
```

**4. Call it from a DuckDB engine that has the `vgi` extension.** The `vgi`
extension currently ships with Query Farm's
[Haybarn](https://github.com/Query-farm-haybarn/haybarn) DuckDB distribution,
which starts with no install via `uvx haybarn-cli`. From your project directory:

```sql
-- Haybarn ships the `vgi` extension. DuckDB LAUNCHES the worker for you;
-- LOCATION is the command it runs, and the alias 'demo' is what you
-- qualify functions with in SQL.
ATTACH 'demo' (TYPE vgi, LOCATION './target/release/my-worker');

SELECT demo.main.upper_case(name) FROM (VALUES ('alice'), ('bob')) t(name);
-- ALICE
-- BOB

-- Or drop the prefix:
USE demo;
SELECT main.upper_case('hello');   -- HELLO
```

> **`LOCATION` gotcha:** the path is resolved relative to the DuckDB process's
> working directory, not your project. If the worker isn't found, use an absolute
> path (e.g. `LOCATION '/abs/path/to/target/release/my-worker'`).

That's it — a native-speed SQL function, shipped as one static binary, with no
extension to compile.

## Iterating

Change your Rust, rebuild, and re-attach. DuckDB pools the worker process per
attachment, so the reliable way to pick up a new build is to re-`ATTACH` (or start
a fresh session):

```sh
cargo build --release
```

```sql
DETACH demo;
ATTACH 'demo' (TYPE vgi, LOCATION './target/release/my-worker');
```

## Troubleshooting

- **`ATTACH` can't find the worker** — `LOCATION` is resolved relative to DuckDB's
  working directory, not your project. Use an absolute path.
- **`Catalog Error: ... upper_case does not exist`** — qualify with the attach
  alias (`demo.main.upper_case`) or run `USE demo;` first.
- **A runtime error in your function** — anything you return as `RpcError` (or any
  panic) surfaces in DuckDB's error message; return descriptive errors from
  `process` to make debugging easy.
- **Type mismatch at the call site** — `argument_specs` is validated at bind time,
  so a wrong-typed column fails fast with a clear message before any rows flow.

## Function types

Register any mix of these via the typed traits in [`vgi`](https://docs.rs/vgi):

| Type | Trait | SQL pattern | Use case |
|------|-------|-------------|----------|
| **Scalar** | `ScalarFunction` | `SELECT f(col) FROM t` | Per-row transforms (1:1) |
| **Table** | `TableFunction` | `SELECT * FROM f(args)` | Generate / scan data |
| **Table-In-Out** | `TableInOutFunction` | `SELECT * FROM f((SELECT …))` | Streaming transforms |
| **Table-Buffering** | `TableBufferingFunction` | `SELECT * FROM f((SELECT …))` | Aggregate-then-emit (sink → combine → source) |
| **Aggregate** | `AggregateFunction` | `SELECT f(col) … GROUP BY …` | Grouped / window / streaming aggregates |

Each trait is small: `name`, `metadata`, `argument_specs`, an `on_bind` to resolve
the output schema, and `process` (or the buffering / aggregate lifecycle methods).
Projection & filter pushdown, ORDER BY / TABLESAMPLE hints, settings, secrets
(two-phase bind), bearer auth, and a cross-process state store are handled for you.

## Beyond functions: full catalogs

`Worker::set_catalog` exposes a complete catalog — schemas, function-backed
**tables**, **views**, and **macros** — with constraints, column statistics, time
travel (`AT`), and secondary catalogs attachable by name:

```sql
ATTACH 'external_db' (TYPE vgi, LOCATION './my-catalog-worker');

SELECT * FROM external_db.main.users;            -- a function-backed table
SELECT * FROM external_db.analytics.daily_view;  -- a view
SELECT external_db.main.transform(col) FROM t;   -- a function
```

A worker can act as a bridge — databases, APIs, filesystems — presented to DuckDB
as native catalogs.

## Transports

`Worker::run` picks the transport from argv:

- **stdio** (default) — DuckDB spawns the worker per query. Nothing to configure.
- **Unix socket** (`--unix <path>`) — one long-lived worker (the launcher contract).
- **HTTP** (`--http`) — Arrow-IPC over HTTP with AEAD-sealed stateless stream
  tokens and optional bearer auth.

## Where to go next

- **[API docs (docs.rs)](https://docs.rs/vgi)** — every trait and type.
- **[Example worker](https://github.com/Query-farm/vgi-rust/tree/main/vgi-example-worker)** — a fixture worker exercising every function kind and full catalogs.

## License

Query Farm Source-Available License v1.0 — see [LICENSE](https://github.com/Query-farm/vgi-rust/blob/main/LICENSE).
Copyright © 2025, 2026 Query Farm LLC.

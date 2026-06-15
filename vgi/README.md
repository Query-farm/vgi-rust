# vgi

[![crates.io](https://img.shields.io/crates/v/vgi.svg)](https://crates.io/crates/vgi)
[![docs.rs](https://docs.rs/vgi/badge.svg)](https://docs.rs/vgi)

<p align="center">
  <img src="https://raw.githubusercontent.com/Query-farm/vgi-rust/main/docs/vgi-logo.png" alt="VGI Logo" width="420">
</p>

<p align="center">
  <strong>Build native, single-binary DuckDB extensions in Rust — no C++, no linking against DuckDB.</strong>
</p>

---

`vgi` is the **Rust SDK for writing VGI (Vector Gateway Interface) workers** —
the worker side of [Query Farm](https://query.farm)'s DuckDB "Hyperfederation"
extension. A worker is a separate process that DuckDB talks to over Apache Arrow
IPC; it exposes scalar / table / aggregate functions and whole catalogs
(schemas, tables, views) that behave like native DuckDB objects, with no
compiled C++ extension and no version coupling.

It is **byte-for-byte wire-compatible** with the canonical
[Python](https://github.com/Query-farm/vgi-python) and Go implementations, so a
Rust worker drops in behind the same `ATTACH ... (TYPE vgi)`. Built on
[`vgi-rpc`](https://crates.io/crates/vgi-rpc); stock `arrow-rs` 58.x, MSRV 1.86.

## Example

```rust
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

Then from any DuckDB-compatible engine:

```sql
INSTALL vgi FROM community; LOAD vgi;      -- first time only
ATTACH 'demo' (TYPE vgi, LOCATION './target/release/my-worker');
SELECT demo.main.upper_case(name) FROM (VALUES ('alice'), ('bob')) t(name);
-- ALICE
-- BOB
```

## Function types

| Type | Trait | Use case |
|------|-------|----------|
| Scalar | `ScalarFunction` | Per-row transforms (1:1) |
| Table | `TableFunction` | Generate / scan data |
| Table-In-Out | `TableInOutFunction` | Streaming transforms |
| Table-Buffering | `TableBufferingFunction` | Aggregate-then-emit (sink → combine → source) |
| Aggregate | `AggregateFunction` | Grouped / window / streaming aggregates |

Beyond functions, `Worker::set_catalog` exposes full catalogs — schemas,
function-backed tables, views, and macros — with constraints, column statistics,
time travel (`AT`), and secondary catalogs attachable by name. Projection &
filter pushdown, ORDER BY / TABLESAMPLE hints, settings, secrets, bearer auth,
and a cross-process state store are handled for you.

## Transports

Selected from argv by [`Worker::run`]: **stdio** (default), **Unix socket**
(`--unix <path>`, the launcher contract), and **HTTP** (`--http`, Arrow-IPC over
HTTP with AEAD-sealed stateless stream tokens and optional bearer auth).

## Status

Verified against the canonical VGI C++ integration suite across all three
transports — subprocess, launcher, and HTTP (8176 / 7774 assertions on
subprocess / HTTP, 0 failures). See the
[repository](https://github.com/Query-farm/vgi-rust) for a complete fixture
worker exercising every function kind.

## License

Query Farm Source-Available License v1.0 — see [LICENSE](https://github.com/Query-farm/vgi-rust/blob/main/LICENSE).
Free for use, modification, and redistribution including in production; a
separate commercial license is required only to offer a *competing* VGI product.
Each release converts to Apache-2.0 ten years after its publication.
Copyright © 2025, 2026 Query Farm LLC.

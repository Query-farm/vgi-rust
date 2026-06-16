# Getting Started: Your First VGI Worker

This guide takes you from an empty directory to a Rust function you can call from
SQL in DuckDB. It assumes you're comfortable with Rust but new to VGI.

A **VGI worker** is a standalone Rust binary that DuckDB launches and talks to over
Apache Arrow IPC. You don't run a server — DuckDB spawns your worker when a query
needs it. By the end you'll have an `upper_case(...)` function callable from SQL.

## Prerequisites

- **Rust 1.86 or newer** (`rustc --version`).
- **A DuckDB-compatible SQL engine.** Stock [DuckDB](https://duckdb.org/docs/installation/)
  is fine — install the CLI and confirm `duckdb --version` works. (Query Farm's
  [Haybarn](https://github.com/Query-farm-haybarn/haybarn) distribution also works
  and ships the `vgi` extension pre-signed.)

## 1. Create a project

```sh
cargo new my-worker
cd my-worker
```

## 2. Add the dependencies

These four crates are everything the example below needs. Pin Arrow to 58.x to
match the SDK.

```toml
# Cargo.toml
[dependencies]
vgi = "0.1"
vgi-rpc = "0.2"
arrow-array = "58"
arrow-schema = "58"
```

## 3. Write a scalar function

Replace `src/main.rs` with:

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
        // Input arrives as Arrow record batches, one column per argument.
        let col = batch.column(0).as_string::<i32>();
        let upper: StringArray = col.iter().map(|v| v.map(str::to_uppercase)).collect();
        let out: ArrayRef = Arc::new(upper);
        // Output is a single `result` column matching params.output_schema.
        RecordBatch::try_new(params.output_schema.clone(), vec![out])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

fn main() {
    let mut worker = Worker::new();
    worker.register_scalar(UpperCase);
    worker.run();
}
```

**What each piece does:**
- `name` — the SQL function name (`upper_case`).
- `metadata` — describes the function to DuckDB's optimizer; `return_type` fixes
  the output type to `VARCHAR`.
- `argument_specs` — declares one column argument at position 0. DuckDB validates
  the call against this at bind time, before any rows flow.
- `process` — runs per batch of rows. Input columns come in as Arrow arrays; you
  return one `result` column with the same row count.
- `Worker::run()` — parses argv and serves. With no flags it uses **stdio**, which
  is what DuckDB's default `ATTACH` expects.

## 4. Build it

```sh
cargo build --release
```

Your worker binary is now at `target/release/my-worker`.

## 5. Call it from DuckDB

Start a DuckDB session **from your project directory** (so the relative path in
`LOCATION` resolves):

```sh
duckdb
```

Then, in the DuckDB prompt:

```sql
-- First time only: pull and load the `vgi` extension. This persists.
INSTALL vgi FROM community;
LOAD vgi;

-- DuckDB launches your worker. LOCATION is the command it runs.
-- 'demo' is the alias you'll qualify functions with in SQL.
ATTACH 'demo' (TYPE vgi, LOCATION './target/release/my-worker');

SELECT demo.main.upper_case(name) FROM (VALUES ('alice'), ('bob')) t(name);
-- ┌────────────────────┐
-- │ upper_case("name") │
-- ├────────────────────┤
-- │ ALICE              │
-- │ BOB                │
-- └────────────────────┘
```

Want to drop the `demo.main.` prefix?

```sql
USE demo;
SELECT main.upper_case('hello');   -- HELLO
```

## 6. Iterate

Change your Rust, rebuild, and re-attach. DuckDB spawns and pools worker
processes, so the reliable way to pick up a new build (or a changed catalog) is to
re-`ATTACH` — `DETACH` and `ATTACH` again, or just start a fresh session:

```sh
cargo build --release
```

```sql
DETACH demo;
ATTACH 'demo' (TYPE vgi, LOCATION './target/release/my-worker');
```

## 7. Troubleshooting

- **`ATTACH` fails to find the worker** — `LOCATION` is resolved relative to
  DuckDB's working directory, not your project. Use an absolute path:
  `LOCATION '/abs/path/to/my-worker/target/release/my-worker'`.
- **`Catalog Error: ... upper_case does not exist`** — qualify with the attach
  alias (`demo.main.upper_case`) or run `USE demo;` first.
- **A runtime error in your function** — anything you return as `RpcError` (or any
  panic) surfaces in DuckDB's error message. Return descriptive errors from
  `process` to make debugging easy.
- **Type mismatch at the call site** — `argument_specs` is validated at bind time,
  so a wrong-typed column fails fast with a clear message before rows flow.

## 8. Where to go next

- **More function kinds** — table, table-in-out, buffering, and aggregate
  functions, registered with `register_table`, `register_table_in_out`,
  `register_buffering`, `register_aggregate`. See the
  [API docs](https://docs.rs/vgi).
- **Full catalogs** — expose schemas, tables, and views with `Worker::set_catalog`
  so a worker behaves like an external database.
- **The example worker** — [`vgi-example-worker/src/`](../vgi-example-worker/src/)
  implements every trait (constants, varargs, type bounds, settings, secrets,
  auth) and is the best reference for real patterns.
- **Other transports** — run under `--unix <path>` (long-lived launcher) or
  `--http` for networked deployments.

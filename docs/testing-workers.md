# Testing your worker

Use `vgi-client` with its optional `testing` feature to exercise your worker over
the normal attach, bind, and scan protocol. The harness checks typed rows and
schemas; you do not need DuckDB for the basic tests.

```toml
[dev-dependencies]
vgi-client = { version = "0.43", default-features = false, features = ["testing"] }
arrow-array = "60"
arrow-schema = "60"
arrow-select = "60"
```

For an unreleased SDK checkout, use its local `vgi-client` path instead of the
published version. Add other client features, such as `http`, only if your tests
use them.

## Check expected rows through the subprocess transport

Place an integration test in `tests/scan.rs`. Cargo builds your package's binary
for integration tests and exposes its path through `CARGO_BIN_EXE_<binary-name>`.
Use that path rather than searching `target/debug` or skipping when the binary is
missing. Replace the binary, catalog, function, and schema below with your own:

```rust,no_run
use std::sync::Arc;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use vgi_client::{BindSpec, ScanOptions};
use vgi_client::testing::{ScanResult, TestWorker};

#[test]
fn returns_exact_values() -> vgi_client::Result<()> {
    let mut worker = TestWorker::connect_subprocess(
        &[env!("CARGO_BIN_EXE_my-worker")],
        "example",
    )?;
    let spec = BindSpec::table("rows").in_schema("main");
    let actual = worker.scan(&spec, &ScanOptions::default())?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(Int64Array::from(vec![9_007_199_254_740_993])),
        Arc::new(StringArray::from(vec![Some("exact")])),
    ]).unwrap();
    let expected = ScanResult::try_new(schema, vec![batch])?;
    actual.assert_matches(&expected)
}
```

`assert_matches` compares the declared schema (including names, types,
nullability, metadata, and column order) and the multiset of rows. Row ordering
and batch boundaries may differ. Duplicate counts and SQL NULLs must match;
large signed and unsigned integers never pass through floating point. Empty
results retain their declared schema, so a wrong empty result type is detected.
The declared schema comes from bind; transport-level nullability relaxation does
not change it. NULLs in fields declared non-nullable fail validation.

The comparison uses Arrow row encoding and supports the types that encoder
supports; unsupported types produce an error. Floating-point bit patterns,
including signed zero and NaN payloads, are distinguished deliberately: the
assertion checks data fidelity, not SQL equality. If an API promises a particular
row order, inspect `batches()` as well. Tests collect results in memory, so use
small fixtures and finite scans.

## Compare pushdown with an independent reference

A pushdown regression test should establish that filtering early returns the
same result as reading all rows and filtering them independently. Do not use the
worker's `PushdownFilters::apply()` for the expected result: the same bug could
exist on both sides.

`check_pushdown` performs these steps:

1. Run a full scan with default scan options.
2. Pass those rows to your independent reference callback.
3. Apply the requested projection to the reference rows.
4. Compare the result with a scan carrying the pushed filter and projection.

The callback must return the full original schema, before projection. It can
use a simple Rust predicate with explicitly defined NULL behavior, hand-authored
expected batches, or a separate SQL engine. Keep the source unchanged between
these two scans. Pair this comparison with an explicit expected full scan:
both scans could otherwise share a decoding or source-reading bug.

```rust,no_run
use std::sync::Arc;
use arrow_array::{Array, BooleanArray, Int64Array};
use arrow_schema::Schema;
use arrow_select::filter::filter_record_batch;
use vgi_client::{BindSpec, Result, RpcError};
use vgi_client::testing::{scan_options, Comparison, FilterExpr, ScanResult, TestWorker};

fn check_filtered_scan(
    worker: &mut TestWorker, spec: &BindSpec, full_schema: &Schema,
) -> Result<()> {
    // WHERE id > 10 AND label IS NOT NULL; select only label.
    let filter = FilterExpr::compare(
        "id", Comparison::Gt, Arc::new(Int64Array::from(vec![10])),
    ).and(FilterExpr::is_not_null("label"));
    let options = scan_options(full_schema, &filter, Some(vec![1]))?;

    worker.check_pushdown(spec, &options, |full| {
        let mut expected = Vec::new();
        for batch in full.batches() {
            let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let labels = batch.column(1);
            // Ordinary Rust comparisons provide the reference. A SQL WHERE
            // clause retains only TRUE, so NULL comparisons do not pass.
            let keep = BooleanArray::from((0..batch.num_rows()).map(|row| {
                ids.is_valid(row) && ids.value(row) > 10 && labels.is_valid(row)
            }).collect::<Vec<_>>());
            expected.push(filter_record_batch(batch, &keep)
                .map_err(|error| RpcError::runtime_error(error.to_string()))?);
        }
        ScanResult::try_new(full.schema().clone(), expected)
    })
}
```

`FilterExpr` accepts typed Arrow arrays. A comparison literal contains exactly
one value with the column's exact type, preserving integer signedness, decimal
precision and scale, timestamp unit and timezone, and float width. It supports
`Eq`, `NotEq`, `Lt`, `LtEq`, `Gt`, and `GtEq`, `is_null`, `is_not_null`,
`in_values`, `not_in_values`, `.and()`, `.or()`, and `!filter`. The IN helpers take
an array of values, including NULLs if desired. The builder serializes required
VGI v2 predicates; it does not evaluate them.

`scan_options` takes the **full bind schema** and projection indices into it.
It automatically includes filter-only columns in the wire projection and lets
the client remove them from the final result. Expression column indices remain
indices into the full bind schema even when the output columns are reordered.

`check_pushdown` tests exact filter application, appropriate for SDK-managed or
worker-managed filtering. It rejects sampling, limits, ordering, split
redemption, and other scan options that would make a simple filtered full scan
an inappropriate reference. Test those operations with `scan` and explicit
expected results. This helper is not a test for an advisory-only scan that
intentionally returns extra rows.

## Cases worth keeping in a first worker

Keep these tests small enough that a reviewer can inspect the expected rows:

| Fixture or query | Failure it catches |
|---|---|
| Full scan with NULLs, strings, and duplicate rows | Silent conversions, missing rows, accidental deduplication |
| Int64 `9007199254740993`, UInt64 above `i64::MAX` | Conversion through `f64` or the wrong signed type |
| Fractional boundary, such as `latitude > 40.75` | Integer rounding during pruning |
| `x = 1 OR a IS NOT NULL` | Treating an unread column as SQL NULL |
| NULL tests, `NOT`, and `NOT IN` with a NULL member | Incorrect SQL three-valued logic |
| Select one column and filter another; reorder selected columns | Filter-only columns dropped too early or wrong column indices |
| No matches, multiple batches, and repeated identical rows | Empty-schema, drain, and multiplicity errors |
| Missing source object versus denied or malformed source | Failed reads converted into fabricated data |

For a pruning implementation, also test that every matching complete row survives
pruning for each supported subset of known columns. Full filtering against
DuckDB provides an independent SQL reference for more complex predicates. The
basic Rust kit does not claim SQL-engine conformance.

## Run locally and in CI

The [external-data example](../examples/external-data/) contains a complete
worker, small on-disk fixtures, and [subprocess tests](../examples/external-data/tests/worker.rs)
using this kit. Its tests provide runnable examples of independent Rust reference
selection, projection, source errors, and exact typed results:

```sh
cargo test -p vgi-external-data-worker
```

For your own package, CI needs only the regular integration-test command:

```sh
cargo test --all-targets
```

For SDK contributors, the kit has self-contained tests, including a real worker
served over local RPC bytes without a prebuilt executable:

```sh
cargo test -p vgi-client --no-default-features --features testing --lib testing::
cargo test -p vgi-client --no-default-features --features testing --test testing_kit
cargo test -p vgi-client --no-default-features --features testing --doc
```

For logging or a different transport, configure `VgiClient` yourself, then use
`TestWorker::attach(client, catalog, attach_options)`. Its log sink remains
installed. The [diagnostics guide](worker-diagnostics.md) shows execution-scoped
logs, structured scan counters, and typed source errors. `client_mut()` exposes
operations beyond the scan helpers. Source errors retain their `RpcError` codes
and messages, so assert on the relevant error as well as testing successful scans.

# Filtering and pruning external data

Use `FilterHandling::Sdk` for a worker that reads external files, objects, or
chunks. The SDK applies the exact pushed predicate before projecting the final
output. Your worker can avoid fetching data that cannot match, while leaving SQL
filter evaluation to the SDK.

```rust
use vgi::{FilterHandling, FunctionMetadata};

let metadata = FunctionMetadata {
    projection_pushdown: true,
    ..Default::default()
}.with_filter_handling(FilterHandling::Sdk);
```

The [external-data example](../examples/external-data/README.md) implements this
contract with local JSONL partitions, typed values, logs, and subprocess tests.

## Who filters the rows?

| Setting | Exact filtering owner | Producer responsibility |
| --- | --- | --- |
| `Disabled` | Engine | Return unfiltered data; do not accept pushed predicates. |
| `Sdk` | Rust SDK | Emit columns needed by the filter, including those absent from the requested projection. Emitting the full bind schema is simplest. |
| `Worker` | Worker implementation | Enforce every accepted required predicate exactly; reject unsupported evaluation rather than ignoring it. |

Pruning is an optimization under either enabled setting. It does not discharge
the obligation to filter surviving rows. Do not assume the engine will apply a
residual predicate merely because `filters_exactly_applied` is false.

The builder is additive: existing struct literals still compile. Legacy
`filter_pushdown: true, auto_apply_filters: true` means `Sdk`, and now advertises
the corresponding exact-filtering guarantee. A declaration with pushdown enabled
and neither SDK filtering nor an explicit worker guarantee is rejected at
discovery/init. Migrate it to `with_filter_handling(FilterHandling::Sdk)`, or use
`Worker` if the implementation already enforces every required predicate.
Filtering flags on a function with pushdown disabled are rejected as inconsistent.
Sending required pushed predicates directly to a `Disabled` function is also an
initialization error; they are never silently ignored.

For table-in-out functions, SDK filtering also covers `finish()` output, before
HTTP continuation batches are persisted. Exchange output is filtered before
projection. When rows are removed, the SDK filters explicit `parent_rows`
metadata with the same mask, or preserves identity correspondence for an
originally one-output-row-per-input-row batch. If an unannotated batch changes
cardinality before SDK filtering, it must provide explicit `parent_rows` through
`TableInOutOutput::emit_with`; otherwise removing rows fails rather than risking
incorrect correlation with the input. Zero surviving rows need no parent map.

## Prune with the columns you already know

`PushdownFilters::may_match(&known)` takes a `RecordBatch` containing any subset
of the full bind schema, in any order. Each row describes one aligned candidate:
for example, coordinates available before reading a multidimensional chunk.

```rust,no_run
# use arrow_array::{BooleanArray, RecordBatch};
# use vgi::{pushdown::PushdownFilters, Result};
fn candidates(filters: &PushdownFilters, coordinates: &RecordBatch) -> Result<BooleanArray> {
    filters.may_match(coordinates)
}
```

Its result is a non-null Boolean mask. `false` proves the candidate cannot match;
`true` means it might match and must still undergo exact filtering.

A missing column is **unknown**, not NULL. For `x = 1 OR payload IS NOT NULL`,
knowing only `x = 2` is insufficient to discard a row. Supplying a NULL payload
is different: neither side can then be true. `NOT`, `IN`, `NOT IN`, and Boolean
composition preserve SQL's three-valued logic.

Names and Arrow types must match the authoritative, unprojected bind schema.
Duplicate or unknown names and mismatched types are errors. Supply a complete
struct to evaluate its fields; dotted field names are not a substitute for the
parent's validity. A batch with no columns can retain its candidate count using
`RecordBatchOptions::with_row_count`.

This API describes concrete candidates, not arbitrary chunk min/max statistics.
To skip a whole partition using one candidate, its known values must hold for
every row in that partition, as the example's manifest coordinate does.

## Ask for a typed column domain

`PushdownFilters::column_domain("latitude")` returns a conservative set of values
that column can have in a matching row. `ColumnDomain::non_null()` is one of:

- `NonNullDomain::Empty`: no non-NULL values are possible.
- `NonNullDomain::All`: no useful restriction on non-NULL values.
- `NonNullDomain::Range { lower, upper }`: bounds using
  `std::ops::Bound<arrow_array::Scalar<ArrayRef>>`.

`may_be_null()` separately indicates whether SQL NULL could match. Consequently,
an empty non-NULL domain can still mean "only NULL". `is_empty()` means neither
NULL nor any non-NULL value can match.

Endpoints retain the Arrow logical type, including unsigned integers, decimals,
timestamp units/timezones, strings, and floating point. Dictionary bounds contain
decoded values, not keys. Use `Scalar::into_inner()` on a cloned endpoint to get
its one-element non-null Arrow array. Exclusive bounds stay exclusive: `x > 1.5`
does not become `x >= 2`, and `UInt64` values never pass through `Float64`.

Use `domain.may_contain(&values)` to test candidate Arrow values against the
domain. It returns a non-null conservative mask, rejects mismatched logical
types, and uses the same SQL comparison rules as filtering.

Domains use the DuckDB standard comparison profile, including SQL equality of
signed zeros and NaNs. DuckDB's [numeric-type documentation](https://duckdb.org/docs/current/sql/data_types/numeric)
describes NaN's ordering. Arrow's raw float comparison kernels have a different
total ordering; `may_contain` handles that distinction for you.

The first implementation handles direct comparisons, reversed operands, Boolean
composition, NULL tests, NULL-safe comparisons, and IN/NOT IN. It deliberately
widens unsupported expressions, arithmetic/cast inversion, nested-field domains,
and Arrow types without a supported scalar ordering. For example, `x IN (1, 3)`
may return `[1, 3]`, retaining `2` as an extra candidate. Domains are not exact
filters or selectivity estimates. The older integer-only `get_column_bounds`
helper remains available; new pruning code should use this API.

Both methods read the current filter state after each applied delta. Recompute
when filters change instead of caching a domain or candidate mask indefinitely.
Unsupported advisory predicates cannot partially restrict pruning when the
exact evaluator would ignore them on error.

## Test the contract

Use the [worker testing kit](testing-workers.md) to compare a pushed scan with an
unfiltered scan evaluated by an independent reference implementation. Include
nullable OR/NOT cases, exclusive floating bounds, large signed/unsigned integers,
duplicates, empty results, and a predicate on a column absent from the projection.
See [worker diagnostics](worker-diagnostics.md) for observing rows, chunks, bytes,
and source failures through the normal client connection.

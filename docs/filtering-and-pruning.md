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
helper is deprecated: it casts every literal to `i64`, truncating float and
decimal bounds. New pruning code should use this API; to read the raw typed
literal of each `column <op> literal` comparison, use
`PushdownFilters::constant_filters()` (or `get_column_constant_filters(col)`).

Both methods read the current filter state after each applied delta. Recompute
when filters change instead of caching a domain or candidate mask indefinitely.
Unsupported advisory predicates cannot partially restrict pruning when the
exact evaluator would ignore them on error.

## Filtered scan planning for catalog tables (protocol 3.2)

A catalog table whose scan redirects to another source (`read_parquet`, a
format branch, a VGI table function, a companion catalog table) can see the
query's predicates before it chooses what to scan. The normative description is
vgi-python's `docs/protocol/scan-planning.md`; this section covers the Rust API.

Declare the table `ScanPlanning::Filtered` and give it a `ScanBranchPlanner`.
The client then calls `catalog_table_scan_branches_get` once per scan, after it
has collected the scan's constant predicates, and sends them as
`pushdown_filters`. The planner receives them as `ScanPlanningFilters`
(decoded lazily against the table's declared columns) and returns the branches
to scan:

```rust,no_run
# use vgi::catalog::{CatBranch, CatTable};
# use vgi::scan_planning::{ScanBranchesPlan, ScanPlanRequest};
# use vgi::statistics::CatColStat;
# fn manifest() -> Vec<(String, Vec<CatColStat>)> { Vec::new() }
fn plan(request: &ScanPlanRequest<'_>) -> vgi::Result<ScanBranchesPlan> {
    let mut files = Vec::new();
    for (path, stats) in manifest() {
        // `may_match` is false only when the predicates exclude every row.
        let keep = match request.pushdown_filters {
            Some(filters) => filters.may_match(&stats)?,
            None => true, // bind-time planning: return everything
        };
        if keep {
            files.push(path);
        }
    }
    let branches = if files.is_empty() {
        Vec::new() // legal for a filtered table: the scan produces no rows
    } else {
        vec![CatBranch {
            format_name: Some("parquet".into()),
            format_locations: files,
            ..Default::default()
        }]
    };
    Ok(ScanBranchesPlan { branches, required_extensions: vec!["parquet".into()] })
}

# fn table(t: CatTable) -> CatTable {
t.with_filtered_scan_planning(plan)
# }
```

Rules:

- Every predicate is advisory. The client re-applies all of them, so return any
  superset of the branches and files that could hold a matching row, and never
  drop one that could.
- `pushdown_filters` is `None` when the client planned at bind time (an engine
  without a planning hook may always do this) or collected no usable predicate.
- A filtered table cannot inline its scan function (`inline_scan`); serving one
  fails. Its `TableInfo.scan_function` is sent as NULL.
- Zero branches mean "no rows" only for a filtered table; a `Bind` table that
  returns none is still rejected by the client.
- A table without a planner gets `ScanPlanRequest::default_plan()`: its declared
  `branches`, or one branch wrapping its scan function, exactly as in 3.1.

`ScanPlanningFilters::may_match` (and `PushdownFilters::may_match_statistics`)
evaluates the predicates against `CatColStat` min/max/null statistics using the
column domains described above. A bound that is NULL, not convertible to the
column's type without loss (a `1.5` float bound is never truncated to an
integer column), or of an unordered type counts as unknown, as does a column the
table does not declare. Use `StatValue::arrow` for typed bounds such as
`Date32` or `Timestamp`.

### Branch statistics

Any branch, on a filtered or a bind table, may carry `CatBranch::row_count` (a
cardinality estimate, never trusted as exact) and `CatBranch::column_statistics`
(conservative claims about every row the branch produces, keyed by the table's
declared column names). The client drops a branch whose statistics prove that
no row can satisfy the query, so wrong statistics silently lose rows: leave a
column out rather than guess. A client may also drop a branch whose
`branch_filter` contradicts the query's predicates.

On the client side, `VgiClient::table_scan_branches_with_filters` sends
`pushdown_filters`, accepts zero branches for a filtered table, and returns each
branch's decoded statistics in `CatalogScanBranches::column_statistics`.

## Test the contract

Use the [worker testing kit](testing-workers.md) to compare a pushed scan with an
unfiltered scan evaluated by an independent reference implementation. Include
nullable OR/NOT cases, exclusive floating bounds, large signed/unsigned integers,
duplicates, empty results, and a predicate on a column absent from the projection.
See [worker diagnostics](worker-diagnostics.md) for observing rows, chunks, bytes,
and source failures through the normal client connection.

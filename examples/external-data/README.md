# A small external-data worker

This worker exposes `read_observations(directory)` over local JSONL files. It
shows the complete path from source data to typed Arrow batches, including
partition pruning, SDK-managed filtering, projection, errors, diagnostics,
and subprocess tests. No remote service or DuckDB installation is needed for
the tests.

From the repository root:

```sh
cargo test -p vgi-external-data-worker
```

To query it from Haybarn, first build the worker:

```sh
cargo build -p vgi-external-data-worker
```

Use absolute paths for the executable and fixture directory in your SQL:

```sql
ATTACH 'observations' (
    TYPE vgi,
    LOCATION '/path/to/vgi-rust/target/debug/vgi-external-data-worker'
);

SELECT label, id
FROM observations.main.read_observations(
    '/path/to/vgi-rust/examples/external-data/fixtures/observations'
)
WHERE latitude > 40.75 AND latitude < 41 AND reading IS NOT NULL;
```

The output contains `south sensor` with ID `9007199254740993` and `duplicate`
with ID `3`. The first ID deliberately exceeds the exact integer range of a
double, so the example cannot silently round it through `f64`.

## Source and schema

The manifest lists partitions:

```json
[
  {"latitude": 40.8, "file": "south.jsonl"},
  {"latitude": 41.2, "file": "central.jsonl"},
  {"latitude": 42.0, "file": "north.jsonl"}
]
```

Every row in a file has the latitude supplied by its manifest entry. A JSONL
record supplies the other four columns:

```json
{"id":9007199254740993,"count":9223372036854775808,"label":"south sensor","reading":10.5}
```

| Column | Arrow type | Nullable |
| --- | --- | --- |
| latitude | Float64 | No |
| id | Int64 | No |
| count | UInt64 | No |
| label | Utf8 | Yes |
| reading | Float64 | Yes |

`Observation` deserializes integers directly into Rust `i64` and `u64`, and
the producer constructs matching Arrow arrays. Strings and SQL NULLs retain
their values. Invalid JSON or values outside those integer types fail the
scan; they never become NULL or fill data.

## Pruning and filtering have separate jobs

`ReadObservations::metadata()` chooses `FilterHandling::Sdk`. The worker may
skip partitions conservatively; the SDK is responsible for evaluating the
complete filter against the actual rows.

At scan initialization, `may_match()` receives a partial `RecordBatch` with
one known latitude per partition. The other columns are absent because they
have not been read. A `false` result proves that the corresponding partition
cannot contribute any matching rows, so its file is never opened.

For `latitude < 41`, the central and northern files can be skipped. For
`latitude < 41 OR reading IS NOT NULL`, they must remain: the unread readings
could satisfy the other side of the OR. Inserting NULL placeholders for
those readings would lose matching rows.

The producer always returns the complete bound schema. Even if a query
selects only `label`, the SDK still needs `reading` to evaluate
`WHERE reading IS NOT NULL`. The SDK filters first, then selects and reorders
the requested columns. Optional pruning does not replace this step.

The producer uses the initial filters for partition selection. It need not
implement further pruning when dynamic filters arrive; the SDK applies the
current filters to each returned batch.

## Errors and execution diagnostics

The source helper preserves machine-readable RPC codes: missing source files
are `NOT_FOUND`, denied reads are `PERMISSION_DENIED`, malformed source data
is `DATA_LOSS`, and an empty directory argument is `INVALID_ARGUMENT`.
Missing and unreadable partitions are errors rather than fabricated rows.
The example uses source aliases in error details so filesystem paths and
source contents do not accidentally appear in diagnostics.

`ExecutionDiagnostics` emits `scan.started` and `scan.finished` through
`OutputCollector`, with the function and execution ID attached. The final
event includes rows read before filtering, partitions read/skipped, and
bytes read (including the manifest). A client can receive these through
`VgiClient::set_worker_log_sink`; they travel with the scan instead of being
looked up through process-global state. The integration tests demonstrate
capturing them using the testing kit. A failed or cancelled scan does not
emit a successful `scan.finished` event.

## Adapting it

Start with [src/lib.rs](src/lib.rs) and replace the manifest/JSONL reader while
keeping the schema and exact typed conversion explicit. Keep the source
stable during a scan; this teaching example does not implement snapshots or
concurrent-file-change detection.

The producer keeps an open file cursor and emits at most two rows per batch
so the fixtures exercise streaming. `main.rs` serves native stdio directly,
and `resume_supported()` explicitly returns `false`. To support stateless
HTTP later, use a serializable partition/byte-offset cursor and implement
continuation before enabling that transport.

The [tests](tests/worker.rs) run the built executable over real RPC. They
compare pushed scans with independently filtered full scans, preserve
duplicate projected rows, check integer precision and strings, and verify
source failures and partition skipping. See the
[worker testing guide](../../docs/testing-workers.md) for using the kit in
your own crate.

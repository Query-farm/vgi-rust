# Observe a worker through its execution

Use `vgi::diagnostics::ExecutionDiagnostics` to send logs and scan counters on
the same RPC stream as query results. Subprocess stderr may be discarded;
a separate `last_scan()` function can run in another pooled worker process.
In-band events stay associated with the execution that produced them.

The [external-data worker](../examples/external-data/) shows this API in a small
file-backed scan. The helper requires no additional Cargo feature or service.

## Producer lifecycle and counters

Construct the context in `TableFunction::producer`:

```rust
use vgi::diagnostics::{ExecutionDiagnostics, ScanStats};

// `params` is the ProcessParams supplied to producer().
let diagnostics = ExecutionDiagnostics::new("read_observations", &params.execution_id);
let stats = ScanStats::default();
// Store both, plus a `started` flag, in the returned TableProducer.
```

In `TableProducer::next_batch`, call `self.diagnostics.scan_started(out)` once
at the beginning of the scan. As source work completes, update `self.stats`.
At successful end of input, call
`self.diagnostics.scan_finished(out, &self.stats)` and return `Ok(None)`.
Return data batches normally; the dispatcher applies SDK filtering and
projection and emits them for you.

Every helper event includes `function`, `execution_id`, and `event` extras.
The execution ID is URL-safe base64 without padding, including for non-UTF-8
IDs. Encode only `params.execution_id`; encoding is not a way to hide bind
state, tokens, or credentials.

| Event | Meaning |
| --- | --- |
| `scan.started` | This producer started source work. |
| `worker.log` | An application message sent with `diagnostics.log(out, level, message)`. |
| `scan.finished` | This producer reached successful EOF, with cumulative source counters. |

`scan.finished` carries these extras as decimal strings. The message also
includes the counters for clients that display only text.

| Counter | Count |
| --- | --- |
| `rows_read` | Rows decoded from the source, before SDK filtering. |
| `chunks_read` | Source read units opened/read, such as files or Zarr chunks. |
| `chunks_skipped` | Read units avoided by conservative pruning. |
| `bytes_read` | Source bytes actually read, not decoded Arrow memory or an estimate. |

These are producer-local cumulative values. With parallel producers, the same
execution ID can appear in multiple finish events; they are not each
query-wide totals. The helper neither aggregates events nor guarantees
exactly-once delivery across retries. Use your own additional fields with
`OutputCollector::client_log_with` when an application needs partition/attempt
identity. Avoid putting source paths, presigned URLs, SQL text, or credentials
in client-visible messages.

For HTTP continuation, keep counters and the `started` flag in the producer's
resume state, alongside its source cursor. `ScanStats` implements serde's
`Serialize` and `Deserialize`, so it works with `vgi::resume_fields!`. Recreate
the diagnostics context from `ProcessParams`; don't send another start event
every time the producer is reconstructed.

There is deliberately no finish event on `Drop`. Cancellation, an early
`LIMIT`, a disconnected client, and a failed scan may all end without EOF.
A successful producer finish also does not imply that the entire SQL query or
transaction succeeded. Events queued in an `OutputCollector` may be discarded
when that turn returns an error. Return a typed error carrying its own context
instead of relying on a last log line.

`on_bind` and `producer` do not receive an `OutputCollector`; bind also has no
execution ID yet. Use structured returned errors there, and start scan events
in `next_batch`. The helper does not invent a new out-of-band channel.

## Preserve source failures

The source adapter must distinguish an absent object from a failed read.
A source format may define an absent chunk as fill-valued data, but a permission
failure or outage must fail the query rather than fabricate rows.

```rust
use vgi::diagnostics::{source_error, source_http_error, source_io_error};
use vgi::vgi_rpc::error_model::Code;

// In on_bind (no execution context), or producer initialization:
let file = std::fs::File::open(path)
    .map_err(|error| source_io_error("open", "observations file", &error))?;

// Once execution context is available:
let denied = self.diagnostics.annotate_error(
    source_http_error("read", "weather store", 403),
);
// Return Err(denied), never Ok(None) or replacement fill values.

// A format decoder can supply a canonical classification explicitly:
let corrupt = source_error(Code::DataLoss, "decode", "observation record");
```

The operation and source arguments are static labels, not the original URL or
path. I/O helpers inspect `ErrorKind` without copying the original message;
HTTP helpers inspect the status without copying URLs, bodies, or headers.
`annotate_error` adds execution/function context and preserves codes and
existing detail types. It is not a sanitizer for arbitrary error text.

| Failure | Canonical code |
| --- | --- |
| Missing file / HTTP 404 | `NotFound` |
| Permission denied / HTTP 403 | `PermissionDenied` |
| HTTP 401 | `Unauthenticated` |
| Connection reset/refused / HTTP 503 | `Unavailable` |
| Timeout / HTTP 504 | `DeadlineExceeded` |
| HTTP 429 | `ResourceExhausted` |
| Invalid source data / premature EOF | `DataLoss` |

All helpers use the stable reason `source_access_failed` and an `ErrorInfo`
detail with `operation` and `source`; HTTP errors also include `http_status`.
I/O errors retain the `io_kind` and, when available, numeric `os_error`.
Clients should branch on `error.code()` and can inspect `error.error_info()`.
Source-specific APIs with different status semantics can call `source_error`
directly. Whether and when to retry remains a source/application decision.

## Capture events in a Rust client

Built-in `VgiClient` transports forward worker messages to the `log` facade
under the target `vgi::worker`, including the JSON extras. Install your usual
logger to see them. To consume structured events directly, install a
`WorkerLogSink` before starting the scan:

```rust
use std::sync::{Arc, Mutex};
use vgi_client::{VgiClient, WorkerLogSink};

let mut client = VgiClient::connect_subprocess(&[
    "./target/debug/vgi-external-data-worker",
])?;
let captured = Arc::new(Mutex::new(Vec::new()));
let sink_events = Arc::clone(&captured);
let sink: WorkerLogSink = Arc::new(move |message| {
    // Keep callbacks short: they run while the client consumes the RPC stream.
    sink_events.lock().unwrap().push(message);
});
assert!(client.set_worker_log_sink(sink));
// Attach, bind, and drain the scan normally. Events arrive as it is read.
// Each LogMessage retains its level, message, and Vec<(String, String)> extras.
```

`set_worker_log_sink` returns `false` for a custom transport without a built-in
log router. Set sinks on the connections executing the work, including each
parallel connection. Terminal RPC exceptions are returned as `Err`; they are
not ordinary log callbacks. Result batches do not include the protocol's
zero-row diagnostic batches.

The [testing kit](testing-workers.md) can run fixture scans using a client
configured with this sink. A transport regression test in
[`execution_diagnostics.rs`](../vgi-client/tests/execution_diagnostics.rs)
checks the entire path over a real TCP connection: source counters, separate
execution IDs, clean result batches, and typed permission errors with context.

## View messages in Haybarn / DuckDB

The VGI extension consumes in-band worker logs through DuckDB's logging system:

```sql
CALL enable_logging(level := 'debug');
-- Run the query against the worker.
SELECT message
FROM duckdb_logs()
WHERE type = 'VGI' AND message LIKE '%source scan%';
```

In the extension implementation inspected for this change,
`HandleBatchLogMessage` retains worker messages and its own execution context
but does not forward arbitrary worker `log_extra` keys to `duckdb_logs`.
The readable finish message therefore includes the counters; use the Rust
`WorkerLogSink` for structured fields. The extension's execution ID is hex,
whereas this helper's extra is base64url; decode before comparing encodings.
No extension build or SQL UI change is required for the readable messages.

<div align="center">
  <img src="https://raw.githubusercontent.com/Query-farm/vgi-rust/main/docs/vgi-logo.png" alt="Vector Gateway Interface" width="320">
</div>

# vgi-client

A Rust client for the **VGI** protocol: attach a remote catalog, discover what
it holds, and call its functions — over any VGI transport.

```rust
use vgi_client::{AttachOptions, FunctionKind, VgiClient};

let mut client = VgiClient::connect_subprocess(&["my-worker"])?;

let cat = client.attach("my_catalog", AttachOptions::default())?;
for schema in client.schemas(&cat)? {
    for table in client.tables(&cat, &schema.name)? {
        println!("{}.{}", schema.name, table.name);
    }
}
client.detach(&cat)?;
```

## Transports

| Transport | Constructor | Feature |
|---|---|:-:|
| subprocess / stdio | `VgiClient::connect_subprocess` | — |
| TCP | `VgiClient::connect_tcp` | — |
| HTTP | `VgiClient::connect_http` | `http` (default) |
| AF_UNIX | `VgiClient::connect_unix` | `unix` |

All of them come from [`vgi-rpc-client`](https://crates.io/crates/vgi-rpc-client)
behind the `UnaryTransport` trait, so the protocol layer holds no assumptions
about I/O.

## Worker logs

Built-in subprocess, TCP, HTTP, and AF_UNIX clients decode in-band VGI log
batches. By default they forward each message and its structured extras to the
Rust `log` facade under target `vgi::worker`. An embedding host can install a
structured `WorkerLogSink` for a checked-out client with
`VgiClient::set_worker_log_sink`; `WorkerPool` restores the default sink before
that connection is reused, preventing diagnostics from leaking between host
sessions. Authenticated HTTP rebuilds retain the active checkout sink.

An `EXCEPTION` batch remains an RPC error and is not downgraded to a log event.
Custom `VgiTransport` implementations own their diagnostic channel, so
`set_worker_log_sink` returns `false` for those clients.

## Design

- **`VgiClient`** owns one connection and carries the method surface. A worker is
  single-threaded per connection, so parallelism means opening several — which is
  also how a scan fans out across the worker's advertised `max_workers`.
- **`AttachedCatalog`** is a value type holding the worker's session token, so
  catalog calls take `&AttachedCatalog` instead of borrowing the client. The
  handle really is just bytes; making it borrow would buy nothing and cost
  lifetimes.
- **Wire types** come from [`vgi-protocol`](https://crates.io/crates/vgi-protocol)
  and are shared with the worker framework, so Rust has exactly one definition of
  the protocol.

## Back-pressure (429 / Retry-After)

`max_workers` is a normative cap on redemption concurrency, and over HTTP a
server enforces it with `429` + `Retry-After`. `VgiClient::connect_http` wraps
the transport in a `RetryTransport`, which applies a `RetryPolicy` — bounded by
both an attempt count and a total-time budget, honouring `Retry-After` in both
its delta-seconds and HTTP-date forms, and adding **full jitter** so N split
readers released by one planner do not re-form the herd the cap exists to break
up. A breached cap fails with an error naming the endpoint and the attempts;
enumeration is never silently truncated.

Two limits are worth knowing:

- **Status fidelity.** `vgi-rpc-client` does not report the HTTP status of a
  data call to its caller — it inspects `415`/`413`/`401` and hands every other
  body to the Arrow decoder, so an unhandled `429` surfaces as `empty IPC
  stream (no schema)`. `retry::classify_error` therefore reads the statuses that
  *do* survive (the external-location fetch and upload-URL PUT format `HTTP
  <status>` into their messages, and a transient auth failure carries a parsed
  `Retry-After`) and treats anything unplaceable as fatal. Classifying every
  data-path status needs the transport crate to surface it.
- **Stream opens.** `init` returns a session that borrows the client, so it
  cannot be retried inside the transport (the retry loop would re-borrow across
  iterations). A caller that owns its client retries it with
  `RetryPolicy::run`.

## Split planning

`VgiClient::plan` follows paginated split enumeration to completion and returns
the full protocol 1.4 plan: redemption context, row and byte estimates,
partition bounds, column statistics, locality, partition transforms, within-split
ordering, cache age, and streaming positions. Plan-level facts are taken from
the first page; later pages contribute splits and cursors only.

Redeem a packed token group with `ScanPlan::redemption_options`. It echoes the
plan's `execution_id` and `init_opaque_data` along with the tokens, avoiding a
subtle cross-process state mismatch that is easy to create by assembling
`ScanOptions` manually.

## Persistent result storage

The opt-in `disk-cache` feature provides bounded storage for complete producer
results. A host supplies a durable `DiskCacheOptions::root`, byte and
entry bounds, and an Arrow IPC codec (`Zstd`, `Lz4`, or `None`). The root is
application state; it must not be a query engine's temporary spill directory.

`DiskCache::begin_capture` takes the result schema and physical partition count
up front, so empty results and empty partitions remain typed. Each
`DiskCapture::push_batch` writes to that partition's Arrow IPC file, and
`DiskCache::commit` publishes the multipart result only after every file and
the manifest are durable. Dropping or aborting an unfinished capture removes
its temporary generation. `lookup` returns only fresh entries after validating
their manifest, schema, sizes, and hashes. A revalidatable result with an ETag
or Last-Modified value may be persisted while immediately stale;
`lookup_for_revalidation_expected_schema` validates and leases those bytes for
a conditional request. `revalidate_freshness` slides only the observed durable
generation after `not_modified` and atomically replaces its validator and grace
policy, while `remove_hit` conditionally revokes that same generation. A
positive retained stale-if-error window is available to callers without
exposing validator values in diagnostics. Transaction-scoped results are
deliberately not persisted.

Objects use ref-last atomic publication, HMAC-obscured paths, private
permissions, and cross-process operation, capture, and replay leases. Byte and
entry eviction, scoped flush, and reap are part of the same API. This is loose
producer-result storage, not correlated/exchange memo packing.

The durable root must be on a local filesystem with Unix advisory-lock and
atomic-rename semantics; network filesystems are not a supported cache root.
LRU touches are intentionally process-local to avoid a durable metadata write
on every hit. After restart or across processes, eviction falls back to the
generation's publication-metadata time, so the LRU order is approximate while byte/count
bounds and entry integrity remain enforced cross-process.

Entry listings and occupancy counters describe committed references, not
temporary captures or old generations retained by an active replay lease.
Those leased orphans still count against new admission bounds and are removed
by reap after the final reader releases them; they are intentionally omitted
from result diagnostics because they are not lookup-visible entries.
The bounds cover encoded Arrow payload admission, not filesystem usage:
metadata is excluded and each concurrent in-progress capture has its own byte
cap. Processes sharing a root should therefore use the same options, which are
host policy and are not persisted in the cache format.

All capture, lookup, replay, flush, and reap filesystem work is blocking. Async
engines must run it on a blocking executor such as Tokio's `spawn_blocking`.

## Blocking

Every call blocks, matching the underlying transport client and both other VGI
clients (Python and Java). Bridge from async with `spawn_blocking`; the
`UnaryTransport` trait is where a native async driver would be added without
disturbing the protocol layer.

`spawn_blocking` is not a style preference on the HTTP transport, which is
`reqwest::blocking` underneath. Measured (`tests/http_under_tokio.rs`): inside
`spawn_blocking` a whole HTTP scan runs clean, while constructing the client
directly on a tokio worker thread panics before the first request — *"Cannot
drop a runtime in a context where blocking is not allowed"*. Same reason the
OAuth path uses `ureq`.

## Status

Catalog attach and discovery are implemented and covered end to end against a
real worker. Function invocation — producer scans, exchange mode, aggregates —
is in progress.

## License

See `LICENSE` at the repository root.

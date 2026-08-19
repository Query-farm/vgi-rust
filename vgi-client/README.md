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

## Blocking

Every call blocks, matching the underlying transport client and both other VGI
clients (Python and Java). Bridge from async with `spawn_blocking`; the
`UnaryTransport` trait is where a native async driver would be added without
disturbing the protocol layer.

## Status

Catalog attach and discovery are implemented and covered end to end against a
real worker. Function invocation — producer scans, exchange mode, aggregates —
is in progress.

## License

See `LICENSE` at the repository root.

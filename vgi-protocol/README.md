<div align="center">
  <img src="https://raw.githubusercontent.com/Query-farm/vgi-rust/main/docs/vgi-logo.png" alt="Vector Gateway Interface" width="320">
</div>

# vgi-protocol

The VGI wire protocol, independent of which side of the wire you are on.

This crate holds the types and codecs that a VGI **worker** and a VGI **client**
both need: the request/response DTOs, the dictionary-encoded enum payloads, the
Arrow `RecordBatch` codec that carries them, and the IPC framing helpers.

It exists so a client does not have to depend on the worker framework
([`vgi`](https://crates.io/crates/vgi)) just to speak the protocol.

## Direction-agnostic by construction

Every DTO derives `VgiArrow`, and the codec goes both ways — `wire::to_batch`
encodes, `wire::from_batch` decodes. The worker and the client each use both, so
there is no "request side" and "response side" split to keep in sync.

```rust
use vgi_protocol::protocol::dtos::TableInfo;
use vgi_protocol::wire;

// A client decodes what a worker encoded, using the same types.
let info: TableInfo = wire::from_batch(&batch)?;
```

`wire::params_schema_for(method)` maps an RPC method name to its parameter
schema.

## Compatibility

These types must stay byte-compatible with the canonical Python
`vgi/protocol.py` — field names, Arrow types, and nullability all follow that
wire schema. Changes here are protocol changes.

## Deliberately minimal

No feature flags, no HTTP stack, no async runtime, no storage backend. The only
dependencies are `vgi-rpc` and the three Arrow crates it needs. That keeps the
crate cheap to depend on and keeps it building for wasm targets.

RPC method *registration* — wiring these types onto an `RpcServer` — is a
worker-side concern and lives in the `vgi` crate as `vgi::protocol::register`.

## License

See `LICENSE` at the repository root.

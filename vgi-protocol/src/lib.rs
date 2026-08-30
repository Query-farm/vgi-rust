// Copyright 2025, 2026 Query Farm LLC - https://query.farm

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/Query-farm/vgi-rust/main/docs/vgi-logo.png"
)]

//! The VGI wire protocol, independent of which side of the wire you are on.
//!
//! This crate holds the types and codecs that a VGI **worker** and a VGI
//! **client** both need: the request/response DTOs, the dictionary-encoded enum
//! payloads, the Arrow `RecordBatch` codec that carries them, and the IPC
//! framing helpers.
//!
//! It exists so a client does not have to depend on the worker framework
//! ([`vgi`](https://docs.rs/vgi)) just to speak the protocol. Everything here is
//! direction-agnostic — [`wire::to_batch`] encodes and [`wire::from_batch`]
//! decodes the same types, so the worker and the client each use both.
//!
//! These types must stay byte-compatible with the canonical Python
//! `vgi/protocol.py`. Field names, Arrow types, and nullability all follow that
//! wire schema.
//!
//! # Deliberately minimal
//!
//! No feature flags, no HTTP stack, no async runtime, no storage backend. That
//! keeps the crate cheap to depend on and keeps it building for wasm targets.
//!
//! # Layout
//!
//! - [`protocol::dtos`] — the request/response structs, each deriving `VgiArrow`
//! - [`protocol::enums`] — dictionary-encoded enum payloads
//! - [`wire`] — `to_batch` / `from_batch` plus [`wire::params_schema_for`]
//! - [`ipc`] — Arrow IPC stream read/write helpers

/// VGI wire protocol version advertised to the C++ extension.
///
/// Enforced as an exact major+minor match at the dispatch boundary (carried in
/// `vgi_rpc.protocol_version` custom metadata), so this must track
/// `VgiProtocol.protocol_version` in vgi-python. 1.1.0 added `schema_name` to
/// `BindRequest`; 1.2.0 adds it to the 15 unary requests that re-resolve the
/// function by name, so a name declared in two schemas cannot mis-route at
/// runtime after binding correctly. 1.3.0 adds `global_functions` and
/// `global_function_prefix` to `CatalogAttachResult` — functions a worker asks
/// the client to publish into its global namespace. 1.5.0 adds `schema_name`
/// to `ScanFunctionResult`/`ScanBranch` — the worker's own authoritative
/// schema for the function it just resolved, so a client no longer has to
/// guess (table's own schema, then `default_schema`) when the same function
/// name is registered in more than one schema.
pub const VGI_PROTOCOL_VERSION: &str = "1.5.0";
/// RPC protocol name; must match the Python `VgiProtocol`.
pub const VGI_PROTOCOL_NAME: &str = "VgiProtocol";

pub mod cache_control;
pub mod generated;
pub mod ipc;
pub mod protocol;
pub mod wire;

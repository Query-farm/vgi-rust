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

pub mod cache_control;
pub mod generated;
pub mod ipc;
pub mod protocol;
pub mod wire;

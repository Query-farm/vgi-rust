// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! VGI wire protocol: DTOs and enum payloads.
//!
//! These types must be byte-compatible with the canonical Python
//! `vgi/protocol.py`. Field names, Arrow types, and nullability all follow that
//! canonical wire schema.
//!
//! RPC method *registration* (wiring these onto an `RpcServer`) is a worker-side
//! concern and lives in the `vgi` crate as `vgi::protocol::register`.

pub mod dtos;
pub mod enums;

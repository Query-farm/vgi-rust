// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! VGI wire protocol: DTOs, enum payloads, and RPC method registration.
//!
//! The C++ DuckDB extension is the client; these types and methods must be
//! byte-compatible with the canonical Python `vgi/protocol.py`. Field names,
//! Arrow types, and nullability mirror the Go port (`vgi-go/vgi/generated/`).

pub mod dtos;
pub mod enums;
pub mod register;

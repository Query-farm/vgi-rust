// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! VGI wire protocol: DTOs, enum payloads, and RPC method registration.
//!
//! The DTOs and enums themselves now live in the direction-agnostic
//! [`vgi-protocol`](https://docs.rs/vgi-protocol) crate — a client speaks the
//! same types, so they must not depend on this worker framework. They are
//! re-exported here at their original paths.
//!
//! [`register`] stays worker-side: it wires those types onto an `RpcServer` and
//! is meaningless to a client.

pub use vgi_protocol::protocol::{dtos, enums};

pub mod register;

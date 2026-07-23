// Copyright 2025, 2026 Query Farm LLC - https://query.farm

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/Query-farm/vgi-rust/main/docs/vgi-logo.png"
)]

//! Build native, single-binary DuckDB extensions in Rust — no C++, no linking
//! against DuckDB.
//!
//! `vgi` is the Rust SDK for writing **VGI (Vector Gateway Interface) workers**:
//! the worker side of [Query Farm](https://query.farm)'s DuckDB
//! "Hyperfederation" extension. A *worker* is an ordinary Rust binary that
//! DuckDB launches and talks to over Apache Arrow IPC. It exposes scalar /
//! table / aggregate functions and whole catalogs (schemas, tables, views) that
//! behave like native DuckDB objects — with no compiled C++ extension and no
//! version coupling to a specific DuckDB build.
//!
//! Workers built with this crate are byte-for-byte wire-compatible with the
//! canonical Python implementation, so a Rust worker drops in behind the
//! same `ATTACH … (TYPE vgi)`. It is built on the
//! [`vgi-rpc`](https://docs.rs/vgi-rpc) crate (wire protocol, RPC server,
//! transports), uses stock `arrow-rs` 59.x, and has an MSRV of 1.97.
//!
//! # Your first worker
//!
//! A worker is a `main()` that registers functions on a [`Worker`] and calls
//! [`Worker::run`]. This one exposes `upper_case(varchar) -> varchar`:
//!
//! ```no_run
//! # #![allow(clippy::needless_doctest_main)]
//! use std::sync::Arc;
//!
//! use arrow_array::{cast::AsArray, ArrayRef, RecordBatch, StringArray};
//! use arrow_schema::DataType;
//! use vgi::{ArgSpec, FunctionMetadata, ProcessParams, ScalarFunction, Worker};
//! use vgi_rpc::{Result, RpcError};
//!
//! /// `upper_case(s)` — uppercase a string column.
//! struct UpperCase;
//!
//! impl ScalarFunction for UpperCase {
//!     fn name(&self) -> &str {
//!         "upper_case"
//!     }
//!
//!     fn metadata(&self) -> FunctionMetadata {
//!         FunctionMetadata {
//!             description: "Convert string values to uppercase".into(),
//!             return_type: Some(DataType::Utf8),
//!             ..Default::default()
//!         }
//!     }
//!
//!     fn argument_specs(&self) -> Vec<ArgSpec> {
//!         vec![ArgSpec::column("value", 0, "varchar", "String to uppercase")]
//!     }
//!
//!     fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
//!         let col = batch.column(0).as_string::<i32>();
//!         let upper: StringArray = col.iter().map(|v| v.map(str::to_uppercase)).collect();
//!         let out: ArrayRef = Arc::new(upper);
//!         RecordBatch::try_new(params.output_schema.clone(), vec![out])
//!             .map_err(|e| RpcError::runtime_error(e.to_string()))
//!     }
//! }
//!
//! fn main() {
//!     let mut worker = Worker::new();
//!     worker.register_scalar(UpperCase);
//!     worker.run(); // serves stdio (default), --unix <path>, or --http
//! }
//! ```
//!
//! Build it (`cargo build --release`), then call it from a DuckDB engine that
//! has the `vgi` extension — Query Farm's [Haybarn] distribution ships it and
//! starts with `uvx haybarn-cli`:
//!
//! ```sql
//! ATTACH 'demo' (TYPE vgi, LOCATION './target/release/my-worker');
//! SELECT demo.main.upper_case(name) FROM (VALUES ('alice'), ('bob')) t(name);
//! -- ALICE
//! -- BOB
//! ```
//!
//! [Haybarn]: https://github.com/Query-farm-haybarn/haybarn
//!
//! # The function model
//!
//! Implement one trait per function kind and register it on the [`Worker`]:
//!
//! | Kind         | Trait                                     | Use case                                  |
//! |--------------|-------------------------------------------|-------------------------------------------|
//! | Scalar       | [`ScalarFunction`]                        | Per-row transforms (1 row in → 1 row out) |
//! | Table        | [`table_function::TableFunction`]         | Generate / scan rows (no row input)       |
//! | Table-in-out | [`table_in_out::TableInOutFunction`]      | Streaming row transforms (N in → M out)   |
//! | Buffering    | [`buffering::TableBufferingFunction`]     | Sink → combine → source (aggregate-emit)  |
//! | Aggregate    | [`aggregate::AggregateFunction`]          | Grouped / window / streaming aggregates   |
//!
//! Every trait shares the same bind/process vocabulary: [`ArgSpec`] declares the
//! arguments, [`FunctionMetadata`] declares optimizer-facing properties,
//! [`BindParams`] / [`BindResponse`] resolve the output schema at bind time, and
//! [`ProcessParams`] carries per-call context (settings, secrets, pushdown
//! hints) into the work method.
//!
//! # Beyond functions
//!
//! [`Worker::set_catalog`] exposes a full catalog — schemas, function-backed
//! tables, views, and macros — with constraints, column statistics, time travel
//! (`AT`), and secondary catalogs attachable by name (see [`catalog`]).
//! Projection and filter pushdown, `ORDER BY` / `TABLESAMPLE` hints, custom
//! settings, secrets, and bearer auth are handled for you.
//!
//! # Transports
//!
//! [`Worker::run`] selects a transport from `argv`: **stdio** (default),
//! **Unix socket** (`--unix <path>`, the launcher contract), or **HTTP**
//! (`--http`, Arrow-IPC over HTTP with AEAD-sealed stateless stream tokens and
//! optional bearer auth). You rarely pass these yourself — DuckDB supplies the
//! right flags when it launches your worker.

pub mod aggregate;
pub mod arguments;
pub mod buffering;
pub mod cache_control;
pub mod catalog;
pub mod copy_from;
pub mod copy_to;
#[cfg(feature = "transport-http")]
pub mod describe;
pub mod dispatch;
pub mod function;
pub mod ipc;
pub mod numeric;
pub mod overload;
pub mod partition;
pub mod protocol;
pub mod pushdown;
pub mod secrets;
pub mod settings;
pub mod statistics;
pub mod storage;
pub mod table_function;
pub mod table_in_out;
pub mod transport;
pub mod wasm_worker;
pub mod wire;
pub mod worker;

#[cfg(test)]
mod http_continuation_tests;

pub use dispatch::FunctionScope;
pub use function::{
    ArgSpec, BindParams, BindResponse, FunctionExample, FunctionMetadata, ProcessParams,
    ScalarFunction,
};
pub use worker::Worker;

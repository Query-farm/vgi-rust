//! VGI (Vector Gateway Interface) worker SDK — Rust port.
//!
//! Builds on the `vgi-rpc` crate (wire protocol, RPC server, transports) to
//! provide the VGI function model (scalar / table / table-in-out / aggregate
//! / buffering), the catalog layer, and the worker dispatcher that the
//! DuckDB C++ VGI extension drives.
//!
//! Canonical reference for behavior: Python `~/Development/vgi-python/vgi/`.
//! Closest structural parallel: Go `~/Development/vgi-go/vgi/`.

pub mod aggregate;
pub mod arguments;
pub mod buffering;
pub mod catalog;
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
pub mod table_function;
pub mod table_in_out;
pub mod transport;
pub mod wire;
pub mod worker;

pub use function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, ScalarFunction};
pub use worker::Worker;

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/Query-farm/vgi-rust/main/docs/vgi-logo.png"
)]
#![warn(missing_docs)]

//! A client for the VGI protocol.
//!
//! Attach a remote catalog, discover what it holds, and call its functions —
//! from Rust, over any VGI transport.
//!
//! ```no_run
//! use vgi_client::{AttachOptions, FunctionKind, VgiClient};
//!
//! # fn main() -> vgi_client::Result<()> {
//! let mut client = VgiClient::connect_subprocess(&["my-worker"])?;
//!
//! for catalog in client.catalogs()? {
//!     println!("{}", catalog.name);
//! }
//!
//! let cat = client.attach("my_catalog", AttachOptions::default())?;
//! for schema in client.schemas(&cat)? {
//!     for table in client.tables(&cat, &schema.name)? {
//!         println!("{}.{}", schema.name, table.name);
//!     }
//!     for f in client.functions(&cat, &schema.name, FunctionKind::Table)? {
//!         println!("{}()", f.name);
//!     }
//! }
//! client.detach(&cat)?;
//! # Ok(())
//! # }
//! ```
//!
//! # How this fits together
//!
//! - [`VgiClient`] owns one connection and carries the whole method surface.
//! - [`AttachedCatalog`] is a value type holding the worker's session token, so
//!   catalog calls take `&AttachedCatalog` rather than borrowing the client.
//! - The wire types come from
//!   [`vgi-protocol`](https://docs.rs/vgi-protocol) and are shared with the
//!   worker framework, so there is exactly one definition of the protocol in
//!   Rust.
//! - Transports come from
//!   [`vgi-rpc-client`](https://docs.rs/vgi-rpc-client) behind the
//!   [`VgiTransport`] trait, which is what keeps the protocol layer free of
//!   any particular I/O model.
//!
//! # Blocking
//!
//! Every call blocks. That matches the underlying `vgi-rpc-client` and both
//! other VGI clients (Python and Java). An async consumer should bridge with
//! `spawn_blocking`; the [`VgiTransport`] seam is where a native async driver
//! would be added.

pub mod aggregate;
pub mod args;
pub mod auth;
pub mod cache;
pub mod catalog;
pub mod client;
pub mod exchange;
#[cfg(feature = "launcher")]
pub mod launcher;
pub mod location;
pub mod pool;
pub mod scan;
pub mod transport;
pub mod wire_call;

pub use aggregate::{with_group_ids, BoundAggregate, GROUP_COLUMN_NAME};
pub use args::{ArgValue, Arguments};
pub use auth::{AuthReason, Unauthorized};
pub use cache::{CacheKey, CacheLimits, CacheStats, CachedEntry, Ineligible, ResultCache};
pub use catalog::{At, AttachOptions, AttachedCatalog, FunctionKind, MacroKind};
pub use client::VgiClient;
pub use location::VgiLocation;
pub use pool::{PoolConfig, PoolStats, PooledClient, WorkerPool};
pub use scan::{
    BindSpec, BoundFunction, FunctionType, NullOrder, OrderBy, Sample, Scan, ScanOptions,
    SortDirection,
};
pub use transport::{ExchangeStream, ProducerStream, StreamTransport, VgiTransport};

#[cfg(feature = "http")]
pub use transport::HttpTransport;

/// The error type every call returns, re-exported from `vgi-rpc`.
pub use vgi_rpc::errors::{Result, RpcError};

/// The wire types a caller receives.
pub use vgi_protocol::protocol::dtos;

/// Cache directives a worker may advertise on its result.
pub use vgi_protocol::cache_control::CacheControl;

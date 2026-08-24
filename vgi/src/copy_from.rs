// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Custom `COPY ... FROM` format readers.
//!
//! A [`CopyFromFunction`] lets a VGI catalog act as a remote file-format
//! reader: the user runs `COPY target FROM 'path' (FORMAT <name>, opt val, ...)`
//! and the worker parses the source and streams Arrow batches that DuckDB
//! inserts into the local `target` table.
//!
//! Mechanically a `CopyFromFunction` is an ordinary producer-mode table
//! function (it reuses the whole table-function bind/init/scan path). What makes
//! it a COPY format is twofold:
//!
//! * it sets [`CopyFromFunction::format`] to the SQL `FORMAT` identifier, and
//! * the catalog advertises it via `catalog_copy_from_formats`, so the VGI
//!   DuckDB extension registers a DuckDB `CopyFunction` for it.
//!
//! [`CopyFromFunction::read`] materializes every batch before the scan yields a
//! row, which is the right shape for a source that fits in memory. A reader that
//! can decode incrementally should implement
//! [`CopyFromFunction::read_stream`] instead and hand back a producer: the scan
//! path is identical (and unchanged on the wire — it already pulls one batch at
//! a time), but peak memory is one batch rather than the whole source.
//!
//! The COPY statement's file path and the target table's schema arrive on the
//! bind through [`crate::protocol::dtos::CopyFromContext`]
//! (`params.copy_from`). The COPY options arrive as the function's normal
//! `Arg`-annotated arguments — declare them in
//! [`CopyFromFunction::argument_specs`] exactly like any other function.
//!
//! Register with [`crate::Worker::register_copy_from`]; mirrors the Python
//! `vgi.copy_from_function.CopyFromFunction`. Scope: **`FROM` only** (no
//! `COPY ... TO`).

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use vgi_rpc::{OutputCollector, Result, RpcError};

use crate::arguments::Arguments;
use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use crate::table_function::{TableFunction, TableProducer};

/// Context handed to [`CopyFromFunction::read`].
pub struct CopyFromReadContext<'a> {
    /// Source path from the `COPY ... FROM 'path'` statement.
    pub path: &'a str,
    /// Parsed COPY options (the function's named arguments).
    pub options: &'a Arguments,
    /// The COPY target's schema. Every emitted batch must have this exact
    /// schema (names + types, in order) — DuckDB inserts no cast.
    pub expected_schema: &'a SchemaRef,
    /// Full process parameters (settings, secrets, storage, auth).
    pub params: &'a ProcessParams,
}

/// A custom `COPY ... FROM` format reader.
///
/// Implement [`format`](Self::format), [`handler_name`](Self::handler_name),
/// [`argument_specs`](Self::argument_specs) (the COPY options — the source
/// `file_path` is supplied by the COPY statement, **not** an option), and
/// [`read`](Self::read) (parse the source and return Arrow batches matching the
/// target schema). Register with
/// [`Worker::register_copy_from`](crate::Worker::register_copy_from).
pub trait CopyFromFunction: Send + Sync {
    /// The SQL `FORMAT` identifier users type, e.g. `example_lines` in
    /// `COPY t FROM 'x' (FORMAT example_lines)`.
    fn format(&self) -> &str;

    /// Registered name of the worker (table) function that performs the read.
    /// Surfaced as `CopyFromFormatInfo.handler` and as the function's name in
    /// `duckdb_functions()`.
    fn handler_name(&self) -> &str;

    /// Optional free-text comment surfaced by `vgi_copy_formats()`.
    fn comment(&self) -> Option<String> {
        None
    }

    /// Optimizer- / discovery-facing metadata. The `description` and `tags`
    /// surface on the advertised `CopyFromFormatInfo`.
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }

    /// The COPY options, declared as named [`ArgSpec`]s (position `-1`). Their
    /// types / docs become the option metadata surfaced by `vgi_copy_formats()`.
    fn argument_specs(&self) -> Vec<ArgSpec>;

    /// Secret types this reader needs (triggers the two-phase secret bind). The
    /// COPY source path is in `params.copy_from` (e.g. for a cloud `COPY ... FROM
    /// 's3://…'`), so a reader can scope its request to it. Resolved secrets then
    /// arrive in `ctx.params.secrets` at [`read`](Self::read). Defaults to none.
    fn secret_lookups(&self, _params: &BindParams) -> Vec<crate::secrets::SecretLookup> {
        Vec::new()
    }

    /// Parse `ctx.path` and return Arrow batches whose schema matches
    /// `ctx.expected_schema` exactly. The whole source is read here (single-shot
    /// — mirrors the Python reader). `out` is provided for `client_log` only.
    ///
    /// Implement [`read_stream`](Self::read_stream) instead when the source is
    /// large enough that materializing every batch is the wrong shape.
    fn read(
        &self,
        ctx: &CopyFromReadContext,
        out: &mut OutputCollector,
    ) -> Result<Vec<RecordBatch>>;

    /// Stream the source instead of materializing it: return a producer whose
    /// `next_batch` is pulled until it yields `None`.
    ///
    /// [`read`](Self::read) must build every batch before DuckDB sees the first
    /// row, so peak memory scales with the whole source. A producer is pulled
    /// one batch at a time by the same scan path, so a reader that decodes
    /// incrementally (an object-store range reader, a `File`, a socket) can hold
    /// one batch instead of the entire file.
    ///
    /// **This changes nothing on the wire.** The COPY-FROM scan already streams:
    /// dispatch pulls `next_batch` in a loop and emits each batch to the stream
    /// as it arrives. This hook only lets a reader plug into that loop directly
    /// rather than through a buffer, so a worker that implements it is
    /// indistinguishable to the extension.
    ///
    /// Returning `None` (the default) uses [`read`](Self::read), so existing
    /// implementations are unaffected. The returned producer must own its state
    /// — `ctx` borrows from the call — so clone what it needs from
    /// `ctx.params`, which is `Clone`.
    fn read_stream(&self, _ctx: &CopyFromReadContext) -> Result<Option<Box<dyn TableProducer>>> {
        Ok(None)
    }
}

/// Adapter that exposes a [`CopyFromFunction`] as an ordinary producer-mode
/// [`TableFunction`], so the entire table bind/init/scan path is reused. The
/// COPY-FROM context arrives via [`ProcessParams::copy_from`].
pub struct CopyFromTable(pub Arc<dyn CopyFromFunction>);

impl CopyFromTable {
    fn missing_ctx_error(&self) -> RpcError {
        RpcError::value_error(format!(
            "{} is a COPY FROM format reader; invoke it via \
             COPY <table> FROM '<path>' (FORMAT {}), not as a table function.",
            self.0.handler_name(),
            self.0.format()
        ))
    }
}

impl TableFunction for CopyFromTable {
    fn name(&self) -> &str {
        self.0.handler_name()
    }

    fn metadata(&self) -> FunctionMetadata {
        self.0.metadata()
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        self.0.argument_specs()
    }

    fn secret_lookups(&self, params: &BindParams) -> Vec<crate::secrets::SecretLookup> {
        // Forward to the COPY reader, which can scope its request to the COPY
        // source path in `params.copy_from`.
        self.0.secret_lookups(params)
    }

    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        // DuckDB forces the scan's output types to the COPY target's columns,
        // so a COPY-FROM reader must produce exactly `expected_schema`.
        let cf = params
            .copy_from
            .as_ref()
            .ok_or_else(|| self.missing_ctx_error())?;
        let output_schema = crate::ipc::read_schema(&cf.expected_schema.0)?;
        Ok(BindResponse {
            output_schema,
            opaque_data: Vec::new(),
        })
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        if params.copy_from.is_none() {
            return Err(self.missing_ctx_error());
        }
        Ok(Box::new(CopyFromProducer {
            inner: self.0.clone(),
            params: params.clone(),
            started: false,
            stream: None,
            batches: Vec::new().into_iter(),
        }))
    }
}

/// Drives a [`CopyFromFunction`] as a producer.
///
/// On the first pull it asks for a streaming producer
/// ([`CopyFromFunction::read_stream`]) and delegates to it if there is one;
/// otherwise it falls back to [`CopyFromFunction::read`] and drains the batches
/// that call materialized. Either way the scan itself is unchanged — batches
/// leave one at a time.
struct CopyFromProducer {
    inner: Arc<dyn CopyFromFunction>,
    params: ProcessParams,
    started: bool,
    /// Set when the reader supplied a streaming producer.
    stream: Option<Box<dyn TableProducer>>,
    /// The buffered fallback's batches.
    batches: std::vec::IntoIter<RecordBatch>,
}

impl CopyFromProducer {
    fn start(&mut self, out: &mut OutputCollector) -> Result<()> {
        // `copy_from` presence is defended at bind/producer build.
        let cf =
            self.params.copy_from.clone().ok_or_else(|| {
                RpcError::value_error("COPY FROM context missing at process time")
            })?;
        let expected_schema = self.params.output_schema.clone();
        let ctx = CopyFromReadContext {
            path: &cf.file_path,
            options: &self.params.arguments,
            expected_schema: &expected_schema,
            params: &self.params,
        };
        self.started = true;
        match self.inner.read_stream(&ctx)? {
            Some(producer) => self.stream = Some(producer),
            None => self.batches = self.inner.read(&ctx, out)?.into_iter(),
        }
        Ok(())
    }
}

impl TableProducer for CopyFromProducer {
    fn next_batch(&mut self, out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if !self.started {
            self.start(out)?;
        }
        match self.stream.as_mut() {
            Some(producer) => producer.next_batch(out),
            None => Ok(self.batches.next()),
        }
    }
    fn resume_supported(&self) -> bool {
        // Single batch: there is nothing to resume. Declared explicitly —
        // `resume_supported` has no default, so every producer states this.
        false
    }
}

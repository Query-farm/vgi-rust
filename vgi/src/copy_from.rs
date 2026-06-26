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

    /// Parse `ctx.path` and return Arrow batches whose schema matches
    /// `ctx.expected_schema` exactly. The whole source is read here (single-shot
    /// — mirrors the Python reader). `out` is provided for `client_log` only.
    fn read(
        &self,
        ctx: &CopyFromReadContext,
        out: &mut OutputCollector,
    ) -> Result<Vec<RecordBatch>>;
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
            done: false,
            batches: Vec::new().into_iter(),
        }))
    }
}

/// Single-shot producer: the first `next_batch` reads the whole source via
/// [`CopyFromFunction::read`] and drains the resulting batches.
struct CopyFromProducer {
    inner: Arc<dyn CopyFromFunction>,
    params: ProcessParams,
    done: bool,
    batches: std::vec::IntoIter<RecordBatch>,
}

impl CopyFromProducer {
    fn fill(&mut self, out: &mut OutputCollector) -> Result<()> {
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
        let batches = self.inner.read(&ctx, out)?;
        self.batches = batches.into_iter();
        self.done = true;
        Ok(())
    }
}

impl TableProducer for CopyFromProducer {
    fn next_batch(&mut self, out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if !self.done {
            self.fill(out)?;
        }
        Ok(self.batches.next())
    }
}

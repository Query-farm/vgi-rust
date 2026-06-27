// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Custom `COPY ... TO` format writers.
//!
//! A [`CopyToFunction`] lets a VGI catalog act as a remote sink: the user runs
//! `COPY (query|table) TO 'path' (FORMAT <name>, opt val, ...)` and DuckDB
//! streams the source rows out to the worker, which writes them to a destination
//! (a proprietary format, a remote API/object store, a custom sink).
//!
//! Mechanically a `CopyToFunction` is a **buffered (Sink+Combine) function with
//! no Source phase** — it reuses the entire `table_buffering_process` /
//! `table_buffering_combine` machinery on both sides:
//!
//! * [`write`](CopyToFunction::write) is called once per input batch (the
//!   buffered `process()` step, fanned out across DuckDB's sink threads /
//!   per-thread workers). Persist the batch to a shard via `ctx.storage`
//!   (`execution_id`-scoped — see below).
//! * [`close`](CopyToFunction::close) is called **exactly once** on the
//!   coordinator worker (the buffered `combine()` step, driven by DuckDB's
//!   once-only `copy_to_finalize`). Read the shards back and perform the terminal
//!   write+flush+close of the destination.
//!
//! There is no finalize/drain phase, so the destination MUST be fully written and
//! closed inside [`close`](CopyToFunction::close).
//!
//! **Cross-process invariant.** `write()` and `close()` may run on different
//! worker processes (pool rotation / HTTP). Any shard state `close()` needs MUST
//! live in cross-process storage scoped by the `execution_id` (`ctx.storage` is
//! the canonical choice). Buffering on `self` silently breaks under rotation.
//!
//! The destination `path` + `format` arrive via the bind's `copy_to` context;
//! the COPY options arrive as the function's normal `Arg`-annotated arguments
//! (`ctx.options`). The source schema is `ctx.input_schema`.
//!
//! Register with [`crate::Worker::register_copy_to`]; mirrors the Python
//! `vgi.copy_to_function.CopyToFunction`. Scope: **`TO` only** (no
//! `COPY ... FROM`).

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use vgi_rpc::{Result, RpcError};

use crate::arguments::Arguments;
use crate::buffering::{BufferingParams, TableBufferingFunction};
use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata};
use crate::storage::FunctionStorage;
use crate::table_function::TableProducer;

/// Context handed to [`CopyToFunction::write`] (per input batch).
pub struct CopyToWriteContext<'a> {
    /// Destination path from the `COPY ... TO 'path'` statement.
    pub path: &'a str,
    /// Parsed COPY options (the function's named arguments).
    pub options: &'a Arguments,
    /// Cross-process state store. Persist shards scoped by `execution_id` here so
    /// [`CopyToFunction::close`] — which may run in another worker process — can
    /// read them back.
    pub storage: &'a Arc<dyn FunctionStorage>,
    /// The execution id scoping this COPY's shard state.
    pub execution_id: &'a [u8],
    /// Full buffering parameters (storage, settings, attach scope, …).
    pub params: &'a BufferingParams,
}

/// Context handed to [`CopyToFunction::close`] (terminal write, once).
pub struct CopyToCloseContext<'a> {
    /// Destination path from the `COPY ... TO 'path'` statement.
    pub path: &'a str,
    /// Parsed COPY options (the function's named arguments).
    pub options: &'a Arguments,
    /// Cross-process state store; read the shards persisted by `write()` here.
    pub storage: &'a Arc<dyn FunctionStorage>,
    /// The execution id scoping this COPY's shard state.
    pub execution_id: &'a [u8],
    /// The COPY source columns' schema, for empty-input header generation
    /// (no shards were written). `None` when the source schema is unavailable.
    pub input_schema: Option<&'a SchemaRef>,
    /// Full buffering parameters.
    pub params: &'a BufferingParams,
}

/// A custom `COPY ... TO` format writer.
///
/// Implement [`format`](Self::format), [`handler_name`](Self::handler_name),
/// [`argument_specs`](Self::argument_specs) (the COPY options — the destination
/// `file_path` is supplied by the COPY statement, **not** an option),
/// [`write`](Self::write) (persist one batch to a shard), and
/// [`close`](Self::close) (terminal destination write). Register with
/// [`Worker::register_copy_to`](crate::Worker::register_copy_to).
pub trait CopyToFunction: Send + Sync {
    /// The SQL `FORMAT` identifier users type, e.g. `example_lines_out` in
    /// `COPY t TO 'x' (FORMAT example_lines_out)`.
    fn format(&self) -> &str;

    /// Registered name of the worker function that performs the write. Surfaced
    /// as `CopyFromFormatInfo.handler` and used as the buffering function name on
    /// the `table_buffering_process` / `table_buffering_combine` RPCs.
    fn handler_name(&self) -> &str;

    /// Optional free-text comment surfaced by `vgi_copy_formats()`.
    fn comment(&self) -> Option<String> {
        None
    }

    /// Optimizer- / discovery-facing metadata. The `description` and `tags`
    /// surface on the advertised format.
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }

    /// The COPY options, declared as named [`ArgSpec`]s (position `-1`). Their
    /// types / docs become the option metadata surfaced by `vgi_copy_formats()`.
    fn argument_specs(&self) -> Vec<ArgSpec>;

    /// Whether this writer requires rows in **source order**. When `true`,
    /// discovery advertises `ordered=true` and the extension installs a
    /// single-thread sink (DuckDB `REGULAR_COPY_TO_FILE`), so one worker receives
    /// every batch in source order. Mirrors `Meta.sink_order_dependent`. Default
    /// `false` (parallel sharded sink).
    fn ordered(&self) -> bool {
        false
    }

    /// Persist one input `batch` to a shard (called per sink batch, possibly in
    /// parallel across threads / worker processes). Store it in cross-process
    /// storage scoped by `ctx.execution_id` (`ctx.storage`) so [`close`](Self::close)
    /// can read it back. Do NOT buffer on `self`.
    fn write(&self, ctx: &CopyToWriteContext, batch: &RecordBatch) -> Result<()>;

    /// Write the destination and close it, once. Returns the row count written.
    /// Read the shards persisted by [`write`](Self::write) (via `ctx.storage`)
    /// and perform the terminal write + flush + close of `ctx.path`. Called even
    /// when zero rows were written (empty COPY) — produce an empty/header-only
    /// destination. The returned count is informational (DuckDB reports its own
    /// `rows_copied`).
    fn close(&self, ctx: &CopyToCloseContext) -> Result<i64>;
}

/// Adapter that exposes a [`CopyToFunction`] as a [`TableBufferingFunction`], so
/// the entire buffering sink/combine RPC path is reused. There is no Source
/// phase: `combine` returns an empty finalize list and `finalize_producer` is
/// never invoked.
pub struct CopyToBuffering(pub Arc<dyn CopyToFunction>);

impl CopyToBuffering {
    fn missing_ctx_error(&self) -> RpcError {
        RpcError::value_error(format!(
            "{} is a COPY TO format writer; invoke it via \
             COPY <source> TO '<path>' (FORMAT {}), not directly.",
            self.0.handler_name(),
            self.0.format()
        ))
    }
}

impl TableBufferingFunction for CopyToBuffering {
    fn name(&self) -> &str {
        self.0.handler_name()
    }

    fn metadata(&self) -> FunctionMetadata {
        self.0.metadata()
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        self.0.argument_specs()
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        // A sink produces no rows — bind to an empty output schema.
        Ok(BindResponse {
            output_schema: Arc::new(arrow_schema::Schema::empty()),
            opaque_data: Vec::new(),
        })
    }

    fn process(&self, params: &BufferingParams, batch: &RecordBatch) -> Result<Vec<u8>> {
        let ct = params
            .copy_to
            .as_ref()
            .ok_or_else(|| self.missing_ctx_error())?;
        let ctx = CopyToWriteContext {
            path: &ct.file_path,
            options: &params.arguments,
            storage: &params.storage,
            execution_id: &params.execution_id,
            params,
        };
        self.0.write(&ctx, batch)?;
        // Round-trip the execution_id as the opaque state_id (all of a query's
        // shards land in one execution-scoped bucket).
        Ok(params.execution_id.clone())
    }

    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        let ct = params
            .copy_to
            .as_ref()
            .ok_or_else(|| self.missing_ctx_error())?;
        let ctx = CopyToCloseContext {
            path: &ct.file_path,
            options: &params.arguments,
            storage: &params.storage,
            execution_id: &params.execution_id,
            input_schema: params.input_schema.as_ref(),
            params,
        };
        self.0.close(&ctx)?;
        // No finalize streams — the COPY-TO path never drains output.
        Ok(Vec::new())
    }

    fn finalize_producer(
        &self,
        _params: &BufferingParams,
        _finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        Err(RpcError::value_error(
            "COPY TO writer has no finalize/source phase",
        ))
    }
}

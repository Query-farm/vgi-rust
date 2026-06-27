// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Table buffering (sink + source) function model.
//!
//! Lifecycle (keyed by execution_id):
//! 1. init phase `TABLE_BUFFERING` (sink) — mint execution_id, header-only.
//! 2. `table_buffering_process` (unary, per input batch) → state_id.
//! 3. `table_buffering_combine` (unary, once) → finalize_state_ids.
//! 4. init phase `TABLE_BUFFERING_FINALIZE` (source, per finalize_state_id) →
//!    a producer that drains the buffered state.
//! 5. `table_buffering_destructor` (unary) — cleanup.
//!
//! State is held in the worker's [`FunctionStorage`](crate::storage) backend
//! (cross-process: the subprocess transport pools workers, so the sink and
//! source phases can run in different PIDs).

use std::sync::Arc;

use arrow_schema::SchemaRef;
use vgi_rpc::Result;

use crate::arguments::Arguments;
use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata};
use crate::settings::Settings;
use crate::storage::FunctionStorage;
use crate::table_function::TableProducer;

/// Parameters for buffering process / combine / finalize.
pub struct BufferingParams {
    pub execution_id: Vec<u8>,
    pub storage: Arc<dyn FunctionStorage>,
    pub output_schema: SchemaRef,
    pub arguments: Arguments,
    pub settings: Settings,
    /// Resolved secrets for this call (two-phase secret bind). Populated at the
    /// finalize phase from the C++-replayed bind call; empty at process/combine
    /// (the connector only replays secrets on bind/init).
    pub secrets: crate::secrets::Secrets,
    /// The (plaintext) attach state for this call, when carried by the request.
    /// Persisted at the sink-init phase and replayed to process/combine, which
    /// otherwise carry no per-attach context (stateful functions scope storage
    /// by this).
    pub attach_opaque_data: Option<Vec<u8>>,
    /// DuckDB per-chunk batch index, when the function declares
    /// `requires_input_batch_index` (only set on the process RPC).
    pub batch_index: Option<i64>,
    /// The `COPY ... TO` context (destination format + path), present only when
    /// this buffering execution backs a [`CopyToFunction`](crate::copy_to). The
    /// process/combine RPCs carry no bind_call, so it is persisted at sink-init
    /// (keyed by `execution_id`) and replayed here. `None` for ordinary buffered
    /// functions.
    pub copy_to: Option<crate::protocol::dtos::CopyToContext>,
    /// The source (input) schema this buffering execution bound to, persisted at
    /// sink-init and replayed to process/combine (which otherwise carry no
    /// schema). Used by COPY-TO writers for empty-input header generation. `None`
    /// when no input schema was bound or persisted.
    pub input_schema: Option<SchemaRef>,
    /// In-band INFO logs to surface in `duckdb_logs()`; the unary process /
    /// combine handlers drain this into the call context after returning.
    pub logs: Arc<std::sync::Mutex<Vec<String>>>,
}

impl BufferingParams {
    /// Queue an INFO-level client log line (surfaced under `duckdb_logs()`).
    pub fn log(&self, message: impl Into<String>) {
        if let Ok(mut g) = self.logs.lock() {
            g.push(message.into());
        }
    }
}

/// A table buffering (sink+source) function.
pub trait TableBufferingFunction: Send + Sync {
    fn name(&self) -> &str;
    fn metadata(&self) -> FunctionMetadata;
    fn argument_specs(&self) -> Vec<ArgSpec>;
    /// Secret types this function needs (triggers the two-phase secret bind).
    /// Returning a non-empty list on the first bind makes the connector resolve
    /// the named/scoped secrets and re-bind with `resolved_secrets_provided`;
    /// the resolved values then arrive in `BindParams::secrets` at `on_bind` and
    /// in `BufferingParams::secrets` at the finalize phase.
    fn secret_lookups(&self, _params: &BindParams) -> Vec<crate::secrets::SecretLookup> {
        Vec::new()
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse>;
    /// Sink one batch; return an opaque state_id.
    fn process(
        &self,
        params: &BufferingParams,
        batch: &arrow_array::RecordBatch,
    ) -> Result<Vec<u8>>;
    /// Merge state_ids into finalize_state_ids.
    fn combine(&self, params: &BufferingParams, state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>>;
    /// Build the per-finalize_state_id source producer.
    fn finalize_producer(
        &self,
        params: &BufferingParams,
        finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>>;
}

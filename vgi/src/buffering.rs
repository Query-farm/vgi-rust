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
    /// The (plaintext) attach state for this call, when carried by the request.
    /// Persisted at the sink-init phase and replayed to process/combine, which
    /// otherwise carry no per-attach context (stateful functions scope storage
    /// by this).
    pub attach_opaque_data: Option<Vec<u8>>,
    /// DuckDB per-chunk batch index, when the function declares
    /// `requires_input_batch_index` (only set on the process RPC).
    pub batch_index: Option<i64>,
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

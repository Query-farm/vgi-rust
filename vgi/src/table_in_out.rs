// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Table-in-out function model: transform input batches to output batches.
//!
//! Driven as an exchange stream: each input batch produces zero or more output
//! batches. The dispatch adapter applies auto-filter pushdown to the output.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use vgi_rpc::{Result, RpcError};

use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};

/// A table-in-out VGI function.
pub trait TableInOutFunction: Send + Sync {
    fn name(&self) -> &str;
    fn metadata(&self) -> FunctionMetadata;
    fn argument_specs(&self) -> Vec<ArgSpec>;
    /// Resolve the output schema. Default: passthrough the input schema.
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("table-in-out requires an input schema"))?;
        Ok(BindResponse {
            output_schema: input,
            opaque_data: Vec::new(),
        })
    }
    /// Transform one input batch into output batches. Default: project the
    /// input to the (possibly narrowed) output schema by column name. A
    /// distributed/accumulating function persists partial state to
    /// `params.storage` here and returns an empty batch.
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(vec![project_batch(batch, &params.output_schema)?])
    }

    /// Whether the function flushes accumulated state at end-of-stream (drives
    /// the `FINALIZE` init phase). Default: no.
    fn has_finish(&self) -> bool {
        false
    }

    /// End-of-stream: drain accumulated per-worker partials from
    /// `params.storage` and emit the final output batches. Only called when
    /// `has_finish()` is true.
    fn finish(&self, _params: &ProcessParams) -> Result<Vec<RecordBatch>> {
        Ok(Vec::new())
    }
}

/// Project a batch to `schema`'s columns by name (projection pushdown).
pub fn project_batch(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    // If the schemas already match, pass through unchanged.
    if batch.schema().fields() == schema.fields() {
        return Ok(batch.clone());
    }
    let mut cols = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        match batch.schema().column_with_name(f.name()) {
            Some((i, _)) => cols.push(batch.column(i).clone()),
            None => {
                return Err(RpcError::runtime_error(format!(
                    "projection column '{}' not found in input",
                    f.name()
                )))
            }
        }
    }
    RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| RpcError::runtime_error(format!("project batch: {e}")))
}

/// Build an `Arc<Schema>`.
pub fn arc(s: Schema) -> Arc<Schema> {
    Arc::new(s)
}

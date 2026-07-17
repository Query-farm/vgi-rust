// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Table-in-out function model: transform input batches to output batches.
//!
//! Driven as an exchange stream: each input batch produces zero or more output
//! batches. The dispatch adapter applies auto-filter pushdown to the output.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use vgi_rpc::{Result, RpcError};

use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};

/// Per-batch wire-metadata key carrying per-output-row provenance for the
/// batched correlated-LATERAL operator: a base64-encoded raw little-endian
/// `int32[]` where element `i` is the 0-based index (into this call's input
/// batch) of the row that produced output row `i`. Absent metadata = identity
/// 1→1 map (the extension assumes it, and requires output rows == input rows).
pub const PARENT_ROW_METADATA_KEY: &str = "vgi_rpc.parent_row#b64";

/// Options for one emitted table-in-out output batch (all optional).
#[derive(Default)]
pub struct EmitOptions {
    /// Arbitrary per-batch wire metadata (merged under the derived keys).
    pub metadata: Option<HashMap<String, String>>,
    /// Per-output-row provenance for the batched correlated-LATERAL operator:
    /// `parent_rows[i]` is the input-row index that produced output row `i`.
    /// MUST carry exactly one entry per emitted row (validated at emit).
    pub parent_rows: Option<Vec<i32>>,
}

/// Collects one `process` call's output batches with optional per-batch
/// metadata — the emit surface for
/// [`TableInOutFunction::process_out`]. Plain 1→1 transforms keep returning
/// `Vec<RecordBatch>` from [`TableInOutFunction::process`]; override
/// `process_out` (and emit through this) only when a batch needs
/// [`EmitOptions`] (LATERAL provenance, raw metadata).
#[derive(Default)]
pub struct TableInOutOutput {
    pub(crate) items: Vec<(RecordBatch, Option<HashMap<String, String>>)>,
}

impl TableInOutOutput {
    /// Emit a batch with no per-batch metadata.
    pub fn emit(&mut self, batch: RecordBatch) {
        self.items.push((batch, None));
    }

    /// Emit a batch with [`EmitOptions`] (metadata / LATERAL provenance).
    /// Mirrors the Python `out.emit(batch, metadata=..., parent_rows=...)`
    /// kwargs.
    pub fn emit_with(&mut self, batch: RecordBatch, opts: EmitOptions) -> Result<()> {
        let mut md: HashMap<String, String> = opts.metadata.unwrap_or_default();
        if let Some(parent_rows) = opts.parent_rows {
            if parent_rows.len() != batch.num_rows() {
                return Err(RpcError::runtime_error(format!(
                    "emit_with(parent_rows=...) length {} != batch.num_rows {}; parent_rows \
                     must carry exactly one input-row index per emitted output row",
                    parent_rows.len(),
                    batch.num_rows()
                )));
            }
            // Nothing to map on an empty emit; skip the base64+pack.
            if !parent_rows.is_empty() {
                let mut raw = Vec::with_capacity(parent_rows.len() * 4);
                for v in &parent_rows {
                    raw.extend_from_slice(&v.to_le_bytes());
                }
                md.insert(
                    PARENT_ROW_METADATA_KEY.to_string(),
                    crate::partition::base64_encode(&raw),
                );
            }
        }
        self.items
            .push((batch, if md.is_empty() { None } else { Some(md) }));
        Ok(())
    }
}

/// A table-in-out VGI function.
pub trait TableInOutFunction: Send + Sync {
    fn name(&self) -> &str;
    fn metadata(&self) -> FunctionMetadata;
    fn argument_specs(&self) -> Vec<ArgSpec>;
    /// Secret lookups to request at bind (two-phase secret resolution). When
    /// non-empty and secrets are not yet resolved, `bind` returns these and the
    /// extension re-binds with the resolved values (preserving the input
    /// schema). The resolved secret is then available on `params.secrets` at
    /// `process` time. Mirrors [`TableFunction::secret_lookups`](crate::table_function::TableFunction::secret_lookups).
    fn secret_lookups(&self, _params: &BindParams) -> Vec<crate::secrets::SecretLookup> {
        Vec::new()
    }
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

    /// Transform one input batch, emitting through `out` — the metadata-capable
    /// variant of [`process`](Self::process). Override this instead of
    /// `process` when an output batch needs [`EmitOptions`]: per-output-row
    /// LATERAL provenance (`parent_rows`) or raw per-batch wire metadata. The default delegates
    /// to `process` and emits each returned batch with no metadata; the
    /// dispatcher always drives this method.
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        for b in self.process(params, batch)? {
            out.emit(b);
        }
        Ok(())
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

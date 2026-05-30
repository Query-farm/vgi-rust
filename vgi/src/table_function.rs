//! Table (producer) function model: generate output batches without input.
//!
//! Mirrors Go `initTable` + `TableProducerState`. The function creates a
//! per-execution [`TableProducer`] whose `produce` is called repeatedly; it
//! emits one batch per tick and calls `out.finish()` when exhausted.

use arrow_schema::SchemaRef;
use vgi_rpc::Result;

use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};

/// Cardinality estimate for a table function.
#[derive(Clone, Copy, Default)]
pub struct TableCardinality {
    pub estimate: Option<i64>,
    pub max: Option<i64>,
}

/// A per-execution producer. Holds the function's mutable scan state.
///
/// Returns the next batch, or `None` when the scan is exhausted. The dispatch
/// adapter applies projection / auto-filter pushdown to each batch before
/// emitting, so producers stay free of that concern. `out` is provided only
/// for `client_log` — do NOT emit through it (the adapter emits the returned
/// batch).
pub trait TableProducer: Send {
    fn next_batch(
        &mut self,
        out: &mut vgi_rpc::OutputCollector,
    ) -> Result<Option<arrow_array::RecordBatch>>;
    /// Serialize the scan position for stateless HTTP continuation (default
    /// 0 — producers whose whole result fits in one HTTP response need none).
    fn save_position(&self) -> u64 {
        0
    }
    /// Restore the scan position after rebuilding from an HTTP state token.
    fn restore_position(&mut self, _pos: u64) {}
    /// Per-batch wire metadata for the batch just returned by `next_batch`
    /// (e.g. `vgi_batch_index` for `supports_batch_index` functions). Default
    /// none. Called once after each `next_batch` that returns `Some`.
    fn last_metadata(&self) -> Option<std::collections::HashMap<String, String>> {
        None
    }
}

/// A table (producer) VGI function.
pub trait TableFunction: Send + Sync {
    fn name(&self) -> &str;
    fn metadata(&self) -> FunctionMetadata;
    fn argument_specs(&self) -> Vec<ArgSpec>;
    /// Resolve the output schema from bind-time arguments.
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse>;
    /// Worker parallelism hint (default single worker).
    fn max_workers(&self, _params: &BindParams) -> i64 {
        1
    }
    /// Primary-worker global init: runs once per execution (when DuckDB issues
    /// the init without an execution_id) before any producer. Use it to push
    /// work items onto `params.storage`'s queue for parallel-scan producers.
    /// Secondary workers (init carrying an execution_id) skip it.
    fn on_init(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }
    /// Optional cardinality estimate.
    fn cardinality(&self, _params: &BindParams) -> Option<TableCardinality> {
        None
    }
    /// Build the per-execution producer. `params.output_schema` is the
    /// (possibly projection-narrowed) schema to emit.
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>>;
}

/// Narrow a full schema to the projected columns (`projection_ids`).
pub fn project_schema(full: &SchemaRef, ids: &Option<Vec<i64>>) -> SchemaRef {
    match ids {
        Some(ids) if !ids.is_empty() => {
            let fields: Vec<_> = ids
                .iter()
                .filter_map(|&i| full.fields().get(i as usize).cloned())
                .collect();
            std::sync::Arc::new(arrow_schema::Schema::new(fields))
        }
        _ => full.clone(),
    }
}

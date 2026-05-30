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

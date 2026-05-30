//! Aggregate function model (UPDATE / COMBINE / FINALIZE).
//!
//! DuckDB drives aggregates with per-group state. The worker keeps state in
//! the cross-process [`crate::buffering::BufferingStore`] (KV mode), keyed by
//! `(execution_id, group_id)`, because update / combine / finalize can run in
//! different pooled worker processes. States are opaque to the C++ extension —
//! the worker chooses any encoding.

use std::collections::HashMap;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use vgi_rpc::Result;

use crate::arguments::Arguments;
use crate::function::{ArgSpec, BindResponse, FunctionMetadata};
use crate::settings::Settings;

/// The reserved group-id column prepended to UPDATE input batches.
pub const GROUP_COLUMN_NAME: &str = "__vgi_group_id";

/// Parameters for `aggregate_bind`.
pub struct AggregateBindParams {
    pub arguments: Arguments,
    pub input_schema: Option<SchemaRef>,
    pub settings: Settings,
}

/// An aggregate VGI function.
pub trait AggregateFunction: Send + Sync {
    fn name(&self) -> &str;
    fn metadata(&self) -> FunctionMetadata;
    fn argument_specs(&self) -> Vec<ArgSpec>;
    /// Resolve the (single-column `result`) output schema.
    fn on_bind(&self, params: &AggregateBindParams) -> Result<BindResponse>;
    /// The serialized initial state for a fresh group.
    fn initial_state(&self) -> Vec<u8>;
    /// Fold the batch's rows into the per-group `states` map. `states` is
    /// pre-loaded (initial state for new groups) for every group id present in
    /// `group_ids`. `columns` are the input columns with the group-id column
    /// already stripped.
    fn update(
        &self,
        states: &mut HashMap<i64, Vec<u8>>,
        group_ids: &Int64Array,
        columns: &[ArrayRef],
    ) -> Result<()>;
    /// Merge `source` state into `target` state, returning the new target.
    fn combine(&self, target: Vec<u8>, source: Vec<u8>) -> Result<Vec<u8>>;
    /// Build the single-column output batch: one row per `group_ids` entry.
    /// `states[i]` is the loaded state for `group_ids[i]` (`None` if unseen).
    fn finalize(
        &self,
        output_schema: &SchemaRef,
        group_ids: &Int64Array,
        states: &[Option<Vec<u8>>],
    ) -> Result<RecordBatch>;
}

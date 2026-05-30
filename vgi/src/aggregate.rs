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
use vgi_rpc::{Result, RpcError};

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
    /// Evaluate the windowed aggregate for each output row. `frames[i]` is the
    /// list of `(begin, end)` sub-frames for output row `i` over the
    /// `partition`'s input columns; returns the output column (one element per
    /// row), matching `output_schema`. Only `supports_window` functions
    /// override this; the default errors.
    fn window(
        &self,
        _partition: &RecordBatch,
        _output_schema: &SchemaRef,
        _frames: &[Vec<(i64, i64)>],
        _filter_mask: Option<&[bool]>,
    ) -> Result<arrow_array::ArrayRef> {
        Err(RpcError::runtime_error("window() not supported by this aggregate"))
    }

    /// Process one chunk of a streaming-partitioned session. The chunk's
    /// columns are `[partition_key_cols.., order_key_cols.., value_cols..]`.
    /// `states` is the cross-chunk per-partition state map (partition-key bytes
    /// → opaque state bytes), loaded and persisted by the framework. Returns a
    /// same-length output column. Only `streaming_partitioned` functions
    /// override this; the default errors.
    fn streaming_chunk(
        &self,
        _chunk: &RecordBatch,
        _partition_key_count: usize,
        _order_key_count: usize,
        _states: &mut HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<ArrayRef> {
        Err(RpcError::runtime_error("streaming_chunk() not supported by this aggregate"))
    }

    /// Like [`finalize`], but with access to the bind-time arguments (stashed
    /// at `aggregate_bind`, reloaded here). Override for `ConstParam(phase=
    /// "finalize")` aggregates like `vgi_percentile`. The default ignores them.
    fn finalize_with_args(
        &self,
        output_schema: &SchemaRef,
        group_ids: &Int64Array,
        states: &[Option<Vec<u8>>],
        _args: &crate::arguments::Arguments,
    ) -> Result<RecordBatch> {
        self.finalize(output_schema, group_ids, states)
    }
}

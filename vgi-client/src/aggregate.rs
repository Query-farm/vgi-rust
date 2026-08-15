// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Aggregate functions: bind, update, combine, finalize, destroy.
//!
//! Aggregates are **grouped**. Every update batch carries a
//! `__vgi_group_id` int64 column alongside the value columns, and finalize is
//! handed the group ids it wants results for. That is the whole shape:
//!
//! 1. **bind** — resolves the output schema and mints an `execution_id`.
//! 2. **update** — fold a batch of `(group_id, values…)` into per-group state.
//! 3. **combine** — merge another execution's state in (parallel aggregation).
//! 4. **finalize** — ask for the result of a set of group ids.
//! 5. **destroy** — release the worker's state.
//!
//! Unlike the scan phases, these RPCs re-resolve the function by
//! `(schema, name)` rather than echoing a bind back, so they take the catalog
//! handle and the function's coordinates directly.
//!
//! The C++ DuckDB extension is the only other client that implements this, so
//! it — not the Python or Java clients — is the reference for the semantics
//! here.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi_protocol::generated::request_params as p;
use vgi_protocol::ipc;
use vgi_protocol::protocol::dtos::{
    AggregateBindRequest, AggregateBindResponse, AggregateCombineRequest,
    AggregateDestructorRequest, AggregateFinalizeRequest, AggregateFinalizeResponse,
    AggregateStreamingChunkRequest, AggregateStreamingChunkResponse,
    AggregateStreamingCloseRequest, AggregateStreamingOpenRequest, AggregateStreamingOpenResponse,
    AggregateUpdateRequest, AggregateWindowBatchRequest, AggregateWindowDestructorRequest,
    AggregateWindowInitRequest, AggregateWindowRequest, AggregateWindowResponse,
};
use vgi_rpc::errors::{Result, RpcError};
use vgi_rpc::Bytes;

use crate::catalog::AttachedCatalog;
use crate::client::VgiClient;
use crate::scan::BindSpec;
use crate::wire_call::{call, call_unit, envelope};

/// The group-id column every update batch must carry.
///
/// Must match `vgi::aggregate::GROUP_COLUMN_NAME` on the worker side.
pub const GROUP_COLUMN_NAME: &str = "__vgi_group_id";

/// A bound aggregate execution.
#[derive(Debug, Clone)]
pub struct BoundAggregate {
    execution_id: Bytes,
    output_schema: SchemaRef,
    raw_output_schema: Bytes,
    function_name: String,
    schema_name: Option<String>,
}

impl BoundAggregate {
    /// The worker-minted id for this aggregation.
    ///
    /// Two executions of the same function have different ids; passing one
    /// execution's id to [`VgiClient::aggregate_combine`] is how parallel
    /// partial aggregates are merged.
    pub fn execution_id(&self) -> &Bytes {
        &self.execution_id
    }

    /// The result schema, resolved at bind.
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }
}

/// Prepend the group-id column to a batch of value columns.
///
/// The worker splits the batch back apart on the column *name*, so a caller
/// that builds the batch by hand must use [`GROUP_COLUMN_NAME`].
pub fn with_group_ids(group_ids: &[i64], values: &RecordBatch) -> Result<RecordBatch> {
    if group_ids.len() != values.num_rows() {
        return Err(RpcError::type_error(format!(
            "{} group ids for {} rows",
            group_ids.len(),
            values.num_rows()
        )));
    }
    let mut fields: Vec<Field> = vec![Field::new(GROUP_COLUMN_NAME, DataType::Int64, false)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(group_ids.to_vec()))];
    for (i, f) in values.schema().fields().iter().enumerate() {
        fields.push(f.as_ref().clone());
        columns.push(values.column(i).clone());
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| RpcError::runtime_error(format!("build update batch: {e}")))
}

/// A one-column batch of the group ids to finalize.
fn group_ids_batch(group_ids: &[i64]) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            GROUP_COLUMN_NAME,
            DataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(group_ids.to_vec())) as ArrayRef],
    )
    .map_err(|e| RpcError::runtime_error(format!("build group-id batch: {e}")))
}

impl VgiClient {
    /// Bind an aggregate, minting an execution to fold into.
    pub fn aggregate_bind(
        &mut self,
        cat: &AttachedCatalog,
        spec: &BindSpec,
        input_schema: &Schema,
    ) -> Result<BoundAggregate> {
        let request = AggregateBindRequest {
            function_name: spec.function_name.clone(),
            arguments: spec.arguments.to_ipc()?,
            input_schema: Some(Bytes(ipc::write_schema(input_schema)?)),
            settings: spec.settings.clone(),
            secrets: None,
            attach_opaque_data: Some(cat.handle().clone()),
            schema_name: spec.schema_name.clone(),
        };
        let response: AggregateBindResponse = call(
            self.transport_mut(),
            "aggregate_bind",
            p::AggregateBindParams {
                request: envelope(request)?,
            },
        )?;
        let output_schema = ipc::read_schema(&response.output_schema.0).map_err(|e| {
            RpcError::type_error(format!("aggregate_bind returned an unreadable schema: {e}"))
        })?;
        Ok(BoundAggregate {
            execution_id: response.execution_id,
            output_schema,
            raw_output_schema: response.output_schema,
            function_name: spec.function_name.clone(),
            schema_name: spec.schema_name.clone(),
        })
    }

    /// Fold a batch of grouped values into the aggregate's state.
    ///
    /// `batch` must carry the [`GROUP_COLUMN_NAME`] column; build it with
    /// [`with_group_ids`].
    pub fn aggregate_update(
        &mut self,
        cat: &AttachedCatalog,
        agg: &BoundAggregate,
        batch: &RecordBatch,
    ) -> Result<()> {
        if batch.schema().column_with_name(GROUP_COLUMN_NAME).is_none() {
            return Err(RpcError::type_error(format!(
                "update batch is missing the `{GROUP_COLUMN_NAME}` column; use with_group_ids()"
            )));
        }
        let request = AggregateUpdateRequest {
            function_name: agg.function_name.clone(),
            execution_id: agg.execution_id.clone(),
            input_batch: Bytes(ipc::write_batch(batch)?),
            attach_opaque_data: Some(cat.handle().clone()),
            schema_name: agg.schema_name.clone(),
        };
        call_unit(
            self.transport_mut(),
            "aggregate_update",
            p::AggregateUpdateParams {
                request: envelope(request)?,
            },
        )
    }

    /// Merge another execution's partial state into this one.
    ///
    /// This is what makes parallel aggregation possible: each worker folds its
    /// own slice into its own execution, then the partials are merged.
    pub fn aggregate_combine(
        &mut self,
        cat: &AttachedCatalog,
        agg: &BoundAggregate,
        merge_batch: &RecordBatch,
    ) -> Result<()> {
        let request = AggregateCombineRequest {
            function_name: agg.function_name.clone(),
            execution_id: agg.execution_id.clone(),
            merge_batch: Bytes(ipc::write_batch(merge_batch)?),
            attach_opaque_data: Some(cat.handle().clone()),
            schema_name: agg.schema_name.clone(),
        };
        call_unit(
            self.transport_mut(),
            "aggregate_combine",
            p::AggregateCombineParams {
                request: envelope(request)?,
            },
        )
    }

    /// Ask for the results of a set of groups.
    ///
    /// The answer has one row per requested group id, in the order asked.
    pub fn aggregate_finalize(
        &mut self,
        cat: &AttachedCatalog,
        agg: &BoundAggregate,
        group_ids: &[i64],
    ) -> Result<RecordBatch> {
        let request = AggregateFinalizeRequest {
            function_name: agg.function_name.clone(),
            execution_id: agg.execution_id.clone(),
            group_ids_batch: Bytes(ipc::write_batch(&group_ids_batch(group_ids)?)?),
            output_schema: agg.raw_output_schema.clone(),
            attach_opaque_data: Some(cat.handle().clone()),
            schema_name: agg.schema_name.clone(),
        };
        let response: AggregateFinalizeResponse = call(
            self.transport_mut(),
            "aggregate_finalize",
            p::AggregateFinalizeParams {
                request: envelope(request)?,
            },
        )?;
        let batch = ipc::read_batch(&response.result_batch.0)?;
        if batch.num_rows() != group_ids.len() {
            return Err(RpcError::type_error(format!(
                "finalize returned {} rows for {} requested groups",
                batch.num_rows(),
                group_ids.len()
            )));
        }
        Ok(batch)
    }

    /// Release the worker's state for this execution.
    ///
    /// Best-effort: a worker that never allocated anything answers happily.
    /// Note the destructor carries no attach handle — the execution id alone
    /// identifies what to free.
    pub fn aggregate_destroy(&mut self, agg: &BoundAggregate) -> Result<()> {
        let request = AggregateDestructorRequest {
            function_name: agg.function_name.clone(),
            execution_id: agg.execution_id.clone(),
            schema_name: agg.schema_name.clone(),
        };
        call_unit(
            self.transport_mut(),
            "aggregate_destructor",
            p::AggregateDestructorParams {
                request: envelope(request)?,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;

    fn values(n: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(n.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    #[test]
    fn group_ids_are_prepended_under_the_agreed_name() {
        let b = with_group_ids(&[0, 0, 1], &values(&[1, 2, 3])).unwrap();
        assert_eq!(b.schema().field(0).name(), GROUP_COLUMN_NAME);
        assert_eq!(b.num_columns(), 2, "group id plus the one value column");
        assert_eq!(b.num_rows(), 3);
    }

    #[test]
    fn a_group_id_per_row_is_required() {
        // Silently padding or truncating would attribute values to the wrong
        // group, which is worse than refusing.
        assert!(with_group_ids(&[0, 1], &values(&[1, 2, 3])).is_err());
        assert!(with_group_ids(&[0, 1, 2, 3], &values(&[1, 2, 3])).is_err());
    }

    #[test]
    fn value_columns_keep_their_names_and_order() {
        let b = with_group_ids(&[9], &values(&[42])).unwrap();
        assert_eq!(b.schema().field(1).name(), "v");
        let v = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(v.value(0), 42);
    }
}

// ---------------------------------------------------------------------------
// Window aggregates
// ---------------------------------------------------------------------------

/// A window partition cached on the worker.
///
/// Window evaluation is two-phase: the whole partition is shipped once, then
/// each output row's frames are evaluated against it. Dropping this releases
/// nothing by itself — call [`VgiClient::window_destroy`], which the worker
/// needs to free the cached partition.
#[derive(Debug, Clone)]
pub struct WindowPartition {
    execution_id: Bytes,
    partition_id: i64,
    function_name: String,
    schema_name: Option<String>,
}

impl WindowPartition {
    /// Which partition this is, within its execution.
    pub fn partition_id(&self) -> i64 {
        self.partition_id
    }
}

impl VgiClient {
    /// Ship a whole window partition to the worker.
    ///
    /// `partition_batch` is every row of the partition, in window order.
    pub fn window_init(
        &mut self,
        agg: &BoundAggregate,
        partition_id: i64,
        partition_batch: &RecordBatch,
    ) -> Result<WindowPartition> {
        let request = AggregateWindowInitRequest {
            function_name: agg.function_name.clone(),
            execution_id: agg.execution_id.clone(),
            partition_id,
            row_count: partition_batch.num_rows() as i64,
            partition_batch: Bytes(ipc::write_batch(partition_batch)?),
            output_schema: agg.raw_output_schema.clone(),
            filter_mask: None,
            frame_stats: None,
            all_valid: None,
            schema_name: agg.schema_name.clone(),
        };
        call_unit(
            self.transport_mut(),
            "aggregate_window_init",
            p::AggregateWindowInitParams {
                request: envelope(request)?,
            },
        )?;
        Ok(WindowPartition {
            execution_id: agg.execution_id.clone(),
            partition_id,
            function_name: agg.function_name.clone(),
            schema_name: agg.schema_name.clone(),
        })
    }

    /// Evaluate one output row over its frames.
    ///
    /// `frames` are `(start, end)` row offsets within the partition. A row
    /// usually has one frame; several appear for frame types that union
    /// disjoint ranges.
    pub fn window_evaluate(
        &mut self,
        part: &WindowPartition,
        row: i64,
        frames: &[(i64, i64)],
    ) -> Result<RecordBatch> {
        let request = AggregateWindowRequest {
            function_name: part.function_name.clone(),
            execution_id: part.execution_id.clone(),
            partition_id: part.partition_id,
            rid: row,
            frame_starts: frames.iter().map(|f| f.0).collect(),
            frame_ends: frames.iter().map(|f| f.1).collect(),
            schema_name: part.schema_name.clone(),
        };
        let response: AggregateWindowResponse = call(
            self.transport_mut(),
            "aggregate_window",
            p::AggregateWindowParams {
                request: envelope(request)?,
            },
        )?;
        ipc::read_batch(&response.result_batch.0)
    }

    /// Evaluate `frames_per_row.len()` consecutive output rows in one call.
    ///
    /// The frame arrays are flattened: `frames_per_row[i]` says how many of the
    /// `frame_starts`/`frame_ends` entries belong to row `row_idx + i`. Batching
    /// here is what keeps a window query from costing one RPC per row.
    pub fn window_evaluate_batch(
        &mut self,
        part: &WindowPartition,
        row_idx: i64,
        frames_per_row: &[i64],
        frames: &[(i64, i64)],
    ) -> Result<RecordBatch> {
        let expected: i64 = frames_per_row.iter().sum();
        if expected != frames.len() as i64 {
            return Err(RpcError::type_error(format!(
                "frames_per_row sums to {expected} but {} frames were supplied",
                frames.len()
            )));
        }
        let request = AggregateWindowBatchRequest {
            function_name: part.function_name.clone(),
            execution_id: part.execution_id.clone(),
            partition_id: part.partition_id,
            row_idx,
            count: frames_per_row.len() as i64,
            frames_per_row: frames_per_row.to_vec(),
            frame_starts: frames.iter().map(|f| f.0).collect(),
            frame_ends: frames.iter().map(|f| f.1).collect(),
            schema_name: part.schema_name.clone(),
        };
        let response: AggregateWindowResponse = call(
            self.transport_mut(),
            "aggregate_window_batch",
            p::AggregateWindowBatchParams {
                request: envelope(request)?,
            },
        )?;
        ipc::read_batch(&response.result_batch.0)
    }

    /// Drop a cached window partition.
    pub fn window_destroy(&mut self, part: &WindowPartition) -> Result<()> {
        let request = AggregateWindowDestructorRequest {
            function_name: part.function_name.clone(),
            execution_id: part.execution_id.clone(),
            partition_id: part.partition_id,
            schema_name: part.schema_name.clone(),
        };
        call_unit(
            self.transport_mut(),
            "aggregate_window_destructor",
            p::AggregateWindowDestructorParams {
                request: envelope(request)?,
            },
        )
    }
}

// ---------------------------------------------------------------------------
// Streaming-partitioned aggregates
// ---------------------------------------------------------------------------

/// An open streaming-aggregate session.
///
/// The streaming protocol skips client-side partition materialisation
/// entirely: input chunks go straight to the worker, which answers each with a
/// same-length output batch.
#[derive(Debug, Clone)]
pub struct StreamingAggregate {
    execution_id: Bytes,
    function_name: String,
    schema_name: Option<String>,
}

impl StreamingAggregate {
    /// The worker's session token.
    pub fn execution_id(&self) -> &Bytes {
        &self.execution_id
    }
}

impl VgiClient {
    /// Open a streaming-aggregate session.
    ///
    /// `partition_key_count` and `order_key_count` say how many leading columns
    /// of the input are the PARTITION BY and ORDER BY keys respectively; the
    /// worker uses them to detect partition boundaries as chunks arrive.
    #[allow(clippy::too_many_arguments)]
    pub fn streaming_open(
        &mut self,
        cat: &AttachedCatalog,
        spec: &BindSpec,
        input_schema: &Schema,
        output_schema: &Schema,
        partition_key_count: i64,
        order_key_count: i64,
    ) -> Result<StreamingAggregate> {
        let request = AggregateStreamingOpenRequest {
            function_name: spec.function_name.clone(),
            arguments: spec.arguments.to_ipc()?,
            input_schema: Bytes(ipc::write_schema(input_schema)?),
            partition_key_count,
            order_key_count,
            output_schema: Bytes(ipc::write_schema(output_schema)?),
            settings: spec.settings.clone(),
            secrets: None,
            attach_opaque_data: Some(cat.handle().clone()),
            schema_name: spec.schema_name.clone(),
        };
        let response: AggregateStreamingOpenResponse = call(
            self.transport_mut(),
            "aggregate_streaming_open",
            p::AggregateStreamingOpenParams {
                request: envelope(request)?,
            },
        )?;
        Ok(StreamingAggregate {
            execution_id: response.execution_id,
            function_name: spec.function_name.clone(),
            schema_name: spec.schema_name.clone(),
        })
    }

    /// Feed one input chunk, receiving the same number of output rows.
    pub fn streaming_chunk(
        &mut self,
        cat: &AttachedCatalog,
        session: &StreamingAggregate,
        input: &RecordBatch,
    ) -> Result<RecordBatch> {
        let request = AggregateStreamingChunkRequest {
            function_name: session.function_name.clone(),
            execution_id: session.execution_id.clone(),
            input_batch: Bytes(ipc::write_batch(input)?),
            attach_opaque_data: Some(cat.handle().clone()),
            schema_name: session.schema_name.clone(),
        };
        let response: AggregateStreamingChunkResponse = call(
            self.transport_mut(),
            "aggregate_streaming_chunk",
            p::AggregateStreamingChunkParams {
                request: envelope(request)?,
            },
        )?;
        let out = ipc::read_batch(&response.result_batch.0)?;
        if out.num_rows() != input.num_rows() {
            return Err(RpcError::type_error(format!(
                "streaming chunk answered {} rows for {} input rows; a streaming \
                 aggregate must answer one row per input row",
                out.num_rows(),
                input.num_rows()
            )));
        }
        Ok(out)
    }

    /// End a streaming session and free its state.
    pub fn streaming_close(
        &mut self,
        cat: &AttachedCatalog,
        session: &StreamingAggregate,
    ) -> Result<()> {
        let request = AggregateStreamingCloseRequest {
            function_name: session.function_name.clone(),
            execution_id: session.execution_id.clone(),
            attach_opaque_data: Some(cat.handle().clone()),
            schema_name: session.schema_name.clone(),
        };
        call_unit(
            self.transport_mut(),
            "aggregate_streaming_close",
            p::AggregateStreamingCloseParams {
                request: envelope(request)?,
            },
        )
    }
}

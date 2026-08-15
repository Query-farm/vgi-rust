// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The buffering example for the vgi-rust documentation.
//!
//! A buffering function is for the case where output depends on the WHOLE input
//! — a global sort, a top-k, a full reduction. It runs in three phases:
//!
//!   - `process`           (sink)   — called per input batch, in parallel across
//!     DuckDB threads. Stash what you need and return an opaque state id.
//!   - `combine`                    — called once, on the coordinator, with every
//!     state id the sink produced. Reduce them into the ids the source drains.
//!   - `finalize_producer` (source) — called per finalize id, streaming the
//!     result out.
//!
//! The phases can run in different worker processes, so nothing may live in a
//! `static` between them. State goes in `params.storage`, scoped to the
//! execution and shared across the workers serving it.
//!
//! ```text
//! cargo build --release --bin rowcount
//! # then, in a Haybarn shell:
//! ATTACH 'buffers' (TYPE vgi, LOCATION './target/release/rowcount');
//! SELECT * FROM buffers.row_count((SELECT * FROM big_table));
//! ```

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::buffering::{BufferingParams, TableBufferingFunction};
use vgi::catalog::CatalogModel;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata};
use vgi::table_function::TableProducer;
use vgi::vgi_rpc::OutputCollector;
use vgi::{Result, RpcError};

const NS: &[u8] = b"rowcount";
const KEY: &[u8] = b"";

/// The source-phase producer: emits the total once, then ends the stream.
struct CountProducer {
    schema: SchemaRef,
    total: Option<i64>,
}

impl TableProducer for CountProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        let Some(total) = self.total.take() else {
            return Ok(None);
        };
        let col: ArrayRef = Arc::new(Int64Array::from(vec![total]));
        RecordBatch::try_new(self.schema.clone(), vec![col])
            .map(Some)
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

struct RowCount;

impl TableBufferingFunction for RowCount {
    fn name(&self) -> &str {
        "row_count"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Counts every row of the input relation".to_string(),
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("data", 0, "table", "Rows to count")]
    }

    /// Output is one BIGINT, whatever the input looked like.
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new(
                "count",
                DataType::Int64,
                true,
            )])),
            opaque_data: Vec::new(),
        })
    }

    /// The sink runs in parallel across DuckDB threads. `append` is an
    /// append-only log, so concurrent appends cannot lose each other the way a
    /// read-modify-write would.
    fn process(&self, params: &BufferingParams, batch: &RecordBatch) -> Result<Vec<u8>> {
        let n = batch.num_rows() as i64;
        params
            .storage
            .append(&params.execution_id, NS, KEY, n.to_le_bytes().to_vec());
        Ok(params.execution_id.clone())
    }

    /// Runs once, on the coordinator. There is a single bucket to drain here,
    /// so it just names the execution; a top-k would reduce the partials first.
    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![params.execution_id.clone()])
    }

    /// The source phase: sum the log and hand back a one-shot producer.
    fn finalize_producer(
        &self,
        params: &BufferingParams,
        _finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        let mut total = 0i64;
        // -1 starts before the first entry; the limit is a page size, not a cap.
        let mut after_id = -1i64;
        loop {
            let rows = params
                .storage
                .scan(&params.execution_id, NS, KEY, after_id, 256);
            if rows.is_empty() {
                break;
            }
            for (id, value) in rows {
                let bytes: [u8; 8] = value
                    .as_slice()
                    .try_into()
                    .map_err(|_| RpcError::runtime_error("corrupt row_count state"))?;
                total += i64::from_le_bytes(bytes);
                after_id = id;
            }
        }
        Ok(Box::new(CountProducer {
            schema: params.output_schema.clone(),
            total: Some(total),
        }))
    }
}

fn main() {
    let mut worker = vgi::Worker::new();
    worker.register_buffering(RowCount);
    worker.set_catalog(CatalogModel {
        name: "buffers".to_string(),
        comment: Some(
            "Documentation example: a buffering (sink → combine → source) function".to_string(),
        ),
        ..Default::default()
    });
    worker.run();
}

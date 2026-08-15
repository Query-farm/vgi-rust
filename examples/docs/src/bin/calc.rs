// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The worker built across the vgi-rust tutorial: one scalar function and one
//! table function in a single catalog.
//!
//! The scalar `double` transforms a column in place. The table function `series`
//! *generates* rows from an argument, so it is called in a FROM clause rather
//! than an expression. One worker can serve any mix of shapes.
//!
//! ```text
//! cargo build --release --bin calc
//! # then, in a Haybarn shell:
//! ATTACH 'calc' (TYPE vgi, LOCATION './target/release/calc');
//! SELECT calc.double(21);
//! SELECT * FROM calc.series(3);
//! ```

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::catalog::CatalogModel;
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, ScalarFunction,
};
use vgi::table_function::{TableFunction, TableProducer};
use vgi::vgi_rpc::OutputCollector;
use vgi::{Result, RpcError};

// ── scalar: double(n) ───────────────────────────────────────────────────────

struct Double;

impl ScalarFunction for Double {
    fn name(&self) -> &str {
        "double"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Doubles a BIGINT".to_string(),
            return_type: Some(DataType::Int64),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("n", 0, "int64", "Value to double")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let n = batch.column(0).as_primitive::<Int64Type>();
        let out: Int64Array = (0..n.len())
            .map(|i| {
                if n.is_valid(i) {
                    Some(n.value(i) * 2)
                } else {
                    None
                }
            })
            .collect();
        RecordBatch::try_new(
            params.output_schema.clone(),
            vec![Arc::new(out) as ArrayRef],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// ── table: series(count) ────────────────────────────────────────────────────

const BATCH_SIZE: i64 = 1024;

/// The per-scan cursor. A table function is *pulled*: the engine calls
/// `next_batch` until it answers `None`, so whatever the function needs between
/// calls lives here rather than in the function itself (which is shared and
/// must stay `Sync`).
struct SeriesProducer {
    schema: SchemaRef,
    next: i64,
    count: i64,
}

impl TableProducer for SeriesProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.next >= self.count {
            // None is end-of-stream. Returning an empty batch instead would
            // loop forever.
            return Ok(None);
        }
        let end = (self.next + BATCH_SIZE).min(self.count);
        let col: ArrayRef = Arc::new((self.next..end).collect::<Int64Array>());
        self.next = end;
        RecordBatch::try_new(self.schema.clone(), vec![col])
            .map(Some)
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

struct Series;

impl TableFunction for Series {
    fn name(&self) -> &str {
        "series"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Generates the integers 0..count-1".to_string(),
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        // const_arg, not column: the value is fixed for the whole scan and read
        // at bind. with_ge(0.0) is enforced by the framework before any row is
        // produced, so series(-1) fails rather than returning nothing.
        vec![ArgSpec::const_arg("count", 0, "int64", "How many numbers to generate").with_ge(0.0)]
    }

    /// Runs once per query, before any data moves. The output shape is fixed
    /// here, so it never inspects the arguments; a function whose columns depend
    /// on its arguments would build the schema from `params` instead.
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
            opaque_data: Vec::new(),
        })
    }

    /// Runs once per scan, after bind. Arguments are fixed for the whole scan,
    /// so this is where they are read — decoding them per batch would be waste.
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(SeriesProducer {
            schema: params.output_schema.clone(),
            next: 0,
            count: params.arguments.const_i64(0).unwrap_or(0),
        }))
    }
}

fn main() {
    let mut worker = vgi::Worker::new();
    worker.register_scalar(Double);
    worker.register_table(Series);
    worker.set_catalog(CatalogModel {
        name: "calc".to_string(),
        comment: Some("Tutorial worker: a scalar and a table function".to_string()),
        ..Default::default()
    });
    worker.run();
}

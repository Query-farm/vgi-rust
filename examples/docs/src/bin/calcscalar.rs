// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The worker built in step 1 of the vgi-rust tutorial: one scalar function,
//! served over stdio, callable from DuckDB as `calc.double()`.
//!
//! A scalar function is the simplest shape — one row in, one value out, with no
//! state and no finalize phase. DuckDB hands the worker a whole Arrow column and
//! expects a column of the same length back.
//!
//! ```text
//! cargo build --release --bin calcscalar
//! # then, in a Haybarn shell:
//! ATTACH 'calc' (TYPE vgi, LOCATION './target/release/calcscalar');
//! SELECT calc.double(21);
//! ```

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_schema::DataType;
use vgi::catalog::CatalogModel;
use vgi::function::{ArgSpec, FunctionMetadata, ProcessParams, ScalarFunction};
use vgi::{Result, RpcError};

/// Doubles each value in its input column.
struct Double;

impl ScalarFunction for Double {
    /// The SQL name, qualified by the catalog: `calc.double(...)`.
    fn name(&self) -> &str {
        "double"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Doubles a BIGINT".to_string(),
            // The declared output type. A function whose output depends on its
            // input leaves this off and decides in on_bind instead.
            return_type: Some(DataType::Int64),
            ..Default::default()
        }
    }

    /// The signature. `column` means the value arrives per row; the type is
    /// named as an Arrow type string, so `int64` rather than SQL's `bigint`.
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("n", 0, "int64", "Value to double")]
    }

    /// Called once per input BATCH, not per row.
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        // The column arrives as the type the spec declared, so this downcast is
        // safe. See the tutorial for what to do when it might not be.
        let n = batch.column(0).as_primitive::<Int64Type>();

        // One output value per input row. Null in, null out — collecting from
        // Option is what carries that through.
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

fn main() {
    let mut worker = vgi::Worker::new();
    worker.register_scalar(Double);

    // Functions are served through a catalog, and its name is the name DuckDB
    // ATTACHes. They must match: attaching under any other name fails.
    worker.set_catalog(CatalogModel {
        name: "calc".to_string(),
        ..Default::default()
    });

    worker.run();
}

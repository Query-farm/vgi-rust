// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The table-in-out example for the vgi-rust documentation.
//!
//! A table-in-out function consumes a relation and streams a transformed
//! relation back, batch by batch. Unlike a scalar it may change the row count,
//! and unlike a buffering function it never holds the whole input — each call
//! emits what it can from the batch in hand, which is what keeps memory flat
//! over an arbitrarily large scan.
//!
//! ```text
//! cargo build --release --bin filter
//! # then, in a Haybarn shell:
//! ATTACH 'filters' (TYPE vgi, LOCATION './target/release/filter');
//! SELECT * FROM filters.filter_positive((SELECT * FROM t));
//! ```

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, BooleanArray, RecordBatch};
use vgi::catalog::CatalogModel;
use vgi::function::{ArgSpec, FunctionMetadata, ProcessParams};
use vgi::table_in_out::TableInOutFunction;
use vgi::{Result, RpcError};

struct FilterPositive;

impl TableInOutFunction for FilterPositive {
    fn name(&self) -> &str {
        "filter_positive"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Keeps only the rows whose `value` column is greater than zero"
                .to_string(),
            ..Default::default()
        }
    }

    /// The TABLE argument is declared like any other, with the Arrow type
    /// string `"table"`. Leave it out and the extension does not know this
    /// function takes a relation: the call fails with
    /// *"Table function cannot contain subqueries"*.
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("data", 0, "table", "Rows to filter")]
    }

    // on_bind is left at its default, which echoes the input schema. This
    // function drops rows, not columns, so the shapes match.

    /// Called once per input batch. Return zero or more batches; an empty Vec
    /// drops the batch entirely.
    fn process(&self, _params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let col = batch
            .column_by_name("value")
            .ok_or_else(|| RpcError::value_error("expected a `value` column"))?;

        // The caller's relation decides the column's width, so normalize to
        // int64 rather than assuming. cast errors instead of guessing, which is
        // what you want here — a `value` column of strings is a caller mistake,
        // not something to paper over.
        let cast = arrow_cast::cast(col, &arrow_schema::DataType::Int64)
            .map_err(|e| RpcError::value_error(format!("`value` must be an integer: {e}")))?;
        let v = cast.as_primitive::<Int64Type>();

        let keep: BooleanArray = (0..v.len())
            .map(|i| Some(v.is_valid(i) && v.value(i) > 0))
            .collect();

        let filtered = arrow_select::filter::filter_record_batch(batch, &keep)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;

        // An empty batch is legal but pointless — skip the round trip.
        if filtered.num_rows() == 0 {
            return Ok(Vec::new());
        }
        Ok(vec![filtered])
    }
}

fn main() {
    let mut worker = vgi::Worker::new();
    worker.register_table_in_out(FilterPositive);
    worker.set_catalog(CatalogModel {
        name: "filters".to_string(),
        comment: Some("Documentation example: a streaming table-in-out function".to_string()),
        ..Default::default()
    });
    worker.run();
}

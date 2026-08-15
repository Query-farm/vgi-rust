// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The aggregate example for the vgi-rust documentation.
//!
//! An aggregate folds many rows into one value per GROUP BY group. It runs in
//! four phases, and the split is what lets DuckDB parallelise it:
//!
//!   - `initial_state` — the identity value for a group (0 for a sum).
//!   - `update`        — fold a batch of rows into per-group state. Runs in
//!     every worker, over that worker's share of the rows.
//!   - `combine`       — merge two partial states for the same group.
//!   - `finalize`      — turn state into one output row per group.
//!
//! ```text
//! cargo build --release --bin sum
//! # then, in a Haybarn shell:
//! ATTACH 'agg' (TYPE vgi, LOCATION './target/release/sum');
//! SELECT category, agg.vgi_sum(value) FROM t GROUP BY category;
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::aggregate::{AggregateBindParams, AggregateFunction};
use vgi::catalog::CatalogModel;
use vgi::function::{ArgSpec, BindResponse, FunctionMetadata};
use vgi::{Result, RpcError};

/// Aggregate state is an opaque `Vec<u8>` — the framework moves it between
/// phases and possibly between processes, so it never inspects it. Encoding is
/// the function's job. Eight little-endian bytes is the whole of it here; a
/// richer state would reach for serde.
fn encode(total: i64) -> Vec<u8> {
    total.to_le_bytes().to_vec()
}

fn decode(bytes: &[u8]) -> Result<i64> {
    bytes
        .try_into()
        .map(i64::from_le_bytes)
        .map_err(|_| RpcError::runtime_error(format!("corrupt sum state: {} bytes", bytes.len())))
}

struct VgiSum;

impl AggregateFunction for VgiSum {
    fn name(&self) -> &str {
        "vgi_sum"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Sums a BIGINT column per group".to_string(),
            // DEFAULT means DuckDB skips NULL inputs, so update never sees one.
            // That is what makes SUM over an all-NULL group return NULL: the
            // group never appears in group_ids, so finalize is handed None.
            null_handling: Some("DEFAULT".to_string()),
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Column to sum")]
    }

    fn on_bind(&self, _params: &AggregateBindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new(
                "result",
                DataType::Int64,
                true,
            )])),
            opaque_data: Vec::new(),
        })
    }

    fn initial_state(&self) -> Vec<u8> {
        encode(0)
    }

    /// `states` arrives pre-loaded with the initial state for every group id in
    /// this batch, so there is no "create if missing" step — just fold.
    fn update(
        &self,
        states: &mut HashMap<i64, Vec<u8>>,
        group_ids: &Int64Array,
        columns: &[ArrayRef],
    ) -> Result<()> {
        let values = columns
            .first()
            .ok_or_else(|| RpcError::value_error("vgi_sum: missing value column"))?
            .as_primitive::<Int64Type>();

        for i in 0..group_ids.len() {
            if !values.is_valid(i) {
                continue;
            }
            let gid = group_ids.value(i);
            let total = states
                .get(&gid)
                .map(|s| decode(s))
                .transpose()?
                .unwrap_or(0);
            states.insert(gid, encode(total + values.value(i)));
        }
        Ok(())
    }

    /// Must be associative and commutative: DuckDB decides how many workers run
    /// and in what order their partials merge.
    fn combine(&self, target: Vec<u8>, source: Vec<u8>) -> Result<Vec<u8>> {
        Ok(encode(decode(&target)? + decode(&source)?))
    }

    /// One row per group id, in the order given. `None` means the group never
    /// contributed a non-null row, which is SQL NULL.
    fn finalize(
        &self,
        output_schema: &SchemaRef,
        group_ids: &Int64Array,
        states: &[Option<Vec<u8>>],
    ) -> Result<RecordBatch> {
        let mut out: Vec<Option<i64>> = Vec::with_capacity(group_ids.len());
        for state in states.iter() {
            out.push(match state {
                Some(bytes) => Some(decode(bytes)?),
                None => None,
            });
        }
        let col: ArrayRef = Arc::new(out.into_iter().collect::<Int64Array>());
        RecordBatch::try_new(output_schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

fn main() {
    let mut worker = vgi::Worker::new();
    worker.register_aggregate(VgiSum);
    worker.set_catalog(CatalogModel {
        name: "agg".to_string(),
        comment: Some("Documentation example: a distributed aggregate".to_string()),
        ..Default::default()
    });
    worker.run();
}

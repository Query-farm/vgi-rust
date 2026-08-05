// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Probe functions for global (`system.main`) registration.
//!
//! WARNING: EXAMPLE/TEST FUNCTIONS ONLY. These exist purely so a client can be
//! observed publishing a worker's functions into its *global* function
//! namespace, and are deliberately separate from every other fixture:
//!
//! * **They are additive for other language implementations.** The example
//!   catalog is a cross-language contract — Python, Go, TypeScript, and Java
//!   workers mirror it. If global registration reused existing fixtures
//!   (`double`, `ten_thousand`, `vgi_sum`, `echo_buffering`), every
//!   implementation would have to make the same semantic change to functions it
//!   already ships.
//! * **They document their own purpose.** Nothing else depends on them, so
//!   changing one cannot break an unrelated test.
//!
//! One per function type, so the client's registration path is exercised for
//! every kind:
//!
//! | Registered name   | Type             | Published as (`vgi_example`)   |
//! |-------------------|------------------|--------------------------------|
//! | `global_scalar`   | scalar           | `vgi_example_global_scalar`    |
//! | `global_table`    | table            | `vgi_example_global_table`     |
//! | `global_agg`      | aggregate        | `vgi_example_global_agg`       |
//! | `global_buffered` | table-buffering  | `vgi_example_global_buffered`  |
//!
//! Each returns a value tagged with its own name so a test can assert that the
//! globally-published name reached the function it was supposed to, rather than
//! some same-named function belonging to another catalog.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::aggregate::{AggregateBindParams, AggregateFunction};
use vgi::buffering::{BufferingParams, TableBufferingFunction};
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FunctionExample, FunctionMetadata, ProcessParams,
    ScalarFunction,
};
use vgi::ipc;
use vgi::protocol::enums;
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

/// The names this module registers, in the order the catalog advertises them
/// as global functions.
pub const NAMES: [&str; 4] = [
    "global_scalar",
    "global_table",
    "global_agg",
    "global_buffered",
];

/// The prefix the client applies when publishing [`NAMES`] globally.
pub const PREFIX: &str = "vgi_example";

/// Register the four global-registration probes.
pub fn register(w: &mut vgi::Worker) {
    w.register_scalar(GlobalScalarFunction);
    w.register_table(GlobalTableFunction);
    w.register_aggregate(GlobalAggFunction);
    w.register_buffering(GlobalBufferedFunction);
}

fn categories() -> Vec<String> {
    vec!["test".to_string(), "global".to_string()]
}

// ---------------------------------------------------------------------------
// global_scalar(value) -> varchar
// ---------------------------------------------------------------------------

/// Scalar probe — labels each input so the caller can prove which impl ran.
///
/// SQL: `SELECT vgi_example_global_scalar(7)` -> `'global_scalar:7'`
pub struct GlobalScalarFunction;

impl ScalarFunction for GlobalScalarFunction {
    fn name(&self) -> &str {
        "global_scalar"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Global-registration probe (scalar)".to_string(),
            categories: categories(),
            return_type: Some(DataType::Utf8),
            examples: vec![FunctionExample {
                sql: "SELECT vgi_example_global_scalar(7)".to_string(),
                description: "Scalar probe published into system.main".to_string(),
                expected_output: None,
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Value to label")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let v = arrow_cast::cast(batch.column(0), &DataType::Int64)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let v = v.as_primitive::<Int64Type>();
        let out: StringArray = (0..v.len())
            .map(|i| (!v.is_null(i)).then(|| format!("global_scalar:{}", v.value(i))))
            .collect();
        RecordBatch::try_new(
            params.output_schema.clone(),
            vec![Arc::new(out) as ArrayRef],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// global_table() -> 3 rows {n: int64, label: varchar}
// ---------------------------------------------------------------------------

fn global_table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

/// Emits the three probe rows once, then finishes.
struct GlobalTableProducer {
    output_schema: SchemaRef,
    emitted: bool,
}

impl TableProducer for GlobalTableProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let n = Arc::new(Int64Array::from(vec![0i64, 1, 2])) as ArrayRef;
        let label = Arc::new(StringArray::from(
            (0..3)
                .map(|i| format!("global_table:{i}"))
                .collect::<Vec<_>>(),
        )) as ArrayRef;
        Ok(Some(
            RecordBatch::try_new(self.output_schema.clone(), vec![n, label])
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
}

/// Table probe — three labelled rows, no arguments.
///
/// SQL: `SELECT * FROM vgi_example_global_table()`
pub struct GlobalTableFunction;

impl TableFunction for GlobalTableFunction {
    fn name(&self) -> &str {
        "global_table"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Global-registration probe (table)".to_string(),
            categories: categories(),
            examples: vec![FunctionExample {
                sql: "SELECT * FROM vgi_example_global_table()".to_string(),
                description: "Table probe published into system.main".to_string(),
                expected_output: None,
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: global_table_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, _params: &BindParams) -> Option<TableCardinality> {
        Some(TableCardinality {
            estimate: Some(3),
            max: Some(3),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(GlobalTableProducer {
            output_schema: params.output_schema.clone(),
            emitted: false,
        }))
    }
}

// ---------------------------------------------------------------------------
// global_agg(value) -> int64
// ---------------------------------------------------------------------------

fn le_i64(v: i64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}
fn read_i64(b: &[u8]) -> i64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8.min(b.len())]);
    i64::from_le_bytes(a)
}

/// Aggregate probe — sums int64 input.
///
/// SQL: `SELECT vgi_example_global_agg(v) FROM t`
pub struct GlobalAggFunction;

impl AggregateFunction for GlobalAggFunction {
    fn name(&self) -> &str {
        "global_agg"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Global-registration probe (aggregate)".to_string(),
            categories: categories(),
            null_handling: Some(enums::null_handling::DEFAULT.to_string()),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Column to sum")]
    }
    fn on_bind(&self, _p: &AggregateBindParams) -> Result<BindResponse> {
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
        le_i64(0)
    }
    fn update(
        &self,
        states: &mut HashMap<i64, Vec<u8>>,
        gids: &Int64Array,
        cols: &[ArrayRef],
    ) -> Result<()> {
        let v = arrow_cast::cast(&cols[0], &DataType::Int64)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let v = v.as_primitive::<Int64Type>();
        for i in 0..gids.len() {
            if v.is_null(i) {
                continue;
            }
            let st = states.entry(gids.value(i)).or_insert_with(|| le_i64(0));
            *st = le_i64(read_i64(st) + v.value(i));
        }
        Ok(())
    }
    fn combine(&self, target: Vec<u8>, source: Vec<u8>) -> Result<Vec<u8>> {
        Ok(le_i64(read_i64(&target) + read_i64(&source)))
    }
    fn finalize(
        &self,
        output_schema: &Arc<Schema>,
        gids: &Int64Array,
        states: &[Option<Vec<u8>>],
    ) -> Result<RecordBatch> {
        // A group with no state yields NULL.
        let out: Int64Array = (0..gids.len())
            .map(|i| states[i].as_ref().map(|s| read_i64(s)))
            .collect();
        RecordBatch::try_new(output_schema.clone(), vec![Arc::new(out)])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// global_buffered(data) -> passthrough of the buffered input
// ---------------------------------------------------------------------------

/// Namespace the buffered batches are logged under (matches the Python probe).
const BUF_NS: &[u8] = b"global_buf";

/// Drains the buffered batch log, one batch per tick.
struct GlobalBufferedDrain {
    storage: Arc<dyn vgi::storage::FunctionStorage>,
    execution_id: Vec<u8>,
    after_id: i64,
}

impl TableProducer for GlobalBufferedDrain {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        let rows = self
            .storage
            .scan(&self.execution_id, BUF_NS, b"", self.after_id, 1);
        let Some((id, value)) = rows.into_iter().next() else {
            return Ok(None);
        };
        self.after_id = id;
        Ok(Some(ipc::read_batch(&value)?))
    }
}

/// Table-buffering probe — buffers all input, replays it on finalize.
///
/// SQL: `SELECT * FROM vgi_example_global_buffered((SELECT * FROM t))`
pub struct GlobalBufferedFunction;

impl TableBufferingFunction for GlobalBufferedFunction {
    fn name(&self) -> &str {
        "global_buffered"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Global-registration probe (table-buffering)".to_string(),
            categories: categories(),
            examples: vec![FunctionExample {
                sql: "SELECT * FROM vgi_example_global_buffered((SELECT 1 AS x))".to_string(),
                description: "Buffering probe published into system.main".to_string(),
                expected_output: None,
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("data", 0, "table", "Input table")]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        // Output schema = input schema (passthrough).
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("global_buffered requires input schema"))?;
        Ok(BindResponse {
            output_schema: input,
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &BufferingParams, batch: &RecordBatch) -> Result<Vec<u8>> {
        params
            .storage
            .append(&params.execution_id, BUF_NS, b"", ipc::write_batch(batch)?);
        Ok(params.execution_id.clone())
    }
    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        // Every state_id is the execution_id; collapse to one stream.
        Ok(vec![params.execution_id.clone()])
    }
    fn finalize_producer(
        &self,
        params: &BufferingParams,
        _finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(GlobalBufferedDrain {
            storage: params.storage.clone(),
            execution_id: params.execution_id.clone(),
            after_id: -1,
        }))
    }
}

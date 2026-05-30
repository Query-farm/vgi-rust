//! Table (producer) example fixtures.

mod more;

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

/// Register all table fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_table(SequenceFunction);
    w.register_table(TenThousandFunction);
    w.register_table(MakeSeries::Count);
    w.register_table(MakeSeries::Range);
    w.register_table(MakeSeries::Step);
    more::register(w);
}

fn schema_n() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
}
fn schema_value() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]))
}

fn gen_meta(desc: &str, cats: &[&str]) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        categories: cats.iter().map(|s| s.to_string()).collect(),
        projection_pushdown: true,
        filter_pushdown: true,
        auto_apply_filters: true,
        ..Default::default()
    }
}

/// Emit `values[offset..]` in `batch_size` chunks into `out`.
struct Countdown {
    values: Vec<i64>,
    offset: usize,
    batch_size: usize,
    schema: SchemaRef,
}
impl TableProducer for Countdown {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.offset >= self.values.len() {
            return Ok(None);
        }
        let end = (self.offset + self.batch_size).min(self.values.len());
        let chunk = &self.values[self.offset..end];
        let arr = Int64Array::from(chunk.to_vec());
        let batch = RecordBatch::try_new(self.schema.clone(), vec![Arc::new(arr)])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.offset = end;
        Ok(Some(batch))
    }
}

// ---------------------------------------------------------------------------
// sequence(count, batch_size := 1000, increment := 1) -> {n: int64}
// ---------------------------------------------------------------------------

pub struct SequenceFunction;
impl TableFunction for SequenceFunction {
    fn name(&self) -> &str {
        "sequence"
    }
    fn metadata(&self) -> FunctionMetadata {
        gen_meta(
            "Generates a sequence of integers from 0 to n-1",
            &["generator", "utility"],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows to generate"),
            ArgSpec::const_arg("batch_size", -1, "int64", "Batch size for output"),
            ArgSpec::const_arg("increment", -1, "int64", "Step between values"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema_n(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let count = params.arguments.const_i64(0)?;
        Some(TableCardinality {
            estimate: Some(count),
            max: Some(count),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let increment = params.arguments.named_i64("increment").unwrap_or(1);
        let batch_size = params.arguments.named_i64("batch_size").unwrap_or(1000).max(1) as usize;
        let values: Vec<i64> = (0..count).map(|i| i * increment).collect();
        Ok(Box::new(Countdown {
            values,
            offset: 0,
            batch_size,
            schema: schema_n(),
        }))
    }
}

// ---------------------------------------------------------------------------
// ten_thousand() -> 10000 rows {n: int64}
// ---------------------------------------------------------------------------

pub struct TenThousandFunction;
impl TableFunction for TenThousandFunction {
    fn name(&self) -> &str {
        "ten_thousand"
    }
    fn metadata(&self) -> FunctionMetadata {
        gen_meta("Generates 10000 rows with integers from 0 to 9999", &["generator"])
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema_n(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, _params: &BindParams) -> Option<TableCardinality> {
        Some(TableCardinality {
            estimate: Some(10000),
            max: Some(10000),
        })
    }
    fn producer(&self, _params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(Countdown {
            values: (0..10000).collect(),
            offset: 0,
            batch_size: 1000,
            schema: schema_n(),
        }))
    }
}

// ---------------------------------------------------------------------------
// make_series overloads -> {value: int64}
// ---------------------------------------------------------------------------

pub enum MakeSeries {
    Count,
    Range,
    Step,
}
impl TableFunction for MakeSeries {
    fn name(&self) -> &str {
        "make_series"
    }
    fn metadata(&self) -> FunctionMetadata {
        gen_meta("Generate a series of integers", &["generator"])
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        match self {
            MakeSeries::Count => vec![ArgSpec::const_arg("count", 0, "int64", "Number of values")],
            MakeSeries::Range => vec![
                ArgSpec::const_arg("start", 0, "int64", "Start (inclusive)"),
                ArgSpec::const_arg("stop", 1, "int64", "Stop (exclusive)"),
            ],
            MakeSeries::Step => vec![
                ArgSpec::const_arg("start", 0, "int64", "Start (inclusive)"),
                ArgSpec::const_arg("stop", 1, "int64", "Stop (exclusive)"),
                ArgSpec::const_arg("step", 2, "int64", "Step"),
            ],
        }
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema_value(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let a = &params.arguments;
        let values: Vec<i64> = match self {
            MakeSeries::Count => {
                let c = a.const_i64(0).unwrap_or(0);
                (0..c).collect()
            }
            MakeSeries::Range => {
                let start = a.const_i64(0).unwrap_or(0);
                let stop = a.const_i64(1).unwrap_or(0);
                (start..stop).collect()
            }
            MakeSeries::Step => {
                let start = a.const_i64(0).unwrap_or(0);
                let stop = a.const_i64(1).unwrap_or(0);
                let step = a.const_i64(2).unwrap_or(1).max(1);
                (start..stop).step_by(step as usize).collect()
            }
        };
        Ok(Box::new(Countdown {
            values,
            offset: 0,
            batch_size: 1024,
            schema: schema_value(),
        }))
    }
}

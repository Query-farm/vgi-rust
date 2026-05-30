//! Table (producer) example fixtures.

mod batch_index;
mod order_modes;
mod settings_fixtures;
mod static_scan;
mod partition;
mod filters;
mod more;
mod proj_repro;

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

/// Register all table fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_table(SequenceFunction);
    w.register_table(NestedSequenceFunction);
    w.register_table(TxCachedValueFunction);
    w.register_table(TenThousandFunction);
    w.register_table(MakeSeries::Count);
    w.register_table(MakeSeries::Range);
    w.register_table(MakeSeries::Step);
    w.register_table(MakeSeriesCsv);
    w.register_table(MakeSeriesFloat);
    more::register(w);
    proj_repro::register(w);
    filters::register(w);
    batch_index::register(w);
    partition::register(w);
    order_modes::register(w);
    static_scan::register(w);
    settings_fixtures::register(w);
}

fn schema_n() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
}
fn schema_value() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]))
}

/// Validate `sequence`/`make_series` args: count must be non-NULL; batch_size
/// and increment, when supplied, must be non-NULL and (for batch_size) >= 1.
fn validate_sequence_args(a: &vgi::arguments::Arguments) -> Result<()> {
    use arrow_array::Array;
    // count (positional 0) is required and must not be NULL.
    let count_null = a
        .positional
        .first()
        .map(|o| o.as_ref().map(|c| c.is_empty() || c.is_null(0)).unwrap_or(true))
        .unwrap_or(true);
    if count_null {
        return Err(RpcError::value_error("sequence: count cannot be NULL"));
    }
    for name in ["batch_size", "increment"] {
        if let Some(arr) = a.named.get(name) {
            if arr.is_null(0) {
                return Err(RpcError::value_error(format!("sequence: {name} cannot be NULL")));
            }
        }
    }
    if let Some(bs) = a.named_i64("batch_size") {
        if bs < 1 {
            return Err(RpcError::value_error("sequence: batch_size must be >= 1"));
        }
    }
    if let Some(inc) = a.named_i64("increment") {
        if inc < 1 {
            return Err(RpcError::value_error("sequence: increment must be >= 1"));
        }
    }
    Ok(())
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
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        validate_sequence_args(&params.arguments)?;
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
    fn statistics(&self, params: &BindParams) -> Option<Vec<vgi::statistics::CatColStat>> {
        let count = params.arguments.const_i64(0)?.max(0);
        let increment = params.arguments.named_i64("increment").unwrap_or(1);
        let max = if count == 0 { 0 } else { (count - 1) * increment };
        Some(vec![vgi::statistics::CatColStat {
            column_name: "n".to_string(),
            min: vgi::statistics::StatValue::Int64(0.min(max)),
            max: vgi::statistics::StatValue::Int64(0.max(max)),
            has_null: false,
            has_not_null: true,
            distinct_count: Some(count),
            contains_unicode: None,
            max_string_length: None,
        }])
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        validate_sequence_args(&params.arguments)?;
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
// tx_cached_value(key, seed) -> {v: int64} cached per (transaction, key)
// ---------------------------------------------------------------------------

pub struct TxCachedValueFunction;
impl TableFunction for TxCachedValueFunction {
    fn name(&self) -> &str {
        "tx_cached_value"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Return a value cached per (transaction_opaque_data, key)".to_string(),
            categories: vec!["test".into(), "transaction-storage".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("key", 0, "varchar", "Cache key, scoped to the transaction"),
            ArgSpec::const_arg("seed", 1, "int64", "Value to cache on first call"),
        ]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let key = params.arguments.const_str(0).unwrap_or_default();
        let seed = params.arguments.const_i64(1).unwrap_or(0);
        // Cache the first seed per (transaction, key). Without a transaction
        // (autocommit → transaction_opaque_data is None) every call is fresh.
        let resolved = match (&params.transaction_opaque_data, &params.storage) {
            (Some(txid), Some(store)) => {
                let cache_key = [b"txcache:", key.as_bytes()].concat();
                if let Some(existing) = store.kv_get(txid, &cache_key) {
                    i64::from_le_bytes(existing[..8].try_into().unwrap_or_default())
                } else {
                    store.kv_put(txid, &cache_key, &seed.to_le_bytes());
                    seed
                }
            }
            _ => seed,
        };
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            opaque_data: resolved.to_le_bytes().to_vec(),
        })
    }
    fn cardinality(&self, _params: &BindParams) -> Option<TableCardinality> {
        Some(TableCardinality { estimate: Some(1), max: Some(1) })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let v = params
            .init_opaque_data
            .get(..8)
            .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
            .unwrap_or(0);
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        Ok(Box::new(Countdown { values: vec![v], offset: 0, batch_size: 1, schema }))
    }
}

// ---------------------------------------------------------------------------
// nested_sequence(count, history_size) -> {n, metadata:struct, history:list}
// ---------------------------------------------------------------------------

fn nested_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new(
            "metadata",
            DataType::Struct(
                vec![
                    Field::new("index", DataType::Int64, true),
                    Field::new("label", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        ),
        Field::new(
            "history",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
    ]))
}

pub struct NestedSequenceFunction;
impl TableFunction for NestedSequenceFunction {
    fn name(&self) -> &str {
        "nested_sequence"
    }
    fn metadata(&self) -> FunctionMetadata {
        gen_meta(
            "Generates a sequence with nested struct and list columns",
            &["generator", "utility", "testing"],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows to generate"),
            ArgSpec::const_arg("batch_size", -1, "int64", "Batch size for output"),
            ArgSpec::const_arg("history_size", -1, "int64", "Max items in history list"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: nested_schema(), opaque_data: Vec::new() })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let count = params.arguments.const_i64(0)?;
        Some(TableCardinality { estimate: Some(count), max: Some(count) })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let history_size = params.arguments.named_i64("history_size").unwrap_or(20).max(1);
        Ok(Box::new(NestedSeqProducer { schema: nested_schema(), count, history_size, done: false }))
    }
}

struct NestedSeqProducer {
    schema: SchemaRef,
    count: i64,
    history_size: i64,
    done: bool,
}
impl TableProducer for NestedSeqProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        use arrow_array::builder::{Int64Builder, ListBuilder};
        use arrow_array::{StringArray, StructArray};
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let n: Int64Array = (0..self.count).collect();
        let index: Int64Array = (0..self.count).collect();
        let label = StringArray::from((0..self.count).map(|i| format!("row_{i}")).collect::<Vec<_>>());
        let DataType::Struct(meta_fields) = self.schema.field(1).data_type().clone() else {
            unreachable!()
        };
        let metadata = StructArray::new(
            meta_fields,
            vec![Arc::new(index) as _, Arc::new(label) as _],
            None,
        );
        let mut hist = ListBuilder::new(Int64Builder::new());
        for i in 0..self.count {
            let start = (i - self.history_size + 1).max(0);
            for v in start..=i {
                hist.values().append_value(v);
            }
            hist.append(true);
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(n), Arc::new(metadata), Arc::new(hist.finish())],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(batch))
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

/// `make_series(csv)` — parse a comma-separated integer string into rows.
pub struct MakeSeriesCsv;
impl TableFunction for MakeSeriesCsv {
    fn name(&self) -> &str {
        "make_series"
    }
    fn metadata(&self) -> FunctionMetadata {
        gen_meta("Parse comma-separated integers into rows", &["generator"])
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("values", 0, "varchar", "Comma-separated integers")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: schema_value(), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let csv = params.arguments.const_str(0).unwrap_or_default();
        let values: Vec<i64> = csv
            .split(',')
            .filter_map(|s| s.trim().parse::<i64>().ok())
            .collect();
        Ok(Box::new(Countdown { values, offset: 0, batch_size: 1024, schema: schema_value() }))
    }
}

/// `make_series(step)` — 10 float values `0.0, step, 2*step, … 9*step`.
pub struct MakeSeriesFloat;
fn schema_value_f64() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("value", DataType::Float64, true)]))
}
struct FloatSeq {
    values: Vec<f64>,
    emitted: bool,
    schema: SchemaRef,
}
impl TableProducer for FloatSeq {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let arr = arrow_array::Float64Array::from(self.values.clone());
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), vec![Arc::new(arr)])
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
}
impl TableFunction for MakeSeriesFloat {
    fn name(&self) -> &str {
        "make_series"
    }
    fn metadata(&self) -> FunctionMetadata {
        gen_meta("Generate 10 float values with given step size", &["generator"])
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("step", 0, "float64", "Step size between values")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: schema_value_f64(), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let step = params.arguments.const_f64(0).unwrap_or(1.0);
        let values: Vec<f64> = (0..10).map(|i| i as f64 * step).collect();
        Ok(Box::new(FloatSeq { values, emitted: false, schema: schema_value_f64() }))
    }
}

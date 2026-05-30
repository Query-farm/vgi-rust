//! Additional table-producer fixtures.

use std::sync::Arc;

use arrow_array::types::UInt32Type;
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, PrimitiveArray, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(ConstantColumnsFunction);
    w.register_table(ProjectedDataFunction);
    w.register_table(GeneratorExceptionFunction);
    w.register_table(LoggingGeneratorFunction);
    w.register_table(OrderEchoFunction);
    w.register_table(SampleEchoFunction);
    w.register_table(DoubleSequenceFunction);
    w.register_table(MakePairs::Int);
    w.register_table(MakePairs::Str);
    w.register_table(MakePairs::IntStr);
    w.register_table(RepeatValue::Int);
    w.register_table(RepeatValue::Str);
}

fn schema_n() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
}

/// `generator_exception(fail_after)` — emits one row per tick, raises after N.
pub struct GeneratorExceptionFunction;
impl TableFunction for GeneratorExceptionFunction {
    fn name(&self) -> &str {
        "generator_exception"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Raises an exception after N batches for testing".to_string(),
            categories: vec!["testing".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("fail_after", 0, "int64", "Batches before failure")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: schema_n(), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(GenExc {
            fail_after: params.arguments.const_i64(0).unwrap_or(0),
            count: 0,
            schema: schema_n(),
        }))
    }
}
struct GenExc {
    fail_after: i64,
    count: i64,
    schema: SchemaRef,
}
impl TableProducer for GenExc {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.count >= self.fail_after {
            return Err(RpcError::value_error(format!(
                "Intentional failure after {} batches",
                self.fail_after
            )));
        }
        let arr = Int64Array::from(vec![self.count]);
        self.count += 1;
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), vec![Arc::new(arr)])
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
}

/// `logging_generator(count)` — emits log messages while generating.
pub struct LoggingGeneratorFunction;
impl TableFunction for LoggingGeneratorFunction {
    fn name(&self) -> &str {
        "logging_generator"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Emits log messages during generation".to_string(),
            categories: vec!["testing".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "Number of values to generate")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: schema_n(), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(LogGen {
            count: params.arguments.const_i64(0).unwrap_or(0).max(0),
            index: 0,
            schema: schema_n(),
        }))
    }
}
struct LogGen {
    count: i64,
    index: i64,
    schema: SchemaRef,
}
impl TableProducer for LogGen {
    fn next_batch(&mut self, out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.index == 0 {
            out.client_log(
                vgi_rpc::LogLevel::Info,
                format!("Starting generation of {} values", self.count),
            );
        }
        if self.index >= self.count {
            out.client_log(vgi_rpc::LogLevel::Info, "Generation complete");
            return Ok(None);
        }
        let arr = Int64Array::from(vec![self.index]);
        self.index += 1;
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), vec![Arc::new(arr)])
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
}

fn gen_meta(desc: &str, projection: bool) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        categories: vec!["generator".into(), "utility".into()],
        projection_pushdown: projection,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// constant_columns(count, values...) — dynamic schema, one col per vararg.
// ---------------------------------------------------------------------------

pub struct ConstantColumnsFunction;

impl ConstantColumnsFunction {
    fn output_schema(params: &BindParams) -> SchemaRef {
        let mut fields = Vec::new();
        for i in 1..params.arguments.num_positional() {
            if let Some(a) = params.arguments.arg(i) {
                let name = format!("col_{}", i - 1);
                // Preserve any field-level metadata (e.g. DuckDB lossless
                // `ARROW:extension:name` for HUGEINT/UUID) so the value decodes
                // back to its original DuckDB type rather than a raw BLOB.
                let meta = params
                    .arguments
                    .arg_field(i)
                    .map(|f| f.metadata().clone())
                    .unwrap_or_default();
                fields.push(
                    Field::new(name, a.data_type().clone(), true).with_metadata(meta),
                );
            }
        }
        Arc::new(Schema::new(fields))
    }
}

impl TableFunction for ConstantColumnsFunction {
    fn name(&self) -> &str {
        "constant_columns"
    }
    fn metadata(&self) -> FunctionMetadata {
        gen_meta("Generates rows with constant values from varargs", false)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows to generate"),
            ArgSpec::any_column("values", 1, "Values to fill each column")
                .varargs()
                .as_const(),
        ]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Self::output_schema(params),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<vgi::table_function::TableCardinality> {
        let c = params.arguments.const_i64(0)?;
        Some(vgi::table_function::TableCardinality {
            estimate: Some(c),
            max: Some(c),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        // The const vararg values are positional args 1..
        let mut values = Vec::new();
        for i in 1..params.arguments.num_positional() {
            if let Some(a) = params.arguments.arg(i) {
                values.push(a.clone());
            }
        }
        Ok(Box::new(ConstantColumns {
            remaining: count,
            values,
            schema: params.output_schema.clone(),
            batch_size: 2048,
        }))
    }
}

struct ConstantColumns {
    remaining: i64,
    values: Vec<ArrayRef>,
    schema: SchemaRef,
    batch_size: i64,
}
impl TableProducer for ConstantColumns {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        let n = self.remaining.min(self.batch_size) as usize;
        let indices = UInt32Array::from(vec![0u32; n]);
        let cols: Vec<ArrayRef> = self
            .values
            .iter()
            .map(|v| {
                arrow_select::take::take(v, &indices, None)
                    .map_err(|e| RpcError::runtime_error(e.to_string()))
            })
            .collect::<Result<_>>()?;
        self.remaining -= n as i64;
        let b = RecordBatch::try_new(self.schema.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(b))
    }
}

// ---------------------------------------------------------------------------
// projected_data(count) — {id, name, value, extra}, projection pushdown.
// ---------------------------------------------------------------------------

pub struct ProjectedDataFunction;
impl TableFunction for ProjectedDataFunction {
    fn name(&self) -> &str {
        "projected_data"
    }
    fn metadata(&self) -> FunctionMetadata {
        gen_meta("Generates data with 4 columns, supporting projection pushdown", true)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "Number of rows to generate")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("value", DataType::Float64, true),
                Field::new("extra", DataType::Int64, true),
            ])),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<vgi::table_function::TableCardinality> {
        let c = params.arguments.const_i64(0)?;
        Some(vgi::table_function::TableCardinality {
            estimate: Some(c),
            max: Some(c),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(ProjectedData {
            remaining: params.arguments.const_i64(0).unwrap_or(0).max(0),
            current: 0,
            schema: params.output_schema.clone(),
            batch_size: 1000,
        }))
    }
}

struct ProjectedData {
    remaining: i64,
    current: i64,
    schema: SchemaRef,
    batch_size: i64,
}
impl TableProducer for ProjectedData {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        let n = self.remaining.min(self.batch_size);
        let start = self.current;
        let cols: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|f| -> ArrayRef {
                match f.name().as_str() {
                    "id" => Arc::new(Int64Array::from_iter_values(start..start + n)),
                    "name" => Arc::new(StringArray::from(
                        (start..start + n).map(|i| format!("item_{i}")).collect::<Vec<_>>(),
                    )),
                    "value" => Arc::new(Float64Array::from_iter_values(
                        (start..start + n).map(|i| i as f64 * 1.5),
                    )),
                    "extra" => Arc::new(PrimitiveArray::<arrow_array::types::Int64Type>::from_iter_values(
                        (start..start + n).map(|i| i * i),
                    )),
                    _ => Arc::new(Int64Array::from_iter_values(start..start + n)),
                }
            })
            .collect();
        self.current += n;
        self.remaining -= n;
        let b = RecordBatch::try_new(self.schema.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(b))
    }
}

// keep type imports used
#[allow(dead_code)]
fn _u(_: PrimitiveArray<UInt32Type>) {}

// ---------------------------------------------------------------------------
// order_echo / sample_echo — echo ORDER BY / TABLESAMPLE pushdown hints.
// ---------------------------------------------------------------------------

fn diag_schema(extra: &[(&str, DataType)]) -> SchemaRef {
    let mut f = vec![
        Field::new("n", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ];
    for (name, ty) in extra {
        f.push(Field::new(*name, ty.clone(), true));
    }
    Arc::new(Schema::new(f))
}

pub struct OrderEchoFunction;
impl TableFunction for OrderEchoFunction {
    fn name(&self) -> &str { "order_echo" }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = gen_meta("Echoes ORDER BY + LIMIT pushdown hints in output", false);
        m.projection_pushdown = true;
        m.filter_pushdown = true;
        m.auto_apply_filters = true;
        m.categories = vec!["generator".into(), "diagnostic".into()];
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows"),
            ArgSpec::const_arg("batch_size", -1, "int64", "Batch size"),
        ]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: diag_schema(&[
                ("order_column", DataType::Utf8),
                ("order_direction", DataType::Utf8),
                ("order_null_order", DataType::Utf8),
                ("order_limit", DataType::Int64),
            ]),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, p: &BindParams) -> Option<vgi::table_function::TableCardinality> {
        let c = p.arguments.const_i64(0)?;
        Some(vgi::table_function::TableCardinality { estimate: Some(c), max: Some(c) })
    }
    fn producer(&self, p: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(DiagEcho {
            remaining: p.arguments.const_i64(0).unwrap_or(0).max(0),
            current: 0,
            batch_size: p.arguments.named_i64("batch_size").unwrap_or(2048).max(1),
            schema: p.output_schema.clone(),
            strs: vec![
                p.order_by_column.clone().unwrap_or_else(|| "(none)".into()),
                p.order_by_direction.clone().unwrap_or_else(|| "(none)".into()),
                p.order_by_null_order.clone().unwrap_or_else(|| "(none)".into()),
            ],
            ints: vec![p.order_by_limit.unwrap_or(-1)],
            floats: vec![],
        }))
    }
}

pub struct SampleEchoFunction;
impl TableFunction for SampleEchoFunction {
    fn name(&self) -> &str { "sample_echo" }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            sampling_pushdown: true,
            ..gen_meta("Echoes TABLESAMPLE pushdown hints in output", false)
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows"),
            ArgSpec::const_arg("batch_size", -1, "int64", "Batch size"),
        ]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: diag_schema(&[
                ("sample_percentage", DataType::Float64),
                ("sample_seed", DataType::Int64),
            ]),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, p: &BindParams) -> Option<vgi::table_function::TableCardinality> {
        let c = p.arguments.const_i64(0)?;
        Some(vgi::table_function::TableCardinality { estimate: Some(c), max: Some(c) })
    }
    fn producer(&self, p: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(DiagEcho {
            remaining: p.arguments.const_i64(0).unwrap_or(0).max(0),
            current: 0,
            batch_size: p.arguments.named_i64("batch_size").unwrap_or(2048).max(1),
            schema: p.output_schema.clone(),
            strs: vec![],
            ints: vec![p.tablesample_seed.unwrap_or(-1)],
            floats: vec![p.tablesample_percentage.unwrap_or(-1.0)],
        }))
    }
}

struct DiagEcho {
    remaining: i64,
    current: i64,
    batch_size: i64,
    schema: SchemaRef,
    strs: Vec<String>,   // order_column/direction/null_order OR none (sample)
    ints: Vec<i64>,      // order_limit OR sample_seed
    floats: Vec<f64>,    // sample_percentage OR none
}
impl TableProducer for DiagEcho {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        let n = self.remaining.min(self.batch_size);
        let start = self.current;
        // Build only the columns present in `self.schema` (already narrowed by
        // projection pushdown), keyed by field name.
        let cols: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|f| -> ArrayRef {
                match f.name().as_str() {
                    "n" => Arc::new(Int64Array::from_iter_values(start..start + n)),
                    "s" => Arc::new(StringArray::from(
                        (start..start + n).map(|i| format!("row_{i}")).collect::<Vec<_>>(),
                    )),
                    "order_column" => rep_str(&self.strs[0], n),
                    "order_direction" => rep_str(&self.strs[1], n),
                    "order_null_order" => rep_str(&self.strs[2], n),
                    "order_limit" => Arc::new(Int64Array::from(vec![self.ints[0]; n as usize])),
                    "sample_percentage" => Arc::new(Float64Array::from(vec![self.floats[0]; n as usize])),
                    "sample_seed" => Arc::new(Int64Array::from(vec![self.ints[0]; n as usize])),
                    _ => Arc::new(Int64Array::from(vec![0i64; n as usize])),
                }
            })
            .collect();
        self.current += n;
        self.remaining -= n;
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), cols)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
}
fn rep_str(s: &str, n: i64) -> ArrayRef {
    Arc::new(StringArray::from(vec![s.to_string(); n as usize]))
}

// ---------------------------------------------------------------------------
// double_sequence(count, increment := 1.0) -> {n: float64}
// ---------------------------------------------------------------------------

pub struct DoubleSequenceFunction;
impl TableFunction for DoubleSequenceFunction {
    fn name(&self) -> &str { "double_sequence" }
    fn metadata(&self) -> FunctionMetadata {
        gen_meta("Generates a sequence of floating-point numbers from 0 to n-1", false)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows"),
            ArgSpec::const_arg("batch_size", -1, "int64", "Batch size"),
            ArgSpec::const_arg("increment", -1, "float64", "Step between values"),
        ]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Float64, true)])),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, p: &BindParams) -> Option<vgi::table_function::TableCardinality> {
        let c = p.arguments.const_i64(0)?;
        Some(vgi::table_function::TableCardinality { estimate: Some(c), max: Some(c) })
    }
    fn statistics(&self, p: &BindParams) -> Option<Vec<vgi::statistics::CatColStat>> {
        let count = p.arguments.const_i64(0)?.max(0);
        let inc = p.arguments.named_f64("increment").unwrap_or(1.0);
        let max = if count == 0 { 0.0 } else { (count - 1) as f64 * inc };
        Some(vec![vgi::statistics::CatColStat {
            column_name: "n".to_string(),
            min: vgi::statistics::StatValue::Float64(0.0_f64.min(max)),
            max: vgi::statistics::StatValue::Float64(0.0_f64.max(max)),
            has_null: false,
            has_not_null: true,
            distinct_count: Some(count),
            contains_unicode: None,
            max_string_length: None,
        }])
    }
    fn producer(&self, p: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let count = p.arguments.const_i64(0).unwrap_or(0).max(0);
        let inc = p.arguments.named_f64("increment").unwrap_or(1.0);
        let bs = p.arguments.named_i64("batch_size").unwrap_or(1000).max(1);
        Ok(Box::new(DoubleSeq {
            count, inc, bs, current: 0,
            schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Float64, true)])),
        }))
    }
}
struct DoubleSeq { count: i64, inc: f64, bs: i64, current: i64, schema: SchemaRef }
impl TableProducer for DoubleSeq {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.current >= self.count {
            return Ok(None);
        }
        let n = (self.count - self.current).min(self.bs);
        let arr = Float64Array::from_iter_values((self.current..self.current + n).map(|i| i as f64 * self.inc));
        self.current += n;
        Ok(Some(RecordBatch::try_new(self.schema.clone(), vec![Arc::new(arr)])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?))
    }
}

// ---------------------------------------------------------------------------
// make_pairs overloads -> {a, b}
// ---------------------------------------------------------------------------

pub enum MakePairs { Int, Str, IntStr }
impl TableFunction for MakePairs {
    fn name(&self) -> &str { "make_pairs" }
    fn metadata(&self) -> FunctionMetadata { gen_meta("Generate pairs", false) }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        match self {
            MakePairs::Int => vec![
                ArgSpec::const_arg("start", 0, "int64", "Start"),
                ArgSpec::const_arg("stop", 1, "int64", "Stop"),
            ],
            MakePairs::Str => vec![
                ArgSpec::const_arg("prefix", 0, "varchar", "Prefix"),
                ArgSpec::const_arg("suffix", 1, "varchar", "Suffix"),
            ],
            MakePairs::IntStr => vec![
                ArgSpec::const_arg("start", 0, "int64", "Start"),
                ArgSpec::const_arg("label", 1, "varchar", "Label"),
            ],
        }
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        let s = match self {
            MakePairs::Int => Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Int64, true),
            ]),
            MakePairs::Str => Schema::new(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Utf8, true),
            ]),
            MakePairs::IntStr => Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Utf8, true),
            ]),
        };
        Ok(BindResponse { output_schema: Arc::new(s), opaque_data: Vec::new() })
    }
    fn producer(&self, p: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let schema = p.output_schema.clone();
        let (a, b): (ArrayRef, ArrayRef) = match self {
            MakePairs::Int => {
                let start = p.arguments.const_i64(0).unwrap_or(0);
                let stop = p.arguments.const_i64(1).unwrap_or(0);
                let vals: Vec<i64> = (start..stop).collect();
                let bvals: Vec<i64> = vals.iter().map(|v| v * 2).collect();
                (Arc::new(Int64Array::from(vals)), Arc::new(Int64Array::from(bvals)))
            }
            MakePairs::Str => {
                let prefix = p.arguments.const_str(0).unwrap_or_default();
                let suffix = p.arguments.const_str(1).unwrap_or_default();
                let a: Vec<String> = (0..5).map(|i| format!("{prefix}{i}")).collect();
                let b: Vec<String> = (0..5).map(|i| format!("{suffix}{i}")).collect();
                (Arc::new(StringArray::from(a)), Arc::new(StringArray::from(b)))
            }
            MakePairs::IntStr => {
                let start = p.arguments.const_i64(0).unwrap_or(0);
                let label = p.arguments.const_str(1).unwrap_or_default();
                let a: Vec<i64> = (0..5).map(|i| start + i).collect();
                let b: Vec<String> = (0..5).map(|i| format!("{label}{i}")).collect();
                (Arc::new(Int64Array::from(a)), Arc::new(StringArray::from(b)))
            }
        };
        Ok(Box::new(OneShot { batch: Some(RecordBatch::try_new(schema, vec![a, b])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?) }))
    }
}

/// Emits a single pre-built batch then finishes.
struct OneShot { batch: Option<RecordBatch> }
impl TableProducer for OneShot {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
}

// ---------------------------------------------------------------------------
// repeat_value(count, values...) -> {v0, v1, ...} N rows
// ---------------------------------------------------------------------------

pub enum RepeatValue { Int, Str }
impl TableFunction for RepeatValue {
    fn name(&self) -> &str { "repeat_value" }
    fn metadata(&self) -> FunctionMetadata { gen_meta("Repeat values for N rows", false) }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        let vty = match self { RepeatValue::Int => "int64", RepeatValue::Str => "varchar" };
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows"),
            ArgSpec::const_arg("values", 1, vty, "Values to repeat").varargs(),
        ]
    }
    fn on_bind(&self, p: &BindParams) -> Result<BindResponse> {
        let ty = match self { RepeatValue::Int => DataType::Int64, RepeatValue::Str => DataType::Utf8 };
        let fields: Vec<Field> = (1..p.arguments.num_positional())
            .map(|i| Field::new(format!("v{}", i - 1), ty.clone(), true))
            .collect();
        Ok(BindResponse { output_schema: Arc::new(Schema::new(fields)), opaque_data: Vec::new() })
    }
    fn producer(&self, p: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let count = p.arguments.const_i64(0).unwrap_or(0).max(0) as usize;
        let indices = UInt32Array::from(vec![0u32; count]);
        let cols: Vec<ArrayRef> = (1..p.arguments.num_positional())
            .filter_map(|i| p.arguments.arg(i).cloned())
            .map(|v| arrow_select::take::take(&v, &indices, None)
                .map_err(|e| RpcError::runtime_error(e.to_string())))
            .collect::<Result<_>>()?;
        let batch = RecordBatch::try_new(p.output_schema.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Box::new(OneShot { batch: Some(batch) }))
    }
}

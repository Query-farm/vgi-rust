//! Table-buffering example fixtures.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use vgi::buffering::{BufferingParams, TableBufferingFunction};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata};
use vgi::ipc;
use vgi::table_function::TableProducer;
use vgi_rpc::{Result, RpcError};

/// Register buffering fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_buffering(BufferInputFunction { name: "buffer_input", pushdown: false });
    w.register_buffering(BufferInputFunction { name: "echo_buffering", pushdown: true });
    w.register_buffering(SumAllColumnsFunction);
}

fn table_arg() -> ArgSpec {
    ArgSpec::column("data", 0, "table", "Input table")
}

const NS: &[u8] = b"buf";

/// Drains the per-execution buffered batch log, one batch per tick.
struct LogDrain {
    storage: Arc<vgi::buffering::BufferingStore>,
    execution_id: Vec<u8>,
    after_id: i64,
    output_schema: arrow_schema::SchemaRef,
}
impl TableProducer for LogDrain {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        let rows = self
            .storage
            .scan(&self.execution_id, NS, b"", self.after_id, 1);
        let Some((id, value)) = rows.into_iter().next() else {
            return Ok(None);
        };
        self.after_id = id;
        let batch = ipc::read_batch(&value)?;
        // Narrow to the (possibly projected) output schema by name.
        let batch = vgi::table_in_out::project_batch(&batch, &self.output_schema)?;
        Ok(Some(batch))
    }
}

/// `buffer_input` / `echo_buffering` — buffer all input, emit on finalize.
pub struct BufferInputFunction {
    name: &'static str,
    pushdown: bool,
}

impl TableBufferingFunction for BufferInputFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Collects all input batches and emits during finalization".to_string(),
            categories: vec!["utility".into(), "buffer".into()],
            projection_pushdown: self.pushdown,
            filter_pushdown: self.pushdown,
            auto_apply_filters: self.pushdown,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg()]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("buffering requires input schema"))?;
        Ok(BindResponse {
            output_schema: input,
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &BufferingParams, batch: &RecordBatch) -> Result<Vec<u8>> {
        let bytes = ipc::write_batch(batch)?;
        params
            .storage
            .append(&params.execution_id, NS, b"", bytes);
        Ok(params.execution_id.clone())
    }
    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        // Every state_id is the execution_id; collapse to one finalize stream.
        Ok(vec![params.execution_id.clone()])
    }
    fn finalize_producer(
        &self,
        params: &BufferingParams,
        _finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(LogDrain {
            storage: params.storage.clone(),
            execution_id: params.execution_id.clone(),
            after_id: -1,
            output_schema: params.output_schema.clone(),
        }))
    }
}

/// `sum_all_columns` — column-wise sum across all input batches. Integer
/// columns promote to int64, floating/decimal to float64; non-numeric columns
/// are dropped. Accumulate per-batch partials in `process`, reduce them in
/// `combine`, and emit the single summary row in `finalize`.
pub struct SumAllColumnsFunction;

const PARTIAL_NS: &[u8] = b"partial";

impl SumAllColumnsFunction {
    /// Sum one (already int64/float64-typed) column to a 1-element array of
    /// the same type.
    fn sum_column(field_type: &DataType, col: &ArrayRef) -> Result<ArrayRef> {
        let cast = arrow_cast::cast(col, field_type)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        match field_type {
            DataType::Int64 => {
                let a = cast.as_primitive::<Int64Type>();
                let s: i64 = (0..a.len()).filter(|&i| a.is_valid(i)).map(|i| a.value(i)).sum();
                Ok(Arc::new(Int64Array::from(vec![s])) as ArrayRef)
            }
            DataType::Float64 => {
                let a = cast.as_primitive::<Float64Type>();
                let s: f64 = (0..a.len()).filter(|&i| a.is_valid(i)).map(|i| a.value(i)).sum();
                Ok(Arc::new(Float64Array::from(vec![s])) as ArrayRef)
            }
            other => Err(RpcError::runtime_error(format!(
                "sum_all_columns: unsupported output type {other}"
            ))),
        }
    }
}

impl TableBufferingFunction for SumAllColumnsFunction {
    fn name(&self) -> &str {
        "sum_all_columns"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Computes column-wise sums across all batches".to_string(),
            categories: vec!["aggregation".into(), "numeric".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg()]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("sum_all_columns requires input schema"))?;
        let mut fields: Vec<Field> = Vec::new();
        for f in input.fields() {
            let t = f.data_type();
            let out = if t.is_integer() {
                DataType::Int64
            } else if matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64)
                || matches!(t, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
            {
                DataType::Float64
            } else {
                continue;
            };
            fields.push(Field::new(f.name(), out, true));
        }
        if fields.is_empty() {
            let summary: Vec<String> = input
                .fields()
                .iter()
                .map(|f| format!("{}: {}", f.name(), f.data_type()))
                .collect();
            return Err(RpcError::value_error(format!(
                "sum_all_columns requires at least one numeric (integer, \
                 floating-point, or decimal) input column, got [{}]",
                summary.join(", ")
            )));
        }
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(fields)),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &BufferingParams, batch: &RecordBatch) -> Result<Vec<u8>> {
        let out = &params.output_schema;
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(out.fields().len());
        for f in out.fields() {
            let col = batch
                .column_by_name(f.name())
                .ok_or_else(|| RpcError::runtime_error(format!("missing column {}", f.name())))?;
            cols.push(Self::sum_column(f.data_type(), col)?);
        }
        let partial = RecordBatch::try_new(out.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        params
            .storage
            .append(&params.execution_id, PARTIAL_NS, b"", ipc::write_batch(&partial)?);
        Ok(params.execution_id.clone())
    }
    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        let out = &params.output_schema;
        // Accumulate per-column running totals (int64 or float64).
        let mut int_acc: Vec<i64> = vec![0; out.fields().len()];
        let mut flt_acc: Vec<f64> = vec![0.0; out.fields().len()];
        for (_id, blob) in params.storage.scan(&params.execution_id, PARTIAL_NS, b"", -1, usize::MAX) {
            let pb = ipc::read_batch(&blob)?;
            for (i, f) in out.fields().iter().enumerate() {
                let c = pb.column(i);
                match f.data_type() {
                    DataType::Int64 => int_acc[i] += c.as_primitive::<Int64Type>().value(0),
                    DataType::Float64 => flt_acc[i] += c.as_primitive::<Float64Type>().value(0),
                    _ => {}
                }
            }
        }
        let cols: Vec<ArrayRef> = out
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| match f.data_type() {
                DataType::Int64 => Arc::new(Int64Array::from(vec![int_acc[i]])) as ArrayRef,
                _ => Arc::new(Float64Array::from(vec![flt_acc[i]])) as ArrayRef,
            })
            .collect();
        let merged = RecordBatch::try_new(out.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        params
            .storage
            .append(&params.execution_id, NS, b"", ipc::write_batch(&merged)?);
        Ok(vec![params.execution_id.clone()])
    }
    fn finalize_producer(
        &self,
        params: &BufferingParams,
        _finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(LogDrain {
            storage: params.storage.clone(),
            execution_id: params.execution_id.clone(),
            after_id: -1,
            output_schema: params.output_schema.clone(),
        }))
    }
}

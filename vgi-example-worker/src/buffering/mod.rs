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
    w.register_buffering(BufferInputFunction::new("buffer_input"));
    w.register_buffering(BufferInputFunction::new("echo_buffering").pushdown());
    w.register_buffering(BufferInputFunction::new("ordered_buffer_input").sink_ordered());
    w.register_buffering(
        BufferInputFunction::new("crash_on_combine")
            .combine_error("Intentional exception during combine()"),
    );
    w.register_buffering(
        BufferInputFunction::new("crash_on_finalize")
            .finalize_error("Intentional exception during finalize()"),
    );
    w.register_buffering(SumAllColumnsFunction::new("sum_all_columns"));
    w.register_buffering(
        SumAllColumnsFunction::new("exception_finalize")
            .finalize_error("Intentional exception during finalize()"),
    );
    w.register_buffering(SumAllColumnsFunction::new("exception_process").process_every_other());
    w.register_buffering(LargeStateFunction);
    w.register_buffering(BatchIndexBufferInputFunction);
    w.register_buffering(OrderedSourceFunction);
}

fn table_arg() -> ArgSpec {
    ArgSpec::column("data", 0, "table", "Input table")
}

const NS: &[u8] = b"buf";

/// Drains a per-execution batch log, one batch per tick. An optional
/// `error` makes the first tick fail (crash_on_finalize / exception_finalize).
struct LogDrain {
    storage: Arc<vgi::buffering::BufferingStore>,
    execution_id: Vec<u8>,
    ns: &'static [u8],
    after_id: i64,
    output_schema: arrow_schema::SchemaRef,
    error: Option<String>,
}
impl TableProducer for LogDrain {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if let Some(msg) = self.error.take() {
            return Err(RpcError::value_error(msg));
        }
        let rows = self.storage.scan(&self.execution_id, self.ns, b"", self.after_id, 1);
        let Some((id, value)) = rows.into_iter().next() else {
            return Ok(None);
        };
        self.after_id = id;
        let batch = ipc::read_batch(&value)?;
        let batch = vgi::table_in_out::project_batch(&batch, &self.output_schema)?;
        Ok(Some(batch))
    }
}

/// Emits exactly one row `{v: <value>}` (ordered_source per finalize_state_id).
struct OneRowProducer {
    output_schema: arrow_schema::SchemaRef,
    value: i64,
    emitted: bool,
}
impl TableProducer for OneRowProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let col = Arc::new(Int64Array::from(vec![self.value])) as ArrayRef;
        Ok(Some(
            RecordBatch::try_new(self.output_schema.clone(), vec![col])
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
}

/// `buffer_input` family — buffer all input, emit on finalize. Config knobs
/// cover the pushdown / ordering / error-injection variants.
pub struct BufferInputFunction {
    name: &'static str,
    pushdown: bool,
    sink_ordered: bool,
    combine_error: Option<&'static str>,
    finalize_error: Option<&'static str>,
}

impl BufferInputFunction {
    fn new(name: &'static str) -> Self {
        BufferInputFunction {
            name,
            pushdown: false,
            sink_ordered: false,
            combine_error: None,
            finalize_error: None,
        }
    }
    fn pushdown(mut self) -> Self {
        self.pushdown = true;
        self
    }
    fn sink_ordered(mut self) -> Self {
        self.sink_ordered = true;
        self
    }
    fn combine_error(mut self, msg: &'static str) -> Self {
        self.combine_error = Some(msg);
        self
    }
    fn finalize_error(mut self, msg: &'static str) -> Self {
        self.finalize_error = Some(msg);
        self
    }
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
            sink_order_dependent: self.sink_ordered,
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
        params
            .storage
            .append(&params.execution_id, NS, b"", ipc::write_batch(batch)?);
        Ok(params.execution_id.clone())
    }
    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        if let Some(msg) = self.combine_error {
            return Err(RpcError::value_error(msg));
        }
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
            ns: NS,
            after_id: -1,
            output_schema: params.output_schema.clone(),
            error: self.finalize_error.map(|s| s.to_string()),
        }))
    }
}

/// `large_state` — append 1 MB per process call; finalize emits one row whose
/// every (passthrough-schema) column carries the total buffered byte count.
pub struct LargeStateFunction;

impl TableBufferingFunction for LargeStateFunction {
    fn name(&self) -> &str {
        "large_state"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Buffers ~1 MB per input batch into state (IPC test)".to_string(),
            categories: vec!["test".into(), "memory".into()],
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
            .ok_or_else(|| RpcError::value_error("large_state requires input schema"))?;
        Ok(BindResponse {
            output_schema: input,
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &BufferingParams, _batch: &RecordBatch) -> Result<Vec<u8>> {
        params
            .storage
            .append(&params.execution_id, b"large", b"", vec![0u8; 1024 * 1024]);
        Ok(params.execution_id.clone())
    }
    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        let total: i64 = params
            .storage
            .scan(&params.execution_id, b"large", b"", -1, usize::MAX)
            .iter()
            .map(|(_, b)| b.len() as i64)
            .sum();
        let out = &params.output_schema;
        let cols: Vec<ArrayRef> = out
            .fields()
            .iter()
            .map(|f| {
                let base = Arc::new(Int64Array::from(vec![total])) as ArrayRef;
                arrow_cast::cast(&base, f.data_type())
                    .map_err(|e| RpcError::runtime_error(e.to_string()))
            })
            .collect::<Result<_>>()?;
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
            ns: NS,
            after_id: -1,
            output_schema: params.output_schema.clone(),
            error: None,
        }))
    }
}

/// `batch_index_buffer_input` — requires the per-chunk batch_index, packs it
/// alongside each buffered batch, then re-orders globally during combine.
pub struct BatchIndexBufferInputFunction;

fn pack_indexed(batch_index: i64, bytes: &[u8]) -> Vec<u8> {
    let mut v = batch_index.to_le_bytes().to_vec();
    v.extend_from_slice(bytes);
    v
}
fn unpack_indexed(blob: &[u8]) -> (i64, Vec<u8>) {
    let mut a = [0u8; 8];
    a.copy_from_slice(&blob[..8]);
    (i64::from_le_bytes(a), blob[8..].to_vec())
}

impl TableBufferingFunction for BatchIndexBufferInputFunction {
    fn name(&self) -> &str {
        "batch_index_buffer_input"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "buffer_input variant using batch_index to reconstruct order".to_string(),
            categories: vec!["test".into(), "ordering".into()],
            requires_input_batch_index: true,
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
            .ok_or_else(|| RpcError::value_error("requires input schema"))?;
        Ok(BindResponse {
            output_schema: input,
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &BufferingParams, batch: &RecordBatch) -> Result<Vec<u8>> {
        let idx = params.batch_index.ok_or_else(|| {
            RpcError::runtime_error(
                "batch_index_buffer_input.process() received batch_index=None \
                 — requires_input_batch_index plumbing is broken",
            )
        })?;
        params.storage.append(
            &params.execution_id,
            b"unsorted",
            b"",
            pack_indexed(idx, &ipc::write_batch(batch)?),
        );
        Ok(params.execution_id.clone())
    }
    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        let mut pairs: Vec<(i64, Vec<u8>)> = params
            .storage
            .scan(&params.execution_id, b"unsorted", b"", -1, usize::MAX)
            .iter()
            .map(|(_, v)| unpack_indexed(v))
            .collect();
        pairs.sort_by_key(|p| p.0);
        for (_idx, bytes) in pairs {
            params.storage.append(&params.execution_id, NS, b"", bytes);
        }
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
            ns: NS,
            after_id: -1,
            output_schema: params.output_schema.clone(),
            error: None,
        }))
    }
}

/// `ordered_source` — `source_order_dependent`; ignores input and emits a
/// fixed 0..15 sequence, one finalize_state_id (and one row) per value.
pub struct OrderedSourceFunction;
const ORDERED_SOURCE_N: u32 = 16;

impl TableBufferingFunction for OrderedSourceFunction {
    fn name(&self) -> &str {
        "ordered_source"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Emits a fixed 0..15 sequence via source_order_dependent=True".to_string(),
            categories: vec!["test".into(), "ordering".into()],
            source_order_dependent: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg()]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &BufferingParams, _batch: &RecordBatch) -> Result<Vec<u8>> {
        Ok(params.execution_id.clone())
    }
    fn combine(&self, _params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        Ok((0..ORDERED_SOURCE_N).map(|i| i.to_be_bytes().to_vec()).collect())
    }
    fn finalize_producer(
        &self,
        params: &BufferingParams,
        finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        let mut a = [0u8; 4];
        a.copy_from_slice(&finalize_state_id[..4.min(finalize_state_id.len())]);
        let value = u32::from_be_bytes(a) as i64;
        Ok(Box::new(OneRowProducer {
            output_schema: params.output_schema.clone(),
            value,
            emitted: false,
        }))
    }
}

/// `sum_all_columns` — column-wise sum across all input batches. Integer
/// columns promote to int64, floating/decimal to float64; non-numeric columns
/// are dropped. Accumulate per-batch partials in `process`, reduce them in
/// `combine`, and emit the single summary row in `finalize`. Config knobs add
/// the `exception_finalize` / `exception_process` error-injection variants.
pub struct SumAllColumnsFunction {
    name: &'static str,
    finalize_error: Option<&'static str>,
    process_every_other: bool,
}

const PARTIAL_NS: &[u8] = b"partial";

impl SumAllColumnsFunction {
    fn new(name: &'static str) -> Self {
        SumAllColumnsFunction {
            name,
            finalize_error: None,
            process_every_other: false,
        }
    }
    fn finalize_error(mut self, msg: &'static str) -> Self {
        self.finalize_error = Some(msg);
        self
    }
    fn process_every_other(mut self) -> Self {
        self.process_every_other = true;
        self
    }
    /// Sum one (already int64/float64-typed) column to a 1-element array.
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
        self.name
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
        if self.process_every_other {
            params.storage.append(&params.execution_id, b"count", b"", Vec::new());
            let count = params
                .storage
                .scan(&params.execution_id, b"count", b"", -1, usize::MAX)
                .len();
            if count % 2 == 0 {
                return Err(RpcError::value_error(format!(
                    "Intentional exception on batch {count}"
                )));
            }
            return Ok(params.execution_id.clone());
        }
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
            ns: NS,
            after_id: -1,
            output_schema: params.output_schema.clone(),
            error: self.finalize_error.map(|s| s.to_string()),
        }))
    }
}

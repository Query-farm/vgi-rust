// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Table-in-out example fixtures.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_in_out::{project_batch, TableInOutFunction};
use vgi_rpc::{Result, RpcError};

/// Register table-in-out fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_table_in_out(EchoFunction);
    w.register_table_in_out(EchoWitnessFunction);
    w.register_table_in_out(FilterBySettingFunction);
    w.register_table_in_out(RepeatInputsFunction);
    w.register_table_in_out(SumAllColumnsSimpleDistributed);
    w.register_table_in_out(SlowCancellableInOutFunction);
}

/// `slow_cancellable_inout(probe_path, input, sleep_ms)` — passthrough with a
/// per-batch sleep and an `on_cancel` probe (the cancel path is exercised by
/// the C++ extension; here it's a simple echo for registration + correctness).
pub struct SlowCancellableInOutFunction;
impl TableInOutFunction for SlowCancellableInOutFunction {
    fn name(&self) -> &str {
        "slow_cancellable_inout"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Slow table-in-out with on_cancel probe (test fixture)".to_string(),
            categories: vec!["test".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg(
                "probe_path",
                0,
                "varchar",
                "Path to append to when on_cancel fires",
            ),
            table_arg("data", 1),
            ArgSpec::const_arg("sleep_ms", -1, "int64", "Sleep per batch (ms)"),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let ms = params.arguments.named_i64("sleep_ms").unwrap_or(50).max(0);
        if ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(ms.min(50) as u64));
        }
        Ok(vec![project_batch(batch, &params.output_schema)?])
    }
}

/// `sum_all_columns_simple_distributed(input)` — distributed column-wise sum.
/// `process` accumulates each batch's partial sums into shared storage and
/// emits nothing; the `FINALIZE` phase merges all partials into one output row.
pub struct SumAllColumnsSimpleDistributed;

const DIST_NS: &[u8] = b"tio_partials";

impl SumAllColumnsSimpleDistributed {
    /// Numeric output schema: integers → int64, floats/decimals → float64.
    fn output_schema(input: &arrow_schema::SchemaRef) -> arrow_schema::SchemaRef {
        use arrow_schema::{DataType, Field, Schema};
        let fields: Vec<Field> = input
            .fields()
            .iter()
            .filter_map(|f| {
                let t = f.data_type();
                let out = if t.is_integer() {
                    DataType::Int64
                } else if matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64)
                    || matches!(t, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
                {
                    DataType::Float64
                } else {
                    return None;
                };
                Some(Field::new(f.name(), out, true))
            })
            .collect();
        Arc::new(Schema::new(fields))
    }
    fn sum_one(
        field_type: &arrow_schema::DataType,
        col: &arrow_array::ArrayRef,
    ) -> Result<arrow_array::ArrayRef> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float64Type, Int64Type};
        use arrow_array::{Float64Array, Int64Array};
        let cast = arrow_cast::cast(col, field_type)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        match field_type {
            arrow_schema::DataType::Int64 => {
                let a = cast.as_primitive::<Int64Type>();
                let s: i64 = (0..a.len())
                    .filter(|&i| a.is_valid(i))
                    .map(|i| a.value(i))
                    .sum();
                Ok(Arc::new(Int64Array::from(vec![s])))
            }
            _ => {
                let a = cast.as_primitive::<Float64Type>();
                let s: f64 = (0..a.len())
                    .filter(|&i| a.is_valid(i))
                    .map(|i| a.value(i))
                    .sum();
                Ok(Arc::new(Float64Array::from(vec![s])))
            }
        }
    }
}

impl TableInOutFunction for SumAllColumnsSimpleDistributed {
    fn name(&self) -> &str {
        "sum_all_columns_simple_distributed"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Distributed sum using simple callback API".to_string(),
            categories: vec!["aggregation".into(), "numeric".into(), "distributed".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg("data", 0)]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = params.input_schema.clone().ok_or_else(|| {
            RpcError::value_error("sum_all_columns_simple_distributed requires input")
        })?;
        Ok(BindResponse {
            output_schema: Self::output_schema(&input),
            opaque_data: Vec::new(),
        })
    }
    fn has_finish(&self) -> bool {
        true
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let out = &params.output_schema;
        let mut cols = Vec::with_capacity(out.fields().len());
        for f in out.fields() {
            let col = batch
                .column_by_name(f.name())
                .ok_or_else(|| RpcError::runtime_error(format!("missing column {}", f.name())))?;
            cols.push(Self::sum_one(f.data_type(), col)?);
        }
        let partial = RecordBatch::try_new(out.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        if let Some(store) = &params.storage {
            store.append(
                &params.execution_id,
                DIST_NS,
                b"",
                vgi::ipc::write_batch(&partial)?,
            );
        }
        // Emit nothing during processing (one empty batch satisfies the exchange).
        Ok(vec![RecordBatch::new_empty(out.clone())])
    }
    fn finish(&self, params: &ProcessParams) -> Result<Vec<RecordBatch>> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::{Float64Type, Int64Type};
        use arrow_array::{ArrayRef, Float64Array, Int64Array};
        use arrow_schema::DataType;
        let out = &params.output_schema;
        let n = out.fields().len();
        let mut int_acc = vec![0i64; n];
        let mut flt_acc = vec![0.0f64; n];
        if let Some(store) = &params.storage {
            for (_id, blob) in store.scan(&params.execution_id, DIST_NS, b"", -1, usize::MAX) {
                let pb = vgi::ipc::read_batch(&blob)?;
                for (i, f) in out.fields().iter().enumerate() {
                    match f.data_type() {
                        DataType::Int64 => {
                            int_acc[i] += pb.column(i).as_primitive::<Int64Type>().value(0)
                        }
                        _ => flt_acc[i] += pb.column(i).as_primitive::<Float64Type>().value(0),
                    }
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
        let batch = RecordBatch::try_new(out.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(vec![batch])
    }
}

fn table_arg(name: &str, pos: i32) -> ArgSpec {
    ArgSpec::column(name, pos, "table", "Input table")
}

/// `echo(input)` — passthrough with projection + filter pushdown.
pub struct EchoFunction;
impl TableInOutFunction for EchoFunction {
    fn name(&self) -> &str {
        "echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Passthrough function that emits each input batch unchanged".to_string(),
            categories: vec!["utility".into(), "debug".into()],
            tags: vec![
                ("category".into(), "debug".into()),
                ("type".into(), "passthrough".into()),
            ],
            projection_pushdown: true,
            filter_pushdown: true,
            auto_apply_filters: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg("data", 0)]
    }
}

/// `echo_witness(input)` — emits the observed (post-projection) column count.
pub struct EchoWitnessFunction;
impl TableInOutFunction for EchoWitnessFunction {
    fn name(&self) -> &str {
        "echo_witness"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Emits len(observed_output_schema) per column — projection probe"
                .to_string(),
            categories: vec!["test".into(), "pushdown".into()],
            projection_pushdown: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg("data", 0)]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let observed = params.output_schema.fields().len() as i64;
        let n = batch.num_rows();
        let cols: Vec<Arc<dyn Array>> = params
            .output_schema
            .fields()
            .iter()
            .map(|f| -> Result<Arc<dyn Array>> {
                let int = arrow_array::Int64Array::from(vec![observed; n]);
                arrow_cast::cast(&(Arc::new(int) as Arc<dyn Array>), f.data_type())
                    .map_err(|e| RpcError::runtime_error(e.to_string()))
            })
            .collect::<Result<_>>()?;
        let b = RecordBatch::try_new(params.output_schema.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(vec![b])
    }
}

/// `filter_by_setting(input)` — keep rows where `value` >= setting `threshold`.
pub struct FilterBySettingFunction;
impl TableInOutFunction for FilterBySettingFunction {
    fn name(&self) -> &str {
        "filter_by_setting"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Filter rows where value column >= threshold setting".to_string(),
            categories: vec!["transform".into(), "settings".into()],
            required_settings: vec!["threshold".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg("data", 0)]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let threshold = params.settings.get_i64("threshold").unwrap_or(0);
        let (idx, _) = batch
            .schema()
            .column_with_name("value")
            .ok_or_else(|| RpcError::runtime_error("filter_by_setting: no 'value' column"))?;
        let col = batch.column(idx);
        let scalar = arrow_array::Int64Array::from(vec![threshold]);
        let scalar = arrow_cast::cast(&(Arc::new(scalar) as Arc<dyn Array>), col.data_type())
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let mask = arrow_ord::cmp::gt_eq(col, &arrow_array::Scalar::new(scalar.slice(0, 1)))
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let out = arrow_select::filter::filter_record_batch(batch, &mask)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(vec![out])
    }
}

/// `repeat_inputs(repeat_count, input)` — duplicate each input batch N times.
pub struct RepeatInputsFunction;
impl TableInOutFunction for RepeatInputsFunction {
    fn name(&self) -> &str {
        "repeat_inputs"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Duplicates each input batch N times".to_string(),
            categories: vec!["transform".into(), "augmentation".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("repeat_count", 0, "int64", "Times to repeat each input"),
            table_arg("data", 1),
        ]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let n = params.arguments.const_i64(0).unwrap_or(1);
        if n < 1 {
            return Err(RpcError::value_error("Repeat count must be at least 1"));
        }
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("input_schema is required but was None"))?;
        Ok(BindResponse {
            output_schema: input,
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let n = params.arguments.const_i64(0).unwrap_or(1).max(1) as usize;
        let projected = project_batch(batch, &params.output_schema)?;
        let repeated: Vec<&RecordBatch> = std::iter::repeat(&projected).take(n).collect();
        let out = arrow_select::concat::concat_batches(&projected.schema(), repeated)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(vec![out])
    }
}

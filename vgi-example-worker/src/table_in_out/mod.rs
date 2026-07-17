// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Table-in-out example fixtures.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{Field, Schema};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_in_out::{project_batch, TableInOutFunction};
use vgi_rpc::{Result, RpcError};

/// Register table-in-out fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_table_in_out(EchoFunction);
    w.register_table_in_out(EchoWitnessFunction);
    w.register_table_in_out(FilterBySettingFunction);
    w.register_table_in_out(SecretInOutFunction);
    w.register_table_in_out(RepeatInputsFunction);
    w.register_table_in_out(SubstreamPartialSumFunction);
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

/// `substream_partial_sum(input)` — per-substream partial sum emitted at
/// finalize; proves parallel streaming FINALIZE (Phase A4).
///
/// A streaming table-in-out *with* a finalize is still a per-substream
/// operation under per-substream worker fan-out: `process` accumulates only
/// THIS substream's rows (emitting nothing), and `finish` emits ONE row = this
/// substream's partial sum. DuckDB fans the input across N substreams and
/// unions their finalize outputs, so the caller re-aggregates with an outer
/// `SELECT sum(...)` to get the global total — correct no matter how the rows
/// were partitioned. State is keyed by the client-minted `substream_id` when
/// present (stable across HTTP backends), else the substream's `execution_id`.
/// This is NOT a global cross-substream combine — that is a
/// `TableBufferingFunction` (see `sum_all_columns_simple_distributed`).
pub struct SubstreamPartialSumFunction;

const SS_NS: &[u8] = b"ss_partial";

impl SubstreamPartialSumFunction {
    /// The storage scope for this substream's accumulated partials.
    fn state_scope(params: &ProcessParams) -> Vec<u8> {
        params
            .substream_id
            .clone()
            .unwrap_or_else(|| params.execution_id.clone())
    }
}

impl TableInOutFunction for SubstreamPartialSumFunction {
    fn name(&self) -> &str {
        "substream_partial_sum"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description:
                "Per-substream partial sum emitted at finalize (parallel streaming finalize)"
                    .to_string(),
            categories: vec!["aggregation".into(), "numeric".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg("data", 0)]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("substream_partial_sum requires input"))?;
        let field = input
            .fields()
            .first()
            .ok_or_else(|| RpcError::value_error("substream_partial_sum requires a column"))?;
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new(
                field.name(),
                arrow_schema::DataType::Int64,
                true,
            )])),
            opaque_data: Vec::new(),
        })
    }
    fn has_finish(&self) -> bool {
        true
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        use arrow_array::cast::AsArray;
        use arrow_array::types::Int64Type;
        let cast = arrow_cast::cast(batch.column(0), &arrow_schema::DataType::Int64)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let a = cast.as_primitive::<Int64Type>();
        let s: i64 = (0..a.len())
            .filter(|&i| a.is_valid(i))
            .map(|i| a.value(i))
            .sum();
        if let Some(store) = &params.storage {
            store.append(
                &Self::state_scope(params),
                SS_NS,
                b"",
                s.to_le_bytes().to_vec(),
            );
        }
        // Accumulate only; emit nothing during processing.
        Ok(Vec::new())
    }
    fn finish(&self, params: &ProcessParams) -> Result<Vec<RecordBatch>> {
        // Sum THIS substream's accumulated partials (one per process call that
        // handled this substream's batches); their sum is this substream's partial.
        let mut total = 0i64;
        if let Some(store) = &params.storage {
            for (_id, blob) in store.scan(&Self::state_scope(params), SS_NS, b"", -1, usize::MAX) {
                if let Ok(arr) = <[u8; 8]>::try_from(blob.as_slice()) {
                    total += i64::from_le_bytes(arr);
                }
            }
        }
        let col = Arc::new(arrow_array::Int64Array::from(vec![total])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(params.output_schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(vec![batch])
    }
}

/// `secret_in_out(input)` — resolves the `vgi_example` secret in on_bind
/// (two-phase) and appends its `secret_string` value as a constant column on
/// every input row. Exercises the secret × table-in-out intersection: the bind
/// must retry with resolved secrets AND preserve the input schema, and the
/// resolved secret must reach `process` via `params.secrets`.
pub struct SecretInOutFunction;
impl TableInOutFunction for SecretInOutFunction {
    fn name(&self) -> &str {
        "secret_in_out"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Append a resolved secret value to each input row".to_string(),
            categories: vec!["transform".into(), "secret".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg("data", 0)]
    }
    fn secret_lookups(&self, _params: &BindParams) -> Vec<vgi::secrets::SecretLookup> {
        vec![vgi::secrets::SecretLookup {
            secret_type: "vgi_example".into(),
            scope: None,
            name: None,
        }]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("secret_in_out requires an input schema"))?;
        let mut fields: Vec<Field> = input.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.push(Field::new(
            "secret_string",
            arrow_schema::DataType::Utf8,
            true,
        ));
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(fields)),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let value = params
            .secrets
            .of_type("vgi_example")
            .next()
            .and_then(|m| m.get("secret_string").cloned());
        let n = batch.num_rows();
        let mut cols: Vec<Arc<dyn Array>> = batch.columns().to_vec();
        let secret_col: StringArray = std::iter::repeat_n(value.as_deref(), n).collect();
        cols.push(Arc::new(secret_col));
        let out = RecordBatch::try_new(params.output_schema.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(vec![out])
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

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


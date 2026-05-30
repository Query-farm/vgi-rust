//! Settings-aware table fixtures: read DuckDB settings (scalar + struct) and
//! reflect them in the generated rows.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableProducer, TableFunction};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(SettingsAwareFunction);
    w.register_table(StructSettingsFunction);
}

fn meta(desc: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        categories: vec!["generator".into(), "settings".into()],
        required_settings: Vec::new(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// settings_aware(count) -> {id, greeting, value} (+ details when verbose)
// ---------------------------------------------------------------------------

fn verbose(params: &BindParams) -> bool {
    params.settings.get_bool("vgi_verbose_mode").unwrap_or(false)
}
fn settings_aware_schema(verbose: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new("greeting", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ];
    if verbose {
        fields.push(Field::new("details", DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

struct SettingsAwareProducer {
    schema: SchemaRef,
    count: i64,
    greeting: String,
    multiplier: i64,
    verbose: bool,
    emitted: bool,
}
impl TableProducer for SettingsAwareProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let ids: Vec<i64> = (0..self.count).collect();
        let greetings: Vec<&str> = vec![self.greeting.as_str(); self.count as usize];
        let values: Vec<f64> = ids.iter().map(|i| *i as f64 * 2.5 * self.multiplier as f64).collect();
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(greetings)),
            Arc::new(Float64Array::from(values)),
        ];
        if self.verbose {
            let details: Vec<String> = ids.iter().map(|i| format!("row_{i}")).collect();
            cols.push(Arc::new(StringArray::from(details)));
        }
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), cols)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
}

pub struct SettingsAwareFunction;
impl TableFunction for SettingsAwareFunction {
    fn name(&self) -> &str {
        "settings_aware"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Echoes setting values in output columns")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "Number of rows")]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: settings_aware_schema(verbose(params)), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let v = params.settings.get_bool("vgi_verbose_mode").unwrap_or(false);
        Ok(Box::new(SettingsAwareProducer {
            schema: settings_aware_schema(v),
            count: params.arguments.const_i64(0).unwrap_or(0).max(0),
            greeting: params.settings.get_str("greeting").unwrap_or_else(|| "Hello".to_string()),
            multiplier: params.settings.get_i64("multiplier").unwrap_or(1),
            verbose: v,
            emitted: false,
        }))
    }
}

// ---------------------------------------------------------------------------
// struct_settings(count) -> {n, label} from the `config` struct setting
// ---------------------------------------------------------------------------

fn struct_settings_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

/// Read the `config` struct setting `{start, step, label}`.
fn read_config(params: &ProcessParams) -> (i64, i64, String) {
    let mut start = 0i64;
    let mut step = 1i64;
    let mut label = "item".to_string();
    if let Some(arr) = params.settings.get("config") {
        if let Some(sa) = arr.as_any().downcast_ref::<arrow_array::StructArray>() {
            if let Some(c) = sa.column_by_name("start").map(|c| arrow_cast::cast(c, &DataType::Int64)) {
                if let Ok(c) = c {
                    start = c.as_primitive::<Int64Type>().value(0);
                }
            }
            if let Some(Ok(c)) = sa.column_by_name("step").map(|c| arrow_cast::cast(c, &DataType::Int64)) {
                step = c.as_primitive::<Int64Type>().value(0);
            }
            if let Some(c) = sa.column_by_name("label") {
                if let Some(s) = c.as_string_opt::<i32>() {
                    label = s.value(0).to_string();
                }
            }
        }
    }
    (start, step, label)
}

struct StructSettingsProducer {
    schema: SchemaRef,
    count: i64,
    start: i64,
    step: i64,
    label: String,
    emitted: bool,
}
impl TableProducer for StructSettingsProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let ns: Vec<i64> = (0..self.count).map(|i| self.start + i * self.step).collect();
        let labels: Vec<String> = (0..self.count).map(|i| format!("{}_{i}", self.label)).collect();
        Ok(Some(
            RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int64Array::from(ns)) as ArrayRef, Arc::new(StringArray::from(labels)) as ArrayRef],
            )
            .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
}

pub struct StructSettingsFunction;
impl TableFunction for StructSettingsFunction {
    fn name(&self) -> &str {
        "struct_settings"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Sequence configured by a struct setting")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "Number of rows")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: struct_settings_schema(), opaque_data: Vec::new() })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let (start, step, label) = read_config(params);
        Ok(Box::new(StructSettingsProducer {
            schema: struct_settings_schema(),
            count: params.arguments.const_i64(0).unwrap_or(0).max(0),
            start,
            step,
            label,
            emitted: false,
        }))
    }
}

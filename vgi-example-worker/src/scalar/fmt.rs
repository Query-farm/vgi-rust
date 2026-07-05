// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Number/string formatting scalar fixtures (overloaded).

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use vgi::function::{ArgSpec, FunctionMetadata, ProcessParams, ScalarFunction};
use vgi_rpc::Result;

use super::util::{arc, result};

pub fn register(w: &mut vgi::Worker) {
    w.register_scalar(FormatNumber::Default);
    w.register_scalar(FormatNumber::Precision);
    w.register_scalar(FormatNumber::Full);
    w.register_scalar(SmartFormat::Width);
    w.register_scalar(SmartFormat::Prefix);
}

fn meta(desc: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        return_type: Some(DataType::Utf8),
        ..Default::default()
    }
}

fn value_col(batch: &RecordBatch) -> Vec<Option<f64>> {
    let casted = match arrow_cast::cast(batch.column(0), &DataType::Float64) {
        Ok(c) => c,
        Err(_) => return vec![None; batch.num_rows()],
    };
    let a = casted.as_primitive::<Float64Type>();
    (0..a.len())
        .map(|i| (!a.is_null(i)).then(|| a.value(i)))
        .collect()
}

/// `format_number` overloads: default / (precision) / (precision, prefix).
pub enum FormatNumber {
    Default,
    Precision,
    Full,
}
impl ScalarFunction for FormatNumber {
    fn name(&self) -> &str {
        "format_number"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta(match self {
            FormatNumber::Default => "Format number with default precision (0 decimals)",
            FormatNumber::Precision => "Format number with specified precision",
            FormatNumber::Full => "Format number with precision and prefix",
        })
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        match self {
            FormatNumber::Default => vec![ArgSpec::column("value", 0, "float64", "Number")],
            FormatNumber::Precision => vec![
                ArgSpec::const_arg("precision", 0, "int64", "Decimals")
                    .with_ge(0.0)
                    .with_le(10.0),
                ArgSpec::column("value", 1, "float64", "Number"),
            ],
            FormatNumber::Full => vec![
                ArgSpec::const_arg("precision", 0, "int64", "Decimals")
                    .with_ge(0.0)
                    .with_le(10.0),
                ArgSpec::const_arg("prefix", 1, "varchar", "Prefix"),
                ArgSpec::column("value", 2, "float64", "Number"),
            ],
        }
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let vals = value_col(batch);
        let out: StringArray = match self {
            FormatNumber::Default => vals.iter().map(|v| v.map(|v| format!("{v:.0}"))).collect(),
            FormatNumber::Precision => {
                let p = params.arguments.const_i64(0).unwrap_or(0).max(0) as usize;
                vals.iter()
                    .map(|v| v.map(|v| format!("{v:.p$}", p = p)))
                    .collect()
            }
            FormatNumber::Full => {
                let p = params.arguments.const_i64(0).unwrap_or(0).max(0) as usize;
                let prefix = params.arguments.const_str(1).unwrap_or_default();
                vals.iter()
                    .map(|v| v.map(|v| format!("{prefix}{v:.p$}", p = p)))
                    .collect()
            }
        };
        result(params, arc(out))
    }
}

/// `smart_format` overloads: (width:int) right-align / (prefix:str) prepend.
pub enum SmartFormat {
    Width,
    Prefix,
}
impl ScalarFunction for SmartFormat {
    fn name(&self) -> &str {
        "smart_format"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Smart-format a value")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        match self {
            SmartFormat::Width => vec![
                ArgSpec::const_arg("width", 0, "int64", "Field width"),
                ArgSpec::any_column("value", 1, "Value"),
            ],
            SmartFormat::Prefix => vec![
                ArgSpec::const_arg("prefix", 0, "varchar", "Prefix"),
                ArgSpec::any_column("value", 1, "Value"),
            ],
        }
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let vals = value_col(batch);
        let out: StringArray = match self {
            SmartFormat::Width => {
                let w = params.arguments.const_i64(0).unwrap_or(0).max(0) as usize;
                vals.iter()
                    .map(|v| v.map(|v| format!("{:>w$}", py_float(v), w = w)))
                    .collect()
            }
            SmartFormat::Prefix => {
                let prefix = params.arguments.const_str(0).unwrap_or_default();
                vals.iter()
                    .map(|v| v.map(|v| format!("{prefix}{}", py_float(v))))
                    .collect()
            }
        };
        result(params, arc(out))
    }
}

/// Render a float the way Python's `str(float)` does (whole numbers keep `.0`).
fn py_float(v: f64) -> String {
    if v.fract() == 0.0 && v.is_finite() {
        format!("{v:.1}")
    } else {
        format!("{v}")
    }
}

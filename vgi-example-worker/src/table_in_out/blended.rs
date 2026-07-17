// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Blended ("UNNEST-style") table-in-out fixtures.
//!
//! A blended function's POSITIONAL args ARE its per-row input columns (real
//! typed args, no synthetic TABLE placeholder), so ONE registration serves the
//! literal call (`geo_encode(52,13)` → single-row scan-mode), the column call
//! (`FROM t, geo_encode(t.x, t.y)` → streaming), and correlated LATERAL.
//! Mirrors the Python SDK's `RowTransformFunction` fixtures.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, Float64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_in_out::TableInOutFunction;
use vgi_rpc::{Result, RpcError};

/// Register the blended fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_table_in_out(GeoEncodeFunction);
    w.register_table_in_out(GeoEncode3Function);
    w.register_table_in_out(RowSumFunction);
}

fn blended_meta(description: &str, categories: &[&str]) -> FunctionMetadata {
    FunctionMetadata {
        description: description.to_string(),
        categories: categories.iter().map(|s| s.to_string()).collect(),
        input_from_args: true,
        ..Default::default()
    }
}

/// Render an `f64` the way Python's `str(float)` does for the fixture range:
/// integral values keep a trailing `.0` (`52.0`, not `52`); everything else
/// uses the shortest round-trip form. Keeps the geohash strings byte-identical
/// to the canonical Python fixture output the shared `.test` files pin.
fn pyfloat(v: f64) -> String {
    if v.is_finite() && v.fract() == 0.0 && v.abs() < 1e16 {
        format!("{v:.1}")
    } else {
        format!("{v}")
    }
}

/// Round to `p` decimal places (the fixture inputs are exact enough that
/// half-to-even vs half-away differences never arise).
fn round_p(v: f64, p: i64) -> f64 {
    let m = 10f64.powi(p.clamp(0, 12) as i32);
    (v * m).round() / m
}

/// Cast a column to Float64 (blended inputs arrive as the declared arg type,
/// but stay defensive for the childless / odd-transport shapes).
fn f64_col(col: &ArrayRef) -> Result<Float64Array> {
    let cast = arrow_cast::cast(col, &DataType::Float64)
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    Ok(cast.as_primitive::<Float64Type>().clone())
}

fn opt_val(a: &Float64Array, i: usize) -> Option<f64> {
    if a.is_valid(i) {
        Some(a.value(i))
    } else {
        None
    }
}

fn geohash_batch(params: &ProcessParams, codes: Vec<Option<String>>) -> Result<Vec<RecordBatch>> {
    let col: StringArray = codes.into_iter().collect();
    let batch = RecordBatch::try_new(params.output_schema.clone(), vec![Arc::new(col)])
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    Ok(vec![batch])
}

fn geohash_bind() -> Result<BindResponse> {
    Ok(BindResponse {
        output_schema: Arc::new(Schema::new(vec![Field::new(
            "geohash",
            DataType::Utf8,
            true,
        )])),
        opaque_data: Vec::new(),
    })
}

/// `geo_encode(latitude, longitude)` — blended per-row geo encoder.
///
/// `latitude`/`longitude` are POSITIONAL args = the per-row input columns
/// (read from `batch` by declared name — the C++ bind builds the input schema
/// from the declared arg names). `precision` is a NAMED arg, surfaced on
/// `params.arguments` (positional args are NOT). Emits one `geohash` string
/// per input row: `"<lat>:<lon>"` rounded to `precision` decimals —
/// deterministic so tests assert exact values.
pub struct GeoEncodeFunction;
impl TableInOutFunction for GeoEncodeFunction {
    fn name(&self) -> &str {
        "geo_encode"
    }
    fn metadata(&self) -> FunctionMetadata {
        blended_meta(
            "Blended per-row geo encoder (lat, lon -> geohash)",
            &["geo", "blended"],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("latitude", 0, "double", "Latitude input column"),
            ArgSpec::column("longitude", 1, "double", "Longitude input column"),
            ArgSpec::const_arg("precision", -1, "int64", "Rounding precision").with_default(4),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        geohash_bind()
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let p = params.arguments.named_i64("precision").unwrap_or(4);
        let lats = f64_col(named_col(batch, "latitude", 0)?)?;
        let lons = f64_col(named_col(batch, "longitude", 1)?)?;
        let codes = (0..batch.num_rows())
            .map(|i| match (opt_val(&lats, i), opt_val(&lons, i)) {
                (Some(lat), Some(lon)) => Some(format!(
                    "{}:{}",
                    pyfloat(round_p(lat, p)),
                    pyfloat(round_p(lon, p))
                )),
                _ => None,
            })
            .collect();
        geohash_batch(params, codes)
    }
}

/// `geo_encode(latitude, longitude, altitude)` — the 3-positional arity
/// overload sharing the name `geo_encode`. Blended functions use REAL value
/// types (no TABLE-typed arg), so DuckDB permits multiple overloads;
/// `geo_encode(52,13)` resolves to the 2-arg overload, `geo_encode(52,13,100)`
/// to this one, in both literal and column shapes.
pub struct GeoEncode3Function;
impl TableInOutFunction for GeoEncode3Function {
    fn name(&self) -> &str {
        "geo_encode"
    }
    fn metadata(&self) -> FunctionMetadata {
        blended_meta(
            "Blended per-row geo encoder (lat, lon, alt -> geohash)",
            &["geo", "blended"],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("latitude", 0, "double", "Latitude input column"),
            ArgSpec::column("longitude", 1, "double", "Longitude input column"),
            ArgSpec::column("altitude", 2, "double", "Altitude input column"),
            ArgSpec::const_arg("precision", -1, "int64", "Rounding precision").with_default(4),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        geohash_bind()
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let p = params.arguments.named_i64("precision").unwrap_or(4);
        let lats = f64_col(named_col(batch, "latitude", 0)?)?;
        let lons = f64_col(named_col(batch, "longitude", 1)?)?;
        let alts = f64_col(named_col(batch, "altitude", 2)?)?;
        let codes = (0..batch.num_rows())
            .map(
                |i| match (opt_val(&lats, i), opt_val(&lons, i), opt_val(&alts, i)) {
                    (Some(lat), Some(lon), Some(alt)) => Some(format!(
                        "{}:{}:{}",
                        pyfloat(round_p(lat, p)),
                        pyfloat(round_p(lon, p)),
                        pyfloat(round_p(alt, p))
                    )),
                    _ => None,
                },
            )
            .collect();
        geohash_batch(params, codes)
    }
}

/// Read a blended input column by its declared arg name, falling back to the
/// position (the varargs / positional shapes name columns `col0..colN-1`).
fn named_col<'a>(batch: &'a RecordBatch, name: &str, pos: usize) -> Result<&'a ArrayRef> {
    batch
        .column_by_name(name)
        .or_else(|| batch.columns().get(pos))
        .ok_or_else(|| RpcError::runtime_error(format!("blended input column '{name}' missing")))
}

/// `row_sum(v1, v2, ...)` — blended VARARGS row-wise sum.
///
/// `values` is a varargs positional arg: the per-row input is N columns of the
/// declared type. A varargs blended function has no per-column declared names,
/// so the columns are read POSITIONALLY. `row_sum(1,2,3)` → 6; `FROM t,
/// row_sum(t.a, t.b, t.c)` sums each row's columns. The `absolute` named
/// option is surfaced on `params.arguments`.
pub struct RowSumFunction;
impl TableInOutFunction for RowSumFunction {
    fn name(&self) -> &str {
        "row_sum"
    }
    fn metadata(&self) -> FunctionMetadata {
        blended_meta("Blended per-row varargs sum", &["numeric", "blended"])
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("values", 0, "double", "Numeric input columns").varargs(),
            ArgSpec::const_arg("absolute", -1, "boolean", "Sum absolute values")
                .with_default(false),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new(
                "row_sum",
                DataType::Float64,
                true,
            )])),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let absolute = params.arguments.named_bool("absolute").unwrap_or(false);
        let n = batch.num_rows();
        let mut acc = vec![0.0f64; n];
        let mut valid = vec![true; n];
        for col in batch.columns() {
            let a = f64_col(col)?;
            for i in 0..n {
                match opt_val(&a, i) {
                    Some(v) => acc[i] += if absolute { v.abs() } else { v },
                    None => valid[i] = false,
                }
            }
        }
        let col: Float64Array = acc
            .into_iter()
            .zip(valid)
            .map(|(v, ok)| if ok { Some(v) } else { None })
            .collect();
        let out = RecordBatch::try_new(params.output_schema.clone(), vec![Arc::new(col)])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(vec![out])
    }
}

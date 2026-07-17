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
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_in_out::{EmitOptions, TableInOutFunction, TableInOutOutput};
use vgi_rpc::{Result, RpcError};

/// Register the blended fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_table_in_out(GeoEncodeFunction);
    w.register_table_in_out(GeoEncode3Function);
    w.register_table_in_out(RowSumFunction);
    w.register_table_in_out(BlendedDropFunction);
    w.register_table_in_out(BlendedExplodeFunction);
    w.register_table_in_out(ProjectableBlendedFunction);
    w.register_table_in_out(HostileProvenanceFunction);
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

/// `blended_drop(x)` — blended 1→0 map: emits a single 0-row output batch for
/// its input row.
///
/// Exercises the literal scan-mode drain loop's "empty-but-not-EOS → keep
/// reading, finish only at true EOS" branch: the worker's whole output for the
/// one synthesized input row is a 0-row batch, so the scan must reach FINISHED
/// cleanly and NOT infinite-loop re-feeding the input.
pub struct BlendedDropFunction;
impl TableInOutFunction for BlendedDropFunction {
    fn name(&self) -> &str {
        "blended_drop"
    }
    fn metadata(&self) -> FunctionMetadata {
        blended_meta(
            "Blended 1->0 map emitting a single 0-row batch (literal scan-mode)",
            &["blended", "test"],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("x", 0, "double", "Input column (ignored)")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &ProcessParams, _batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(vec![RecordBatch::new_empty(params.output_schema.clone())])
    }
}

/// `blended_explode(n)` — blended 1→N fan-out map carrying per-output-row
/// provenance.
///
/// For each input row with count `n`, emits `n` output rows (the integers
/// `0..n-1`). Because the output row count differs from the input row count,
/// the worker declares per-output-row provenance via
/// `EmitOptions::parent_rows` — `parent_rows[i]` is the index (into this
/// call's input batch) of the row that produced output row `i`. That lets the
/// batched correlated-LATERAL operator ship a whole input chunk in ONE
/// exchange and still stamp each output row's outer columns from the right
/// input row. `n=0` → 1→0 (filter), `n=1` → 1→1, `n=3` → 1→N.
pub struct BlendedExplodeFunction;
impl TableInOutFunction for BlendedExplodeFunction {
    fn name(&self) -> &str {
        "blended_explode"
    }
    fn metadata(&self) -> FunctionMetadata {
        blended_meta(
            "Blended 1->N fan-out (emit 0..n-1 per input row) with row provenance",
            &["blended", "test"],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column(
            "n",
            0,
            "int64",
            "Fan-out count: emit rows 0..n-1 for this input row",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)])),
            opaque_data: Vec::new(),
        })
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        let counts = i64_col(named_col(batch, "n", 0)?)?;
        let mut out_vals: Vec<i64> = Vec::new();
        let mut parent_rows: Vec<i32> = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let fan = if counts.is_valid(row_idx) {
                counts.value(row_idx).max(0)
            } else {
                0
            };
            out_vals.extend(0..fan);
            parent_rows.extend(std::iter::repeat_n(row_idx as i32, fan as usize));
        }
        let col = Arc::new(Int64Array::from(out_vals)) as ArrayRef;
        let out_batch = RecordBatch::try_new(params.output_schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        // Whole-chunk fan-out: one emit for the whole input batch, carrying the
        // per-output-row parent index so the batched-LATERAL operator can stamp
        // the correlated columns. (Identity provenance is omitted for 1->1 maps
        // — the extension assumes it — but here the row count changes, so it's
        // required.)
        out.emit_with(
            out_batch,
            EmitOptions {
                parent_rows: Some(parent_rows),
                ..Default::default()
            },
        )
    }
}

/// Cast a column to Int64.
fn i64_col(col: &ArrayRef) -> Result<Int64Array> {
    let cast = arrow_cast::cast(col, &DataType::Int64)
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    Ok(cast.as_primitive::<Int64Type>().clone())
}

/// `projectable_blended(x)` — blended 1→1 map advertising projection_pushdown,
/// with TWO output columns (`a = x*10`, `b = x*100`).
///
/// Regression fixture for the batched correlated-LATERAL operator vs
/// projection pushdown: when a correlated LATERAL query projects only a SUBSET
/// of the worker's output columns, the operator must thread the projection to
/// the worker (narrow emit) and remap it — NOT read worker column 0 into the
/// `b` slot. The emit builds against `params.output_schema` (the
/// projection-narrowed schema), computing only the referenced columns.
pub struct ProjectableBlendedFunction;
impl TableInOutFunction for ProjectableBlendedFunction {
    fn name(&self) -> &str {
        "projectable_blended"
    }
    fn metadata(&self) -> FunctionMetadata {
        let mut meta = blended_meta(
            "Blended 1->1 map with projection_pushdown + two output columns",
            &["blended", "test"],
        );
        meta.projection_pushdown = true;
        meta
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("x", 0, "int64", "Input column")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Int64, true),
            ])),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let xs = i64_col(named_col(batch, "x", 0)?)?;
        let mul = |factor: i64| -> ArrayRef {
            let vals: Int64Array = (0..xs.len())
                .map(|i| {
                    if xs.is_valid(i) {
                        Some(xs.value(i) * factor)
                    } else {
                        None
                    }
                })
                .collect();
            Arc::new(vals)
        };
        // Emit only the (possibly projection-narrowed) output columns.
        let cols: Vec<ArrayRef> = params
            .output_schema
            .fields()
            .iter()
            .map(|f| match f.name().as_str() {
                "a" => Ok(mul(10)),
                "b" => Ok(mul(100)),
                other => Err(RpcError::runtime_error(format!(
                    "projectable_blended: unexpected projected column '{other}'"
                ))),
            })
            .collect::<Result<_>>()?;
        let out = RecordBatch::try_new(params.output_schema.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(vec![out])
    }
}

/// `hostile_provenance(x, mode := ...)` — adversarial blended fixture: emits a
/// MALFORMED `vgi_rpc.parent_row` payload.
///
/// Simulates a buggy or hostile worker (especially a remote HTTP one) that
/// sends provenance the batched correlated-LATERAL operator must reject rather
/// than use as an unchecked array index. Emits one output row per input row
/// (so the row count matches — the metadata is present, not the identity
/// path), but attaches a poisoned `vgi_rpc.parent_row#b64` per `mode`:
///
/// * `range`  — a well-formed int32[] of the right length whose values are all
///   `num_rows` (one past the last valid index): the range check must throw.
/// * `length` — a valid-base64 int32[] blob one element TOO LONG: the length
///   check must throw.
/// * `base64` — not valid base64 at all: the decode must throw.
///
/// The payload is set via raw `EmitOptions::metadata` so it bypasses the
/// framework's length-only `parent_rows` check and reaches the client's
/// validation unfiltered.
pub struct HostileProvenanceFunction;
impl TableInOutFunction for HostileProvenanceFunction {
    fn name(&self) -> &str {
        "hostile_provenance"
    }
    fn metadata(&self) -> FunctionMetadata {
        blended_meta(
            "Adversarial blended fixture emitting malformed vgi_rpc.parent_row",
            &["blended", "test", "adversarial"],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("x", 0, "int64", "Input column (echoed as output)"),
            ArgSpec::const_arg("mode", -1, "varchar", "range | length | base64")
                .with_default("range"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("hv", DataType::Int64, true)])),
            opaque_data: Vec::new(),
        })
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        let n = batch.num_rows();
        let xs = i64_col(named_col(batch, "x", 0)?)?;
        let out_batch =
            RecordBatch::try_new(params.output_schema.clone(), vec![Arc::new(xs) as ArrayRef])
                .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let mode = params
            .arguments
            .named_str("mode")
            .unwrap_or_else(|| "range".to_string());
        let pack = |vals: &[i32]| -> Vec<u8> {
            let mut raw = Vec::with_capacity(vals.len() * 4);
            for v in vals {
                raw.extend_from_slice(&v.to_le_bytes());
            }
            raw
        };
        let payload = match mode.as_str() {
            // Not valid base64 at all.
            "base64" => "@@@ this is not base64 @@@".to_string(),
            // One int32 too many for the emitted row count.
            "length" => b64(&pack(&vec![0i32; n + 1])),
            // Every parent index == n (one past the last valid index n-1).
            _ => b64(&pack(&vec![n as i32; n])),
        };
        out.emit_with(
            out_batch,
            EmitOptions {
                metadata: Some(std::collections::HashMap::from([(
                    vgi::table_in_out::PARENT_ROW_METADATA_KEY.to_string(),
                    payload,
                )])),
                ..Default::default()
            },
        )
    }
}

/// Standard base64 encoding (fixture-local; the SDK's encoder is not public).
fn b64(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(T[((n >> 18) & 63) as usize] as char);
        out.push(T[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            T[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            T[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

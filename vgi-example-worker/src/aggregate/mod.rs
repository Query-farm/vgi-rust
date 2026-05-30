//! Aggregate example fixtures.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use vgi::aggregate::{AggregateBindParams, AggregateFunction};
use vgi::function::{ArgSpec, BindResponse, FunctionMetadata};
use vgi::protocol::enums;
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_aggregate(SumFunction);
    w.register_aggregate(CountFunction);
    w.register_aggregate(AvgFunction);
    w.register_aggregate(WeightedSumFunction);
    w.register_aggregate(GenericSumFunction);
    w.register_aggregate(SumAllFunction);
    w.register_aggregate(ListAggFunction);
}

fn agg_meta(desc: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        order_preservation: None,
        ..Default::default()
    }
}

fn result_schema(ty: DataType) -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("result", ty, true)]))
}

fn le_i64(v: i64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}
fn read_i64(b: &[u8]) -> i64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8.min(b.len())]);
    i64::from_le_bytes(a)
}
fn cast_i64(col: &ArrayRef) -> Result<Int64Array> {
    Ok(arrow_cast::cast(col, &DataType::Int64)
        .map_err(|e| RpcError::runtime_error(e.to_string()))?
        .as_primitive::<Int64Type>()
        .clone())
}

// ---------------------------------------------------------------------------
// vgi_sum(value) -> int64
// ---------------------------------------------------------------------------

pub struct SumFunction;
impl AggregateFunction for SumFunction {
    fn name(&self) -> &str { "vgi_sum" }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = agg_meta("Sum integer values");
        m.null_handling = Some(enums::null_handling::DEFAULT.into());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Column to sum")]
    }
    fn on_bind(&self, _p: &AggregateBindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: result_schema(DataType::Int64), opaque_data: Vec::new() })
    }
    fn initial_state(&self) -> Vec<u8> { le_i64(0) }
    fn update(&self, states: &mut HashMap<i64, Vec<u8>>, gids: &Int64Array, cols: &[ArrayRef]) -> Result<()> {
        let v = cast_i64(&cols[0])?;
        for i in 0..gids.len() {
            if v.is_null(i) { continue; }
            let st = states.entry(gids.value(i)).or_insert_with(|| le_i64(0));
            *st = le_i64(read_i64(st) + v.value(i));
        }
        Ok(())
    }
    fn combine(&self, target: Vec<u8>, source: Vec<u8>) -> Result<Vec<u8>> {
        Ok(le_i64(read_i64(&target) + read_i64(&source)))
    }
    fn finalize(&self, output_schema: &Arc<Schema>, gids: &Int64Array, states: &[Option<Vec<u8>>]) -> Result<RecordBatch> {
        let out: Int64Array = (0..gids.len())
            .map(|i| states[i].as_ref().map(|s| read_i64(s)))
            .collect();
        RecordBatch::try_new(output_schema.clone(), vec![Arc::new(out)])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// vgi_count() -> int64 (counts all rows, SPECIAL null handling)
// ---------------------------------------------------------------------------

pub struct CountFunction;
impl AggregateFunction for CountFunction {
    fn name(&self) -> &str { "vgi_count" }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = agg_meta("Count rows");
        m.null_handling = Some(enums::null_handling::SPECIAL.into());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> { vec![] }
    fn on_bind(&self, _p: &AggregateBindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: result_schema(DataType::Int64), opaque_data: Vec::new() })
    }
    fn initial_state(&self) -> Vec<u8> { le_i64(0) }
    fn update(&self, states: &mut HashMap<i64, Vec<u8>>, gids: &Int64Array, _cols: &[ArrayRef]) -> Result<()> {
        for i in 0..gids.len() {
            let st = states.entry(gids.value(i)).or_insert_with(|| le_i64(0));
            *st = le_i64(read_i64(st) + 1);
        }
        Ok(())
    }
    fn combine(&self, target: Vec<u8>, source: Vec<u8>) -> Result<Vec<u8>> {
        Ok(le_i64(read_i64(&target) + read_i64(&source)))
    }
    fn finalize(&self, output_schema: &Arc<Schema>, gids: &Int64Array, states: &[Option<Vec<u8>>]) -> Result<RecordBatch> {
        let out: Int64Array = (0..gids.len())
            .map(|i| Some(states[i].as_ref().map(|s| read_i64(s)).unwrap_or(0)))
            .collect();
        RecordBatch::try_new(output_schema.clone(), vec![Arc::new(out)])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// vgi_avg(value) -> double  (state: f64 total + i64 count)
// ---------------------------------------------------------------------------

pub struct AvgFunction;
fn avg_enc(total: f64, count: i64) -> Vec<u8> {
    let mut v = total.to_le_bytes().to_vec();
    v.extend_from_slice(&count.to_le_bytes());
    v
}
fn avg_dec(b: &[u8]) -> (f64, i64) {
    let mut t = [0u8; 8];
    t.copy_from_slice(&b[0..8]);
    let mut c = [0u8; 8];
    c.copy_from_slice(&b[8..16]);
    (f64::from_le_bytes(t), i64::from_le_bytes(c))
}
impl AggregateFunction for AvgFunction {
    fn name(&self) -> &str { "vgi_avg" }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = agg_meta("Average of integer values");
        m.null_handling = Some(enums::null_handling::DEFAULT.into());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Column to average")]
    }
    fn on_bind(&self, _p: &AggregateBindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: result_schema(DataType::Float64), opaque_data: Vec::new() })
    }
    fn initial_state(&self) -> Vec<u8> { avg_enc(0.0, 0) }
    fn update(&self, states: &mut HashMap<i64, Vec<u8>>, gids: &Int64Array, cols: &[ArrayRef]) -> Result<()> {
        let v = cast_i64(&cols[0])?;
        for i in 0..gids.len() {
            if v.is_null(i) { continue; }
            let st = states.entry(gids.value(i)).or_insert_with(|| avg_enc(0.0, 0));
            let (t, c) = avg_dec(st);
            *st = avg_enc(t + v.value(i) as f64, c + 1);
        }
        Ok(())
    }
    fn combine(&self, target: Vec<u8>, source: Vec<u8>) -> Result<Vec<u8>> {
        let (tt, tc) = avg_dec(&target);
        let (st, sc) = avg_dec(&source);
        Ok(avg_enc(tt + st, tc + sc))
    }
    fn finalize(&self, output_schema: &Arc<Schema>, gids: &Int64Array, states: &[Option<Vec<u8>>]) -> Result<RecordBatch> {
        let out: Float64Array = (0..gids.len())
            .map(|i| {
                states[i].as_ref().and_then(|s| {
                    let (t, c) = avg_dec(s);
                    if c > 0 { Some(t / c as f64) } else { None }
                })
            })
            .collect();
        RecordBatch::try_new(output_schema.clone(), vec![Arc::new(out)])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// keep Float64Type import used
#[allow(dead_code)]
fn _f(_: arrow_array::PrimitiveArray<Float64Type>) {}

fn le_f64(v: f64) -> Vec<u8> { v.to_le_bytes().to_vec() }
fn read_f64(b: &[u8]) -> f64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[..8.min(b.len())]);
    f64::from_le_bytes(a)
}
fn cast_f64(col: &ArrayRef) -> Result<Float64Array> {
    Ok(arrow_cast::cast(col, &DataType::Float64)
        .map_err(|e| RpcError::runtime_error(e.to_string()))?
        .as_primitive::<Float64Type>()
        .clone())
}

/// `vgi_weighted_sum(value, weight)` -> double.
pub struct WeightedSumFunction;
impl AggregateFunction for WeightedSumFunction {
    fn name(&self) -> &str { "vgi_weighted_sum" }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = agg_meta("Weighted sum of values");
        m.null_handling = Some(enums::null_handling::DEFAULT.into());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("value", 0, "float64", "Values"),
            ArgSpec::column("weight", 1, "float64", "Weights"),
        ]
    }
    fn on_bind(&self, _p: &AggregateBindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: result_schema(DataType::Float64), opaque_data: Vec::new() })
    }
    fn initial_state(&self) -> Vec<u8> { le_f64(0.0) }
    fn update(&self, states: &mut HashMap<i64, Vec<u8>>, gids: &Int64Array, cols: &[ArrayRef]) -> Result<()> {
        let v = cast_f64(&cols[0])?;
        let w = cast_f64(&cols[1])?;
        for i in 0..gids.len() {
            if v.is_null(i) || w.is_null(i) { continue; }
            let st = states.entry(gids.value(i)).or_insert_with(|| le_f64(0.0));
            *st = le_f64(read_f64(st) + v.value(i) * w.value(i));
        }
        Ok(())
    }
    fn combine(&self, t: Vec<u8>, s: Vec<u8>) -> Result<Vec<u8>> { Ok(le_f64(read_f64(&t) + read_f64(&s))) }
    fn finalize(&self, os: &Arc<Schema>, gids: &Int64Array, states: &[Option<Vec<u8>>]) -> Result<RecordBatch> {
        let out: Float64Array = (0..gids.len()).map(|i| states[i].as_ref().map(|s| read_f64(s))).collect();
        RecordBatch::try_new(os.clone(), vec![Arc::new(out)]).map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

/// `vgi_generic_sum(value)` -> input type. State is f64; output cast to input type.
pub struct GenericSumFunction;
impl AggregateFunction for GenericSumFunction {
    fn name(&self) -> &str { "vgi_generic_sum" }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = agg_meta("Sum any numeric type");
        m.null_handling = Some(enums::null_handling::DEFAULT.into());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> { vec![ArgSpec::any_column("value", 0, "Numeric value")] }
    fn on_bind(&self, p: &AggregateBindParams) -> Result<BindResponse> {
        let ty = p.input_schema.as_ref()
            .and_then(|s| s.fields().first().map(|f| f.data_type().clone()))
            .unwrap_or(DataType::Float64);
        Ok(BindResponse { output_schema: result_schema(ty), opaque_data: Vec::new() })
    }
    fn initial_state(&self) -> Vec<u8> { le_f64(0.0) }
    fn update(&self, states: &mut HashMap<i64, Vec<u8>>, gids: &Int64Array, cols: &[ArrayRef]) -> Result<()> {
        let v = cast_f64(&cols[0])?;
        for i in 0..gids.len() {
            if v.is_null(i) { continue; }
            let st = states.entry(gids.value(i)).or_insert_with(|| le_f64(0.0));
            *st = le_f64(read_f64(st) + v.value(i));
        }
        Ok(())
    }
    fn combine(&self, t: Vec<u8>, s: Vec<u8>) -> Result<Vec<u8>> { Ok(le_f64(read_f64(&t) + read_f64(&s))) }
    fn finalize(&self, os: &Arc<Schema>, gids: &Int64Array, states: &[Option<Vec<u8>>]) -> Result<RecordBatch> {
        let f: Float64Array = (0..gids.len()).map(|i| states[i].as_ref().map(|s| read_f64(s))).collect();
        let ty = os.field(0).data_type();
        let col = arrow_cast::cast(&(Arc::new(f) as ArrayRef), ty).map_err(|e| RpcError::runtime_error(e.to_string()))?;
        RecordBatch::try_new(os.clone(), vec![col]).map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

/// `vgi_sum_all(cols...)` -> double. Varargs numeric.
pub struct SumAllFunction;
impl AggregateFunction for SumAllFunction {
    fn name(&self) -> &str { "vgi_sum_all" }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = agg_meta("Sum all numeric columns");
        m.null_handling = Some(enums::null_handling::DEFAULT.into());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::any_column("columns", 0, "Numeric columns").varargs()]
    }
    fn on_bind(&self, _p: &AggregateBindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: result_schema(DataType::Float64), opaque_data: Vec::new() })
    }
    fn initial_state(&self) -> Vec<u8> { le_f64(0.0) }
    fn update(&self, states: &mut HashMap<i64, Vec<u8>>, gids: &Int64Array, cols: &[ArrayRef]) -> Result<()> {
        let fcols: Vec<Float64Array> = cols.iter().map(cast_f64).collect::<Result<_>>()?;
        for i in 0..gids.len() {
            let mut row = 0.0;
            for c in &fcols { if !c.is_null(i) { row += c.value(i); } }
            let st = states.entry(gids.value(i)).or_insert_with(|| le_f64(0.0));
            *st = le_f64(read_f64(st) + row);
        }
        Ok(())
    }
    fn combine(&self, t: Vec<u8>, s: Vec<u8>) -> Result<Vec<u8>> { Ok(le_f64(read_f64(&t) + read_f64(&s))) }
    fn finalize(&self, os: &Arc<Schema>, gids: &Int64Array, states: &[Option<Vec<u8>>]) -> Result<RecordBatch> {
        let out: Float64Array = (0..gids.len()).map(|i| states[i].as_ref().map(|s| read_f64(s))).collect();
        RecordBatch::try_new(os.clone(), vec![Arc::new(out)]).map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

/// `vgi_listagg(value)` -> string. Order-dependent comma join.
pub struct ListAggFunction;
impl AggregateFunction for ListAggFunction {
    fn name(&self) -> &str { "vgi_listagg" }
    fn metadata(&self) -> FunctionMetadata {
        let mut m = agg_meta("Concatenate strings with comma separator");
        m.null_handling = Some(enums::null_handling::DEFAULT.into());
        m
    }
    fn argument_specs(&self) -> Vec<ArgSpec> { vec![ArgSpec::column("value", 0, "varchar", "String column")] }
    fn on_bind(&self, _p: &AggregateBindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: result_schema(DataType::Utf8), opaque_data: Vec::new() })
    }
    fn initial_state(&self) -> Vec<u8> { Vec::new() }
    fn update(&self, states: &mut HashMap<i64, Vec<u8>>, gids: &Int64Array, cols: &[ArrayRef]) -> Result<()> {
        let v = cols[0].as_string::<i32>();
        for i in 0..gids.len() {
            if v.is_null(i) { continue; }
            let st = states.entry(gids.value(i)).or_default();
            let cur = String::from_utf8_lossy(st).to_string();
            let next = if cur.is_empty() { v.value(i).to_string() } else { format!("{cur},{}", v.value(i)) };
            *st = next.into_bytes();
        }
        Ok(())
    }
    fn combine(&self, t: Vec<u8>, s: Vec<u8>) -> Result<Vec<u8>> {
        let tt = String::from_utf8_lossy(&t).to_string();
        let ss = String::from_utf8_lossy(&s).to_string();
        let r = if !tt.is_empty() && !ss.is_empty() { format!("{tt},{ss}") }
                else if !tt.is_empty() { tt } else { ss };
        Ok(r.into_bytes())
    }
    fn finalize(&self, os: &Arc<Schema>, gids: &Int64Array, states: &[Option<Vec<u8>>]) -> Result<RecordBatch> {
        let out: arrow_array::StringArray = (0..gids.len())
            .map(|i| states[i].as_ref().and_then(|s| {
                let st = String::from_utf8_lossy(s).to_string();
                if st.is_empty() { None } else { Some(st) }
            }))
            .collect();
        RecordBatch::try_new(os.clone(), vec![Arc::new(out)]).map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

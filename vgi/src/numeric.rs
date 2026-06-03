// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Numeric scalar helpers (port of Python `_promote_for_addition` +
//! `NumericDispatch`).
//!
//! Output type is computed by promotion at bind; `process` casts the input
//! column(s) to that type and applies the op via Arrow's type-preserving
//! `add` kernel (covers int / uint / float / decimal uniformly).

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;
use vgi_rpc::{Result, RpcError};

use crate::function::ProcessParams;

/// Read any integer/float array element widened to `i64` (None if null).
pub fn array_value_i64(arr: &ArrayRef, i: usize) -> Option<i64> {
    if arr.is_null(i) {
        return None;
    }
    use DataType::*;
    Some(match arr.data_type() {
        Int8 => arr.as_primitive::<Int8Type>().value(i) as i64,
        Int16 => arr.as_primitive::<Int16Type>().value(i) as i64,
        Int32 => arr.as_primitive::<Int32Type>().value(i) as i64,
        Int64 => arr.as_primitive::<Int64Type>().value(i),
        UInt8 => arr.as_primitive::<UInt8Type>().value(i) as i64,
        UInt16 => arr.as_primitive::<UInt16Type>().value(i) as i64,
        UInt32 => arr.as_primitive::<UInt32Type>().value(i) as i64,
        UInt64 => arr.as_primitive::<UInt64Type>().value(i) as i64,
        Float32 => arr.as_primitive::<Float32Type>().value(i) as i64,
        Float64 => arr.as_primitive::<Float64Type>().value(i) as i64,
        _ => return None,
    })
}

/// Read any numeric array element widened to `f64` (None if null).
pub fn array_value_f64(arr: &ArrayRef, i: usize) -> Option<f64> {
    if arr.is_null(i) {
        return None;
    }
    use DataType::*;
    Some(match arr.data_type() {
        Float32 => arr.as_primitive::<Float32Type>().value(i) as f64,
        Float64 => arr.as_primitive::<Float64Type>().value(i),
        // DuckDB sends fractional literals (e.g. 0.5) as DECIMAL — cast.
        Decimal128(_, _) | Decimal256(_, _) => {
            let casted = arrow_cast::cast(&arr.slice(i, 1), &Float64).ok()?;
            casted.as_primitive::<Float64Type>().value(0)
        }
        _ => array_value_i64(arr, i)? as f64,
    })
}

/// Promote a numeric type for addition/doubling, matching Python
/// `_promote_for_addition`: integers widen to the next size, floats to
/// `float64`, decimals gain one digit of precision (capped at 38).
pub fn promote_for_addition(ty: &DataType) -> DataType {
    use DataType::*;
    match ty {
        // temporal: unchanged
        Date32 | Date64 | Time32(_) | Time64(_) | Timestamp(_, _) | Duration(_) | Interval(_) => {
            ty.clone()
        }
        Float16 | Float32 => Float64,
        Float64 => Float64,
        Int8 => Int16,
        Int16 => Int32,
        Int32 | Int64 => Int64,
        UInt8 => UInt16,
        UInt16 => UInt32,
        UInt32 | UInt64 => UInt64,
        Decimal128(p, s) => Decimal128((*p as u32 + 1).min(38) as u8, *s),
        other => other.clone(),
    }
}

/// Common addition type for two inputs: the numeric common type of the two,
/// then promoted for overflow headroom (matches Python
/// `_promote_for_addition(pc.add(nulls(t1), nulls(t2)).type)`).
pub fn common_type_for_addition(a: &DataType, b: &DataType) -> DataType {
    promote_for_addition(&common_numeric(a, b))
}

/// The numeric common type for adding two Arrow types (DuckDB / pyarrow
/// semantics): any float → float64; mixed-sign ints → int64; otherwise the
/// wider same-signedness integer; decimals widen precision and scale.
fn common_numeric(a: &DataType, b: &DataType) -> DataType {
    use DataType::*;
    if a == b {
        return a.clone();
    }
    let is_float = |t: &DataType| matches!(t, Float16 | Float32 | Float64);
    if is_float(a) || is_float(b) {
        return Float64;
    }
    if let (Decimal128(pa, sa), Decimal128(pb, sb)) = (a, b) {
        return Decimal128((*pa).max(*pb), (*sa).max(*sb));
    }
    if let (Some((ba, sgna)), Some((bb, sgnb))) = (int_info(a), int_info(b)) {
        if sgna == sgnb {
            // wider same-signedness integer
            return if ba >= bb { a.clone() } else { b.clone() };
        }
        // mixed sign → signed type wide enough to hold both
        return Int64;
    }
    Int64
}

/// `(bit_width, is_signed)` for an integer type.
fn int_info(t: &DataType) -> Option<(u8, bool)> {
    use DataType::*;
    Some(match t {
        Int8 => (8, true),
        Int16 => (16, true),
        Int32 => (32, true),
        Int64 => (64, true),
        UInt8 => (8, false),
        UInt16 => (16, false),
        UInt32 => (32, false),
        UInt64 => (64, false),
        _ => return None,
    })
}

fn output_type(params: &ProcessParams) -> Result<DataType> {
    params
        .output_schema
        .fields()
        .first()
        .map(|f| f.data_type().clone())
        .ok_or_else(|| RpcError::runtime_error("output schema has no fields"))
}

fn cast(col: &ArrayRef, ty: &DataType) -> Result<ArrayRef> {
    arrow_cast::cast(col, ty).map_err(|e| RpcError::runtime_error(format!("cast to {ty:?}: {e}")))
}

fn result_batch(params: &ProcessParams, arr: ArrayRef) -> Result<RecordBatch> {
    RecordBatch::try_new(params.output_schema.clone(), vec![arr])
        .map_err(|e| RpcError::runtime_error(format!("build result batch: {e}")))
}

/// Double the first input column (cast to the bound output type, add to self,
/// cast back so Arrow's decimal widening doesn't drift the result type).
pub fn double_first(params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
    let ty = output_type(params)?;
    if let DataType::Decimal128(p, s) = ty {
        // Work in a wide decimal then narrow back; overflow at the capped
        // precision must surface the canonical "does not fit in precision N".
        let wide = DataType::Decimal256(76, s);
        let c = cast(batch.column(0), &wide)?;
        let summed = arrow_arith::numeric::add(&c, &c)
            .map_err(|_| precision_error(p))?;
        let narrowed = arrow_cast::cast_with_options(
            &summed,
            &ty,
            &arrow_cast::CastOptions { safe: false, ..Default::default() },
        )
        .map_err(|_| precision_error(p))?;
        return result_batch(params, narrowed);
    }
    let c = cast(batch.column(0), &ty)?;
    let out = arrow_arith::numeric::add(&c, &c)
        .map_err(|e| RpcError::runtime_error(format!("double add: {e}")))?;
    result_batch(params, cast(&out, &ty)?)
}

fn precision_error(p: u8) -> RpcError {
    RpcError::value_error(format!("Decimal value does not fit in precision {p}"))
}

/// Add the first two input columns (cast to the bound output type).
pub fn add_two(params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
    let ty = output_type(params)?;
    let a = cast(batch.column(0), &ty)?;
    let b = cast(batch.column(1), &ty)?;
    let out = arrow_arith::numeric::add(&a, &b)
        .map_err(|e| RpcError::runtime_error(format!("add: {e}")))?;
    result_batch(params, cast(&out, &ty)?)
}

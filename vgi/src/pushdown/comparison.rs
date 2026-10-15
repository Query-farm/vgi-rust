// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Comparisons without materializing broadcast scalars or normalized floats.
//!
//! Arrow's float kernels use IEEE totalOrder. DuckDB instead equates signed
//! zeros and all NaN payloads, and sorts NaNs above every non-NaN value. Compare
//! Float32/Float64 directly with those semantics; keep Arrow's typed kernels
//! for other types so integers, decimals and timestamps retain their precision.

use arrow_array::{Array, ArrayRef, BooleanArray, Datum, Float32Array, Float64Array, Scalar};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::ArrowError;

type Result<T> = std::result::Result<T, ArrowError>;

/// Arrow handles scalar broadcast without materializing a repeated array.
fn arrow_compare(left: &ArrayRef, right: &ArrayRef, op: &str) -> Result<BooleanArray> {
    let left_scalar = (left.len() == 1).then(|| Scalar::new(left.clone()));
    let right_scalar = (right.len() == 1).then(|| Scalar::new(right.clone()));
    let left: &dyn Datum = match &left_scalar {
        Some(s) => s,
        None => left,
    };
    let right: &dyn Datum = match &right_scalar {
        Some(s) => s,
        None => right,
    };
    match op {
        "eq" => arrow_ord::cmp::eq(left, right),
        "ne" => arrow_ord::cmp::neq(left, right),
        "lt" => arrow_ord::cmp::lt(left, right),
        "le" => arrow_ord::cmp::lt_eq(left, right),
        "gt" => arrow_ord::cmp::gt(left, right),
        "ge" => arrow_ord::cmp::gt_eq(left, right),
        "distinct_from" => arrow_ord::cmp::distinct(left, right),
        "not_distinct_from" => arrow_ord::cmp::not_distinct(left, right),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "unknown comparison {op}"
        ))),
    }
}

fn finish_float(
    left: &dyn Array,
    right: &dyn Array,
    values: BooleanBuffer,
    op: &str,
) -> BooleanArray {
    let n = values.len();
    let li = |i| if left.len() == 1 { 0 } else { i };
    let ri = |i| if right.len() == 1 { 0 } else { i };
    if matches!(op, "distinct_from" | "not_distinct_from") {
        return BooleanArray::new(
            BooleanBuffer::collect_bool(n, |i| {
                let (ln, rn) = (left.is_null(li(i)), right.is_null(ri(i)));
                if op == "distinct_from" {
                    (ln != rn) || (!ln && !rn && values.value(i))
                } else {
                    (ln && rn) || (!ln && !rn && values.value(i))
                }
            }),
            None,
        );
    }
    let nulls = if left.len() == 1 {
        if left.is_null(0) {
            Some(NullBuffer::new_null(n))
        } else {
            right.nulls().cloned()
        }
    } else if right.len() == 1 {
        if right.is_null(0) {
            Some(NullBuffer::new_null(n))
        } else {
            left.nulls().cloned()
        }
    } else {
        NullBuffer::union(left.nulls(), right.nulls())
    };
    BooleanArray::new(values, nulls)
}

macro_rules! float_comparator {
    ($name:ident, $array:ty, $float:ty) => {
        fn $name(left: &$array, right: &$array, op: &str) -> Result<BooleanArray> {
            if left.len() != right.len() && left.len() != 1 && right.len() != 1 {
                return Err(ArrowError::InvalidArgumentError(
                    "incompatible lengths".into(),
                ));
            }
            let n = if left.len() == 1 {
                right.len()
            } else {
                left.len()
            };
            macro_rules! values {
                ($compare:expr) => {{
                    let compare = $compare;
                    if left.len() == 1 {
                        let scalar = left.value(0);
                        BooleanBuffer::collect_bool(n, |i| compare(scalar, right.value(i)))
                    } else if right.len() == 1 {
                        let scalar = right.value(0);
                        BooleanBuffer::collect_bool(n, |i| compare(left.value(i), scalar))
                    } else {
                        BooleanBuffer::collect_bool(n, |i| compare(left.value(i), right.value(i)))
                    }
                }};
            }
            // SQL order: signed zeros compare equal; all NaNs compare equal
            // and greater than every non-NaN value, including +infinity.
            let values = match op {
                "eq" | "not_distinct_from" => {
                    values!(|a: $float, b: $float| a == b || (a.is_nan() && b.is_nan()))
                }
                "ne" | "distinct_from" => {
                    values!(|a: $float, b: $float| !(a == b || (a.is_nan() && b.is_nan())))
                }
                "lt" => values!(|a: $float, b: $float| a < b || (!a.is_nan() && b.is_nan())),
                "le" => values!(|a: $float, b: $float| a <= b || b.is_nan()),
                "gt" => values!(|a: $float, b: $float| a > b || (a.is_nan() && !b.is_nan())),
                "ge" => values!(|a: $float, b: $float| a >= b || a.is_nan()),
                _ => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "unknown comparison {op}"
                    )))
                }
            };
            Ok(finish_float(left, right, values, op))
        }
    };
}

float_comparator!(float32_compare, Float32Array, f32);
float_comparator!(float64_compare, Float64Array, f64);

pub(super) fn sql_compare(left: &ArrayRef, right: &ArrayRef, op: &str) -> Result<BooleanArray> {
    if let (Some(left), Some(right)) = (
        left.as_any().downcast_ref::<Float32Array>(),
        right.as_any().downcast_ref::<Float32Array>(),
    ) {
        return float32_compare(left, right, op);
    }
    if let (Some(left), Some(right)) = (
        left.as_any().downcast_ref::<Float64Array>(),
        right.as_any().downcast_ref::<Float64Array>(),
    ) {
        return float64_compare(left, right, op);
    }
    arrow_compare(left, right, op)
}

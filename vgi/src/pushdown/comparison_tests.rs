// Copyright 2025, 2026 Query Farm LLC - https://query.farm

use super::*;
use arrow_array::{
    types::Int8Type, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DictionaryArray,
    Float32Array, Float64Array, Int64Array, Int8Array, LargeStringArray, TimestampMicrosecondArray,
    UInt64Array,
};
use std::cmp::Ordering;

const OPS: [&str; 8] = [
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "distinct_from",
    "not_distinct_from",
];

fn check(left: &ArrayRef, right: &ArrayRef, op: &str, expected: &BooleanArray, scalar: bool) {
    for reversed in [false, true] {
        let column = |index, name: &str, array: &ArrayRef| Expr::Column {
            index,
            name: name.into(),
            data_type: array.data_type().clone(),
        };
        let batch = if scalar {
            RecordBatch::try_from_iter(vec![("lhs", left.clone())]).unwrap()
        } else {
            RecordBatch::try_from_iter(vec![("lhs", left.clone()), ("rhs", right.clone())]).unwrap()
        };
        let lhs = column(0, "lhs", left);
        let rhs = if scalar {
            Expr::Literal(right.clone())
        } else {
            column(1, "rhs", right)
        };
        // Reverse the operator along with the operands: the expected truth
        // values are unchanged, and both scalar orientations are exercised.
        let reverse_op = match op {
            "lt" => "gt",
            "le" => "ge",
            "gt" => "lt",
            "ge" => "le",
            other => other,
        };
        let (a, b, lhs, rhs, op) = if reversed {
            (right, left, rhs, lhs, reverse_op)
        } else {
            (left, right, lhs, rhs, op)
        };
        let expression = Expr::Comparison {
            op: op.into(),
            left: Box::new(lhs),
            right: Box::new(rhs),
        };
        assert_eq!(
            &compare_arrays(a, b, op).unwrap(),
            expected,
            "kernel: {:?}, rows={}, op={op}, scalar={scalar}, reversed={reversed}",
            left.data_type(),
            left.len(),
        );
        assert_eq!(
            &eval_bool(&expression, &batch).unwrap(),
            expected,
            "expression: {:?}, rows={}, op={op}, scalar={scalar}, reversed={reversed}",
            left.data_type(),
            left.len(),
        );
    }
}

// An independent scalar reference: normalize the two SQL equivalence classes
// and then use Rust's total ordering, rather than the kernel's IEEE predicates.
fn float_reference(left: Option<f64>, right: Option<f64>, op: &str) -> Option<bool> {
    let normalize = |v: f64| {
        if v == 0.0 {
            0.0
        } else if v.is_nan() {
            f64::NAN
        } else {
            v
        }
    };
    let ordering = left
        .zip(right)
        .map(|(a, b)| normalize(a).total_cmp(&normalize(b)));
    if matches!(op, "distinct_from" | "not_distinct_from") {
        let equal = ordering == Some(Ordering::Equal) || (left.is_none() && right.is_none());
        return Some(if op == "distinct_from" { !equal } else { equal });
    }
    ordering.map(|order| match op {
        "eq" => order.is_eq(),
        "ne" => !order.is_eq(),
        "lt" => order.is_lt(),
        "le" => !order.is_gt(),
        "gt" => order.is_gt(),
        "ge" => !order.is_lt(),
        _ => unreachable!(),
    })
}

fn float_value(array: &ArrayRef, index: usize) -> Option<f64> {
    if array.is_null(index) {
        None
    } else if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        Some(array.value(index))
    } else {
        Some(f64::from(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(index),
        ))
    }
}

fn check_floats(left: &ArrayRef, right: &ArrayRef, scalar: bool) {
    for op in OPS {
        let expected = BooleanArray::from_iter((0..left.len()).map(|row| {
            float_reference(
                float_value(left, row),
                float_value(right, if scalar { 0 } else { row }),
                op,
            )
        }));
        check(left, right, op, &expected, scalar);
    }
}

#[test]
fn float_special_values_nulls_scalars_and_slices() {
    // Include both NaN signs and signaling payloads at their original width.
    let f64_bits = [
        0,
        1,
        0x8000_0000_0000_0000,
        0x8000_0000_0000_0001,
        0x3ff0_0000_0000_0000,
        0xbff0_0000_0000_0000,
        0x7fef_ffff_ffff_ffff,
        0x7ff0_0000_0000_0000,
        0xfff0_0000_0000_0000,
        0x7ff8_0000_0000_0000,
        0xfff8_0000_0000_0001,
        0x7ff0_0000_0000_0001,
        0xfff0_0000_0000_0001,
    ];
    let f32_bits = [
        0,
        1,
        0x8000_0000,
        0x8000_0001,
        0x3f80_0000,
        0xbf80_0000,
        0x7f7f_ffff,
        0x7f80_0000,
        0xff80_0000,
        0x7fc0_0000,
        0xffc0_0001,
        0x7f80_0001,
        0xff80_0001,
    ];
    let cases: Vec<ArrayRef> = vec![
        Arc::new(Float64Array::from_iter(
            std::iter::once(None).chain(f64_bits.map(|v| Some(f64::from_bits(v)))),
        )),
        Arc::new(Float32Array::from_iter(
            std::iter::once(None).chain(f32_bits.map(|v| Some(f32::from_bits(v)))),
        )),
    ];
    for values in cases {
        let take = |indices: Vec<u32>| {
            arrow_select::take::take(&values, &arrow_array::UInt32Array::from(indices), None)
                .unwrap()
        };
        let n = values.len() as u32;
        let left = take(
            (0..n)
                .flat_map(|i| std::iter::repeat_n(i, n as usize))
                .collect(),
        );
        let right = take((0..n).cycle().take((n * n) as usize).collect());
        check_floats(&left, &right, false);
        // Offset slices exercise non-byte-aligned validity buffers as well as
        // the zero-row and single-row cases on either side of a literal.
        for offset in [0, 1, 7] {
            for len in [0, 1, 2, 65] {
                check_floats(&left.slice(offset, len), &right.slice(offset, len), false);
                for row in 0..values.len() {
                    check_floats(&left.slice(offset, len), &values.slice(row, 1), true);
                }
            }
        }
    }
}

#[test]
fn random_float_bits_with_and_without_nulls() {
    let mut seed = 0xdead_beef_1234_5678_u64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for nullable in [false, true] {
        let a: Vec<u64> = (0..4096).map(|_| next()).collect();
        let b: Vec<u64> = (0..4096).map(|_| next()).collect();
        let arrays = |bits: &[u64], divisor| -> [ArrayRef; 2] {
            [
                Arc::new(Float64Array::from_iter(bits.iter().enumerate().map(
                    |(i, v)| (!(nullable && i % divisor == 0)).then(|| f64::from_bits(*v)),
                ))),
                Arc::new(Float32Array::from_iter(bits.iter().enumerate().map(
                    |(i, v)| (!(nullable && i % divisor == 0)).then(|| f32::from_bits(*v as u32)),
                ))),
            ]
        };
        for (left, right) in arrays(&a, 13).into_iter().zip(arrays(&b, 17)) {
            check_floats(&left, &right, false);
            for row in [0, 1, 17, 63] {
                check_floats(&left, &right.slice(row, 1), true);
            }
        }
    }
}

#[test]
fn scalar_arrow_kernels_preserve_types_precision_and_nulls() {
    let cases: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])),
        Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])),
        Arc::new(Int64Array::from(vec![
            Some(9_007_199_254_740_992),
            None,
            Some(9_007_199_254_740_993),
        ])),
        Arc::new(UInt64Array::from(vec![
            Some(u64::MAX - 1),
            None,
            Some(u64::MAX),
        ])),
        Arc::new(StringArray::from(vec![Some("alpha"), None, Some("zulu")])),
        Arc::new(LargeStringArray::from(vec![
            Some("alpha"),
            None,
            Some("zulu"),
        ])),
        Arc::new(BinaryArray::from(vec![
            Some(b"a".as_slice()),
            None,
            Some(b"z".as_slice()),
        ])),
        Arc::new(Date32Array::from(vec![Some(-10), None, Some(10)])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![
                Some(9_007_199_254_740_992),
                None,
                Some(9_007_199_254_740_993),
            ])
            .with_timezone("UTC"),
        ),
        Arc::new(
            Decimal128Array::from(vec![
                Some(9_007_199_254_740_992_i128),
                None,
                Some(9_007_199_254_740_993_i128),
            ])
            .with_precision_and_scale(20, 3)
            .unwrap(),
        ),
    ];
    for values in cases {
        for scalar_row in 0..values.len() {
            for len in [0, 1, 3] {
                let left = values.slice(0, len);
                let right = values.slice(scalar_row, 1);
                // Each fixture is [low, NULL, high]. Use these ranks to check
                // the semantics without another Arrow comparison kernel.
                let rank = |i| [Some(0.0), None, Some(1.0)][i];
                for op in OPS {
                    let expected = BooleanArray::from_iter(
                        (0..len).map(|i| float_reference(rank(i), rank(scalar_row), op)),
                    );
                    check(&left, &right, op, &expected, true);
                }
            }
        }
    }
}

#[test]
fn dictionary_keys_and_values_can_both_be_null() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), Some(2), None, Some(3)]),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(f64::NAN),
                None,
                Some(7.0),
            ])),
        )
        .unwrap(),
    );
    let decoded = [Some(-0.0), Some(f64::NAN), None, None, Some(7.0)];
    for value in [None, Some(0.0), Some(f64::NAN), Some(7.0)] {
        let scalar: ArrayRef = Arc::new(Float64Array::from(vec![value]));
        for op in OPS {
            let expected = BooleanArray::from_iter(decoded.map(|v| float_reference(v, value, op)));
            check(&dictionary, &scalar, op, &expected, true);
        }
    }
}

#[test]
fn comparison_rejects_incompatible_lengths_types_and_operators() {
    let floats: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0]));
    let ints: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    for values in [&floats, &ints] {
        for op in OPS {
            assert!(compare_arrays(values, &values.slice(0, 2), op).is_err());
            assert!(compare_arrays(values, &values.slice(0, 0), op).is_err());
        }
        assert!(compare_arrays(values, values, "unknown").is_err());
    }
    assert!(compare_arrays(&floats, &ints, "eq").is_err());
}

#[test]
fn constant_comparisons_keep_batch_cardinality_and_nulls() {
    for rows in [0, 1, 65] {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .unwrap();
        for (left, right, expected) in [
            (Some(-0.0), Some(0.0), Some(true)),
            (Some(f64::NAN), Some(f64::NAN), Some(true)),
            (Some(1.0), Some(2.0), Some(false)),
            (None, Some(0.0), None),
        ] {
            let expression = Expr::Comparison {
                op: "eq".into(),
                left: Box::new(Expr::Literal(Arc::new(Float64Array::from(vec![left])))),
                right: Box::new(Expr::Literal(Arc::new(Float64Array::from(vec![right])))),
            };
            assert_eq!(
                eval_bool(&expression, &batch).unwrap(),
                BooleanArray::from(vec![expected; rows])
            );
        }
    }
}

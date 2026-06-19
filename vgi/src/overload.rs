// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Function overload resolution.
//!
//! Given several registered functions sharing a name, pick the one whose
//! argument specs best match the call: count compatibility first, then a type
//! score (exact = 2, same family = 1, ANY = 0, incompatible = reject).

use arrow_schema::{DataType, SchemaRef};

use crate::arguments::Arguments;
use crate::catalog::arg_type_to_arrow_pub as arg_type_to_arrow;
use crate::function::ArgSpec;

/// Resolve the best candidate index, or `None` if none are compatible.
pub fn resolve_overload<F>(
    count: usize,
    specs_of: F,
    args: &Arguments,
    input_schema: Option<&SchemaRef>,
) -> Option<usize>
where
    F: Fn(usize) -> Vec<ArgSpec>,
{
    if count == 1 {
        return Some(0);
    }
    let mut best: Option<(usize, i64)> = None;
    for idx in 0..count {
        let specs = specs_of(idx);
        if let Some(score) = score_candidate(&specs, args, input_schema) {
            if best.map(|(_, b)| score > b).unwrap_or(true) {
                best = Some((idx, score));
            }
        }
    }
    best.map(|(i, _)| i)
}

fn score_candidate(
    specs: &[ArgSpec],
    args: &Arguments,
    input_schema: Option<&SchemaRef>,
) -> Option<i64> {
    let const_specs: Vec<&ArgSpec> = specs
        .iter()
        .filter(|s| s.position >= 0 && s.is_const)
        .collect();
    let nonconst_specs: Vec<&ArgSpec> = specs
        .iter()
        .filter(|s| s.position >= 0 && !s.is_const)
        .collect();
    let varargs_spec = specs.iter().find(|s| s.is_varargs);
    let has_varargs = varargs_spec.is_some();

    let num_pos = args.num_positional();
    let num_const = const_specs.len();
    let input_fields = input_schema.map(|s| s.fields().len()).unwrap_or(0);

    if has_varargs {
        let vs = varargs_spec.unwrap();
        if vs.is_const {
            let mut non_varargs = num_const;
            non_varargs = non_varargs.saturating_sub(1);
            if num_pos < non_varargs {
                return None;
            }
        } else {
            if num_pos < num_const {
                return None;
            }
            if input_schema.is_some() {
                let non_varargs_nonconst = nonconst_specs.len().saturating_sub(1);
                if input_fields < non_varargs_nonconst {
                    return None;
                }
            }
        }
    } else {
        if num_pos != num_const {
            return None;
        }
        if input_schema.is_some()
            && !nonconst_specs.is_empty()
            && input_fields != nonconst_specs.len()
        {
            return None;
        }
    }

    let mut score: i64 = 0;

    // Score const args against the positional arg arrays.
    for spec in &const_specs {
        if spec.is_varargs {
            continue;
        }
        if let Some(a) = args.arg(spec.position as usize) {
            match score_type(a.data_type(), spec) {
                Some(s) => score += s,
                None => return None,
            }
        }
    }

    // Score const VARARGS: every positional arg at or past the varargs spec's
    // declared position must match the varargs type (so `repeat_value(2, 'a')`
    // picks the string overload, not the int one).
    if let Some(vs) = varargs_spec {
        if vs.is_const && vs.position >= 0 {
            for pos in (vs.position as usize)..num_pos {
                if let Some(a) = args.arg(pos) {
                    match score_type(a.data_type(), vs) {
                        Some(s) => score += s,
                        None => return None,
                    }
                }
            }
        }
    }

    // Score non-const (column) args against the input schema fields.
    if let Some(schema) = input_schema {
        let fields = schema.fields();
        if has_varargs && !varargs_spec.unwrap().is_const {
            // Score every input field against the varargs column type.
            let vs = varargs_spec.unwrap();
            for f in fields {
                match score_type(f.data_type(), vs) {
                    Some(s) => score += s,
                    None => return None,
                }
            }
        } else {
            for (i, spec) in nonconst_specs.iter().enumerate() {
                if let Some(f) = fields.get(i) {
                    match score_type(f.data_type(), spec) {
                        Some(s) => score += s,
                        None => return None,
                    }
                }
            }
        }
    }

    Some(score)
}

fn score_type(actual: &DataType, spec: &ArgSpec) -> Option<i64> {
    let expected = spec
        .arrow_data_type
        .clone()
        .unwrap_or_else(|| arg_type_to_arrow(&spec.arrow_type));
    // A genuinely ANY-typed arg (no concrete `arrow_data_type`) matches
    // anything with neutral score. `column_typed` leaves `arrow_type` empty
    // but sets a concrete `arrow_data_type` — those must score by exact match
    // so overloads like `type_info(int32)` vs `type_info(int64)` disambiguate.
    if (spec.arrow_data_type.is_none() && (spec.arrow_type == "any" || spec.arrow_type.is_empty()))
        || expected == DataType::Null
    {
        return Some(0);
    }
    if *actual == expected {
        return Some(2);
    }
    // Nested types (List/Struct/Map/…) compare structurally: `DataType` equality
    // includes inner field *names* and *nullability*, which a hand-built
    // `column_typed(List(Field("item", …)))` will never match against the list
    // type DuckDB actually sends (different child field name/nullability). Treat
    // structurally-equal nested types as an exact match so nested `column_typed`
    // overloads resolve.
    if nested_structurally_equal(actual, &expected) {
        return Some(2);
    }
    if same_family(actual, &expected) {
        return Some(1);
    }
    None
}

/// Structural type equality that ignores field names and nullability (so it sees
/// through the field-name/nullability differences in DuckDB-supplied nested
/// types). Scalars must still be exactly equal.
fn nested_structurally_equal(a: &DataType, b: &DataType) -> bool {
    use DataType::*;
    match (a, b) {
        (List(x), List(y))
        | (LargeList(x), LargeList(y))
        | (List(x), LargeList(y))
        | (LargeList(x), List(y)) => nested_structurally_equal(x.data_type(), y.data_type()),
        (FixedSizeList(x, nx), FixedSizeList(y, ny)) => {
            nx == ny && nested_structurally_equal(x.data_type(), y.data_type())
        }
        (Map(x, _), Map(y, _)) => nested_structurally_equal(x.data_type(), y.data_type()),
        (Struct(fx), Struct(fy)) => {
            fx.len() == fy.len()
                && fx
                    .iter()
                    .zip(fy.iter())
                    .all(|(f1, f2)| nested_structurally_equal(f1.data_type(), f2.data_type()))
        }
        // Non-nested: require exact equality (no widening here; `same_family`
        // handles same-family scalar widening separately).
        _ => a == b,
    }
}

fn is_integer(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
    )
}
fn is_float_or_decimal(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        Float16 | Float32 | Float64 | Decimal128(_, _) | Decimal256(_, _)
    )
}
fn is_string(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8)
}
fn is_binary(t: &DataType) -> bool {
    matches!(t, DataType::Binary | DataType::LargeBinary)
}
fn same_family(a: &DataType, b: &DataType) -> bool {
    (is_integer(a) && is_integer(b))
        || (is_float_or_decimal(a) && is_float_or_decimal(b))
        || (is_string(a) && is_string(b))
        || (is_binary(a) && is_binary(b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::ArgSpec;
    use std::sync::Arc;

    fn list_u8(item_name: &str, nullable: bool) -> DataType {
        DataType::List(Arc::new(arrow_schema::Field::new(
            item_name,
            DataType::UInt8,
            nullable,
        )))
    }

    #[test]
    fn column_typed_list_matches_despite_field_name_and_nullability() {
        // Spec built with one inner field name/nullability; the actual list the
        // engine supplies uses different ones — must still resolve (score 2).
        let spec = ArgSpec::column_typed("qual", 1, list_u8("item", true), "");
        let actual = list_u8("l", false);
        assert_eq!(score_type(&actual, &spec), Some(2));
    }

    #[test]
    fn column_typed_list_rejects_mismatched_child() {
        let spec = ArgSpec::column_typed("qual", 1, list_u8("item", true), "");
        let actual = DataType::List(Arc::new(arrow_schema::Field::new(
            "l",
            DataType::Utf8,
            true,
        )));
        assert_eq!(score_type(&actual, &spec), None);
    }
}

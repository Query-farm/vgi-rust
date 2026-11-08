// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Column-statistics serialization: the sparse-union IPC batch DuckDB's VGI
//! extension reads to seed the optimizer. Mirrors the canonical Python
//! `serialize_column_statistics`.

use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Int64Builder, StringBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch, UnionArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, Schema, UnionFields};
use vgi_rpc::{Result, RpcError};

use crate::ipc;

/// A typed min/max statistic value.
#[derive(Clone)]
pub enum StatValue {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    /// Raw bytes — used for GEOMETRY stats (WKB; the extension reinterprets the
    /// BLOB as the column's GEOMETRY type).
    Binary(Vec<u8>),
    /// A one-element Arrow array of any type — e.g. a `Date32`, `Timestamp`,
    /// `Int32` or `Decimal128` statistic that should keep the column's own
    /// logical type on the wire (as vgi-python's typed `pa.Scalar` does). A
    /// null element means the bound is unknown.
    Arrow(ArrayRef),
}
impl StatValue {
    /// A typed statistic from a one-element Arrow array (see [`StatValue::Arrow`]).
    pub fn arrow(value: ArrayRef) -> Self {
        StatValue::Arrow(value)
    }

    fn data_type(&self) -> DataType {
        match self {
            StatValue::Int64(_) => DataType::Int64,
            StatValue::Float64(_) => DataType::Float64,
            StatValue::Utf8(_) => DataType::Utf8,
            StatValue::Binary(_) => DataType::Binary,
            StatValue::Arrow(value) => value.data_type().clone(),
        }
    }

    /// The value as a one-element Arrow array.
    pub fn to_array(&self) -> Result<ArrayRef> {
        Ok(match self {
            StatValue::Int64(v) => Arc::new(arrow_array::Int64Array::from(vec![*v])),
            StatValue::Float64(v) => Arc::new(arrow_array::Float64Array::from(vec![*v])),
            StatValue::Utf8(v) => Arc::new(arrow_array::StringArray::from(vec![v.as_str()])),
            StatValue::Binary(v) => Arc::new(arrow_array::BinaryArray::from(vec![v.as_slice()])),
            StatValue::Arrow(value) => {
                if value.len() != 1 {
                    return Err(RpcError::value_error(format!(
                        "StatValue::Arrow must hold exactly one element, got {}",
                        value.len()
                    )));
                }
                value.clone()
            }
        })
    }
}

/// Optimizer statistics for one column.
#[derive(Clone)]
pub struct CatColStat {
    pub column_name: String,
    pub min: StatValue,
    pub max: StatValue,
    pub has_null: bool,
    pub has_not_null: bool,
    pub distinct_count: Option<i64>,
    pub contains_unicode: Option<bool>,
    pub max_string_length: Option<u64>,
}

/// Build a sparse-union (min or max) array over the stats. The union has one
/// child per distinct value type; each child has length `n` with the value at
/// rows of its type and null elsewhere.
fn build_union(
    stats: &[CatColStat],
    min: bool,
    order: &[DataType],
) -> Result<(UnionFields, ArrayRef)> {
    let values: Vec<ArrayRef> = stats
        .iter()
        .map(|s| if min { &s.min } else { &s.max }.to_array())
        .collect::<Result<_>>()?;
    let type_ids: Vec<i8> = values
        .iter()
        .map(|v| order.iter().position(|o| o == v.data_type()).unwrap_or(0) as i8)
        .collect();

    let mut children: Vec<ArrayRef> = Vec::with_capacity(order.len());
    let mut fields: Vec<(i8, Arc<Field>)> = Vec::with_capacity(order.len());
    for (code, dt) in order.iter().enumerate() {
        let parts: Vec<ArrayRef> = values
            .iter()
            .map(|v| {
                if v.data_type() == dt {
                    v.clone()
                } else {
                    arrow_array::new_null_array(dt, 1)
                }
            })
            .collect();
        let refs: Vec<&dyn arrow_array::Array> = parts.iter().map(|a| a.as_ref()).collect();
        let child = arrow_select::concat::concat(&refs)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        children.push(child);
        fields.push((
            code as i8,
            Arc::new(Field::new(format!("{code}"), dt.clone(), true)),
        ));
    }
    let union_fields: UnionFields = fields.into_iter().collect();
    let union = UnionArray::try_new(
        union_fields.clone(),
        ScalarBuffer::from(type_ids),
        None,
        children,
    )
    .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    Ok((union_fields, Arc::new(union)))
}

/// Serialize per-column statistics to the IPC batch the extension expects.
pub fn serialize_column_statistics(stats: &[CatColStat]) -> Result<Vec<u8>> {
    if stats.is_empty() {
        return ipc::write_schema(&Schema::empty());
    }
    // Distinct value types in insertion order (min governs the union layout;
    // max reuses the same field set).
    let mut order: Vec<DataType> = Vec::new();
    for s in stats {
        for dt in [s.min.data_type(), s.max.data_type()] {
            if !order.contains(&dt) {
                order.push(dt);
            }
        }
    }

    let (min_fields, min_union) = build_union(stats, true, &order)?;
    let (_max_fields, max_union) = build_union(stats, false, &order)?;

    let mut name_b = StringBuilder::new();
    let mut has_null_b = BooleanBuilder::new();
    let mut has_not_null_b = BooleanBuilder::new();
    let mut distinct_b = Int64Builder::new();
    let mut uni_b = BooleanBuilder::new();
    let mut maxlen_b = UInt64Builder::new();
    for s in stats {
        name_b.append_value(&s.column_name);
        has_null_b.append_value(s.has_null);
        has_not_null_b.append_value(s.has_not_null);
        match s.distinct_count {
            Some(d) => distinct_b.append_value(d),
            None => distinct_b.append_null(),
        }
        match s.contains_unicode {
            Some(u) => uni_b.append_value(u),
            None => uni_b.append_null(),
        }
        match s.max_string_length {
            Some(m) => maxlen_b.append_value(m),
            None => maxlen_b.append_null(),
        }
    }

    let union_dt = DataType::Union(min_fields, arrow_schema::UnionMode::Sparse);
    let schema = Arc::new(Schema::new(vec![
        Field::new("column_name", DataType::Utf8, true),
        Field::new("min", union_dt.clone(), true),
        Field::new("max", union_dt, true),
        Field::new("has_null", DataType::Boolean, true),
        Field::new("has_not_null", DataType::Boolean, true),
        Field::new("distinct_count", DataType::Int64, true),
        Field::new("contains_unicode", DataType::Boolean, true),
        Field::new("max_string_length", DataType::UInt64, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(name_b.finish()),
        min_union,
        max_union,
        Arc::new(has_null_b.finish()),
        Arc::new(has_not_null_b.finish()),
        Arc::new(distinct_b.finish()),
        Arc::new(uni_b.finish()),
        Arc::new(maxlen_b.finish()),
    ];
    let batch =
        RecordBatch::try_new(schema, cols).map_err(|e| RpcError::runtime_error(e.to_string()))?;
    ipc::write_batch(&batch)
}

/// Decode a [`serialize_column_statistics`] payload (from any SDK) back into
/// [`CatColStat`]s, in the order they were serialized.
///
/// `Int64` / `Float64` / `Utf8` / `Binary` values decode to their dedicated
/// [`StatValue`] variants; every other type (and an unknown, NULL bound)
/// decodes to [`StatValue::Arrow`]. An empty payload (no columns or no rows)
/// decodes to an empty list.
pub fn deserialize_column_statistics(bytes: &[u8]) -> Result<Vec<CatColStat>> {
    use arrow_array::cast::AsArray;
    use arrow_array::Array;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let schema = ipc::read_schema(bytes)?;
    if schema.fields().is_empty() {
        return Ok(Vec::new());
    }
    let batch = ipc::read_batch(bytes)?;
    let invalid = |what: &str| RpcError::type_error(format!("column statistics: {what}"));
    let column = |name: &str| {
        batch
            .column_by_name(name)
            .ok_or_else(|| invalid(&format!("missing column '{name}'")))
    };
    let names = column("column_name")?
        .as_string_opt::<i32>()
        .ok_or_else(|| invalid("column_name is not utf8"))?;
    let union = |name: &str| -> Result<&UnionArray> {
        column(name)?
            .as_any()
            .downcast_ref::<UnionArray>()
            .ok_or_else(|| invalid(&format!("{name} is not a union")))
    };
    let (mins, maxs) = (union("min")?, union("max")?);
    let bool_at = |name: &str, row: usize, default: bool| -> Result<bool> {
        let col = column(name)?
            .as_boolean_opt()
            .ok_or_else(|| invalid(&format!("{name} is not boolean")))?;
        Ok(if col.is_valid(row) {
            col.value(row)
        } else {
            default
        })
    };
    let value_at = |u: &UnionArray, row: usize| -> StatValue {
        let child = u.value(row);
        if child.is_null(0) {
            return StatValue::Arrow(child);
        }
        match child.data_type() {
            DataType::Int64 => StatValue::Int64(
                child
                    .as_primitive::<arrow_array::types::Int64Type>()
                    .value(0),
            ),
            DataType::Float64 => StatValue::Float64(
                child
                    .as_primitive::<arrow_array::types::Float64Type>()
                    .value(0),
            ),
            DataType::Utf8 => StatValue::Utf8(child.as_string::<i32>().value(0).to_string()),
            DataType::Binary => StatValue::Binary(child.as_binary::<i32>().value(0).to_vec()),
            _ => StatValue::Arrow(child),
        }
    };
    let distinct = column("distinct_count")?
        .as_primitive_opt::<arrow_array::types::Int64Type>()
        .ok_or_else(|| invalid("distinct_count is not int64"))?;
    let max_len = column("max_string_length")?
        .as_primitive_opt::<arrow_array::types::UInt64Type>()
        .ok_or_else(|| invalid("max_string_length is not uint64"))?;
    let unicode = column("contains_unicode")?
        .as_boolean_opt()
        .ok_or_else(|| invalid("contains_unicode is not boolean"))?;
    (0..batch.num_rows())
        .map(|row| {
            if names.is_null(row) {
                return Err(invalid("null column_name"));
            }
            Ok(CatColStat {
                column_name: names.value(row).to_string(),
                min: value_at(mins, row),
                max: value_at(maxs, row),
                has_null: bool_at("has_null", row, true)?,
                has_not_null: bool_at("has_not_null", row, true)?,
                distinct_count: distinct.is_valid(row).then(|| distinct.value(row)),
                contains_unicode: unicode.is_valid(row).then(|| unicode.value(row)),
                max_string_length: max_len.is_valid(row).then(|| max_len.value(row)),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stat(name: &str, min: StatValue, max: StatValue) -> CatColStat {
        CatColStat {
            column_name: name.to_string(),
            min,
            max,
            has_null: false,
            has_not_null: true,
            distinct_count: Some(3),
            contains_unicode: None,
            max_string_length: None,
        }
    }

    #[test]
    fn typed_statistics_round_trip() {
        let day = |d: i32| StatValue::arrow(Arc::new(arrow_array::Date32Array::from(vec![d])));
        let stats = vec![
            stat("id", StatValue::Int64(1), StatValue::Int64(9)),
            stat("day", day(20_000), day(20_004)),
            stat(
                "name",
                StatValue::Utf8("a".into()),
                StatValue::Utf8("z".into()),
            ),
        ];
        let bytes = serialize_column_statistics(&stats).unwrap();
        let back = deserialize_column_statistics(&bytes).unwrap();
        assert_eq!(back.len(), 3);
        assert!(matches!(back[0].min, StatValue::Int64(1)));
        assert!(matches!(back[0].max, StatValue::Int64(9)));
        match (&back[1].min, &back[1].max) {
            (StatValue::Arrow(lo), StatValue::Arrow(hi)) => {
                assert_eq!(lo.data_type(), &DataType::Date32);
                assert_eq!(
                    lo.as_any()
                        .downcast_ref::<arrow_array::Date32Array>()
                        .unwrap()
                        .value(0),
                    20_000
                );
                assert_eq!(
                    hi.as_any()
                        .downcast_ref::<arrow_array::Date32Array>()
                        .unwrap()
                        .value(0),
                    20_004
                );
            }
            _ => panic!("date statistics must stay typed"),
        }
        assert!(matches!(&back[2].max, StatValue::Utf8(v) if v == "z"));
        assert_eq!(back[0].distinct_count, Some(3));
        assert!(!back[0].has_null && back[0].has_not_null);
    }

    #[test]
    fn empty_statistics_decode_empty() {
        let bytes = serialize_column_statistics(&[]).unwrap();
        assert!(deserialize_column_statistics(&bytes).unwrap().is_empty());
        assert!(deserialize_column_statistics(&[]).unwrap().is_empty());
    }

    #[test]
    fn arrow_stat_must_be_one_element() {
        let bad = stat(
            "x",
            StatValue::arrow(Arc::new(arrow_array::Int32Array::from(vec![1, 2]))),
            StatValue::Int64(1),
        );
        assert!(serialize_column_statistics(&[bad]).is_err());
    }
}

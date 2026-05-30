//! Column-statistics serialization: the sparse-union IPC batch DuckDB's VGI
//! extension reads to seed the optimizer. Mirrors Go `column_statistics.go` /
//! Python `serialize_column_statistics`.

use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, Int8Array, RecordBatch, UnionArray};
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
}
impl StatValue {
    fn data_type(&self) -> DataType {
        match self {
            StatValue::Int64(_) => DataType::Int64,
            StatValue::Float64(_) => DataType::Float64,
            StatValue::Utf8(_) => DataType::Utf8,
        }
    }
    fn type_key(&self) -> u8 {
        match self {
            StatValue::Int64(_) => 0,
            StatValue::Float64(_) => 1,
            StatValue::Utf8(_) => 2,
        }
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
fn build_union(stats: &[CatColStat], min: bool, order: &[StatValue]) -> Result<(UnionFields, ArrayRef)> {
    let n = stats.len();
    let type_ids: Vec<i8> = stats
        .iter()
        .map(|s| {
            let v = if min { &s.min } else { &s.max };
            order.iter().position(|o| o.type_key() == v.type_key()).unwrap_or(0) as i8
        })
        .collect();

    let mut children: Vec<ArrayRef> = Vec::with_capacity(order.len());
    let mut fields: Vec<(i8, Arc<Field>)> = Vec::with_capacity(order.len());
    for (code, proto) in order.iter().enumerate() {
        let dt = proto.data_type();
        let child: ArrayRef = match dt {
            DataType::Int64 => {
                let mut b = Int64Builder::new();
                for s in stats {
                    match if min { &s.min } else { &s.max } {
                        StatValue::Int64(v) if proto.type_key() == 0 => b.append_value(*v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            DataType::Float64 => {
                let mut b = Float64Builder::new();
                for s in stats {
                    match if min { &s.min } else { &s.max } {
                        StatValue::Float64(v) if proto.type_key() == 1 => b.append_value(*v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            _ => {
                let mut b = StringBuilder::new();
                for s in stats {
                    match if min { &s.min } else { &s.max } {
                        StatValue::Utf8(v) if proto.type_key() == 2 => b.append_value(v),
                        _ => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
        };
        children.push(child);
        fields.push((code as i8, Arc::new(Field::new(format!("{code}"), dt, true))));
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
    let mut order: Vec<StatValue> = Vec::new();
    for s in stats {
        if !order.iter().any(|o| o.type_key() == s.min.type_key()) {
            order.push(s.min.clone());
        }
        if !order.iter().any(|o| o.type_key() == s.max.type_key()) {
            order.push(s.max.clone());
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
    let batch = RecordBatch::try_new(schema, cols)
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    ipc::write_batch(&batch)
}

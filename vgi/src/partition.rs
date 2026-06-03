// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Partition-column support: mark schema fields as partition columns and
//! compute the per-batch `vgi_partition_values#b64` metadata (base64 of a
//! 2-row min/max IPC batch) that the C++ extension reads to plan partitioned
//! aggregates. Mirrors Go `batch_emit.go` / Python `partition_field`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi_rpc::{Result, RpcError};

use crate::ipc;

/// Field-metadata key marking a column as a VGI partition column.
pub const PARTITION_COLUMN_KEY: &str = "vgi.partition_column";
/// Per-batch wire-metadata key carrying base64 partition (min,max) values.
pub const PARTITION_VALUES_META: &str = "vgi_partition_values#b64";

/// Build a field marked as a VGI partition column.
pub fn partition_field(name: &str, ty: DataType) -> Field {
    Field::new(name, ty, true)
        .with_metadata(HashMap::from([(PARTITION_COLUMN_KEY.to_string(), "true".to_string())]))
}

fn is_partition_field(f: &Field) -> bool {
    f.metadata().get(PARTITION_COLUMN_KEY).map(|v| v == "true").unwrap_or(false)
}

/// Compute the `vgi_partition_values#b64` metadata value for a batch, or
/// `None` if the (full) schema declares no partition columns or the batch is
/// empty. The value is base64(IPC of a 2-row batch holding [min, max] for each
/// partition column).
pub fn partition_values_b64(full_schema: &SchemaRef, batch: &RecordBatch) -> Result<Option<String>> {
    let part_fields: Vec<&Arc<Field>> =
        full_schema.fields().iter().filter(|f| is_partition_field(f)).collect();
    if part_fields.is_empty() || batch.num_rows() == 0 {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(part_fields.len());
    let mut cols = Vec::with_capacity(part_fields.len());
    for pf in &part_fields {
        let col = batch
            .column_by_name(pf.name())
            .ok_or_else(|| RpcError::value_error(format!(
                "partition column {:?} is partition-annotated but absent from emitted batch",
                pf.name()
            )))?;
        // min = first index, max = last index after an ascending sort.
        let order = arrow_ord::sort::sort_to_indices(col, None, None)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let lo = order.value(0);
        let hi = order.value(order.len() - 1);
        let pair = arrow_select::take::take(col, &UInt32Array::from(vec![lo, hi]), None)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        fields.push((***pf).clone());
        cols.push(pair);
    }
    let pv_schema = Arc::new(Schema::new(fields));
    let pv_batch = RecordBatch::try_new(pv_schema, cols)
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    Ok(Some(base64_encode(&ipc::write_batch(&pv_batch)?)))
}

/// Build the per-batch metadata map for a partition-aware emit.
pub fn partition_metadata(
    full_schema: &SchemaRef,
    batch: &RecordBatch,
) -> Result<Option<HashMap<String, String>>> {
    Ok(partition_values_b64(full_schema, batch)?
        .map(|b64| HashMap::from([(PARTITION_VALUES_META.to_string(), b64)])))
}

/// Standard base64 encoding (no line breaks).
fn base64_encode(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(T[((n >> 18) & 63) as usize] as char);
        out.push(T[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 { T[((n >> 6) & 63) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { T[(n & 63) as usize] as char } else { '=' });
    }
    out
}

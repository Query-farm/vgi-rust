// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Flat wire serialization for VGI protocol DTOs.
//!
//! The vgi_rpc wire format carries a method's request/response as a
//! **single-row [`RecordBatch`] whose columns are the DTO's fields**
//! (a *flat* schema). `vgi_rpc`'s [`VgiArrow`] derive instead models a
//! struct as a nested `Struct<fields>` column. This module bridges the
//! two: it flattens the derived `StructArray` into the top-level
//! columns the C++ extension expects, byte-compatible with the Python /
//! Go / Java workers.
//!
//! Define every DTO with `#[derive(VgiArrow)]` and round-trip it with
//! [`to_batch`] / [`from_batch`]; obtain its flat schema with
//! [`flat_schema`].

use std::sync::Arc;

use arrow_array::{Array, BinaryArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi_rpc::{Result, RpcError, VgiArrow};

/// The flat Arrow schema for DTO `T` — one column per struct field.
///
/// `T::arrow_data_type()` must be a `Struct`; the struct's children
/// become the schema's top-level fields.
pub fn flat_schema<T: VgiArrow>() -> SchemaRef {
    match T::arrow_data_type() {
        arrow_schema::DataType::Struct(fields) => Arc::new(Schema::new(fields)),
        other => unreachable!(
            "flat_schema requires a struct VgiArrow type, got {other:?} for {}",
            T::describe_name()
        ),
    }
}

/// Serialize `value` into a 1-row [`RecordBatch`] with [`flat_schema`].
pub fn to_batch<T: VgiArrow>(value: T) -> Result<RecordBatch> {
    let arr = T::build_singleton(value)?;
    let sa = arr
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| RpcError::type_error("VgiArrow DTO did not build a StructArray"))?;
    Ok(RecordBatch::from(sa))
}

/// Parse DTO `T` out of the request's 1-row [`RecordBatch`].
///
/// Columns are matched by name (extra columns are ignored, missing
/// required columns error), so wire column ordering is irrelevant.
pub fn from_batch<T: VgiArrow>(batch: &RecordBatch) -> Result<T> {
    if batch.num_rows() == 0 {
        return Err(RpcError::type_error(format!(
            "empty request batch for {}",
            T::describe_name()
        )));
    }
    let sa = StructArray::from(batch.clone());
    T::read(&sa, 0)
}

/// Parse DTO `T` out of an arbitrary row of a [`StructArray`] column
/// (used for nested struct columns in list/struct DTOs).
pub fn read_struct<T: VgiArrow>(arr: &dyn Array, idx: usize) -> Result<T> {
    T::read(arr, idx)
}

/// The schema of every unary *response*: a single `result: binary` column.
///
/// The canonical wire wraps unary results as one `result` column holding the
/// IPC-serialized flat DTO batch (Go `serializeResult`); the flat DTO schema
/// is the schema of those inner bytes, not of the response batch itself.
pub fn result_binary_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Binary,
        false,
    )]))
}

/// Serialize a response DTO into the wire `{result: binary}` envelope.
pub fn to_result_batch<T: VgiArrow>(value: T) -> Result<RecordBatch> {
    let inner = to_batch(value)?;
    let bytes = crate::ipc::write_batch(&inner)?;
    let arr = BinaryArray::from(vec![bytes.as_slice()]);
    RecordBatch::try_new(result_binary_schema(), vec![Arc::new(arr)])
        .map_err(|e| RpcError::runtime_error(format!("build result envelope: {e}")))
}

/// Wrap already-serialized IPC bytes in the `{result: binary}` envelope (for
/// methods whose result is a pre-built batch, e.g. column statistics).
pub fn result_batch_from_bytes(bytes: &[u8]) -> Result<RecordBatch> {
    let arr = BinaryArray::from(vec![bytes]);
    RecordBatch::try_new(result_binary_schema(), vec![Arc::new(arr)])
        .map_err(|e| RpcError::runtime_error(format!("build result envelope: {e}")))
}

/// The `{result: binary}` envelope wrapping an empty (0-column) response —
/// used by methods whose response DTO has no fields (aggregate update/combine).
pub fn empty_result_batch() -> Result<RecordBatch> {
    use arrow_array::RecordBatchOptions;
    let inner = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(0)),
    )
    .map_err(|e| RpcError::runtime_error(format!("empty inner: {e}")))?;
    let bytes = crate::ipc::write_batch(&inner)?;
    let arr = BinaryArray::from(vec![bytes.as_slice()]);
    RecordBatch::try_new(result_binary_schema(), vec![Arc::new(arr)])
        .map_err(|e| RpcError::runtime_error(format!("build empty envelope: {e}")))
}

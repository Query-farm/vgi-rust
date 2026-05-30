//! Arrow IPC stream helpers for the binary-valued wire fields.
//!
//! Many VGI wire fields are `binary` columns carrying an IPC-serialized
//! Arrow object — either a full record batch (schema + 1 row + EOS) or a
//! bare schema (schema message + EOS). These mirror the Go helpers
//! `SerializeSchema` / per-type `ipc.NewWriter(...)` so the bytes are
//! byte-compatible with Python / Go / the C++ extension.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{Schema, SchemaRef};
use vgi_rpc::wire::StreamReader as VgiStreamReader;
use vgi_rpc::{Result, RpcError};

/// Serialize a record batch to an Arrow IPC stream (schema + batch + EOS).
pub fn write_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    write_batch_with_schema(batch, batch.schema().as_ref())
}

/// Like [`write_batch`] but emits the IPC schema message from `schema` instead
/// of `batch.schema()`. The columns are written as-is. Used to declare a column
/// non-nullable on the wire while its array still carries NULLs (arrow's safe
/// constructors reject this, but the C++ extension requires it for inlined
/// optimizer hints — see `catalog::serialize_items`).
pub fn write_batch_with_schema(batch: &RecordBatch, schema: &Schema) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| RpcError::runtime_error(format!("ipc writer: {e}")))?;
        w.write(batch)
            .map_err(|e| RpcError::runtime_error(format!("ipc write: {e}")))?;
        w.finish()
            .map_err(|e| RpcError::runtime_error(format!("ipc finish: {e}")))?;
    }
    Ok(buf)
}

/// Read the first record batch from an Arrow IPC stream.
///
/// Uses `vgi_rpc`'s nullability-relaxing reader because the C++ extension and
/// pyarrow declare `Annotated[T | None]` DTO fields as `nullable=false` yet
/// legitimately send null values; arrow-rust's strict reader would reject
/// them. `relax_nullability()` promotes every field to nullable before the
/// batch is constructed, matching the canonical wire behavior.
pub fn read_batch(bytes: &[u8]) -> Result<RecordBatch> {
    let mut reader = VgiStreamReader::new(Cursor::new(bytes))?.relax_nullability();
    match reader.read_next()? {
        Some((batch, _meta)) => Ok(batch),
        None => Err(RpcError::type_error("ipc stream had no record batch")),
    }
}

/// Read the schema of an Arrow IPC stream without requiring a batch.
pub fn read_schema(bytes: &[u8]) -> Result<SchemaRef> {
    let reader = VgiStreamReader::new(Cursor::new(bytes))?;
    Ok(reader.schema())
}

/// Serialize a bare schema to an Arrow IPC stream (schema message + EOS).
pub fn write_schema(schema: &Schema) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| RpcError::runtime_error(format!("ipc schema writer: {e}")))?;
        w.finish()
            .map_err(|e| RpcError::runtime_error(format!("ipc schema finish: {e}")))?;
    }
    Ok(buf)
}

/// Convenience: serialize a [`SchemaRef`].
pub fn write_schema_ref(schema: &SchemaRef) -> Result<Vec<u8>> {
    write_schema(schema.as_ref())
}

/// Convenience: wrap a schema in an `Arc`.
pub fn arc_schema(schema: Schema) -> SchemaRef {
    Arc::new(schema)
}

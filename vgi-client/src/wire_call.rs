// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Turning a typed request into a typed response.
//!
//! Every VGI unary method has the same two-layer shape, and this module is the
//! only place that knows it:
//!
//! - **Request** — either a flat params batch (`catalog_table_get` and friends)
//!   or the wrapped `{request: binary}` envelope carrying an IPC-encoded inner
//!   DTO. The generated structs in [`vgi_protocol::generated::request_params`]
//!   cover both cases uniformly, so callers just hand over a struct.
//! - **Response** — always the `{result: binary}` envelope whose single cell is
//!   an IPC-encoded batch of the flat response DTO.
//!
//! List-shaped responses add a third layer: the inner DTO is `ItemsResult`,
//! whose `items` is a list of *further* IPC-encoded batches, one per entry.

use vgi_protocol::ipc;
use vgi_protocol::protocol::dtos::ItemsResult;
use vgi_protocol::wire;
use vgi_rpc::errors::{Result, RpcError};
use vgi_rpc::{Bytes, VgiArrow};

use crate::transport::VgiTransport;

/// IPC-encode a DTO for carriage in a wrapped `{request: binary}` envelope.
///
/// Methods whose params are a single `request` column expect the real request
/// dataclass serialized as an Arrow IPC stream inside that cell.
pub fn envelope<T: VgiArrow>(inner: T) -> Result<Bytes> {
    let batch = wire::to_batch(inner)?;
    Ok(Bytes(ipc::write_batch(&batch)?))
}

/// Unwrap the `{result: binary}` response envelope into its inner batch.
fn unwrap_result(
    batch: &arrow_array::RecordBatch,
    method: &str,
) -> Result<arrow_array::RecordBatch> {
    use arrow_array::{Array, BinaryArray};

    let col = batch.column_by_name("result").ok_or_else(|| {
        RpcError::type_error(format!(
            "{method}: response is missing the `result` column (got {:?})",
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        ))
    })?;
    let arr = col
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| RpcError::type_error(format!("{method}: `result` column is not Binary")))?;
    if arr.is_empty() {
        return Err(RpcError::type_error(format!(
            "{method}: response envelope carried no rows"
        )));
    }
    ipc::read_batch(arr.value(0))
}

/// Call a method that returns a single typed DTO, given a pre-built params batch.
///
/// The batch form exists for the handful of methods whose params carry no
/// columns at all (`catalog_catalogs`), which have no generated struct because
/// `VgiArrow` cannot derive on a field-less type.
pub fn call_raw<R: VgiArrow>(
    tr: &mut dyn VgiTransport,
    method: &str,
    params: &arrow_array::RecordBatch,
) -> Result<R> {
    let response = tr.call_unary(method, params)?;
    let inner = unwrap_result(&response, method)?;
    wire::from_batch(&inner)
}

/// Call a method that returns a single typed DTO.
pub fn call<P, R>(tr: &mut dyn VgiTransport, method: &str, params: P) -> Result<R>
where
    P: VgiArrow,
    R: VgiArrow,
{
    let batch = wire::to_batch(params)?;
    call_raw(tr, method, &batch)
}

/// Decode an `ItemsResult` given a pre-built params batch.
pub fn call_items_raw<I: VgiArrow>(
    tr: &mut dyn VgiTransport,
    method: &str,
    params: &arrow_array::RecordBatch,
) -> Result<Vec<I>> {
    let items: ItemsResult = call_raw(tr, method, params)?;
    decode_items(items, method)
}

/// Call a method that returns nothing.
///
/// Void methods (`catalog_detach`, the transaction enders, the DDL family)
/// register an **empty result schema**, so the worker replies with a batch that
/// has no columns at all — not a `{result: binary}` envelope wrapping an empty
/// inner batch. See `register_void` in the worker's `protocol::register`.
///
/// Both shapes are accepted: a zero-column response is the canonical void
/// reply, and a wrapped one is unwrapped and discarded so a worker that chooses
/// to send an envelope still interoperates. Anything else is an error, so a
/// method that unexpectedly returns data does not pass silently.
pub fn call_unit<P: VgiArrow>(tr: &mut dyn VgiTransport, method: &str, params: P) -> Result<()> {
    let batch = wire::to_batch(params)?;
    let response = tr.call_unary(method, &batch)?;
    if response.num_columns() == 0 {
        return Ok(());
    }
    unwrap_result(&response, method)?;
    Ok(())
}

/// Call a method that returns `ItemsResult`, decoding each item.
///
/// Catalog discovery is all this shape: the outer DTO holds a list of binary
/// blobs, each an IPC batch of one `SchemaInfo` / `TableInfo` / `FunctionInfo`.
pub fn call_items<P, I>(tr: &mut dyn VgiTransport, method: &str, params: P) -> Result<Vec<I>>
where
    P: VgiArrow,
    I: VgiArrow,
{
    let items: ItemsResult = call(tr, method, params)?;
    decode_items(items, method)
}

fn decode_items<I: VgiArrow>(items: ItemsResult, method: &str) -> Result<Vec<I>> {
    items
        .items
        .into_iter()
        .enumerate()
        .map(|(i, blob)| {
            let batch = ipc::read_batch(&blob.0).map_err(|e| {
                RpcError::type_error(format!(
                    "{method}: item {i} is not a readable IPC batch: {e}"
                ))
            })?;
            wire::from_batch(&batch).map_err(|e| {
                RpcError::type_error(format!("{method}: item {i} failed to decode: {e}"))
            })
        })
        .collect()
}

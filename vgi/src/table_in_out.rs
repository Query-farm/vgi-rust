// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Table-in-out function model: transform input batches to output batches.
//!
//! Driven as an exchange stream: each input batch produces zero or more output
//! batches. The dispatch adapter applies auto-filter pushdown to the output.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use vgi_rpc::{Result, RpcError};

use crate::cache_control::CacheControl;
use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};

/// Per-batch wire-metadata key carrying per-output-row provenance for the
/// batched correlated-LATERAL operator: a base64-encoded raw little-endian
/// `int32[]` where element `i` is the 0-based index (into this call's input
/// batch) of the row that produced output row `i`. Absent metadata = identity
/// 1→1 map (the extension assumes it, and requires output rows == input rows).
pub const PARENT_ROW_METADATA_KEY: &str = "vgi_rpc.parent_row#b64";

/// Options for one emitted table-in-out output batch (all optional).
#[derive(Default)]
pub struct EmitOptions {
    /// Arbitrary per-batch wire metadata (merged under the derived keys).
    pub metadata: Option<HashMap<String, String>>,
    /// Result-cache advertisement — rendered to `vgi.cache.*` keys.
    pub cache_control: Option<CacheControl>,
    /// Per-output-row provenance for the batched correlated-LATERAL operator:
    /// `parent_rows[i]` is the input-row index that produced output row `i`.
    /// MUST carry exactly one entry per emitted row (validated at emit).
    pub parent_rows: Option<Vec<i32>>,
}

/// Collects one `process` call's output batches with optional per-batch
/// metadata — the emit surface for
/// [`TableInOutFunction::process_out`]. Plain 1→1 transforms keep returning
/// `Vec<RecordBatch>` from [`TableInOutFunction::process`]; override
/// `process_out` (and emit through this) only when a batch needs
/// [`EmitOptions`] (cache control, LATERAL provenance, raw metadata).
#[derive(Default)]
pub struct TableInOutOutput {
    pub(crate) items: Vec<(RecordBatch, Option<HashMap<String, String>>)>,
}

impl TableInOutOutput {
    /// Emit a batch with no per-batch metadata.
    pub fn emit(&mut self, batch: RecordBatch) {
        self.items.push((batch, None));
    }

    /// Emit a batch with [`EmitOptions`] (metadata / cache control / LATERAL
    /// provenance). Mirrors the Python `out.emit(batch, metadata=...,
    /// cache_control=..., parent_rows=...)` kwargs.
    pub fn emit_with(&mut self, batch: RecordBatch, opts: EmitOptions) -> Result<()> {
        let mut md: HashMap<String, String> = opts.metadata.unwrap_or_default();
        if let Some(parent_rows) = opts.parent_rows {
            if parent_rows.len() != batch.num_rows() {
                return Err(RpcError::runtime_error(format!(
                    "emit_with(parent_rows=...) length {} != batch.num_rows {}; parent_rows \
                     must carry exactly one input-row index per emitted output row",
                    parent_rows.len(),
                    batch.num_rows()
                )));
            }
            // Nothing to map on an empty emit; skip the base64+pack.
            if !parent_rows.is_empty() {
                let mut raw = Vec::with_capacity(parent_rows.len() * 4);
                for v in &parent_rows {
                    raw.extend_from_slice(&v.to_le_bytes());
                }
                md.insert(
                    PARENT_ROW_METADATA_KEY.to_string(),
                    crate::partition::base64_encode(&raw),
                );
            }
        }
        if let Some(cc) = opts.cache_control {
            md.extend(cc.to_metadata());
        }
        self.items
            .push((batch, if md.is_empty() { None } else { Some(md) }));
        Ok(())
    }
}

/// A table-in-out VGI function.
pub trait TableInOutFunction: Send + Sync {
    fn name(&self) -> &str;
    fn metadata(&self) -> FunctionMetadata;
    fn argument_specs(&self) -> Vec<ArgSpec>;
    /// Secret lookups to request at bind (two-phase secret resolution). When
    /// non-empty and secrets are not yet resolved, `bind` returns these and the
    /// extension re-binds with the resolved values (preserving the input
    /// schema). The resolved secret is then available on `params.secrets` at
    /// `process` time. Mirrors [`TableFunction::secret_lookups`](crate::table_function::TableFunction::secret_lookups).
    fn secret_lookups(&self, _params: &BindParams) -> Vec<crate::secrets::SecretLookup> {
        Vec::new()
    }
    /// Resolve the output schema. Default: passthrough the input schema.
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("table-in-out requires an input schema"))?;
        Ok(BindResponse {
            output_schema: input,
            opaque_data: Vec::new(),
        })
    }
    /// Transform one input batch into output batches. Default: pass through
    /// the input; the dispatcher filters and then projects it by column name. A
    /// distributed/accumulating function persists partial state to
    /// `params.storage` here and returns an empty batch.
    ///
    /// Accumulate in the **execution's** scope (`params.execution_id`), which
    /// every connection of the execution shares and the finalize carries —
    /// never under a process id. One process serves many connections under the
    /// launcher, TCP or HTTP, and a client may fan one execution across several
    /// of them, each its own substream (`params.substream_id`), then finalize
    /// once: per-process state is overwritten by its neighbours, and
    /// per-substream state is invisible to a `finish` running on another
    /// substream. Appending (`FunctionStorage::append`) is safe for any number
    /// of writers; state overwritten in place must not share a key between two
    /// connections. `substream_partial_sum` in the example worker is the
    /// pattern.
    fn process(&self, _params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(vec![batch.clone()])
    }

    /// Transform one input batch, emitting through `out` — the metadata-capable
    /// variant of [`process`](Self::process). Override this instead of
    /// `process` when an output batch needs [`EmitOptions`]: a `vgi.cache.*`
    /// result-cache advertisement, per-output-row LATERAL provenance
    /// (`parent_rows`), or raw per-batch wire metadata. The default delegates
    /// to `process` and emits each returned batch with no metadata; the
    /// dispatcher always drives this method.
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        for b in self.process(params, batch)? {
            out.emit(b);
        }
        Ok(())
    }

    /// Whether the function flushes accumulated state at end-of-stream (drives
    /// the `FINALIZE` init phase). Default: no.
    fn has_finish(&self) -> bool {
        false
    }

    /// End-of-stream: drain the execution's accumulated partials from
    /// `params.storage` — every substream's, not only this finalize's — and
    /// emit the final output batches. Only called when `has_finish()` is true.
    fn finish(&self, _params: &ProcessParams) -> Result<Vec<RecordBatch>> {
        Ok(Vec::new())
    }
}

/// Project a batch to `schema`'s columns by name (projection pushdown).
pub fn project_batch(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    // If the schemas already match, pass through unchanged.
    if batch.schema().fields() == schema.fields() {
        return Ok(batch.clone());
    }
    let mut cols = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        match batch.schema().column_with_name(f.name()) {
            Some((i, _)) => cols.push(batch.column(i).clone()),
            None => {
                return Err(RpcError::runtime_error(format!(
                    "projection column '{}' not found in input",
                    f.name()
                )))
            }
        }
    }
    RecordBatch::try_new_with_options(
        schema.clone(),
        cols,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| RpcError::runtime_error(format!("project batch: {e}")))
}

/// Build an `Arc<Schema>`.
pub fn arc(s: Schema) -> Arc<Schema> {
    Arc::new(s)
}

/// Preserve row correlation when SDK filtering changes an exchange's output.
pub(crate) fn filter_parent_rows(
    metadata: &mut Option<HashMap<String, String>>,
    keep: &BooleanArray,
    input_rows: usize,
) -> Result<()> {
    use base64::{engine::general_purpose::STANDARD, Engine};

    let selected = keep
        .iter()
        .enumerate()
        .filter_map(|(index, value)| (value == Some(true)).then_some(index))
        .collect::<Vec<_>>();
    let encoded = metadata
        .as_ref()
        .and_then(|m| m.get(PARENT_ROW_METADATA_KEY));
    let parents = if let Some(encoded) = encoded {
        let bytes = STANDARD
            .decode(encoded)
            .map_err(|_| RpcError::value_error("invalid parent_rows base64"))?;
        if bytes.len() % 4 != 0 || bytes.len() / 4 != keep.len() {
            return Err(RpcError::value_error(
                "parent_rows length differs from unfiltered output",
            ));
        }
        let parents = bytes
            .chunks_exact(4)
            .map(|b| i32::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        if parents.iter().any(|p| *p < 0 || *p as usize >= input_rows) {
            return Err(RpcError::value_error(
                "parent_rows index outside input batch",
            ));
        }
        Some(parents)
    } else {
        None
    };
    if selected.is_empty() {
        if let Some(metadata) = metadata {
            metadata.remove(PARENT_ROW_METADATA_KEY);
        }
        return Ok(());
    }
    if selected.len() == keep.len() {
        return Ok(());
    }
    if parents.is_none() && keep.len() != input_rows {
        return Err(RpcError::value_error(
            "SDK filtering changes an unannotated table-in-out batch with non-identity cardinality; emit explicit parent_rows with TableInOutOutput::emit_with",
        ));
    }
    let mut bytes = Vec::with_capacity(selected.len() * 4);
    for index in selected {
        let parent = match &parents {
            Some(parents) => parents[index],
            None => i32::try_from(index)
                .map_err(|_| RpcError::value_error("parent_rows index exceeds int32"))?,
        };
        bytes.extend_from_slice(&parent.to_le_bytes());
    }
    metadata
        .get_or_insert_with(HashMap::new)
        .insert(PARENT_ROW_METADATA_KEY.into(), STANDARD.encode(bytes));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};

    #[test]
    fn zero_column_projection_preserves_rows() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let projected = project_batch(&batch, &Arc::new(Schema::empty())).unwrap();
        assert_eq!(projected.num_columns(), 0);
        assert_eq!(projected.num_rows(), 3);
    }

    fn parent_metadata(parents: &[i32]) -> Option<HashMap<String, String>> {
        Some(HashMap::from([
            ("custom".into(), "preserved".into()),
            (
                PARENT_ROW_METADATA_KEY.into(),
                crate::partition::base64_encode(
                    &parents
                        .iter()
                        .flat_map(|p| p.to_le_bytes())
                        .collect::<Vec<_>>(),
                ),
            ),
        ]))
    }

    #[test]
    fn sdk_filter_selects_explicit_and_identity_parent_rows() {
        let keep = BooleanArray::from(vec![Some(false), Some(true), None, Some(true)]);
        let mut explicit = parent_metadata(&[1, 0, 2, 1]);
        filter_parent_rows(&mut explicit, &keep, 3).unwrap();
        assert_eq!(explicit, parent_metadata(&[0, 1]));
        let mut identity = None;
        filter_parent_rows(&mut identity, &keep, 4).unwrap();
        assert_eq!(
            identity.unwrap()[PARENT_ROW_METADATA_KEY],
            parent_metadata(&[1, 3]).unwrap()[PARENT_ROW_METADATA_KEY]
        );
        filter_parent_rows(
            &mut explicit,
            &BooleanArray::from(vec![Some(false), None]),
            3,
        )
        .unwrap();
        assert!(!explicit
            .as_ref()
            .unwrap()
            .contains_key(PARENT_ROW_METADATA_KEY));
        assert_eq!(explicit.unwrap()["custom"], "preserved");
    }

    #[test]
    fn sdk_filter_rejects_malformed_or_ambiguous_parent_rows() {
        let keep = BooleanArray::from(vec![false, true, true]);
        assert!(filter_parent_rows(&mut parent_metadata(&[0, 1]), &keep, 3).is_err());
        assert!(filter_parent_rows(&mut parent_metadata(&[0, -1, 1]), &keep, 3).is_err());
        assert!(filter_parent_rows(&mut parent_metadata(&[0, 3, 1]), &keep, 3).is_err());
        let mut malformed = Some(HashMap::from([(
            PARENT_ROW_METADATA_KEY.into(),
            "%%%".into(),
        )]));
        assert!(filter_parent_rows(&mut malformed, &keep, 3).is_err());
        // Two surviving rows do not mean the original three rows were a 1:1
        // transformation of the two input rows.
        assert!(filter_parent_rows(&mut None, &keep, 2).is_err());
    }
}

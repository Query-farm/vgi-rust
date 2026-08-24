// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Table (producer) function model: generate output batches without input.
//!
//! The function creates a
//! per-execution [`TableProducer`] whose `produce` is called repeatedly; it
//! emits one batch per tick and calls `out.finish()` when exhausted.

use arrow_schema::SchemaRef;
use vgi_rpc::Result;

use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};

/// Cardinality estimate for a table function.
#[derive(Clone, Copy, Default)]
pub struct TableCardinality {
    pub estimate: Option<i64>,
    pub max: Option<i64>,
}

/// A per-execution producer. Holds the function's mutable scan state.
///
/// Returns the next batch, or `None` when the scan is exhausted. The dispatch
/// adapter applies projection / auto-filter pushdown to each batch before
/// emitting, so producers stay free of that concern. `out` is provided only
/// for `client_log` — do NOT emit through it (the adapter emits the returned
/// batch).
pub trait TableProducer: Send {
    /// Produce the next output batch, or `None` when the scan is exhausted.
    /// Called repeatedly until it returns `None`. `out` is for `client_log`
    /// only — return the batch, do not emit through `out`.
    fn next_batch(
        &mut self,
        out: &mut vgi_rpc::OutputCollector,
    ) -> Result<Option<arrow_array::RecordBatch>>;
    /// Serialize the producer's in-progress scan position for HTTP continuation
    /// (default empty — producers whose whole result is regenerable from the
    /// shared work queue alone need none). Work-queue producers that span a
    /// popped chunk across multiple batches MUST encode their partial-chunk
    /// cursor here, since the chunk is destructively removed from the queue on
    /// pop and cannot be re-derived on resume.
    fn encode_resume(&self) -> Vec<u8> {
        Vec::new()
    }
    /// Restore the scan position after rebuilding from an HTTP state token.
    /// Inverse of [`encode_resume`](Self::encode_resume).
    fn restore_resume(&mut self, _bytes: &[u8]) {}
    /// Whether this producer can serialize its scan position for HTTP
    /// continuation. When `true`, the HTTP transport returns one batch per
    /// response and resumes via a state token (so the whole result set never
    /// has to fit in memory), exactly like the Python and Go workers.
    ///
    /// When `false` (the default) the producer gets exactly ONE HTTP turn: it
    /// completes inside the `/init` response if it has a single batch to give,
    /// and is REFUSED with a clear error if it has more. So `false` is only
    /// safe for a producer that is genuinely one batch (or none), or that will
    /// never be served over HTTP. Anything that chunks its output — any loop,
    /// any batch-size argument — must implement these three methods. Over the
    /// byte-stream transports (pipe / unix / tcp) this flag is not consulted at
    /// all: the client ticks the producer directly, one batch per tick.
    ///
    /// A producer is rebuilt fresh from its bind params on resume and then has
    /// [`restore_resume`](Self::restore_resume) called, so
    /// [`encode_resume`](Self::encode_resume) only needs to carry the scan
    /// *position* — never anything regenerable from the params. Override this to
    /// `true` whenever you implement those two hooks.
    fn resume_supported(&self) -> bool {
        false
    }
    /// Per-batch wire metadata for the batch just returned by `next_batch`
    /// (e.g. `vgi_batch_index` for `supports_batch_index` functions). Default
    /// none. Called once after each `next_batch` that returns `Some`.
    fn last_metadata(&self) -> Option<std::collections::HashMap<String, String>> {
        None
    }
    /// Called before each `next_batch` with the per-tick dynamic pushdown
    /// filters (from the `vgi_pushdown_filters` request metadata), if any. Lets
    /// a producer observe a tightening Top-N filter. Default ignores them.
    fn on_dynamic_filters(&mut self, _filters: Option<&crate::pushdown::PushdownFilters>) {}
    /// Called before each `next_batch` with the client's conditional-revalidation
    /// validators, when it sent any. The client holds a stale-but-revalidatable
    /// cached result (one this function advertised with
    /// [`CacheControl::with_revalidatable`](crate::cache_control::CacheControl::with_revalidatable))
    /// and is asking whether it is still fresh. A producer that recognizes the
    /// validators answers with a 0-row batch whose
    /// [`last_metadata`](Self::last_metadata) carries
    /// [`CacheControl::with_not_modified`](crate::cache_control::CacheControl::with_not_modified),
    /// instead of re-streaming the payload. Default ignores them, which simply
    /// recomputes the result.
    fn on_conditional_request(&mut self, _request: &crate::cache_control::ConditionalRequest) {}
}

/// A table (producer) VGI function: generates rows with no row input.
///
/// A table function is a *factory*: at bind time it resolves an output schema
/// ([`on_bind`](Self::on_bind)), and for each execution it builds a
/// [`TableProducer`] ([`producer`](Self::producer)) that yields output batches
/// until exhausted. Implement [`name`](Self::name), [`metadata`](Self::metadata),
/// [`argument_specs`](Self::argument_specs), [`on_bind`](Self::on_bind), and
/// [`producer`](Self::producer); everything else (cardinality, statistics,
/// parallelism, secrets) has a default. Projection and pushed-down filters are
/// applied to each emitted batch by the framework, so producers don't handle
/// them. Register with [`Worker::register_table`](crate::Worker::register_table).
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use arrow_array::{ArrayRef, Int64Array, RecordBatch};
/// use arrow_schema::{DataType, Field, Schema, SchemaRef};
/// use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
/// use vgi::table_function::{TableFunction, TableProducer};
/// use vgi_rpc::{OutputCollector, Result, RpcError};
///
/// /// `count_to(n)` — emit a single `value` column 0..n.
/// struct CountTo;
///
/// struct CountProducer {
///     schema: SchemaRef,
///     n: i64,
///     done: bool,
/// }
///
/// impl TableProducer for CountProducer {
///     fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
///         if self.done {
///             return Ok(None);
///         }
///         self.done = true;
///         let col: ArrayRef = Arc::new((0..self.n).collect::<Int64Array>());
///         let batch = RecordBatch::try_new(self.schema.clone(), vec![col])
///             .map_err(|e| RpcError::runtime_error(e.to_string()))?;
///         Ok(Some(batch))
///     }
/// }
///
/// impl TableFunction for CountTo {
///     fn name(&self) -> &str {
///         "count_to"
///     }
///     fn metadata(&self) -> FunctionMetadata {
///         FunctionMetadata::default()
///     }
///     fn argument_specs(&self) -> Vec<ArgSpec> {
///         vec![ArgSpec::const_arg("n", 0, "int64", "Upper bound (exclusive)")]
///     }
///     fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
///         let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]));
///         Ok(BindResponse { output_schema: schema, opaque_data: Vec::new() })
///     }
///     fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
///         Ok(Box::new(CountProducer {
///             schema: params.output_schema.clone(),
///             n: params.arguments.const_i64(0).unwrap_or(0),
///             done: false,
///         }))
///     }
/// }
/// ```
pub trait TableFunction: Send + Sync {
    /// The SQL name this table function is exposed as (e.g. `"count_to"`).
    fn name(&self) -> &str;
    /// Optimizer- and discovery-facing properties. Start from
    /// [`FunctionMetadata::default`].
    fn metadata(&self) -> FunctionMetadata;
    /// The argument list, built with the [`ArgSpec`] constructors.
    fn argument_specs(&self) -> Vec<ArgSpec>;
    /// Resolve the output schema from bind-time arguments.
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse>;
    /// Worker parallelism hint (default single worker).
    fn max_workers(&self, _params: &BindParams) -> i64 {
        1
    }
    /// Primary-worker global init: runs once per execution (when DuckDB issues
    /// the init without an execution_id) before any producer. Use it to push
    /// work items onto `params.storage`'s queue for parallel-scan producers.
    /// Secondary workers (init carrying an execution_id) skip it.
    fn on_init(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }
    /// Optional cardinality estimate.
    fn cardinality(&self, _params: &BindParams) -> Option<TableCardinality> {
        None
    }

    /// Divide this scan into named, independently redeemable splits.
    ///
    /// Returning `None` (the default) means this function is not split-capable:
    /// the whole scan is one unit of work, and the client falls back to
    /// primary/secondary init. Override together with [`TableFunction::on_split`]
    /// and `FunctionMetadata::supports_splits`.
    ///
    /// A split *names* work rather than describing it. "These three files at
    /// version 47" survives a retry; "rows 0-999 of whatever this returns now"
    /// does not — and a distributed engine WILL retry, so the difference is
    /// correctness, not tidiness. The same split may also be redeemed more than
    /// once (recursive CTEs, re-collected DataFrames, task retry) and may be
    /// abandoned mid-stream (LIMIT, TopK, an empty join build side); neither is
    /// an error.
    ///
    /// Set only `payload` on each split. The framework stamps the consistency
    /// anchor, the bind fingerprint and (where a key exists) the seal, so an
    /// author cannot forget the anchor or mis-bind the fingerprint.
    ///
    /// Size splits into comparable units of work and honour
    /// `request.target_split_bytes`: a claiming client treats them as
    /// interchangeable because it cannot see per-split cost, so wildly uneven
    /// splits leave its makespan bounded by the largest one.
    fn on_plan(
        &self,
        _params: &BindParams,
        _request: &crate::protocol::dtos::TableFunctionPlanRequest,
    ) -> Result<Option<crate::split_token::PlanOutcome>> {
        Ok(None)
    }

    /// Called on a split init, with the verified payloads on
    /// `params.split_payloads`.
    ///
    /// Any state carried from planning to reading must live in cross-process
    /// storage keyed by `execution_id`: the process that plans is, in the
    /// general case, not the process that reads — and under a distributed engine
    /// it is not even the same host.
    fn on_split(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }
    /// Optional per-column optimizer statistics for this call.
    fn statistics(&self, _params: &BindParams) -> Option<Vec<crate::statistics::CatColStat>> {
        None
    }
    /// Secret types this function needs (triggers the two-phase secret bind).
    fn secret_lookups(&self, _params: &BindParams) -> Vec<crate::secrets::SecretLookup> {
        Vec::new()
    }
    /// Build the per-execution producer. `params.output_schema` is the
    /// (possibly projection-narrowed) schema to emit.
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>>;

    /// Post-execution diagnostics surfaced as Extra Info under EXPLAIN ANALYZE.
    /// Reads whatever the producer persisted to `storage` keyed by
    /// `global_execution_id`. Default: no extra info.
    fn dynamic_to_string(
        &self,
        _global_execution_id: &[u8],
        _storage: &dyn crate::storage::FunctionStorage,
    ) -> Vec<(String, String)> {
        Vec::new()
    }
}

/// Helpers for serializing a producer's scan position into an HTTP continuation
/// token. A resumable producer is rebuilt fresh from its bind params and then
/// re-seeded via [`TableProducer::restore_resume`], so these encode only the
/// cursor integers (current index, remaining, a 0/1 done flag, …) — never
/// anything that the params already regenerate.
pub mod resume {
    /// Pack a slice of `i64` cursor values little-endian.
    pub fn pack(vals: &[i64]) -> Vec<u8> {
        let mut v = Vec::with_capacity(vals.len() * 8);
        for x in vals {
            v.extend_from_slice(&x.to_le_bytes());
        }
        v
    }

    /// Unpack exactly `n` `i64` values; returns `None` if the byte length does
    /// not match (e.g. an empty/corrupt token), so callers degrade to a fresh
    /// start rather than panic.
    pub fn unpack(bytes: &[u8], n: usize) -> Option<Vec<i64>> {
        if bytes.len() != n * 8 {
            return None;
        }
        Some(
            (0..n)
                .map(|i| {
                    let o = i * 8;
                    i64::from_le_bytes(bytes[o..o + 8].try_into().unwrap())
                })
                .collect(),
        )
    }
}

/// Narrow a full schema to the projected columns (`projection_ids`).
pub fn project_schema(full: &SchemaRef, ids: &Option<Vec<i64>>) -> SchemaRef {
    match ids {
        Some(ids) if !ids.is_empty() => {
            let fields: Vec<_> = ids
                .iter()
                .filter_map(|&i| full.fields().get(i as usize).cloned())
                .collect();
            std::sync::Arc::new(arrow_schema::Schema::new(fields))
        }
        _ => full.clone(),
    }
}

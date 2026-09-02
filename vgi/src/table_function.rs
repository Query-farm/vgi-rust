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
/// emitting, so producers stay free of that concern. `out` is provided for
/// `client_log` and the transport's response-budget snapshot — do NOT emit
/// through it (the adapter emits the returned batch). HTTP-aware producers can
/// size their next batch toward `out.preferred_response_bytes()` and must treat
/// `out.response_limit_bytes()` as the hard decoded, uncompressed Arrow IPC
/// response ceiling. Both are `None` on transports or deployments that
/// supplied no budget.
pub trait TableProducer: Send {
    /// Produce the next output batch, or `None` when the scan is exhausted.
    /// Called repeatedly until it returns `None`. `out` exposes client logging
    /// plus immutable `response_limit_bytes()` / `preferred_response_bytes()`
    /// snapshots for this turn; return the batch, do not emit through `out`.
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
    /// [`encode_resume`](Self::encode_resume) only needs to carry what ADVANCES
    /// — never anything regenerable from the params.
    ///
    /// Do not implement these three by hand. [`resume_fields!`](crate::resume_fields)
    /// writes all of them from a list of the advancing fields:
    ///
    /// ```ignore
    /// vgi::resume_fields!(cursor, rows_emitted);
    /// ```
    ///
    /// The `false` default is a historical wart, kept only because 84
    /// implementations rely on it. It is the wrong default: a producer that
    /// chunks its output and forgets to override it compiles, passes the
    /// byte-stream transports (which never consult this flag), and fails only
    /// over HTTP. That is how several producers shipped drain-only —
    /// `batch_limit` used to make declining harmless, and when vgi-rpc 0.23.0
    /// removed it the bill arrived all at once.
    ///
    /// So state the decision explicitly even when it is `false`, and say why —
    /// the two producers that legitimately could not resume both turned out to
    /// be fixable once their STATE travelled instead of just a position.
    ///
    /// REQUIRED — deliberately no default. A default of `false` let a producer
    /// decline continuation by saying nothing, which is how several shipped
    /// drain-only and stayed that way until lock-step removed the escape hatch.
    /// A wrong answer here is still possible, but it is now a written one.
    fn resume_supported(&self) -> bool;
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
///     fn resume_supported(&self) -> bool {
///         // Single batch — nothing to resume. Required, so it is stated.
///         false
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

// ---------------------------------------------------------------------------
// Resume via serde — the ergonomic path
// ---------------------------------------------------------------------------

/// Encode a producer's advancing scan position with bincode.
///
/// The counterpart to [`decode_resume_state`]. Used by
/// [`resume_fields!`](crate::resume_fields), not usually called directly.
pub fn encode_resume_state<T: serde::Serialize>(value: &T) -> Vec<u8> {
    // A failure here would silently lose the position and restart the scan at
    // row 0, so it must not be swallowed. Encoding a tuple of plain scalars
    // cannot realistically fail; if it ever does, that is a bug worth seeing.
    vgi_rpc::stream_codec::bincode_encode(value).unwrap_or_else(|e| panic!("encode_resume: {e}"))
}

/// Decode what [`encode_resume_state`] wrote, or `None` if the bytes are not
/// what this producer wrote.
///
/// `None` rather than a panic: the bytes arrive from a client-supplied token.
/// They are AEAD-sealed, so they cannot be forged — but a worker rolled back to
/// an older build can legitimately meet a token minted by a newer one, and
/// dropping the position is survivable where a panic is not.
pub fn decode_resume_state<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Option<T> {
    vgi_rpc::stream_codec::bincode_decode(bytes).ok()
}

/// Implement the three [`TableProducer`] resume hooks from a list of fields.
///
/// A producer that chunks its output MUST be resumable, or it cannot serve
/// HTTP: vgi-rpc 0.23.0 made producers strictly lock-step (one invocation, at
/// most one batch per response), so every multi-batch scan takes a
/// continuation. Writing the three hooks by hand is what that used to mean, and
/// it is why producers kept shipping without them — `resume_supported`
/// defaults to `false`, so declining was silent and, before lock-step,
/// harmless.
///
/// Name the fields that ADVANCE and nothing else:
///
/// ```ignore
/// impl TableProducer for MyProducer {
///     fn next_batch(&mut self, out: &mut OutputCollector) -> Result<Option<RecordBatch>> { ... }
///     vgi::resume_fields!(cursor, rows_emitted);
/// }
/// ```
///
/// Only advancing fields, because a resumed producer is rebuilt from its bind
/// params FIRST and this then overwrites the cursor — so the schema, the
/// argument values, storage handles and anything else derived from the params
/// are already correct and must not be carried. That is also why this takes a
/// field list rather than serializing the producer whole: most producers hold a
/// `SchemaRef` or an `Arc<dyn FunctionStorage>`, neither of which is
/// `Serialize`, and neither of which should travel.
///
/// The listed fields must be `Serialize + DeserializeOwned` — cursors, counts
/// and offsets are.
#[macro_export]
macro_rules! resume_fields {
    ($($field:ident),+ $(,)?) => {
        fn resume_supported(&self) -> bool {
            true
        }

        fn encode_resume(&self) -> ::std::vec::Vec<u8> {
            $crate::table_function::encode_resume_state(&($(self.$field.clone(),)+))
        }

        fn restore_resume(&mut self, bytes: &[u8]) {
            if bytes.is_empty() {
                return;
            }
            if let ::std::option::Option::Some(($($field,)+)) =
                $crate::table_function::decode_resume_state::<($(
                    $crate::resume_fields!(@ty $field),
                )+)>(bytes)
            {
                $(self.$field = $field;)+
            }
        }
    };
    // Field types are inferred from the assignment target, so the tuple element
    // type only needs a placeholder the compiler can unify.
    (@ty $field:ident) => { _ };
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Exchange-mode result-cache table-in-out fixtures: a cacheable classic
//! (TABLE-input) passthrough, a cacheable blended map, and the
//! always-revalidate (304 `not_modified`) variants of both.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use vgi::cache_control::CacheControl;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_in_out::{project_batch, EmitOptions, TableInOutFunction, TableInOutOutput};
use vgi_rpc::{Result, RpcError};

/// Register the exchange-cache fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_table_in_out(CachedEchoFunction);
    w.register_table_in_out(CachedDoubleFunction);
    w.register_table_in_out(CachedExplodeFunction);
    w.register_table_in_out(CachedRevalidatingEchoFunction);
    w.register_table_in_out(CachedRevalidatingDoubleFunction);
}

const CACHE_TTL: i64 = 300;

fn cache_meta(description: &str, categories: &[&str], blended: bool) -> FunctionMetadata {
    FunctionMetadata {
        description: description.to_string(),
        categories: categories.iter().map(|s| s.to_string()).collect(),
        input_from_args: blended,
        ..Default::default()
    }
}

/// Stable etag from a batch's content (deterministic across runs for equal
/// data). Only compared against itself by the same worker, so any stable
/// content digest works — FNV-1a over the batch's IPC bytes, hex-rendered.
fn content_etag(batch: &RecordBatch) -> Result<String> {
    let bytes = vgi::ipc::write_batch(batch)?;
    let mut h: u64 = 0xcbf29ce484222325;
    for b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    Ok(format!("{h:016x}"))
}

/// Compute `x * 2` (int64) from the blended input column.
fn doubled_batch(params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
    let cast = arrow_cast::cast(batch.column(0), &DataType::Int64)
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    let xs = cast.as_primitive::<Int64Type>();
    let vals: Int64Array = (0..xs.len())
        .map(|i| {
            if xs.is_valid(i) {
                Some(xs.value(i) * 2)
            } else {
                None
            }
        })
        .collect();
    RecordBatch::try_new(
        params.output_schema.clone(),
        vec![Arc::new(vals) as ArrayRef],
    )
    .map_err(|e| RpcError::runtime_error(e.to_string()))
}

fn doubled_bind() -> Result<BindResponse> {
    Ok(BindResponse {
        output_schema: Arc::new(Schema::new(vec![Field::new(
            "doubled",
            DataType::Int64,
            true,
        )])),
        opaque_data: Vec::new(),
    })
}

/// `cached_echo(input)` — cacheable CLASSIC (TABLE-input) streaming
/// table-in-out passthrough.
///
/// Called as `FROM cached_echo((SELECT ...))` — a non-correlated table-in-out
/// routed through the streaming exchange's per-input-batch memoization.
/// Passthrough output (input schema) advertising a ttl on each output batch;
/// a cache hit on a repeat scan is proven by a zero `write_input` count.
pub struct CachedEchoFunction;
impl TableInOutFunction for CachedEchoFunction {
    fn name(&self) -> &str {
        "cached_echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta(
            "Cacheable classic (TABLE-input) passthrough (advertises vgi.cache.ttl)",
            &["cache", "test"],
            false,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("data", 0, "table", "Input table")]
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        out.emit_with(
            project_batch(batch, &params.output_schema)?,
            EmitOptions {
                cache_control: Some(CacheControl::ttl(CACHE_TTL)),
                ..Default::default()
            },
        )
    }
}

/// `cached_double(x)` — cacheable blended 1→1 map (`x → x*2`) advertising
/// `vgi.cache.*`.
///
/// Backs exchange-mode result-cache tests on BOTH call shapes served by the
/// same registration: the streaming column form (per-input-batch memoization)
/// and the correlated LATERAL form (the batched operator's per-chunk / per-value
/// memoization). Deterministic output so a cache hit returns identical values.
pub struct CachedDoubleFunction;
impl TableInOutFunction for CachedDoubleFunction {
    fn name(&self) -> &str {
        "cached_double"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta(
            "Cacheable blended map x -> x*2 (advertises vgi.cache.ttl)",
            &["blended", "cache", "test"],
            true,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("x", 0, "int64", "Input column")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        doubled_bind()
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        out.emit_with(
            doubled_batch(params, batch)?,
            EmitOptions {
                cache_control: Some(CacheControl::ttl(CACHE_TTL).with_per_value()),
                ..Default::default()
            },
        )
    }
}

/// `cached_explode(n)` — cacheable blended 1→N fan-out advertising
/// `vgi.cache.per_value`.
///
/// Same shape as `blended_explode` (emit `0..n-1` per input row, with
/// per-output-row provenance) but opts into the per-value memo tier, so it
/// covers the cardinalities the 1:1 `cached_double` cannot reach: `n=0` is a
/// NEGATIVE memo (a length-0 slot) and `n>1` a 1:N slot whose rows must survive
/// the store's gather, the `[cached|fresh]` splice, and (on disk) the per-slot
/// Arrow-IPC round trip. Deterministic (emits `range(n)`), so tests assert exact
/// values and equivalence with per-value off.
///
/// A test choice, not production advice — memoizing a trivial fan-out is a net
/// loss; the point is deterministic coverage of the 1:N and negative-memo paths.
pub struct CachedExplodeFunction;
impl TableInOutFunction for CachedExplodeFunction {
    fn name(&self) -> &str {
        "cached_explode"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta(
            "Cacheable blended 1->N fan-out (per_value) — 1:0 / 1:1 / 1:N by input",
            &["blended", "cache", "test"],
            true,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column(
            "n",
            0,
            "int64",
            "Fan-out count: emit rows 0..n-1 for this input row",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)])),
            opaque_data: Vec::new(),
        })
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        let cast = arrow_cast::cast(batch.column(0), &DataType::Int64)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let ns = cast.as_primitive::<Int64Type>();
        let counts: Vec<i64> = (0..ns.len())
            .map(|i| {
                if ns.is_valid(i) {
                    ns.value(i).max(0)
                } else {
                    0
                }
            })
            .collect();
        // INTERLEAVE parents round-robin (round k emits value k for every parent
        // with n>k), so within one chunk a given parent's output rows are
        // NON-contiguous (parents [1,2,3] -> parent indices [0,1,2, 1,2, 2]).
        // A per-value gather that assumed contiguous runs per parent would
        // corrupt the result; this makes that failure visible.
        let mut vals: Vec<i64> = Vec::new();
        let mut parent_rows: Vec<i32> = Vec::new();
        for k in 0..counts.iter().copied().max().unwrap_or(0) {
            for (row_idx, &n) in counts.iter().enumerate() {
                if n > k {
                    vals.push(k);
                    parent_rows.push(row_idx as i32);
                }
            }
        }
        let col = Arc::new(Int64Array::from(vals)) as ArrayRef;
        let out_batch = RecordBatch::try_new(params.output_schema.clone(), vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        out.emit_with(
            out_batch,
            EmitOptions {
                parent_rows: Some(parent_rows),
                cache_control: Some(CacheControl::ttl(CACHE_TTL).with_per_value()),
                ..Default::default()
            },
        )
    }
}

/// `cached_reval_echo(input)` — classic (TABLE-input) passthrough with the
/// always-revalidate (304) contract.
///
/// Advertises `CacheControl(ttl=0, etag, revalidatable)` on its output — the
/// "no-cache" semantic: stored but immediately stale, so every repeat sends a
/// conditional request (`vgi.cache.if_none_match`). On a matching validator
/// the worker answers with a 0-row `not_modified` batch and the client reuses
/// the stored bytes instead of re-streaming. The etag is derived from the
/// input content so it is stable across identical repeats.
pub struct CachedRevalidatingEchoFunction;
impl TableInOutFunction for CachedRevalidatingEchoFunction {
    fn name(&self) -> &str {
        "cached_reval_echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta(
            "Classic passthrough with always-revalidate (304 not_modified) contract",
            &["cache", "test"],
            false,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("data", 0, "table", "Input table")]
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        let batch = project_batch(batch, &params.output_schema)?;
        let etag = content_etag(&batch)?;
        let fresh = CacheControl::ttl(0)
            .with_etag(etag.clone())
            .with_revalidatable();
        if params.if_none_match.as_deref() == Some(etag.as_str()) {
            // 304 Not Modified: the client's stored copy for this input is
            // still valid — answer with a 0-row batch of the same schema.
            return out.emit_with(
                batch.slice(0, 0),
                EmitOptions {
                    cache_control: Some(fresh.with_not_modified()),
                    ..Default::default()
                },
            );
        }
        out.emit_with(
            batch,
            EmitOptions {
                cache_control: Some(fresh),
                ..Default::default()
            },
        )
    }
}

/// `cached_reval_double(x)` — blended map (`x → x*2`) with the
/// always-revalidate (304) contract.
///
/// Like [`CachedRevalidatingEchoFunction`] but blended, so it exercises the
/// LATERAL exchange-cache revalidation path. The etag is derived from the
/// worker-input content (the positional arg) — stable across identical
/// repeats. On a matching `if_none_match` it answers 0-row `not_modified`.
pub struct CachedRevalidatingDoubleFunction;
impl TableInOutFunction for CachedRevalidatingDoubleFunction {
    fn name(&self) -> &str {
        "cached_reval_double"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta(
            "Blended map x->x*2 with always-revalidate (304 not_modified) contract",
            &["blended", "cache", "test"],
            true,
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("x", 0, "int64", "Input column")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        doubled_bind()
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        let etag = content_etag(batch)?;
        let fresh = CacheControl::ttl(0)
            .with_etag(etag.clone())
            .with_revalidatable();
        if params.if_none_match.as_deref() == Some(etag.as_str()) {
            return out.emit_with(
                RecordBatch::new_empty(params.output_schema.clone()),
                EmitOptions {
                    cache_control: Some(fresh.with_not_modified()),
                    ..Default::default()
                },
            );
        }
        out.emit_with(
            doubled_batch(params, batch)?,
            EmitOptions {
                cache_control: Some(fresh),
                ..Default::default()
            },
        )
    }
}

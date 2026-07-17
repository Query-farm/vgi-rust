// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Exchange-mode result-cache table-in-out fixtures: a cacheable classic
//! (TABLE-input) passthrough and a cacheable blended map.

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
                cache_control: Some(CacheControl::ttl(CACHE_TTL)),
                ..Default::default()
            },
        )
    }
}

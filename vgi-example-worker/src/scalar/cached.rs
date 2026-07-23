// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Cacheable scalar fixtures — result-cache opt-in via
//! [`ScalarFunction::cache_control`]: the returned `vgi.cache.*` metadata
//! rides every output batch so the extension can memoize the scalar's output
//! per distinct input value. Pure, deterministic scalars only.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use vgi::cache_control::CacheControl;
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, ScalarFunction,
};
use vgi_rpc::{Result, RpcError};

/// Register the cacheable scalar fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_scalar(CachedDoubleScalarFunction);
    w.register_scalar(CachedAddConstScalarFunction);
    w.register_scalar(CachedLabelScalarFunction);
}

const CACHE_TTL: i64 = 300;

fn i64_col(col: &ArrayRef) -> Result<Int64Array> {
    let cast = arrow_cast::cast(col, &DataType::Int64)
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    Ok(cast.as_primitive::<Int64Type>().clone())
}

fn result_batch(params: &ProcessParams, col: ArrayRef) -> Result<RecordBatch> {
    RecordBatch::try_new(params.output_schema.clone(), vec![col])
        .map_err(|e| RpcError::runtime_error(e.to_string()))
}

/// `cached_double_scalar(value)` — doubles a BIGINT value and advertises
/// `vgi.cache.*`; backs the scalar per-value memoization tests. A
/// deterministic 1:1 map, so opting into the result cache is sound.
pub struct CachedDoubleScalarFunction;
impl ScalarFunction for CachedDoubleScalarFunction {
    fn name(&self) -> &str {
        "cached_double_scalar"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Doubles a BIGINT value (advertises vgi.cache.ttl for per-value memo)"
                .to_string(),
            return_type: Some(DataType::Int64),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Value to double")]
    }
    fn cache_control(&self) -> Option<CacheControl> {
        Some(CacheControl::ttl(CACHE_TTL).with_per_value())
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let v = i64_col(batch.column(0))?;
        let out: Int64Array = (0..v.len())
            .map(|i| {
                if v.is_valid(i) {
                    Some(v.value(i) * 2)
                } else {
                    None
                }
            })
            .collect();
        result_batch(params, Arc::new(out))
    }
}

/// `cached_add_const(value, addend)` — `value + addend` (a CONST param),
/// cacheable. Backs the per-value const-param keying tests: two calls with the
/// same `value` but different `addend` must NOT cross-serve — the const arg is
/// folded into the cache key.
pub struct CachedAddConstScalarFunction;
impl ScalarFunction for CachedAddConstScalarFunction {
    fn name(&self) -> &str {
        "cached_add_const"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "value + const addend (advertises vgi.cache.ttl)".to_string(),
            return_type: Some(DataType::Int64),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("value", 0, "int64", "Value"),
            ArgSpec::const_arg("addend", 1, "int64", "Constant addend"),
        ]
    }
    fn cache_control(&self) -> Option<CacheControl> {
        Some(CacheControl::ttl(CACHE_TTL).with_per_value())
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(DataType::Int64))
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let addend = params.arguments.const_i64(1).unwrap_or(0);
        let v = i64_col(batch.column(0))?;
        let out: Int64Array = (0..v.len())
            .map(|i| {
                if v.is_valid(i) {
                    Some(v.value(i) + addend)
                } else {
                    None
                }
            })
            .collect();
        result_batch(params, Arc::new(out))
    }
}

/// `cached_label(value)` — `'lbl-<value>'` for `value >= 0`, NULL otherwise,
/// cacheable. Exercises a heap-string + NULL round-trip through the per-value
/// cache.
pub struct CachedLabelScalarFunction;
impl ScalarFunction for CachedLabelScalarFunction {
    fn name(&self) -> &str {
        "cached_label"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "value -> 'lbl-<value>' or NULL for negatives (advertises vgi.cache.ttl)"
                .to_string(),
            return_type: Some(DataType::Utf8),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Value")]
    }
    fn cache_control(&self) -> Option<CacheControl> {
        Some(CacheControl::ttl(CACHE_TTL).with_per_value())
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let v = i64_col(batch.column(0))?;
        let out: StringArray = (0..v.len())
            .map(|i| {
                if v.is_valid(i) && v.value(i) >= 0 {
                    Some(format!("lbl-{}", v.value(i)))
                } else {
                    None
                }
            })
            .collect();
        result_batch(params, Arc::new(out))
    }
}

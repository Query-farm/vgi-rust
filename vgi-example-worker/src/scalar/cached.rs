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
    w.register_scalar(CachedRevalidatingDoubleScalarFunction);
    w.register_scalar(CachedRevalidationPolicyScalarFunction);
    w.register_scalar(CachedAddConstScalarFunction);
    w.register_scalar(CachedLabelScalarFunction);
}

fn input_etag(batch: &RecordBatch) -> Result<String> {
    let bytes = vgi::ipc::write_batch(batch)?;
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    Ok(format!("{hash:016x}"))
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

/// Always-stale per-value scalar used to prove conditional exchange
/// revalidation. A matching validator returns an empty `not_modified` answer;
/// the client must replay its stored one-row result.
pub struct CachedRevalidatingDoubleScalarFunction;
impl ScalarFunction for CachedRevalidatingDoubleScalarFunction {
    fn name(&self) -> &str {
        "cached_reval_double_scalar"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Doubles BIGINT with ttl=0 conditional per-value caching".to_string(),
            return_type: Some(DataType::Int64),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Value to double")]
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
    ) -> Result<(RecordBatch, Option<CacheControl>)> {
        let etag = input_etag(batch)?;
        let control = CacheControl::ttl(0)
            .with_etag(etag.clone())
            .with_revalidatable()
            .with_per_value();
        if params.if_none_match.as_deref() == Some(etag.as_str()) {
            std::thread::sleep(std::time::Duration::from_millis(250));
            return Ok((
                RecordBatch::new_empty(params.output_schema.clone()),
                Some(control.with_not_modified()),
            ));
        }
        Ok((self.process(params, batch)?, Some(control)))
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let values = i64_col(batch.column(0))?;
        let output: Int64Array = (0..values.len())
            .map(|index| values.is_valid(index).then(|| values.value(index) * 2))
            .collect();
        result_batch(params, Arc::new(output))
    }
}

/// Per-value counterpart to `cached_reval_policy`, for adapter tests that
/// verify stale-if-error and explicit revocation on scalar exchanges.
pub struct CachedRevalidationPolicyScalarFunction;
impl ScalarFunction for CachedRevalidationPolicyScalarFunction {
    fn name(&self) -> &str {
        "cached_reval_policy_scalar"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Doubles BIGINT with selectable conditional cache policy".to_string(),
            return_type: Some(DataType::Int64),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("value", 0, "int64", "Value to double"),
            ArgSpec::const_arg("policy", 1, "varchar", "Conditional response policy"),
        ]
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
    ) -> Result<(RecordBatch, Option<CacheControl>)> {
        let etag = input_etag(batch)?;
        let control = CacheControl::ttl(0)
            .with_etag(etag.clone())
            .with_revalidatable()
            .with_stale_if_error(60)
            .with_per_value();
        if params.if_none_match.as_deref() == Some(etag.as_str()) {
            let empty = RecordBatch::new_empty(params.output_schema.clone());
            return match params.arguments.const_str(1).unwrap_or_default().as_str() {
                "error" => Err(RpcError::runtime_error(
                    "intentional conditional cache failure",
                )),
                "not_modified_no_store" => {
                    Ok((empty, Some(CacheControl::no_store().with_not_modified())))
                }
                "fresh_no_store" => {
                    Ok((self.process(params, batch)?, Some(CacheControl::no_store())))
                }
                "transaction" => Ok((
                    empty,
                    Some(
                        CacheControl::ttl(60)
                            .with_transaction_scope()
                            .with_not_modified(),
                    ),
                )),
                _ => Ok((empty, Some(control.with_not_modified()))),
            };
        }
        Ok((self.process(params, batch)?, Some(control)))
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let values = i64_col(batch.column(0))?;
        let output: Int64Array = (0..values.len())
            .map(|index| values.is_valid(index).then(|| values.value(index) * 2))
            .collect();
        result_batch(params, Arc::new(output))
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

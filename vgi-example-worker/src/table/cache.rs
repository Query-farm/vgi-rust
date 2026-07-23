// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Result-cache fixtures — table producers that advertise `vgi.cache.*`.
//!
//! These exist so the SQL integration tests (and the C++ result cache) can
//! exercise cacheable table-function results end to end. Each producer returns a
//! small deterministic result and folds cache-control metadata onto its **first**
//! emitted batch via [`TableProducer::last_metadata`].
//!
//! Mirrors `vgi-python`'s `vgi/_test_fixtures/table/cache.py` fixture-for-fixture.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use arrow_array::builder::{Int64Builder, ListBuilder, StringBuilder};
use arrow_array::{
    ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use vgi::cache_control::{CacheControl, ConditionalRequest};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::partition::partition_field;
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

/// Default freshness lifetime (seconds) for the fixtures that don't take a
/// `ttl` argument. Long enough that TTL never lapses mid-test.
const DEFAULT_TTL_SECONDS: i64 = 300;

/// Process-global monotonic counter, bumped once per *real* invocation of the
/// nonce-bearing fixtures (in `producer()`, which the client only reaches on a
/// cache MISS). A pooled worker persists it across calls, so a served-from-cache
/// hit never advances it — that's exactly the HIT/MISS signal tests assert on.
static NONCE_COUNTER: AtomicI64 = AtomicI64::new(0);

fn next_nonce() -> i64 {
    NONCE_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn register(w: &mut vgi::Worker) {
    w.register_table(CacheableNumbersFunction);
    w.register_table(CacheNonceFunction);
    w.register_table(CacheNoStoreFunction);
    w.register_table(CacheScopedTxnFunction);
    w.register_table(CacheBigFunction);
    w.register_table(CacheRevalidatableFunction);
    w.register_table(CacheMultiColFunction);
    w.register_table(CacheWhoamiFunction);
    w.register_table(CacheVersionedFunction);
    w.register_table(CacheProjectionFunction);
    w.register_table(CachePoisonFunction);
    w.register_table(CacheExternalFailFunction);
    w.register_table(CacheBenchFunction);
    w.register_table(CacheParallelFunction);
    w.register_table(CacheOrderedFunction);
    w.register_table(CacheTypesFunction);
    w.register_table(CacheFilteredFunction);
    w.register_table(CachePartitionedFunction);
    w.register_table(CachePartitionScopeFunction);
    w.register_table(CachePartitionParallelFunction);
    w.register_table(CachePartitionMultiColFunction);
    w.register_table(CachePartitionProjFunction);
    // `cache_multicol` backs only the `ex.data.cache_multicol` table — it stays
    // bindable for that scan but is not advertised as a SQL callable, matching
    // the Python fixture worker (which lists it under `tables`, not `functions`).
    w.hide_function("cache_multicol");
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// The `vgi_batch_index` wire key (a `supports_batch_index` producer tags each
/// batch with its source partition so a cached serve can restore source order).
const BATCH_TAG: &str = "vgi_batch_index";

fn cache_meta(description: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: description.to_string(),
        categories: vec!["generator".into(), "cache".into(), "testing".into()],
        tags: vec![("category".to_string(), "cache".to_string())],
        ..Default::default()
    }
}

fn single_i64_schema(name: &str) -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]))
}

fn i64_batch(schema: &SchemaRef, values: Vec<i64>) -> Result<RecordBatch> {
    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
        .map_err(|e| RpcError::runtime_error(e.to_string()))
}

/// `(start..end)` as an i64 vector.
fn range_vec(start: i64, end: i64) -> Vec<i64> {
    (start..end).collect()
}

/// A bind response for a fixed schema with no opaque data.
fn fixed_bind(schema: SchemaRef) -> Result<BindResponse> {
    Ok(BindResponse {
        output_schema: schema,
        opaque_data: Vec::new(),
    })
}

/// The rows-emitting cursor shared by the plain countdown fixtures. Advertises
/// `cc` on the first batch only (`current_index == 0`), like the Python fixtures.
struct Countdown {
    schema: SchemaRef,
    remaining: i64,
    current_index: i64,
    batch_size: i64,
    cache_control: CacheControl,
    meta: Option<HashMap<String, String>>,
}

impl Countdown {
    fn new(schema: SchemaRef, rows: i64, batch_size: i64, cache_control: CacheControl) -> Self {
        Countdown {
            schema,
            remaining: rows.max(0),
            current_index: 0,
            batch_size,
            cache_control,
            meta: None,
        }
    }
}

impl TableProducer for Countdown {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        let first_batch = self.current_index == 0;
        let size = self.remaining.min(self.batch_size);
        let batch = i64_batch(
            &self.schema,
            range_vec(self.current_index, self.current_index + size),
        )?;
        self.meta = first_batch.then(|| self.cache_control.to_metadata());
        self.current_index += size;
        self.remaining -= size;
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        vgi::table_function::resume::pack(&[self.current_index, self.remaining])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = vgi::table_function::resume::unpack(bytes, 2) {
            self.current_index = v[0];
            self.remaining = v[1];
        }
    }
}

// ---------------------------------------------------------------------------
// cacheable_numbers(n := 10, ttl := 300) -> {n: int64}
// ---------------------------------------------------------------------------

/// The baseline cacheable result: a fresh call MISSes and stores; an identical
/// repeat within `ttl` seconds serves from the client cache.
pub struct CacheableNumbersFunction;
impl TableFunction for CacheableNumbersFunction {
    fn name(&self) -> &str {
        "cacheable_numbers"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Emits n rows [0..n) and advertises a cache TTL")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("n", -1, "int64", "Number of rows to generate"),
            ArgSpec::const_arg("ttl", -1, "int64", "Cache TTL in seconds"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("n"))
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let n = params.arguments.named_i64("n").unwrap_or(10);
        Some(TableCardinality {
            estimate: Some(n),
            max: Some(n),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let n = params.arguments.named_i64("n").unwrap_or(10);
        let ttl = params
            .arguments
            .named_i64("ttl")
            .unwrap_or(DEFAULT_TTL_SECONDS);
        Ok(Box::new(Countdown::new(
            params.output_schema.clone(),
            n,
            1000,
            CacheControl::ttl(ttl),
        )))
    }
}

// ---------------------------------------------------------------------------
// cache_nonce() -> {nonce: int64}
// ---------------------------------------------------------------------------

/// Emits ONE row whose `nonce` changes on every real invocation. A cache HIT is
/// provable by the value NOT changing across calls; a MISS by it changing.
struct OneRow {
    schema: SchemaRef,
    value: i64,
    cache_control: CacheControl,
    done: bool,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for OneRow {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        self.meta = Some(self.cache_control.to_metadata());
        Ok(Some(i64_batch(&self.schema, vec![self.value])?))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CacheNonceFunction;
impl TableFunction for CacheNonceFunction {
    fn name(&self) -> &str {
        "cache_nonce"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Emits one row with a per-invocation nonce; cacheable")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("nonce"))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(OneRow {
            schema: params.output_schema.clone(),
            value: next_nonce(),
            cache_control: CacheControl::ttl(DEFAULT_TTL_SECONDS),
            done: false,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_no_store(n := 10) -> {n: int64}
// ---------------------------------------------------------------------------

/// Emits `n` rows but advertises `vgi.cache.no_store` — the client must never
/// cache it, so every scan re-invokes the worker.
pub struct CacheNoStoreFunction;
impl TableFunction for CacheNoStoreFunction {
    fn name(&self) -> &str {
        "cache_no_store"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Emits n rows but advertises no_store (never cached)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "n",
            -1,
            "int64",
            "Number of rows to generate",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("n"))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(Countdown::new(
            params.output_schema.clone(),
            params.arguments.named_i64("n").unwrap_or(10),
            1000,
            CacheControl::no_store(),
        )))
    }
}

// ---------------------------------------------------------------------------
// cache_scoped_txn(n := 10) -> {n: int64, nonce: int64}
// ---------------------------------------------------------------------------

/// Emits `(n, nonce)` rows and advertises `scope = transaction`. A
/// same-transaction repeat HITs (nonce stable); a fresh transaction MISSes.
struct ScopedTxnProducer {
    schema: SchemaRef,
    remaining: i64,
    current_index: i64,
    nonce: i64,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for ScopedTxnProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        let first_batch = self.current_index == 0;
        let size = self.remaining.min(1000);
        let ns = range_vec(self.current_index, self.current_index + size);
        let nonces = vec![self.nonce; size as usize];
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(ns)) as ArrayRef,
                Arc::new(Int64Array::from(nonces)) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.meta = first_batch.then(|| {
            CacheControl::ttl(DEFAULT_TTL_SECONDS)
                .with_transaction_scope()
                .to_metadata()
        });
        self.current_index += size;
        self.remaining -= size;
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CacheScopedTxnFunction;
impl TableFunction for CacheScopedTxnFunction {
    fn name(&self) -> &str {
        "cache_scoped_txn"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Emits n rows and advertises scope=transaction")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "n",
            -1,
            "int64",
            "Number of rows to generate",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("nonce", DataType::Int64, true),
        ])))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(ScopedTxnProducer {
            schema: params.output_schema.clone(),
            remaining: params.arguments.named_i64("n").unwrap_or(10).max(0),
            current_index: 0,
            nonce: next_nonce(),
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_big(rows := 5000) -> {n: int64}
// ---------------------------------------------------------------------------

/// Emits `rows` rows across MANY small batches (batch size 1000) so multi-batch
/// capture / parallel serve and the size ceiling are exercised.
pub struct CacheBigFunction;
impl TableFunction for CacheBigFunction {
    fn name(&self) -> &str {
        "cache_big"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Emits many small batches totaling `rows` rows; cacheable")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "rows",
            -1,
            "int64",
            "Number of rows to generate",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("n"))
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let rows = params.arguments.named_i64("rows").unwrap_or(5000);
        Some(TableCardinality {
            estimate: Some(rows),
            max: Some(rows),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(Countdown::new(
            params.output_schema.clone(),
            params.arguments.named_i64("rows").unwrap_or(5000),
            1000,
            CacheControl::ttl(DEFAULT_TTL_SECONDS),
        )))
    }
}

// ---------------------------------------------------------------------------
// cache_revalidatable() -> {nonce: int64}  (conditional revalidation / 304)
// ---------------------------------------------------------------------------

/// The always-revalidate contract: `ttl=0` + `etag` + `revalidatable`. The
/// client stores the payload but marks it immediately stale, so every repeat
/// sends a conditional request. This fixture's data never changes, so it answers
/// a matching `if_none_match` with a 0-row `not_modified` batch and the client
/// reuses the STORED nonce — a stable nonce proves the 304 path served cached
/// bytes without re-streaming.
const REVALIDATABLE_ETAG: &str = "\"rev-v1\"";

struct RevalidatableProducer {
    schema: SchemaRef,
    nonce: i64,
    if_none_match: Option<String>,
    done: bool,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for RevalidatableProducer {
    fn on_conditional_request(&mut self, request: &ConditionalRequest) {
        self.if_none_match = request.if_none_match.clone();
    }
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let fresh = CacheControl::ttl(0)
            .with_etag(REVALIDATABLE_ETAG)
            .with_revalidatable();
        if self.if_none_match.as_deref() == Some(REVALIDATABLE_ETAG) {
            // 304 Not Modified: the client's stored copy is still valid. A 0-row
            // batch with fresh validators (ttl=0 so it keeps revalidating).
            self.meta = Some(fresh.with_not_modified().to_metadata());
            return Ok(Some(i64_batch(&self.schema, Vec::new())?));
        }
        self.meta = Some(fresh.to_metadata());
        Ok(Some(i64_batch(&self.schema, vec![self.nonce])?))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CacheRevalidatableFunction;
impl TableFunction for CacheRevalidatableFunction {
    fn name(&self) -> &str {
        "cache_revalidatable"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Emits one nonce row; always-revalidate (304 not_modified)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("nonce"))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(RevalidatableProducer {
            schema: params.output_schema.clone(),
            nonce: next_nonce(),
            if_none_match: None,
            done: false,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_multicol(n := 4, ttl := 300) -> {a, b, c: int64}
// ---------------------------------------------------------------------------

/// Row `i` is `(i, i*10, i*100)`. Not projection-pushdown, so `SELECT b` reuses
/// the `SELECT *` entry (projection-coverage reuse) rather than keying its own.
struct MultiColProducer {
    schema: SchemaRef,
    rows: i64,
    ttl: i64,
    done: bool,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for MultiColProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let a = range_vec(0, self.rows);
        let b: Vec<i64> = a.iter().map(|i| i * 10).collect();
        let c: Vec<i64> = a.iter().map(|i| i * 100).collect();
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(a)) as ArrayRef,
                Arc::new(Int64Array::from(b)) as ArrayRef,
                Arc::new(Int64Array::from(c)) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.meta = Some(CacheControl::ttl(self.ttl).to_metadata());
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

fn abc_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Int64, true),
    ]))
}

pub struct CacheMultiColFunction;
impl TableFunction for CacheMultiColFunction {
    fn name(&self) -> &str {
        "cache_multicol"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Emits n rows of (a, b, c); cacheable, multi-column")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("n", -1, "int64", "Number of rows to generate"),
            ArgSpec::const_arg("ttl", -1, "int64", "Cache TTL in seconds"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(abc_schema())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(MultiColProducer {
            schema: params.output_schema.clone(),
            rows: params.arguments.named_i64("n").unwrap_or(4).max(0),
            ttl: params
                .arguments
                .named_i64("ttl")
                .unwrap_or(DEFAULT_TTL_SECONDS),
            done: false,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_whoami() -> {who: string}
// ---------------------------------------------------------------------------

/// Emits ONE row = the caller's auth principal (`anonymous` if none). Two
/// attaches with different bearer tokens map to different principals, so their
/// results must land under different identity-scoped cache keys. Bearer identity
/// is HTTP-only; over subprocess every caller is `anonymous`.
struct WhoamiProducer {
    schema: SchemaRef,
    who: String,
    done: bool,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for WhoamiProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(StringArray::from(vec![self.who.as_str()])) as ArrayRef],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.meta = Some(CacheControl::ttl(DEFAULT_TTL_SECONDS).to_metadata());
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CacheWhoamiFunction;
impl TableFunction for CacheWhoamiFunction {
    fn name(&self) -> &str {
        "cache_whoami"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Emits the caller's auth principal; cacheable (identity-scoped)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(Arc::new(Schema::new(vec![Field::new(
            "who",
            DataType::Utf8,
            true,
        )])))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(WhoamiProducer {
            schema: params.output_schema.clone(),
            who: params
                .auth_principal
                .clone()
                .unwrap_or_else(|| "anonymous".to_string()),
            done: false,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_versioned_scan(version) -> {v: int64}
// ---------------------------------------------------------------------------

/// Version-specific rows (fixed schema); cacheable. The catalog maps the `AT`
/// clause to the `version` argument, so `AT (VERSION => 1)` / `=> 2` / live must
/// produce distinct cache entries whose bytes never cross-serve.
const VERSIONED_CURRENT: i64 = 3;

fn versioned_rows(version: i64) -> Vec<i64> {
    match version {
        1 => vec![101, 102, 103],
        2 => vec![201, 202],
        _ => vec![301, 302, 303, 304],
    }
}

pub struct CacheVersionedFunction;
impl TableFunction for CacheVersionedFunction {
    fn name(&self) -> &str {
        "cache_versioned_scan"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Version-specific rows; cacheable (AT-keyed)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "version",
            0,
            "int64",
            "Data version, resolved from the AT clause by the catalog",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("v"))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let version = params.arguments.const_i64(0).unwrap_or(VERSIONED_CURRENT);
        Ok(Box::new(StaticRows {
            schema: params.output_schema.clone(),
            values: versioned_rows(version),
            done: false,
            meta: None,
        }))
    }
}

/// Emits a fixed row set in one batch, advertising the default TTL.
struct StaticRows {
    schema: SchemaRef,
    values: Vec<i64>,
    done: bool,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for StaticRows {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        self.meta = Some(CacheControl::ttl(DEFAULT_TTL_SECONDS).to_metadata());
        Ok(Some(i64_batch(&self.schema, self.values.clone())?))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

// ---------------------------------------------------------------------------
// cache_projection() -> {a, b, c: int64}  (projection pushdown)
// ---------------------------------------------------------------------------

/// 3-column generator that PUSHES projection. `SELECT a` and `SELECT b` push
/// distinct `projection_ids` that are part of the cache key, so each column's
/// scan caches only its own bytes and can never be served for another's.
struct ProjectionProducer {
    schema: SchemaRef,
    done: bool,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for ProjectionProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let cols: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|f| -> Result<ArrayRef> {
                let values: Vec<i64> = match f.name().as_str() {
                    "a" => vec![1, 2, 3],
                    "b" => vec![10, 20, 30],
                    "c" => vec![100, 200, 300],
                    other => {
                        return Err(RpcError::runtime_error(format!(
                            "cache_projection: unknown column {other}"
                        )))
                    }
                };
                Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
            })
            .collect::<Result<_>>()?;
        let batch = RecordBatch::try_new(self.schema.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.meta = Some(CacheControl::ttl(DEFAULT_TTL_SECONDS).to_metadata());
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CacheProjectionFunction;
impl TableFunction for CacheProjectionFunction {
    fn name(&self) -> &str {
        "cache_projection"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            projection_pushdown: true,
            ..cache_meta("3-column projection-pushdown generator; cacheable")
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(abc_schema())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(ProjectionProducer {
            schema: params.output_schema.clone(),
            done: false,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_poison() / cache_external_fail() -> {n: int64}
// ---------------------------------------------------------------------------

/// How the poison producer fails after its cacheable first batch.
#[derive(Clone, Copy)]
enum PoisonMode {
    /// Raise a worker error on the next tick.
    Error,
    /// Emit a 0-row pointer batch to an unreachable external location; the
    /// client's resolution throws and aborts the scan.
    ExternalLocation,
}

/// An unreachable loopback URL (http, no TLS handshake). Port 9 (discard) is
/// closed, so resolution fails fast with connection-refused.
const UNRESOLVABLE_LOCATION: &str = "http://127.0.0.1:9/vgi-cache-poison-nonexistent";

/// Adversarial check of the never-partial invariant: a failure AFTER a cacheable
/// batch has streamed must commit NOTHING to the cache (the failing thread never
/// reaches EOS, so `eos < launched` and no entry is stored).
struct PoisonProducer {
    schema: SchemaRef,
    mode: PoisonMode,
    emitted: bool,
    poisoned: bool,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for PoisonProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if !self.emitted {
            self.emitted = true;
            self.meta = Some(CacheControl::ttl(DEFAULT_TTL_SECONDS).to_metadata());
            return Ok(Some(i64_batch(&self.schema, vec![0, 1, 2])?));
        }
        match self.mode {
            PoisonMode::Error => Err(RpcError::runtime_error(
                "cache_poison: intentional mid-stream failure after a cacheable batch",
            )),
            PoisonMode::ExternalLocation => {
                if self.poisoned {
                    // Reached only if resolution somehow succeeded; keeps the
                    // producer from looping forever on transports that don't
                    // resolve external locations.
                    return Ok(None);
                }
                self.poisoned = true;
                self.meta = Some(HashMap::from([(
                    vgi_rpc::metadata::LOCATION_KEY.to_string(),
                    UNRESOLVABLE_LOCATION.to_string(),
                )]));
                Ok(Some(i64_batch(&self.schema, Vec::new())?))
            }
        }
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CachePoisonFunction;
impl TableFunction for CachePoisonFunction {
    fn name(&self) -> &str {
        "cache_poison"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Cacheable first batch then a mid-stream error (never-partial check)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("n"))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(PoisonProducer {
            schema: params.output_schema.clone(),
            mode: PoisonMode::Error,
            emitted: false,
            poisoned: false,
            meta: None,
        }))
    }
}

pub struct CacheExternalFailFunction;
impl TableFunction for CacheExternalFailFunction {
    fn name(&self) -> &str {
        "cache_external_fail"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Cacheable first batch then an unresolvable external-location pointer")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("n"))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(PoisonProducer {
            schema: params.output_schema.clone(),
            mode: PoisonMode::ExternalLocation,
            emitted: false,
            poisoned: false,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_bench(rows) -> {v: int64}
// ---------------------------------------------------------------------------

/// A caller-controlled result size (POSITIONAL `rows`, unlike the other cache
/// fixtures' named-with-default args) so the scaling bench and the flat-RAM
/// disk-streaming guard can build a result of a size they choose.
pub struct CacheBenchFunction;
impl TableFunction for CacheBenchFunction {
    fn name(&self) -> &str {
        "cache_bench"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Emits `rows` int64 rows (positional arg); cacheable — scaling bench fixture")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "rows",
            0,
            "int64",
            "Number of rows to generate",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("v"))
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let rows = params.arguments.const_i64(0)?;
        Some(TableCardinality {
            estimate: Some(rows),
            max: Some(rows),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(Countdown::new(
            params.output_schema.clone(),
            params.arguments.const_i64(0).unwrap_or(0),
            2048,
            CacheControl::ttl(DEFAULT_TTL_SECONDS),
        )))
    }
}

// ---------------------------------------------------------------------------
// Work-queue fan-out plumbing (cache_parallel / cache_ordered)
// ---------------------------------------------------------------------------

/// A `(partition_id, start, end)` work item. `cache_parallel` ignores the id.
fn pack_item(partition_id: i64, start: i64, end: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(24);
    for x in [partition_id, start, end] {
        v.extend_from_slice(&x.to_le_bytes());
    }
    v
}
fn unpack_item(b: &[u8]) -> (i64, i64, i64) {
    let g = |o: usize| i64::from_le_bytes(b[o..o + 8].try_into().unwrap_or([0; 8]));
    (g(0), g(8), g(16))
}

/// Push `[0, rows)` onto the execution's shared work queue as `chunk`-sized
/// items.
fn push_chunks(params: &ProcessParams, rows: i64, chunk: i64) -> Result<()> {
    let store = params
        .storage
        .as_ref()
        .ok_or_else(|| RpcError::runtime_error("cache work-queue fixture requires storage"))?;
    let chunk = chunk.max(1);
    let mut items = Vec::new();
    let mut pid = 0i64;
    let mut start = 0i64;
    while start < rows {
        items.push(pack_item(pid, start, (start + chunk).min(rows)));
        pid += 1;
        start += chunk;
    }
    // Always push (even when empty) so the invocation is registered.
    store.queue_push(&params.execution_id, &items);
    Ok(())
}

/// Drains the shared work queue. `tag_batch_index` tags each batch with its
/// source `partition_id` so a cached serve can restore source order. The
/// cache-control advertisement rides this worker's FIRST batch, so it latches
/// regardless of which worker emits first.
struct QueueProducer {
    schema: SchemaRef,
    storage: Arc<dyn vgi::storage::FunctionStorage>,
    execution_id: Vec<u8>,
    batch_size: i64,
    tag_batch_index: bool,
    advertised: bool,
    /// `(partition_id, idx, end)` of the chunk currently being drained.
    cur: Option<(i64, i64, i64)>,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for QueueProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        loop {
            if self.cur.is_none() {
                match self.storage.queue_pop(&self.execution_id) {
                    None => return Ok(None),
                    Some(data) => {
                        let (pid, start, end) = unpack_item(&data);
                        self.cur = Some((pid, start, end));
                    }
                }
            }
            let (pid, idx, end) = self.cur.expect("chunk present");
            if idx >= end {
                self.cur = None;
                continue;
            }
            let batch_end = (idx + self.batch_size).min(end);
            let batch = i64_batch(&self.schema, range_vec(idx, batch_end))?;
            self.cur = Some((pid, batch_end, end));

            let mut meta = HashMap::new();
            if !self.advertised {
                meta.extend(CacheControl::ttl(DEFAULT_TTL_SECONDS).to_metadata());
                self.advertised = true;
            }
            if self.tag_batch_index {
                meta.insert(BATCH_TAG.to_string(), pid.to_string());
            }
            self.meta = (!meta.is_empty()).then_some(meta);
            return Ok(Some(batch));
        }
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

fn queue_producer(
    params: &ProcessParams,
    batch_size: i64,
    tag_batch_index: bool,
) -> Result<Box<dyn TableProducer>> {
    let storage = params
        .storage
        .clone()
        .ok_or_else(|| RpcError::runtime_error("cache work-queue fixture requires storage"))?;
    Ok(Box::new(QueueProducer {
        schema: params.output_schema.clone(),
        storage,
        execution_id: params.execution_id.clone(),
        batch_size: batch_size.max(1),
        tag_batch_index,
        advertised: false,
        cur: None,
        meta: None,
    }))
}

// ---------------------------------------------------------------------------
// cache_parallel(rows, batch_size := 24000) -> {v: int64}
// ---------------------------------------------------------------------------

/// Multi-worker cacheable sequence — one capture substream per worker. The only
/// cache fixture that drives `num_substreams > 1`. Values are the plain sequence
/// `[0..rows)`, so COUNT and SUM hold regardless of how chunks were distributed.
pub struct CacheParallelFunction;

/// ~24 chunks max regardless of size, so remote cost scales with fan-out rather
/// than row count.
const PARALLEL_MAX_CHUNKS: i64 = 24;

impl TableFunction for CacheParallelFunction {
    fn name(&self) -> &str {
        "cache_parallel"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta(
            "Multi-worker cacheable sequence (one substream per worker); parallel-capture fixture",
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("rows", 0, "int64", "Total number of rows to generate"),
            ArgSpec::const_arg("batch_size", -1, "int64", "Rows per output batch"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("v"))
    }
    fn max_workers(&self, _params: &BindParams) -> i64 {
        8
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let rows = params.arguments.const_i64(0)?;
        Some(TableCardinality {
            estimate: Some(rows),
            max: Some(rows),
        })
    }
    fn on_init(&self, params: &ProcessParams) -> Result<()> {
        let rows = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let chunk = ((rows + PARALLEL_MAX_CHUNKS - 1) / PARALLEL_MAX_CHUNKS).max(1);
        push_chunks(params, rows, chunk)
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let batch_size = params.arguments.named_i64("batch_size").unwrap_or(24000);
        queue_producer(params, batch_size, false)
    }
}

// ---------------------------------------------------------------------------
// cache_ordered(rows := 200000, chunk_size := 1000) -> {n: int64}
// ---------------------------------------------------------------------------

/// Order-sensitive cacheable sequence: `preserves_order = FIXED_ORDER` +
/// `supports_batch_index`, so the correct output is strictly `0,1,…,rows-1`. A
/// cache HIT must replay in batch_index order, so tests assert row ORDER, not
/// merely the row set. Named-with-default args so it can back a catalog data
/// table (the parallel + order-sensitive capture path only exists on the catalog
/// scan; the direct path serializes FIXED_ORDER to one thread).
pub struct CacheOrderedFunction;
impl TableFunction for CacheOrderedFunction {
    fn name(&self) -> &str {
        "cache_ordered"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            supports_batch_index: true,
            order_preservation: Some(
                vgi::protocol::enums::order_preservation::FIXED_ORDER.to_string(),
            ),
            ..cache_meta(
                "Multi-worker order-sensitive cacheable sequence (batch_index); order-preservation cache fixture",
            )
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("rows", -1, "int64", "Total number of rows to generate"),
            ArgSpec::const_arg("chunk_size", -1, "int64", "Rows per partition"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("n"))
    }
    fn max_workers(&self, _params: &BindParams) -> i64 {
        8
    }
    fn on_init(&self, params: &ProcessParams) -> Result<()> {
        let rows = params.arguments.named_i64("rows").unwrap_or(200_000).max(0);
        let chunk = params.arguments.named_i64("chunk_size").unwrap_or(1000);
        push_chunks(params, rows, chunk)
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        queue_producer(params, 256, true)
    }
}

// ---------------------------------------------------------------------------
// cache_types(rows) -> nested / wide / NULL columns
// ---------------------------------------------------------------------------

/// Every other cacheable fixture emits flat int64/string, so the disk blob and
/// the streaming TOC (seek-past-payload) path is only exercised on fixed-width
/// int64. This one emits STRUCT / LIST / DECIMAL / TIMESTAMP / string columns
/// with interleaved NULLs (validity bitmaps + variable/nested buffers) across
/// many batches, so a spilled + streamed serve must reassemble all of that.
fn attrs_fields() -> Fields {
    Fields::from(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("y", DataType::Utf8, true),
    ])
}

fn cache_types_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new("attrs", DataType::Struct(attrs_fields()), true),
        Field::new("amt", DataType::Decimal128(18, 2), true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

struct TypesProducer {
    schema: SchemaRef,
    remaining: i64,
    current_index: i64,
    meta: Option<HashMap<String, String>>,
}

impl TypesProducer {
    /// Row `j`: `id = j`; every 5th row is NULL in every nullable column so
    /// validity bitmaps must round-trip.
    fn build(&self, start: i64, size: i64) -> Result<RecordBatch> {
        let rows = (start..start + size).collect::<Vec<_>>();
        let is_null = |j: i64| j % 5 == 0;

        let ids = Int64Array::from(rows.clone());

        let mut tags = ListBuilder::new(Int64Builder::new());
        for &j in &rows {
            if is_null(j) {
                tags.append_null();
            } else {
                tags.values().append_slice(&[j, j + 1, j + 2]);
                tags.append(true);
            }
        }

        let mut attr_x = Int64Builder::new();
        let mut attr_y = StringBuilder::new();
        for &j in &rows {
            if is_null(j) {
                attr_x.append_null();
                attr_y.append_null();
            } else {
                attr_x.append_value(j);
                attr_y.append_value(format!("y{j}"));
            }
        }
        let attrs = StructArray::try_new(
            attrs_fields(),
            vec![
                Arc::new(attr_x.finish()) as ArrayRef,
                Arc::new(attr_y.finish()) as ArrayRef,
            ],
            Some(NullBuffer::from_iter(rows.iter().map(|&j| !is_null(j)))),
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;

        // Decimal(j.{j % 100:02}) at scale 2 is the scaled integer j*100 + j%100.
        let amt = rows
            .iter()
            .map(|&j| (!is_null(j)).then_some(j as i128 * 100 + (j % 100) as i128))
            .collect::<Decimal128Array>()
            .with_precision_and_scale(18, 2)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;

        let ts = rows
            .iter()
            .map(|&j| (!is_null(j)).then_some(j))
            .collect::<TimestampMicrosecondArray>();

        let label = rows
            .iter()
            .map(|&j| (!is_null(j)).then(|| format!("label-{j}")))
            .collect::<StringArray>();

        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(ids) as ArrayRef,
                Arc::new(tags.finish()) as ArrayRef,
                Arc::new(attrs) as ArrayRef,
                Arc::new(amt) as ArrayRef,
                Arc::new(ts) as ArrayRef,
                Arc::new(label) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

impl TableProducer for TypesProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        let first_batch = self.current_index == 0;
        let size = self.remaining.min(2048);
        let batch = self.build(self.current_index, size)?;
        self.meta = first_batch.then(|| CacheControl::ttl(DEFAULT_TTL_SECONDS).to_metadata());
        self.current_index += size;
        self.remaining -= size;
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CacheTypesFunction;
impl TableFunction for CacheTypesFunction {
    fn name(&self) -> &str {
        "cache_types"
    }
    fn metadata(&self) -> FunctionMetadata {
        cache_meta("Nested/wide/NULL cacheable result (STRUCT/LIST/DECIMAL/TIMESTAMP + NULLs)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "rows",
            0,
            "int64",
            "Total number of rows to generate",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(cache_types_schema())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(TypesProducer {
            schema: params.output_schema.clone(),
            remaining: params.arguments.const_i64(0).unwrap_or(0).max(0),
            current_index: 0,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_filtered(rows := 100) -> {n: int64}  (static filter pushdown)
// ---------------------------------------------------------------------------

/// The cache key includes `filter_bytes`, but no other cacheable fixture pushes
/// filters — so this covers the "pushed `WHERE n>=5` must never cross-serve a
/// pushed `WHERE n>=7`" boundary. `auto_apply_filters` means the framework
/// applies the pushed predicate to the emitted rows.
pub struct CacheFilteredFunction;
impl TableFunction for CacheFilteredFunction {
    fn name(&self) -> &str {
        "cache_filtered"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            filter_pushdown: true,
            auto_apply_filters: true,
            ..cache_meta("Cacheable sequence with static filter pushdown (filter_bytes keying)")
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "rows",
            -1,
            "int64",
            "Total number of rows to generate",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(single_i64_schema("n"))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(Countdown::new(
            params.output_schema.clone(),
            params.arguments.named_i64("rows").unwrap_or(100),
            2048,
            CacheControl::ttl(DEFAULT_TTL_SECONDS),
        )))
    }
}

// ---------------------------------------------------------------------------
// cache_partitioned(rows_per_country) -> {country: string, sales: int64}
// ---------------------------------------------------------------------------

/// No other cacheable fixture emits `partition_values`, so the non-empty
/// `pv_bytes` framing in the disk blob is otherwise untested. One single-country
/// batch per tick makes the framework emit `pv` per batch; forced to spill and
/// served back, any misframed `pv_len` would misalign the streaming TOC seek.
const COUNTRIES: [&str; 5] = ["AU", "BR", "CA", "FR", "US"];

struct PartitionedProducer {
    schema: SchemaRef,
    rows_per_country: i64,
    country_idx: usize,
    advertised: bool,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for PartitionedProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.country_idx >= COUNTRIES.len() {
            return Ok(None);
        }
        let country = COUNTRIES[self.country_idx];
        let rpc = self.rows_per_country.max(0);
        let base = self.country_idx as i64 * 1_000_000;
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![country; rpc as usize])) as ArrayRef,
                Arc::new(Int64Array::from(range_vec(base, base + rpc))) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;

        // Partition values (min/max per batch) ride every batch; the
        // cache-control advertisement rides only the first.
        let mut meta =
            vgi::partition::partition_metadata(&self.schema, &batch)?.unwrap_or_default();
        if !self.advertised {
            meta.extend(CacheControl::ttl(DEFAULT_TTL_SECONDS).to_metadata());
            self.advertised = true;
        }
        self.meta = Some(meta);
        self.country_idx += 1;
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CachePartitionedFunction;
impl TableFunction for CachePartitionedFunction {
    fn name(&self) -> &str {
        "cache_partitioned"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            partition_kind: Some(
                vgi::protocol::enums::partition_kind::SINGLE_VALUE_PARTITIONS.to_string(),
            ),
            ..cache_meta(
                "Cacheable single-value-partitioned result (partition_values through the spill blob)",
            )
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "rows_per_country",
            0,
            "int64",
            "Rows per country partition",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(Arc::new(Schema::new(vec![
            partition_field("country", DataType::Utf8),
            Field::new("sales", DataType::Int64, true),
        ])))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(PartitionedProducer {
            schema: params.output_schema.clone(),
            rows_per_country: params.arguments.const_i64(0).unwrap_or(1),
            country_idx: 0,
            advertised: false,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// Per-partition result cache (`vgi.cache.partition_scope`)
// ---------------------------------------------------------------------------
// A SINGLE_VALUE_PARTITIONS function that also advertises
// `vgi.cache.partition_scope` gets its result cached BOTH whole-scan and split
// by partition value, so a later `=`/`IN` scan on the partition column(s) is
// served per-partition without reaching the worker.
//
// `filter_pushdown` + `auto_apply_filters` are what make that safe: the
// predicate arrives as a real filter (so the client can enumerate the requested
// set) and the framework prunes emitted batches to it — DuckDB does NOT
// re-apply a pushed predicate above the scan, so a fall-through worker scan has
// to be row-exact on its own.
//
// Mirrors vgi-python's `cache_partition_{scope,parallel,multicol,proj}`.

/// Build the `{country, sales}` batch for partition `idx` of `countries`.
/// `sales` starts at `idx * 1_000_000` so every partition's values are
/// unmistakably its own.
fn country_sales_batch(
    schema: &SchemaRef,
    country: Option<&str>,
    idx: usize,
    rows: i64,
) -> Result<RecordBatch> {
    let rows = rows.max(0);
    let base = idx as i64 * 1_000_000;
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![country; rows as usize])) as ArrayRef,
            Arc::new(Int64Array::from(range_vec(base, base + rows))) as ArrayRef,
        ],
    )
    .map_err(|e| RpcError::runtime_error(e.to_string()))
}

/// `{country: string, sales: int64}` with `country` partition-annotated.
fn country_sales_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        partition_field("country", DataType::Utf8),
        Field::new("sales", DataType::Int64, true),
    ]))
}

/// Cache-control every per-partition fixture advertises: a normal whole-scan TTL
/// PLUS the per-partition opt-in.
fn partition_scope_cc() -> HashMap<String, String> {
    CacheControl::ttl(DEFAULT_TTL_SECONDS)
        .with_partition_scope()
        .to_metadata()
}

/// Partition metadata for a single-valued *string* partition column, computed
/// from a synthetic 1-row batch rather than the emitted one. Needed when the
/// partition column isn't in the emitted batch at all (projected away), where
/// auto-extraction has nothing to read.
fn explicit_country_pv(name: &str, value: Option<&str>) -> Result<HashMap<String, String>> {
    let schema: SchemaRef = Arc::new(Schema::new(vec![partition_field(name, DataType::Utf8)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![value])) as ArrayRef],
    )
    .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    Ok(vgi::partition::partition_metadata(&schema, &batch)?.unwrap_or_default())
}

/// Shared metadata for the per-partition fixtures.
fn partition_scope_meta(description: &str) -> FunctionMetadata {
    FunctionMetadata {
        partition_kind: Some(
            vgi::protocol::enums::partition_kind::SINGLE_VALUE_PARTITIONS.to_string(),
        ),
        filter_pushdown: true,
        auto_apply_filters: true,
        ..cache_meta(description)
    }
}

// ---------------------------------------------------------------------------
// cache_partition_scope(rows_per_country) -> {country: string, sales: int64}
// ---------------------------------------------------------------------------

/// One single-country batch per tick over [`COUNTRIES`]. The partition-scope
/// opt-in rides EVERY batch (not just the first, as the plain cache fixtures
/// do): on a fall-through scan whose leading country is filtered away to 0
/// rows, a first-batch-only advertisement would never be seen and the client
/// would silently stop caching per partition.
struct PartitionScopeProducer {
    schema: SchemaRef,
    rows_per_country: i64,
    country_idx: usize,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for PartitionScopeProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.country_idx >= COUNTRIES.len() {
            return Ok(None);
        }
        let batch = country_sales_batch(
            &self.schema,
            Some(COUNTRIES[self.country_idx]),
            self.country_idx,
            self.rows_per_country,
        )?;
        let mut meta =
            vgi::partition::partition_metadata(&self.schema, &batch)?.unwrap_or_default();
        meta.extend(partition_scope_cc());
        self.meta = Some(meta);
        self.country_idx += 1;
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CachePartitionScopeFunction;
impl TableFunction for CachePartitionScopeFunction {
    fn name(&self) -> &str {
        "cache_partition_scope"
    }
    fn metadata(&self) -> FunctionMetadata {
        partition_scope_meta(
            "Per-partition cacheable single-value-partitioned result (vgi.cache.partition_scope)",
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "rows_per_country",
            0,
            "int64",
            "Rows per country partition",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(country_sales_schema())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(PartitionScopeProducer {
            schema: params.output_schema.clone(),
            rows_per_country: params.arguments.const_i64(0).unwrap_or(1),
            country_idx: 0,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_partition_parallel(rows_per_country) -> {country: string, sales: int64}
// ---------------------------------------------------------------------------

/// Partitions for the parallel fixture. The trailing `None` is a genuine NULL
/// partition (SINGLE_VALUE permits it), which exercises capture/serve of a NULL
/// tuple — and the fact that `IS NULL` is correctly NOT enumerable, so it must
/// fall through rather than partition-serve.
const PSCOPE_COUNTRIES: [Option<&str>; 4] = [Some("AU"), Some("CA"), Some("US"), None];

/// Work-queue fan-out: unlike the single-worker fixtures, a `threads=N` +
/// `pool false` scan spreads these partitions over N workers, so the split at
/// commit has to bucket batches drawn from MULTIPLE capture substreams.
struct PartitionParallelProducer {
    schema: SchemaRef,
    storage: Arc<dyn vgi::storage::FunctionStorage>,
    execution_id: Vec<u8>,
    rows_per_country: i64,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for PartitionParallelProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        // One queue item per partition; each pop yields that partition's whole batch.
        let Some(item) = self.storage.queue_pop(&self.execution_id) else {
            return Ok(None);
        };
        let idx = unpack_item(&item).0 as usize;
        let country = *PSCOPE_COUNTRIES.get(idx).unwrap_or(&None);
        let batch = country_sales_batch(&self.schema, country, idx, self.rows_per_country)?;
        // Explicit pv keeps the NULL partition's scalar type pinned to the
        // column's own type rather than inferring it from an all-NULL array.
        let mut meta = explicit_country_pv("country", country)?;
        meta.extend(partition_scope_cc());
        self.meta = Some(meta);
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CachePartitionParallelFunction;
impl TableFunction for CachePartitionParallelFunction {
    fn name(&self) -> &str {
        "cache_partition_parallel"
    }
    fn metadata(&self) -> FunctionMetadata {
        partition_scope_meta(
            "Per-partition cacheable; work-queue fan-out (parallel capture); one NULL partition",
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "rows_per_country",
            0,
            "int64",
            "Rows per country partition",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(country_sales_schema())
    }
    fn max_workers(&self, _params: &BindParams) -> i64 {
        8
    }
    fn on_init(&self, params: &ProcessParams) -> Result<()> {
        let store = params
            .storage
            .as_ref()
            .ok_or_else(|| RpcError::runtime_error("cache_partition_parallel requires storage"))?;
        // `start`/`end` are unused here — the partition index is the whole item.
        let items: Vec<Vec<u8>> = (0..PSCOPE_COUNTRIES.len())
            .map(|i| pack_item(i as i64, 0, 0))
            .collect();
        store.queue_push(&params.execution_id, &items);
        Ok(())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let storage = params
            .storage
            .clone()
            .ok_or_else(|| RpcError::runtime_error("cache_partition_parallel requires storage"))?;
        Ok(Box::new(PartitionParallelProducer {
            schema: params.output_schema.clone(),
            storage,
            execution_id: params.execution_id.clone(),
            rows_per_country: params.arguments.const_i64(0).unwrap_or(1),
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_partition_multicol(rows_per_partition) -> {region, year, amount}
// ---------------------------------------------------------------------------

/// Two partition columns, so the client must enumerate the CROSS PRODUCT
/// (`region IN (…) × year IN (…)`) and canonicalize a 2-column tuple. Years are
/// deliberately NON-contiguous: DuckDB rewrites `year IN (2020, 2021)` into a
/// BETWEEN range (which is not enumerable), so the gap keeps the pushed filter
/// a real IN_FILTER and the cross-product path is actually exercised.
const PSCOPE_REGIONS: [&str; 2] = ["EU", "US"];
const PSCOPE_YEARS: [i64; 2] = [2020, 2022];

/// `(region, year)` in region-major order, matching the Python fixture.
fn pscope_ry(idx: usize) -> (&'static str, i64) {
    (
        PSCOPE_REGIONS[idx / PSCOPE_YEARS.len()],
        PSCOPE_YEARS[idx % PSCOPE_YEARS.len()],
    )
}

struct PartitionMultiColProducer {
    schema: SchemaRef,
    rows_per_partition: i64,
    idx: usize,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for PartitionMultiColProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        let total = PSCOPE_REGIONS.len() * PSCOPE_YEARS.len();
        if self.idx >= total {
            return Ok(None);
        }
        let (region, year) = pscope_ry(self.idx);
        let rows = self.rows_per_partition.max(0);
        let base = self.idx as i64 * 1000;
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![region; rows as usize])) as ArrayRef,
                Arc::new(Int64Array::from(vec![year; rows as usize])) as ArrayRef,
                Arc::new(Int64Array::from(range_vec(base, base + rows))) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let mut meta =
            vgi::partition::partition_metadata(&self.schema, &batch)?.unwrap_or_default();
        meta.extend(partition_scope_cc());
        self.meta = Some(meta);
        self.idx += 1;
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CachePartitionMultiColFunction;
impl TableFunction for CachePartitionMultiColFunction {
    fn name(&self) -> &str {
        "cache_partition_multicol"
    }
    fn metadata(&self) -> FunctionMetadata {
        partition_scope_meta(
            "Per-partition cacheable over (region, year) SINGLE_VALUE partition columns",
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "rows_per_partition",
            0,
            "int64",
            "Rows per (region, year) partition",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(Arc::new(Schema::new(vec![
            partition_field("region", DataType::Utf8),
            partition_field("year", DataType::Int64),
            Field::new("amount", DataType::Int64, true),
        ])))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(PartitionMultiColProducer {
            schema: params.output_schema.clone(),
            rows_per_partition: params.arguments.const_i64(0).unwrap_or(1),
            idx: 0,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// cache_partition_proj(rows_per_country) -> {country, sales, extra}
// ---------------------------------------------------------------------------

/// `projection_pushdown` makes the projection part of the cache key, so
/// `SELECT country, sales` and `SELECT sales` key separately. `extra` exists
/// purely as a column to project away while keeping `country` pushable.
///
/// The partition value is always supplied EXPLICITLY: when `country` is itself
/// projected out, it is absent from the emitted batch and auto-extraction has
/// nothing to read, yet the split must still bucket the rows correctly.
const PSCOPE_PROJ_COUNTRIES: [&str; 2] = ["CA", "US"];

struct PartitionProjProducer {
    schema: SchemaRef,
    rows_per_country: i64,
    country_idx: usize,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for PartitionProjProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.country_idx >= PSCOPE_PROJ_COUNTRIES.len() {
            return Ok(None);
        }
        let country = PSCOPE_PROJ_COUNTRIES[self.country_idx];
        let rows = self.rows_per_country.max(0);
        let base = self.country_idx as i64 * 1_000_000;
        // `output_schema` already reflects the pushed projection — emit exactly
        // the columns it asks for, in its order.
        let cols: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|f| -> Result<ArrayRef> {
                Ok(match f.name().as_str() {
                    "country" => {
                        Arc::new(StringArray::from(vec![country; rows as usize])) as ArrayRef
                    }
                    "sales" => Arc::new(Int64Array::from(range_vec(base, base + rows))) as ArrayRef,
                    "extra" => Arc::new(Int64Array::from(range_vec(base + 500, base + 500 + rows)))
                        as ArrayRef,
                    other => {
                        return Err(RpcError::runtime_error(format!(
                            "cache_partition_proj: unknown column {other}"
                        )))
                    }
                })
            })
            .collect::<Result<_>>()?;
        let batch = RecordBatch::try_new(self.schema.clone(), cols)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let mut meta = explicit_country_pv("country", Some(country))?;
        meta.extend(partition_scope_cc());
        self.meta = Some(meta);
        self.country_idx += 1;
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

pub struct CachePartitionProjFunction;
impl TableFunction for CachePartitionProjFunction {
    fn name(&self) -> &str {
        "cache_partition_proj"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            projection_pushdown: true,
            ..partition_scope_meta(
                "Per-partition cacheable with projection pushdown + explicit partition_values",
            )
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "rows_per_country",
            0,
            "int64",
            "Rows per country partition",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(Arc::new(Schema::new(vec![
            partition_field("country", DataType::Utf8),
            Field::new("sales", DataType::Int64, true),
            Field::new("extra", DataType::Int64, true),
        ])))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(PartitionProjProducer {
            schema: params.output_schema.clone(),
            rows_per_country: params.arguments.const_i64(0).unwrap_or(1),
            country_idx: 0,
            meta: None,
        }))
    }
}

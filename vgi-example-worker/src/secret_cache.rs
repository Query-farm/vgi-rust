// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Cacheable fixtures whose results depend on a secret.
//!
//! The C++ result cache keys a secret-dependent result on a fingerprint of the
//! secrets its bind resolved (never their values), so a result is reused while
//! the secret is unchanged and recomputed the moment it is rotated, re-fielded
//! or dropped. Each fixture here reads the `vgi_example` secret's
//! `secret_string` and advertises cacheability — one per cache path the
//! fingerprint has to reach:
//!
//! - `secret_cache_nonce()` — table producer; the secret is declared in
//!   [`FunctionMetadata::required_secrets`], so the extension resolves it
//!   before bind rather than being asked for it mid-bind. Also backs the
//!   `data.secret_cache_nonce` table. The Python worker pre-binds that table
//!   (`inline_bind`) to cover the client's no-bind-RPC path; this SDK has no
//!   inline bind, so here it is an ordinary function-backed table like
//!   `cache_nonce` and takes the bind RPC.
//! - `secret_cached_scalar(value)` — scalar, per-value memoized; the secret is
//!   requested through [`ScalarFunction::secret_lookups`], as
//!   `return_secret_value` does.
//! - `secret_cached_lateral(x)` — blended map, per-value memoized, called under
//!   `LATERAL`; the secret is requested during bind (the two-phase secret
//!   request, as `secret_in_out` does), so the dependency is discovered at bind
//!   time rather than declared in the catalog.
//!
//! Every output carries a `nonce` minted only when the worker really runs. It
//! is random rather than a counter (unlike `cache_nonce`'s) because a pooled
//! worker may run several processes, and per-process counters repeat across
//! them: an equal nonce proves a cache HIT and a different one a MISS on any
//! pool size.
//!
//! Port of vgi-python's `vgi/_test_fixtures/secret_cache.py`; driven by
//! `test/sql/integration/cache/secret_scope.test`.

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::cache_control::CacheControl;
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FunctionExample, FunctionMetadata, ProcessParams,
    ScalarFunction,
};
use vgi::secrets::{SecretLookup, Secrets};
use vgi::table_function::{TableFunction, TableProducer};
use vgi::table_in_out::{EmitOptions, TableInOutFunction, TableInOutOutput};
use vgi_rpc::{Result, RpcError};

/// The secret type every fixture here reads. Registered by the worker as the
/// `vgi_example` secret type (see `register_secrets_and_settings` in `main.rs`).
const SECRET_TYPE: &str = "vgi_example";

/// Long enough that TTL never lapses mid-test.
const TTL_SECONDS: i64 = 300;

/// Register the secret-dependent cacheable fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_table(SecretCacheNonceFunction);
    w.register_scalar(SecretCachedScalarFunction);
    w.register_table_in_out(SecretCachedLateralFunction);
}

/// A value unique to this invocation, across every process in a worker pool.
///
/// 56 random bits, so the value is a non-negative BIGINT, matching the Python
/// fixture. `RandomState` is keyed from the OS RNG (once per thread, then
/// stepped per instance), and the SipHash of its fresh key is uniformly
/// distributed — OS-seeded randomness without a dependency on an RNG crate.
fn nonce() -> i64 {
    let bits = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    (bits >> 8) as i64
}

/// The `vgi_example` secret for this call — `None` when no such secret resolved.
fn example_secret(secrets: &Secrets) -> Option<&HashMap<String, String>> {
    secrets.of_type(SECRET_TYPE).next()
}

/// The `secret_string` field of the resolved secret, or `None` when there is
/// no secret (or it has no such field).
fn secret_string(secrets: &Secrets) -> Option<String> {
    example_secret(secrets).and_then(|s| s.get("secret_string").cloned())
}

fn lookup() -> SecretLookup {
    SecretLookup {
        secret_type: SECRET_TYPE.to_string(),
        scope: None,
        name: None,
    }
}

/// `(secret_string VARCHAR, nonce BIGINT)` — the producer's and the lateral
/// map's output schema.
fn secret_nonce_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("secret_string", DataType::Utf8, true),
        Field::new("nonce", DataType::Int64, true),
    ]))
}

/// `rows` rows of `(secret_string, nonce)`, built against `schema` by column
/// name so a narrowed output schema still gets exactly its own columns.
fn secret_nonce_batch(
    schema: &SchemaRef,
    secret: Option<&str>,
    nonce: i64,
    rows: usize,
) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .map(|f| match f.name().as_str() {
            "secret_string" => Ok(Arc::new(StringArray::from(vec![secret; rows])) as ArrayRef),
            "nonce" => Ok(Arc::new(Int64Array::from(vec![nonce; rows])) as ArrayRef),
            other => Err(RpcError::runtime_error(format!(
                "unexpected output column {other:?}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| RpcError::runtime_error(e.to_string()))
}

// ---------------------------------------------------------------------------
// secret_cache_nonce() -> {secret_string: varchar, nonce: int64}
// ---------------------------------------------------------------------------

/// The one row to emit, minted when the producer is built.
struct SecretNonceRow {
    schema: SchemaRef,
    secret_string: Option<String>,
    nonce: i64,
    done: bool,
    meta: Option<HashMap<String, String>>,
}

impl TableProducer for SecretNonceRow {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        self.meta = Some(CacheControl::ttl(TTL_SECONDS).to_metadata());
        Ok(Some(secret_nonce_batch(
            &self.schema,
            self.secret_string.as_deref(),
            self.nonce,
            1,
        )?))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
    fn resume_supported(&self) -> bool {
        // Single batch: there is nothing to resume.
        false
    }
}

/// `secret_cache_nonce()` — one row: the secret's `secret_string` (NULL with no
/// secret) and a per-invocation nonce; cacheable.
///
/// The producer is built only on a cache MISS, so the nonce is stable across
/// HITs. A rotated secret must MISS and report the new value; restoring the
/// original secret must HIT the entry it produced. Single worker (the
/// [`TableFunction::max_workers`] default), so one scan is one nonce.
pub struct SecretCacheNonceFunction;
impl TableFunction for SecretCacheNonceFunction {
    fn name(&self) -> &str {
        "secret_cache_nonce"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description:
                "One row with a secret's value and a per-invocation nonce; cacheable per secret"
                    .to_string(),
            categories: vec![
                "generator".into(),
                "cache".into(),
                "secret".into(),
                "testing".into(),
            ],
            // Declared, not requested mid-bind: the extension resolves it
            // before bind and the fingerprint covers what it resolved.
            required_secrets: vec![lookup()],
            examples: vec![FunctionExample {
                sql: "SELECT * FROM secret_cache_nonce()".to_string(),
                description: "The nonce is stable while the vgi_example secret is unchanged"
                    .to_string(),
                expected_output: None,
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: secret_nonce_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(SecretNonceRow {
            schema: params.output_schema.clone(),
            secret_string: secret_string(&params.secrets),
            nonce: nonce(),
            done: false,
            meta: None,
        }))
    }
}

// ---------------------------------------------------------------------------
// secret_cached_scalar(value) -> varchar
// ---------------------------------------------------------------------------

/// `secret_cached_scalar(value)` — `'<secret_string>|<nonce>'` for every row,
/// memoized per value per secret.
///
/// With no secret resolved the label is `'|<nonce>'` — a dropped secret is a
/// state the test drives, so this must not error. One nonce per `process`
/// call, shared by the batch, so a served value keeps the nonce of the call
/// that produced it. `per_value` is a test choice, as on
/// `cached_double_scalar`: the point is coverage of the tier, not economics.
pub struct SecretCachedScalarFunction;
impl ScalarFunction for SecretCachedScalarFunction {
    fn name(&self) -> &str {
        "secret_cached_scalar"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description:
                "Returns '<secret_string>|<nonce>' per value; memoized per value per secret"
                    .to_string(),
            return_type: Some(DataType::Utf8),
            stability: Some(vgi::protocol::enums::stability::CONSISTENT.to_string()),
            examples: vec![FunctionExample {
                sql: "SELECT secret_cached_scalar(1)".to_string(),
                description: "Stable while the vgi_example secret is unchanged".to_string(),
                expected_output: None,
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column(
            "value",
            0,
            "int64",
            "Any value; the output ignores it",
        )]
    }
    fn secret_lookups(&self, _params: &BindParams) -> Vec<SecretLookup> {
        vec![lookup()]
    }
    fn cache_control(&self) -> Option<CacheControl> {
        Some(CacheControl::ttl(TTL_SECONDS).with_per_value())
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let label = format!(
            "{}|{}",
            secret_string(&params.secrets).unwrap_or_default(),
            nonce()
        );
        let out = StringArray::from(vec![label.as_str(); batch.num_rows()]);
        RecordBatch::try_new(params.output_schema.clone(), vec![Arc::new(out)])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// secret_cached_lateral(x) -> {secret_string: varchar, nonce: int64}
// ---------------------------------------------------------------------------

/// `secret_cached_lateral(x)` — blended 1→1 map emitting the secret's
/// `secret_string` (NULL with no secret) and a per-call nonce.
///
/// Requests the secret during bind — the two-phase bind, so the dependency is
/// discovered at bind time rather than declared — and advertises `per_value`
/// so a correlated `LATERAL` call is memoized per input value per secret.
pub struct SecretCachedLateralFunction;
impl TableInOutFunction for SecretCachedLateralFunction {
    fn name(&self) -> &str {
        "secret_cached_lateral"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description:
                "Blended map emitting a secret's value and a per-call nonce; memoized per secret"
                    .to_string(),
            categories: vec![
                "blended".into(),
                "cache".into(),
                "secret".into(),
                "test".into(),
            ],
            input_from_args: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("x", 0, "int64", "Input column")]
    }
    fn secret_lookups(&self, _params: &BindParams) -> Vec<SecretLookup> {
        vec![lookup()]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: secret_nonce_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        out.emit_with(
            secret_nonce_batch(
                &params.output_schema,
                secret_string(&params.secrets).as_deref(),
                nonce(),
                batch.num_rows(),
            )?,
            EmitOptions {
                cache_control: Some(CacheControl::ttl(TTL_SECONDS).with_per_value()),
                ..Default::default()
            },
        )
    }
}

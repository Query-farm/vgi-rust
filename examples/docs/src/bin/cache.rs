// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The result-caching example for the vgi-rust documentation.
//!
//! Caching is advertised, not requested: the worker attaches `vgi.cache.*`
//! metadata to the FIRST data batch it emits, and the client (the DuckDB
//! extension) decides what to do with it. Nothing is cached unless you say so.
//!
//! ```text
//! cargo build --release --bin cache
//! # then, in a Haybarn shell:
//! ATTACH 'rates' (TYPE vgi, LOCATION './target/release/cache');
//! SELECT * FROM rates.rates();   -- repeat calls inside the TTL never land here
//! SELECT hits, misses, inserts FROM vgi_result_cache_stats();
//! SELECT * FROM rates.upstream_calls();   -- proves the worker was not re-run
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::cache_control::{CacheControl, ConditionalRequest};
use vgi::catalog::CatalogModel;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
use vgi::vgi_rpc::OutputCollector;
use vgi::{Result, RpcError};

/// Counts real invocations, so the caching can be observed rather than assumed.
/// A worker is one process, so a plain atomic is enough.
static UPSTREAM_CALLS: AtomicI64 = AtomicI64::new(0);

const TTL_SECONDS: i64 = 300;

/// A strong validator for the payload below. Anything opaque and stable works —
/// a content hash, a database version, an upstream ETag — as long as it changes
/// exactly when the payload does.
const ETAG: &str = "\"rates-v1\"";

fn rates_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pair", DataType::Utf8, true),
        Field::new("rate", DataType::Int64, true),
    ]))
}

struct RatesProducer {
    schema: SchemaRef,
    done: bool,
    /// Set by `on_conditional_request` before the first `next_batch`.
    if_none_match: Option<String>,
    /// Set alongside the batch; the framework reads it back through
    /// `last_metadata` and puts it on the wire.
    meta: Option<HashMap<String, String>>,
}

impl TableProducer for RatesProducer {
    /// The client already has a payload and is asking whether it is still good.
    /// It only ever asks because the result advertised `with_revalidatable`.
    fn on_conditional_request(&mut self, request: &ConditionalRequest) {
        self.if_none_match = request.if_none_match.clone();
    }

    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let mut cc = CacheControl::ttl(TTL_SECONDS)
            .with_etag(ETAG)
            .with_revalidatable()
            // Grace windows: serve stale immediately while refreshing in the
            // background, and keep serving stale if a refresh RPC fails.
            .with_stale_while_revalidate(60)
            .with_stale_if_error(3600);

        let still_fresh = self.if_none_match.as_deref() == Some(ETAG);
        if still_fresh {
            cc = cc.with_not_modified();
        }

        // Metadata rides the FIRST data batch. It cannot go on the schema — the
        // IPC stream fixes that when the stream opens, before this runs.
        self.meta = Some(cc.to_metadata());

        let (pairs, rates): (Vec<&str>, Vec<i64>) = if still_fresh {
            // A zero-row batch carrying not_modified is the 304 equivalent:
            // keep what you have. The client reuses its stored rows.
            (Vec::new(), Vec::new())
        } else {
            UPSTREAM_CALLS.fetch_add(1, Ordering::Relaxed);
            (vec!["EURUSD", "GBPUSD", "USDJPY"], vec![108, 127, 15_700])
        };

        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(pairs)),
            Arc::new(Int64Array::from(rates)),
        ];
        RecordBatch::try_new(self.schema.clone(), cols)
            .map(Some)
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }

    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
    fn resume_supported(&self) -> bool {
        // Single batch: there is nothing to resume. Declared explicitly —
        // `resume_supported` has no default, so every producer states this.
        false
    }
}

struct Rates;

impl TableFunction for Rates {
    fn name(&self) -> &str {
        "rates"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Exchange rates from a slow upstream, cached on the client".to_string(),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: rates_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(RatesProducer {
            schema: params.output_schema.clone(),
            done: false,
            if_none_match: None,
            meta: None,
        }))
    }
}

/// Reports how many times the upstream was actually hit, so a query can prove
/// the cache engaged rather than take it on faith.
struct UpstreamCalls;

struct CallsProducer {
    schema: SchemaRef,
    done: bool,
}

impl TableProducer for CallsProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let col: ArrayRef = Arc::new(Int64Array::from(vec![
            UPSTREAM_CALLS.load(Ordering::Relaxed)
        ]));
        RecordBatch::try_new(self.schema.clone(), vec![col])
            .map(Some)
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
    fn resume_supported(&self) -> bool {
        // Single batch: there is nothing to resume. Declared explicitly —
        // `resume_supported` has no default, so every producer states this.
        false
    }
}

impl TableFunction for UpstreamCalls {
    fn name(&self) -> &str {
        "upstream_calls"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "How many times rates() actually computed a result".to_string(),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new(
                "calls",
                DataType::Int64,
                true,
            )])),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(CallsProducer {
            schema: params.output_schema.clone(),
            done: false,
        }))
    }
}

fn main() {
    let mut worker = vgi::Worker::new();
    worker.register_table(Rates);
    worker.register_table(UpstreamCalls);
    worker.set_catalog(CatalogModel {
        name: "rates".to_string(),
        comment: Some("Documentation example: advertising a cacheable result".to_string()),
        ..Default::default()
    });
    worker.run();
}

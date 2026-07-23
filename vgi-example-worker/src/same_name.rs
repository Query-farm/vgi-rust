// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Same-name-in-two-schemas fixtures for the exchange, aggregate and
//! result-cache surfaces.
//!
//! Companion to [`crate::scalar::same_name`] (which collides a *scalar* name
//! across `main` and `data`) and [`crate::twin_catalogs`] (which collides a
//! name across two catalogs). This module covers the four remaining shapes,
//! each declared under one name in BOTH the `main` and `data` schemas of the
//! `example` catalog:
//!
//! - `test_same_name_transform` — table-in-out (streaming exchange),
//! - `test_same_name_buffered` — table-buffering (Sink + Source),
//! - `test_same_name_agg` — aggregate,
//! - `test_same_name_cached` — cacheable table producer (result cache).
//!
//! Each shape binds and *runs* through different machinery, which is why all
//! are needed:
//!
//! - The exchange pair binds through `VgiTableInOutBind`, which builds its
//!   bind-time connection directly rather than through
//!   `AcquireAndBindConnection`. It originally never named the schema on that
//!   request, so every exchange-mode bind reached the worker with no
//!   `BindRequest.schema_name` and was unresolvable across two schemas.
//! - The buffered pair shares that bind site but acquires its runtime
//!   connections through the buffering operator's own `BuildAcquireParams`, so
//!   the Sink phase is independent coverage. It tags in `process` (the Sink),
//!   proving the sink-side worker resolved the right implementation.
//! - The aggregate is the widest surface: every aggregate RPC re-resolves the
//!   function by name over `InvokePooledUnaryRpc`, which is stateless and holds
//!   no bound connection, so the request is the only carrier of the schema —
//!   the reason protocol 1.2.0 puts `schema_name` on all of them. The tag is
//!   stamped at finalize while accumulation happens in update, so a *partial*
//!   mis-route (bind one implementation, update/finalize another) is visible.
//! - The cacheable producer probes a *different layer* — the C++ result cache,
//!   not dispatch. Its one row advertises `vgi.cache.ttl`, so the complete
//!   result is memoized. The cache key was catalog + auth + function name with
//!   no schema dimension, so the two implementations produced byte-identical
//!   keys and one schema's memoized row cross-served the other. The tag makes
//!   that visible: `example.data.test_same_name_cached()` would return a `main`
//!   row. With the schema in the key each schema gets its own entry, so
//!   `vgi_result_cache()` holds two rows for the one function name and each
//!   returns its own tag.
//!
//! Every implementation tags its output with its own schema, so a mis-routed
//! call reads as the wrong tag rather than a plausible answer. Ports
//! vgi-python's `_test_fixtures/table_in_out_same_name.py`,
//! `_test_fixtures/aggregate/same_name.py` and
//! `_test_fixtures/table/same_name_cached.py`; driven by
//! `test/sql/integration/{table_in_out,aggregate,cache}/same_name_schemas.test`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::aggregate::{AggregateBindParams, AggregateFunction};
use vgi::buffering::{BufferingParams, TableBufferingFunction};
use vgi::cache_control::CacheControl;
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FunctionExample, FunctionMetadata, ProcessParams,
};
use vgi::ipc;
use vgi::protocol::enums;
use vgi::table_function::{TableFunction, TableProducer};
use vgi::table_in_out::{TableInOutFunction, TableInOutOutput};
use vgi_rpc::{Result, RpcError};

/// The catalog all six implementations live in.
const CATALOG: &str = "example";
/// The two schemas each name is declared in.
const SCHEMAS: [&str; 2] = ["main", "data"];

const TRANSFORM_NAME: &str = "test_same_name_transform";
const BUFFERED_NAME: &str = "test_same_name_buffered";
const AGG_NAME: &str = "test_same_name_agg";
const CACHED_NAME: &str = "test_same_name_cached";

/// Long enough that the cache TTL never lapses mid-test.
const CACHE_TTL_SECONDS: i64 = 300;

/// Storage namespace for the buffered pair's sink log.
const NS: &[u8] = b"same_name_buf";

/// The single output column the exchange pair emits.
fn tag_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("tag", DataType::Utf8, true)]))
}

/// Render `<schema>:<value>` for every row of the first input column.
fn tag_batch(schema_name: &str, batch: &RecordBatch) -> Result<RecordBatch> {
    let cast = arrow_cast::cast(batch.column(0), &DataType::Int64)
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    let v = cast.as_primitive::<Int64Type>();
    let out: StringArray = (0..v.len())
        .map(|i| (!v.is_null(i)).then(|| format!("{schema_name}:{}", v.value(i))))
        .collect();
    RecordBatch::try_new(tag_schema(), vec![Arc::new(out) as ArrayRef])
        .map_err(|e| RpcError::runtime_error(e.to_string()))
}

fn table_arg() -> ArgSpec {
    ArgSpec::column("data", 0, "table", "Input table")
}

// ---------------------------------------------------------------------------
// Table-in-out (streaming exchange) pair
// ---------------------------------------------------------------------------

/// `test_same_name_transform(data)` — tags each input row with the schema the
/// implementation is declared in.
pub struct SameNameTransform {
    schema: &'static str,
}

impl TableInOutFunction for SameNameTransform {
    fn name(&self) -> &str {
        TRANSFORM_NAME
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: format!(
                "Schema-disambiguation probe; the {}-schema table-in-out",
                self.schema
            ),
            categories: vec!["testing".into()],
            examples: vec![FunctionExample {
                sql: format!(
                    "SELECT * FROM example.{}.test_same_name_transform((SELECT 1 AS n))",
                    self.schema
                ),
                description: format!("Returns '{}:1'", self.schema),
                expected_output: Some(format!("{}:1", self.schema)),
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg()]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: tag_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn process_out(
        &self,
        _params: &vgi::function::ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        out.emit(tag_batch(self.schema, batch)?);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Table-buffering (Sink + Source) pair
// ---------------------------------------------------------------------------

/// `test_same_name_buffered(data)` — tags in the SINK phase and replays the
/// tagged batches in the Source phase.
pub struct SameNameBuffered {
    schema: &'static str,
}

impl TableBufferingFunction for SameNameBuffered {
    fn name(&self) -> &str {
        BUFFERED_NAME
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: format!(
                "Schema-disambiguation probe; the {}-schema buffered function",
                self.schema
            ),
            categories: vec!["testing".into()],
            examples: vec![FunctionExample {
                sql: format!(
                    "SELECT * FROM example.{}.test_same_name_buffered((SELECT 1 AS n))",
                    self.schema
                ),
                description: format!("Returns '{}:1'", self.schema),
                expected_output: Some(format!("{}:1", self.schema)),
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg()]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: tag_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &BufferingParams, batch: &RecordBatch) -> Result<Vec<u8>> {
        // Tagging here rather than in the Source phase is deliberate: it proves
        // the SINK-side worker resolved the right implementation, and the Sink
        // acquires a different connection than the Source does.
        let tagged = tag_batch(self.schema, batch)?;
        params
            .storage
            .append(&params.execution_id, NS, b"", ipc::write_batch(&tagged)?);
        Ok(params.execution_id.clone())
    }
    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        // Collapse every Sink bucket into one finalize stream.
        Ok(vec![params.execution_id.clone()])
    }
    fn finalize_producer(
        &self,
        params: &BufferingParams,
        _finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(TaggedDrain {
            storage: params.storage.clone(),
            execution_id: params.execution_id.clone(),
            after_id: -1,
        }))
    }
}

/// Replays the batches the Sink buffered, one per tick.
struct TaggedDrain {
    storage: Arc<dyn vgi::storage::FunctionStorage>,
    execution_id: Vec<u8>,
    after_id: i64,
}

impl TableProducer for TaggedDrain {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        let rows = self
            .storage
            .scan(&self.execution_id, NS, b"", self.after_id, 1);
        let Some((log_id, value)) = rows.into_iter().next() else {
            return Ok(None);
        };
        self.after_id = log_id;
        Ok(Some(ipc::read_batch(&value)?))
    }
}

// ---------------------------------------------------------------------------
// Aggregate pair
// ---------------------------------------------------------------------------

fn le_i64(v: i64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

fn read_i64(b: &[u8]) -> i64 {
    let mut a = [0u8; 8];
    let n = b.len().min(8);
    a[..n].copy_from_slice(&b[..n]);
    i64::from_le_bytes(a)
}

/// `test_same_name_agg(value)` — sums its input and tags the total with the
/// schema the implementation is declared in.
pub struct SameNameAgg {
    schema: &'static str,
}

impl AggregateFunction for SameNameAgg {
    fn name(&self) -> &str {
        AGG_NAME
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: format!(
                "Schema-disambiguation probe; the {}-schema aggregate",
                self.schema
            ),
            categories: vec!["testing".into()],
            null_handling: Some(enums::null_handling::SPECIAL.into()),
            order_preservation: None,
            examples: vec![FunctionExample {
                sql: format!(
                    "SELECT example.{}.test_same_name_agg(n) FROM range(3) t(n)",
                    self.schema
                ),
                description: format!("Returns '{}:3'", self.schema),
                expected_output: Some(format!("{}:3", self.schema)),
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column(
            "value",
            0,
            "int64",
            "Integer value to accumulate",
        )]
    }
    fn on_bind(&self, _params: &AggregateBindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new(
                "result",
                DataType::Utf8,
                true,
            )])),
            opaque_data: Vec::new(),
        })
    }
    fn initial_state(&self) -> Vec<u8> {
        le_i64(0)
    }
    fn update(
        &self,
        states: &mut HashMap<i64, Vec<u8>>,
        group_ids: &Int64Array,
        columns: &[ArrayRef],
    ) -> Result<()> {
        let cast = arrow_cast::cast(&columns[0], &DataType::Int64)
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let v = cast.as_primitive::<Int64Type>();
        for i in 0..group_ids.len() {
            if group_ids.is_null(i) || v.is_null(i) {
                continue;
            }
            let gid = group_ids.value(i);
            let total = states.get(&gid).map(|b| read_i64(b)).unwrap_or(0);
            states.insert(gid, le_i64(total + v.value(i)));
        }
        Ok(())
    }
    fn combine(&self, target: Vec<u8>, source: Vec<u8>) -> Result<Vec<u8>> {
        Ok(le_i64(read_i64(&target) + read_i64(&source)))
    }
    fn finalize(
        &self,
        output_schema: &SchemaRef,
        group_ids: &Int64Array,
        states: &[Option<Vec<u8>>],
    ) -> Result<RecordBatch> {
        // The tag is stamped here while accumulation happened in update(), so a
        // partial mis-route across the two RPCs shows up in the result.
        let out: StringArray = (0..group_ids.len())
            .map(|i| {
                let total = states
                    .get(i)
                    .and_then(|s| s.as_ref())
                    .map(|b| read_i64(b))
                    .unwrap_or(0);
                Some(format!("{}:{total}", self.schema))
            })
            .collect();
        RecordBatch::try_new(output_schema.clone(), vec![Arc::new(out) as ArrayRef])
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Cacheable table-producer pair (result cache)
// ---------------------------------------------------------------------------

/// `test_same_name_cached()` — a one-row producer that advertises
/// `vgi.cache.ttl` and tags its single row with the schema the implementation
/// is declared in, so the result cache stores one entry per schema and a
/// cross-serve reads as the wrong tag. See the module doc.
pub struct SameNameCached {
    schema: &'static str,
}

impl TableFunction for SameNameCached {
    fn name(&self) -> &str {
        CACHED_NAME
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: format!(
                "Schema-disambiguation probe; the {}-schema cacheable producer",
                self.schema
            ),
            categories: vec!["generator".into(), "cache".into(), "testing".into()],
            examples: vec![FunctionExample {
                sql: format!(
                    "SELECT * FROM example.{}.test_same_name_cached()",
                    self.schema
                ),
                description: format!("One cacheable row tagged '{}'", self.schema),
                expected_output: Some(self.schema.to_string()),
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: tag_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(CachedTagRow {
            schema: params.output_schema.clone(),
            tag: self.schema,
            done: false,
            meta: None,
        }))
    }
}

/// Emits the single schema-tagged row once, advertising a cache TTL on it.
struct CachedTagRow {
    schema: SchemaRef,
    tag: &'static str,
    done: bool,
    meta: Option<HashMap<String, String>>,
}

impl TableProducer for CachedTagRow {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        self.meta = Some(CacheControl::ttl(CACHE_TTL_SECONDS).to_metadata());
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(StringArray::from(vec![self.tag])) as ArrayRef],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

/// Declare all four pairs, each implementation into its own schema of
/// `example`. Only the primary catalog carries them.
pub fn register(w: &mut vgi::Worker) {
    for schema in SCHEMAS {
        w.register_table_in_out_in(CATALOG, schema, SameNameTransform { schema });
        w.register_buffering_in(CATALOG, schema, SameNameBuffered { schema });
        w.register_aggregate_in(CATALOG, schema, SameNameAgg { schema });
        w.register_table_in(CATALOG, schema, SameNameCached { schema });
    }
}

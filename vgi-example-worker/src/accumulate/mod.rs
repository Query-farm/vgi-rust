// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `accumulate` fixture catalog — a per-ATTACH, name-keyed row accumulator
//! backed by an attach-scoped, file-backed persistent store. Port of
//! vgi-python's `accumulate` test fixture (mirrors the Go port structurally).
//!
//! - `accumulate(name, <rows>, ttl, max_row_size, result)` — append rows to a
//!   named collection (stamping one call-time `_timestamp`) and optionally
//!   return its contents. A table-buffering (sink→combine→source) function.
//! - `accumulate_read(name)` — read a collection's rows without modifying it.
//! - `accumulate_clear(name)` — drop a collection; returns rows removed.
//!
//! Collections persist across queries (the storage scope is the random
//! per-ATTACH `attach_opaque_data`), so they survive the fresh worker a
//! subprocess-transport query spawns, and two ATTACH sessions never collide.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::IntervalMonthDayNanoType;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use vgi::buffering::{BufferingParams, TableBufferingFunction};
use vgi::catalog::{CatSchema, CatalogModel};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::ipc;
use vgi::table_function::{TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

const TS_COL: &str = "_timestamp";
const MAX_NAME_BYTES: usize = 255;
/// Target rows per emitted result batch.
const OUT_BATCH_ROWS: usize = 65536;
/// Data version advertised by `vgi_catalogs()` for the accumulate catalog.
const DATA_VERSION: &str = "2.0.0";
const IMPLEMENTATION_VERSION: &str = "vgi-fixture";

/// Execution-scoped staging namespaces (transient per query).
const NS_IN: &[u8] = b"acc_in";
const NS_OUT: &[u8] = b"acc_out";

// ---------------------------------------------------------------------------
// Schema / time helpers
// ---------------------------------------------------------------------------

/// Tz-naive microsecond timestamp → DuckDB TIMESTAMP (not WITH TIME ZONE).
fn ts_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn output_schema_of(input: &Schema) -> SchemaRef {
    let mut fields: Vec<Field> = input.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new(TS_COL, ts_type(), false));
    Arc::new(Schema::new(fields))
}

fn input_schema_of(output: &Schema) -> SchemaRef {
    let fields: Vec<Field> = output
        .fields()
        .iter()
        .filter(|f| f.name() != TS_COL)
        .map(|f| f.as_ref().clone())
        .collect();
    Arc::new(Schema::new(fields))
}

/// Compare a pinned input schema against an incoming one (names + types,
/// ignoring metadata).
fn input_fields_match(pinned: &Schema, incoming: &Schema) -> bool {
    if pinned.fields().len() != incoming.fields().len() {
        return false;
    }
    pinned
        .fields()
        .iter()
        .zip(incoming.fields().iter())
        .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
}

fn validate_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        return Err(RpcError::value_error(
            "collection name must be a non-empty string",
        ));
    }
    if name.len() > MAX_NAME_BYTES {
        return Err(RpcError::value_error(format!(
            "collection name must be at most {MAX_NAME_BYTES} bytes"
        )));
    }
    Ok(())
}

fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// The storage scope for a call: the (stable, per-ATTACH) attach_opaque_data,
/// or a shared default when none was carried.
fn scope_of(attach: &Option<Vec<u8>>) -> Vec<u8> {
    match attach {
        Some(b) if !b.is_empty() => b.clone(),
        _ => b"default".to_vec(),
    }
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

// ---------------------------------------------------------------------------
// Persistent, attach-scoped collection store
// ---------------------------------------------------------------------------
//
// Layout: $TMPDIR/vgi-rust-accumulate/<scope>/<name>/
//   schema.ipc        — pinned output schema (input + _timestamp)
//   seg/<ts:020>_<seq:010>.bin — one IPC batch per call, keyed by call time so
//                                segments sort oldest-first and a TTL cutoff is
//                                a filename range.

struct AccumulateStore {
    base: PathBuf,
}

impl AccumulateStore {
    fn new() -> Self {
        let mut base = std::env::temp_dir();
        base.push("vgi-rust-accumulate");
        let _ = std::fs::create_dir_all(&base);
        AccumulateStore { base }
    }

    fn coll_dir(&self, scope: &[u8], name: &str) -> PathBuf {
        let mut p = self.base.clone();
        p.push(hex(scope));
        p.push(hex(name.as_bytes()));
        p
    }

    fn seg_dir(&self, scope: &[u8], name: &str) -> PathBuf {
        self.coll_dir(scope, name).join("seg")
    }

    fn schema_path(&self, scope: &[u8], name: &str) -> PathBuf {
        self.coll_dir(scope, name).join("schema.ipc")
    }

    fn get_schema(&self, scope: &[u8], name: &str) -> Option<SchemaRef> {
        std::fs::read(self.schema_path(scope, name))
            .ok()
            .and_then(|b| ipc::read_schema(&b).ok())
    }

    fn put_schema(&self, scope: &[u8], name: &str, schema: &SchemaRef) -> Result<()> {
        let _ = std::fs::create_dir_all(self.coll_dir(scope, name));
        std::fs::write(
            self.schema_path(scope, name),
            ipc::write_schema_ref(schema)?,
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))
    }

    /// Segment files sorted oldest-first, as `(ts_micros, path)`.
    fn segments(&self, scope: &[u8], name: &str) -> Vec<(i64, PathBuf)> {
        let dir = self.seg_dir(scope, name);
        let mut files: Vec<(String, i64, PathBuf)> = std::fs::read_dir(&dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                let fname = e.file_name().to_str()?.to_string();
                let ts: i64 = fname.split('_').next()?.parse().ok()?;
                Some((fname, ts, e.path()))
            })
            .collect();
        files.sort_by(|a, b| a.0.cmp(&b.0));
        files.into_iter().map(|(_, ts, p)| (ts, p)).collect()
    }

    fn append_segment(&self, scope: &[u8], name: &str, batch: &RecordBatch, ts: i64) -> Result<()> {
        let dir = self.seg_dir(scope, name);
        let _ = std::fs::create_dir_all(&dir);
        // A monotonic seq within the (possibly shared) timestamp keeps the
        // filename unique and preserves insertion order.
        let seq = std::fs::read_dir(&dir)
            .into_iter()
            .flatten()
            .flatten()
            .count();
        let path = dir.join(format!("{ts:020}_{seq:010}.bin"));
        std::fs::write(&path, ipc::write_batch(batch)?)
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }

    fn read_all(&self, scope: &[u8], name: &str) -> Result<Vec<RecordBatch>> {
        self.segments(scope, name)
            .into_iter()
            .map(|(_, path)| {
                let bytes =
                    std::fs::read(&path).map_err(|e| RpcError::runtime_error(e.to_string()))?;
                ipc::read_batch(&bytes)
            })
            .collect()
    }

    fn count(&self, scope: &[u8], name: &str) -> i64 {
        self.read_all(scope, name)
            .map(|b| b.iter().map(|r| r.num_rows() as i64).sum())
            .unwrap_or(0)
    }

    /// Drop whole segments whose call time is `< cutoff` (one filename range).
    fn evict_ttl(&self, scope: &[u8], name: &str, cutoff: i64) {
        if cutoff <= 0 {
            return;
        }
        for (ts, path) in self.segments(scope, name) {
            if ts < cutoff {
                let _ = std::fs::remove_file(&path);
            }
        }
    }

    /// Drop the oldest rows until at most `max` remain (whole oldest segments
    /// plus at most one trimmed boundary segment — never a full rewrite).
    fn evict_max_rows(&self, scope: &[u8], name: &str, max: i64) -> Result<()> {
        let total = self.count(scope, name);
        if total <= max {
            return Ok(());
        }
        let overflow = total - max;
        let mut removed: i64 = 0;
        for (_, path) in self.segments(scope, name) {
            if removed >= overflow {
                break;
            }
            let bytes = std::fs::read(&path).map_err(|e| RpcError::runtime_error(e.to_string()))?;
            let batch = ipc::read_batch(&bytes)?;
            let n = batch.num_rows() as i64;
            if removed + n <= overflow {
                let _ = std::fs::remove_file(&path);
                removed += n;
            } else {
                // Boundary segment: keep its newest rows, drop the oldest.
                let offset = (overflow - removed) as usize;
                let kept = batch.slice(offset, batch.num_rows() - offset);
                std::fs::write(&path, ipc::write_batch(&kept)?)
                    .map_err(|e| RpcError::runtime_error(e.to_string()))?;
                removed = overflow;
            }
        }
        Ok(())
    }

    fn clear(&self, scope: &[u8], name: &str) -> i64 {
        let total = self.count(scope, name);
        let _ = std::fs::remove_dir_all(self.coll_dir(scope, name));
        total
    }
}

/// Stamp `input` with an `n`-row `_timestamp` column of `ts` micros.
fn stamp(input: &RecordBatch, output_schema: &SchemaRef, ts: i64) -> Result<RecordBatch> {
    let mut cols: Vec<ArrayRef> = input.columns().to_vec();
    let n = input.num_rows();
    cols.push(Arc::new(TimestampMicrosecondArray::from(vec![ts; n])) as ArrayRef);
    RecordBatch::try_new(output_schema.clone(), cols)
        .map_err(|e| RpcError::runtime_error(e.to_string()))
}

/// Read a named INTERVAL (month_day_nano) argument as microseconds; months are
/// treated as 30 days. `None` when the argument is absent/null.
fn named_interval_micros(args: &vgi::arguments::Arguments, name: &str) -> Option<i64> {
    let arr = args.named(name)?;
    let iv = arr.as_primitive_opt::<IntervalMonthDayNanoType>()?;
    let v = iv.value(0);
    Some((v.months as i64 * 30 + v.days as i64) * 86_400_000_000 + v.nanoseconds / 1000)
}

// ---------------------------------------------------------------------------
// Shared producer over a staged / in-memory batch list
// ---------------------------------------------------------------------------

/// Emits a fixed list of batches, one per tick, projected to `output_schema`.
struct BatchListProducer {
    batches: Vec<RecordBatch>,
    pos: usize,
    output_schema: SchemaRef,
}

impl TableProducer for BatchListProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.pos >= self.batches.len() {
            return Ok(None);
        }
        let batch = self.batches[self.pos].clone();
        self.pos += 1;
        Ok(Some(vgi::table_in_out::project_batch(
            &batch,
            &self.output_schema,
        )?))
    }
}

/// Drains the execution-scoped output log (NS_OUT), one batch per tick.
struct OutDrain {
    storage: Arc<vgi::buffering::BufferingStore>,
    execution_id: Vec<u8>,
    after_id: i64,
    output_schema: SchemaRef,
}

impl TableProducer for OutDrain {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        let rows = self
            .storage
            .scan(&self.execution_id, NS_OUT, b"", self.after_id, 1);
        let Some((id, value)) = rows.into_iter().next() else {
            return Ok(None);
        };
        self.after_id = id;
        let batch = ipc::read_batch(&value)?;
        Ok(Some(vgi::table_in_out::project_batch(
            &batch,
            &self.output_schema,
        )?))
    }
}

/// Re-chunk `table` (a list of batches) into `≤ OUT_BATCH_ROWS` slices staged
/// into the execution-scoped output log for the source phase to drain.
fn stage_batches(
    storage: &vgi::buffering::BufferingStore,
    exec: &[u8],
    batches: &[RecordBatch],
) -> Result<()> {
    for batch in batches {
        let mut off = 0;
        while off < batch.num_rows() {
            let len = (batch.num_rows() - off).min(OUT_BATCH_ROWS);
            let slice = batch.slice(off, len);
            storage.append(exec, NS_OUT, b"", ipc::write_batch(&slice)?);
            off += len;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// accumulate(name, <rows>, ttl, max_row_size, result)
// ---------------------------------------------------------------------------

pub struct AccumulateFunction;

impl TableBufferingFunction for AccumulateFunction {
    fn name(&self) -> &str {
        "accumulate"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description:
                "Append rows to a named collection; return all/new/no rows with a _timestamp column"
                    .to_string(),
            categories: vec!["stateful".to_string(), "utility".to_string()],
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg(
                "name",
                0,
                "varchar",
                "Name of the collection to accumulate into",
            ),
            ArgSpec::column(
                "data",
                1,
                "table",
                "Rows to accumulate (any table expression)",
            ),
            ArgSpec::const_typed(
                "ttl",
                -1,
                DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
                "Evict rows older than this INTERVAL before returning (months treated as 30 days)",
            ),
            ArgSpec::const_arg(
                "max_row_size",
                -1,
                "int64",
                "Maximum rows retained per name; oldest dropped first (0 = unlimited)",
            ),
            ArgSpec::const_arg(
                "result",
                -1,
                "varchar",
                "What to return: 'all' (default), 'new', or 'none'",
            ),
        ]
    }

    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let name = params.arguments.const_str(0).unwrap_or_default();
        validate_name(&name)?;
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("accumulate requires a table input"))?;
        if input.fields().iter().any(|f| f.name() == TS_COL) {
            return Err(RpcError::value_error(format!(
                "input may not contain a reserved '{TS_COL}' column; accumulate adds this column to its output"
            )));
        }
        let out = output_schema_of(&input);

        let store = AccumulateStore::new();
        let scope = scope_of(&params.attach_opaque_data);
        // Lock-free schema pin: write it if absent, else reject a mismatch.
        match store.get_schema(&scope, &name) {
            None => store.put_schema(&scope, &name, &out)?,
            Some(existing) => {
                if !input_fields_match(&input_schema_of(&existing), &input) {
                    return Err(RpcError::value_error(format!(
                        "input schema for accumulate('{name}', ...) does not match the schema already \
                         accumulated under that name"
                    )));
                }
            }
        }
        Ok(BindResponse {
            output_schema: out,
            opaque_data: Vec::new(),
        })
    }

    fn process(&self, params: &BufferingParams, batch: &RecordBatch) -> Result<Vec<u8>> {
        params
            .storage
            .append(&params.execution_id, NS_IN, b"", ipc::write_batch(batch)?);
        Ok(params.execution_id.clone())
    }

    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        let name = params.arguments.const_str(0).unwrap_or_default();
        let scope = scope_of(&params.attach_opaque_data);
        let output_schema = params.output_schema.clone();
        let input_schema = input_schema_of(&output_schema);
        let store = AccumulateStore::new();

        // Reassemble this call's input from the execution-scoped staging log.
        let staged = params
            .storage
            .scan(&params.execution_id, NS_IN, b"", -1, usize::MAX);
        let mut input_batches = Vec::with_capacity(staged.len());
        for (_, value) in staged {
            input_batches.push(ipc::read_batch(&value)?);
        }
        let new_input = if input_batches.is_empty() {
            RecordBatch::new_empty(input_schema.clone())
        } else {
            arrow_select::concat::concat_batches(&input_schema, &input_batches)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?
        };

        let ts = now_micros();
        let new_table = stamp(&new_input, &output_schema, ts)?;
        if new_table.num_rows() > 0 {
            store.append_segment(&scope, &name, &new_table, ts)?;
        }

        // Eviction: ttl (drop rows older than call_time - ttl), then the row cap.
        if let Some(micros) = named_interval_micros(&params.arguments, "ttl") {
            store.evict_ttl(&scope, &name, ts - micros);
        }
        let max_rows = params.arguments.named_i64("max_row_size").unwrap_or(0);
        if max_rows > 0 {
            store.evict_max_rows(&scope, &name, max_rows)?;
        }

        // Stage the requested result for the source phase.
        match params
            .arguments
            .named_str("result")
            .as_deref()
            .unwrap_or("all")
        {
            "none" => {}
            "new" => stage_batches(&params.storage, &params.execution_id, &[new_table])?,
            _ => {
                let all = store.read_all(&scope, &name)?;
                stage_batches(&params.storage, &params.execution_id, &all)?;
            }
        }
        Ok(vec![params.execution_id.clone()])
    }

    fn finalize_producer(
        &self,
        params: &BufferingParams,
        _finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(OutDrain {
            storage: params.storage.clone(),
            execution_id: params.execution_id.clone(),
            after_id: -1,
            output_schema: params.output_schema.clone(),
        }))
    }
}

// ---------------------------------------------------------------------------
// accumulate_read(name)
// ---------------------------------------------------------------------------

pub struct AccumulateReadFunction;

impl TableFunction for AccumulateReadFunction {
    fn name(&self) -> &str {
        "accumulate_read"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Read an accumulated collection's rows without modifying it".to_string(),
            categories: vec!["stateful".to_string(), "utility".to_string()],
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "name",
            0,
            "varchar",
            "Name of the collection to read",
        )]
    }

    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let name = params.arguments.const_str(0).unwrap_or_default();
        validate_name(&name)?;
        let store = AccumulateStore::new();
        let scope = scope_of(&params.attach_opaque_data);
        let schema = store.get_schema(&scope, &name).ok_or_else(|| {
            RpcError::value_error(format!("no accumulation named '{name}' in this session"))
        })?;
        Ok(BindResponse {
            output_schema: schema,
            opaque_data: Vec::new(),
        })
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let name = params.arguments.const_str(0).unwrap_or_default();
        let store = AccumulateStore::new();
        let scope = scope_of(&params.attach_opaque_data);
        let batches = store.read_all(&scope, &name)?;
        Ok(Box::new(BatchListProducer {
            batches,
            pos: 0,
            output_schema: params.output_schema.clone(),
        }))
    }
}

// ---------------------------------------------------------------------------
// accumulate_clear(name)
// ---------------------------------------------------------------------------

fn clear_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("rows_cleared", DataType::Int64, false),
    ]))
}

pub struct AccumulateClearFunction;

impl TableFunction for AccumulateClearFunction {
    fn name(&self) -> &str {
        "accumulate_clear"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Remove an accumulated collection by name; returns rows cleared"
                .to_string(),
            categories: vec!["stateful".to_string(), "utility".to_string()],
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "name",
            0,
            "varchar",
            "Name of the collection to clear",
        )]
    }

    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let name = params.arguments.const_str(0).unwrap_or_default();
        validate_name(&name)?;
        Ok(BindResponse {
            output_schema: clear_schema(),
            opaque_data: Vec::new(),
        })
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let name = params.arguments.const_str(0).unwrap_or_default();
        let store = AccumulateStore::new();
        let scope = scope_of(&params.attach_opaque_data);
        let rows_cleared = store.clear(&scope, &name);
        let batch = RecordBatch::try_new(
            clear_schema(),
            vec![
                Arc::new(StringArray::from(vec![name])) as ArrayRef,
                Arc::new(Int64Array::from(vec![rows_cleared])) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Box::new(BatchListProducer {
            batches: vec![batch],
            pos: 0,
            output_schema: params.output_schema.clone(),
        }))
    }
}

// ---------------------------------------------------------------------------
// Registration & catalog
// ---------------------------------------------------------------------------

/// Register the accumulate functions in the worker's (global) registries.
pub fn register(w: &mut vgi::Worker) {
    w.register_buffering(AccumulateFunction);
    w.register_table(AccumulateReadFunction);
    w.register_table(AccumulateClearFunction);
}

/// The function names the accumulate catalog owns (scopes its function listing).
pub fn function_names() -> Vec<String> {
    vec![
        "accumulate".to_string(),
        "accumulate_read".to_string(),
        "accumulate_clear".to_string(),
    ]
}

/// The `accumulate` secondary catalog (one `main` schema; functions resolve
/// from the global registry).
pub fn catalog() -> CatalogModel {
    CatalogModel {
        name: "accumulate".to_string(),
        implementation_version: Some(IMPLEMENTATION_VERSION.to_string()),
        data_version_spec: Some(DATA_VERSION.to_string()),
        comment: Some(
            "Row accumulation keyed by name, persisted via FunctionStorage and scoped per ATTACH"
                .to_string(),
        ),
        supports_time_travel: false,
        schemas: vec![CatSchema {
            name: "main".to_string(),
            comment: Some("Stateful row accumulation functions".to_string()),
            views: Vec::new(),
            macros: Vec::new(),
            tables: Vec::new(),
        }],
        ..Default::default()
    }
}

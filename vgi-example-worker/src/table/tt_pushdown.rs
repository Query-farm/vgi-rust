// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Time-travel + filter-pushdown fixtures (back `time_travel_pushdown.test`).
//!
//! Both echo `seen_version` (the version actually scanned) and `pushed_filters`
//! (the DuckDB-SQL predicate pushed down), so one query asserts both signals.
//!
//! - `tt_pushdown_scan` — **function-backed**: reads the `AT` clause from the
//!   init request (`params.at_unit`/`params.at_value`), which only works once
//!   the framework threads AT onto the bind request. Backs
//!   `example.data.tt_pushdown_fn`.
//! - `tt_pushdown_cols_scan` — **columns-based**: gets the resolved version as a
//!   scan-function argument (the worker resolves `AT` → version in
//!   `catalog_table_scan_function_get`, the native columns-based mechanism).
//!   Backs `example.data.tt_pushdown_cols`.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{resume, TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(TimeTravelPushdownFunction);
    w.register_table(TtPushdownColsScanFunction);
}

/// Output schema (version-INDEPENDENT — only the row data changes per version).
pub fn tt_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("val", DataType::Int64, true),
        Field::new("seen_version", DataType::Int64, true),
        Field::new("pushed_filters", DataType::Utf8, true),
    ]))
}

const CURRENT_VERSION: i64 = 2;

/// Per-version row ids (val = id * 10). v2 is a strict superset of v1.
fn version_ids(version: i64) -> Vec<i64> {
    match version {
        1 => (1..=5).collect(),
        _ => (1..=10).collect(),
    }
}

/// Resolve an `AT` clause to one of this fixture's versions (1 or 2). `None`
/// unit → current (2); `VERSION => n` → n; `TIMESTAMP` → year <= 2020 → 1 else 2.
pub fn resolve_tt_version(at_unit: Option<&str>, at_value: Option<&str>) -> Result<i64> {
    let unit = match at_unit {
        None => return Ok(CURRENT_VERSION),
        Some("") => return Ok(CURRENT_VERSION),
        Some(u) => u.to_uppercase(),
    };
    match unit.as_str() {
        "VERSION" => {
            let v: i64 = at_value
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| RpcError::value_error("invalid AT VERSION value"))?;
            if v == 1 || v == 2 {
                Ok(v)
            } else {
                Err(RpcError::value_error(format!(
                    "Unknown version {v}; valid: [1, 2]"
                )))
            }
        }
        "TIMESTAMP" => {
            let year: i32 = at_value
                .and_then(|s| s.get(..4))
                .and_then(|y| y.parse().ok())
                .ok_or_else(|| RpcError::value_error("invalid AT TIMESTAMP value"))?;
            Ok(if year <= 2020 { 1 } else { 2 })
        }
        other => Err(RpcError::value_error(format!(
            "Unsupported at_unit: {other:?}"
        ))),
    }
}

/// The SQL-like string of whatever DuckDB pushed down ("(none)" if nothing).
fn pushed_filter_str(params: &ProcessParams) -> String {
    match &params.pushdown_filters {
        Some(bytes) => {
            vgi::pushdown::PushdownFilters::parse_with_join_keys(bytes, &params.join_keys)
                .map(|f| f.format_pushed())
                .unwrap_or_else(|_| "(none)".to_string())
        }
        None => "(none)".to_string(),
    }
}

/// Build one full-schema batch for `version` (the dispatch adapter narrows it to
/// the projected output schema and auto-applies pushed-down filters).
fn build_batch(version: i64, pushed: &str) -> Result<RecordBatch> {
    let ids = version_ids(version);
    let n = ids.len();
    let vals: Vec<i64> = ids.iter().map(|i| i * 10).collect();
    let seen = vec![version; n];
    let pf: Vec<&str> = vec![pushed; n];
    RecordBatch::try_new(
        tt_schema(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(Int64Array::from(vals)) as ArrayRef,
            Arc::new(Int64Array::from(seen)) as ArrayRef,
            Arc::new(StringArray::from(pf)) as ArrayRef,
        ],
    )
    .map_err(|e| RpcError::runtime_error(e.to_string()))
}

fn meta(desc: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        categories: vec!["generator".into(), "diagnostic".into(), "testing".into()],
        projection_pushdown: true,
        filter_pushdown: true,
        auto_apply_filters: true,
        ..Default::default()
    }
}

struct OneShot {
    batch: Option<RecordBatch>,
    done: bool,
}
impl TableProducer for OneShot {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        Ok(self.batch.take())
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[if self.done { 1 } else { 0 }])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.done = v[0] != 0;
        }
    }
}

/// `tt_pushdown_scan` — function-backed: version comes from the AT clause read
/// at init (`params.at_unit`/`params.at_value`), not from an argument.
pub struct TimeTravelPushdownFunction;
impl TableFunction for TimeTravelPushdownFunction {
    fn name(&self) -> &str {
        "tt_pushdown_scan"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Function-backed time-travel + filter-pushdown scan (reads AT at init).")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: tt_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let version = resolve_tt_version(params.at_unit.as_deref(), params.at_value.as_deref())?;
        let batch = build_batch(version, &pushed_filter_str(params))?;
        Ok(Box::new(OneShot {
            batch: Some(batch),
            done: false,
        }))
    }
}

/// `tt_pushdown_cols_scan(version)` — columns-based: the worker resolves AT →
/// version in `catalog_table_scan_function_get` and passes it as argument 0.
pub struct TtPushdownColsScanFunction;
impl TableFunction for TtPushdownColsScanFunction {
    fn name(&self) -> &str {
        "tt_pushdown_cols_scan"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Columns-based time-travel + filter-pushdown scan (version via arg).")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "version",
            0,
            "int64",
            "Resolved data version",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: tt_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let v = params.arguments.const_i64(0).unwrap_or(CURRENT_VERSION);
        let n = version_ids(v).len() as i64;
        Some(TableCardinality {
            estimate: Some(n),
            max: Some(n),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let version = params.arguments.const_i64(0).unwrap_or(CURRENT_VERSION);
        let batch = build_batch(version, &pushed_filter_str(params))?;
        Ok(Box::new(OneShot {
            batch: Some(batch),
            done: false,
        }))
    }
}

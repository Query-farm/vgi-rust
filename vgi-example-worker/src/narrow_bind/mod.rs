// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `narrow_bind` reproducer catalog — port of vgi-python's `narrow_bind`
//! fixture, served (MetaWorker-style) as a secondary catalog alongside
//! `example`.
//!
//! Two function-backed tables expose a bind-vs-advertise schema mismatch the
//! C++ client must reject at bind (a clear BinderException) rather than
//! segfault at scan time (walking off the end of the worker's batch in
//! `ArrowTableFunction::ArrowToDuckDB`):
//!
//! - `mismatch` advertises columns `{id, val}` but its scan function
//!   `narrow_scan` binds to `{id}` only → must fail closed at bind.
//! - `consistent` advertises `{id, val}` and `wide_scan` binds `{id, val}` →
//!   the positive control; must keep working and return 3 rows.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::arguments::Arguments;
use vgi::catalog::{CatSchema, CatTable, CatalogModel};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

const CATALOG_NAME: &str = "narrow_bind";

/// What the catalog advertises for both tables: two columns.
fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("val", DataType::Int64, true),
    ]))
}

/// What the narrow scan function actually binds to: one column.
fn narrow_bind_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]))
}

/// The (possibly projection-narrowed) `out` schema's columns, drawn from the
/// fixed 3-row table `{id: 0,1,2; val: 10,20,30}`.
fn column_for(name: &str) -> ArrayRef {
    match name {
        "val" => Arc::new(Int64Array::from(vec![10i64, 20, 30])),
        _ => Arc::new(Int64Array::from(vec![0i64, 1, 2])),
    }
}

fn one_shot(out: &SchemaRef) -> Result<Box<dyn TableProducer>> {
    let cols: Vec<ArrayRef> = out.fields().iter().map(|f| column_for(f.name())).collect();
    let batch = RecordBatch::try_new(out.clone(), cols)
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
    Ok(Box::new(OneShot { batch: Some(batch) }))
}

struct OneShot {
    batch: Option<RecordBatch>,
}
impl TableProducer for OneShot {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
}

/// `narrow_scan(count)` — binds to a NARROWER schema than the catalog
/// advertises (the bug). The scan is never reached in the test: the client
/// refuses at bind once the 1-column output_schema disagrees with the
/// 2-column advertised schema.
pub struct NarrowScan;
impl TableFunction for NarrowScan {
    fn name(&self) -> &str {
        "narrow_scan"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "bind reports a narrower schema than the table advertises".to_string(),
            categories: vec!["catalog".into(), "testing".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "rows")]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: narrow_bind_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        one_shot(&params.output_schema)
    }
}

/// `wide_scan(count)` — binds to the full advertised schema (positive
/// control; must work unchanged).
pub struct WideScan;
impl TableFunction for WideScan {
    fn name(&self) -> &str {
        "wide_scan"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "bind matches the table's advertised schema".to_string(),
            categories: vec!["catalog".into(), "testing".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "rows")]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: table_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        one_shot(&params.output_schema)
    }
}

/// A function-backed table advertising the 2-column schema, scanned lazily by
/// `scan_fn(3)` (resolution via `catalog_table_scan_function_get`).
fn cat_table(name: &str, scan_fn: &str) -> CatTable {
    let scan_arguments =
        Arguments::serialize_scan_args(&[Arc::new(Int64Array::from(vec![3i64])) as ArrayRef])
            .unwrap_or_default();
    CatTable::new(
        name,
        table_schema(),
        scan_fn,
        scan_arguments,
        Some(format!("narrow-bind reproducer table -> {scan_fn}")),
        None,
    )
}

/// Register the scan functions in the worker's (global) table registry; the
/// secondary catalog scopes them out of the primary's function listing.
pub fn register(w: &mut vgi::Worker) {
    w.register_table(NarrowScan);
    w.register_table(WideScan);
}

/// The function names the narrow_bind catalog owns (scopes its listing).
pub fn function_names() -> Vec<String> {
    vec!["narrow_scan".to_string(), "wide_scan".to_string()]
}

/// The `narrow_bind` secondary catalog (one `main` schema, two function-backed
/// tables).
pub fn catalog() -> CatalogModel {
    CatalogModel {
        name: CATALOG_NAME.to_string(),
        comment: Some("narrow-bind reproducer catalog".to_string()),
        schemas: vec![CatSchema {
            name: "main".to_string(),
            comment: Some("narrow-bind reproducer catalog".to_string()),
            tags: Vec::new(),
            views: Vec::new(),
            macros: Vec::new(),
            tables: vec![
                cat_table("mismatch", "narrow_scan"),
                cat_table("consistent", "wide_scan"),
            ],
        }],
        ..Default::default()
    }
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! A read-only companion-catalog fixture for the DataFusion adapter.
//!
//! The primary catalog advertises a VGI companion served by the same worker.
//! Its `hot_cold` table consists solely of catalog-table source arms, so a
//! successful query proves the client attached and scanned the companion rather
//! than silently falling back to a worker function.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::catalog::{CatBranch, CatSchema, CatTable, CatalogModel};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub const ROOT_CATALOG: &str = "datafusion_companion";
pub const SOURCE_CATALOG: &str = "datafusion_source";
pub const SOURCE_ALIAS: &str = "datafusion_source_alias";

const EVENTS_SCAN: &str = "datafusion_companion_events_scan";

fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("weight", DataType::Int64, false),
    ]))
}

fn source_branch(table: &str, filter: Option<&str>) -> CatBranch {
    CatBranch {
        source_catalog: Some(SOURCE_CATALOG.to_string()),
        source_schema: Some("main".to_string()),
        source_table: Some(table.to_string()),
        branch_filter: filter.map(str::to_string),
        ..Default::default()
    }
}

fn root_branch(table: &str) -> CatBranch {
    CatBranch {
        source_catalog: Some(ROOT_CATALOG.to_string()),
        source_schema: Some("main".to_string()),
        source_table: Some(table.to_string()),
        ..Default::default()
    }
}

fn branch_table(name: &str, branches: Vec<CatBranch>, comment: &str) -> CatTable {
    let mut table = CatTable::new(
        name,
        events_schema(),
        "",
        Vec::new(),
        Some(comment.to_string()),
        Some(5),
    );
    table.branches = Some(branches);
    table
}

pub fn root_catalog() -> CatalogModel {
    CatalogModel {
        name: ROOT_CATALOG.to_string(),
        comment: Some("DataFusion companion-catalog integration fixture".to_string()),
        schemas: vec![CatSchema {
            name: "main".to_string(),
            tables: vec![
                branch_table(
                    "hot_cold",
                    vec![
                        source_branch("events", Some("id < 100")),
                        source_branch("events", Some("id >= 100")),
                    ],
                    "Two filtered arms over a companion catalog table",
                ),
                branch_table(
                    "cycle_entry",
                    vec![source_branch("cycle_back", None)],
                    "Intentional indirect catalog-table source cycle",
                ),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn source_catalog() -> CatalogModel {
    let events = CatTable::new(
        "events",
        events_schema(),
        EVENTS_SCAN,
        Vec::new(),
        Some("Static rows consumed through catalog-table source arms".to_string()),
        Some(5),
    );
    CatalogModel {
        name: SOURCE_CATALOG.to_string(),
        comment: Some("Read-only source catalog for DataFusion federation tests".to_string()),
        schemas: vec![CatSchema {
            name: "main".to_string(),
            tables: vec![
                events,
                branch_table(
                    "cycle_back",
                    vec![root_branch("cycle_entry")],
                    "Back edge for the intentional indirect source cycle",
                ),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Register the source implementation, its secondary catalog, and the required
/// same-location companion declaration.
pub fn register(worker: &mut vgi::Worker) {
    worker.register_table(EventsScan);
    worker.register_secondary_catalog(source_catalog(), vec![EVENTS_SCAN.to_string()]);
    worker.register_attach_catalog(vgi::protocol::dtos::AttachCatalogInfo {
        alias: SOURCE_ALIAS.to_string(),
        target: SOURCE_CATALOG.to_string(),
        db_type: "vgi".to_string(),
        options: Vec::new(),
        hidden: false,
        required: true,
        secret_ref: String::new(),
    });
}

struct EventsScan;

impl TableFunction for EventsScan {
    fn name(&self) -> &str {
        EVENTS_SCAN
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Static companion-catalog rows".to_string(),
            categories: vec!["catalog".to_string(), "datafusion".to_string()],
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: events_schema(),
            opaque_data: Vec::new(),
        })
    }

    fn cardinality(&self, _params: &BindParams) -> Option<TableCardinality> {
        Some(TableCardinality {
            estimate: Some(5),
            max: Some(5),
        })
    }

    fn producer(&self, _params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 100, 200])),
            Arc::new(StringArray::from(vec![
                "cold-a", "cold-b", "cold-c", "hot-x", "hot-y",
            ])),
            Arc::new(Int64Array::from(vec![10, 20, 30, 1000, 2000])),
        ];
        let batch = RecordBatch::try_new(events_schema(), columns)
            .map_err(|error| RpcError::runtime_error(error.to_string()))?;
        Ok(Box::new(OneShot { batch: Some(batch) }))
    }
}

struct OneShot {
    batch: Option<RecordBatch>,
}

impl TableProducer for OneShot {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }

    fn resume_supported(&self) -> bool {
        false
    }
}

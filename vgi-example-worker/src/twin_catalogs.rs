// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Two catalogs, one worker, colliding function names.
//!
//! `twin_a` and `twin_b` are separate VGI catalogs served by the same worker
//! process (as secondary catalogs alongside `example`). Each declares a schema
//! literally named `main` holding a scalar literally named
//! `test_same_name_catalog` — so neither the function name nor the schema name
//! distinguishes them. Only the catalog does.
//!
//! Attaching both from the same worker LOCATION and calling
//! `a.main.test_same_name_catalog(1)` vs `b.main.test_same_name_catalog(1)`
//! must reach different implementations. The routing key is the per-attach
//! `attach_opaque_data`: a secondary attach encodes the catalog name into it,
//! so bind and init land on the catalog the caller attached rather than on
//! whichever implementation happens to hold the name first.
//!
//! Companion to [`crate::scalar::same_name`], which collides one name within a
//! *single* catalog across two schemas. Port of vgi-python's
//! `vgi/_test_fixtures/twin_catalogs.py`; driven by
//! `test/sql/integration/scalar/same_name_catalogs.test`.

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use vgi::catalog::{CatSchema, CatalogModel};
use vgi::function::{ArgSpec, FunctionExample, FunctionMetadata, ProcessParams, ScalarFunction};
use vgi_rpc::Result;

use crate::scalar::util::{arc, result};

/// Deliberately identical in both catalogs — the collision is the point.
const FUNCTION_NAME: &str = "test_same_name_catalog";
const SCHEMA_NAME: &str = "main";
const CATALOG_A: &str = "twin_a";
const CATALOG_B: &str = "twin_b";

/// `test_same_name_catalog(value)` — tags each row with the catalog that owns
/// this instance (`twin_a` / `twin_b`).
pub struct TwinFunction {
    catalog: &'static str,
}

impl ScalarFunction for TwinFunction {
    fn name(&self) -> &str {
        FUNCTION_NAME
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: format!(
                "Catalog-disambiguation probe; the {} implementation",
                self.catalog
            ),
            return_type: Some(DataType::Utf8),
            examples: vec![FunctionExample {
                sql: format!("SELECT {}.main.test_same_name_catalog(1)", self.alias()),
                description: format!("Returns '{}:1'", self.catalog),
                expected_output: Some(format!("{}:1", self.catalog)),
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Integer value to tag")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let v = batch
            .column(0)
            .as_primitive::<arrow_array::types::Int64Type>();
        let out: StringArray = (0..v.len())
            .map(|i| (!v.is_null(i)).then(|| format!("{}:{}", self.catalog, v.value(i))))
            .collect();
        result(params, arc(out))
    }
}

impl TwinFunction {
    /// The ATTACH alias the doc example uses (`twin_a` is attached as `a`).
    fn alias(&self) -> &'static str {
        if self.catalog == CATALOG_A {
            "a"
        } else {
            "b"
        }
    }
}

fn catalog_model(name: &str) -> CatalogModel {
    CatalogModel {
        name: name.to_string(),
        comment: Some(format!("Catalog-disambiguation twin ({name})")),
        schemas: vec![CatSchema {
            name: SCHEMA_NAME.to_string(),
            comment: Some(format!("Colliding function name served by {name}")),
            tags: Vec::new(),
            views: Vec::new(),
            macros: Vec::new(),
            tables: Vec::new(),
        }],
        ..Default::default()
    }
}

/// The function names the twin catalogs own (scopes their listings, and hides
/// the name from the primary `example` catalog).
fn function_names() -> Vec<String> {
    vec![FUNCTION_NAME.to_string()]
}

/// Declare both twins: one scalar each, scoped to that catalog's `main` schema,
/// plus the secondary catalog that makes them ATTACHable by name.
pub fn register(w: &mut vgi::Worker) {
    for name in [CATALOG_A, CATALOG_B] {
        w.register_scalar_in(name, SCHEMA_NAME, TwinFunction { catalog: name });
        w.register_secondary_catalog(catalog_model(name), function_names());
    }
}

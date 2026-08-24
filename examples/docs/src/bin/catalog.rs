// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The catalog example for the vgi-rust documentation.
//!
//! A worker does not have to be a bag of functions. It can present itself as a
//! database: a named catalog you ATTACH, holding schemas that hold tables and
//! views, queried with ordinary qualified names.
//!
//! The table here is *function-backed*: `CatTable` names a table function plus
//! the arguments to call it with, so `SELECT * FROM cat.data.cities` runs the
//! function with those arguments baked in. The user never passes them, and
//! never sees the function.
//!
//! ```text
//! cargo build --release --bin catalog
//! # then, in a Haybarn shell:
//! ATTACH 'cat' (TYPE vgi, LOCATION './target/release/catalog');
//! SELECT * FROM cat.data.cities;
//! SELECT * FROM cat.data.big_cities;   -- a view over the table
//! ```

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::arguments::Arguments;
use vgi::catalog::{CatSchema, CatTable, CatView, CatalogModel};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
use vgi::vgi_rpc::OutputCollector;
use vgi::{Result, RpcError};

/// The table's shape. Declaring it on the CatTable lets DuckDB describe the
/// table without calling the worker at all.
fn cities_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("population", DataType::Int64, true),
    ]))
}

/// Stands in for whatever the worker actually fronts — a remote API, a file
/// format, a device.
const CITIES: &[(&str, i64)] = &[
    ("Charlottesville", 51_000),
    ("Richmond", 230_000),
    ("Virginia Beach", 457_000),
];

const BATCH_SIZE: usize = 1024;

/// The scan cursor.
///
/// It holds an *index*, not the rows. That matters: a producer is carried
/// between `next_batch` calls and may be serialized for an HTTP continuation,
/// so materializing the whole result here would be paid for repeatedly. Three
/// rows would not notice; three million would.
struct CitiesProducer {
    schema: SchemaRef,
    min_population: i64,
    next: usize,
}

impl TableProducer for CitiesProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        let mut names: Vec<&str> = Vec::new();
        let mut pops: Vec<i64> = Vec::new();

        while self.next < CITIES.len() && names.len() < BATCH_SIZE {
            let (name, pop) = CITIES[self.next];
            self.next += 1;
            if pop >= self.min_population {
                names.push(name);
                pops.push(pop);
            }
        }

        if names.is_empty() {
            return Ok(None);
        }

        let cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(pops)),
        ];
        RecordBatch::try_new(self.schema.clone(), cols)
            .map(Some)
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
    fn resume_supported(&self) -> bool {
        // Multi-batch; a docs example, never served over HTTP. Declared so
        // the decision is visible rather than inherited from a default.
        false
    }
}

/// The scan behind the table. It is an ordinary table function — nothing about
/// it knows it is backing a catalog table.
struct CitiesScan;

impl TableFunction for CitiesScan {
    fn name(&self) -> &str {
        "cities_scan"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Scans the cities table, optionally filtered by population".to_string(),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "min_population",
            0,
            "int64",
            "Only cities at least this large",
        )
        .with_ge(0.0)]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: cities_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(CitiesProducer {
            schema: params.output_schema.clone(),
            min_population: params.arguments.const_i64(0).unwrap_or(0),
            next: 0,
        }))
    }
}

fn main() {
    let mut worker = vgi::Worker::new();
    worker.register_table(CitiesScan);

    let cities = CatTable {
        name: "cities".to_string(),
        columns: cities_schema(),
        // Function-backed: the scan function plus the arguments to call it with.
        scan_function: "cities_scan".to_string(),
        scan_arguments: Arguments::serialize_scan_args(&[Arc::new(Int64Array::from(vec![0i64]))])
            .unwrap_or_default(),
        comment: Some("Every city the worker knows about".to_string()),
        // Column 0 (`name`) is NOT NULL — surfaced in duckdb_columns().
        not_null: vec![0],
        ..Default::default()
    };

    let big_cities = CatView {
        name: "big_cities".to_string(),
        // Pure SQL that DuckDB evaluates — no worker round trip for the view
        // itself, only for the table it reads.
        //
        // `cat` is hardcoded, which would normally be fragile. It is not: the
        // name in ATTACH must match this catalog's name, so `cat` is the only
        // name this view is ever read under.
        definition: "SELECT * FROM cat.data.cities WHERE population >= 100000".to_string(),
        comment: Some("Cities with a population of at least 100,000".to_string()),
        ..Default::default()
    };

    worker.set_catalog(CatalogModel {
        name: "cat".to_string(),
        comment: Some("Documentation example: a worker presented as a database".to_string()),
        schemas: vec![CatSchema {
            name: "data".to_string(),
            tables: vec![cities],
            views: vec![big_cities],
            ..Default::default()
        }],
        ..Default::default()
    });

    worker.run();
}

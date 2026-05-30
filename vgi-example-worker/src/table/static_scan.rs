//! Static catalog scan functions: emit a fixed dataset for the constraint /
//! reference tables (departments, employees, projects, products, colors).

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    let i = |v: Vec<i64>| Arc::new(Int64Array::from(v)) as ArrayRef;
    let s = |v: Vec<&str>| Arc::new(StringArray::from(v)) as ArrayRef;
    let f = |v: Vec<f64>| Arc::new(Float64Array::from(v)) as ArrayRef;

    w.register_table(StaticScan::new(
        "departments_scan",
        &[("id", DataType::Int64), ("name", DataType::Utf8), ("budget", DataType::Float64)],
        vec![i(vec![1, 2, 3]), s(vec!["Engineering", "Sales", "HR"]), f(vec![500000.0, 300000.0, 200000.0])],
    ));
    w.register_table(StaticScan::new(
        "employees_scan",
        &[("id", DataType::Int64), ("name", DataType::Utf8), ("email", DataType::Utf8), ("department_id", DataType::Int64)],
        vec![
            i(vec![1, 2, 3, 4, 5]),
            s(vec!["Alice", "Bob", "Carol", "Dave", "Eve"]),
            s(vec!["alice@co.com", "bob@co.com", "carol@co.com", "dave@co.com", "eve@co.com"]),
            i(vec![1, 1, 2, 2, 3]),
        ],
    ));
    w.register_table(StaticScan::new(
        "projects_scan",
        &[("department_id", DataType::Int64), ("project_code", DataType::Utf8), ("title", DataType::Utf8)],
        vec![i(vec![1, 1, 2]), s(vec!["P001", "P002", "P003"]), s(vec!["Backend API", "Frontend UI", "Sales Portal"])],
    ));
    w.register_table(StaticScan::new(
        "products_scan",
        &[("id", DataType::Int64), ("name", DataType::Utf8), ("quantity", DataType::Int64), ("price", DataType::Float64)],
        vec![i(vec![1, 2, 3]), s(vec!["Widget", "Gadget", "Doohickey"]), i(vec![100, 50, 200]), f(vec![9.99, 24.99, 4.99])],
    ));
    w.register_table(StaticScan::new(
        "colors_scan",
        &[("id", DataType::Int64), ("color", DataType::Utf8), ("hex_code", DataType::Utf8)],
        vec![i(vec![1, 2, 3]), s(vec!["blue", "green", "red"]), s(vec!["#0000FF", "#00FF00", "#FF0000"])],
    ));
}

pub struct StaticScan {
    name: &'static str,
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
}
impl StaticScan {
    fn new(name: &'static str, cols: &[(&str, DataType)], columns: Vec<ArrayRef>) -> Self {
        let schema = Arc::new(Schema::new(
            cols.iter().map(|(n, t)| Field::new(*n, t.clone(), true)).collect::<Vec<_>>(),
        ));
        StaticScan { name, schema, columns }
    }
}

struct OneShot {
    batch: Option<RecordBatch>,
}
impl TableProducer for OneShot {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
}

impl TableFunction for StaticScan {
    fn name(&self) -> &str {
        self.name
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Static catalog scan".to_string(),
            categories: vec!["catalog".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: self.schema.clone(), opaque_data: Vec::new() })
    }
    fn cardinality(&self, _params: &BindParams) -> Option<TableCardinality> {
        let n = self.columns.first().map(|c| c.len() as i64).unwrap_or(0);
        Some(TableCardinality { estimate: Some(n), max: Some(n) })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        // Emit the full natural schema; the dispatch adapter narrows to the
        // (possibly projection-pushed) wire schema in `params.output_schema`.
        let _ = params;
        let batch = RecordBatch::try_new(self.schema.clone(), self.columns.clone())
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Box::new(OneShot { batch: Some(batch) }))
    }
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Static catalog scan functions: emit a fixed dataset for the constraint /
//! reference tables (departments, employees, projects, products, colors).

use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{resume, TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    let i = |v: Vec<i64>| Arc::new(Int64Array::from(v)) as ArrayRef;
    let s = |v: Vec<&str>| Arc::new(StringArray::from(v)) as ArrayRef;
    let f = |v: Vec<f64>| Arc::new(Float64Array::from(v)) as ArrayRef;

    // Note: the time-travel `versioned_data` / `versioned_constraints` tables
    // are backed by the parameterized `versioned_data_scan(version)` /
    // `versioned_constraints_scan(version)` functions (see versioned_scan.rs),
    // not per-version static scans.

    w.register_table(StaticScan::new(
        "departments_scan",
        &[
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("budget", DataType::Float64),
        ],
        vec![
            i(vec![1, 2, 3]),
            s(vec!["Engineering", "Sales", "HR"]),
            f(vec![500000.0, 300000.0, 200000.0]),
        ],
    ));
    w.register_table(StaticScan::new(
        "employees_scan",
        &[
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("email", DataType::Utf8),
            ("department_id", DataType::Int64),
        ],
        vec![
            i(vec![1, 2, 3, 4, 5]),
            s(vec!["Alice", "Bob", "Carol", "Dave", "Eve"]),
            s(vec![
                "alice@co.com",
                "bob@co.com",
                "carol@co.com",
                "dave@co.com",
                "eve@co.com",
            ]),
            i(vec![1, 1, 2, 2, 3]),
        ],
    ));
    w.register_table(StaticScan::new(
        "projects_scan",
        &[
            ("department_id", DataType::Int64),
            ("project_code", DataType::Utf8),
            ("title", DataType::Utf8),
        ],
        vec![
            i(vec![1, 1, 2]),
            s(vec!["P001", "P002", "P003"]),
            s(vec!["Backend API", "Frontend UI", "Sales Portal"]),
        ],
    ));
    w.register_table(StaticScan::new(
        "products_scan",
        &[
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("quantity", DataType::Int64),
            ("price", DataType::Float64),
        ],
        vec![
            i(vec![1, 2, 3]),
            s(vec!["Widget", "Gadget", "Doohickey"]),
            i(vec![100, 50, 200]),
            f(vec![9.99, 24.99, 4.99]),
        ],
    ));
    w.register_table(StaticScan::new(
        "colors_scan",
        &[
            ("id", DataType::Int64),
            ("color", DataType::Utf8),
            ("hex_code", DataType::Utf8),
        ],
        vec![
            i(vec![1, 2, 3]),
            s(vec!["blue", "green", "red"]),
            s(vec!["#0000FF", "#00FF00", "#FF0000"]),
        ],
    ));
    w.register_table(RowIdSequenceFunction);
}

/// Per-table scan helpers for the `versioned_tables` catalog only.
pub fn register_versioned_tables(w: &mut vgi::Worker) {
    let i = |v: Vec<i64>| Arc::new(Int64Array::from(v)) as ArrayRef;
    let s = |v: Vec<&str>| Arc::new(StringArray::from(v)) as ArrayRef;
    let f = |v: Vec<f64>| Arc::new(Float64Array::from(v)) as ArrayRef;

    w.register_table(StaticScan::new(
        "versioned_tables_animals_scan",
        &[
            ("name", DataType::Utf8),
            ("legs", DataType::Int64),
            ("sound", DataType::Utf8),
        ],
        vec![
            s(vec!["chicken", "cow", "horse", "pig", "sheep"]),
            i(vec![2, 4, 4, 4, 4]),
            s(vec!["cluck", "moo", "neigh", "oink", "baa"]),
        ],
    ));
    w.register_table(StaticScan::new(
        "versioned_tables_animals_color_scan",
        &[
            ("name", DataType::Utf8),
            ("legs", DataType::Int64),
            ("sound", DataType::Utf8),
            ("color", DataType::Utf8),
        ],
        vec![
            s(vec!["chicken", "cow", "horse", "pig", "sheep"]),
            i(vec![2, 4, 4, 4, 4]),
            s(vec!["cluck", "moo", "neigh", "oink", "baa"]),
            s(vec!["red", "brown", "black", "pink", "white"]),
        ],
    ));
    w.register_table(StaticScan::new(
        "versioned_tables_plants_scan",
        &[
            ("name", DataType::Utf8),
            ("kind", DataType::Utf8),
            ("height_m", DataType::Float64),
        ],
        vec![
            s(vec!["oak", "pine", "rose", "tomato", "wheat"]),
            s(vec!["tree", "tree", "flower", "vegetable", "grass"]),
            f(vec![20.0, 25.0, 0.6, 1.5, 1.0]),
        ],
    ));
}

// ---------------------------------------------------------------------------
// rowid_sequence(count, layout:=first|middle|last, row_id_type:=int64|string|struct)
// ---------------------------------------------------------------------------

use arrow_array::builder::{Int64Builder, StringBuilder, StructBuilder};

pub struct RowIdSequenceFunction;

impl RowIdSequenceFunction {
    fn build_schema(layout: &str, row_id_type: &str) -> SchemaRef {
        let rid_ty = match row_id_type {
            "string" => DataType::Utf8,
            "struct" => DataType::Struct(
                vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into(),
            ),
            _ => DataType::Int64,
        };
        let rid = Field::new("row_id", rid_ty, true).with_metadata(
            std::collections::HashMap::from([("is_row_id".to_string(), String::new())]),
        );
        let name = Field::new("name", DataType::Utf8, true);
        let value = Field::new("value", DataType::Utf8, true);
        let fields = match layout {
            "middle" => vec![name, rid, value],
            "last" => vec![name, value, rid],
            _ => vec![rid, name, value],
        };
        Arc::new(Schema::new(fields))
    }
}

struct RowIdProducer {
    schema: SchemaRef,
    count: i64,
    row_id_type: String,
    emitted: bool,
}
impl TableProducer for RowIdProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let n = self.count.max(0);
        let rid: ArrayRef = match self.row_id_type.as_str() {
            "string" => Arc::new(StringArray::from(
                (0..n).map(|i| format!("rid_{i}")).collect::<Vec<_>>(),
            )),
            "struct" => {
                let mut b = StructBuilder::from_fields(
                    vec![
                        Field::new("a", DataType::Int64, true),
                        Field::new("b", DataType::Utf8, true),
                    ],
                    n as usize,
                );
                for i in 0..n {
                    b.field_builder::<Int64Builder>(0).unwrap().append_value(i);
                    b.field_builder::<StringBuilder>(1)
                        .unwrap()
                        .append_value(format!("s_{i}"));
                    b.append(true);
                }
                Arc::new(b.finish())
            }
            _ => Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())),
        };
        let name: ArrayRef = Arc::new(StringArray::from(
            (0..n).map(|i| format!("item_{i}")).collect::<Vec<_>>(),
        ));
        let value: ArrayRef = Arc::new(StringArray::from(
            (0..n).map(|i| format!("val_{i}")).collect::<Vec<_>>(),
        ));
        // Assemble columns in the schema's field order (by name).
        let cols: Vec<ArrayRef> = self
            .schema
            .fields()
            .iter()
            .map(|f| match f.name().as_str() {
                "row_id" => rid.clone(),
                "name" => name.clone(),
                _ => value.clone(),
            })
            .collect();
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), cols)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[if self.emitted { 1 } else { 0 }])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.emitted = v[0] != 0;
        }
    }
}

impl TableFunction for RowIdSequenceFunction {
    fn name(&self) -> &str {
        "rowid_sequence"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Sequence with row_id column".to_string(),
            categories: vec!["catalog".into()],
            projection_pushdown: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows"),
            ArgSpec::const_arg("layout", -1, "varchar", "Row ID column position"),
            ArgSpec::const_arg("row_id_type", -1, "varchar", "Row ID type"),
        ]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let layout = params
            .arguments
            .named_str("layout")
            .unwrap_or_else(|| "first".to_string());
        let row_id_type = params
            .arguments
            .named_str("row_id_type")
            .unwrap_or_else(|| "int64".to_string());
        if !["first", "middle", "last"].contains(&layout.as_str()) {
            return Err(RpcError::value_error(format!(
                "rowid_sequence: layout {layout:?} must be one of the allowed choices: first, middle, last"
            )));
        }
        if !["int64", "string", "struct"].contains(&row_id_type.as_str()) {
            return Err(RpcError::value_error(format!(
                "rowid_sequence: row_id_type {row_id_type:?} must be one of the allowed choices: int64, string, struct"
            )));
        }
        Ok(BindResponse {
            output_schema: Self::build_schema(&layout, &row_id_type),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let count = params.arguments.const_i64(0)?;
        Some(TableCardinality {
            estimate: Some(count),
            max: Some(count),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let layout = params
            .arguments
            .named_str("layout")
            .unwrap_or_else(|| "first".to_string());
        let row_id_type = params
            .arguments
            .named_str("row_id_type")
            .unwrap_or_else(|| "int64".to_string());
        Ok(Box::new(RowIdProducer {
            schema: Self::build_schema(&layout, &row_id_type),
            count: params.arguments.const_i64(0).unwrap_or(0),
            row_id_type,
            emitted: false,
        }))
    }
}

pub struct StaticScan {
    name: &'static str,
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
}
impl StaticScan {
    fn new(name: &'static str, cols: &[(&str, DataType)], columns: Vec<ArrayRef>) -> Self {
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|(n, t)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        ));
        StaticScan {
            name,
            schema,
            columns,
        }
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
        Ok(BindResponse {
            output_schema: self.schema.clone(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, _params: &BindParams) -> Option<TableCardinality> {
        let n = self.columns.first().map(|c| c.len() as i64).unwrap_or(0);
        Some(TableCardinality {
            estimate: Some(n),
            max: Some(n),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        // Emit the full natural schema; the dispatch adapter narrows to the
        // (possibly projection-pushed) wire schema in `params.output_schema`.
        let _ = params;
        let batch = RecordBatch::try_new(self.schema.clone(), self.columns.clone())
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Box::new(OneShot {
            batch: Some(batch),
            done: false,
        }))
    }
}

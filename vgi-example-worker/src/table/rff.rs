//! Scan functions backing the `rff_*` catalog tables that exercise
//! `Table.required_field_filter_paths` (the C++ optimizer enforces the
//! WHERE-filter requirement; the worker just serves the metadata + rows).

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(RffScan::simple());
    w.register_table(RffScan::struct_());
    w.register_table(RffScan::nested());
    w.register_table(RffScan::multi());
    w.register_table(RffScan::none());
}

fn i64a(v: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(v)) as ArrayRef
}

/// `s: struct{a,b}` with the given per-row (a,b) values.
fn struct_ab(a: Vec<i64>, b: Vec<i64>) -> (Field, ArrayRef) {
    let fields: Fields = vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]
    .into();
    let arr = StructArray::new(fields.clone(), vec![i64a(a), i64a(b)], None);
    (Field::new("s", DataType::Struct(fields), true), Arc::new(arr))
}

pub struct RffScan {
    name: &'static str,
    description: &'static str,
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
}

impl RffScan {
    fn flat(name: &'static str, description: &'static str) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        RffScan { name, description, schema, columns: vec![i64a(vec![1, 2, 3]), i64a(vec![10, 20, 30])] }
    }
    fn simple() -> Self {
        Self::flat("rff_simple_scan", "rff_simple — flat columns (a, b)")
    }
    fn none() -> Self {
        Self::flat("rff_none_scan", "rff_none — control table with no required_field_filter_paths")
    }
    fn struct_() -> Self {
        let (sf, sa) = struct_ab(vec![1, 2, 3], vec![10, 20, 30]);
        let schema = Arc::new(Schema::new(vec![sf, Field::new("other", DataType::Int64, true)]));
        RffScan {
            name: "rff_struct_scan",
            description: "rff_struct — STRUCT(s.a, s.b) + other",
            schema,
            columns: vec![sa, i64a(vec![100, 200, 300])],
        }
    }
    fn nested() -> Self {
        // wrapper: struct{ mid: struct{ leaf: int64 } }
        let leaf: Fields = vec![Field::new("leaf", DataType::Int64, true)].into();
        let mid_arr = StructArray::new(leaf.clone(), vec![i64a(vec![1, 2, 3])], None);
        let mid: Fields = vec![Field::new("mid", DataType::Struct(leaf), true)].into();
        let wrapper_arr = StructArray::new(mid.clone(), vec![Arc::new(mid_arr) as ArrayRef], None);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wrapper",
            DataType::Struct(mid),
            true,
        )]));
        RffScan {
            name: "rff_nested_scan",
            description: "rff_nested — nested STRUCT(wrapper.mid.leaf)",
            schema,
            columns: vec![Arc::new(wrapper_arr) as ArrayRef],
        }
    }
    fn multi() -> Self {
        let (sf, sa) = struct_ab(vec![1, 2], vec![10, 20]);
        let schema = Arc::new(Schema::new(vec![sf, Field::new("top", DataType::Int64, true)]));
        RffScan {
            name: "rff_multi_scan",
            description: "rff_multi — top-level + struct subfield required paths",
            schema,
            columns: vec![sa, i64a(vec![100, 200])],
        }
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

impl TableFunction for RffScan {
    fn name(&self) -> &str {
        self.name
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: self.description.to_string(),
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
    fn producer(&self, _params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let batch = RecordBatch::try_new(self.schema.clone(), self.columns.clone())
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Box::new(OneShot { batch: Some(batch) }))
    }
}

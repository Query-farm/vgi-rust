// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Parameterized time-travel scans: one `versioned_data_scan(version)` /
//! `versioned_constraints_scan(version)` returning the version-specific schema +
//! rows (matches the canonical single-function shape). Used via the legacy
//! (non-inline) scan path so each query re-resolves the version.

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{resume, TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(VersionedDataScan);
    w.register_table(VersionedConstraintsScan);
}

fn i(v: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(v))
}
fn s(v: Vec<&str>) -> ArrayRef {
    Arc::new(StringArray::from(v))
}
fn f(v: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(v))
}
fn b(v: Vec<bool>) -> ArrayRef {
    Arc::new(BooleanArray::from(v))
}
fn fld(n: &str, t: DataType) -> Field {
    Field::new(n, t, true)
}

pub struct VersionedDataScan;
impl VersionedDataScan {
    fn build(version: i64) -> (SchemaRef, Vec<ArrayRef>) {
        use DataType::{Boolean, Float64, Int64, Utf8};
        match version {
            1 => (
                Arc::new(Schema::new(vec![fld("id", Int64)])),
                vec![i(vec![1, 2, 3])],
            ),
            2 => (
                Arc::new(Schema::new(vec![
                    fld("id", Int64),
                    fld("name", Utf8),
                    fld("score", Float64),
                    fld("active", Boolean),
                ])),
                vec![
                    i(vec![1, 2, 3, 4, 5]),
                    s(vec!["alice", "bob", "carol", "dave", "eve"]),
                    f(vec![10.0, 20.0, 30.0, 40.0, 50.0]),
                    b(vec![true, false, true, false, true]),
                ],
            ),
            _ => (
                Arc::new(Schema::new(vec![fld("id", Int64), fld("score", Float64)])),
                vec![i(vec![1, 2, 3, 4]), f(vec![15.0, 25.0, 35.0, 45.0])],
            ),
        }
    }
}
impl TableFunction for VersionedDataScan {
    fn name(&self) -> &str {
        "versioned_data_scan"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Versioned data scan (time travel)".to_string(),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "version",
            0,
            "int64",
            "Data version to return",
        )]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let v = params.arguments.const_i64(0).unwrap_or(3);
        Ok(BindResponse {
            output_schema: Self::build(v).0,
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let v = params.arguments.const_i64(0).unwrap_or(3);
        let n = Self::build(v)
            .1
            .first()
            .map(|a| a.len() as i64)
            .unwrap_or(0);
        Some(TableCardinality {
            estimate: Some(n),
            max: Some(n),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let v = params.arguments.const_i64(0).unwrap_or(3);
        let (schema, cols) = Self::build(v);
        Ok(Box::new(OneShot {
            batch: Some(RecordBatch::try_new(schema, cols).map_err(cvt)?),
            done: false,
        }))
    }
}

pub struct VersionedConstraintsScan;
impl VersionedConstraintsScan {
    fn build(version: i64) -> (SchemaRef, Vec<ArrayRef>) {
        use DataType::{Int64, Utf8};
        match version {
            1 => (
                Arc::new(Schema::new(vec![fld("id", Int64), fld("name", Utf8)])),
                vec![i(vec![1, 2]), s(vec!["Alice", "Bob"])],
            ),
            2 => (
                Arc::new(Schema::new(vec![
                    fld("id", Int64),
                    fld("name", Utf8),
                    fld("email", Utf8),
                ])),
                vec![
                    i(vec![1, 2, 3]),
                    s(vec!["Alice", "Bob", "Carol"]),
                    s(vec!["a@co", "b@co", "c@co"]),
                ],
            ),
            _ => (
                Arc::new(Schema::new(vec![
                    fld("id", Int64),
                    fld("name", Utf8),
                    fld("email", Utf8),
                    fld("department_id", Int64),
                ])),
                vec![
                    i(vec![1, 2, 3]),
                    s(vec!["Alice", "Bob", "Carol"]),
                    s(vec!["a@co", "b@co", "c@co"]),
                    i(vec![1, 2, 1]),
                ],
            ),
        }
    }
}
impl TableFunction for VersionedConstraintsScan {
    fn name(&self) -> &str {
        "versioned_constraints_scan"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Versioned constraints scan (time travel)".to_string(),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "version",
            0,
            "int64",
            "Data version to return",
        )]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let v = params.arguments.const_i64(0).unwrap_or(3);
        Ok(BindResponse {
            output_schema: Self::build(v).0,
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let v = params.arguments.const_i64(0).unwrap_or(3);
        let n = Self::build(v)
            .1
            .first()
            .map(|a| a.len() as i64)
            .unwrap_or(0);
        Some(TableCardinality {
            estimate: Some(n),
            max: Some(n),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let v = params.arguments.const_i64(0).unwrap_or(3);
        let (schema, cols) = Self::build(v);
        Ok(Box::new(OneShot {
            batch: Some(RecordBatch::try_new(schema, cols).map_err(cvt)?),
            done: false,
        }))
    }
}

fn cvt(e: impl std::fmt::Display) -> RpcError {
    RpcError::runtime_error(e.to_string())
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

// Copyright 2025, 2026 Query Farm LLC - https://query.farm
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use vgi::{
    function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams},
    projection::{ProjectionAcceptance, ProjectionProposal, PROFILE},
    table_function::{BindRepeatability, TableFunction, TableProducer},
};
use vgi_rpc::{OutputCollector, Result, RpcError};
pub struct ProjectionSequence;
impl TableFunction for ProjectionSequence {
    fn name(&self) -> &str {
        "projection_sequence"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            projection_pushdown: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, true)])),
            opaque_data: Vec::new(),
        })
    }
    fn projection_expression_profiles(&self, _: &BindParams) -> Vec<String> {
        vec![PROFILE.into()]
    }
    fn repeatability(&self, _: &BindParams) -> BindRepeatability {
        BindRepeatability::StableWithinQuery
    }
    fn projection(
        &self,
        _: &BindParams,
        p: &ProjectionProposal,
    ) -> Result<Option<ProjectionAcceptance>> {
        let mut indices = Vec::new();
        let mut fields = p.source_schema.fields().to_vec();
        for (i, e) in p.expressions.iter().enumerate() {
            let c = &e.evaluation_context;
            if e.sql == "CAST(\"n\" AS BIGINT)"
                && c.timezone == "UTC"
                && c.collation == "binary"
                && c.default_order == "asc"
                && c.default_null_order == "nulls_last"
                && c.preserve_insertion_order
                && p.result_schema.field(i).data_type() == &DataType::Int64
            {
                indices.push(i as i64);
                fields.push(p.result_schema.fields()[i].clone());
            }
        }
        if indices.is_empty() {
            return Ok(None);
        }
        let opaque_data = indices.len().to_string().into_bytes();
        Ok(Some(ProjectionAcceptance {
            accepted_indices: indices,
            bind_result: BindResponse {
                output_schema: Arc::new(Schema::new_with_metadata(
                    fields,
                    p.source_schema.metadata().clone(),
                )),
                opaque_data,
            },
        }))
    }
    fn producer(&self, p: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        if p.output_schema.fields().iter().any(|f| f.name() != "n") && p.bind_opaque_data.is_empty()
        {
            return Err(RpcError::value_error("projection state missing"));
        }
        Ok(Box::new(Rows {
            schema: p.output_schema.clone(),
            done: false,
        }))
    }
}
struct Rows {
    schema: Arc<Schema>,
    done: bool,
}
impl TableProducer for Rows {
    fn next_batch(&mut self, _: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        let columns = self
            .schema
            .fields()
            .iter()
            .map(|f| {
                if f.name() == "n" {
                    Arc::new(Int32Array::from_iter_values(0..10)) as ArrayRef
                } else {
                    Arc::new(Int64Array::from_iter_values(0..10)) as ArrayRef
                }
            })
            .collect();
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), columns)
                .map_err(|e| RpcError::value_error(e.to_string()))?,
        ))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        vec![self.done as u8]
    }
    fn restore_resume(&mut self, b: &[u8]) {
        self.done = b.first() == Some(&1);
    }
}

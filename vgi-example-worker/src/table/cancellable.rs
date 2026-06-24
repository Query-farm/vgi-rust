// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `slow_cancellable(probe_path, sleep_ms := 50, count := 1_000_000)` — a slow
//! single-worker producer that emits `{n: int64}` one row per tick after a short
//! sleep. Used by the client-side cancellation smoke (not the integration
//! suite); registered here so it surfaces in `duckdb_functions()`.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{resume, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(SlowCancellableFunction);
}

fn schema_n() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
}

pub struct SlowCancellableFunction;
impl TableFunction for SlowCancellableFunction {
    fn name(&self) -> &str {
        "slow_cancellable"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Slow producer with an on_cancel file-writing probe (test fixture)"
                .to_string(),
            categories: vec!["test".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg(
                "probe_path",
                0,
                "varchar",
                "Path to append to when on_cancel fires",
            ),
            ArgSpec::const_arg("sleep_ms", -1, "int64", "Sleep per batch (ms)"),
            ArgSpec::const_arg("count", -1, "int64", "Total rows to produce"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema_n(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let count = params
            .arguments
            .named_i64("count")
            .unwrap_or(1_000_000)
            .max(0);
        let sleep_ms = params.arguments.named_i64("sleep_ms").unwrap_or(50).max(0) as u64;
        Ok(Box::new(SlowProducer {
            remaining: count,
            sleep_ms,
            next: 0,
        }))
    }
}

struct SlowProducer {
    remaining: i64,
    sleep_ms: u64,
    next: i64,
}
impl TableProducer for SlowProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        if self.sleep_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.sleep_ms));
        }
        let batch = RecordBatch::try_new(
            schema_n(),
            vec![Arc::new(Int64Array::from(vec![self.next]))],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.next += 1;
        self.remaining -= 1;
        Ok(Some(batch))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.next, self.remaining])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 2) {
            self.next = v[0];
            self.remaining = v[1];
        }
    }
}

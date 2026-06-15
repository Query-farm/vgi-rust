// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Queue-driven parallel sequence fixtures: `partitioned_sequence` and the
//! three `partitioned_*` order-preservation-mode variants. All push (start,
//! end) work items at `on_init` and emit `n = idx * increment` in 1k batches.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::buffering::BufferingStore;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

static CLAIM_COUNTER: AtomicU64 = AtomicU64::new(0);
const BATCH_SIZE: i64 = 1000;

pub fn register(w: &mut vgi::Worker) {
    w.register_table(QueueSeqFunction {
        name: "partitioned_sequence",
        order: None,
        max_partitions: Some(24),
        has_increment: true,
    });
    w.register_table(QueueSeqFunction {
        name: "partitioned_preserves_order",
        order: Some(vgi::protocol::enums::order_preservation::PRESERVES_ORDER),
        max_partitions: None,
        has_increment: false,
    });
    w.register_table(QueueSeqFunction {
        name: "partitioned_no_order_guarantee",
        order: Some(vgi::protocol::enums::order_preservation::NO_ORDER_GUARANTEE),
        max_partitions: None,
        has_increment: false,
    });
    w.register_table(QueueSeqFunction {
        name: "partitioned_fixed_order",
        order: Some(vgi::protocol::enums::order_preservation::FIXED_ORDER),
        max_partitions: None,
        has_increment: false,
    });
}

fn schema_n() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
}

struct QueueSeqProducer {
    schema: SchemaRef,
    storage: Arc<BufferingStore>,
    execution_id: Vec<u8>,
    claim_tag: String,
    increment: i64,
    cur: Option<(i64, i64)>, // (idx, end)
}
impl TableProducer for QueueSeqProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        loop {
            if self.cur.is_none() {
                match self.storage.queue_pop(&self.execution_id, &self.claim_tag) {
                    None => return Ok(None),
                    Some(data) => {
                        let g = |o: usize| {
                            let mut a = [0u8; 8];
                            a.copy_from_slice(&data[o..o + 8]);
                            i64::from_le_bytes(a)
                        };
                        self.cur = Some((g(0), g(8)));
                    }
                }
            }
            let (idx, end) = self.cur.unwrap();
            if idx >= end {
                self.cur = None;
                continue;
            }
            let bend = (idx + BATCH_SIZE).min(end);
            let vals: Vec<i64> = (idx..bend).map(|i| i * self.increment).collect();
            let batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int64Array::from(vals)) as ArrayRef],
            )
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
            self.cur = Some((bend, end));
            return Ok(Some(batch));
        }
    }
    /// Encode the partial-chunk cursor `(idx, end)` so an HTTP continuation can
    /// resume mid-chunk — the chunk was destructively popped from the queue, so
    /// its remaining rows live only in `cur`. Empty when between chunks.
    fn encode_resume(&self) -> Vec<u8> {
        match self.cur {
            Some((idx, end)) if idx < end => {
                let mut v = Vec::with_capacity(16);
                v.extend_from_slice(&idx.to_le_bytes());
                v.extend_from_slice(&end.to_le_bytes());
                v
            }
            _ => Vec::new(),
        }
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if bytes.len() == 16 {
            let g = |o: usize| i64::from_le_bytes(bytes[o..o + 8].try_into().unwrap());
            self.cur = Some((g(0), g(8)));
        }
    }
}

pub struct QueueSeqFunction {
    name: &'static str,
    order: Option<&'static str>,
    max_partitions: Option<i64>,
    has_increment: bool,
}

impl TableFunction for QueueSeqFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Generates a partitioned sequence for multi-worker execution".to_string(),
            categories: vec!["generator".into(), "utility".into()],
            order_preservation: self.order.map(|s| s.to_string()),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        let mut v = vec![ArgSpec::const_arg(
            "count",
            0,
            "int64",
            "Total integers to generate",
        )];
        if self.has_increment {
            v.push(ArgSpec::const_arg(
                "increment",
                -1,
                "int64",
                "Step between values",
            ));
        }
        v
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema_n(),
            opaque_data: Vec::new(),
        })
    }
    fn max_workers(&self, _params: &BindParams) -> i64 {
        4
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let count = params.arguments.const_i64(0)?;
        let inc = if self.has_increment {
            params.arguments.named_i64("increment").unwrap_or(1)
        } else {
            1
        };
        let _ = inc;
        Some(TableCardinality {
            estimate: Some(count),
            max: Some(count),
        })
    }
    fn on_init(&self, params: &ProcessParams) -> Result<()> {
        let store = params
            .storage
            .as_ref()
            .ok_or_else(|| RpcError::runtime_error("requires storage"))?;
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let chunk = match self.max_partitions {
            Some(mp) => ((count + mp - 1) / mp).max(1),
            None => 1000,
        };
        let mut items = Vec::new();
        let mut start = 0i64;
        while start < count {
            let end = (start + chunk).min(count);
            let mut b = start.to_le_bytes().to_vec();
            b.extend_from_slice(&end.to_le_bytes());
            items.push(b);
            start = end;
        }
        store.queue_push(&params.execution_id, &items);
        Ok(())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let storage = params
            .storage
            .clone()
            .ok_or_else(|| RpcError::runtime_error("requires storage"))?;
        let increment = if self.has_increment {
            params.arguments.named_i64("increment").unwrap_or(1)
        } else {
            1
        };
        let tag = format!(
            "{}_{}",
            std::process::id(),
            CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        Ok(Box::new(QueueSeqProducer {
            schema: schema_n(),
            storage,
            execution_id: params.execution_id.clone(),
            claim_tag: tag,
            increment,
            cur: None,
        }))
    }
}

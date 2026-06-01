//! `supports_batch_index` table fixtures: a primary worker pushes per-chunk
//! work items `(partition_id, start, end)` onto a shared queue at `on_init`;
//! parallel workers pop items and emit batches tagged with `vgi_batch_index`
//! so DuckDB's ordered sinks reassemble parallel output in partition order.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::buffering::BufferingStore;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(PartitionedBatchIndexFunction);
    w.register_table(PartitionedBatchIndexMarkedFunction);
    w.register_table(BrokenBatchIndexFunction { name: "broken_missing_batch_index_tag", mode: BrokenMode::Missing });
    w.register_table(BrokenBatchIndexFunction { name: "broken_non_monotone_batch_index", mode: BrokenMode::NonMonotone });
    w.register_table(BrokenBatchIndexFunction { name: "broken_batch_index_overflow", mode: BrokenMode::Overflow });
}

// ---------------------------------------------------------------------------
// Deliberately-broken fixtures: exercise the C++ contract checks.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum BrokenMode {
    Missing,     // emit with no vgi_batch_index metadata
    NonMonotone, // emit tag=10 then tag=3
    Overflow,    // emit tag=2^60
}

struct BrokenProducer {
    schema: SchemaRef,
    count: i64,
    mode: BrokenMode,
    step: u32,
}
impl TableProducer for BrokenProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        let mk = |vals: Vec<i64>, schema: &SchemaRef| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals)) as ArrayRef])
                .map_err(|e| RpcError::runtime_error(e.to_string()))
        };
        match (self.mode, self.step) {
            (BrokenMode::NonMonotone, 0) => {
                self.step = 1;
                Ok(Some(mk((0..self.count).collect(), &self.schema)?))
            }
            (BrokenMode::NonMonotone, 1) => {
                self.step = 2;
                Ok(Some(mk(vec![42], &self.schema)?))
            }
            (_, 0) => {
                self.step = 1;
                Ok(Some(mk((0..self.count).collect(), &self.schema)?))
            }
            _ => Ok(None),
        }
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        match self.mode {
            BrokenMode::Missing => None,
            // step has already advanced past the emitted batch.
            BrokenMode::NonMonotone => Some(HashMap::from([(
                BATCH_TAG.to_string(),
                if self.step <= 1 { "10" } else { "3" }.to_string(),
            )])),
            BrokenMode::Overflow => {
                Some(HashMap::from([(BATCH_TAG.to_string(), (1i64 << 60).to_string())]))
            }
        }
    }
}

pub struct BrokenBatchIndexFunction {
    name: &'static str,
    mode: BrokenMode,
}
impl TableFunction for BrokenBatchIndexFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Deliberately-broken batch_index fixture".to_string(),
            categories: vec!["testing".into(), "broken".into()],
            supports_batch_index: true,
            order_preservation: Some(
                vgi::protocol::enums::order_preservation::FIXED_ORDER.to_string(),
            ),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "Rows to generate")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: schema_n(), opaque_data: Vec::new() })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let count = params.arguments.const_i64(0)?;
        Some(TableCardinality { estimate: Some(count), max: Some(count) })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(BrokenProducer {
            schema: schema_n(),
            count: params.arguments.const_i64(0).unwrap_or(1).max(0),
            mode: self.mode,
            step: 0,
        }))
    }
}

const BATCH_TAG: &str = "vgi_batch_index";
const BATCH_SIZE: i64 = 1000;
static CLAIM_COUNTER: AtomicU64 = AtomicU64::new(0);

fn batch_index_meta(desc: &str, projection: bool) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        categories: vec!["generator".into(), "utility".into()],
        supports_batch_index: true,
        order_preservation: Some(vgi::protocol::enums::order_preservation::FIXED_ORDER.to_string()),
        projection_pushdown: projection,
        ..Default::default()
    }
}

fn pack_item(partition_id: i64, start: i64, end: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(24);
    v.extend_from_slice(&partition_id.to_le_bytes());
    v.extend_from_slice(&start.to_le_bytes());
    v.extend_from_slice(&end.to_le_bytes());
    v
}
fn unpack_item(b: &[u8]) -> (i64, i64, i64) {
    let g = |o: usize| {
        let mut a = [0u8; 8];
        a.copy_from_slice(&b[o..o + 8]);
        i64::from_le_bytes(a)
    };
    (g(0), g(8), g(16))
}

/// Push `count`-into-`chunk_size` work items onto the execution queue.
fn push_work(params: &ProcessParams, count: i64, chunk_size: i64) -> Result<()> {
    let store = params
        .storage
        .as_ref()
        .ok_or_else(|| RpcError::runtime_error("batch_index requires storage"))?;
    let mut items = Vec::new();
    let mut pid = 0i64;
    let mut start = 0i64;
    while start < count {
        let end = (start + chunk_size).min(count);
        items.push(pack_item(pid, start, end));
        pid += 1;
        start = end;
    }
    store.queue_push(&params.execution_id, &items);
    Ok(())
}

/// Producer that drains the shared work queue, one item's batches at a time.
struct QueueProducer {
    schema: SchemaRef,
    storage: Arc<BufferingStore>,
    execution_id: Vec<u8>,
    claim_tag: String,
    marked: bool,
    // current item: (partition_id, idx, end, start)
    cur: Option<(i64, i64, i64, i64)>,
    partition_id: i64,
}
impl QueueProducer {
    fn build_batch(&self, pid: i64, start: i64, idx: i64, end: i64) -> Result<RecordBatch> {
        if self.marked {
            let rows = (end - idx) as usize;
            let pids: Vec<i64> = vec![pid; rows];
            let seqs: Vec<i64> = (idx - start..end - start).collect();
            RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(Int64Array::from(pids)) as ArrayRef,
                    Arc::new(Int64Array::from(seqs)) as ArrayRef,
                ],
            )
        } else {
            let ns: Vec<i64> = (idx..end).collect();
            RecordBatch::try_new(self.schema.clone(), vec![Arc::new(Int64Array::from(ns)) as ArrayRef])
        }
        .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}
impl TableProducer for QueueProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        loop {
            if self.cur.is_none() {
                match self.storage.queue_pop(&self.execution_id, &self.claim_tag) {
                    None => return Ok(None),
                    Some(data) => {
                        let (pid, start, end) = unpack_item(&data);
                        self.cur = Some((pid, start, end, start));
                        self.partition_id = pid;
                    }
                }
            }
            let (pid, idx, end, start) = self.cur.unwrap();
            if idx >= end {
                self.cur = None;
                continue;
            }
            let bend = (idx + BATCH_SIZE).min(end);
            let batch = self.build_batch(pid, start, idx, bend)?;
            self.cur = Some((pid, bend, end, start));
            self.partition_id = pid;
            return Ok(Some(batch));
        }
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        Some(HashMap::from([(BATCH_TAG.to_string(), self.partition_id.to_string())]))
    }
    /// Encode the partial-chunk cursor `(pid, idx, end, start)` for HTTP
    /// continuation — the chunk was destructively popped, so a mid-chunk yield
    /// must carry it or those rows are lost. Empty when between chunks.
    fn encode_resume(&self) -> Vec<u8> {
        match self.cur {
            Some((pid, idx, end, start)) if idx < end => {
                let mut v = Vec::with_capacity(32);
                for x in [pid, idx, end, start] {
                    v.extend_from_slice(&x.to_le_bytes());
                }
                v
            }
            _ => Vec::new(),
        }
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if bytes.len() == 32 {
            let g = |o: usize| i64::from_le_bytes(bytes[o..o + 8].try_into().unwrap());
            self.cur = Some((g(0), g(8), g(16), g(24)));
            self.partition_id = g(0);
        }
    }
}

fn make_producer(params: &ProcessParams, schema: SchemaRef, marked: bool) -> Result<Box<dyn TableProducer>> {
    let storage = params
        .storage
        .clone()
        .ok_or_else(|| RpcError::runtime_error("batch_index requires storage"))?;
    let tag = format!("{}_{}", std::process::id(), CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed));
    Ok(Box::new(QueueProducer {
        schema,
        storage,
        execution_id: params.execution_id.clone(),
        claim_tag: tag,
        marked,
        cur: None,
        partition_id: 0,
    }))
}

// ---------------------------------------------------------------------------
// partitioned_batch_index(count) -> {n}
// ---------------------------------------------------------------------------

fn schema_n() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
}

pub struct PartitionedBatchIndexFunction;
impl TableFunction for PartitionedBatchIndexFunction {
    fn name(&self) -> &str {
        "partitioned_batch_index"
    }
    fn metadata(&self) -> FunctionMetadata {
        batch_index_meta("Multi-worker partitioned sequence with per-batch batch_index tagging", true)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "Total integers to generate")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: schema_n(), opaque_data: Vec::new() })
    }
    fn max_workers(&self, _params: &BindParams) -> i64 {
        4
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let count = params.arguments.const_i64(0)?;
        Some(TableCardinality { estimate: Some(count), max: Some(count) })
    }
    fn on_init(&self, params: &ProcessParams) -> Result<()> {
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        push_work(params, count, 1000)
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        make_producer(params, schema_n(), false)
    }
}

// ---------------------------------------------------------------------------
// partitioned_batch_index_marked(count, chunk_size) -> {partition_id, seq}
// ---------------------------------------------------------------------------

fn marked_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("partition_id", DataType::Int64, true),
        Field::new("seq", DataType::Int64, true),
    ]))
}

pub struct PartitionedBatchIndexMarkedFunction;
impl TableFunction for PartitionedBatchIndexMarkedFunction {
    fn name(&self) -> &str {
        "partitioned_batch_index_marked"
    }
    fn metadata(&self) -> FunctionMetadata {
        // projection_pushdown OFF so the partition_id column survives.
        batch_index_meta("Two-column batch_index demo: rows are (partition_id, seq)", false)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Total rows to generate"),
            ArgSpec::const_arg("chunk_size", -1, "int64", "Rows per partition"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: marked_schema(), opaque_data: Vec::new() })
    }
    fn max_workers(&self, _params: &BindParams) -> i64 {
        4
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let count = params.arguments.const_i64(0)?;
        Some(TableCardinality { estimate: Some(count), max: Some(count) })
    }
    fn on_init(&self, params: &ProcessParams) -> Result<()> {
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let chunk_size = params.arguments.named_i64("chunk_size").unwrap_or(1000).max(1);
        push_work(params, count, chunk_size)
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        make_producer(params, marked_schema(), true)
    }
}

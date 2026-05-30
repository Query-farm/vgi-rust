//! Partition-column table fixtures: queue-driven, one partition per emitted
//! batch, each tagged with `vgi_partition_values#b64` so DuckDB can plan
//! partitioned aggregates.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::buffering::BufferingStore;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::partition::partition_field;
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

static CLAIM_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn register(w: &mut vgi::Worker) {
    w.register_table(PartitionFunction::CountrySales);
    w.register_table(PartitionFunction::RegionYear);
    w.register_table(PartitionFunction::Override);
    w.register_table(PartitionFunction::Disjoint);
}

const COUNTRIES: [&str; 5] = ["AU", "BR", "CA", "FR", "US"];
const REGIONS_YEARS: [(&str, i64); 6] = [
    ("AMER", 2023),
    ("AMER", 2024),
    ("EMEA", 2023),
    ("EMEA", 2024),
    ("APAC", 2023),
    ("APAC", 2024),
];
const CATEGORIES: [&str; 3] = ["books", "music", "video"];

#[derive(Clone, Copy)]
pub enum PartitionFunction {
    CountrySales,
    RegionYear,
    Override,
    Disjoint,
}

impl PartitionFunction {
    fn schema(&self) -> SchemaRef {
        match self {
            PartitionFunction::CountrySales => Arc::new(Schema::new(vec![
                partition_field("country", DataType::Utf8),
                Field::new("sales", DataType::Int64, true),
            ])),
            PartitionFunction::RegionYear => Arc::new(Schema::new(vec![
                partition_field("region", DataType::Utf8),
                partition_field("year", DataType::Int64),
                Field::new("value", DataType::Float64, true),
            ])),
            PartitionFunction::Override => Arc::new(Schema::new(vec![
                partition_field("category", DataType::Utf8),
                Field::new("revenue", DataType::Int64, true),
            ])),
            PartitionFunction::Disjoint => Arc::new(Schema::new(vec![
                partition_field("key", DataType::Int64),
                Field::new("value", DataType::Int64, true),
            ])),
        }
    }
    fn num_partitions(&self, params: &ProcessParams) -> i64 {
        match self {
            PartitionFunction::CountrySales => COUNTRIES.len() as i64,
            PartitionFunction::RegionYear => REGIONS_YEARS.len() as i64,
            PartitionFunction::Override => CATEGORIES.len() as i64,
            PartitionFunction::Disjoint => params.arguments.const_i64(0).unwrap_or(0).max(0),
        }
    }
    fn rows(&self, params: &ProcessParams) -> i64 {
        match self {
            PartitionFunction::Disjoint => params.arguments.named_i64("rows_per_partition").unwrap_or(10).max(1),
            _ => params.arguments.const_i64(0).unwrap_or(1).max(1),
        }
    }
    fn build_batch(&self, schema: &SchemaRef, idx: i64, rows: i64) -> Result<RecordBatch> {
        let n = rows as usize;
        let cols: Vec<ArrayRef> = match self {
            PartitionFunction::CountrySales => {
                let c = COUNTRIES[idx as usize];
                let base = idx * 1_000_000;
                vec![
                    Arc::new(StringArray::from(vec![c; n])),
                    Arc::new(Int64Array::from((0..rows).map(|i| base + i).collect::<Vec<_>>())),
                ]
            }
            PartitionFunction::RegionYear => {
                let (region, year) = REGIONS_YEARS[idx as usize];
                let base = (idx * 1000) as f64;
                vec![
                    Arc::new(StringArray::from(vec![region; n])),
                    Arc::new(Int64Array::from(vec![year; n])),
                    Arc::new(Float64Array::from((0..rows).map(|i| base + i as f64).collect::<Vec<_>>())),
                ]
            }
            PartitionFunction::Override => {
                let c = CATEGORIES[idx as usize];
                vec![
                    Arc::new(StringArray::from(vec![c; n])),
                    Arc::new(Int64Array::from((0..rows).map(|i| (idx + 1) * 100 + i).collect::<Vec<_>>())),
                ]
            }
            PartitionFunction::Disjoint => {
                let base = idx * 1000;
                vec![
                    Arc::new(Int64Array::from((0..rows).map(|i| base + i).collect::<Vec<_>>())),
                    Arc::new(Int64Array::from((0..rows).map(|i| idx * 10 + i).collect::<Vec<_>>())),
                ]
            }
        };
        RecordBatch::try_new(schema.clone(), cols).map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

struct PartitionProducer {
    kind: PartitionFunction,
    schema: SchemaRef,
    storage: Arc<BufferingStore>,
    execution_id: Vec<u8>,
    claim_tag: String,
    rows: i64,
    meta: Option<HashMap<String, String>>,
}
impl TableProducer for PartitionProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        let Some(item) = self.storage.queue_pop(&self.execution_id, &self.claim_tag) else {
            return Ok(None);
        };
        let mut a = [0u8; 8];
        a.copy_from_slice(&item[..8]);
        let idx = i64::from_le_bytes(a);
        let batch = self.kind.build_batch(&self.schema, idx, self.rows)?;
        self.meta = vgi::partition::partition_metadata(&self.schema, &batch)?;
        Ok(Some(batch))
    }
    fn last_metadata(&self) -> Option<HashMap<String, String>> {
        self.meta.clone()
    }
}

impl TableFunction for PartitionFunction {
    fn name(&self) -> &str {
        match self {
            PartitionFunction::CountrySales => "country_partitioned_sales",
            PartitionFunction::RegionYear => "region_year_partitioned",
            PartitionFunction::Override => "partitioned_with_explicit_override",
            PartitionFunction::Disjoint => "disjoint_range_partitioned",
        }
    }
    fn metadata(&self) -> FunctionMetadata {
        let kind = match self {
            PartitionFunction::Disjoint => vgi::protocol::enums::partition_kind::DISJOINT_PARTITIONS,
            _ => vgi::protocol::enums::partition_kind::SINGLE_VALUE_PARTITIONS,
        };
        FunctionMetadata {
            description: "Partition-column table fixture".to_string(),
            categories: vec!["generator".into(), "partitioning".into()],
            partition_kind: Some(kind.to_string()),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        match self {
            PartitionFunction::Disjoint => vec![
                ArgSpec::const_arg("partitions", 0, "int64", "Number of disjoint partitions"),
                ArgSpec::const_arg("rows_per_partition", -1, "int64", "Rows per partition"),
            ],
            _ => vec![ArgSpec::const_arg("rows", 0, "int64", "Rows per partition")],
        }
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse { output_schema: self.schema(), opaque_data: Vec::new() })
    }
    fn max_workers(&self, _params: &BindParams) -> i64 {
        4
    }
    fn cardinality(&self, _params: &BindParams) -> Option<TableCardinality> {
        None
    }
    fn on_init(&self, params: &ProcessParams) -> Result<()> {
        let store = params
            .storage
            .as_ref()
            .ok_or_else(|| RpcError::runtime_error("partition fixture requires storage"))?;
        let items: Vec<Vec<u8>> =
            (0..self.num_partitions(params)).map(|i| i.to_le_bytes().to_vec()).collect();
        store.queue_push(&params.execution_id, &items);
        Ok(())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let storage = params
            .storage
            .clone()
            .ok_or_else(|| RpcError::runtime_error("partition fixture requires storage"))?;
        let tag = format!("{}_{}", std::process::id(), CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed));
        Ok(Box::new(PartitionProducer {
            kind: *self,
            schema: self.schema(),
            storage,
            execution_id: params.execution_id.clone(),
            claim_tag: tag,
            rows: self.rows(params),
            meta: None,
        }))
    }
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Filter-pushdown diagnostic table fixtures (`filter_echo`, `value_prune`).

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(FilterEchoFunction);
    w.register_table(ValuePruneFunction);
    w.register_table(NamedParamsEchoFunction);
    w.register_table(FilterEchoPartitionedFunction);
    w.register_table(DictFilterEchoFunction);
    w.register_table(DynamicFilterEchoFunction);
    w.register_table(SpatialFilterExampleFunction);
    w.register_table(ExpressionFilterTestFunction);
    w.register_table(FilterEchoTableScan);
}

/// `filter_echo_table_scan` — no-arg catalog-table scan: 100 rows
/// `{n, s='row_<n>', pushed_filters}` where `pushed_filters` echoes the
/// DuckDB-SQL representation of the filters DuckDB pushed into the scan. Backs
/// `example.data.filter_echo_table` (filter_pushdown_through_view.test). The
/// framework auto-applies the filters so results stay correct.
pub struct FilterEchoTableScan;
impl FilterEchoTableScan {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("pushed_filters", DataType::Utf8, true),
        ]))
    }
}
impl TableFunction for FilterEchoTableScan {
    fn name(&self) -> &str {
        "filter_echo_table_scan"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Catalog table that echoes pushed-down filters".to_string(),
            categories: vec!["catalog".into(), "filter".into()],
            projection_pushdown: true,
            filter_pushdown: true,
            auto_apply_filters: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Self::schema(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, _p: &BindParams) -> Option<TableCardinality> {
        Some(TableCardinality {
            estimate: Some(100),
            max: Some(100),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let pushed = params
            .pushdown_filters
            .as_ref()
            .and_then(|b| {
                vgi::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys).ok()
            })
            .map(|f| f.format_pushed())
            .unwrap_or_else(|| "(none)".to_string());
        let ns: Int64Array = (0..100).collect();
        let ss = StringArray::from((0..100).map(|i| format!("row_{i}")).collect::<Vec<_>>());
        let pf = StringArray::from(vec![pushed; 100]);
        let batch = RecordBatch::try_new(
            Self::schema(),
            vec![Arc::new(ns), Arc::new(ss), Arc::new(pf)],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Box::new(OneBatch { batch: Some(batch) }))
    }
}

struct OneBatch {
    batch: Option<RecordBatch>,
}
impl TableProducer for OneBatch {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
}

/// Little-endian WKB 2D point: byte_order=1, type=1 (Point), x, y.
fn wkb_point(x: f64, y: f64) -> Vec<u8> {
    let mut v = vec![0x01, 0x01, 0x00, 0x00, 0x00];
    v.extend_from_slice(&x.to_le_bytes());
    v.extend_from_slice(&y.to_le_bytes());
    v
}

/// `spatial_filter_example(count, batch_size := 1024)` — grid of points in
/// [0,1)x[0,1) with a `geom` GEOMETRY (geoarrow.wkb) column for spatial filter
/// pushdown testing. Point i: x=(i%cols)/cols, y=(i//cols)/cols, cols=ceil(sqrt(N)).
pub struct SpatialFilterExampleFunction;
impl SpatialFilterExampleFunction {
    fn schema() -> SchemaRef {
        let geom = Field::new("geom", DataType::Binary, true).with_metadata(
            std::collections::HashMap::from([
                (
                    "ARROW:extension:name".to_string(),
                    "geoarrow.wkb".to_string(),
                ),
                ("ARROW:extension:metadata".to_string(), "{}".to_string()),
            ]),
        );
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
            geom,
        ]))
    }
}
impl TableFunction for SpatialFilterExampleFunction {
    fn name(&self) -> &str {
        "spatial_filter_example"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Generates points on a grid with geometry for spatial filter testing"
                .to_string(),
            categories: vec!["generator".into(), "spatial".into(), "testing".into()],
            projection_pushdown: true,
            filter_pushdown: true,
            auto_apply_filters: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of points to generate"),
            ArgSpec::const_arg("batch_size", -1, "int64", "Rows per batch"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Self::schema(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let c = params.arguments.const_i64(0)?;
        Some(TableCardinality {
            estimate: Some(c),
            max: Some(c),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let batch_size = params
            .arguments
            .named_i64("batch_size")
            .unwrap_or(1024)
            .max(1);
        Ok(Box::new(SpatialProducer {
            schema: Self::schema(),
            total: count,
            cols: ((count as f64).sqrt().ceil() as i64).max(1),
            batch_size,
            index: 0,
        }))
    }
}

struct SpatialProducer {
    schema: SchemaRef,
    total: i64,
    cols: i64,
    batch_size: i64,
    index: i64,
}
impl TableProducer for SpatialProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        use arrow_array::BinaryArray;
        if self.index >= self.total {
            return Ok(None);
        }
        let size = (self.total - self.index).min(self.batch_size);
        let ns: Vec<i64> = (self.index..self.index + size).collect();
        let xs: Vec<f64> = ns
            .iter()
            .map(|&i| (i % self.cols) as f64 / self.cols as f64)
            .collect();
        let ys: Vec<f64> = ns
            .iter()
            .map(|&i| (i / self.cols) as f64 / self.cols as f64)
            .collect();
        let geoms: Vec<Vec<u8>> = xs.iter().zip(&ys).map(|(&x, &y)| wkb_point(x, y)).collect();
        let geom_arr = BinaryArray::from_iter_values(geoms.iter().map(|g| g.as_slice()));
        self.index += size;
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(ns)),
                Arc::new(Float64Array::from(xs)),
                Arc::new(Float64Array::from(ys)),
                Arc::new(geom_arr),
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(batch))
    }
}

/// `expression_filter_test(count, batch_size := 1024)` — rows with
/// `{id, name='item_<i>', tags=['tag_<i%5>','tag_<(i+1)%5>'], score=i*1.1}` for
/// non-spatial expression filter pushdown testing.
pub struct ExpressionFilterTestFunction;
impl ExpressionFilterTestFunction {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("score", DataType::Float64, true),
        ]))
    }
}
impl TableFunction for ExpressionFilterTestFunction {
    fn name(&self) -> &str {
        "expression_filter_test"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Generates rows for non-spatial expression filter testing".to_string(),
            categories: vec!["generator".into(), "testing".into()],
            projection_pushdown: true,
            filter_pushdown: true,
            auto_apply_filters: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows to generate"),
            ArgSpec::const_arg("batch_size", -1, "int64", "Rows per batch"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Self::schema(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let c = params.arguments.const_i64(0)?;
        Some(TableCardinality {
            estimate: Some(c),
            max: Some(c),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let batch_size = params
            .arguments
            .named_i64("batch_size")
            .unwrap_or(1024)
            .max(1);
        Ok(Box::new(ExprFilterProducer {
            schema: Self::schema(),
            total: count,
            batch_size,
            index: 0,
        }))
    }
}

struct ExprFilterProducer {
    schema: SchemaRef,
    total: i64,
    batch_size: i64,
    index: i64,
}
impl TableProducer for ExprFilterProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        use arrow_array::builder::{ListBuilder, StringBuilder};
        if self.index >= self.total {
            return Ok(None);
        }
        let size = (self.total - self.index).min(self.batch_size);
        let ids: Vec<i64> = (self.index..self.index + size).collect();
        let names = StringArray::from(ids.iter().map(|i| format!("item_{i}")).collect::<Vec<_>>());
        let scores = Float64Array::from(ids.iter().map(|&i| i as f64 * 1.1).collect::<Vec<_>>());
        let mut tags = ListBuilder::new(StringBuilder::new());
        for &i in &ids {
            tags.values().append_value(format!("tag_{}", i % 5));
            tags.values().append_value(format!("tag_{}", (i + 1) % 5));
            tags.append(true);
        }
        self.index += size;
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(names),
                Arc::new(tags.finish()),
                Arc::new(scores),
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(batch))
    }
}

/// `dynamic_filter_echo(count)` — descending integers; each batch's
/// `pushed_filters` echoes the current (per-tick) dynamic pushdown filter, so a
/// tightening `ORDER BY n LIMIT k` Top-N produces multiple distinct witnesses.
pub struct DynamicFilterEchoFunction;
impl TableFunction for DynamicFilterEchoFunction {
    fn name(&self) -> &str {
        "dynamic_filter_echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Generates descending integers, echoes dynamic tick filter per batch"
                .to_string(),
            categories: vec!["generator".into(), "diagnostic".into()],
            projection_pushdown: true,
            filter_pushdown: true,
            auto_apply_filters: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows to generate"),
            ArgSpec::const_arg("batch_size", -1, "int64", "Batch size for output"),
        ]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, true),
                Field::new("pushed_filters", DataType::Utf8, true),
            ])),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let c = params.arguments.const_i64(0)?;
        Some(TableCardinality {
            estimate: Some(c),
            max: Some(c),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let batch_size = params
            .arguments
            .named_i64("batch_size")
            .unwrap_or(2048)
            .max(1);
        // Init witness from the static filter (if any).
        let witness = params
            .pushdown_filters
            .as_ref()
            .and_then(|b| {
                vgi::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys).ok()
            })
            .map(|f| f.format_repr())
            .unwrap_or_default();
        Ok(Box::new(DynFilterEchoProducer {
            schema: Arc::new(Schema::new(vec![
                Field::new("n", DataType::Int64, true),
                Field::new("pushed_filters", DataType::Utf8, true),
            ])),
            count,
            batch_size,
            offset: 0,
            witness,
        }))
    }
}

struct DynFilterEchoProducer {
    schema: SchemaRef,
    count: i64,
    batch_size: i64,
    offset: i64,
    witness: String,
}
impl TableProducer for DynFilterEchoProducer {
    fn on_dynamic_filters(&mut self, filters: Option<&vgi::pushdown::PushdownFilters>) {
        if let Some(f) = filters {
            self.witness = f.format_repr();
        }
    }
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.offset >= self.count {
            return Ok(None);
        }
        let end = (self.offset + self.batch_size).min(self.count);
        // Descending order: first batch has the highest values.
        let ns: Int64Array = (self.offset..end).map(|i| self.count - 1 - i).collect();
        let pushed = StringArray::from(vec![self.witness.clone(); (end - self.offset) as usize]);
        self.offset = end;
        RecordBatch::try_new(self.schema.clone(), vec![Arc::new(ns), Arc::new(pushed)])
            .map(Some)
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// dict_filter_echo(count) -> {n, s: dictionary<int8, utf8>}
// ---------------------------------------------------------------------------

const DICT_VALUES: [&str; 3] = ["red", "green", "blue"];

fn dict_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new(
            "s",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        ),
    ]))
}

struct DictEchoProducer {
    schema: SchemaRef,
    remaining: i64,
    cursor: i64,
}
impl TableProducer for DictEchoProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        let size = self.remaining.min(2048);
        let ns: Vec<i64> = (self.cursor..self.cursor + size).collect();
        let keys: Vec<i8> = ns.iter().map(|n| (n.rem_euclid(3)) as i8).collect();
        let values = Arc::new(StringArray::from(DICT_VALUES.to_vec())) as ArrayRef;
        let dict = arrow_array::DictionaryArray::<arrow_array::types::Int8Type>::try_new(
            arrow_array::Int8Array::from(keys),
            values,
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(ns)) as ArrayRef,
                Arc::new(dict) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.cursor += size;
        self.remaining -= size;
        Ok(Some(batch))
    }
}

pub struct DictFilterEchoFunction;
impl TableFunction for DictFilterEchoFunction {
    fn name(&self) -> &str {
        "dict_filter_echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Emits a dictionary-encoded VARCHAR column for filter-pushdown testing")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "Number of rows")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: dict_schema(),
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
        Ok(Box::new(DictEchoProducer {
            schema: dict_schema(),
            remaining: params.arguments.const_i64(0).unwrap_or(0).max(0),
            cursor: 0,
        }))
    }
}

// ---------------------------------------------------------------------------
// filter_echo_partitioned(count) -> {n, worker_pid, pushed_filters}
// Queue-distributed multi-worker filter echo.
// ---------------------------------------------------------------------------

fn fep_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("worker_pid", DataType::Int64, true),
        Field::new("pushed_filters", DataType::Utf8, true),
    ]))
}

struct FepProducer {
    schema: SchemaRef,
    storage: std::sync::Arc<vgi::buffering::BufferingStore>,
    execution_id: Vec<u8>,
    claim_tag: String,
    filter_str: String,
    pid: i64,
    cur: Option<(i64, i64)>,
}
impl TableProducer for FepProducer {
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
            let bend = (idx + 1000).min(end);
            let ns: Vec<i64> = (idx..bend).collect();
            let n = ns.len();
            let pids = vec![self.pid; n];
            let fs: Vec<&str> = vec![self.filter_str.as_str(); n];
            let batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ns)) as ArrayRef,
                    Arc::new(Int64Array::from(pids)) as ArrayRef,
                    Arc::new(StringArray::from(fs)) as ArrayRef,
                ],
            )
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
            self.cur = Some((bend, end));
            return Ok(Some(batch));
        }
    }
}

pub struct FilterEchoPartitionedFunction;
impl TableFunction for FilterEchoPartitionedFunction {
    fn name(&self) -> &str {
        "filter_echo_partitioned"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Multi-worker partitioned sequence that echoes pushed-down filters"
                .to_string(),
            categories: vec!["generator".into(), "diagnostic".into(), "testing".into()],
            projection_pushdown: true,
            filter_pushdown: true,
            auto_apply_filters: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "count",
            0,
            "int64",
            "Number of rows to generate",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: fep_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn max_workers(&self, _params: &BindParams) -> i64 {
        4
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let count = params.arguments.const_i64(0)?;
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
        let chunk = ((count + 23) / 24).max(1);
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
        Ok(Box::new(FepProducer {
            schema: fep_schema(),
            storage,
            execution_id: params.execution_id.clone(),
            claim_tag: format!("{}_{}", std::process::id(), params.execution_id.len()),
            filter_str: pushed_filter_str(params),
            pid: std::process::id() as i64,
            cur: None,
        }))
    }
}

fn meta(desc: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        categories: vec!["generator".into(), "diagnostic".into()],
        projection_pushdown: true,
        filter_pushdown: true,
        auto_apply_filters: true,
        ..Default::default()
    }
}

/// The SQL-like string of whatever DuckDB pushed down ("(none)" if nothing).
fn pushed_filter_str(params: &ProcessParams) -> String {
    match &params.pushdown_filters {
        Some(bytes) => {
            vgi::pushdown::PushdownFilters::parse_with_join_keys(bytes, &params.join_keys)
                .map(|f| f.format_pushed())
                .unwrap_or_else(|_| "(none)".to_string())
        }
        None => "(none)".to_string(),
    }
}

fn args_count_batchsize() -> Vec<ArgSpec> {
    vec![
        ArgSpec::const_arg("count", 0, "int64", "Number of rows to generate"),
        ArgSpec::const_arg("batch_size", -1, "int64", "Batch size for output"),
    ]
}

// ---------------------------------------------------------------------------
// filter_echo(count) -> {n, s, pushed_filters}
// ---------------------------------------------------------------------------

fn filter_echo_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("pushed_filters", DataType::Utf8, true),
    ]))
}

struct FilterEchoProducer {
    schema: SchemaRef,
    remaining: i64,
    cursor: i64,
    batch_size: i64,
    filter_str: String,
}
impl TableProducer for FilterEchoProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        let size = self.remaining.min(self.batch_size);
        let ns: Vec<i64> = (self.cursor..self.cursor + size).collect();
        let ss: Vec<String> = ns.iter().map(|i| format!("row_{i}")).collect();
        let fs: Vec<&str> = vec![self.filter_str.as_str(); size as usize];
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(ns)) as ArrayRef,
                Arc::new(StringArray::from(ss)) as ArrayRef,
                Arc::new(StringArray::from(fs)) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.cursor += size;
        self.remaining -= size;
        Ok(Some(batch))
    }
}

pub struct FilterEchoFunction;
impl TableFunction for FilterEchoFunction {
    fn name(&self) -> &str {
        "filter_echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Echoes pushed-down filter predicates in output")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        args_count_batchsize()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: filter_echo_schema(),
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
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let batch_size = params
            .arguments
            .named_i64("batch_size")
            .unwrap_or(2048)
            .max(1);
        Ok(Box::new(FilterEchoProducer {
            schema: filter_echo_schema(),
            remaining: count,
            cursor: 0,
            batch_size,
            filter_str: pushed_filter_str(params),
        }))
    }
}

// ---------------------------------------------------------------------------
// value_prune(count) -> {n, resolved} — emits only get_column_values('n')
// ---------------------------------------------------------------------------

fn value_prune_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("resolved", DataType::Utf8, true),
    ]))
}

struct ValuePruneProducer {
    schema: SchemaRef,
    values: Vec<i64>,
    resolved: String,
    cursor: usize,
    batch_size: usize,
}
impl TableProducer for ValuePruneProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.cursor >= self.values.len() {
            return Ok(None);
        }
        let end = (self.cursor + self.batch_size).min(self.values.len());
        let chunk = &self.values[self.cursor..end];
        let resolved: Vec<&str> = vec![self.resolved.as_str(); chunk.len()];
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(chunk.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(resolved)) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.cursor = end;
        Ok(Some(batch))
    }
}

pub struct ValuePruneFunction;
impl TableFunction for ValuePruneFunction {
    fn name(&self) -> &str {
        "value_prune"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Prunes the key set via get_column_values('n'); echoes the resolved discrete values")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        args_count_batchsize()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: value_prune_schema(),
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
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let batch_size = params
            .arguments
            .named_i64("batch_size")
            .unwrap_or(2048)
            .max(1) as usize;
        let discrete = params
            .pushdown_filters
            .as_ref()
            .and_then(|b| {
                vgi::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys).ok()
            })
            .and_then(|f| f.get_column_values("n"));
        let (values, resolved) = match discrete {
            Some(mut vs) => {
                vs.sort_unstable();
                vs.dedup();
                let resolved = vs
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let emit: Vec<i64> = vs.into_iter().filter(|&v| v >= 0 && v < count).collect();
                (emit, resolved)
            }
            None => ((0..count).collect(), "(scan)".to_string()),
        };
        Ok(Box::new(ValuePruneProducer {
            schema: value_prune_schema(),
            values,
            resolved,
            cursor: 0,
            batch_size,
        }))
    }
}

// ---------------------------------------------------------------------------
// named_params_echo(count, greeting:=, multiplier:=, scale:=, enabled:=)
// ---------------------------------------------------------------------------

fn named_params_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("greeting", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
        Field::new("float_value", DataType::Float64, true),
        Field::new("enabled", DataType::Boolean, true),
    ]))
}

struct NamedParamsProducer {
    schema: SchemaRef,
    remaining: i64,
    cursor: i64,
    batch_size: i64,
    greeting: String,
    multiplier: i64,
    scale: f64,
    enabled: bool,
}
impl TableProducer for NamedParamsProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        let size = self.remaining.min(self.batch_size);
        let ids: Vec<i64> = (self.cursor..self.cursor + size).collect();
        let values: Vec<i64> = ids.iter().map(|i| i * self.multiplier).collect();
        let floats: Vec<f64> = ids.iter().map(|i| *i as f64 * self.scale).collect();
        let greetings: Vec<&str> = vec![self.greeting.as_str(); size as usize];
        let enabled: Vec<bool> = vec![self.enabled; size as usize];
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(greetings)) as ArrayRef,
                Arc::new(Int64Array::from(values)) as ArrayRef,
                Arc::new(Float64Array::from(floats)) as ArrayRef,
                Arc::new(BooleanArray::from(enabled)) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.cursor += size;
        self.remaining -= size;
        Ok(Some(batch))
    }
}

pub struct NamedParamsEchoFunction;
impl TableFunction for NamedParamsEchoFunction {
    fn name(&self) -> &str {
        "named_params_echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Echoes named parameter values in output columns".to_string(),
            categories: vec!["generator".into(), "testing".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("count", 0, "int64", "Number of rows to generate"),
            ArgSpec::const_arg("greeting", -1, "varchar", "Greeting text echoed in output"),
            ArgSpec::const_arg("multiplier", -1, "int64", "Multiplier for value column"),
            ArgSpec::const_arg("scale", -1, "double", "Scale factor for float_value column"),
            ArgSpec::const_arg("enabled", -1, "boolean", "Boolean echoed in output"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: named_params_schema(),
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
        let count = params.arguments.const_i64(0).unwrap_or(0).max(0);
        Ok(Box::new(NamedParamsProducer {
            schema: named_params_schema(),
            remaining: count,
            cursor: 0,
            batch_size: 2048,
            greeting: params
                .arguments
                .named_str("greeting")
                .unwrap_or_else(|| "hello".to_string()),
            multiplier: params.arguments.named_i64("multiplier").unwrap_or(1),
            scale: params.arguments.named_f64("scale").unwrap_or(1.0),
            enabled: params.arguments.named_bool("enabled").unwrap_or(true),
        }))
    }
}

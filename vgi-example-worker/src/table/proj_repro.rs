// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Projection-pushdown reproducer fixtures. Four table functions sharing a
//! wide 12-column schema (mirrors vgi-kafka's `kafka_consume`). Each declares
//! `projection_pushdown = true` and emits the full schema; the framework
//! narrows each batch to the planner's `projection_ids`.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, Int32Builder, Int64Builder, ListBuilder, StringBuilder, StructBuilder,
};
use arrow_array::{RecordBatch, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{resume, TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(ProjRepro {
        name: "proj_repro_strict",
        chunk: 0,
    });
    w.register_table(ProjRepro {
        name: "proj_repro_full_schema",
        chunk: 0,
    });
    w.register_table(ProjRepro {
        name: "proj_repro_chunked",
        chunk: 1,
    });
    w.register_table(ProjRepro {
        name: "proj_repro_multi_worker",
        chunk: 0,
    });
}

fn header_struct_fields() -> Fields {
    vec![
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Binary, true),
    ]
    .into()
}

fn wide_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("timestamp_type", DataType::Utf8, true),
        Field::new("key", DataType::Binary, true),
        Field::new("key_string", DataType::Utf8, true),
        Field::new("key_schema_id", DataType::Int32, true),
        Field::new("value", DataType::Binary, true),
        Field::new("value_string", DataType::Utf8, true),
        Field::new("value_schema_id", DataType::Int32, true),
        Field::new(
            "headers",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(header_struct_fields()),
                true,
            ))),
            false,
        ),
    ]))
}

/// Build a full-schema batch for rows `[start, start+size)`.
fn build_wide_batch(schema: &SchemaRef, start: i64, size: i64) -> Result<RecordBatch> {
    let mut topic = StringBuilder::new();
    let mut partition = Int32Builder::new();
    let mut offset = Int64Builder::new();
    let mut ts_type = StringBuilder::new();
    let mut key = BinaryBuilder::new();
    let mut key_string = StringBuilder::new();
    let mut value = BinaryBuilder::new();
    let mut value_string = StringBuilder::new();
    for i in start..start + size {
        topic.append_value("demo_topic");
        partition.append_value((i % 4) as i32);
        offset.append_value(i);
        ts_type.append_null();
        key.append_value(format!("k{i}").as_bytes());
        key_string.append_value(format!("k{i}"));
        value.append_value(format!("v{i}").as_bytes());
        value_string.append_value(format!("v{i}"));
    }
    let n = size as usize;
    let timestamp = TimestampMillisecondArray::from(vec![None; n]).with_timezone("UTC");
    let key_schema_id = arrow_array::Int32Array::from(vec![None; n]);
    let value_schema_id = arrow_array::Int32Array::from(vec![None; n]);
    // headers: one empty struct-list per row.
    let struct_builder = StructBuilder::new(
        header_struct_fields(),
        vec![
            Box::new(StringBuilder::new()),
            Box::new(BinaryBuilder::new()),
        ],
    );
    let mut header_builder = ListBuilder::new(struct_builder);
    for _ in 0..size {
        header_builder.append(true);
    }
    let headers = header_builder.finish();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(topic.finish()),
            Arc::new(partition.finish()),
            Arc::new(offset.finish()),
            Arc::new(timestamp),
            Arc::new(ts_type.finish()),
            Arc::new(key.finish()),
            Arc::new(key_string.finish()),
            Arc::new(key_schema_id),
            Arc::new(value.finish()),
            Arc::new(value_string.finish()),
            Arc::new(value_schema_id),
            Arc::new(headers),
        ],
    )
    .map_err(|e| RpcError::runtime_error(e.to_string()))
}

pub struct ProjRepro {
    name: &'static str,
    /// When >0, emit `chunk` rows per `next_batch` tick (multi-tick variant).
    chunk: i64,
}
impl TableFunction for ProjRepro {
    fn name(&self) -> &str {
        self.name
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "projection-pushdown reproducer".to_string(),
            projection_pushdown: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "n",
            0,
            "int64",
            "Number of rows to generate",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: wide_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let n = params.arguments.const_i64(0)?;
        Some(TableCardinality {
            estimate: Some(n),
            max: Some(n),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let n = params.arguments.const_i64(0).unwrap_or(0).max(0);
        let chunk = if self.chunk > 0 { self.chunk } else { n.max(1) };
        Ok(Box::new(WideProducer {
            schema: wide_schema(),
            n,
            pos: 0,
            chunk,
        }))
    }
}

struct WideProducer {
    schema: SchemaRef,
    n: i64,
    pos: i64,
    chunk: i64,
}
impl TableProducer for WideProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.pos >= self.n {
            return Ok(None);
        }
        let size = (self.n - self.pos).min(self.chunk);
        let batch = build_wide_batch(&self.schema, self.pos, size)?;
        self.pos += size;
        Ok(Some(batch))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.pos])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.pos = v[0];
        }
    }
}

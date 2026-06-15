// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `attach_options` catalog fixture: 20 ATTACH options of every supported type,
//! round-tripped through `attach_opaque_data` and echoed by
//! `echo_attach_options()`. Mirrors `vgi-python/_test_fixtures/attach_options.py`.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, ListArray, RecordBatch, StringArray,
    StructArray, Time64MicrosecondArray, TimestampMicrosecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

/// (name, description, arrow type, one-row default array) for each option.
fn option_defs() -> Vec<(&'static str, &'static str, DataType, ArrayRef)> {
    let list_int64 = {
        let values = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let field = Arc::new(Field::new("item", DataType::Int64, true));
        Arc::new(ListArray::new(
            field,
            OffsetBuffer::new(vec![0i32, 3].into()),
            values,
            None,
        )) as ArrayRef
    };
    let struct_default = {
        let fields: arrow_schema::Fields = vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]
        .into();
        Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            ],
            None,
        )) as ArrayRef
    };
    let decimal = Arc::new(
        Decimal128Array::from(vec![1_234_500i128])
            .with_precision_and_scale(18, 4)
            .unwrap(),
    ) as ArrayRef;
    // 2026-04-24 = 20567 days since epoch; 12:34:56 = 45_296_000_000 µs of day.
    let ts = 1_777_034_096_000_000i64; // 2026-04-24 12:34:56 UTC in µs since epoch
    vec![
        (
            "opt_bool",
            "Boolean option",
            DataType::Boolean,
            Arc::new(BooleanArray::from(vec![true])),
        ),
        (
            "opt_int8",
            "int8",
            DataType::Int8,
            Arc::new(Int8Array::from(vec![-8])),
        ),
        (
            "opt_int16",
            "int16",
            DataType::Int16,
            Arc::new(Int16Array::from(vec![-16])),
        ),
        (
            "opt_int32",
            "int32",
            DataType::Int32,
            Arc::new(Int32Array::from(vec![-32])),
        ),
        (
            "opt_int64",
            "int64",
            DataType::Int64,
            Arc::new(Int64Array::from(vec![-64])),
        ),
        (
            "opt_uint8",
            "uint8",
            DataType::UInt8,
            Arc::new(UInt8Array::from(vec![8])),
        ),
        (
            "opt_uint16",
            "uint16",
            DataType::UInt16,
            Arc::new(UInt16Array::from(vec![16])),
        ),
        (
            "opt_uint32",
            "uint32",
            DataType::UInt32,
            Arc::new(UInt32Array::from(vec![32])),
        ),
        (
            "opt_uint64",
            "uint64",
            DataType::UInt64,
            Arc::new(UInt64Array::from(vec![64])),
        ),
        (
            "opt_float32",
            "float32",
            DataType::Float32,
            Arc::new(Float32Array::from(vec![1.5])),
        ),
        (
            "opt_float64",
            "float64",
            DataType::Float64,
            Arc::new(Float64Array::from(vec![2.5])),
        ),
        (
            "opt_string",
            "UTF-8 string",
            DataType::Utf8,
            Arc::new(StringArray::from(vec!["hello"])),
        ),
        (
            "opt_blob",
            "Binary blob",
            DataType::Binary,
            Arc::new(BinaryArray::from(vec![&[0u8, 1, 2][..]])),
        ),
        (
            "opt_date",
            "Date",
            DataType::Date32,
            Arc::new(Date32Array::from(vec![20567])),
        ),
        (
            "opt_time",
            "Time of day",
            DataType::Time64(TimeUnit::Microsecond),
            Arc::new(Time64MicrosecondArray::from(vec![45_296_000_000])),
        ),
        (
            "opt_timestamp",
            "Naive timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![ts])),
        ),
        (
            "opt_timestamp_tz",
            "Timestamp with UTC tz",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC")),
        ),
        (
            "opt_decimal",
            "Decimal(18,4)",
            DataType::Decimal128(18, 4),
            decimal,
        ),
        (
            "opt_list",
            "List of int64",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            list_int64,
        ),
        (
            "opt_struct",
            "Struct",
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into(),
            ),
            struct_default,
        ),
    ]
}

fn echo_schema() -> SchemaRef {
    Arc::new(Schema::new(
        option_defs()
            .iter()
            .map(|(n, _, t, _)| Field::new(*n, t.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

fn default_batch() -> Result<RecordBatch> {
    let defs = option_defs();
    let cols: Vec<ArrayRef> = defs.iter().map(|(_, _, _, a)| a.clone()).collect();
    RecordBatch::try_new(echo_schema(), cols).map_err(|e| RpcError::runtime_error(e.to_string()))
}

pub fn register(w: &mut vgi::Worker) {
    w.register_table(EchoAttachOptionsFunction);
}

/// The `attach_options` catalog: advertises the 20 option specs and stashes the
/// default option batch (the dispatcher merges user options over it at attach).
pub fn catalog() -> vgi::catalog::CatalogModel {
    let specs: Vec<Vec<u8>> = option_defs()
        .iter()
        .map(|(n, d, t, a)| vgi::catalog::serialize_attach_option_spec(n, d, t, Some(a)).unwrap())
        .collect();
    let default = default_batch()
        .ok()
        .and_then(|b| vgi::ipc::write_batch(&b).ok());
    vgi::catalog::CatalogModel {
        name: "attach_options".to_string(),
        attach_option_specs: specs,
        attach_options_default_batch: default,
        comment: Some("Catalog exercising every ATTACH option type".to_string()),
        schemas: vec![vgi::catalog::CatSchema {
            name: "main".to_string(),
            comment: None,
            views: Vec::new(),
            macros: Vec::new(),
            tables: Vec::new(),
        }],
        ..Default::default()
    }
}

/// `echo_attach_options()` — emit the attach-time option values (one row).
pub struct EchoAttachOptionsFunction;
impl TableFunction for EchoAttachOptionsFunction {
    fn name(&self) -> &str {
        "echo_attach_options"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Echo the attach-time option values carried in attach_opaque_data"
                .to_string(),
            categories: vec!["generator".into(), "testing".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: echo_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, _p: &BindParams) -> Option<vgi::table_function::TableCardinality> {
        Some(vgi::table_function::TableCardinality {
            estimate: Some(1),
            max: Some(1),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        // Decode `<16-byte id>\0<ipc batch>` from attach_opaque_data.
        let batch = match &params.attach_opaque_data {
            Some(raw) if raw.len() > 17 && raw[16] == 0 => vgi::ipc::read_batch(&raw[17..])?,
            _ => default_batch()?,
        };
        Ok(Box::new(OneShot { batch: Some(batch) }))
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

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `typed_probe` — typed const-argument binding + typed-column emit fixture.
//!
//! Exercises the less-common scalar const-arg getters cross-language:
//! TIMESTAMPTZ, INTERVAL, BLOB and UBIGINT, each declared with a default so
//! `typed_probe(n)` drives the default-binding path while passing named args
//! drives scalar extraction. The bound values are echoed into uint64 / int64 /
//! blob / double columns in normalized integer/byte form so the Rust, Go and
//! Python fixtures produce byte-identical results for the shared test.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{IntervalMonthDayNanoType, TimestampMicrosecondType, UInt64Type};
use arrow_array::{BinaryArray, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableCardinality, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(TypedProbeFunction);
}

// Default const values (match the Go fixture exactly).
const DEFAULT_TS_US: i64 = 1_767_323_045_000_000; // 2026-01-02T03:04:05Z
const DEFAULT_IV_MS: i64 = 1500; // 1500ms
const DEFAULT_BLOB: &[u8] = b"vgi";
const DEFAULT_UB: u64 = 9;
const DEFAULT_F: f64 = 2.5;

fn typed_probe_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("idx", DataType::UInt64, true),
        Field::new("ts_us", DataType::Int64, true),
        Field::new("iv_ms", DataType::Int64, true),
        Field::new("payload", DataType::Binary, true),
        Field::new("ub", DataType::UInt64, true),
        Field::new("f", DataType::Float64, true),
    ]))
}

/// Read the named TIMESTAMPTZ const `ts` as unix microseconds (the underlying
/// TimestampMicrosecond value is already micros).
fn named_ts_us(args: &vgi::arguments::Arguments) -> Option<i64> {
    let arr = args.named("ts")?;
    arr.as_primitive_opt::<TimestampMicrosecondType>()
        .map(|a| a.value(0))
}

/// Read the named INTERVAL const `iv` as whole milliseconds (months → 30 days).
fn named_iv_ms(args: &vgi::arguments::Arguments) -> Option<i64> {
    let arr = args.named("iv")?;
    let iv = arr.as_primitive_opt::<IntervalMonthDayNanoType>()?;
    let v = iv.value(0);
    Some((v.months as i64 * 30 + v.days as i64) * 86_400_000 + v.nanoseconds / 1_000_000)
}

/// Read the named BLOB const `blob` bytes.
fn named_blob(args: &vgi::arguments::Arguments) -> Option<Vec<u8>> {
    let arr = args.named("blob")?;
    if let Some(b) = arr.as_binary_opt::<i32>() {
        return Some(b.value(0).to_vec());
    }
    arr.as_binary_opt::<i64>().map(|b| b.value(0).to_vec())
}

/// Read the named UBIGINT const `ub` as u64.
fn named_ub(args: &vgi::arguments::Arguments) -> Option<u64> {
    let arr = args.named("ub")?;
    arr.as_primitive_opt::<UInt64Type>().map(|a| a.value(0))
}

pub struct TypedProbeFunction;
impl TableFunction for TypedProbeFunction {
    fn name(&self) -> &str {
        "typed_probe"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description:
                "Echoes typed const args (timestamp/interval/blob/ubigint) into typed columns"
                    .to_string(),
            categories: vec!["generator".into(), "testing".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("n", 0, "int64", "Number of rows to emit"),
            ArgSpec::const_typed(
                "ts",
                -1,
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                "Timestamp const (TIMESTAMPTZ)",
            ),
            ArgSpec::const_typed(
                "iv",
                -1,
                DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
                "Interval const (INTERVAL)",
            ),
            ArgSpec::const_typed("blob", -1, DataType::Binary, "Blob const (BLOB)"),
            ArgSpec::const_typed("ub", -1, DataType::UInt64, "Unsigned const (UBIGINT)"),
            ArgSpec::const_arg("f", -1, "double", "Float const (DOUBLE)"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: typed_probe_schema(),
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
        let a = &params.arguments;
        let count = a.const_i64(0).unwrap_or(0).max(0);
        Ok(Box::new(TypedProbeProducer {
            schema: typed_probe_schema(),
            remaining: count,
            cursor: 0,
            ts_us: named_ts_us(a).unwrap_or(DEFAULT_TS_US),
            iv_ms: named_iv_ms(a).unwrap_or(DEFAULT_IV_MS),
            payload: named_blob(a).unwrap_or_else(|| DEFAULT_BLOB.to_vec()),
            ub: named_ub(a).unwrap_or(DEFAULT_UB),
            f: a.named_f64("f").unwrap_or(DEFAULT_F),
        }))
    }
}

struct TypedProbeProducer {
    schema: SchemaRef,
    remaining: i64,
    cursor: i64,
    ts_us: i64,
    iv_ms: i64,
    payload: Vec<u8>,
    ub: u64,
    f: f64,
}
impl TableProducer for TypedProbeProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.remaining <= 0 {
            return Ok(None);
        }
        let size = self.remaining.min(2048);
        let start = self.cursor;
        let n = size as usize;
        let idx = UInt64Array::from((start..start + size).map(|i| i as u64).collect::<Vec<_>>());
        let ts = Int64Array::from(vec![self.ts_us; n]);
        let iv = Int64Array::from(vec![self.iv_ms; n]);
        let payload = BinaryArray::from(vec![self.payload.as_slice(); n]);
        let ub = UInt64Array::from(vec![self.ub; n]);
        let f = Float64Array::from(
            (start..start + size)
                .map(|i| self.f + i as f64)
                .collect::<Vec<_>>(),
        );
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(idx),
                Arc::new(ts),
                Arc::new(iv),
                Arc::new(payload),
                Arc::new(ub),
                Arc::new(f),
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.cursor += size;
        self.remaining -= size;
        Ok(Some(batch))
    }
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `blob_test`: functions sized by their arguments, for externalization tests.
//!
//! EXAMPLE/TEST FUNCTIONS ONLY. The cross-SDK fixture catalog the vgi
//! extension's `test/sql/external_storage/*.test` files attach
//! (`docs/testing/blob-test-catalog.md` in the vgi repo is the contract; port
//! of vgi-python's `vgi/_test_fixtures/blob_catalog.py`). With the blob vendor
//! configured (`VGI_BLOB_VENDOR_URL`, HTTP):
//!
//! - results over `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES` come back as vendor
//!   pointers, and requests over `VGI_RPC_MAX_REQUEST_BYTES` go up through
//!   vendor upload URLs;
//! - with `VGI_STORAGE_EXTERNALIZE=1`, storage values of at least
//!   `VGI_STORAGE_EXTERNALIZE_BYTES` are kept in the vendor.
//!
//! | Function | Kind | Moves |
//! |---|---|---|
//! | `blob_repeat(s, n)` | scalar | large output: `s` repeated `n` times |
//! | `blob_len(s)` | scalar | large input: `strlen(s)` in UTF-8 bytes |
//! | `blob_rows(rows, width)` | table | large output: row `i` is `repeat(chr(97 + i % 26), width)` |
//! | `blob_param_echo(payload)` | table | large bind parameter: its length and SHA-256 |
//! | `blob_buffer_echo(t)` | table-buffering | each input batch as one log value, replayed |
//! | `blob_tio(t)` | table-in-out | each batch stored, read back and deleted |
//! | `blob_store_values(t)` | table-in-out | each value of column 0 as its own storage value |
//! | `blob_param_batches(payload, batches)` | table | large bind parameter over many turns |
//! | `blob_param_streams(payload, streams)` | table (one split per stream) | large bind parameter, parallel streams |
//! | `blob_param_scalar(i, payload)` | scalar (constant parameter) | large constant parameter of a scalar |
//! | `blob_param_table` | catalog table | a scan binding `blob_param_echo`'s twin |
//!
//! The four `blob_param_*` fixtures back the extension's protocol 3.4
//! argument-reference tests (`test/sql/external_storage/arguments_ref*.test`,
//! "Protocol 3.4 large values" in the catalog document): `blob_param_batches`
//! emits `batches` one-row batches, one per stream turn; `blob_param_streams`
//! plans `streams` splits that each emit one row; `blob_param_scalar` returns
//! `sha256(payload)` for each non-NULL `i` with `payload` a constant
//! parameter; `blob_param_table` binds `repeat('t', 65536)`, reports
//! `STABLE_WITHIN_QUERY` and is not inline-bound, so every scan binds over
//! RPC.
//!
//! The checksum columns `payload_len` / `payload_sha256` are the UTF-8 byte
//! length and lowercase hex SHA-256 of a value as text (DuckDB's `strlen` and
//! `sha256`), NULL for NULL. The storage functions compute them from what
//! storage returns, never from their input, so the round trip is what is
//! tested.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use sha2::Digest as _;
use vgi::buffering::{BufferingParams, TableBufferingFunction};
use vgi::catalog::{CatSchema, CatalogModel};
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, ScalarFunction,
};
use vgi::ipc;
use vgi::split_token::{PlanOutcome, PlannedSplit};
use vgi::table_function::{
    resume, BindRepeatability, TableCardinality, TableFunction, TableProducer,
};
use vgi::table_in_out::TableInOutFunction;
use vgi_rpc::{Result, RpcError};

/// The catalog's name.
pub const CATALOG_NAME: &str = "blob_test";
const ROWS_PER_BATCH: i64 = 2048;
const BUFFER_NS: &[u8] = b"blob_buffer";
const VALUES_NS: &[u8] = b"blob_values";

const FUNCTIONS: [&str; 11] = [
    "blob_repeat",
    "blob_len",
    "blob_rows",
    "blob_param_echo",
    "blob_buffer_echo",
    "blob_tio",
    "blob_store_values",
    "blob_param_batches",
    "blob_param_streams",
    "blob_param_scalar",
    "blob_param_table_scan",
];

/// The fixed argument `blob_param_table` binds: `repeat('t', 65536)`.
pub const BLOB_PARAM_TABLE_ARGUMENT_LEN: usize = 65536;

fn err(e: impl std::fmt::Display) -> RpcError {
    RpcError::runtime_error(e.to_string())
}

fn sha256_hex(bytes: &[u8]) -> String {
    sha2::Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// A column cast to UTF-8 text.
fn as_text(col: &ArrayRef) -> Result<StringArray> {
    let text = arrow_cast::cast(col, &DataType::Utf8).map_err(err)?;
    Ok(text.as_string::<i32>().clone())
}

fn checksum_fields() -> [Field; 2] {
    [
        Field::new("payload_len", DataType::Int64, true),
        Field::new("payload_sha256", DataType::Utf8, true),
    ]
}

/// `fields`, then `payload_len` and `payload_sha256`.
fn checksum_schema<'a>(fields: impl IntoIterator<Item = &'a Arc<Field>>) -> SchemaRef {
    let mut out: Vec<Field> = fields.into_iter().map(|f| f.as_ref().clone()).collect();
    out.extend(checksum_fields());
    Arc::new(Schema::new(out))
}

/// The checksum columns of `values` (`None` stays NULL).
fn checksum_columns<'a>(values: impl Iterator<Item = Option<&'a [u8]>>) -> [ArrayRef; 2] {
    let (lens, digests): (Vec<Option<i64>>, Vec<Option<String>>) = values
        .map(|v| match v {
            Some(raw) => (Some(raw.len() as i64), Some(sha256_hex(raw))),
            None => (None, None),
        })
        .unzip();
    [
        Arc::new(Int64Array::from(lens)),
        Arc::new(StringArray::from(digests)),
    ]
}

/// `batch` plus the checksum columns of its first column as text.
fn with_checksums(batch: &RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    if batch.num_columns() == 0 {
        return Err(RpcError::value_error("expected at least one input column"));
    }
    let text = as_text(batch.column(0))?;
    columns.extend(checksum_columns(
        (0..text.len()).map(|i| (!text.is_null(i)).then(|| text.value(i).as_bytes())),
    ));
    RecordBatch::try_new(output_schema.clone(), columns).map_err(err)
}

fn input_schema(params: &BindParams) -> Result<SchemaRef> {
    params
        .input_schema
        .clone()
        .ok_or_else(|| RpcError::value_error("blob_test functions need a table input"))
}

fn meta(description: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: description.to_string(),
        categories: vec!["testing".into()],
        ..Default::default()
    }
}

fn table_arg() -> ArgSpec {
    ArgSpec::column("data", 0, "table", "Input table")
}

// ---------------------------------------------------------------------------
// Scalars
// ---------------------------------------------------------------------------

/// `blob_repeat(s, n)`: `s` repeated `n` times (`n < 0` counts as 0).
pub struct BlobRepeatFunction;
impl ScalarFunction for BlobRepeatFunction {
    fn name(&self) -> &str {
        "blob_repeat"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            return_type: Some(DataType::Utf8),
            ..meta("Repeat a string n times (externalization test: large scalar output)")
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("s", 0, "varchar", "String to repeat"),
            ArgSpec::column("n", 1, "int64", "Number of copies"),
        ]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let s = as_text(batch.column(0))?;
        let n = arrow_cast::cast(batch.column(1), &DataType::Int64).map_err(err)?;
        let n = n.as_primitive::<Int64Type>();
        let out: StringArray = (0..batch.num_rows())
            .map(|i| {
                (!s.is_null(i) && !n.is_null(i))
                    .then(|| s.value(i).repeat(n.value(i).max(0) as usize))
            })
            .collect();
        RecordBatch::try_new(params.output_schema.clone(), vec![Arc::new(out)]).map_err(err)
    }
}

/// `blob_len(s)`: the UTF-8 byte length of `s`.
pub struct BlobLenFunction;
impl ScalarFunction for BlobLenFunction {
    fn name(&self) -> &str {
        "blob_len"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            return_type: Some(DataType::Int64),
            ..meta("UTF-8 byte length of a string (externalization test: large scalar input)")
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("s", 0, "varchar", "String to measure")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let s = as_text(batch.column(0))?;
        let out: Int64Array = (0..s.len())
            .map(|i| (!s.is_null(i)).then(|| s.value(i).len() as i64))
            .collect();
        RecordBatch::try_new(params.output_schema.clone(), vec![Arc::new(out)]).map_err(err)
    }
}

// ---------------------------------------------------------------------------
// Table functions
// ---------------------------------------------------------------------------

fn rows_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("i", DataType::Int64, true),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

fn non_negative(params: &vgi::arguments::Arguments, pos: usize, name: &str) -> Result<i64> {
    let v = params
        .const_i64(pos)
        .ok_or_else(|| RpcError::value_error(format!("blob_rows: {name} is required")))?;
    if v < 0 {
        return Err(RpcError::value_error(format!(
            "blob_rows: {name} must be >= 0, got {v}"
        )));
    }
    Ok(v)
}

/// `blob_rows(rows, width)`: row `i` is `repeat(chr(97 + i % 26), width)`.
pub struct BlobRowsFunction;
impl TableFunction for BlobRowsFunction {
    fn name(&self) -> &str {
        "blob_rows"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Rows of a given width (externalization test: large table output)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("rows", 0, "int64", "Number of rows").with_ge(0.0),
            ArgSpec::const_arg("width", 1, "int64", "Characters per row").with_ge(0.0),
        ]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        non_negative(&params.arguments, 0, "rows")?;
        non_negative(&params.arguments, 1, "width")?;
        Ok(BindResponse {
            output_schema: rows_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let rows = params.arguments.const_i64(0)?.max(0);
        Some(TableCardinality {
            estimate: Some(rows),
            max: Some(rows),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(RowsProducer {
            rows: non_negative(&params.arguments, 0, "rows")?,
            width: non_negative(&params.arguments, 1, "width")? as usize,
            next: 0,
        }))
    }
}

struct RowsProducer {
    rows: i64,
    width: usize,
    next: i64,
}

impl TableProducer for RowsProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.next >= self.rows {
            return Ok(None);
        }
        let stop = (self.next + ROWS_PER_BATCH).min(self.rows);
        let ids: Int64Array = (self.next..stop).map(Some).collect();
        let payload: StringArray = (self.next..stop)
            .map(|i| {
                Some(
                    ((b'a' + (i % 26) as u8) as char)
                        .to_string()
                        .repeat(self.width),
                )
            })
            .collect();
        self.next = stop;
        RecordBatch::try_new(rows_schema(), vec![Arc::new(ids), Arc::new(payload)])
            .map(Some)
            .map_err(err)
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.next])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.next = v[0];
        }
    }
}

fn checksum_only_schema() -> SchemaRef {
    Arc::new(Schema::new(checksum_fields().to_vec()))
}

/// `blob_param_echo(payload)`: one row, the parameter's length and SHA-256.
pub struct BlobParamEchoFunction;
impl TableFunction for BlobParamEchoFunction {
    fn name(&self) -> &str {
        "blob_param_echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Length and SHA-256 of a parameter (externalization test: large bind parameter)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "payload",
            0,
            "varchar",
            "Parameter to measure",
        )]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: checksum_only_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, _params: &BindParams) -> Option<TableCardinality> {
        Some(TableCardinality {
            estimate: Some(1),
            max: Some(1),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let payload = params.arguments.const_str(0);
        let columns = checksum_columns(std::iter::once(payload.as_deref().map(str::as_bytes)));
        let batch = RecordBatch::try_new(checksum_only_schema(), columns.to_vec()).map_err(err)?;
        Ok(Box::new(OneBatch { batch: Some(batch) }))
    }
}

/// Emits one batch. Resumable: over HTTP the rebuilt producer is told it
/// already emitted.
struct OneBatch {
    batch: Option<RecordBatch>,
}

impl TableProducer for OneBatch {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        Ok(self.batch.take())
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.batch.is_none() as i64])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if resume::unpack(bytes, 1).is_some_and(|v| v[0] != 0) {
            self.batch = None;
        }
    }
}

// ---------------------------------------------------------------------------
// Protocol 3.4 argument-reference fixtures
// ---------------------------------------------------------------------------

/// `strlen(payload)` and `sha256(payload)` as one-row columns.
fn digest_columns(payload: Option<&str>) -> [ArrayRef; 2] {
    checksum_columns(std::iter::once(payload.map(str::as_bytes)))
}

fn index_digest_schema(index: &str) -> SchemaRef {
    let mut fields = vec![Field::new(index, DataType::Int64, true)];
    fields.extend(checksum_fields());
    Arc::new(Schema::new(fields))
}

fn index_digest_batch(index: &str, i: i64, payload: Option<&str>) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![i]))];
    columns.extend(digest_columns(payload));
    RecordBatch::try_new(index_digest_schema(index), columns).map_err(err)
}

/// `blob_param_batches(payload, batches)`: `batches` one-row batches, one per
/// stream turn. Row `i` is `(i, strlen(payload), sha256(payload))`, so a
/// 60-batch call is at least 60 continuations, each carrying the stream's
/// `init` state.
pub struct BlobParamBatchesFunction;
impl TableFunction for BlobParamBatchesFunction {
    fn name(&self) -> &str {
        "blob_param_batches"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Length and SHA-256 of a parameter, once per batch (argument references over many turns)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("payload", 0, "varchar", "Parameter to measure"),
            ArgSpec::const_arg("batches", 1, "int64", "Number of one-row batches").with_ge(0.0),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: index_digest_schema("i"),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let n = params.arguments.const_i64(1)?.max(0);
        Some(TableCardinality {
            estimate: Some(n),
            max: Some(n),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(BatchesProducer {
            payload: params.arguments.const_str(0),
            batches: params.arguments.const_i64(1).unwrap_or(0).max(0),
            next: 0,
        }))
    }
}

struct BatchesProducer {
    payload: Option<String>,
    batches: i64,
    next: i64,
}

impl TableProducer for BatchesProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.next >= self.batches {
            return Ok(None);
        }
        let batch = index_digest_batch("i", self.next, self.payload.as_deref())?;
        self.next += 1;
        Ok(Some(batch))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.next])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.next = v[0];
        }
    }
}

/// `blob_param_streams(payload, streams)`: one bind planned as `streams`
/// splits, each emitting `(stream index, strlen(payload), sha256(payload))`.
/// Every split's init embeds the same bind call, so with an argument
/// reference the worker process fetches the argument once (vgi-rpc's object
/// cache).
pub struct BlobParamStreamsFunction;
impl TableFunction for BlobParamStreamsFunction {
    fn name(&self) -> &str {
        "blob_param_streams"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            supports_splits: true,
            ..meta("Length and SHA-256 of a parameter from parallel streams (argument references)")
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("payload", 0, "varchar", "Parameter to measure"),
            ArgSpec::const_arg(
                "streams",
                1,
                "int64",
                "Number of parallel streams (one split each)",
            )
            .with_ge(1.0),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: index_digest_schema("stream"),
            opaque_data: Vec::new(),
        })
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        let n = params.arguments.const_i64(1)?.max(1);
        Some(TableCardinality {
            estimate: Some(n),
            max: Some(n),
        })
    }
    fn on_plan(
        &self,
        params: &BindParams,
        _request: &vgi::protocol::dtos::TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let count = params.arguments.const_i64(1).unwrap_or(1).max(1);
        Ok(Some(PlanOutcome {
            splits: (0..count)
                .map(|i| PlannedSplit {
                    payload: i.to_le_bytes().to_vec(),
                    estimated_rows: Some(1),
                    rows_exact: true,
                    ..Default::default()
                })
                .collect(),
            estimated_total_splits: Some(count),
            estimated_total_rows: Some(count),
            ..Default::default()
        }))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let Some(payloads) = params.split_payloads.as_ref() else {
            return Err(RpcError::runtime_error(
                "blob_param_streams is split-only; vgi_split_scans is probably off",
            ));
        };
        let streams = payloads
            .iter()
            .map(|p| {
                <[u8; 8]>::try_from(p.as_slice())
                    .map(i64::from_le_bytes)
                    .map_err(|_| RpcError::runtime_error("split payload must be 8 bytes"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(StreamsProducer {
            payload: params.arguments.const_str(0),
            streams,
            next: 0,
        }))
    }
}

struct StreamsProducer {
    payload: Option<String>,
    streams: Vec<i64>,
    next: i64,
}

impl TableProducer for StreamsProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        let Some(stream) = self.streams.get(self.next as usize).copied() else {
            return Ok(None);
        };
        self.next += 1;
        index_digest_batch("stream", stream, self.payload.as_deref()).map(Some)
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.next])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.next = v[0];
        }
    }
}

/// `blob_param_scalar(i, payload)`: `sha256(payload)` for every row whose `i`
/// is not NULL. `payload` is a constant parameter, so it rides the scalar's
/// bind (and with it an argument reference), not its input batches.
pub struct BlobParamScalarFunction;
impl ScalarFunction for BlobParamScalarFunction {
    fn name(&self) -> &str {
        "blob_param_scalar"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            return_type: Some(DataType::Utf8),
            ..meta(
                "SHA-256 of a constant parameter for every row (argument references for scalars)",
            )
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column("i", 0, "int64", "Row value; NULL gives NULL"),
            ArgSpec::const_arg("payload", 1, "varchar", "Constant parameter to hash"),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse::result(DataType::Utf8))
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        let digest = params
            .arguments
            .const_str(1)
            .map(|p| sha256_hex(p.as_bytes()));
        let i = batch.column(0);
        let out: StringArray = (0..batch.num_rows())
            .map(|row| (!i.is_null(row)).then(|| digest.clone()).flatten())
            .collect();
        RecordBatch::try_new(params.output_schema.clone(), vec![Arc::new(out)]).map_err(err)
    }
}

/// The scan behind the `blob_param_table` catalog table: `blob_param_echo`
/// with a fixed argument. It reports `STABLE_WITHIN_QUERY` and is never
/// inline-bound, so every scan binds over RPC and a serialized-plan rebind
/// must reproduce the same bind bytes (the extension's per-query
/// argument-reference memo).
pub struct BlobParamTableScan;
impl TableFunction for BlobParamTableScan {
    fn name(&self) -> &str {
        "blob_param_table_scan"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Length and SHA-256 of blob_param_table's fixed parameter (STABLE_WITHIN_QUERY)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        BlobParamEchoFunction.argument_specs()
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        BlobParamEchoFunction.on_bind(params)
    }
    fn repeatability(&self, _params: &BindParams) -> BindRepeatability {
        BindRepeatability::StableWithinQuery
    }
    fn cardinality(&self, params: &BindParams) -> Option<TableCardinality> {
        BlobParamEchoFunction.cardinality(params)
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        BlobParamEchoFunction.producer(params)
    }
}

/// The `blob_param_table` catalog table: `blob_param_table_scan('t' x 65536)`,
/// bound over RPC on every scan (not inline).
fn blob_param_table() -> Result<vgi::catalog::CatTable> {
    let argument: ArrayRef = Arc::new(StringArray::from(vec![
        "t".repeat(BLOB_PARAM_TABLE_ARGUMENT_LEN)
    ]));
    Ok(vgi::catalog::CatTable::new(
        "blob_param_table",
        checksum_only_schema(),
        "blob_param_table_scan",
        vgi::arguments::Arguments::serialize_scan_args(&[argument])?,
        Some(
            "blob_param_echo(repeat('t', 65536)), STABLE_WITHIN_QUERY, bound over RPC on every scan"
                .into(),
        ),
        Some(1),
    ))
}

// ---------------------------------------------------------------------------
// Table-buffering and table-in-out
// ---------------------------------------------------------------------------

/// `blob_buffer_echo(t)`: each input batch stored (IPC stream) as one log value
/// in the execution's scope; finalize replays them one per tick, with the
/// checksums of their first column.
pub struct BlobBufferEchoFunction;
impl TableBufferingFunction for BlobBufferEchoFunction {
    fn name(&self) -> &str {
        "blob_buffer_echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Buffer input batches in storage and replay them (externalization test: storage values)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg()]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: checksum_schema(input_schema(params)?.fields().iter()),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &BufferingParams, batch: &RecordBatch) -> Result<Vec<u8>> {
        params.storage.append(
            &params.execution_id,
            BUFFER_NS,
            b"",
            ipc::write_batch(batch)?,
        );
        Ok(params.execution_id.clone())
    }
    fn combine(&self, params: &BufferingParams, _state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![params.execution_id.clone()])
    }
    fn finalize_producer(
        &self,
        params: &BufferingParams,
        _finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(BufferReplay {
            storage: params.storage.clone(),
            execution_id: params.execution_id.clone(),
            output_schema: params.output_schema.clone(),
            after_id: -1,
        }))
    }
}

struct BufferReplay {
    storage: Arc<dyn vgi::storage::FunctionStorage>,
    execution_id: Vec<u8>,
    output_schema: SchemaRef,
    after_id: i64,
}

impl TableProducer for BufferReplay {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        let rows = self
            .storage
            .scan(&self.execution_id, BUFFER_NS, b"", self.after_id, 1);
        let Some((id, value)) = rows.into_iter().next() else {
            return Ok(None);
        };
        self.after_id = id;
        with_checksums(&ipc::read_batch(&value)?, &self.output_schema).map(Some)
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.after_id])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.after_id = v[0];
        }
    }
}

fn storage(params: &ProcessParams) -> Result<&Arc<dyn vgi::storage::FunctionStorage>> {
    params
        .storage
        .as_ref()
        .ok_or_else(|| RpcError::runtime_error("blob_test: no function storage"))
}

/// `blob_tio(t)`: each batch stored (IPC stream) under a fresh key, read
/// straight back, deleted, and emitted as read back with the checksums of its
/// first column.
pub struct BlobTioFunction;
impl TableInOutFunction for BlobTioFunction {
    fn name(&self) -> &str {
        "blob_tio"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Round-trip each batch through storage (externalization test: table-in-out state)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg()]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: checksum_schema(input_schema(params)?.fields().iter()),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let store = storage(params)?;
        let key = rand::random::<[u8; 16]>();
        store.kv_put(&params.execution_id, &key, &ipc::write_batch(batch)?);
        let stored = store
            .kv_get(&params.execution_id, &key)
            .ok_or_else(|| RpcError::runtime_error("blob_tio: the batch just stored is missing"))?;
        store.kv_del(&params.execution_id, &key);
        Ok(vec![with_checksums(
            &ipc::read_batch(&stored)?,
            &params.output_schema,
        )?])
    }
}

/// `blob_store_values(t)`: each non-NULL value of column 0 (as text) stored as
/// its own storage value of exactly that size, all read back in one call and
/// deleted; emits columns 2..n with the checksums of the bytes read back.
pub struct BlobStoreValuesFunction;
impl TableInOutFunction for BlobStoreValuesFunction {
    fn name(&self) -> &str {
        "blob_store_values"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Store each value of the first column exactly and read it back (externalization test)")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![table_arg()]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = input_schema(params)?;
        Ok(BindResponse {
            output_schema: checksum_schema(input.fields().iter().skip(1)),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let store = storage(params)?;
        if batch.num_columns() == 0 {
            return Err(RpcError::value_error("expected at least one input column"));
        }
        let text = as_text(batch.column(0))?;
        let prefix = rand::random::<[u8; 16]>();
        let keys: Vec<Vec<u8>> = (0..text.len() as u32)
            .map(|i| [VALUES_NS, &prefix[..], &i.to_be_bytes()].concat())
            .collect();
        let items: Vec<(Vec<u8>, Vec<u8>)> = (0..text.len())
            .filter(|&i| !text.is_null(i))
            .map(|i| (keys[i].clone(), text.value(i).as_bytes().to_vec()))
            .collect();
        let mut back: std::collections::HashMap<Vec<u8>, Vec<u8>> = Default::default();
        if !items.is_empty() {
            let stored_keys: Vec<Vec<u8>> = items.iter().map(|(k, _)| k.clone()).collect();
            store.kv_put_many(&params.execution_id, &items);
            let stored = store.kv_get_many(&params.execution_id, &stored_keys);
            for key in &stored_keys {
                store.kv_del(&params.execution_id, key);
            }
            for (key, got) in stored_keys.into_iter().zip(stored) {
                let got = got.ok_or_else(|| {
                    RpcError::runtime_error("blob_store_values: a value just stored is missing")
                })?;
                back.insert(key, got);
            }
        }
        let mut columns: Vec<ArrayRef> = batch.columns()[1..].to_vec();
        columns.extend(checksum_columns(
            (0..text.len()).map(|i| (!text.is_null(i)).then(|| back[&keys[i]].as_slice())),
        ));
        Ok(vec![RecordBatch::try_new(
            params.output_schema.clone(),
            columns,
        )
        .map_err(err)?])
    }
}

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

/// The `blob_test` catalog model: one schema, `main`, holding the functions.
pub fn catalog() -> CatalogModel {
    CatalogModel {
        name: CATALOG_NAME.to_string(),
        comment: Some("Functions sized by their arguments, for blob externalization tests".into()),
        schemas: vec![CatSchema {
            path: Vec::new(),
            name: "main".to_string(),
            comment: Some("Externalization test functions".into()),
            tags: Vec::new(),
            views: Vec::new(),
            macros: Vec::new(),
            tables: vec![blob_param_table().expect("blob_param_table scan arguments")],
        }],
        catalog_version_frozen: true,
        ..Default::default()
    }
}

/// Register the functions and the `blob_test` catalog.
pub fn register(w: &mut vgi::Worker) {
    w.register_scalar_in(CATALOG_NAME, "main", BlobRepeatFunction);
    w.register_scalar_in(CATALOG_NAME, "main", BlobLenFunction);
    w.register_table_in(CATALOG_NAME, "main", BlobRowsFunction);
    w.register_table_in(CATALOG_NAME, "main", BlobParamEchoFunction);
    w.register_buffering_in(CATALOG_NAME, "main", BlobBufferEchoFunction);
    w.register_table_in_out_in(CATALOG_NAME, "main", BlobTioFunction);
    w.register_table_in_out_in(CATALOG_NAME, "main", BlobStoreValuesFunction);
    w.register_table_in(CATALOG_NAME, "main", BlobParamBatchesFunction);
    w.register_table_in(CATALOG_NAME, "main", BlobParamStreamsFunction);
    w.register_scalar_in(CATALOG_NAME, "main", BlobParamScalarFunction);
    w.register_table_in(CATALOG_NAME, "main", BlobParamTableScan);
    w.register_secondary_catalog(catalog(), FUNCTIONS.iter().map(|s| s.to_string()).collect());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksums_match_duckdb() {
        let [lens, digests] =
            checksum_columns([Some(&b"abc"[..]), None, Some("é".as_bytes())].into_iter());
        let lens = lens.as_primitive::<Int64Type>();
        let digests = digests.as_string::<i32>();
        assert_eq!(lens.value(0), 3);
        assert!(lens.is_null(1) && digests.is_null(1));
        assert_eq!(lens.value(2), 2);
        assert_eq!(
            digests.value(0),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}

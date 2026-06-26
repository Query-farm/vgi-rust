// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! End-to-end HTTP continuation tests for table-scan producers.
//!
//! These assert the property the language-agnostic DuckDB integration suite
//! cannot observe (DuckDB follows continuation tokens transparently): over HTTP
//! a resumable table scan returns ONE bounded batch per response and resumes via
//! a stateless continuation token, so the whole result set never has to fit in
//! memory — matching the Python and Go workers. A producer that does NOT support
//! resume drains in a single response (the [`crate::dispatch`] batch-limit
//! guard), which is the "maximum batch size returned over HTTP without
//! externalization" boundary.

use std::io::Read;
use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi_rpc::http::{HttpState, ARROW_CONTENT_TYPE};
use vgi_rpc::metadata::{
    REQUEST_ID_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY, STATE_KEY,
};
use vgi_rpc::wire::{md_get, StreamReader, StreamWriter};
use vgi_rpc::{Bytes, DictString, OutputCollector, Result, RpcError};

use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use crate::protocol::dtos::{BindRequest, InitRequest};
use crate::table_function::{resume, TableFunction, TableProducer};
use crate::worker::Worker;
use crate::{ipc, wire};

/// Rows per emitted batch for the test producers.
const BATCH: i64 = 10;

fn schema_n() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
}

// --- A resumable sequence producer (`test_seq`) and a non-resumable twin
//     (`test_drain`) that is identical except it declines to serialize its
//     position, so the framework must drain it. ---

struct SeqProducer {
    n: i64,
    count: i64,
    resumable: bool,
}
impl TableProducer for SeqProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.n >= self.count {
            return Ok(None);
        }
        let end = (self.n + BATCH).min(self.count);
        let vals: Vec<i64> = (self.n..end).collect();
        let batch = RecordBatch::try_new(
            schema_n(),
            vec![Arc::new(Int64Array::from(vals)) as ArrayRef],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        self.n = end;
        Ok(Some(batch))
    }
    fn resume_supported(&self) -> bool {
        self.resumable
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.n])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.n = v[0];
        }
    }
}

struct SeqFunction {
    name: &'static str,
    resumable: bool,
}
impl TableFunction for SeqFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "rows to generate")]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema_n(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, p: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(SeqProducer {
            n: 0,
            count: p.arguments.const_i64(0).unwrap_or(0).max(0),
            resumable: self.resumable,
        }))
    }
}

/// Boot the worker (both fixtures registered) on a loopback HTTP server with the
/// production batch limit of 1, and return its port. The server thread is
/// detached — it dies with the test process.
fn start_server() -> u16 {
    let mut w = Worker::new();
    w.register_table(SeqFunction {
        name: "test_seq",
        resumable: true,
    });
    w.register_table(SeqFunction {
        name: "test_drain",
        resumable: false,
    });
    let server = Arc::new(w.build_server());
    let state = HttpState::builder()
        .server(server)
        // The production value (see `transport::serve_http`): one batch per
        // producer HTTP response, then a continuation token.
        .producer_batch_limit(1)
        .build();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        rt.block_on(vgi_rpc::http::serve_with_shutdown(state, listener))
            .ok();
    });
    // Wait for the listener to start accepting.
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    port
}

/// Build the IPC-stream request body for `init`/`exchange` carrying `batch`
/// under `method`, with the RPC metadata the server expects.
fn frame(batch: &RecordBatch, method: &str, state_token: Option<&str>) -> Vec<u8> {
    let mut md = std::collections::HashMap::<String, String>::from([
        (RPC_METHOD_KEY.to_string(), method.to_string()),
        (REQUEST_VERSION_KEY.to_string(), REQUEST_VERSION.to_string()),
        (REQUEST_ID_KEY.to_string(), "test".to_string()),
    ]);
    if let Some(t) = state_token {
        md.insert(STATE_KEY.to_string(), t.to_string());
    }
    let schema = batch.schema();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::new(&mut buf, schema.as_ref()).unwrap();
        w.write(batch, Some(&md)).unwrap();
        w.finish().unwrap();
    }
    buf
}

/// The boxed `init` request body for `function(count)`.
fn init_body(function: &str, count: i64) -> Vec<u8> {
    let args = crate::arguments::Arguments::serialize_positional(&[
        Arc::new(Int64Array::from(vec![count])) as ArrayRef,
    ])
    .unwrap();
    let bind = BindRequest {
        function_name: function.to_string(),
        arguments: Bytes::from(args),
        function_type: DictString("table".to_string()),
        input_schema: None,
        settings: None,
        secrets: None,
        attach_opaque_data: None,
        transaction_opaque_data: None,
        resolved_secrets_provided: false,
        at_unit: None,
        at_value: None,
    };
    let bind_bytes = ipc::write_batch(&wire::to_batch(bind).unwrap()).unwrap();
    let init = InitRequest {
        bind_call: Bytes::from(bind_bytes),
        output_schema: Bytes::from(ipc::write_schema_ref(&schema_n()).unwrap()),
        bind_opaque_data: None,
        projection_ids: None,
        pushdown_filters: None,
        join_keys: None,
        phase: None,
        execution_id: None,
        init_opaque_data: None,
        order_by_column_name: None,
        order_by_direction: None,
        order_by_null_order: None,
        order_by_limit: None,
        tablesample_percentage: None,
        tablesample_seed: None,
        finalize_state_id: None,
    };
    let inner = ipc::write_batch(&wire::to_batch(init).unwrap()).unwrap();
    let req_schema = Arc::new(Schema::new(vec![Field::new(
        "request",
        DataType::Binary,
        false,
    )]));
    let req = RecordBatch::try_new(
        req_schema,
        vec![Arc::new(BinaryArray::from(vec![inner.as_slice()])) as ArrayRef],
    )
    .unwrap();
    frame(&req, "init", None)
}

/// The `exchange` continuation body: an empty batch carrying the state token.
fn exchange_body(token: &str) -> Vec<u8> {
    let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
    frame(&empty, "init", Some(token))
}

fn post(port: u16, path: &str, body: Vec<u8>) -> Vec<u8> {
    let url = format!("http://127.0.0.1:{port}/{path}");
    match ureq::post(&url)
        .set("Content-Type", ARROW_CONTENT_TYPE)
        .send_bytes(&body)
    {
        Ok(resp) => {
            let mut buf = Vec::new();
            resp.into_reader().read_to_end(&mut buf).unwrap();
            buf
        }
        Err(ureq::Error::Status(code, resp)) => {
            let mut body = String::new();
            resp.into_reader().read_to_string(&mut body).ok();
            panic!("POST {path} -> {code}: {body}");
        }
        Err(e) => panic!("POST {path} failed: {e}"),
    }
}

/// A parsed producer response: the `n` values it carried, the continuation
/// token if any, and the largest single data batch (rows).
struct Parsed {
    values: Vec<i64>,
    token: Option<String>,
    max_batch_rows: usize,
}

/// Parse a producer response body. The body is *concatenated* Arrow IPC streams
/// — a flat header stream (the `GlobalInitResponse`) followed by the data stream
/// ({n} batches + the continuation-token sentinel). We read every stream off one
/// cursor; only `n`-bearing batches contribute values.
fn parse(body: &[u8]) -> Parsed {
    let mut cursor = std::io::Cursor::new(body);
    let mut values = Vec::new();
    let mut token = None;
    let mut max_batch_rows = 0;
    while (cursor.position() as usize) < body.len() {
        let mut r = match StreamReader::new(&mut cursor) {
            Ok(r) => r,
            Err(_) => break,
        };
        while let Some((rb, md)) = r.read_next().unwrap() {
            if let Some(t) = md_get(&md, STATE_KEY) {
                token = Some(t.to_string());
            }
            if let Some(col) = rb
                .schema()
                .index_of("n")
                .ok()
                .and_then(|i| rb.column(i).as_any().downcast_ref::<Int64Array>())
            {
                max_batch_rows = max_batch_rows.max(col.len());
                for i in 0..col.len() {
                    values.push(col.value(i));
                }
            }
        }
    }
    Parsed {
        values,
        token,
        max_batch_rows,
    }
}

/// A resumable producer paginates: `count` rows arrive across ⌈count/BATCH⌉
/// bounded responses, each tying to the next via a continuation token, and the
/// reassembled sequence is exactly 0..count with no gaps or duplicates.
#[test]
fn resumable_scan_paginates_over_http() {
    let port = start_server();
    let count = 35; // 10 + 10 + 10 + 5 = four batches across four responses

    let mut all = Vec::new();
    let mut responses = 0i64;
    let first = parse(&post(port, "init/init", init_body("test_seq", count)));
    assert!(
        first.max_batch_rows as i64 <= BATCH,
        "first response carried {} rows (> batch limit {BATCH}) — producer drained",
        first.max_batch_rows
    );
    all.extend(first.values);
    let mut token = first.token;
    responses += 1;

    while let Some(t) = token.take() {
        let r = parse(&post(port, "init/exchange", exchange_body(&t)));
        assert!(
            r.max_batch_rows as i64 <= BATCH,
            "a continuation response carried {} rows (> batch limit {BATCH})",
            r.max_batch_rows
        );
        all.extend(r.values);
        token = r.token;
        responses += 1;
        assert!(responses <= count + 5, "continuation did not terminate");
    }

    assert_eq!(all, (0..count).collect::<Vec<_>>(), "rows or order wrong");
    // Proof of pagination (vs. a single in-memory drain): the scan spanned many
    // bounded responses. There is one response per data batch plus a terminal
    // probe — with a limit of one batch per response the producer cannot signal
    // exhaustion on the same cycle as its final batch, so a last empty response
    // discovers `None` (matching the Python/Go workers).
    let data_batches = (count + BATCH - 1) / BATCH;
    assert!(
        responses > 1,
        "scan did not paginate (drained in one response)"
    );
    assert_eq!(
        responses,
        data_batches + 1,
        "expected one bounded response per batch plus a terminal probe"
    );
}

/// A non-resumable producer drains: with no serializable position, the guard
/// makes it return the whole result set in a single response (no token), which
/// is the maximum batch size obtainable over HTTP without state externalization.
#[test]
fn non_resumable_scan_drains_in_one_response() {
    let port = start_server();
    let count = 35;

    let r = parse(&post(port, "init/init", init_body("test_drain", count)));
    assert!(
        r.token.is_none(),
        "a non-resumable producer must not mint a continuation token"
    );
    assert_eq!(r.values, (0..count).collect::<Vec<_>>());
    // All rows arrived in this single response: the whole scan was drained.
    assert!(
        r.values.len() as i64 == count,
        "drain must return the full result set in one response"
    );
}

// --- Two-phase secret bind for a TABLE-BUFFERING function. The buffering bind
//     path used to hardcode empty secret lookups, so a buffering sink could not
//     request DuckDB secrets the way scalar/table functions can. These tests
//     drive the `bind` RPC directly and assert the buffering function now both
//     triggers the lookup request (first pass) and binds normally once the
//     connector re-binds with resolved_secrets_provided. ---

use crate::buffering::{BufferingParams, TableBufferingFunction};
use crate::secrets::SecretLookup;

/// A buffering sink that needs an `s3` secret scoped to its `path` argument.
struct SecretSink;
impl TableBufferingFunction for SecretSink {
    fn name(&self) -> &str {
        "secret_sink"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("path", 0, "varchar", "destination path")]
    }
    fn secret_lookups(&self, params: &BindParams) -> Vec<SecretLookup> {
        let scope = params.arguments.const_str(0);
        vec![SecretLookup {
            secret_type: "s3".to_string(),
            scope,
            name: None,
        }]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema_n(),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, _p: &BufferingParams, _b: &RecordBatch) -> Result<Vec<u8>> {
        unimplemented!()
    }
    fn combine(&self, _p: &BufferingParams, _s: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }
    fn finalize_producer(
        &self,
        _p: &BufferingParams,
        _f: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        unimplemented!()
    }
}

fn start_secret_server() -> u16 {
    let mut w = Worker::new();
    w.register_buffering(SecretSink);
    let server = Arc::new(w.build_server());
    let state = HttpState::builder().server(server).build();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let listener = rt
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        rt.block_on(vgi_rpc::http::serve_with_shutdown(state, listener))
            .ok();
    });
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    port
}

/// Frame a unary `bind` request body for `function(path)`.
fn bind_body(function: &str, path: &str, resolved_secrets_provided: bool) -> Vec<u8> {
    let args = crate::arguments::Arguments::serialize_positional(&[Arc::new(
        arrow_array::StringArray::from(vec![path]),
    ) as ArrayRef])
    .unwrap();
    let bind = BindRequest {
        function_name: function.to_string(),
        arguments: Bytes::from(args),
        function_type: DictString("table_buffering".to_string()),
        input_schema: None,
        settings: None,
        secrets: None,
        attach_opaque_data: None,
        transaction_opaque_data: None,
        resolved_secrets_provided,
        at_unit: None,
        at_value: None,
    };
    let inner = ipc::write_batch(&wire::to_batch(bind).unwrap()).unwrap();
    let req_schema = Arc::new(Schema::new(vec![Field::new(
        "request",
        DataType::Binary,
        false,
    )]));
    let req = RecordBatch::try_new(
        req_schema,
        vec![Arc::new(BinaryArray::from(vec![inner.as_slice()])) as ArrayRef],
    )
    .unwrap();
    frame(&req, "bind", None)
}

/// Decode the `{result: binary}` envelope of a unary `bind` response into the
/// wire `BindResponse` DTO.
fn parse_bind_response(body: &[u8]) -> crate::protocol::dtos::BindResponse {
    let mut cursor = std::io::Cursor::new(body);
    let mut r = StreamReader::new(&mut cursor).unwrap();
    let (envelope, _) = r.read_next().unwrap().expect("a bind response batch");
    let col = envelope
        .column(envelope.schema().index_of("result").unwrap())
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let inner = ipc::read_batch(col.value(0)).unwrap();
    wire::from_batch::<crate::protocol::dtos::BindResponse>(&inner).unwrap()
}

/// First pass (resolved_secrets_provided=false): a buffering function with a
/// non-empty `secret_lookups` makes bind return the lookup request, scoped to
/// the path argument — so the connector knows to resolve and re-bind.
#[test]
fn buffering_bind_requests_secrets_first_pass() {
    let port = start_secret_server();
    let resp = parse_bind_response(&post(
        port,
        "bind",
        bind_body("secret_sink", "s3://bucket/out.dat", false),
    ));
    assert_eq!(resp.lookup_secret_types, vec!["s3".to_string()]);
    assert_eq!(resp.lookup_scopes, vec!["s3://bucket/out.dat".to_string()]);
    // The lookup short-circuits before on_bind, so no output schema yet.
    assert!(resp.output_schema.0.is_empty());
}

/// Second pass (resolved_secrets_provided=true): bind runs on_bind normally and
/// returns the output schema with no further secret lookups.
#[test]
fn buffering_bind_resolves_after_secrets_provided() {
    let port = start_secret_server();
    let resp = parse_bind_response(&post(
        port,
        "bind",
        bind_body("secret_sink", "s3://bucket/out.dat", true),
    ));
    assert!(resp.lookup_secret_types.is_empty());
    assert!(
        !resp.output_schema.0.is_empty(),
        "on_bind should have produced the output schema"
    );
}

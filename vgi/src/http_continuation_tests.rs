// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! End-to-end HTTP continuation tests for table-scan producers.
//!
//! These assert the property the language-agnostic DuckDB integration suite
//! cannot observe (DuckDB follows continuation tokens transparently): over HTTP
//! a resumable table scan returns ONE bounded batch per response and resumes via
//! a stateless continuation token, so the whole result set never has to fit in
//! memory — matching the Python and Go workers. A producer that does NOT support
//! resume gets exactly ONE lock-step turn: it completes inside the `/init`
//! response when it has a single batch to give, and is refused with a clear
//! error when it has more (see [`crate::dispatch`]).

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi_rpc::http::{HttpState, ARROW_CONTENT_TYPE};
use vgi_rpc::metadata::{
    CALL_STATE_KEY, REQUEST_ID_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY, STATE_KEY,
};
use vgi_rpc::wire::{md_get, StreamReader, StreamWriter};
use vgi_rpc::{Bytes, DictString, LargeBytes, OutputCollector, Result, RpcError};

use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use crate::protocol::dtos::{BindRequest, InitRequest};
use crate::table_function::{resume, TableFunction, TableProducer};
use crate::worker::Worker;
use crate::{ipc, wire};

/// Rows per emitted batch for the test producers.
const BATCH: i64 = 10;

#[test]
fn producer_can_read_the_transport_response_budget() {
    fn snapshot(out: &OutputCollector) -> (Option<usize>, Option<usize>) {
        (out.response_limit_bytes(), out.preferred_response_bytes())
    }

    // Keeping this as a typed function pointer makes the producer-facing API
    // part of this crate's compile gate without needing to construct the
    // transport-owned collector directly.
    let _: fn(&OutputCollector) -> (Option<usize>, Option<usize>) = snapshot;
}

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
        FunctionMetadata {
            filter_pushdown: true,
            auto_apply_filters: true,
            ..Default::default()
        }
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
    w.register_table_in_out(MultiBatchFinishFunction);
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
    init_body_with_filter(function, count, None, None)
}

fn init_body_with_filter(
    function: &str,
    count: i64,
    pushdown_filters: Option<Vec<u8>>,
    join_keys: Option<Vec<Vec<u8>>>,
) -> Vec<u8> {
    init_body_for_schema(function, count, &schema_n(), pushdown_filters, join_keys)
}

/// The boxed `init` request body for `function(count)`, whose bind output
/// schema is `output_schema`.
fn init_body_for_schema(
    function: &str,
    count: i64,
    output_schema: &SchemaRef,
    pushdown_filters: Option<Vec<u8>>,
    join_keys: Option<Vec<Vec<u8>>>,
) -> Vec<u8> {
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
        // The extension names the owning schema on every bind (protocol 1.1.0);
        // these functions are registered without an explicit home, so they live
        // in the worker's own catalog under `main`.
        schema_path: Some(vec![crate::catalog::MAIN_SCHEMA.to_string()]),
        argument_names: Some(vec![Some("count".to_string())]),
    };
    let bind_bytes = ipc::write_batch(&wire::to_batch(bind).unwrap()).unwrap();
    let init = InitRequest {
        bind_call: Bytes::from(bind_bytes),
        output_schema: Bytes::from(ipc::write_schema_ref(output_schema).unwrap()),
        bind_opaque_data: None,
        projection_ids: None,
        pushdown_filters: pushdown_filters.map(LargeBytes),
        join_keys: join_keys.map(|batches| batches.into_iter().map(LargeBytes).collect()),
        phase: None,
        execution_id: None,
        init_opaque_data: None,
        substream_id: None,
        order_by_column_name: None,
        order_by_direction: None,
        order_by_null_order: None,
        order_by_limit: None,
        tablesample_percentage: None,
        tablesample_seed: None,
        finalize_state_id: None,
        split_tokens: None,
        row_limit: None,
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

fn join_key_filter(values: &[i64]) -> (Vec<u8>, Vec<Vec<u8>>) {
    join_key_filter_named(values, "n")
}

fn join_key_filter_named(values: &[i64], column_name: &str) -> (Vec<u8>, Vec<Vec<u8>>) {
    let filter_schema = Arc::new(
        Schema::new(vec![Field::new("filter_spec", DataType::Utf8, false)]).with_metadata(
            [
                (
                    "vgi_filter_encoding".to_string(),
                    "vgi.filters.v2".to_string(),
                ),
                ("vgi_filter_version".to_string(), "2".to_string()),
                (
                    "vgi_evaluation_context".to_string(),
                    "vgi.none.v1".to_string(),
                ),
            ]
            .into_iter()
            .collect(),
        ),
    );
    let filter = RecordBatch::try_new(
        filter_schema,
        vec![Arc::new(StringArray::from(vec![format!(
            r#"{{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{{"id":"join-n","revision":0,"mode":"required","source":"join","expression":{{"node":"in","expression":{{"node":"column_ref","column_index":0,"column_name":"{column_name}"}},"set":{{"kind":"external","batch_index":0,"column_index":0,"column_name":"n"}},"negated":false}}}}]}}"#,
        )])) as ArrayRef],
    )
    .unwrap();

    let keys_schema = Arc::new(
        Schema::new(vec![Field::new("n", DataType::Int64, true)]).with_metadata(
            [("vgi_join_keys_version".to_string(), "2".to_string())]
                .into_iter()
                .collect(),
        ),
    );
    let keys = RecordBatch::try_new(
        keys_schema,
        vec![Arc::new(Int64Array::from(values.to_vec())) as ArrayRef],
    )
    .unwrap();
    (
        ipc::write_batch(&filter).unwrap(),
        vec![ipc::write_batch(&keys).unwrap()],
    )
}

/// The `exchange` continuation body: an empty batch carrying the state token.
fn exchange_body(token: &str) -> Vec<u8> {
    let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
    frame(&empty, "init", Some(token))
}

/// A continuation carrying the stream's call token as well as its state token,
/// the way a real client does. Only needed when the continuation may land on an
/// instance other than the one that minted it — the minting instance answers
/// from its own call cache, an instance that never saw the stream cannot.
fn exchange_body_with_call(token: &str, call_state: Option<&str>) -> Vec<u8> {
    let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let schema = empty.schema();
    let mut md = std::collections::HashMap::<String, String>::from([
        (RPC_METHOD_KEY.to_string(), "init".to_string()),
        (REQUEST_VERSION_KEY.to_string(), REQUEST_VERSION.to_string()),
        (REQUEST_ID_KEY.to_string(), "test".to_string()),
        (STATE_KEY.to_string(), token.to_string()),
    ]);
    if let Some(call) = call_state {
        md.insert(CALL_STATE_KEY.to_string(), call.to_string());
    }
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::new(&mut buf, schema.as_ref()).unwrap();
        w.write(&empty, Some(&md)).unwrap();
        w.finish().unwrap();
    }
    buf
}

fn post(port: u16, path: &str, body: Vec<u8>) -> Vec<u8> {
    let url = format!("http://127.0.0.1:{port}/{path}");
    match ureq::post(&url)
        .header("Content-Type", ARROW_CONTENT_TYPE)
        .send(&body[..])
    {
        Ok(mut resp) => resp.body_mut().read_to_vec().unwrap(),
        Err(ureq::Error::StatusCode(code)) => {
            panic!("POST {path} -> {code}");
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
    /// The stream's call token. A continuation carries it alongside the state
    /// token; a server that did not mint it has no cached call to resolve
    /// without it, which is what makes a cross-instance resume possible.
    call_state: Option<String>,
}

/// Parse a producer response body. The body is *concatenated* Arrow IPC streams
/// — a flat header stream (the `GlobalInitResponse`) followed by the data stream
/// ({n} batches + the continuation-token sentinel). We read every stream off one
/// cursor; only `n`-bearing batches contribute values.
fn parse(body: &[u8]) -> Parsed {
    let mut cursor = std::io::Cursor::new(body);
    let mut values = Vec::new();
    let mut token = None;
    let mut call_state = None;
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
            if let Some(t) = md_get(&md, CALL_STATE_KEY) {
                call_state = Some(t.to_string());
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
        call_state,
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

/// Init-time join-key side batches survive every stateless HTTP rebuild. Before
/// they were folded into `ExchangeBlob`, only the first response was filtered;
/// resumed batches silently lost the values referenced by the filter AST.
#[test]
fn join_key_filter_survives_http_continuations() {
    let port = start_server();
    let count = 35;
    let expected = vec![1, 12, 23, 34];
    let (filter, join_keys) = join_key_filter(&expected);

    let first = parse(&post(
        port,
        "init/init",
        init_body_with_filter("test_seq", count, Some(filter), Some(join_keys)),
    ));
    let mut all = first.values;
    let mut token = first.token;
    let mut responses = 1;
    while let Some(t) = token.take() {
        let response = parse(&post(port, "init/exchange", exchange_body(&t)));
        all.extend(response.values);
        token = response.token;
        responses += 1;
        assert!(responses <= count + 5, "continuation did not terminate");
    }

    assert_eq!(all, expected);
    assert!(responses > 1, "the filtered scan did not exercise resume");
}

/// Worker init must bind every v2 column reference against the unprojected
/// bind output schema. A matching numeric index with a mismatched name is not
/// allowed to reach user code or wait until the first produced batch.
#[test]
fn worker_init_rejects_filter_column_name_mismatch() {
    let port = start_server();
    let (filter, join_keys) = join_key_filter_named(&[1], "not_n");

    let raw = post(
        port,
        "init/init",
        init_body_with_filter("test_seq", 5, Some(filter), Some(join_keys)),
    );
    let text = String::from_utf8_lossy(&raw);
    assert!(
        text.contains("column_ref name does not match authoritative index"),
        "expected strict bind-schema rejection, got: {text}"
    );
}

/// A non-resumable producer with ONE batch still completes over HTTP, in a
/// single response and with no continuation token.
///
/// This is the half of the pre-0.23 contract that had to survive. While
/// `ProducerState::batch_limit` existed, returning `Some(0)` let a
/// non-resumable producer drain its whole result in one response, so no
/// continuation was ever needed. 0.23 made producers strictly lock-step and
/// removed the knob — and with it, the one-batch case broke too: `produce`
/// emitted the batch, left the stream unfinished, and the framework asked for
/// a cursor the producer could not mint. That is a large class of ordinary
/// functions (every fixture that returns a single batch), not an exotic one.
///
/// Confirming exhaustion inside the same turn restores it without reintroducing
/// draining: exactly one batch is still emitted per response.
#[test]
fn single_batch_non_resumable_scan_completes_in_one_response() {
    let port = start_server();
    let count = BATCH / 2; // one batch, comfortably under the per-batch size

    let r = parse(&post(port, "init/init", init_body("test_drain", count)));
    assert_eq!(
        r.values,
        (0..count).collect::<Vec<_>>(),
        "the whole result must arrive in the init response"
    );
    assert!(
        r.token.is_none(),
        "a completed scan must not mint a continuation token it cannot honour"
    );
}

/// A non-resumable producer with MORE than one batch is REFUSED over HTTP,
/// rather than quietly repeating its first batch.
///
/// vgi-rpc 0.23.0 made HTTP producers strictly lock-step: one invocation, at
/// most one data batch per response. A second batch therefore needs a
/// continuation, and for a producer with no serialized scan position that is
/// not merely slower — it is wrong. Measured before this test was rewritten: a
/// 35-row scan returned rows 0..9 with a token, and the token rebuilt the
/// producer at row 0, so following it yielded 0..9 again, forever.
///
/// So the only honest answer is to refuse, which the framework renders as an
/// error envelope. A worker that needs this shape uses a byte-stream transport
/// or implements resume.
#[test]
fn non_resumable_scan_is_refused_over_http() {
    let port = start_server();
    let count = 35;

    let raw = post(port, "init/init", init_body("test_drain", count));
    let r = parse(&raw);
    let text = String::from_utf8_lossy(&raw);

    // The refusal must arrive as a first-class error, not as a short read that
    // a client would mistake for the end of the scan.
    assert!(
        text.contains("cannot serve an HTTP continuation"),
        "expected a resumability refusal naming the cause"
    );
    assert!(
        r.values.len() < count as usize,
        "a refused scan must not claim to have returned every row"
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
        // The extension names the owning schema on every bind (protocol 1.1.0);
        // these functions are registered without an explicit home, so they live
        // in the worker's own catalog under `main`.
        schema_path: Some(vec![crate::catalog::MAIN_SCHEMA.to_string()]),
        argument_names: Some(vec![Some("path".to_string())]),
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

/// A table-in-out function whose FINALIZE flush is MORE THAN ONE batch.
///
/// This shape had no fixture in any SDK, which is why nothing caught that it
/// was a hard error over HTTP: the dispatcher builds a `VecProducer` for the
/// flush, and a producer that cannot resume gets exactly one turn — so the
/// second batch tripped "emits more than one batch but cannot serve an HTTP
/// continuation". Every existing finalize fixture returns `vec![batch]`.
struct MultiBatchFinishFunction;

impl crate::table_in_out::TableInOutFunction for MultiBatchFinishFunction {
    fn name(&self) -> &str {
        "test_multi_finish"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        // A positional arg is not incidental: `Arguments::serialize_positional`
        // over an empty slice writes an IPC stream with no record batch, which
        // the bind decoder rejects with "ipc stream had no record batch".
        vec![ArgSpec::const_arg(
            "n_batches",
            0,
            "int64",
            "Batches the finalize flush emits",
        )]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema_n(),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, _p: &ProcessParams, _b: &RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(Vec::new())
    }
    fn has_finish(&self) -> bool {
        true
    }
    fn finish(&self, p: &ProcessParams) -> Result<Vec<RecordBatch>> {
        let n = p.arguments.const_i64(0).unwrap_or(FINISH_BATCHES);
        // One row per batch, so batch boundaries are unambiguous in the assert.
        Ok((0..n)
            .map(|i| {
                RecordBatch::try_new(
                    schema_n(),
                    vec![Arc::new(Int64Array::from(vec![i])) as ArrayRef],
                )
                .unwrap()
            })
            .collect())
    }
}

const FINISH_BATCHES: i64 = 4;

/// The `init` body for a FINALIZE-phase call on a table-in-out function.
fn finalize_init_body(function: &str) -> Vec<u8> {
    finalize_init_body_sized(function, FINISH_BATCHES, b"finalize-exec")
}

/// As [`finalize_init_body`], but with the flush size and the execution id
/// spelled out. Two streams sharing one execution id is the parallel-query
/// shape; a big flush is what separates a linear drain from a quadratic one.
fn finalize_init_body_sized(function: &str, n_batches: i64, execution_id: &[u8]) -> Vec<u8> {
    let args = crate::arguments::Arguments::serialize_positional(&[
        Arc::new(Int64Array::from(vec![n_batches])) as ArrayRef,
    ])
    .unwrap();
    let bind = BindRequest {
        function_name: function.to_string(),
        arguments: Bytes::from(args),
        function_type: DictString("table".to_string()),
        // A table-in-out bind carries the input schema; FINALIZE reuses the
        // same bind call the INPUT phase used.
        input_schema: Some(Bytes::from(ipc::write_schema_ref(&schema_n()).unwrap())),
        settings: None,
        secrets: None,
        attach_opaque_data: None,
        transaction_opaque_data: None,
        resolved_secrets_provided: false,
        at_unit: None,
        at_value: None,
        schema_path: Some(vec![crate::catalog::MAIN_SCHEMA.to_string()]),
        argument_names: Some(vec![Some("count".to_string())]),
    };
    let bind_bytes = ipc::write_batch(&wire::to_batch(bind).unwrap()).unwrap();
    let init = InitRequest {
        bind_call: Bytes::from(bind_bytes),
        output_schema: Bytes::from(ipc::write_schema_ref(&schema_n()).unwrap()),
        bind_opaque_data: None,
        projection_ids: None,
        pushdown_filters: None,
        join_keys: None,
        phase: Some(DictString(
            crate::protocol::enums::phase::FINALIZE.to_string(),
        )),
        execution_id: Some(Bytes::from(execution_id.to_vec())),
        init_opaque_data: None,
        substream_id: None,
        order_by_column_name: None,
        order_by_direction: None,
        order_by_null_order: None,
        order_by_limit: None,
        tablesample_percentage: None,
        tablesample_seed: None,
        finalize_state_id: None,
        split_tokens: None,
        row_limit: None,
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

/// A multi-batch table-in-out FINALIZE flush paginates over HTTP instead of
/// failing — every row exactly once, in order, one batch per response.
///
/// Before the fix this returned an error naming the producer, because the flush
/// producer declared `resume_supported() = false` AND was built with no rebuild
/// blob. The fix does NOT re-run `finish()` to rebuild: `finish()` drains
/// accumulated partials, so a second call is not obliged to return the same
/// rows. The flush is persisted once at init and the continuation replays it,
/// with only the position in the token.
#[test]
fn multi_batch_finalize_paginates_over_http() {
    let port = start_server();

    let mut all = Vec::new();
    let mut responses = 0i64;
    let first = parse(&post(
        port,
        "init/init",
        finalize_init_body("test_multi_finish"),
    ));
    assert!(
        first.max_batch_rows <= 1,
        "first finalize response carried {} rows — the flush drained instead of paginating",
        first.max_batch_rows
    );
    all.extend(first.values);
    let mut token = first.token;
    responses += 1;

    while let Some(t) = token.take() {
        let r = parse(&post(port, "init/exchange", exchange_body(&t)));
        all.extend(r.values);
        token = r.token;
        responses += 1;
        assert!(
            responses <= FINISH_BATCHES + 5,
            "finalize continuation did not terminate"
        );
    }

    assert_eq!(
        all,
        (0..FINISH_BATCHES).collect::<Vec<_>>(),
        "finalize rows or order wrong — a replayed flush must not drop or repeat rows"
    );
    assert!(
        responses > 1,
        "finalize did not paginate (drained in one response)"
    );
}

// ---------------------------------------------------------------------------
// The FINALIZE flush must cost the same on turn 5000 as on turn 1
// ---------------------------------------------------------------------------
//
// Over HTTP a producer is strictly lock-step: one batch per response, and the
// next turn rebuilds the producer from a continuation token. A flush of N
// batches therefore takes N turns — so anything a turn does in proportion to
// the FLUSH SIZE is paid N times, and the drain is O(N^2).
//
// That is not hypothetical. Storing the flush as one blob and decoding all of
// it to emit the batch at the cursor cost 51.3s for a 5000-row flush and 97.4s
// for 6400 — growth per doubling 4.6x, quadratic — against 2.6s and 3.4s once a
// turn read only the row at its cursor, and 8.7s for the Python reference
// (which scans exactly one state-log row per tick, and grew at 2.00x). The
// byte-stream transports never serialize a token, so only HTTP paid.
//
// These tests pin the cost SHAPE rather than a wall-clock number, so they are
// cheap and not load-sensitive: a couple of hundred batches already separate
// linear from quadratic by two orders of magnitude, and there is no reason to
// make the suite drain five thousand.

/// Read/write totals observed by [`CountingStorage`].
#[derive(Default)]
struct StorageCounts {
    read_calls: std::sync::atomic::AtomicU64,
    read_bytes: std::sync::atomic::AtomicU64,
    written_bytes: std::sync::atomic::AtomicU64,
}

impl StorageCounts {
    fn read_bytes(&self) -> u64 {
        self.read_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn read_calls(&self) -> u64 {
        self.read_calls.load(std::sync::atomic::Ordering::Relaxed)
    }
    fn written_bytes(&self) -> u64 {
        self.written_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// A [`FunctionStorage`](crate::storage::FunctionStorage) that counts the bytes
/// a drain reads back. Wall-clock timing would be the flaky way to ask the same
/// question; bytes-read is exact, and it is the quantity that actually grew.
struct CountingStorage {
    inner: crate::storage::MemoryStorage,
    counts: Arc<StorageCounts>,
}

impl CountingStorage {
    fn new() -> (Arc<Self>, Arc<StorageCounts>) {
        let counts = Arc::new(StorageCounts::default());
        (
            Arc::new(CountingStorage {
                inner: crate::storage::MemoryStorage::new(),
                counts: counts.clone(),
            }),
            counts,
        )
    }
}

impl crate::storage::FunctionStorage for CountingStorage {
    fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        use std::sync::atomic::Ordering::Relaxed;
        let v = self.inner.kv_get(scope, key);
        self.counts.read_calls.fetch_add(1, Relaxed);
        self.counts
            .read_bytes
            .fetch_add(v.as_ref().map_or(0, |b| b.len()) as u64, Relaxed);
        v
    }
    fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]) {
        self.counts
            .written_bytes
            .fetch_add(value.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.inner.kv_put(scope, key, value)
    }
    fn kv_del(&self, scope: &[u8], key: &[u8]) {
        self.inner.kv_del(scope, key)
    }
    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
        self.counts
            .written_bytes
            .fetch_add(value.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.inner.append(scope, ns, key, value)
    }
    fn scan(
        &self,
        scope: &[u8],
        ns: &[u8],
        key: &[u8],
        after_id: i64,
        limit: usize,
    ) -> Vec<(i64, Vec<u8>)> {
        use std::sync::atomic::Ordering::Relaxed;
        let rows = self.inner.scan(scope, ns, key, after_id, limit);
        self.counts.read_calls.fetch_add(1, Relaxed);
        self.counts.read_bytes.fetch_add(
            rows.iter().map(|(_, v)| v.len() as u64).sum::<u64>(),
            Relaxed,
        );
        rows
    }
    fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]) {
        self.inner.queue_push(scope, items)
    }
    fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>> {
        self.inner.queue_pop(scope)
    }
    fn clear(&self, scope: &[u8]) {
        self.inner.clear(scope)
    }
}

/// A fixed token key, so two independently built servers can open each other's
/// continuation tokens — the cold-worker test needs that, and the default is a
/// per-process ephemeral key.
const TEST_TOKEN_KEY: &[u8; 32] = b"vgi-finalize-flush-test-key-0123";

/// Boot a worker carrying only the multi-batch finalize fixture, on the given
/// store, with the production producer batch limit (one batch per response).
fn start_finalize_server(store: crate::storage::SharedStorage) -> u16 {
    let mut w = Worker::new();
    w.set_storage(store);
    w.register_table_in_out(MultiBatchFinishFunction);
    let server = Arc::new(w.build_server());
    let state = HttpState::builder()
        .server(server)
        .producer_batch_limit(1)
        .token_key(TEST_TOKEN_KEY)
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
    for _ in 0..100 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    port
}

/// Drain a whole finalize flush over HTTP, following continuations to the end.
/// Returns every `n` value in arrival order and the number of responses.
fn drain_finalize(port: u16, n_batches: i64, execution_id: &[u8]) -> (Vec<i64>, usize) {
    let first = parse(&post(
        port,
        "init/init",
        finalize_init_body_sized("test_multi_finish", n_batches, execution_id),
    ));
    let mut all = first.values;
    let mut token = first.token;
    let mut responses = 1usize;
    while let Some(t) = token.take() {
        let r = parse(&post(port, "init/exchange", exchange_body(&t)));
        all.extend(r.values);
        token = r.token;
        responses += 1;
        assert!(
            responses <= n_batches as usize + 5,
            "finalize continuation did not terminate"
        );
    }
    (all, responses)
}

/// Drain one flush and report what the whole drain cost in storage reads.
fn drain_cost(n_batches: i64) -> (Arc<StorageCounts>, usize) {
    let (store, counts) = CountingStorage::new();
    let port = start_finalize_server(store);
    let (values, responses) = drain_finalize(port, n_batches, b"cost-exec");
    assert_eq!(
        values,
        (0..n_batches).collect::<Vec<_>>(),
        "a {n_batches}-batch flush drained wrong rows"
    );
    (counts, responses)
}

/// THE regression guard. Quadrupling the flush must roughly quadruple the work,
/// not multiply it by sixteen.
///
/// Reading the whole flush on each of its N turns is O(N^2): 4x the batches
/// means 4x the turns each reading 4x the bytes. Reading only the batch at the
/// cursor is O(N). The two are far enough apart that a loose threshold still
/// separates them decisively — no timing, no tuning.
#[test]
fn finalize_flush_drain_is_linear_in_flush_size() {
    let (small, _) = drain_cost(64);
    let (large, _) = drain_cost(256);

    let ratio = large.read_bytes() as f64 / small.read_bytes().max(1) as f64;
    assert!(
        ratio < 8.0,
        "quadrupling the finalize flush multiplied storage reads by {ratio:.1}x \
         (64 batches -> {} bytes, 256 -> {} bytes). Linear is ~4x and quadratic is ~16x: \
         a drain turn must read only the batch at its cursor, not the whole flush. \
         Over a 5000-batch flush that difference was 51.3s against 2.6s.",
        small.read_bytes(),
        large.read_bytes()
    );
}

/// The other half of the same property, stated absolutely rather than as a
/// growth rate: draining a flush reads it ONCE, not once per turn.
#[test]
fn finalize_flush_drain_reads_each_batch_once() {
    let (counts, responses) = drain_cost(256);
    let written = counts.written_bytes();
    assert!(written > 0, "the flush was never persisted");
    assert!(
        counts.read_bytes() < written * 4,
        "draining a {written}-byte flush read {} bytes back — the drain re-reads the \
         whole flush on every one of its {responses} turns instead of reading the \
         batch at its cursor",
        counts.read_bytes()
    );
    // One read per turn, plus the init turn's own. Anything proportional to the
    // flush size per turn would blow past this.
    assert!(
        counts.read_calls() <= responses as u64 + 4,
        "{} storage reads across {responses} turns: a turn must take one",
        counts.read_calls()
    );
}

/// Two finalize substreams under ONE execution id must not drain each other.
///
/// This is the parallel-query shape: DuckDB opens a finalize substream per
/// thread and they share the execution id. A flush stored under a key that is
/// only execution-scoped means the second stream's flush overwrites the first's,
/// and the first then drains the second's rows — a wrong COUNT(*), which is
/// exactly what the integration fixture watches for.
#[test]
fn concurrent_finalize_flushes_do_not_drain_each_other() {
    let (store, _) = CountingStorage::new();
    let port = start_finalize_server(store);
    let exec = b"shared-exec";

    // Open both streams BEFORE draining either, so a shared key has already
    // clobbered the first stream's flush by the time it is read.
    let a = parse(&post(
        port,
        "init/init",
        finalize_init_body_sized("test_multi_finish", 3, exec),
    ));
    let b = parse(&post(
        port,
        "init/init",
        finalize_init_body_sized("test_multi_finish", 7, exec),
    ));

    fn finish(port: u16, first: Parsed) -> Vec<i64> {
        let mut all = first.values;
        let mut token = first.token;
        let mut turns = 0;
        while let Some(t) = token.take() {
            let r = parse(&post(port, "init/exchange", exchange_body(&t)));
            all.extend(r.values);
            token = r.token;
            turns += 1;
            assert!(turns < 64, "finalize continuation did not terminate");
        }
        all
    }

    // Interleaved: finish the SECOND one first, then the first.
    let vals_b = finish(port, b);
    let vals_a = finish(port, a);
    assert_eq!(
        vals_b,
        (0..7).collect::<Vec<_>>(),
        "the 7-batch stream drained the wrong rows"
    );
    assert_eq!(
        vals_a,
        (0..3).collect::<Vec<_>>(),
        "the 3-batch stream drained the other stream's rows: the flush must be keyed \
         per STREAM, not per execution id"
    );
}

/// Stateless resume, which the offload must not cost us: a continuation may
/// arrive at a worker process that has never seen this stream, and must work
/// from the token plus the shared store alone.
///
/// Here a SECOND, independently built worker — its own dispatcher, holding none
/// of the first's state — finishes draining a flush the first one started. It
/// works because the batches live in `FunctionStorage` rather than in process
/// memory.
#[test]
fn finalize_flush_resumes_on_a_second_worker() {
    let (store, _) = CountingStorage::new();
    let port_a = start_finalize_server(store.clone());
    let port_b = start_finalize_server(store);

    let first = parse(&post(
        port_a,
        "init/init",
        finalize_init_body_sized("test_multi_finish", 8, b"cold-exec"),
    ));
    let mut all = first.values;
    let mut token = first.token;
    let mut call = first.call_state;
    assert!(token.is_some(), "an 8-batch flush must paginate");

    // Every continuation goes to the OTHER worker.
    let mut turns = 0;
    while let Some(t) = token.take() {
        let r = parse(&post(
            port_b,
            "init/exchange",
            exchange_body_with_call(&t, call.as_deref()),
        ));
        all.extend(r.values);
        token = r.token;
        call = r.call_state.or(call);
        turns += 1;
        assert!(turns < 32, "finalize continuation did not terminate");
    }
    assert_eq!(
        all,
        (0..8).collect::<Vec<_>>(),
        "a worker that never saw this stream could not finish it: resume must depend \
         only on the token and the shared store"
    );
}

// --- Dynamic filters across HTTP turns. Each turn of an HTTP producer rebuilds
//     its filters from the tokens: the init snapshot plus the deltas the cursor
//     carries. The cursor used to append every tick's delta and replay all of
//     them on every turn — quadratic in the tick count, with a token growing
//     every tick. These tests play the client (every request is hand-built; every
//     response is the worker's) against two workers sharing a token key, sending
//     each continuation to the worker that did NOT serve the previous turn, so
//     every turn's filters come from the tokens alone. ---

/// `{n, pushed_filters}` — the dynamic-filter echo's output.
fn echo_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("pushed_filters", DataType::Utf8, true),
    ]))
}

/// Descending integers `count-1 ..= 0`, BATCH per batch; each batch's
/// `pushed_filters` echoes the filters the producer was handed for that tick.
struct EchoProducer {
    count: i64,
    offset: i64,
    witness: String,
}
impl TableProducer for EchoProducer {
    fn on_dynamic_filters(&mut self, filters: Option<&crate::pushdown::PushdownFilters>) {
        if let Some(f) = filters {
            self.witness = f.format_repr();
        }
    }
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.offset >= self.count {
            return Ok(None);
        }
        let end = (self.offset + BATCH).min(self.count);
        let n: Int64Array = (self.offset..end).map(|i| self.count - 1 - i).collect();
        let witness = StringArray::from(vec![self.witness.clone(); (end - self.offset) as usize]);
        self.offset = end;
        RecordBatch::try_new(echo_schema(), vec![Arc::new(n), Arc::new(witness)])
            .map(Some)
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.offset])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.offset = v[0];
        }
    }
}

struct EchoFunction;
impl TableFunction for EchoFunction {
    fn name(&self) -> &str {
        "test_filter_echo"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            filter_pushdown: true,
            auto_apply_filters: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "rows to generate")]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: echo_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, p: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(EchoProducer {
            count: p.arguments.const_i64(0).unwrap_or(0).max(0),
            offset: 0,
            witness: p
                .current_pushdown_filters
                .as_ref()
                .map(|f| f.format_repr())
                .unwrap_or_default(),
        }))
    }
}

/// Two workers serving the echo under one token key: a continuation minted by
/// either can be served by the other.
fn start_echo_servers() -> [u16; 2] {
    std::array::from_fn(|_| {
        let mut w = Worker::new();
        w.register_table(EchoFunction);
        let server = Arc::new(w.build_server());
        let state = HttpState::builder()
            .server(server)
            .producer_batch_limit(1)
            .token_key(TEST_TOKEN_KEY)
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
        for _ in 0..100 {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        port
    })
}

/// One Filter Encoding v2 document batch (`filter_spec` + int64 `value_i`
/// payload columns), IPC-encoded the way the C++ client frames it.
fn filter_document(json: &str, values: &[i64]) -> Vec<u8> {
    let mut fields = vec![Field::new("filter_spec", DataType::Utf8, false)];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![json]))];
    for (i, value) in values.iter().enumerate() {
        fields.push(Field::new(format!("value_{i}"), DataType::Int64, true));
        arrays.push(Arc::new(Int64Array::from(vec![*value])));
    }
    let metadata = [
        ("vgi_filter_encoding", "vgi.filters.v2"),
        ("vgi_filter_version", "2"),
        ("vgi_evaluation_context", "vgi.none.v1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    let schema = Arc::new(Schema::new(fields).with_metadata(metadata));
    ipc::write_batch(&RecordBatch::try_new(schema, arrays).unwrap()).unwrap()
}

fn empty_snapshot() -> Vec<u8> {
    filter_document(
        r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[]}"#,
        &[],
    )
}

/// An advisory `n <op> value_<value_ref>` upsert.
fn upsert(id: &str, revision: u64, op: &str, value_ref: u64) -> String {
    format!(
        r#"{{"operation":"upsert","id":"{id}","revision":{revision},"mode":"advisory","source":"top_n","expression":{{"node":"comparison","op":"{op}","left":{{"node":"column_ref","column_index":0,"column_name":"n"}},"right":{{"node":"literal","value_ref":{value_ref}}}}}}}"#
    )
}

fn remove(id: &str, revision: u64) -> String {
    format!(r#"{{"operation":"remove","id":"{id}","revision":{revision}}}"#)
}

/// A tick's `vgi_pushdown_filters` value: one delta, standard base64.
fn delta(updates: &[String], values: &[i64]) -> String {
    let json = format!(
        r#"{{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"delta","updates":[{}]}}"#,
        updates.join(",")
    );
    base64(&filter_document(&json, values))
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let bits = chunk
            .iter()
            .enumerate()
            .fold(0u32, |acc, (i, b)| acc | (u32::from(*b) << (16 - 8 * i)));
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(ALPHABET[(bits >> (18 - 6 * i)) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// A parsed echo response.
struct EchoTurn {
    values: Vec<i64>,
    witnesses: Vec<String>,
    token: Option<String>,
    call_state: Option<String>,
}

fn parse_echo(body: &[u8]) -> EchoTurn {
    let mut cursor = std::io::Cursor::new(body);
    let mut turn = EchoTurn {
        values: Vec::new(),
        witnesses: Vec::new(),
        token: None,
        call_state: None,
    };
    while (cursor.position() as usize) < body.len() {
        let Ok(mut r) = StreamReader::new(&mut cursor) else {
            break;
        };
        while let Some((rb, md)) = r.read_next().unwrap() {
            if let Some(t) = md_get(&md, STATE_KEY) {
                turn.token = Some(t.to_string());
            }
            if let Some(t) = md_get(&md, CALL_STATE_KEY) {
                turn.call_state = Some(t.to_string());
            }
            let schema = rb.schema();
            let (Ok(n), Ok(w)) = (schema.index_of("n"), schema.index_of("pushed_filters")) else {
                continue;
            };
            let n = rb.column(n).as_any().downcast_ref::<Int64Array>().unwrap();
            let w = rb.column(w).as_any().downcast_ref::<StringArray>().unwrap();
            turn.values.extend(n.values().iter().copied());
            turn.witnesses
                .extend((0..w.len()).map(|i| w.value(i).to_string()));
        }
    }
    turn
}

/// A client-side echo stream: the init turn, then one continuation per `tick`.
struct EchoStream {
    ports: [u16; 2],
    turns: usize,
    token: String,
    call_state: Option<String>,
    /// The size of every continuation request sent, in order.
    request_bytes: Vec<usize>,
}

impl EchoStream {
    fn open(ports: [u16; 2], count: i64) -> (Self, EchoTurn) {
        let first = parse_echo(&post(
            ports[0],
            "init/init",
            init_body_for_schema(
                "test_filter_echo",
                count,
                &echo_schema(),
                Some(empty_snapshot()),
                None,
            ),
        ));
        assert_eq!(first.values.len(), BATCH as usize, "the init turn's batch");
        let stream = EchoStream {
            ports,
            turns: 1,
            token: first
                .token
                .clone()
                .expect("a paginating scan mints a cursor"),
            call_state: first.call_state.clone(),
            request_bytes: Vec::new(),
        };
        (stream, first)
    }

    /// Send one continuation, carrying `delta` as its tick metadata, to the
    /// worker that did not serve the previous turn.
    fn tick(&mut self, delta: Option<String>) -> EchoTurn {
        let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let mut md = std::collections::HashMap::<String, String>::from([
            (RPC_METHOD_KEY.to_string(), "init".to_string()),
            (REQUEST_VERSION_KEY.to_string(), REQUEST_VERSION.to_string()),
            (REQUEST_ID_KEY.to_string(), "test".to_string()),
            (STATE_KEY.to_string(), self.token.clone()),
        ]);
        if let Some(call) = &self.call_state {
            md.insert(CALL_STATE_KEY.to_string(), call.clone());
        }
        if let Some(delta) = delta {
            md.insert("vgi_pushdown_filters".to_string(), delta);
        }
        let mut body = Vec::new();
        {
            let mut w = StreamWriter::new(&mut body, empty.schema().as_ref()).unwrap();
            w.write(&empty, Some(&md)).unwrap();
            w.finish().unwrap();
        }
        self.request_bytes.push(body.len());
        let port = self.ports[self.turns % 2];
        self.turns += 1;
        let raw = post(port, "init/exchange", body);
        let turn = parse_echo(&raw);
        assert!(
            !turn.values.is_empty(),
            "turn {} returned no rows: {}",
            self.turns,
            String::from_utf8_lossy(&raw)
        );
        self.token = turn.token.clone().expect("the scan is not exhausted");
        self.call_state = turn.call_state.clone().or(self.call_state.take());
        turn
    }
}

/// A tightening Top-N bound sends a delta on every tick; the continuation must
/// not grow with the tick count. Rows descend from 999 in batches of 10 and tick
/// `r` narrows the bound to `n < 1000 - 10r`, exactly the rows it returns — so
/// every tick's delta changes the live predicate, the case the client cannot
/// skip.
///
/// Before compaction every tick appended its delta to the cursor: the
/// continuation request grew every turn, and turn `k` replayed `k` deltas.
#[test]
fn dynamic_filter_continuation_stays_flat_as_ticks_accumulate() {
    let (mut stream, _) = EchoStream::open(start_echo_servers(), 1000);
    let ticks = 90u64;
    for revision in 1..=ticks {
        let bound = 1000 - 10 * revision as i64;
        let turn = stream.tick(Some(delta(
            &[upsert("top_n:0", revision, "lt", 0)],
            &[bound],
        )));
        assert_eq!(
            turn.values,
            (bound - 10..bound).rev().collect::<Vec<_>>(),
            "tick {revision}"
        );
        assert_eq!(
            turn.witnesses[0],
            format!("PushdownFilters([ConstantFilter(n < {bound})])"),
            "tick {revision}: the rebuilt filters must be exactly the live bound"
        );
    }
    // The first request carries no delta yet in its cursor; from the second on,
    // the cursor carries the one live delta. It must stay that size.
    let sizes = &stream.request_bytes;
    let settled = sizes[1];
    let largest = *sizes[1..].iter().max().unwrap();
    assert!(
        largest <= settled + 64,
        "the continuation grew with the tick count: {settled} bytes at tick 2, \
         {largest} at worst, {} at tick {ticks} (every size: {sizes:?})",
        sizes[sizes.len() - 1]
    );
}

/// Resending a revision with another value is a stale no-op: it filters
/// nothing, and it is not kept for replay.
#[test]
fn a_resent_revision_is_stale_and_adds_nothing_to_replay() {
    let (mut stream, _) = EchoStream::open(start_echo_servers(), 1000);
    stream.tick(Some(delta(&[upsert("top_n:0", 1, "lt", 0)], &[100_000])));
    let settled = stream.request_bytes.len();
    for value in 0..60 {
        // A different value each time, so that were they kept, no two would be
        // byte-identical (the token is compressed, and identical repeats are
        // nearly free).
        let turn = stream.tick(Some(delta(&[upsert("top_n:0", 1, "lt", 0)], &[value])));
        assert_eq!(
            turn.values.len(),
            BATCH as usize,
            "n < {value} would have emptied it"
        );
        assert_eq!(
            turn.witnesses[0],
            "PushdownFilters([ConstantFilter(n < 100000)])"
        );
    }
    let sizes = &stream.request_bytes[settled..];
    let largest = *sizes.iter().max().unwrap();
    assert!(
        largest <= sizes[0] + 16,
        "stale deltas were kept for replay: the continuation grew from {} to {largest} \
         bytes (every size: {sizes:?})",
        sizes[0]
    );
}

/// A removed-then-re-added predicate keeps its position across turns.
///
/// Deltas: {a:1, b:1} -> [a, b]; {remove a:2, b:1} -> [b]; {a:3, b:1} -> [b, a].
/// The compacted history is the deltas that first carried a:3 and b:1 — the
/// third and the first — and replaying those alone yields [a, b]. A turn after
/// the third delta, rebuilt purely from the tokens, must show [b, a].
#[test]
fn rebuilt_filters_keep_the_order_the_worker_had() {
    let (mut stream, _) = EchoStream::open(start_echo_servers(), 1000);
    stream.tick(Some(delta(
        &[upsert("top_n:0", 1, "lt", 0), upsert("top_n:1", 1, "gt", 1)],
        &[100_000, 5],
    )));
    stream.tick(Some(delta(
        &[remove("top_n:0", 2), upsert("top_n:1", 1, "gt", 0)],
        &[5],
    )));
    let applied = stream.tick(Some(delta(
        &[upsert("top_n:0", 3, "lt", 0), upsert("top_n:1", 1, "gt", 1)],
        &[99_999, 5],
    )));
    let rebuilt = stream.tick(None);
    assert_eq!(
        applied.witnesses[0],
        "PushdownFilters([ConstantFilter(n > 5), ConstantFilter(n < 99999)])"
    );
    assert_eq!(rebuilt.witnesses[0], applied.witnesses[0]);
    assert_eq!(applied.values.len(), BATCH as usize);
    assert_eq!(rebuilt.values.len(), BATCH as usize);
}

/// A removal stays in force after its upsert is compacted away: a stale upsert
/// cannot resurrect the predicate.
#[test]
fn a_tombstone_survives_compaction() {
    let (mut stream, _) = EchoStream::open(start_echo_servers(), 1000);
    stream.tick(Some(delta(&[upsert("top_n:0", 1, "lt", 0)], &[100_000])));
    stream.tick(Some(delta(&[remove("top_n:0", 2)], &[])));
    assert_eq!(stream.tick(None).witnesses[0], "(none)");
    let stale = stream.tick(Some(delta(&[upsert("top_n:0", 1, "lt", 0)], &[5])));
    assert_eq!(stale.witnesses[0], "(none)");
    assert_eq!(stale.values.len(), BATCH as usize);
}

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

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi_rpc::http::{HttpState, ARROW_CONTENT_TYPE};
use vgi_rpc::metadata::{
    REQUEST_ID_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY, STATE_KEY,
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
        schema_name: Some(crate::catalog::MAIN_SCHEMA.to_string()),
    };
    let bind_bytes = ipc::write_batch(&wire::to_batch(bind).unwrap()).unwrap();
    let init = InitRequest {
        bind_call: Bytes::from(bind_bytes),
        output_schema: Bytes::from(ipc::write_schema_ref(&schema_n()).unwrap()),
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
    let filter_schema = Arc::new(Schema::new(vec![Field::new(
        "filter_spec",
        DataType::Utf8,
        false,
    )
    .with_metadata(
        [("vgi_filter_version".to_string(), "1".to_string())]
            .into_iter()
            .collect(),
    )]));
    let filter = RecordBatch::try_new(
        filter_schema,
        vec![Arc::new(StringArray::from(vec![
            r#"[{"type":"join_keys","column_name":"n","column_index":0,"keys_column":"n"}]"#,
        ])) as ArrayRef],
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
        schema_name: Some(crate::catalog::MAIN_SCHEMA.to_string()),
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
    let args = crate::arguments::Arguments::serialize_positional(&[
        Arc::new(Int64Array::from(vec![FINISH_BATCHES])) as ArrayRef,
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
        schema_name: Some(crate::catalog::MAIN_SCHEMA.to_string()),
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
        execution_id: Some(Bytes::from(b"finalize-exec".to_vec())),
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

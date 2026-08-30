// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Regression tests: `init()` called via the wrong RPC call shape.
//!
//! Found live (and already fixed in vgi-python and vgi-typescript): a client
//! that calls a table-in-out / row-transform function via the plain-producer
//! call shape (`bind()` + `scan()`, no input schema) instead of the exchange
//! shape (`bind_with_input()` + `open_exchange()`) -- or the mirror-image
//! mistake -- used to produce a **silent, non-terminating hang** against a
//! real deployed worker, not a clean error. Both sides were independently,
//! locally correct: the server only stops on `finish()` (a row-transform
//! function never calls it, since it is designed to consume input rows that
//! never arrive when the wrong RPC shape was used), and the client only stops
//! when the server quits sending a continuation token (which never happens
//! either, since the server-side handler was never designed to reach that
//! state).
//!
//! Root cause: `Dispatcher::handle_init`'s table-in-out branch silently
//! substituted an empty input schema (`input_schema.unwrap_or_else(||
//! Schema::empty())`) when the incoming request's `bind_call.input_schema`
//! was missing, instead of treating "missing" as a red flag -- and the
//! producer (table) branch had no check at all on an unexpected table-in-out
//! init phase. These tests pin that both directions are now rejected
//! immediately, with a clear message naming the function and the fix, as an
//! ordinary RPC error (not a hang, not a panic).
//!
//! These build `InitRequest`s by hand (rather than going through
//! `VgiClient`, whose public methods always send a correctly-paired
//! bind_call/phase) to reproduce the exact malformed wire shape a mismatched
//! or hand-rolled client would send. Modeled on the request-building helpers
//! in `http_continuation_tests.rs`.

use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi_rpc::http::{HttpState, ARROW_CONTENT_TYPE};
use vgi_rpc::metadata::{
    LOG_LEVEL_KEY, LOG_MESSAGE_KEY, REQUEST_ID_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY,
    RPC_METHOD_KEY,
};
use vgi_rpc::wire::{md_get, StreamReader, StreamWriter};
use vgi_rpc::{Bytes, DictString, OutputCollector, Result, RpcError};

use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use crate::protocol::dtos::{BindRequest, InitRequest};
use crate::table_function::{TableFunction, TableProducer};
use crate::table_in_out::TableInOutFunction;
use crate::worker::Worker;
use crate::{ipc, wire};

fn schema_n() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
}

/// A minimal plain producer -- the body never runs in these tests, since the
/// new guard rejects a mismatched call before `producer()` is ever reached.
struct ProbeProducer {
    n: i64,
    count: i64,
}
impl TableProducer for ProbeProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.n >= self.count {
            return Ok(None);
        }
        let vals: Vec<i64> = (self.n..self.count).collect();
        self.n = self.count;
        Ok(Some(
            RecordBatch::try_new(
                schema_n(),
                vec![Arc::new(Int64Array::from(vals)) as ArrayRef],
            )
            .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
    fn resume_supported(&self) -> bool {
        false
    }
}

struct ProbeTableFunction;
impl TableFunction for ProbeTableFunction {
    fn name(&self) -> &str {
        "shape_probe_table"
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
        Ok(Box::new(ProbeProducer {
            n: 0,
            count: p.arguments.const_i64(0).unwrap_or(0).max(0),
        }))
    }
}

/// A minimal table-in-out function -- the body never runs in these tests
/// either, since the new guard rejects a mismatched call before `process()`
/// is ever reached.
struct ProbeTableInOutFunction;
impl TableInOutFunction for ProbeTableInOutFunction {
    fn name(&self) -> &str {
        "shape_probe_table_in_out"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema_n(),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, _p: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(vec![batch.clone()])
    }
}

/// Boot the worker on a loopback HTTP server. The server thread is detached
/// -- it dies with the test process.
fn start_server() -> u16 {
    let mut w = Worker::new();
    w.register_table(ProbeTableFunction);
    w.register_table_in_out(ProbeTableInOutFunction);
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

/// Frame the IPC-stream request body for `init` carrying `batch`, with the
/// RPC metadata the server expects.
fn frame(batch: &RecordBatch, method: &str) -> Vec<u8> {
    let md = std::collections::HashMap::<String, String>::from([
        (RPC_METHOD_KEY.to_string(), method.to_string()),
        (REQUEST_VERSION_KEY.to_string(), REQUEST_VERSION.to_string()),
        (REQUEST_ID_KEY.to_string(), "test".to_string()),
    ]);
    let schema = batch.schema();
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::new(&mut buf, schema.as_ref()).unwrap();
        w.write(batch, Some(&md)).unwrap();
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

/// Build the boxed `init` request body: `function_name` bound with
/// `input_schema`/`phase` set exactly as given -- the mismatch signal under
/// test -- rather than whatever a real `VgiClient` call would produce.
fn init_body(function: &str, input_schema: Option<&Schema>, phase: Option<&str>) -> Vec<u8> {
    let args = crate::arguments::Arguments::serialize_positional(&[
        Arc::new(Int64Array::from(vec![5i64])) as ArrayRef,
    ])
    .unwrap();
    let bind = BindRequest {
        function_name: function.to_string(),
        arguments: Bytes::from(args),
        function_type: DictString("table".to_string()),
        input_schema: input_schema
            .map(|s| Bytes::from(ipc::write_schema(s).unwrap()))
            .or(None),
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
        phase: phase.map(|p| DictString(p.to_string())),
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
    frame(&req, "init")
}

/// Read the EXCEPTION-level `vgi_rpc.log_message` out of an `init` response
/// body. `None` when the response carried no error envelope (a real stream
/// started successfully).
fn error_message(body: &[u8]) -> Option<String> {
    let mut cursor = std::io::Cursor::new(body);
    while (cursor.position() as usize) < body.len() {
        let mut r = match StreamReader::new(&mut cursor) {
            Ok(r) => r,
            Err(_) => break,
        };
        while let Some((_rb, md)) = r.read_next().unwrap() {
            if md_get(&md, LOG_LEVEL_KEY) == Some("EXCEPTION") {
                return md_get(&md, LOG_MESSAGE_KEY).map(str::to_string);
            }
        }
    }
    None
}

#[test]
fn table_in_out_function_rejects_the_producer_call_shape() {
    let port = start_server();

    // The plain-producer call shape: no input schema, no init phase -- what a
    // caller sends when it drives this function via `bind()` + `scan()`
    // instead of `bind_with_input()` + `open_exchange()`.
    let body = post(
        port,
        "init/init",
        init_body("shape_probe_table_in_out", None, None),
    );
    let message = error_message(&body).expect(
        "a table-in-out function called with no input schema must fail with an error, \
         not silently start an exchange (or hang)",
    );
    assert!(
        message.contains("shape_probe_table_in_out"),
        "error must name the mismatched function: {message}"
    );
    assert!(
        message.contains("table-in-out"),
        "error must explain the mismatch, not just the missing schema: {message}"
    );
    assert!(
        message.contains("bind_with_input") && message.contains("open_exchange"),
        "error must name the fix: {message}"
    );
}

#[test]
fn plain_table_function_rejects_the_exchange_call_shape() {
    let port = start_server();

    // The exchange call shape: an input schema AND an init phase set -- what
    // a caller sends when it drives this function via `bind_with_input()` +
    // `open_exchange()` instead of `bind()` + `scan()`.
    let body = post(
        port,
        "init/init",
        init_body(
            "shape_probe_table",
            Some(&schema_n()),
            Some(crate::protocol::enums::phase::INPUT),
        ),
    );
    let message = error_message(&body).expect(
        "a plain table function called with a table-in-out init phase must fail with an \
         error, not silently run as an ordinary producer while the caller feeds rows nobody \
         reads",
    );
    assert!(
        message.contains("shape_probe_table"),
        "error must name the mismatched function: {message}"
    );
    assert!(
        message.contains("plain table function"),
        "error must explain the mismatch: {message}"
    );
    assert!(
        message.contains("`bind`") && message.contains("`scan`"),
        "error must name the fix: {message}"
    );
}

#[test]
fn plain_table_function_rejects_a_phase_alone_with_no_input_schema() {
    let port = start_server();

    // The phase-only half of the exchange signal (input schema left at
    // `None`) must be rejected too -- a mismatched call may carry either
    // signal on its own.
    let body = post(
        port,
        "init/init",
        init_body(
            "shape_probe_table",
            None,
            Some(crate::protocol::enums::phase::INPUT),
        ),
    );
    let message = error_message(&body).expect(
        "a plain table function called with an init phase (even with no input schema) must \
         fail with an error",
    );
    assert!(message.contains("shape_probe_table"), "{message}");
}

#[test]
fn a_well_formed_producer_call_still_works() {
    let port = start_server();

    // Sanity check: the guard must not reject the correctly-shaped call --
    // no phase, no input schema.
    let body = post(
        port,
        "init/init",
        init_body("shape_probe_table", None, None),
    );
    assert!(
        error_message(&body).is_none(),
        "a well-formed producer call must not be rejected: {:?}",
        error_message(&body)
    );
}

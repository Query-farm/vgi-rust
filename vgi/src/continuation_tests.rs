// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Continued table-in-out output over HTTP: one input answered across several
//! replies, each later one asked for with a continuation tick.
//!
//! HTTP is the transport where it can go wrong: every reply is its own request,
//! possibly on another worker, so the resume position and the "more pending"
//! flag must survive the sealed state token. These tests drive the raw wire
//! (the DuckDB suite cannot send a malformed tick), and cover the refusals the
//! protocol requires of every worker: a tick with nothing pending, a tick with
//! rows, real input while output is pending, cache control on a tick's reply,
//! and "more" from a function that cannot resume.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi_rpc::http::{HttpState, ARROW_CONTENT_TYPE};
use vgi_rpc::metadata::{
    LOG_LEVEL_KEY, LOG_MESSAGE_KEY, REQUEST_ID_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY,
    RPC_METHOD_KEY, STATE_KEY,
};
use vgi_rpc::wire::{md_get, StreamReader, StreamWriter};
use vgi_rpc::{Bytes, DictString, Result};

use crate::cache_control::CacheControl;
use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use crate::protocol::dtos::{BindRequest, InitRequest};
use crate::table_in_out::{
    decode_resume_state, EmitOptions, TableInOutFunction, TableInOutOutput, STATUS_CONTINUE,
    STATUS_HAVE_MORE_OUTPUT, STATUS_METADATA_KEY,
};
use crate::worker::Worker;
use crate::{ipc, wire};

/// Output rows per reply.
const PAGE: usize = 2;

fn schema_n() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
}

fn n_batch(values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        schema_n(),
        vec![Arc::new(Int64Array::from(values.to_vec())) as ArrayRef],
    )
    .unwrap()
}

/// For each input row with count `n`, rows `0..n-1`, `PAGE` per reply. The
/// flags break the contract in the one way each refusal test needs.
struct Pager {
    name: &'static str,
    /// `resume_supported()`.
    resumable: bool,
    /// Record the resume position on a reply that says more.
    records_state: bool,
    /// Advertise cache control on every reply, a tick's included.
    cache_every_reply: bool,
}

impl Pager {
    const fn new(name: &'static str) -> Self {
        Pager {
            name,
            resumable: true,
            records_state: true,
            cache_every_reply: false,
        }
    }

    /// Emit the next page of `rest` (the output still to send).
    fn emit(&self, mut rest: Vec<i64>, out: &mut TableInOutOutput) -> Result<()> {
        let tail = rest.split_off(rest.len().min(PAGE));
        let more = !tail.is_empty();
        if more && self.records_state {
            out.set_resume_state(&tail)?;
        }
        out.emit_with(
            n_batch(&rest),
            EmitOptions {
                more,
                cache_control: self.cache_every_reply.then(|| CacheControl::ttl(60)),
                ..Default::default()
            },
        )
    }
}

impl TableInOutFunction for Pager {
    fn name(&self) -> &str {
        self.name
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        // `Arguments::serialize_positional` over an empty slice writes no
        // record batch, which the bind decoder rejects; one const arg it is.
        vec![ArgSpec::const_arg("unused", 0, "int64", "Unused")]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema_n(),
            opaque_data: Vec::new(),
        })
    }
    fn resume_supported(&self) -> bool {
        self.resumable
    }
    fn process_out(
        &self,
        _params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rows = (0..counts.len())
            .flat_map(|i| 0..counts.value(i).max(0))
            .collect();
        self.emit(rows, out)
    }
    fn resume(
        &self,
        _params: &ProcessParams,
        state: &[u8],
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        self.emit(decode_resume_state(state)?, out)
    }
}

/// Boot the worker on a loopback HTTP server. The server thread is detached;
/// it dies with the test process.
fn start_server() -> u16 {
    let mut w = Worker::new();
    w.register_table_in_out(Pager::new("test_pager"));
    w.register_table_in_out(Pager {
        resumable: false,
        ..Pager::new("test_pager_no_resume")
    });
    w.register_table_in_out(Pager {
        records_state: false,
        ..Pager::new("test_pager_no_state")
    });
    w.register_table_in_out(Pager {
        cache_every_reply: true,
        ..Pager::new("test_pager_cache_every_reply")
    });
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

/// Frame `batch` as a request body for `method`, with the RPC metadata the
/// server expects plus `extra`.
fn frame(batch: &RecordBatch, method: &str, extra: &[(&str, &str)]) -> Vec<u8> {
    let mut md = HashMap::<String, String>::from([
        (RPC_METHOD_KEY.to_string(), method.to_string()),
        (REQUEST_VERSION_KEY.to_string(), REQUEST_VERSION.to_string()),
        (
            vgi_rpc::metadata::PROTOCOL_VERSION_KEY.to_string(),
            vgi_protocol::VGI_PROTOCOL_VERSION.to_string(),
        ),
        (REQUEST_ID_KEY.to_string(), "test".to_string()),
    ]);
    for (k, v) in extra {
        md.insert(k.to_string(), v.to_string());
    }
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::new(&mut buf, batch.schema().as_ref()).unwrap();
        w.write(batch, Some(&md)).unwrap();
        w.finish().unwrap();
    }
    buf
}

/// The `init` body opening an INPUT-phase exchange on `function`.
fn init_body(function: &str) -> Vec<u8> {
    let args = crate::arguments::Arguments::serialize_positional(&[
        Arc::new(Int64Array::from(vec![0i64])) as ArrayRef,
    ])
    .unwrap();
    let bind = BindRequest {
        function_name: function.to_string(),
        arguments: Bytes::from(args),
        function_type: DictString("TABLE".to_string()),
        input_schema: Some(Bytes::from(ipc::write_schema_ref(&schema_n()).unwrap())),
        settings: None,
        secrets: None,
        attach_opaque_data: None,
        transaction_opaque_data: None,
        resolved_secrets_provided: false,
        at_unit: None,
        at_value: None,
        copy_from: None,
        copy_to: None,
        schema_path: Some(vec![crate::catalog::MAIN_SCHEMA.to_string()]),
        argument_names: Some(vec![Some("unused".to_string())]),
    };
    let bind_bytes = ipc::write_batch(&wire::to_batch(bind).unwrap()).unwrap();
    let init = InitRequest {
        bind_call: Bytes::from(bind_bytes),
        output_schema: Bytes::from(ipc::write_schema_ref(&schema_n()).unwrap()),
        bind_opaque_data: None,
        projection_ids: None,
        pushdown_filters: None,
        join_keys: None,
        phase: Some(DictString(crate::protocol::enums::phase::INPUT.to_string())),
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
    let req = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "request",
            DataType::Binary,
            false,
        )])),
        vec![Arc::new(BinaryArray::from(vec![inner.as_slice()])) as ArrayRef],
    )
    .unwrap();
    frame(&req, "init", &[])
}

fn post(port: u16, path: &str, body: Vec<u8>) -> Vec<u8> {
    let url = format!(
        "http://127.0.0.1:{port}/{}/{path}",
        vgi_protocol::VGI_PROTOCOL_NAME
    );
    match ureq::post(&url)
        .header("Content-Type", ARROW_CONTENT_TYPE)
        .send(&body[..])
    {
        Ok(mut resp) => resp.body_mut().read_to_vec().unwrap(),
        Err(ureq::Error::StatusCode(code)) => panic!("POST {path} -> {code}"),
        Err(e) => panic!("POST {path} failed: {e}"),
    }
}

/// One parsed exchange response.
#[derive(Debug)]
struct Reply {
    values: Vec<i64>,
    /// The data batch said `HAVE_MORE_OUTPUT`.
    more: bool,
    /// The data batch carried `vgi.cache.*` keys.
    cache_control: bool,
    token: Option<String>,
    error: Option<String>,
}

fn parse(body: &[u8]) -> Reply {
    let mut reply = Reply {
        values: Vec::new(),
        more: false,
        cache_control: false,
        token: None,
        error: None,
    };
    let mut cursor = std::io::Cursor::new(body);
    while (cursor.position() as usize) < body.len() {
        let Ok(mut r) = StreamReader::new(&mut cursor) else {
            break;
        };
        while let Some((rb, md)) = r.read_next().unwrap() {
            if md_get(&md, LOG_LEVEL_KEY) == Some("EXCEPTION") {
                reply.error = md_get(&md, LOG_MESSAGE_KEY).map(str::to_string);
            }
            if let Some(t) = md_get(&md, STATE_KEY) {
                reply.token = Some(t.to_string());
            }
            reply.more |= md_get(&md, STATUS_METADATA_KEY) == Some(STATUS_HAVE_MORE_OUTPUT);
            reply.cache_control |= md.keys().any(|k| k.starts_with("vgi.cache."));
            if let Some(col) = rb
                .schema()
                .index_of("n")
                .ok()
                .and_then(|i| rb.column(i).as_any().downcast_ref::<Int64Array>())
            {
                reply.values.extend((0..col.len()).map(|i| col.value(i)));
            }
        }
    }
    reply
}

/// An open exchange stream on the test server, one HTTP request per turn.
struct Stream {
    port: u16,
    token: String,
}

impl Stream {
    fn open(port: u16, function: &str) -> Self {
        let init = parse(&post(port, "init/init", init_body(function)));
        assert_eq!(init.error, None, "init failed");
        Stream {
            port,
            token: init.token.expect("init returns a state token"),
        }
    }

    /// Send `batch`, with `vgi.status: CONTINUE` when `tick`, and advance the
    /// state token on success.
    fn turn(&mut self, batch: &RecordBatch, tick: bool) -> Reply {
        let mut extra = vec![(STATE_KEY, self.token.as_str())];
        if tick {
            extra.push((STATUS_METADATA_KEY, STATUS_CONTINUE));
        }
        let reply = parse(&post(
            self.port,
            "init/exchange",
            frame(batch, "init", &extra),
        ));
        if let Some(t) = &reply.token {
            self.token = t.clone();
        }
        reply
    }

    fn send(&mut self, values: &[i64]) -> Reply {
        self.turn(&n_batch(values), false)
    }

    fn tick(&mut self) -> Reply {
        self.turn(&n_batch(&[]), true)
    }
}

/// One input's output arrives across replies, each later one answering a tick
/// on a fresh request: the resume position and the pending flag ride the state
/// token. The input after the last reply is a real one again.
#[test]
fn continued_output_resumes_from_the_state_token() {
    let port = start_server();
    let mut s = Stream::open(port, "test_pager");

    let mut replies = vec![s.send(&[3, 0, 2])];
    while replies.last().unwrap().more {
        replies.push(s.tick());
    }
    assert!(replies.iter().all(|r| r.error.is_none()), "{replies:?}");
    let values: Vec<i64> = replies.iter().flat_map(|r| r.values.clone()).collect();
    assert_eq!(values, vec![0, 1, 2, 0, 1]);
    assert_eq!(replies.len(), 3, "5 rows at {PAGE} per reply: {replies:?}");

    // Output drained: a real input is valid again, a tick is not.
    let next = s.send(&[1]);
    assert_eq!((next.error, next.values, next.more), (None, vec![0], false));
    let stray = s.tick();
    let message = stray
        .error
        .expect("a tick with no output pending must fail");
    assert!(message.contains("no output is pending"), "{message}");
}

/// While output is pending a tick is the only valid input.
#[test]
fn real_input_while_output_is_pending_is_refused() {
    let port = start_server();
    let mut s = Stream::open(port, "test_pager");
    assert!(s.send(&[5]).more);
    let message = s.send(&[1]).error.expect("input while pending must fail");
    assert!(
        message.contains("expected a continuation tick"),
        "{message}"
    );
}

/// A tick carries no rows.
#[test]
fn a_tick_with_rows_is_refused() {
    let port = start_server();
    let mut s = Stream::open(port, "test_pager");
    assert!(s.send(&[5]).more);
    let message = s
        .turn(&n_batch(&[1]), true)
        .error
        .expect("a tick with rows must fail");
    assert!(message.contains("must have no rows"), "{message}");
}

/// A function that does not implement `resume` cannot say more: nothing would
/// answer the tick.
#[test]
fn more_without_resume_is_refused() {
    let port = start_server();
    let mut s = Stream::open(port, "test_pager_no_resume");
    let message = s.send(&[5]).error.expect("more without resume must fail");
    assert!(message.contains("does not implement resume"), "{message}");
}

/// A reply that says more must record where to resume.
#[test]
fn more_without_a_resume_state_is_refused() {
    let port = start_server();
    let mut s = Stream::open(port, "test_pager_no_state");
    let message = s.send(&[5]).error.expect("more without state must fail");
    assert!(message.contains("set_resume_state"), "{message}");
}

/// Cache control describes an input's whole output, so only its first reply
/// may carry it.
#[test]
fn cache_control_on_a_tick_reply_is_refused() {
    let port = start_server();
    let mut s = Stream::open(port, "test_pager_cache_every_reply");
    let first = s.send(&[5]);
    assert!(first.more && first.cache_control, "{first:?}");
    let message = s.tick().error.expect("cache control on a tick must fail");
    assert!(
        message.contains("only valid on an input's first reply"),
        "{message}"
    );
}

/// Registration refuses what the client cannot drive.
#[test]
#[should_panic(expected = "not supported with max_workers = 1")]
fn resume_with_max_workers_one_is_refused_at_registration() {
    struct Serial;
    impl TableInOutFunction for Serial {
        fn name(&self) -> &str {
            "serial_pager"
        }
        fn metadata(&self) -> FunctionMetadata {
            FunctionMetadata {
                max_workers: 1,
                ..Default::default()
            }
        }
        fn argument_specs(&self) -> Vec<ArgSpec> {
            Vec::new()
        }
        fn resume_supported(&self) -> bool {
            true
        }
    }
    Worker::new().register_table_in_out(Serial);
}

struct RowsPerCall {
    blended: bool,
    rows: i64,
}

impl TableInOutFunction for RowsPerCall {
    fn name(&self) -> &str {
        "rows_per_call"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            input_from_args: self.blended,
            input_rows_per_call: Some(self.rows),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("x", 0, "int64", "Input column")]
    }
}

#[test]
#[should_panic(expected = "input_rows_per_call must be a positive row count, got 0")]
fn input_rows_per_call_must_be_positive() {
    Worker::new().register_table_in_out(RowsPerCall {
        blended: true,
        rows: 0,
    });
}

#[test]
#[should_panic(expected = "input_rows_per_call applies only to a blended")]
fn input_rows_per_call_needs_a_blended_function() {
    Worker::new().register_table_in_out(RowsPerCall {
        blended: false,
        rows: 3,
    });
}

/// The limit reaches the catalog entry the client reads.
#[test]
fn input_rows_per_call_is_advertised() {
    let info = crate::catalog::table_in_out_function_info(&RowsPerCall {
        blended: true,
        rows: 3,
    })
    .unwrap();
    assert_eq!(info.input_rows_per_call, Some(3));
    let unset = crate::catalog::table_in_out_function_info(&Pager::new("p")).unwrap();
    assert_eq!(unset.input_rows_per_call, None);
}

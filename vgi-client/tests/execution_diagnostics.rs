// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Diagnostics traverse real Arrow IPC and the built-in client's log router.
//! No prebuilt fixture executable, log facade singleton, or optional HTTP stack.

#![cfg(not(target_arch = "wasm32"))]

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::diagnostics::{source_http_error, ExecutionDiagnostics, ScanStats};
use vgi::table_function::{TableFunction, TableProducer};
use vgi::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams, Worker};
use vgi_client::{AttachOptions, BindSpec, ScanOptions, VgiClient};
use vgi_rpc::error_model::Code;
use vgi_rpc::{LogLevel, LogMessage, OutputCollector, Result};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]))
}

struct Observations;

impl TableFunction for Observations {
    fn name(&self) -> &str {
        "observations"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "fail",
            0,
            "int64",
            "Simulate a denied chunk",
        )]
    }
    fn on_bind(&self, _: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(ObservationProducer {
            diagnostics: ExecutionDiagnostics::new(self.name(), &params.execution_id),
            fail: params.arguments.const_i64(0) == Some(1),
            done: false,
        }))
    }
}

struct ObservationProducer {
    diagnostics: ExecutionDiagnostics,
    fail: bool,
    done: bool,
}

impl TableProducer for ObservationProducer {
    fn next_batch(&mut self, out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            self.diagnostics.scan_finished(
                out,
                &ScanStats {
                    rows_read: 2,
                    chunks_read: 1,
                    chunks_skipped: 3,
                    bytes_read: 16,
                },
            );
            return Ok(None);
        }
        self.diagnostics.scan_started(out);
        if self.fail {
            // The terminal error itself must carry context: logs queued on a
            // failed turn are not guaranteed to leave the OutputCollector.
            return Err(self.diagnostics.annotate_error(source_http_error(
                "read",
                "weather store",
                403,
            )));
        }
        self.diagnostics
            .log(out, LogLevel::Debug, "Decoded one source chunk");
        self.done = true;
        Ok(Some(
            RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![10, 20]))]).unwrap(),
        ))
    }

    fn resume_supported(&self) -> bool {
        false // This fixture is served only over a byte stream.
    }
}

fn with_worker(test: impl FnOnce(&mut VgiClient)) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut worker = Worker::new();
        worker.register_table(Observations);
        worker.serve_reader_writer(stream.try_clone().unwrap(), stream);
    });
    let mut client = VgiClient::connect_tcp("127.0.0.1", port).unwrap();
    test(&mut client);
    drop(client);
    server.join().unwrap();
}

fn extra<'a>(message: &'a LogMessage, key: &str) -> &'a str {
    &message.extras.iter().find(|(k, _)| k == key).unwrap().1
}

#[test]
fn in_band_scan_events_reach_worker_log_sink_and_keep_executions_separate() {
    with_worker(|client| {
        let captured = Arc::new(Mutex::new(Vec::<LogMessage>::new()));
        let sink = captured.clone();
        assert!(client.set_worker_log_sink(Arc::new(move |message| {
            sink.lock().unwrap().push(message);
        })));
        let catalog = client
            .attach("diagnostics", AttachOptions::default())
            .unwrap();
        let bound = client
            .bind(
                &catalog,
                &BindSpec::table("observations")
                    .in_schema(catalog.default_schema())
                    .with_arguments(vgi_client::Arguments::new().positional(0i64)),
            )
            .unwrap();
        for _ in 0..2 {
            let batches = client
                .scan(&bound, &ScanOptions::default())
                .unwrap()
                .collect()
                .unwrap();
            assert_eq!(batches.len(), 1, "log batches must not become result rows");
            assert_eq!(batches[0].num_rows(), 2);
            assert_eq!(batches[0].schema(), schema());
        }
        let messages = captured.lock().unwrap();
        let events: Vec<_> = messages
            .iter()
            .filter(|m| m.extras.iter().any(|(k, _)| k == "event"))
            .collect();
        assert_eq!(events.len(), 6);
        let first_id = extra(events[0], "execution_id");
        let second_id = extra(events[3], "execution_id");
        assert!(!first_id.is_empty());
        assert_ne!(first_id, second_id);
        for scan in events.chunks_exact(3) {
            assert_eq!(extra(scan[0], "event"), "scan.started");
            assert_eq!(extra(scan[1], "event"), "worker.log");
            assert_eq!(extra(scan[2], "event"), "scan.finished");
            for message in scan {
                assert_eq!(extra(message, "function"), "observations");
                assert_eq!(
                    extra(message, "execution_id"),
                    extra(scan[0], "execution_id")
                );
            }
            assert_eq!(extra(scan[2], "rows_read"), "2");
            assert_eq!(extra(scan[2], "chunks_read"), "1");
            assert_eq!(extra(scan[2], "chunks_skipped"), "3");
            assert_eq!(extra(scan[2], "bytes_read"), "16");
            assert!(scan[2].message.contains("observations"));
            assert!(scan[2].message.contains("chunks_skipped=3"));
        }
    });
}

#[test]
fn denied_chunk_remains_a_typed_error_with_source_and_execution_context_on_wire() {
    with_worker(|client| {
        let catalog = client
            .attach("diagnostics", AttachOptions::default())
            .unwrap();
        let bound = client
            .bind(
                &catalog,
                &BindSpec::table("observations")
                    .in_schema(catalog.default_schema())
                    .with_arguments(vgi_client::Arguments::new().positional(1i64)),
            )
            .unwrap();
        let error = match client.scan(&bound, &ScanOptions::default()) {
            Ok(mut scan) => scan.collect().unwrap_err(),
            Err(error) => error,
        };
        assert_eq!(error.code(), Code::PermissionDenied);
        assert_eq!(error.error_kind(), "source_access_failed");
        let info = error
            .error_info()
            .expect("typed details must survive wire validation");
        assert_eq!(info["source"], "weather store");
        assert_eq!(info["operation"], "read");
        assert_eq!(info["http_status"], "403");
        assert_eq!(info["function"], "observations");
        assert!(!info["execution_id"].is_empty());
    });
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Self-contained testing-kit exercise over real Arrow RPC bytes. No separately
//! built worker or SQL engine is required; the external-data example additionally
//! tests the subprocess launcher against its own Cargo-built binary.
#![cfg(feature = "testing")]

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FilterHandling, FunctionMetadata, ProcessParams,
};
use vgi::table_function::{TableFunction, TableProducer};
use vgi_client::testing::{scan_options, Comparison, FilterExpr, ScanResult, TestWorker};
use vgi_client::{AttachOptions, BindSpec, Result, StreamTransport, VgiClient};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
    ]))
}

fn batch(ids: Vec<i64>, labels: Vec<Option<&str>>) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(labels)),
        ],
    )
    .unwrap()
}

struct Fixture(FilterHandling);

impl TableFunction for Fixture {
    fn name(&self) -> &str {
        "rows"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            projection_pushdown: true,
            ..FunctionMetadata::default()
        }
        .with_filter_handling(self.0)
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn on_bind(&self, _: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: schema(),
            opaque_data: vec![],
        })
    }
    fn producer(&self, _: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(Rows {
            next: Some(batch(
                vec![1, 2, 2, 3],
                vec![Some("one"), Some("two"), Some("two"), None],
            )),
        }))
    }
}

struct Rows {
    next: Option<RecordBatch>,
}

impl TableProducer for Rows {
    fn next_batch(&mut self, _: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        Ok(self.next.take())
    }
    fn resume_supported(&self) -> bool {
        false
    } // One batch per scan.
}

fn connect_fixture(handling: FilterHandling) -> Result<(TestWorker, std::thread::JoinHandle<()>)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut worker = vgi::Worker::new();
        worker.register_table(Fixture(handling));
        worker.register_table_in_out(InputFixture);
        worker.register_buffering(BufferedFixture);
        worker.serve_reader_writer(stream.try_clone().unwrap(), stream);
    });
    let rpc = vgi_rpc_client::RpcClient::tcp_connect_with_timeout(
        "127.0.0.1",
        port,
        Some(Duration::from_secs(10)),
    )?
    .protocol(vgi_protocol::VGI_PROTOCOL_NAME)
    .protocol_version(vgi_protocol::VGI_PROTOCOL_VERSION)
    .relax_nullability(true);
    let client = VgiClient::new(Box::new(StreamTransport::new(rpc, "test-fixture")));
    let catalog = std::env::var("VGI_WORKER_CATALOG_NAME").unwrap_or_else(|_| "example".into());
    Ok((
        TestWorker::attach(client, &catalog, AttachOptions::default())?,
        server,
    ))
}

#[test]
fn harness_compares_real_wire_scans_to_independent_fixtures() -> Result<()> {
    let (mut worker, server) = connect_fixture(FilterHandling::Sdk)?;
    let spec = BindSpec::table("rows").in_schema(
        worker
            .catalog()
            .default_schema()
            .expect("fixture has a default schema"),
    );

    // id is intentionally absent from the requested projection; the kit must
    // add it for filtering, keep its full-schema index, then trim it away.
    let filter = FilterExpr::compare("id", Comparison::Gt, Arc::new(Int64Array::from(vec![1])))
        .and(FilterExpr::is_not_null("label"));
    let options = scan_options(&schema(), &filter, Some(vec![1]))?;
    assert_eq!(options.wire_projection(), Some(vec![1, 0]));
    worker.check_pushdown(&spec, &options, |full| {
        assert_eq!(full.num_rows(), 4);
        // Expected rows are written independently of the SDK filter evaluator.
        ScanResult::try_new(
            schema(),
            vec![batch(vec![2, 2], vec![Some("two"), Some("two")])],
        )
    })?;

    // Even two identical matching rows are not interchangeable with one row.
    let wrong = worker
        .check_pushdown(&spec, &options, |_| {
            ScanResult::try_new(schema(), vec![batch(vec![2], vec![Some("two")])])
        })
        .unwrap_err();
    assert!(wrong.message.contains("row counts differ"));

    let impossible =
        FilterExpr::compare("id", Comparison::Gt, Arc::new(Int64Array::from(vec![100])));
    let options = scan_options(&schema(), &impossible, Some(vec![1, 0]))?;
    let empty = worker.scan(&spec, &options)?;
    assert_eq!(empty.num_rows(), 0);
    assert_eq!(empty.schema().field(0).name(), "label");
    assert_eq!(empty.schema().field(1).name(), "id");

    let unsupported = vgi_client::ScanOptions {
        row_limit: Some(1),
        ..options
    };
    assert!(worker
        .check_pushdown(&spec, &unsupported, |_| unreachable!())
        .unwrap_err()
        .message
        .contains("only filters and projection"));
    drop(worker);
    server.join().unwrap();
    Ok(())
}

#[test]
fn disabled_filtering_rejects_required_predicates_over_rpc() -> Result<()> {
    let (mut worker, server) = connect_fixture(FilterHandling::Disabled)?;
    let spec = BindSpec::table("rows").in_schema(
        worker
            .catalog()
            .default_schema()
            .expect("fixture has a default schema"),
    );
    assert_eq!(
        worker
            .scan(&spec, &vgi_client::ScanOptions::default())?
            .num_rows(),
        4
    );

    // A direct client can send predicates even when discovery did not advertise
    // pushdown. The worker must reject required filters instead of returning
    // all four rows as if it had applied this predicate.
    let filter = FilterExpr::compare("id", Comparison::Gt, Arc::new(Int64Array::from(vec![100])));
    let options = scan_options(&schema(), &filter, None)?;
    let error = worker.scan(&spec, &options).unwrap_err();
    assert!(error.message.contains("required pushed filters"), "{error}");
    drop(worker);
    server.join().unwrap();
    Ok(())
}

struct InputFixture;

impl vgi::table_in_out::TableInOutFunction for InputFixture {
    fn name(&self) -> &str {
        "input_rows"
    }
    fn metadata(&self) -> FunctionMetadata {
        Fixture(FilterHandling::Sdk).metadata()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        Fixture(FilterHandling::Sdk).on_bind(params)
    }
    fn process(&self, _: &ProcessParams, _: &RecordBatch) -> Result<Vec<RecordBatch>> {
        Ok(vec![batch(
            vec![1, 2, 2, 3],
            vec![Some("one"), Some("two"), Some("two"), None],
        )])
    }
    fn has_finish(&self) -> bool {
        true
    }
    fn finish(&self, _: &ProcessParams) -> Result<Vec<RecordBatch>> {
        // Both finish batches must be filtered while id is still available.
        Ok(vec![
            batch(vec![1, 2], vec![Some("one"), Some("two")]),
            batch(vec![2, 3], vec![Some("two"), None]),
        ])
    }
}

struct BufferedFixture;

impl vgi::buffering::TableBufferingFunction for BufferedFixture {
    fn name(&self) -> &str {
        "buffered_rows"
    }
    fn metadata(&self) -> FunctionMetadata {
        Fixture(FilterHandling::Sdk).metadata()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        Fixture(FilterHandling::Sdk).on_bind(params)
    }
    fn process(&self, _: &vgi::buffering::BufferingParams, _: &RecordBatch) -> Result<Vec<u8>> {
        Ok(b"fixture-state".to_vec())
    }
    fn combine(
        &self,
        _: &vgi::buffering::BufferingParams,
        states: &[Vec<u8>],
    ) -> Result<Vec<Vec<u8>>> {
        Ok(states.to_vec())
    }
    fn finalize_producer(
        &self,
        _: &vgi::buffering::BufferingParams,
        _: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(Rows {
            next: Some(batch(
                vec![1, 2, 2, 3],
                vec![Some("one"), Some("two"), Some("two"), None],
            )),
        }))
    }
}

fn expected_filtered_labels() -> Result<ScanResult> {
    let expected = batch(vec![2, 2, 3], vec![Some("two"), Some("two"), None])
        .project(&[1])
        .unwrap();
    ScanResult::try_new(expected.schema(), vec![expected])
}

fn label_projection() -> Result<vgi_client::ScanOptions> {
    let filter = FilterExpr::compare("id", Comparison::Gt, Arc::new(Int64Array::from(vec![1])));
    scan_options(&schema(), &filter, Some(vec![1]))
}

#[test]
fn table_in_out_filters_process_and_finish_before_projecting() -> Result<()> {
    let (mut worker, server) = connect_fixture(FilterHandling::Sdk)?;
    let catalog = worker.catalog().clone();
    let client = worker.client_mut();
    let mut spec = BindSpec::table("input_rows").in_schema(
        catalog
            .default_schema()
            .expect("fixture has a default schema"),
    );
    spec.function_type = vgi_client::FunctionType::TableInOut;
    let bound = client.bind_with_input(&catalog, &spec, &schema())?;
    let options = label_projection()?;
    let execution_id;
    {
        let mut exchange = client.open_exchange(&bound, &options)?;
        execution_id = exchange.execution_id().clone();
        let declared = exchange.schema().clone();
        let actual = exchange
            .send(&batch(vec![0, 0, 0, 0], vec![None, None, None, None]))?
            .expect("process answer");
        assert_eq!(exchange.parent_rows(), Some([1, 2, 3].as_slice()));
        ScanResult::try_new(declared, vec![actual])?.assert_matches(&expected_filtered_labels()?)?;
        exchange.close()?;
    }
    {
        let mut scan =
            client.finalize_table_in_out_with_options(&bound, &execution_id, &options)?;
        let declared = scan.schema().clone();
        ScanResult::try_new(declared, scan.collect()?)?
            .assert_matches(&expected_filtered_labels()?)?;
    }
    drop(worker);
    server.join().unwrap();
    Ok(())
}

#[test]
fn buffering_finalize_filters_full_schema_then_projects() -> Result<()> {
    let (mut worker, server) = connect_fixture(FilterHandling::Sdk)?;
    let catalog = worker.catalog().clone();
    let client = worker.client_mut();
    let mut spec = BindSpec::table("buffered_rows").in_schema(
        catalog
            .default_schema()
            .expect("fixture has a default schema"),
    );
    spec.function_type = vgi_client::FunctionType::TableBuffering;
    let bound = client.bind_with_input(&catalog, &spec, &schema())?;
    let execution_id = client.buffering_begin(&bound)?;
    let state = client.buffering_process(
        &catalog,
        &spec,
        &execution_id,
        &batch(vec![0], vec![None]),
        None,
    )?;
    let states = client.buffering_combine(&catalog, &spec, &execution_id, vec![state])?;
    assert_eq!(states.len(), 1);
    {
        let mut scan = client.buffering_finalize_with_options(
            &bound,
            &execution_id,
            &states[0],
            &label_projection()?,
        )?;
        let declared = scan.schema().clone();
        ScanResult::try_new(declared, scan.collect()?)?
            .assert_matches(&expected_filtered_labels()?)?;
    }
    drop(worker);
    server.join().unwrap();
    Ok(())
}

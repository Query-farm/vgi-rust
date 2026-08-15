// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! End-to-end exchange-mode calls against a real worker.
//!
//! Covers the three shapes that send rows to the worker: a scalar map, a
//! streaming table-in-out, and a buffered sink-then-source function.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{cast::AsArray, types::Int64Type, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use vgi_client::{
    ArgValue, Arguments, AttachOptions, BindSpec, FunctionType, ScanOptions, VgiClient,
};

fn example_worker() -> Option<PathBuf> {
    let mut dir = std::env::current_exe().ok()?;
    dir.pop();
    dir.pop();
    let exe = dir.join(if cfg!(windows) {
        "vgi-example-worker.exe"
    } else {
        "vgi-example-worker"
    });
    exe.exists().then_some(exe)
}

macro_rules! skip_without_worker {
    () => {
        if example_worker().is_none() {
            eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
            return;
        }
    };
}

fn connect() -> VgiClient {
    let worker = example_worker().expect("worker");
    VgiClient::connect_subprocess(&[worker.as_os_str()]).expect("connect")
}

fn i64_schema(name: &str) -> Schema {
    Schema::new(vec![Field::new(name, DataType::Int64, true)])
}

fn i64_batch(name: &str, values: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(i64_schema(name)),
        vec![Arc::new(Int64Array::from(values.to_vec()))],
    )
    .expect("input batch")
}

fn column_values(batch: &RecordBatch, col: usize) -> Vec<i64> {
    let a = batch.column(col).as_primitive::<Int64Type>();
    (0..batch.num_rows()).map(|i| a.value(i)).collect()
}

#[test]
fn a_scalar_function_maps_a_batch() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    // `double(value)` takes its argument as a column, so the argument slot
    // carries a typed placeholder and the values arrive in the input batch.
    let mut spec = BindSpec::table("double").in_schema(&schema_name);
    spec.function_type = FunctionType::Scalar;
    spec.arguments = Arguments::new().positional(ArgValue::Placeholder(DataType::Int64));

    let input_schema = i64_schema("value");
    let bound = client
        .bind_with_input(&cat, &spec, &input_schema)
        .expect("bind");

    let mut ex = client
        .open_exchange(&bound, &ScanOptions::default())
        .expect("open exchange");

    let out = ex
        .send(&i64_batch("value", &[1, 2, 3]))
        .expect("exchange")
        .expect("an answer for the first batch");
    assert_eq!(column_values(&out, 0), vec![2, 4, 6]);

    // A second batch on the same exchange.
    let out = ex
        .send(&i64_batch("value", &[10, 20]))
        .expect("exchange")
        .expect("an answer for the second batch");
    assert_eq!(column_values(&out, 0), vec![20, 40]);

    ex.close().expect("close");
}

#[test]
fn a_scalar_exchange_refuses_sends_after_close() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let mut spec = BindSpec::table("double").in_schema(&schema_name);
    spec.function_type = FunctionType::Scalar;
    spec.arguments = Arguments::new().positional(ArgValue::Placeholder(DataType::Int64));

    let bound = client
        .bind_with_input(&cat, &spec, &i64_schema("value"))
        .expect("bind");
    let mut ex = client
        .open_exchange(&bound, &ScanOptions::default())
        .expect("open");
    ex.close().expect("close");

    assert!(
        ex.send(&i64_batch("value", &[1])).is_err(),
        "sending after input EOS must be an error, not silent corruption"
    );
    // Closing twice is a no-op rather than an error.
    ex.close().expect("idempotent close");
}

#[test]
fn a_streaming_table_in_out_echoes_its_input() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let mut spec = BindSpec::table("echo").in_schema(&schema_name);
    spec.function_type = FunctionType::TableInOut;

    let input_schema = i64_schema("n");
    let bound = client
        .bind_with_input(&cat, &spec, &input_schema)
        .expect("bind");
    let mut ex = client
        .open_exchange(&bound, &ScanOptions::default())
        .expect("open exchange");

    let out = ex
        .send(&i64_batch("n", &[7, 8, 9]))
        .expect("exchange")
        .expect("an answer");
    assert_eq!(
        column_values(&out, 0),
        vec![7, 8, 9],
        "`echo` should hand back what it was given"
    );

    ex.close().expect("close");
}

#[test]
fn a_one_to_n_transform_reports_row_provenance() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    // `repeat_inputs` emits each input row more than once, which is exactly
    // when a worker must say which input row each output row came from.
    let mut spec = BindSpec::table("repeat_inputs").in_schema(&schema_name);
    spec.function_type = FunctionType::TableInOut;

    // Bind failure here means the fixture was renamed or removed — that should
    // break the test loudly rather than turn it into a silent pass.
    let bound = client
        .bind_with_input(&cat, &spec, &i64_schema("n"))
        .expect("`repeat_inputs` must exist in the example catalog");
    let mut ex = client
        .open_exchange(&bound, &ScanOptions::default())
        .expect("open exchange");

    let out = ex
        .send(&i64_batch("n", &[1, 2]))
        .expect("exchange")
        .expect("repeat_inputs must answer its first input batch");

    if let Some(parents) = ex.parent_rows() {
        assert_eq!(
            parents.len(),
            out.num_rows(),
            "provenance must have one entry per output row"
        );
        assert!(
            parents.iter().all(|&p| p >= 0 && (p as usize) < 2),
            "every parent index must point at a real input row: {parents:?}"
        );
    } else {
        // No provenance means the worker claims an identity map, which is only
        // coherent if the row counts match.
        assert_eq!(
            out.num_rows(),
            2,
            "a worker that omits parent_row is claiming 1:1, but emitted {} rows for 2 inputs",
            out.num_rows()
        );
    }

    ex.close().expect("close");
}

#[test]
fn a_buffered_function_sinks_then_sources() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let mut spec = BindSpec::table("buffer_input").in_schema(&schema_name);
    spec.function_type = FunctionType::TableBuffering;

    let input_schema = i64_schema("n");
    let bound = client
        .bind_with_input(&cat, &spec, &input_schema)
        .expect("`buffer_input` must exist in the example catalog");

    // 1. Open the execution. The worker mints the id every later call echoes.
    let execution_id = client.buffering_begin(&bound).expect("begin");

    // 2. Sink two chunks.
    let mut state_ids = Vec::new();
    for chunk in [&[1i64, 2][..], &[3, 4, 5][..]] {
        state_ids.push(
            client
                .buffering_process(&cat, &spec, &execution_id, &i64_batch("n", chunk), None)
                .expect("process"),
        );
    }

    // 3. Collapse the per-chunk state.
    let finalize_ids = client
        .buffering_combine(&cat, &spec, &execution_id, state_ids)
        .expect("combine");
    assert!(
        !finalize_ids.is_empty(),
        "combine must name at least one state to drain"
    );

    // 4. Drain each finalize state as a producer stream.
    let mut all = Vec::new();
    for fid in &finalize_ids {
        let mut scan = client
            .buffering_finalize(&bound, &execution_id, fid)
            .expect("finalize");
        for b in scan.collect().expect("drain") {
            all.extend(column_values(&b, 0));
        }
    }

    all.sort_unstable();
    assert_eq!(
        all,
        vec![1, 2, 3, 4, 5],
        "a buffered function must see every row from every chunk"
    );
}

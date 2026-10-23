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
    let schema_path = cat.default_schema().to_string();

    // `double(value)` takes its argument as a column, so the argument slot
    // carries a typed placeholder and the values arrive in the input batch.
    let mut spec = BindSpec::table("double").in_schema(&schema_path);
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
    let schema_path = cat.default_schema().to_string();

    let mut spec = BindSpec::table("double").in_schema(&schema_path);
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
    let schema_path = cat.default_schema().to_string();

    let mut spec = BindSpec::table("echo").in_schema(&schema_path);
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
    let schema_path = cat.default_schema().to_string();

    // `repeat_inputs` emits each input row more than once, which is exactly
    // when a worker must say which input row each output row came from.
    let mut spec = BindSpec::table("repeat_inputs").in_schema(&schema_path);
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

/// Drain one input's continued output: its first answer, then a tick per
/// answer that said more. Returns each answer's values and provenance.
fn drain_continued(
    ex: &mut vgi_client::exchange::Exchange<'_>,
    input: &RecordBatch,
) -> Vec<(Vec<i64>, Option<Vec<i32>>)> {
    let mut answers = Vec::new();
    let mut out = ex.send(input).expect("exchange").expect("an answer");
    loop {
        answers.push((
            column_values(&out, 0),
            ex.parent_rows().map(<[i32]>::to_vec),
        ));
        if !ex.has_more_output() {
            return answers;
        }
        out = ex
            .continue_output()
            .expect("continue")
            .expect("a tick is answered");
    }
}

#[test]
fn a_classic_table_in_out_continues_its_output() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_path = cat.default_schema().to_string();

    // `paged_explode` emits 0..n-1 per input row, `page_size` rows per answer,
    // the rest through continuation ticks.
    let mut spec = BindSpec::table("paged_explode").in_schema(&schema_path);
    spec.function_type = FunctionType::TableInOut;
    spec.arguments = Arguments::new().named("page_size", ArgValue::Int(3));
    let bound = client
        .bind_with_input(&cat, &spec, &i64_schema("n"))
        .expect("`paged_explode` must exist in the example catalog");
    let mut ex = client
        .open_exchange(&bound, &ScanOptions::default())
        .expect("open exchange");

    let answers = drain_continued(&mut ex, &i64_batch("n", &[4, 0, 3]));
    let values: Vec<Vec<i64>> = answers.into_iter().map(|(v, _)| v).collect();
    assert_eq!(values, vec![vec![0, 1, 2], vec![3, 0, 1], vec![2]]);

    // Drained: the next input is a real one, and a tick is refused locally.
    assert!(ex.continue_output().is_err());
    let answers = drain_continued(&mut ex, &i64_batch("n", &[2]));
    assert_eq!(answers, vec![(vec![0, 1], None)]);

    ex.close().expect("close");
}

#[test]
fn a_blended_function_continues_with_provenance() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_path = cat.default_schema().to_string();

    let mut spec = BindSpec::table("blended_paged").in_schema(&schema_path);
    spec.function_type = FunctionType::TableInOut;
    spec.arguments = Arguments::new()
        .positional(ArgValue::Placeholder(DataType::Int64))
        .named("page_size", ArgValue::Int(2));
    let bound = client
        .bind_with_input(&cat, &spec, &i64_schema("n"))
        .expect("`blended_paged` must exist in the example catalog");
    let mut ex = client
        .open_exchange(&bound, &ScanOptions::default())
        .expect("open exchange");

    // Every answer's provenance indexes the one real input, ticks included.
    let answers = drain_continued(&mut ex, &i64_batch("n", &[1, 3]));
    assert_eq!(
        answers,
        vec![
            (vec![0, 0], Some(vec![0, 1])),
            (vec![1, 2], Some(vec![1, 1])),
        ]
    );
    ex.close().expect("close");
    drop(ex);

    // While output is pending, a real input is refused before it is sent.
    let mut ex2 = client
        .open_exchange(&bound, &ScanOptions::default())
        .expect("open exchange");
    ex2.send(&i64_batch("n", &[5])).expect("first page");
    assert!(ex2.has_more_output());
    assert!(ex2.send(&i64_batch("n", &[1])).is_err());
    ex2.close().expect("close drops the pending output");
}

/// The continued-output fixtures' failure and revalidation modes, on both
/// call paths: `blended_paged` (batched LATERAL) and `paged_explode` (stock
/// in-out). Contracts: the extension's
/// `table_in_out/continuation_resume_error_revalidate.test` header, with
/// `lateral_continuation.test` and `continuation_classic.test`.
const PAGED_FIXTURES: [&str; 2] = ["blended_paged", "paged_explode"];

/// Bind a paged fixture with `named` arguments, its input column `n`.
fn bind_paged(
    client: &mut VgiClient,
    cat: &vgi_client::AttachedCatalog,
    function: &str,
    named: &[(&str, ArgValue)],
) -> vgi_client::BoundFunction {
    let mut spec = BindSpec::table(function).in_schema(cat.default_schema());
    spec.function_type = FunctionType::TableInOut;
    let mut args = Arguments::new();
    if function == "blended_paged" {
        args = args.positional(ArgValue::Placeholder(DataType::Int64));
    }
    for (name, value) in named {
        args = args.named(*name, value.clone());
    }
    spec.arguments = args;
    client
        .bind_with_input(cat, &spec, &i64_schema("n"))
        .unwrap_or_else(|e| panic!("bind `{function}`: {e}"))
}

#[test]
fn fail_on_page_fails_that_reply_counting_per_input_batch() {
    skip_without_worker!();
    for function in PAGED_FIXTURES {
        let mut client = connect();
        let cat = client
            .attach("example", AttachOptions::default())
            .expect("attach");
        let bound = bind_paged(
            &mut client,
            &cat,
            function,
            &[
                ("page_size", ArgValue::Int(2)),
                ("fail_on_page", ArgValue::Int(3)),
            ],
        );
        let mut ex = client
            .open_exchange(&bound, &ScanOptions::default())
            .expect("open exchange");
        // Two replies: page 3 is never reached.
        let answers = drain_continued(&mut ex, &i64_batch("n", &[3]));
        assert_eq!(answers.len(), 2, "{function}: {answers:?}");
        // The count restarts with the next input batch: pages 1 and 2 go out,
        // the third (a tick's reply) fails.
        ex.send(&i64_batch("n", &[6])).expect("page 1");
        assert!(ex.has_more_output());
        ex.continue_output().expect("page 2");
        assert!(ex.has_more_output());
        let error = ex.continue_output().expect_err("page 3 fails");
        assert!(
            error.to_string().contains("failing on page 3 as asked"),
            "{function}: {error}"
        );
        drop(ex);

        // fail_on_page = 1 fails the input's first reply.
        let bound = bind_paged(
            &mut client,
            &cat,
            function,
            &[("fail_on_page", ArgValue::Int(1))],
        );
        let mut ex = client
            .open_exchange(&bound, &ScanOptions::default())
            .expect("open exchange");
        let error = ex.send(&i64_batch("n", &[1])).expect_err("page 1 fails");
        assert!(
            error.to_string().contains("failing on page 1 as asked"),
            "{function}: {error}"
        );
    }
}

#[test]
fn revalidate_answers_by_whether_the_request_is_conditional() {
    skip_without_worker!();
    for function in PAGED_FIXTURES {
        let mut client = connect();
        let cat = client
            .attach("example", AttachOptions::default())
            .expect("attach");
        // cache_ttl is ignored under revalidate.
        let named = [
            ("page_size", ArgValue::Int(1)),
            ("cache_ttl", ArgValue::Int(300)),
            ("revalidate", ArgValue::Bool(true)),
        ];
        let bound = bind_paged(&mut client, &cat, function, &named);

        // A plain request: the whole output in one reply, ttl 0, etag v1,
        // revalidatable.
        let mut ex = client
            .open_exchange(&bound, &ScanOptions::default())
            .expect("open exchange");
        let answers = drain_continued(&mut ex, &i64_batch("n", &[3]));
        let values: Vec<Vec<i64>> = answers.into_iter().map(|(v, _)| v).collect();
        assert_eq!(values, vec![vec![0, 1, 2]], "{function}");
        let cc = ex.cache_control().expect("cache control").clone();
        assert_eq!(cc.ttl_seconds, Some(0), "{function}");
        assert_eq!(cc.etag.as_deref(), Some("v1"), "{function}");
        assert!(cc.revalidatable && !cc.not_modified, "{function}");
        ex.close().expect("close");
        drop(ex);

        // A conditional request: the data changed (+1000), paged as usual,
        // the first reply ttl 0, etag v2, revalidatable, not not_modified.
        let opts = ScanOptions {
            if_none_match: Some("v1".to_string()),
            ..Default::default()
        };
        let mut ex = client.open_exchange(&bound, &opts).expect("open exchange");
        let first = ex
            .send(&i64_batch("n", &[3]))
            .expect("send")
            .expect("reply");
        assert_eq!(column_values(&first, 0), vec![1000], "{function}");
        let cc = ex.cache_control().expect("cache control").clone();
        assert_eq!(cc.ttl_seconds, Some(0), "{function}");
        assert_eq!(cc.etag.as_deref(), Some("v2"), "{function}");
        assert!(cc.revalidatable && !cc.not_modified, "{function}");
        let mut values = vec![1000];
        while ex.has_more_output() {
            let page = ex.continue_output().expect("tick").expect("reply");
            values.extend(column_values(&page, 0));
        }
        assert_eq!(values, vec![1000, 1001, 1002], "{function}");
        ex.close().expect("close");
    }
}

#[test]
fn not_modified_more_is_refused_by_the_client() {
    skip_without_worker!();
    for function in PAGED_FIXTURES {
        let mut client = connect();
        let cat = client
            .attach("example", AttachOptions::default())
            .expect("attach");
        let named = [
            ("page_size", ArgValue::Int(2)),
            ("revalidate", ArgValue::Bool(true)),
            ("not_modified_more", ArgValue::Bool(true)),
        ];
        let bound = bind_paged(&mut client, &cat, function, &named);

        // Only a conditional request is answered with it: a plain one is the
        // whole output in one reply, as under revalidate alone.
        let mut ex = client
            .open_exchange(&bound, &ScanOptions::default())
            .expect("open exchange");
        let answers = drain_continued(&mut ex, &i64_batch("n", &[3]));
        assert_eq!(answers.len(), 1, "{function}: {answers:?}");
        ex.close().expect("close");
        drop(ex);

        // On a conditional request the first reply is empty, not_modified and
        // says more: a 304 that is not its input's only reply.
        let opts = ScanOptions {
            if_none_match: Some("v1".to_string()),
            ..Default::default()
        };
        let mut ex = client.open_exchange(&bound, &opts).expect("open exchange");
        let error = ex
            .send(&i64_batch("n", &[3]))
            .expect_err("not_modified with more must be refused");
        assert!(
            error
                .to_string()
                .contains("not_modified on a continued reply"),
            "{function}: {error}"
        );
    }
}

#[test]
fn a_buffered_function_sinks_then_sources() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_path = cat.default_schema().to_string();

    let mut spec = BindSpec::table("buffer_input").in_schema(&schema_path);
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

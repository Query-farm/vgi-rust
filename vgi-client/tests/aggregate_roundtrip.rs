// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! End-to-end aggregate calls against a real worker.
//!
//! Aggregates are the one surface no other VGI client implements, so this file
//! is the only executable statement of what the client must do.

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{cast::AsArray, types::Int64Type, ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use vgi_client::{
    with_group_ids, ArgValue, Arguments, AttachOptions, BindSpec, FunctionType, VgiClient,
    GROUP_COLUMN_NAME,
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

fn value_schema() -> Schema {
    Schema::new(vec![Field::new("value", DataType::Int64, true)])
}

fn values(n: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(value_schema()),
        vec![Arc::new(Int64Array::from(n.to_vec())) as ArrayRef],
    )
    .expect("value batch")
}

/// `vgi_sum(value)` — the simplest grouped aggregate in the example catalog.
fn sum_spec(schema_name: &str) -> BindSpec {
    let mut spec = BindSpec::table("vgi_sum").in_schema(schema_name);
    spec.function_type = FunctionType::Aggregate;
    spec.arguments = Arguments::new().positional(ArgValue::Placeholder(DataType::Int64));
    spec
}

#[test]
fn sums_one_group() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = sum_spec(&schema_name);
    let agg = client
        .aggregate_bind(&cat, &spec, &value_schema())
        .expect("aggregate_bind");
    assert!(
        !agg.execution_id().0.is_empty(),
        "bind must mint an execution id"
    );

    let batch = with_group_ids(&[0, 0, 0], &values(&[1, 2, 3])).expect("group batch");
    client
        .aggregate_update(&cat, &agg, &batch)
        .expect("aggregate_update");

    let out = client
        .aggregate_finalize(&cat, &agg, &[0])
        .expect("aggregate_finalize");
    assert_eq!(out.num_rows(), 1);
    assert_eq!(out.column(0).as_primitive::<Int64Type>().value(0), 6);

    client.aggregate_destroy(&cat, &agg, &[0]).expect("destroy");
}

#[test]
fn keeps_groups_apart() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = sum_spec(&schema_name);
    let agg = client
        .aggregate_bind(&cat, &spec, &value_schema())
        .expect("bind");

    // Two groups interleaved in one batch, plus a second batch — the state has
    // to survive across calls and stay partitioned by group id.
    client
        .aggregate_update(
            &cat,
            &agg,
            &with_group_ids(&[0, 1, 0, 1], &values(&[1, 10, 2, 20])).unwrap(),
        )
        .expect("update 1");
    client
        .aggregate_update(
            &cat,
            &agg,
            &with_group_ids(&[1, 0], &values(&[100, 3])).unwrap(),
        )
        .expect("update 2");

    let out = client
        .aggregate_finalize(&cat, &agg, &[0, 1])
        .expect("finalize");
    assert_eq!(out.num_rows(), 2, "one row per requested group, in order");
    let col = out.column(0).as_primitive::<Int64Type>();
    assert_eq!(col.value(0), 6, "group 0 = 1+2+3");
    assert_eq!(col.value(1), 130, "group 1 = 10+20+100");

    client
        .aggregate_destroy(&cat, &agg, &[0, 1])
        .expect("destroy");
}

#[test]
fn finalize_answers_in_the_order_asked() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = sum_spec(&schema_name);
    let agg = client
        .aggregate_bind(&cat, &spec, &value_schema())
        .expect("bind");
    client
        .aggregate_update(
            &cat,
            &agg,
            &with_group_ids(&[0, 1, 2], &values(&[1, 2, 3])).unwrap(),
        )
        .expect("update");

    // Ask out of order: the answer must follow the request, not the group id.
    let out = client
        .aggregate_finalize(&cat, &agg, &[2, 0, 1])
        .expect("finalize");
    let col = out.column(0).as_primitive::<Int64Type>();
    assert_eq!(
        vec![col.value(0), col.value(1), col.value(2)],
        vec![3, 1, 2],
        "results must line up with the requested group ids"
    );

    client
        .aggregate_destroy(&cat, &agg, &[2, 0, 1])
        .expect("destroy");
}

#[test]
fn a_batch_without_group_ids_is_refused_before_the_rpc() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = sum_spec(&schema_name);
    let agg = client
        .aggregate_bind(&cat, &spec, &value_schema())
        .expect("bind");

    // Raw values with no group column — the worker would reject this, but the
    // client should say so first and name the fix.
    let err = client
        .aggregate_update(&cat, &agg, &values(&[1, 2]))
        .expect_err("must be refused");
    assert!(
        err.to_string().contains(GROUP_COLUMN_NAME),
        "the error should name the missing column: {err}"
    );

    client.aggregate_destroy(&cat, &agg, &[0]).expect("destroy");
}

#[test]
fn two_executions_aggregate_independently() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();
    let spec = sum_spec(&schema_name);

    // Each bind mints its own execution; folding into one must not disturb the
    // other. This is the property parallel aggregation rests on.
    let a = client
        .aggregate_bind(&cat, &spec, &value_schema())
        .expect("bind a");
    let b = client
        .aggregate_bind(&cat, &spec, &value_schema())
        .expect("bind b");
    assert_ne!(
        a.execution_id().0,
        b.execution_id().0,
        "two binds must not share an execution id"
    );

    client
        .aggregate_update(&cat, &a, &with_group_ids(&[0], &values(&[5])).unwrap())
        .expect("update a");
    client
        .aggregate_update(&cat, &b, &with_group_ids(&[0], &values(&[50])).unwrap())
        .expect("update b");

    let ra = client
        .aggregate_finalize(&cat, &a, &[0])
        .expect("finalize a");
    let rb = client
        .aggregate_finalize(&cat, &b, &[0])
        .expect("finalize b");
    assert_eq!(ra.column(0).as_primitive::<Int64Type>().value(0), 5);
    assert_eq!(rb.column(0).as_primitive::<Int64Type>().value(0), 50);

    client.aggregate_destroy(&cat, &a, &[0]).expect("destroy a");
    client.aggregate_destroy(&cat, &b, &[0]).expect("destroy b");
}

/// `vgi_window_sum(value)` — a windowed aggregate over a materialised partition.
fn window_sum_spec(schema_name: &str) -> BindSpec {
    let mut spec = BindSpec::table("vgi_window_sum").in_schema(schema_name);
    spec.function_type = FunctionType::Aggregate;
    spec.arguments = Arguments::new().positional(ArgValue::Placeholder(DataType::Int64));
    spec
}

#[test]
fn evaluates_a_window_frame_over_a_shipped_partition() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = window_sum_spec(&schema_name);
    let agg = client
        .aggregate_bind(&cat, &spec, &value_schema())
        .expect("bind");

    // Ship the whole partition once, then ask for individual rows' frames.
    let partition = values(&[1, 2, 3, 4]);
    let part = client
        .window_init(&agg, 0, &partition)
        .expect("window_init");

    // Frame [0, 4) is the whole partition: 1+2+3+4.
    let out = client
        .window_evaluate(&part, 0, &[(0, 4)])
        .expect("window_evaluate");
    assert_eq!(out.num_rows(), 1);
    assert_eq!(out.column(0).as_primitive::<Int64Type>().value(0), 10);

    // A narrower frame sums only its rows: [1, 3) is 2+3.
    let out = client
        .window_evaluate(&part, 1, &[(1, 3)])
        .expect("window_evaluate");
    assert_eq!(out.column(0).as_primitive::<Int64Type>().value(0), 5);

    client.window_destroy(&part).expect("window_destroy");
    client.aggregate_destroy(&cat, &agg, &[0]).expect("destroy");
}

#[test]
fn evaluates_several_window_rows_in_one_call() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = window_sum_spec(&schema_name);
    let agg = client
        .aggregate_bind(&cat, &spec, &value_schema())
        .expect("bind");
    let part = client
        .window_init(&agg, 0, &values(&[1, 2, 3, 4]))
        .expect("window_init");

    // Three rows, one frame each — running prefix sums.
    let out = client
        .window_evaluate_batch(&part, 0, &[1, 1, 1], &[(0, 1), (0, 2), (0, 3)])
        .expect("window_evaluate_batch");
    assert_eq!(out.num_rows(), 3, "one output row per requested row");
    let col = out.column(0).as_primitive::<Int64Type>();
    assert_eq!(
        vec![col.value(0), col.value(1), col.value(2)],
        vec![1, 3, 6],
        "prefix sums over the shipped partition"
    );

    client.window_destroy(&part).expect("window_destroy");
    client.aggregate_destroy(&cat, &agg, &[0]).expect("destroy");
}

#[test]
fn a_frame_count_that_disagrees_with_the_frame_list_is_refused() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = window_sum_spec(&schema_name);
    let agg = client
        .aggregate_bind(&cat, &spec, &value_schema())
        .expect("bind");
    let part = client
        .window_init(&agg, 0, &values(&[1, 2]))
        .expect("window_init");

    // The flattened frame arrays are only interpretable if the per-row counts
    // sum to their length; a mismatch would silently misattribute frames.
    assert!(
        client
            .window_evaluate_batch(&part, 0, &[1, 1], &[(0, 1)])
            .is_err(),
        "frames_per_row summing to 2 with 1 frame supplied must be refused"
    );

    client.window_destroy(&part).expect("window_destroy");
    client.aggregate_destroy(&cat, &agg, &[0]).expect("destroy");
}

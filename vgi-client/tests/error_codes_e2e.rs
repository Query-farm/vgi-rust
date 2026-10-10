// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The SDK's own errors reach a client with their canonical code.
//!
//! A caller decides "my input was wrong" versus "the worker broke" from
//! `vgi_rpc.error_code` alone (vgi-rpc `WIRE_PROTOCOL.md` §8). These calls
//! mirror the DuckDB extension's `unary_error_propagation.test`, which runs
//! against every SDK's example worker, and assert the code decoded off the wire
//! -- not just the message.

use std::path::PathBuf;

use arrow_schema::{DataType, Field, Schema};
use vgi_client::{ArgValue, Arguments, AttachOptions, BindSpec, FunctionType, VgiClient};

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

/// `SELECT example.main.double('abc')`: the SDK's type-bound check rejects a
/// string input column.
#[test]
fn scalar_type_rejection_is_invalid_argument() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");

    let mut spec = BindSpec::table("double").in_schema(cat.default_schema());
    spec.function_type = FunctionType::Scalar;
    spec.arguments = Arguments::new().positional(ArgValue::Placeholder(DataType::Utf8));
    let input = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);

    let err = client
        .bind_with_input(&cat, &spec, &input)
        .expect_err("a string is not multipliable");
    assert_eq!(err.error_code(), "INVALID_ARGUMENT", "{err}");
    assert_eq!(err.error_kind(), "", "no error_kind is invented: {err}");
}

/// `SELECT * FROM example.main.sequence(10, batch_size := 0)`: the example
/// worker's own argument check.
#[test]
fn table_argument_constraint_is_invalid_argument() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");

    let spec = BindSpec::table("sequence")
        .in_schema(cat.default_schema())
        .with_arguments(Arguments::new().positional(10i64).named("batch_size", 0i64));

    let err = client
        .bind(&cat, &spec)
        .expect_err("batch_size 0 is rejected");
    assert!(err.message.contains("must be >= 1"), "{err}");
    assert_eq!(err.error_code(), "INVALID_ARGUMENT", "{err}");

    // The attachment stays usable after a coded error.
    let ok = BindSpec::table("sequence")
        .in_schema(cat.default_schema())
        .with_arguments(Arguments::new().positional(10i64));
    client.bind(&cat, &ok).expect("a valid bind still succeeds");
}

/// A function the catalog does not declare is a failed lookup.
#[test]
fn unknown_function_is_not_found() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");

    let spec = BindSpec::table("no_such_function_anywhere").in_schema(cat.default_schema());
    let err = client.bind(&cat, &spec).expect_err("unknown function");
    assert_eq!(err.error_code(), "NOT_FOUND", "{err}");
}

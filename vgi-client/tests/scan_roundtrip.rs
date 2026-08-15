// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! End-to-end producer scans against a real worker.
//!
//! Drives bind → init → drain over the subprocess transport, and covers the
//! things that ride the init request: projection, the worker's `max_workers`
//! advice, and the `vgi.cache.*` directives a cacheable result advertises.

use std::path::PathBuf;

use arrow_array::{cast::AsArray, types::Int64Type};
use vgi_client::{Arguments, AttachOptions, BindSpec, ScanOptions, VgiClient};

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

/// Total rows across a batch list.
fn total_rows(batches: &[arrow_array::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[test]
fn scans_a_table_function_end_to_end() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = BindSpec::table("rowid_sequence")
        .in_schema(&schema_name)
        .with_arguments(Arguments::new().positional(5i64));
    let bound = client.bind(&cat, &spec).expect("bind");

    assert!(
        !bound.output_schema().fields().is_empty(),
        "bind must resolve an output schema"
    );

    let mut scan = client.scan(&bound, &ScanOptions::default()).expect("init");
    assert!(
        !scan.execution_id().0.is_empty(),
        "the worker must mint an execution_id"
    );
    assert!(scan.max_workers() >= 1, "max_workers must be positive");

    let batches = scan.collect().expect("drain");
    assert_eq!(total_rows(&batches), 5, "asked for 5 rows");
}

#[test]
fn a_large_scan_drains_every_row_in_order() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    const N: i64 = 5000;
    let spec = BindSpec::table("rowid_sequence")
        .in_schema(&schema_name)
        .with_arguments(Arguments::new().positional(N));
    let bound = client.bind(&cat, &spec).expect("bind");
    let mut scan = client.scan(&bound, &ScanOptions::default()).expect("init");
    let batches = scan.collect().expect("drain");

    assert_eq!(total_rows(&batches), N as usize);

    // Assert on the values rather than the batch count: how the worker chunks
    // its output is its business, and an earlier version of this test wrongly
    // assumed 5000 rows could not fit in one batch. What the client must
    // guarantee is that draining loses nothing and reorders nothing.
    let mut seen = Vec::with_capacity(N as usize);
    for b in &batches {
        let col = b.column(0).as_primitive::<Int64Type>();
        seen.extend((0..b.num_rows()).map(|i| col.value(i)));
    }
    assert_eq!(seen.len(), N as usize);
    assert_eq!(seen.first().copied(), Some(0));
    assert_eq!(seen.last().copied(), Some(N - 1));
    assert!(
        seen.windows(2).all(|w| w[1] == w[0] + 1),
        "the drained sequence has a gap or a repeat"
    );
}

#[test]
fn a_zero_row_scan_ends_cleanly() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = BindSpec::table("rowid_sequence")
        .in_schema(&schema_name)
        .with_arguments(Arguments::new().positional(0i64));
    let bound = client.bind(&cat, &spec).expect("bind");
    let mut scan = client.scan(&bound, &ScanOptions::default()).expect("init");

    assert_eq!(total_rows(&scan.collect().expect("drain")), 0);
    // A drained scan stays drained rather than blocking or repeating.
    assert!(scan.next_batch().expect("post-EOS tick").is_none());
}

#[test]
fn projection_narrows_both_the_schema_and_the_batches() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = BindSpec::table("rowid_sequence")
        .in_schema(&schema_name)
        .with_arguments(Arguments::new().positional(3i64));
    let bound = client.bind(&cat, &spec).expect("bind");
    let full_width = bound.output_schema().fields().len();
    if full_width < 2 {
        return; // nothing to narrow
    }

    let opts = ScanOptions {
        projection: Some(vec![0]),
        ..Default::default()
    };
    let mut scan = client.scan(&bound, &opts).expect("init");
    assert_eq!(
        scan.schema().fields().len(),
        1,
        "the client's view of the schema must narrow with the projection"
    );

    let batches = scan.collect().expect("drain");
    for b in &batches {
        assert_eq!(
            b.num_columns(),
            1,
            "worker emitted {} columns for a 1-column projection",
            b.num_columns()
        );
    }
    assert_eq!(total_rows(&batches), 3);
}

#[test]
fn an_out_of_range_projection_is_rejected_before_any_rpc() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = BindSpec::table("rowid_sequence")
        .in_schema(&schema_name)
        .with_arguments(Arguments::new().positional(1i64));
    let bound = client.bind(&cat, &spec).expect("bind");

    let opts = ScanOptions {
        projection: Some(vec![99]),
        ..Default::default()
    };
    assert!(
        client.scan(&bound, &opts).is_err(),
        "a projection index past the end of the bind schema must not reach the worker"
    );
}

#[test]
fn named_arguments_reach_the_worker() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    // `cacheable_numbers(n, ttl)` takes both arguments by name.
    let spec = BindSpec::table("cacheable_numbers")
        .in_schema(&schema_name)
        .with_arguments(Arguments::new().named("n", 4i64).named("ttl", 300i64));
    let bound = client.bind(&cat, &spec).expect("bind");
    let mut scan = client.scan(&bound, &ScanOptions::default()).expect("init");
    let batches = scan.collect().expect("drain");

    assert_eq!(
        total_rows(&batches),
        4,
        "the named `n` argument must have reached the worker"
    );

    // And the values should be the sequence the fixture generates.
    let first = &batches[0];
    let col = first.column(0).as_primitive::<Int64Type>();
    assert_eq!(col.value(0), 0, "expected a 0-based sequence");
}

#[test]
fn a_cacheable_result_advertises_its_directives() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = BindSpec::table("cacheable_numbers")
        .in_schema(&schema_name)
        .with_arguments(Arguments::new().named("n", 2i64).named("ttl", 300i64));
    let bound = client.bind(&cat, &spec).expect("bind");
    let mut scan = client.scan(&bound, &ScanOptions::default()).expect("init");

    assert!(
        scan.cache_control().is_none(),
        "nothing is advertised before the first batch arrives"
    );

    let _ = scan.collect().expect("drain");

    let cc = scan
        .cache_control()
        .expect("`cacheable_numbers` advertises vgi.cache.* on its first batch");
    assert_eq!(
        cc.ttl_seconds,
        Some(300),
        "the fixture was asked for a 300s TTL"
    );
    assert!(cc.is_cacheable(), "a TTL is the opt-in; got {cc:?}");
    assert!(!cc.no_store);
}

#[test]
fn a_no_store_result_says_so() {
    skip_without_worker!();
    let mut client = connect();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = BindSpec::table("cache_no_store").in_schema(&schema_name);
    let Ok(bound) = client.bind(&cat, &spec) else {
        return; // fixture not present in this catalog build
    };
    let mut scan = client.scan(&bound, &ScanOptions::default()).expect("init");
    let _ = scan.collect().expect("drain");

    if let Some(cc) = scan.cache_control() {
        assert!(
            cc.no_store,
            "the `cache_no_store` fixture must set no_store"
        );
        assert!(
            !cc.is_cacheable(),
            "no_store overrides any freshness key: {cc:?}"
        );
    }
}

#[test]
fn parallel_connections_share_one_scan_and_partition_the_rows() {
    skip_without_worker!();

    // `partitioned_batch_index` advertises max_workers = 4 and hands each
    // connection a disjoint slice. This is the fan-out contract: the first
    // connection's init mints an execution_id, and every later connection
    // passes it back so the worker knows they are the same scan rather than
    // four independent ones.
    const N: i64 = 400;

    let mut primary = connect();
    let cat = primary
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = BindSpec::table("partitioned_batch_index")
        .in_schema(&schema_name)
        .with_arguments(Arguments::new().positional(N));
    let bound = primary.bind(&cat, &spec).expect("bind");

    let mut all: Vec<i64> = Vec::new();
    let execution_id;
    let max_workers;
    {
        let mut scan = primary.scan(&bound, &ScanOptions::default()).expect("init");
        execution_id = scan.execution_id().clone();
        max_workers = scan.max_workers();
        assert!(
            max_workers > 1,
            "this fixture exists to exercise fan-out; got max_workers={max_workers}"
        );
        for b in scan.collect().expect("drain primary") {
            let col = b.column(0).as_primitive::<Int64Type>();
            all.extend((0..b.num_rows()).map(|i| col.value(i)));
        }
    }

    // Secondary connections join the same execution.
    let mut secondaries = Vec::new();
    for _ in 1..max_workers {
        let mut client = connect();
        let cat2 = client
            .attach("example", AttachOptions::default())
            .expect("attach");
        let bound2 = client.bind(&cat2, &spec).expect("bind");
        secondaries.push((client, bound2));
    }
    for (client, bound2) in &mut secondaries {
        let opts = ScanOptions {
            execution_id: Some(execution_id.clone()),
            ..Default::default()
        };
        let mut scan = client.scan(bound2, &opts).expect("secondary init");
        for b in scan.collect().expect("drain secondary") {
            let col = b.column(0).as_primitive::<Int64Type>();
            all.extend((0..b.num_rows()).map(|i| col.value(i)));
        }
    }

    all.sort_unstable();
    let before = all.len();
    all.dedup();
    assert_eq!(
        all.len(),
        before,
        "fan-out duplicated rows — the secondaries did not join the primary's execution"
    );
    assert_eq!(
        all.len(),
        N as usize,
        "the union of all connections must be the whole scan"
    );
    assert_eq!(all.first().copied(), Some(0));
    assert_eq!(all.last().copied(), Some(N - 1));
}

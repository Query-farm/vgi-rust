// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! End-to-end catalog discovery against a real worker.
//!
//! Spawns `vgi-example-worker` over the subprocess transport and walks the path
//! a query engine takes at ATTACH time: list catalogs, attach, enumerate
//! schemas, tables, views, functions and macros, read a single table back, then
//! detach.
//!
//! This is the P2 exit criterion. Unit tests can prove the wire encoding is
//! self-consistent; only a real worker proves the client and the worker agree.

use std::path::PathBuf;

use vgi_client::{
    Arguments, AttachOptions, FunctionKind, MacroKind, ScanBranchesResolution, VgiClient,
};

/// Locate the sibling `vgi-example-worker` binary.
///
/// The test executable lives in `target/<profile>/deps/`, so the worker is two
/// levels up. Deriving it from `current_exe` rather than hardcoding `debug`
/// keeps the test working under `cargo test --release`.
fn example_worker() -> Option<PathBuf> {
    let mut dir = std::env::current_exe().ok()?;
    dir.pop(); // deps/
    dir.pop(); // <profile>/
    let exe = dir.join(if cfg!(windows) {
        "vgi-example-worker.exe"
    } else {
        "vgi-example-worker"
    });
    exe.exists().then_some(exe)
}

/// Skip rather than fail when the worker hasn't been built — `cargo test -p
/// vgi-client` alone does not build a sibling binary.
macro_rules! worker_or_skip {
    () => {
        match example_worker() {
            Some(p) => p,
            None => {
                eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
                return;
            }
        }
    };
}

fn connect() -> Option<VgiClient> {
    let worker = example_worker()?;
    Some(VgiClient::connect_subprocess(&[worker.as_os_str()]).expect("connect to example worker"))
}

#[test]
fn lists_the_catalogs_the_worker_serves() {
    let _ = worker_or_skip!();
    let mut client = connect().unwrap();

    let catalogs = client.catalogs().expect("catalog_catalogs");
    assert!(
        !catalogs.is_empty(),
        "worker must advertise at least one catalog"
    );
    assert!(
        catalogs.iter().any(|c| c.name == "example"),
        "expected the `example` catalog, got {:?}",
        catalogs.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
}

#[test]
fn attaches_and_reports_a_default_schema() {
    let _ = worker_or_skip!();
    let mut client = connect().unwrap();

    let cat = client
        .attach("example", AttachOptions::default())
        .expect("catalog_attach");
    assert!(
        !cat.handle().0.is_empty(),
        "attach must return a session handle"
    );
    assert!(
        !cat.default_schema().is_empty(),
        "attach must name a default schema"
    );

    client.detach(&cat).expect("catalog_detach");
}

#[test]
fn walks_the_whole_discovery_surface() {
    let _ = worker_or_skip!();
    let mut client = connect().unwrap();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");

    let schemas = client.schemas(&cat).expect("catalog_schemas");
    assert!(!schemas.is_empty(), "expected at least one schema");

    let mut total_tables = 0usize;
    let mut total_functions = 0usize;
    for schema in &schemas {
        // Every listed schema must also resolve individually.
        let one = client
            .schema_get(&cat, &schema.name)
            .expect("catalog_schema_get")
            .unwrap_or_else(|| panic!("schema `{}` was listed but does not resolve", schema.name));
        assert_eq!(one.name, schema.name);

        total_tables += client.tables(&cat, &schema.name).expect("tables").len();
        client.views(&cat, &schema.name).expect("views");
        client
            .macros(&cat, &schema.name, MacroKind::Scalar)
            .expect("macros");

        // Three kinds, not five: `SchemaObjectType` has a single
        // `TABLE_FUNCTION` member covering producer, buffered and streaming
        // table-in-out shapes. Which shape a function is comes back on
        // `FunctionInfo::function_type`, not on the listing filter.
        for kind in [
            FunctionKind::Table,
            FunctionKind::Scalar,
            FunctionKind::Aggregate,
        ] {
            total_functions += client
                .functions(&cat, &schema.name, kind)
                .unwrap_or_else(|e| panic!("functions({kind:?}) failed: {e}"))
                .len();
        }
    }

    assert!(total_tables > 0, "the example catalog defines tables");
    assert!(total_functions > 0, "the example catalog defines functions");

    client.detach(&cat).expect("detach");
}

/// `catalog_schema_contents_functions` carries a `type` column that must reach
/// the wire spelled `type`, not `r#type`. Before the derive was fixed this call
/// failed outright — so a green assertion here is the regression guard.
#[test]
fn function_listing_survives_the_raw_identifier_column() {
    let _ = worker_or_skip!();
    let mut client = connect().unwrap();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema = cat.default_schema().to_string();

    let tables = client
        .functions(&cat, &schema, FunctionKind::Table)
        .expect("listing table functions must not fail on the `type` column");
    assert!(
        !tables.is_empty(),
        "the example catalog defines table functions in `{schema}`"
    );

    // The kind filter must actually filter, not return everything.
    let scalars = client
        .functions(&cat, &schema, FunctionKind::Scalar)
        .expect("scalar functions");
    let table_names: Vec<&String> = tables.iter().map(|f| &f.name).collect();
    let scalar_names: Vec<&String> = scalars.iter().map(|f| &f.name).collect();
    assert_ne!(
        table_names, scalar_names,
        "table and scalar listings should differ; the `type` filter looks inert"
    );

    client.detach(&cat).expect("detach");
}

#[test]
fn reads_one_table_back_by_name() {
    let _ = worker_or_skip!();
    let mut client = connect().unwrap();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema = cat.default_schema().to_string();

    let listed = client.tables(&cat, &schema).expect("tables");
    let Some(first) = listed.first() else {
        client.detach(&cat).ok();
        return; // no tables in the default schema; the walk test covers the rest
    };

    let fetched = client
        .table_get(&cat, &schema, &first.name, None)
        .expect("catalog_table_get")
        .expect("a listed table must resolve by name");
    assert_eq!(fetched.name, first.name);
    assert_eq!(fetched.schema_name, first.schema_name);

    assert!(
        client
            .table_get(&cat, &schema, "definitely_not_a_real_table", None)
            .expect("a miss is not an error")
            .is_none(),
        "an unknown table must come back as None, not an error",
    );

    client.detach(&cat).expect("detach");
}

#[test]
fn decodes_and_validates_catalog_scan_branches() {
    let _ = worker_or_skip!();
    let mut client = connect().unwrap();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");

    let table = |client: &mut VgiClient, name: &str| {
        client
            .table_get(&cat, "data", name, None)
            .unwrap_or_else(|error| panic!("catalog_table_get({name}) failed: {error}"))
            .unwrap_or_else(|| panic!("example worker did not advertise data.{name}"))
    };

    let mut numbers = table(&mut client, "multi_branch_numbers");
    let inlined_single_branch = table(&mut client, "large_sequence").scan_function;
    assert!(
        inlined_single_branch
            .as_ref()
            .is_some_and(|bytes| !bytes.0.is_empty()),
        "large_sequence must provide valid legacy inline scan metadata"
    );
    numbers.scan_function = inlined_single_branch;
    let numbers = client
        .table_scan_branches(&cat, &numbers, None)
        .expect("decode multi_branch_numbers");
    assert_eq!(numbers.resolution, ScanBranchesResolution::BranchesRpc);
    assert_eq!(numbers.branches.len(), 2);
    assert!(numbers
        .branches
        .iter()
        .all(|branch| branch.function_name == "sequence"));
    assert!(numbers
        .branches
        .iter()
        .all(|branch| branch.branch_filter.is_none()));

    let filtered = table(&mut client, "multi_branch_filtered_numbers");
    let filtered = client
        .table_scan_branches(&cat, &filtered, None)
        .expect("decode multi_branch_filtered_numbers");
    assert_eq!(
        filtered
            .branches
            .iter()
            .map(|branch| branch.branch_filter.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("n < 50"), Some("n >= 50")],
        "branch order and branch-local filters must survive nested IPC decoding"
    );

    let format = table(&mut client, "multi_branch_format");
    let format = client
        .table_scan_branches(&cat, &format, None)
        .expect("decode multi_branch_format");
    let [format] = format.branches.as_slice() else {
        panic!("multi_branch_format must have exactly one branch")
    };
    assert_eq!(format.format_name.as_deref(), Some("csv"));
    assert!(format.function_name.is_empty());
    let options =
        Arguments::from_scan_arguments(&format.format_options.as_ref().expect("format options").0)
            .expect("decode format options");
    assert_eq!(
        options
            .named_values()
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<Vec<_>>(),
        ["delim", "header", "nullstr"]
    );

    let empty = table(&mut client, "multi_branch_empty");
    let error = client
        .table_scan_branches(&cat, &empty, None)
        .expect_err("an empty branches list is invalid");
    assert!(
        error.message.contains("zero scan branches"),
        "unexpected error: {error}"
    );

    let two_writable = table(&mut client, "multi_branch_two_writable");
    let error = client
        .table_scan_branches(&cat, &two_writable, None)
        .expect_err("more than one writable branch is ambiguous");
    assert!(
        error.message.contains("declared 2 writable branches"),
        "unexpected error: {error}"
    );

    client.detach(&cat).expect("detach");
}

#[test]
fn reports_a_catalog_version() {
    let _ = worker_or_skip!();
    let mut client = connect().unwrap();
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");

    let version = client.catalog_version(&cat).expect("catalog_version");
    assert!(version >= 0, "version counter should not be negative");

    client.detach(&cat).expect("detach");
}

#[test]
fn transactions_are_optional_and_round_trip_when_offered() {
    let _ = worker_or_skip!();
    let mut client = connect().unwrap();
    let mut cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");

    client.begin_transaction(&mut cat).expect("begin");
    if cat.transaction().is_some() {
        // Reads inside the transaction thread the handle automatically.
        client.schemas(&cat).expect("schemas inside a transaction");
        client.commit(&mut cat).expect("commit");
        assert!(cat.transaction().is_none(), "commit must clear the handle");
    } else {
        // A worker may decline to open one; committing then must be a no-op
        // rather than an error.
        client
            .commit(&mut cat)
            .expect("commit with no open transaction");
    }

    client.detach(&cat).expect("detach");
}

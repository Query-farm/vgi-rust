// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `VgiClient::load_catalog` against the cross-SDK `catalog_contents` fixture
//! catalogs the example worker serves (`contents_probe` / `_broken` /
//! `_legacy` / `_reval` / `_memory`), asserting which RPCs were sent.
//!
//! The client honours the capability the way the DuckDB extension does: one
//! `catalog_contents` call when advertised, the per-schema calls when not (never
//! sending `catalog_contents` then), a fallback when it fails, and conditional
//! revalidation with the held etag.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use vgi_client::{
    AttachOptions, CatalogLoadSource, CatalogSnapshot, ExchangeStream, OnConflict, ProducerStream,
    Result, StreamTransport, VgiClient, VgiTransport,
};
use vgi_rpc::wire::Metadata;

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

/// Records the method of every unary call, then forwards it.
struct Recording {
    inner: StreamTransport,
    calls: Arc<Mutex<Vec<String>>>,
}

impl VgiTransport for Recording {
    fn call_unary(&mut self, method: &str, params: &RecordBatch) -> Result<RecordBatch> {
        self.calls.lock().unwrap().push(method.to_string());
        self.inner.call_unary(method, params)
    }
    fn open_producer<'a>(
        &'a mut self,
        method: &str,
        params: &RecordBatch,
        metadata: Option<Metadata>,
        has_header: bool,
    ) -> Result<Box<dyn ProducerStream + 'a>> {
        self.calls.lock().unwrap().push(method.to_string());
        self.inner
            .open_producer(method, params, metadata, has_header)
    }
    fn open_exchange<'a>(
        &'a mut self,
        method: &str,
        params: &RecordBatch,
        has_header: bool,
    ) -> Result<Box<dyn ExchangeStream + 'a>> {
        self.calls.lock().unwrap().push(method.to_string());
        self.inner.open_exchange(method, params, has_header)
    }
    fn label(&self) -> &str {
        self.inner.label()
    }
}

/// A client over the example worker whose unary calls are recorded.
struct Probe {
    client: VgiClient,
    calls: Arc<Mutex<Vec<String>>>,
}

impl Probe {
    fn connect() -> Option<Probe> {
        let worker = example_worker()?;
        let rpc = vgi_rpc_client::RpcClient::connect(&[worker.as_os_str()])
            .expect("spawn example worker")
            .protocol(vgi_protocol::VGI_PROTOCOL_NAME)
            .protocol_version(vgi_protocol::VGI_PROTOCOL_VERSION)
            .relax_nullability(true);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = Recording {
            inner: StreamTransport::new(rpc, "example"),
            calls: calls.clone(),
        };
        Some(Probe {
            client: VgiClient::new(Box::new(transport)),
            calls,
        })
    }

    /// The methods called since the last `take`, then forget them.
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }
}

macro_rules! probe_or_skip {
    () => {
        match Probe::connect() {
            Some(p) => p,
            None => {
                eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
                return;
            }
        }
    };
}

fn count(calls: &[String], method: &str) -> usize {
    calls.iter().filter(|c| *c == method).count()
}

fn per_schema(calls: &[String]) -> usize {
    calls
        .iter()
        .filter(|c| *c == "catalog_schemas" || c.starts_with("catalog_schema_contents_"))
        .count()
}

fn path(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|s| s.to_string()).collect()
}

/// Every kind of the static fixture catalog came through, in the right schema.
fn assert_static_catalog(snapshot: &CatalogSnapshot) {
    let paths: Vec<Vec<String>> = snapshot
        .schemas
        .iter()
        .map(|s| s.schema.path.clone())
        .collect();
    let mut sorted = paths.clone();
    sorted.sort();
    assert_eq!(sorted, vec![path(&["extra"]), path(&["main"])], "{paths:?}");
    let main = snapshot.schema(&path(&["main"])).expect("main");
    let names = |v: Vec<&String>| -> Vec<String> { v.into_iter().cloned().collect() };
    assert_eq!(
        names(main.tables.iter().map(|t| &t.name).collect()),
        ["ten"]
    );
    assert_eq!(
        names(main.views.iter().map(|v| &v.name).collect()),
        ["answer"]
    );
    assert_eq!(
        names(main.scalar_functions.iter().map(|f| &f.name).collect()),
        ["double"]
    );
    assert_eq!(
        names(main.aggregate_functions.iter().map(|f| &f.name).collect()),
        ["vgi_sum"]
    );
    assert_eq!(
        names(main.table_functions.iter().map(|f| &f.name).collect()),
        ["sequence"]
    );
    assert_eq!(
        names(main.scalar_macros.iter().map(|m| &m.name).collect()),
        ["contents_triple"]
    );
    assert_eq!(
        names(main.table_macros.iter().map(|m| &m.name).collect()),
        ["contents_range"]
    );
    assert!(main.indexes.is_empty());
    assert_eq!(main.schema.comment.as_deref(), Some("Every object kind"));
    let extra = snapshot.schema(&path(&["extra"])).expect("extra");
    assert_eq!(
        names(extra.tables.iter().map(|t| &t.name).collect()),
        ["five"]
    );
    assert!(extra.views.is_empty() && extra.scalar_functions.is_empty());
}

/// Advertised: the whole catalog in ONE `catalog_contents` call, no
/// per-schema calls, every kind decoded.
#[test]
fn advertised_catalog_loads_in_one_call() {
    let mut p = probe_or_skip!();
    let cat = p
        .client
        .attach("contents_probe", AttachOptions::default())
        .expect("attach");
    assert!(cat.supports_catalog_contents());
    assert!(cat.info().catalog_version_frozen);
    p.take();

    let snapshot = p.client.load_catalog(&cat, None).expect("load_catalog");
    let calls = p.take();
    assert_eq!(calls, ["catalog_contents"], "exactly one RPC");
    assert_eq!(snapshot.source, CatalogLoadSource::CatalogContents);
    assert_eq!(snapshot.fallback_reason, None);
    assert_eq!(snapshot.etag, None, "contents_probe does not revalidate");
    assert_static_catalog(&snapshot);

    // No etag: a reload is a plain full load (still one call, no
    // if_none_match short-circuit to rely on).
    let again = p
        .client
        .load_catalog(&cat, Some(&snapshot))
        .expect("reload");
    assert_eq!(p.take(), ["catalog_contents"]);
    assert!(!again.not_modified);
    assert_static_catalog(&again);
}

/// Advertised but failing: one failed `catalog_contents`, then the per-schema
/// calls — and the catalog is still complete.
#[test]
fn failing_catalog_contents_falls_back_to_per_schema() {
    let mut p = probe_or_skip!();
    let cat = p
        .client
        .attach("contents_broken", AttachOptions::default())
        .expect("attach");
    assert!(cat.supports_catalog_contents());
    p.take();

    let snapshot = p.client.load_catalog(&cat, None).expect("load_catalog");
    let calls = p.take();
    assert_eq!(calls.first().map(String::as_str), Some("catalog_contents"));
    assert_eq!(
        count(&calls, "catalog_contents"),
        1,
        "not retried: {calls:?}"
    );
    assert_eq!(count(&calls, "catalog_schemas"), 1, "{calls:?}");
    assert!(
        count(&calls, "catalog_schema_contents_tables") >= 2,
        "{calls:?}"
    );
    assert_eq!(snapshot.source, CatalogLoadSource::PerSchema);
    let reason = snapshot.fallback_reason.clone().expect("a fallback reason");
    assert!(
        reason.contains("catalog_contents deliberately fails"),
        "{reason}"
    );
    assert_static_catalog(&snapshot);
}

/// Not advertised (an older worker): `catalog_contents` is never sent — not by
/// `load_catalog`, and the raw call refuses without a round trip.
#[test]
fn unadvertised_catalog_contents_is_never_sent() {
    let mut p = probe_or_skip!();
    let cat = p
        .client
        .attach("contents_legacy", AttachOptions::default())
        .expect("attach");
    assert!(!cat.supports_catalog_contents());
    p.take();

    let snapshot = p.client.load_catalog(&cat, None).expect("load_catalog");
    let calls = p.take();
    assert_eq!(count(&calls, "catalog_contents"), 0, "{calls:?}");
    assert_eq!(count(&calls, "catalog_schemas"), 1);
    assert_eq!(snapshot.source, CatalogLoadSource::PerSchema);
    assert_eq!(snapshot.fallback_reason, None, "nothing failed");
    assert_static_catalog(&snapshot);

    let err = p.client.contents_response(&cat, None).unwrap_err();
    assert!(err.message.contains("supports_catalog_contents"), "{err:?}");
    assert!(p.take().is_empty(), "the raw call never reached the worker");
}

/// Revalidation: an unchanged catalog answers the held etag with
/// `not_modified` (the snapshot is kept, no schemas transferred); after a DDL
/// the same call returns the new catalog in full, with a new etag.
#[test]
fn revalidates_with_the_held_etag() {
    let mut p = probe_or_skip!();
    let cat = p
        .client
        .attach("contents_reval", AttachOptions::default())
        .expect("attach");
    let first = p.client.load_catalog(&cat, None).expect("load");
    assert_eq!(first.source, CatalogLoadSource::CatalogContents);
    let etag = first.etag.clone().expect("contents_reval revalidates");
    assert!(etag.starts_with("gen-"), "{etag}");
    assert_eq!(first.schemas.len(), 1);
    assert!(first.schemas[0].views.is_empty());
    p.take();

    let same = p
        .client
        .load_catalog(&cat, Some(&first))
        .expect("revalidate");
    assert_eq!(p.take(), ["catalog_contents"]);
    assert!(same.not_modified, "unchanged: not_modified");
    assert_eq!(same.etag.as_deref(), Some(etag.as_str()));
    assert_eq!(
        same.schemas.len(),
        first.schemas.len(),
        "previous content kept"
    );

    p.client
        .view_create(
            &cat,
            &path(&["main"]),
            "v",
            "SELECT 1 AS x",
            OnConflict::Error,
        )
        .expect("CREATE VIEW");
    p.take();

    let changed = p
        .client
        .load_catalog(&cat, Some(&same))
        .expect("revalidate");
    assert_eq!(p.take(), ["catalog_contents"], "still one call");
    assert!(!changed.not_modified, "a full answer replaces the snapshot");
    assert_ne!(changed.etag.as_deref(), Some(etag.as_str()));
    assert!(changed.catalog_version > first.catalog_version);
    let views: Vec<&String> = changed.schemas[0].views.iter().map(|v| &v.name).collect();
    assert_eq!(views, ["v"]);

    p.client.detach(&cat).expect("detach");
}

/// A snapshot that did not come from `catalog_contents` carries no etag, so it
/// is never sent as `if_none_match`; and a load inside a transaction always
/// takes the (transaction-aware) per-schema path.
#[test]
fn per_schema_snapshots_and_transactions_do_not_use_catalog_contents() {
    let mut p = probe_or_skip!();
    let cat = p
        .client
        .attach("contents_memory", AttachOptions::default())
        .expect("attach");
    assert!(cat.supports_catalog_contents());
    assert_eq!(cat.info().catalog_version, 0);
    let first = p.client.load_catalog(&cat, None).expect("load");
    assert_eq!(first.source, CatalogLoadSource::CatalogContents);
    assert_eq!(first.etag, None);
    assert_eq!(first.catalog_version, Some(0));
    p.take();

    // A DDL is visible to the next load (version 0: nothing is assumed current).
    p.client
        .view_create(
            &cat,
            &path(&["main"]),
            "v",
            "SELECT 3 AS x",
            OnConflict::Error,
        )
        .expect("CREATE VIEW");
    let next = p.client.load_catalog(&cat, Some(&first)).expect("reload");
    assert!(!next.not_modified);
    assert_eq!(next.schemas[0].views.len(), 1);

    // Inside a transaction (when the worker hands one out) the load is
    // per-schema. contents_memory has no transactions, so simulate the
    // transactional caller with a catalog that offers them.
    let mut example = p
        .client
        .attach("example", AttachOptions::default())
        .expect("attach example");
    p.client.begin_transaction(&mut example).expect("begin");
    assert!(example.transaction().is_some());
    p.take();
    let txn = p.client.load_catalog(&example, None).expect("load in txn");
    let calls = p.take();
    assert_eq!(count(&calls, "catalog_contents"), 0, "{calls:?}");
    assert!(per_schema(&calls) > 0);
    assert_eq!(txn.source, CatalogLoadSource::PerSchema);
    p.client.commit(&mut example).expect("commit");
}

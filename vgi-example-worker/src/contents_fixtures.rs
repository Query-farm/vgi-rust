// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The six `catalog_contents` fixture catalogs every SDK's test worker serves.
//!
//! Port of vgi-python's `vgi/_test_fixtures/catalog_contents.py` (the
//! cross-SDK contract); driven by
//! `vgi/test/sql/integration/catalog/catalog_contents*.test`.
//!
//! The same static two-schema catalog is served under three names, differing
//! only in how they answer `catalog_contents`:
//!
//! - `contents_probe` — advertises `supports_catalog_contents` and serves it:
//!   version-frozen, no etag, built once and reused. A client loads it in one
//!   RPC.
//! - `contents_broken` — advertises it, but its `catalog_contents` fails. A
//!   client must fall back to `catalog_schemas` + the per-schema RPCs.
//! - `contents_legacy` — does not advertise it, like an older worker. A client
//!   must never call `catalog_contents`.
//!
//! Three DDL-capable [`StoredCatalog`]s (version not frozen) advertise it too.
//! Every ATTACH gets its own empty catalog (one `main` schema), so tests sharing
//! a warm worker never see each other's objects:
//!
//! - `contents_memory` — reports catalog version 0 ("unknown") and no etag: the
//!   client's version-0 rule.
//! - `contents_reval` — revalidates with a cheap validator, etag `gen-<n>` (n =
//!   the catalog version, bumped by every DDL), answering a matching
//!   `if_none_match` with `not_modified` before building anything.
//! - `contents_hash` — no etag of its own, but the framework's opt-in
//!   content-hash etag (SHA-256 of the snapshot).
//!
//! The static catalog holds every kind a client seeds from `catalog_contents`
//! (tables, a view, scalar / aggregate / table functions, scalar and table
//! macros) over two schemas, `main` and `extra`. Its functions are this
//! worker's `double`, `vgi_sum` and `sequence`, each also registered straight
//! into the fixture catalog's `main` schema — so they are reachable only
//! through that attach, and the example catalog's own `main.double` is
//! untouched.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::{DataType, Field, Schema};
use vgi::arguments::Arguments;
use vgi::catalog::{CatMacro, CatSchema, CatTable, CatView, CatalogModel};
use vgi::catalog_contents::{
    CatalogContentsEtag, CatalogContentsProvider, CatalogContentsRequest, CatalogContentsResult,
};
use vgi::stored_catalog::StoredCatalog;
use vgi::RpcError;

pub const CATALOG_PROBE: &str = "contents_probe";
pub const CATALOG_BROKEN: &str = "contents_broken";
pub const CATALOG_LEGACY: &str = "contents_legacy";
pub const CATALOG_MEMORY: &str = "contents_memory";
pub const CATALOG_REVAL: &str = "contents_reval";
pub const CATALOG_HASH: &str = "contents_hash";

/// The error `contents_broken`'s `catalog_contents` raises (the tests match
/// on "catalog_contents deliberately fails").
pub const BROKEN_MESSAGE: &str = "contents_broken: catalog_contents deliberately fails";

/// A table scanning `sequence(count)`: column `n`, integers `0..count`.
fn sequence_table(name: &str, count: i64, comment: &str) -> CatTable {
    let args: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![count]))];
    CatTable::new(
        name,
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
        "sequence",
        Arguments::serialize_scan_args(&args).unwrap_or_default(),
        Some(comment.to_string()),
        None,
    )
}

/// The static two-schema catalog, under `name`.
fn static_catalog(name: &str) -> CatalogModel {
    CatalogModel {
        name: name.to_string(),
        comment: Some(format!("catalog_contents test catalog ({name})")),
        schemas: vec![
            CatSchema {
                name: "main".to_string(),
                comment: Some("Every object kind".to_string()),
                tables: vec![sequence_table("ten", 10, "Integers 0..9")],
                views: vec![CatView {
                    name: "answer".to_string(),
                    definition: "SELECT 42 AS answer".to_string(),
                    comment: Some("One row".to_string()),
                    ..Default::default()
                }],
                macros: vec![
                    CatMacro {
                        name: "contents_triple".to_string(),
                        parameters: vec!["x".to_string()],
                        definition: "x * 3".to_string(),
                        table_macro: false,
                        comment: Some("Triple a value".to_string()),
                        ..Default::default()
                    },
                    CatMacro {
                        name: "contents_range".to_string(),
                        parameters: vec!["n".to_string()],
                        definition: "SELECT * FROM range(n)".to_string(),
                        table_macro: true,
                        comment: Some("Table macro over range(n)".to_string()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            CatSchema {
                name: "extra".to_string(),
                comment: Some("A second schema, tables only".to_string()),
                tables: vec![sequence_table("five", 5, "Integers 0..4")],
                ..Default::default()
            },
        ],
        // A static catalog: its objects never change while the worker serves
        // it (vgi-python's ReadOnlyCatalogInterface).
        catalog_version_frozen: true,
        ..Default::default()
    }
}

/// `contents_broken`: advertises `catalog_contents`, then fails it.
struct Broken;

impl CatalogContentsProvider for Broken {
    fn catalog_contents(
        &self,
        _req: &CatalogContentsRequest<'_>,
    ) -> vgi::Result<CatalogContentsResult> {
        Err(RpcError::runtime_error(BROKEN_MESSAGE))
    }
}

/// `contents_reval`: etag `gen-<catalog version>`, checked before building.
struct GenerationEtag;

impl CatalogContentsProvider for GenerationEtag {
    fn catalog_contents(
        &self,
        req: &CatalogContentsRequest<'_>,
    ) -> vgi::Result<CatalogContentsResult> {
        let etag = format!("gen-{}", req.catalog_version());
        if req.if_none_match() == Some(etag.as_str()) {
            return Ok(CatalogContentsResult::not_modified(etag));
        }
        Ok(CatalogContentsResult::full(req.build()?, Some(etag)))
    }
}

/// Register all six catalogs on `w`.
pub fn register(w: &mut vgi::Worker) {
    for name in [CATALOG_PROBE, CATALOG_BROKEN, CATALOG_LEGACY] {
        w.register_scalar_in(name, "main", crate::scalar::DoubleFunction);
        w.register_aggregate_in(name, "main", crate::aggregate::SumFunction);
        w.register_table_in(name, "main", crate::table::SequenceFunction);
    }

    let mut probe = static_catalog(CATALOG_PROBE);
    // Frozen and attach-independent: built once, served to every call.
    probe.supports_catalog_contents = Some(true);
    probe.catalog_contents_attach_independent = true;
    w.register_secondary_catalog(probe, Vec::new());

    let mut broken = static_catalog(CATALOG_BROKEN);
    broken.supports_catalog_contents = Some(true);
    broken.contents_provider = Some(Arc::new(Broken));
    w.register_secondary_catalog(broken, Vec::new());

    let mut legacy = static_catalog(CATALOG_LEGACY);
    legacy.supports_catalog_contents = Some(false);
    w.register_secondary_catalog(legacy, Vec::new());

    w.register_stored_catalog(
        StoredCatalog::new(CATALOG_MEMORY)
            .comment("catalog_contents version-0 fixture")
            .unversioned(),
    );
    w.register_stored_catalog(
        StoredCatalog::new(CATALOG_REVAL)
            .comment("catalog_contents revalidation fixture (gen-<version> etag)")
            .contents_provider(Arc::new(GenerationEtag)),
    );
    w.register_stored_catalog(
        StoredCatalog::new(CATALOG_HASH)
            .comment("catalog_contents revalidation fixture (content-hash etag)")
            .catalog_contents_etag(CatalogContentsEtag::ContentHash),
    );
}

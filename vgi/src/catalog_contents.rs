// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `catalog_contents` (protocol 2.1.0): the whole catalog in one call, with
//! etag revalidation.
//!
//! The dispatcher builds the snapshot from the very producers the per-schema
//! RPCs serve (so every item is byte-identical to theirs). A catalog shapes the
//! answer in two optional ways, both set on its
//! [`CatalogModel`](crate::catalog::CatalogModel):
//!
//! - a [`CatalogContentsProvider`] — the catalog author's hook. It receives the
//!   client's `if_none_match` and returns the snapshot plus an etag, or "not
//!   modified", so a cheap validator (a generation counter, a schema version, a
//!   git sha) can short-circuit *before* anything is built. The default (no
//!   provider) builds the snapshot and returns no etag.
//! - [`CatalogContentsEtag::ContentHash`] — the opt-in framework etag: when the
//!   catalog itself returned none, the etag is the hex SHA-256 of the snapshot
//!   ([`catalog_contents_digest`]) and a matching `if_none_match` becomes
//!   `not_modified`. It still builds every time (it saves the transfer and the
//!   client's decode), which is why it is off by default.
//!
//! Whatever the catalog returns, [`finish`] enforces the wire rules before it
//! leaves the worker: `not_modified` only with an etag equal to
//! `if_none_match` and no schemas; a full answer whose etag equals
//! `if_none_match` becomes `not_modified`; with no etag, `if_none_match` is
//! ignored; schema paths are unique, each equals its `SchemaInfo.path`, every
//! parent is present, and parents come before children.

use std::collections::HashSet;

use sha2::{Digest, Sha256};
use vgi_rpc::{Result, RpcError};

use crate::protocol::dtos::{CatalogContentsResponse, SchemaContents, SchemaInfo};

/// What a catalog answers `catalog_contents` with: a snapshot, or "not
/// modified". The Rust analogue of vgi-python's `CatalogContentsResult`.
#[derive(Debug, Clone, Default)]
pub struct CatalogContentsResult {
    /// One entry per schema (any order — the worker emits parents first).
    /// Must be empty when `not_modified` is set.
    pub schemas: Vec<SchemaContents>,
    /// Opaque validator for this snapshot, sent back by the client as
    /// `if_none_match`. `None`: the catalog does not revalidate (unless it opts
    /// in to [`CatalogContentsEtag::ContentHash`]).
    pub etag: Option<String>,
    /// The request's `if_none_match` equals the current etag, so the catalog
    /// skipped building the snapshot. Requires `etag` (that validator).
    pub not_modified: bool,
}

impl CatalogContentsResult {
    /// A full snapshot with an optional etag.
    pub fn full(schemas: Vec<SchemaContents>, etag: Option<String>) -> Self {
        CatalogContentsResult {
            schemas,
            etag,
            not_modified: false,
        }
    }

    /// "The snapshot you hold, `etag`, is current" — no schemas.
    pub fn not_modified(etag: impl Into<String>) -> Self {
        CatalogContentsResult {
            schemas: Vec::new(),
            etag: Some(etag.into()),
            not_modified: true,
        }
    }
}

/// The framework etag a catalog opts in to (vgi-python:
/// `catalog_contents_etag`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CatalogContentsEtag {
    /// No etag unless the catalog's provider returns one (the default).
    #[default]
    None,
    /// When the catalog returns no etag, use the hex SHA-256 of the snapshot
    /// ([`catalog_contents_digest`]) and answer a matching `if_none_match` with
    /// `not_modified`. Builds the snapshot on every call — so for a catalog
    /// whose version is not frozen the client trades a cheap `catalog_version`
    /// poll per transaction for a full build; opt in deliberately.
    ContentHash,
}

/// One `catalog_contents` call, as a [`CatalogContentsProvider`] sees it.
pub struct CatalogContentsRequest<'a> {
    pub(crate) attach_opaque_data: &'a [u8],
    pub(crate) if_none_match: Option<&'a str>,
    pub(crate) catalog_version: i64,
    pub(crate) generation: u64,
    pub(crate) build: &'a dyn Fn() -> Result<Vec<SchemaContents>>,
}

impl CatalogContentsRequest<'_> {
    /// The attach handle the call arrived with.
    pub fn attach_opaque_data(&self) -> &[u8] {
        self.attach_opaque_data
    }

    /// The etag of the snapshot the client already holds, if any.
    pub fn if_none_match(&self) -> Option<&str> {
        self.if_none_match
    }

    /// The catalog version the snapshot is taken at.
    pub fn catalog_version(&self) -> i64 {
        self.catalog_version
    }

    /// The dispatcher's catalog generation: bumped by every change to what the
    /// worker serves (a function registration, `set_catalog`, a secondary
    /// catalog). A ready-made cheap validator for a catalog with no DDL —
    /// equal generations in one process mean equal contents.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Build the snapshot the framework would serve: every schema of the
    /// catalog, each kind composed from the per-schema producers. This is the
    /// expensive part — call it only after deciding the answer is not
    /// `not_modified`.
    pub fn build(&self) -> Result<Vec<SchemaContents>> {
        (self.build)()
    }

    /// The default answer: [`Self::build`] with no etag (what a catalog without
    /// a provider serves).
    pub fn default_contents(&self) -> Result<CatalogContentsResult> {
        Ok(CatalogContentsResult::full(self.build()?, None))
    }
}

/// A catalog author's `catalog_contents` hook (vgi-python: overriding
/// `CatalogInterface.catalog_contents`).
///
/// Return [`CatalogContentsResult::not_modified`] when `if_none_match` equals
/// your current etag — before calling [`CatalogContentsRequest::build`] — and
/// otherwise the built snapshot with that etag:
///
/// ```
/// use vgi::catalog_contents::{
///     CatalogContentsProvider, CatalogContentsRequest, CatalogContentsResult,
/// };
///
/// struct Generation;
/// impl CatalogContentsProvider for Generation {
///     fn catalog_contents(
///         &self,
///         req: &CatalogContentsRequest<'_>,
///     ) -> vgi::Result<CatalogContentsResult> {
///         let etag = format!("gen-{}", req.generation());
///         if req.if_none_match() == Some(etag.as_str()) {
///             return Ok(CatalogContentsResult::not_modified(etag));
///         }
///         Ok(CatalogContentsResult::full(req.build()?, Some(etag)))
///     }
/// }
/// ```
pub trait CatalogContentsProvider: Send + Sync {
    /// Answer one `catalog_contents` call.
    fn catalog_contents(&self, req: &CatalogContentsRequest<'_>) -> Result<CatalogContentsResult>;
}

/// Hex SHA-256 over a `catalog_contents` snapshot: the
/// [`CatalogContentsEtag::ContentHash`] etag.
///
/// Byte-for-byte the encoding of vgi-python's `catalog_contents_digest()`, so
/// one snapshot hashes alike in every SDK: the schema count, then per schema
/// its path parts, its `SchemaInfo` item and each of the eight kinds in wire
/// order — every list prefixed by its length and every byte string by its
/// length, both as little-endian `u64`, so no two different snapshots share an
/// input. Deterministic because the items' encoding is (map columns are
/// written in key order).
pub fn catalog_contents_digest(schemas: &[SchemaContents]) -> String {
    let mut digest = Sha256::new();
    fn chunk(digest: &mut Sha256, data: &[u8]) {
        digest.update((data.len() as u64).to_le_bytes());
        digest.update(data);
    }
    digest.update((schemas.len() as u64).to_le_bytes());
    for entry in schemas {
        digest.update((entry.path.len() as u64).to_le_bytes());
        for part in &entry.path {
            chunk(&mut digest, part.as_bytes());
        }
        chunk(&mut digest, &entry.schema.0);
        for kind in [
            &entry.tables,
            &entry.views,
            &entry.scalar_functions,
            &entry.aggregate_functions,
            &entry.table_functions,
            &entry.scalar_macros,
            &entry.table_macros,
            &entry.indexes,
        ] {
            digest.update((kind.len() as u64).to_le_bytes());
            for item in kind {
                chunk(&mut digest, &item.0);
            }
        }
    }
    digest
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Shape a catalog's answer into the wire response, enforcing the
/// revalidation rules and validating the snapshot (see the module docs).
pub fn finish(
    result: CatalogContentsResult,
    if_none_match: Option<&str>,
    catalog_version: i64,
    etag_mode: CatalogContentsEtag,
) -> Result<CatalogContentsResponse> {
    let not_modified = |etag: String| CatalogContentsResponse {
        catalog_version,
        etag: Some(etag),
        not_modified: true,
        schemas: Vec::new(),
    };
    if result.not_modified {
        return match (result.etag, if_none_match) {
            (Some(etag), Some(inm)) if etag == inm => {
                if result.schemas.is_empty() {
                    Ok(not_modified(etag))
                } else {
                    Err(RpcError::value_error(
                        "catalog_contents returned not_modified with schemas; it must return none",
                    ))
                }
            }
            _ => Err(RpcError::value_error(
                "catalog_contents returned not_modified, but only a catalog whose etag equals \
                 if_none_match may (and it must return that etag)",
            )),
        };
    }
    let mut schemas = result.schemas;
    validate_paths(&schemas)?;
    // Parents before children (stable: siblings keep the catalog's order).
    schemas.sort_by_key(|s| s.path.len());
    let etag = match (result.etag, etag_mode) {
        (None, CatalogContentsEtag::ContentHash) => Some(catalog_contents_digest(&schemas)),
        (etag, _) => etag,
    };
    if let (Some(etag), Some(inm)) = (&etag, if_none_match) {
        if etag == inm {
            return Ok(not_modified(etag.clone()));
        }
    }
    Ok(CatalogContentsResponse {
        catalog_version,
        etag,
        not_modified: false,
        schemas,
    })
}

/// Paths are unique, each equals the `SchemaInfo.path` inside its `schema`
/// item, and every multi-part path's parent is present.
fn validate_paths(schemas: &[SchemaContents]) -> Result<()> {
    let mut seen: HashSet<&[String]> = HashSet::with_capacity(schemas.len());
    for entry in schemas {
        if entry.path.is_empty() {
            return Err(RpcError::value_error(
                "catalog_contents returned a schema with an empty path",
            ));
        }
        if !seen.insert(entry.path.as_slice()) {
            return Err(RpcError::value_error(format!(
                "catalog_contents returned duplicate schema path {:?}",
                entry.path
            )));
        }
        let batch = vgi_protocol::ipc::read_batch(&entry.schema.0)?;
        let info: SchemaInfo = vgi_protocol::wire::from_batch(&batch)?;
        if info.path != entry.path {
            return Err(RpcError::value_error(format!(
                "catalog_contents: path {:?} differs from its SchemaInfo.path {:?}",
                entry.path, info.path
            )));
        }
    }
    for entry in schemas {
        let parent = &entry.path[..entry.path.len() - 1];
        if !parent.is_empty() && !seen.contains(parent) {
            return Err(RpcError::value_error(format!(
                "catalog_contents returned schema path {:?} without its parent",
                entry.path
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use vgi_rpc::Bytes;

    fn b(s: &[u8]) -> Bytes {
        Bytes::from(s.to_vec())
    }

    fn bare(path: &[&str], schema: &[u8]) -> SchemaContents {
        SchemaContents {
            path: path.iter().map(|s| s.to_string()).collect(),
            schema: b(schema),
            tables: Vec::new(),
            views: Vec::new(),
            scalar_functions: Vec::new(),
            aggregate_functions: Vec::new(),
            table_functions: Vec::new(),
            scalar_macros: Vec::new(),
            table_macros: Vec::new(),
            indexes: Vec::new(),
        }
    }

    /// A schema entry whose `schema` item is a real `SchemaInfo` for `path`.
    fn entry(path: &[&str]) -> SchemaContents {
        let parts: Vec<String> = path.iter().map(|s| s.to_string()).collect();
        let info = crate::catalog::schema_info(&parts, None, b"cat");
        let item = crate::catalog::serialize_items(vec![info])
            .unwrap()
            .pop()
            .unwrap();
        SchemaContents {
            schema: item,
            ..bare(path, b"")
        }
    }

    /// The content-hash etag is vgi-python's `catalog_contents_digest()` bit
    /// for bit, so one snapshot gets one etag in every SDK. Expected values
    /// computed with vgi-python cc83818:
    /// `catalog_contents_digest([])` and the two-schema snapshot below.
    #[test]
    fn digest_matches_vgi_python() {
        assert_eq!(
            catalog_contents_digest(&[]),
            "af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc"
        );
        let main = SchemaContents {
            tables: vec![b(b"t1"), b(b"")],
            scalar_functions: vec![b(b"f")],
            indexes: vec![b(b"ix")],
            ..bare(&["main"], b"\x01\x02")
        };
        let sub = SchemaContents {
            table_macros: vec![b(b"m1"), b(b"m2")],
            ..bare(&["main", "sub"], b"s")
        };
        assert_eq!(
            catalog_contents_digest(&[main, sub]),
            "3fb43c461c96eddbe3239ae340c151a8ce932c6a7abdc35175ed9f7d7e520fc4"
        );
    }

    /// Length prefixes keep moved bytes from colliding: an item moved from one
    /// kind to the next, or split in two, hashes differently.
    #[test]
    fn digest_distinguishes_layouts() {
        let a = SchemaContents {
            tables: vec![b(b"ab")],
            ..bare(&["main"], b"s")
        };
        let moved = SchemaContents {
            views: vec![b(b"ab")],
            ..bare(&["main"], b"s")
        };
        let split = SchemaContents {
            tables: vec![b(b"a"), b(b"b")],
            ..bare(&["main"], b"s")
        };
        let d = catalog_contents_digest(&[a]);
        assert_ne!(d, catalog_contents_digest(&[moved]));
        assert_ne!(d, catalog_contents_digest(&[split]));
    }

    const NONE: CatalogContentsEtag = CatalogContentsEtag::None;
    const HASH: CatalogContentsEtag = CatalogContentsEtag::ContentHash;

    #[test]
    fn full_answer_orders_parents_first_and_keeps_its_etag() {
        let result = CatalogContentsResult::full(
            vec![entry(&["a", "b"]), entry(&["main"]), entry(&["a"])],
            Some("e1".into()),
        );
        let resp = finish(result, Some("other"), 7, NONE).unwrap();
        assert!(!resp.not_modified);
        assert_eq!(resp.etag.as_deref(), Some("e1"));
        assert_eq!(resp.catalog_version, 7);
        let paths: Vec<_> = resp.schemas.iter().map(|s| s.path.join(".")).collect();
        assert_eq!(paths, ["main", "a", "a.b"]);
    }

    #[test]
    fn no_etag_ignores_if_none_match() {
        let resp = finish(
            CatalogContentsResult::full(vec![entry(&["main"])], None),
            Some("anything"),
            1,
            NONE,
        )
        .unwrap();
        assert!(!resp.not_modified);
        assert_eq!(resp.etag, None);
        assert_eq!(resp.schemas.len(), 1);
    }

    #[test]
    fn full_answer_with_the_asked_etag_becomes_not_modified() {
        let resp = finish(
            CatalogContentsResult::full(vec![entry(&["main"])], Some("e1".into())),
            Some("e1"),
            3,
            NONE,
        )
        .unwrap();
        assert!(resp.not_modified);
        assert_eq!(resp.etag.as_deref(), Some("e1"));
        assert_eq!(resp.catalog_version, 3);
        assert!(resp.schemas.is_empty());
    }

    #[test]
    fn not_modified_needs_the_matching_etag_and_no_schemas() {
        let ok = finish(
            CatalogContentsResult::not_modified("e1"),
            Some("e1"),
            1,
            NONE,
        )
        .unwrap();
        assert!(ok.not_modified && ok.schemas.is_empty());
        assert_eq!(ok.etag.as_deref(), Some("e1"));
        // A different etag, or no if_none_match at all.
        assert!(finish(
            CatalogContentsResult::not_modified("e1"),
            Some("e2"),
            1,
            NONE
        )
        .is_err());
        assert!(finish(CatalogContentsResult::not_modified("e1"), None, 1, NONE).is_err());
        // not_modified with no etag.
        let no_etag = CatalogContentsResult {
            not_modified: true,
            ..Default::default()
        };
        assert!(finish(no_etag, Some("e1"), 1, NONE).is_err());
        // not_modified carrying schemas.
        let with_schemas = CatalogContentsResult {
            schemas: vec![entry(&["main"])],
            ..CatalogContentsResult::not_modified("e1")
        };
        assert!(finish(with_schemas, Some("e1"), 1, NONE).is_err());
    }

    #[test]
    fn content_hash_etag_and_its_revalidation() {
        let snapshot = || vec![entry(&["main"]), entry(&["main", "sub"])];
        let full = finish(CatalogContentsResult::full(snapshot(), None), None, 1, HASH).unwrap();
        let etag = full.etag.clone().expect("content-hash sets an etag");
        assert_eq!(etag, catalog_contents_digest(&full.schemas));
        assert_eq!(etag.len(), 64);
        // Same snapshot, same etag → not_modified.
        let again = finish(
            CatalogContentsResult::full(snapshot(), None),
            Some(&etag),
            1,
            HASH,
        )
        .unwrap();
        assert!(again.not_modified && again.schemas.is_empty());
        assert_eq!(again.etag.as_deref(), Some(etag.as_str()));
        // A different snapshot hashes differently.
        let other = finish(
            CatalogContentsResult::full(vec![entry(&["main"])], None),
            Some(&etag),
            1,
            HASH,
        )
        .unwrap();
        assert!(!other.not_modified);
        assert_ne!(other.etag.as_deref(), Some(etag.as_str()));
        // The catalog's own etag wins over the content hash.
        let own = finish(
            CatalogContentsResult::full(snapshot(), Some("mine".into())),
            None,
            1,
            HASH,
        )
        .unwrap();
        assert_eq!(own.etag.as_deref(), Some("mine"));
    }

    #[test]
    fn paths_are_validated() {
        let run = |schemas| finish(CatalogContentsResult::full(schemas, None), None, 1, NONE);
        let err = |r: Result<CatalogContentsResponse>| r.unwrap_err().to_string();
        assert!(err(run(vec![entry(&["main"]), entry(&["main"])])).contains("duplicate"));
        assert!(err(run(vec![entry(&["a", "b"])])).contains("without its parent"));
        let mismatched = SchemaContents {
            path: vec!["other".to_string()],
            ..entry(&["main"])
        };
        assert!(err(run(vec![mismatched])).contains("differs from its SchemaInfo.path"));
        assert!(err(run(vec![bare(&[], b"")])).contains("empty path"));
    }
}

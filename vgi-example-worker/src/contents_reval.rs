// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! How the example catalog answers `catalog_contents` revalidation
//! (`if_none_match`), chosen by `VGI_CATALOG_CONTENTS_ETAG`:
//!
//! - unset / `generation` (default): a cheap validator, like vgi-python's
//!   `contents_reval` fixture. The etag names the catalog generation — the
//!   dispatcher bumps it on every change to what the worker serves (this SDK's
//!   catalogs have no DDL, so registrations are the only way they change) — and
//!   a matching `if_none_match` is answered `not_modified` before anything is
//!   built. This is what makes the cross-SDK conformance test exercise the
//!   conditional path against this worker.
//! - `content-hash`: no etag of its own; the framework's opt-in SHA-256 of the
//!   snapshot (built on every call).
//! - `none`: no etag at all — the client never revalidates.
//!
//! `VGI_CATALOG_CONTENTS_CACHE=1` additionally declares the catalog's contents
//! attach-independent, so the framework builds the response once and reuses
//! its encoded batch.

use std::sync::Arc;

use vgi::catalog::CatalogModel;
use vgi::catalog_contents::{
    CatalogContentsEtag, CatalogContentsProvider, CatalogContentsRequest, CatalogContentsResult,
};

/// Etag = worker build + catalog version + catalog generation.
pub struct GenerationEtag;

impl GenerationEtag {
    fn etag(req: &CatalogContentsRequest<'_>) -> String {
        format!(
            "vgi-rust-example/{}/v{}/g{}",
            env!("CARGO_PKG_VERSION"),
            req.catalog_version(),
            req.generation()
        )
    }
}

impl CatalogContentsProvider for GenerationEtag {
    fn catalog_contents(
        &self,
        req: &CatalogContentsRequest<'_>,
    ) -> vgi::Result<CatalogContentsResult> {
        let etag = Self::etag(req);
        // Short-circuit before building: the client's snapshot is current.
        if req.if_none_match() == Some(etag.as_str()) {
            return Ok(CatalogContentsResult::not_modified(etag));
        }
        Ok(CatalogContentsResult::full(req.build()?, Some(etag)))
    }
}

/// Apply `VGI_CATALOG_CONTENTS_ETAG` / `VGI_CATALOG_CONTENTS_CACHE` to `model`.
pub fn configure(model: &mut CatalogModel) {
    let env = |name: &str| {
        std::env::var(name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
    };
    match env("VGI_CATALOG_CONTENTS_ETAG").as_str() {
        "none" | "off" => {}
        "content-hash" => model.catalog_contents_etag = CatalogContentsEtag::ContentHash,
        _ => model.contents_provider = Some(Arc::new(GenerationEtag)),
    }
    model.catalog_contents_attach_independent = matches!(
        env("VGI_CATALOG_CONTENTS_CACHE").as_str(),
        "1" | "true" | "on"
    );
}

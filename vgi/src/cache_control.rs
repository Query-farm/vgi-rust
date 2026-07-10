// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Result-cache control metadata (`vgi.cache.*`).
//!
//! A table function advertises that its result is cacheable by the client (the
//! DuckDB extension) by attaching `vgi.cache.*` metadata to the **first** data
//! batch it emits. The vocabulary mirrors HTTP caching (RFC 9111/9110): a
//! freshness lifetime (`ttl`/`expires`), a reuse [`scope`](CacheControl::scope),
//! validators (ETag / Last-Modified) for conditional revalidation, and
//! stale-serving grace windows.
//!
//! The key strings are the single source of truth shared with the C++
//! extension, which reads them by string. [`CacheControl::to_metadata`] renders
//! a set of these fields to the `HashMap<String, String>` of `vgi.cache.*` keys
//! that rides on batch `custom_metadata` — the map a
//! [`TableProducer::last_metadata`](crate::table_function::TableProducer::last_metadata)
//! returns.
//!
//! Booleans render as `"1"` (present) and are omitted when false; timestamps are
//! RFC 3339 UTC strings; durations are integer seconds.
//!
//! # Examples
//!
//! ```
//! use vgi::cache_control::CacheControl;
//!
//! // Advertise a 5-minute freshness lifetime on the first emitted batch.
//! let md = CacheControl::ttl(300).to_metadata();
//! assert_eq!(md.get("vgi.cache.ttl").map(String::as_str), Some("300"));
//! assert_eq!(md.get("vgi.cache.scope").map(String::as_str), Some("catalog"));
//! ```

use std::collections::HashMap;

// --- Response-side metadata keys (worker -> client) ------------------------
// Defined once here; the C++ extension reads these exact strings.

/// Freshness lifetime in whole seconds, relative to full-result receipt.
pub const CACHE_TTL_KEY: &str = "vgi.cache.ttl";
/// Absolute RFC 3339 UTC freshness deadline.
pub const CACHE_EXPIRES_KEY: &str = "vgi.cache.expires";
/// Explicit "never cache"; overrides any freshness key.
pub const CACHE_NO_STORE_KEY: &str = "vgi.cache.no_store";
/// Reuse scope: [`CACHE_SCOPE_CATALOG`] or [`CACHE_SCOPE_TRANSACTION`].
pub const CACHE_SCOPE_KEY: &str = "vgi.cache.scope";
/// Strong validator (opaque quoted string) for conditional revalidation.
pub const CACHE_ETAG_KEY: &str = "vgi.cache.etag";
/// Weaker RFC 3339 UTC validator; fallback when no ETag.
pub const CACHE_LAST_MODIFIED_KEY: &str = "vgi.cache.last_modified";
/// The worker can check freshness cheaply without recomputing.
pub const CACHE_REVALIDATABLE_KEY: &str = "vgi.cache.revalidatable";
/// Grace window (seconds) to serve stale while revalidating in the background.
pub const CACHE_STALE_WHILE_REVALIDATE_KEY: &str = "vgi.cache.stale_while_revalidate";
/// Grace window (seconds) to serve stale if a revalidation RPC fails.
pub const CACHE_STALE_IF_ERROR_KEY: &str = "vgi.cache.stale_if_error";
/// 304-equivalent, set on a 0-row batch in reply to a conditional request.
pub const CACHE_NOT_MODIFIED_KEY: &str = "vgi.cache.not_modified";

// --- Request-side metadata keys (client -> worker) -------------------------

/// The client's stored ETag, sent on a conditional revalidation request.
/// Surfaced to a producer via
/// [`TableProducer::on_conditional_request`](crate::table_function::TableProducer::on_conditional_request).
pub const CACHE_IF_NONE_MATCH_KEY: &str = "vgi.cache.if_none_match";
/// The client's stored Last-Modified, sent on a conditional revalidation
/// request. Companion to [`CACHE_IF_NONE_MATCH_KEY`].
pub const CACHE_IF_MODIFIED_SINCE_KEY: &str = "vgi.cache.if_modified_since";

// --- Reuse-scope values ----------------------------------------------------

/// Reusable across transactions within the calling catalog identity (default).
pub const CACHE_SCOPE_CATALOG: &str = "catalog";
/// Reused only within the transaction that produced it.
pub const CACHE_SCOPE_TRANSACTION: &str = "transaction";

/// Cacheability advertised by a table function on its first result batch.
///
/// Presence of [`ttl`](Self::ttl_seconds) **or** [`expires`](Self::expires) is
/// what makes a result cacheable; [`no_store`](Self::no_store) overrides any
/// freshness key. [`scope`](Self::scope) defaults to [`CACHE_SCOPE_CATALOG`].
///
/// Build one with [`CacheControl::ttl`] / [`CacheControl::no_store`] /
/// [`CacheControl::default`] plus the `with_*` setters, then render it with
/// [`to_metadata`](Self::to_metadata).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheControl {
    /// Freshness lifetime in whole seconds, relative to full-result receipt
    /// (skew-immune; wins over `expires`). Negative values are clamped to 0.
    pub ttl_seconds: Option<i64>,
    /// Absolute RFC 3339 UTC deadline. Lifetime is `expires - now` at receipt.
    pub expires: Option<String>,
    /// Reuse scope — [`CACHE_SCOPE_CATALOG`] or [`CACHE_SCOPE_TRANSACTION`].
    pub scope: String,
    /// Explicit "never cache"; overrides any freshness key.
    pub no_store: bool,
    /// Strong validator (opaque quoted string) for conditional revalidation.
    pub etag: Option<String>,
    /// Weaker RFC 3339 UTC validator; fallback when no ETag.
    pub last_modified: Option<String>,
    /// The worker can check freshness cheaply without recomputing; gates
    /// whether the client ever sends a conditional request.
    pub revalidatable: bool,
    /// Grace window (seconds) to serve stale immediately while revalidating.
    pub stale_while_revalidate: Option<i64>,
    /// Grace window (seconds) to serve stale if a revalidation RPC fails.
    pub stale_if_error: Option<i64>,
    /// 304-equivalent — set on a 0-row batch in reply to a conditional request
    /// to assert the client's stored payload is still fresh (the client reuses
    /// it instead of re-streaming).
    pub not_modified: bool,
}

impl Default for CacheControl {
    fn default() -> Self {
        CacheControl {
            ttl_seconds: None,
            expires: None,
            scope: CACHE_SCOPE_CATALOG.to_string(),
            no_store: false,
            etag: None,
            last_modified: None,
            revalidatable: false,
            stale_while_revalidate: None,
            stale_if_error: None,
            not_modified: false,
        }
    }
}

impl CacheControl {
    /// Cacheable for `seconds` after the client receives the full result.
    /// Negative values clamp to 0 (immediately stale).
    pub fn ttl(seconds: i64) -> Self {
        CacheControl {
            ttl_seconds: Some(seconds.max(0)),
            ..Default::default()
        }
    }

    /// Never cache this result, whatever else is advertised.
    pub fn no_store() -> Self {
        CacheControl {
            no_store: true,
            ..Default::default()
        }
    }

    /// Restrict reuse to the producing transaction.
    pub fn with_transaction_scope(mut self) -> Self {
        self.scope = CACHE_SCOPE_TRANSACTION.to_string();
        self
    }

    /// Attach a strong validator for conditional revalidation.
    pub fn with_etag(mut self, etag: impl Into<String>) -> Self {
        self.etag = Some(etag.into());
        self
    }

    /// Attach an RFC 3339 UTC `Last-Modified` validator.
    pub fn with_last_modified(mut self, last_modified: impl Into<String>) -> Self {
        self.last_modified = Some(last_modified.into());
        self
    }

    /// Declare that freshness can be rechecked without recomputing the result,
    /// so the client may send conditional requests instead of full scans.
    pub fn with_revalidatable(mut self) -> Self {
        self.revalidatable = true;
        self
    }

    /// Absolute RFC 3339 UTC freshness deadline.
    pub fn with_expires(mut self, expires: impl Into<String>) -> Self {
        self.expires = Some(expires.into());
        self
    }

    /// Serve stale for up to `seconds` while revalidating in the background.
    pub fn with_stale_while_revalidate(mut self, seconds: i64) -> Self {
        self.stale_while_revalidate = Some(seconds.max(0));
        self
    }

    /// Serve stale for up to `seconds` when a revalidation RPC fails.
    pub fn with_stale_if_error(mut self, seconds: i64) -> Self {
        self.stale_if_error = Some(seconds.max(0));
        self
    }

    /// Mark this a 304 reply: the client's stored payload is still fresh.
    /// Emit it on a **0-row** batch in answer to a conditional request.
    pub fn with_not_modified(mut self) -> Self {
        self.not_modified = true;
        self
    }

    /// Render to the `vgi.cache.*` batch-metadata map.
    ///
    /// Booleans render as `"1"` and are omitted when false; unset optional
    /// fields are omitted entirely. `scope` is always emitted so the client
    /// never has to infer the default.
    pub fn to_metadata(&self) -> HashMap<String, String> {
        let mut md = HashMap::new();
        if let Some(ttl) = self.ttl_seconds {
            md.insert(CACHE_TTL_KEY.to_string(), ttl.to_string());
        }
        if let Some(expires) = &self.expires {
            md.insert(CACHE_EXPIRES_KEY.to_string(), expires.clone());
        }
        if self.no_store {
            md.insert(CACHE_NO_STORE_KEY.to_string(), "1".to_string());
        }
        md.insert(CACHE_SCOPE_KEY.to_string(), self.scope.clone());
        if let Some(etag) = &self.etag {
            md.insert(CACHE_ETAG_KEY.to_string(), etag.clone());
        }
        if let Some(lm) = &self.last_modified {
            md.insert(CACHE_LAST_MODIFIED_KEY.to_string(), lm.clone());
        }
        if self.revalidatable {
            md.insert(CACHE_REVALIDATABLE_KEY.to_string(), "1".to_string());
        }
        if let Some(s) = self.stale_while_revalidate {
            md.insert(CACHE_STALE_WHILE_REVALIDATE_KEY.to_string(), s.to_string());
        }
        if let Some(s) = self.stale_if_error {
            md.insert(CACHE_STALE_IF_ERROR_KEY.to_string(), s.to_string());
        }
        if self.not_modified {
            md.insert(CACHE_NOT_MODIFIED_KEY.to_string(), "1".to_string());
        }
        md
    }
}

/// The validators a client sends when revalidating a stale-but-revalidatable
/// cached result. Handed to
/// [`TableProducer::on_conditional_request`](crate::table_function::TableProducer::on_conditional_request)
/// before the producer's first batch; both fields are `None` on a normal call.
///
/// A producer that advertised [`CacheControl::with_revalidatable`] compares
/// [`if_none_match`](Self::if_none_match) against its current ETag and, when
/// unchanged, emits a 0-row batch carrying
/// [`CacheControl::with_not_modified`] instead of re-streaming the payload.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConditionalRequest {
    /// The client's stored ETag ([`CACHE_IF_NONE_MATCH_KEY`]).
    pub if_none_match: Option<String>,
    /// The client's stored Last-Modified ([`CACHE_IF_MODIFIED_SINCE_KEY`]).
    pub if_modified_since: Option<String>,
}

impl ConditionalRequest {
    /// Whether the client sent any validator at all.
    pub fn is_conditional(&self) -> bool {
        self.if_none_match.is_some() || self.if_modified_since.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_renders_ttl_and_default_scope() {
        let md = CacheControl::ttl(300).to_metadata();
        assert_eq!(md[CACHE_TTL_KEY], "300");
        assert_eq!(md[CACHE_SCOPE_KEY], CACHE_SCOPE_CATALOG);
        assert!(!md.contains_key(CACHE_NO_STORE_KEY));
        assert!(!md.contains_key(CACHE_REVALIDATABLE_KEY));
    }

    #[test]
    fn negative_ttl_clamps_to_zero() {
        assert_eq!(CacheControl::ttl(-5).ttl_seconds, Some(0));
    }

    #[test]
    fn no_store_omits_freshness_and_sets_flag() {
        let md = CacheControl::no_store().to_metadata();
        assert_eq!(md[CACHE_NO_STORE_KEY], "1");
        assert!(!md.contains_key(CACHE_TTL_KEY));
    }

    #[test]
    fn transaction_scope_renders() {
        let md = CacheControl::ttl(300)
            .with_transaction_scope()
            .to_metadata();
        assert_eq!(md[CACHE_SCOPE_KEY], CACHE_SCOPE_TRANSACTION);
    }

    #[test]
    fn revalidatable_contract_renders_etag_and_flags() {
        let md = CacheControl::ttl(0)
            .with_etag("\"rev-v1\"")
            .with_revalidatable()
            .to_metadata();
        assert_eq!(md[CACHE_TTL_KEY], "0");
        assert_eq!(md[CACHE_ETAG_KEY], "\"rev-v1\"");
        assert_eq!(md[CACHE_REVALIDATABLE_KEY], "1");
        assert!(!md.contains_key(CACHE_NOT_MODIFIED_KEY));
    }

    #[test]
    fn not_modified_renders() {
        let md = CacheControl::ttl(0)
            .with_etag("\"rev-v1\"")
            .with_revalidatable()
            .with_not_modified()
            .to_metadata();
        assert_eq!(md[CACHE_NOT_MODIFIED_KEY], "1");
    }

    #[test]
    fn stale_windows_render_as_seconds() {
        let md = CacheControl::ttl(10)
            .with_stale_while_revalidate(5)
            .with_stale_if_error(7)
            .with_expires("2026-01-01T00:00:00Z")
            .with_last_modified("2025-01-01T00:00:00Z")
            .to_metadata();
        assert_eq!(md[CACHE_STALE_WHILE_REVALIDATE_KEY], "5");
        assert_eq!(md[CACHE_STALE_IF_ERROR_KEY], "7");
        assert_eq!(md[CACHE_EXPIRES_KEY], "2026-01-01T00:00:00Z");
        assert_eq!(md[CACHE_LAST_MODIFIED_KEY], "2025-01-01T00:00:00Z");
    }

    #[test]
    fn conditional_request_detects_validators() {
        assert!(!ConditionalRequest::default().is_conditional());
        assert!(ConditionalRequest {
            if_none_match: Some("\"x\"".into()),
            ..Default::default()
        }
        .is_conditional());
    }
}

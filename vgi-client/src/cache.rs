// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! An in-memory result cache.
//!
//! A worker opts a result in by advertising `vgi.cache.*` on its first data
//! batch; identical later scans are then served without touching the worker.
//! Nothing is cached unless the worker asked for it.
//!
//! # Identity is part of the key, and that is a security boundary
//!
//! A key that named only the query would serve one principal's rows to another.
//! Every key carries an identity scope, and an identity that is *configured but
//! unresolved* has no scope at all — such a scan is refused rather than cached
//! under a guess. See [`crate::auth::identity`].
//!
//! # Bounds
//!
//! Three independent caps, because each catches what the others miss:
//!
//! - **per-entry bytes** — one enormous result cannot evict everything else;
//! - **total bytes** — the cache cannot grow without limit;
//! - **entry count** — hundreds of thousands of *tiny* entries fit comfortably
//!   under a byte cap while still making every lookup and sweep expensive.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use vgi_protocol::cache_control::CacheControl;

/// Everything that makes one scan's result different from another's.
///
/// Field order here is the definition of cache identity; adding a dimension the
/// worker can vary on without adding it here is how a cache serves wrong rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// Catalog name, retained separately for targeted diagnostics and flushes.
    pub catalog: String,
    /// Catalog name plus the caller's identity fingerprint.
    pub identity_scope: String,
    /// Which worker this came from.
    pub worker_label: String,
    /// Schema-qualified function name.
    pub function: String,
    /// Canonical encoding of the call arguments.
    pub arguments: Vec<u8>,
    /// Projection, if one was pushed.
    pub projection: Option<Vec<i64>>,
    /// Serialized pushdown filters, if any.
    pub filters: Option<Vec<u8>>,
    /// The catalog version this was read at.
    pub catalog_version: i64,
    /// Time-travel coordinate, if any.
    pub at: Option<(String, String)>,
    /// Canonical session/worker setting values that can affect results.
    pub settings: Vec<u8>,
    /// Canonical catalog attach options and resolved version pins.
    pub attach_options: Vec<u8>,
    /// Plain scan limit, distinct from a Top-N hint.
    pub row_limit: Option<i64>,
    /// Canonical ORDER BY/Top-N request, if one was pushed.
    pub ordering: Option<Vec<u8>>,
    /// Canonical sampling request, if one was pushed.
    pub sample: Option<Vec<u8>>,
    /// Stable planning/split shape when it affects the emitted result.
    pub plan: Option<Vec<u8>>,
}

/// A stored result.
#[derive(Debug, Clone)]
pub struct CachedEntry {
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
    stored_at: Instant,
    ttl: Option<Duration>,
    /// Validator for a conditional revalidation.
    pub etag: Option<String>,
    /// Weaker validator used when no ETag was supplied.
    pub last_modified: Option<String>,
    /// Whether the worker said it can check freshness cheaply.
    pub revalidatable: bool,
    stale_while_revalidate: Duration,
    stale_if_error: Duration,
    hits: u64,
}

impl CachedEntry {
    /// The cached batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Total rows across every batch.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Approximate bytes held.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// How many times this entry has been served.
    pub fn hits(&self) -> u64 {
        self.hits
    }

    /// Freshness lifetime last advertised for this payload.
    pub fn freshness_lifetime(&self) -> Option<Duration> {
        self.ttl
    }

    /// Whether the entry is past its freshness lifetime.
    ///
    /// An entry with no TTL never goes stale — the worker said so.
    pub fn is_stale_at(&self, now: Instant) -> bool {
        match self.ttl {
            None => false,
            Some(ttl) => now.duration_since(self.stored_at) >= ttl,
        }
    }

    /// Age of this entry since it became stale.
    pub fn stale_age_at(&self, now: Instant) -> Option<Duration> {
        let ttl = self.ttl?;
        now.checked_duration_since(self.stored_at + ttl)
    }

    /// Whether worker policy permits an immediate stale serve while refreshing.
    pub fn may_serve_while_revalidate_at(&self, now: Instant) -> bool {
        self.stale_age_at(now)
            .is_some_and(|age| age <= self.stale_while_revalidate)
    }

    /// Whether worker policy permits a stale serve after refresh failure.
    pub fn may_serve_on_error_at(&self, now: Instant) -> bool {
        self.stale_age_at(now)
            .is_some_and(|age| age <= self.stale_if_error)
    }
}

/// Safe, immutable information about one cache entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntryInfo {
    /// Stable process-local fingerprint of the complete key.
    pub key_fingerprint: String,
    /// Catalog and schema-qualified function, without credentials.
    pub catalog: String,
    /// Schema-qualified function name.
    pub function: String,
    /// Stored result size.
    pub rows: usize,
    /// Approximate Arrow memory held by the result.
    pub bytes: usize,
    /// Age and freshness at snapshot time.
    pub age: Duration,
    /// Whether the freshness lifetime has elapsed.
    pub stale: bool,
    /// Number of successful serves.
    pub hits: u64,
    /// Validators are reported only as presence flags.
    pub has_etag: bool,
    /// Whether a Last-Modified validator is present.
    pub has_last_modified: bool,
    /// Whether the worker permits conditional validation.
    pub revalidatable: bool,
}

/// Why a scan was not cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ineligible {
    /// The worker advertised nothing, or advertised `no_store`.
    NotCacheable,
    /// The worker asked for caching but named no freshness key.
    NoFreshness,
    /// The caller's identity is configured but unresolved — fail closed.
    IdentityUnresolved,
    /// The result is larger than one entry may be.
    EntryTooLarge,
    /// The result is scoped to a transaction, which this cache does not hold.
    TransactionScoped,
}

/// Caps.
#[derive(Debug, Clone, Copy)]
pub struct CacheLimits {
    /// Largest single entry.
    pub max_entry_bytes: usize,
    /// Total bytes across all entries.
    pub max_total_bytes: usize,
    /// Total number of entries.
    pub max_entries: usize,
    /// Freshness to use when a worker opts in without naming one. Zero means
    /// "require the worker to say", which is the safe default.
    pub default_ttl: Duration,
}

impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            max_entry_bytes: 64 * 1024 * 1024,
            max_total_bytes: 256 * 1024 * 1024,
            max_entries: 131_072,
            default_ttl: Duration::ZERO,
        }
    }
}

/// Counters, for diagnosis.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Lookups served from cache.
    pub hits: u64,
    /// Lookups that found nothing fresh.
    pub misses: u64,
    /// Entries stored.
    pub inserts: u64,
    /// Entries dropped to stay under a cap.
    pub evictions_lru: u64,
    /// Entries dropped for being stale.
    pub evictions_ttl: u64,
    /// Results refused before being stored.
    pub refusals: u64,
    /// Stale entries served under an explicit worker grace window.
    pub stale_serves: u64,
    /// Conditional validations completed with not-modified.
    pub revalidations: u64,
    /// Captures abandoned because execution did not complete successfully.
    pub capture_aborts: u64,
    /// Entries currently held.
    pub entries: usize,
    /// Bytes currently held.
    pub total_bytes: usize,
}

struct Slot {
    entry: CachedEntry,
    /// Monotonic tick of last use, for LRU. A counter rather than a clock so
    /// two uses in the same instant still order.
    last_used: u64,
}

/// The cache.
pub struct ResultCache {
    limits: CacheLimits,
    inner: Mutex<Inner>,
}

struct Inner {
    map: HashMap<CacheKey, Slot>,
    stats: CacheStats,
    tick: u64,
}

impl ResultCache {
    /// A cache with the given caps.
    pub fn new(limits: CacheLimits) -> Self {
        Self {
            limits,
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                stats: CacheStats::default(),
                tick: 0,
            }),
        }
    }

    /// Decide whether a result may be stored, given what the worker advertised.
    ///
    /// `identity_scope` is `None` when the caller's identity is configured but
    /// unresolved, which is refused rather than cached under a guess.
    pub fn eligibility(
        &self,
        control: Option<&CacheControl>,
        identity_scope: Option<&str>,
        bytes: usize,
    ) -> Result<Duration, Ineligible> {
        if identity_scope.is_none() {
            return Err(Ineligible::IdentityUnresolved);
        }
        let Some(cc) = control else {
            return Err(Ineligible::NotCacheable);
        };
        if cc.no_store {
            return Err(Ineligible::NotCacheable);
        }
        if cc.scope == vgi_protocol::cache_control::CACHE_SCOPE_TRANSACTION {
            return Err(Ineligible::TransactionScoped);
        }
        if bytes > self.limits.max_entry_bytes {
            return Err(Ineligible::EntryTooLarge);
        }
        let ttl = match cc.ttl_seconds {
            Some(s) if s > 0 => Duration::from_secs(s as u64),
            // `ttl = 0` with a validator is the HTTP "no-cache" semantic: store
            // it, but treat it as immediately stale so every read revalidates.
            Some(0) if cc.revalidatable && (cc.etag.is_some() || cc.last_modified.is_some()) => {
                Duration::ZERO
            }
            // An explicit zero is never replaced by the client's configured
            // default. It means immediately stale, and is useful only when a
            // conditional validator makes the stored bytes reusable.
            Some(0) => return Err(Ineligible::NoFreshness),
            _ if cc.expires.is_some() => {
                let expires = cc.expires.as_deref().and_then(|value| {
                    chrono::DateTime::parse_from_rfc3339(value)
                        .ok()
                        .map(|value| value.with_timezone(&chrono::Utc))
                });
                let Some(expires) = expires else {
                    return Err(Ineligible::NoFreshness);
                };
                let now = chrono::Utc::now();
                expires
                    .signed_duration_since(now)
                    .to_std()
                    .unwrap_or(Duration::ZERO)
            }
            _ => {
                if self.limits.default_ttl.is_zero() {
                    return Err(Ineligible::NoFreshness);
                }
                self.limits.default_ttl
            }
        };
        Ok(ttl)
    }

    /// Look up a fresh entry, counting the hit or miss.
    ///
    /// A stale entry is dropped rather than returned, unless it is
    /// `revalidatable` — those survive so a conditional request can slide them.
    pub fn get(&self, key: &CacheKey) -> Option<CachedEntry> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        inner.tick += 1;
        let tick = inner.tick;

        let Some(slot) = inner.map.get_mut(key) else {
            inner.stats.misses += 1;
            return None;
        };
        if slot.entry.is_stale_at(now) {
            if !slot.entry.revalidatable {
                let bytes = slot.entry.bytes;
                inner.map.remove(key);
                inner.stats.evictions_ttl += 1;
                inner.stats.total_bytes -= bytes;
                inner.stats.entries -= 1;
            }
            inner.stats.misses += 1;
            return None;
        }
        slot.last_used = tick;
        slot.entry.hits += 1;
        // Clone before touching the stats: `slot` borrows `inner.map`, so the
        // borrow has to end before `inner.stats` can be reached.
        let entry = slot.entry.clone();
        inner.stats.hits += 1;
        Some(entry)
    }

    /// Look up an entry even if stale, for conditional revalidation.
    ///
    /// Separate from [`Self::get`] because `get` *drops* a stale entry, which
    /// would throw away exactly the bytes a 304 lets us reuse.
    pub fn get_for_revalidation(&self, key: &CacheKey) -> Option<CachedEntry> {
        let inner = self.inner.lock().unwrap();
        inner
            .map
            .get(key)
            .filter(|s| s.entry.revalidatable)
            .map(|s| s.entry.clone())
    }

    /// Store a result.
    pub fn insert(
        &self,
        key: CacheKey,
        batches: Vec<RecordBatch>,
        ttl: Duration,
        control: Option<&CacheControl>,
    ) {
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let bytes = batches.iter().map(approx_bytes).sum();
        if bytes > self.limits.max_entry_bytes {
            let mut inner = self.inner.lock().unwrap();
            inner.stats.refusals += 1;
            return;
        }

        let entry = CachedEntry {
            batches,
            rows,
            bytes,
            stored_at: Instant::now(),
            ttl: if ttl.is_zero() && control.is_some_and(|c| c.revalidatable) {
                // Immediately stale but retained: every read revalidates.
                Some(Duration::ZERO)
            } else {
                Some(ttl)
            },
            etag: control.and_then(|c| c.etag.clone()),
            last_modified: control.and_then(|c| c.last_modified.clone()),
            revalidatable: control.is_some_and(|c| c.revalidatable),
            stale_while_revalidate: control
                .and_then(|c| c.stale_while_revalidate)
                .and_then(|v| u64::try_from(v).ok())
                .map(Duration::from_secs)
                .unwrap_or_default(),
            stale_if_error: control
                .and_then(|c| c.stale_if_error)
                .and_then(|v| u64::try_from(v).ok())
                .map(Duration::from_secs)
                .unwrap_or_default(),
            hits: 0,
        };

        let mut inner = self.inner.lock().unwrap();
        inner.tick += 1;
        let tick = inner.tick;
        if let Some(old) = inner.map.remove(&key) {
            inner.stats.total_bytes -= old.entry.bytes;
            inner.stats.entries -= 1;
        }
        inner.stats.total_bytes += bytes;
        inner.stats.entries += 1;
        inner.stats.inserts += 1;
        inner.map.insert(
            key,
            Slot {
                entry,
                last_used: tick,
            },
        );
        self.enforce_caps(&mut inner);
    }

    /// Slide a revalidated entry's lifetime forward.
    ///
    /// What a 304 buys: the stored bytes stay, and only the clock moves.
    pub fn slide(&self, key: &CacheKey, ttl: Duration) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.tick += 1;
        let tick = inner.tick;
        match inner.map.get_mut(key) {
            Some(slot) => {
                slot.entry.stored_at = Instant::now();
                slot.entry.ttl = Some(ttl);
                slot.last_used = tick;
                inner.stats.revalidations += 1;
                true
            }
            None => false,
        }
    }

    /// Remove one exact cache key. Used when a conditional response explicitly
    /// revokes reuse (`no_store`, transaction scope, or otherwise ineligible).
    pub fn remove(&self, key: &CacheKey) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let Some(slot) = inner.map.remove(key) else {
            return false;
        };
        inner.stats.total_bytes = inner.stats.total_bytes.saturating_sub(slot.entry.bytes);
        inner.stats.entries = inner.stats.entries.saturating_sub(1);
        true
    }

    /// Drop everything for one catalog identity.
    ///
    /// Scoped by prefix so one tenant's flush cannot touch another's entries
    /// even when they name the same catalog.
    pub fn flush_scope(&self, identity_scope: &str) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let doomed: Vec<CacheKey> = inner
            .map
            .keys()
            .filter(|k| k.identity_scope == identity_scope)
            .cloned()
            .collect();
        let n = doomed.len();
        for k in doomed {
            if let Some(s) = inner.map.remove(&k) {
                inner.stats.total_bytes -= s.entry.bytes;
                inner.stats.entries -= 1;
            }
        }
        n
    }

    /// Drop every identity's entries for one catalog.
    pub fn flush_catalog(&self, catalog: &str) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let doomed: Vec<_> = inner
            .map
            .keys()
            .filter(|key| key.catalog == catalog)
            .cloned()
            .collect();
        let count = doomed.len();
        for key in doomed {
            if let Some(slot) = inner.map.remove(&key) {
                inner.stats.total_bytes -= slot.entry.bytes;
                inner.stats.entries -= 1;
            }
        }
        count
    }

    /// Drop everything.
    pub fn flush_all(&self) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let n = inner.map.len();
        inner.map.clear();
        inner.stats.entries = 0;
        inner.stats.total_bytes = 0;
        n
    }

    /// Drop every stale entry, returning how many went.
    ///
    /// A `revalidatable` entry is kept: it is *meant* to read as stale, and
    /// reaping it would throw away the bytes a 304 exists to reuse.
    pub fn reap(&self) -> usize {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        let doomed: Vec<CacheKey> = inner
            .map
            .iter()
            .filter(|(_, s)| s.entry.is_stale_at(now) && !s.entry.revalidatable)
            .map(|(k, _)| k.clone())
            .collect();
        let n = doomed.len();
        for k in doomed {
            if let Some(s) = inner.map.remove(&k) {
                inner.stats.total_bytes -= s.entry.bytes;
                inner.stats.entries -= 1;
                inner.stats.evictions_ttl += 1;
            }
        }
        n
    }

    /// A snapshot of the counters.
    pub fn stats(&self) -> CacheStats {
        self.inner.lock().unwrap().stats
    }

    /// Record that an all-or-nothing capture was abandoned.
    pub fn record_capture_abort(&self) {
        self.inner.lock().unwrap().stats.capture_aborts += 1;
    }

    /// Record a stale serve allowed by worker policy.
    pub fn record_stale_serve(&self) {
        self.inner.lock().unwrap().stats.stale_serves += 1;
    }

    /// Snapshot entries without exposing identity scopes or validator values.
    pub fn entries(&self) -> Vec<CacheEntryInfo> {
        use std::hash::{Hash, Hasher};

        let now = Instant::now();
        self.inner
            .lock()
            .unwrap()
            .map
            .iter()
            .map(|(key, slot)| {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key.hash(&mut hasher);
                CacheEntryInfo {
                    key_fingerprint: format!("{:016x}", hasher.finish()),
                    catalog: key.catalog.clone(),
                    function: key.function.clone(),
                    rows: slot.entry.rows,
                    bytes: slot.entry.bytes,
                    age: now.duration_since(slot.entry.stored_at),
                    stale: slot.entry.is_stale_at(now),
                    hits: slot.entry.hits,
                    has_etag: slot.entry.etag.is_some(),
                    has_last_modified: slot.entry.last_modified.is_some(),
                    revalidatable: slot.entry.revalidatable,
                }
            })
            .collect()
    }

    /// Evict least-recently-used entries until every cap is satisfied.
    fn enforce_caps(&self, inner: &mut Inner) {
        while inner.stats.entries > self.limits.max_entries
            || inner.stats.total_bytes > self.limits.max_total_bytes
        {
            let Some(victim) = inner
                .map
                .iter()
                .min_by_key(|(_, s)| s.last_used)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(s) = inner.map.remove(&victim) {
                inner.stats.total_bytes -= s.entry.bytes;
                inner.stats.entries -= 1;
                inner.stats.evictions_lru += 1;
            }
        }
    }
}

/// Approximate the memory a batch holds.
fn approx_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|c| c.get_array_memory_size())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array};
    use arrow_schema::{DataType, Field, Schema};

    fn batch(n: usize) -> RecordBatch {
        let vals: Vec<i64> = (0..n as i64).collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vals)) as ArrayRef],
        )
        .unwrap()
    }

    fn key(scope: &str, func: &str) -> CacheKey {
        CacheKey {
            catalog: "cat".into(),
            identity_scope: scope.into(),
            worker_label: "w".into(),
            function: func.into(),
            arguments: vec![],
            projection: None,
            filters: None,
            catalog_version: 1,
            at: None,
            settings: vec![],
            attach_options: vec![],
            row_limit: None,
            ordering: None,
            sample: None,
            plan: None,
        }
    }

    fn cc(ttl: i64) -> CacheControl {
        CacheControl {
            ttl_seconds: Some(ttl),
            ..CacheControl::ttl(ttl)
        }
    }

    #[test]
    fn stores_and_serves() {
        let c = ResultCache::new(CacheLimits::default());
        let k = key("s", "f");
        assert!(c.get(&k).is_none(), "cold cache misses");

        c.insert(
            k.clone(),
            vec![batch(10)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        let hit = c.get(&k).expect("hit");
        assert_eq!(hit.rows(), 10);
        assert_eq!(c.stats().hits, 1);
        assert_eq!(c.stats().misses, 1);
    }

    #[test]
    fn identity_is_part_of_the_key() {
        // The security property: two principals must never share an entry.
        let c = ResultCache::new(CacheLimits::default());
        c.insert(
            key("alice", "f"),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        assert!(
            c.get(&key("bob", "f")).is_none(),
            "bob must not see alice's cached rows"
        );
        assert!(c.get(&key("alice", "f")).is_some());
    }

    #[test]
    fn an_unresolved_identity_is_refused() {
        let c = ResultCache::new(CacheLimits::default());
        assert_eq!(
            c.eligibility(Some(&cc(60)), None, 100),
            Err(Ineligible::IdentityUnresolved),
            "fail closed rather than cache under a guessed identity"
        );
    }

    #[test]
    fn nothing_is_cached_without_an_opt_in() {
        let c = ResultCache::new(CacheLimits::default());
        assert_eq!(
            c.eligibility(None, Some("s"), 100),
            Err(Ineligible::NotCacheable)
        );

        let no_store = CacheControl::no_store();
        assert_eq!(
            c.eligibility(Some(&no_store), Some("s"), 100),
            Err(Ineligible::NotCacheable),
            "no_store overrides any freshness key"
        );
    }

    #[test]
    fn an_opt_in_without_a_freshness_key_is_refused_by_default() {
        let c = ResultCache::new(CacheLimits::default());
        let bare = CacheControl {
            ttl_seconds: None,
            ..Default::default()
        };
        assert_eq!(
            c.eligibility(Some(&bare), Some("s"), 100),
            Err(Ineligible::NoFreshness)
        );
    }

    #[test]
    fn a_default_ttl_fills_in_for_a_worker_that_names_none() {
        let c = ResultCache::new(CacheLimits {
            default_ttl: Duration::from_secs(30),
            ..Default::default()
        });
        let bare = CacheControl {
            ttl_seconds: None,
            ..Default::default()
        };
        assert_eq!(
            c.eligibility(Some(&bare), Some("s"), 100),
            Ok(Duration::from_secs(30))
        );
    }

    #[test]
    fn an_explicit_zero_ttl_never_falls_back_to_the_client_default() {
        let c = ResultCache::new(CacheLimits {
            default_ttl: Duration::from_secs(30),
            ..Default::default()
        });
        assert_eq!(
            c.eligibility(Some(&CacheControl::ttl(0)), Some("s"), 100),
            Err(Ineligible::NoFreshness)
        );
        assert_eq!(
            c.eligibility(
                Some(&CacheControl::ttl(0).with_revalidatable()),
                Some("s"),
                100
            ),
            Err(Ineligible::NoFreshness),
            "revalidation without a validator cannot reuse stored bytes"
        );
        assert_eq!(
            c.eligibility(
                Some(
                    &CacheControl::ttl(0)
                        .with_etag("\"v1\"")
                        .with_revalidatable()
                ),
                Some("s"),
                100
            ),
            Ok(Duration::ZERO)
        );
    }

    #[test]
    fn a_transaction_scoped_result_is_not_held() {
        let c = ResultCache::new(CacheLimits::default());
        let txn = CacheControl::ttl(60).with_transaction_scope();
        assert_eq!(
            c.eligibility(Some(&txn), Some("s"), 100),
            Err(Ineligible::TransactionScoped)
        );
    }

    #[test]
    fn an_oversized_result_is_refused_rather_than_evicting_everything() {
        let c = ResultCache::new(CacheLimits {
            max_entry_bytes: 64,
            ..Default::default()
        });
        assert_eq!(
            c.eligibility(Some(&cc(60)), Some("s"), 65),
            Err(Ineligible::EntryTooLarge)
        );

        // And insert refuses too, even if a caller skipped the check.
        c.insert(
            key("s", "big"),
            vec![batch(10_000)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        assert!(c.get(&key("s", "big")).is_none());
        assert_eq!(c.stats().refusals, 1);
    }

    #[test]
    fn a_stale_entry_misses_and_is_dropped() {
        let c = ResultCache::new(CacheLimits::default());
        let k = key("s", "f");
        c.insert(k.clone(), vec![batch(1)], Duration::ZERO, None);
        assert!(c.get(&k).is_none(), "a zero TTL is immediately stale");
        assert_eq!(c.stats().evictions_ttl, 1);
        assert_eq!(c.stats().entries, 0);
    }

    #[test]
    fn a_revalidatable_entry_survives_being_stale() {
        // ttl=0 + revalidatable is the "no-cache" semantic: keep the bytes so a
        // 304 can reuse them, but revalidate on every read.
        let c = ResultCache::new(CacheLimits::default());
        let control = CacheControl::ttl(0)
            .with_etag("\"v1\"")
            .with_revalidatable();
        let k = key("s", "f");
        c.insert(k.clone(), vec![batch(5)], Duration::ZERO, Some(&control));

        assert!(c.get(&k).is_none(), "stale, so a plain get misses");
        assert_eq!(c.stats().entries, 1, "but the bytes are still held");

        let e = c.get_for_revalidation(&k).expect("available to revalidate");
        assert_eq!(e.etag.as_deref(), Some("\"v1\""));
        assert_eq!(e.rows(), 5);
    }

    #[test]
    fn sliding_a_revalidated_entry_makes_it_fresh_again() {
        let c = ResultCache::new(CacheLimits::default());
        let control = CacheControl::ttl(0)
            .with_etag("\"v1\"")
            .with_revalidatable();
        let k = key("s", "f");
        c.insert(k.clone(), vec![batch(5)], Duration::ZERO, Some(&control));
        assert!(c.get(&k).is_none());

        assert!(c.slide(&k, Duration::from_secs(300)));
        assert_eq!(
            c.get(&k).expect("fresh after the slide").rows(),
            5,
            "a 304 reuses the stored bytes rather than refetching"
        );
    }

    #[test]
    fn revalidation_refreshes_lru_and_explicit_removal_updates_accounting() {
        let c = ResultCache::new(CacheLimits {
            max_entries: 2,
            ..Default::default()
        });
        let control = CacheControl::ttl(0).with_etag("v1").with_revalidatable();
        let (a, b, d) = (key("s", "a"), key("s", "b"), key("s", "d"));
        c.insert(a.clone(), vec![batch(1)], Duration::ZERO, Some(&control));
        c.insert(
            b.clone(),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        assert!(c.slide(&a, Duration::from_secs(60)));
        c.insert(
            d.clone(),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        assert!(c.get(&a).is_some(), "revalidation refreshed a's LRU tick");
        assert!(c.get(&b).is_none(), "b remained the least recently used");

        let before = c.stats().total_bytes;
        assert!(c.remove(&a));
        assert_eq!(c.stats().entries, 1);
        assert!(c.stats().total_bytes < before);
        assert!(!c.remove(&a), "removal is idempotent");
    }

    #[test]
    fn the_entry_cap_evicts_even_when_bytes_are_tiny() {
        // The cap a byte limit alone misses: many small entries.
        let c = ResultCache::new(CacheLimits {
            max_entries: 3,
            ..Default::default()
        });
        for i in 0..5 {
            c.insert(
                key("s", &format!("f{i}")),
                vec![batch(1)],
                Duration::from_secs(60),
                Some(&cc(60)),
            );
        }
        assert_eq!(c.stats().entries, 3);
        assert_eq!(c.stats().evictions_lru, 2);
    }

    #[test]
    fn eviction_takes_the_least_recently_used() {
        let c = ResultCache::new(CacheLimits {
            max_entries: 2,
            ..Default::default()
        });
        let (a, b, d) = (key("s", "a"), key("s", "b"), key("s", "d"));
        c.insert(
            a.clone(),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        c.insert(
            b.clone(),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        // Touch `a` so `b` becomes the oldest use.
        assert!(c.get(&a).is_some());
        c.insert(
            d.clone(),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );

        assert!(c.get(&a).is_some(), "recently used, so kept");
        assert!(c.get(&d).is_some(), "just inserted");
        assert!(c.get(&b).is_none(), "least recently used, so evicted");
    }

    #[test]
    fn reaping_drops_stale_entries_but_keeps_revalidatable_ones() {
        let c = ResultCache::new(CacheLimits::default());
        c.insert(key("s", "dead"), vec![batch(1)], Duration::ZERO, None);
        let reval = CacheControl::ttl(0).with_etag("\"e\"").with_revalidatable();
        c.insert(
            key("s", "keep"),
            vec![batch(1)],
            Duration::ZERO,
            Some(&reval),
        );

        assert_eq!(c.reap(), 1, "only the plain stale entry goes");
        assert_eq!(c.stats().entries, 1);
        assert!(c.get_for_revalidation(&key("s", "keep")).is_some());
    }

    #[test]
    fn flushing_a_scope_leaves_other_tenants_alone() {
        let c = ResultCache::new(CacheLimits::default());
        c.insert(
            key("alice", "f"),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        c.insert(
            key("bob", "f"),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );

        assert_eq!(c.flush_scope("alice"), 1);
        assert!(c.get(&key("alice", "f")).is_none());
        assert!(
            c.get(&key("bob", "f")).is_some(),
            "bob's entry is untouched"
        );
    }

    #[test]
    fn re_inserting_a_key_replaces_rather_than_double_counts() {
        let c = ResultCache::new(CacheLimits::default());
        let k = key("s", "f");
        c.insert(
            k.clone(),
            vec![batch(10)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        let bytes_once = c.stats().total_bytes;
        c.insert(
            k.clone(),
            vec![batch(10)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );

        assert_eq!(c.stats().entries, 1);
        assert_eq!(
            c.stats().total_bytes,
            bytes_once,
            "byte accounting must not drift on replace"
        );
    }

    #[test]
    fn projection_and_filters_are_part_of_the_key() {
        // A narrower scan is a different result; serving one for the other
        // would hand back columns or rows the caller did not ask for.
        let c = ResultCache::new(CacheLimits::default());
        let mut full = key("s", "f");
        c.insert(
            full.clone(),
            vec![batch(3)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );

        full.projection = Some(vec![0]);
        assert!(
            c.get(&full).is_none(),
            "a projected scan is a different key"
        );

        let mut filtered = key("s", "f");
        filtered.filters = Some(vec![1, 2, 3]);
        assert!(
            c.get(&filtered).is_none(),
            "a filtered scan is a different key"
        );
    }

    #[test]
    fn a_catalog_version_bump_invalidates() {
        let c = ResultCache::new(CacheLimits::default());
        let mut k = key("s", "f");
        c.insert(
            k.clone(),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        k.catalog_version = 2;
        assert!(c.get(&k).is_none(), "a new catalog version is a new key");
    }

    #[test]
    fn hits_are_counted_per_entry() {
        let c = ResultCache::new(CacheLimits::default());
        let k = key("s", "f");
        c.insert(
            k.clone(),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        c.get(&k);
        let e = c.get(&k).unwrap();
        assert_eq!(e.hits(), 2);
    }

    #[test]
    fn every_new_result_shape_dimension_is_keyed() {
        let c = ResultCache::new(CacheLimits::default());
        let base = key("s", "f");
        c.insert(
            base.clone(),
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        let mutations: Vec<Box<dyn Fn(&mut CacheKey)>> = vec![
            Box::new(|key| key.settings = vec![1]),
            Box::new(|key| key.attach_options = vec![2]),
            Box::new(|key| key.row_limit = Some(10)),
            Box::new(|key| key.ordering = Some(vec![3])),
            Box::new(|key| key.sample = Some(vec![4])),
            Box::new(|key| key.plan = Some(vec![5])),
        ];
        for mutate in mutations {
            let mut changed = base.clone();
            mutate(&mut changed);
            assert!(c.get(&changed).is_none());
        }
        assert!(c.get(&base).is_some());
    }

    #[test]
    fn absolute_expiry_is_a_freshness_key() {
        let c = ResultCache::new(CacheLimits::default());
        let expires = (chrono::Utc::now() + chrono::Duration::seconds(30)).to_rfc3339();
        let lifetime = c
            .eligibility(
                Some(&CacheControl::default().with_expires(expires)),
                Some("s"),
                1,
            )
            .expect("valid future expiry");
        assert!(lifetime > Duration::from_secs(20));
        assert!(lifetime <= Duration::from_secs(30));
    }

    #[test]
    fn stale_grace_windows_are_preserved() {
        let c = ResultCache::new(CacheLimits::default());
        let control = CacheControl {
            ttl_seconds: Some(0),
            revalidatable: true,
            stale_while_revalidate: Some(30),
            stale_if_error: Some(60),
            ..Default::default()
        };
        let k = key("s", "f");
        c.insert(k.clone(), vec![batch(1)], Duration::ZERO, Some(&control));
        let entry = c.get_for_revalidation(&k).expect("retained stale entry");
        let now = Instant::now();
        assert!(entry.may_serve_while_revalidate_at(now));
        assert!(entry.may_serve_on_error_at(now));
    }

    #[test]
    fn catalog_flush_and_entry_listing_are_safe() {
        let c = ResultCache::new(CacheLimits::default());
        let mut other = key("secret identity", "other");
        other.catalog = "other".into();
        c.insert(
            key("secret identity", "f"),
            vec![batch(2)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        c.insert(
            other,
            vec![batch(1)],
            Duration::from_secs(60),
            Some(&cc(60)),
        );
        let entries = c.entries();
        assert_eq!(entries.len(), 2);
        assert!(entries
            .iter()
            .all(|entry| !entry.key_fingerprint.contains("secret")));
        assert_eq!(c.flush_catalog("cat"), 1);
        assert_eq!(c.stats().entries, 1);
    }
}

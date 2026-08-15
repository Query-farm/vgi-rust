// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Reuse worker connections instead of spawning one per call.
//!
//! # Why this is not an optimisation
//!
//! A VGI worker is a process. Starting one costs whatever its runtime costs to
//! boot — measured at **1.8 s** for the Python reference fixture worker, or
//! **~4 s** when launched through `uv run`, and the JVM is worse. Meanwhile a
//! catalog client is naturally chatty: discovery alone issues one call per
//! schema and one bind per table, and every scan partition wants its own
//! connection so the scan can fan out.
//!
//! Without pooling those multiply. Attaching a catalog with 200 table functions
//! costs 200 process starts before a single row moves — around thirteen
//! minutes. With pooling it is one.
//!
//! The DuckDB extension reached the same conclusion and ships `VgiWorkerPool`;
//! this mirrors it, including the shapes that matter for correctness rather
//! than speed:
//!
//! * **Keyed on the full [`VgiLocation`]**, so two catalogs pointing at
//!   different workers never share a connection.
//! * **Idle eviction**, so a long-lived process does not hold worker processes
//!   open forever.
//! * **A capacity bound**, past which connections are closed rather than kept.
//! * **Hit/miss counters**, because "is the pool working" is otherwise
//!   invisible — and a pool that silently never hits looks exactly like a slow
//!   worker.
//!
//! # What is not pooled, and why
//!
//! [`VgiLocation::Launch`] and [`VgiLocation::Unix`] connections are pooled
//! here too, but they matter far less: those transports are *already* shared at
//! the OS level — one worker process serves every client of the same socket,
//! across processes. The extension documents the same asymmetry, which is why
//! its `vgi_worker_pool()` diagnostic returns no rows for them. Pooling still
//! saves the connect, just not a process start.
//!
//! # Checkout discipline
//!
//! [`PooledClient`] derefs to [`VgiClient`] and returns the connection on drop.
//! A connection that saw an error is **not** returned: [`PooledClient::poison`]
//! marks it, and a poisoned connection is closed instead. This is deliberate —
//! a VGI connection carries per-session state (bind results, transaction
//! tokens), so recycling one that failed mid-protocol would hand the next
//! caller a worker in an unknown state.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use vgi_rpc::errors::Result;

use crate::client::VgiClient;
use crate::location::VgiLocation;

/// How long an idle connection is kept before eviction.
///
/// Matches the extension's `vgi_worker_pool_idle_limit_seconds` default.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(5);

/// How many connections the pool holds before it starts closing them.
///
/// Matches the extension's `vgi_worker_pool_max` default.
pub const DEFAULT_MAX_IDLE: usize = 256;

/// Tunables for a [`WorkerPool`].
#[derive(Debug, Clone, Copy)]
pub struct PoolConfig {
    /// Evict a connection idle for longer than this. Zero disables pooling.
    pub idle_timeout: Duration,
    /// Hold at most this many idle connections. Zero disables pooling.
    pub max_idle: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            max_idle: DEFAULT_MAX_IDLE,
        }
    }
}

impl PoolConfig {
    /// A configuration that never pools — every acquire opens a fresh worker.
    ///
    /// The equivalent of the extension's `pool false` ATTACH option, and the
    /// way to prove a test does not depend on connection reuse.
    pub fn disabled() -> Self {
        Self {
            idle_timeout: Duration::ZERO,
            max_idle: 0,
        }
    }

    fn pooling_enabled(&self) -> bool {
        self.max_idle > 0 && !self.idle_timeout.is_zero()
    }
}

/// Cumulative pool counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Acquires served from an idle connection.
    pub hits: u64,
    /// Acquires that had to open a new connection.
    pub misses: u64,
    /// Connections closed because they went idle too long.
    pub evicted_idle: u64,
    /// Connections closed because the pool was full.
    pub evicted_full: u64,
    /// Connections closed because the caller poisoned them.
    pub discarded_poisoned: u64,
    /// Idle connections held right now.
    pub idle: u64,
}

struct Idle {
    client: VgiClient,
    since: Instant,
}

#[derive(Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    evicted_idle: AtomicU64,
    evicted_full: AtomicU64,
    discarded_poisoned: AtomicU64,
}

/// A pool of worker connections, keyed by location.
///
/// Cheap to clone — clones share one underlying pool.
#[derive(Clone)]
pub struct WorkerPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    idle: Mutex<HashMap<VgiLocation, Vec<Idle>>>,
    config: PoolConfig,
    counters: Counters,
}

impl std::fmt::Debug for WorkerPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerPool")
            .field("config", &self.inner.config)
            .field("stats", &self.stats())
            .finish()
    }
}

impl Default for WorkerPool {
    fn default() -> Self {
        Self::new(PoolConfig::default())
    }
}

impl WorkerPool {
    /// Build a pool.
    pub fn new(config: PoolConfig) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                idle: Mutex::new(HashMap::new()),
                config,
                counters: Counters::default(),
            }),
        }
    }

    /// Take a connection for `location`, reusing an idle one when possible.
    ///
    /// The connection returns to the pool when the guard drops, unless it was
    /// [poisoned](PooledClient::poison).
    pub fn acquire(&self, location: &VgiLocation) -> Result<PooledClient> {
        if let Some(client) = self.take_idle(location) {
            self.inner.counters.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(PooledClient::new(self.clone(), location.clone(), client));
        }
        self.inner.counters.misses.fetch_add(1, Ordering::Relaxed);
        let client = VgiClient::connect_location(location)?;
        Ok(PooledClient::new(self.clone(), location.clone(), client))
    }

    /// Pop a live idle connection, discarding any that timed out on the way.
    fn take_idle(&self, location: &VgiLocation) -> Option<VgiClient> {
        if !self.inner.config.pooling_enabled() {
            return None;
        }
        let mut idle = self.inner.idle.lock().ok()?;
        let bucket = idle.get_mut(location)?;
        let timeout = self.inner.config.idle_timeout;
        // Newest first: the most recently returned connection is the one most
        // likely to still be alive, and taking it lets the stale tail age out
        // together rather than being churned one at a time.
        while let Some(entry) = bucket.pop() {
            if entry.since.elapsed() <= timeout {
                return Some(entry.client);
            }
            self.inner
                .counters
                .evicted_idle
                .fetch_add(1, Ordering::Relaxed);
            drop(entry.client);
        }
        None
    }

    fn release(&self, location: VgiLocation, client: VgiClient) {
        if !self.inner.config.pooling_enabled() {
            return;
        }
        let Ok(mut idle) = self.inner.idle.lock() else {
            return;
        };
        let held: usize = idle.values().map(Vec::len).sum();
        if held >= self.inner.config.max_idle {
            self.inner
                .counters
                .evicted_full
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        idle.entry(location).or_default().push(Idle {
            client,
            since: Instant::now(),
        });
    }

    /// Drop every idle connection, closing those workers.
    ///
    /// Returns how many were closed. Checked-out connections are unaffected —
    /// they return to an empty pool as usual.
    pub fn flush(&self) -> usize {
        let Ok(mut idle) = self.inner.idle.lock() else {
            return 0;
        };
        let n: usize = idle.values().map(Vec::len).sum();
        idle.clear();
        n
    }

    /// Close connections that have been idle past the timeout.
    ///
    /// Called opportunistically on acquire; expose it so a long-lived host can
    /// reclaim workers without waiting for the next query.
    pub fn reap(&self) -> usize {
        let Ok(mut idle) = self.inner.idle.lock() else {
            return 0;
        };
        let timeout = self.inner.config.idle_timeout;
        let mut closed = 0;
        for bucket in idle.values_mut() {
            bucket.retain(|e| {
                let live = e.since.elapsed() <= timeout;
                if !live {
                    closed += 1;
                }
                live
            });
        }
        idle.retain(|_, b| !b.is_empty());
        self.inner
            .counters
            .evicted_idle
            .fetch_add(closed as u64, Ordering::Relaxed);
        closed
    }

    /// Current counters.
    pub fn stats(&self) -> PoolStats {
        let c = &self.inner.counters;
        PoolStats {
            hits: c.hits.load(Ordering::Relaxed),
            misses: c.misses.load(Ordering::Relaxed),
            evicted_idle: c.evicted_idle.load(Ordering::Relaxed),
            evicted_full: c.evicted_full.load(Ordering::Relaxed),
            discarded_poisoned: c.discarded_poisoned.load(Ordering::Relaxed),
            idle: self
                .inner
                .idle
                .lock()
                .map(|m| m.values().map(Vec::len).sum::<usize>() as u64)
                .unwrap_or(0),
        }
    }
}

/// A checked-out connection that returns itself to the pool on drop.
pub struct PooledClient {
    pool: WorkerPool,
    location: VgiLocation,
    client: Option<VgiClient>,
    poisoned: bool,
}

impl PooledClient {
    fn new(pool: WorkerPool, location: VgiLocation, client: VgiClient) -> Self {
        Self {
            pool,
            location,
            client: Some(client),
            poisoned: false,
        }
    }

    /// Do not return this connection to the pool; close it on drop.
    ///
    /// Call after any protocol-level failure. A VGI connection carries session
    /// state — bind results, transaction tokens, an open stream — so one that
    /// failed mid-protocol is not safe to hand to the next caller even though
    /// the socket may still be writable.
    pub fn poison(&mut self) {
        self.poisoned = true;
    }

    /// Run `f`, poisoning the connection if it fails.
    ///
    /// The ergonomic form of [`poison`](Self::poison): every fallible use of a
    /// pooled connection should go through this, so "did anyone forget to
    /// poison" is not a thing to audit.
    pub fn with<T>(&mut self, f: impl FnOnce(&mut VgiClient) -> Result<T>) -> Result<T> {
        let out = f(self);
        if out.is_err() {
            self.poison();
        }
        out
    }
}

impl Deref for PooledClient {
    type Target = VgiClient;
    fn deref(&self) -> &VgiClient {
        self.client.as_ref().expect("client taken only on drop")
    }
}

impl DerefMut for PooledClient {
    fn deref_mut(&mut self) -> &mut VgiClient {
        self.client.as_mut().expect("client taken only on drop")
    }
}

impl Drop for PooledClient {
    fn drop(&mut self) {
        let Some(client) = self.client.take() else {
            return;
        };
        if self.poisoned {
            self.pool
                .inner
                .counters
                .discarded_poisoned
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.pool.release(self.location.clone(), client);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A location that would fail to connect, used to drive the bookkeeping
    /// paths that never touch a worker.
    fn loc(n: &str) -> VgiLocation {
        VgiLocation::Subprocess(vec![format!("/nonexistent/{n}")])
    }

    #[test]
    fn a_failed_connect_counts_as_a_miss_and_pools_nothing() {
        let pool = WorkerPool::default();
        assert!(pool.acquire(&loc("a")).is_err());
        let s = pool.stats();
        assert_eq!((s.hits, s.misses, s.idle), (0, 1, 0));
    }

    #[test]
    fn disabled_config_reports_itself() {
        assert!(!PoolConfig::disabled().pooling_enabled());
        assert!(PoolConfig::default().pooling_enabled());
        // A zero timeout disables pooling even with capacity, and vice versa.
        assert!(!PoolConfig {
            idle_timeout: Duration::ZERO,
            max_idle: 8
        }
        .pooling_enabled());
        assert!(!PoolConfig {
            idle_timeout: Duration::from_secs(5),
            max_idle: 0
        }
        .pooling_enabled());
    }

    #[test]
    fn flush_and_reap_are_safe_on_an_empty_pool() {
        let pool = WorkerPool::default();
        assert_eq!(pool.flush(), 0);
        assert_eq!(pool.reap(), 0);
    }

    #[test]
    fn locations_are_distinct_keys() {
        // The pool keys on the whole location, so these never share a bucket.
        assert_ne!(loc("a"), loc("b"));
        assert_ne!(
            VgiLocation::Subprocess(vec!["w".into()]),
            VgiLocation::Launch(vec!["w".into()])
        );
    }

    #[test]
    fn clones_share_one_pool() {
        let a = WorkerPool::default();
        let b = a.clone();
        assert!(a.acquire(&loc("x")).is_err());
        assert_eq!(b.stats().misses, 1, "clone sees the same counters");
    }
}

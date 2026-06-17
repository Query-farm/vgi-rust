// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Cross-process state storage for VGI workers.
//!
//! The subprocess transport *pools* workers, so the sink (`process`) and source
//! (`finalize`) phases of a buffering function — or the per-group state of an
//! aggregate, or a parallel scan's work queue — can run in different OS
//! processes. State must therefore outlive any single process.
//!
//! [`FunctionStorage`] is the backend contract (mirroring vgi-python's
//! `FunctionStorage`); a worker holds one `Arc<dyn FunctionStorage>` and every
//! execution shares it. Backends:
//!
//! - [`MemoryStorage`] — in-process `BTreeMap`; for tests and single-process use.
//! - [`FsStorage`] — the original filesystem store (atomic-rename queue); the
//!   zero-dependency fallback.
//! - [`SqliteStorage`] — durable single-file SQLite (feature `sqlite`, default).
//! - [`HttpStorage`] — a client to a remote storage service, so stateless
//!   workers (e.g. several fly.io instances) share one durable store
//!   (feature `http-storage`).
//!
//! The method surface is intentionally **infallible**: a backend that can't
//! complete an operation logs and degrades (reads → empty, writes → dropped),
//! matching the original filesystem store's best-effort contract. The remote
//! [`HttpStorage`] retries internally and, on an unrecoverable error, panics —
//! which the RPC layer's panic isolation converts into a clean per-call error
//! rather than taking down the worker. (Surfacing storage errors as `Result`
//! to callers is a planned refinement.)

use std::sync::Arc;
use std::time::Duration;

/// A shared, thread-safe handle to the worker's storage backend.
pub type SharedStorage = Arc<dyn FunctionStorage>;

mod fs;
mod memory;
#[cfg(feature = "sqlite")]
mod sqlite;

pub use fs::FsStorage;
pub use memory::MemoryStorage;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteStorage;

#[cfg(test)]
mod conformance;

/// Backend contract for cross-process worker state. Keys are opaque byte
/// strings; `scope` is the execution id (or a transaction id). Implementations
/// must be safe to share across threads and processes.
pub trait FunctionStorage: Send + Sync {
    // --- key/value (overwrite), keyed by (scope, key) ---

    /// Read the value at `(scope, key)`, or `None` if absent.
    fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>>;
    /// Overwrite the value at `(scope, key)`.
    fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]);
    /// Delete `(scope, key)` if present.
    fn kv_del(&self, scope: &[u8], key: &[u8]);

    // --- append-only log, keyed by (scope, ns, key) ---

    /// Append `value` under `(scope, ns, key)`; returns its monotonic id.
    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64;
    /// Scan log entries under `(scope, ns, key)` with `id > after_id`, in id
    /// order, up to `limit` entries.
    fn scan(
        &self,
        scope: &[u8],
        ns: &[u8],
        key: &[u8],
        after_id: i64,
        limit: usize,
    ) -> Vec<(i64, Vec<u8>)>;

    // --- FIFO work queue per scope ---

    /// Push items onto the per-scope queue (FIFO). Single-pusher by contract.
    fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]);
    /// Atomically claim and remove the next queue item, or `None` if empty.
    /// Safe across concurrent poppers in different processes.
    fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>>;

    // --- lifecycle ---

    /// Drop all state (kv, log, queue) for `scope`.
    fn clear(&self, scope: &[u8]);

    /// Best-effort sweep of state idle longer than `ttl` (orphan cleanup after a
    /// crashed worker). Default: no-op.
    fn gc(&self, ttl: Duration) {
        let _ = ttl;
    }
}

/// Default age after which idle state is treated as a crashed-worker orphan.
/// Overridable via `VGI_BUFFERING_STORE_TTL_SECS`.
pub const DEFAULT_ORPHAN_TTL_SECS: u64 = 24 * 60 * 60;

/// The configured orphan TTL.
pub fn orphan_ttl() -> Duration {
    let secs = std::env::var("VGI_BUFFERING_STORE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_ORPHAN_TTL_SECS);
    Duration::from_secs(secs)
}

/// Construct the worker's storage backend from `VGI_WORKER_SHARED_STORAGE`
/// (`memory` | `fs` | `sqlite` | `http`). Unset selects `sqlite` when compiled
/// in, else `fs`. Runs an orphan GC pass on construction.
pub fn default_storage() -> Arc<dyn FunctionStorage> {
    let choice = std::env::var("VGI_WORKER_SHARED_STORAGE").unwrap_or_default();
    let store: Arc<dyn FunctionStorage> = match choice.as_str() {
        "memory" => Arc::new(MemoryStorage::new()),
        "fs" => Arc::new(FsStorage::new()),
        #[cfg(feature = "sqlite")]
        "sqlite" => Arc::new(SqliteStorage::new()),
        #[cfg(feature = "http-storage")]
        "http" => Arc::new(http::HttpStorage::from_env()),
        "" => {
            #[cfg(feature = "sqlite")]
            {
                Arc::new(SqliteStorage::new())
            }
            #[cfg(not(feature = "sqlite"))]
            {
                Arc::new(FsStorage::new())
            }
        }
        other => {
            log::warn!("unknown VGI_WORKER_SHARED_STORAGE={other:?}; falling back to fs");
            Arc::new(FsStorage::new())
        }
    };
    store.gc(orphan_ttl());
    store
}

#[cfg(feature = "http-storage")]
pub mod http;
#[cfg(feature = "http-storage")]
pub use http::HttpStorage;

/// Owner of the store, used to namespace per-user base dirs/paths. On unix this
/// is the real uid; elsewhere temp dirs are already per-user.
pub(crate) fn process_uid() -> String {
    #[cfg(unix)]
    {
        // Safe: getuid() always succeeds and has no preconditions.
        (unsafe { libc::getuid() }).to_string()
    }
    #[cfg(not(unix))]
    {
        "user".to_string()
    }
}

/// Restrict a directory to owner-only (0o700) on unix; no-op elsewhere.
pub(crate) fn harden_dir(path: &std::path::Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

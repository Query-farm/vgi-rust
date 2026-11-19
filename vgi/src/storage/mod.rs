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
//! - `HttpStorage` — a client to a remote storage service, so stateless
//!   workers (e.g. several fly.io instances) share one durable store
//!   (feature `http-storage`).
//! - `CfDoStorage` — a client to the Cloudflare Durable Object storage service
//!   the other VGI SDKs use, for multi-host deployments with no storage server
//!   of their own to run (feature `cloudflare-do`).
//!
//! The method surface is intentionally **infallible**: a local backend that
//! can't complete an operation logs and degrades (reads → empty, writes →
//! dropped), matching the original filesystem store's best-effort contract.
//! The remote backends (`HttpStorage`, `CfDoStorage`) retry internally and, on
//! an unrecoverable error or a reply they cannot decode, panic. That is
//! deliberate: vgi-rpc runs every handler under panic isolation, so the panic
//! becomes a clean per-call error for the one RPC (never a crashed worker),
//! and an infallible trait leaves no way to report the failure that would not
//! silently hand the caller an empty value. (Surfacing storage errors as
//! `Result` would change the trait every user function sees, and is left for
//! a breaking release.)
//!
//! The framework's own context (buffered-function init records, aggregate
//! arguments, streaming-aggregate sessions) is written **sealed**; see
//! [`seal`].

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

pub mod seal;

#[cfg(test)]
mod conformance;

/// The Durable Object routing key for a storage scope:
/// `"exe-"` + the first 16 bytes of `sha256(scope)` in lowercase hex.
///
/// 36 characters, inside the `cloudflare-do` server's
/// `^[A-Za-z0-9._-]{1,128}$`, whatever the scope's length or bytes. Also the
/// `shard_key` a [`seal`]ed payload's AAD binds, on every backend.
pub fn shard_key_for_scope(scope: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(scope);
    format!("exe-{}", hex(&digest[..16]))
}

/// Lowercase hex.
pub(crate) fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0xf) as usize] as char);
    }
    s
}

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

    /// Read several keys of one scope, in order: one entry per key, `None`
    /// where absent. Defaults to one [`kv_get`](Self::kv_get) per key.
    ///
    /// Worth overriding wherever a call carries fixed cost (a network round
    /// trip, a lock, a statement): an aggregate `update` reads every group of
    /// a batch through this, once per batch.
    fn kv_get_many(&self, scope: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        keys.iter().map(|k| self.kv_get(scope, k)).collect()
    }

    /// Overwrite several `(key, value)` pairs of one scope. Defaults to one
    /// [`kv_put`](Self::kv_put) per pair. A key that repeats ends with its
    /// last value.
    fn kv_put_many(&self, scope: &[u8], items: &[(Vec<u8>, Vec<u8>)]) {
        for (k, v) in items {
            self.kv_put(scope, k, v);
        }
    }

    // --- append-only log, keyed by (scope, ns, key) ---

    /// Append `value` under `(scope, ns, key)`; returns its monotonic id.
    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64;
    /// Append several values under `(scope, ns, key)` in order, returning their
    /// monotonic ids. Defaults to one [`append`](Self::append) per value.
    ///
    /// Worth overriding wherever a write carries fixed per-statement cost:
    /// SQLite commits a WAL frame per statement, so a few thousand appends
    /// outside a transaction cost far more than the same rows inside one. The
    /// table-in-out FINALIZE flush writes one row per batch and a flush of
    /// several thousand batches is exactly the case that made this worth having.
    fn append_many(&self, scope: &[u8], ns: &[u8], key: &[u8], values: Vec<Vec<u8>>) -> Vec<i64> {
        values
            .into_iter()
            .map(|v| self.append(scope, ns, key, v))
            .collect()
    }
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
/// (`memory` | `fs` | `sqlite` | `http` | `cloudflare-do`). Unset selects
/// `sqlite` when compiled in, else `fs`. Runs an orphan GC pass on construction.
pub fn default_storage() -> Arc<dyn FunctionStorage> {
    let choice = std::env::var("VGI_WORKER_SHARED_STORAGE").unwrap_or_default();
    let store: Arc<dyn FunctionStorage> = match choice.as_str() {
        "memory" => Arc::new(MemoryStorage::new()),
        "fs" => Arc::new(FsStorage::new()),
        #[cfg(feature = "sqlite")]
        "sqlite" => Arc::new(SqliteStorage::new()),
        #[cfg(feature = "http-storage")]
        "http" => Arc::new(http::HttpStorage::from_env()),
        #[cfg(feature = "cloudflare-do")]
        "cloudflare-do" => Arc::new(cfdo::CfDoStorage::from_env()),
        // Falling back to a per-machine store would silently split state across
        // hosts, so a deployment asking for the shared backend must get it.
        #[cfg(not(feature = "cloudflare-do"))]
        "cloudflare-do" => panic!(
            "VGI_WORKER_SHARED_STORAGE=cloudflare-do requires building vgi with \
             the `cloudflare-do` cargo feature"
        ),
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

#[cfg(feature = "cloudflare-do")]
pub mod cfdo;
#[cfg(feature = "cloudflare-do")]
pub use cfdo::CfDoStorage;

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

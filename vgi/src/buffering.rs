// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Table buffering (sink + source) function model.
//!
//! Lifecycle (keyed by execution_id):
//! 1. init phase `TABLE_BUFFERING` (sink) — mint execution_id, header-only.
//! 2. `table_buffering_process` (unary, per input batch) → state_id.
//! 3. `table_buffering_combine` (unary, once) → finalize_state_ids.
//! 4. init phase `TABLE_BUFFERING_FINALIZE` (source, per finalize_state_id) →
//!    a producer that drains the buffered state.
//! 5. `table_buffering_destructor` (unary) — cleanup.
//!
//! State lives in an in-process [`BufferingStore`] (the launcher runs a single
//! long-lived worker; subprocess transport is one worker per connection).

use std::path::PathBuf;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use vgi_rpc::Result;

use crate::arguments::Arguments;
use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata};
use crate::settings::Settings;
use crate::table_function::TableProducer;

/// Cross-process append-log store backed by the filesystem, keyed by
/// `execution_id` then `(namespace, key)`. Subprocess transport pools workers,
/// so the sink (`process`) and source (`finalize`) phases of a buffering
/// function can run in different processes — state must outlive the process,
/// like Python's SQLite-backed storage.
pub struct BufferingStore {
    base: PathBuf,
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

/// Owner of the store tree, used to namespace the base dir. On unix this is the
/// real uid; elsewhere temp dirs are already per-user, so a fixed token is fine.
fn process_uid() -> String {
    #[cfg(unix)]
    {
        // Safe: getuid() is always successful and has no preconditions.
        (unsafe { libc::getuid() }).to_string()
    }
    #[cfg(not(unix))]
    {
        "user".to_string()
    }
}

/// Restrict a directory to owner-only (0o700) so other users on a shared host
/// can't traverse into (and read) buffered state, which may be secret-derived.
/// No-op on non-unix, where the temp dir is already per-user.
fn harden_dir(path: &std::path::Path) {
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

fn orphan_ttl() -> std::time::Duration {
    let secs = std::env::var("VGI_BUFFERING_STORE_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_ORPHAN_TTL_SECS);
    std::time::Duration::from_secs(secs)
}

/// Most recent modification time found anywhere in `path`'s subtree (including
/// `path` itself), or `None` if nothing could be stat'd.
fn newest_mtime(path: &std::path::Path) -> Option<std::time::SystemTime> {
    let mut newest = std::fs::metadata(path).and_then(|m| m.modified()).ok();
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            let m = if p.is_dir() {
                newest_mtime(&p)
            } else {
                std::fs::metadata(&p).and_then(|m| m.modified()).ok()
            };
            if let Some(t) = m {
                newest = Some(newest.map_or(t, |cur| cur.max(t)));
            }
        }
    }
    newest
}

impl Default for BufferingStore {
    fn default() -> Self {
        BufferingStore::new()
    }
}

/// Default age after which an idle execution directory is treated as a
/// crashed-worker orphan and swept on startup. Overridable via
/// `VGI_BUFFERING_STORE_TTL_SECS`. Conservative (24h) so a long-running
/// buffering job is never reclaimed out from under itself.
const DEFAULT_ORPHAN_TTL_SECS: u64 = 24 * 60 * 60;

impl BufferingStore {
    pub fn new() -> Self {
        let mut base = std::env::temp_dir();
        // Namespace by uid: on a host with a shared /tmp, two users must not
        // share (and so be able to read) each other's buffered state, and the
        // first creator's 0o700 dir must not lock the others out.
        base.push(format!("vgi-rust-buffering-{}", process_uid()));
        let _ = std::fs::create_dir_all(&base);
        harden_dir(&base);
        let store = BufferingStore { base };
        store.gc_orphans(orphan_ttl());
        store
    }

    /// Sweep execution directories whose most recent activity is older than
    /// `ttl`. Buffering state is exec-scoped and reclaimed by the
    /// `table_buffering_destructor` RPC; a worker that crashes mid-execution
    /// never sends it, leaking its directory forever. This bounds that leak
    /// without touching live executions (which have recent mtimes).
    pub fn gc_orphans(&self, ttl: std::time::Duration) {
        self.gc_orphans_at(std::time::SystemTime::now(), ttl);
    }

    /// `gc_orphans` against an explicit clock — the seam tests drive so they
    /// don't depend on wall-clock timing or mtime manipulation.
    fn gc_orphans_at(&self, now: std::time::SystemTime, ttl: std::time::Duration) {
        let entries = match std::fs::read_dir(&self.base) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            // Use the newest mtime anywhere in the subtree: appends land in
            // nested `ns__key/` dirs that don't bump the top-level mtime, so a
            // shallow check could reclaim an actively-appending execution.
            let last = newest_mtime(&path).unwrap_or(now);
            if now.duration_since(last).map(|age| age > ttl).unwrap_or(false) {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }

    fn log_dir(&self, exec: &[u8], ns: &[u8], key: &[u8]) -> PathBuf {
        let mut p = self.base.clone();
        p.push(hex(exec));
        p.push(format!("{}__{}", hex(ns), hex(key)));
        p
    }

    /// Append `value` under `(execution_id, namespace, key)`; returns the new
    /// monotonically increasing log id. Cross-process unique via O_EXCL retry.
    pub fn append(&self, exec: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
        use std::io::Write;
        let dir = self.log_dir(exec, ns, key);
        let _ = std::fs::create_dir_all(&dir);
        // Start past the current max to reduce contention.
        let mut id = self.max_id(&dir) + 1;
        loop {
            let path = dir.join(format!("{id:020}.bin"));
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut f) => {
                    let _ = f.write_all(&value);
                    return id;
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    id += 1;
                }
                Err(_) => return id,
            }
        }
    }

    fn max_id(&self, dir: &PathBuf) -> i64 {
        std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                e.file_name()
                    .to_str()
                    .and_then(|n| n.strip_suffix(".bin"))
                    .and_then(|n| n.parse::<i64>().ok())
            })
            .max()
            .unwrap_or(-1)
    }

    /// Scan log entries with `id > after_id`, up to `limit`, ordered by id.
    pub fn scan(
        &self,
        exec: &[u8],
        ns: &[u8],
        key: &[u8],
        after_id: i64,
        limit: usize,
    ) -> Vec<(i64, Vec<u8>)> {
        let dir = self.log_dir(exec, ns, key);
        let mut ids: Vec<i64> = std::fs::read_dir(&dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                e.file_name()
                    .to_str()
                    .and_then(|n| n.strip_suffix(".bin"))
                    .and_then(|n| n.parse::<i64>().ok())
            })
            .filter(|id| *id > after_id)
            .collect();
        ids.sort_unstable();
        ids.into_iter()
            .take(limit)
            .filter_map(|id| {
                std::fs::read(dir.join(format!("{id:020}.bin")))
                    .ok()
                    .map(|v| (id, v))
            })
            .collect()
    }

    /// Drop all state for an execution.
    pub fn clear(&self, exec: &[u8]) {
        let mut p = self.base.clone();
        p.push(hex(exec));
        let _ = std::fs::remove_dir_all(&p);
    }

    fn kv_path(&self, exec: &[u8], key: &[u8]) -> PathBuf {
        let mut p = self.base.clone();
        p.push(hex(exec));
        let _ = std::fs::create_dir_all(&p);
        p.push(format!("kv_{}", hex(key)));
        p
    }

    /// Key-value overwrite put (for per-group aggregate state).
    pub fn kv_put(&self, exec: &[u8], key: &[u8], value: &[u8]) {
        let path = self.kv_path(exec, key);
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, value).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }

    /// Key-value get.
    pub fn kv_get(&self, exec: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        std::fs::read(self.kv_path(exec, key)).ok()
    }

    /// Delete a key.
    pub fn kv_del(&self, exec: &[u8], key: &[u8]) {
        let _ = std::fs::remove_file(self.kv_path(exec, key));
    }

    fn queue_dir(&self, exec: &[u8]) -> PathBuf {
        let mut p = self.base.clone();
        p.push(hex(exec));
        p.push("__queue__");
        p
    }

    /// Push work items onto the per-execution queue (primary worker only, so
    /// no push/push contention). Files are named by ascending push order.
    pub fn queue_push(&self, exec: &[u8], items: &[Vec<u8>]) {
        use std::io::Write;
        let dir = self.queue_dir(exec);
        let _ = std::fs::create_dir_all(&dir);
        let mut id = self.max_id(&dir) + 1;
        for item in items {
            let path = dir.join(format!("{id:020}.bin"));
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                let _ = f.write_all(item);
            }
            id += 1;
        }
    }

    /// Atomically claim the next queue item across pooled workers. Returns
    /// `None` when the queue is empty. The claim is a `rename` (atomic on
    /// POSIX): only one worker wins each item.
    pub fn queue_pop(&self, exec: &[u8], claim_tag: &str) -> Option<Vec<u8>> {
        let dir = self.queue_dir(exec);
        let mut ids: Vec<i64> = std::fs::read_dir(&dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| {
                e.file_name()
                    .to_str()
                    .and_then(|n| n.strip_suffix(".bin"))
                    .and_then(|n| n.parse::<i64>().ok())
            })
            .collect();
        ids.sort_unstable();
        for id in ids {
            let src = dir.join(format!("{id:020}.bin"));
            let claimed = dir.join(format!("claimed_{claim_tag}_{id:020}.bin"));
            if std::fs::rename(&src, &claimed).is_ok() {
                let data = std::fs::read(&claimed).ok();
                let _ = std::fs::remove_file(&claimed);
                if data.is_some() {
                    return data;
                }
            }
        }
        None
    }
}

/// Parameters for buffering process / combine / finalize.
pub struct BufferingParams {
    pub execution_id: Vec<u8>,
    pub storage: Arc<BufferingStore>,
    pub output_schema: SchemaRef,
    pub arguments: Arguments,
    pub settings: Settings,
    /// The (plaintext) attach state for this call, when carried by the request.
    /// Persisted at the sink-init phase and replayed to process/combine, which
    /// otherwise carry no per-attach context (stateful functions scope storage
    /// by this).
    pub attach_opaque_data: Option<Vec<u8>>,
    /// DuckDB per-chunk batch index, when the function declares
    /// `requires_input_batch_index` (only set on the process RPC).
    pub batch_index: Option<i64>,
    /// In-band INFO logs to surface in `duckdb_logs()`; the unary process /
    /// combine handlers drain this into the call context after returning.
    pub logs: Arc<std::sync::Mutex<Vec<String>>>,
}

impl BufferingParams {
    /// Queue an INFO-level client log line (surfaced under `duckdb_logs()`).
    pub fn log(&self, message: impl Into<String>) {
        if let Ok(mut g) = self.logs.lock() {
            g.push(message.into());
        }
    }
}

/// A table buffering (sink+source) function.
pub trait TableBufferingFunction: Send + Sync {
    fn name(&self) -> &str;
    fn metadata(&self) -> FunctionMetadata;
    fn argument_specs(&self) -> Vec<ArgSpec>;
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse>;
    /// Sink one batch; return an opaque state_id.
    fn process(
        &self,
        params: &BufferingParams,
        batch: &arrow_array::RecordBatch,
    ) -> Result<Vec<u8>>;
    /// Merge state_ids into finalize_state_ids.
    fn combine(&self, params: &BufferingParams, state_ids: &[Vec<u8>]) -> Result<Vec<Vec<u8>>>;
    /// Build the per-finalize_state_id source producer.
    fn finalize_producer(
        &self,
        params: &BufferingParams,
        finalize_state_id: Vec<u8>,
    ) -> Result<Box<dyn TableProducer>>;
}

#[cfg(test)]
mod store_hardening_tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    // Isolate each test in its own base dir so they don't see each other's
    // (or a real worker's) executions in the shared uid-namespaced tree.
    fn isolated_store(tag: &str) -> BufferingStore {
        let mut base = std::env::temp_dir();
        base.push(format!("vgi-rust-buffering-test-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::create_dir_all(&base);
        harden_dir(&base);
        BufferingStore { base }
    }

    #[test]
    fn base_dir_is_owner_only_on_unix() {
        let s = isolated_store("perms");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&s.base).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700, "base dir must be owner-only");
        }
        let _ = std::fs::remove_dir_all(&s.base);
    }

    #[test]
    fn gc_sweeps_stale_executions_but_keeps_fresh_ones() {
        let s = isolated_store("gc");
        // Two executions with live state.
        s.kv_put(b"old-exec", b"k", b"v");
        s.append(b"old-exec", b"ns", b"key", b"payload".to_vec());
        s.kv_put(b"fresh-exec", b"k", b"v");

        // Nothing is older than `now`, so a same-clock sweep keeps both.
        s.gc_orphans_at(SystemTime::now(), Duration::from_secs(3600));
        assert!(s.kv_get(b"old-exec", b"k").is_some());
        assert!(s.kv_get(b"fresh-exec", b"k").is_some());

        // Advance the clock 48h: relative to that, both dirs are >24h idle.
        let future = SystemTime::now() + Duration::from_secs(48 * 3600);
        s.gc_orphans_at(future, Duration::from_secs(24 * 3600));
        assert!(s.kv_get(b"old-exec", b"k").is_none(), "stale exec must be swept");
        assert!(s.kv_get(b"fresh-exec", b"k").is_none());
        let _ = std::fs::remove_dir_all(&s.base);
    }

    #[test]
    fn gc_uses_newest_mtime_so_active_appends_survive() {
        // An execution that only appends into a nested ns dir must not be
        // reclaimed: newest_mtime walks the subtree, not just the top dir.
        let s = isolated_store("active");
        s.append(b"appending-exec", b"ns", b"key", b"a".to_vec());
        let newest = newest_mtime(&{
            let mut p = s.base.clone();
            p.push(hex(b"appending-exec"));
            p
        });
        assert!(newest.is_some());
        // Just-written → not older than a 1h ttl at the real clock.
        s.gc_orphans_at(SystemTime::now(), Duration::from_secs(3600));
        assert_eq!(s.scan(b"appending-exec", b"ns", b"key", -1, 10).len(), 1);
        let _ = std::fs::remove_dir_all(&s.base);
    }

    #[test]
    fn gc_on_empty_base_is_a_noop() {
        let s = isolated_store("empty");
        s.gc_orphans(Duration::from_secs(1));
        let _ = std::fs::remove_dir_all(&s.base);
    }
}

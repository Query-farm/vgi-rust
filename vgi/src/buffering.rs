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

impl Default for BufferingStore {
    fn default() -> Self {
        BufferingStore::new()
    }
}

impl BufferingStore {
    pub fn new() -> Self {
        let mut base = std::env::temp_dir();
        base.push("vgi-rust-buffering");
        let _ = std::fs::create_dir_all(&base);
        BufferingStore { base }
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
    pub fn scan(&self, exec: &[u8], ns: &[u8], key: &[u8], after_id: i64, limit: usize) -> Vec<(i64, Vec<u8>)> {
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
            if let Ok(mut f) = std::fs::OpenOptions::new().write(true).create_new(true).open(&path) {
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

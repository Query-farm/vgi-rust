// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Filesystem-backed [`FunctionStorage`]: the original, zero-dependency store.
//! State lives under `$TMPDIR/vgi-rust-buffering-<uid>/<scope>/`. The work queue
//! claims items with an atomic `rename` (lock-free across processes).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use super::{harden_dir, process_uid, FunctionStorage};

/// Cross-process append-log + kv + queue store backed by the filesystem.
pub struct FsStorage {
    base: PathBuf,
}

fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push_str(&format!("{x:02x}"));
    }
    s
}

impl Default for FsStorage {
    fn default() -> Self {
        FsStorage::new()
    }
}

impl FsStorage {
    pub fn new() -> Self {
        let mut base = std::env::temp_dir();
        // Namespace by uid: on a host with a shared /tmp, two users must not
        // share (and so be able to read) each other's buffered state, and the
        // first creator's 0o700 dir must not lock the others out.
        base.push(format!("vgi-rust-buffering-{}", process_uid()));
        let _ = std::fs::create_dir_all(&base);
        harden_dir(&base);
        FsStorage { base }
    }

    /// Construct rooted at an explicit base dir (tests).
    #[cfg(test)]
    pub fn with_base(base: PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&base);
        harden_dir(&base);
        FsStorage { base }
    }

    fn log_dir(&self, scope: &[u8], ns: &[u8], key: &[u8]) -> PathBuf {
        let mut p = self.base.clone();
        p.push(hex(scope));
        p.push(format!("{}__{}", hex(ns), hex(key)));
        p
    }

    fn max_id(&self, dir: &Path) -> i64 {
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

    fn kv_path(&self, scope: &[u8], key: &[u8]) -> PathBuf {
        let mut p = self.base.clone();
        p.push(hex(scope));
        let _ = std::fs::create_dir_all(&p);
        p.push(format!("kv_{}", hex(key)));
        p
    }

    fn queue_dir(&self, scope: &[u8]) -> PathBuf {
        let mut p = self.base.clone();
        p.push(hex(scope));
        p.push("__queue__");
        p
    }

    /// `gc` against an explicit clock (test seam).
    fn gc_at(&self, now: SystemTime, ttl: Duration) {
        let entries = match std::fs::read_dir(&self.base) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let last = newest_mtime(&path).unwrap_or(now);
            if now.duration_since(last).map(|age| age > ttl).unwrap_or(false) {
                let _ = std::fs::remove_dir_all(&path);
            }
        }
    }
}

impl FunctionStorage for FsStorage {
    fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        std::fs::read(self.kv_path(scope, key)).ok()
    }

    fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]) {
        let path = self.kv_path(scope, key);
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, value).is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        }
    }

    fn kv_del(&self, scope: &[u8], key: &[u8]) {
        let _ = std::fs::remove_file(self.kv_path(scope, key));
    }

    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
        use std::io::Write;
        let dir = self.log_dir(scope, ns, key);
        let _ = std::fs::create_dir_all(&dir);
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
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => id += 1,
                Err(_) => return id,
            }
        }
    }

    fn scan(
        &self,
        scope: &[u8],
        ns: &[u8],
        key: &[u8],
        after_id: i64,
        limit: usize,
    ) -> Vec<(i64, Vec<u8>)> {
        let dir = self.log_dir(scope, ns, key);
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

    fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]) {
        use std::io::Write;
        let dir = self.queue_dir(scope);
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

    fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>> {
        // Unique per-claim tag so concurrent poppers never collide on the
        // rename target (pid + a process-local counter).
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let tag = format!(
            "{}_{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let dir = self.queue_dir(scope);
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
            let claimed = dir.join(format!("claimed_{tag}_{id:020}.bin"));
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

    fn clear(&self, scope: &[u8]) {
        let mut p = self.base.clone();
        p.push(hex(scope));
        let _ = std::fs::remove_dir_all(&p);
    }

    fn gc(&self, ttl: Duration) {
        self.gc_at(SystemTime::now(), ttl);
    }
}

/// Most recent modification time anywhere in `path`'s subtree (including `path`
/// itself). Appends land in nested `ns__key/` dirs that don't bump the
/// top-level mtime, so a shallow check could reclaim an actively-appending
/// execution.
fn newest_mtime(path: &Path) -> Option<SystemTime> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn isolated(tag: &str) -> FsStorage {
        let mut base = std::env::temp_dir();
        base.push(format!("vgi-fs-test-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&base);
        FsStorage::with_base(base)
    }

    #[test]
    fn base_dir_is_owner_only_on_unix() {
        let s = isolated("perms");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&s.base).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o700);
        }
        let _ = std::fs::remove_dir_all(&s.base);
    }

    #[test]
    fn gc_sweeps_stale_but_keeps_fresh() {
        let s = isolated("gc");
        s.kv_put(b"old", b"k", b"v");
        s.append(b"old", b"ns", b"key", b"p".to_vec());
        s.kv_put(b"fresh", b"k", b"v");
        s.gc_at(SystemTime::now(), Duration::from_secs(3600));
        assert!(s.kv_get(b"old", b"k").is_some());
        let future = SystemTime::now() + Duration::from_secs(48 * 3600);
        s.gc_at(future, Duration::from_secs(24 * 3600));
        assert!(s.kv_get(b"old", b"k").is_none());
        assert!(s.kv_get(b"fresh", b"k").is_none());
        let _ = std::fs::remove_dir_all(&s.base);
    }
}

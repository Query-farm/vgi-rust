// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! SQLite-backed [`FunctionStorage`]. Durable, single-file, WAL mode so multiple
//! worker processes on one host share it. Atomicity is per-statement
//! (`DELETE ... RETURNING`, `INSERT ... ON CONFLICT`), so no caller-side locks.

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension};

use super::{harden_dir, process_uid, FunctionStorage};

pub struct SqliteStorage {
    conn: Mutex<Connection>,
}

/// `usize` row limit → SQLite `LIMIT` value (`-1` is unlimited).
fn limit_param(limit: usize) -> i64 {
    if limit >= i64::MAX as usize {
        -1
    } else {
        limit as i64
    }
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Default for SqliteStorage {
    fn default() -> Self {
        SqliteStorage::new()
    }
}

impl SqliteStorage {
    pub fn new() -> Self {
        let mut dir = std::env::temp_dir();
        dir.push(format!("vgi-rust-store-{}", process_uid()));
        let _ = std::fs::create_dir_all(&dir);
        harden_dir(&dir);
        let mut path = dir;
        path.push("state.db");
        SqliteStorage::open(path)
    }

    /// Open (or create) the database at `path` (tests use a temp path or
    /// `":memory:"`-style shared cache).
    pub fn open(path: PathBuf) -> Self {
        let conn = Connection::open(&path).unwrap_or_else(|e| {
            panic!("vgi sqlite: cannot open {}: {e}", path.display());
        });
        Self::configure(&conn);
        SqliteStorage {
            conn: Mutex::new(conn),
        }
    }

    fn configure(conn: &Connection) {
        // WAL: readers don't block the single writer, and it works across
        // processes sharing the file. busy_timeout absorbs writer contention.
        let _ = conn.pragma_update(None, "journal_mode", "WAL");
        let _ = conn.pragma_update(None, "synchronous", "NORMAL");
        let _ = conn.busy_timeout(Duration::from_secs(30));
        let _ = conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS function_state (
                 scope BLOB NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL,
                 PRIMARY KEY (scope, key)
             ) WITHOUT ROWID;
             CREATE TABLE IF NOT EXISTS function_log (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 scope BLOB NOT NULL, ns BLOB NOT NULL, key BLOB NOT NULL, value BLOB NOT NULL
             );
             CREATE INDEX IF NOT EXISTS function_log_key ON function_log (scope, ns, key, id);
             CREATE TABLE IF NOT EXISTS work_queue (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, scope BLOB NOT NULL, value BLOB NOT NULL
             );
             CREATE INDEX IF NOT EXISTS work_queue_scope ON work_queue (scope, id);
             CREATE TABLE IF NOT EXISTS scope_touch (
                 scope BLOB PRIMARY KEY, touched_at INTEGER NOT NULL
             ) WITHOUT ROWID;",
        );
    }

    fn touch(conn: &Connection, scope: &[u8]) {
        let _ = conn.execute(
            "INSERT INTO scope_touch (scope, touched_at) VALUES (?1, ?2)
             ON CONFLICT(scope) DO UPDATE SET touched_at = excluded.touched_at",
            params![scope, now_secs()],
        );
    }
}

impl FunctionStorage for SqliteStorage {
    fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT value FROM function_state WHERE scope = ?1 AND key = ?2",
            params![scope, key],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .unwrap_or_else(|e| {
            log::warn!("vgi sqlite kv_get: {e}");
            None
        })
    }

    fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]) {
        let conn = self.conn.lock().unwrap();
        if let Err(e) = conn.execute(
            "INSERT INTO function_state (scope, key, value) VALUES (?1, ?2, ?3)
             ON CONFLICT(scope, key) DO UPDATE SET value = excluded.value",
            params![scope, key, value],
        ) {
            log::warn!("vgi sqlite kv_put: {e}");
        }
        Self::touch(&conn, scope);
    }

    fn kv_del(&self, scope: &[u8], key: &[u8]) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "DELETE FROM function_state WHERE scope = ?1 AND key = ?2",
            params![scope, key],
        );
    }

    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
        let conn = self.conn.lock().unwrap();
        match conn.execute(
            "INSERT INTO function_log (scope, ns, key, value) VALUES (?1, ?2, ?3, ?4)",
            params![scope, ns, key, value],
        ) {
            Ok(_) => {
                Self::touch(&conn, scope);
                conn.last_insert_rowid()
            }
            Err(e) => {
                log::warn!("vgi sqlite append: {e}");
                -1
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
        let conn = self.conn.lock().unwrap();
        let mut stmt = match conn.prepare(
            "SELECT id, value FROM function_log
             WHERE scope = ?1 AND ns = ?2 AND key = ?3 AND id > ?4
             ORDER BY id LIMIT ?5",
        ) {
            Ok(s) => s,
            Err(e) => {
                log::warn!("vgi sqlite scan prepare: {e}");
                return Vec::new();
            }
        };
        let rows = stmt.query_map(
            params![scope, ns, key, after_id, limit_param(limit)],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        );
        match rows {
            Ok(it) => it.filter_map(|r| r.ok()).collect(),
            Err(e) => {
                log::warn!("vgi sqlite scan: {e}");
                Vec::new()
            }
        }
    }

    fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]) {
        let mut conn = self.conn.lock().unwrap();
        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(e) => {
                log::warn!("vgi sqlite queue_push tx: {e}");
                return;
            }
        };
        for item in items {
            if let Err(e) = tx.execute(
                "INSERT INTO work_queue (scope, value) VALUES (?1, ?2)",
                params![scope, item],
            ) {
                log::warn!("vgi sqlite queue_push: {e}");
            }
        }
        let _ = tx.commit();
        Self::touch(&conn, scope);
    }

    fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>> {
        let conn = self.conn.lock().unwrap();
        // Atomic claim: delete the lowest-id row for the scope and return it.
        conn.query_row(
            "DELETE FROM work_queue WHERE id = (
                 SELECT id FROM work_queue WHERE scope = ?1 ORDER BY id LIMIT 1
             ) RETURNING value",
            params![scope],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .unwrap_or_else(|e| {
            log::warn!("vgi sqlite queue_pop: {e}");
            None
        })
    }

    fn clear(&self, scope: &[u8]) {
        let conn = self.conn.lock().unwrap();
        for sql in [
            "DELETE FROM function_state WHERE scope = ?1",
            "DELETE FROM function_log WHERE scope = ?1",
            "DELETE FROM work_queue WHERE scope = ?1",
            "DELETE FROM scope_touch WHERE scope = ?1",
        ] {
            let _ = conn.execute(sql, params![scope]);
        }
    }

    fn gc(&self, ttl: Duration) {
        let cutoff = now_secs() - ttl.as_secs() as i64;
        let conn = self.conn.lock().unwrap();
        let stale: Vec<Vec<u8>> = match conn
            .prepare("SELECT scope FROM scope_touch WHERE touched_at < ?1")
            .and_then(|mut s| {
                s.query_map(params![cutoff], |row| row.get::<_, Vec<u8>>(0))
                    .map(|it| it.filter_map(|r| r.ok()).collect())
            }) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("vgi sqlite gc: {e}");
                return;
            }
        };
        for scope in stale {
            for sql in [
                "DELETE FROM function_state WHERE scope = ?1",
                "DELETE FROM function_log WHERE scope = ?1",
                "DELETE FROM work_queue WHERE scope = ?1",
                "DELETE FROM scope_touch WHERE scope = ?1",
            ] {
                let _ = conn.execute(sql, params![&scope]);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp() -> SqliteStorage {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "vgi-sqlite-test-{}-{:p}.db",
            std::process::id(),
            &0u8 as *const u8
        ));
        let _ = std::fs::remove_file(&p);
        SqliteStorage::open(p)
    }

    #[test]
    fn gc_drops_only_stale_scopes() {
        let s = temp();
        s.kv_put(b"a", b"k", b"v");
        s.append(b"a", b"ns", b"key", b"x".to_vec());
        // Backdate scope `a`'s touch far into the past, leave `b` fresh.
        s.kv_put(b"b", b"k", b"v");
        {
            let conn = s.conn.lock().unwrap();
            conn.execute(
                "UPDATE scope_touch SET touched_at = 0 WHERE scope = ?1",
                params![b"a".as_slice()],
            )
            .unwrap();
        }
        s.gc(Duration::from_secs(60));
        assert!(s.kv_get(b"a", b"k").is_none(), "stale scope swept");
        assert!(s.scan(b"a", b"ns", b"key", -1, 100).is_empty());
        assert!(s.kv_get(b"b", b"k").is_some(), "fresh scope kept");
    }
}

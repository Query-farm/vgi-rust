// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! HTTP client [`FunctionStorage`] + the wire protocol it shares with
//! `vgi-storage-server`.
//!
//! Stateless workers (e.g. several fly.io instances) point at one durable
//! storage service so they share buffering / aggregate / queue state. The
//! client is **synchronous** (`ureq`), so it is safe to call from a worker
//! served over stdio *or* from inside a tokio HTTP runtime — no async nesting.
//!
//! Non-idempotent ops (`append`, `append_many`, `queue_push`, `queue_pop`)
//! carry an idempotency key so a retried request can't double-apply; the
//! server replays the original result.
//!
//! The batched ops (`KvGetMany`, `KvPutMany`, `AppendMany`) are appended to
//! [`StorageOp`], so every older variant keeps its bincode tag. A server that
//! predates them rejects the frame with HTTP 400; the client then falls back
//! to the single-key ops for the rest of the process, so a new worker still
//! works against an old `vgi-storage-server`.
//!
//! # Errors
//!
//! [`FunctionStorage`] is infallible, so an unrecoverable failure (a
//! definitive HTTP status, exhausted retries, or a reply of the wrong shape)
//! panics with a message naming the cause. vgi-rpc runs every handler under
//! panic isolation, so the panic is a clean error for that one RPC, never a
//! crashed worker. [`HttpStorage::try_call`] is the non-panicking core.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::{process_uid, FunctionStorage};

/// One storage operation. Shared by the client and `vgi-storage-server`.
#[derive(Debug, Serialize, Deserialize)]
pub enum StorageOp {
    KvGet {
        scope: Vec<u8>,
        key: Vec<u8>,
    },
    KvPut {
        scope: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    KvDel {
        scope: Vec<u8>,
        key: Vec<u8>,
    },
    Append {
        scope: Vec<u8>,
        ns: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Scan {
        scope: Vec<u8>,
        ns: Vec<u8>,
        key: Vec<u8>,
        after_id: i64,
        limit: u64,
    },
    QueuePush {
        scope: Vec<u8>,
        items: Vec<Vec<u8>>,
    },
    QueuePop {
        scope: Vec<u8>,
    },
    Clear {
        scope: Vec<u8>,
    },
    Gc {
        ttl_secs: u64,
    },
    // Appended after the original ops so their bincode tags are unchanged.
    KvGetMany {
        scope: Vec<u8>,
        keys: Vec<Vec<u8>>,
    },
    KvPutMany {
        scope: Vec<u8>,
        items: Vec<(Vec<u8>, Vec<u8>)>,
    },
    AppendMany {
        scope: Vec<u8>,
        ns: Vec<u8>,
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
    },
}

/// A request envelope: an op plus an optional idempotency key (set for
/// non-idempotent ops so retries don't double-apply).
#[derive(Debug, Serialize, Deserialize)]
pub struct StorageRequest {
    pub idempotency_key: Option<String>,
    pub op: StorageOp,
}

/// An op result, shaped to each op's return type.
#[derive(Debug, Serialize, Deserialize)]
pub enum StorageReply {
    Unit,
    MaybeBytes(Option<Vec<u8>>),
    Id(i64),
    Entries(Vec<(i64, Vec<u8>)>),
    // Appended after the original replies so their bincode tags are unchanged.
    ManyMaybeBytes(Vec<Option<Vec<u8>>>),
    Ids(Vec<i64>),
}

/// A failed logical storage call.
#[derive(Debug)]
pub enum CallError {
    /// The server answered with a definitive HTTP status (not retried).
    Status(u16),
    /// Every attempt failed in transport or decoding.
    Unreachable(String),
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::Status(code) => write!(f, "vgi storage server returned HTTP {code}"),
            CallError::Unreachable(e) => f.write_str(e),
        }
    }
}

impl std::error::Error for CallError {}

/// Number of attempts before the client gives up (and panics).
const MAX_ATTEMPTS: u32 = 5;

/// Wire encoding for the storage RPC frame. `legacy()` is bincode 1's layout
/// (fixint, little-endian) — pinned explicitly so bumping bincode never shifts
/// the bytes on the wire and a client can still talk to an older server.
pub const WIRE: bincode::config::Configuration<
    bincode::config::LittleEndian,
    bincode::config::Fixint,
> = bincode::config::legacy();

/// HTTP client backend. Talks bincode over `POST {url}/rpc`.
pub struct HttpStorage {
    url: String,
    token: Option<String>,
    agent: ureq::Agent,
    seq: AtomicU64,
    /// Set once the server rejected a batched op (it predates them).
    no_batch_ops: AtomicBool,
}

impl HttpStorage {
    /// Construct from `VGI_STORAGE_URL` (required) and `VGI_STORAGE_TOKEN`
    /// (optional bearer token). Panics if the URL is unset — selecting the
    /// `http` backend without a target is a configuration error.
    pub fn from_env() -> Self {
        let url = std::env::var("VGI_STORAGE_URL").unwrap_or_else(|_| {
            panic!("VGI_WORKER_SHARED_STORAGE=http requires VGI_STORAGE_URL");
        });
        let token = std::env::var("VGI_STORAGE_TOKEN").ok();
        Self::new(url, token)
    }

    pub fn new(url: impl Into<String>, token: Option<String>) -> Self {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_connect(Some(Duration::from_secs(5)))
            .timeout_global(Some(Duration::from_secs(30)))
            .build()
            .into();
        HttpStorage {
            url: url.into().trim_end_matches('/').to_string(),
            token,
            agent,
            seq: AtomicU64::new(0),
            no_batch_ops: AtomicBool::new(false),
        }
    }

    /// A per-call idempotency key, stable across retries of the same logical
    /// op (uid + pid + a process-local counter + the wall clock).
    fn idem_key(&self) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!(
            "{}-{}-{}-{}",
            process_uid(),
            std::process::id(),
            self.seq.fetch_add(1, Ordering::Relaxed),
            nanos
        )
    }

    /// Send a request, retrying transient failures; panics on failure (see
    /// the [module docs](self)).
    fn call(&self, op: StorageOp, idempotent: bool) -> StorageReply {
        self.try_call(op, idempotent)
            .unwrap_or_else(|e| panic!("vgi storage: {e}"))
    }

    /// A batched op, or `None` when the server predates batched ops (it
    /// rejects the frame with 400), so the caller falls back to single ops.
    fn call_batch(&self, op: StorageOp, idempotent: bool) -> Option<StorageReply> {
        if self.no_batch_ops.load(Ordering::Relaxed) {
            return None;
        }
        match self.try_call(op, idempotent) {
            Ok(reply) => Some(reply),
            Err(CallError::Status(400)) => {
                log::warn!(
                    "vgi storage server at {} does not support batched ops; \
                     falling back to one call per key (upgrade vgi-storage-server)",
                    self.url
                );
                self.no_batch_ops.store(true, Ordering::Relaxed);
                None
            }
            Err(e) => panic!("vgi storage: {e}"),
        }
    }

    /// Send a request, retrying transport and decode failures up to
    /// `MAX_ATTEMPTS` times. A definitive HTTP status is not retried.
    pub fn try_call(&self, op: StorageOp, idempotent: bool) -> Result<StorageReply, CallError> {
        let idempotency_key = (!idempotent).then(|| self.idem_key());
        let req = StorageRequest {
            idempotency_key,
            op,
        };
        let body = bincode::serde::encode_to_vec(&req, WIRE).expect("serialize storage request");
        let endpoint = format!("{}/rpc", self.url);

        let mut last_err = String::new();
        for attempt in 0..MAX_ATTEMPTS {
            let mut http = self
                .agent
                .post(&endpoint)
                .header("content-type", "application/octet-stream");
            if let Some(tok) = &self.token {
                http = http.header("authorization", &format!("Bearer {tok}"));
            }
            match http.send(&body[..]) {
                // NB: `read_to_vec()` alone would cap the body at ureq's 10 MB
                // default. Stored values (and `Entries` replies) are arbitrary
                // user blobs with no such ceiling, and the ureq 2 reader this
                // replaced was unbounded — so the limit is lifted explicitly
                // rather than silently truncating large reads into a decode
                // error. The endpoint is operator-configured, not untrusted.
                Ok(mut resp) => match resp.body_mut().with_config().limit(u64::MAX).read_to_vec() {
                    Err(e) => last_err = format!("read body: {e}"),
                    Ok(buf) => {
                        match bincode::serde::decode_from_slice::<StorageReply, _>(&buf, WIRE) {
                            Ok((reply, _)) => return Ok(reply),
                            Err(e) => last_err = format!("decode reply: {e}"),
                        }
                    }
                },
                Err(ureq::Error::StatusCode(code)) => {
                    // A definitive server-side rejection (auth, bad request)
                    // won't improve on retry.
                    return Err(CallError::Status(code));
                }
                Err(e) => last_err = format!("transport: {e}"),
            }
            // Back off briefly before retrying transport/decoding failures.
            std::thread::sleep(Duration::from_millis(50 * (attempt as u64 + 1)));
        }
        Err(CallError::Unreachable(format!(
            "{endpoint} unreachable after {MAX_ATTEMPTS} attempts: {last_err}"
        )))
    }
}

/// A reply of the wrong shape for its op is a protocol error, never a
/// silently empty value.
fn unexpected(op: &str, reply: &StorageReply) -> ! {
    panic!("vgi storage: unexpected reply to {op}: {reply:?}")
}

impl FunctionStorage for HttpStorage {
    fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        match self.call(
            StorageOp::KvGet {
                scope: scope.to_vec(),
                key: key.to_vec(),
            },
            true,
        ) {
            StorageReply::MaybeBytes(b) => b,
            r => unexpected("KvGet", &r),
        }
    }

    fn kv_get_many(&self, scope: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let op = StorageOp::KvGetMany {
            scope: scope.to_vec(),
            keys: keys.to_vec(),
        };
        match self.call_batch(op, true) {
            Some(StorageReply::ManyMaybeBytes(v)) if v.len() == keys.len() => v,
            Some(r) => unexpected("KvGetMany", &r),
            None => keys.iter().map(|k| self.kv_get(scope, k)).collect(),
        }
    }

    fn kv_put_many(&self, scope: &[u8], items: &[(Vec<u8>, Vec<u8>)]) {
        if items.is_empty() {
            return;
        }
        let op = StorageOp::KvPutMany {
            scope: scope.to_vec(),
            items: items.to_vec(),
        };
        match self.call_batch(op, true) {
            Some(StorageReply::Unit) => {}
            Some(r) => unexpected("KvPutMany", &r),
            None => {
                for (k, v) in items {
                    self.kv_put(scope, k, v);
                }
            }
        }
    }

    fn append_many(&self, scope: &[u8], ns: &[u8], key: &[u8], values: Vec<Vec<u8>>) -> Vec<i64> {
        if values.is_empty() {
            return Vec::new();
        }
        let n = values.len();
        if self.no_batch_ops.load(Ordering::Relaxed) {
            return values
                .into_iter()
                .map(|v| self.append(scope, ns, key, v))
                .collect();
        }
        let op = StorageOp::AppendMany {
            scope: scope.to_vec(),
            ns: ns.to_vec(),
            key: key.to_vec(),
            values: values.clone(),
        };
        match self.call_batch(op, false) {
            Some(StorageReply::Ids(ids)) if ids.len() == n => ids,
            Some(r) => unexpected("AppendMany", &r),
            None => values
                .into_iter()
                .map(|v| self.append(scope, ns, key, v))
                .collect(),
        }
    }

    fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]) {
        self.call(
            StorageOp::KvPut {
                scope: scope.to_vec(),
                key: key.to_vec(),
                value: value.to_vec(),
            },
            true,
        );
    }

    fn kv_del(&self, scope: &[u8], key: &[u8]) {
        self.call(
            StorageOp::KvDel {
                scope: scope.to_vec(),
                key: key.to_vec(),
            },
            true,
        );
    }

    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
        match self.call(
            StorageOp::Append {
                scope: scope.to_vec(),
                ns: ns.to_vec(),
                key: key.to_vec(),
                value,
            },
            false,
        ) {
            StorageReply::Id(id) => id,
            r => unexpected("Append", &r),
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
        match self.call(
            StorageOp::Scan {
                scope: scope.to_vec(),
                ns: ns.to_vec(),
                key: key.to_vec(),
                after_id,
                limit: limit as u64,
            },
            true,
        ) {
            StorageReply::Entries(e) => e,
            r => unexpected("Scan", &r),
        }
    }

    fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]) {
        self.call(
            StorageOp::QueuePush {
                scope: scope.to_vec(),
                items: items.to_vec(),
            },
            false,
        );
    }

    fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>> {
        match self.call(
            StorageOp::QueuePop {
                scope: scope.to_vec(),
            },
            false,
        ) {
            StorageReply::MaybeBytes(b) => b,
            r => unexpected("QueuePop", &r),
        }
    }

    fn clear(&self, scope: &[u8]) {
        self.call(
            StorageOp::Clear {
                scope: scope.to_vec(),
            },
            true,
        );
    }

    fn gc(&self, ttl: Duration) {
        self.call(
            StorageOp::Gc {
                ttl_secs: ttl.as_secs(),
            },
            true,
        );
    }
}

/// Apply a decoded [`StorageOp`] against a backend, producing the
/// [`StorageReply`]. Used by `vgi-storage-server` so the op→backend mapping
/// lives next to the protocol definition.
pub fn apply_op(store: &dyn FunctionStorage, op: StorageOp) -> StorageReply {
    match op {
        StorageOp::KvGet { scope, key } => StorageReply::MaybeBytes(store.kv_get(&scope, &key)),
        StorageOp::KvPut { scope, key, value } => {
            store.kv_put(&scope, &key, &value);
            StorageReply::Unit
        }
        StorageOp::KvDel { scope, key } => {
            store.kv_del(&scope, &key);
            StorageReply::Unit
        }
        StorageOp::Append {
            scope,
            ns,
            key,
            value,
        } => StorageReply::Id(store.append(&scope, &ns, &key, value)),
        StorageOp::Scan {
            scope,
            ns,
            key,
            after_id,
            limit,
        } => StorageReply::Entries(store.scan(
            &scope,
            &ns,
            &key,
            after_id,
            limit.min(usize::MAX as u64) as usize,
        )),
        StorageOp::QueuePush { scope, items } => {
            store.queue_push(&scope, &items);
            StorageReply::Unit
        }
        StorageOp::QueuePop { scope } => StorageReply::MaybeBytes(store.queue_pop(&scope)),
        StorageOp::Clear { scope } => {
            store.clear(&scope);
            StorageReply::Unit
        }
        StorageOp::Gc { ttl_secs } => {
            store.gc(Duration::from_secs(ttl_secs));
            StorageReply::Unit
        }
        StorageOp::KvGetMany { scope, keys } => {
            StorageReply::ManyMaybeBytes(store.kv_get_many(&scope, &keys))
        }
        StorageOp::KvPutMany { scope, items } => {
            store.kv_put_many(&scope, &items);
            StorageReply::Unit
        }
        StorageOp::AppendMany {
            scope,
            ns,
            key,
            values,
        } => StorageReply::Ids(store.append_many(&scope, &ns, &key, values)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    /// A loopback server answering each connection with the next canned
    /// `(status, body)`, recording each decoded request op.
    fn mock(replies: Vec<(u16, Vec<u8>)>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        std::thread::spawn(move || {
            for (status, body) in replies {
                let (stream, _) = listener.accept().unwrap();
                let mut r = BufReader::new(stream.try_clone().unwrap());
                let mut len = 0usize;
                loop {
                    let mut line = String::new();
                    r.read_line(&mut line).unwrap();
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap();
                    }
                    if line == "\r\n" {
                        break;
                    }
                }
                let mut buf = vec![0; len];
                r.read_exact(&mut buf).unwrap();
                let (req, _): (StorageRequest, _) =
                    bincode::serde::decode_from_slice(&buf, WIRE).unwrap();
                let op = format!("{:?}", req.op);
                seen2
                    .lock()
                    .unwrap()
                    .push(op.split([' ', '{']).next().unwrap().to_string());
                let mut w = stream;
                write!(
                    w,
                    "HTTP/1.1 {status} X\r\ncontent-type: application/octet-stream\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                w.write_all(&body).unwrap();
            }
        });
        (url, seen)
    }

    fn reply(r: &StorageReply) -> Vec<u8> {
        bincode::serde::encode_to_vec(r, WIRE).unwrap()
    }

    #[test]
    fn batched_reads_are_one_call() {
        let (url, seen) = mock(vec![(
            200,
            reply(&StorageReply::ManyMaybeBytes(vec![
                Some(b"a".to_vec()),
                None,
            ])),
        )]);
        let s = HttpStorage::new(url, None);
        assert_eq!(
            s.kv_get_many(b"s", &[b"x".to_vec(), b"y".to_vec()]),
            vec![Some(b"a".to_vec()), None]
        );
        assert_eq!(*seen.lock().unwrap(), ["KvGetMany"]);
    }

    #[test]
    fn an_old_server_falls_back_to_single_ops_once() {
        let (url, seen) = mock(vec![
            (400, b"decode request: unknown variant".to_vec()),
            (200, reply(&StorageReply::MaybeBytes(Some(b"a".to_vec())))),
            (200, reply(&StorageReply::MaybeBytes(None))),
            (200, reply(&StorageReply::Unit)),
        ]);
        let s = HttpStorage::new(url, None);
        assert_eq!(
            s.kv_get_many(b"s", &[b"x".to_vec(), b"y".to_vec()]),
            vec![Some(b"a".to_vec()), None]
        );
        // Remembered: the next batch goes straight to single ops.
        s.kv_put_many(b"s", &[(b"x".to_vec(), b"1".to_vec())]);
        assert_eq!(
            *seen.lock().unwrap(),
            ["KvGetMany", "KvGet", "KvGet", "KvPut"]
        );
    }

    #[test]
    fn definitive_status_is_an_error_not_a_retry() {
        let (url, seen) = mock(vec![(401, b"no".to_vec())]);
        let s = HttpStorage::new(url, Some("t".into()));
        let err = s
            .try_call(
                StorageOp::KvGet {
                    scope: b"s".to_vec(),
                    key: b"k".to_vec(),
                },
                true,
            )
            .unwrap_err();
        assert!(matches!(err, CallError::Status(401)), "{err}");
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[test]
    #[should_panic(expected = "unexpected reply to KvGet")]
    fn a_reply_of_the_wrong_shape_is_an_error_not_a_default() {
        let (url, _) = mock(vec![(200, reply(&StorageReply::Id(3)))]);
        HttpStorage::new(url, None).kv_get(b"s", b"k");
    }
}

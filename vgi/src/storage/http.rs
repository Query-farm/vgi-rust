// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! HTTP client [`FunctionStorage`] + the wire protocol it shares with
//! `vgi-storage-server`.
//!
//! Stateless workers (e.g. several fly.io instances) point at one durable
//! storage service so they share buffering / aggregate / queue state. The
//! client is **synchronous** (`ureq`), so it is safe to call from a worker
//! served over stdio *or* from inside a tokio HTTP runtime — no async nesting.
//!
//! Non-idempotent ops (`append`, `queue_push`, `queue_pop`) carry an
//! idempotency key so a retried request can't double-apply; the server replays
//! the original result. On unrecoverable failure the client panics, which the
//! RPC layer's panic isolation turns into a clean per-call error.

use std::sync::atomic::{AtomicU64, Ordering};
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
}

/// Number of attempts before the client gives up (and panics).
const MAX_ATTEMPTS: u32 = 5;

/// HTTP client backend. Talks bincode over `POST {url}/rpc`.
pub struct HttpStorage {
    url: String,
    token: Option<String>,
    agent: ureq::Agent,
    seq: AtomicU64,
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
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build();
        HttpStorage {
            url: url.into().trim_end_matches('/').to_string(),
            token,
            agent,
            seq: AtomicU64::new(0),
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

    /// Send a request, retrying transient failures. Panics after `MAX_ATTEMPTS`.
    fn call(&self, op: StorageOp, idempotent: bool) -> StorageReply {
        let idempotency_key = (!idempotent).then(|| self.idem_key());
        let req = StorageRequest {
            idempotency_key,
            op,
        };
        let body = bincode::serialize(&req).expect("serialize storage request");
        let endpoint = format!("{}/rpc", self.url);

        let mut last_err = String::new();
        for attempt in 0..MAX_ATTEMPTS {
            let mut http = self
                .agent
                .post(&endpoint)
                .set("content-type", "application/octet-stream");
            if let Some(tok) = &self.token {
                http = http.set("authorization", &format!("Bearer {tok}"));
            }
            match http.send_bytes(&body) {
                Ok(resp) => {
                    let mut buf = Vec::new();
                    if let Err(e) = std::io::Read::read_to_end(&mut resp.into_reader(), &mut buf) {
                        last_err = format!("read body: {e}");
                    } else {
                        match bincode::deserialize::<StorageReply>(&buf) {
                            Ok(reply) => return reply,
                            Err(e) => last_err = format!("decode reply: {e}"),
                        }
                    }
                }
                Err(ureq::Error::Status(code, _)) => {
                    // A definitive server-side rejection (auth, bad request)
                    // won't improve on retry.
                    panic!("vgi storage server returned HTTP {code}");
                }
                Err(e) => last_err = format!("transport: {e}"),
            }
            // Back off briefly before retrying transport/decoding failures.
            std::thread::sleep(Duration::from_millis(50 * (attempt as u64 + 1)));
        }
        panic!("vgi storage: {endpoint} unreachable after {MAX_ATTEMPTS} attempts: {last_err}");
    }
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
            _ => None,
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
            _ => -1,
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
            _ => Vec::new(),
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
            _ => None,
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
    }
}

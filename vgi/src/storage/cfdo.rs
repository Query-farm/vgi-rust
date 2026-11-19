// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Cloudflare Durable Object [`FunctionStorage`] client (feature
//! `cloudflare-do`).
//!
//! Talks to the `vgi-cloudflare-durable-object-storage` Worker, the same
//! service vgi-python's `FunctionStorageCfDo`, vgi-go's `cfdo`, vgi-java's
//! `CfdoStorage` and vgi-typescript's cf-do backend use. Selected with
//! `VGI_WORKER_SHARED_STORAGE=cloudflare-do`; configured by `VGI_CF_DO_URL`
//! (required) and `VGI_CF_DO_TOKEN` (the per-worker API key minted by the
//! service's `vgi-keys` CLI, sent as `Authorization: Bearer`).
//!
//! # Wire format
//!
//! Every call is `POST {url}/{endpoint}` with a JSON body. Byte strings are
//! standard (padded) base64. Every body carries a `shard_key`, which the Worker
//! composes with the tenant resolved from the API key to pick one Durable
//! Object (`idFromName([tenant, shard_key])`).
//!
//! | trait method   | endpoint(s)                                        |
//! |----------------|----------------------------------------------------|
//! | `kv_get`       | `state_get_many` (`ns = "kv"`, one key)            |
//! | `kv_get_many`  | `state_get_many` (`ns = "kv"`, up to 1,000 keys)   |
//! | `kv_put`       | `state_put_many` (`ns = "kv"`, one item)           |
//! | `kv_put_many`  | `state_put_many` (`ns = "kv"`, ≤ 4 MiB per call)   |
//! | `kv_del`       | `state_delete` (`ns = "kv"`, one key)              |
//! | `append`       | `state_append` (`item` = value → `ordinal`)        |
//! | `append_many`  | one `state_append` per value (see below)           |
//! | `scan`         | `state_log_scan` (`after_id`, optional `limit`)    |
//! | `queue_push`   | `queue_push` (`execution_id` = scope)              |
//! | `queue_pop`    | `queue_pop`                                        |
//! | `clear`        | `execution_clear` then `queue_clear`               |
//! | `gc`           | no-op: each DO evicts itself through an idle alarm |
//!
//! `append_many` is the trait's default loop: the server has no batched log
//! append (`state_append` takes one `item`, and its replay detection is one
//! row per `attempt_id`), so a table-in-out FINALIZE flush over HTTP still
//! costs one round trip per batch on this backend. A `state_append_many`
//! endpoint (`items: [b64]` → `ordinals: [int]`, one transaction, replayed by
//! `attempt_id`) would make it one call.
//!
//! # Sharding
//!
//! The Rust [`FunctionStorage`] methods carry no attach context, only a
//! `scope` (execution or transaction id), and every method touches exactly one
//! scope. So the shard key is derived from the scope:
//! `"exe-" + hex(sha256(scope)[..16])` (see [`shard_key_for_scope`]). Every
//! process, on any machine, that holds the same scope lands on the same Durable
//! Object, and all of one scope's state lives together. The other SDKs shard
//! per ATTACH (`"att-" + hex(attach uuid)`); a Rust execution's state is only
//! ever read by the Rust worker that serves it, so the two layouts never need
//! to meet.
//!
//! # Retries and idempotency
//!
//! Mirrors the Python client's lockstep contract: each logical call is
//! retried synchronously up to three times (transport errors, 5xx, non-JSON
//! replies). Every destructive endpoint carries a fresh 32-char lowercase-hex
//! `attempt_id` minted **once per logical call** and reused on every retry, so
//! the server replays the original result (`state_append` returns the same
//! ordinal, `queue_pop` the same item, `queue_push` doesn't double-insert)
//! instead of re-executing. 401, 400 and other 4xx replies are not retried.
//!
//! The trait is infallible; an unrecoverable error panics (like `HttpStorage`),
//! which the RPC layer's panic isolation turns into a per-call error. The API
//! key never appears in a panic message, a log line or `Debug` output.

use std::fmt;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde_json::{json, Map, Value};

use super::FunctionStorage;

/// Namespace the trait's `(scope, key)` key/value family is stored under in the
/// server's `(scope_id, ns, key)` state table.
pub const KV_NS: &[u8] = b"kv";

/// Attempts per logical call, matching the Python and Go clients.
const POST_ATTEMPTS: u32 = 3;

/// Largest integer a JSON number carries exactly into the JavaScript server.
const MAX_SAFE_JSON_INT: u64 = (1 << 53) - 1;

/// Keys per `state_get_many` call from [`FunctionStorage::kv_get_many`]. The
/// server chunks its own `IN` clause; this bounds the request, and a reply
/// over the server's 8 MiB read budget (a 413) is retried in halves.
const GET_MANY_KEYS: usize = 1000;

/// Items per `state_put_many` call from [`FunctionStorage::kv_put_many`], and
/// the raw key + value bytes per call (base64 grows it by a third).
const PUT_MANY_ITEMS: usize = 1000;
const PUT_MANY_BYTES: usize = 4 * 1024 * 1024;

/// A failed logical call: the last HTTP status seen (if any) and why.
#[derive(Debug)]
pub(crate) struct PostError {
    pub status: Option<u16>,
    pub message: String,
}

impl fmt::Display for PostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

use super::hex;
pub use super::shard_key_for_scope;

/// A fresh idempotency token: 16 random bytes as 32 lowercase hex characters,
/// the shape the server's `ATTEMPT_ID_RE` requires.
pub fn new_attempt_id() -> String {
    hex(&rand::random::<[u8; 16]>())
}

fn b64(bytes: &[u8]) -> Value {
    Value::String(B64.encode(bytes))
}

/// Build the JSON body for one call: `fields` plus `shard_key` and, for a
/// destructive endpoint, `attempt_id`.
pub(crate) fn build_body(
    mut fields: Map<String, Value>,
    shard_key: &str,
    attempt_id: Option<&str>,
) -> Vec<u8> {
    fields.insert("shard_key".into(), Value::String(shard_key.to_string()));
    if let Some(a) = attempt_id {
        fields.insert("attempt_id".into(), Value::String(a.to_string()));
    }
    serde_json::to_vec(&Value::Object(fields)).expect("serialize JSON body")
}

/// What to do with one HTTP reply.
#[derive(Debug, PartialEq)]
pub(crate) enum Outcome {
    /// A 2xx JSON reply.
    Done(Value),
    /// Transient: retry with the same `attempt_id`.
    Retry(String),
    /// Definitive: retrying cannot help.
    Fatal(String),
}

/// Classify a reply the way vgi-python's `_post` does: non-JSON and 5xx are
/// transient; 401, 400 and other 4xx are definitive.
pub(crate) fn classify(endpoint: &str, status: u16, body: &[u8]) -> Outcome {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let snippet = || String::from_utf8_lossy(&body[..body.len().min(200)]).into_owned();
    let Some(data) = parsed else {
        return Outcome::Retry(format!(
            "{endpoint}: non-JSON response (status={status}): {}",
            snippet()
        ));
    };
    let field = |name: &str| data.get(name).and_then(Value::as_str).map(str::to_string);
    match status {
        401 => Outcome::Fatal(format!(
            "{endpoint}: authentication failed: {}",
            field("error").unwrap_or_else(|| "unauthorized".into())
        )),
        400 => Outcome::Fatal(format!(
            "{endpoint}: request rejected: {}",
            field("message")
                .or_else(|| field("error"))
                .unwrap_or_else(|| data.to_string())
        )),
        500..=599 => Outcome::Retry(format!("{endpoint}: server error {status}: {data}")),
        402..=499 => Outcome::Fatal(format!("{endpoint}: error {status}: {data}")),
        _ => Outcome::Done(data),
    }
}

/// Cloudflare Durable Object storage backend. See the [module docs](self).
pub struct CfDoStorage {
    url: String,
    token: Option<String>,
    agent: ureq::Agent,
}

impl fmt::Debug for CfDoStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CfDoStorage")
            .field("url", &self.url)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl CfDoStorage {
    /// Construct from `VGI_CF_DO_URL` (required) and `VGI_CF_DO_TOKEN`
    /// (optional; an empty value counts as unset). Panics if the URL is
    /// unset: selecting `cloudflare-do` without a target is a configuration
    /// error.
    pub fn from_env() -> Self {
        let url = std::env::var("VGI_CF_DO_URL")
            .ok()
            .filter(|u| !u.is_empty())
            .unwrap_or_else(|| {
                panic!(
                    "VGI_CF_DO_URL environment variable is required when \
                     VGI_WORKER_SHARED_STORAGE=cloudflare-do"
                )
            });
        let token = std::env::var("VGI_CF_DO_TOKEN")
            .ok()
            .filter(|t| !t.is_empty());
        Self::new(url, token)
    }

    /// A client for the Worker at `url` (e.g.
    /// `https://vgi-storage.<account>.workers.dev`), authenticating with
    /// `token` as a bearer API key.
    pub fn new(url: impl Into<String>, token: Option<String>) -> Self {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            // Read every status's body ourselves: 4xx/5xx carry JSON `error`s.
            .http_status_as_error(false)
            .timeout_connect(Some(Duration::from_secs(10)))
            .timeout_global(Some(Duration::from_secs(30)))
            .build()
            .into();
        CfDoStorage {
            url: url.into().trim_end_matches('/').to_string(),
            token,
            agent,
        }
    }

    /// POST one logical call, retrying transient failures with the same body
    /// (and so the same `attempt_id`). Panics on a definitive error or when
    /// every attempt fails.
    fn post(
        &self,
        endpoint: &str,
        fields: Map<String, Value>,
        scope: &[u8],
        attempt_id: Option<&str>,
    ) -> Value {
        match self.try_post(endpoint, fields, scope, attempt_id) {
            Ok(v) => v,
            Err(e) => panic!("vgi cloudflare-do storage: {e}"),
        }
    }

    /// [`post`](Self::post) without the panic.
    fn try_post(
        &self,
        endpoint: &str,
        fields: Map<String, Value>,
        scope: &[u8],
        attempt_id: Option<&str>,
    ) -> Result<Value, PostError> {
        let body = build_body(fields, &shard_key_for_scope(scope), attempt_id);
        self.post_body(endpoint, &body)
    }

    /// The retry loop over a prepared body.
    pub(crate) fn post_body(&self, endpoint: &str, body: &[u8]) -> Result<Value, PostError> {
        let target = format!("{}/{endpoint}", self.url);
        let mut last_err = String::new();
        let mut last_status = None;
        for attempt in 0..POST_ATTEMPTS {
            if attempt > 0 {
                std::thread::sleep(Duration::from_millis(50 * u64::from(attempt)));
            }
            let mut req = self
                .agent
                .post(&target)
                .header("content-type", "application/json");
            if let Some(tok) = &self.token {
                req = req.header("authorization", &format!("Bearer {tok}"));
            }
            let mut resp = match req.send(body) {
                Ok(r) => r,
                Err(e) => {
                    last_err = format!("{endpoint}: transport error: {e}");
                    log::debug!("cf-do {endpoint} attempt={attempt} transport error: {e}");
                    continue;
                }
            };
            let status = resp.status().as_u16();
            last_status = Some(status);
            // `state_log_scan` replies are unbounded by the server, so lift
            // ureq's 10 MB default rather than truncate into a decode error.
            let bytes = match resp.body_mut().with_config().limit(u64::MAX).read_to_vec() {
                Ok(b) => b,
                Err(e) => {
                    last_err = format!("{endpoint}: reading response: {e}");
                    log::debug!("cf-do {endpoint} attempt={attempt} read error: {e}");
                    continue;
                }
            };
            match classify(endpoint, status, &bytes) {
                Outcome::Done(v) => return Ok(v),
                Outcome::Fatal(e) => {
                    return Err(PostError {
                        status: Some(status),
                        message: e,
                    })
                }
                Outcome::Retry(e) => {
                    log::debug!("cf-do {endpoint} attempt={attempt} retrying: {e}");
                    last_err = e;
                }
            }
        }
        Err(PostError {
            status: last_status,
            message: format!("{last_err} (gave up after {POST_ATTEMPTS} attempts)"),
        })
    }

    /// One `state_get_many` over `keys`, halving on a 413 (the reply would
    /// exceed the server's read budget).
    fn get_many_chunk(&self, scope: &[u8], keys: &[Vec<u8>], out: &mut Vec<Option<Vec<u8>>>) {
        let mut f = Self::scoped(scope, KV_NS);
        f.insert(
            "keys".into(),
            Value::Array(keys.iter().map(|k| b64(k)).collect()),
        );
        match self.try_post("state_get_many", f, scope, None) {
            Ok(data) => {
                let rows = rows(&data, "state_get_many");
                assert!(
                    rows.len() == keys.len(),
                    "vgi cloudflare-do storage: state_get_many returned {} rows for {} keys",
                    rows.len(),
                    keys.len()
                );
                out.extend(
                    rows.iter().map(|r| {
                        (!r.is_null()).then(|| decode(&r["value"], "state_get_many value"))
                    }),
                );
            }
            Err(e) if e.status == Some(413) && keys.len() > 1 => {
                let (a, b) = keys.split_at(keys.len() / 2);
                self.get_many_chunk(scope, a, out);
                self.get_many_chunk(scope, b, out);
            }
            Err(e) => panic!("vgi cloudflare-do storage: {e}"),
        }
    }

    fn scoped(scope: &[u8], ns: &[u8]) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("scope_id".into(), b64(scope));
        m.insert("ns".into(), b64(ns));
        m
    }

    fn execution(scope: &[u8]) -> Map<String, Value> {
        let mut m = Map::new();
        m.insert("execution_id".into(), b64(scope));
        m
    }
}

fn decode(v: &Value, what: &str) -> Vec<u8> {
    let s = v
        .as_str()
        .unwrap_or_else(|| panic!("vgi cloudflare-do storage: {what} is not a string: {v}"));
    B64.decode(s)
        .unwrap_or_else(|e| panic!("vgi cloudflare-do storage: {what} is not base64: {e}"))
}

fn int(v: &Value, what: &str) -> i64 {
    v.as_i64()
        .unwrap_or_else(|| panic!("vgi cloudflare-do storage: {what} is not an integer: {v}"))
}

fn rows<'a>(data: &'a Value, endpoint: &str) -> &'a Vec<Value> {
    data.get("rows")
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("vgi cloudflare-do storage: {endpoint} reply has no rows"))
}

impl FunctionStorage for CfDoStorage {
    fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        let mut f = Self::scoped(scope, KV_NS);
        f.insert("keys".into(), json!([B64.encode(key)]));
        let data = self.post("state_get_many", f, scope, None);
        rows(&data, "state_get_many")
            .first()
            .filter(|r| !r.is_null())
            .map(|r| decode(&r["value"], "state_get_many value"))
    }

    fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]) {
        let mut f = Self::scoped(scope, KV_NS);
        f.insert(
            "items".into(),
            json!([{ "key": B64.encode(key), "value": B64.encode(value) }]),
        );
        self.post("state_put_many", f, scope, Some(&new_attempt_id()));
    }

    fn kv_get_many(&self, scope: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        let mut out = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(GET_MANY_KEYS) {
            self.get_many_chunk(scope, chunk, &mut out);
        }
        out
    }

    /// One `state_put_many` per chunk of at most `PUT_MANY_ITEMS` items and
    /// `PUT_MANY_BYTES` of keys and values, each atomic on the server and
    /// each with its own `attempt_id` (the server's replay check looks at a
    /// call's first item).
    fn kv_put_many(&self, scope: &[u8], items: &[(Vec<u8>, Vec<u8>)]) {
        let mut start = 0;
        while start < items.len() {
            let mut end = start;
            let mut bytes = 0usize;
            while end < items.len()
                && end - start < PUT_MANY_ITEMS
                && (end == start
                    || bytes + items[end].0.len() + items[end].1.len() <= PUT_MANY_BYTES)
            {
                bytes += items[end].0.len() + items[end].1.len();
                end += 1;
            }
            let mut f = Self::scoped(scope, KV_NS);
            f.insert(
                "items".into(),
                Value::Array(
                    items[start..end]
                        .iter()
                        .map(|(k, v)| json!({ "key": B64.encode(k), "value": B64.encode(v) }))
                        .collect(),
                ),
            );
            self.post("state_put_many", f, scope, Some(&new_attempt_id()));
            start = end;
        }
    }

    fn kv_del(&self, scope: &[u8], key: &[u8]) {
        let mut f = Self::scoped(scope, KV_NS);
        f.insert("keys".into(), json!([B64.encode(key)]));
        self.post("state_delete", f, scope, Some(&new_attempt_id()));
    }

    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
        let mut f = Self::scoped(scope, ns);
        f.insert("key".into(), b64(key));
        f.insert("item".into(), b64(&value));
        let data = self.post("state_append", f, scope, Some(&new_attempt_id()));
        int(&data["ordinal"], "state_append ordinal")
    }

    fn scan(
        &self,
        scope: &[u8],
        ns: &[u8],
        key: &[u8],
        after_id: i64,
        limit: usize,
    ) -> Vec<(i64, Vec<u8>)> {
        if limit == 0 {
            return Vec::new();
        }
        let mut f = Self::scoped(scope, ns);
        f.insert("key".into(), b64(key));
        f.insert("after_id".into(), json!(after_id));
        // `usize::MAX` means unbounded: omit `limit` (the server's default).
        if limit != usize::MAX {
            f.insert("limit".into(), json!((limit as u64).min(MAX_SAFE_JSON_INT)));
        }
        let data = self.post("state_log_scan", f, scope, None);
        rows(&data, "state_log_scan")
            .iter()
            .map(|r| {
                (
                    int(&r["id"], "state_log_scan id"),
                    decode(&r["value"], "state_log_scan value"),
                )
            })
            .collect()
    }

    fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]) {
        if items.is_empty() {
            return;
        }
        let mut f = Self::execution(scope);
        f.insert(
            "items".into(),
            Value::Array(items.iter().map(|i| b64(i)).collect()),
        );
        self.post("queue_push", f, scope, Some(&new_attempt_id()));
    }

    fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>> {
        let data = self.post(
            "queue_pop",
            Self::execution(scope),
            scope,
            Some(&new_attempt_id()),
        );
        match &data["item"] {
            Value::Null => None,
            v => Some(decode(v, "queue_pop item")),
        }
    }

    fn clear(&self, scope: &[u8]) {
        let mut f = Map::new();
        f.insert("scope_id".into(), b64(scope));
        self.post("execution_clear", f, scope, Some(&new_attempt_id()));
        self.post(
            "queue_clear",
            Self::execution(scope),
            scope,
            Some(&new_attempt_id()),
        );
    }

    // `gc`: the default no-op. Each Durable Object deletes itself through an
    // alarm once idle (12 h, or 5 min after a terminal op such as `clear`).
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    fn shard_ok(s: &str) -> bool {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    }

    #[test]
    fn shard_key_is_stable_valid_and_scope_specific() {
        let k = shard_key_for_scope(b"exec-1");
        assert_eq!(k, shard_key_for_scope(b"exec-1"));
        assert_ne!(k, shard_key_for_scope(b"exec-2"));
        assert_eq!(k.len(), 36);
        assert!(k.starts_with("exe-"));
        assert!(shard_ok(&k));
        // Arbitrary bytes and lengths still give a valid key.
        assert!(shard_ok(&shard_key_for_scope(&[0xff; 4096])));
        assert!(shard_ok(&shard_key_for_scope(b"")));
        // Pinned vector: sha256("abc") = ba7816bf8f01cfea414140de5dae2223...
        assert_eq!(
            shard_key_for_scope(b"abc"),
            "exe-ba7816bf8f01cfea414140de5dae2223"
        );
    }

    #[test]
    fn attempt_ids_are_32_lowercase_hex_and_unique() {
        let a = new_attempt_id();
        assert_eq!(a.len(), 32);
        assert!(a
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
        assert_ne!(a, new_attempt_id());
    }

    #[test]
    fn body_carries_shard_and_attempt() {
        let mut f = CfDoStorage::scoped(b"s", b"kv");
        f.insert("keys".into(), json!([B64.encode(b"k")]));
        let v: Value = serde_json::from_slice(&build_body(f, "exe-x", Some("ab"))).unwrap();
        assert_eq!(v["scope_id"], "cw==");
        assert_eq!(v["ns"], "a3Y=");
        assert_eq!(v["keys"], json!(["aw=="]));
        assert_eq!(v["shard_key"], "exe-x");
        assert_eq!(v["attempt_id"], "ab");
        let v: Value = serde_json::from_slice(&build_body(Map::new(), "exe-x", None)).unwrap();
        assert!(v.get("attempt_id").is_none());
    }

    #[test]
    fn classify_matches_the_python_client() {
        assert_eq!(
            classify("e", 200, br#"{"n":1}"#),
            Outcome::Done(json!({"n": 1}))
        );
        assert!(matches!(classify("e", 200, b"<html>"), Outcome::Retry(_)));
        assert!(matches!(
            classify("e", 502, b"bad gateway"),
            Outcome::Retry(_)
        ));
        assert!(matches!(
            classify("e", 500, br#"{"error":"internal"}"#),
            Outcome::Retry(_)
        ));
        match classify("e", 401, br#"{"error":"unauthorized"}"#) {
            Outcome::Fatal(m) => assert!(m.contains("authentication failed")),
            o => panic!("{o:?}"),
        }
        match classify(
            "e",
            400,
            br#"{"error":"bad_request","message":"attempt_id is required"}"#,
        ) {
            Outcome::Fatal(m) => assert!(m.contains("attempt_id is required")),
            o => panic!("{o:?}"),
        }
        assert!(matches!(
            classify("e", 413, br#"{"error":"payload_too_large"}"#),
            Outcome::Fatal(_)
        ));
        assert!(matches!(
            classify("e", 404, br#"{"error":"not_found"}"#),
            Outcome::Fatal(_)
        ));
    }

    #[test]
    fn debug_redacts_token() {
        let s = CfDoStorage::new("http://x/", Some("vgi_t_secret".into()));
        let d = format!("{s:?}");
        assert!(!d.contains("vgi_t_secret"), "{d}");
        assert!(d.contains("<redacted>"));
        assert_eq!(s.url, "http://x");
    }

    /// A one-shot loopback server: answers each connection with the next
    /// canned `(status, body)` and records each request (head + body).
    type Seen = Arc<Mutex<Vec<(String, String)>>>;

    fn mock(replies: Vec<(u16, &'static str)>) -> (String, Seen) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        std::thread::spawn(move || {
            for (status, body) in replies {
                let (stream, _) = listener.accept().unwrap();
                let mut r = BufReader::new(stream.try_clone().unwrap());
                let mut head = String::new();
                let mut len = 0usize;
                loop {
                    let mut line = String::new();
                    r.read_line(&mut line).unwrap();
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap();
                    }
                    head.push_str(&line);
                    if line == "\r\n" {
                        break;
                    }
                }
                let mut buf = vec![0; len];
                r.read_exact(&mut buf).unwrap();
                seen2
                    .lock()
                    .unwrap()
                    .push((head, String::from_utf8(buf).unwrap()));
                let mut w = stream;
                write!(
                    w,
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        (url, seen)
    }

    #[test]
    fn retries_reuse_the_attempt_id_and_send_the_bearer() {
        let (url, seen) = mock(vec![
            (503, r#"{"error":"busy"}"#),
            (200, "not json"),
            (200, r#"{"ordinal":7}"#),
        ]);
        let s = CfDoStorage::new(url, Some("tok123".into()));
        assert_eq!(s.append(b"scope", b"ns", b"k", b"v".to_vec()), 7);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        let bodies: Vec<Value> = seen
            .iter()
            .map(|(_, b)| serde_json::from_str(b).unwrap())
            .collect();
        let a = bodies[0]["attempt_id"].as_str().unwrap();
        assert_eq!(a.len(), 32);
        assert!(bodies.iter().all(|b| b["attempt_id"] == a));
        assert!(bodies
            .iter()
            .all(|b| b["shard_key"] == shard_key_for_scope(b"scope")));
        assert!(seen[0].0.starts_with("POST /state_append "));
        assert!(seen[0]
            .0
            .to_ascii_lowercase()
            .contains("authorization: bearer tok123"));
    }

    #[test]
    fn definitive_errors_do_not_retry_and_never_leak_the_token() {
        let (url, seen) = mock(vec![(401, r#"{"error":"unauthorized"}"#)]);
        let s = CfDoStorage::new(url, Some("tok-secret-xyz".into()));
        let err = s.post_body("queue_pop", b"{}").unwrap_err().to_string();
        assert!(err.contains("authentication failed"), "{err}");
        assert!(!err.contains("tok-secret-xyz"));
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[test]
    fn exhausted_retries_report_the_last_error() {
        let (url, seen) = mock(vec![(500, r#"{"error":"a"}"#); 3]);
        let s = CfDoStorage::new(url, None);
        let err = s.post_body("queue_pop", b"{}").unwrap_err().to_string();
        assert!(err.contains("gave up after 3 attempts"), "{err}");
        assert_eq!(seen.lock().unwrap().len(), 3);
        // No token configured → no Authorization header.
        assert!(!seen.lock().unwrap()[0]
            .0
            .to_ascii_lowercase()
            .contains("authorization"));
    }

    #[test]
    fn get_many_batches_and_halves_on_413() {
        let (url, seen) = mock(vec![
            (413, r#"{"error":"payload_too_large"}"#),
            (200, r#"{"rows":[{"value":"YQ=="},null]}"#),
            (200, r#"{"rows":[{"value":"Yw=="},{"value":"ZA=="}]}"#),
        ]);
        let s = CfDoStorage::new(url, None);
        let keys: Vec<Vec<u8>> = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()];
        assert_eq!(
            s.kv_get_many(b"s", &keys),
            vec![
                Some(b"a".to_vec()),
                None,
                Some(b"c".to_vec()),
                Some(b"d".to_vec())
            ]
        );
        let seen = seen.lock().unwrap();
        let first: Value = serde_json::from_str(&seen[0].1).unwrap();
        assert_eq!(first["keys"].as_array().unwrap().len(), 4);
        let half: Value = serde_json::from_str(&seen[1].1).unwrap();
        assert_eq!(half["keys"], json!(["YQ==", "Yg=="]));
        assert!(seen[0].0.starts_with("POST /state_get_many "));
    }

    #[test]
    fn put_many_is_one_call_with_one_attempt_id() {
        let (url, seen) = mock(vec![(200, "{}")]);
        let s = CfDoStorage::new(url, None);
        s.kv_put_many(
            b"s",
            &[
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
            ],
        );
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let body: Value = serde_json::from_str(&seen[0].1).unwrap();
        assert_eq!(body["items"].as_array().unwrap().len(), 2);
        assert_eq!(body["ns"], "a3Y=");
        assert_eq!(body["attempt_id"].as_str().unwrap().len(), 32);
    }

    #[test]
    fn read_paths_decode_replies() {
        let (url, seen) = mock(vec![
            (200, r#"{"rows":[{"value":"djE="}]}"#),
            (200, r#"{"rows":[null]}"#),
            (
                200,
                r#"{"rows":[{"id":3,"value":"YQ=="},{"id":9,"value":"Yg=="}]}"#,
            ),
            (200, r#"{"item":"aXQ="}"#),
            (200, r#"{"item":null}"#),
        ]);
        let s = CfDoStorage::new(url, None);
        assert_eq!(s.kv_get(b"s", b"k"), Some(b"v1".to_vec()));
        assert_eq!(s.kv_get(b"s", b"k"), None);
        assert_eq!(
            s.scan(b"s", b"ns", b"k", -1, usize::MAX),
            vec![(3, b"a".to_vec()), (9, b"b".to_vec())]
        );
        assert_eq!(s.queue_pop(b"s"), Some(b"it".to_vec()));
        assert_eq!(s.queue_pop(b"s"), None);
        let seen = seen.lock().unwrap();
        let get: Value = serde_json::from_str(&seen[0].1).unwrap();
        assert!(get.get("attempt_id").is_none(), "reads carry no attempt_id");
        let scan: Value = serde_json::from_str(&seen[2].1).unwrap();
        assert!(scan.get("limit").is_none(), "unbounded scan omits limit");
        assert_eq!(scan["after_id"], -1);
        let pop: Value = serde_json::from_str(&seen[3].1).unwrap();
        assert_eq!(pop["execution_id"], "cw==");
        assert!(pop["attempt_id"].is_string(), "queue_pop is destructive");
    }
}

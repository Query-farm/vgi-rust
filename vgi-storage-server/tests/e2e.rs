// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! End-to-end: launch the real `vgi-storage-server` binary, drive it through the
//! `http` [`vgi::storage::HttpStorage`] client (real `ureq` over a real socket),
//! and assert the same behavior the in-process backends pass in the conformance
//! suite — plus bearer auth and idempotent-retry replay.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use vgi::storage::http::{StorageOp, StorageRequest, WIRE};
use vgi::storage::{FunctionStorage, HttpStorage};

struct Server {
    child: Child,
    port: u16,
    db: std::path::PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.db);
    }
}

/// Launch the server on an ephemeral port; block until it prints `PORT:<n>`.
fn start_server(token: Option<&str>) -> Server {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let mut db = std::env::temp_dir();
    db.push(format!(
        "vgi-storage-e2e-{}-{}.db",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_file(&db);

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_vgi-storage-server"));
    cmd.env("VGI_STORAGE_BIND", "127.0.0.1:0")
        .env("VGI_STORAGE_DB", &db)
        .stdout(Stdio::piped());
    if let Some(t) = token {
        cmd.env("VGI_STORAGE_TOKEN", t);
    }
    let mut child = cmd.spawn().expect("spawn vgi-storage-server");

    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);
    let mut port = 0u16;
    let mut line = String::new();
    for _ in 0..50 {
        line.clear();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }
        if let Some(p) = line.trim().strip_prefix("PORT:") {
            port = p.parse().expect("port");
            break;
        }
    }
    assert!(port != 0, "server never announced a port");
    Server { child, port, db }
}

fn client(server: &Server, token: Option<&str>) -> HttpStorage {
    HttpStorage::new(
        format!("http://127.0.0.1:{}", server.port),
        token.map(str::to_string),
    )
}

#[test]
fn http_backend_conforms_end_to_end() {
    let server = start_server(None);
    let store = client(&server, None);

    // kv
    assert_eq!(store.kv_get(b"e1", b"k"), None);
    store.kv_put(b"e1", b"k", b"v1");
    assert_eq!(store.kv_get(b"e1", b"k").as_deref(), Some(&b"v1"[..]));
    store.kv_put(b"e1", b"k", b"v2");
    assert_eq!(store.kv_get(b"e1", b"k").as_deref(), Some(&b"v2"[..]));
    store.kv_del(b"e1", b"k");
    assert_eq!(store.kv_get(b"e1", b"k"), None);

    // append-log
    let id0 = store.append(b"e1", b"ns", b"", b"a".to_vec());
    let id1 = store.append(b"e1", b"ns", b"", b"b".to_vec());
    assert!(id1 > id0);
    let all = store.scan(b"e1", b"ns", b"", -1, usize::MAX);
    assert_eq!(
        all.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
    assert_eq!(store.scan(b"e1", b"ns", b"", id0, usize::MAX).len(), 1);

    // queue FIFO
    store.queue_push(b"q", &[b"one".to_vec(), b"two".to_vec()]);
    assert_eq!(store.queue_pop(b"q").as_deref(), Some(&b"one"[..]));
    assert_eq!(store.queue_pop(b"q").as_deref(), Some(&b"two"[..]));
    assert_eq!(store.queue_pop(b"q"), None);

    // clear
    store.kv_put(b"c", b"k", b"v");
    store.clear(b"c");
    assert_eq!(store.kv_get(b"c", b"k"), None);
}

#[test]
fn bearer_auth_is_enforced() {
    let server = start_server(Some("s3cret"));
    // Correct token works.
    let good = client(&server, Some("s3cret"));
    good.kv_put(b"e", b"k", b"v");
    assert_eq!(good.kv_get(b"e", b"k").as_deref(), Some(&b"v"[..]));
    // Wrong/no token: the client panics on the server's 401 — assert that.
    let bad = client(&server, Some("wrong"));
    let panicked =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| bad.kv_get(b"e", b"k"))).is_err();
    assert!(panicked, "unauthorized request should be rejected");
}

#[test]
fn idempotent_retry_replays_queue_pop() {
    // The same idempotency key must not claim two queue items: replaying a
    // non-idempotent op returns the original result.
    let server = start_server(None);
    let url = format!("http://127.0.0.1:{}/rpc", server.port);
    let store = client(&server, None);
    store.queue_push(b"qi", &[b"first".to_vec(), b"second".to_vec()]);

    // Issue a raw QueuePop twice with the SAME idempotency key.
    let req = StorageRequest {
        idempotency_key: Some("fixed-key-123".to_string()),
        op: StorageOp::QueuePop {
            scope: b"qi".to_vec(),
        },
    };
    let body = bincode::serde::encode_to_vec(&req, WIRE).unwrap();
    let pop_once = || -> Vec<u8> {
        let mut resp = ureq::post(&url)
            .header("content-type", "application/octet-stream")
            .send(&body[..])
            .unwrap();
        resp.body_mut().read_to_vec().unwrap()
    };
    let a = pop_once();
    let b = pop_once();
    assert_eq!(a, b, "replayed idempotent pop must return the same item");
    // And only ONE item was actually consumed: the other is still queued.
    assert_eq!(store.queue_pop(b"qi").as_deref(), Some(&b"second"[..]));
    assert_eq!(store.queue_pop(b"qi"), None);
}

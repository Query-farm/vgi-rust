// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Backend-agnostic conformance suite: every [`FunctionStorage`] impl must pass
//! the same behavioral checks. Run against memory, fs, and (when built) sqlite.

use super::*;

fn check_kv(store: &dyn FunctionStorage) {
    assert_eq!(store.kv_get(b"e1", b"k"), None);
    store.kv_put(b"e1", b"k", b"v1");
    assert_eq!(store.kv_get(b"e1", b"k").as_deref(), Some(&b"v1"[..]));
    store.kv_put(b"e1", b"k", b"v2"); // overwrite
    assert_eq!(store.kv_get(b"e1", b"k").as_deref(), Some(&b"v2"[..]));
    // Scoping: a different scope is isolated.
    assert_eq!(store.kv_get(b"e2", b"k"), None);
    store.kv_del(b"e1", b"k");
    assert_eq!(store.kv_get(b"e1", b"k"), None);
}

fn check_log(store: &dyn FunctionStorage) {
    let id0 = store.append(b"e1", b"ns", b"", b"a".to_vec());
    let id1 = store.append(b"e1", b"ns", b"", b"b".to_vec());
    assert!(id1 > id0, "log ids must be monotonic");
    // Full scan in order.
    let all = store.scan(b"e1", b"ns", b"", -1, usize::MAX);
    assert_eq!(
        all.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
    // Cursor: entries after id0.
    let after = store.scan(b"e1", b"ns", b"", id0, usize::MAX);
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].1, b"b".to_vec());
    // Limit.
    assert_eq!(store.scan(b"e1", b"ns", b"", -1, 1).len(), 1);
    // Namespace isolation.
    assert!(store.scan(b"e1", b"other", b"", -1, usize::MAX).is_empty());
}

/// `append_many` must be indistinguishable from the same `append` calls made
/// one at a time — same order, same monotonic ids, same interleaving with
/// single appends. Three backends override it for speed, so the three must be
/// pinned to the one contract.
fn check_append_many(store: &dyn FunctionStorage) {
    let first = store.append(b"am", b"ns", b"k", b"0".to_vec());
    let ids = store.append_many(
        b"am",
        b"ns",
        b"k",
        vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
    );
    assert_eq!(ids.len(), 3, "append_many returns one id per value");
    assert!(
        ids[0] > first && ids[1] > ids[0] && ids[2] > ids[1],
        "append_many ids must be monotonic and follow prior appends: {first} then {ids:?}"
    );
    let last = store.append(b"am", b"ns", b"k", b"4".to_vec());
    assert!(last > ids[2], "a later append must follow the batch");

    let all = store.scan(b"am", b"ns", b"k", -1, usize::MAX);
    assert_eq!(
        all.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
        vec![
            b"0".to_vec(),
            b"1".to_vec(),
            b"2".to_vec(),
            b"3".to_vec(),
            b"4".to_vec()
        ],
        "append_many must preserve the order it was given"
    );
    // The cursor walk the FINALIZE flush drain does: one row per turn, never
    // repeating and never skipping.
    let mut cursor = -1i64;
    let mut walked = Vec::new();
    while let Some((id, v)) = store.scan(b"am", b"ns", b"k", cursor, 1).into_iter().next() {
        cursor = id;
        walked.push(v);
    }
    assert_eq!(
        walked,
        all.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
        "a one-row-at-a-time cursor walk must see exactly what a full scan sees"
    );
    // Empty input writes nothing.
    assert!(store
        .append_many(b"am", b"ns", b"empty", Vec::new())
        .is_empty());
    assert!(store
        .scan(b"am", b"ns", b"empty", -1, usize::MAX)
        .is_empty());
    store.clear(b"am");
}

/// `kv_get_many` / `kv_put_many` must be indistinguishable from the same
/// single calls: order kept, absent keys `None`, a repeated key ends with its
/// last value, scopes isolated, and an empty batch is a no-op.
fn check_kv_many(store: &dyn FunctionStorage) {
    let keys: Vec<Vec<u8>> = (0u8..40).map(|i| vec![b'g', i]).collect();
    assert!(store.kv_get_many(b"km", &keys).iter().all(Option::is_none));
    let items: Vec<(Vec<u8>, Vec<u8>)> = keys
        .iter()
        .enumerate()
        .filter(|(i, _)| i % 3 != 0)
        .map(|(i, k)| (k.clone(), vec![i as u8; i + 1]))
        .collect();
    store.kv_put_many(b"km", &items);
    store.kv_put_many(b"km", &[]);
    let got = store.kv_get_many(b"km", &keys);
    assert_eq!(got.len(), keys.len());
    for (i, (k, v)) in keys.iter().zip(&got).enumerate() {
        let want = (i % 3 != 0).then(|| vec![i as u8; i + 1]);
        assert_eq!(v, &want, "key {i}");
        assert_eq!(store.kv_get(b"km", k), want, "single read of key {i}");
    }
    // Overwrite through the batch; a key repeated in one batch keeps its last value.
    store.kv_put_many(
        b"km",
        &[
            (keys[1].clone(), b"x".to_vec()),
            (keys[1].clone(), b"y".to_vec()),
        ],
    );
    assert_eq!(store.kv_get(b"km", &keys[1]).as_deref(), Some(&b"y"[..]));
    // A key may repeat in a read too.
    assert_eq!(
        store.kv_get_many(b"km", &[keys[1].clone(), keys[0].clone(), keys[1].clone()]),
        vec![Some(b"y".to_vec()), None, Some(b"y".to_vec())]
    );
    assert!(store.kv_get_many(b"km", &[]).is_empty());
    assert!(store
        .kv_get_many(b"km-other", &keys[..2])
        .iter()
        .all(Option::is_none));
    store.clear(b"km");
    assert!(store.kv_get_many(b"km", &keys).iter().all(Option::is_none));
}

fn check_queue(store: &dyn FunctionStorage) {
    store.queue_push(b"q", &[b"one".to_vec(), b"two".to_vec(), b"three".to_vec()]);
    // FIFO order, each item claimed exactly once.
    assert_eq!(store.queue_pop(b"q").as_deref(), Some(&b"one"[..]));
    assert_eq!(store.queue_pop(b"q").as_deref(), Some(&b"two"[..]));
    assert_eq!(store.queue_pop(b"q").as_deref(), Some(&b"three"[..]));
    assert_eq!(store.queue_pop(b"q"), None);
    // Empty/unknown scope.
    assert_eq!(store.queue_pop(b"nope"), None);
}

fn check_clear(store: &dyn FunctionStorage) {
    store.kv_put(b"c", b"k", b"v");
    store.append(b"c", b"ns", b"", b"x".to_vec());
    store.queue_push(b"c", &[b"i".to_vec()]);
    store.clear(b"c");
    assert_eq!(store.kv_get(b"c", b"k"), None);
    assert!(store.scan(b"c", b"ns", b"", -1, usize::MAX).is_empty());
    assert_eq!(store.queue_pop(b"c"), None);
}

fn run_all(store: &dyn FunctionStorage) {
    check_kv(store);
    check_kv_many(store);
    check_log(store);
    check_append_many(store);
    check_queue(store);
    check_clear(store);
}

#[test]
fn memory_backend_conforms() {
    run_all(&MemoryStorage::new());
}

#[test]
fn fs_backend_conforms() {
    let mut base = std::env::temp_dir();
    base.push(format!("vgi-conf-fs-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let store = FsStorage::with_base(base.clone());
    run_all(&store);
    let _ = std::fs::remove_dir_all(&base);
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_backend_conforms() {
    let mut p = std::env::temp_dir();
    p.push(format!("vgi-conf-sqlite-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&p);
    let store = SqliteStorage::open(p.clone());
    run_all(&store);
    let _ = std::fs::remove_file(&p);
}

/// Live checks against a running `vgi-cloudflare-durable-object-storage`
/// Worker. Opt-in: they run only when `VGI_CF_DO_TEST_URL` (and, for the
/// multi-tenant service, `VGI_CF_DO_TEST_TOKEN`) are set, e.g. against a local
/// `npx wrangler dev`; otherwise each test returns early.
#[cfg(feature = "cloudflare-do")]
mod cloudflare_do_live {
    use super::*;
    use crate::storage::cfdo::{build_body, new_attempt_id, shard_key_for_scope, CfDoStorage};
    use serde_json::json;

    fn live() -> Option<CfDoStorage> {
        let url = std::env::var("VGI_CF_DO_TEST_URL")
            .ok()
            .filter(|u| !u.is_empty())?;
        let token = std::env::var("VGI_CF_DO_TEST_TOKEN")
            .ok()
            .filter(|t| !t.is_empty());
        Some(CfDoStorage::new(url, token))
    }

    /// The Durable Object outlives a test run, so every scope is prefixed with
    /// a per-run nonce: the suite's fixed scopes (`e1`, `q`, ...) start empty.
    struct Fresh<S> {
        prefix: Vec<u8>,
        inner: S,
    }

    impl<S> Fresh<S> {
        fn new(inner: S) -> Self {
            Fresh {
                prefix: format!("run-{}/", new_attempt_id()).into_bytes(),
                inner,
            }
        }
        fn s(&self, scope: &[u8]) -> Vec<u8> {
            [&self.prefix[..], scope].concat()
        }
    }

    impl<S: FunctionStorage> FunctionStorage for Fresh<S> {
        fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>> {
            self.inner.kv_get(&self.s(scope), key)
        }
        fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]) {
            self.inner.kv_put(&self.s(scope), key, value)
        }
        fn kv_del(&self, scope: &[u8], key: &[u8]) {
            self.inner.kv_del(&self.s(scope), key)
        }
        fn kv_get_many(&self, scope: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
            self.inner.kv_get_many(&self.s(scope), keys)
        }
        fn kv_put_many(&self, scope: &[u8], items: &[(Vec<u8>, Vec<u8>)]) {
            self.inner.kv_put_many(&self.s(scope), items)
        }
        fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
            self.inner.append(&self.s(scope), ns, key, value)
        }
        fn append_many(
            &self,
            scope: &[u8],
            ns: &[u8],
            key: &[u8],
            values: Vec<Vec<u8>>,
        ) -> Vec<i64> {
            self.inner.append_many(&self.s(scope), ns, key, values)
        }
        fn scan(
            &self,
            scope: &[u8],
            ns: &[u8],
            key: &[u8],
            after_id: i64,
            limit: usize,
        ) -> Vec<(i64, Vec<u8>)> {
            self.inner.scan(&self.s(scope), ns, key, after_id, limit)
        }
        fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]) {
            self.inner.queue_push(&self.s(scope), items)
        }
        fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>> {
            self.inner.queue_pop(&self.s(scope))
        }
        fn clear(&self, scope: &[u8]) {
            self.inner.clear(&self.s(scope))
        }
    }

    #[test]
    fn cloudflare_do_backend_conforms() {
        let Some(store) = live() else {
            eprintln!("skipped: set VGI_CF_DO_TEST_URL to run");
            return;
        };
        run_all(&Fresh::new(store));
    }

    #[test]
    fn cloudflare_do_replays_a_retried_append_and_pop() {
        let Some(store) = live() else { return };
        let scope = format!("replay-{}", new_attempt_id()).into_bytes();
        let shard = shard_key_for_scope(&scope);
        let attempt = new_attempt_id();
        let append = || {
            let body = build_body(
                json!({
                    "scope_id": B64.encode(&scope),
                    "ns": B64.encode(b"ns"),
                    "key": B64.encode(b"k"),
                    "item": B64.encode(b"once"),
                })
                .as_object()
                .unwrap()
                .clone(),
                &shard,
                Some(&attempt),
            );
            store.post_body("state_append", &body).unwrap()["ordinal"]
                .as_i64()
                .unwrap()
        };
        // A lost-response retry (same attempt_id) returns the first ordinal and
        // writes no second row.
        let first = append();
        assert_eq!(append(), first);
        assert_eq!(store.scan(&scope, b"ns", b"k", -1, usize::MAX).len(), 1);

        store.queue_push(&scope, &[b"a".to_vec(), b"b".to_vec()]);
        let pop_attempt = new_attempt_id();
        let pop = || {
            let body = build_body(
                json!({ "execution_id": B64.encode(&scope) })
                    .as_object()
                    .unwrap()
                    .clone(),
                &shard,
                Some(&pop_attempt),
            );
            store.post_body("queue_pop", &body).unwrap()["item"].clone()
        };
        let item = pop();
        assert_eq!(pop(), item, "a replayed pop returns the same item");
        assert_eq!(store.queue_pop(&scope).as_deref(), Some(&b"b"[..]));
        assert_eq!(store.queue_pop(&scope), None);
        store.clear(&scope);
        assert!(store.scan(&scope, b"ns", b"k", -1, usize::MAX).is_empty());
    }

    #[test]
    fn cloudflare_do_concurrent_poppers_claim_each_item_once() {
        let Some(store) = live() else { return };
        let store = std::sync::Arc::new(store);
        let scope = format!("pop-{}", new_attempt_id()).into_bytes();
        let items: Vec<Vec<u8>> = (0..40).map(|i| format!("{i}").into_bytes()).collect();
        store.queue_push(&scope, &items);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let (store, scope) = (store.clone(), scope.clone());
                std::thread::spawn(move || {
                    let mut got = Vec::new();
                    while let Some(i) = store.queue_pop(&scope) {
                        got.push(i);
                    }
                    got
                })
            })
            .collect();
        let mut all: Vec<Vec<u8>> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        all.sort();
        let mut want = items;
        want.sort();
        assert_eq!(all, want);
        store.clear(&scope);
    }

    #[test]
    fn cloudflare_do_large_values_round_trip_and_oversize_is_refused() {
        let Some(store) = live() else { return };
        let scope = format!("big-{}", new_attempt_id()).into_bytes();
        let big: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
        store.kv_put(&scope, b"k", &big);
        assert_eq!(store.kv_get(&scope, b"k").as_deref(), Some(&big[..]));
        let too_big = vec![0u8; 1024 * 1024 + 1];
        let err = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.kv_put(&scope, b"k2", &too_big)
        }))
        .expect_err("a value over 1 MiB is refused");
        let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(msg.contains("413"), "{msg}");
        store.clear(&scope);
        assert_eq!(store.kv_get(&scope, b"k"), None);
    }

    /// Batches past one request's bounds: more keys than one
    /// `state_get_many` / `state_put_many` carries, and values whose total
    /// exceeds the server's 8 MiB read budget (the 413 is retried in halves).
    #[test]
    fn cloudflare_do_kv_many_chunks_and_bisects() {
        let Some(store) = live() else { return };
        let store = Fresh::new(store);
        let keys: Vec<Vec<u8>> = (0u32..2500).map(|i| i.to_be_bytes().to_vec()).collect();
        let items: Vec<(Vec<u8>, Vec<u8>)> = keys
            .iter()
            .map(|k| (k.clone(), [k.as_slice(), b"-v"].concat()))
            .collect();
        store.kv_put_many(b"many", &items);
        let got = store.kv_get_many(b"many", &keys);
        assert_eq!(got.len(), keys.len());
        for ((_, v), g) in items.iter().zip(&got) {
            assert_eq!(g.as_ref(), Some(v));
        }

        let big: Vec<(Vec<u8>, Vec<u8>)> = (0u8..12)
            .map(|i| (vec![b'b', i], vec![i; 900 * 1024]))
            .collect();
        store.kv_put_many(b"big", &big);
        let bk: Vec<Vec<u8>> = big.iter().map(|(k, _)| k.clone()).collect();
        let got = store.kv_get_many(b"big", &bk);
        for ((_, v), g) in big.iter().zip(&got) {
            assert_eq!(g.as_ref(), Some(v));
        }
        store.clear(b"many");
        store.clear(b"big");
    }

    /// A sealed framework record round-trips through the service and does not
    /// open once moved to another execution's slot.
    #[test]
    fn cloudflare_do_sealed_record_round_trips() {
        let Some(store) = live() else { return };
        let store = Fresh::new(store);
        let k = crate::storage::seal::derive_key(b"live-test-key");
        crate::storage::seal::sealed_kv_put(&store, &k, b"exec-a", b"_vgi/x", b"context");
        assert_eq!(
            crate::storage::seal::sealed_kv_get(&store, &k, b"exec-a", b"_vgi/x").unwrap(),
            Some(b"context".to_vec())
        );
        let blob = store.kv_get(b"exec-a", b"_vgi/x").unwrap();
        store.kv_put(b"exec-b", b"_vgi/x", &blob);
        assert!(crate::storage::seal::sealed_kv_get(&store, &k, b"exec-b", b"_vgi/x").is_err());
        store.clear(b"exec-a");
        store.clear(b"exec-b");
    }

    #[test]
    fn cloudflare_do_bad_token_is_a_definitive_error() {
        let Ok(url) = std::env::var("VGI_CF_DO_TEST_URL") else {
            return;
        };
        let store = CfDoStorage::new(url, Some("vgi_nobody_not-a-real-key".into()));
        let err =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.kv_get(b"s", b"k")))
                .expect_err("an unknown key is refused");
        let msg = err.downcast_ref::<String>().cloned().unwrap_or_default();
        assert!(msg.contains("authentication failed"), "{msg}");
        assert!(
            !msg.contains("not-a-real-key"),
            "the token never leaks: {msg}"
        );
    }

    /// Cross-SDK check, driven by an external harness:
    /// with `VGI_CF_DO_XSDK_SCOPE` set, read what another SDK wrote under that
    /// scope's shard (`kv` = `py-value`, a two-entry log, a queue item), then
    /// write Rust's own values for the other SDK to read back.
    #[test]
    fn cloudflare_do_cross_sdk_exchange() {
        let (Some(store), Ok(scope)) = (live(), std::env::var("VGI_CF_DO_XSDK_SCOPE")) else {
            return;
        };
        let scope = scope.into_bytes();
        assert_eq!(
            store.kv_get(&scope, b"from-py").as_deref(),
            Some(&b"py-value"[..])
        );
        let log = store.scan(&scope, b"log", b"k", -1, usize::MAX);
        assert_eq!(
            log.iter().map(|(_, v)| v.clone()).collect::<Vec<_>>(),
            vec![b"py-1".to_vec(), b"py-2".to_vec()]
        );
        assert_eq!(store.queue_pop(&scope).as_deref(), Some(&b"py-item"[..]));
        store.kv_put(&scope, b"from-rust", b"rust-value");
        store.append(&scope, b"log", b"k", b"rust-3".to_vec());
        store.queue_push(&scope, &[b"rust-item".to_vec()]);
    }

    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine as _;
}

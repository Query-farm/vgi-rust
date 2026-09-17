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

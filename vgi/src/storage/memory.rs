// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! In-process [`FunctionStorage`] backed by `BTreeMap`s under a single `Mutex`.
//! `BTreeMap` gives byte-lexicographic key ordering for free. For tests and
//! single-process use; state dies with the process.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Mutex;

use super::FunctionStorage;

type Scope = Vec<u8>;
type Key = Vec<u8>;
type Ns = Vec<u8>;

#[derive(Default)]
struct Inner {
    kv: BTreeMap<(Scope, Key), Vec<u8>>,
    log: BTreeMap<(Scope, Ns, Key), Vec<(i64, Vec<u8>)>>,
    queue: BTreeMap<Scope, VecDeque<Vec<u8>>>,
    next_id: i64,
}

pub struct MemoryStorage {
    inner: Mutex<Inner>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        MemoryStorage::new()
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            inner: Mutex::new(Inner::default()),
        }
    }
}

impl FunctionStorage for MemoryStorage {
    fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        let g = self.inner.lock().unwrap();
        g.kv.get(&(scope.to_vec(), key.to_vec())).cloned()
    }

    fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]) {
        let mut g = self.inner.lock().unwrap();
        g.kv.insert((scope.to_vec(), key.to_vec()), value.to_vec());
    }

    fn kv_del(&self, scope: &[u8], key: &[u8]) {
        let mut g = self.inner.lock().unwrap();
        g.kv.remove(&(scope.to_vec(), key.to_vec()));
    }

    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
        let mut g = self.inner.lock().unwrap();
        let id = g.next_id;
        g.next_id += 1;
        g.log
            .entry((scope.to_vec(), ns.to_vec(), key.to_vec()))
            .or_default()
            .push((id, value));
        id
    }

    fn scan(
        &self,
        scope: &[u8],
        ns: &[u8],
        key: &[u8],
        after_id: i64,
        limit: usize,
    ) -> Vec<(i64, Vec<u8>)> {
        let g = self.inner.lock().unwrap();
        match g.log.get(&(scope.to_vec(), ns.to_vec(), key.to_vec())) {
            Some(entries) => entries
                .iter()
                .filter(|(id, _)| *id > after_id)
                .take(limit)
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]) {
        let mut g = self.inner.lock().unwrap();
        let q = g.queue.entry(scope.to_vec()).or_default();
        for item in items {
            q.push_back(item.clone());
        }
    }

    fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>> {
        let mut g = self.inner.lock().unwrap();
        g.queue.get_mut(scope).and_then(|q| q.pop_front())
    }

    fn clear(&self, scope: &[u8]) {
        let mut g = self.inner.lock().unwrap();
        let s = scope.to_vec();
        g.kv.retain(|(sc, _), _| sc != &s);
        g.log.retain(|(sc, _, _), _| sc != &s);
        g.queue.remove(&s);
    }
}

// Copyright 2026 Query Farm LLC - https://query.farm

use super::*;
use crate::storage::MemoryStorage;
use arrow_array::StringArray;
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
struct CountingStorage {
    inner: MemoryStorage,
    reads: AtomicUsize,
}

impl FunctionStorage for CountingStorage {
    fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.kv_get(scope, key)
    }
    fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]) {
        self.inner.kv_put(scope, key, value)
    }
    fn kv_del(&self, scope: &[u8], key: &[u8]) {
        self.inner.kv_del(scope, key)
    }
    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
        self.inner.append(scope, ns, key, value)
    }
    fn scan(
        &self,
        scope: &[u8],
        ns: &[u8],
        key: &[u8],
        after: i64,
        limit: usize,
    ) -> Vec<(i64, Vec<u8>)> {
        self.inner.scan(scope, ns, key, after, limit)
    }
    fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]) {
        self.inner.queue_push(scope, items)
    }
    fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>> {
        self.inner.queue_pop(scope)
    }
    fn clear(&self, scope: &[u8]) {
        self.inner.clear(scope)
    }
}

fn request<T: VgiArrow>(dto: T) -> Request {
    let bytes = ipc::write_batch(&wire::to_batch(dto).unwrap()).unwrap();
    Request {
        method: String::new(),
        protocol: String::new(),
        request_id: String::new(),
        batch: RecordBatch::try_from_iter([(
            "request",
            Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())])) as ArrayRef,
        )])
        .unwrap(),
        metadata: Arc::new(Default::default()),
    }
}

fn dispatcher(store: &Arc<CountingStorage>, max_entries: usize, max_bytes: usize) -> Dispatcher {
    let mut dispatcher = Dispatcher::new("test");
    dispatcher.store = store.clone();
    dispatcher.window_partitions = WindowPartitionCache::new(max_entries, max_bytes);
    dispatcher
}

fn init(dispatcher: &Dispatcher, exec: &[u8], id: i64, mask: Option<Vec<u8>>) {
    let partition = RecordBatch::try_from_iter([(
        "value",
        Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])) as ArrayRef,
    )])
    .unwrap();
    dispatcher
        .handle_aggregate_window_init(&request(AggregateWindowInitRequest {
            function_name: "test".into(),
            execution_id: Bytes::from(exec.to_vec()),
            partition_id: id,
            row_count: 3,
            partition_batch: Bytes::from(ipc::write_batch(&partition).unwrap()),
            output_schema: Bytes::from(ipc::write_schema_ref(&partition.schema()).unwrap()),
            filter_mask: mask.map(Bytes::from),
            frame_stats: None,
            all_valid: None,
            schema_path: None,
        }))
        .unwrap();
}

#[test]
fn init_reuses_decoded_buffers_and_other_workers_fill_from_storage_once() {
    let store = Arc::new(CountingStorage::default());
    let first = dispatcher(&store, 2, usize::MAX);
    let second = dispatcher(&store, 2, usize::MAX);
    init(&first, b"exec", 1, Some(vec![0b101]));
    let a = first.load_window_partition(b"exec", 1).unwrap();
    let b = first.load_window_partition(b"exec", 1).unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    assert_eq!(store.reads.load(Ordering::Relaxed), 0);
    assert_eq!(a.mask, Some(vec![true, false, true]));
    assert!(a.partition.column(0).is_null(1));
    let c = second.load_window_partition(b"exec", 1).unwrap();
    assert_eq!(a.partition, c.partition);
    assert_eq!(a.output_schema, c.output_schema);
    assert_eq!(a.mask, c.mask);
    assert!(Arc::ptr_eq(
        &c,
        &second.load_window_partition(b"exec", 1).unwrap()
    ));
    assert_eq!(store.reads.load(Ordering::Relaxed), 3);
}

#[test]
fn eviction_and_disabled_cache_preserve_storage_fallback() {
    let store = Arc::new(CountingStorage::default());
    let first = dispatcher(&store, 1, usize::MAX);
    init(&first, b"exec", 1, None);
    init(&first, b"exec", 2, None);
    store.reads.store(0, Ordering::Relaxed);
    assert!(first.window_partitions.lookup(b"exec", 1).0.is_none());
    assert_eq!(
        first
            .load_window_partition(b"exec", 1)
            .unwrap()
            .partition
            .num_rows(),
        3
    );
    assert_eq!(store.reads.load(Ordering::Relaxed), 4); // one liveness check + the miss
    let disabled = dispatcher(&store, 256, 0);
    let a = disabled.load_window_partition(b"exec", 1).unwrap();
    let b = disabled.load_window_partition(b"exec", 1).unwrap();
    assert!(!Arc::ptr_eq(&a, &b));
    assert_eq!(a.partition, b.partition);
    assert_eq!(store.reads.load(Ordering::Relaxed), 10);
}

#[test]
fn reinit_and_destructors_release_partitions_and_masks() {
    let store = Arc::new(CountingStorage::default());
    let first = dispatcher(&store, 256, usize::MAX);
    init(&first, b"exec", 1, Some(vec![0]));
    let old = Arc::downgrade(&first.load_window_partition(b"exec", 1).unwrap());
    init(&first, b"exec", 1, None);
    assert!(old.upgrade().is_none());
    let new = first.load_window_partition(b"exec", 1).unwrap();
    assert!(new.mask.is_none());
    assert!(store
        .inner
        .kv_get(b"exec", &Dispatcher::win_key(1, "m"))
        .is_none());
    let retained = Arc::downgrade(&new);
    drop(new);
    first
        .handle_aggregate_window_destructor(&request(AggregateWindowDestructorRequest {
            function_name: "test".into(),
            execution_id: Bytes::from(b"exec".to_vec()),
            partition_id: 1,
            schema_path: None,
        }))
        .unwrap();
    assert!(retained.upgrade().is_none());
    assert!(first.load_window_partition(b"exec", 1).is_err());
    init(&first, b"exec", 2, None);
    init(&first, b"other", 2, None);
    let retained = Arc::downgrade(&first.load_window_partition(b"exec", 2).unwrap());
    first
        .handle_aggregate_destructor(&request(AggregateDestructorRequest {
            function_name: "test".into(),
            execution_id: Bytes::from(b"exec".to_vec()),
            group_ids_batch: Bytes::from(Vec::new()),
            attach_opaque_data: None,
            schema_path: None,
        }))
        .unwrap();
    assert!(retained.upgrade().is_none());
    assert!(first.load_window_partition(b"exec", 2).is_err());
    assert!(first.load_window_partition(b"other", 2).is_ok());
}

#[test]
fn another_workers_destructor_is_observed_on_next_admission() {
    let store = Arc::new(CountingStorage::default());
    let first = dispatcher(&store, 256, usize::MAX);
    let second = dispatcher(&store, 256, usize::MAX);
    init(&first, b"exec", 1, None);
    let cached = second.load_window_partition(b"exec", 1).unwrap();
    let old = Arc::downgrade(&cached);
    drop(cached);
    first
        .handle_aggregate_window_destructor(&request(AggregateWindowDestructorRequest {
            function_name: "test".into(),
            execution_id: Bytes::from(b"exec".to_vec()),
            partition_id: 1,
            schema_path: None,
        }))
        .unwrap();
    // Other processes retain copies until their next admission or LRU eviction.
    assert!(old.upgrade().is_some());
    init(&second, b"next-execution", 2, None);
    assert!(old.upgrade().is_none());
    assert!(second.load_window_partition(b"next-execution", 2).is_ok());
}

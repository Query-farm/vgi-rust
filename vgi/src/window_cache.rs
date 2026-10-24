// Copyright 2026 Query Farm LLC - https://query.farm

//! Decoded window partitions. Durable FunctionStorage remains authoritative on
//! a miss; this cache avoids reading/validating the same Arrow buffers per row.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_schema::SchemaRef;

const DEFAULT_MAX_BYTES: usize = 256 * 1024 * 1024;
const MAX_ENTRIES: usize = 256;
type Key = (Vec<u8>, i64);

pub(crate) struct CachedWindowPartition {
    pub partition: RecordBatch,
    pub output_schema: SchemaRef,
    pub mask: Option<Vec<bool>>,
}

impl CachedWindowPartition {
    fn accounted_bytes(&self) -> usize {
        // IPC offsets, values and validity often share a single allocation.
        // Array::get_array_memory_size counts that allocation once per buffer
        // view, so a 128 MiB string partition can appear to exceed 256 MiB.
        // Deduplicate allocation addresses within a partition; slices still
        // charge the entire backing capacity, not just their visible bytes.
        let mut allocations = HashMap::new();
        let mut arrays = std::mem::size_of::<RecordBatch>();
        for array in self.partition.columns() {
            arrays = arrays.saturating_add(array_bytes(&array.to_data(), &mut allocations));
        }
        allocations
            .values()
            .fold(arrays, |sum, bytes| sum.saturating_add(*bytes))
            .saturating_add(schema_bytes(&self.output_schema))
            .saturating_add(schema_bytes(&self.partition.schema()))
            .saturating_add(self.mask.as_ref().map_or(0, Vec::capacity))
            .saturating_add(std::mem::size_of::<Self>())
    }
}

fn buffer_bytes(buffer: &Buffer, allocations: &mut HashMap<usize, usize>) {
    // capacity is zero for foreign-owned buffers; use their known extent.
    // The IPC decoder normally supplies owned buffers with full capacities.
    let bytes = buffer
        .capacity()
        .max(buffer.ptr_offset().saturating_add(buffer.len()));
    let allocated = allocations
        .entry(buffer.data_ptr().as_ptr() as usize)
        .or_default();
    *allocated = (*allocated).max(bytes);
}

fn array_bytes(data: &ArrayData, allocations: &mut HashMap<usize, usize>) -> usize {
    let mut size = std::mem::size_of::<ArrayData>().saturating_add(
        data.buffers()
            .len()
            .saturating_mul(std::mem::size_of::<Buffer>()),
    );
    for buffer in data.buffers() {
        buffer_bytes(buffer, allocations);
    }
    if let Some(nulls) = data.nulls() {
        buffer_bytes(nulls.buffer(), allocations);
    }
    for child in data.child_data() {
        size = size.saturating_add(array_bytes(child, allocations));
    }
    size
}

fn schema_bytes(schema: &SchemaRef) -> usize {
    let metadata = schema.metadata();
    metadata.iter().fold(
        std::mem::size_of::<arrow_schema::Schema>()
            .saturating_add(schema.fields().size())
            .saturating_add(
                metadata
                    .len()
                    .saturating_mul(2 * std::mem::size_of::<String>()),
            ),
        |size, (key, value)| {
            size.saturating_add(key.capacity())
                .saturating_add(value.capacity())
        },
    )
}

#[derive(Default)]
struct Inner {
    entries: HashMap<Key, (Arc<CachedWindowPartition>, usize)>,
    order: VecDeque<Key>,
    bytes: usize,
    // Invalidates in-flight cache fills after destruction/reinitialization.
    // A global generation avoids retaining unbounded per-execution tombstones.
    generation: u64,
}

pub(crate) struct WindowPartitionCache {
    inner: Mutex<Inner>,
    max_bytes: usize,
    max_entries: usize,
}

impl WindowPartitionCache {
    pub fn from_env() -> Self {
        let bytes = match std::env::var("VGI_WINDOW_CACHE_MAX_BYTES") {
            Ok(value) => value.parse().unwrap_or_else(|_| {
                log::warn!("invalid VGI_WINDOW_CACHE_MAX_BYTES; using {DEFAULT_MAX_BYTES}");
                DEFAULT_MAX_BYTES
            }),
            Err(_) => DEFAULT_MAX_BYTES,
        };
        Self::new(MAX_ENTRIES, bytes)
    }

    pub fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            max_bytes,
            max_entries,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn lookup(&self, exec: &[u8], partition: i64) -> (Option<Arc<CachedWindowPartition>>, u64) {
        let mut inner = self.lock();
        let key = (exec.to_vec(), partition);
        let entry = inner.entries.get(&key).map(|(entry, _)| Arc::clone(entry));
        if entry.is_some() {
            inner.order.retain(|k| k != &key);
            inner.order.push_back(key);
        }
        (entry, inner.generation)
    }

    // Storage reads/Arrow decoding happen outside the lock. A destructor that
    // completes during that work must prevent the old partition being retained.
    pub fn insert_if_current(
        &self,
        exec: &[u8],
        partition: i64,
        entry: Arc<CachedWindowPartition>,
        generation: u64,
    ) {
        let bytes = entry.accounted_bytes().saturating_add(exec.len());
        let mut inner = self.lock();
        if generation != inner.generation || self.max_entries == 0 || bytes > self.max_bytes {
            return;
        }
        let key = (exec.to_vec(), partition);
        Self::remove_key(&mut inner, &key);
        while inner.entries.len() >= self.max_entries
            || bytes > self.max_bytes.saturating_sub(inner.bytes)
        {
            let Some(oldest) = inner.order.pop_front() else {
                break;
            };
            if let Some((_, old_bytes)) = inner.entries.remove(&oldest) {
                inner.bytes -= old_bytes;
            }
        }
        inner.bytes += bytes;
        inner.order.push_back(key.clone());
        inner.entries.insert(key, (entry, bytes));
    }

    fn remove_key(inner: &mut Inner, key: &Key) {
        if let Some((_, bytes)) = inner.entries.remove(key) {
            inner.bytes -= bytes;
        }
        inner.order.retain(|k| k != key);
    }

    pub fn remove(&self, exec: &[u8], partition: i64) -> u64 {
        let mut inner = self.lock();
        inner.generation = inner.generation.wrapping_add(1);
        Self::remove_key(&mut inner, &(exec.to_vec(), partition));
        inner.generation
    }

    pub fn clear_execution(&self, exec: &[u8]) {
        let mut inner = self.lock();
        inner.generation = inner.generation.wrapping_add(1);
        let keys: Vec<_> = inner
            .entries
            .keys()
            .filter(|(e, _)| e == exec)
            .cloned()
            .collect();
        for key in keys {
            Self::remove_key(&mut inner, &key);
        }
    }

    /// Check shared-storage liveness without holding the cache lock. A pooled
    /// worker may have received the destructor for an entry decoded here.
    pub fn prune(&self, is_live: impl Fn(&[u8], i64) -> bool) {
        let entries: Vec<_> = self
            .lock()
            .entries
            .iter()
            .map(|(key, (entry, _))| (key.clone(), Arc::downgrade(entry)))
            .collect();
        for (key, entry) in entries {
            if is_live(&key.0, key.1) {
                continue;
            }
            let mut inner = self.lock();
            // A concurrent initialization may have replaced the same key.
            if inner.entries.get(&key).is_some_and(|(current, _)| {
                std::sync::Weak::ptr_eq(&entry, &Arc::downgrade(current))
            }) {
                inner.generation = inner.generation.wrapping_add(1);
                Self::remove_key(&mut inner, &key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;

    fn entry() -> Arc<CachedWindowPartition> {
        let partition = RecordBatch::try_from_iter([(
            "s",
            Arc::new(StringArray::from(vec![Some("payload"), None])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        Arc::new(CachedWindowPartition {
            output_schema: partition.schema(),
            partition,
            mask: Some(vec![true, false]),
        })
    }

    fn put(cache: &WindowPartitionCache, exec: &[u8], id: i64, entry: Arc<CachedWindowPartition>) {
        let (_, generation) = cache.lookup(exec, id);
        cache.insert_if_current(exec, id, entry, generation);
    }

    #[test]
    fn lru_shares_buffers_and_preserves_execution_isolation() {
        let cache = WindowPartitionCache::new(2, usize::MAX);
        let a = entry();
        put(&cache, b"a", 1, Arc::clone(&a));
        put(&cache, b"b", 1, entry());
        assert!(Arc::ptr_eq(&a, &cache.lookup(b"a", 1).0.unwrap()));
        put(&cache, b"c", 1, entry());
        assert!(cache.lookup(b"b", 1).0.is_none());
        assert!(cache.lookup(b"a", 1).0.is_some());
        cache.clear_execution(b"a");
        assert!(cache.lookup(b"a", 1).0.is_none());
        assert!(cache.lookup(b"c", 1).0.is_some());
    }

    #[test]
    fn byte_limit_evicts_and_oversized_entries_do_not_displace_resident_entries() {
        let a = entry();
        let bytes = a.accounted_bytes() + 1;
        let cache = WindowPartitionCache::new(256, bytes);
        put(&cache, b"a", 1, Arc::clone(&a));
        put(&cache, b"b", 1, Arc::clone(&a));
        assert!(cache.lookup(b"a", 1).0.is_none());
        put(&cache, b"oversized-key", 1, a);
        assert!(cache.lookup(b"b", 1).0.is_some());
        assert!(cache.lookup(b"oversized-key", 1).0.is_none());
        cache.remove(b"b", 1);
        assert_eq!(cache.lock().bytes, 0);
    }

    #[test]
    fn invalidation_rejects_in_flight_fills() {
        let cache = WindowPartitionCache::new(256, usize::MAX);
        let (_, generation) = cache.lookup(b"a", 1);
        cache.remove(b"a", 1);
        cache.insert_if_current(b"a", 1, entry(), generation);
        assert!(cache.lookup(b"a", 1).0.is_none());
        let (_, generation) = cache.lookup(b"a", 1);
        cache.clear_execution(b"a");
        cache.insert_if_current(b"a", 1, entry(), generation);
        assert!(cache.lookup(b"a", 1).0.is_none());
    }

    #[test]
    fn zero_budget_disables_retention() {
        let cache = WindowPartitionCache::new(256, 0);
        put(&cache, b"a", 1, entry());
        assert!(cache.lookup(b"a", 1).0.is_none());
    }

    #[test]
    fn pruning_drops_dead_entries_without_holding_locks_or_removing_replacements() {
        let cache = WindowPartitionCache::new(256, usize::MAX);
        let a = entry();
        let weak = Arc::downgrade(&a);
        put(&cache, b"a", 1, a);
        put(&cache, b"b", 1, entry());
        cache.prune(|exec, _| exec == b"b");
        assert!(weak.upgrade().is_none());
        cache.prune(|exec, partition| {
            // This would deadlock if the predicate ran under the cache lock.
            // Its stale answer must not evict the replacement.
            cache.remove(exec, partition);
            put(&cache, exec, partition, entry());
            false
        });
        assert!(cache.lookup(b"b", 1).0.is_some());
    }

    #[test]
    fn ipc_shared_buffers_count_once_but_slices_charge_the_whole_allocation() {
        let payload = "x".repeat(1024 * 1024);
        let batch = RecordBatch::try_from_iter([(
            "s",
            Arc::new(StringArray::from(vec![
                Some(payload.as_str()),
                None,
                Some("tail"),
            ])) as arrow_array::ArrayRef,
        )])
        .unwrap();
        let bytes = crate::ipc::write_batch(&batch).unwrap();
        let partition = crate::ipc::read_batch(&bytes).unwrap();
        let entry = Arc::new(CachedWindowPartition {
            output_schema: partition.schema(),
            partition,
            mask: None,
        });
        let budget = bytes.len() + 8192;
        // Arrow's aggregate estimate repeats the shared IPC body capacity.
        assert!(entry.partition.get_array_memory_size() > budget);
        assert!(entry.accounted_bytes() <= budget);
        assert!(entry.accounted_bytes() >= payload.len());
        let cache = WindowPartitionCache::new(256, budget);
        put(&cache, b"a", 1, Arc::clone(&entry));
        assert!(cache.lookup(b"a", 1).0.is_some());

        let sliced = CachedWindowPartition {
            partition: entry.partition.slice(2, 1),
            output_schema: entry.output_schema.clone(),
            mask: None,
        };
        assert!(sliced.accounted_bytes() >= payload.len());
    }
}

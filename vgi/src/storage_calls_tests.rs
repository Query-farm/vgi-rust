// Copyright 2026 Query Farm LLC - https://query.farm

//! Storage round trips and sealing of the framework's own context.
//!
//! A remote backend (`http`, `cloudflare-do`) pays a network round trip per
//! [`FunctionStorage`] call, so these tests count calls through a wrapping
//! backend and pin the budget per RPC: a buffered `process` / `combine` costs
//! one framework read per execution per worker process and none after, and an
//! aggregate `update` costs two calls per batch whatever its group count.

use std::collections::HashMap as Map;
use std::sync::Mutex;

use super::*;
use crate::framework_state::{BufferingInit, AGGREGATE_ARGS_KEY, BUFFERING_INIT_KEY};
use crate::storage::{seal, MemoryStorage};
use arrow_schema::{DataType, Field, Schema};

/// Counts every trait call by method name (a batched call counts once).
#[derive(Default)]
struct Counting {
    inner: MemoryStorage,
    calls: Mutex<Map<&'static str, usize>>,
}

impl Counting {
    fn hit(&self, m: &'static str) {
        *self.calls.lock().unwrap().entry(m).or_default() += 1;
    }
    fn total(&self) -> usize {
        self.calls.lock().unwrap().values().sum()
    }
    fn get(&self, m: &str) -> usize {
        self.calls.lock().unwrap().get(m).copied().unwrap_or(0)
    }
    fn reset(&self) {
        self.calls.lock().unwrap().clear();
    }
}

impl FunctionStorage for Counting {
    fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        self.hit("kv_get");
        self.inner.kv_get(scope, key)
    }
    fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]) {
        self.hit("kv_put");
        self.inner.kv_put(scope, key, value)
    }
    fn kv_del(&self, scope: &[u8], key: &[u8]) {
        self.hit("kv_del");
        self.inner.kv_del(scope, key)
    }
    fn kv_get_many(&self, scope: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        self.hit("kv_get_many");
        self.inner.kv_get_many(scope, keys)
    }
    fn kv_put_many(&self, scope: &[u8], items: &[(Vec<u8>, Vec<u8>)]) {
        self.hit("kv_put_many");
        self.inner.kv_put_many(scope, items)
    }
    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
        self.hit("append");
        self.inner.append(scope, ns, key, value)
    }
    fn append_many(&self, scope: &[u8], ns: &[u8], key: &[u8], values: Vec<Vec<u8>>) -> Vec<i64> {
        self.hit("append_many");
        self.inner.append_many(scope, ns, key, values)
    }
    fn scan(
        &self,
        scope: &[u8],
        ns: &[u8],
        key: &[u8],
        after: i64,
        limit: usize,
    ) -> Vec<(i64, Vec<u8>)> {
        self.hit("scan");
        self.inner.scan(scope, ns, key, after, limit)
    }
    fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]) {
        self.hit("queue_push");
        self.inner.queue_push(scope, items)
    }
    fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>> {
        self.hit("queue_pop");
        self.inner.queue_pop(scope)
    }
    fn clear(&self, scope: &[u8]) {
        self.hit("clear");
        self.inner.clear(scope)
    }
}

const SIGNING_KEY: &[u8] = b"storage-calls-test-signing-key";

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

/// A worker process: its own caches, a shared backend.
fn worker(store: Arc<dyn FunctionStorage>) -> Dispatcher {
    let mut d = Dispatcher::new("test");
    d.store = store;
    d.storage_seal_root = Some(SIGNING_KEY.to_vec());
    d
}

/// What a buffered function's process/combine saw, per call.
#[derive(Default, Clone, Debug)]
struct Seen {
    attach: Option<Vec<u8>>,
    copy_to: Option<String>,
    secret_names: Vec<String>,
    output: Option<SchemaRef>,
}

/// A buffered function that records the params it was handed.
struct Recorder {
    tag: &'static str,
    seen: Arc<Mutex<Vec<(&'static str, Seen)>>>,
}

impl crate::buffering::TableBufferingFunction for Recorder {
    fn name(&self) -> &str {
        "recorder"
    }
    fn metadata(&self) -> crate::function::FunctionMetadata {
        Default::default()
    }
    fn argument_specs(&self) -> Vec<crate::function::ArgSpec> {
        vec![]
    }
    fn on_bind(&self, _p: &BindParams) -> Result<crate::function::BindResponse> {
        unreachable!("process/combine never rebind")
    }
    fn process(&self, p: &BufferingParams, _b: &RecordBatch) -> Result<Vec<u8>> {
        self.record(p);
        Ok(b"state".to_vec())
    }
    fn combine(&self, p: &BufferingParams, s: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
        self.record(p);
        Ok(s.to_vec())
    }
    fn finalize_producer(
        &self,
        _p: &BufferingParams,
        _f: Vec<u8>,
    ) -> Result<Box<dyn crate::table_function::TableProducer>> {
        unimplemented!()
    }
}

impl Recorder {
    fn record(&self, p: &BufferingParams) {
        let mut secret_names: Vec<String> = p.secrets.iter().map(|(n, _)| n.to_string()).collect();
        secret_names.sort();
        self.seen.lock().unwrap().push((
            self.tag,
            Seen {
                attach: p.attach_opaque_data.clone(),
                copy_to: p.copy_to.as_ref().map(|c| c.file_path.clone()),
                secret_names,
                output: Some(p.output_schema.clone()),
            },
        ));
    }
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("s", DataType::Float64, true)]))
}

/// A one-row secrets IPC blob: secret `api` with field `token`.
fn secrets_blob(token: &str) -> Vec<u8> {
    let token: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![token]));
    let api = arrow_array::StructArray::from(vec![(
        Arc::new(Field::new("token", DataType::Utf8, true)),
        token,
    )]);
    let batch = RecordBatch::try_from_iter([("api", Arc::new(api) as ArrayRef)]).unwrap();
    ipc::write_batch(&batch).unwrap()
}

fn init_record(copy_to: Option<&str>, secret: Option<&str>) -> BufferingInit {
    BufferingInit {
        output_schema: output_schema(),
        input_schema: Some(Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Int64,
            true,
        )]))),
        arguments: Vec::new(),
        secrets: secret.map(secrets_blob),
        logging: false,
        copy_to: copy_to.map(|p| CopyToContext {
            format: "fmt".into(),
            file_path: p.into(),
        }),
        attach_opaque_data: Some(b"init-attach".to_vec()),
        schema_path: Some(vec!["s1".into()]),
    }
}

fn batch() -> Vec<u8> {
    let b =
        RecordBatch::try_from_iter([("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)])
            .unwrap();
    ipc::write_batch(&b).unwrap()
}

fn process_req(
    exec: &[u8],
    attach: Option<&[u8]>,
    schema: Option<&str>,
) -> TableBufferingProcessRequest {
    TableBufferingProcessRequest {
        function_name: "recorder".into(),
        execution_id: Bytes::from(exec.to_vec()),
        input_batch: Bytes::from(batch()),
        attach_opaque_data: attach.map(|a| Bytes::from(a.to_vec())),
        transaction_id: None,
        batch_index: None,
        schema_path: schema.map(|s| vec![s.to_string()]),
    }
}

fn combine_req(exec: &[u8]) -> TableBufferingCombineRequest {
    TableBufferingCombineRequest {
        function_name: "recorder".into(),
        execution_id: Bytes::from(exec.to_vec()),
        state_ids: vec![Bytes::from(b"state".to_vec())],
        attach_opaque_data: None,
        transaction_id: None,
        schema_path: None,
    }
}

/// Register `Recorder`s in schemas `s1` and `s2` of catalog `test`.
fn with_recorders(d: &mut Dispatcher) -> Arc<Mutex<Vec<(&'static str, Seen)>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    for (tag, schema) in [("s1", "s1"), ("s2", "s2")] {
        d.register_buffering_scoped(
            Arc::new(Recorder {
                tag,
                seen: seen.clone(),
            }),
            FunctionScope::new("test", schema),
        );
    }
    seen
}

#[test]
fn buffered_process_reads_storage_once_per_worker_then_never() {
    let store = Arc::new(Counting::default());
    let exec = b"exec-calls".to_vec();

    // The sink init: ONE sealed write (was up to nine keys).
    let sink = worker(store.clone());
    sink.persist_buffering_init(&exec, init_record(Some("/out/file"), Some("tok")))
        .unwrap();
    assert_eq!(store.total(), 1);
    assert_eq!(store.get("kv_put"), 1);

    // The sink's own process calls: zero storage calls (was 7 per batch).
    let mut sink = sink;
    with_recorders(&mut sink);
    store.reset();
    for _ in 0..4 {
        sink.buffering_process(process_req(&exec, Some(b"a"), None))
            .unwrap();
    }
    sink.buffering_combine(combine_req(&exec)).unwrap();
    assert_eq!(store.total(), 0, "{:?}", store.calls.lock().unwrap());

    // Another pooled worker: one read on its first call, then none.
    let mut other = worker(store.clone());
    let seen = with_recorders(&mut other);
    store.reset();
    for _ in 0..4 {
        other
            .buffering_process(process_req(&exec, Some(b"a"), None))
            .unwrap();
    }
    other.buffering_combine(combine_req(&exec)).unwrap();
    assert_eq!(store.total(), 1, "{:?}", store.calls.lock().unwrap());
    assert_eq!(store.get("kv_get"), 1);
    // ...and it saw the whole context the sink recorded.
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 5);
    for (_, s) in seen.iter() {
        assert_eq!(s.copy_to.as_deref(), Some("/out/file"));
        assert_eq!(s.secret_names, ["api"]);
        assert_eq!(s.output.as_ref().unwrap(), &output_schema());
    }

    // The destructor drops the cached record; the next call reads storage
    // again and finds the execution gone.
    other
        .handle_buffering_destructor(&request(TableBufferingDestructorRequest {
            execution_id: Bytes::from(exec.clone()),
            function_name: "recorder".into(),
            attach_opaque_data: None,
            transaction_id: None,
            schema_path: None,
        }))
        .unwrap();
    assert!(other
        .buffering_process(process_req(&exec, Some(b"a"), None))
        .is_err());
}

#[test]
fn request_attach_and_schema_win_over_the_recorded_copies() {
    let store: Arc<dyn FunctionStorage> = Arc::new(MemoryStorage::new());
    let mut d = worker(store);
    let seen = with_recorders(&mut d);
    let exec = b"exec-precedence".to_vec();
    d.persist_buffering_init(&exec, init_record(None, None))
        .unwrap();

    // The request names schema s2 and its own attach: both win.
    d.buffering_process(process_req(&exec, Some(b"req-attach"), Some("s2")))
        .unwrap();
    // A peer that omits both falls back to the recorded s1 / init-attach.
    d.buffering_process(process_req(&exec, None, None)).unwrap();

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].0, "s2");
    assert_eq!(seen[0].1.attach.as_deref(), Some(&b"req-attach"[..]));
    assert_eq!(seen[1].0, "s1");
    assert_eq!(seen[1].1.attach.as_deref(), Some(&b"init-attach"[..]));
}

/// A rewritten init record (e.g. a COPY TO path pointed elsewhere) never
/// reaches the function: neither a forged plaintext record nor one sealed for
/// another execution opens.
#[test]
fn tampered_copy_to_path_is_rejected() {
    let store: Arc<dyn FunctionStorage> = Arc::new(MemoryStorage::new());
    let exec = b"exec-victim".to_vec();
    let writer = worker(store.clone());
    writer
        .persist_buffering_init(&exec, init_record(Some("/safe/out"), Some("tok")))
        .unwrap();

    // A fresh worker (empty cache), so every call reads storage.
    let attempt = |label: &str| {
        let mut d = worker(store.clone());
        let seen = with_recorders(&mut d);
        let r = d.buffering_process(process_req(&exec, None, None));
        assert!(seen.lock().unwrap().is_empty(), "{label}: function ran");
        r.unwrap_err().message
    };

    // 1. A well-formed, unsealed record naming the attacker's path.
    let forged = init_record(Some("/attacker/out"), Some("tok"))
        .encode()
        .unwrap();
    store.kv_put(&exec, BUFFERING_INIT_KEY, &forged);
    assert!(attempt("plaintext").contains("framework storage payload"));

    // 2. A correctly sealed record lifted from the attacker's own execution.
    let attacker = b"exec-attacker".to_vec();
    writer
        .persist_buffering_init(&attacker, init_record(Some("/attacker/out"), None))
        .unwrap();
    let lifted = store.kv_get(&attacker, BUFFERING_INIT_KEY).unwrap();
    store.kv_put(&exec, BUFFERING_INIT_KEY, &lifted);
    assert!(attempt("moved").contains("failed authentication"));

    // 3. A bit flip in the genuine record.
    let mut genuine = seal::seal(
        &seal::derive_key(SIGNING_KEY),
        &seal::Slot {
            shard_key: &crate::storage::shard_key_for_scope(&exec),
            scope_id: &exec,
            namespace: seal::KV_NAMESPACE,
            key: BUFFERING_INIT_KEY,
        },
        &init_record(Some("/safe/out"), Some("tok"))
            .encode()
            .unwrap(),
    );
    let n = genuine.len();
    genuine[n / 2] ^= 0x20;
    store.kv_put(&exec, BUFFERING_INIT_KEY, &genuine);
    assert!(attempt("flipped").contains("failed authentication"));

    // 4. A worker holding another signing key cannot open the genuine one.
    writer
        .persist_buffering_init(&exec, init_record(Some("/safe/out"), Some("tok")))
        .unwrap();
    let mut stranger = Dispatcher::new("test");
    stranger.store = store.clone();
    stranger.storage_seal_root = Some(b"another-key".to_vec());
    with_recorders(&mut stranger);
    assert!(stranger
        .buffering_process(process_req(&exec, None, None))
        .is_err());
}

const MARKER: &str = "DUMMY-SECRET-7f3c9e1d";

fn assert_no_marker_under(path: &std::path::Path) {
    fn walk(p: &std::path::Path, out: &mut Vec<u8>) {
        if p.is_dir() {
            for e in std::fs::read_dir(p).unwrap().flatten() {
                walk(&e.path(), out);
            }
        } else if let Ok(b) = std::fs::read(p) {
            out.extend(b);
        }
    }
    let mut all = Vec::new();
    walk(path, &mut all);
    assert!(
        !all.is_empty(),
        "nothing was written under {}",
        path.display()
    );
    assert!(
        !all.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()),
        "plaintext secret found under {}",
        path.display()
    );
}

#[test]
fn no_plaintext_secret_reaches_sqlite_or_fs_storage() {
    let record = init_record(Some("/out"), Some(MARKER));
    assert!(record
        .encode()
        .unwrap()
        .windows(MARKER.len())
        .any(|w| w == MARKER.as_bytes()));
    let base = std::env::temp_dir().join(format!("vgi-seal-plain-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    let fs_dir = base.join("fs");
    let fs: Arc<dyn FunctionStorage> =
        Arc::new(crate::storage::FsStorage::with_base(fs_dir.clone()));
    let d = worker(fs.clone());
    d.persist_buffering_init(b"exec-fs", record.clone())
        .unwrap();
    // It reads back (another worker, through storage).
    let reader = worker(fs);
    let back = reader.buffering_init(b"exec-fs").unwrap();
    assert_eq!(back.secrets, record.secrets);
    assert_no_marker_under(&fs_dir);

    #[cfg(feature = "sqlite")]
    {
        let db_dir = base.join("sqlite");
        std::fs::create_dir_all(&db_dir).unwrap();
        let sq: Arc<dyn FunctionStorage> =
            Arc::new(crate::storage::SqliteStorage::open(db_dir.join("state.db")));
        let d = worker(sq.clone());
        d.persist_buffering_init(b"exec-sq", record.clone())
            .unwrap();
        // Aggregate arguments are sealed too.
        d.put_sealed(b"exec-sq", AGGREGATE_ARGS_KEY, MARKER.as_bytes())
            .unwrap();
        let reader = worker(sq.clone());
        assert_eq!(
            reader.buffering_init(b"exec-sq").unwrap().secrets,
            record.secrets
        );
        drop((d, reader, sq));
        assert_no_marker_under(&db_dir);
    }
    let _ = std::fs::remove_dir_all(&base);
}

// -- aggregates ------------------------------------------------------------

/// SUM(a) per group; the state is an i64.
struct Sum;

impl AggregateFunction for Sum {
    fn name(&self) -> &str {
        "sum_test"
    }
    fn metadata(&self) -> crate::function::FunctionMetadata {
        Default::default()
    }
    fn argument_specs(&self) -> Vec<crate::function::ArgSpec> {
        vec![]
    }
    fn on_bind(&self, _p: &AggregateBindParams) -> Result<crate::function::BindResponse> {
        Ok(crate::function::BindResponse::result(DataType::Int64))
    }
    fn initial_state(&self) -> Vec<u8> {
        0i64.to_le_bytes().to_vec()
    }
    fn update(
        &self,
        states: &mut HashMap<i64, Vec<u8>>,
        group_ids: &Int64Array,
        columns: &[ArrayRef],
    ) -> Result<()> {
        let v = columns[0].as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..group_ids.len() {
            let s = states
                .entry(group_ids.value(i))
                .or_insert_with(|| self.initial_state());
            let n = i64::from_le_bytes(s[..8].try_into().unwrap()) + v.value(i);
            *s = n.to_le_bytes().to_vec();
        }
        Ok(())
    }
    fn combine(&self, target: Vec<u8>, source: Vec<u8>) -> Result<Vec<u8>> {
        let t = i64::from_le_bytes(target[..8].try_into().unwrap());
        let s = i64::from_le_bytes(source[..8].try_into().unwrap());
        Ok((t + s).to_le_bytes().to_vec())
    }
    fn finalize(
        &self,
        output_schema: &SchemaRef,
        _group_ids: &Int64Array,
        states: &[Option<Vec<u8>>],
    ) -> Result<RecordBatch> {
        let col: Int64Array = states
            .iter()
            .map(|s| {
                s.as_ref()
                    .map(|b| i64::from_le_bytes(b[..8].try_into().unwrap()))
            })
            .collect();
        Ok(RecordBatch::try_new(output_schema.clone(), vec![Arc::new(col)]).unwrap())
    }
}

fn update_batch(gids: Vec<i64>, values: Vec<i64>) -> Vec<u8> {
    let b = RecordBatch::try_from_iter([
        (
            GROUP_COLUMN_NAME,
            Arc::new(Int64Array::from(gids)) as ArrayRef,
        ),
        ("a", Arc::new(Int64Array::from(values)) as ArrayRef),
    ])
    .unwrap();
    ipc::write_batch(&b).unwrap()
}

#[test]
fn grouped_aggregate_costs_two_calls_per_batch() {
    const GROUPS: i64 = 50;
    const BATCHES: usize = 4;
    let store = Arc::new(Counting::default());
    let mut d = worker(store.clone());
    d.register_aggregate(Arc::new(Sum));
    let exec = b"exec-agg".to_vec();
    // What aggregate_bind persists (sealed, empty arguments).
    d.put_sealed(&exec, AGGREGATE_ARGS_KEY, &[]).unwrap();
    store.reset();

    // Every group appears in every batch, twice.
    for _ in 0..BATCHES {
        let gids: Vec<i64> = (0..GROUPS).chain(0..GROUPS).collect();
        let values = vec![1i64; gids.len()];
        d.handle_aggregate_update(&request(AggregateUpdateRequest {
            function_name: "sum_test".into(),
            execution_id: Bytes::from(exec.clone()),
            input_batch: Bytes::from(update_batch(gids, values)),
            attach_opaque_data: None,
            schema_path: None,
        }))
        .unwrap();
    }
    // Was 2 * GROUPS per batch (a get and a put per distinct group).
    assert_eq!(
        store.total(),
        2 * BATCHES,
        "{:?}",
        store.calls.lock().unwrap()
    );
    assert_eq!(store.get("kv_get_many"), BATCHES);
    assert_eq!(store.get("kv_put_many"), BATCHES);

    // Combine: groups 0..25 merge into 25..50. Was 3 per pair.
    store.reset();
    let merge = RecordBatch::try_from_iter([
        (
            "source_group_id",
            Arc::new(Int64Array::from((0..25).collect::<Vec<i64>>())) as ArrayRef,
        ),
        (
            "target_group_id",
            Arc::new(Int64Array::from((25..50).collect::<Vec<i64>>())) as ArrayRef,
        ),
    ])
    .unwrap();
    d.handle_aggregate_combine(&request(AggregateCombineRequest {
        function_name: "sum_test".into(),
        execution_id: Bytes::from(exec.clone()),
        merge_batch: Bytes::from(ipc::write_batch(&merge).unwrap()),
        attach_opaque_data: None,
        schema_path: None,
    }))
    .unwrap();
    assert_eq!(store.total(), 2, "{:?}", store.calls.lock().unwrap());

    // Finalize over every group: was GROUPS + 1.
    store.reset();
    let gids = RecordBatch::try_from_iter([(
        "group_id",
        Arc::new(Int64Array::from((0..GROUPS).collect::<Vec<i64>>())) as ArrayRef,
    )])
    .unwrap();
    let out = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Int64,
        true,
    )]));
    let resp = d
        .handle_aggregate_finalize(&request(AggregateFinalizeRequest {
            function_name: "sum_test".into(),
            execution_id: Bytes::from(exec.clone()),
            group_ids_batch: Bytes::from(ipc::write_batch(&gids).unwrap()),
            output_schema: Bytes::from(ipc::write_schema_ref(&out).unwrap()),
            attach_opaque_data: None,
            schema_path: None,
        }))
        .unwrap()
        .unwrap();
    assert_eq!(store.total(), 2, "{:?}", store.calls.lock().unwrap());

    // And the answers are right: 2 per batch per group, targets doubled.
    let inner = resp
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .value(0)
        .to_vec();
    let resp: AggregateFinalizeResponse =
        wire::from_batch(&ipc::read_batch(&inner).unwrap()).unwrap();
    let result = ipc::read_batch(&resp.result_batch.0).unwrap();
    let col = result
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let per_group = 2 * BATCHES as i64;
    for g in 0..GROUPS as usize {
        let want = if g >= 25 { 2 * per_group } else { per_group };
        assert_eq!(col.value(g), want, "group {g}");
    }
}

/// Combine pairs apply in batch order: a target merged earlier in the batch
/// is the source of a later pair, as the per-pair round trips gave.
#[test]
fn aggregate_combine_chains_within_one_batch() {
    let store: Arc<dyn FunctionStorage> = Arc::new(MemoryStorage::new());
    let mut d = worker(store.clone());
    d.register_aggregate(Arc::new(Sum));
    let exec = b"exec-chain".to_vec();
    d.put_sealed(&exec, AGGREGATE_ARGS_KEY, &[]).unwrap();
    d.handle_aggregate_update(&request(AggregateUpdateRequest {
        function_name: "sum_test".into(),
        execution_id: Bytes::from(exec.clone()),
        input_batch: Bytes::from(update_batch(vec![0, 1, 2], vec![1, 10, 100])),
        attach_opaque_data: None,
        schema_path: None,
    }))
    .unwrap();
    // 0 -> 1, then 1 -> 2, then 3 (stateless) -> 4 (stateless).
    let merge = RecordBatch::try_from_iter([
        (
            "source_group_id",
            Arc::new(Int64Array::from(vec![0, 1, 3])) as ArrayRef,
        ),
        (
            "target_group_id",
            Arc::new(Int64Array::from(vec![1, 2, 4])) as ArrayRef,
        ),
    ])
    .unwrap();
    d.handle_aggregate_combine(&request(AggregateCombineRequest {
        function_name: "sum_test".into(),
        execution_id: Bytes::from(exec.clone()),
        merge_batch: Bytes::from(ipc::write_batch(&merge).unwrap()),
        attach_opaque_data: None,
        schema_path: None,
    }))
    .unwrap();
    let get = |g: i64| {
        store
            .kv_get(&exec, &Dispatcher::agg_key(g))
            .map(|b| i64::from_le_bytes(b[..8].try_into().unwrap()))
    };
    assert_eq!(get(1), Some(11));
    assert_eq!(get(2), Some(111));
    assert_eq!(get(4), None, "two stateless groups stay stateless");
}

#[test]
fn aggregate_finalize_refuses_missing_or_unsealed_arguments() {
    let store: Arc<dyn FunctionStorage> = Arc::new(MemoryStorage::new());
    let mut d = worker(store.clone());
    d.register_aggregate(Arc::new(Sum));
    let exec = b"exec-noargs".to_vec();
    let out = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Int64,
        true,
    )]));
    let gids =
        RecordBatch::try_from_iter([("group_id", Arc::new(Int64Array::from(vec![0])) as ArrayRef)])
            .unwrap();
    let finalize = || {
        d.handle_aggregate_finalize(&request(AggregateFinalizeRequest {
            function_name: "sum_test".into(),
            execution_id: Bytes::from(exec.clone()),
            group_ids_batch: Bytes::from(ipc::write_batch(&gids).unwrap()),
            output_schema: Bytes::from(ipc::write_schema_ref(&out).unwrap()),
            attach_opaque_data: None,
            schema_path: None,
        }))
    };
    assert!(finalize().is_err(), "missing arguments");
    store.kv_put(&exec, AGGREGATE_ARGS_KEY, b"");
    assert!(finalize().is_err(), "unsealed arguments");
    d.put_sealed(&exec, AGGREGATE_ARGS_KEY, &[]).unwrap();
    assert!(finalize().is_ok());
}

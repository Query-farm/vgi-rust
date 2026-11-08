// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Writable catalog tables, transaction hooks and the main-catalog attach
//! payload: dispatcher-level tests (every transport) plus an end-to-end HTTP
//! test of the sealed path (the hooks and the functions see the same
//! plaintext values while the wire carries only sealed ones).

use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi_protocol::generated::request_params as p;
use vgi_rpc::{Bytes, Request, Result, RpcError};

use crate::catalog::{
    self, attach_payload, CatSchema, CatTable, CatalogModel, CatalogTransactions, TableWrites,
    WriteFunction,
};
use crate::constraints::{
    catalog_capabilities, constraint_record, encode_record, read_catalog_capabilities,
    ConstraintKind, ConstraintRecord, CONSTRAINT_ENFORCE, CONSTRAINT_RECORDS_READ,
};
use crate::dispatch::Dispatcher;
use crate::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata};
use crate::protocol::dtos::{
    CatalogAttachRequest, CatalogAttachResult, CatalogTransactionBeginResult, ItemsResult,
    ScanFunctionResult, TableInfo,
};
use crate::table_in_out::TableInOutFunction;
use crate::write_results::{
    row_id_field, write_function_arg_specs, write_result_schema, WriteOperation, WriteOptions,
    WriteResultMode,
};
use crate::{ipc, wire};

// ------------------------------------------------------------------ fixtures

/// What the transaction hooks saw, in order: `(kind, attach payload, tx)`.
type Log = Arc<Mutex<Vec<(String, Option<Vec<u8>>, Vec<u8>)>>>;

struct Hooks {
    log: Log,
    refuse_commit: bool,
}

fn transactional(options: Option<&RecordBatch>) -> bool {
    options
        .and_then(|o| o.column_by_name("transactional"))
        .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
        .map(|b| !b.is_null(0) && b.value(0))
        .unwrap_or(false)
}

impl CatalogTransactions for Hooks {
    fn supports_transactions(&self, options: Option<&RecordBatch>) -> Result<bool> {
        Ok(transactional(options))
    }
    fn begin(&self, attach: &[u8]) -> Result<Option<Vec<u8>>> {
        let id = vec![0xAB; 16];
        self.log
            .lock()
            .unwrap()
            .push(("begin".into(), attach_payload(attach), id.clone()));
        Ok(Some(id))
    }
    fn commit(&self, attach: &[u8], tx: &[u8]) -> Result<()> {
        self.log
            .lock()
            .unwrap()
            .push(("commit".into(), attach_payload(attach), tx.to_vec()));
        if self.refuse_commit {
            return Err(RpcError::runtime_error("commit refused by the hook"));
        }
        Ok(())
    }
    fn rollback(&self, attach: &[u8], tx: &[u8]) -> Result<()> {
        self.log
            .lock()
            .unwrap()
            .push(("rollback".into(), attach_payload(attach), tx.to_vec()));
        Ok(())
    }
}

/// The attach payload: the secret `token` option, verbatim.
fn token_payload(options: Option<&RecordBatch>) -> Result<Vec<u8>> {
    let token = options
        .and_then(|o| o.column_by_name("token"))
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .filter(|s| !s.is_null(0))
        .map(|s| s.value(0).to_string())
        .unwrap_or_default();
    Ok(format!("token={token}").into_bytes())
}

fn items_columns() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("qty", DataType::Int64, true),
        row_id_field("rowid"),
    ]))
}

/// A write function that only binds (the dispatcher tests never run it).
struct ItemsUpdate;

impl TableInOutFunction for ItemsUpdate {
    fn name(&self) -> &str {
        "items_update"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata::default()
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        write_function_arg_specs("rows to update")
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let mode = WriteOptions::from_arguments(&params.arguments)?.result_mode;
        Ok(BindResponse {
            output_schema: write_result_schema(mode, &items_columns()),
            opaque_data: Vec::new(),
        })
    }
}

fn pk_record(table: &str) -> Bytes {
    constraint_record(
        &ConstraintRecord::new(&["main"], table, "pk", ConstraintKind::PrimaryKey)
            .with_columns(&[&["id"]])
            .enforced(),
    )
    .unwrap()
}

fn model(log: &Log, refuse_commit: bool) -> CatalogModel {
    let items = CatTable::new(
        "items",
        items_columns(),
        "items_scan",
        Vec::new(),
        None,
        None,
    )
    .with_update_function(Arc::new(ItemsUpdate), WriteResultMode::Changes);
    let mut items = items;
    items.constraints = vec![pk_record("items")];
    let mut labels = CatTable::new(
        "labels",
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        "labels_scan",
        Vec::new(),
        None,
        None,
    );
    labels.constraints = vec![pk_record("labels")];
    CatalogModel {
        name: "txcat".into(),
        attach_payload: Some(Arc::new(token_payload)),
        transactions: Some(Arc::new(Hooks {
            log: log.clone(),
            refuse_commit,
        })),
        schemas: vec![CatSchema {
            name: "main".into(),
            tables: vec![items, labels],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn dispatcher(log: &Log, refuse_commit: bool) -> Dispatcher {
    let mut d = Dispatcher::new("txcat");
    d.register_table_in_out(Arc::new(ItemsUpdate));
    d.set_catalog(model(log, refuse_commit));
    d
}

fn request(method: &str, batch: RecordBatch) -> Request {
    Request {
        method: method.to_string(),
        protocol: String::new(),
        request_id: String::new(),
        batch,
        metadata: Arc::new(Default::default()),
    }
}

fn inner(result: RecordBatch) -> RecordBatch {
    let envelope = result
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    ipc::read_batch(envelope.value(0)).unwrap()
}

fn options(token: &str, tx: Option<bool>) -> RecordBatch {
    let mut fields = vec![Field::new("token", DataType::Utf8, true)];
    let mut cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![token]))];
    if let Some(t) = tx {
        fields.push(Field::new("transactional", DataType::Boolean, true));
        cols.push(Arc::new(BooleanArray::from(vec![t])));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
}

fn offer(read: &[&str], write: &[&str]) -> Bytes {
    encode_record(crate::protocol::dtos::ClientCapabilities {
        engine: "test".into(),
        native_formats: vec![],
        catalogs: vec![],
        can_stream: true,
        filter_encodings: vec![],
        query_pushdown: None,
        catalog: Some(catalog_capabilities(read, &[], write, &[]).unwrap()),
    })
    .unwrap()
}

fn attach_body(name: &str, opts: Option<&RecordBatch>, client: Option<Bytes>) -> RecordBatch {
    let inner_req = wire::to_batch(CatalogAttachRequest {
        name: name.to_string(),
        options: opts.map(|o| Bytes::from(ipc::write_batch(o).unwrap())),
        data_version_spec: None,
        implementation_version: None,
        client_capabilities: client,
    })
    .unwrap();
    wire::to_batch(p::CatalogAttachParams {
        request: Bytes::from(ipc::write_batch(&inner_req).unwrap()),
    })
    .unwrap()
}

fn attach(d: &Dispatcher, opts: Option<&RecordBatch>) -> Result<CatalogAttachResult> {
    attach_with(d, "txcat", opts, None)
}

fn attach_with(
    d: &Dispatcher,
    name: &str,
    opts: Option<&RecordBatch>,
    client: Option<Bytes>,
) -> Result<CatalogAttachResult> {
    let result = d
        .handle_catalog_attach(&request("catalog_attach", attach_body(name, opts, client)))?
        .unwrap();
    Ok(wire::from_batch(&inner(result)).unwrap())
}

fn begin(d: &Dispatcher, attach: &[u8]) -> Result<Vec<u8>> {
    let batch = wire::to_batch(p::CatalogTransactionBeginParams {
        attach_opaque_data: Bytes::from(attach.to_vec()),
    })
    .unwrap();
    let result = d
        .handle_transaction_begin(&request("catalog_transaction_begin", batch))?
        .unwrap();
    let r: CatalogTransactionBeginResult = wire::from_batch(&inner(result)).unwrap();
    Ok(r.transaction_opaque_data.unwrap().0)
}

fn end(d: &Dispatcher, commit: bool, attach: &[u8], tx: &[u8]) -> Result<()> {
    if commit {
        let batch = wire::to_batch(p::CatalogTransactionCommitParams {
            attach_opaque_data: Bytes::from(attach.to_vec()),
            transaction_opaque_data: Bytes::from(tx.to_vec()),
        })
        .unwrap();
        d.handle_transaction_commit(&request("catalog_transaction_commit", batch))?;
    } else {
        let batch = wire::to_batch(p::CatalogTransactionRollbackParams {
            attach_opaque_data: Bytes::from(attach.to_vec()),
            transaction_opaque_data: Bytes::from(tx.to_vec()),
        })
        .unwrap();
        d.handle_transaction_rollback(&request("catalog_transaction_rollback", batch))?;
    }
    Ok(())
}

// ---------------------------------------------------- attach and transactions

#[test]
fn supports_transactions_is_decided_per_attach() {
    let log = Log::default();
    let d = dispatcher(&log, false);
    assert!(
        attach(&d, Some(&options("t", Some(true))))
            .unwrap()
            .supports_transactions
    );
    assert!(
        !attach(&d, Some(&options("t", Some(false))))
            .unwrap()
            .supports_transactions
    );
    assert!(
        !attach(&d, Some(&options("t", None)))
            .unwrap()
            .supports_transactions
    );
    assert!(!attach(&d, None).unwrap().supports_transactions);
    // Without hooks: the SDK's historical `true`.
    let mut plain = Dispatcher::new("cat");
    plain.set_catalog(CatalogModel {
        name: "cat".into(),
        ..Default::default()
    });
    assert!(
        attach_with(&plain, "cat", None, None)
            .unwrap()
            .supports_transactions
    );
}

#[test]
fn a_secondary_catalog_decides_too() {
    let log = Log::default();
    let mut d = Dispatcher::new("primary");
    let mut sec = model(&log, false);
    sec.name = "sec".into();
    d.register_secondary_catalog(sec, Vec::new());
    let on = attach_with(&d, "sec", Some(&options("s", Some(true))), None).unwrap();
    assert!(on.supports_transactions);
    let off = attach_with(&d, "sec", Some(&options("s", None)), None).unwrap();
    assert!(!off.supports_transactions);
    // ... and its hooks serve its transactions.
    let tx = begin(&d, &on.attach_opaque_data.0).unwrap();
    assert_eq!(tx, vec![0xAB; 16]);
    assert_eq!(log.lock().unwrap()[0].1.as_deref(), Some(&b"token=s"[..]));
}

#[test]
fn the_main_catalog_attach_payload_carries_a_secret_option_to_functions() {
    let log = Log::default();
    let d = dispatcher(&log, false);
    let a = attach(&d, Some(&options("sk-secret-123", Some(true)))).unwrap();
    assert_eq!(
        attach_payload(&a.attach_opaque_data.0).as_deref(),
        Some(&b"token=sk-secret-123"[..])
    );
    // Two attaches are two sessions with distinct values.
    let b = attach(&d, Some(&options("sk-secret-123", Some(true)))).unwrap();
    assert_ne!(a.attach_opaque_data.0, b.attach_opaque_data.0);
    // A value this SDK did not mint carries no payload.
    assert_eq!(attach_payload(b"txcat"), None);
}

#[test]
fn the_main_catalog_payload_refuses_conflicting_encodings() {
    let mut d = Dispatcher::new("cat");
    d.set_catalog(CatalogModel {
        name: "cat".into(),
        attach_payload: Some(Arc::new(token_payload)),
        attach_options_default_batch: Some(ipc::write_batch(&options("d", None)).unwrap()),
        ..Default::default()
    });
    let err = attach_with(&d, "cat", None, None).unwrap_err();
    assert!(err.message.contains("attach_payload"), "{}", err.message);
}

#[test]
fn hooks_mint_commit_and_roll_back_with_the_same_id() {
    let log = Log::default();
    let d = dispatcher(&log, false);
    let a = attach(&d, Some(&options("tok", Some(true))))
        .unwrap()
        .attach_opaque_data
        .0;
    let tx = begin(&d, &a).unwrap();
    assert_eq!(tx, vec![0xAB; 16], "the hook minted the id");
    end(&d, true, &a, &tx).unwrap();
    let tx2 = begin(&d, &a).unwrap();
    end(&d, false, &a, &tx2).unwrap();
    let log = log.lock().unwrap();
    let kinds: Vec<&str> = log.iter().map(|e| e.0.as_str()).collect();
    assert_eq!(kinds, ["begin", "commit", "begin", "rollback"]);
    for (_, payload, id) in log.iter() {
        assert_eq!(payload.as_deref(), Some(&b"token=tok"[..]));
        assert_eq!(id, &vec![0xAB; 16]);
    }
}

#[test]
fn a_refused_commit_fails_the_commit() {
    let log = Log::default();
    let d = dispatcher(&log, true);
    let a = attach(&d, Some(&options("tok", Some(true))))
        .unwrap()
        .attach_opaque_data
        .0;
    let tx = begin(&d, &a).unwrap();
    let err = end(&d, true, &a, &tx).unwrap_err();
    assert!(err.message.contains("commit refused by the hook"));
}

#[test]
fn without_hooks_begin_mints_an_sdk_id_and_commit_is_a_no_op() {
    let mut d = Dispatcher::new("cat");
    d.set_catalog(CatalogModel {
        name: "cat".into(),
        ..Default::default()
    });
    let a = attach_with(&d, "cat", None, None)
        .unwrap()
        .attach_opaque_data
        .0;
    let t1 = begin(&d, &a).unwrap();
    let t2 = begin(&d, &a).unwrap();
    assert_eq!(t1.len(), 29);
    assert_ne!(t1, t2, "a fresh id per BEGIN");
    end(&d, true, &a, &t1).unwrap();
    end(&d, false, &a, &t2).unwrap();
}

// ------------------------------------------------------------ writable tables

fn table_infos(d: &Dispatcher, attach: &[u8]) -> Vec<TableInfo> {
    let batch = wire::to_batch(p::CatalogSchemaContentsTablesParams {
        attach_opaque_data: Bytes::from(attach.to_vec()),
        path: vec!["main".into()],
        transaction_opaque_data: None,
    })
    .unwrap();
    let result = d
        .handle_contents_tables(&request("catalog_schema_contents_tables", batch))
        .unwrap()
        .unwrap();
    let items: ItemsResult = wire::from_batch(&inner(result)).unwrap();
    items
        .items
        .iter()
        .map(|b| wire::from_batch(&ipc::read_batch(&b.0).unwrap()).unwrap())
        .collect()
}

fn scan_result(bytes: &Option<Bytes>) -> Option<ScanFunctionResult> {
    let bytes = bytes.as_ref()?;
    (!bytes.0.is_empty()).then(|| wire::from_batch(&ipc::read_batch(&bytes.0).unwrap()).unwrap())
}

#[test]
fn table_info_inlines_the_write_function_and_its_modes() {
    let log = Log::default();
    let d = dispatcher(&log, false);
    let a = attach(&d, None).unwrap().attach_opaque_data.0;
    let infos = table_infos(&d, &a);
    let items = infos.iter().find(|t| t.name == "items").unwrap();
    assert_eq!(
        items.write_result_modes,
        vec![("update".to_string(), "changes".to_string())]
    );
    let update = scan_result(&items.update_function).expect("update function inlined");
    assert_eq!(update.function_name, "items_update");
    assert_eq!(update.schema_path, Some(vec!["main".to_string()]));
    assert!(scan_result(&items.insert_function).is_none());
    assert!(scan_result(&items.delete_function).is_none());
    // The read-only table declares nothing, so DuckDB says "does not support".
    let labels = infos.iter().find(|t| t.name == "labels").unwrap();
    assert!(labels.write_result_modes.is_empty());
    assert!(scan_result(&labels.update_function).is_none());
}

#[test]
fn write_function_getters_answer_and_name_unsupported_operations() {
    let log = Log::default();
    let d = dispatcher(&log, false);
    let a = attach(&d, None).unwrap().attach_opaque_data.0;
    let req = |name: &str| {
        request(
            "catalog_table_update_function_get",
            wire::to_batch(p::CatalogTableUpdateFunctionGetParams {
                attach_opaque_data: Bytes::from(a.clone()),
                schema_path: vec!["main".into()],
                name: name.into(),
                transaction_opaque_data: None,
            })
            .unwrap(),
        )
    };
    let out = d
        .handle_table_update_function_get(&req("items"))
        .unwrap()
        .unwrap();
    let r: ScanFunctionResult = wire::from_batch(&inner(out)).unwrap();
    assert_eq!(r.function_name, "items_update");
    let err = d
        .handle_table_write_function_get(&req("items"), WriteOperation::Delete)
        .unwrap_err();
    assert!(
        err.message.contains("does not support DELETE"),
        "{}",
        err.message
    );
    let err = d
        .handle_table_update_function_get(&req("labels"))
        .unwrap_err();
    assert!(
        err.message.contains("does not support UPDATE"),
        "{}",
        err.message
    );
}

#[test]
fn update_and_delete_require_a_scan_function() {
    let mut t = CatTable::new("t", items_columns(), "", Vec::new(), None, None);
    t.writes = TableWrites {
        update: Some(WriteFunction::named("t_update", WriteResultMode::Count)),
        ..Default::default()
    };
    let err = catalog::table_info(&["main".into()], &t, None).unwrap_err();
    assert!(
        err.message.contains("requires a scan function"),
        "{}",
        err.message
    );
    // INSERT needs no row ids.
    t.writes = TableWrites {
        insert: Some(WriteFunction::named("t_insert", WriteResultMode::Rows)),
        ..Default::default()
    };
    let info = catalog::table_info(&["main".into()], &t, None).unwrap();
    assert_eq!(
        info.write_result_modes,
        vec![("insert".to_string(), "rows".to_string())]
    );
}

#[test]
fn a_writable_constrained_table_negotiates_the_enforce_profile() {
    let log = Log::default();
    let d = dispatcher(&log, false);
    let client = offer(&[CONSTRAINT_RECORDS_READ], &[CONSTRAINT_ENFORCE]);
    let a = attach_with(&d, "txcat", None, Some(client)).unwrap();
    let selected = read_catalog_capabilities(&a.selected_capabilities.unwrap().0).unwrap();
    assert_eq!(selected.write, vec![CONSTRAINT_ENFORCE]);
    // A catalog whose constrained tables are all read-only does not promise it.
    let mut ro = Dispatcher::new("ro");
    let mut m = model(&log, false);
    m.name = "ro".into();
    for t in &mut m.schemas[0].tables {
        t.writes = TableWrites::default();
    }
    assert!(!m.has_writable_constrained_tables());
    ro.set_catalog(m);
    let client = offer(&[CONSTRAINT_RECORDS_READ], &[CONSTRAINT_ENFORCE]);
    let a = attach_with(&ro, "ro", None, Some(client)).unwrap();
    let available = read_catalog_capabilities(&a.catalog_capabilities.unwrap().0).unwrap();
    assert!(available.write.is_empty());
}

#[test]
fn the_exchange_blob_keeps_the_transaction_across_http_continuations() {
    let blob = crate::dispatch::ExchangeBlob {
        kind: "table_in_out".into(),
        function_name: "f".into(),
        output_schema: Vec::new(),
        filter_schema: Vec::new(),
        input_schema: Vec::new(),
        arguments: Vec::new(),
        settings: Vec::new(),
        secrets: Vec::new(),
        execution_id: Vec::new(),
        substream_id: Vec::new(),
        init_opaque: Vec::new(),
        pushdown_filters: Vec::new(),
        join_keys: Vec::new(),
        auto_apply: false,
        inner_resume: Vec::new(),
        at_unit: String::new(),
        at_value: String::new(),
        catalog_name: String::new(),
        schema_path: Vec::new(),
        split_payloads: Vec::new(),
        attach_opaque_data: Vec::new(),
        finalize_state_id: Vec::new(),
        plan_init_opaque: Vec::new(),
        filter_deltas: Vec::new(),
        filter_predicate_order: Vec::new(),
        continuation: Default::default(),
        transaction_opaque_data: vec![7; 16],
    };
    let bytes = vgi_rpc::stream_codec::bincode_encode(&blob).unwrap();
    let back: crate::dispatch::ExchangeBlob =
        vgi_rpc::stream_codec::bincode_decode(&bytes).unwrap();
    assert_eq!(back.transaction_opaque_data, vec![7; 16]);
}

// ------------------------------------------------------------- over HTTP

#[cfg(feature = "transport-http")]
mod http {
    use super::*;
    use crate::function::ProcessParams;
    use crate::table_function::{TableFunction, TableProducer};
    use vgi_rpc::http::{HttpState, ARROW_CONTENT_TYPE};
    use vgi_rpc::metadata::{REQUEST_ID_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY};
    use vgi_rpc::wire::{StreamReader, StreamWriter};
    use vgi_rpc::DictString;

    /// `tx_probe()`: records the plaintext attach payload and transaction id its
    /// producer sees (at produce time, not bind).
    /// `(attach payload, transaction id)` per producer built.
    type Seen = Arc<Mutex<Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>>>;

    struct TxProbe(Seen);

    impl TableFunction for TxProbe {
        fn name(&self) -> &str {
            "tx_probe"
        }
        fn metadata(&self) -> FunctionMetadata {
            FunctionMetadata::default()
        }
        fn argument_specs(&self) -> Vec<ArgSpec> {
            Vec::new()
        }
        fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
            Ok(BindResponse {
                output_schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)])),
                opaque_data: Vec::new(),
            })
        }
        fn producer(&self, p: &ProcessParams) -> Result<Box<dyn TableProducer>> {
            self.0.lock().unwrap().push((
                p.attach_opaque_data.as_deref().and_then(attach_payload),
                p.transaction_opaque_data.clone(),
            ));
            Ok(Box::new(Empty))
        }
    }

    struct Empty;
    impl TableProducer for Empty {
        fn next_batch(
            &mut self,
            _out: &mut vgi_rpc::OutputCollector,
        ) -> Result<Option<RecordBatch>> {
            Ok(None)
        }
        fn resume_supported(&self) -> bool {
            false
        }
    }

    fn start(w: crate::Worker) -> u16 {
        let server = Arc::new(w.build_server_for(crate::ServeTransport::Http));
        let state = HttpState::builder()
            .server(server)
            .producer_batch_limit(1)
            .build();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let listener = rt
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            rt.block_on(vgi_rpc::http::serve_with_shutdown(state, listener))
                .ok();
        });
        for _ in 0..100 {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        port
    }

    fn frame(batch: &RecordBatch, method: &str) -> Vec<u8> {
        let md = std::collections::HashMap::<String, String>::from([
            (RPC_METHOD_KEY.to_string(), method.to_string()),
            (REQUEST_VERSION_KEY.to_string(), REQUEST_VERSION.to_string()),
            (
                vgi_rpc::metadata::PROTOCOL_VERSION_KEY.to_string(),
                vgi_protocol::VGI_PROTOCOL_VERSION.to_string(),
            ),
            (REQUEST_ID_KEY.to_string(), "test".to_string()),
        ]);
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::new(&mut buf, batch.schema().as_ref()).unwrap();
            w.write(batch, Some(&md)).unwrap();
            w.finish().unwrap();
        }
        buf
    }

    /// POST `method` (`path` under the vgi.v3 prefix); `Ok(body)` on 200,
    /// `Err(status)` otherwise.
    fn post(port: u16, path: &str, body: Vec<u8>) -> std::result::Result<Vec<u8>, u16> {
        let url = format!(
            "http://127.0.0.1:{port}/{}/{path}",
            vgi_protocol::VGI_PROTOCOL_NAME
        );
        match ureq::post(&url)
            .header("Content-Type", ARROW_CONTENT_TYPE)
            .send(&body[..])
        {
            Ok(mut resp) => Ok(resp.body_mut().read_to_vec().unwrap()),
            Err(ureq::Error::StatusCode(code)) => Err(code),
            Err(e) => panic!("POST {path} failed: {e}"),
        }
    }

    /// The first batch of a unary response, unwrapped from `{result}`.
    fn unary_result(body: &[u8]) -> RecordBatch {
        let mut cursor = std::io::Cursor::new(body);
        let mut r = StreamReader::new(&mut cursor).unwrap();
        let (batch, _) = r.read_next().unwrap().expect("a result batch");
        inner(batch)
    }

    fn init_body(attach: &[u8], tx: &[u8]) -> Vec<u8> {
        let bind = crate::protocol::dtos::BindRequest {
            function_name: "tx_probe".to_string(),
            arguments: Bytes::from(Vec::new()),
            function_type: DictString("TABLE".to_string()),
            input_schema: None,
            settings: None,
            secrets: None,
            attach_opaque_data: Some(Bytes::from(attach.to_vec())),
            transaction_opaque_data: Some(Bytes::from(tx.to_vec())),
            resolved_secrets_provided: false,
            at_unit: None,
            at_value: None,
            copy_from: None,
            copy_to: None,
            schema_path: Some(vec!["main".to_string()]),
            argument_names: Some(Vec::new()),
        };
        let init = crate::protocol::dtos::InitRequest {
            bind_call: Bytes::from(ipc::write_batch(&wire::to_batch(bind).unwrap()).unwrap()),
            output_schema: Bytes::from(
                ipc::write_schema_ref(&Arc::new(Schema::new(vec![Field::new(
                    "x",
                    DataType::Utf8,
                    true,
                )])))
                .unwrap(),
            ),
            bind_opaque_data: None,
            projection_ids: None,
            pushdown_filters: None,
            join_keys: None,
            phase: None,
            execution_id: None,
            init_opaque_data: None,
            substream_id: None,
            order_by_column_name: None,
            order_by_direction: None,
            order_by_null_order: None,
            order_by_limit: None,
            tablesample_percentage: None,
            tablesample_seed: None,
            finalize_state_id: None,
            split_tokens: None,
            row_limit: None,
        };
        let inner = ipc::write_batch(&wire::to_batch(init).unwrap()).unwrap();
        let req = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "request",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from(vec![inner.as_slice()])) as ArrayRef],
        )
        .unwrap();
        frame(&req, "init")
    }

    /// Over HTTP the wire carries sealed values only, yet the begin hook, the
    /// function (at produce time) and the commit hook all see the same
    /// plaintext transaction id and the secret attach payload.
    #[test]
    fn hooks_and_functions_see_the_same_plaintext_over_sealed_http() {
        let log = Log::default();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut w = crate::Worker::new();
        w.register_table(TxProbe(seen.clone()));
        w.set_catalog(model(&log, false));
        let port = start(w);

        let body = attach_body("txcat", Some(&options("sk-http-secret", Some(true))), None);
        let attached: CatalogAttachResult = wire::from_batch(&unary_result(
            &post(port, "catalog_attach", frame(&body, "catalog_attach")).unwrap(),
        ))
        .unwrap();
        assert!(attached.supports_transactions);
        let sealed_attach = attached.attach_opaque_data.0;
        assert!(
            !sealed_attach.windows(14).any(|w| w == b"sk-http-secret"),
            "the attach payload is sealed on the wire"
        );
        assert_eq!(attach_payload(&sealed_attach), None);

        let begin_body = wire::to_batch(p::CatalogTransactionBeginParams {
            attach_opaque_data: Bytes::from(sealed_attach.clone()),
        })
        .unwrap();
        let begun: CatalogTransactionBeginResult = wire::from_batch(&unary_result(
            &post(
                port,
                "catalog_transaction_begin",
                frame(&begin_body, "catalog_transaction_begin"),
            )
            .unwrap(),
        ))
        .unwrap();
        let sealed_tx = begun.transaction_opaque_data.unwrap().0;
        assert_ne!(
            sealed_tx,
            vec![0xAB; 16],
            "the transaction id is sealed on the wire"
        );

        post(port, "init/init", init_body(&sealed_attach, &sealed_tx)).unwrap();

        let commit_body = wire::to_batch(p::CatalogTransactionCommitParams {
            attach_opaque_data: Bytes::from(sealed_attach.clone()),
            transaction_opaque_data: Bytes::from(sealed_tx.clone()),
        })
        .unwrap();
        post(
            port,
            "catalog_transaction_commit",
            frame(&commit_body, "catalog_transaction_commit"),
        )
        .unwrap();

        {
            let log = log.lock().unwrap();
            let kinds: Vec<&str> = log.iter().map(|e| e.0.as_str()).collect();
            assert_eq!(kinds, ["begin", "commit"]);
            for (_, payload, tx) in log.iter() {
                assert_eq!(payload.as_deref(), Some(&b"token=sk-http-secret"[..]));
                assert_eq!(tx, &vec![0xAB; 16]);
            }
        }
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            assert_eq!(seen[0].0.as_deref(), Some(&b"token=sk-http-secret"[..]));
            assert_eq!(
                seen[0].1.as_deref(),
                Some(&[0xAB; 16][..]),
                "produce-time tx"
            );
        }

        // A forged (plaintext) transaction value never reaches the hook.
        let forged = wire::to_batch(p::CatalogTransactionRollbackParams {
            attach_opaque_data: Bytes::from(sealed_attach),
            transaction_opaque_data: Bytes::from(vec![0xAB; 16]),
        })
        .unwrap();
        // vgi-rpc reports the error in-band; the uniform rejection names the field.
        let refused = post(
            port,
            "catalog_transaction_rollback",
            frame(&forged, "catalog_transaction_rollback"),
        )
        .unwrap_or_default();
        let needle = b"transaction_opaque_data not recognized";
        assert!(
            refused.windows(needle.len()).any(|w| w == needle),
            "a forged transaction value is refused"
        );
        assert_eq!(log.lock().unwrap().len(), 2, "no rollback hook ran");
    }
}

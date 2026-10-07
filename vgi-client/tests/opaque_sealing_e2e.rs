// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Opaque-value sealing over HTTP, against the example worker
//! (vgi-python `docs/protocol/vgi-opaque-data-sealing.md`).
//!
//! Raw calls, so the test controls every opaque byte: the owner's own values
//! work; a replay as another principal, a flipped byte, a transaction under
//! another attach of the same catalog and plaintext of the shapes this SDK
//! used to send are all the same `<field> not recognized`; and nothing the
//! worker logs carries a value.

#![cfg(all(feature = "http", feature = "oauth"))]

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BinaryArray, RecordBatch};
use vgi_protocol::protocol::dtos::{
    CatalogAttachRequest, CatalogTransactionBeginParams, CatalogVersionParams,
};
use vgi_rpc::{Bytes, RpcError};

fn example_worker() -> Option<PathBuf> {
    let mut dir = std::env::current_exe().ok()?;
    dir.pop();
    dir.pop();
    let exe = dir.join(if cfg!(windows) {
        "vgi-example-worker.exe"
    } else {
        "vgi-example-worker"
    });
    exe.exists().then_some(exe)
}

/// The fixture worker over HTTP with the test bearers, its log captured.
struct HttpWorker {
    child: Child,
    port: u16,
    log: PathBuf,
}

impl HttpWorker {
    fn start(exe: &PathBuf) -> Self {
        let log = std::env::temp_dir().join(format!(
            "vgi-opaque-sealing-{}-{}.log",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut child = Command::new(exe)
            .arg("--http")
            .env(
                "VGI_OPTIONAL_BEARER_TOKENS",
                "vgi-test-alice=alice,vgi-test-bob=bob",
            )
            // Everything the worker and vgi-rpc will say, so the log check
            // sees the most a deployment could record.
            .env("VGI_LOG", "trace")
            .env("RUST_LOG", "trace")
            .env_remove("VGI_SIGNING_KEY")
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .expect("spawn worker");
        let mut reader = BufReader::new(child.stdout.take().expect("stdout"));
        let mut line = String::new();
        let port = loop {
            line.clear();
            let n = reader.read_line(&mut line).expect("read PORT line");
            assert!(n > 0, "worker exited before announcing a port");
            if let Some(p) = line.trim().strip_prefix("PORT:") {
                break p.parse::<u16>().expect("port");
            }
        };
        std::thread::spawn(move || {
            let mut sink = String::new();
            while reader.read_line(&mut sink).map(|n| n > 0).unwrap_or(false) {
                sink.clear();
            }
        });
        Self { child, port, log }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

impl Drop for HttpWorker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.log);
    }
}

/// One raw `vgi.v2` unary call as `bearer`; the decoded result record.
fn call(
    url: &str,
    bearer: &str,
    method: &str,
    params: &RecordBatch,
) -> vgi_rpc::Result<RecordBatch> {
    use vgi_rpc::metadata::{
        PROTOCOL_KEY, PROTOCOL_VERSION_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY,
    };
    use vgi_rpc::wire::{StreamReader, StreamWriter};
    use vgi_rpc_client::{classify, BatchKind};

    let md = std::collections::HashMap::<String, String>::from([
        (RPC_METHOD_KEY.to_string(), method.to_string()),
        (REQUEST_VERSION_KEY.to_string(), REQUEST_VERSION.to_string()),
        (
            PROTOCOL_KEY.to_string(),
            vgi_protocol::VGI_PROTOCOL_NAME.to_string(),
        ),
        (
            PROTOCOL_VERSION_KEY.to_string(),
            vgi_protocol::VGI_PROTOCOL_VERSION.to_string(),
        ),
    ]);
    let mut body = Vec::new();
    {
        let mut w = StreamWriter::new(&mut body, params.schema().as_ref())?;
        w.write(params, Some(&md))?;
        w.finish()?;
    }
    let mut resp = ureq::post(&format!(
        "{url}/{}/{method}",
        vgi_protocol::VGI_PROTOCOL_NAME
    ))
    .config()
    .http_status_as_error(false)
    .build()
    .header("Content-Type", "application/vnd.apache.arrow.stream")
    .header("Authorization", &format!("Bearer {bearer}"))
    .send(&body[..])
    .map_err(|e| RpcError::new("TransportError", e.to_string()))?;
    let bytes = resp
        .body_mut()
        .read_to_vec()
        .map_err(|e| RpcError::new("TransportError", e.to_string()))?;
    let mut cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::new(&mut cursor)?;
    while let Some((batch, md)) = reader.read_next()? {
        match classify(&batch, &md) {
            BatchKind::Exception(e) => return Err(e),
            BatchKind::Data => {
                let nested = batch.column(0).as_binary::<i32>().value(0).to_vec();
                return vgi_protocol::ipc::read_batch(&nested);
            }
            _ => {}
        }
    }
    Err(RpcError::new("TransportError", "no result batch"))
}

fn attach(url: &str, bearer: &str, catalog: &str) -> Vec<u8> {
    let inner = vgi_protocol::wire::to_batch(CatalogAttachRequest {
        name: catalog.to_string(),
        options: None,
        data_version_spec: None,
        implementation_version: None,
        client_capabilities: None,
    })
    .unwrap();
    let params = RecordBatch::try_new(
        vgi_protocol::wire::params_schema_for("catalog_attach"),
        vec![
            Arc::new(BinaryArray::from(vec![vgi_protocol::ipc::write_batch(
                &inner,
            )
            .unwrap()
            .as_slice()])) as ArrayRef,
        ],
    )
    .unwrap();
    let result = call(url, bearer, "catalog_attach", &params).expect("attach");
    result
        .column_by_name("attach_opaque_data")
        .unwrap()
        .as_binary::<i32>()
        .value(0)
        .to_vec()
}

fn begin(url: &str, bearer: &str, attach: &[u8]) -> Vec<u8> {
    let params = vgi_protocol::wire::to_batch(CatalogTransactionBeginParams {
        attach_opaque_data: Bytes(attach.to_vec()),
    })
    .unwrap();
    let result = call(url, bearer, "catalog_transaction_begin", &params).expect("begin");
    result
        .column_by_name("transaction_opaque_data")
        .unwrap()
        .as_binary::<i32>()
        .value(0)
        .to_vec()
}

/// `catalog_version`, which opens both values.
fn version(url: &str, bearer: &str, attach: &[u8], tx: Option<&[u8]>) -> vgi_rpc::Result<i64> {
    let params = vgi_protocol::wire::to_batch(CatalogVersionParams {
        attach_opaque_data: Bytes(attach.to_vec()),
        transaction_opaque_data: tx.map(|t| Bytes(t.to_vec())),
    })
    .unwrap();
    let result = call(url, bearer, "catalog_version", &params)?;
    Ok(result
        .column_by_name("version")
        .unwrap()
        .as_primitive::<arrow_array::types::Int64Type>()
        .value(0))
}

fn flipped(value: &[u8], at: usize) -> Vec<u8> {
    let mut v = value.to_vec();
    v[at] ^= 0x01;
    v
}

/// Everything about an error a caller could branch on.
fn shape(
    e: &RpcError,
) -> (
    String,
    String,
    Option<String>,
    String,
    Vec<serde_json::Value>,
) {
    (
        e.error_type.clone(),
        e.message.clone(),
        e.error_kind.as_deref().map(str::to_string),
        e.error_code().to_string(),
        e.error_details().to_vec(),
    )
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[test]
fn opaque_values_are_sealed_bound_and_uniformly_rejected() {
    let Some(exe) = example_worker() else {
        eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
        return;
    };
    let worker = HttpWorker::start(&exe);
    let url = worker.url();
    let (a, b) = ("vgi-test-alice", "vgi-test-bob");

    // The owner's own values work, first -- a worker that rejects everything
    // must not pass.
    let attach_a = attach(&url, a, "example");
    let attach_a2 = attach(&url, a, "example");
    assert_ne!(attach_a, attach_a2, "fresh nonce per attach");
    assert_eq!(attach_a[0], 0x02, "envelope version byte");
    assert!(
        !attach_a.windows(7).any(|w| w == b"example"),
        "the catalog name is sealed, not visible"
    );
    let tx_a = begin(&url, a, &attach_a);
    assert_eq!(version(&url, a, &attach_a, None).unwrap(), 1);
    assert_eq!(version(&url, a, &attach_a, Some(&tx_a)).unwrap(), 1);

    let mut attach_errors = Vec::new();
    // Replay as another principal.
    attach_errors.push(version(&url, b, &attach_a, None).unwrap_err());
    // One flipped byte: first, middle, last.
    for at in [0, attach_a.len() / 2, attach_a.len() - 1] {
        attach_errors.push(version(&url, a, &flipped(&attach_a, at), None).unwrap_err());
    }
    // Plaintext of the shapes this SDK issued unsealed: the bare catalog name,
    // `<16-byte id>\0<ipc>`, and a secondary-catalog blob.
    for forged in [
        b"example".to_vec(),
        [b"example\0\0\0\0\0\0\0\0\0".as_slice(), b"\0", b"ipc"].concat(),
        b"\x00sec\x00twin_a\x00vgi-exec-0123456789abcdefghij".to_vec(),
        vec![0x02; 60],
    ] {
        attach_errors.push(version(&url, a, &forged, None).unwrap_err());
    }

    let mut tx_errors = Vec::new();
    // Transaction replayed as another principal (with the right attach for
    // that principal) and under a different attach of the same catalog.
    let attach_b = attach(&url, b, "example");
    tx_errors.push(version(&url, b, &attach_b, Some(&tx_a)).unwrap_err());
    tx_errors.push(version(&url, a, &attach_a2, Some(&tx_a)).unwrap_err());
    for at in [0, tx_a.len() / 2, tx_a.len() - 1] {
        tx_errors.push(version(&url, a, &attach_a, Some(&flipped(&tx_a, at))).unwrap_err());
    }
    tx_errors.push(version(&url, a, &attach_a, Some(b"vgi-exec-plaintext-tx-id-xyz")).unwrap_err());

    let first = shape(&attach_errors[0]);
    assert_eq!(first.1, "attach_opaque_data not recognized", "{first:?}");
    assert_eq!(first.2.as_deref(), Some("opaque_data_not_recognized"));
    assert_eq!(first.3, "INVALID_ARGUMENT");
    assert!(first.4.is_empty(), "no details");
    for e in &attach_errors {
        assert_eq!(shape(e), first);
    }
    let first_tx = shape(&tx_errors[0]);
    assert_eq!(
        first_tx.1, "transaction_opaque_data not recognized",
        "{first_tx:?}"
    );
    for e in &tx_errors {
        assert_eq!(shape(e), first_tx);
    }
    // The two fields differ only in the field name.
    assert_eq!(
        first.1.replace("attach_opaque_data", "<field>"),
        first_tx.1.replace("transaction_opaque_data", "<field>")
    );
    assert_eq!(
        (first.0, first.2, first.3),
        (first_tx.0, first_tx.2, first_tx.3)
    );

    // Never logged raw: no 12-byte window of any value's hex is in the log.
    let log = std::fs::read_to_string(&worker.log).unwrap_or_default();
    for value in [&attach_a, &attach_a2, &attach_b, &tx_a] {
        let h = hex(value);
        for start in (0..=h.len().saturating_sub(24)).step_by(2) {
            assert!(
                !log.contains(&h[start..start + 24]),
                "the worker logged an opaque value raw"
            );
        }
    }
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Attach tickets end to end over HTTP, against the example worker's
//! `ticket_probe` catalog (vgi-python `docs/protocol/vgi-attach-tickets.md`).
//!
//! 1. alice, freshly logged in (`Bearer vgi-test-alice`), attaches
//!    `ticket_probe` with `region` and the secret `api_key` and reads the probe
//!    row;
//! 2. she calls `vgi.attach_tickets.v1` `seal_attach` and
//!    `vgi_rpc.Identity.v1` `issue_grant`;
//! 3. a second client authenticated only by `Bearer <grant>` attaches with
//!    nothing but `vgi_attach_ticket` and reads the same row;
//! 4. bob's grant with alice's ticket is refused (`attach_ticket_invalid`), and
//!    so is the ticket beside any other option (`invalid_request`).

#![cfg(all(feature = "http", feature = "oauth"))]

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use vgi::attach_ticket::{AttachTicket, SealAttachRequest, ATTACH_TICKETS_PROTOCOL_NAME};
use vgi_client::auth::BearerAuth;
use vgi_client::{AttachOptions, BindSpec, FunctionKind, ScanOptions, VgiClient};
use vgi_rpc::{Bytes, RpcError};

/// The secret alice attaches with; only its digest ever comes back.
const API_KEY: &str = "sk-test-0123456789";
const API_KEY_DIGEST: &str = "0d3b56072291";
const REGION: &str = "eu-west-2";

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

/// The fixture HTTP worker, configured as the spec's §7.1 says, killed on drop.
struct HttpWorker {
    child: Child,
    port: u16,
}

impl HttpWorker {
    fn start(exe: &PathBuf, signing_key: Option<&str>) -> Self {
        let mut cmd = Command::new(exe);
        cmd.arg("--http")
            .env(
                "VGI_OPTIONAL_BEARER_TOKENS",
                "vgi-test-alice=alice,vgi-test-bob=bob",
            )
            // base64 of 32 bytes of 0x07.
            .env(
                "VGI_RPC_GRANT_KEYS",
                "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc=",
            )
            .env_remove("VGI_SIGNING_KEY")
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(key) = signing_key {
            cmd.env("VGI_SIGNING_KEY", key);
        }
        let mut child = cmd.spawn().expect("spawn worker");
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
        Self { child, port }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

impl Drop for HttpWorker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn client_as(url: &str, bearer: &str) -> VgiClient {
    VgiClient::connect_http_with_auth(url, Arc::new(BearerAuth::new(bearer)), None)
}

/// A one-row all-VARCHAR options record, as DuckDB sends string options.
fn options(pairs: &[(&str, &str)]) -> Bytes {
    let fields: Vec<Field> = pairs
        .iter()
        .map(|(k, _)| Field::new(*k, DataType::Utf8, true))
        .collect();
    let cols: Vec<ArrayRef> = pairs
        .iter()
        .map(|(_, v)| Arc::new(StringArray::from(vec![*v])) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap();
    Bytes(vgi::ipc::write_batch(&batch).unwrap())
}

/// Attach with `opts` as the client's principal and read `main.ticket_probe`.
fn attach_and_probe(
    client: &mut VgiClient,
    name: &str,
    opts: Bytes,
) -> vgi_rpc::Result<(String, String)> {
    let cat = client.attach(
        name,
        AttachOptions {
            options: Some(opts),
            ..Default::default()
        },
    )?;
    let bound = client.bind(&cat, &BindSpec::table("ticket_probe").in_schema("main"))?;
    let batches = client.scan(&bound, &ScanOptions::default())?.collect()?;
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one probe row");
    let col = |n: &str| {
        batch
            .column_by_name(n)
            .unwrap()
            .as_string::<i32>()
            .value(0)
            .to_string()
    };
    Ok((col("region"), col("api_key_sha256")))
}

/// One raw unary call over HTTP: `POST {url}/{protocol}/{method}` carrying
/// `params` as an Arrow IPC stream, optionally as `bearer`. Returns the
/// response's data batch, or the error batch it carried.
///
/// Raw on purpose: the protocol under test is not `vgi.v2`, so it is reached
/// the way any vgi-rpc client reaches a co-hosted protocol -- by its routing
/// key -- with no client library between the test and the wire.
fn raw_call(
    url: &str,
    bearer: Option<&str>,
    protocol: &str,
    method: &str,
    params: &RecordBatch,
) -> vgi_rpc::Result<RecordBatch> {
    use vgi_rpc::metadata::{PROTOCOL_KEY, REQUEST_VERSION, REQUEST_VERSION_KEY, RPC_METHOD_KEY};
    use vgi_rpc::wire::{StreamReader, StreamWriter};
    use vgi_rpc_client::{classify, BatchKind};

    let mut md = std::collections::HashMap::<String, String>::from([
        (RPC_METHOD_KEY.to_string(), method.to_string()),
        (REQUEST_VERSION_KEY.to_string(), REQUEST_VERSION.to_string()),
        (PROTOCOL_KEY.to_string(), protocol.to_string()),
    ]);
    // A versioned protocol gates every call on the client's declared version.
    if protocol == ATTACH_TICKETS_PROTOCOL_NAME {
        md.insert(
            vgi_rpc::metadata::PROTOCOL_VERSION_KEY.to_string(),
            vgi::attach_ticket::ATTACH_TICKETS_PROTOCOL_VERSION.to_string(),
        );
    }
    let mut body = Vec::new();
    {
        let mut w = StreamWriter::new(&mut body, params.schema().as_ref())?;
        w.write(params, Some(&md))?;
        w.finish()?;
    }
    let mut req = ureq::post(&format!("{url}/{protocol}/{method}"))
        .config()
        .http_status_as_error(false)
        .build()
        .header("Content-Type", "application/vnd.apache.arrow.stream");
    if let Some(token) = bearer {
        req = req.header("Authorization", &format!("Bearer {token}"));
    }
    let mut resp = req
        .send(&body[..])
        .map_err(|e| RpcError::new("TransportError", e.to_string()))?;
    let status = resp.status().as_u16();
    let bytes = resp
        .body_mut()
        .read_to_vec()
        .map_err(|e| RpcError::new("TransportError", e.to_string()))?;
    if status == 401 {
        return Err(RpcError::new("AuthenticationError", "HTTP 401"));
    }
    let mut cursor = std::io::Cursor::new(bytes);
    let mut reader = StreamReader::new(&mut cursor)?;
    while let Some((batch, md)) = reader.read_next()? {
        match classify(&batch, &md) {
            BatchKind::Exception(e) => return Err(e),
            BatchKind::Data => {
                let nested = batch.column(0).as_binary::<i32>().value(0).to_vec();
                return vgi::ipc::read_batch(&nested);
            }
            _ => {}
        }
    }
    Err(RpcError::new(
        "TransportError",
        format!("HTTP {status}: no result batch"),
    ))
}

fn call(
    url: &str,
    bearer: &str,
    protocol: &str,
    method: &str,
    params: &RecordBatch,
) -> vgi_rpc::Result<RecordBatch> {
    raw_call(url, Some(bearer), protocol, method, params)
}

fn seal_attach(
    url: &str,
    bearer: Option<&str>,
    request: SealAttachRequest,
) -> vgi_rpc::Result<String> {
    let inner = vgi::wire::to_batch(request)?;
    let params = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "request",
            DataType::Binary,
            false,
        )])),
        vec![Arc::new(BinaryArray::from(vec![
            vgi::ipc::write_batch(&inner)?.as_slice()
        ])) as ArrayRef],
    )
    .unwrap();
    let result = raw_call(
        url,
        bearer,
        ATTACH_TICKETS_PROTOCOL_NAME,
        "seal_attach",
        &params,
    )?;
    let ticket: AttachTicket = vgi::wire::from_batch(&result)?;
    Ok(ticket.ticket)
}

fn issue_grant(url: &str, bearer: &str) -> String {
    use vgi_rpc::token_identity::{issue_grant_params_schema, IDENTITY_PROTOCOL_NAME};
    let mut scopes =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
    scopes.append(true);
    let params = RecordBatch::try_new(
        issue_grant_params_schema(),
        vec![
            Arc::new(StringArray::from(vec!["nightly"])) as ArrayRef,
            Arc::new(scopes.finish()) as ArrayRef,
            Arc::new(Int64Array::from(vec![600])) as ArrayRef,
        ],
    )
    .unwrap();
    let grant = call(url, bearer, IDENTITY_PROTOCOL_NAME, "issue_grant", &params)
        .expect("issue_grant as a fresh login");
    grant
        .column_by_name("token")
        .unwrap()
        .as_string::<i32>()
        .value(0)
        .to_string()
}

/// `vgi_rpc.Reflection.v1` `list_protocols`, as `(name, version, hash)`.
fn hosted_protocols(url: &str) -> Vec<(String, String, String)> {
    let empty = RecordBatch::try_new_with_options(
        Arc::new(Schema::empty()),
        vec![],
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .unwrap();
    let listing = raw_call(url, None, "vgi_rpc.Reflection.v1", "list_protocols", &empty).unwrap();
    let protocols = listing
        .column_by_name("protocols")
        .unwrap()
        .as_list::<i32>();
    let items = protocols.value(0);
    let items = items.as_struct();
    let text = |field: &str, i: usize| {
        items
            .column_by_name(field)
            .unwrap()
            .as_string::<i32>()
            .value(i)
            .to_string()
    };
    (0..items.len())
        .map(|i| {
            (
                text("protocol", i),
                text("protocol_version", i),
                text("protocol_hash", i),
            )
        })
        .collect()
}

fn kind(err: &RpcError) -> Option<&str> {
    err.error_kind.as_deref()
}

#[test]
fn a_runner_holding_a_grant_reattaches_from_a_ticket_without_the_secret() {
    let Some(exe) = example_worker() else {
        eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
        return;
    };
    let worker = HttpWorker::start(&exe, Some("attach-ticket-e2e-signing-key"));
    let url = worker.url();

    // 1. alice attaches with both options and reads the probe.
    let attached = options(&[("region", REGION), ("api_key", API_KEY)]);
    let mut alice = client_as(&url, "vgi-test-alice");
    let row = attach_and_probe(&mut alice, "ticket_probe", attached.clone()).unwrap();
    assert_eq!(row, (REGION.to_string(), API_KEY_DIGEST.to_string()));

    // 2. seal_attach + issue_grant, as alice.
    let ticket = seal_attach(
        &url,
        Some("vgi-test-alice"),
        SealAttachRequest {
            catalog_name: "ticket_probe".into(),
            options: Some(attached),
            data_version_spec: String::new(),
            implementation_version: String::new(),
            ttl_seconds: 0,
        },
    )
    .unwrap();
    assert!(ticket.starts_with("vgia1."), "{ticket}");
    let alice_grant = issue_grant(&url, "vgi-test-alice");
    assert!(alice_grant.starts_with("vgig1."));

    // 3. A runner with only the grant and the ticket reads the same row --
    //    under a name it made up, which the ticket overrides.
    let mut runner = client_as(&url, &alice_grant);
    let row = attach_and_probe(
        &mut runner,
        "anything",
        options(&[("vgi_attach_ticket", &ticket)]),
    )
    .unwrap();
    assert_eq!(row, (REGION.to_string(), API_KEY_DIGEST.to_string()));

    // Without the ticket the runner cannot attach: api_key is required.
    let mut bare = client_as(&url, &alice_grant);
    assert!(attach_and_probe(&mut bare, "ticket_probe", options(&[("region", REGION)])).is_err());

    // 4a. bob's grant with alice's ticket: refused, indistinguishably from a forgery.
    let bob_grant = issue_grant(&url, "vgi-test-bob");
    let mut bob = client_as(&url, &bob_grant);
    let err = attach_and_probe(
        &mut bob,
        "ticket_probe",
        options(&[("vgi_attach_ticket", &ticket)]),
    )
    .unwrap_err();
    assert_eq!(kind(&err), Some("attach_ticket_invalid"), "{err}");
    assert!(!err.message.contains(&ticket), "the error quotes no ticket");

    // 4b. The ticket beside another option: invalid_request, before opening.
    let mut mixed = client_as(&url, &alice_grant);
    let err = attach_and_probe(
        &mut mixed,
        "ticket_probe",
        options(&[("vgi_attach_ticket", &ticket), ("region", "us-west-1")]),
    )
    .unwrap_err();
    assert_eq!(kind(&err), Some("invalid_request"), "{err}");

    // 4c. An anonymous caller may not seal.
    let err = seal_attach(
        &url,
        None,
        SealAttachRequest {
            catalog_name: "ticket_probe".into(),
            options: Some(options(&[("api_key", API_KEY)])),
            data_version_spec: String::new(),
            implementation_version: String::new(),
            ttl_seconds: 0,
        },
    )
    .unwrap_err();
    assert_eq!(kind(&err), Some("action_denied"), "{err}");

    // 4d. seal_attach validates against the catalog's declared options.
    let err = seal_attach(
        &url,
        Some("vgi-test-alice"),
        SealAttachRequest {
            catalog_name: "ticket_probe".into(),
            options: Some(options(&[("region", REGION), ("bogus", "x")])),
            data_version_spec: String::new(),
            implementation_version: String::new(),
            ttl_seconds: -1,
        },
    )
    .unwrap_err();
    assert_eq!(kind(&err), Some("invalid_request"), "{err}");
}

/// Hosting (spec §5.1): with grant keys but no signing key the protocol is
/// absent -- reflection does not list it -- and a ticket attach is refused.
#[test]
fn without_a_signing_key_tickets_are_not_hosted() {
    let Some(exe) = example_worker() else {
        eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
        return;
    };
    let worker = HttpWorker::start(&exe, None);
    let url = worker.url();
    let hosted = hosted_protocols(&url);
    assert!(
        hosted.iter().any(|(n, _, _)| n == "vgi_rpc.Identity.v1"),
        "grants are on: {hosted:?}"
    );
    assert!(
        !hosted
            .iter()
            .any(|(n, _, _)| n == ATTACH_TICKETS_PROTOCOL_NAME),
        "{hosted:?}"
    );

    let mut alice = client_as(&url, "vgi-test-alice");
    let err = attach_and_probe(
        &mut alice,
        "ticket_probe",
        options(&[("vgi_attach_ticket", "vgia1.AAAA")]),
    )
    .unwrap_err();
    assert_eq!(kind(&err), Some("attach_ticket_invalid"), "{err}");
}

/// With both a signing key and grant keys, reflection lists the protocol after
/// the worker's own, with the reference implementation's protocol hash.
#[test]
fn tickets_are_hosted_with_the_reference_hash() {
    let Some(exe) = example_worker() else {
        eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
        return;
    };
    let worker = HttpWorker::start(&exe, Some("k"));
    let hosted = hosted_protocols(&worker.url());
    let (_, version, hash) = hosted
        .iter()
        .find(|(n, _, _)| n == ATTACH_TICKETS_PROTOCOL_NAME)
        .unwrap_or_else(|| panic!("{hosted:?}"));
    assert_eq!(version, "1.0.0");
    // vgi-python f5e99c7: RpcServer(...).bindings["vgi.attach_tickets.v1"].protocol_hash
    assert_eq!(
        hash,
        "241fffa801dd073c76fa933b9ad5ac790a95b81fee02e73a8332da4d3526b4e0"
    );
}

/// The fixture catalog adds no function to `example`: `ticket_probe` lives in
/// `ticket_probe.main` only, so the extension's function_registration counts
/// for `example` are unchanged.
#[test]
fn ticket_probe_is_scoped_to_its_own_catalog() {
    let Some(exe) = example_worker() else {
        eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
        return;
    };
    let worker = HttpWorker::start(&exe, None);
    let mut client = VgiClient::connect_http(&worker.url()).unwrap();
    let names = |client: &mut VgiClient, catalog: &str| -> Vec<String> {
        let cat = client.attach(catalog, AttachOptions::default()).unwrap();
        client
            .functions_path(&cat, &["main".to_string()], FunctionKind::Table)
            .unwrap()
            .into_iter()
            .map(|f| f.name)
            .collect()
    };
    assert!(!names(&mut client, "example").contains(&"ticket_probe".to_string()));
    let catalogs: Vec<String> = client
        .catalogs()
        .unwrap()
        .into_iter()
        .map(|c| c.name)
        .collect();
    assert!(
        catalogs.contains(&"ticket_probe".to_string()),
        "{catalogs:?}"
    );
}

/// Opaque-data sealing rule 5: the secret `api_key` never travels in
/// `attach_opaque_data`, even over stdio where nothing is sealed. The fixture
/// carries only `region` and a digest of the key.
#[test]
fn ticket_probe_never_puts_the_secret_in_the_attach_value_on_stdio() {
    let Some(exe) = example_worker() else {
        eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
        return;
    };
    let exe = exe.to_string_lossy().to_string();
    let mut client = VgiClient::connect_subprocess(&[exe.as_str()]).unwrap();
    let cat = client
        .attach(
            "ticket_probe",
            AttachOptions {
                options: Some(options(&[("region", REGION), ("api_key", API_KEY)])),
                ..Default::default()
            },
        )
        .unwrap();
    let value = cat.handle().0.clone();
    assert!(
        !value
            .windows(API_KEY.len())
            .any(|w| w == API_KEY.as_bytes()),
        "the secret api_key is in attach_opaque_data"
    );
    let hex: String = value.iter().map(|b| format!("{b:02x}")).collect();
    let key_hex: String = API_KEY.bytes().map(|b| format!("{b:02x}")).collect();
    assert!(!hex.contains(&key_hex));
    // What it does carry is the digest, which is what the probe reports.
    assert!(value
        .windows(API_KEY_DIGEST.len())
        .any(|w| w == API_KEY_DIGEST.as_bytes()));
}

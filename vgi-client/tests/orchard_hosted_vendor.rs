// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Orchard hosted mode with the blob vendor, end to end.
//!
//! Orchard runs every worker image with `VGI_ORCHARD_HOSTED=1` and the blob
//! vendor (`VGI_BLOB_VENDOR_URL` / `VGI_BLOB_VENDOR_TOKEN`). This runs the
//! example worker that way, against vgi-rpc-python's fake vendor and a
//! loopback JWKS, and checks that:
//!
//! - every response, a 401 included, advertises `VGI-Upload-URL-Support: 2`,
//!   `VGI-Arguments-Ref-Bytes`, `VGI-Externalization-Enabled: true` and the
//!   hosted request cap (`VGI-Max-Request-Bytes`, 32 MiB by default);
//! - an upload grant carries `object_id` and a sealed `ref`;
//! - a bind presenting that ref resolves for the Pass principal it was granted
//!   to and is refused (`invalid_arguments_ref`) for any other;
//! - a result over the threshold (a 400-split plan) is externalized.
//!
//! The port of vgi-csharp's `OrchardHostedVendorTests`. Set
//! `VGI_RPC_FAKE_VENDOR_BIN` to vgi-rpc-python's `vgi-rpc-fake-vendor`
//! executable to run it (CI's hosted-mode job does); it skips otherwise.

#![cfg(all(feature = "http", feature = "oauth"))]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use vgi_client::dtos::{
    ArgumentsLocation, BindRequest, BindResponse, CatalogAttachRequest, CatalogAttachResult,
    Nested, PlanResponse, TableFunctionPlanRequest,
};
use vgi_client::transport::HttpTransport;
use vgi_client::wire_call::{call, envelope};
use vgi_client::{ArgValue, Arguments};
use vgi_protocol::generated::request_params as p;
use vgi_rpc::external::UploadRequest;
use vgi_rpc::DictString;

const ISSUER: &str = "https://auth.orchard.test";
const AUDIENCE: &str = "https://desk.orchard.test/d/dep_123";
const SIGNING_KEY: &str = "orchard-hosted-test-signing-key-0001";
const KID: &str = "desk-es256-1";
const VENDOR_TOKEN: &str = "test-token";
const SPLITS: i64 = 400;

/// Environment the worker must not inherit from the test's own.
const SCRUBBED_ENV: [&str; 16] = [
    "VGI_SIGNING_KEY_MINTED",
    "VGI_BEARER_TOKENS",
    "VGI_OPTIONAL_BEARER_TOKENS",
    "VGI_JWT_ISSUER",
    "VGI_JWT_AUDIENCE",
    "VGI_JWT_JWKS_URI",
    "VGI_JWT_ALGORITHMS",
    "VGI_RPC_GRANT_KEYS",
    "VGI_OAUTH_RESOURCE",
    "VGI_PROXY_PROOF_MODE",
    "VGI_INTROSPECT_PRINCIPALS",
    "VGI_ORCHARD_HOSTED",
    "VGI_SIGNING_KEY",
    "VGI_RPC_MAX_REQUEST_BYTES",
    "VGI_ARGUMENTS_REF_BYTES",
    "VGI_TEST_DISABLE_ARGUMENTS_REF",
];

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

/// Kill a child process on drop.
struct Killed(Child);

impl Drop for Killed {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Read stdout lines until one starts with `prefix`; keep draining afterwards.
fn read_announcement(child: &mut Child, prefix: &str, what: &str) -> String {
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).expect("read stdout");
        assert!(n > 0, "{what} exited before printing {prefix}");
        if let Some(rest) = line.trim().strip_prefix(prefix) {
            let value = rest.trim().to_string();
            std::thread::spawn(move || {
                let mut sink = String::new();
                while reader.read_line(&mut sink).map(|n| n > 0).unwrap_or(false) {
                    sink.clear();
                }
            });
            return value;
        }
    }
}

// ---------------------------------------------------------------------------
// The Front Desk: an ES256 key, its JWKS on loopback, and Passes
// ---------------------------------------------------------------------------

struct Desk {
    encoding: EncodingKey,
    jwks_uri: String,
}

fn desk() -> Desk {
    let alg = &ECDSA_P256_SHA256_FIXED_SIGNING;
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(alg, &SystemRandom::new()).unwrap();
    let pair = EcdsaKeyPair::from_pkcs8(alg, pkcs8.as_ref()).unwrap();
    let point = pair.public_key().as_ref();
    let b64 = |b: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b);
    let body = json!({ "keys": [{
        "kty": "EC", "crv": "P-256", "kid": KID,
        "x": b64(&point[1..33]), "y": b64(&point[33..]),
    }] })
    .to_string();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
        }
    });
    Desk {
        encoding: EncodingKey::from_ec_der(pkcs8.as_ref()),
        jwks_uri: format!("http://127.0.0.1:{port}/jwks"),
    }
}

impl Desk {
    fn pass(&self, sub: &str) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let claims = json!({
            "iss": ISSUER, "aud": AUDIENCE, "sub": sub,
            "iat": now, "nbf": now, "exp": now + 900, "jti": format!("jti-{sub}-{now}"),
        });
        let mut header = Header::new(Algorithm::ES256);
        header.typ = Some("at+jwt".into());
        header.kid = Some(KID.into());
        jsonwebtoken::encode(&header, &claims, &self.encoding).unwrap()
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

fn agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .http_status_as_error(false)
        .timeout_global(Some(Duration::from_secs(60)))
        .build()
        .into()
}

/// One raw request; the status and the headers.
fn raw(method: &str, url: &str, token: Option<&str>) -> (u16, Vec<(String, String)>) {
    let mut req = ureq::http::Request::builder().method(method).uri(url);
    if let Some(t) = token {
        req = req.header("Authorization", format!("Bearer {t}"));
    }
    let resp = agent().run(req.body(()).unwrap()).expect("request");
    let headers = resp
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();
    (resp.status().as_u16(), headers)
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> &'a str {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
        .unwrap_or("")
}

fn vendor_stats(vendor: &str) -> Value {
    let mut resp = agent()
        .get(format!("{vendor}/__stats__"))
        .call()
        .expect("vendor stats");
    serde_json::from_str(&resp.body_mut().read_to_string().unwrap()).unwrap()
}

/// A vgi.v3 connection as `pass`'s holder, resolving externalized results.
fn connect(url: &str, pass: &str) -> HttpTransport {
    let client = vgi_rpc_client::HttpClient::connect(url.to_string())
        .protocol(vgi_protocol::VGI_PROTOCOL_NAME)
        .protocol_version(vgi_protocol::VGI_PROTOCOL_VERSION)
        .header("Authorization", &format!("Bearer {pass}"))
        .unwrap()
        .external_resolution_any()
        .timeout(Some(Duration::from_secs(60)))
        .build()
        .expect("http client");
    HttpTransport::new(client, url.to_string())
}

/// Attach `blob_test` as the connection's principal: the attach handle is
/// sealed for the caller, so every principal attaches for itself.
fn attach(tr: &mut HttpTransport) -> vgi_rpc::Bytes {
    let request = envelope(CatalogAttachRequest {
        name: "blob_test".into(),
        options: None,
        data_version_spec: None,
        implementation_version: None,
        client_capabilities: None,
    })
    .unwrap();
    let info: CatalogAttachResult =
        call(tr, "catalog_attach", p::CatalogAttachParams { request }).expect("attach");
    info.attach_opaque_data
}

fn bind_request(attach: vgi_rpc::Bytes, location: ArgumentsLocation) -> BindRequest {
    BindRequest {
        function_name: "blob_param_streams".into(),
        arguments: None,
        function_type: DictString("table".into()),
        input_schema: None,
        settings: None,
        secrets: None,
        attach_opaque_data: Some(attach),
        transaction_opaque_data: None,
        resolved_secrets_provided: false,
        at_unit: None,
        at_value: None,
        copy_from: None,
        copy_to: None,
        schema_path: Some(vec!["main".into()]),
        argument_names: None,
        arguments_location: Some(location),
    }
}

fn bind(tr: &mut HttpTransport, request: BindRequest) -> vgi_rpc::Result<BindResponse> {
    call(
        tr,
        "bind",
        p::BindParams {
            request: envelope(request)?,
        },
    )
}

fn plan(tr: &mut HttpTransport, request: BindRequest) -> vgi_rpc::Result<PlanResponse> {
    let request = TableFunctionPlanRequest {
        bind_call: Nested::from_bytes(envelope(request)?.0),
        bind_opaque_data: None,
        projection_ids: None,
        pushdown_filters: None,
        join_keys: None,
        row_limit: None,
        target_split_bytes: None,
        min_splits: None,
        max_splits_per_response: None,
        cursor: None,
        refined_filters: None,
        filters_complete: true,
        start_position: None,
        end_position: None,
        order_by_column_name: None,
        order_by_direction: None,
        order_by_null_order: None,
        order_by_limit: None,
        tablesample_percentage: None,
        tablesample_seed: None,
    };
    call(
        tr,
        "table_function_plan",
        p::TableFunctionPlanParams {
            request: envelope(request)?,
        },
    )
}

#[test]
fn grants_bind_to_the_pass_principal_and_large_results_are_externalized() {
    let Some(exe) = example_worker() else {
        eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
        return;
    };
    let Some(vendor_bin) = std::env::var_os("VGI_RPC_FAKE_VENDOR_BIN").filter(|v| !v.is_empty())
    else {
        eprintln!(
            "skipping: set VGI_RPC_FAKE_VENDOR_BIN to vgi-rpc-python's vgi-rpc-fake-vendor to run \
             the hosted blob-vendor test"
        );
        return;
    };

    // The fake vendor, with its test routes (__stats__).
    let mut vendor = Command::new(vendor_bin)
        .args([
            "--test-routes",
            "--token",
            &format!("{VENDOR_TOKEN}=tenant-a"),
        ])
        .args(["--port", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the fake vendor");
    let vendor_url = read_announcement(&mut vendor, "URL:", "the fake vendor");
    let _vendor = Killed(vendor);

    // The worker, hosted, with the vendor and a 4 KiB externalization threshold.
    let desk = desk();
    let mut cmd = Command::new(exe);
    for name in SCRUBBED_ENV {
        cmd.env_remove(name);
    }
    let mut worker = cmd
        .arg("--http")
        .env("VGI_ORCHARD_HOSTED", "1")
        .env("VGI_JWT_ISSUER", ISSUER)
        .env("VGI_JWT_AUDIENCE", AUDIENCE)
        .env("VGI_JWT_JWKS_URI", &desk.jwks_uri)
        .env("VGI_SIGNING_KEY", SIGNING_KEY)
        .env("VGI_BLOB_VENDOR_URL", &vendor_url)
        .env("VGI_BLOB_VENDOR_TOKEN", VENDOR_TOKEN)
        .env("VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES", "4096")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn the worker");
    let port = read_announcement(&mut worker, "PORT:", "the hosted worker");
    let _worker = Killed(worker);
    let url = format!("http://127.0.0.1:{port}");
    let alice = desk.pass("user_alice");
    let bob = desk.pass("user_bob");

    // The capability headers, behind the Pass; a 401 tells the same truth.
    for (what, token, status) in [
        ("with a Pass", Some(alice.as_str()), 200),
        ("without one", None, 401),
    ] {
        let (got, headers) = raw("GET", &format!("{url}/health"), token);
        assert_eq!(got, status, "GET /health {what}");
        assert_eq!(header(&headers, "vgi-upload-url-support"), "2", "{what}");
        assert_eq!(
            header(&headers, "vgi-arguments-ref-bytes"),
            "4194304",
            "{what}"
        );
        assert_eq!(
            header(&headers, "vgi-externalization-enabled"),
            "true",
            "{what}"
        );
        assert_eq!(
            header(&headers, "vgi-max-request-bytes"),
            "33554432",
            "{what}"
        );
    }

    // A grant: object_id and ref; the arguments go up to the vendor's signed URL.
    let arguments = Arguments::new()
        .positional(ArgValue::Text("p".repeat(5000)))
        .positional(ArgValue::Int(SPLITS))
        .to_ipc()
        .unwrap()
        .0;
    let sha256 = format!("{:x}", Sha256::digest(&arguments));
    let grant = {
        let mut client = vgi_rpc_client::HttpClient::connect(url.clone())
            .protocol(vgi_protocol::VGI_PROTOCOL_NAME)
            .protocol_version(vgi_protocol::VGI_PROTOCOL_VERSION)
            .header("Authorization", &format!("Bearer {alice}"))
            .unwrap()
            .build()
            .unwrap();
        let mut grants = client
            .request_upload_urls(&[UploadRequest {
                size: arguments.len() as u64,
                sha256: sha256.clone(),
            }])
            .expect("upload grant");
        assert_eq!(grants.len(), 1);
        grants.remove(0)
    };
    assert!(!grant.object_id.is_empty(), "the grant has no object_id");
    assert!(!grant.argument_ref.is_empty(), "the grant has no ref");
    let mut put = ureq::http::Request::builder()
        .method("PUT")
        .uri(&grant.upload_url);
    for (k, v) in &grant.headers {
        put = put.header(k.as_str(), v.as_str());
    }
    let resp = agent()
        .run(put.body(arguments.clone()).unwrap())
        .expect("PUT");
    assert!(resp.status().is_success(), "vendor PUT: {}", resp.status());
    let location = ArgumentsLocation {
        r#ref: vgi_rpc::Bytes(grant.argument_ref.clone()),
        size: arguments.len() as i64,
        sha256,
    };

    // The ref opens for the principal it was granted to, and for no other.
    let mut alice_tr = connect(&url, &alice);
    let alice_attach = attach(&mut alice_tr);
    bind(
        &mut alice_tr,
        bind_request(alice_attach.clone(), location.clone()),
    )
    .expect("alice binds with her ref");
    let mut bob_tr = connect(&url, &bob);
    let bob_attach = attach(&mut bob_tr);
    let refused = bind(&mut bob_tr, bind_request(bob_attach, location.clone()))
        .expect_err("bob's bind with alice's ref must be refused");
    assert!(
        format!("{refused:?}").contains("invalid_arguments_ref"),
        "bob's bind was refused for the wrong reason: {refused:?}"
    );

    // A plan of 400 splits is far over the 4096-byte threshold: a vendor
    // pointer the client fetches, built from the arguments behind the ref.
    let before = vendor_stats(&vendor_url);
    let response = plan(&mut alice_tr, bind_request(alice_attach, location)).expect("alice plans");
    assert_eq!(response.splits.len(), SPLITS as usize);
    let after = vendor_stats(&vendor_url);
    assert!(
        after["puts_rpc"].as_i64() > before["puts_rpc"].as_i64(),
        "the plan result was not externalized: {before} -> {after}"
    );
    assert!(
        after["gets_upload"].as_i64().unwrap_or(0) >= 1,
        "the argument object was never fetched: {after}"
    );
}

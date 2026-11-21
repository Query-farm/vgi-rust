// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Orchard hosted mode (`VGI_ORCHARD_HOSTED=1`), end to end.
//!
//! Runs the example worker with `--http` in hosted mode against a loopback
//! JWKS holding a freshly generated ES256 key, then checks the contract in
//! `vgi::hosted` (the port of vgi-python's `tests/test_orchard_hosted.py`):
//!
//! - a valid Pass reaches function code (`pass_claims`), with claims in their
//!   JSON types;
//! - no Pass, a tampered claim, the wrong `aud`, an expired Pass, the wrong
//!   `typ`, HS256 and the rest all get 401 with
//!   `WWW-Authenticate: Bearer error="invalid_token"`, the capability headers
//!   and no `resource_metadata`; `HEAD /health` alone is anonymous;
//! - the `Authorization` header never reaches the logs, even at debug;
//! - every misconfiguration refuses to start, with a non-zero exit.

#![cfg(all(feature = "http", feature = "oauth"))]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow_array::{cast::AsArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde_json::{json, Value};
use vgi_client::auth::BearerAuth;
use vgi_client::{
    ArgValue, Arguments, AttachOptions, BindSpec, FunctionType, ScanOptions, VgiClient,
};

const ISSUER: &str = "https://auth.orchard.test";
const AUDIENCE: &str = "https://desk.orchard.test/d/dep_123";
const SIGNING_KEY: &str = "orchard-hosted-test-signing-key-0001";
const KID: &str = "desk-es256-1";
/// Never contacted: every refusal happens before the first request.
const UNUSED_JWKS: &str = "http://127.0.0.1:9/jwks";

/// Environment the worker must not inherit from the test's own.
const AUTH_ENV: [&str; 13] = [
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

macro_rules! worker_or_skip {
    () => {
        match example_worker() {
            Some(exe) => exe,
            None => {
                eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
                return;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// The Front Desk's key, its JWKS endpoint, and Passes
// ---------------------------------------------------------------------------

struct DeskKey {
    encoding: EncodingKey,
    jwk: Value,
}

fn desk_key() -> &'static DeskKey {
    static KEY: OnceLock<DeskKey> = OnceLock::new();
    KEY.get_or_init(|| {
        let alg = &ECDSA_P256_SHA256_FIXED_SIGNING;
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(alg, &SystemRandom::new()).unwrap();
        let pair = EcdsaKeyPair::from_pkcs8(alg, pkcs8.as_ref()).unwrap();
        let point = pair.public_key().as_ref();
        let b64 = |b: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b);
        DeskKey {
            encoding: EncodingKey::from_ec_der(pkcs8.as_ref()),
            jwk: json!({
                "kty": "EC", "crv": "P-256", "kid": KID,
                "x": b64(&point[1..33]), "y": b64(&point[33..]),
            }),
        }
    })
}

/// Serve the JWKS on loopback for the life of the test process.
fn jwks_uri() -> &'static str {
    static URI: OnceLock<String> = OnceLock::new();
    URI.get_or_init(|| {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let body = json!({ "keys": [desk_key().jwk] }).to_string();
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
        format!("http://127.0.0.1:{port}/jwks")
    })
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn claims() -> serde_json::Map<String, Value> {
    let now = now();
    json!({
        "iss": ISSUER, "aud": AUDIENCE, "sub": "user_01M7KNJYC18T", "kind": "user",
        "email": "ada@example.com", "email_verified": true, "name": "Ada Lovelace",
        "org_id": "org_01M7", "org_name": "Acme Analytics", "client_id": "vgi-duckdb",
        "iat": now, "nbf": now, "exp": now + 900,
        "jti": format!("jti-{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()),
    })
    .as_object()
    .unwrap()
    .clone()
}

fn pass_with(typ: &str, edit: impl FnOnce(&mut serde_json::Map<String, Value>)) -> String {
    let mut c = claims();
    edit(&mut c);
    let mut header = Header::new(Algorithm::ES256);
    header.typ = Some(typ.into());
    header.kid = Some(KID.into());
    use_token(jsonwebtoken::encode(&header, &c, &desk_key().encoding).unwrap())
}

fn pass() -> String {
    pass_with("at+jwt", |_| {})
}

/// Every token this module sends, for the log-leak check.
fn used_tokens() -> &'static Mutex<Vec<String>> {
    static USED: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
    USED.get_or_init(|| Mutex::new(Vec::new()))
}

fn use_token(token: String) -> String {
    used_tokens().lock().unwrap().push(token.clone());
    token
}

fn tamper(token: &str, name: &str, value: Value) -> String {
    let parts: Vec<&str> = token.split('.').collect();
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let mut c: serde_json::Map<String, Value> =
        serde_json::from_slice(&engine.decode(parts[1]).unwrap()).unwrap();
    c.insert(name.into(), value);
    let forged = engine.encode(serde_json::to_vec(&c).unwrap());
    use_token(format!("{}.{forged}.{}", parts[0], parts[2]))
}

// ---------------------------------------------------------------------------
// A running hosted worker
// ---------------------------------------------------------------------------

fn hosted_command(exe: &PathBuf, jwks: &str) -> Command {
    let mut cmd = Command::new(exe);
    for name in AUTH_ENV {
        cmd.env_remove(name);
    }
    cmd.env("VGI_ORCHARD_HOSTED", "1")
        .env("VGI_JWT_ISSUER", ISSUER)
        .env("VGI_JWT_AUDIENCE", AUDIENCE)
        .env("VGI_JWT_JWKS_URI", jwks)
        .env("VGI_SIGNING_KEY", SIGNING_KEY);
    cmd
}

/// The hosted worker, at debug log level with stderr in a file; killed on drop.
struct HostedWorker {
    child: Child,
    port: u16,
    log: PathBuf,
}

impl HostedWorker {
    fn start(exe: &PathBuf) -> Self {
        let log = std::env::temp_dir().join(format!(
            "vgi-orchard-hosted-{}-{}.log",
            std::process::id(),
            now()
        ));
        let mut child = hosted_command(exe, jwks_uri())
            .arg("--http")
            .env("VGI_LOG", "debug")
            .env("RUST_LOG", "debug")
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .expect("spawn worker");
        let mut reader = BufReader::new(child.stdout.take().expect("stdout"));
        let mut line = String::new();
        let port = loop {
            line.clear();
            let n = reader.read_line(&mut line).expect("read PORT line");
            assert!(
                n > 0,
                "hosted worker exited before announcing a port:\n{}",
                std::fs::read_to_string(&log).unwrap_or_default()
            );
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
        HostedWorker { child, port, log }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// One raw request; returns the status and the lower-cased headers.
    fn request(
        &self,
        method: &str,
        path: &str,
        token: Option<&str>,
        headers: &[(&str, &str)],
    ) -> (u16, Vec<(String, String)>) {
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .timeout_global(Some(Duration::from_secs(30)))
            .build()
            .into();
        let uri = format!("{}{path}", self.url());
        let mut req = ureq::http::Request::builder().method(method).uri(&uri);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        if let Some(t) = token {
            req = req.header("Authorization", format!("Bearer {t}"));
        }
        let resp = agent.run(req.body(()).unwrap()).expect("request");
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        (status, headers)
    }
}

impl Drop for HostedWorker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.log);
    }
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

fn assert_401(status: u16, headers: &[(String, String)], what: &str) {
    assert_eq!(status, 401, "{what}");
    let challenge = header(headers, "www-authenticate").unwrap_or("");
    assert_eq!(challenge, "Bearer error=\"invalid_token\"", "{what}");
    assert!(header(headers, "vgi-auth-reason").is_some(), "{what}");
    assert!(
        header(headers, "vgi-supported-encodings").is_some(),
        "{what}"
    );
}

/// Call `pass_claims` as the Pass's holder and parse its JSON answer.
fn pass_claims(worker: &HostedWorker, token: &str) -> vgi_rpc::Result<Value> {
    let mut client =
        VgiClient::connect_http_with_auth(&worker.url(), Arc::new(BearerAuth::new(token)), None);
    let cat = client.attach("example", AttachOptions::default())?;
    let mut spec = BindSpec::table("pass_claims").in_schema("main");
    spec.function_type = FunctionType::Scalar;
    spec.arguments = Arguments::new().positional(ArgValue::Placeholder(DataType::Int64));
    let schema = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
    let bound = client.bind_with_input(&cat, &spec, &schema)?;
    let mut ex = client.open_exchange(&bound, &ScanOptions::default())?;
    let input =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
    let out = ex.send(&input)?.expect("an answer");
    let text = out.column(0).as_string::<i32>().value(0).to_string();
    ex.close()?;
    Ok(serde_json::from_str(&text).expect("pass_claims returns JSON"))
}

#[test]
fn a_hosted_worker_serves_only_passes() {
    let exe = worker_or_skip!();
    let worker = HostedWorker::start(&exe);

    // A valid Pass reaches function code, claims in their JSON types.
    let result = pass_claims(&worker, &pass()).expect("pass_claims");
    assert_eq!(result["principal"], "user_01M7KNJYC18T");
    assert_eq!(result["domain"], "orchard");
    let c = &result["claims"];
    assert_eq!(c["email_verified"], json!(true));
    assert!(c["exp"].is_i64(), "{c}");
    assert_eq!(c["kind"], "user");
    assert_eq!(c["email"], "ada@example.com");
    assert_eq!(c["name"], "Ada Lovelace");
    assert_eq!(c["org_id"], "org_01M7");
    assert_eq!(c["org_name"], "Acme Analytics");
    assert_eq!(c["client_id"], "vgi-duckdb");

    // HEAD /health is the only anonymous request.
    let (status, headers) = worker.request("HEAD", "/health", None, &[]);
    assert_eq!(status, 200);
    assert!(header(&headers, "vgi-supported-encodings").is_some());

    // No Pass is never anonymous.
    let origin = [("Origin", "https://shell.example.com")];
    for (method, path) in [
        ("GET", "/health"),
        ("OPTIONS", "/health"),
        ("GET", "/"),
        ("GET", "/describe"),
        ("GET", "/no/such/path"),
        ("POST", "/vgi.v3/catalog_attach"),
    ] {
        let (status, headers) = worker.request(method, path, None, &origin);
        assert_401(status, &headers, &format!("{method} {path}"));
        assert_eq!(
            header(&headers, "vgi-auth-reason"),
            Some("missing_credential")
        );
        assert!(!header(&headers, "www-authenticate")
            .unwrap()
            .contains("resource_metadata"));
    }

    // Every invalid Pass gets the same 401.
    let now = now();
    let hs256 = {
        let mut h = Header::new(Algorithm::HS256);
        h.typ = Some("at+jwt".into());
        h.kid = Some(KID.into());
        use_token(
            jsonwebtoken::encode(&h, &claims(), &EncodingKey::from_secret(&[b'k'; 32])).unwrap(),
        )
    };
    let cases: Vec<(&str, String)> = vec![
        ("garbage", use_token("not-a-jwt".into())),
        ("tampered", tamper(&pass(), "org_id", json!("org_EVIL"))),
        (
            "wrong_aud",
            pass_with("at+jwt", |c| {
                c.insert("aud".into(), json!("https://desk.orchard.test/d/dep_other"));
            }),
        ),
        (
            "multi_aud",
            pass_with("at+jwt", |c| {
                c.insert(
                    "aud".into(),
                    json!([AUDIENCE, "https://desk.orchard.test/d/dep_other"]),
                );
            }),
        ),
        (
            "expired",
            pass_with("at+jwt", |c| {
                c.insert("iat".into(), json!(now - 1200));
                c.insert("nbf".into(), json!(now - 1200));
                c.insert("exp".into(), json!(now - 120));
            }),
        ),
        ("wrong_typ", pass_with("JWT", |_| {})),
        ("hs256", hs256),
        (
            "wrong_iss",
            pass_with("at+jwt", |c| {
                c.insert("iss".into(), json!("https://evil.test"));
            }),
        ),
        (
            "no_exp",
            pass_with("at+jwt", |c| {
                c.remove("exp");
            }),
        ),
    ];
    for (name, token) in &cases {
        let (status, headers) =
            worker.request("POST", "/vgi.v3/catalog_attach", Some(token), &origin);
        assert_401(status, &headers, name);
        let expected = if *name == "expired" {
            "expired_credential"
        } else {
            "invalid_credential"
        };
        assert_eq!(
            header(&headers, "vgi-auth-reason"),
            Some(expected),
            "{name}"
        );
    }
    // A bad Pass cannot call a function either.
    assert!(pass_claims(&worker, &cases[1].1).is_err());

    // CORS is open; a preflight needs no Pass.
    let (status, headers) = worker.request(
        "OPTIONS",
        "/vgi.v3/catalog_attach",
        None,
        &[
            ("Origin", "https://shell.example.com"),
            ("Access-Control-Request-Method", "POST"),
            ("Access-Control-Request-Headers", "authorization"),
        ],
    );
    assert_eq!(status, 200);
    assert_eq!(header(&headers, "access-control-allow-origin"), Some("*"));

    // A valid Pass on GET /health works.
    let (status, _) = worker.request("GET", "/health", Some(&pass()), &[]);
    assert_eq!(status, 200);

    // Nothing this test sent reaches the worker's debug log.
    std::thread::sleep(Duration::from_millis(300));
    let text = std::fs::read_to_string(&worker.log).unwrap();
    assert!(
        !text.is_empty(),
        "expected the worker's debug log to be captured"
    );
    for token in used_tokens().lock().unwrap().iter() {
        assert!(!text.contains(token.as_str()), "a token reached the log");
        if token.matches('.').count() == 2 {
            let signature = token.rsplit('.').next().unwrap();
            assert!(
                !text.contains(signature),
                "a token signature reached the log"
            );
        }
    }
    assert!(
        !text.contains("Bearer "),
        "an Authorization header reached the log"
    );
}

// ---------------------------------------------------------------------------
// Startup refusals
// ---------------------------------------------------------------------------

/// Run the worker with `edit` applied to the hosted environment and `args`;
/// assert it refuses to start and return its stderr.
fn refused(exe: &PathBuf, args: &[&str], edit: impl FnOnce(&mut Command)) -> String {
    let mut cmd = hosted_command(exe, UNUSED_JWKS);
    cmd.args(args);
    edit(&mut cmd);
    let out = cmd.stdin(Stdio::null()).output().expect("run worker");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(
        !out.status.success(),
        "worker started when it should have refused ({args:?}):\n{stdout}\n{stderr}"
    );
    assert!(!stdout.contains("PORT:"), "{stdout}");
    stderr
}

#[test]
fn every_misconfiguration_refuses_to_start() {
    let exe = worker_or_skip!();
    let http = ["--http"];
    for name in ["VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"] {
        let unset = refused(&exe, &http, |c| {
            c.env_remove(name);
        });
        assert!(unset.contains(name), "unset {name}: {unset}");
        let empty = refused(&exe, &http, |c| {
            c.env(name, "  ");
        });
        assert!(empty.contains(name), "empty {name}: {empty}");
    }
    let unset = refused(&exe, &http, |c| {
        c.env_remove("VGI_SIGNING_KEY");
    });
    assert!(unset.contains("VGI_SIGNING_KEY"), "{unset}");
    let short = refused(&exe, &http, |c| {
        c.env("VGI_SIGNING_KEY", "short-key");
    });
    assert!(
        short.contains("VGI_SIGNING_KEY") && short.contains("32 bytes"),
        "{short}"
    );
    let minted = refused(&exe, &http, |c| {
        c.env(
            "VGI_SIGNING_KEY",
            "minted-by-a-parent-vgi-serve-process-0001",
        )
        .env("VGI_SIGNING_KEY_MINTED", "1");
    });
    assert!(minted.contains("VGI_SIGNING_KEY"), "{minted}");
    let comma = refused(&exe, &http, |c| {
        c.env(
            "VGI_JWT_AUDIENCE",
            format!("{AUDIENCE},https://desk.orchard.test/d/other"),
        );
    });
    assert!(
        comma.contains("VGI_JWT_AUDIENCE") && comma.contains("exactly one"),
        "{comma}"
    );
    let bearer = refused(&exe, &http, |c| {
        c.env("VGI_BEARER_TOKENS", "tok=alice");
    });
    assert!(bearer.contains("VGI_BEARER_TOKENS"), "{bearer}");
    let grant_key = base64::engine::general_purpose::STANDARD.encode((0u8..32).collect::<Vec<_>>());
    let grants_env = refused(&exe, &http, |c| {
        c.env("VGI_RPC_GRANT_KEYS", &grant_key);
    });
    assert!(grants_env.contains("VGI_RPC_GRANT_KEYS"), "{grants_env}");
    let grants_flag = refused(&exe, &["--http", "--grant-key", &grant_key], |_| {});
    assert!(
        grants_flag.to_lowercase().contains("grant"),
        "{grants_flag}"
    );
    let oauth = refused(&exe, &http, |c| {
        c.env("VGI_OAUTH_RESOURCE", "https://x.test")
            .env("VGI_OAUTH_AUTH_SERVERS", ISSUER);
    });
    assert!(oauth.contains("VGI_OAUTH_RESOURCE"), "{oauth}");
    let hs = refused(&exe, &http, |c| {
        c.env("VGI_JWT_ALGORITHMS", "ES256,HS256");
    });
    assert!(hs.contains("VGI_JWT_ALGORITHMS"), "{hs}");
    let typo = refused(&exe, &http, |c| {
        c.env("VGI_ORCHARD_HOSTED", "yes please");
    });
    assert!(typo.contains("VGI_ORCHARD_HOSTED"), "{typo}");
    // The fixture's --identity mode installs its own authenticator and
    // resolve_token: a custom authenticate callback.
    let custom = refused(&exe, &["--http", "--identity"], |_| {});
    assert!(custom.contains("authenticate"), "{custom}");
    // Transports that carry no Pass.
    let sock = std::env::temp_dir().join(format!("vgi-orchard-refuse-{}.sock", std::process::id()));
    for args in [
        vec!["--tcp", "127.0.0.1:0"],
        vec!["--unix", sock.to_str().unwrap()],
        vec!["--iroh-raw-upstream", "127.0.0.1:0", "--iroh-issuer", "x"],
        vec![],
        vec!["--http", "--iroh-issuer", "x"],
    ] {
        let stderr = refused(&exe, &args, |_| {});
        assert!(stderr.contains("only --http"), "{args:?}: {stderr}");
    }
}

#[test]
fn hosted_mode_off_values_serve_normally() {
    let exe = worker_or_skip!();
    for value in ["0", "false", "", "off"] {
        let mut child = Command::new(&exe)
            .arg("--http")
            .env("VGI_ORCHARD_HOSTED", value)
            .env_remove("VGI_BEARER_TOKENS")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let _ = child.kill();
        let _ = child.wait();
        assert!(line.starts_with("PORT:"), "{value:?}: {line}");
    }
}

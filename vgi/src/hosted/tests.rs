// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! In-process tests of Orchard hosted mode: configuration, startup conflicts
//! and Pass verification against a JWKS served by an injected fetch.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
use base64::Engine;
use jsonwebtoken::{EncodingKey, Header};
use serde_json::{json, Value};

use super::*;
use crate::worker::ServeTransport;

const ISSUER: &str = "https://auth.orchard.test";
const AUDIENCE: &str = "https://desk.orchard.test/d/dep_123";
const JWKS: &str = "https://auth.orchard.test/jwks";
const SIGNING_KEY: &str = "orchard-hosted-test-signing-key-0001";

/// An ES256 key: its signer and its public JWK.
struct EcKey {
    encoding: EncodingKey,
    jwk: Value,
}

fn ec_key(kid: &str) -> EcKey {
    let alg = &ECDSA_P256_SHA256_FIXED_SIGNING;
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(alg, &SystemRandom::new()).unwrap();
    let pair = EcdsaKeyPair::from_pkcs8(alg, pkcs8.as_ref()).unwrap();
    let point = pair.public_key().as_ref();
    assert_eq!(point.len(), 65, "uncompressed P-256 point");
    let b64 = |b: &[u8]| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b);
    EcKey {
        encoding: EncodingKey::from_ec_der(pkcs8.as_ref()),
        jwk: json!({
            "kty": "EC", "crv": "P-256", "kid": kid, "alg": "ES256", "use": "sig",
            "x": b64(&point[1..33]), "y": b64(&point[33..]),
        }),
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn claims() -> serde_json::Map<String, Value> {
    let now = now();
    json!({
        "iss": ISSUER, "aud": AUDIENCE, "sub": "user_01M7KNJYC18T", "kind": "user",
        "email": "ada@example.com", "email_verified": true, "name": "Ada Lovelace",
        "org_id": "org_01M7", "org_name": "Acme Analytics", "client_id": "vgi-duckdb",
        "federated_iss": "https://accounts.google.com", "federated_sub": "1234",
        "iat": now, "nbf": now, "exp": now + 900, "jti": "jti-1",
    })
    .as_object()
    .unwrap()
    .clone()
}

fn mint(
    key: &EcKey,
    kid: &str,
    typ: &str,
    edit: impl FnOnce(&mut serde_json::Map<String, Value>),
) -> String {
    let mut c = claims();
    edit(&mut c);
    let mut header = Header::new(Algorithm::ES256);
    header.typ = Some(typ.into());
    header.kid = Some(kid.into());
    jsonwebtoken::encode(&header, &c, &key.encoding).unwrap()
}

fn config() -> HostedConfig {
    HostedConfig {
        issuer: ISSUER.into(),
        audience: AUDIENCE.into(),
        jwks_uri: JWKS.into(),
        algorithms: vec![Algorithm::ES256],
    }
}

/// A JWKS endpoint the test controls: its keys, its `max-age`, whether it is
/// down, and how often it was asked.
#[derive(Clone)]
struct FakeJwks {
    keys: Arc<Mutex<Vec<Value>>>,
    max_age: Arc<Mutex<Option<u64>>>,
    down: Arc<AtomicBool>,
    fetches: Arc<AtomicUsize>,
}

impl FakeJwks {
    fn new(keys: Vec<Value>) -> Self {
        FakeJwks {
            keys: Arc::new(Mutex::new(keys)),
            max_age: Arc::new(Mutex::new(None)),
            down: Arc::new(AtomicBool::new(false)),
            fetches: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn fetch(&self) -> JwksFetch {
        let me = self.clone();
        Arc::new(move |uri: &str| {
            assert_eq!(uri, JWKS);
            me.fetches.fetch_add(1, Ordering::SeqCst);
            if me.down.load(Ordering::SeqCst) {
                return Err("down".into());
            }
            let body = json!({ "keys": *me.keys.lock().unwrap() }).to_string();
            Ok((parse_jwks(&body)?, *me.max_age.lock().unwrap()))
        })
    }
}

fn call(auth: &Authenticate, authorization: Option<&str>) -> AuthResult {
    let headers: Vec<(String, String)> = authorization
        .map(|v| vec![("authorization".to_string(), v.to_string())])
        .unwrap_or_default();
    auth(&AuthRequest {
        method: "x",
        headers: &headers,
        peer_addr: None,
    })
}

fn bearer(auth: &Authenticate, token: &str) -> AuthResult {
    call(auth, Some(&format!("Bearer {token}")))
}

fn reason(result: AuthResult) -> &'static str {
    let err = result.expect_err("expected a rejection");
    assert!(!err.is_auth_unavailable(), "unexpected outage: {err}");
    err.auth_reason.expect("classified").as_str()
}

/// Rewind a verifier's clock-dependent state by `by`: makes the cached keys
/// and the last fetch look that much older.
fn age_cache(auth_state: &Arc<Verifier>, by: Duration) {
    let mut cache = auth_state.cache.lock().unwrap();
    if let Some(entry) = cache.entry.as_mut() {
        entry.expires_at -= by;
    }
    if let Some(t) = cache.last_fetch_attempt.as_mut() {
        *t -= by;
    }
}

fn verifier(jwks: &FakeJwks) -> (Arc<Verifier>, Authenticate) {
    let v = Arc::new(Verifier {
        config: config(),
        fetch: jwks.fetch(),
        cache: Mutex::new(KeyCache {
            entry: None,
            last_fetch_attempt: None,
            generation: 0,
        }),
    });
    let inner = v.clone();
    (
        v,
        Arc::new(move |req: &AuthRequest<'_>| inner.authenticate(req)),
    )
}

// -- Configuration ----------------------------------------------------------

fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let map: HashMap<String, String> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |name: &str| map.get(name).cloned()
}

fn good_env() -> Vec<(&'static str, &'static str)> {
    vec![
        ("VGI_JWT_ISSUER", ISSUER),
        ("VGI_JWT_AUDIENCE", AUDIENCE),
        ("VGI_JWT_JWKS_URI", JWKS),
        ("VGI_SIGNING_KEY", SIGNING_KEY),
    ]
}

fn with(overrides: &[(&'static str, Option<&'static str>)]) -> impl Fn(&str) -> Option<String> {
    let mut pairs = good_env();
    for (name, value) in overrides {
        pairs.retain(|(k, _)| k != name);
        if let Some(v) = value {
            pairs.push((name, v));
        }
    }
    env(&pairs)
}

#[test]
fn flag_values() {
    for on in ["1", "true", "YES", " on "] {
        assert_eq!(parse_hosted_flag(Some(on)), Ok(true), "{on}");
    }
    for off in ["0", "false", "", "off", "No"] {
        assert_eq!(parse_hosted_flag(Some(off)), Ok(false), "{off}");
    }
    assert_eq!(parse_hosted_flag(None), Ok(false));
    let err = parse_hosted_flag(Some("yes please")).unwrap_err();
    assert!(err.contains(HOSTED_ENV), "{err}");
}

#[test]
fn config_requires_each_variable_once() {
    for name in ["VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"] {
        let unset = hosted_config_from(&with(&[(name, None)])).unwrap_err();
        assert!(unset.contains(name), "{unset}");
        let empty = hosted_config_from(&with(&[(name, Some("  "))])).unwrap_err();
        assert!(empty.contains(name), "{empty}");
        let listed = hosted_config_from(&with(&[(name, Some("a,b"))])).unwrap_err();
        assert!(
            listed.contains(name) && listed.contains("exactly one"),
            "{listed}"
        );
    }
    assert_eq!(hosted_config_from(&with(&[])).unwrap(), config());
}

#[test]
fn algorithm_override_refuses_none_and_hmac() {
    for bad in ["ES256,HS256", "none", "hs512", " , "] {
        let err = hosted_config_from(&with(&[("VGI_JWT_ALGORITHMS", Some(bad))])).unwrap_err();
        assert!(err.contains("VGI_JWT_ALGORITHMS"), "{bad}: {err}");
    }
    let ok = hosted_config_from(&with(&[("VGI_JWT_ALGORITHMS", Some("ES256, ES384"))])).unwrap();
    assert_eq!(ok.algorithms, vec![Algorithm::ES256, Algorithm::ES384]);
}

fn surface(transport: ServeTransport) -> WorkerSurface<'static> {
    WorkerSurface {
        transport,
        authenticate: false,
        resolve_token: false,
        mint_grant: false,
        grant_keys: false,
        explicit_signing_key: None,
    }
}

#[test]
fn a_clean_http_worker_has_no_conflict() {
    assert_eq!(
        worker_conflict(&surface(ServeTransport::Http), &with(&[])),
        None
    );
}

#[test]
fn only_http_is_allowed() {
    for t in [
        ServeTransport::Pipe,
        ServeTransport::Unix,
        ServeTransport::Tcp,
        ServeTransport::Iroh,
    ] {
        let reason = worker_conflict(&surface(t), &with(&[])).unwrap();
        assert!(reason.contains("only --http"), "{t:?}: {reason}");
    }
}

#[test]
fn every_other_credential_refuses() {
    for (name, value) in [
        ("VGI_BEARER_TOKENS", "tok=alice"),
        ("VGI_OPTIONAL_BEARER_TOKENS", "tok=alice"),
        (
            "VGI_RPC_GRANT_KEYS",
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
        ),
        ("VGI_OAUTH_RESOURCE", "https://x.test"),
        ("VGI_PROXY_PROOF_MODE", "require"),
    ] {
        let reason = worker_conflict(
            &surface(ServeTransport::Http),
            &with(&[(name, Some(value))]),
        )
        .unwrap();
        assert!(reason.contains(name), "{name}: {reason}");
    }
    assert_eq!(
        worker_conflict(
            &surface(ServeTransport::Http),
            &with(&[("VGI_PROXY_PROOF_MODE", Some("off"))])
        ),
        None
    );
    type Set = fn(&mut WorkerSurface<'static>);
    let cases: [(Set, &str); 4] = [
        (|s| s.authenticate = true, "authenticate"),
        (|s| s.grant_keys = true, "grant"),
        (|s| s.resolve_token = true, "resolve_token"),
        (|s| s.mint_grant = true, "mint_grant"),
    ];
    for (set, needle) in cases {
        let mut s = surface(ServeTransport::Http);
        set(&mut s);
        let reason = worker_conflict(&s, &with(&[])).unwrap();
        assert!(reason.contains(needle), "{needle}: {reason}");
    }
}

#[test]
fn signing_key_must_be_configured_and_long_enough() {
    let http = surface(ServeTransport::Http);
    let unset = worker_conflict(&http, &with(&[("VGI_SIGNING_KEY", None)])).unwrap();
    assert!(unset.contains("VGI_SIGNING_KEY"), "{unset}");
    let short = worker_conflict(&http, &with(&[("VGI_SIGNING_KEY", Some("short-key"))])).unwrap();
    assert!(
        short.contains("VGI_SIGNING_KEY") && short.contains("32 bytes"),
        "{short}"
    );
    let minted = worker_conflict(&http, &with(&[("VGI_SIGNING_KEY_MINTED", Some("1"))])).unwrap();
    assert!(minted.contains("VGI_SIGNING_KEY"), "{minted}");
    // An explicit Worker::signing_key counts as configured.
    let explicit = WorkerSurface {
        explicit_signing_key: Some(&[7u8; 32]),
        ..surface(ServeTransport::Http)
    };
    assert_eq!(
        worker_conflict(&explicit, &with(&[("VGI_SIGNING_KEY", None)])),
        None
    );
}

#[test]
fn cache_control_max_age_parsing() {
    assert_eq!(cache_control_max_age("max-age=30"), Some(30));
    assert_eq!(
        cache_control_max_age("public, MAX-AGE = \"45\" , must-revalidate"),
        Some(45)
    );
    assert_eq!(cache_control_max_age("no-cache"), None);
    assert_eq!(cache_control_max_age("max-age=-1"), None);
    assert_eq!(cache_control_max_age("s-maxage=10"), None);
}

#[test]
fn typ_normalization() {
    assert_eq!(normalize_typ("AT+JWT"), "at+jwt");
    assert_eq!(normalize_typ("application/at+jwt"), "at+jwt");
    assert_eq!(normalize_typ("Application/At+Jwt"), "at+jwt");
}

// -- Pass verification -----------------------------------------------------

#[test]
fn a_valid_pass_authenticates_with_typed_claims() {
    let key = ec_key("desk-1");
    let jwks = FakeJwks::new(vec![key.jwk.clone()]);
    let (_, auth) = verifier(&jwks);
    let ctx = bearer(&auth, &mint(&key, "desk-1", "at+jwt", |_| {})).unwrap();
    assert!(ctx.authenticated);
    assert_eq!(ctx.domain, "orchard");
    assert_eq!(ctx.principal, "user_01M7KNJYC18T");
    assert_eq!(ctx.claim_value("email_verified"), Some(json!(true)));
    assert!(ctx.claim_value("exp").unwrap().is_i64());
    for name in [
        "sub",
        "kind",
        "email",
        "email_verified",
        "name",
        "org_id",
        "org_name",
        "federated_iss",
        "federated_sub",
        "client_id",
        "iat",
        "nbf",
        "exp",
        "jti",
    ] {
        assert!(ctx.claim_values.contains_key(name), "{name}");
    }
    // The application/ prefix and any letter case are accepted.
    assert!(bearer(&auth, &mint(&key, "desk-1", "application/AT+JWT", |_| {})).is_ok());
}

#[test]
fn bad_passes_are_rejected() {
    let key = ec_key("desk-1");
    let other = ec_key("desk-1"); // same kid, different key
    let jwks = FakeJwks::new(vec![key.jwk.clone()]);
    let (_, auth) = verifier(&jwks);
    let now = now();
    let good = mint(&key, "desk-1", "at+jwt", |_| {});

    let tampered = {
        let mut parts: Vec<String> = good.split('.').map(str::to_string).collect();
        let mut c = claims();
        c.insert("org_id".into(), json!("org_EVIL"));
        parts[1] = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&c).unwrap());
        parts.join(".")
    };
    let hs256 = {
        let mut header = Header::new(Algorithm::HS256);
        header.typ = Some("at+jwt".into());
        header.kid = Some("desk-1".into());
        jsonwebtoken::encode(&header, &claims(), &EncodingKey::from_secret(&[b'k'; 32])).unwrap()
    };
    // RS256 is refused by the allow-list before any key is consulted.
    let rs256 = {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"alg":"RS256","typ":"at+jwt","kid":"desk-rs"}"#);
        let mut parts: Vec<&str> = good.split('.').collect();
        parts[0] = &header;
        parts.join(".")
    };
    let cases: Vec<(&str, String, &str)> = vec![
        ("garbage", "not-a-jwt".into(), "invalid_credential"),
        ("tampered", tampered, "invalid_credential"),
        (
            "other key",
            mint(&other, "desk-1", "at+jwt", |_| {}),
            "invalid_credential",
        ),
        (
            "wrong aud",
            mint(&key, "desk-1", "at+jwt", |c| {
                c.insert("aud".into(), json!("https://desk.orchard.test/d/dep_other"));
            }),
            "invalid_credential",
        ),
        (
            "multi aud",
            mint(&key, "desk-1", "at+jwt", |c| {
                c.insert(
                    "aud".into(),
                    json!([AUDIENCE, "https://desk.orchard.test/d/other"]),
                );
            }),
            "invalid_credential",
        ),
        (
            "expired",
            mint(&key, "desk-1", "at+jwt", |c| {
                c.insert("iat".into(), json!(now - 1200));
                c.insert("nbf".into(), json!(now - 1200));
                c.insert("exp".into(), json!(now - 120));
            }),
            "expired_credential",
        ),
        (
            "not yet valid",
            mint(&key, "desk-1", "at+jwt", |c| {
                c.insert("nbf".into(), json!(now + 600));
            }),
            "invalid_credential",
        ),
        (
            "wrong typ",
            mint(&key, "desk-1", "JWT", |_| {}),
            "invalid_credential",
        ),
        ("rs256", rs256, "invalid_credential"),
        ("hs256", hs256, "invalid_credential"),
        (
            "wrong iss",
            mint(&key, "desk-1", "at+jwt", |c| {
                c.insert("iss".into(), json!("https://evil.test"));
            }),
            "invalid_credential",
        ),
        (
            "no exp",
            mint(&key, "desk-1", "at+jwt", |c| {
                c.remove("exp");
            }),
            "invalid_credential",
        ),
        (
            "string exp",
            mint(&key, "desk-1", "at+jwt", |c| {
                c.insert("exp".into(), json!((now + 900).to_string()));
            }),
            "invalid_credential",
        ),
    ];
    for (name, token, expected) in cases {
        assert_eq!(reason(bearer(&auth, &token)), expected, "{name}");
    }
    // Within the 60 s leeway, a just-expired Pass still works.
    let lenient = mint(&key, "desk-1", "at+jwt", |c| {
        c.insert("exp".into(), json!(now - 30));
    });
    assert!(bearer(&auth, &lenient).is_ok());
}

#[test]
fn missing_and_non_bearer_credentials() {
    let key = ec_key("desk-1");
    let jwks = FakeJwks::new(vec![key.jwk]);
    let (_, auth) = verifier(&jwks);
    assert_eq!(reason(call(&auth, None)), "missing_credential");
    assert_eq!(
        reason(call(&auth, Some("Basic YTpi"))),
        "invalid_credential"
    );
    assert_eq!(reason(call(&auth, Some("Bearer "))), "invalid_credential");
    // No JWKS fetch for any of them.
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 0);
}

#[test]
fn jwks_is_cached_and_never_used_stale() {
    let key = ec_key("desk-1");
    let jwks = FakeJwks::new(vec![key.jwk.clone()]);
    let (state, auth) = verifier(&jwks);
    let token = mint(&key, "desk-1", "at+jwt", |_| {});
    assert!(bearer(&auth, &token).is_ok());
    assert!(bearer(&auth, &token).is_ok());
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 1, "cached");

    // Past the 10-minute bound with the JWKS down: 503, not stale keys.
    jwks.down.store(true, Ordering::SeqCst);
    age_cache(&state, JWKS_MAX_AGE);
    for _ in 0..2 {
        let err = bearer(&auth, &token).unwrap_err();
        assert!(err.is_auth_unavailable(), "{err}");
    }
    assert!(
        state.cache.lock().unwrap().entry.is_none(),
        "stale keys dropped"
    );

    // Back up: verification resumes.
    jwks.down.store(false, Ordering::SeqCst);
    assert!(bearer(&auth, &token).is_ok());
}

#[test]
fn a_shorter_cache_control_max_age_wins() {
    let key = ec_key("desk-1");
    let jwks = FakeJwks::new(vec![key.jwk.clone()]);
    *jwks.max_age.lock().unwrap() = Some(30);
    let (state, auth) = verifier(&jwks);
    let token = mint(&key, "desk-1", "at+jwt", |_| {});
    assert!(bearer(&auth, &token).is_ok());
    age_cache(&state, Duration::from_secs(31));
    assert!(bearer(&auth, &token).is_ok());
    assert_eq!(
        jwks.fetches.load(Ordering::SeqCst),
        2,
        "refetched after max-age"
    );

    // A longer max-age is capped at 10 minutes.
    *jwks.max_age.lock().unwrap() = Some(86_400);
    age_cache(&state, Duration::from_secs(31));
    assert!(bearer(&auth, &token).is_ok());
    let expires = state
        .cache
        .lock()
        .unwrap()
        .entry
        .as_ref()
        .unwrap()
        .expires_at;
    assert!(expires <= Instant::now() + JWKS_MAX_AGE);
}

#[test]
fn an_unknown_kid_refetches_at_most_once_a_minute() {
    let old = ec_key("desk-1");
    let new = ec_key("desk-2");
    let jwks = FakeJwks::new(vec![old.jwk.clone()]);
    let (state, auth) = verifier(&jwks);
    assert!(bearer(&auth, &mint(&old, "desk-1", "at+jwt", |_| {})).is_ok());
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 1);

    // Rotation: the new key is published, but the last fetch was just now.
    jwks.keys.lock().unwrap().push(new.jwk.clone());
    let rotated = mint(&new, "desk-2", "at+jwt", |_| {});
    assert_eq!(reason(bearer(&auth, &rotated)), "invalid_credential");
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 1, "rate limited");

    // A minute later the unknown kid triggers one refetch, and verifies.
    age_cache(&state, JWKS_REFRESH_MIN_INTERVAL);
    assert!(bearer(&auth, &rotated).is_ok());
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 2);

    // A kid nobody publishes, right after: rejected without a fetch.
    let stranger = ec_key("desk-9");
    assert_eq!(
        reason(bearer(&auth, &mint(&stranger, "desk-9", "at+jwt", |_| {}))),
        "invalid_credential"
    );
    assert_eq!(jwks.fetches.load(Ordering::SeqCst), 2);
}

#[test]
fn unusable_jwks_entries_are_skipped() {
    let key = ec_key("desk-1");
    let body = json!({ "keys": [ {"kty": "weird", "kid": "x"}, key.jwk ] }).to_string();
    let set = parse_jwks(&body).unwrap();
    assert_eq!(set.keys.len(), 1);
    assert!(parse_jwks("{}").is_err());
}

#[test]
fn http_token_key_is_stable_and_key_specific() {
    assert_eq!(http_token_key(b"a"), http_token_key(b"a"));
    assert_ne!(http_token_key(b"a"), http_token_key(b"b"));
}

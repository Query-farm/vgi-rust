// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Orchard hosted mode (`VGI_ORCHARD_HOSTED=1`).
//!
//! A hosted worker (a *Field Worker*) serves VGI over HTTP to callers who hold
//! a *Pass*: a short-lived [RFC 9068] access token (`typ: at+jwt`, ES256)
//! issued by the Orchard Front Desk for exactly one canonical Desk Address.
//! The worker verifies Passes locally against the Front Desk's JWKS and
//! accepts nothing else. Everything here fails closed:
//!
//! - Startup is refused unless `VGI_JWT_ISSUER`, `VGI_JWT_AUDIENCE` and
//!   `VGI_JWT_JWKS_URI` are each set to a single non-empty value, and
//!   `VGI_SIGNING_KEY` (or [`Worker::signing_key`](crate::Worker::signing_key))
//!   is at least 32 bytes.
//! - Startup is refused when any other way in is configured: static bearer
//!   tokens, sealed-grant keys, `resolve_token` / `mint_grant`, a custom
//!   [`Worker::authenticate`](crate::Worker::authenticate), OAuth resource
//!   metadata, a proxy proof, Iroh peer identity, or a transport other than
//!   `--http`.
//! - Every request except `HEAD /health` (and CORS preflights, which carry no
//!   credentials) must present a valid Pass, or gets a 401 with
//!   `WWW-Authenticate: Bearer error="invalid_token"`. Authentication runs
//!   before the request body is read.
//! - The JWKS is cached for at most 10 minutes (less if its
//!   `Cache-Control: max-age` says so). When that passes and the re-fetch
//!   fails, requests get a 503 rather than being checked against stale keys.
//!
//! The design is in the extension repository's `docs/location-delegation.md`
//! ("The Pass" and "The Field Worker: SDK hosted mode"); the reference
//! implementation is vgi-python's `vgi.hosted`.
//!
//! [RFC 9068]: https://www.rfc-editor.org/rfc/rfc9068

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::{Jwk, JwkSet};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use vgi_rpc::unauthorized::AuthReason;
use vgi_rpc::{AuthContext, AuthRequest, AuthResult, Authenticate, RpcError};

/// Environment variable that turns hosted mode on. Baked into the hosted base
/// image; not set per deployment.
pub const HOSTED_ENV: &str = "VGI_ORCHARD_HOSTED";

/// The challenge on every hosted-mode 401. No `resource_metadata`: a Field
/// Worker never sends a client off to discover an authorization server.
pub const HOSTED_CHALLENGE: &str = "Bearer error=\"invalid_token\"";

/// The [`AuthContext::domain`] of a caller authenticated by a Pass.
pub const HOSTED_AUTH_DOMAIN: &str = "orchard";

/// Required JOSE `typ` (RFC 9068), compared case-insensitively with the
/// `application/` prefix optional.
pub const REQUIRED_TYP: &str = "at+jwt";

/// Clock skew allowed on `exp` / `nbf`, in seconds.
pub const LEEWAY_SECONDS: u64 = 60;

/// Hard upper bound on how long JWKS keys are trusted.
pub const JWKS_MAX_AGE: Duration = Duration::from_secs(600);

/// Minimum time between JWKS re-fetches triggered by an unknown `kid`.
pub const JWKS_REFRESH_MIN_INTERVAL: Duration = Duration::from_secs(60);

/// Timeout for each JWKS request.
pub const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// The shortest `VGI_SIGNING_KEY` a hosted worker accepts.
pub const MIN_SIGNING_KEY_BYTES: usize = 32;

/// The request cap (`VGI-Max-Request-Bytes`) a hosted worker serves with when
/// `VGI_RPC_MAX_REQUEST_BYTES` is unset: 32 MiB, vgi-python's
/// `HOSTED_DEFAULT_MAX_REQUEST_BYTES`. With the blob vendor, a larger request
/// goes up through an upload URL instead.
pub const HOSTED_DEFAULT_MAX_REQUEST_BYTES: usize = 32 * 1024 * 1024;

/// Largest JWKS document read.
const JWKS_MAX_BYTES: u64 = 1 << 20;

/// Print a hosted-mode startup error and exit with status 1.
pub fn refuse(reason: &str) -> ! {
    eprintln!("Error: {HOSTED_ENV}=1 (Orchard hosted mode): {reason}");
    std::process::exit(1);
}

/// Parse a `VGI_ORCHARD_HOSTED` value. `None` (unset) is off.
///
/// `1`/`true`/`yes`/`on` (any case) are on; empty and `0`/`false`/`no`/`off`
/// are off. Anything else is an error rather than "off", so a typo cannot
/// quietly start a worker without authentication.
pub fn parse_hosted_flag(raw: Option<&str>) -> Result<bool, String> {
    let Some(raw) = raw else {
        return Ok(false);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "" | "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!(
            "Error: {HOSTED_ENV} must be 1 or 0 (or true/false), got {raw:?}"
        )),
    }
}

/// Whether `VGI_ORCHARD_HOSTED` turns hosted mode on. Exits with status 1 on
/// an unrecognised value (see [`parse_hosted_flag`]).
pub fn hosted_mode_enabled() -> bool {
    parse_hosted_flag(std::env::var(HOSTED_ENV).ok().as_deref()).unwrap_or_else(|message| {
        eprintln!("{message}");
        std::process::exit(1);
    })
}

/// Verified hosted-mode settings, read from the environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostedConfig {
    /// The Front Desk issuer (`iss`), matched exactly.
    pub issuer: String,
    /// The one canonical Desk Address (`aud`), matched exactly.
    pub audience: String,
    /// Where the Front Desk publishes its signing keys.
    pub jwks_uri: String,
    /// Accepted JWS algorithms (`ES256` unless `VGI_JWT_ALGORITHMS` says
    /// otherwise; never `none` or `HS*`).
    pub algorithms: Vec<Algorithm>,
}

fn single_value(get: &dyn Fn(&str) -> Option<String>, name: &str) -> Result<String, String> {
    let value = get(name).unwrap_or_default().trim().to_string();
    if value.is_empty() {
        return Err(format!(
            "{name} must be set (unset and empty are both errors)."
        ));
    }
    if value.contains(',') {
        return Err(format!(
            "{name} must be exactly one value; got a comma-separated list. A Pass is issued \
             for one canonical Desk Address and one issuer."
        ));
    }
    Ok(value)
}

/// Read and validate the hosted-mode configuration through `get` (an
/// environment lookup). The error is the reason to [`refuse`] with.
pub fn hosted_config_from(get: &dyn Fn(&str) -> Option<String>) -> Result<HostedConfig, String> {
    let issuer = single_value(get, "VGI_JWT_ISSUER")?;
    let audience = single_value(get, "VGI_JWT_AUDIENCE")?;
    let jwks_uri = single_value(get, "VGI_JWT_JWKS_URI")?;
    let raw = get("VGI_JWT_ALGORITHMS").unwrap_or_default();
    let algorithms = if raw.trim().is_empty() {
        vec![Algorithm::ES256]
    } else {
        let names: Vec<&str> = raw
            .split(',')
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .collect();
        let refused = || {
            format!(
                "VGI_JWT_ALGORITHMS={raw:?} is not allowed; Passes are asymmetric (default \
                 ES256), never 'none' or HS*."
            )
        };
        if names.is_empty() {
            return Err(refused());
        }
        let mut algorithms = Vec::new();
        for name in names {
            let lowered = name.to_ascii_lowercase();
            if lowered == "none" || lowered.starts_with("hs") {
                return Err(refused());
            }
            let alg: Algorithm = name
                .parse()
                .map_err(|_| format!("VGI_JWT_ALGORITHMS names an unknown algorithm {name:?}."))?;
            algorithms.push(alg);
        }
        algorithms
    };
    Ok(HostedConfig {
        issuer,
        audience,
        jwks_uri,
        algorithms,
    })
}

/// [`hosted_config_from`] the process environment, exiting on an error.
pub fn resolve_hosted_config() -> HostedConfig {
    hosted_config_from(&|name| std::env::var(name).ok()).unwrap_or_else(|reason| refuse(&reason))
}

/// The first credential the environment configures besides a Pass, as the
/// reason to [`refuse`] with.
pub fn environment_conflict(get: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    let set = |name: &str| !get(name).unwrap_or_default().trim().is_empty();
    for (name, what) in [
        ("VGI_BEARER_TOKENS", "static bearer tokens"),
        (
            "VGI_OPTIONAL_BEARER_TOKENS",
            "optional static bearer tokens",
        ),
        (vgi_rpc::grants::GRANT_KEYS_ENV, "sealed-grant keys"),
        (
            "VGI_OAUTH_RESOURCE",
            "OAuth resource metadata (a Field Worker never advertises resource_metadata)",
        ),
    ] {
        if set(name) {
            return Some(format!(
                "{name} is set ({what}); the only credential a hosted worker accepts is a Pass."
            ));
        }
    }
    let proof = get("VGI_PROXY_PROOF_MODE").unwrap_or_else(|| "off".into());
    let proof = proof.trim().to_ascii_lowercase();
    if !proof.is_empty() && proof != "off" {
        return Some(
            "VGI_PROXY_PROOF_MODE must be off; hosted workers authenticate by Pass only.".into(),
        );
    }
    None
}

/// What a worker is configured with that hosted mode cares about. Built by
/// [`Worker`](crate::Worker) before it serves anything.
pub(crate) struct WorkerSurface<'a> {
    pub transport: crate::worker::ServeTransport,
    pub authenticate: bool,
    pub resolve_token: bool,
    pub mint_grant: bool,
    pub grant_keys: bool,
    /// [`Worker::signing_key`](crate::Worker::signing_key), when set.
    pub explicit_signing_key: Option<&'a [u8]>,
}

/// The configured signing key a hosted worker uses: the explicit one, else
/// `VGI_SIGNING_KEY` unless `VGI_SIGNING_KEY_MINTED` marks it as minted by a
/// parent process rather than configured by the operator.
pub(crate) fn configured_signing_key(
    explicit: Option<&[u8]>,
    get: &dyn Fn(&str) -> Option<String>,
) -> Option<Vec<u8>> {
    if let Some(key) = explicit {
        return Some(key.to_vec());
    }
    if !get("VGI_SIGNING_KEY_MINTED")
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        return None;
    }
    crate::worker::signing_key_bytes(get(crate::worker::SIGNING_KEY_ENV))
}

/// Every reason `surface` cannot run in hosted mode, first one first. Reads
/// the environment through `get`.
pub(crate) fn worker_conflict(
    surface: &WorkerSurface<'_>,
    get: &dyn Fn(&str) -> Option<String>,
) -> Option<String> {
    use crate::worker::ServeTransport;
    if surface.transport != ServeTransport::Http {
        return Some(
            "only --http is allowed; stdio, --unix, --tcp and --iroh-raw-upstream carry no Pass."
                .into(),
        );
    }
    if let Err(reason) = hosted_config_from(get) {
        return Some(reason);
    }
    if let Some(reason) = environment_conflict(get) {
        return Some(reason);
    }
    if surface.authenticate {
        return Some(
            "a custom authenticate callback is configured (Worker::authenticate); hosted workers \
             verify Passes only."
                .into(),
        );
    }
    if surface.grant_keys {
        return Some(
            "sealed-grant keys (--grant-key / VGI_RPC_GRANT_KEYS / Worker::grant_keys) are \
             configured."
                .into(),
        );
    }
    if surface.resolve_token {
        return Some("the worker sets Worker::resolve_token (resolve_token).".into());
    }
    if surface.mint_grant {
        return Some("the worker sets Worker::mint_grant (mint_grant / issue_grant).".into());
    }
    match configured_signing_key(surface.explicit_signing_key, get) {
        None => Some(
            "VGI_SIGNING_KEY is not set; hosted workers need a stable key shared by every \
             machine of the deployment, or every restart breaks attached sessions."
                .into(),
        ),
        Some(key) if key.len() < MIN_SIGNING_KEY_BYTES => Some(format!(
            "VGI_SIGNING_KEY must be at least {MIN_SIGNING_KEY_BYTES} bytes."
        )),
        Some(_) => None,
    }
}

/// The HTTP state-token key of a hosted worker: derived from its signing key,
/// so stream continuations survive restarts and work on every machine of the
/// deployment.
pub(crate) fn http_token_key(signing_key: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"vgi-orchard-hosted-http-token-key\0");
    h.update(signing_key);
    h.finalize().into()
}

// ---------------------------------------------------------------------------
// Pass verification
// ---------------------------------------------------------------------------

/// A JWKS fetch: the keys, and the response's `Cache-Control: max-age` in
/// seconds when it carries one. An error is an outage (HTTP 503); its text is
/// logged, so it must not carry URL credentials.
pub type JwksFetch = Arc<dyn Fn(&str) -> Result<(JwkSet, Option<u64>), String> + Send + Sync>;

/// Keys and when they stop being usable.
struct CachedKeys {
    keys: Arc<JwkSet>,
    expires_at: Instant,
    /// Bumped on every successful fetch, so a thread that waited on the lock
    /// can tell another thread already refreshed.
    generation: u64,
}

struct KeyCache {
    entry: Option<CachedKeys>,
    last_fetch_attempt: Option<Instant>,
    generation: u64,
}

struct Verifier {
    config: HostedConfig,
    fetch: JwksFetch,
    cache: Mutex<KeyCache>,
}

fn invalid(detail: &str) -> RpcError {
    RpcError::auth_failure(AuthReason::InvalidCredential, detail)
}

fn unavailable() -> RpcError {
    RpcError::auth_unavailable("Orchard JWKS unavailable")
}

/// Normalize a JOSE `typ` for comparison (RFC 7515 §4.1.9, RFC 9068 §2.1).
fn normalize_typ(value: &str) -> String {
    let lowered = value.trim().to_ascii_lowercase();
    lowered
        .strip_prefix("application/")
        .map(str::to_string)
        .unwrap_or(lowered)
}

/// The `max-age` directive of a `Cache-Control` header, in seconds.
pub(crate) fn cache_control_max_age(value: &str) -> Option<u64> {
    value.split(',').find_map(|directive| {
        let (name, arg) = directive.split_once('=')?;
        if !name.trim().eq_ignore_ascii_case("max-age") {
            return None;
        }
        let arg = arg.trim();
        let arg = arg
            .strip_prefix('"')
            .and_then(|a| a.strip_suffix('"'))
            .unwrap_or(arg);
        if arg.is_empty() || !arg.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        arg.parse().ok()
    })
}

/// Run a blocking call without starving the async runtime it may be on.
fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

impl Verifier {
    /// Fetch and replace the cache. The caller holds the lock.
    fn fetch_locked(&self, cache: &mut KeyCache) -> Result<Arc<JwkSet>, RpcError> {
        let started = Instant::now();
        cache.last_fetch_attempt = Some(started);
        match blocking(|| (self.fetch)(&self.config.jwks_uri)) {
            Ok((keys, max_age)) => {
                let ttl = max_age
                    .map(|s| Duration::from_secs(s).min(JWKS_MAX_AGE))
                    .unwrap_or(JWKS_MAX_AGE);
                let keys = Arc::new(keys);
                cache.generation += 1;
                // Measured from when the fetch started, so a slow response
                // cannot stretch the bound.
                cache.entry = Some(CachedKeys {
                    keys: keys.clone(),
                    expires_at: started + ttl,
                    generation: cache.generation,
                });
                Ok(keys)
            }
            Err(reason) => {
                // Never fall back to stale keys.
                if cache
                    .entry
                    .as_ref()
                    .is_some_and(|e| Instant::now() >= e.expires_at)
                {
                    cache.entry = None;
                }
                log::warn!("Orchard JWKS fetch failed: {reason}");
                Err(unavailable())
            }
        }
    }

    /// Keys within their maximum age, fetching if needed.
    fn current_keys(&self) -> Result<(Arc<JwkSet>, u64), RpcError> {
        let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(entry) = cache.entry.as_ref() {
            if Instant::now() < entry.expires_at {
                return Ok((entry.keys.clone(), entry.generation));
            }
        }
        let keys = self.fetch_locked(&mut cache)?;
        Ok((keys, cache.generation))
    }

    /// Re-fetch after an unknown `kid`; `None` when rate limited.
    fn refresh_for_unknown_kid(&self, seen: u64) -> Result<Option<Arc<JwkSet>>, RpcError> {
        let mut cache = self.cache.lock().unwrap_or_else(|p| p.into_inner());
        let now = Instant::now();
        if let Some(entry) = cache.entry.as_ref() {
            if entry.generation != seen && now < entry.expires_at {
                return Ok(Some(entry.keys.clone()));
            }
        }
        if cache
            .last_fetch_attempt
            .is_some_and(|t| now.duration_since(t) < JWKS_REFRESH_MIN_INTERVAL)
        {
            return Ok(None);
        }
        self.fetch_locked(&mut cache).map(Some)
    }

    fn authenticate(&self, req: &AuthRequest<'_>) -> AuthResult {
        let header = req.header("authorization").unwrap_or("").trim();
        if header.is_empty() {
            return Err(RpcError::auth_failure(
                AuthReason::MissingCredential,
                "missing Authorization header",
            ));
        }
        let token = match header.split_once(' ') {
            Some((scheme, rest)) if scheme.eq_ignore_ascii_case("bearer") => rest.trim(),
            _ => return Err(invalid("Authorization header is not a Bearer credential")),
        };
        if token.is_empty() {
            return Err(invalid("empty bearer token"));
        }
        let header = jsonwebtoken::decode_header(token).map_err(|_| invalid("malformed Pass"))?;
        if !self.config.algorithms.contains(&header.alg) {
            return Err(invalid("Pass algorithm not allowed"));
        }
        if header.typ.as_deref().map(normalize_typ).as_deref() != Some(REQUIRED_TYP) {
            return Err(invalid("Pass typ must be at+jwt"));
        }
        let (keys, generation) = self.current_keys()?;
        let claims = match self.decode_with(token, &header, &keys) {
            Err(Decode::UnknownKid) => match self.refresh_for_unknown_kid(generation)? {
                Some(keys) => self.decode_with(token, &header, &keys),
                None => Err(Decode::UnknownKid),
            },
            other => other,
        }
        .map_err(|failure| {
            log::debug!("Orchard Pass rejected: {}", failure.describe());
            failure.into_error()
        })?;
        self.check_claims(&claims).inspect_err(|e| {
            log::debug!("Orchard Pass rejected: {}", e.message);
        })?;
        let principal = match claims.get("sub") {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        };
        let mut ctx = AuthContext::for_principal(HOSTED_AUTH_DOMAIN, principal);
        for (name, value) in claims {
            ctx = ctx.with_claim_value(name, value);
        }
        Ok(ctx)
    }

    fn decode_with(
        &self,
        token: &str,
        header: &jsonwebtoken::Header,
        keys: &JwkSet,
    ) -> Result<serde_json::Map<String, serde_json::Value>, Decode> {
        let jwk = match header.kid.as_deref() {
            Some(kid) => keys.find(kid).ok_or(Decode::UnknownKid)?,
            // A Pass always names its key; without a kid, only a JWKS holding
            // exactly one key is unambiguous.
            None => match keys.keys.as_slice() {
                [only] => only,
                _ => return Err(Decode::UnknownKid),
            },
        };
        let key = DecodingKey::from_jwk(jwk).map_err(Decode::Jwt)?;
        let mut validation = Validation::new(header.alg);
        validation.algorithms = vec![header.alg];
        validation.leeway = LEEWAY_SECONDS;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        // `iss` and `aud` are checked exactly by `check_claims`.
        validation.validate_aud = false;
        validation.required_spec_claims.clear();
        validation.required_spec_claims.insert("exp".to_string());
        jsonwebtoken::decode::<serde_json::Map<String, serde_json::Value>>(token, &key, &validation)
            .map(|data| data.claims)
            .map_err(Decode::Jwt)
    }

    fn check_claims(
        &self,
        claims: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), RpcError> {
        if claims.get("iss").and_then(|v| v.as_str()) != Some(self.config.issuer.as_str()) {
            return Err(invalid("Pass issuer mismatch"));
        }
        let audience = match claims.get("aud") {
            Some(serde_json::Value::String(s)) => Some(s.as_str()),
            Some(serde_json::Value::Array(items)) if items.len() == 1 => items[0].as_str(),
            _ => None,
        };
        if audience != Some(self.config.audience.as_str()) {
            return Err(invalid("Pass audience mismatch (exactly one aud required)"));
        }
        if !claims
            .get("exp")
            .is_some_and(|v| v.is_i64() || v.is_u64() || v.is_f64())
        {
            return Err(invalid("Pass exp missing or not a number"));
        }
        Ok(())
    }
}

/// Why a Pass did not decode.
enum Decode {
    UnknownKid,
    Jwt(jsonwebtoken::errors::Error),
}

impl Decode {
    fn describe(&self) -> String {
        match self {
            Decode::UnknownKid => "unknown kid".into(),
            Decode::Jwt(e) => format!("{:?}", e.kind()),
        }
    }

    fn into_error(self) -> RpcError {
        match self {
            Decode::Jwt(e)
                if matches!(e.kind(), jsonwebtoken::errors::ErrorKind::ExpiredSignature) =>
            {
                RpcError::auth_failure(AuthReason::ExpiredCredential, "Pass expired")
            }
            Decode::UnknownKid => invalid("Pass signed with an unknown key"),
            Decode::Jwt(_) => invalid("Pass did not verify"),
        }
    }
}

/// Parse a JWKS document, skipping keys this verifier cannot use rather than
/// rejecting the whole set.
pub(crate) fn parse_jwks(body: &str) -> Result<JwkSet, String> {
    let doc: serde_json::Value =
        serde_json::from_str(body).map_err(|e| format!("JWKS is not JSON: {e}"))?;
    let keys = doc
        .get("keys")
        .and_then(|k| k.as_array())
        .ok_or_else(|| "JWKS has no keys array".to_string())?;
    Ok(JwkSet {
        keys: keys
            .iter()
            .filter_map(|k| serde_json::from_value::<Jwk>(k.clone()).ok())
            .collect(),
    })
}

/// The default JWKS fetch: a blocking GET with a 10 s timeout.
pub fn http_jwks_fetch(uri: &str) -> Result<(JwkSet, Option<u64>), String> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(JWKS_FETCH_TIMEOUT))
        .http_status_as_error(false)
        .build()
        .into();
    // Errors name the failure class only: the URI may carry credentials.
    let mut resp = agent
        .get(uri)
        .call()
        .map_err(|e| format!("request failed ({})", error_class(&e)))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(format!("HTTP {}", status.as_u16()));
    }
    let max_age = resp
        .headers()
        .get("cache-control")
        .and_then(|v| v.to_str().ok())
        .and_then(cache_control_max_age);
    let body = resp
        .body_mut()
        .with_config()
        .limit(JWKS_MAX_BYTES)
        .read_to_string()
        .map_err(|e| format!("reading body failed ({})", error_class(&e)))?;
    Ok((parse_jwks(&body)?, max_age))
}

fn error_class(e: &ureq::Error) -> &'static str {
    match e {
        ureq::Error::Timeout(_) => "timeout",
        ureq::Error::Io(_) => "io",
        ureq::Error::HostNotFound => "host not found",
        ureq::Error::ConnectionFailed => "connection failed",
        ureq::Error::BodyExceedsLimit(_) => "body too large",
        _ => "error",
    }
}

/// Build the Pass verifier for `config`, fetching the JWKS over HTTP(S).
pub fn hosted_authenticate(config: HostedConfig) -> Authenticate {
    hosted_authenticate_with(config, Arc::new(http_jwks_fetch))
}

/// [`hosted_authenticate`] with the JWKS fetch supplied (tests).
pub fn hosted_authenticate_with(config: HostedConfig, fetch: JwksFetch) -> Authenticate {
    let verifier = Arc::new(Verifier {
        config,
        fetch,
        cache: Mutex::new(KeyCache {
            entry: None,
            last_fetch_attempt: None,
            generation: 0,
        }),
    });
    Arc::new(move |req: &AuthRequest<'_>| verifier.authenticate(req))
}

#[cfg(test)]
mod tests;

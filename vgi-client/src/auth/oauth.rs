// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! OAuth discovery and grants.
//!
//! # Discovery is two stages, and only the second is standard OIDC
//!
//! ```text
//! 401 + WWW-Authenticate  ->  resource_metadata URL      (RFC 9728 + VGI extensions)
//!                         ->  authorization_servers[0]
//!                         ->  {issuer}/.well-known/openid-configuration   (OIDC)
//! ```
//!
//! The first stage is why this is hand-rolled rather than handed to an OIDC
//! library: the protected-resource document carries VGI extensions no crate
//! models — a `token_endpoint` override pointing at the worker's own PKCE
//! proxy, a separate `device_code_client_id`, and `use_id_token_as_bearer`.
//!
//! # Transport security
//!
//! Every discovered URL must be `https://`, with an exception only for
//! loopback. The loopback check is boundary-aware, so `http://127.0.0.1.evil.com`
//! is rejected — a prefix match would not be.

use std::time::{Duration, Instant};

use serde::Deserialize;
use vgi_rpc::errors::{Result, RpcError};

use super::challenge::OAuthChallenge;

/// How a human completes an interactive login.
///
/// **Required, not optional.** The C++ extension routes device-code prompts
/// through DuckDB's log manager, so on any client that does not render those
/// logs a login silently appears to hang — the single worst UX trap in that
/// implementation. Making this a mandatory constructor argument means a client
/// cannot accidentally have nowhere to show the prompt.
pub trait UserInteraction: Send + Sync {
    /// Tell the user to visit a URL and enter a code.
    ///
    /// Called once when a device-code flow starts. Implementations should make
    /// this genuinely visible — stderr, a dialog, a log the operator reads.
    fn prompt_device_code(&self, info: &DeviceCodePrompt);

    /// Report that the flow is still waiting.
    ///
    /// Called periodically during a long poll so a user who stepped away can
    /// tell the client has not wedged. The default does nothing.
    fn still_waiting(&self, _elapsed: Duration) {}

    /// Report that authentication succeeded.
    fn authenticated(&self) {}
}

/// What to show a user starting a device-code login.
#[derive(Debug, Clone)]
pub struct DeviceCodePrompt {
    /// Where to go.
    pub verification_uri: String,
    /// The code to type.
    pub user_code: String,
    /// A URL with the code already embedded, when the provider offers one.
    pub verification_uri_complete: Option<String>,
    /// What is being authenticated, for context.
    pub resource_name: Option<String>,
}

/// Writes prompts to stderr.
///
/// A reasonable default for a CLI. A server-side embedder should supply its own
/// so the prompt reaches an operator rather than a log nobody tails.
#[derive(Debug, Default, Clone, Copy)]
pub struct StderrInteraction;

impl UserInteraction for StderrInteraction {
    fn prompt_device_code(&self, info: &DeviceCodePrompt) {
        let what = info.resource_name.as_deref().unwrap_or("this service");
        eprintln!("[vgi] Authentication required for {what}.");
        eprintln!("[vgi] Visit: {}", info.verification_uri);
        eprintln!("[vgi] Enter code: {}", info.user_code);
        if let Some(c) = &info.verification_uri_complete {
            eprintln!("[vgi] Or open directly: {c}");
        }
    }

    fn still_waiting(&self, elapsed: Duration) {
        eprintln!(
            "[vgi] Still waiting for authentication ({}s)…",
            elapsed.as_secs()
        );
    }

    fn authenticated(&self) {
        eprintln!("[vgi] Authentication successful.");
    }
}

/// RFC 9728 protected-resource metadata, plus the VGI extensions.
#[derive(Clone, Default, Deserialize)]
pub struct ResourceMetadata {
    /// Issuers that can authenticate for this resource. Only the first is used,
    /// matching the reference implementation.
    #[serde(default)]
    pub authorization_servers: Vec<String>,
    /// Scopes to request.
    #[serde(default)]
    pub scopes_supported: Vec<String>,
    /// A human-readable name, used in the login prompt.
    #[serde(default)]
    pub resource_name: Option<String>,
    /// Client id to present.
    #[serde(default)]
    pub client_id: Option<String>,
    /// Client secret, where the provider treats it as public.
    #[serde(default)]
    pub client_secret: Option<String>,
    /// Separate client id for the device-code flow.
    #[serde(default)]
    pub device_code_client_id: Option<String>,
    /// Secret paired with `device_code_client_id`.
    #[serde(default)]
    pub device_code_client_secret: Option<String>,
    /// **Non-standard.** Overrides the provider's token endpoint, pointing at
    /// the VGI server's own exchange proxy so it can inject a server-side
    /// client secret.
    #[serde(default)]
    pub token_endpoint: Option<String>,
    /// Present the `id_token` rather than the access token.
    #[serde(default)]
    pub use_id_token_as_bearer: bool,
}

impl std::fmt::Debug for ResourceMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResourceMetadata")
            .field("authorization_servers", &self.authorization_servers)
            .field("scopes_supported", &self.scopes_supported)
            .field("resource_name", &self.resource_name)
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("device_code_client_id", &self.device_code_client_id)
            .field(
                "device_code_client_secret",
                &self
                    .device_code_client_secret
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field("token_endpoint", &self.token_endpoint)
            .field("use_id_token_as_bearer", &self.use_id_token_as_bearer)
            .finish()
    }
}

/// The subset of OIDC provider metadata this client needs.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProviderMetadata {
    /// Where to exchange a grant for tokens.
    #[serde(default)]
    pub token_endpoint: Option<String>,
    /// Where a browser flow starts.
    #[serde(default)]
    pub authorization_endpoint: Option<String>,
    /// Where a device-code flow starts.
    #[serde(default)]
    pub device_authorization_endpoint: Option<String>,
}

/// Everything needed to run a flow, resolved from the challenge.
#[derive(Clone)]
pub struct DiscoveredEndpoints {
    /// Token endpoint used for refreshes, with the resource metadata's proxy
    /// override applied when present.
    pub token_endpoint: String,
    /// The identity provider's token endpoint. Device-code polling must go
    /// directly here: a VGI token-exchange proxy handles refresh/browser
    /// grants, but does not necessarily accept the RFC 8628 device grant.
    pub device_token_endpoint: String,
    /// Whether `token_endpoint` is a resource-owned proxy rather than the
    /// identity provider endpoint.
    pub token_endpoint_is_proxy: bool,
    /// Device-authorization endpoint, when the provider offers one.
    pub device_authorization_endpoint: Option<String>,
    /// Browser authorization endpoint, when the provider offers one.
    pub authorization_endpoint: Option<String>,
    /// Client id for the device flow.
    pub device_client_id: Option<String>,
    /// Client secret for the device flow.
    pub device_client_secret: Option<String>,
    /// Client id for other flows and for refresh.
    pub client_id: Option<String>,
    /// Client secret for other flows and for refresh.
    pub client_secret: Option<String>,
    /// Space-joined scopes.
    pub scope: String,
    /// Human-readable resource name for prompts.
    pub resource_name: Option<String>,
    /// Present the id_token as the bearer.
    pub use_id_token_as_bearer: bool,
}

impl std::fmt::Debug for DiscoveredEndpoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscoveredEndpoints")
            .field("token_endpoint", &self.token_endpoint)
            .field("device_token_endpoint", &self.device_token_endpoint)
            .field("token_endpoint_is_proxy", &self.token_endpoint_is_proxy)
            .field(
                "device_authorization_endpoint",
                &self.device_authorization_endpoint,
            )
            .field("authorization_endpoint", &self.authorization_endpoint)
            .field("device_client_id", &self.device_client_id)
            .field(
                "device_client_secret",
                &self.device_client_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("client_id", &self.client_id)
            .field(
                "client_secret",
                &self.client_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("scope", &self.scope)
            .field("resource_name", &self.resource_name)
            .field("use_id_token_as_bearer", &self.use_id_token_as_bearer)
            .finish()
    }
}

/// Something that can perform the HTTP calls discovery and grants need.
///
/// A trait so the flows are testable against a mock provider without a network.
pub trait HttpTransport: Send + Sync {
    /// GET a URL, returning the body.
    fn get(&self, url: &str) -> Result<String>;
    /// Read an OAuth challenge from an unauthenticated endpoint.
    ///
    /// Mock transports historically returned the challenge string from `get`,
    /// so the default preserves that behavior. Real HTTP transports override
    /// this to read the `WWW-Authenticate` response header on a 401.
    fn www_authenticate(&self, url: &str) -> Result<Option<String>> {
        self.get(url).map(Some)
    }
    /// POST a form-encoded body, returning `(status, body)`.
    ///
    /// The status is returned rather than folded into an error because the
    /// device-code poll has to read the error *body* on a 4xx to distinguish
    /// "still pending" from "denied".
    fn post_form(&self, url: &str, form: &[(&str, &str)]) -> Result<(u16, String)>;
}

/// Reject any URL that is not https, allowing loopback http for local testing.
pub fn enforce_https(url: &str) -> Result<()> {
    if url.starts_with("https://") {
        return Ok(());
    }
    if let Some(rest) = url.strip_prefix("http://") {
        // Boundary-aware: the host must END at a delimiter, so
        // `127.0.0.1.evil.com` does not pass as loopback.
        for host in ["localhost", "127.0.0.1", "[::1]"] {
            if let Some(after) = rest.strip_prefix(host) {
                if after.is_empty() || after.starts_with([':', '/', '?', '#']) {
                    return Ok(());
                }
            }
        }
    }
    Err(RpcError::new(
        "AuthError",
        format!("refusing to use a non-https OAuth endpoint: {url}"),
    ))
}

/// Walk the discovery chain from a challenge to concrete endpoints.
pub fn discover(
    http: &dyn HttpTransport,
    challenge: &OAuthChallenge,
) -> Result<DiscoveredEndpoints> {
    enforce_https(&challenge.resource_metadata)?;
    let body = http.get(&challenge.resource_metadata)?;
    let rm: ResourceMetadata = serde_json::from_str(&body).map_err(|e| {
        RpcError::new(
            "AuthError",
            format!("protected-resource metadata is not readable JSON: {e}"),
        )
    })?;

    let issuer = rm.authorization_servers.first().ok_or_else(|| {
        RpcError::new(
            "AuthError",
            "protected-resource metadata names no authorization_servers",
        )
    })?;
    enforce_https(issuer)?;

    let config_url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let body = http.get(&config_url)?;
    let pm: ProviderMetadata = serde_json::from_str(&body).map_err(|e| {
        RpcError::new(
            "AuthError",
            format!("provider metadata at {config_url} is not readable JSON: {e}"),
        )
    })?;

    let device_token_endpoint = pm
        .token_endpoint
        .clone()
        .ok_or_else(|| RpcError::new("AuthError", "no token endpoint was discovered"))?;
    enforce_https(&device_token_endpoint)?;

    // The resource metadata's token_endpoint wins for refresh: it points at
    // the VGI server's exchange proxy, which holds a client secret this client
    // does not. Device polling still uses the provider endpoint above.
    let token_endpoint_is_proxy = rm.token_endpoint.is_some();
    let token_endpoint = rm
        .token_endpoint
        .clone()
        .unwrap_or_else(|| device_token_endpoint.clone());
    enforce_https(&token_endpoint)?;
    if let Some(d) = &pm.device_authorization_endpoint {
        enforce_https(d)?;
    }

    if pm.device_authorization_endpoint.is_none() && pm.authorization_endpoint.is_none() {
        return Err(RpcError::new(
            "AuthError",
            "the provider offers neither a device-code nor a browser flow",
        ));
    }

    let scope = if rm.scopes_supported.is_empty() {
        "openid".to_string()
    } else {
        rm.scopes_supported.join(" ")
    };

    Ok(DiscoveredEndpoints {
        token_endpoint,
        device_token_endpoint,
        token_endpoint_is_proxy,
        device_authorization_endpoint: pm.device_authorization_endpoint,
        authorization_endpoint: pm.authorization_endpoint,
        device_client_id: challenge
            .device_code_client_id
            .clone()
            .or(rm.device_code_client_id.clone()),
        device_client_secret: challenge
            .device_code_client_secret
            .clone()
            .or(rm.device_code_client_secret.clone()),
        client_id: challenge.client_id.clone().or(rm.client_id.clone()),
        client_secret: challenge.client_secret.clone().or(rm.client_secret.clone()),
        scope,
        resource_name: rm.resource_name.clone(),
        use_id_token_as_bearer: challenge.use_id_token_as_bearer || rm.use_id_token_as_bearer,
    })
}

/// A token set, with the client-side expiry it was received with.
#[derive(Clone)]
pub struct TokenSet {
    /// The access token.
    pub access_token: String,
    /// The id token, when the provider returned one.
    pub id_token: Option<String>,
    /// The refresh token, when the provider returned one.
    pub refresh_token: Option<String>,
    /// When the access token stops being valid. `None` means it never expires.
    pub expires_at: Option<Instant>,
    /// Whether to present the id token rather than the access token.
    pub use_id_token: bool,
    /// `(issuer, subject)` from the id token, if it carried them.
    pub identity: Option<(String, String)>,
}

impl std::fmt::Debug for TokenSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenSet")
            .field("access_token", &"<redacted>")
            .field("id_token", &self.id_token.as_ref().map(|_| "<redacted>"))
            .field(
                "refresh_token",
                &self.refresh_token.as_ref().map(|_| "<redacted>"),
            )
            .field("expires_at", &self.expires_at)
            .field("use_id_token", &self.use_id_token)
            .field("identity", &self.identity)
            .finish()
    }
}

impl TokenSet {
    /// The value to put in the `Authorization` header.
    pub fn bearer(&self) -> &str {
        if self.use_id_token {
            self.id_token.as_deref().unwrap_or(&self.access_token)
        } else {
            &self.access_token
        }
    }

    /// Whether the token is still usable, allowing a skew margin.
    ///
    /// The margin is why this is not a bare `now < expires_at`: refreshing a
    /// little early costs one extra exchange, while refreshing a little late
    /// costs a wasted round trip that re-uploads the whole request body.
    pub fn is_valid_at(&self, now: Instant, skew: Duration) -> bool {
        match self.expires_at {
            None => true,
            Some(exp) => now + skew < exp,
        }
    }
}

/// The token endpoint's answer.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    id_token: Option<String>,
    refresh_token: Option<String>,
    expires_in: Option<i64>,
    error: Option<String>,
}

/// Build a `TokenSet` from a token-endpoint response body.
fn parse_token_response(
    body: &str,
    now: Instant,
    use_id_token: bool,
    carried_refresh: Option<String>,
) -> Result<TokenSet> {
    let tr: TokenResponse = serde_json::from_str(body)
        .map_err(|e| RpcError::new("AuthError", format!("token response is not JSON: {e}")))?;
    if let Some(err) = tr.error {
        return Err(RpcError::new(
            "AuthError",
            format!("token endpoint refused: {err}"),
        ));
    }
    let access_token = tr
        .access_token
        .ok_or_else(|| RpcError::new("AuthError", "token response carried no access_token"))?;
    let identity = tr.id_token.as_deref().and_then(parse_id_token_identity);
    Ok(TokenSet {
        access_token,
        id_token: tr.id_token,
        // Google omits the refresh token on a refresh; carry the old one forward
        // rather than losing the ability to refresh again.
        refresh_token: tr.refresh_token.or(carried_refresh),
        expires_at: tr
            .expires_in
            .filter(|s| *s > 0)
            .map(|s| now + Duration::from_secs(s as u64)),
        use_id_token,
        identity,
    })
}

/// Pull `(iss, sub)` out of an id token's payload.
///
/// **The signature is not verified**, deliberately: the token arrived over TLS
/// from an exchange this client initiated, so it is an identity *hint*. It is
/// still used as a cache-isolation key, so treat `iss`/`sub` as trusted only
/// because of that TLS path.
fn parse_id_token_identity(jwt: &str) -> Option<(String, String)> {
    let payload_b64 = jwt.split('.').nth(1)?;
    let payload = base64url_decode(payload_b64)?;
    let json: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    let iss = json.get("iss")?.as_str()?.to_string();
    let sub = json.get("sub")?.as_str()?.to_string();
    Some((iss, sub))
}

/// base64url decode, tolerating missing padding and the standard alphabet.
fn base64url_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for b in s.bytes() {
        let v = match b {
            b'A'..=b'Z' => u32::from(b - b'A'),
            b'a'..=b'z' => u32::from(b - b'a') + 26,
            b'0'..=b'9' => u32::from(b - b'0') + 52,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            b'=' => break,
            _ => return None,
        };
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

/// The device-authorization endpoint's answer.
#[derive(Debug, Deserialize)]
struct DeviceAuthResponse {
    device_code: String,
    user_code: String,
    // Providers disagree on the spelling: RFC 8628 says `verification_uri`,
    // Google sends `verification_url`.
    verification_uri: Option<String>,
    verification_url: Option<String>,
    verification_uri_complete: Option<String>,
    expires_in: Option<i64>,
    interval: Option<i64>,
}

/// Run the RFC 8628 device-code flow to completion.
pub fn device_code_flow(
    http: &dyn HttpTransport,
    endpoints: &DiscoveredEndpoints,
    interaction: &dyn UserInteraction,
    timeout: Duration,
) -> Result<TokenSet> {
    let device_endpoint = endpoints
        .device_authorization_endpoint
        .as_deref()
        .ok_or_else(|| RpcError::new("AuthError", "this provider offers no device-code flow"))?;
    let client_id = endpoints
        .device_client_id
        .as_deref()
        .or(endpoints.client_id.as_deref())
        .ok_or_else(|| RpcError::new("AuthError", "no client_id for the device-code flow"))?;

    let mut form: Vec<(&str, &str)> = vec![("client_id", client_id), ("scope", &endpoints.scope)];
    if let Some(sec) = endpoints
        .device_client_secret
        .as_deref()
        .or(endpoints.client_secret.as_deref())
    {
        form.push(("client_secret", sec));
    }
    let (_status, body) = http.post_form(device_endpoint, &form)?;
    let da: DeviceAuthResponse = serde_json::from_str(&body).map_err(|e| {
        RpcError::new(
            "AuthError",
            format!("device-authorization response is not JSON: {e}"),
        )
    })?;

    let verification_uri = da
        .verification_uri
        .or(da.verification_url)
        .ok_or_else(|| RpcError::new("AuthError", "device response named no verification URI"))?;

    interaction.prompt_device_code(&DeviceCodePrompt {
        verification_uri,
        user_code: da.user_code,
        verification_uri_complete: da.verification_uri_complete,
        resource_name: endpoints.resource_name.clone(),
    });

    // The provider's own expiry can shorten the window but never lengthen it.
    let provider_window = Duration::from_secs(da.expires_in.unwrap_or(300).max(0) as u64);
    let deadline = Instant::now() + timeout.min(provider_window);
    let mut interval = Duration::from_secs(da.interval.unwrap_or(5).max(1) as u64);

    // The device-code client is the one that must also present at refresh: a
    // refresh presenting the *other* client id is rejected by Google, which is
    // a live bug in the C++ implementation.
    let refresh_client = client_id.to_string();

    let started = Instant::now();
    let mut last_notice = Instant::now();
    let mut transient_failures = 0u32;

    loop {
        std::thread::sleep(interval);
        if Instant::now() >= deadline {
            return Err(RpcError::new(
                "AuthError",
                "timed out waiting for the user to complete authentication",
            ));
        }
        if last_notice.elapsed() >= Duration::from_secs(30) {
            interaction.still_waiting(started.elapsed());
            last_notice = Instant::now();
        }

        let mut form: Vec<(&str, &str)> = vec![
            ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ("device_code", &da.device_code),
            ("client_id", &refresh_client),
        ];
        if let Some(sec) = endpoints
            .device_client_secret
            .as_deref()
            .or(endpoints.client_secret.as_deref())
        {
            form.push(("client_secret", sec));
        }

        let (status, body) = match http.post_form(&endpoints.device_token_endpoint, &form) {
            Ok(v) => {
                transient_failures = 0;
                v
            }
            Err(e) => {
                transient_failures += 1;
                if transient_failures > 3 {
                    return Err(e);
                }
                continue;
            }
        };

        if status == 200 {
            interaction.authenticated();
            return parse_token_response(
                &body,
                Instant::now(),
                endpoints.use_id_token_as_bearer,
                None,
            );
        }

        // The error body is authoritative, not the status code: providers
        // return `slow_down` on 403 and `authorization_pending` on 428.
        let err = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_string));
        match err.as_deref() {
            Some("authorization_pending") => continue,
            Some("slow_down") => {
                interval += Duration::from_secs(5);
                continue;
            }
            Some(other) => {
                return Err(RpcError::new(
                    "AuthError",
                    format!("device-code flow failed: {other}"),
                ))
            }
            None if status >= 500 => {
                transient_failures += 1;
                if transient_failures > 3 {
                    return Err(RpcError::new(
                        "AuthError",
                        format!("token endpoint kept failing ({status})"),
                    ));
                }
                continue;
            }
            None if status == 429 => {
                interval += Duration::from_secs(5);
                continue;
            }
            None => {
                return Err(RpcError::new(
                    "AuthError",
                    format!("device-code poll failed with status {status}"),
                ))
            }
        }
    }
}

/// Exchange a refresh token for a fresh access token.
pub fn refresh(
    http: &dyn HttpTransport,
    endpoints: &DiscoveredEndpoints,
    refresh_token: &str,
) -> Result<TokenSet> {
    // A refresh token issued by the device flow belongs to the device client,
    // not the ordinary browser client.
    let client_id = endpoints
        .device_client_id
        .as_deref()
        .or(endpoints.client_id.as_deref())
        .ok_or_else(|| RpcError::new("AuthError", "no client_id available to refresh with"))?;
    let mut form: Vec<(&str, &str)> = vec![
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_token),
        ("client_id", client_id),
    ];
    let client_secret = if endpoints.token_endpoint_is_proxy {
        None
    } else {
        endpoints
            .device_client_secret
            .as_deref()
            .or(endpoints.client_secret.as_deref())
    };
    if let Some(sec) = client_secret {
        form.push(("client_secret", sec));
    }
    if !endpoints.scope.is_empty() {
        form.push(("scope", &endpoints.scope));
    }
    let (status, body) = http.post_form(&endpoints.token_endpoint, &form)?;
    if status != 200 {
        let err = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_string))
            .unwrap_or_else(|| format!("status {status}"));
        return Err(RpcError::new("AuthError", format!("refresh failed: {err}")));
    }
    parse_token_response(
        &body,
        Instant::now(),
        endpoints.use_id_token_as_bearer,
        Some(refresh_token.to_string()),
    )
}

/// Whether a refresh failure means the refresh token itself is dead.
///
/// `invalid_grant` is the provider saying "this token is no good" — the next
/// attempt must fall through to an interactive flow rather than retrying.
pub fn is_invalid_grant(err: &RpcError) -> bool {
    err.to_string().contains("invalid_grant")
}

/// A `ureq`-backed transport.
#[derive(Debug, Default)]
pub struct UreqTransport;

impl HttpTransport for UreqTransport {
    fn get(&self, url: &str) -> Result<String> {
        ureq::get(url)
            .call()
            .map_err(|e| RpcError::new("AuthError", format!("GET {url} failed: {e}")))?
            .body_mut()
            .read_to_string()
            .map_err(|e| RpcError::new("AuthError", format!("reading {url} failed: {e}")))
    }

    fn post_form(&self, url: &str, form: &[(&str, &str)]) -> Result<(u16, String)> {
        let body = form
            .iter()
            .map(|(k, v)| format!("{}={}", urlencode(k), urlencode(v)))
            .collect::<Vec<_>>()
            .join("&");
        // A 4xx is a normal outcome here (the device poll reads its body), so
        // configure ureq to return the response rather than discarding its
        // `authorization_pending` / `slow_down` JSON body in StatusCode.
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .new_agent();
        match agent
            .post(url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send(&body)
        {
            Ok(mut r) => {
                let status = r.status().as_u16();
                let text = r.body_mut().read_to_string().unwrap_or_default();
                Ok((status, text))
            }
            Err(e) => Err(RpcError::new(
                "AuthError",
                format!("POST {url} failed: {e}"),
            )),
        }
    }

    fn www_authenticate(&self, url: &str) -> Result<Option<String>> {
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .new_agent();
        let response = agent
            .get(url)
            .call()
            .map_err(|e| RpcError::new("AuthError", format!("GET {url} failed: {e}")))?;
        Ok(response
            .headers()
            .get("www-authenticate")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string))
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// A recorded set of canned HTTP answers, for tests.
/// One recorded POST: the URL and its form fields.
#[cfg(test)]
pub(crate) type RecordedPost = (String, Vec<(String, String)>);

#[cfg(test)]
pub(crate) struct MockHttp {
    pub gets: std::collections::HashMap<String, String>,
    pub posts: std::sync::Mutex<Vec<RecordedPost>>,
    pub post_answers: std::sync::Mutex<Vec<(u16, String)>>,
}

#[cfg(test)]
impl HttpTransport for MockHttp {
    fn get(&self, url: &str) -> Result<String> {
        self.gets
            .get(url)
            .cloned()
            .ok_or_else(|| RpcError::new("AuthError", format!("no canned GET for {url}")))
    }

    fn post_form(&self, url: &str, form: &[(&str, &str)]) -> Result<(u16, String)> {
        self.posts.lock().unwrap().push((
            url.to_string(),
            form.iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        ));
        let mut answers = self.post_answers.lock().unwrap();
        if answers.is_empty() {
            return Err(RpcError::new("AuthError", "no canned POST answer left"));
        }
        Ok(answers.remove(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn https_is_required_but_loopback_is_allowed() {
        assert!(enforce_https("https://idp.example.com/x").is_ok());
        assert!(enforce_https("http://localhost:8080/x").is_ok());
        assert!(enforce_https("http://127.0.0.1:9/x").is_ok());
        assert!(enforce_https("http://[::1]:9/x").is_ok());
    }

    #[test]
    fn a_hostname_that_merely_starts_with_loopback_is_rejected() {
        // The whole reason the check is boundary-aware.
        assert!(enforce_https("http://127.0.0.1.evil.com/x").is_err());
        assert!(enforce_https("http://localhost.evil.com/x").is_err());
        assert!(enforce_https("http://idp.example.com/x").is_err());
    }

    fn mock(gets: &[(&str, &str)], posts: &[(u16, &str)]) -> MockHttp {
        MockHttp {
            gets: gets
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            posts: Mutex::new(Vec::new()),
            post_answers: Mutex::new(posts.iter().map(|(s, b)| (*s, (*b).to_string())).collect()),
        }
    }

    const RM_URL: &str = "https://api.example.com/.well-known/oauth-protected-resource";

    fn discovery_docs(extra_rm: &str) -> Vec<(String, String)> {
        vec![
            (
                RM_URL.to_string(),
                format!(
                    r#"{{"authorization_servers":["https://idp.example.com"],"scopes_supported":["openid","email"],"resource_name":"Example API"{extra_rm}}}"#
                ),
            ),
            (
                "https://idp.example.com/.well-known/openid-configuration".to_string(),
                r#"{"token_endpoint":"https://idp.example.com/token","device_authorization_endpoint":"https://idp.example.com/device","authorization_endpoint":"https://idp.example.com/auth"}"#.to_string(),
            ),
        ]
    }

    fn challenge() -> OAuthChallenge {
        OAuthChallenge {
            resource_metadata: RM_URL.to_string(),
            client_id: Some("web-client".into()),
            ..Default::default()
        }
    }

    #[test]
    fn discovery_walks_both_stages() {
        let docs = discovery_docs("");
        let refs: Vec<(&str, &str)> = docs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let http = mock(&refs, &[]);
        let e = discover(&http, &challenge()).expect("discovery");
        assert_eq!(e.token_endpoint, "https://idp.example.com/token");
        assert_eq!(e.device_token_endpoint, "https://idp.example.com/token");
        assert!(!e.token_endpoint_is_proxy);
        assert_eq!(
            e.device_authorization_endpoint.as_deref(),
            Some("https://idp.example.com/device")
        );
        assert_eq!(e.scope, "openid email");
        assert_eq!(e.resource_name.as_deref(), Some("Example API"));
    }

    #[test]
    fn the_resource_metadata_token_endpoint_overrides_the_providers() {
        // The VGI extension: the worker proxies the exchange so it can inject a
        // secret the client does not hold.
        let docs = discovery_docs(r#","token_endpoint":"https://api.example.com/_oauth/token""#);
        let refs: Vec<(&str, &str)> = docs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let http = mock(&refs, &[]);
        let e = discover(&http, &challenge()).expect("discovery");
        assert_eq!(e.token_endpoint, "https://api.example.com/_oauth/token");
        assert_eq!(e.device_token_endpoint, "https://idp.example.com/token");
        assert!(e.token_endpoint_is_proxy);
    }

    #[test]
    fn discovery_refuses_a_document_that_names_no_issuer() {
        let http = mock(&[(RM_URL, r#"{"authorization_servers":[]}"#)], &[]);
        assert!(discover(&http, &challenge()).is_err());
    }

    #[test]
    fn a_provider_with_no_usable_flow_is_rejected() {
        let http = mock(
            &[
                (
                    RM_URL,
                    r#"{"authorization_servers":["https://idp.example.com"]}"#,
                ),
                (
                    "https://idp.example.com/.well-known/openid-configuration",
                    r#"{"token_endpoint":"https://idp.example.com/token"}"#,
                ),
            ],
            &[],
        );
        let err = discover(&http, &challenge()).expect_err("no flow");
        assert!(err.to_string().contains("neither"), "{err}");
    }

    #[test]
    fn a_token_expiry_respects_the_skew_margin() {
        let now = Instant::now();
        let t = TokenSet {
            access_token: "a".into(),
            id_token: None,
            refresh_token: None,
            expires_at: Some(now + Duration::from_secs(20)),
            use_id_token: false,
            identity: None,
        };
        assert!(t.is_valid_at(now, Duration::from_secs(0)));
        assert!(
            !t.is_valid_at(now, Duration::from_secs(30)),
            "a 30s margin should refresh a token with 20s left"
        );
    }

    #[test]
    fn a_token_without_an_expiry_never_goes_stale() {
        let t = TokenSet {
            access_token: "a".into(),
            id_token: None,
            refresh_token: None,
            expires_at: None,
            use_id_token: false,
            identity: None,
        };
        assert!(t.is_valid_at(Instant::now(), Duration::from_secs(3600)));
    }

    #[test]
    fn the_id_token_is_presented_when_asked_for() {
        let t = TokenSet {
            access_token: "access".into(),
            id_token: Some("idtok".into()),
            refresh_token: None,
            expires_at: None,
            use_id_token: true,
            identity: None,
        };
        assert_eq!(t.bearer(), "idtok");
    }

    #[test]
    fn identity_is_lifted_from_an_unverified_id_token() {
        // header.payload.signature — only the payload is read, and the
        // signature is deliberately not checked.
        let payload = r#"{"iss":"https://idp","sub":"alice","email":"a@b"}"#;
        let b64 = {
            const A: &[u8; 64] =
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
            let raw = payload.as_bytes();
            let mut out = String::new();
            for c in raw.chunks(3) {
                let b = [c[0], *c.get(1).unwrap_or(&0), *c.get(2).unwrap_or(&0)];
                let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
                out.push(A[(n >> 18) as usize & 63] as char);
                out.push(A[(n >> 12) as usize & 63] as char);
                if c.len() > 1 {
                    out.push(A[(n >> 6) as usize & 63] as char);
                }
                if c.len() > 2 {
                    out.push(A[n as usize & 63] as char);
                }
            }
            out
        };
        let jwt = format!("hdr.{b64}.sig");
        assert_eq!(
            parse_id_token_identity(&jwt),
            Some(("https://idp".to_string(), "alice".to_string()))
        );
    }

    #[test]
    fn a_refresh_carries_the_old_token_forward_when_the_provider_omits_it() {
        // Google does this, and losing it would mean never being able to
        // refresh again.
        let body = r#"{"access_token":"new","expires_in":3600}"#;
        let t = parse_token_response(body, Instant::now(), false, Some("old-refresh".into()))
            .expect("parsed");
        assert_eq!(t.refresh_token.as_deref(), Some("old-refresh"));
    }

    #[test]
    fn an_error_in_a_token_response_is_an_error() {
        let body = r#"{"error":"invalid_grant"}"#;
        let err = parse_token_response(body, Instant::now(), false, None).expect_err("refused");
        assert!(is_invalid_grant(&err), "{err}");
    }

    #[test]
    fn the_device_flow_polls_until_the_user_finishes() {
        struct Silent;
        impl UserInteraction for Silent {
            fn prompt_device_code(&self, _i: &DeviceCodePrompt) {}
        }

        let http = mock(
            &[],
            &[
                // device authorization
                (
                    200,
                    r#"{"device_code":"dev","user_code":"ABCD","verification_uri":"https://idp/act","interval":1}"#,
                ),
                // first poll: still pending
                (428, r#"{"error":"authorization_pending"}"#),
                // second poll: done
                (200, r#"{"access_token":"tok","expires_in":3600}"#),
            ],
        );
        let e = DiscoveredEndpoints {
            token_endpoint: "https://api/refresh-proxy".into(),
            device_token_endpoint: "https://idp/token".into(),
            token_endpoint_is_proxy: true,
            device_authorization_endpoint: Some("https://idp/device".into()),
            authorization_endpoint: None,
            device_client_id: Some("tv".into()),
            device_client_secret: None,
            client_id: None,
            client_secret: None,
            scope: "openid".into(),
            resource_name: None,
            use_id_token_as_bearer: false,
        };
        let t = device_code_flow(&http, &e, &Silent, Duration::from_secs(30)).expect("flow");
        assert_eq!(t.access_token, "tok");

        // The device client id must be the one presented at the token endpoint,
        // or a later refresh is rejected by providers that scope grants to a
        // client. This is the C++ defect this implementation avoids.
        let posts = http.posts.lock().unwrap();
        let poll = &posts[1];
        assert_eq!(poll.0, "https://idp/token");
        assert!(poll.1.iter().any(|(k, v)| {
            k == "grant_type" && v == "urn:ietf:params:oauth:grant-type:device_code"
        }));
        assert!(poll.1.iter().any(|(k, v)| k == "client_id" && v == "tv"));
    }

    #[test]
    fn google_s_verification_url_spelling_is_accepted() {
        struct Capture(Mutex<Option<String>>);
        impl UserInteraction for Capture {
            fn prompt_device_code(&self, i: &DeviceCodePrompt) {
                *self.0.lock().unwrap() = Some(i.verification_uri.clone());
            }
        }
        let http = mock(
            &[],
            &[
                (
                    200,
                    r#"{"device_code":"d","user_code":"C","verification_url":"https://g/device","interval":1}"#,
                ),
                (200, r#"{"access_token":"tok"}"#),
            ],
        );
        let e = DiscoveredEndpoints {
            token_endpoint: "https://idp/token".into(),
            device_token_endpoint: "https://idp/token".into(),
            token_endpoint_is_proxy: false,
            device_authorization_endpoint: Some("https://idp/device".into()),
            authorization_endpoint: None,
            device_client_id: Some("tv".into()),
            device_client_secret: None,
            client_id: None,
            client_secret: None,
            scope: "openid".into(),
            resource_name: None,
            use_id_token_as_bearer: false,
        };
        let cap = Capture(Mutex::new(None));
        device_code_flow(&http, &e, &cap, Duration::from_secs(10)).expect("flow");
        assert_eq!(
            cap.0.lock().unwrap().as_deref(),
            Some("https://g/device"),
            "the non-RFC spelling must still reach the user"
        );
    }
}

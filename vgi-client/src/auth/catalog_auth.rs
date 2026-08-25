// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! What holds a credential, and what it does when one is refused.

use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use vgi_rpc::errors::{Result, RpcError};

use super::challenge::OAuthChallenge;
use super::identity::Identity;
use super::oauth::{
    device_code_flow, discover, is_invalid_grant, refresh, DiscoveredEndpoints, HttpTransport,
    TokenSet, UserInteraction,
};

/// Refresh this far before a token actually expires.
///
/// Without a margin every expiry costs a wasted round trip: the request goes
/// out unauthenticated, comes back 401, and the whole body — including the
/// Arrow payload — is re-sent. Refreshing slightly early costs one cheap token
/// exchange instead.
const SKEW: Duration = Duration::from_secs(45);

/// A credential holder for one attached catalog.
pub trait CatalogAuth: Send + Sync {
    /// The bearer value to present, or `None` to send the request without one.
    ///
    /// Returning `None` for an expired token is deliberate: the resulting 401
    /// carries the `WWW-Authenticate` challenge that discovery needs.
    fn bearer_token(&self) -> Option<String>;

    /// React to a 401. Returns the credential to retry with.
    ///
    /// The caller retries **exactly once**; a second 401 is fatal.
    fn handle_unauthorized(&self, challenge: Option<&OAuthChallenge>) -> Result<String>;

    /// Whether any credential was configured.
    ///
    /// Distinguishes "this catalog is anonymous" from "this catalog has an
    /// identity we cannot resolve yet", which the cache treats very differently.
    fn is_explicitly_configured(&self) -> bool;

    /// The identity for cache isolation.
    fn identity(&self) -> Identity;
}

/// A static bearer token.
///
/// Cannot recover from a 401: there is no refresh, no discovery, and retrying
/// the same token unchanged is exactly what the spec forbids. So a rejection is
/// terminal and says so.
#[derive(Clone)]
pub struct BearerAuth {
    token: String,
}

impl std::fmt::Debug for BearerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BearerAuth")
            .field("token", &"<redacted>")
            .finish()
    }
}

impl BearerAuth {
    /// Hold a static token.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

impl CatalogAuth for BearerAuth {
    fn bearer_token(&self) -> Option<String> {
        Some(self.token.clone())
    }

    fn handle_unauthorized(&self, _challenge: Option<&OAuthChallenge>) -> Result<String> {
        Err(RpcError::new(
            "AuthError",
            "the bearer token was rejected by the server. A static token cannot be \
             refreshed — check the token, or attach with OAuth instead.",
        ))
    }

    fn is_explicitly_configured(&self) -> bool {
        true
    }

    fn identity(&self) -> Identity {
        Identity::Bearer(self.token.clone())
    }
}

/// No credential at all.
#[derive(Debug, Default, Clone, Copy)]
pub struct AnonymousAuth;

impl CatalogAuth for AnonymousAuth {
    fn bearer_token(&self) -> Option<String> {
        None
    }

    fn handle_unauthorized(&self, _challenge: Option<&OAuthChallenge>) -> Result<String> {
        Err(RpcError::new(
            "AuthError",
            "the server requires authentication but no credential was configured. \
             Attach with a bearer token or with OAuth.",
        ))
    }

    fn is_explicitly_configured(&self) -> bool {
        false
    }

    fn identity(&self) -> Identity {
        Identity::Anonymous
    }
}

/// What the shared OAuth state is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    InProgress,
    Complete,
    Failed,
}

struct State {
    phase: Phase,
    tokens: Option<TokenSet>,
    refresh_token: Option<String>,
    endpoints: Option<DiscoveredEndpoints>,
    error: Option<String>,
}

/// An OAuth identity that can refresh and, when it must, run an interactive flow.
///
/// # Single-flight
///
/// Several threads hitting an expired token all get a 401 at once. One becomes
/// the leader and performs the exchange; the rest wait and reuse its result, so
/// there is exactly one token exchange rather than N.
///
/// The wait is **bounded**. The C++ implementation waits without a timeout, so
/// every other thread blocks for the full duration of a device-code poll — up
/// to two minutes of a human typing a code. A bounded wait turns that into a
/// clear error instead of an apparent hang.
pub struct OAuthAuth {
    http: Box<dyn HttpTransport>,
    interaction: Box<dyn UserInteraction>,
    flow_timeout: Duration,
    wait_timeout: Duration,
    state: Mutex<State>,
    cv: Condvar,
}

impl OAuthAuth {
    /// A fresh OAuth identity with nothing seeded.
    ///
    /// `interaction` is required rather than optional: a device-code prompt
    /// with nowhere to go is a login that appears to hang forever.
    pub fn new(http: Box<dyn HttpTransport>, interaction: Box<dyn UserInteraction>) -> Self {
        Self {
            http,
            interaction,
            flow_timeout: Duration::from_secs(120),
            wait_timeout: Duration::from_secs(180),
            state: Mutex::new(State {
                phase: Phase::Idle,
                tokens: None,
                refresh_token: None,
                endpoints: None,
                error: None,
            }),
            cv: Condvar::new(),
        }
    }

    /// Seed a refresh token, so the first call can refresh instead of prompting.
    #[must_use]
    pub fn with_refresh_token(self, token: impl Into<String>) -> Self {
        self.state.lock().unwrap().refresh_token = Some(token.into());
        self
    }

    /// How long a human has to complete an interactive flow.
    #[must_use]
    pub fn with_flow_timeout(mut self, t: Duration) -> Self {
        self.flow_timeout = t;
        self
    }

    /// How long a non-leader thread waits for the leader's exchange.
    #[must_use]
    pub fn with_wait_timeout(mut self, t: Duration) -> Self {
        self.wait_timeout = t;
        self
    }

    /// Whether a usable token is held right now.
    pub fn is_authenticated(&self) -> bool {
        let s = self.state.lock().unwrap();
        s.phase == Phase::Complete
            && s.tokens
                .as_ref()
                .is_some_and(|t| t.is_valid_at(Instant::now(), SKEW))
    }

    /// Forget every token, as for a logout.
    pub fn clear(&self) {
        let mut s = self.state.lock().unwrap();
        s.phase = Phase::Idle;
        s.tokens = None;
        s.refresh_token = None;
        s.error = None;
        self.cv.notify_all();
    }

    /// Perform the exchange, with the lock released.
    fn acquire(&self, challenge: Option<&OAuthChallenge>) -> Result<TokenSet> {
        let (seeded_refresh, cached_endpoints) = {
            let s = self.state.lock().unwrap();
            (s.refresh_token.clone(), s.endpoints.clone())
        };

        // Endpoints from a previous discovery let a proactive refresh happen
        // without a challenge — the C++ implementation cannot do this, which is
        // why it needs a 401 before every refresh.
        let endpoints = match cached_endpoints {
            Some(e) => e,
            None => {
                let ch = challenge.ok_or_else(|| {
                    RpcError::new(
                        "AuthError",
                        "no OAuth challenge and no cached endpoints — the server did not \
                         advertise WWW-Authenticate, so there is nothing to discover. \
                         Seed a refresh token or supply a bearer token.",
                    )
                })?;
                let e = discover(self.http.as_ref(), ch)?;
                self.state.lock().unwrap().endpoints = Some(e.clone());
                e
            }
        };

        if let Some(rt) = seeded_refresh {
            match refresh(self.http.as_ref(), &endpoints, &rt) {
                Ok(t) => return Ok(t),
                Err(e) if is_invalid_grant(&e) => {
                    // The token is dead. Drop it so the interactive flow below
                    // is reachable, and stays reachable next time.
                    self.state.lock().unwrap().refresh_token = None;
                }
                Err(e) => return Err(e),
            }
        }

        device_code_flow(
            self.http.as_ref(),
            &endpoints,
            self.interaction.as_ref(),
            self.flow_timeout,
        )
    }
}

impl CatalogAuth for OAuthAuth {
    fn bearer_token(&self) -> Option<String> {
        let s = self.state.lock().unwrap();
        s.tokens
            .as_ref()
            .filter(|t| t.is_valid_at(Instant::now(), SKEW))
            .map(|t| t.bearer().to_string())
    }

    fn handle_unauthorized(&self, challenge: Option<&OAuthChallenge>) -> Result<String> {
        let mut s = self.state.lock().unwrap();
        loop {
            match s.phase {
                Phase::InProgress => {
                    let (guard, timed_out) = self
                        .cv
                        .wait_timeout(s, self.wait_timeout)
                        .map_err(|_| RpcError::new("AuthError", "auth state poisoned"))?;
                    s = guard;
                    if timed_out.timed_out() && s.phase == Phase::InProgress {
                        return Err(RpcError::new(
                            "AuthError",
                            "timed out waiting for another thread to finish authenticating",
                        ));
                    }
                    // Loop back and re-read the phase the leader published.
                }
                Phase::Complete => {
                    if let Some(t) = s.tokens.as_ref() {
                        if t.is_valid_at(Instant::now(), SKEW) {
                            return Ok(t.bearer().to_string());
                        }
                    }
                    // Stale: become the leader and refresh.
                    s.phase = Phase::InProgress;
                    break;
                }
                Phase::Idle | Phase::Failed => {
                    s.phase = Phase::InProgress;
                    break;
                }
            }
        }
        drop(s);

        let outcome = self.acquire(challenge);

        let mut s = self.state.lock().unwrap();
        match outcome {
            Ok(tokens) => {
                let bearer = tokens.bearer().to_string();
                if let Some(rt) = &tokens.refresh_token {
                    s.refresh_token = Some(rt.clone());
                }
                s.tokens = Some(tokens);
                s.phase = Phase::Complete;
                s.error = None;
                self.cv.notify_all();
                Ok(bearer)
            }
            Err(e) => {
                s.phase = Phase::Failed;
                s.error = Some(e.to_string());
                // Notify even on failure, or every waiter blocks until timeout.
                self.cv.notify_all();
                Err(e)
            }
        }
    }

    fn is_explicitly_configured(&self) -> bool {
        let s = self.state.lock().unwrap();
        s.refresh_token.is_some() || s.phase == Phase::Complete
    }

    fn identity(&self) -> Identity {
        let s = self.state.lock().unwrap();
        if !(s.refresh_token.is_some() || s.phase == Phase::Complete) {
            return Identity::Anonymous;
        }
        match s.tokens.as_ref().and_then(|t| t.identity.clone()) {
            Some((issuer, subject)) => Identity::OAuth { issuer, subject },
            // Configured but not yet resolved — the cache must fail closed here
            // rather than fall back to anonymous, which would let one
            // principal's rows be served to another.
            None => Identity::Unresolved,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::oauth::DeviceCodePrompt;

    struct NoInteraction;
    impl UserInteraction for NoInteraction {
        fn prompt_device_code(&self, _i: &DeviceCodePrompt) {}
    }

    struct DeadHttp;
    impl HttpTransport for DeadHttp {
        fn get(&self, _url: &str) -> Result<String> {
            Err(RpcError::new("AuthError", "no network in this test"))
        }
        fn post_form(&self, _url: &str, _f: &[(&str, &str)]) -> Result<(u16, String)> {
            Err(RpcError::new("AuthError", "no network in this test"))
        }
    }

    fn oauth() -> OAuthAuth {
        OAuthAuth::new(Box::new(DeadHttp), Box::new(NoInteraction))
    }

    #[test]
    fn a_bearer_token_is_presented_and_cannot_recover() {
        let a = BearerAuth::new("tok");
        assert_eq!(a.bearer_token().as_deref(), Some("tok"));
        let err = a.handle_unauthorized(None).expect_err("terminal");
        assert!(
            err.to_string().contains("cannot be refreshed"),
            "the error should say why retrying is pointless: {err}"
        );
        assert!(a.is_explicitly_configured());
    }

    #[test]
    fn an_anonymous_catalog_presents_nothing_and_says_what_to_do() {
        let a = AnonymousAuth;
        assert!(a.bearer_token().is_none());
        assert!(!a.is_explicitly_configured());
        assert_eq!(a.identity(), Identity::Anonymous);
        let err = a.handle_unauthorized(None).expect_err("no credential");
        assert!(
            err.to_string().contains("no credential was configured"),
            "{err}"
        );
    }

    #[test]
    fn an_unconfigured_oauth_catalog_reads_as_anonymous() {
        // Nothing seeded and no flow completed: this catalog has no identity,
        // which is different from having one we cannot resolve.
        assert_eq!(oauth().identity(), Identity::Anonymous);
        assert!(!oauth().is_explicitly_configured());
    }

    #[test]
    fn a_seeded_but_unauthenticated_oauth_catalog_refuses_to_cache() {
        // The load-bearing distinction: we were told there is an identity and
        // cannot yet say which, so caching must fail closed.
        let a = oauth().with_refresh_token("seed");
        assert!(a.is_explicitly_configured());
        assert_eq!(a.identity(), Identity::Unresolved);
        assert!(!a.identity().is_cacheable());
        assert!(a.identity().fingerprint(b"salt").is_empty());
    }

    #[test]
    fn no_challenge_and_no_cached_endpoints_is_an_actionable_error() {
        let a = oauth().with_refresh_token("seed");
        let err = a
            .handle_unauthorized(None)
            .expect_err("nothing to discover");
        let msg = err.to_string();
        assert!(msg.contains("did not advertise"), "{msg}");
        assert!(msg.contains("bearer token"), "should name a way out: {msg}");
    }

    #[test]
    fn a_failed_acquisition_leaves_the_state_retryable() {
        // Failed is not terminal: the next 401 must be able to try again, and
        // the phase must not be stuck in InProgress or every other thread hangs.
        let a = oauth().with_refresh_token("seed");
        assert!(a.handle_unauthorized(None).is_err());
        assert!(
            a.handle_unauthorized(None).is_err(),
            "second attempt still runs"
        );
        assert!(!a.is_authenticated());
    }

    #[test]
    fn clearing_forgets_everything() {
        let a = oauth().with_refresh_token("seed");
        assert!(a.is_explicitly_configured());
        a.clear();
        assert!(!a.is_explicitly_configured());
        assert_eq!(a.identity(), Identity::Anonymous);
    }

    #[test]
    fn an_expired_token_is_withheld_so_the_401_carries_a_challenge() {
        let a = oauth();
        {
            let mut s = a.state.lock().unwrap();
            s.phase = Phase::Complete;
            s.tokens = Some(TokenSet {
                access_token: "stale".into(),
                id_token: None,
                refresh_token: None,
                // Already past the skew margin.
                expires_at: Some(Instant::now() + Duration::from_secs(1)),
                use_id_token: false,
                identity: None,
            });
        }
        assert!(
            a.bearer_token().is_none(),
            "a token inside the skew margin must not be presented"
        );
        assert!(!a.is_authenticated());
    }

    #[test]
    fn a_live_token_is_presented() {
        let a = oauth();
        {
            let mut s = a.state.lock().unwrap();
            s.phase = Phase::Complete;
            s.tokens = Some(TokenSet {
                access_token: "fresh".into(),
                id_token: None,
                refresh_token: None,
                expires_at: Some(Instant::now() + Duration::from_secs(3600)),
                use_id_token: false,
                identity: Some(("https://idp".into(), "alice".into())),
            });
        }
        assert_eq!(a.bearer_token().as_deref(), Some("fresh"));
        assert!(a.is_authenticated());
        assert_eq!(
            a.identity(),
            Identity::OAuth {
                issuer: "https://idp".into(),
                subject: "alice".into()
            }
        );
        assert!(a.identity().is_cacheable());
    }
}

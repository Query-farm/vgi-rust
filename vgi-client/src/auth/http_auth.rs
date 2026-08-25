// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! An HTTP transport that presents a credential and recovers from a 401.
//!
//! # Why the client is rebuilt rather than mutated
//!
//! `vgi-rpc-client`'s `HttpClient` takes its headers at construction, so a
//! token that changes means a new client. That is cheap — the underlying
//! connection pool is per-client but a refresh is rare — and it keeps this
//! layer from reaching into the transport's internals.
//!
//! # One retry, then fatal
//!
//! A 401 triggers exactly one recovery attempt. If the retry is also refused,
//! that is the answer: looping would turn a rejected credential into a hot
//! loop against the identity provider.

use std::sync::Arc;

use arrow_array::RecordBatch;
use vgi_rpc::errors::Result;
use vgi_rpc_client::HttpClient;

use super::catalog_auth::CatalogAuth;
use super::challenge::OAuthChallenge;
use super::oauth::HttpTransport as OAuthHttp;
use crate::transport::{ExchangeStream, ProducerStream, VgiTransport};

/// An HTTP transport that authenticates.
pub struct AuthenticatedHttpTransport {
    base_url: String,
    auth: Arc<dyn CatalogAuth>,
    probe: Box<dyn OAuthHttp>,
    client: Option<HttpClient>,
    /// The token the live client was built with, so a change is detectable.
    built_with: Option<String>,
    label: String,
}

impl AuthenticatedHttpTransport {
    /// Build a transport that presents `auth`'s credential.
    pub fn new(base_url: impl Into<String>, auth: Arc<dyn CatalogAuth>) -> Self {
        Self::with_probe(base_url, auth, Box::new(super::oauth::UreqTransport))
    }

    /// As [`Self::new`], with a custom transport for the challenge probe.
    ///
    /// The probe talks to the *worker* to read its `WWW-Authenticate` header,
    /// so tests can supply a canned answer.
    pub fn with_probe(
        base_url: impl Into<String>,
        auth: Arc<dyn CatalogAuth>,
        probe: Box<dyn OAuthHttp>,
    ) -> Self {
        let base_url = base_url.into();
        Self {
            label: base_url.clone(),
            base_url,
            auth,
            probe,
            client: None,
            built_with: None,
        }
    }

    /// Rebuild the client if the credential has changed since it was made.
    fn ensure_client(&mut self) -> Result<&mut HttpClient> {
        let token = self.auth.bearer_token();
        if self.client.is_none() || self.built_with != token {
            let mut builder = HttpClient::connect(self.base_url.clone())
                .protocol_version(vgi_protocol::VGI_PROTOCOL_VERSION)
                .on_log(crate::client::worker_log_sink());
            if let Some(t) = &token {
                builder = builder.header("Authorization", &format!("Bearer {t}"))?;
            }
            self.client = Some(builder.build()?);
            self.built_with = token;
        }
        Ok(self.client.as_mut().expect("just built"))
    }

    /// Ask the worker for its `WWW-Authenticate` challenge.
    ///
    /// Needed only for a cold interactive login: once discovery has run its
    /// endpoints are cached, and a bearer-only catalog never needs one. The
    /// probe is unauthenticated on purpose — that is what provokes the header.
    fn fetch_challenge(&self) -> Option<OAuthChallenge> {
        // Probe the VGI endpoint itself. Health endpoints are commonly public
        // (the Volcanoes deployment is one), so they cannot be relied on to
        // carry the protected resource's challenge.
        let url = self.base_url.clone();
        // A challenge is advisory; a probe that fails just means discovery has
        // nothing to go on, which `handle_unauthorized` reports clearly.
        let header = self.probe.www_authenticate(&url).ok()??;
        OAuthChallenge::parse(&header)
    }

    /// Run `op`, and on a 401 recover the credential and try once more.
    fn with_retry<T>(&mut self, mut op: impl FnMut(&mut HttpClient) -> Result<T>) -> Result<T> {
        let first = {
            let client = self.ensure_client()?;
            op(client)
        };
        let err = match first {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        // vgi-rpc-client versions through 0.23.x surface HTTP 401 as an
        // `AuthenticationError` whose body is the JSON envelope, but do not
        // populate RpcError.auth_reason. Recognize both representations so the
        // auth layer actually gets a chance to recover.
        if err.auth_reason.is_none() && err.error_type != "AuthenticationError" {
            return Err(err);
        }

        let challenge = self.fetch_challenge();
        // A credential that cannot recover — a rejected bearer token — reports
        // its own reason, which is more useful than the 401.
        self.auth.handle_unauthorized(challenge.as_ref())?;

        // Force a rebuild with whatever the recovery produced.
        self.built_with = None;
        let client = self.ensure_client()?;
        op(client)
    }
}

impl VgiTransport for AuthenticatedHttpTransport {
    fn call_unary(&mut self, method: &str, params: &RecordBatch) -> Result<RecordBatch> {
        let method = method.to_string();
        let params = params.clone();
        self.with_retry(move |c| c.call_unary(&method, &params, None).map(|(b, _md)| b))
    }

    fn open_producer<'a>(
        &'a mut self,
        method: &str,
        params: &RecordBatch,
        has_header: bool,
    ) -> Result<Box<dyn ProducerStream + 'a>> {
        // A stream borrows the client for its whole life, so the retry dance
        // cannot wrap it the way a unary call can. Refresh the credential
        // up front instead: by the time a scan opens, any 401 has already been
        // seen and handled by an earlier unary call (bind, at minimum).
        let client = self.ensure_client()?;
        let session = client.open_producer(method, params, None, has_header)?;
        Ok(Box::new(super::http_stream::HttpProducerAdapter(session)))
    }

    fn open_exchange<'a>(
        &'a mut self,
        method: &str,
        params: &RecordBatch,
        has_header: bool,
    ) -> Result<Box<dyn ExchangeStream + 'a>> {
        let client = self.ensure_client()?;
        let session = client.open_exchange(method, params, None, has_header)?;
        Ok(Box::new(super::http_stream::HttpExchangeAdapter(session)))
    }

    fn label(&self) -> &str {
        &self.label
    }
}

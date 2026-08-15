// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Authentication: presenting a credential, and recovering when one is refused.
//!
//! - [`unauthorized`] implements the normative 401 contract
//!   (`vgi-rpc/docs/unauthorized-spec.md`) — how to read a refusal and decide
//!   whether retrying could help.
//! - [`challenge`] parses the `WWW-Authenticate` header that starts discovery.
//! - [`oauth`] walks the discovery chain and runs the grants.
//! - [`identity`] turns a resolved credential into the cache-isolation
//!   fingerprint, which is a security boundary.
//! - [`CatalogAuth`] ties them together: what holds a credential and what it
//!   does when one is refused.

pub mod challenge;
pub mod unauthorized;

#[cfg(feature = "oauth")]
pub mod identity;
#[cfg(feature = "oauth")]
pub mod oauth;

pub use challenge::OAuthChallenge;
pub use unauthorized::{AuthReason, Unauthorized, AUTH_REASON_HEADER};

#[cfg(feature = "oauth")]
pub use identity::{identity_scope, Identity};
#[cfg(feature = "oauth")]
pub use oauth::{
    DeviceCodePrompt, DiscoveredEndpoints, HttpTransport, StderrInteraction, TokenSet,
    UserInteraction,
};

#[cfg(feature = "oauth")]
mod catalog_auth;
#[cfg(feature = "oauth")]
mod http_auth;
#[cfg(feature = "oauth")]
pub(crate) mod http_stream;
#[cfg(feature = "oauth")]
pub use catalog_auth::{AnonymousAuth, BearerAuth, CatalogAuth, OAuthAuth};
#[cfg(feature = "oauth")]
pub use http_auth::AuthenticatedHttpTransport;

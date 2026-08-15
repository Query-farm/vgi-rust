// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Parsing the `WWW-Authenticate` challenge that starts OAuth discovery.
//!
//! A VGI service answers an unauthenticated request with
//!
//! ```text
//! WWW-Authenticate: Bearer resource_metadata="https://api/.well-known/oauth-protected-resource",
//!                          client_id="...", device_code_client_id="..."
//! ```
//!
//! and that `resource_metadata` URL is the entry point to the whole discovery
//! chain. Everything else in the header is an optional VGI extension.
//!
//! This is a real RFC 9110 auth-param parser rather than the substring scan the
//! C++ extension uses. That matters in both directions: this accepts headers
//! the C++ rejects (parameters in a different order, extra whitespace,
//! backslash escapes) and rejects some it would accept. When interoperating
//! with a specific server, check its actual header format.

use std::collections::HashMap;

/// A parsed `Bearer` challenge.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OAuthChallenge {
    /// Where the protected-resource metadata document lives. Required — without
    /// it there is nothing to discover.
    pub resource_metadata: String,
    /// Client id to present. A VGI extension; RFC 9728 does not carry one.
    pub client_id: Option<String>,
    /// Client secret, when the provider treats it as public (Google PKCE does).
    pub client_secret: Option<String>,
    /// Separate client id for the device-code flow, which some providers
    /// register as a distinct "TV/device" client.
    pub device_code_client_id: Option<String>,
    /// Client secret paired with `device_code_client_id`.
    pub device_code_client_secret: Option<String>,
    /// Whether to present the `id_token` rather than the access token.
    pub use_id_token_as_bearer: bool,
}

impl OAuthChallenge {
    /// Parse a `WWW-Authenticate` header value.
    ///
    /// Returns `None` unless this is a `Bearer` challenge carrying
    /// `resource_metadata` — anything else is not a challenge this client can
    /// act on.
    pub fn parse(header: &str) -> Option<Self> {
        let rest = strip_scheme(header, "Bearer")?;
        let params = parse_auth_params(rest);
        let resource_metadata = params.get("resource_metadata")?.clone();
        if resource_metadata.is_empty() {
            return None;
        }
        Some(Self {
            resource_metadata,
            client_id: params.get("client_id").cloned().filter(|s| !s.is_empty()),
            client_secret: params
                .get("client_secret")
                .cloned()
                .filter(|s| !s.is_empty()),
            device_code_client_id: params
                .get("device_code_client_id")
                .cloned()
                .filter(|s| !s.is_empty()),
            device_code_client_secret: params
                .get("device_code_client_secret")
                .cloned()
                .filter(|s| !s.is_empty()),
            use_id_token_as_bearer: params
                .get("use_id_token_as_bearer")
                .is_some_and(|v| v.eq_ignore_ascii_case("true") || v == "1"),
        })
    }
}

/// Match a scheme token case-insensitively and return what follows.
fn strip_scheme<'a>(header: &'a str, scheme: &str) -> Option<&'a str> {
    let h = header.trim_start();
    if h.len() < scheme.len() || !h[..scheme.len()].eq_ignore_ascii_case(scheme) {
        return None;
    }
    let rest = &h[scheme.len()..];
    // The scheme must be a whole token, not a prefix of a longer one.
    if !rest.is_empty() && !rest.starts_with(|c: char| c.is_ascii_whitespace()) {
        return None;
    }
    Some(rest.trim_start())
}

/// Parse comma-separated `name=value` / `name="quoted value"` auth params.
///
/// Quoted strings may contain commas, equals signs and backslash escapes, which
/// is exactly what a naive split on `,` gets wrong.
fn parse_auth_params(src: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let bytes: Vec<char> = src.chars().collect();
    let mut i = 0usize;

    while i < bytes.len() {
        while i < bytes.len() && (bytes[i].is_ascii_whitespace() || bytes[i] == ',') {
            i += 1;
        }
        let name_start = i;
        while i < bytes.len()
            && bytes[i] != '='
            && bytes[i] != ','
            && !bytes[i].is_ascii_whitespace()
        {
            i += 1;
        }
        if i == name_start {
            break;
        }
        let name: String = bytes[name_start..i]
            .iter()
            .collect::<String>()
            .to_lowercase();

        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != '=' {
            // A bare token with no value (e.g. a token68 credential). Skip it.
            continue;
        }
        i += 1; // '='
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }

        let value = if i < bytes.len() && bytes[i] == '"' {
            i += 1;
            let mut v = String::new();
            while i < bytes.len() {
                match bytes[i] {
                    '\\' if i + 1 < bytes.len() => {
                        v.push(bytes[i + 1]);
                        i += 2;
                    }
                    '"' => {
                        i += 1;
                        break;
                    }
                    c => {
                        v.push(c);
                        i += 1;
                    }
                }
            }
            v
        } else {
            let start = i;
            while i < bytes.len() && bytes[i] != ',' && !bytes[i].is_ascii_whitespace() {
                i += 1;
            }
            bytes[start..i].iter().collect()
        };
        out.insert(name, value);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_canonical_challenge() {
        let h = r#"Bearer resource_metadata="https://api.example.com/.well-known/oauth-protected-resource", client_id="abc123""#;
        let c = OAuthChallenge::parse(h).expect("a Bearer challenge");
        assert_eq!(
            c.resource_metadata,
            "https://api.example.com/.well-known/oauth-protected-resource"
        );
        assert_eq!(c.client_id.as_deref(), Some("abc123"));
        assert!(!c.use_id_token_as_bearer);
    }

    #[test]
    fn parameter_order_does_not_matter() {
        // The substring scan this replaces assumed resource_metadata came first.
        let h = r#"Bearer client_id="abc", device_code_client_id="tv", resource_metadata="https://x/rm""#;
        let c = OAuthChallenge::parse(h).expect("parsed");
        assert_eq!(c.resource_metadata, "https://x/rm");
        assert_eq!(c.device_code_client_id.as_deref(), Some("tv"));
    }

    #[test]
    fn the_scheme_is_matched_case_insensitively() {
        for scheme in ["Bearer", "bearer", "BEARER"] {
            let h = format!(r#"{scheme} resource_metadata="https://x/rm""#);
            assert!(OAuthChallenge::parse(&h).is_some(), "{scheme}");
        }
    }

    #[test]
    fn a_different_scheme_is_not_a_bearer_challenge() {
        assert!(OAuthChallenge::parse(r#"Basic realm="x""#).is_none());
        // Nor is a scheme that merely starts with the same letters.
        assert!(OAuthChallenge::parse(r#"BearerToken resource_metadata="https://x""#).is_none());
    }

    #[test]
    fn a_challenge_without_resource_metadata_is_not_actionable() {
        assert!(OAuthChallenge::parse(r#"Bearer realm="api""#).is_none());
        assert!(OAuthChallenge::parse(r#"Bearer resource_metadata="""#).is_none());
    }

    #[test]
    fn quoted_values_may_contain_commas_and_equals() {
        // Exactly what a naive comma split gets wrong.
        let h = r#"Bearer resource_metadata="https://x/rm?a=1,b=2", client_id="k=v""#;
        let c = OAuthChallenge::parse(h).expect("parsed");
        assert_eq!(c.resource_metadata, "https://x/rm?a=1,b=2");
        assert_eq!(c.client_id.as_deref(), Some("k=v"));
    }

    #[test]
    fn backslash_escapes_are_unescaped() {
        let h = r#"Bearer resource_metadata="https://x/rm", client_id="say \"hi\"""#;
        let c = OAuthChallenge::parse(h).expect("parsed");
        assert_eq!(c.client_id.as_deref(), Some(r#"say "hi""#));
    }

    #[test]
    fn unquoted_values_are_accepted() {
        let h = "Bearer resource_metadata=https://x/rm, client_id=abc";
        let c = OAuthChallenge::parse(h).expect("parsed");
        assert_eq!(c.resource_metadata, "https://x/rm");
        assert_eq!(c.client_id.as_deref(), Some("abc"));
    }

    #[test]
    fn the_id_token_flag_accepts_the_spellings_servers_use() {
        for v in ["true", "TRUE", "1"] {
            let h =
                format!(r#"Bearer resource_metadata="https://x", use_id_token_as_bearer="{v}""#);
            assert!(
                OAuthChallenge::parse(&h).unwrap().use_id_token_as_bearer,
                "{v}"
            );
        }
        let h = r#"Bearer resource_metadata="https://x", use_id_token_as_bearer="false""#;
        assert!(!OAuthChallenge::parse(h).unwrap().use_id_token_as_bearer);
    }

    #[test]
    fn extra_whitespace_is_tolerated() {
        let h = "Bearer   resource_metadata = \"https://x/rm\" ,  client_id = \"abc\"  ";
        let c = OAuthChallenge::parse(h).expect("parsed");
        assert_eq!(c.resource_metadata, "https://x/rm");
        assert_eq!(c.client_id.as_deref(), Some("abc"));
    }
}

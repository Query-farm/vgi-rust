// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The 401 contract, per `vgi-rpc/docs/unauthorized-spec.md`.
//!
//! That document is the normative cross-language contract, and §6 is a client
//! checklist. The four MUSTs it lays on a client are all implemented here:
//!
//! 1. Read `reason` from the JSON body, fall back to the `VGI-Auth-Reason`
//!    header, fall back to [`AuthReason::Unauthorized`].
//! 2. Surface `proxy_hint` to whoever sees the error.
//! 3. **Degrade without raising** on a non-envelope body — a 401 can come from
//!    a gateway, a WAF or an SSO portal that has never heard of VGI.
//! 4. Never attempt to parse Arrow IPC from a 401 body.
//!
//! The reason code is what drives recovery: `expired_credential` means refresh
//! and retry, `invalid_credential` means do not retry unchanged, and
//! `insufficient_scope` is deliberately a 401 rather than a 403 because the
//! authenticate callback runs before any method is resolved.

use std::collections::HashMap;

/// Why a request was refused.
///
/// A closed set — an unrecognised code from a newer peer maps to
/// [`AuthReason::Unauthorized`] rather than failing, so a client keeps working
/// against a service that has learned a new code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthReason {
    /// No credential was presented at all. Send one.
    MissingCredential,
    /// A credential was presented and rejected. Do not retry unchanged.
    InvalidCredential,
    /// A well-formed credential outside its validity window. Refresh and retry.
    ExpiredCredential,
    /// Identified but not permitted. Do not retry; escalate.
    InsufficientScope,
    /// The request did not arrive through the trusted proxy. Operator problem.
    ProxyRequired,
    /// Refused, unclassified.
    Unauthorized,
}

impl AuthReason {
    /// The wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MissingCredential => "missing_credential",
            Self::InvalidCredential => "invalid_credential",
            Self::ExpiredCredential => "expired_credential",
            Self::InsufficientScope => "insufficient_scope",
            Self::ProxyRequired => "proxy_required",
            Self::Unauthorized => "unauthorized",
        }
    }

    /// Parse a wire code, mapping anything unrecognised to `Unauthorized`.
    pub fn parse(s: &str) -> Self {
        match s {
            "missing_credential" => Self::MissingCredential,
            "invalid_credential" => Self::InvalidCredential,
            "expired_credential" => Self::ExpiredCredential,
            "insufficient_scope" => Self::InsufficientScope,
            "proxy_required" => Self::ProxyRequired,
            _ => Self::Unauthorized,
        }
    }

    /// Whether refreshing the credential and retrying could plausibly help.
    ///
    /// Only `expired_credential` qualifies. Retrying an `invalid_credential`
    /// unchanged is explicitly forbidden by the spec, and re-presenting the
    /// same identity for `insufficient_scope` cannot change the answer.
    pub fn is_retryable_after_refresh(self) -> bool {
        matches!(self, Self::ExpiredCredential)
    }
}

/// A parsed 401.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unauthorized {
    /// Why the request was refused.
    pub reason: AuthReason,
    /// Human-readable detail. May be empty.
    pub detail: String,
    /// Operator-facing hint when a proxy is required. Absent, never empty.
    pub proxy_hint: Option<String>,
}

/// Header carrying the reason code.
pub const AUTH_REASON_HEADER: &str = "VGI-Auth-Reason";

/// Cap on how much of an unrecognised body is kept as detail.
///
/// A gateway's HTML error page can be arbitrarily large and is not useful
/// verbatim; the reference implementation bounds it the same way.
const MAX_DETAIL: usize = 500;

impl Unauthorized {
    /// Parse a 401 from its body and headers.
    ///
    /// Never fails. A body that is not the VGI envelope degrades to
    /// `Unauthorized` with a bounded excerpt as detail, because a 401 from a
    /// gateway or SSO portal is still a 401 and must not become a parse error.
    pub fn parse(body: &str, headers: &HashMap<String, String>) -> Self {
        // Header lookup is case-insensitive: HTTP header names are, and the
        // exact casing depends on which proxy last touched the response.
        let header_reason = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(AUTH_REASON_HEADER))
            .map(|(_, v)| AuthReason::parse(v));

        match parse_envelope(body) {
            Some((reason, detail, proxy_hint)) => Self {
                // The body wins over the header when both are present; the
                // header is the fallback for a body that did not survive.
                reason: reason.or(header_reason).unwrap_or(AuthReason::Unauthorized),
                detail,
                proxy_hint,
            },
            None => Self {
                reason: header_reason.unwrap_or(AuthReason::Unauthorized),
                detail: excerpt(body),
                proxy_hint: None,
            },
        }
    }

    /// A message suitable for surfacing to a caller.
    ///
    /// Includes `proxy_hint` when present — the spec requires it reach whoever
    /// sees the error, and it is usually the only actionable part.
    pub fn message(&self) -> String {
        let mut out = format!("unauthorized ({})", self.reason.as_str());
        if !self.detail.is_empty() {
            out.push_str(": ");
            out.push_str(&self.detail);
        }
        if let Some(hint) = &self.proxy_hint {
            out.push_str(" [");
            out.push_str(hint);
            out.push(']');
        }
        out
    }
}

/// Keep an unrecognised body short, and collapse HTML to a one-liner.
fn excerpt(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let looks_html = trimmed.starts_with('<')
        || trimmed.get(..64).is_some_and(|h| {
            let l = h.to_ascii_lowercase();
            l.contains("<html") || l.contains("<!doctype")
        });
    if looks_html {
        return "(HTML error page)".to_string();
    }
    if trimmed.len() <= MAX_DETAIL {
        trimmed.to_string()
    } else {
        let mut cut = MAX_DETAIL;
        while cut > 0 && !trimmed.is_char_boundary(cut) {
            cut -= 1;
        }
        format!("{}…", &trimmed[..cut])
    }
}

/// Pull `reason` / `detail` / `proxy_hint` out of the JSON envelope.
///
/// Hand-rolled so the crate needs no JSON dependency for one flat object of
/// string fields. Returns `None` when the body is not the VGI envelope at all,
/// which is the signal to degrade rather than raise.
fn parse_envelope(body: &str) -> Option<(Option<AuthReason>, String, Option<String>)> {
    let t = body.trim();
    if !t.starts_with('{') || !t.ends_with('}') {
        return None;
    }
    // `error` is the marker that this is the VGI envelope rather than some
    // other service's JSON.
    json_field(t, "error")?;
    let reason = json_field(t, "reason").map(|r| AuthReason::parse(&r));
    let detail = json_field(t, "detail").unwrap_or_default();
    // Absent, never empty — an empty hint is treated as no hint.
    let proxy_hint = json_field(t, "proxy_hint").filter(|h| !h.is_empty());
    Some((reason, excerpt(&detail), proxy_hint))
}

/// Extract one top-level string field from a flat JSON object.
fn json_field(src: &str, name: &str) -> Option<String> {
    let key = format!("\"{name}\"");
    let mut from = 0usize;
    loop {
        let at = src[from..].find(&key)? + from;
        // Must be followed by a colon (allowing whitespace) to be a key rather
        // than a value that happens to match.
        let rest = &src[at + key.len()..];
        let rest_trim = rest.trim_start();
        if !rest_trim.starts_with(':') {
            from = at + key.len();
            continue;
        }
        let after = rest_trim[1..].trim_start();
        if !after.starts_with('"') {
            return None; // non-string value; not a shape we carry
        }
        let mut out = String::new();
        let mut chars = after[1..].chars();
        while let Some(c) = chars.next() {
            match c {
                '"' => return Some(out),
                '\\' => match chars.next()? {
                    'n' => out.push('\n'),
                    't' => out.push('\t'),
                    'r' => out.push('\r'),
                    'u' => {
                        let hex: String = chars.by_ref().take(4).collect();
                        let cp = u32::from_str_radix(&hex, 16).ok()?;
                        out.push(char::from_u32(cp)?);
                    }
                    other => out.push(other),
                },
                other => out.push(other),
            }
        }
        return None; // unterminated string
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn reads_the_reason_from_the_body() {
        let body =
            r#"{"error":"unauthorized","reason":"expired_credential","detail":"token aged out"}"#;
        let u = Unauthorized::parse(body, &HashMap::new());
        assert_eq!(u.reason, AuthReason::ExpiredCredential);
        assert_eq!(u.detail, "token aged out");
        assert!(u.reason.is_retryable_after_refresh());
    }

    #[test]
    fn falls_back_to_the_header_when_the_body_is_not_the_envelope() {
        let u = Unauthorized::parse(
            "Access denied by policy",
            &headers(&[("vgi-auth-reason", "insufficient_scope")]),
        );
        assert_eq!(u.reason, AuthReason::InsufficientScope);
        assert_eq!(u.detail, "Access denied by policy");
        assert!(!u.reason.is_retryable_after_refresh());
    }

    #[test]
    fn header_lookup_ignores_case() {
        for spelling in ["VGI-Auth-Reason", "vgi-auth-reason", "VGI-AUTH-REASON"] {
            let u = Unauthorized::parse("", &headers(&[(spelling, "missing_credential")]));
            assert_eq!(u.reason, AuthReason::MissingCredential, "{spelling}");
        }
    }

    #[test]
    fn an_html_gateway_page_degrades_instead_of_raising() {
        // MUST #3: a 401 may come from something that has never heard of VGI.
        let body = "<!DOCTYPE html><html><body>Sign in to continue</body></html>";
        let u = Unauthorized::parse(body, &HashMap::new());
        assert_eq!(u.reason, AuthReason::Unauthorized);
        assert_eq!(u.detail, "(HTML error page)");
    }

    #[test]
    fn an_unrecognised_reason_maps_to_the_fallback() {
        let body = r#"{"error":"unauthorized","reason":"something_new","detail":""}"#;
        assert_eq!(
            Unauthorized::parse(body, &HashMap::new()).reason,
            AuthReason::Unauthorized,
        );
    }

    #[test]
    fn proxy_hint_is_surfaced_and_absent_when_empty() {
        let with = r#"{"error":"unauthorized","reason":"proxy_required","detail":"no proof","proxy_hint":"route via the gateway"}"#;
        let u = Unauthorized::parse(with, &HashMap::new());
        assert_eq!(u.proxy_hint.as_deref(), Some("route via the gateway"));
        assert!(u.message().contains("route via the gateway"));

        let empty =
            r#"{"error":"unauthorized","reason":"proxy_required","detail":"","proxy_hint":""}"#;
        assert_eq!(Unauthorized::parse(empty, &HashMap::new()).proxy_hint, None);
    }

    #[test]
    fn a_long_body_is_bounded() {
        let long = "x".repeat(5000);
        let u = Unauthorized::parse(&long, &HashMap::new());
        assert!(
            u.detail.len() <= MAX_DETAIL + 4,
            "got {} chars",
            u.detail.len()
        );
        assert!(u.detail.ends_with('…'));
    }

    #[test]
    fn multibyte_bodies_are_cut_on_a_char_boundary() {
        // Truncating mid-codepoint would panic on a slice.
        let long = "é".repeat(5000);
        let u = Unauthorized::parse(&long, &HashMap::new());
        assert!(u.detail.ends_with('…'));
    }

    #[test]
    fn json_escapes_survive() {
        let body = r#"{"error":"unauthorized","reason":"invalid_credential","detail":"bad \"token\"\nline two"}"#;
        let u = Unauthorized::parse(body, &HashMap::new());
        assert_eq!(u.detail, "bad \"token\"\nline two");
    }

    #[test]
    fn a_json_body_from_some_other_service_is_not_mistaken_for_the_envelope() {
        // No `error` key — this is somebody else's JSON, so degrade.
        let body = r#"{"message":"nope","reason":"expired_credential"}"#;
        let u = Unauthorized::parse(body, &HashMap::new());
        assert_eq!(
            u.reason,
            AuthReason::Unauthorized,
            "a stray `reason` key must not be read as ours"
        );
    }

    #[test]
    fn every_reason_code_round_trips() {
        for r in [
            AuthReason::MissingCredential,
            AuthReason::InvalidCredential,
            AuthReason::ExpiredCredential,
            AuthReason::InsufficientScope,
            AuthReason::ProxyRequired,
            AuthReason::Unauthorized,
        ] {
            assert_eq!(AuthReason::parse(r.as_str()), r);
        }
    }
}

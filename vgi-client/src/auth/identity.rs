// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Who the caller is, for cache isolation.
//!
//! A result cache keyed only on the query would serve one principal's rows to
//! another. The fingerprint here is what keeps entries apart, so it is a
//! **security boundary**, not a convenience.
//!
//! Three outcomes, and the difference between the last two is load-bearing:
//!
//! | Situation | Fingerprint | Meaning |
//! |---|---|---|
//! | no credential configured | `"anon"` | one shared anonymous identity; caching is fine |
//! | resolved OAuth or bearer | `"oauth:…"` / `"bearer:…"` | a specific principal |
//! | configured but unresolved | `""` (empty) | **refuse to cache** |
//!
//! The empty string is not "no identity" — it is "we were told there is an
//! identity and cannot yet say which", and every consumer must treat it as
//! fail-closed. An OAuth catalog that has not completed its flow is exactly
//! this case.
//!
//! The OAuth branch hashes the stable `(issuer, subject)` pair rather than the
//! token, so a refresh does not change the fingerprint and a cache entry
//! survives it.

use sha2::{Digest, Sha256};

/// Domain separator for the OAuth branch. Part of the hashed preimage.
const OAUTH_DOMAIN: &str = "vgi-cache-oauth:v1";
/// Domain separator for the bearer branch.
const BEARER_DOMAIN: &str = "vgi-cache-principal:v1";
/// Field separator: ASCII unit separator, which cannot occur in an issuer or
/// subject, so `(a, bc)` and `(ab, c)` cannot collide.
const SEP: u8 = 0x1f;

/// The identity a set of credentials resolves to.
#[derive(Clone, PartialEq, Eq)]
pub enum Identity {
    /// No credential is configured. All such callers share one cache scope.
    Anonymous,
    /// A resolved OAuth principal.
    OAuth {
        /// The `iss` claim.
        issuer: String,
        /// The `sub` claim.
        subject: String,
    },
    /// A static bearer token.
    Bearer(String),
    /// Credentials are configured but have not resolved yet.
    Unresolved,
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anonymous => f.write_str("Anonymous"),
            Self::OAuth { issuer, subject } => f
                .debug_struct("OAuth")
                .field("issuer", issuer)
                .field("subject", subject)
                .finish(),
            Self::Bearer(_) => f.write_str("Bearer(<redacted>)"),
            Self::Unresolved => f.write_str("Unresolved"),
        }
    }
}

impl Identity {
    /// The cache-isolation fingerprint.
    ///
    /// An empty string means **do not cache** — see the module docs.
    pub fn fingerprint(&self, salt: &[u8]) -> String {
        match self {
            Self::Anonymous => "anon".to_string(),
            Self::OAuth { issuer, subject } => {
                format!(
                    "oauth:{}",
                    hash(
                        &[
                            OAUTH_DOMAIN.as_bytes(),
                            issuer.as_bytes(),
                            subject.as_bytes()
                        ],
                        &[]
                    )
                )
            }
            Self::Bearer(token) => {
                // Keyed with a per-install salt, unlike the C++ implementation's
                // constant prefix. Without a secret in the preimage, anyone who
                // can read the on-disk cache index and guess a token can confirm
                // it by recomputing the hash. Caches are not shared between the
                // two implementations, so this is free to be stronger.
                format!(
                    "bearer:{}",
                    hash(&[BEARER_DOMAIN.as_bytes(), token.as_bytes()], salt)
                )
            }
            Self::Unresolved => String::new(),
        }
    }

    /// Whether results for this identity may be cached at all.
    pub fn is_cacheable(&self) -> bool {
        !matches!(self, Self::Unresolved)
    }
}

/// SHA-256 over `SEP`-joined parts, optionally keyed, as lowercase hex.
fn hash(parts: &[&[u8]], salt: &[u8]) -> String {
    let mut h = Sha256::new();
    if !salt.is_empty() {
        h.update(salt);
        h.update([SEP]);
    }
    for (i, p) in parts.iter().enumerate() {
        if i > 0 {
            h.update([SEP]);
        }
        h.update(p);
    }
    format!("{:x}", h.finalize())
}

/// Scope a cache key to a catalog *and* the identity reading it.
///
/// Returns `None` when the identity is unresolved, which callers must treat as
/// "do not cache" rather than substituting a default.
pub fn identity_scope(catalog: &str, identity: &Identity, salt: &[u8]) -> Option<String> {
    let fp = identity.fingerprint(salt);
    if fp.is_empty() {
        return None;
    }
    Some(format!("{catalog}\u{1f}{fp}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SALT: &[u8] = b"install-salt";

    #[test]
    fn anonymous_callers_share_one_scope() {
        assert_eq!(Identity::Anonymous.fingerprint(SALT), "anon");
    }

    #[test]
    fn an_unresolved_identity_refuses_to_cache() {
        assert!(Identity::Unresolved.fingerprint(SALT).is_empty());
        assert!(!Identity::Unresolved.is_cacheable());
        assert_eq!(identity_scope("cat", &Identity::Unresolved, SALT), None);
    }

    #[test]
    fn different_principals_never_collide() {
        let a = Identity::OAuth {
            issuer: "https://idp".into(),
            subject: "alice".into(),
        };
        let b = Identity::OAuth {
            issuer: "https://idp".into(),
            subject: "bob".into(),
        };
        assert_ne!(a.fingerprint(SALT), b.fingerprint(SALT));
    }

    #[test]
    fn the_separator_prevents_field_smearing() {
        // Without a separator, ("ab","c") and ("a","bc") would hash the same
        // and two different principals would share a cache scope.
        let x = Identity::OAuth {
            issuer: "ab".into(),
            subject: "c".into(),
        };
        let y = Identity::OAuth {
            issuer: "a".into(),
            subject: "bc".into(),
        };
        assert_ne!(x.fingerprint(SALT), y.fingerprint(SALT));
    }

    #[test]
    fn an_oauth_fingerprint_survives_a_token_refresh() {
        // The whole reason to hash (iss, sub) and not the token: a refresh must
        // not invalidate the cache.
        let before = Identity::OAuth {
            issuer: "https://idp".into(),
            subject: "alice".into(),
        };
        let after = before.clone();
        assert_eq!(before.fingerprint(SALT), after.fingerprint(SALT));
    }

    #[test]
    fn oauth_and_bearer_live_in_different_namespaces() {
        let o = Identity::OAuth {
            issuer: "x".into(),
            subject: "y".into(),
        };
        let b = Identity::Bearer("x\u{1f}y".into());
        assert_ne!(o.fingerprint(SALT), b.fingerprint(SALT));
        assert!(o.fingerprint(SALT).starts_with("oauth:"));
        assert!(b.fingerprint(SALT).starts_with("bearer:"));
    }

    #[test]
    fn the_bearer_salt_changes_the_fingerprint() {
        // This is what stops an attacker who can read the cache index from
        // confirming a guessed token.
        let t = Identity::Bearer("secret".into());
        assert_ne!(t.fingerprint(b"salt-a"), t.fingerprint(b"salt-b"));
    }

    #[test]
    fn a_scope_names_both_the_catalog_and_the_principal() {
        let id = Identity::OAuth {
            issuer: "i".into(),
            subject: "s".into(),
        };
        let a = identity_scope("cat_a", &id, SALT).unwrap();
        let b = identity_scope("cat_b", &id, SALT).unwrap();
        assert_ne!(a, b, "the same principal in two catalogs is two scopes");

        let other = Identity::OAuth {
            issuer: "i".into(),
            subject: "other".into(),
        };
        assert_ne!(
            a,
            identity_scope("cat_a", &other, SALT).unwrap(),
            "two principals in one catalog are two scopes"
        );
    }

    #[test]
    fn fingerprints_are_stable_across_calls() {
        let id = Identity::Bearer("t".into());
        assert_eq!(id.fingerprint(SALT), id.fingerprint(SALT));
    }
}

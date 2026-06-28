// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Secret access for functions.
//!
//! VGI uses a two-phase bind: the first `bind` returns secret lookup requests
//! (`lookup_secret_types`/`scopes`/`names`); the extension resolves them and
//! re-binds with `resolved_secrets_provided=true` and a `secrets` blob (an IPC
//! batch keyed by secret name → struct of fields). This module parses that
//! blob and exposes field access.

use std::collections::HashMap;

use arrow_array::cast::AsArray;
use arrow_array::{Array, StructArray};
use vgi_rpc::Result;

use crate::ipc;

/// A request for a secret to be resolved at bind time.
#[derive(Clone, Debug)]
pub struct SecretLookup {
    pub secret_type: String,
    pub scope: Option<String>,
    pub name: Option<String>,
}

/// Parsed resolved secrets: secret name → field map (string-rendered values).
#[derive(Clone, Default)]
pub struct Secrets {
    /// name → { field → value-as-string }
    pub by_name: HashMap<String, HashMap<String, String>>,
}

impl Secrets {
    /// Parse the IPC secrets blob. The shape mirrors Python: one column per
    /// secret name, each a struct of the secret's fields.
    pub fn parse(bytes: &[u8]) -> Result<Secrets> {
        if bytes.is_empty() {
            return Ok(Secrets::default());
        }
        let batch = ipc::read_batch(bytes)?;
        let mut by_name = HashMap::new();
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(i);
            let mut fields = HashMap::new();
            if let Some(sa) = col.as_any().downcast_ref::<StructArray>() {
                for (j, sf) in sa.fields().iter().enumerate() {
                    fields.insert(sf.name().clone(), render(sa.column(j), 0));
                }
            } else {
                fields.insert(field.name().clone(), render(col, 0));
            }
            by_name.insert(field.name().clone(), fields);
        }
        Ok(Secrets { by_name })
    }

    /// Get a secret field value (first matching secret of any name).
    pub fn field(&self, field: &str) -> Option<String> {
        self.by_name.values().find_map(|m| m.get(field).cloned())
    }

    /// Get a named secret's field.
    pub fn named_field(&self, name: &str, field: &str) -> Option<String> {
        self.by_name.get(name).and_then(|m| m.get(field).cloned())
    }

    /// Iterate every resolved secret as `(name, fields)`. Resolved secrets are
    /// keyed by their unique DuckDB secret name, so several secrets of the same
    /// type (e.g. one per S3 bucket) all appear here.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &HashMap<String, String>)> {
        self.by_name.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// The DuckDB secret type of the named secret (its serialized `type` field).
    pub fn secret_type(&self, name: &str) -> Option<String> {
        self.named_field(name, "type")
    }

    /// Every resolved secret of `secret_type`, matched on each secret's
    /// serialized `type` field (since secrets are keyed by name, not type).
    pub fn of_type<'a>(
        &'a self,
        secret_type: &'a str,
    ) -> impl Iterator<Item = &'a HashMap<String, String>> + 'a {
        self.by_name
            .values()
            .filter(move |m| m.get("type").map(String::as_str) == Some(secret_type))
    }

    /// The fields of the resolved secret whose `scope` is the longest prefix of
    /// `path`. Use this when a worker requested secrets for several scopes (e.g.
    /// one per cloud path / bucket) and must pick the right one per path. The
    /// connector serializes each secret's `scope` as a newline-joined list of
    /// prefixes; a secret with no (or empty) `scope` matches as a last-resort
    /// fallback (covers unscoped secrets and older connectors that don't send a
    /// scope). Returns `None` only when there are no candidate secrets.
    pub fn for_scope(&self, path: &str) -> Option<&HashMap<String, String>> {
        self.select_for_scope(path, None)
    }

    /// Like [`for_scope`](Self::for_scope) but only over secrets of `secret_type`
    /// — the precise selector for cloud paths (e.g. the `s3` secret matching a
    /// given `s3://…` URL when several buckets are in play).
    pub fn for_scope_of_type(
        &self,
        path: &str,
        secret_type: &str,
    ) -> Option<&HashMap<String, String>> {
        self.select_for_scope(path, Some(secret_type))
    }

    fn select_for_scope(
        &self,
        path: &str,
        secret_type: Option<&str>,
    ) -> Option<&HashMap<String, String>> {
        let typed = |m: &HashMap<String, String>| {
            secret_type.is_none_or(|t| m.get("type").map(String::as_str) == Some(t))
        };
        let mut best: Option<(usize, &HashMap<String, String>)> = None;
        let mut fallback: Option<&HashMap<String, String>> = None;
        for fields in self.by_name.values().filter(|m| typed(m)) {
            match fields.get("scope") {
                Some(scope) if !scope.is_empty() => {
                    for prefix in scope.split('\n').filter(|p| !p.is_empty()) {
                        if path.starts_with(prefix)
                            && best.is_none_or(|(blen, _)| prefix.len() > blen)
                        {
                            best = Some((prefix.len(), fields));
                        }
                    }
                }
                _ => {
                    if fallback.is_none() {
                        fallback = Some(fields);
                    }
                }
            }
        }
        best.map(|(_, f)| f).or(fallback)
    }

    /// A field of the best scope-matching secret for `path` (see
    /// [`for_scope`](Self::for_scope)).
    pub fn field_for(&self, path: &str, field: &str) -> Option<String> {
        self.for_scope(path).and_then(|m| m.get(field).cloned())
    }
}

/// Render a single array element as a string (best-effort).
fn render(arr: &dyn Array, i: usize) -> String {
    if arr.is_null(i) {
        return String::new();
    }
    if let Some(s) = arr.as_string_opt::<i32>() {
        return s.value(i).to_string();
    }
    if let Some(s) = arr.as_string_opt::<i64>() {
        return s.value(i).to_string();
    }
    if let Some(b) = arr.as_boolean_opt() {
        return b.value(i).to_string();
    }
    if let Some(v) = crate::numeric::array_value_i64(&arr_to_ref(arr), i) {
        return v.to_string();
    }
    String::new()
}

fn arr_to_ref(arr: &dyn Array) -> arrow_array::ArrayRef {
    arrow_array::make_array(arr.to_data())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret(fields: &[(&str, &str)]) -> HashMap<String, String> {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn secrets(entries: &[(&str, HashMap<String, String>)]) -> Secrets {
        Secrets {
            by_name: entries
                .iter()
                .map(|(n, m)| (n.to_string(), m.clone()))
                .collect(),
        }
    }

    #[test]
    fn for_scope_picks_longest_prefix_match() {
        let s = secrets(&[
            ("s3", secret(&[("key_id", "A"), ("scope", "s3://bucket-a")])),
            (
                "s3:other",
                secret(&[("key_id", "B"), ("scope", "s3://bucket-b\ns3://bucket-b2")]),
            ),
        ]);
        assert_eq!(
            s.field_for("s3://bucket-a/data/x.dat", "key_id").as_deref(),
            Some("A")
        );
        assert_eq!(
            s.field_for("s3://bucket-b/x.dat", "key_id").as_deref(),
            Some("B")
        );
        assert_eq!(
            s.field_for("s3://bucket-b2/y.dat", "key_id").as_deref(),
            Some("B")
        );
    }

    #[test]
    fn for_scope_prefers_more_specific_scope() {
        let s = secrets(&[
            (
                "s3",
                secret(&[("key_id", "broad"), ("scope", "s3://bucket")]),
            ),
            (
                "s3:narrow",
                secret(&[("key_id", "narrow"), ("scope", "s3://bucket/data")]),
            ),
        ]);
        // Longer (more specific) prefix wins for a path under it.
        assert_eq!(
            s.field_for("s3://bucket/data/x.dat", "key_id").as_deref(),
            Some("narrow")
        );
        // A path outside the narrow scope falls to the broad one.
        assert_eq!(
            s.field_for("s3://bucket/other/x.dat", "key_id").as_deref(),
            Some("broad")
        );
    }

    #[test]
    fn for_scope_falls_back_to_unscoped() {
        // No scope field (old connector) → the single secret is the fallback.
        let s = secrets(&[("s3", secret(&[("key_id", "only")]))]);
        assert_eq!(
            s.field_for("s3://any/x.dat", "key_id").as_deref(),
            Some("only")
        );
        // Empty scope also counts as unscoped.
        let s2 = secrets(&[("s3", secret(&[("key_id", "u"), ("scope", "")]))]);
        assert_eq!(
            s2.field_for("s3://any/x.dat", "key_id").as_deref(),
            Some("u")
        );
    }

    #[test]
    fn for_scope_none_when_no_match_and_no_fallback() {
        let s = secrets(&[("s3", secret(&[("key_id", "A"), ("scope", "s3://bucket-a")]))]);
        // No unscoped fallback and the path matches no scope.
        assert!(s.for_scope("s3://bucket-z/x.dat").is_none());
    }

    #[test]
    fn for_scope_empty_secrets_is_none() {
        assert!(Secrets::default().for_scope("s3://b/x").is_none());
    }

    #[test]
    fn type_aware_accessors() {
        let s = secrets(&[
            (
                "my_s3",
                secret(&[("type", "s3"), ("key_id", "A"), ("scope", "s3://a")]),
            ),
            (
                "my_s3_b",
                secret(&[("type", "s3"), ("key_id", "B"), ("scope", "s3://b")]),
            ),
            ("my_gcs", secret(&[("type", "gcs"), ("key_id", "G")])),
        ]);
        // know the type of a named secret
        assert_eq!(s.secret_type("my_s3").as_deref(), Some("s3"));
        assert_eq!(s.secret_type("my_gcs").as_deref(), Some("gcs"));
        // all secrets of a type (multiple s3 instances coexist)
        assert_eq!(s.of_type("s3").count(), 2);
        assert_eq!(s.of_type("gcs").count(), 1);
        assert_eq!(s.of_type("azure").count(), 0);
        // scope + type selection picks the right s3 secret per path
        assert_eq!(
            s.for_scope_of_type("s3://b/x.dat", "s3")
                .and_then(|m| m.get("key_id"))
                .map(String::as_str),
            Some("B")
        );
        // iter exposes names
        let mut names: Vec<&str> = s.iter().map(|(n, _)| n).collect();
        names.sort();
        assert_eq!(names, vec!["my_gcs", "my_s3", "my_s3_b"]);
    }
}

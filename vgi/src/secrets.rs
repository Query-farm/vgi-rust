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
///
/// Used both statically ([`FunctionMetadata::required_secrets`]) and
/// dynamically (a function's `secret_lookups`, the two-phase bind). The
/// simplest form — "a secret of type T" — is [`SecretLookup::of_type`] or a
/// bare `&str` via `From` (`"s3".into()`); add a scope or name with
/// [`with_scope`](Self::with_scope) / [`with_name`](Self::with_name).
///
/// A lookup with an empty `secret_type` is rejected when the catalog or bind
/// response is built (see [`SecretLookup::validate`]) instead of being sent: a
/// requirement the client cannot resolve would otherwise fail silently.
///
/// [`FunctionMetadata::required_secrets`]: crate::function::FunctionMetadata::required_secrets
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretLookup {
    pub secret_type: String,
    pub scope: Option<String>,
    pub name: Option<String>,
}

impl SecretLookup {
    /// A lookup for a secret of `secret_type`, with no scope and no name.
    pub fn of_type(secret_type: impl Into<String>) -> Self {
        SecretLookup {
            secret_type: secret_type.into(),
            scope: None,
            name: None,
        }
    }

    /// Restrict the lookup to secrets whose scope best matches `scope`.
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Ask for the secret named `name` (it must also have the lookup's type).
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Reject a lookup the client could never resolve (an empty or blank
    /// `secret_type`). Called for every static and dynamic lookup before it is
    /// put on the wire.
    pub fn validate(&self) -> Result<()> {
        if self.secret_type.trim().is_empty() {
            return Err(vgi_rpc::RpcError::value_error(format!(
                "secret lookup has an empty secret_type (scope={:?}, name={:?}); \
                 every required secret must name its DuckDB secret type",
                self.scope, self.name
            )));
        }
        Ok(())
    }
}

impl From<&str> for SecretLookup {
    fn from(secret_type: &str) -> Self {
        SecretLookup::of_type(secret_type)
    }
}

impl From<String> for SecretLookup {
    fn from(secret_type: String) -> Self {
        SecretLookup::of_type(secret_type)
    }
}

/// Validate every lookup in `lookups` (see [`SecretLookup::validate`]).
pub fn validate_lookups(lookups: &[SecretLookup]) -> Result<()> {
    lookups.iter().try_for_each(SecretLookup::validate)
}

/// Parsed resolved secrets: secret name → field map (string-rendered values).
///
/// The extension keys each resolved secret by its unique DuckDB **name** (an
/// unnamed `CREATE SECRET (TYPE s3, ...)` arrives as `__default_s3`), not by
/// its type, and adds `type`, `name`, `provider` and `scope` fields. Select
/// with the type-aware accessors — [`get`](Self::get) /
/// [`require`](Self::require) (by type), [`named`](Self::named) (by name and
/// type), [`for_scope_of_type`](Self::for_scope_of_type) (by path) — rather
/// than indexing `by_name` with a type.
#[derive(Clone, Default)]
pub struct Secrets {
    /// name → { field → value-as-string }. Null fields are omitted.
    pub by_name: HashMap<String, HashMap<String, String>>,
}

/// Whether the secret stored under column `name` is of `secret_type`: its
/// serialized `type` field when present, else (older connectors that do not
/// send one) its column name.
fn is_of_type(name: &str, fields: &HashMap<String, String>, secret_type: &str) -> bool {
    match fields.get("type") {
        Some(t) => t == secret_type,
        None => name == secret_type,
    }
}

/// A resolved secret as `(name, fields)`.
type Entry<'a> = (&'a str, &'a HashMap<String, String>);

fn is_unscoped(fields: &HashMap<String, String>) -> bool {
    fields.get("scope").is_none_or(|s| s.is_empty())
}

impl Secrets {
    /// Parse the IPC secrets blob. The shape mirrors Python: one column per
    /// secret name, each a struct of the secret's fields. A null column is an
    /// unresolved secret and is skipped; null fields are omitted.
    pub fn parse(bytes: &[u8]) -> Result<Secrets> {
        if bytes.is_empty() {
            return Ok(Secrets::default());
        }
        let batch = ipc::read_batch(bytes)?;
        let mut by_name = HashMap::new();
        if batch.num_rows() == 0 {
            return Ok(Secrets { by_name });
        }
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(i);
            if col.is_null(0) {
                continue;
            }
            let mut fields = HashMap::new();
            if let Some(sa) = col.as_any().downcast_ref::<StructArray>() {
                for (j, sf) in sa.fields().iter().enumerate() {
                    if let Some(v) = render(sa.column(j), 0) {
                        fields.insert(sf.name().clone(), v);
                    }
                }
            } else if let Some(v) = render(col, 0) {
                fields.insert(field.name().clone(), v);
            }
            by_name.insert(field.name().clone(), fields);
        }
        Ok(Secrets { by_name })
    }

    /// Every resolved secret as `(name, fields)`, ordered by name so selection
    /// is deterministic.
    fn sorted(&self) -> Vec<(&str, &HashMap<String, String>)> {
        let mut v: Vec<_> = self.iter().collect();
        v.sort_unstable_by_key(|(n, _)| *n);
        v
    }

    /// Whether no secret was resolved.
    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// Get a secret field value (first secret, by name order, carrying it).
    /// Ignores types; prefer [`get`](Self::get) + a field read.
    pub fn field(&self, field: &str) -> Option<String> {
        self.sorted()
            .into_iter()
            .find_map(|(_, m)| m.get(field).cloned())
    }

    /// Get a named secret's field (by column name, any type).
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

    /// Every resolved secret of `secret_type`, ordered by name. Matched on each
    /// secret's serialized `type` field (secrets are keyed by name, not type),
    /// falling back to the column name for a secret without a `type` field.
    pub fn of_type<'a>(
        &'a self,
        secret_type: &'a str,
    ) -> impl Iterator<Item = &'a HashMap<String, String>> + 'a {
        self.entries_of_type(secret_type).map(|(_, m)| m)
    }

    /// Like [`of_type`](Self::of_type), yielding `(name, fields)`.
    pub fn entries_of_type<'a>(
        &'a self,
        secret_type: &'a str,
    ) -> impl Iterator<Item = (&'a str, &'a HashMap<String, String>)> + 'a {
        self.sorted()
            .into_iter()
            .filter(move |(n, m)| is_of_type(n, m, secret_type))
    }

    /// Select by **type**: the unscoped secret of `secret_type`, else the first
    /// one (by name). This is the selector for a requirement declared as "a
    /// secret of type T" — it finds an unnamed `__default_T` secret and one
    /// named unlike its type alike. `None` when no secret of the type resolved.
    pub fn get(&self, secret_type: &str) -> Option<&HashMap<String, String>> {
        self.lookup(secret_type).map(|(_, m)| m)
    }

    /// Like [`get`](Self::get), also returning the secret's name.
    pub fn lookup(&self, secret_type: &str) -> Option<(&str, &HashMap<String, String>)> {
        let mut first = None;
        for (n, m) in self.sorted() {
            if !is_of_type(n, m, secret_type) {
                continue;
            }
            if is_unscoped(m) {
                return Some((n, m));
            }
            first.get_or_insert((n, m));
        }
        first
    }

    /// Like [`get`](Self::get), but a missing secret is an error naming the
    /// type (and never any secret value). Use from `on_bind` to fail the bind
    /// when a required secret was not resolved.
    pub fn require(&self, secret_type: &str) -> Result<&HashMap<String, String>> {
        self.get(secret_type).ok_or_else(|| {
            vgi_rpc::RpcError::value_error(format!(
                "a secret of type '{secret_type}' is required but none was resolved; \
                 create one with CREATE SECRET (TYPE {secret_type}, ...)"
            ))
        })
    }

    /// Select by **name**: the secret named `name`, only if it is of
    /// `secret_type`.
    pub fn named(&self, name: &str, secret_type: &str) -> Option<&HashMap<String, String>> {
        self.by_name
            .get(name)
            .filter(|m| is_of_type(name, m, secret_type))
    }

    /// The fields of the resolved secret whose `scope` is the longest prefix of
    /// `path`. Use this when a worker requested secrets for several scopes (e.g.
    /// one per cloud path / bucket) and must pick the right one per path. The
    /// connector serializes each secret's `scope` as a newline-joined list of
    /// prefixes; a secret with no (or empty) `scope` matches as a last-resort
    /// fallback (covers unscoped secrets and older connectors that don't send a
    /// scope). Returns `None` when no scope matches and no unscoped secret
    /// exists.
    pub fn for_scope(&self, path: &str) -> Option<&HashMap<String, String>> {
        self.select_for_scope(path, None).map(|(_, m)| m)
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
            .map(|(_, m)| m)
    }

    /// Like [`for_scope_of_type`](Self::for_scope_of_type), also returning the
    /// secret's name.
    pub fn lookup_for_scope_of_type(
        &self,
        path: &str,
        secret_type: &str,
    ) -> Option<(&str, &HashMap<String, String>)> {
        self.select_for_scope(path, Some(secret_type))
    }

    fn select_for_scope(
        &self,
        path: &str,
        secret_type: Option<&str>,
    ) -> Option<(&str, &HashMap<String, String>)> {
        let mut best: Option<(usize, Entry<'_>)> = None;
        let mut fallback: Option<Entry<'_>> = None;
        for (name, fields) in self.sorted() {
            if !secret_type.is_none_or(|t| is_of_type(name, fields, t)) {
                continue;
            }
            match fields.get("scope") {
                Some(scope) if !scope.is_empty() => {
                    for prefix in scope.split('\n').filter(|p| !p.is_empty()) {
                        if path.starts_with(prefix)
                            && best.is_none_or(|(blen, _)| prefix.len() > blen)
                        {
                            best = Some((prefix.len(), (name, fields)));
                        }
                    }
                }
                _ => {
                    fallback.get_or_insert((name, fields));
                }
            }
        }
        best.map(|(_, e)| e).or(fallback)
    }

    /// A field of the best scope-matching secret for `path` (see
    /// [`for_scope`](Self::for_scope)).
    pub fn field_for(&self, path: &str, field: &str) -> Option<String> {
        self.for_scope(path).and_then(|m| m.get(field).cloned())
    }
}

/// Render a single array element as a string: strings as-is, booleans as
/// `true`/`false`, numbers in plain decimal. `None` for a null element or a
/// type with no text rendering.
fn render(arr: &dyn Array, i: usize) -> Option<String> {
    if arr.is_null(i) {
        return None;
    }
    if let Some(s) = arr.as_string_opt::<i32>() {
        return Some(s.value(i).to_string());
    }
    if let Some(s) = arr.as_string_opt::<i64>() {
        return Some(s.value(i).to_string());
    }
    if let Some(s) = arr.as_string_view_opt() {
        return Some(s.value(i).to_string());
    }
    if let Some(b) = arr.as_boolean_opt() {
        return Some(b.value(i).to_string());
    }
    let r = arr_to_ref(arr);
    if matches!(
        arr.data_type(),
        arrow_schema::DataType::Float32 | arrow_schema::DataType::Float64
    ) {
        return crate::numeric::array_value_f64(&r, i).map(|v| v.to_string());
    }
    crate::numeric::array_value_i64(&r, i).map(|v| v.to_string())
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

    // ---- secret-lookup conformance contract (vgi docs/secret_lookup_fixtures.md) ----

    fn example(name: &str, scope: &str, key: &str) -> (String, HashMap<String, String>) {
        (
            name.to_string(),
            secret(&[
                ("type", "vgi_example"),
                ("name", name),
                ("provider", "config"),
                ("scope", scope),
                ("api_key", key),
            ]),
        )
    }

    fn of(entries: Vec<(String, HashMap<String, String>)>) -> Secrets {
        Secrets {
            by_name: entries.into_iter().collect(),
        }
    }

    #[test]
    fn get_by_type_finds_unnamed_default_secret() {
        // An unnamed CREATE SECRET arrives keyed `__default_<type>`: a column
        // lookup by type would miss it.
        let s = of(vec![example("__default_vgi_example", "", "k")]);
        let (name, m) = s.lookup("vgi_example").expect("found by type");
        assert_eq!(name, "__default_vgi_example");
        assert_eq!(m.get("api_key").map(String::as_str), Some("k"));
        assert!(!s.by_name.contains_key("vgi_example"));
    }

    #[test]
    fn get_by_type_finds_secret_named_unlike_its_type() {
        let s = of(vec![example("conformance_creds", "", "named")]);
        assert_eq!(
            s.get("vgi_example")
                .and_then(|m| m.get("api_key"))
                .map(String::as_str),
            Some("named")
        );
        assert!(s.get("s3").is_none());
    }

    #[test]
    fn get_by_type_prefers_unscoped_then_first_by_name() {
        let s = of(vec![
            example("b_scoped", "x://b/", "b"),
            example("z_unscoped", "", "z"),
            example("a_scoped", "x://a/", "a"),
        ]);
        assert_eq!(s.lookup("vgi_example").map(|(n, _)| n), Some("z_unscoped"));
        let s = of(vec![
            example("b_scoped", "x://b/", "b"),
            example("a_scoped", "x://a/", "a"),
        ]);
        assert_eq!(s.lookup("vgi_example").map(|(n, _)| n), Some("a_scoped"));
    }

    #[test]
    fn get_by_type_falls_back_to_column_name_without_type_field() {
        // Older connectors keyed the column by type and sent no `type` field.
        let s = secrets(&[("vgi_example", secret(&[("api_key", "legacy")]))]);
        assert_eq!(
            s.get("vgi_example")
                .and_then(|m| m.get("api_key"))
                .map(String::as_str),
            Some("legacy")
        );
        // ...but a column whose `type` field says otherwise is not a match.
        let s = secrets(&[("vgi_example", secret(&[("type", "s3")]))]);
        assert!(s.get("vgi_example").is_none());
    }

    #[test]
    fn require_errors_naming_the_type_and_no_value() {
        let s = of(vec![]);
        let err = s.require("vgi_example").unwrap_err().to_string();
        assert!(err.contains("vgi_example"), "{err}");
        let s = secrets(&[(
            "other",
            secret(&[("type", "s3"), ("secret", "sk-do-not-leak")]),
        )]);
        let err = s.require("vgi_example").unwrap_err().to_string();
        assert!(!err.contains("sk-do-not-leak"), "{err}");
        let s = of(vec![example("__default_vgi_example", "", "k")]);
        assert!(s.require("vgi_example").is_ok());
    }

    #[test]
    fn named_requires_matching_type() {
        let mut entries = vec![example("one", "", "1"), example("two", "", "2")];
        entries.push((
            "conformance_s3".to_string(),
            secret(&[("type", "s3"), ("name", "conformance_s3"), ("key_id", "AK")]),
        ));
        let s = of(entries);
        assert_eq!(
            s.named("two", "vgi_example")
                .and_then(|m| m.get("api_key"))
                .map(String::as_str),
            Some("2")
        );
        assert!(s.named("missing", "vgi_example").is_none());
        assert!(s.named("conformance_s3", "vgi_example").is_none());
        assert!(s.named("conformance_s3", "s3").is_some());
    }

    #[test]
    fn scope_selection_longest_prefix_fallback_and_none() {
        let mut entries = vec![
            example("conformance_a", "vgi-conformance://a/", "key-a"),
            example("conformance_ab", "vgi-conformance://a/b/", "key-ab"),
            example("conformance_any", "", "key-any"),
        ];
        // A same-scope secret of another type never wins.
        entries.push((
            "s3_ab".to_string(),
            secret(&[("type", "s3"), ("scope", "vgi-conformance://a/b/c/")]),
        ));
        let s = of(entries.clone());
        let pick = |s: &Secrets, p: &str| {
            s.lookup_for_scope_of_type(p, "vgi_example")
                .map(|(n, _)| n.to_string())
        };
        assert_eq!(
            pick(&s, "vgi-conformance://a/b/c/x").as_deref(),
            Some("conformance_ab")
        );
        assert_eq!(
            pick(&s, "vgi-conformance://a/other").as_deref(),
            Some("conformance_a")
        );
        assert_eq!(
            pick(&s, "vgi-conformance://z/x").as_deref(),
            Some("conformance_any")
        );
        entries.retain(|(n, _)| n != "conformance_any");
        let s = of(entries.clone());
        assert_eq!(pick(&s, "vgi-conformance://z/x"), None);
        entries.retain(|(n, _)| n != "conformance_ab");
        let s = of(entries);
        assert_eq!(
            pick(&s, "vgi-conformance://a/b/x").as_deref(),
            Some("conformance_a")
        );
    }

    #[test]
    fn lookup_declaration_forms_and_validation() {
        let bare: SecretLookup = "vgi_example".into();
        assert_eq!(bare, SecretLookup::of_type("vgi_example"));
        assert_eq!(bare.scope, None);
        assert_eq!(bare.name, None);
        let scoped = SecretLookup::of_type("s3").with_scope("s3://b/");
        assert_eq!(scoped.scope.as_deref(), Some("s3://b/"));
        let named = SecretLookup::from(String::from("s3")).with_name("prod");
        assert_eq!(named.name.as_deref(), Some("prod"));
        assert!(validate_lookups(&[bare, scoped, named]).is_ok());
        let err = SecretLookup::of_type(" ")
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.contains("empty secret_type"), "{err}");
        assert!(validate_lookups(&["s3".into(), "".into()]).is_err());
    }

    #[test]
    fn parse_skips_null_columns_and_null_fields() {
        use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray};
        use arrow_buffer::NullBuffer;
        use arrow_schema::{DataType, Field, Fields, Schema};
        use std::sync::Arc;

        let fields = Fields::from(vec![
            Field::new("type", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("api_key", DataType::Utf8, true),
            Field::new("port", DataType::Int32, true),
            Field::new("use_ssl", DataType::Boolean, true),
        ]);
        let cols = |name: &str| -> Vec<arrow_array::ArrayRef> {
            vec![
                Arc::new(StringArray::from(vec![Some("vgi_example")])),
                Arc::new(StringArray::from(vec![Some(name)])),
                Arc::new(StringArray::from(vec![None::<&str>])),
                Arc::new(Int32Array::from(vec![Some(7)])),
                Arc::new(BooleanArray::from(vec![Some(true)])),
            ]
        };
        let present = StructArray::new(fields.clone(), cols("__default_vgi_example"), None);
        let absent = StructArray::new(
            fields.clone(),
            cols("gone"),
            Some(NullBuffer::from(vec![false])),
        );
        let schema = Schema::new(vec![
            Field::new(
                "__default_vgi_example",
                DataType::Struct(fields.clone()),
                true,
            ),
            Field::new("gone", DataType::Struct(fields), true),
        ]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(present), Arc::new(absent)])
                .unwrap();
        let s = Secrets::parse(&ipc::write_batch(&batch).unwrap()).unwrap();
        assert_eq!(s.by_name.len(), 1, "null column is an unresolved secret");
        let m = s.get("vgi_example").unwrap();
        assert_eq!(m.get("port").map(String::as_str), Some("7"));
        assert_eq!(m.get("use_ssl").map(String::as_str), Some("true"));
        assert!(!m.contains_key("api_key"), "null field is omitted");
    }
}

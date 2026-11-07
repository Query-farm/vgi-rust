// Copyright 2026 Query Farm LLC - https://query.farm

//! Secret-lookup conformance fixtures.
//!
//! Implements the language-neutral contract the shared
//! `test/sql/integration/secret/secret_lookup_*.test` files run against every
//! SDK's example worker (vgi repo, `docs/secret_lookup_fixtures.md`). Every
//! function reads secrets of the `vgi_example` type the worker registers, and
//! selects them through the public [`Secrets`] selectors — never by indexing
//! the batch — because the resolved-secrets batch keys each column by the
//! secret's unique DuckDB *name* (`__default_vgi_example` for an unnamed one),
//! not its type.
//!
//! - `secret_value_by_type(field)` — static requirement in the simplest form
//!   (`"vgi_example".into()`); selected with [`Secrets::lookup`].
//! - `secret_value_required(field)` — as above; the bind fails via
//!   [`Secrets::require`] when no secret resolved.
//! - `secret_value_by_name(name, field)` — dynamic (two-phase) request by name;
//!   selected with [`Secrets::named`].
//! - `secret_value_for_scope(path, field)` — three static requirements of one
//!   type (unscoped, `vgi-conformance://a/`, `vgi-conformance://a/b/`);
//!   selected with [`Secrets::lookup_for_scope_of_type`].
//! - `secret_types_seen()` — static `vgi_example` + `s3` scoped to
//!   `s3://vgi-conformance/`; one row per delivered secret.
//!
//! Port of vgi-python's `vgi/_test_fixtures/secret_lookup.py`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{
    ArgSpec, BindParams, BindResponse, FunctionExample, FunctionMetadata, ProcessParams,
};
use vgi::secrets::{SecretLookup, Secrets};
use vgi::table_function::{TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

/// The secret type every value fixture reads (registered in `main.rs`).
const SECRET_TYPE: &str = "vgi_example";
/// Nested static scopes of `secret_value_for_scope`.
const SCOPE_A: &str = "vgi-conformance://a/";
const SCOPE_AB: &str = "vgi-conformance://a/b/";
/// `secret_types_seen`'s second type is DuckDB's `s3`, whose secrets default
/// to the scope `s3://`, so it is requested by scope.
const S3_SCOPE: &str = "s3://vgi-conformance/";

/// Register the five conformance fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_table(SecretValueByType);
    w.register_table(SecretValueRequired);
    w.register_table(SecretValueByName);
    w.register_table(SecretValueForScope);
    w.register_table(SecretTypesSeen);
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("found", DataType::Boolean, true),
        Field::new("secret_name", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
    ]))
}

fn meta(description: &str, example_sql: &str, required: Vec<SecretLookup>) -> FunctionMetadata {
    FunctionMetadata {
        description: description.to_string(),
        categories: vec!["secret".into(), "testing".into()],
        required_secrets: required,
        examples: vec![FunctionExample {
            sql: example_sql.to_string(),
            description: description.to_string(),
            expected_output: None,
        }],
        ..Default::default()
    }
}

fn field_arg(position: i32) -> ArgSpec {
    ArgSpec::const_arg("field", position, "varchar", "Secret field to return")
}

fn const_str(params: &ProcessParams, pos: usize, what: &str) -> Result<String> {
    params
        .arguments
        .const_str(pos)
        .ok_or_else(|| RpcError::value_error(format!("{what} must be a non-NULL VARCHAR")))
}

/// The one `(found, secret_name, value)` row for a selection.
fn value_row(
    schema: &SchemaRef,
    selected: Option<(&str, &HashMap<String, String>)>,
    field: &str,
) -> Box<dyn TableProducer> {
    let (found, name, value) = match selected {
        Some((column, fields)) => (
            true,
            Some(
                fields
                    .get("name")
                    .cloned()
                    .unwrap_or_else(|| column.to_string()),
            ),
            fields.get(field).cloned(),
        ),
        None => (false, None, None),
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(BooleanArray::from(vec![Some(found)])),
        Arc::new(StringArray::from(vec![name])),
        Arc::new(StringArray::from(vec![value])),
    ];
    Box::new(OneBatch {
        batch: RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| RpcError::runtime_error(e.to_string())),
        done: false,
    })
}

/// Emits one prebuilt batch, then finishes.
struct OneBatch {
    batch: std::result::Result<RecordBatch, RpcError>,
    done: bool,
}

impl TableProducer for OneBatch {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;
        match std::mem::replace(&mut self.batch, Err(RpcError::runtime_error("consumed"))) {
            Ok(b) if b.num_rows() == 0 => Ok(None),
            Ok(b) => Ok(Some(b)),
            Err(e) => Err(e),
        }
    }
    fn resume_supported(&self) -> bool {
        false
    }
}

fn fixed_bind(schema: SchemaRef) -> Result<BindResponse> {
    Ok(BindResponse {
        output_schema: schema,
        opaque_data: Vec::new(),
    })
}

// ---------------------------------------------------------------------------
// secret_value_by_type(field)
// ---------------------------------------------------------------------------

/// A field of the `vgi_example` secret, selected by type from a static
/// requirement declared in the simplest form.
pub struct SecretValueByType;
impl TableFunction for SecretValueByType {
    fn name(&self) -> &str {
        "secret_value_by_type"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta(
            "Conformance: a field of the vgi_example secret, selected by type",
            "SELECT * FROM secret_value_by_type('api_key')",
            vec![SECRET_TYPE.into()],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![field_arg(0)]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(value_schema())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let field = const_str(params, 0, "field")?;
        Ok(value_row(
            &params.output_schema,
            params.secrets.lookup(SECRET_TYPE),
            &field,
        ))
    }
}

// ---------------------------------------------------------------------------
// secret_value_required(field)
// ---------------------------------------------------------------------------

/// Like `secret_value_by_type`, but the bind fails when the secret is absent.
pub struct SecretValueRequired;
impl TableFunction for SecretValueRequired {
    fn name(&self) -> &str {
        "secret_value_required"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta(
            "Conformance: a field of the required vgi_example secret; errors when absent",
            "SELECT * FROM secret_value_required('api_key')",
            vec![SECRET_TYPE.into()],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![field_arg(0)]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        params.secrets.require(SECRET_TYPE)?;
        fixed_bind(value_schema())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let field = const_str(params, 0, "field")?;
        Ok(value_row(
            &params.output_schema,
            params.secrets.lookup(SECRET_TYPE),
            &field,
        ))
    }
}

// ---------------------------------------------------------------------------
// secret_value_by_name(name, field)
// ---------------------------------------------------------------------------

/// A field of the `vgi_example` secret named `name`, requested by name at bind.
pub struct SecretValueByName;
impl TableFunction for SecretValueByName {
    fn name(&self) -> &str {
        "secret_value_by_name"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta(
            "Conformance: a field of the named vgi_example secret",
            "SELECT * FROM secret_value_by_name('my_secret', 'api_key')",
            Vec::new(),
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("name", 0, "varchar", "Secret name to look up"),
            field_arg(1),
        ]
    }
    fn secret_lookups(&self, params: &BindParams) -> Vec<SecretLookup> {
        match params.arguments.const_str(0) {
            Some(name) => vec![SecretLookup::of_type(SECRET_TYPE).with_name(name)],
            None => Vec::new(),
        }
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(value_schema())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let name = const_str(params, 0, "name")?;
        let field = const_str(params, 1, "field")?;
        let selected = params
            .secrets
            .named(&name, SECRET_TYPE)
            .map(|m| (name.as_str(), m));
        Ok(value_row(&params.output_schema, selected, &field))
    }
}

// ---------------------------------------------------------------------------
// secret_value_for_scope(path, field)
// ---------------------------------------------------------------------------

/// A field of the `vgi_example` secret whose scope is the longest prefix of
/// `path`, falling back to the unscoped one.
pub struct SecretValueForScope;
impl TableFunction for SecretValueForScope {
    fn name(&self) -> &str {
        "secret_value_for_scope"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta(
            "Conformance: a field of the vgi_example secret whose scope best matches a path",
            "SELECT * FROM secret_value_for_scope('vgi-conformance://a/b/x', 'api_key')",
            vec![
                SECRET_TYPE.into(),
                SecretLookup::of_type(SECRET_TYPE).with_scope(SCOPE_A),
                SecretLookup::of_type(SECRET_TYPE).with_scope(SCOPE_AB),
            ],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg(
                "path",
                0,
                "varchar",
                "Path to select a secret for (longest scope prefix)",
            ),
            field_arg(1),
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(value_schema())
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let path = const_str(params, 0, "path")?;
        let field = const_str(params, 1, "field")?;
        Ok(value_row(
            &params.output_schema,
            params.secrets.lookup_for_scope_of_type(&path, SECRET_TYPE),
            &field,
        ))
    }
}

// ---------------------------------------------------------------------------
// secret_types_seen()
// ---------------------------------------------------------------------------

/// Every secret delivered to a function that requires two secret types.
pub struct SecretTypesSeen;
impl TableFunction for SecretTypesSeen {
    fn name(&self) -> &str {
        "secret_types_seen"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta(
            "Conformance: the secrets delivered for a vgi_example + s3 requirement",
            "SELECT * FROM secret_types_seen()",
            vec![
                SECRET_TYPE.into(),
                SecretLookup::of_type("s3").with_scope(S3_SCOPE),
            ],
        )
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        fixed_bind(Arc::new(Schema::new(vec![
            Field::new("secret_name", DataType::Utf8, true),
            Field::new("secret_type", DataType::Utf8, true),
            Field::new("scope", DataType::Utf8, true),
        ])))
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let rows = seen_rows(&params.secrets);
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.0.as_str()),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.2.as_str()),
            )),
        ];
        Ok(Box::new(OneBatch {
            batch: RecordBatch::try_new(params.output_schema.clone(), columns)
                .map_err(|e| RpcError::runtime_error(e.to_string())),
            done: false,
        }))
    }
}

/// `(secret_name, secret_type, scope)` per delivered secret, ordered by name.
fn seen_rows(secrets: &Secrets) -> Vec<(String, Option<String>, String)> {
    let mut rows: Vec<_> = secrets
        .iter()
        .map(|(name, fields)| {
            (
                name.to_string(),
                fields.get("type").cloned(),
                fields.get("scope").cloned().unwrap_or_default(),
            )
        })
        .collect();
    rows.sort();
    rows
}

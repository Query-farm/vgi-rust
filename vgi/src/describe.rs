// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Producer for the standardized VGI HTTP landing contract.
//!
//! The shared static landing page — one self-contained `landing.html` served
//! byte-identically by every VGI language worker — fetches
//! `GET {prefix}/describe.json` (same origin) and renders it. This module is
//! the Rust producer for that contract, mirroring the Python reference in
//! `vgi/http/describe_json.py`. The HTTP transport (`vgi-rpc`) owns the routes
//! and the runtime-derived `oauth` / `server_id`; this module supplies the
//! catalog-introspection JSON via the [`vgi_rpc::http::DescribeProvider`] trait.
//!
//! See `~/Development/vgi/docs/http-landing-contract.md` for the normative spec.

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::{json, Map, Value};

use crate::catalog::{self, CatSchema, CatalogModel};
use crate::dispatch::{Dispatcher, FnKind};
use crate::protocol::dtos::{FunctionInfo, MacroInfo};

/// This contract's version (independent of the VGI wire protocol version).
const LANDING_SCHEMA_VERSION: i64 = 1;
/// Cupola base URL surfaced to the shared page.
const CUPOLA_BASE: &str = "https://cupola.query-farm.services";
/// Functions with this name prefix are scoped to the `projection_repro`
/// catalog only; the primary listing hides them (mirrors the dispatcher).
const PROJ_REPRO_PREFIX: &str = "proj_repro";

/// The reserved `vgi.*` catalog tags surfaced in the landing page's catalog
/// card. Each entry is `(json_key, tag_key)`.
const STRING_TAGS: &[(&str, &str)] = &[
    ("title", "vgi.title"),
    ("doc_md", "vgi.doc_md"),
    ("source_url", "vgi.source_url"),
    ("license", "vgi.license"),
    ("author", "vgi.author"),
    ("copyright", "vgi.copyright"),
    ("support_contact", "vgi.support_contact"),
    ("support_policy_url", "vgi.support_policy_url"),
];
const KEYWORDS_TAG: &str = "vgi.keywords";

/// Wraps a [`Dispatcher`] as a [`vgi_rpc::http::DescribeProvider`] so the HTTP
/// transport can serve the landing contract from the worker's catalog.
pub struct VgiDescribeProvider {
    disp: Arc<Dispatcher>,
    worker_name: String,
    worker_doc: String,
    worker_version: String,
}

impl VgiDescribeProvider {
    /// Build a provider from a dispatcher. `worker_name` / `worker_doc`
    /// default to the primary catalog's name / comment (first line).
    pub fn new(disp: Arc<Dispatcher>, worker_name: String, worker_doc: String) -> Self {
        let worker_doc = worker_doc.lines().next().unwrap_or("").to_string();
        VgiDescribeProvider {
            disp,
            worker_name,
            worker_doc,
            worker_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

impl vgi_rpc::http::DescribeProvider for VgiDescribeProvider {
    fn describe_json(&self, oauth: bool, server_id: &str) -> String {
        let doc = build_describe(
            &self.disp,
            oauth,
            server_id,
            &self.worker_name,
            &self.worker_doc,
            &self.worker_version,
        );
        doc.to_string()
    }

    fn columns_json(&self, catalog: &str, schema: &str, table: &str) -> Option<String> {
        build_columns(&self.disp, catalog, schema, table).map(|v| v.to_string())
    }
}

// ---------------------------------------------------------------------------
// describe.json
// ---------------------------------------------------------------------------

fn build_describe(
    disp: &Dispatcher,
    oauth: bool,
    server_id: &str,
    worker_name: &str,
    worker_doc: &str,
    worker_version: &str,
) -> Value {
    let mut catalogs = Vec::new();
    // Primary catalog: functions are the worker-global registries minus any
    // names a secondary catalog owns (and minus the proj_repro-scoped set).
    catalogs.push(build_catalog(&disp.catalog, disp, None));
    // Secondary (MetaWorker-style) catalogs: each lists only its own functions.
    for (i, sec) in disp.secondary.iter().enumerate() {
        let owned = disp
            .secondary_functions
            .get(i)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        catalogs.push(build_catalog(sec, disp, Some(owned)));
    }

    json!({
        "landing_schema_version": LANDING_SCHEMA_VERSION,
        "worker": {
            "name": worker_name,
            "doc": worker_doc,
            "version": worker_version,
            "lang": "rust",
        },
        "server_id": server_id,
        "oauth": oauth,
        "cupola_base": CUPOLA_BASE,
        "catalogs": catalogs,
    })
}

/// `owned` is `None` for the primary catalog (every registered name a secondary
/// does not own) and `Some(names)` for a secondary (only the names it owns).
/// Functions are placed per schema — see [`Dispatcher::declared_in`] — so the
/// landing contract agrees with `catalog_schema_contents_functions` about which
/// schema a function lives in.
fn build_catalog(model: &CatalogModel, disp: &Dispatcher, owned: Option<&[String]>) -> Value {
    let version = model.default_data_version.as_deref();
    let schemas_slice = model.schemas_for(version);

    // Schema names in the order the dispatcher advertises them (ensuring a
    // synthetic `main` exists even when the model declares none — functions
    // live there).
    let schema_names = catalog_schema_names(schemas_slice);

    let mut schemas = Vec::new();
    let mut n_schemas = 0i64;
    let mut n_tables = 0i64;
    let mut n_views = 0i64;
    let mut n_functions = 0i64;

    for name in &schema_names {
        let sch = schemas_slice.iter().find(|s| &s.name == name);
        let tables = sch.map(schema_tables).unwrap_or_default();
        let views = sch.map(schema_views).unwrap_or_default();
        // Match homes against the dispatcher's identity for this catalog, not
        // the model name: a worker that never installs a catalog has an empty
        // one, while its functions are homed under the worker's catalog name.
        let identity = disp.catalog_identity(model);
        let infos = match owned {
            None => primary_function_infos(disp, identity, name),
            Some(names) => secondary_function_infos(disp, names, identity, name),
        };
        let mut fns: Vec<Value> = infos.iter().map(function_json).collect();
        // Fold the schema's declarative macros into the same `functions` array:
        // a scalar macro is invoked exactly like a scalar function in SQL and a
        // table macro like a table function, so they share the landing page's
        // scalar/table buckets (mirrors the Python reference producer, which
        // commonly exposes a catalog's callable surface as macros).
        if let Some(s) = sch {
            for m in &s.macros {
                fns.push(macro_json(&catalog::macro_info(&s.name, m)));
            }
        }
        // Deterministic ordering across functions + macros by (type, name).
        sort_function_values(&mut fns);
        n_schemas += 1;
        n_tables += tables.len() as i64;
        n_views += views.len() as i64;
        n_functions += fns.len() as i64;
        let mut schema_obj = Map::new();
        schema_obj.insert("name".to_string(), Value::String(name.clone()));
        // Optional per-schema doc: the schema's comment when non-empty (omit
        // otherwise), mirroring the Python reference producer.
        if let Some(doc) = sch.and_then(|s| s.comment.as_deref()) {
            if !doc.is_empty() {
                schema_obj.insert("doc".to_string(), Value::String(doc.to_string()));
            }
        }
        schema_obj.insert("tables".to_string(), Value::Array(tables));
        schema_obj.insert("views".to_string(), Value::Array(views));
        schema_obj.insert("functions".to_string(), Value::Array(fns));
        schemas.push(Value::Object(schema_obj));
    }

    json!({
        "name": model.name,
        "implementation_version": model.implementation_version,
        "data_version_spec": model.default_data_version.clone().or_else(|| model.data_version_spec.clone()),
        "data_versions": data_versions(model),
        "attach_options": attach_options(model),
        "tags": catalog_tags(model),
        "counts": {
            "schemas": n_schemas,
            "tables": n_tables,
            "views": n_views,
            "functions": n_functions,
        },
        "schemas": schemas,
    })
}

/// Schema names in dispatcher order, guaranteeing `main` is present (mirrors
/// `Dispatcher::catalog_schema_names`).
fn catalog_schema_names(schemas: &[CatSchema]) -> Vec<String> {
    let mut names: Vec<String> = schemas.iter().map(|s| s.name.clone()).collect();
    if !names.iter().any(|n| n == catalog::MAIN_SCHEMA) {
        names.insert(0, catalog::MAIN_SCHEMA.to_string());
    }
    names
}

fn schema_tables(s: &CatSchema) -> Vec<Value> {
    s.tables
        .iter()
        .map(|t| {
            json!({
                "name": t.name,
                "cols": t.columns.fields().len() as i64,
                "comment": t.comment.clone().unwrap_or_default(),
            })
        })
        .collect()
}

fn schema_views(s: &CatSchema) -> Vec<Value> {
    s.views
        .iter()
        .map(|v| {
            json!({
                "name": v.name,
                "cols": v.column_comments.len() as i64,
                "comment": v.comment.clone().unwrap_or_default(),
                "def": v.definition,
            })
        })
        .collect()
}

fn data_versions(model: &CatalogModel) -> Vec<Value> {
    // Published releases, newest first. The Rust catalog model carries only
    // the version strings (no per-release label).
    model
        .supported_data_versions
        .iter()
        .rev()
        .map(|spec| json!({ "spec": spec }))
        .collect()
}

/// Best-effort decode of the serialized `AttachOptionSpec` records. The value
/// default is display-only and hard to reconstruct generically, so it is left
/// empty; name / description / type are recovered from the IPC batch.
fn attach_options(model: &CatalogModel) -> Vec<Value> {
    let mut out = Vec::new();
    for bytes in &model.attach_option_specs {
        let Ok(batch) = crate::ipc::read_batch(bytes) else {
            continue;
        };
        let name = string_cell(&batch, "name");
        let description = string_cell(&batch, "description");
        let ty = binary_cell(&batch, "type")
            .and_then(|b| crate::ipc::read_schema(b).ok())
            .and_then(|s| s.fields().first().map(|f| type_str(f.data_type())))
            .unwrap_or_default();
        out.push(json!({
            "name": name.unwrap_or_default(),
            "type": ty,
            "default": "",
            "description": description.unwrap_or_default(),
        }));
    }
    out
}

fn catalog_tags(model: &CatalogModel) -> Value {
    let get = |key: &str| {
        model
            .tags
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
    };
    let mut out = Map::new();
    for (json_key, tag_key) in STRING_TAGS {
        if let Some(v) = get(tag_key) {
            if !v.is_empty() {
                out.insert((*json_key).to_string(), Value::String(v));
            }
        }
    }
    if let Some(kw) = get(KEYWORDS_TAG) {
        if let Ok(Value::Array(items)) = serde_json::from_str::<Value>(&kw) {
            let keywords: Vec<Value> = items
                .into_iter()
                .map(|v| Value::String(value_to_plain_string(&v)))
                .collect();
            out.insert("keywords".to_string(), Value::Array(keywords));
        }
    }
    Value::Object(out)
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

/// Function infos the primary catalog places in `schema`: every registered
/// function declared there whose name no secondary catalog owns (and which is
/// not a proj_repro-scoped fixture), sorted by (type, name).
fn primary_function_infos(disp: &Dispatcher, catalog: &str, schema: &str) -> Vec<FunctionInfo> {
    let all_sec: HashSet<&str> = disp
        .secondary_functions
        .iter()
        .flatten()
        .map(String::as_str)
        .collect();
    let visible = |name: &str| !name.starts_with(PROJ_REPRO_PREFIX) && !all_sec.contains(name);
    let mut infos = Vec::new();
    for name in registry_names(disp) {
        if visible(&name) {
            infos.extend(infos_for_name(disp, &name, catalog, schema));
        }
    }
    sort_infos(&mut infos);
    infos
}

/// Function infos a secondary catalog places in `schema`: only the names it
/// owns, sorted by (type, name).
fn secondary_function_infos(
    disp: &Dispatcher,
    owned: &[String],
    catalog: &str,
    schema: &str,
) -> Vec<FunctionInfo> {
    let mut infos = Vec::new();
    for name in owned {
        infos.extend(infos_for_name(disp, name, catalog, schema));
    }
    sort_infos(&mut infos);
    infos
}

/// Every distinct registered function name across all registries.
fn registry_names(disp: &Dispatcher) -> Vec<String> {
    let mut names: HashSet<String> = HashSet::new();
    names.extend(disp.scalars.keys().cloned());
    names.extend(disp.tables.keys().cloned());
    names.extend(disp.tableinouts.keys().cloned());
    names.extend(disp.buffering.keys().cloned());
    names.extend(disp.aggregates.keys().cloned());
    let mut v: Vec<String> = names.into_iter().collect();
    v.sort();
    v
}

/// The `FunctionInfo`s (overloads) registered under `name` that are declared in
/// `(catalog, schema)`, across registries.
fn infos_for_name(disp: &Dispatcher, name: &str, catalog: &str, schema: &str) -> Vec<FunctionInfo> {
    let mut out = Vec::new();
    let here = |kind: FnKind, i: usize| disp.declared_in(kind, name, i, catalog, schema);
    // Report the schema the function is being listed in, not the placeholder
    // `default_function_info` leaves behind.
    let in_schema = |mut fi: FunctionInfo, s: &str| {
        fi.schema_name = s.to_string();
        fi
    };
    if let Some(fs) = disp.scalars.get(name) {
        for (i, f) in fs.iter().enumerate() {
            if here(FnKind::Scalar, i) {
                if let Ok(fi) = catalog::scalar_function_info(f.as_ref()) {
                    out.push(in_schema(fi, schema));
                }
            }
        }
    }
    if let Some(fs) = disp.tables.get(name) {
        for (i, f) in fs.iter().enumerate() {
            if here(FnKind::Table, i) {
                if let Ok(fi) = catalog::table_function_info(f.as_ref()) {
                    out.push(in_schema(fi, schema));
                }
            }
        }
    }
    if let Some(fs) = disp.tableinouts.get(name) {
        for (i, f) in fs.iter().enumerate() {
            if here(FnKind::TableInOut, i) {
                if let Ok(fi) = catalog::table_in_out_function_info(f.as_ref()) {
                    out.push(in_schema(fi, schema));
                }
            }
        }
    }
    if let Some(fs) = disp.buffering.get(name) {
        for (i, f) in fs.iter().enumerate() {
            if here(FnKind::Buffering, i) {
                if let Ok(fi) = catalog::buffering_function_info(f.as_ref()) {
                    out.push(in_schema(fi, schema));
                }
            }
        }
    }
    if let Some(fs) = disp.aggregates.get(name) {
        for (i, f) in fs.iter().enumerate() {
            if here(FnKind::Aggregate, i) {
                if let Ok(fi) = catalog::aggregate_function_info(f.as_ref()) {
                    out.push(in_schema(fi, schema));
                }
            }
        }
    }
    out
}

fn sort_infos(infos: &mut [FunctionInfo]) {
    infos.sort_by(|a, b| {
        (a.function_type.0.as_str(), a.name.as_str())
            .cmp(&(b.function_type.0.as_str(), b.name.as_str()))
    });
}

fn function_json(fi: &FunctionInfo) -> Value {
    let mut obj = Map::new();
    obj.insert("name".to_string(), Value::String(fi.name.clone()));
    obj.insert(
        "type".to_string(),
        Value::String(function_display_type(fi).to_string()),
    );
    obj.insert("doc".to_string(), Value::String(fi.description.clone()));
    obj.insert("args".to_string(), Value::Array(function_args(fi)));
    if let Some(returns) = function_returns(fi) {
        obj.insert("returns".to_string(), Value::String(returns));
    }
    Value::Object(obj)
}

/// scalar | table | aggregate | table_in_out — mirrors the Python reference.
fn function_display_type(fi: &FunctionInfo) -> &'static str {
    match fi.function_type.0.as_str() {
        "scalar" => "scalar",
        "aggregate" => "aggregate",
        _ if fi.has_finalize => "table_in_out",
        _ => "table",
    }
}

fn function_args(fi: &FunctionInfo) -> Vec<Value> {
    let Ok(schema) = crate::ipc::read_schema(&fi.arguments.0) else {
        return Vec::new();
    };
    let mut args = Vec::new();
    for field in schema.fields() {
        let md = field.metadata();
        // Skip the piped input relation of a table-in-out function.
        if md.get("vgi_type").map(String::as_str) == Some("table") {
            continue;
        }
        let mut arg = Map::new();
        arg.insert("name".to_string(), Value::String(field.name().clone()));
        arg.insert(
            "type".to_string(),
            Value::String(type_str(field.data_type())),
        );
        if md.get("vgi_arg").map(String::as_str) == Some("named") {
            arg.insert("named".to_string(), Value::Bool(true));
        }
        if let Some(doc) = md.get("vgi_doc") {
            if !doc.is_empty() {
                arg.insert("desc".to_string(), Value::String(doc.clone()));
            }
        }
        if let Some(default) = md.get("vgi_default") {
            // Stored as a JSON scalar; re-encode for a stable display form.
            let display = serde_json::from_str::<Value>(default)
                .map(|v| v.to_string())
                .unwrap_or_else(|_| default.clone());
            arg.insert("default".to_string(), Value::String(display));
        }
        args.push(Value::Object(arg));
    }
    args
}

fn function_returns(fi: &FunctionInfo) -> Option<String> {
    let schema = crate::ipc::read_schema(&fi.output_schema.0).ok()?;
    if schema.fields().is_empty() {
        return None;
    }
    match fi.function_type.0.as_str() {
        "scalar" | "aggregate" => Some(type_str(schema.field(0).data_type())),
        _ => {
            let cols: Vec<String> = schema
                .fields()
                .iter()
                .map(|f| format!("{} {}", f.name(), type_str(f.data_type())))
                .collect();
            Some(format!("TABLE({})", cols.join(", ")))
        }
    }
}

/// Sort the combined function + macro JSON objects by `(type, name)` — the
/// display `type` field, not the raw registry type — matching the Python
/// reference producer so both languages list the merged callable surface in the
/// same order.
fn sort_function_values(fns: &mut [Value]) {
    fns.sort_by_key(|v| {
        (
            v.get("type")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            v.get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        )
    });
}

// ---------------------------------------------------------------------------
// Macros (scalar + table macros fold into the `functions` array)
// ---------------------------------------------------------------------------

fn macro_json(mi: &MacroInfo) -> Value {
    let mut obj = Map::new();
    obj.insert("name".to_string(), Value::String(mi.name.clone()));
    obj.insert(
        "type".to_string(),
        Value::String(macro_display_type(mi).to_string()),
    );
    obj.insert(
        "doc".to_string(),
        Value::String(mi.comment.clone().unwrap_or_default()),
    );
    obj.insert("args".to_string(), Value::Array(macro_args(mi)));
    // Macros carry no `returns` field (their result type is not declared).
    Value::Object(obj)
}

/// A scalar macro surfaces as `scalar`, a table macro as `table` — they are
/// invoked exactly like the corresponding function kind in SQL.
fn macro_display_type(mi: &MacroInfo) -> &'static str {
    if mi.macro_type.0 == "table" {
        "table"
    } else {
        "scalar"
    }
}

fn macro_args(mi: &MacroInfo) -> Vec<Value> {
    // Defaulted parameters are optional and callable by name in DuckDB, so they
    // are presented as named args carrying their default value.
    let defaults = macro_defaults(&mi.parameter_default_values.0);
    // `arguments_schema` has one nullable field per parameter (in order): the
    // field's type pins the parameter type (or Null when untyped), and the
    // `vgi_doc` metadata carries its description. Fall back to the bare
    // parameter names when a worker supplied no schema.
    let schema = crate::ipc::read_schema(&mi.arguments_schema.0).ok();
    let names: Vec<String> = match &schema {
        Some(s) => s.fields().iter().map(|f| f.name().clone()).collect(),
        None => mi.parameters.clone(),
    };
    let mut args = Vec::new();
    for name in names {
        let field = schema
            .as_ref()
            .and_then(|s| s.fields().iter().find(|f| f.name() == &name).cloned());
        let mut arg = Map::new();
        arg.insert("name".to_string(), Value::String(name.clone()));
        // An untyped macro param (no typed default pinning it) surfaces as ANY
        // rather than the Arrow null placeholder.
        let ty = match &field {
            Some(f) if !matches!(f.data_type(), arrow_schema::DataType::Null) => {
                type_str(f.data_type())
            }
            _ => "ANY".to_string(),
        };
        arg.insert("type".to_string(), Value::String(ty));
        if let Some(doc) = field.as_ref().and_then(|f| f.metadata().get("vgi_doc")) {
            if !doc.is_empty() {
                arg.insert("desc".to_string(), Value::String(doc.clone()));
            }
        }
        if let Some(default) = defaults.get(&name) {
            arg.insert("named".to_string(), Value::Bool(true));
            // Stored JSON-encoded for a stable display form (int 0 -> "0").
            arg.insert("default".to_string(), Value::String(default.to_string()));
        }
        args.push(Value::Object(arg));
    }
    args
}

/// Decode the 1-row macro parameter-default batch into `name -> JSON value`
/// (defaults are int64 in the catalog model). Empty or undecodable bytes yield
/// an empty map.
fn macro_defaults(bytes: &[u8]) -> std::collections::HashMap<String, Value> {
    use arrow_array::Array;
    let mut out = std::collections::HashMap::new();
    let Ok(batch) = crate::ipc::read_batch(bytes) else {
        return out;
    };
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        if col.is_empty() || col.is_null(0) {
            continue;
        }
        if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
            out.insert(field.name().clone(), Value::from(a.value(0)));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Lazy per-object columns
// ---------------------------------------------------------------------------

fn build_columns(
    disp: &Dispatcher,
    catalog_name: &str,
    schema: &str,
    table: &str,
) -> Option<Value> {
    let model = find_catalog(disp, catalog_name)?;
    let sch = model
        .schemas_for(model.default_data_version.as_deref())
        .iter()
        .find(|s| s.name == schema)?;
    if let Some(t) = sch.tables.iter().find(|t| t.name == table) {
        let columns: Vec<Value> = t
            .columns
            .fields()
            .iter()
            .map(|f| {
                let mut col = Map::new();
                col.insert("name".to_string(), Value::String(f.name().clone()));
                col.insert("type".to_string(), Value::String(type_str(f.data_type())));
                if let Some(c) = f
                    .metadata()
                    .get("comment")
                    .or_else(|| f.metadata().get("vgi_doc"))
                {
                    if !c.is_empty() {
                        col.insert("comment".to_string(), Value::String(c.clone()));
                    }
                }
                Value::Object(col)
            })
            .collect();
        return Some(json!({ "columns": columns }));
    }
    if let Some(v) = sch.views.iter().find(|v| v.name == table) {
        // View column types are only known after binding the SQL (not done
        // here), so expose the declared column comments with empty types.
        let columns: Vec<Value> = v
            .column_comments
            .iter()
            .map(|(n, c)| json!({ "name": n, "type": "", "comment": c }))
            .collect();
        return Some(json!({ "columns": columns }));
    }
    None
}

fn find_catalog<'a>(disp: &'a Dispatcher, name: &str) -> Option<&'a CatalogModel> {
    if disp.catalog.name == name {
        return Some(&disp.catalog);
    }
    disp.secondary.iter().find(|c| c.name == name)
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Render an Arrow data type as a display string for the contract.
fn type_str(dt: &arrow_schema::DataType) -> String {
    dt.to_string()
}

/// A `serde_json` scalar rendered as a plain string (unquoted for strings).
fn value_to_plain_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn string_cell(batch: &arrow_array::RecordBatch, col: &str) -> Option<String> {
    use arrow_array::Array;
    let idx = batch.schema().index_of(col).ok()?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()?;
    if arr.len() == 0 || arr.is_null(0) {
        return None;
    }
    Some(arr.value(0).to_string())
}

fn binary_cell<'a>(batch: &'a arrow_array::RecordBatch, col: &str) -> Option<&'a [u8]> {
    use arrow_array::Array;
    let idx = batch.schema().index_of(col).ok()?;
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow_array::BinaryArray>()?;
    if arr.len() == 0 || arr.is_null(0) {
        return None;
    }
    Some(arr.value(0))
}

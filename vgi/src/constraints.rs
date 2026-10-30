// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Canonical constraint records for catalogs that state their own facts.
//!
//! Under `vgi.v3` a table's constraints travel as canonical `ConstraintInfo`
//! IPC records in [`CatTable::constraints`](crate::catalog::CatTable::constraints).
//! The worker *attests* each fact: the SDK never derives a record from data or
//! from the legacy `not_null` / `primary_key` / `unique` / `check` /
//! `foreign_keys` descriptor fields, which stay rejected (and are
//! `#[deprecated]`).
//!
//! The record codec itself lives in [`vgi_protocol::constraints`] (so clients
//! can decode records without this crate) and is re-exported here.
//!
//! The DuckDB `vgi` extension reads these records only when:
//!
//! 1. the attach negotiated [`CONSTRAINT_RECORDS_READ`] (and
//!    [`ROW_SCALAR_EXPRESSION`] for CHECK). The SDK advertises and selects both
//!    automatically for a [`CatalogModel`](crate::catalog::CatalogModel) whose
//!    tables carry records — see [`select_read_profiles`];
//! 2. every record passes the extension's ATTACH checks — ENFORCED + ALL_ROWS,
//!    owned by the serving table and schema, unique identities and SQL labels,
//!    at most one PRIMARY KEY, resolvable column paths, same-schema
//!    NO ACTION/RESTRICT foreign keys, numeric-comparison CHECKs. The SDK runs
//!    the same checks when serving ([`validate_table_constraints`]), so a
//!    worker's own tests fail with the table, constraint and path instead of
//!    DuckDB failing the ATTACH;
//! 3. every table's Arrow nullability equals membership in the NOT_NULL ∪
//!    PRIMARY_KEY column-path set, descending through ordinary STRUCTs. The SDK
//!    makes this true when serving ([`served_columns`]): a field no record names
//!    is served nullable, a field a record names is served non-nullable. A
//!    non-nullable Arrow field is therefore never itself a claim.
//!
//! ```
//! use vgi::constraints::{
//!     constraint_record, read_constraint_record, ConstraintEnforcement, ConstraintKind,
//!     ConstraintRecord, ConstraintValidation,
//! };
//!
//! // The provider attests that every row it can ever return has a unique,
//! // non-NULL `id`: ENFORCED + ALL_ROWS.
//! let spec = ConstraintRecord::new(&["main"], "users", "users.pk", ConstraintKind::PrimaryKey)
//!     .with_columns(&[&["id"]])
//!     .enforced();
//! let bytes = constraint_record(&spec).unwrap(); // push onto CatTable::constraints
//! let record = read_constraint_record(&bytes.0).unwrap();
//! assert_eq!(record.kind, ConstraintKind::PrimaryKey);
//! assert_eq!(record.enforcement, ConstraintEnforcement::Enforced);
//! assert_eq!(record.validation, ConstraintValidation::AllRows);
//! assert_eq!(record.columns, vec![vec!["id".to_string()]]);
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use vgi_rpc::{Bytes, Result, RpcError};

pub use vgi_protocol::constraints::{
    catalog_capabilities, client_catalog_capabilities, constraint_record, decode_record,
    default_evaluation_context, encode_record, is_default_evaluation_context,
    normalize_read_profiles, read_catalog_capabilities, read_constraint_record,
    select_read_profiles, CatalogCapabilities, CatalogObjectRef, ConstraintDefinition,
    ConstraintEnforcement, ConstraintInfo, ConstraintKind, ConstraintRecord, ConstraintValidation,
    ForeignKeyDefinition, ForeignKeyReference, QueryEvaluationContext, SqlExpression,
    CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION,
};

/// Field-metadata key marking the synthetic row-id column (exempt from the
/// nullability rule, as in the extension).
const ROW_ID_METADATA_KEY: &str = "is_row_id";

/// Bound on STRUCT nesting walked when serving.
const MAX_DEPTH: usize = 64;
/// The extension's per-table bounds.
const MAX_RECORDS: usize = 1024;
const MAX_RECORD_BYTES: usize = 8 * 1024 * 1024;
const MAX_PATHS: usize = 1024;
const MAX_IDENTIFIER: usize = 4096;

fn empty_capabilities() -> CatalogCapabilities {
    CatalogCapabilities {
        read: Vec::new(),
        ddl: Vec::new(),
        write: Vec::new(),
        transaction: Vec::new(),
    }
}

fn sorted_union(a: Vec<String>, b: Vec<String>) -> Vec<String> {
    let mut out: Vec<String> = a.into_iter().chain(b).collect();
    out.sort();
    out.dedup();
    out
}

/// The catalog-capability agreement for an attach: the SDK's automatic
/// constraint-read profiles (`auto_read`, empty when no served table carries
/// records) merged with an explicit
/// [`CapabilityAgreementHook`](crate::catalog::CapabilityAgreementHook) result.
///
/// Returns `(available, selected)` `CatalogCapabilities` IPC, or `None` when
/// neither side has anything to say. A hook result alone is forwarded
/// byte-for-byte; when both contribute, each category is the sorted union, and
/// the scalar-expression read stays selected only alongside constraint records.
pub(crate) fn agree_capabilities(
    auto_read: &[String],
    hook: Option<(Bytes, Bytes)>,
    client_capabilities: Option<&[u8]>,
) -> Result<Option<(Bytes, Bytes)>> {
    if auto_read.is_empty() {
        return Ok(hook);
    }
    let selected_read = select_read_profiles(auto_read, client_capabilities)?;
    let (available, selected) = match hook {
        None => (empty_capabilities(), empty_capabilities()),
        Some((available, selected)) => (
            read_catalog_capabilities(&available.0)?,
            read_catalog_capabilities(&selected.0)?,
        ),
    };
    let merge = |base: CatalogCapabilities, read: Vec<String>| CatalogCapabilities {
        read: normalize_read_profiles(sorted_union(base.read, read)),
        ddl: sorted_union(base.ddl, Vec::new()),
        write: sorted_union(base.write, Vec::new()),
        transaction: sorted_union(base.transaction, Vec::new()),
    };
    // Advertising is not selecting: every implemented profile is available.
    let available = merge(available, auto_read.to_vec());
    let selected = merge(selected, selected_read);
    Ok(Some((encode_record(available)?, encode_record(selected)?)))
}

/// The `(available, selected)` agreement for a catalog that serves records
/// but is not a [`CatalogModel`](crate::catalog::CatalogModel) — e.g. a
/// custom `catalog_attach` handler: advertise both constraint read profiles
/// and select those `client_capabilities` offers. Put the pair in
/// `CatalogAttachResult.catalog_capabilities` / `selected_capabilities`.
pub fn constraint_read_agreement(client_capabilities: Option<&[u8]>) -> Result<(Bytes, Bytes)> {
    agree_capabilities(&read_profiles_for(true), None, client_capabilities)?
        .ok_or_else(|| RpcError::runtime_error("constraint read agreement was empty"))
}

/// Read profiles the SDK implements for a table set: constraint records (with
/// scalar CHECK expressions) when any record is present, else none.
pub(crate) fn read_profiles_for(has_records: bool) -> Vec<String> {
    if has_records {
        vec![
            CONSTRAINT_RECORDS_READ.to_string(),
            ROW_SCALAR_EXPRESSION.to_string(),
        ]
    } else {
        Vec::new()
    }
}

fn ordinary_struct_children(field: &Field) -> Option<&Fields> {
    let DataType::Struct(children) = field.data_type() else {
        return None;
    };
    if field
        .metadata()
        .keys()
        .any(|key| key == "ARROW:extension:name" || key.starts_with("vgi.logical_type."))
    {
        return None;
    }
    Some(children)
}

fn identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_IDENTIFIER && !value.contains('\0')
}

fn is_row_id(field: &Field) -> bool {
    field.metadata().contains_key(ROW_ID_METADATA_KEY)
}

/// The extension's `ResolveNotNullPath`: the first top-level field named
/// `path[0]` (not the row id), then unique children of ordinary STRUCTs.
fn resolve_path(columns: &Schema, path: &[String]) -> std::result::Result<(), String> {
    let missing = || {
        format!(
            "column path {} is missing, ambiguous or the synthetic row-id column",
            path.join(".")
        )
    };
    let Some(mut field) = columns.fields().iter().find(|f| f.name() == &path[0]) else {
        return Err(missing());
    };
    if is_row_id(field) {
        return Err(missing());
    }
    for part in &path[1..] {
        let Some(children) = ordinary_struct_children(field) else {
            return Err(format!(
                "column path {} must traverse ordinary STRUCT fields, not storage members \
                 or extension/logical types",
                path.join(".")
            ));
        };
        let mut matches = children.iter().filter(|c| c.name() == part);
        match (matches.next(), matches.next()) {
            (Some(child), None) => field = child,
            _ => return Err(missing()),
        }
    }
    Ok(())
}

/// The raw FK / CHECK payloads [`read_constraint_record`] summarizes away.
fn check_payloads(
    raw: &[u8],
    schema_path: &[String],
    record: &ConstraintRecord,
) -> std::result::Result<(), String> {
    let info: ConstraintInfo = decode_record(raw).map_err(|e| e.message)?;
    let definition: ConstraintDefinition =
        decode_record(&info.definition.0).map_err(|e| e.message)?;
    if let Some(expression) = &definition.check_expression {
        let expression: SqlExpression = decode_record(&expression.0).map_err(|e| e.message)?;
        if expression.profile != ROW_SCALAR_EXPRESSION {
            return Err(format!(
                "CHECK expression profile '{}' is not {ROW_SCALAR_EXPRESSION}",
                expression.profile
            ));
        }
        if expression.schema_path != schema_path {
            return Err(format!(
                "CHECK expression belongs to schema {:?}, not {schema_path:?}",
                expression.schema_path
            ));
        }
        if !is_default_evaluation_context(&expression.evaluation_context) {
            return Err(
                "CHECK evaluation context must be UTC / binary / asc / nulls_last / \
                 preserve_insertion_order"
                    .into(),
            );
        }
    }
    if let (Some(fk), Some(key)) = (&definition.foreign_key, &info.referenced_constraint) {
        let fk: ForeignKeyDefinition = decode_record(&fk.0).map_err(|e| e.message)?;
        let table: CatalogObjectRef =
            decode_record(&fk.referenced_table.0).map_err(|e| e.message)?;
        let key: CatalogObjectRef = decode_record(&key.0).map_err(|e| e.message)?;
        if table.kind != "table" || table.owner_table.is_some() {
            return Err("FOREIGN KEY referenced_table must be a table reference".into());
        }
        if key.kind != "constraint" {
            return Err("FOREIGN KEY referenced_constraint must be a constraint reference".into());
        }
        if table.schema_path != schema_path || key.schema_path != schema_path {
            return Err(format!(
                "FOREIGN KEY must reference a key in the same schema {schema_path:?} \
                 (references {:?})",
                table.schema_path
            ));
        }
        if key.owner_table.as_deref() != Some(table.name.as_str()) {
            return Err(format!(
                "FOREIGN KEY referenced constraint '{}' is not owned by referenced table '{}'",
                key.name, table.name
            ));
        }
        if fk.r#match.0 != "SIMPLE" {
            return Err(format!(
                "FOREIGN KEY MATCH {} is unsupported; use SIMPLE",
                fk.r#match.0
            ));
        }
        for (label, action) in [
            ("ON UPDATE", &fk.on_update.0),
            ("ON DELETE", &fk.on_delete.0),
        ] {
            if action != "NO_ACTION" && action != "RESTRICT" {
                return Err(format!(
                    "FOREIGN KEY {label} {action} is unsupported; only NO_ACTION / RESTRICT"
                ));
            }
        }
        let target = record.references.as_ref().map(|r| &r.columns);
        if target.is_some_and(|paths| paths.iter().any(|p| p.len() != 1)) {
            return Err("FOREIGN KEY referenced columns must be top-level columns".into());
        }
    }
    Ok(())
}

fn check_paths(paths: &[Vec<String>]) -> std::result::Result<(), String> {
    if paths.len() > MAX_PATHS {
        return Err(format!("more than {MAX_PATHS} column paths"));
    }
    let mut seen = HashSet::new();
    for path in paths {
        if path.is_empty() || path.len() > MAX_DEPTH {
            return Err("empty or oversized column path".into());
        }
        if !path.iter().all(|c| identifier(c)) {
            return Err("column path components must be nonempty identifiers without NUL".into());
        }
        if !seen.insert(path) {
            return Err(format!("duplicate column path {}", path.join(".")));
        }
    }
    Ok(())
}

/// Decode and check a table's records exactly as the extension does at
/// ATTACH (`DecodeNativeConstraints`), except nullability, which serving
/// normalizes. Returns the decoded records.
fn checked_records(
    schema_path: &[String],
    table: &str,
    columns: &Schema,
    constraints: &[Bytes],
) -> Result<Vec<ConstraintRecord>> {
    let fail = |message: String| {
        Err(RpcError::value_error(format!(
            "Table '{}.{table}': {message}",
            schema_path.join(".")
        )))
    };
    if constraints.len() > MAX_RECORDS {
        return fail(format!("more than {MAX_RECORDS} constraint records"));
    }
    let mut total = 0;
    for raw in constraints {
        total += raw.0.len();
        if raw.0.is_empty() || raw.0.len() > MAX_RECORD_BYTES || total > MAX_RECORD_BYTES {
            return fail("constraint records exceed the 8 MiB size limit (or one is empty)".into());
        }
    }
    let mut records = Vec::with_capacity(constraints.len());
    let mut ids = HashSet::new();
    let mut labels = HashSet::new();
    let mut primary_key: Option<String> = None;
    for (index, raw) in constraints.iter().enumerate() {
        let record = match read_constraint_record(&raw.0) {
            Ok(record) => record,
            Err(e) => {
                return fail(format!(
                    "constraint record #{index} is not a canonical ConstraintInfo: {}",
                    e.message
                ))
            }
        };
        let id = record.id.clone();
        let constraint = |message: String| fail(format!("constraint '{id}': {message}"));
        if schema_path.len() != 1 || record.schema_path != schema_path {
            return constraint(format!(
                "belongs to schema {:?}; records must name the serving table's one-component \
                 schema {schema_path:?}",
                record.schema_path
            ));
        }
        if record.table != table {
            return constraint(format!("is owned by table '{}'", record.table));
        }
        if !identifier(&record.id) || !identifier(&record.table) {
            return constraint("identity must be nonempty identifiers without NUL".into());
        }
        if !ids.insert(record.id.clone()) {
            return fail(format!("duplicate constraint identity '{id}'"));
        }
        if !record.is_readable() {
            return constraint(format!(
                "is {}/{}; DuckDB accepts only ENFORCED + ALL_ROWS records",
                record.enforcement, record.validation
            ));
        }
        if let Some(label) = &record.declared_name {
            if !identifier(label) {
                return constraint("declared_name must be a nonempty identifier".into());
            }
            if !labels.insert(label.to_ascii_lowercase()) {
                return constraint(format!(
                    "duplicate SQL constraint name '{label}' (names compare case-insensitively)"
                ));
            }
        }
        if let Err(e) = check_paths(&record.columns) {
            return constraint(e);
        }
        if let Some(target) = &record.references {
            if let Err(e) = check_paths(&target.columns) {
                return constraint(format!("referenced columns: {e}"));
            }
        }
        if let Err(e) = check_payloads(&raw.0, schema_path, &record) {
            return constraint(e);
        }
        match record.kind {
            ConstraintKind::Check => {
                let sql = record.check_sql.as_deref().unwrap_or_default();
                if let Err(e) = crate::sql_expression::check_numeric_comparison(sql, columns) {
                    return constraint(e.message);
                }
            }
            ConstraintKind::NotNull => {
                if let Err(e) = resolve_path(columns, &record.columns[0]) {
                    return constraint(e);
                }
            }
            ConstraintKind::PrimaryKey | ConstraintKind::Unique | ConstraintKind::ForeignKey => {
                for path in &record.columns {
                    if path.len() != 1 {
                        return constraint(format!(
                            "column path {} is nested; DuckDB supports nested paths only for \
                             NOT_NULL",
                            path.join(".")
                        ));
                    }
                    if let Err(e) = resolve_path(columns, path) {
                        return constraint(e);
                    }
                }
                if record.kind == ConstraintKind::PrimaryKey {
                    if let Some(first) = primary_key.replace(record.id.clone()) {
                        return constraint(format!(
                            "is a second PRIMARY KEY (table already has '{first}')"
                        ));
                    }
                }
            }
        }
        records.push(record);
    }
    Ok(records)
}

/// Check a table's records the way the DuckDB extension does at ATTACH.
///
/// Every record must decode; belong to `table` in `schema_path` (a
/// one-component schema path, as the extension requires); carry a unique
/// identity and, when set, a case-insensitively unique `declared_name`; and be
/// `ENFORCED` + `ALL_ROWS`. A table has at most one PRIMARY KEY. NOT_NULL paths
/// must resolve (descending only through ordinary STRUCTs — not those tagged
/// `ARROW:extension:name` or `vgi.logical_type.*`); PRIMARY KEY / UNIQUE /
/// FOREIGN KEY paths must be top-level columns. A FOREIGN KEY must reference a
/// key in the same schema, `MATCH SIMPLE`, with `NO_ACTION` / `RESTRICT`
/// actions (see also [`validate_foreign_key_targets`]). A CHECK must be one
/// numeric comparison of primitive signed numeric columns and numeric literals.
/// Errors name the table, constraint and column path.
///
/// Arrow nullability is *not* checked: serving rewrites it to match the records
/// (see [`served_columns`]).
pub fn validate_table_constraints(
    schema_path: &[String],
    table: &str,
    columns: &Schema,
    constraints: &[Bytes],
) -> Result<()> {
    checked_records(schema_path, table, columns, constraints).map(|_| ())
}

/// The columns as DuckDB must see them: [`validate_table_constraints`], then
/// every field's nullability set to membership in the NOT_NULL ∪ PRIMARY_KEY
/// path set (descending through ordinary STRUCTs; the top-level `is_row_id`
/// column is left untouched). A non-nullable field no record names is served
/// nullable; a nullable field a record names is served non-nullable — the
/// ENFORCED + ALL_ROWS record is the attestation. Metadata is preserved.
pub fn served_columns(
    schema_path: &[String],
    table: &str,
    columns: &Schema,
    constraints: &[Bytes],
) -> Result<Schema> {
    let records = checked_records(schema_path, table, columns, constraints)?;
    let required: HashSet<Vec<String>> = records
        .into_iter()
        .filter(|r| matches!(r.kind, ConstraintKind::NotNull | ConstraintKind::PrimaryKey))
        .flat_map(|r| r.columns)
        .collect();
    let fields = normalize_fields(columns.fields(), &[], &required)?;
    Ok(Schema::new_with_metadata(
        fields,
        columns.metadata().clone(),
    ))
}

fn normalize_fields(
    fields: &Fields,
    prefix: &[String],
    required: &HashSet<Vec<String>>,
) -> Result<Fields> {
    if prefix.len() > MAX_DEPTH {
        return Err(RpcError::value_error(format!(
            "column nesting exceeds {MAX_DEPTH} levels"
        )));
    }
    fields
        .iter()
        .map(|field| -> Result<FieldRef> {
            if prefix.is_empty() && is_row_id(field) {
                return Ok(field.clone());
            }
            let mut path = prefix.to_vec();
            path.push(field.name().clone());
            let nullable = !required.contains(&path);
            let mut out = field.as_ref().clone().with_nullable(nullable);
            if let Some(children) = ordinary_struct_children(field) {
                out = out.with_data_type(DataType::Struct(normalize_fields(
                    children, &path, required,
                )?));
            }
            Ok(if out == **field {
                field.clone()
            } else {
                Arc::new(out)
            })
        })
        .collect()
}

/// Check that every FOREIGN KEY among one schema's tables references an
/// existing table of that schema and, on it, an existing PRIMARY KEY or UNIQUE
/// record whose column set equals the FK's referenced columns.
///
/// `tables` is `(table name, its constraint records)` for every table in
/// `schema_path`. Records that do not decode are skipped here (serving the
/// table reports them).
pub fn validate_foreign_key_targets<'a>(
    schema_path: &[String],
    tables: impl IntoIterator<Item = (&'a str, &'a [Bytes])>,
) -> Result<()> {
    let decoded: Vec<(&str, Vec<ConstraintRecord>)> = tables
        .into_iter()
        .map(|(name, records)| {
            let records = records
                .iter()
                .filter_map(|raw| read_constraint_record(&raw.0).ok())
                .collect();
            (name, records)
        })
        .collect();
    let keys: HashMap<String, &[ConstraintRecord]> = decoded
        .iter()
        .map(|(name, records)| (name.to_ascii_lowercase(), records.as_slice()))
        .collect();
    for (table, records) in &decoded {
        for record in records {
            let Some(target) = &record.references else {
                continue;
            };
            let fail = |message: String| {
                Err(RpcError::value_error(format!(
                    "Table '{}.{table}': constraint '{}': {message}",
                    schema_path.join("."),
                    record.id
                )))
            };
            let Some(target_records) = keys.get(&target.table.to_ascii_lowercase()) else {
                return fail(format!(
                    "FOREIGN KEY references table '{}', which this schema does not serve",
                    target.table
                ));
            };
            let Some(key) = target_records.iter().find(|r| r.id == target.constraint_id) else {
                return fail(format!(
                    "FOREIGN KEY references constraint '{}', which table '{}' does not declare",
                    target.constraint_id, target.table
                ));
            };
            if !matches!(
                key.kind,
                ConstraintKind::PrimaryKey | ConstraintKind::Unique
            ) {
                return fail(format!(
                    "FOREIGN KEY references '{}', a {} constraint; it must be a PRIMARY_KEY or \
                     UNIQUE",
                    key.id, key.kind
                ));
            }
            let want: HashSet<&Vec<String>> = target.columns.iter().collect();
            let have: HashSet<&Vec<String>> = key.columns.iter().collect();
            if want != have {
                return fail(format!(
                    "FOREIGN KEY referenced columns {:?} differ from key '{}' columns {:?}",
                    target.columns, key.id, key.columns
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn main() -> Vec<String> {
        vec!["main".to_string()]
    }

    fn pk(table: &str, id: &str, column: &[&str]) -> ConstraintRecord {
        ConstraintRecord::new(&["main"], table, id, ConstraintKind::PrimaryKey)
            .with_columns(&[column])
            .enforced()
    }

    fn not_null(table: &str, id: &str, column: &[&str]) -> ConstraintRecord {
        ConstraintRecord::new(&["main"], table, id, ConstraintKind::NotNull)
            .with_columns(&[column])
            .enforced()
    }

    fn fk(
        table: &str,
        id: &str,
        column: &str,
        target: &str,
        key: &str,
        key_col: &str,
    ) -> ConstraintRecord {
        let mut spec = ConstraintRecord::new(&["main"], table, id, ConstraintKind::ForeignKey)
            .with_columns(&[&[column]])
            .enforced();
        spec.references = Some(ForeignKeyReference {
            schema_path: main(),
            table: target.into(),
            columns: vec![vec![key_col.into()]],
            constraint_id: key.into(),
        });
        spec
    }

    fn check(table: &str, id: &str, sql: &str) -> ConstraintRecord {
        let mut spec =
            ConstraintRecord::new(&["main"], table, id, ConstraintKind::Check).enforced();
        spec.check_sql = Some(sql.into());
        spec
    }

    fn records(specs: &[ConstraintRecord]) -> Vec<Bytes> {
        specs
            .iter()
            .map(|s| constraint_record(s).unwrap())
            .collect()
    }

    fn validate(schema: &Schema, specs: &[ConstraintRecord]) -> Result<()> {
        validate_table_constraints(&main(), "t", schema, &records(specs))
    }

    fn err(schema: &Schema, specs: &[ConstraintRecord]) -> String {
        validate(schema, specs).unwrap_err().to_string()
    }

    fn two_ints() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ])
    }

    #[test]
    fn only_enforced_all_rows_records_are_served() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        assert!(validate(&schema, &[pk("t", "k", &["a"])]).is_ok());
        for (enforcement, validation) in [
            (
                ConstraintEnforcement::MetadataOnly,
                ConstraintValidation::AllRows,
            ),
            (
                ConstraintEnforcement::Enforced,
                ConstraintValidation::FutureWrites,
            ),
            (
                ConstraintEnforcement::Enforced,
                ConstraintValidation::Unvalidated,
            ),
        ] {
            let mut spec = pk("t", "k", &["a"]);
            spec.enforcement = enforcement;
            spec.validation = validation;
            let e = err(&schema, &[spec]);
            assert!(
                e.contains("Table 'main.t'") && e.contains("constraint 'k'"),
                "{e}"
            );
            assert!(e.contains("ENFORCED + ALL_ROWS"), "{e}");
        }
    }

    #[test]
    fn records_must_belong_to_the_table_and_schema() {
        let schema = two_ints();
        assert!(err(&schema, &[pk("other", "k", &["a"])]).contains("owned by table 'other'"));
        let foreign = ConstraintRecord::new(&["other"], "t", "k", ConstraintKind::PrimaryKey)
            .with_columns(&[&["a"]])
            .enforced();
        let e = err(&schema, &[foreign]);
        assert!(
            e.contains("constraint 'k'") && e.contains("belongs to schema"),
            "{e}"
        );
        // The extension reads one-component schema paths only.
        let nested = vec!["a".to_string(), "b".to_string()];
        let deep = ConstraintRecord::new(&["a", "b"], "t", "k", ConstraintKind::PrimaryKey)
            .with_columns(&[&["a"]])
            .enforced();
        assert!(validate_table_constraints(&nested, "t", &schema, &records(&[deep])).is_err());
        let e = err(&schema, &[pk("t", "k", &["a"]), not_null("t", "k", &["b"])]);
        assert!(e.contains("duplicate constraint identity 'k'"), "{e}");
        assert!(
            validate_table_constraints(&main(), "t", &schema, &[Bytes::from(vec![1, 2])]).is_err()
        );
    }

    #[test]
    fn declared_names_primary_keys_and_paths_follow_attach() {
        let schema = two_ints();
        let mut a = not_null("t", "n1", &["a"]);
        a.declared_name = Some("Label".into());
        let mut b = not_null("t", "n2", &["b"]);
        b.declared_name = Some("label".into());
        let e = err(&schema, &[a, b]);
        assert!(
            e.contains("constraint 'n2'") && e.contains("duplicate SQL constraint name"),
            "{e}"
        );
        let e = err(&schema, &[pk("t", "p1", &["a"]), pk("t", "p2", &["b"])]);
        assert!(
            e.contains("constraint 'p2'") && e.contains("second PRIMARY KEY"),
            "{e}"
        );
        let e = err(&schema, &[not_null("t", "n", &["zz"])]);
        assert!(
            e.contains("constraint 'n'") && e.contains("column path zz"),
            "{e}"
        );
        let unique = ConstraintRecord::new(&["main"], "t", "u", ConstraintKind::Unique)
            .with_columns(&[&["a"], &["a"]])
            .enforced();
        assert!(err(&schema, &[unique]).contains("duplicate column path a"));
        let unique = ConstraintRecord::new(&["main"], "t", "u", ConstraintKind::Unique)
            .with_columns(&[&["missing"]])
            .enforced();
        assert!(err(&schema, &[unique]).contains("column path missing"));
        // Row id is synthetic.
        let row_id = Schema::new(vec![Field::new("rowid", DataType::Int64, false)
            .with_metadata(std::collections::HashMap::from([(
                ROW_ID_METADATA_KEY.to_string(),
                "true".to_string(),
            )]))]);
        assert!(validate(&row_id, &[not_null("t", "n", &["rowid"])]).is_err());
    }

    #[test]
    fn nested_paths_only_for_not_null_through_ordinary_structs() {
        let inner = Fields::from(vec![Field::new("x", DataType::Float64, true)]);
        let schema = |metadata: Option<(&str, &str)>| {
            let mut field = Field::new("s", DataType::Struct(inner.clone()), true);
            if let Some((k, v)) = metadata {
                field = field.with_metadata(std::collections::HashMap::from([(
                    k.to_string(),
                    v.to_string(),
                )]));
            }
            Schema::new(vec![field])
        };
        assert!(validate(&schema(None), &[not_null("t", "sx", &["s", "x"])]).is_ok());
        let unique = ConstraintRecord::new(&["main"], "t", "u", ConstraintKind::Unique)
            .with_columns(&[&["s", "x"]])
            .enforced();
        assert!(err(&schema(None), &[unique]).contains("nested"));
        for key in ["ARROW:extension:name", "vgi.logical_type.name"] {
            let e = err(
                &schema(Some((key, "geo"))),
                &[not_null("t", "sx", &["s", "x"])],
            );
            assert!(e.contains("ordinary STRUCT"), "{e}");
        }
    }

    #[test]
    fn check_sql_follows_the_numeric_comparison_rule() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]);
        assert!(validate(&schema, &[check("t", "c", "a >= 0")]).is_ok());
        for sql in ["a + 1 > 0", "s > 0", "a > 0 AND a < 9", "length(s) > 0"] {
            let e = err(&schema, &[check("t", "c", sql)]);
            assert!(
                e.contains("constraint 'c'") && e.contains("CHECK"),
                "{sql}: {e}"
            );
        }
    }

    #[test]
    fn foreign_keys_must_stay_in_schema_with_supported_actions() {
        let schema = two_ints();
        assert!(validate(&schema, &[fk("t", "f", "a", "u", "u.pk", "id")]).is_ok());
        let mut cross = fk("t", "f", "a", "u", "u.pk", "id");
        cross.references.as_mut().unwrap().schema_path = vec!["other".into()];
        assert!(err(&schema, &[cross]).contains("same schema"));
        // A hand-built record with CASCADE.
        let raw = constraint_record(&fk("t", "f", "a", "u", "u.pk", "id")).unwrap();
        let mut info: ConstraintInfo = decode_record(&raw.0).unwrap();
        let mut definition: ConstraintDefinition = decode_record(&info.definition.0).unwrap();
        let mut foreign: ForeignKeyDefinition =
            decode_record(&definition.foreign_key.unwrap().0).unwrap();
        foreign.on_delete = vgi_rpc::DictString("CASCADE".into());
        definition.foreign_key = Some(encode_record(foreign).unwrap());
        info.definition = encode_record(definition).unwrap();
        let e = validate_table_constraints(&main(), "t", &schema, &[encode_record(info).unwrap()])
            .unwrap_err()
            .to_string();
        assert!(e.contains("ON DELETE CASCADE"), "{e}");
    }

    #[test]
    fn foreign_key_targets_resolve_across_the_schema() {
        let users = records(&[pk("u", "u.pk", &["id"])]);
        let orders = records(&[fk("o", "o.fk", "user_id", "u", "u.pk", "id")]);
        let ok = [("u", users.as_slice()), ("o", orders.as_slice())];
        assert!(validate_foreign_key_targets(&main(), ok).is_ok());
        let e = validate_foreign_key_targets(&main(), [("o", orders.as_slice())])
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("constraint 'o.fk'") && e.contains("table 'u'"),
            "{e}"
        );
        let wrong_key = records(&[fk("o", "o.fk", "user_id", "u", "u.missing", "id")]);
        let e = validate_foreign_key_targets(
            &main(),
            [("u", users.as_slice()), ("o", wrong_key.as_slice())],
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("u.missing"), "{e}");
        let wrong_cols = records(&[fk("o", "o.fk", "user_id", "u", "u.pk", "other")]);
        assert!(validate_foreign_key_targets(
            &main(),
            [("u", users.as_slice()), ("o", wrong_cols.as_slice())]
        )
        .is_err());
    }

    #[test]
    fn serving_normalizes_nullability_to_the_records() {
        let inner = Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ]);
        let columns = Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int64, false).with_metadata(
                    std::collections::HashMap::from([("comment".to_string(), "kept".to_string())]),
                ),
                Field::new("b", DataType::Int64, true),
                Field::new("s", DataType::Struct(inner), false),
                Field::new("rowid", DataType::Int64, false).with_metadata(
                    std::collections::HashMap::from([(
                        ROW_ID_METADATA_KEY.to_string(),
                        "true".to_string(),
                    )]),
                ),
            ],
            std::collections::HashMap::from([("schema".to_string(), "meta".to_string())]),
        );
        // No records: every user field (and nested child) is served nullable;
        // the row id is untouched.
        let served = served_columns(&main(), "t", &columns, &[]).unwrap();
        assert!(served.field(0).is_nullable());
        assert_eq!(served.field(0).metadata()["comment"], "kept");
        assert_eq!(served.metadata()["schema"], "meta");
        let DataType::Struct(children) = served.field(2).data_type() else {
            panic!()
        };
        assert!(served.field(2).is_nullable());
        assert!(children.iter().all(|c| c.is_nullable()));
        assert!(!served.field(3).is_nullable());
        // Record-named fields are served non-nullable, even when the Arrow field
        // was nullable.
        let named = records(&[pk("t", "k", &["b"]), not_null("t", "sx", &["s", "x"])]);
        let served = served_columns(&main(), "t", &columns, &named).unwrap();
        assert!(served.field(0).is_nullable());
        assert!(!served.field(1).is_nullable());
        let DataType::Struct(children) = served.field(2).data_type() else {
            panic!()
        };
        assert!(served.field(2).is_nullable());
        assert!(!children[0].is_nullable());
        assert!(children[1].is_nullable());
        // Opaque structs keep their children as declared.
        let opaque = Schema::new(vec![Field::new(
            "g",
            DataType::Struct(Fields::from(vec![Field::new(
                "x",
                DataType::Float64,
                false,
            )])),
            false,
        )
        .with_metadata(std::collections::HashMap::from([(
            "ARROW:extension:name".to_string(),
            "geo".to_string(),
        )]))]);
        let served = served_columns(&main(), "t", &opaque, &[]).unwrap();
        assert!(served.field(0).is_nullable());
        let DataType::Struct(children) = served.field(0).data_type() else {
            panic!()
        };
        assert!(!children[0].is_nullable());
    }

    fn offer(read: &[&str]) -> Bytes {
        encode_record(crate::protocol::dtos::ClientCapabilities {
            engine: "test".into(),
            native_formats: vec![],
            catalogs: vec![],
            can_stream: true,
            filter_encodings: vec![],
            query_pushdown: None,
            catalog: Some(catalog_capabilities(read, &[], &[], &[]).unwrap()),
        })
        .unwrap()
    }

    #[test]
    fn read_profiles_are_intersected_with_scalar_prerequisite() {
        let available = read_profiles_for(true);
        assert!(select_read_profiles(&available, None).unwrap().is_empty());
        let r = CONSTRAINT_RECORDS_READ;
        let s = ROW_SCALAR_EXPRESSION;
        for (offered, expected) in [
            (vec![], vec![]),
            (vec![s], vec![]),
            (vec![r], vec![r]),
            (vec![s, "vgi.unknown.v1", r], vec![r, s]),
        ] {
            let selected = select_read_profiles(&available, Some(&offer(&offered).0)).unwrap();
            assert_eq!(selected, expected);
        }
    }

    #[test]
    fn agreement_merges_auto_profiles_with_hook() {
        let r = CONSTRAINT_RECORDS_READ;
        let s = ROW_SCALAR_EXPRESSION;
        let client = offer(&[r, s, "vgi.index.read.v1"]);
        // No records: the hook result passes through untouched.
        let hook = (Bytes::from(vec![9]), Bytes::from(vec![8]));
        let out = agree_capabilities(&[], Some(hook.clone()), Some(&client.0)).unwrap();
        assert_eq!(out.unwrap().0 .0, hook.0 .0);
        assert!(agree_capabilities(&[], None, Some(&client.0))
            .unwrap()
            .is_none());
        // Records, no hook.
        let (available, selected) = constraint_read_agreement(Some(&client.0)).unwrap();
        let available = read_catalog_capabilities(&available.0).unwrap();
        let selected = read_catalog_capabilities(&selected.0).unwrap();
        assert_eq!(available.read, vec![r, s]);
        assert_eq!(selected.read, vec![r, s]);
        // Records plus a hook selecting an extra profile: union, deduplicated.
        let caps = |read: &[&str]| catalog_capabilities(read, &[], &[], &[]).unwrap();
        let hook = (
            caps(&[r, "vgi.index.read.v1"]),
            caps(&[r, "vgi.index.read.v1"]),
        );
        let (available, selected) =
            agree_capabilities(&read_profiles_for(true), Some(hook), Some(&client.0))
                .unwrap()
                .unwrap();
        let available = read_catalog_capabilities(&available.0).unwrap();
        let selected = read_catalog_capabilities(&selected.0).unwrap();
        assert_eq!(available.read, vec![r, s, "vgi.index.read.v1"]);
        assert_eq!(selected.read, vec![r, s, "vgi.index.read.v1"]);
        // No offer: advertised, nothing selected.
        let (_, selected) = constraint_read_agreement(None).unwrap();
        assert!(read_catalog_capabilities(&selected.0)
            .unwrap()
            .read
            .is_empty());
    }
}

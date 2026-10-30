// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Canonical constraint records for catalogs that state their own facts.
//!
//! Under `vgi.v3` a table's constraints travel as canonical `ConstraintInfo`
//! IPC records in [`CatTable::constraints`](crate::catalog::CatTable::constraints).
//! The worker *attests* each fact: the SDK never derives a record from data or
//! from the legacy `not_null` / `primary_key` / `unique` / `check` /
//! `foreign_keys` descriptor fields, which stay rejected.
//!
//! The DuckDB `vgi` extension reads these records only when:
//!
//! 1. the attach negotiated [`CONSTRAINT_RECORDS_READ`] (and
//!    [`ROW_SCALAR_EXPRESSION`] for CHECK). The SDK advertises and selects both
//!    automatically for a [`CatalogModel`](crate::catalog::CatalogModel) whose
//!    tables carry records — see [`select_read_profiles`];
//! 2. every record is [`ConstraintEnforcement::Enforced`] +
//!    [`ConstraintValidation::AllRows`];
//! 3. every table's Arrow nullability equals membership in the NOT_NULL ∪
//!    PRIMARY_KEY column-path set, descending through ordinary STRUCTs.
//!
//! The SDK checks 2 and 3 when serving a table
//! ([`validate_table_constraints`]), so a worker's own tests fail with the
//! table and column path instead of DuckDB failing the ATTACH.
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

use std::collections::HashSet;
use std::fmt;

use arrow_schema::{DataType, Field, Fields, Schema};
use vgi_rpc::{Bytes, DictString, Result, RpcError, VgiArrow};

use crate::{ipc, wire};

/// The generated wire records, for authors who need the raw shapes.
pub use vgi_protocol::generated::protocol_types::{
    CatalogCapabilities, CatalogObjectRef, ConstraintDefinition, ConstraintInfo,
    ForeignKeyDefinition, QueryEvaluationContext, SqlExpression,
};

/// Read profile: the catalog serves canonical `ConstraintInfo` records.
pub const CONSTRAINT_RECORDS_READ: &str = "vgi.constraint.records.read.v1";
/// Read profile: CHECK constraints carry DuckDB scalar SQL expressions.
/// Selected only alongside [`CONSTRAINT_RECORDS_READ`].
pub const ROW_SCALAR_EXPRESSION: &str = "vgi.expression.duckdb.scalar.v1";

/// Field-metadata key marking the synthetic row-id column (exempt from the
/// nullability rule, as in the extension).
const ROW_ID_METADATA_KEY: &str = "is_row_id";

/// Bound on STRUCT nesting walked by [`validate_table_constraints`].
const MAX_DEPTH: usize = 64;

macro_rules! wire_enum {
    ($(#[$meta:meta])* $name:ident { $($(#[$vmeta:meta])* $variant:ident => $wire:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum $name {
            $($(#[$vmeta])* $variant),+
        }

        impl $name {
            /// The canonical wire spelling.
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire),+
                }
            }

            /// Parse the canonical wire spelling.
            pub fn parse(value: &str) -> Result<Self> {
                match value {
                    $($wire => Ok(Self::$variant),)+
                    other => Err(RpcError::value_error(format!(
                        concat!("unknown ", stringify!($name), " '{}'"),
                        other
                    ))),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

wire_enum!(
    /// The constraint's kind.
    ConstraintKind {
        /// One column path must never be NULL.
        NotNull => "NOT_NULL",
        /// The table's (single) primary key; implies NOT NULL on every path.
        PrimaryKey => "PRIMARY_KEY",
        /// Non-NULL key tuples are distinct.
        Unique => "UNIQUE",
        /// A DuckDB scalar expression holds for every row.
        Check => "CHECK",
        /// Non-NULL key tuples exist in a referenced PRIMARY KEY / UNIQUE key.
        ForeignKey => "FOREIGN_KEY",
    }
);

wire_enum!(
    /// Whether the authority enforces the constraint on every write path.
    ConstraintEnforcement {
        /// Every write path enforces the constraint.
        Enforced => "ENFORCED",
        /// Informational only; DuckDB will not read the record.
        MetadataOnly => "METADATA_ONLY",
    }
);

wire_enum!(
    /// Coverage of existing rows. Only `AllRows` justifies engine facts.
    ConstraintValidation {
        /// Every row the table can return satisfies the constraint.
        AllRows => "ALL_ROWS",
        /// Only rows written after the constraint was declared.
        FutureWrites => "FUTURE_WRITES",
        /// Never validated.
        Unvalidated => "UNVALIDATED",
    }
);

/// The key a FOREIGN_KEY record references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyReference {
    /// Schema path of the referenced table.
    pub schema_path: Vec<String>,
    /// Referenced table name.
    pub table: String,
    /// Referenced column paths, matching the local paths' arity.
    pub columns: Vec<Vec<String>>,
    /// `id` of the referenced table's PRIMARY_KEY or UNIQUE record.
    pub constraint_id: String,
}

/// One canonical constraint, in authoring vocabulary.
///
/// [`constraint_record`] encodes it as `ConstraintInfo` IPC;
/// [`read_constraint_record`] is the inverse. There is deliberately no
/// `Default`: `enforcement` and `validation` are the provider's attestation and
/// must be stated. DuckDB reads only `Enforced` + `AllRows` records, so state
/// that only when every row the table can ever return satisfies the constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintRecord {
    /// Schema path of the owning table.
    pub schema_path: Vec<String>,
    /// Owning table name.
    pub table: String,
    /// Stable provider-owned identity, unique within the table. Never derived
    /// by the SDK.
    pub id: String,
    pub kind: ConstraintKind,
    /// Raw component paths, e.g. `[["id"]]` or `[["bbox", "xmin"]]`; empty for
    /// CHECK.
    pub columns: Vec<Vec<String>>,
    pub enforcement: ConstraintEnforcement,
    pub validation: ConstraintValidation,
    /// Optional SQL label (`CONSTRAINT <name>`).
    pub declared_name: Option<String>,
    /// CHECK only: a DuckDB scalar expression over the table's columns.
    pub check_sql: Option<String>,
    /// FOREIGN_KEY only: the referenced key and its constraint identity.
    pub references: Option<ForeignKeyReference>,
}

impl ConstraintRecord {
    /// A record skeleton with no columns, attested `METADATA_ONLY` +
    /// `UNVALIDATED` — the honest default, which serving rejects because DuckDB
    /// will not read it. Call [`enforced`](Self::enforced) (or set
    /// `enforcement`/`validation`) once the provider guarantees the fact for
    /// every row the table can return.
    pub fn new(schema_path: &[&str], table: &str, id: &str, kind: ConstraintKind) -> Self {
        Self {
            schema_path: schema_path.iter().map(|s| (*s).to_string()).collect(),
            table: table.to_string(),
            id: id.to_string(),
            kind,
            columns: Vec::new(),
            enforcement: ConstraintEnforcement::MetadataOnly,
            validation: ConstraintValidation::Unvalidated,
            declared_name: None,
            check_sql: None,
            references: None,
        }
    }

    /// Set the constrained column paths.
    pub fn with_columns(mut self, columns: &[&[&str]]) -> Self {
        self.columns = paths(columns);
        self
    }

    /// Attest `ENFORCED` + `ALL_ROWS`: every row the table can return satisfies
    /// this constraint.
    pub fn enforced(mut self) -> Self {
        self.enforcement = ConstraintEnforcement::Enforced;
        self.validation = ConstraintValidation::AllRows;
        self
    }

    /// Whether DuckDB will read this record (`ENFORCED` + `ALL_ROWS`).
    pub fn is_readable(&self) -> bool {
        self.enforcement == ConstraintEnforcement::Enforced
            && self.validation == ConstraintValidation::AllRows
    }

    /// Encode as `ConstraintInfo` IPC; see [`constraint_record`].
    pub fn encode(&self) -> Result<Bytes> {
        constraint_record(self)
    }
}

fn paths(columns: &[&[&str]]) -> Vec<Vec<String>> {
    columns
        .iter()
        .map(|path| path.iter().map(|c| (*c).to_string()).collect())
        .collect()
}

fn encode<T: VgiArrow>(value: T) -> Result<Bytes> {
    Ok(Bytes::from(ipc::write_batch(&wire::to_batch(value)?)?))
}

fn decode<T: VgiArrow>(bytes: &[u8]) -> Result<T> {
    wire::from_batch(&ipc::read_batch(bytes)?)
}

fn object_ref(
    kind: &str,
    schema_path: &[String],
    name: &str,
    owner: Option<&str>,
) -> Result<Bytes> {
    encode(CatalogObjectRef {
        kind: kind.to_string(),
        schema_path: schema_path.to_vec(),
        name: name.to_string(),
        owner_table: owner.map(str::to_string),
    })
}

fn check_shape(spec: &ConstraintRecord) -> Result<()> {
    let invalid = |message: &str| {
        Err(RpcError::value_error(format!(
            "constraint '{}' on table '{}': {message}",
            spec.id, spec.table
        )))
    };
    if spec.id.is_empty() || spec.id.contains('\0') {
        return invalid("id must be nonempty without NUL");
    }
    if spec.table.is_empty() {
        return invalid("owning table must be named");
    }
    let is_check = spec.kind == ConstraintKind::Check;
    let is_fk = spec.kind == ConstraintKind::ForeignKey;
    if is_check != spec.check_sql.is_some() {
        return invalid("check_sql is required for CHECK and only CHECK");
    }
    if is_check != spec.columns.is_empty() {
        return invalid(if is_check {
            "CHECK takes no column paths"
        } else {
            "requires column paths"
        });
    }
    if spec
        .columns
        .iter()
        .any(|p| p.is_empty() || p.iter().any(|c| c.is_empty() || c.contains('\0')))
    {
        return invalid("column paths must be nonempty components without NUL");
    }
    if spec.kind == ConstraintKind::NotNull && spec.columns.len() != 1 {
        return invalid("NOT_NULL requires exactly one column path");
    }
    if is_fk != spec.references.is_some() {
        return invalid("references is required for FOREIGN_KEY and only FOREIGN_KEY");
    }
    if let Some(target) = &spec.references {
        if target.columns.len() != spec.columns.len() {
            return invalid("foreign key local and referenced arity must match");
        }
        if target.constraint_id.is_empty() {
            return invalid("foreign key must name the referenced key's constraint id");
        }
    }
    Ok(())
}

/// Encode one canonical `ConstraintInfo` record for
/// [`CatTable::constraints`](crate::catalog::CatTable::constraints).
///
/// Validates the record's shape (nonempty identity, CHECK ⇔ `check_sql`,
/// FOREIGN_KEY ⇔ `references`, NOT_NULL takes one path, FK arity). It encodes
/// whatever enforcement/validation the provider attests; serving rejects a
/// record DuckDB would not read.
pub fn constraint_record(spec: &ConstraintRecord) -> Result<Bytes> {
    check_shape(spec)?;
    let check_expression = spec
        .check_sql
        .as_ref()
        .map(|sql| {
            encode(SqlExpression {
                sql: sql.clone(),
                profile: ROW_SCALAR_EXPRESSION.to_string(),
                schema_path: spec.schema_path.clone(),
                evaluation_context: QueryEvaluationContext {
                    timezone: "UTC".into(),
                    collation: "binary".into(),
                    default_order: "asc".into(),
                    default_null_order: "nulls_last".into(),
                    preserve_insertion_order: true,
                },
            })
        })
        .transpose()?;
    let foreign_key = spec
        .references
        .as_ref()
        .map(|target| {
            encode(ForeignKeyDefinition {
                referenced_table: object_ref("table", &target.schema_path, &target.table, None)?,
                referenced_column_paths: target.columns.clone(),
                r#match: DictString("SIMPLE".into()),
                on_update: DictString("NO_ACTION".into()),
                on_delete: DictString("NO_ACTION".into()),
            })
        })
        .transpose()?;
    let referenced_constraint = spec
        .references
        .as_ref()
        .map(|target| {
            object_ref(
                "constraint",
                &target.schema_path,
                &target.constraint_id,
                Some(&target.table),
            )
        })
        .transpose()?;
    encode(ConstraintInfo {
        r#ref: object_ref("constraint", &spec.schema_path, &spec.id, Some(&spec.table))?,
        definition: encode(ConstraintDefinition {
            kind: DictString(spec.kind.as_str().into()),
            column_paths: spec.columns.clone(),
            declared_name: spec.declared_name.clone(),
            check_expression,
            foreign_key,
        })?,
        enforcement: DictString(spec.enforcement.as_str().into()),
        validation: DictString(spec.validation.as_str().into()),
        referenced_constraint,
    })
}

/// Decode a `TableInfo.constraints` entry (the inverse of
/// [`constraint_record`]). Fails on an unknown kind / enforcement / validation,
/// a non-constraint identity, or a record whose shape [`constraint_record`]
/// would refuse to build.
pub fn read_constraint_record(bytes: &[u8]) -> Result<ConstraintRecord> {
    let info: ConstraintInfo = decode(bytes)?;
    let identity: CatalogObjectRef = decode(&info.r#ref.0)?;
    if identity.kind != "constraint" {
        return Err(RpcError::value_error(format!(
            "constraint record identity has kind '{}', expected 'constraint'",
            identity.kind
        )));
    }
    let definition: ConstraintDefinition = decode(&info.definition.0)?;
    let check_sql = definition
        .check_expression
        .as_ref()
        .map(|raw| decode::<SqlExpression>(&raw.0).map(|e| e.sql))
        .transpose()?;
    let references = match &definition.foreign_key {
        None => None,
        Some(raw) => {
            let fk: ForeignKeyDefinition = decode(&raw.0)?;
            let target: CatalogObjectRef = decode(&fk.referenced_table.0)?;
            let key = info
                .referenced_constraint
                .as_ref()
                .map(|raw| decode::<CatalogObjectRef>(&raw.0))
                .transpose()?
                .ok_or_else(|| {
                    RpcError::value_error("FOREIGN_KEY record lacks referenced_constraint")
                })?;
            Some(ForeignKeyReference {
                schema_path: target.schema_path,
                table: target.name,
                columns: fk.referenced_column_paths,
                constraint_id: key.name,
            })
        }
    };
    let record = ConstraintRecord {
        schema_path: identity.schema_path,
        table: identity.owner_table.unwrap_or_default(),
        id: identity.name,
        kind: ConstraintKind::parse(&definition.kind.0)?,
        columns: definition.column_paths,
        enforcement: ConstraintEnforcement::parse(&info.enforcement.0)?,
        validation: ConstraintValidation::parse(&info.validation.0)?,
        declared_name: definition.declared_name,
        check_sql,
        references,
    };
    check_shape(&record)?;
    Ok(record)
}

/// The client-offered profiles from `available`, sorted and deduplicated.
///
/// `client_capabilities` is the attach request's serialized
/// `ClientCapabilities`; its `catalog.read` list is the offer. A
/// [`ROW_SCALAR_EXPRESSION`] read is selected only alongside
/// [`CONSTRAINT_RECORDS_READ`].
pub fn select_read_profiles(
    available: &[String],
    client_capabilities: Option<&[u8]>,
) -> Result<Vec<String>> {
    let Some(raw) = client_capabilities.filter(|raw| !raw.is_empty()) else {
        return Ok(Vec::new());
    };
    let client: crate::protocol::dtos::ClientCapabilities = decode(raw)?;
    let offered = match client.catalog {
        Some(catalog) if !catalog.0.is_empty() => decode::<CatalogCapabilities>(&catalog.0)?.read,
        _ => Vec::new(),
    };
    let selected: Vec<String> = available
        .iter()
        .filter(|p| offered.contains(p))
        .cloned()
        .collect();
    Ok(require_records_for_scalar(selected))
}

/// Sort, dedupe, and drop the scalar-expression profile unless constraint
/// records are also selected.
fn require_records_for_scalar(mut read: Vec<String>) -> Vec<String> {
    read.sort();
    read.dedup();
    if !read.iter().any(|p| p == CONSTRAINT_RECORDS_READ) {
        read.retain(|p| p != ROW_SCALAR_EXPRESSION);
    }
    read
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
    let empty = || CatalogCapabilities {
        read: Vec::new(),
        ddl: Vec::new(),
        write: Vec::new(),
        transaction: Vec::new(),
    };
    let (available, selected) = match hook {
        None => (empty(), empty()),
        Some((available, selected)) => {
            let read = |raw: &Bytes| -> Result<CatalogCapabilities> {
                if raw.0.is_empty() {
                    Ok(empty())
                } else {
                    decode(&raw.0)
                }
            };
            (read(&available)?, read(&selected)?)
        }
    };
    let merge = |base: CatalogCapabilities, read: Vec<String>| CatalogCapabilities {
        read: require_records_for_scalar(sorted_union(base.read, read)),
        ddl: sorted_union(base.ddl, Vec::new()),
        write: sorted_union(base.write, Vec::new()),
        transaction: sorted_union(base.transaction, Vec::new()),
    };
    // Advertising is not selecting: every implemented profile is available.
    let available = merge(available, auto_read.to_vec());
    let selected = merge(selected, selected_read);
    Ok(Some((encode(available)?, encode(selected)?)))
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

/// Check a table's records the way the DuckDB extension does at ATTACH.
///
/// Every record must decode, belong to `table`, carry a unique identity and be
/// `ENFORCED` + `ALL_ROWS`; and for every field (descending through ordinary
/// STRUCTs — not those tagged `ARROW:extension:name` or `vgi.logical_type.*`),
/// Arrow nullability must be the complement of membership in the NOT_NULL ∪
/// PRIMARY_KEY column-path set. The top-level `is_row_id` column is exempt.
/// Applies to every table, including one with no records (then every field
/// must be nullable). Errors name the table, constraint and column path.
pub fn validate_table_constraints(
    table: &str,
    columns: &Schema,
    constraints: &[Bytes],
) -> Result<()> {
    let fail = |message: String| Err(RpcError::value_error(format!("Table '{table}': {message}")));
    let mut required: HashSet<Vec<String>> = HashSet::new();
    let mut ids: HashSet<String> = HashSet::new();
    for (index, raw) in constraints.iter().enumerate() {
        let record = read_constraint_record(&raw.0).map_err(|e| {
            RpcError::value_error(format!(
                "Table '{table}': constraint record #{index} is not a canonical ConstraintInfo: {e}"
            ))
        })?;
        if record.table != table {
            return fail(format!(
                "constraint '{}' is owned by table '{}'",
                record.id, record.table
            ));
        }
        if !ids.insert(record.id.clone()) {
            return fail(format!("duplicate constraint identity '{}'", record.id));
        }
        if !record.is_readable() {
            return fail(format!(
                "constraint '{}' is {}/{}; DuckDB accepts only ENFORCED + ALL_ROWS records",
                record.id, record.enforcement, record.validation
            ));
        }
        if matches!(
            record.kind,
            ConstraintKind::NotNull | ConstraintKind::PrimaryKey
        ) {
            required.extend(record.columns);
        }
    }
    let mut named: HashSet<Vec<String>> = HashSet::new();
    fn walk(
        table: &str,
        fields: &Fields,
        prefix: &[String],
        required: &HashSet<Vec<String>>,
        named: &mut HashSet<Vec<String>>,
    ) -> Result<()> {
        if prefix.len() > MAX_DEPTH {
            return Err(RpcError::value_error(format!(
                "Table '{table}': column nesting exceeds {MAX_DEPTH} levels"
            )));
        }
        for field in fields {
            if prefix.is_empty() && field.metadata().contains_key(ROW_ID_METADATA_KEY) {
                continue;
            }
            let mut path = prefix.to_vec();
            path.push(field.name().clone());
            let is_required = required.contains(&path);
            if field.is_nullable() == is_required {
                let dotted = path.join(".");
                return Err(RpcError::value_error(if is_required {
                    format!(
                        "Table '{table}': column {dotted} is in a NOT_NULL/PRIMARY_KEY record \
                         but its Arrow field is nullable; mark the field non-nullable"
                    )
                } else {
                    format!(
                        "Table '{table}': column {dotted} is non-nullable in Arrow but no \
                         NOT_NULL/PRIMARY_KEY record names it; declare one with \
                         vgi::constraints::constraint_record or make the field nullable"
                    )
                }));
            }
            if is_required {
                named.insert(path.clone());
            }
            if let Some(children) = ordinary_struct_children(field) {
                walk(table, children, &path, required, named)?;
            }
        }
        Ok(())
    }
    walk(table, columns.fields(), &[], &required, &mut named)?;
    if let Some(path) = required.iter().find(|path| !named.contains(*path)) {
        return fail(format!(
            "NOT_NULL/PRIMARY_KEY record names column {}, which is not an ordinary column path",
            path.join(".")
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn every_kind_round_trips() {
        let mut check = ConstraintRecord::new(&["main"], "t", "t.check", ConstraintKind::Check);
        check.check_sql = Some("a >= 0".into());
        check.declared_name = Some("a_positive".into());
        let mut fk = ConstraintRecord::new(&["main"], "t", "t.fk", ConstraintKind::ForeignKey)
            .with_columns(&[&["b"]])
            .enforced();
        fk.references = Some(ForeignKeyReference {
            schema_path: vec!["main".into()],
            table: "u".into(),
            columns: vec![vec!["id".into()]],
            constraint_id: "u.pk".into(),
        });
        let unique = ConstraintRecord::new(&["main"], "t", "t.uq", ConstraintKind::Unique)
            .with_columns(&[&["a"], &["s", "x"]]);
        for spec in [
            pk("t", "t.pk", &["a"]),
            not_null("t", "t.nn", &["s", "x"]),
            unique,
            check,
            fk,
        ] {
            let bytes = constraint_record(&spec).unwrap();
            assert_eq!(read_constraint_record(&bytes.0).unwrap(), spec);
            assert_eq!(spec.encode().unwrap().0, bytes.0);
        }
    }

    #[test]
    fn builder_rejects_malformed_shapes() {
        let base = || ConstraintRecord::new(&["main"], "t", "id", ConstraintKind::NotNull);
        // No columns.
        assert!(constraint_record(&base()).is_err());
        // NOT_NULL with two paths.
        assert!(constraint_record(&base().with_columns(&[&["a"], &["b"]])).is_err());
        // Empty identity.
        let mut empty = base().with_columns(&[&["a"]]);
        empty.id.clear();
        assert!(constraint_record(&empty).is_err());
        // CHECK with columns / without SQL.
        let check = ConstraintRecord::new(&["main"], "t", "c", ConstraintKind::Check);
        assert!(constraint_record(&check).is_err());
        let mut check_cols = check.clone().with_columns(&[&["a"]]);
        check_cols.check_sql = Some("a > 0".into());
        assert!(constraint_record(&check_cols).is_err());
        // FK without references / arity mismatch.
        let fk = ConstraintRecord::new(&["main"], "t", "f", ConstraintKind::ForeignKey)
            .with_columns(&[&["a"]]);
        assert!(constraint_record(&fk).is_err());
        let mut arity = fk;
        arity.references = Some(ForeignKeyReference {
            schema_path: vec!["main".into()],
            table: "u".into(),
            columns: vec![vec!["x".into()], vec!["y".into()]],
            constraint_id: "u.pk".into(),
        });
        assert!(constraint_record(&arity).is_err());
    }

    fn records(specs: &[ConstraintRecord]) -> Vec<Bytes> {
        specs
            .iter()
            .map(|s| constraint_record(s).unwrap())
            .collect()
    }

    #[test]
    fn only_enforced_all_rows_records_are_served() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        assert!(
            validate_table_constraints("t", &schema, &records(&[pk("t", "k", &["a"])])).is_ok()
        );
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
            let err = validate_table_constraints("t", &schema, &records(&[spec]))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("Table 't'") && err.contains("constraint 'k'"),
                "{err}"
            );
            assert!(err.contains("ENFORCED + ALL_ROWS"), "{err}");
        }
    }

    #[test]
    fn records_must_belong_to_the_table_and_be_unique() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        assert!(
            validate_table_constraints("t", &schema, &records(&[pk("other", "k", &["a"])]))
                .is_err()
        );
        let dup = records(&[pk("t", "k", &["a"]), not_null("t", "k", &["a"])]);
        assert!(validate_table_constraints("t", &schema, &dup).is_err());
        assert!(validate_table_constraints("t", &schema, &[Bytes::from(vec![1, 2])]).is_err());
    }

    #[test]
    fn nullability_must_match_records_on_every_table() {
        let nullable = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let required = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        // A table with no records: every field must be nullable.
        assert!(validate_table_constraints("t", &nullable, &[]).is_ok());
        let err = validate_table_constraints("t", &required, &[])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Table 't'") && err.contains("column a is non-nullable"),
            "{err}"
        );
        // A NOT_NULL record over a nullable field.
        let err =
            validate_table_constraints("t", &nullable, &records(&[not_null("t", "n", &["a"])]))
                .unwrap_err()
                .to_string();
        assert!(
            err.contains("column a is in a NOT_NULL/PRIMARY_KEY record"),
            "{err}"
        );
        // The row-id column is exempt.
        let row_id = Schema::new(vec![Field::new("rowid", DataType::Int64, false)
            .with_metadata([(ROW_ID_METADATA_KEY.to_string(), "true".to_string())])]);
        assert!(validate_table_constraints("t", &row_id, &[]).is_ok());
        // A record naming a column that does not exist.
        let err =
            validate_table_constraints("t", &nullable, &records(&[not_null("t", "n", &["zz"])]))
                .unwrap_err()
                .to_string();
        assert!(err.contains("column zz"), "{err}");
    }

    #[test]
    fn nullability_recurses_into_ordinary_structs_only() {
        let inner = |nullable| {
            Fields::from(vec![
                Field::new("x", DataType::Float64, nullable),
                Field::new("y", DataType::Float64, true),
            ])
        };
        let schema = |nullable, metadata: Option<(&str, &str)>| {
            let mut field = Field::new("s", DataType::Struct(inner(nullable)), true);
            if let Some((k, v)) = metadata {
                field = field.with_metadata([(k.to_string(), v.to_string())]);
            }
            Schema::new(vec![field])
        };
        let nested = records(&[not_null("t", "sx", &["s", "x"])]);
        assert!(validate_table_constraints("t", &schema(false, None), &nested).is_ok());
        let err = validate_table_constraints("t", &schema(true, None), &nested)
            .unwrap_err()
            .to_string();
        assert!(err.contains("column s.x is in a NOT_NULL"), "{err}");
        let err = validate_table_constraints("t", &schema(false, None), &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("column s.x is non-nullable"), "{err}");
        // Extension / logical-type structs are opaque: their children are not walked.
        for key in ["ARROW:extension:name", "vgi.logical_type.name"] {
            let opaque = schema(false, Some((key, "geo")));
            assert!(validate_table_constraints("t", &opaque, &[]).is_ok());
            assert!(validate_table_constraints("t", &opaque, &nested).is_err());
        }
    }

    fn offer(read: &[&str]) -> Bytes {
        encode(crate::protocol::dtos::ClientCapabilities {
            engine: "test".into(),
            native_formats: vec![],
            catalogs: vec![],
            can_stream: true,
            filter_encodings: vec![],
            query_pushdown: None,
            catalog: Some(
                encode(CatalogCapabilities {
                    read: read.iter().map(|s| (*s).to_string()).collect(),
                    ddl: vec![],
                    write: vec![],
                    transaction: vec![],
                })
                .unwrap(),
            ),
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
        let (available, selected) =
            agree_capabilities(&read_profiles_for(true), None, Some(&client.0))
                .unwrap()
                .unwrap();
        let available: CatalogCapabilities = decode(&available.0).unwrap();
        let selected: CatalogCapabilities = decode(&selected.0).unwrap();
        assert_eq!(available.read, vec![r, s]);
        assert_eq!(selected.read, vec![r, s]);
        // Records plus a hook selecting an extra profile: union, deduplicated.
        let caps = |read: &[&str]| {
            encode(CatalogCapabilities {
                read: read.iter().map(|s| (*s).to_string()).collect(),
                ddl: vec![],
                write: vec![],
                transaction: vec![],
            })
            .unwrap()
        };
        let hook = (
            caps(&[r, "vgi.index.read.v1"]),
            caps(&[r, "vgi.index.read.v1"]),
        );
        let (available, selected) =
            agree_capabilities(&read_profiles_for(true), Some(hook), Some(&client.0))
                .unwrap()
                .unwrap();
        let available: CatalogCapabilities = decode(&available.0).unwrap();
        let selected: CatalogCapabilities = decode(&selected.0).unwrap();
        assert_eq!(available.read, vec![r, s, "vgi.index.read.v1"]);
        assert_eq!(selected.read, vec![r, s, "vgi.index.read.v1"]);
        // No offer: advertised, nothing selected.
        let (_, selected) = agree_capabilities(&read_profiles_for(true), None, None)
            .unwrap()
            .unwrap();
        let selected: CatalogCapabilities = decode(&selected.0).unwrap();
        assert!(selected.read.is_empty());
    }
}

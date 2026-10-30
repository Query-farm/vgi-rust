// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The canonical constraint-record codec and catalog-capability profiles,
//! shared by workers and clients.
//!
//! Under `vgi.v3` a table's constraints travel as canonical `ConstraintInfo`
//! IPC records in `TableInfo.constraints`. [`constraint_record`] encodes one
//! from authoring vocabulary ([`ConstraintRecord`]) and
//! [`read_constraint_record`] decodes it, so a client can read the records a
//! worker serves without depending on the worker framework. The worker side
//! (`vgi::constraints`) re-exports all of this and adds the serving checks.
//!
//! A client that understands the records offers [`CONSTRAINT_RECORDS_READ`]
//! (and [`ROW_SCALAR_EXPRESSION`] for CHECK) in
//! `ClientCapabilities.catalog`; [`client_catalog_capabilities`] builds the
//! offer.
//!
//! ```
//! use vgi_protocol::constraints::{
//!     constraint_record, read_constraint_record, ConstraintKind, ConstraintRecord,
//! };
//!
//! let spec = ConstraintRecord::new(&["main"], "users", "users.pk", ConstraintKind::PrimaryKey)
//!     .with_columns(&[&["id"]])
//!     .enforced();
//! let bytes = constraint_record(&spec).unwrap();
//! assert_eq!(read_constraint_record(&bytes.0).unwrap(), spec);
//! ```

use std::fmt;

use vgi_rpc::{Bytes, DictString, Result, RpcError, VgiArrow};

use crate::{ipc, wire};

/// The generated wire records, for authors who need the raw shapes.
pub use crate::generated::protocol_types::{
    CatalogCapabilities, CatalogObjectRef, ConstraintDefinition, ConstraintInfo,
    ForeignKeyDefinition, QueryEvaluationContext, SqlExpression,
};

/// Read profile: the catalog serves canonical `ConstraintInfo` records.
pub const CONSTRAINT_RECORDS_READ: &str = "vgi.constraint.records.read.v1";
/// Read profile: CHECK constraints (and column expressions) carry DuckDB scalar
/// SQL expressions. Selected only alongside [`CONSTRAINT_RECORDS_READ`].
pub const ROW_SCALAR_EXPRESSION: &str = "vgi.expression.duckdb.scalar.v1";

/// The only evaluation context the DuckDB extension reads for a CHECK or
/// column expression: UTC, binary collation, ascending, NULLs last, insertion
/// order preserved.
pub fn default_evaluation_context() -> QueryEvaluationContext {
    QueryEvaluationContext {
        timezone: "UTC".into(),
        collation: "binary".into(),
        default_order: "asc".into(),
        default_null_order: "nulls_last".into(),
        preserve_insertion_order: true,
    }
}

/// Whether `context` is [`default_evaluation_context`].
pub fn is_default_evaluation_context(context: &QueryEvaluationContext) -> bool {
    context.timezone == "UTC"
        && context.collation == "binary"
        && context.default_order == "asc"
        && context.default_null_order == "nulls_last"
        && context.preserve_insertion_order
}

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

/// Encode one wire record as single-batch IPC.
pub fn encode_record<T: VgiArrow>(value: T) -> Result<Bytes> {
    Ok(Bytes::from(ipc::write_batch(&wire::to_batch(value)?)?))
}

/// Decode one single-batch IPC wire record.
pub fn decode_record<T: VgiArrow>(bytes: &[u8]) -> Result<T> {
    wire::from_batch(&ipc::read_batch(bytes)?)
}

fn object_ref(
    kind: &str,
    schema_path: &[String],
    name: &str,
    owner: Option<&str>,
) -> Result<Bytes> {
    encode_record(CatalogObjectRef {
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
/// `TableInfo.constraints` (`vgi::catalog::CatTable::constraints` on a worker).
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
            encode_record(SqlExpression {
                sql: sql.clone(),
                profile: ROW_SCALAR_EXPRESSION.to_string(),
                schema_path: spec.schema_path.clone(),
                evaluation_context: default_evaluation_context(),
            })
        })
        .transpose()?;
    let foreign_key = spec
        .references
        .as_ref()
        .map(|target| {
            encode_record(ForeignKeyDefinition {
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
    encode_record(ConstraintInfo {
        r#ref: object_ref("constraint", &spec.schema_path, &spec.id, Some(&spec.table))?,
        definition: encode_record(ConstraintDefinition {
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
    let info: ConstraintInfo = decode_record(bytes)?;
    let identity: CatalogObjectRef = decode_record(&info.r#ref.0)?;
    if identity.kind != "constraint" {
        return Err(RpcError::value_error(format!(
            "constraint record identity has kind '{}', expected 'constraint'",
            identity.kind
        )));
    }
    let definition: ConstraintDefinition = decode_record(&info.definition.0)?;
    let check_sql = definition
        .check_expression
        .as_ref()
        .map(|raw| decode_record::<SqlExpression>(&raw.0).map(|e| e.sql))
        .transpose()?;
    let references = match &definition.foreign_key {
        None => None,
        Some(raw) => {
            let fk: ForeignKeyDefinition = decode_record(&raw.0)?;
            let target: CatalogObjectRef = decode_record(&fk.referenced_table.0)?;
            let key = info
                .referenced_constraint
                .as_ref()
                .map(|raw| decode_record::<CatalogObjectRef>(&raw.0))
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
    let client: crate::protocol::dtos::ClientCapabilities = decode_record(raw)?;
    let offered = match client.catalog {
        Some(catalog) if !catalog.0.is_empty() => {
            decode_record::<CatalogCapabilities>(&catalog.0)?.read
        }
        _ => Vec::new(),
    };
    let selected: Vec<String> = available
        .iter()
        .filter(|p| offered.contains(p))
        .cloned()
        .collect();
    Ok(normalize_read_profiles(selected))
}

/// Sort and deduplicate read profiles, dropping [`ROW_SCALAR_EXPRESSION`]
/// unless [`CONSTRAINT_RECORDS_READ`] is also present.
pub fn normalize_read_profiles(mut read: Vec<String>) -> Vec<String> {
    read.sort();
    read.dedup();
    if !read.iter().any(|p| p == CONSTRAINT_RECORDS_READ) {
        read.retain(|p| p != ROW_SCALAR_EXPRESSION);
    }
    read
}

/// Encode a `CatalogCapabilities` record (each category sorted and
/// deduplicated) — the bytes of `ClientCapabilities.catalog` and of
/// `CatalogAttachResult.catalog_capabilities` / `selected_capabilities`.
pub fn catalog_capabilities(
    read: &[&str],
    ddl: &[&str],
    write: &[&str],
    transaction: &[&str],
) -> Result<Bytes> {
    let list = |values: &[&str]| {
        let mut out: Vec<String> = values.iter().map(|s| (*s).to_string()).collect();
        out.sort();
        out.dedup();
        out
    };
    encode_record(CatalogCapabilities {
        read: list(read),
        ddl: list(ddl),
        write: list(write),
        transaction: list(transaction),
    })
}

/// Decode a `CatalogCapabilities` record; empty bytes decode as no profiles.
pub fn read_catalog_capabilities(bytes: &[u8]) -> Result<CatalogCapabilities> {
    if bytes.is_empty() {
        return Ok(CatalogCapabilities {
            read: Vec::new(),
            ddl: Vec::new(),
            write: Vec::new(),
            transaction: Vec::new(),
        });
    }
    decode_record(bytes)
}

/// The catalog profiles a client that decodes constraint records offers:
/// [`CONSTRAINT_RECORDS_READ`] and [`ROW_SCALAR_EXPRESSION`] reads, nothing
/// else. Encoded, it is `ClientCapabilities.catalog`.
pub fn client_catalog_capabilities() -> CatalogCapabilities {
    CatalogCapabilities {
        read: vec![
            CONSTRAINT_RECORDS_READ.to_string(),
            ROW_SCALAR_EXPRESSION.to_string(),
        ],
        ddl: Vec::new(),
        write: Vec::new(),
        transaction: Vec::new(),
    }
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

    #[test]
    fn client_offer_round_trips() {
        let offer = client_catalog_capabilities();
        let bytes = encode_record(offer).unwrap();
        let back = read_catalog_capabilities(&bytes.0).unwrap();
        assert_eq!(
            back.read,
            vec![CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION]
        );
        assert!(read_catalog_capabilities(&[]).unwrap().read.is_empty());
        assert_eq!(
            normalize_read_profiles(vec![ROW_SCALAR_EXPRESSION.into()]),
            Vec::<String>::new()
        );
    }
}

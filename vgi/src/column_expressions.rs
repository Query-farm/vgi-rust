// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//! Canonical VIRTUAL generated-column carriers.
//!
//! A generated column is a field carrying [`GENERATED`] (canonical base64
//! `SqlExpression` IPC, see [`generated_metadata`]) and [`GENERATION`]
//! (`virtual`). Serving checks the rules the DuckDB extension applies at
//! ATTACH (`ReadNativeColumnGenerated` / `ValidateNativeGeneratedColumns`):
//! VIRTUAL only; nullable; BOOLEAN / signed integer / VARCHAR (`Utf8`) type, not
//! an extension type; no default; the default evaluation context; and the
//! bounded expression grammar (literals, unqualified column references of those
//! types, `CAST`, comparisons, `AND`/`OR`/`NOT`, `IS [NOT] NULL`, `COALESCE`,
//! `+ - * % || abs lower upper`). It does not bind SQL or infer its result
//! type, which DuckDB still checks.
//!
//! The legacy `generated_expression` key (raw SQL) is translated to the
//! canonical pair when serving a catalog table
//! ([`translate_legacy_generated`]).
use arrow_array::Array;
use arrow_schema::{DataType, Field, Schema};
use base64::Engine;
use std::io::Cursor;
use vgi_protocol::generated::protocol_types::{QueryEvaluationContext, SqlExpression};
use vgi_rpc::{Result, RpcError};

pub const GENERATED: &str = "vgi.column.generated.v1";
pub const GENERATION: &str = "vgi.column.generation.v1";
const PROFILE: &str = "vgi.expression.duckdb.scalar.v1";
const MAXIMUM: usize = 8 * 1024 * 1024;
fn invalid(message: &str) -> RpcError {
    RpcError::value_error(message)
}
fn text(value: &str, maximum: usize) -> Result<()> {
    if value.is_empty() || value.contains('\0') || value.len() > maximum {
        return Err(invalid("invalid expression text"));
    }
    Ok(())
}
fn expression(value: &SqlExpression, owner: &[String]) -> Result<()> {
    text(&value.sql, 1024 * 1024)?;
    if value.sql.trim().is_empty() || value.profile != PROFILE {
        return Err(invalid("unsupported generated profile or empty SQL"));
    }
    if value.schema_path.len() > 32 {
        return Err(invalid("invalid expression owner"));
    }
    for part in &value.schema_path {
        text(part, 4096)?;
    }
    if value.schema_path != owner {
        return Err(invalid(
            "generated expression owner differs from table schema",
        ));
    }
    let context = &value.evaluation_context;
    text(&context.timezone, 256)?;
    text(&context.collation, 256)?;
    if !["asc", "desc"].contains(&context.default_order.as_str())
        || ![
            "nulls_first",
            "nulls_last",
            "nulls_first_on_asc_last_on_desc",
            "nulls_last_on_asc_first_on_desc",
        ]
        .contains(&context.default_null_order.as_str())
    {
        return Err(invalid("invalid expression evaluation context"));
    }
    Ok(())
}
pub(crate) fn encoded(value: &str, owner: &[String]) -> Result<SqlExpression> {
    if value.is_empty() || value.len() > 4 * MAXIMUM.div_ceil(3) {
        return Err(invalid("invalid expression size"));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value)
        .map_err(|_| invalid("invalid expression base64"))?;
    if bytes.is_empty()
        || bytes.len() > MAXIMUM
        || base64::engine::general_purpose::STANDARD.encode(&bytes) != value
    {
        return Err(invalid("expression requires canonical base64 IPC"));
    }
    let mut cursor = Cursor::new(bytes.as_slice());
    let row = {
        let mut reader = arrow_ipc::reader::StreamReader::try_new(&mut cursor, None)
            .map_err(|e| invalid(&e.to_string()))?;
        if reader.schema().fields() != crate::wire::flat_schema::<SqlExpression>().fields() {
            return Err(invalid("invalid SqlExpression record schema"));
        }
        let row = reader
            .next()
            .ok_or_else(|| invalid("missing expression row"))?
            .map_err(|e| invalid(&e.to_string()))?;
        if row.num_rows() != 1 || row.columns().iter().any(|a| a.null_count() != 0) {
            return Err(invalid("expression requires one nonnull row"));
        }
        for column in row.columns() {
            column
                .to_data()
                .validate_full()
                .map_err(|e| invalid(&e.to_string()))?;
        }
        if reader.next().is_some() {
            return Err(invalid("expression requires exactly one IPC batch"));
        }
        row
    };
    if cursor.position() != bytes.len() as u64 {
        return Err(invalid("trailing expression IPC"));
    }
    let value = crate::wire::from_batch::<SqlExpression>(&row)?;
    expression(&value, owner)?;
    Ok(value)
}
/// Translate the convenience SQL value using an explicit owner and default context.
pub fn generated_metadata(sql: &str, owner: &[String]) -> Result<String> {
    let value = SqlExpression {
        sql: sql.into(),
        profile: PROFILE.into(),
        schema_path: owner.into(),
        evaluation_context: QueryEvaluationContext {
            timezone: "UTC".into(),
            collation: "binary".into(),
            default_order: "asc".into(),
            default_null_order: "nulls_last".into(),
            preserve_insertion_order: true,
        },
    };
    expression(&value, owner)?;
    Ok(base64::engine::general_purpose::STANDARD
        .encode(crate::ipc::write_batch(&crate::wire::to_batch(value)?)?))
}
/// The legacy field-metadata key carrying a generated column's raw SQL.
pub const LEGACY_GENERATED: &str = "generated_expression";

/// Translate the legacy `generated_expression` key (raw DuckDB SQL) on each
/// top-level field to the canonical [`GENERATED`] / [`GENERATION`] (`virtual`)
/// pair owned by `owner`. Unambiguous only when the field carries neither
/// canonical key; a field mixing both is an error. Other fields are unchanged.
pub fn translate_legacy_generated(schema: &Schema, owner: &[String]) -> Result<Schema> {
    if !schema
        .fields()
        .iter()
        .any(|f| f.metadata().contains_key(LEGACY_GENERATED))
    {
        return Ok(schema.clone());
    }
    let fields = schema
        .fields()
        .iter()
        .map(|f| -> Result<Field> {
            let mut metadata = f.metadata().clone();
            let Some(sql) = metadata.remove(LEGACY_GENERATED) else {
                return Ok(f.as_ref().clone());
            };
            if metadata.contains_key(GENERATED) || metadata.contains_key(GENERATION) {
                return Err(invalid(&format!(
                    "column '{}' carries both legacy {LEGACY_GENERATED} and canonical \
                     {GENERATED}/{GENERATION} metadata; keep only the canonical pair",
                    f.name()
                )));
            }
            metadata.insert(GENERATED.to_string(), generated_metadata(&sql, owner)?);
            metadata.insert(GENERATION.to_string(), "virtual".to_string());
            Ok(f.as_ref().clone().with_metadata(metadata))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// The extension's top-level generated-column rules for `f`, whose canonical
/// carrier `value` already decoded: nullable, a supported type, the default
/// evaluation context, and the bounded grammar over `schema`'s columns.
fn generated_column(f: &Field, value: &str, schema: &Schema, owner: &[String]) -> Result<()> {
    let name = f.name();
    if !f.is_nullable() {
        return Err(invalid(&format!(
            "generated column '{name}' must be nullable (DuckDB reads only nullable VIRTUAL \
             columns; drop the NOT_NULL/PRIMARY_KEY record naming it)"
        )));
    }
    if f.metadata().contains_key("ARROW:extension:name")
        || !crate::sql_expression::generated_type(f.data_type())
    {
        return Err(invalid(&format!(
            "generated column '{name}' has type {}; DuckDB generated columns must be Boolean, \
             Int8..Int64 or Utf8",
            f.data_type()
        )));
    }
    let expression = encoded(value, owner)?;
    if !vgi_protocol::constraints::is_default_evaluation_context(&expression.evaluation_context) {
        return Err(invalid(&format!(
            "generated column '{name}': DuckDB reads only the default evaluation context \
             (UTC / binary / asc / nulls_last / preserve_insertion_order)"
        )));
    }
    if owner.len() != 1 {
        return Err(invalid(&format!(
            "generated column '{name}': DuckDB reads generated columns only in one-component \
             schemas"
        )));
    }
    crate::sql_expression::check_generated(&expression.sql, schema)
        .map_err(|e| invalid(&format!("generated column '{name}': {}", e.message)))
}

pub fn validate(schema: &Schema, owner: &[String], allow_generated: bool) -> Result<()> {
    fn field(f: &Field, owner: &[String], allow_generated: bool) -> Result<()> {
        let values = f.metadata();
        for key in values.keys() {
            if (key.starts_with("vgi.column.generated.") && key != GENERATED)
                || (key.starts_with("vgi.column.generation.") && key != GENERATION)
            {
                return Err(invalid("unknown generated metadata version"));
            }
        }
        if values.contains_key(LEGACY_GENERATED) {
            return Err(invalid(
                "legacy generated_expression metadata is translated only on top-level catalog \
                 table columns; use vgi::column_expressions::generated_metadata",
            ));
        }
        if values.contains_key(GENERATED) != values.contains_key(GENERATION) {
            return Err(invalid(
                "generated expression and category must occur together",
            ));
        }
        if let Some(value) = values.get(GENERATED) {
            if values[GENERATION] != "virtual" {
                return Err(invalid("only VIRTUAL generated columns are supported"));
            }
            if values.contains_key("default") || values.contains_key("vgi.column.default.v1") {
                return Err(invalid("generated column cannot have a default"));
            }
            encoded(value, owner)?;
            if !allow_generated {
                return Err(RpcError::method_not_implemented(
                    "generated columns unsupported by this catalog",
                ));
            }
        }
        data_type(f.data_type(), owner, allow_generated)
    }
    fn data_type(t: &DataType, owner: &[String], allow: bool) -> Result<()> {
        match t {
            DataType::Struct(fields) => {
                for f in fields {
                    field(f, owner, allow)?;
                }
            }
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => field(f, owner, allow)?,
            DataType::Dictionary(_, value) => data_type(value, owner, allow)?,
            DataType::Union(fields, _) => {
                for (_, f) in fields.iter() {
                    field(f, owner, allow)?;
                }
            }
            DataType::RunEndEncoded(a, b) => {
                field(a, owner, allow)?;
                field(b, owner, allow)?;
            }
            _ => {}
        }
        Ok(())
    }
    let mut physical = false;
    let mut generated = false;
    for f in schema.fields() {
        field(f, owner, allow_generated)?;
        match f.metadata().get(GENERATED) {
            Some(value) => {
                generated_column(f, value, schema, owner)?;
                generated = true;
            }
            None => physical |= !f.metadata().contains_key("is_row_id"),
        }
    }
    if generated && !physical {
        return Err(invalid(
            "a table with generated columns needs at least one physical column",
        ));
    }
    Ok(())
}

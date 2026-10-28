// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//! Canonical VIRTUAL carriers; validation does not evaluate SQL.
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
pub(crate) fn encoded(value: &str, owner: &[String]) -> Result<()> {
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
    expression(&crate::wire::from_batch::<SqlExpression>(&row)?, owner)
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
        if values.contains_key("generated_expression") {
            return Err(invalid(
                "legacy generated expression requires explicit canonical translation",
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
    for f in schema.fields() {
        field(f, owner, allow_generated)?;
    }
    Ok(())
}

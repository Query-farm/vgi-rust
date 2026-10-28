// Copyright 2025, 2026 Query Farm LLC - https://query.farm
use arrow_schema::{DataType, Field, Schema};
use serde::de::{self, MapAccess, Visitor};
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt;

struct StringTags;
impl<'de> Deserialize<'de> for StringTags {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct TagsVisitor;
        impl<'de> Visitor<'de> for TagsVisitor {
            type Value = StringTags;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON object of unique string keys and string values")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<StringTags, A::Error> {
                let mut seen = HashSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !seen.insert(key) {
                        return Err(de::Error::custom("duplicate column tag key"));
                    }
                    let _: String = map.next_value()?;
                }
                Ok(StringTags)
            }
        }
        d.deserialize_map(TagsVisitor)
    }
}
/// Validate reserved field metadata without changing any Arrow schema or value.
pub fn validate_column_metadata(schema: &Schema) -> crate::Result<()> {
    for field in schema.fields() {
        validate_field(field)?;
    }
    Ok(())
}
fn validate_field(field: &Field) -> crate::Result<()> {
    // Arrow Rust metadata is String: its IPC reader rejects invalid UTF-8.
    if let Some(tags) = field.metadata().get("tags") {
        serde_json::from_str::<StringTags>(tags).map_err(|e| {
            vgi_rpc::RpcError::value_error(format!("column {} tags: {e}", field.name()))
        })?;
    }
    validate_type(field.data_type())
}
fn validate_type(t: &DataType) -> crate::Result<()> {
    match t {
        DataType::Struct(fields) => {
            for f in fields {
                validate_field(f)?;
            }
        }
        DataType::List(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::Map(f, _) => validate_field(f)?,
        DataType::Dictionary(_, value) => validate_type(value)?,
        DataType::Union(fields, _) => {
            for (_, f) in fields.iter() {
                validate_field(f)?;
            }
        }
        DataType::RunEndEncoded(run_ends, values) => {
            validate_field(run_ends)?;
            validate_field(values)?;
        }
        _ => {}
    }
    Ok(())
}

/// Validate before converting Arrow FlatBuffer metadata vectors to Rust maps.
pub fn validate_column_metadata_ipc(bytes: &[u8]) -> crate::Result<()> {
    let invalid = |message: String| vgi_rpc::RpcError::value_error(message);
    let prefix = if bytes.starts_with(&[255, 255, 255, 255]) {
        8
    } else {
        4
    };
    if bytes.len() < prefix {
        return Err(invalid("invalid column schema IPC".into()));
    }
    let size = i32::from_le_bytes(bytes[prefix - 4..prefix].try_into().unwrap());
    if size <= 0 || size as usize > bytes.len() - prefix {
        return Err(invalid("invalid column schema IPC length".into()));
    }
    let message = arrow_ipc::root_as_message(&bytes[prefix..prefix + size as usize])
        .map_err(|e| invalid(format!("invalid column schema IPC: {e}")))?;
    let schema = message
        .header_as_schema()
        .ok_or_else(|| invalid("expected column schema message".into()))?;
    fn field(f: arrow_ipc::Field<'_>, depth: usize) -> crate::Result<()> {
        if depth > 64 {
            return Err(vgi_rpc::RpcError::value_error(
                "column metadata nesting too deep",
            ));
        }
        let mut seen = HashSet::new();
        if let Some(metadata) = f.custom_metadata() {
            for m in metadata {
                let key = m.key().unwrap_or("");
                if !seen.insert(key) {
                    return Err(vgi_rpc::RpcError::value_error(
                        "duplicate column metadata key",
                    ));
                }
                if key == "tags" {
                    serde_json::from_str::<StringTags>(m.value().unwrap_or("")).map_err(|e| {
                        vgi_rpc::RpcError::value_error(format!("invalid column tags: {e}"))
                    })?;
                }
            }
        }
        if let Some(children) = f.children() {
            for child in children {
                field(child, depth + 1)?;
            }
        }
        Ok(())
    }
    if let Some(fields) = schema.fields() {
        for f in fields {
            field(f, 0)?;
        }
    }
    Ok(())
}

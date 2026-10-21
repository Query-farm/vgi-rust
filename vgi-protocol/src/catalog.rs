// Copyright 2026 Query Farm LLC - https://query.farm

//! Collection codecs for the six catalog-wide metadata streams (wire 2.2).
//! These helpers also adapt the legacy schema-grouped author API. They never
//! concatenate IPC byte strings, and consume every row of every input batch.

use crate::protocol::dtos::*;
use arrow_array::{Array, ListArray, RecordBatch, StructArray};
use arrow_ipc::writer::StreamWriter;
use std::collections::HashMap;
use std::io::Cursor;
use vgi_rpc::wire::StreamReader;
use vgi_rpc::{Bytes, Result, RpcError, VgiArrow};

/// Encode ordinary records as one stream, with bounded record batches.
pub fn encode_collection<T: VgiArrow>(values: Vec<T>) -> Result<Option<Bytes>> {
    encode_collection_with_batch_size(values, 1024)
}

/// Like [`encode_collection`], with an explicit positive batch row limit.
pub fn encode_collection_with_batch_size<T: VgiArrow>(
    values: Vec<T>,
    batch_size: usize,
) -> Result<Option<Bytes>> {
    if batch_size == 0 {
        return Err(RpcError::value_error("catalog batch size must be positive"));
    }
    if values.is_empty() {
        return Ok(None);
    }
    let schema = crate::wire::flat_schema::<T>();
    let mut output = Vec::new();
    let mut writer = StreamWriter::try_new(&mut output, &schema).map_err(RpcError::from)?;
    let mut values = values.into_iter().peekable();
    while values.peek().is_some() {
        // Vec's VgiArrow implementation builds arrays using the same field
        // conversions as unary records and unifies dictionaries on concat.
        let array = Vec::<T>::build_singleton(values.by_ref().take(batch_size).collect())?;
        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        let records = list
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        writer
            .write(&RecordBatch::from(records))
            .map_err(RpcError::from)?;
    }
    writer.finish().map_err(RpcError::from)?;
    drop(writer);
    Ok(Some(Bytes::from(output)))
}

/// Decode all rows and batches, rejecting a stream with another metadata schema.
pub fn decode_collection<T: VgiArrow>(bytes: Option<&Bytes>) -> Result<Vec<T>> {
    let Some(bytes) = bytes else {
        return Ok(Vec::new());
    };
    let declared = crate::ipc::read_schema(&bytes.0)?;
    let expected = crate::wire::flat_schema::<T>();
    if declared.fields().len() != expected.fields().len()
        || declared
            .fields()
            .iter()
            .zip(expected.fields())
            .any(|(a, b)| a.name() != b.name() || !a.data_type().equals_datatype(b.data_type()))
    {
        return Err(RpcError::type_error(format!(
            "catalog collection schema does not match {}",
            T::describe_name()
        )));
    }
    let mut cursor = Cursor::new(&bytes.0);
    let mut reader = StreamReader::new(&mut cursor)?.relax_nullability();
    let mut records = Vec::new();
    while let Some((batch, metadata)) = reader.read_next()? {
        if metadata.keys().any(|key| {
            key.starts_with("vgi_rpc.location")
                || key.starts_with("vgi_rpc.log_")
                || key.starts_with("vgi_rpc.error_")
        }) {
            return Err(RpcError::value_error(
                "nested metadata cannot contain external pointers",
            ));
        }
        for (column, field) in batch.columns().iter().zip(expected.fields()) {
            for row in 0..batch.num_rows() {
                validate_value(column.as_ref(), field, row)?;
            }
        }
        let array = StructArray::from(batch);
        for row in 0..array.len() {
            records.push(T::read(&array, row)?);
        }
    }
    drop(reader);
    if cursor.position() != bytes.0.len() as u64 {
        return Err(RpcError::value_error("trailing catalog metadata bytes"));
    }
    Ok(records)
}

fn validate_value(array: &dyn Array, field: &arrow_schema::Field, row: usize) -> Result<()> {
    if array.is_null(row) {
        return if field.is_nullable() {
            Ok(())
        } else {
            Err(RpcError::value_error(format!(
                "required catalog field {} is null",
                field.name()
            )))
        };
    }
    match field.data_type() {
        arrow_schema::DataType::Struct(fields) => {
            let value = array.as_any().downcast_ref::<StructArray>().unwrap();
            for (column, field) in value.columns().iter().zip(fields) {
                validate_value(column.as_ref(), field, row)?;
            }
        }
        arrow_schema::DataType::List(field) => {
            let value = array.as_any().downcast_ref::<ListArray>().unwrap();
            let offsets = value.value_offsets();
            for index in offsets[row]..offsets[row + 1] {
                validate_value(value.values().as_ref(), field, index as usize)?;
            }
        }
        arrow_schema::DataType::Map(field, _) => {
            let value = array
                .as_any()
                .downcast_ref::<arrow_array::MapArray>()
                .unwrap();
            let offsets = value.value_offsets();
            for index in offsets[row]..offsets[row + 1] {
                validate_value(value.entries(), field, index as usize)?;
            }
        }
        arrow_schema::DataType::Dictionary(index_type, _) => {
            let (key, values): (usize, &dyn Array) = match index_type.as_ref() {
                arrow_schema::DataType::Int16 => {
                    let value = array.as_any().downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int16Type>>().ok_or_else(|| RpcError::type_error("expected int16 dictionary"))?;
                    (value.keys().value(row) as usize, value.values().as_ref())
                }
                arrow_schema::DataType::Int32 => {
                    let value = array.as_any().downcast_ref::<arrow_array::DictionaryArray<arrow_array::types::Int32Type>>().ok_or_else(|| RpcError::type_error("expected int32 dictionary"))?;
                    (value.keys().value(row) as usize, value.values().as_ref())
                }
                _ => return Err(RpcError::type_error("unsupported catalog dictionary index type")),
            };
            if key >= values.len() || (values.is_null(key) && !field.is_nullable()) {
                return Err(RpcError::value_error("invalid catalog dictionary value"));
            }
        }
        _ => {}
    }
    Ok(())
}

fn merge<T: VgiArrow>(items: impl Iterator<Item = Bytes>) -> Result<Option<Bytes>> {
    let mut records = Vec::new();
    for item in items {
        records.extend(decode_collection::<T>(Some(&item))?);
    }
    encode_collection(records)
}

/// Migrate schema-grouped metadata into the catalog-wide wire response.
pub fn from_schema_contents(
    catalog_version: i64,
    etag: Option<String>,
    schemas: &[SchemaContents],
) -> Result<CatalogContentsResponse> {
    Ok(CatalogContentsResponse {
        catalog_version,
        etag,
        not_modified: false,
        schemas: merge::<SchemaInfo>(schemas.iter().map(|s| s.schema.clone()))?,
        tables: merge::<TableInfo>(schemas.iter().flat_map(|s| s.tables.iter().cloned()))?,
        views: merge::<ViewInfo>(schemas.iter().flat_map(|s| s.views.iter().cloned()))?,
        functions: merge::<FunctionInfo>(schemas.iter().flat_map(|s| {
            s.scalar_functions
                .iter()
                .chain(&s.aggregate_functions)
                .chain(&s.table_functions)
                .cloned()
        }))?,
        macros: merge::<MacroInfo>(
            schemas
                .iter()
                .flat_map(|s| s.scalar_macros.iter().chain(&s.table_macros).cloned()),
        )?,
        indexes: merge::<IndexInfo>(schemas.iter().flat_map(|s| s.indexes.iter().cloned()))?,
    })
}

impl CatalogContentsResponse {
    /// A conditional response never contains a category stream.
    pub fn unchanged(catalog_version: i64, etag: String) -> Self {
        Self {
            catalog_version,
            etag: Some(etag),
            not_modified: true,
            schemas: None,
            tables: None,
            views: None,
            functions: None,
            macros: None,
            indexes: None,
        }
    }

    pub fn streams(&self) -> [Option<&Bytes>; 6] {
        [
            self.schemas.as_ref(),
            self.tables.as_ref(),
            self.views.as_ref(),
            self.functions.as_ref(),
            self.macros.as_ref(),
            self.indexes.as_ref(),
        ]
    }

    /// Adapter for existing callers that use schema-grouped item records.
    /// Every category is opened once, then rows are indexed by the raw path.
    pub fn schema_contents(&self) -> Result<Vec<SchemaContents>> {
        if self.not_modified {
            if self.streams().iter().any(|s| s.is_some()) || self.etag.is_none() {
                return Err(RpcError::type_error(
                    "not_modified requires an ETag and six null streams",
                ));
            }
            return Ok(Vec::new());
        }
        let mut schemas = Vec::new();
        let mut paths = HashMap::new();
        for info in decode_collection::<SchemaInfo>(self.schemas.as_ref())? {
            let path = info.path.clone();
            if path.is_empty() || path.iter().any(String::is_empty) || paths.contains_key(&path) {
                return Err(RpcError::type_error(
                    "catalog contains empty or duplicate schema path",
                ));
            }
            if path.len() > 1 && !paths.contains_key(&path[..path.len() - 1]) {
                return Err(RpcError::type_error(
                    "catalog schema parent missing or out of order",
                ));
            }
            paths.insert(path.clone(), schemas.len());
            schemas.push(SchemaContents {
                path,
                schema: item(info)?,
                tables: vec![],
                views: vec![],
                scalar_functions: vec![],
                aggregate_functions: vec![],
                table_functions: vec![],
                scalar_macros: vec![],
                table_macros: vec![],
                indexes: vec![],
            });
        }
        fn target<'a>(
            schemas: &'a mut [SchemaContents],
            paths: &HashMap<Vec<String>, usize>,
            path: &[String],
        ) -> Result<&'a mut SchemaContents> {
            let index = paths.get(path).ok_or_else(|| {
                RpcError::type_error(format!("catalog object refers to absent schema {path:?}"))
            })?;
            Ok(&mut schemas[*index])
        }
        for row in decode_collection::<TableInfo>(self.tables.as_ref())? {
            target(&mut schemas, &paths, &row.schema_path)?
                .tables
                .push(item(row)?);
        }
        for row in decode_collection::<ViewInfo>(self.views.as_ref())? {
            target(&mut schemas, &paths, &row.schema_path)?
                .views
                .push(item(row)?);
        }
        for row in decode_collection::<FunctionInfo>(self.functions.as_ref())? {
            let schema = target(&mut schemas, &paths, &row.schema_path)?;
            let kind = row.function_type.0.as_str();
            let items = match kind {
                "SCALAR" | "scalar" | "SCALAR_FUNCTION" => &mut schema.scalar_functions,
                "AGGREGATE" | "aggregate" | "AGGREGATE_FUNCTION" => &mut schema.aggregate_functions,
                "TABLE" | "TABLE_BUFFERING" | "table" | "table_buffering" | "table_in_out" | "TABLE_FUNCTION" => {
                    &mut schema.table_functions
                }
                _ => {
                    return Err(RpcError::type_error(format!(
                        "unsupported catalog function type {kind}"
                    )))
                }
            };
            items.push(item(row)?);
        }
        for row in decode_collection::<MacroInfo>(self.macros.as_ref())? {
            let schema = target(&mut schemas, &paths, &row.schema_path)?;
            let items = match row.macro_type.0.as_str() {
                "SCALAR" | "scalar" | "SCALAR_MACRO" => &mut schema.scalar_macros,
                "TABLE" | "table" | "TABLE_MACRO" => &mut schema.table_macros,
                other => {
                    return Err(RpcError::type_error(format!(
                        "unsupported catalog macro type {other}"
                    )))
                }
            };
            items.push(item(row)?);
        }
        for row in decode_collection::<IndexInfo>(self.indexes.as_ref())? {
            target(&mut schemas, &paths, &row.schema_path)?
                .indexes
                .push(item(row)?);
        }
        Ok(schemas)
    }
}

fn item<T: VgiArrow>(value: T) -> Result<Bytes> {
    Ok(Bytes::from(crate::ipc::write_batch(
        &crate::wire::to_batch(value)?,
    )?))
}

#[cfg(test)]
mod collection_tests {
    use super::*;
    #[test]
    fn dictionaries_round_trip_all_rows_and_batches_across_schemas() {
        let rows = (0..7)
            .map(|i| IndexInfo {
                comment: if i % 2 == 0 {
                    None
                } else {
                    Some("comment".into())
                },
                tags: vec![("tag".into(), "value".into())],
                name: format!("i{i}"),
                schema_path: vec![if i % 2 == 0 { "main" } else { "other" }.into()],
                table_name: "t".into(),
                index_type: "ART".into(),
                constraint_type: vgi_rpc::DictString(
                    if i % 2 == 0 { "PRIMARY" } else { "UNIQUE" }.into(),
                ),
                expressions: vec!["column".into()],
                options: vec![],
            })
            .collect::<Vec<_>>();
        let stream = encode_collection_with_batch_size(rows.clone(), 2).unwrap();
        let decoded = decode_collection::<IndexInfo>(stream.as_ref()).unwrap();
        assert_eq!(format!("{rows:?}"), format!("{decoded:?}"));
        assert!(encode_collection::<IndexInfo>(vec![]).unwrap().is_none());
        let mut trailing = stream.unwrap();
        trailing.0.push(1);
        assert!(decode_collection::<IndexInfo>(Some(&trailing)).is_err());
    }
    #[test]
    fn required_metadata_values_cannot_be_null() {
        let schema = crate::wire::flat_schema::<SchemaInfo>();
        let valid = crate::wire::to_batch(SchemaInfo {
            comment: None,
            tags: vec![],
            path: vec!["main".into()],
            estimated_object_count: None,
        })
        .unwrap();
        let mut fields = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>();
        fields[2] = fields[2].clone().with_nullable(true);
        let nullable = std::sync::Arc::new(arrow_schema::Schema::new(fields));
        let mut columns = valid.columns().to_vec();
        columns[2] = arrow_array::new_null_array(nullable.field(2).data_type(), 1);
        let batch = RecordBatch::try_new(nullable, columns).unwrap();
        let bytes = vgi_rpc::Bytes(crate::ipc::write_batch(&batch).unwrap());
        assert!(decode_collection::<SchemaInfo>(Some(&bytes)).is_err());
    }
}

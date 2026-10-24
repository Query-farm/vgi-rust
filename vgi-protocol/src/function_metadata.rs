// Copyright 2026 Query Farm LLC - https://query.farm
//! Function metadata keeps Bytes in the author API while dictionary-encoding
//! repeated binary payloads on the wire. No RPC dependency change is required.
use arrow_array::{types::Int32Type, Array, ArrayRef, BinaryArray, DictionaryArray};
use arrow_schema::DataType;
use std::sync::Arc;
use vgi_rpc::{Bytes, Result, RpcError};

pub(crate) fn binary_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Binary))
}
pub(crate) fn read_binary(array: &dyn Array, index: usize) -> Result<Bytes> {
    read_optional_binary(array, index)?
        .ok_or_else(|| RpcError::value_error("required binary metadata is null"))
}
pub(crate) fn read_optional_binary(array: &dyn Array, index: usize) -> Result<Option<Bytes>> {
    let dictionary = array
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| RpcError::type_error("expected dictionary<int32,binary>"))?;
    let Some(key) = dictionary.key(index) else {
        return Ok(None);
    };
    let values = dictionary
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| RpcError::type_error("expected binary dictionary values"))?;
    if key >= values.len() {
        return Err(RpcError::value_error("invalid binary dictionary index"));
    }
    Ok(if values.is_null(key) {
        None
    } else {
        Some(Bytes::from(values.value(key).to_vec()))
    })
}
pub(crate) fn build_binary(value: Option<Bytes>) -> Result<ArrayRef> {
    let mut builder = arrow_array::builder::BinaryDictionaryBuilder::<Int32Type>::new();
    match value {
        Some(bytes) => builder.append_value(bytes.0),
        None => builder.append_null(),
    }
    Ok(Arc::new(builder.finish()))
}

macro_rules! field_type {
    (arguments, $ty:ty) => {
        $crate::function_metadata::binary_type()
    };
    (output_schema, $ty:ty) => {
        $crate::function_metadata::binary_type()
    };
    (parameter_default_values, $ty:ty) => {
        $crate::function_metadata::binary_type()
    };
    ($field:ident, $ty:ty) => {
        <$ty as vgi_rpc::VgiArrow>::arrow_data_type()
    };
}
macro_rules! field_read {
    (arguments, $ty:ty, $arr:expr, $idx:expr) => {
        $crate::function_metadata::read_binary($arr, $idx)
    };
    (output_schema, $ty:ty, $arr:expr, $idx:expr) => {
        $crate::function_metadata::read_binary($arr, $idx)
    };
    (parameter_default_values, $ty:ty, $arr:expr, $idx:expr) => {
        $crate::function_metadata::read_optional_binary($arr, $idx)
    };
    ($field:ident, $ty:ty, $arr:expr, $idx:expr) => {
        <$ty as vgi_rpc::VgiArrow>::read($arr, $idx)
    };
}
macro_rules! field_build {
    (arguments, $ty:ty, $value:expr) => {
        $crate::function_metadata::build_binary(Some($value))
    };
    (output_schema, $ty:ty, $value:expr) => {
        $crate::function_metadata::build_binary(Some($value))
    };
    (parameter_default_values, $ty:ty, $value:expr) => {
        $crate::function_metadata::build_binary($value)
    };
    ($field:ident, $ty:ty, $value:expr) => {
        <$ty as vgi_rpc::VgiArrow>::build_singleton($value)
    };
}
macro_rules! function_info {
    ($(#[$attr:meta])* pub struct $name:ident { $($(#[$doc:meta])* pub $field:ident: $ty:ty,)* }) => {
        $(#[$attr])*
        #[derive(Debug, Clone)]
        pub struct $name { $($(#[$doc])* pub $field: $ty,)* }
        impl vgi_rpc::VgiArrow for $name {
            fn arrow_data_type() -> arrow_schema::DataType {
                arrow_schema::DataType::Struct(vec![$(arrow_schema::Field::new(stringify!($field), $crate::function_metadata::field_type!($field, $ty), <$ty as vgi_rpc::VgiArrow>::nullable())),*].into())
            }
            fn describe_name() -> String { stringify!($name).into() }
            fn read(array: &dyn arrow_array::Array, index: usize) -> vgi_rpc::Result<Self> {
                let structure = array.as_any().downcast_ref::<arrow_array::StructArray>()
                    .ok_or_else(|| vgi_rpc::RpcError::type_error("expected FunctionInfo struct"))?;
                Ok(Self { $($field: {
                    let column = structure.column_by_name(stringify!($field)).ok_or_else(|| vgi_rpc::RpcError::type_error(concat!("missing function field ", stringify!($field))))?;
                    $crate::function_metadata::field_read!($field, $ty, column.as_ref(), index)?
                }),* })
            }
            fn build_singleton(value: Self) -> vgi_rpc::Result<arrow_array::ArrayRef> {
                let columns = vec![$((std::sync::Arc::new(arrow_schema::Field::new(stringify!($field), $crate::function_metadata::field_type!($field, $ty), <$ty as vgi_rpc::VgiArrow>::nullable())), $crate::function_metadata::field_build!($field, $ty, value.$field)?)),*];
                Ok(std::sync::Arc::new(arrow_array::StructArray::from(columns)))
            }
        }
    };
}
pub(crate) use {field_build, field_read, field_type, function_info};

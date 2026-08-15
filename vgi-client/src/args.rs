// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Building the `arguments` blob a bind carries.
//!
//! The wire shape is one `args` **struct** column whose fields are named
//! `positional_0`, `positional_1`, … and `named_<name>`, each a one-row array.
//! Const (bind-time) arguments carry their value at row 0; a column argument
//! carries a null placeholder, because its real data arrives later in the
//! process input batch. The field *types* always describe the argument types.
//!
//! See the worker-side reader in `vgi::arguments` — this module is its inverse.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, RecordBatchOptions, StringArray,
    StructArray,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use vgi_protocol::ipc;
use vgi_rpc::errors::{Result, RpcError};
use vgi_rpc::Bytes;

/// One argument value.
///
/// [`ArgValue::Placeholder`] is how a *column* argument is expressed: the type
/// is stated but the value is null, because the data arrives per-row later.
#[derive(Debug, Clone, PartialEq)]
pub enum ArgValue {
    /// A 64-bit integer.
    Int(i64),
    /// A double.
    Float(f64),
    /// A UTF-8 string.
    Text(String),
    /// A boolean.
    Bool(bool),
    /// A typed null — a bind-time argument explicitly passed as NULL.
    Null(DataType),
    /// A typed placeholder for a column argument whose values arrive later.
    Placeholder(DataType),
}

impl ArgValue {
    fn data_type(&self) -> DataType {
        match self {
            Self::Int(_) => DataType::Int64,
            Self::Float(_) => DataType::Float64,
            Self::Text(_) => DataType::Utf8,
            Self::Bool(_) => DataType::Boolean,
            Self::Null(t) | Self::Placeholder(t) => t.clone(),
        }
    }

    fn to_array(&self) -> Result<ArrayRef> {
        Ok(match self {
            Self::Int(v) => Arc::new(Int64Array::from(vec![*v])) as ArrayRef,
            Self::Float(v) => Arc::new(Float64Array::from(vec![*v])),
            Self::Text(v) => Arc::new(StringArray::from(vec![v.clone()])),
            Self::Bool(v) => Arc::new(BooleanArray::from(vec![*v])),
            Self::Null(t) | Self::Placeholder(t) => arrow_array::new_null_array(t, 1),
        })
    }
}

impl From<i64> for ArgValue {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}
impl From<f64> for ArgValue {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}
impl From<bool> for ArgValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}
impl From<&str> for ArgValue {
    fn from(v: &str) -> Self {
        Self::Text(v.to_string())
    }
}
impl From<String> for ArgValue {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}

/// A call's arguments, positional and named.
#[derive(Debug, Clone, Default)]
pub struct Arguments {
    positional: Vec<ArgValue>,
    named: Vec<(String, ArgValue)>,
}

impl Arguments {
    /// No arguments.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a positional argument.
    #[must_use]
    pub fn positional(mut self, value: impl Into<ArgValue>) -> Self {
        self.positional.push(value.into());
        self
    }

    /// Set a named argument. A repeated name replaces the earlier value.
    #[must_use]
    pub fn named(mut self, name: impl Into<String>, value: impl Into<ArgValue>) -> Self {
        let name = name.into();
        let value = value.into();
        if let Some(slot) = self.named.iter_mut().find(|(n, _)| *n == name) {
            slot.1 = value;
        } else {
            self.named.push((name, value));
        }
        self
    }

    /// Whether any argument was supplied.
    pub fn is_empty(&self) -> bool {
        self.positional.is_empty() && self.named.is_empty()
    }

    /// Encode to the IPC blob a `BindRequest` carries.
    ///
    /// An empty argument list encodes to empty bytes, which the worker reads as
    /// "no arguments" — see `Arguments::parse`.
    pub fn to_ipc(&self) -> Result<Bytes> {
        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for (i, v) in self.positional.iter().enumerate() {
            fields.push(Field::new(format!("positional_{i}"), v.data_type(), true));
            arrays.push(v.to_array()?);
        }
        for (name, v) in &self.named {
            fields.push(Field::new(format!("named_{name}"), v.data_type(), true));
            arrays.push(v.to_array()?);
        }

        let struct_fields = Fields::from(fields);
        // A no-argument call is still a real `Arguments`: `BindRequest.arguments`
        // is NOT optional, so the worker always parses these bytes as an IPC
        // stream. Sending nothing is therefore not "no arguments" — it is a
        // truncated stream, and the Python reference worker dies on it with
        // "Tried reading schema message, was null or length 0", taking the
        // connection with it. The canonical empty encoding is a ONE-row batch
        // over `args: struct<>` (248 bytes, per `Arguments().serialize_to_bytes()`).
        let args: ArrayRef = if struct_fields.is_empty() {
            Arc::new(StructArray::new_empty_fields(1, None))
        } else {
            Arc::new(StructArray::new(struct_fields.clone(), arrays, None))
        };
        let schema = Arc::new(Schema::new(vec![Field::new(
            "args",
            DataType::Struct(struct_fields),
            false,
        )]));
        let batch = RecordBatch::try_new_with_options(
            schema,
            vec![args],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(|e| RpcError::runtime_error(format!("build arguments batch: {e}")))?;
        Ok(Bytes(ipc::write_batch(&batch)?))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;

    use super::*;

    #[test]
    fn empty_arguments_encode_to_a_valid_one_row_stream() {
        // Not empty bytes: `BindRequest.arguments` is non-optional, so the
        // worker always opens these bytes as an IPC stream. This mirrors
        // Python's `Arguments().serialize_to_bytes()` — one row over
        // `args: struct<>`.
        let bytes = Arguments::new().to_ipc().unwrap().0;
        assert!(!bytes.is_empty(), "a truncated stream kills the worker");

        let batch = ipc::read_batch(&bytes).expect("parses as an IPC stream");
        assert_eq!(batch.num_rows(), 1, "one row, like Python");
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "args");
        match batch.schema().field(0).data_type() {
            DataType::Struct(f) => assert!(f.is_empty(), "no argument fields"),
            other => panic!("args should be a struct, got {other:?}"),
        }
    }

    #[test]
    fn positional_and_named_use_the_documented_field_names() {
        let args = Arguments::new()
            .positional(5i64)
            .positional("hello")
            .named("batch_size", 100i64);
        let bytes = args.to_ipc().unwrap();
        let batch = ipc::read_batch(&bytes.0).unwrap();

        let col = batch.column_by_name("args").expect("args column");
        let st = col.as_any().downcast_ref::<StructArray>().expect("struct");
        let names: Vec<&str> = st.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["positional_0", "positional_1", "named_batch_size"]
        );
        assert_eq!(st.len(), 1, "each argument carries exactly one row");
    }

    #[test]
    fn a_repeated_named_argument_replaces_rather_than_duplicates() {
        let args = Arguments::new().named("n", 1i64).named("n", 2i64);
        let batch = ipc::read_batch(&args.to_ipc().unwrap().0).unwrap();
        let st = batch
            .column_by_name("args")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        assert_eq!(
            st.fields().len(),
            1,
            "duplicate name must not emit two fields"
        );
        let v = st
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(v, 2, "the later value wins");
    }

    #[test]
    fn a_column_placeholder_states_its_type_but_carries_null() {
        let args = Arguments::new().positional(ArgValue::Placeholder(DataType::Utf8));
        let batch = ipc::read_batch(&args.to_ipc().unwrap().0).unwrap();
        let st = batch
            .column_by_name("args")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .clone();
        assert_eq!(st.fields()[0].data_type(), &DataType::Utf8);
        assert!(
            st.column(0).is_null(0),
            "a column argument carries no value"
        );
    }
}

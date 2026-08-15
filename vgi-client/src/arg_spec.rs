// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! What a function declares about its parameters.
//!
//! [`FunctionInfo::arguments`](vgi_protocol::protocol::dtos::FunctionInfo) is an
//! IPC-encoded Arrow **schema** in which each field is one parameter: its name
//! and type are the field's, and everything else rides in field *metadata*.
//!
//! # Why a caller cannot skip this
//!
//! The distinction that matters most is [`ArgSpec::is_const`]. A const
//! parameter is a **bind-time constant**, not a column: its value belongs in the
//! bind's `Arguments`, and it must *not* appear in the per-row input batch a
//! scalar or table-in-out call ships.
//!
//! Getting that wrong fails quietly. A worker whose `compute(value, addend)`
//! declares `addend` as a `ConstParam` looks for it among the bind arguments,
//! finds nothing, and answers with a column of NULLs — no error, just wrong
//! data. That is exactly how it was found: a corpus query expecting `66` got
//! `NULL`.
//!
//! The DuckDB extension reads the same metadata (`vgi_arrow_utils.cpp`) and
//! keeps a `positional_is_const` vector for the same reason.

use arrow_schema::{DataType, Field};
use vgi_protocol::ipc;
use vgi_rpc::errors::Result;

/// Field-metadata key marking a parameter as a bind-time constant.
const CONST_KEY: &str = "vgi_const";
/// Field-metadata key marking the function as variadic.
const VARARGS_KEY: &str = "vgi_varargs";
/// Field-metadata key carrying a parameter's documentation.
const DOC_KEY: &str = "vgi_doc";
/// Prefix on a field name that marks a named (rather than positional) argument.
const NAMED_PREFIX: &str = "named_";
/// Metadata value meaning "yes" for the boolean markers above.
const TRUE_VALUE: &str = "true";

/// One declared parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgSpec {
    /// The parameter's name, as the worker declares it.
    pub name: String,
    /// Its declared type.
    pub data_type: DataType,
    /// A bind-time constant rather than a per-row column.
    ///
    /// Its value goes in the bind's `Arguments`; it must not be shipped in the
    /// input batch. See the module docs for what happens otherwise.
    pub is_const: bool,
    /// A named argument (`f(x := 1)`) rather than a positional one.
    pub is_named: bool,
    /// The variadic tail.
    pub is_varargs: bool,
    /// The worker's documentation for this parameter, when it gave any.
    pub doc: Option<String>,
}

impl ArgSpec {
    fn from_field(field: &Field) -> Self {
        let md = field.metadata();
        let flag = |key: &str| md.get(key).map(|v| v == TRUE_VALUE).unwrap_or(false);
        let name = field.name();
        Self {
            // The `named_` prefix is a marker, not part of the name the caller
            // uses, so it is stripped here rather than at every call site.
            name: name.strip_prefix(NAMED_PREFIX).unwrap_or(name).to_string(),
            data_type: field.data_type().clone(),
            is_const: flag(CONST_KEY),
            is_named: name.starts_with(NAMED_PREFIX),
            is_varargs: flag(VARARGS_KEY),
            doc: md.get(DOC_KEY).cloned(),
        }
    }
}

/// A function's declared parameters, in order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArgSpecs(pub Vec<ArgSpec>);

impl ArgSpecs {
    /// Decode `FunctionInfo::arguments`.
    ///
    /// Empty bytes mean "no parameters", which is how a nullary function is
    /// advertised.
    pub fn parse(arguments: &[u8]) -> Result<Self> {
        if arguments.is_empty() {
            return Ok(Self::default());
        }
        let schema = ipc::read_schema(arguments)?;
        Ok(Self(
            schema
                .fields()
                .iter()
                .map(|f| ArgSpec::from_field(f))
                .collect(),
        ))
    }

    /// The positional parameters, in declared order.
    pub fn positional(&self) -> impl Iterator<Item = &ArgSpec> {
        self.0.iter().filter(|a| !a.is_named)
    }

    /// Whether the parameter at positional index `i` is a bind-time constant.
    ///
    /// `false` past the end, so a varargs tail is treated as columnar — which
    /// matches the extension, where only declared parameters carry the marker.
    pub fn positional_is_const(&self, i: usize) -> bool {
        self.positional()
            .nth(i)
            .map(|a| a.is_const)
            .unwrap_or(false)
    }

    /// Whether any parameter is a bind-time constant.
    ///
    /// The cheap check that decides whether a call needs argument splitting at
    /// all; most functions do not.
    pub fn has_const(&self) -> bool {
        self.0.iter().any(|a| a.is_const)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow_schema::Schema;

    fn field(name: &str, ty: DataType, meta: &[(&str, &str)]) -> Field {
        let md: HashMap<String, String> = meta
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Field::new(name, ty, true).with_metadata(md)
    }

    fn encode(fields: Vec<Field>) -> Vec<u8> {
        ipc::write_schema(&Schema::new(fields)).expect("writes")
    }

    #[test]
    fn no_arguments_decodes_to_nothing() {
        assert_eq!(ArgSpecs::parse(&[]).unwrap(), ArgSpecs::default());
        assert!(!ArgSpecs::parse(&[]).unwrap().has_const());
    }

    #[test]
    fn a_const_parameter_is_recognised() {
        let bytes = encode(vec![
            field("value", DataType::Int64, &[]),
            field("addend", DataType::Int64, &[("vgi_const", "true")]),
        ]);
        let specs = ArgSpecs::parse(&bytes).unwrap();
        assert!(specs.has_const());
        assert!(!specs.positional_is_const(0), "value is columnar");
        assert!(specs.positional_is_const(1), "addend is a bind constant");
        assert!(!specs.positional_is_const(9), "past the end is not const");
    }

    #[test]
    fn a_named_argument_loses_its_marker_prefix() {
        let bytes = encode(vec![field("named_batch_size", DataType::Int64, &[])]);
        let specs = ArgSpecs::parse(&bytes).unwrap();
        assert_eq!(specs.0[0].name, "batch_size");
        assert!(specs.0[0].is_named);
        // Named arguments are not positional, so they never shift the indices
        // the const lookup uses.
        assert_eq!(specs.positional().count(), 0);
    }

    #[test]
    fn varargs_and_docs_survive() {
        let bytes = encode(vec![field(
            "rest",
            DataType::Utf8,
            &[("vgi_varargs", "true"), ("vgi_doc", "the tail")],
        )]);
        let spec = &ArgSpecs::parse(&bytes).unwrap().0[0];
        assert!(spec.is_varargs);
        assert_eq!(spec.doc.as_deref(), Some("the tail"));
        assert!(!spec.is_const);
    }

    #[test]
    fn only_the_exact_true_value_sets_a_flag() {
        // The extension compares against the literal "true"; anything else is
        // not a const, and guessing otherwise would silently drop an argument
        // from the input batch.
        let bytes = encode(vec![field(
            "x",
            DataType::Int64,
            &[("vgi_const", "TRUE"), ("vgi_varargs", "1")],
        )]);
        let spec = &ArgSpecs::parse(&bytes).unwrap().0[0];
        assert!(!spec.is_const);
        assert!(!spec.is_varargs);
    }

    #[test]
    fn positional_indices_skip_named_arguments() {
        let bytes = encode(vec![
            field("a", DataType::Int64, &[]),
            field("named_opt", DataType::Int64, &[("vgi_const", "true")]),
            field("b", DataType::Int64, &[("vgi_const", "true")]),
        ]);
        let specs = ArgSpecs::parse(&bytes).unwrap();
        assert!(!specs.positional_is_const(0), "a");
        assert!(specs.positional_is_const(1), "b, not the named one");
    }

    #[test]
    fn arc_schema_round_trips_through_ipc() {
        // Guards the decode path itself: a schema written by the worker must
        // come back with its field metadata intact, which is where every flag
        // lives.
        let bytes = encode(vec![field("x", DataType::Int64, &[("vgi_const", "true")])]);
        let schema: Arc<Schema> = ipc::read_schema(&bytes).unwrap();
        assert_eq!(schema.field(0).metadata().get("vgi_const").unwrap(), "true");
    }
}

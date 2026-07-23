// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Same-name-in-two-schemas scalar fixtures (`test_same_name_bind`).
//!
//! Two distinct [`ScalarFunction`] implementations register under the *same*
//! function name but are declared in different schemas of the `example`
//! catalog (`main` and `data`). They exist to prove that VGI resolves a
//! schema-qualified call to the implementation in that schema —
//! `example.main.test_same_name_bind(x)` must reach the `main` impl and
//! `example.data.test_same_name_bind(x)` the `data` one — rather than
//! collapsing both into one flat by-name registry entry and failing the call as
//! an ambiguous overload.
//!
//! Each returns a VARCHAR tagged with its own schema, so a mis-routed call is
//! visible in the query result rather than silently plausible. Port of
//! vgi-python's `vgi/_test_fixtures/scalar/same_name.py`; driven by
//! `test/sql/integration/scalar/same_name_schemas.test`.

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use vgi::function::{ArgSpec, FunctionExample, FunctionMetadata, ProcessParams, ScalarFunction};
use vgi_rpc::Result;

use super::util::{arc, result};

/// The catalog both implementations live in.
pub const CATALOG: &str = "example";
/// Deliberately identical for both implementations — the collision is the point.
const FUNCTION_NAME: &str = "test_same_name_bind";

/// Render `<tag>:<value>` for every row, preserving nulls.
fn tag_rows(tag: &str, batch: &RecordBatch, params: &ProcessParams) -> Result<RecordBatch> {
    let v = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    let out: StringArray = (0..v.len())
        .map(|i| (!v.is_null(i)).then(|| format!("{tag}:{}", v.value(i))))
        .collect();
    result(params, arc(out))
}

/// `test_same_name_bind(value)` as declared in the `main` schema.
pub struct SameNameMainFunction;
impl ScalarFunction for SameNameMainFunction {
    fn name(&self) -> &str {
        FUNCTION_NAME
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Schema-disambiguation probe; the main-schema implementation".to_string(),
            return_type: Some(DataType::Utf8),
            examples: vec![FunctionExample {
                sql: "SELECT example.main.test_same_name_bind(1)".to_string(),
                description: "Returns 'main:1'".to_string(),
                expected_output: Some("main:1".to_string()),
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Integer value to tag")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        tag_rows("main", batch, params)
    }
}

/// `test_same_name_bind(value)` as declared in the `data` schema.
pub struct SameNameDataFunction;
impl ScalarFunction for SameNameDataFunction {
    fn name(&self) -> &str {
        FUNCTION_NAME
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Schema-disambiguation probe; the data-schema implementation".to_string(),
            return_type: Some(DataType::Utf8),
            examples: vec![FunctionExample {
                sql: "SELECT example.data.test_same_name_bind(1)".to_string(),
                description: "Returns 'data:1'".to_string(),
                expected_output: Some("data:1".to_string()),
            }],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("value", 0, "int64", "Integer value to tag")]
    }
    fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
        tag_rows("data", batch, params)
    }
}

/// Declare both implementations, each into its own schema of `example`.
///
/// Only the primary `example` catalog carries them — the versioned /
/// attach_options catalogs share this binary and must not advertise them.
pub fn register(w: &mut vgi::Worker) {
    w.register_scalar_in(CATALOG, "main", SameNameMainFunction);
    w.register_scalar_in(CATALOG, "data", SameNameDataFunction);
}

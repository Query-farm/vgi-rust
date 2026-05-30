//! The declarative example catalog (schemas, views, macros, tables).

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::{DataType, Field, Schema};
use vgi::arguments::Arguments;
use vgi::catalog::{CatMacro, CatSchema, CatTable, CatView, CatalogModel};

fn col_schema(fields: &[(&str, DataType)]) -> Arc<Schema> {
    Arc::new(Schema::new(
        fields.iter().map(|(n, t)| Field::new(*n, t.clone(), true)).collect::<Vec<_>>(),
    ))
}

/// A function-backed table whose scan is `scan_fn(positional...)`.
fn fn_table(
    name: &str,
    cols: &[(&str, DataType)],
    scan_fn: &str,
    positional: Vec<ArrayRef>,
    cardinality: Option<i64>,
    comment: &str,
) -> CatTable {
    CatTable::new(
        name,
        col_schema(cols),
        scan_fn,
        Arguments::serialize_scan_args(&positional).unwrap_or_default(),
        Some(comment.to_string()),
        cardinality,
    )
}

fn i64_arg(v: i64) -> ArrayRef {
    Arc::new(Int64Array::from(vec![v]))
}

fn view(name: &str, def: &str) -> CatView {
    CatView { name: name.to_string(), definition: def.to_string(), comment: None }
}
fn smacro(name: &str, params: &[&str], def: &str) -> CatMacro {
    CatMacro {
        name: name.to_string(),
        parameters: params.iter().map(|s| s.to_string()).collect(),
        definition: def.to_string(),
        table_macro: false,
        comment: None,
    }
}
fn tmacro(name: &str, params: &[&str], def: &str) -> CatMacro {
    CatMacro { table_macro: true, ..smacro(name, params, def) }
}

/// Build the `example` catalog model.
pub fn build() -> CatalogModel {
    CatalogModel {
        comment: Some("Example VGI catalog for testing".to_string()),
        tags: vec![
            ("source".to_string(), "vgi-fixture-worker".to_string()),
            ("version".to_string(), "1".to_string()),
        ],
        schemas: vec![
            CatSchema {
                name: "main".to_string(),
                comment: Some("Example functions for testing VGI".to_string()),
                views: vec![
                    view("first_ten", "SELECT * FROM sequence(10)"),
                    view("even_numbers", "SELECT * FROM sequence(100) WHERE n % 2 = 0"),
                ],
                macros: vec![
                    smacro("vgi_multiply", &["x", "y"], "x * y"),
                    smacro("vgi_clamp", &["val", "lo", "hi"], "GREATEST(lo, LEAST(hi, val))"),
                    tmacro("vgi_range_table", &["n"], "SELECT * FROM range(n)"),
                ],
                tables: vec![],
            },
            CatSchema {
                name: "data".to_string(),
                comment: Some("Example tables backed by functions".to_string()),
                views: vec![view("small_numbers", "SELECT * FROM numbers WHERE value < 10")],
                macros: vec![],
                tables: vec![
                    fn_table("large_sequence", &[("n", DataType::Int64)], "sequence",
                        vec![i64_arg(1_000_000)], Some(1_000_000),
                        "A large sequence of integers from 0 to 1,000,000"),
                    fn_table("ten_thousand_table", &[("n", DataType::Int64)], "ten_thousand",
                        vec![], None, "Function-backed table over the no-arg ten_thousand function"),
                    fn_table("cardinality_inlined_table", &[("n", DataType::Int64)], "ten_thousand",
                        vec![], Some(10000), "Function-backed table with inlined cardinality"),
                    fn_table("numbers", &[("value", DataType::Int64)], "sequence",
                        vec![i64_arg(100)], Some(100), "First 100 integers"),
                    fn_table("volatile_numbers", &[("value", DataType::Int64)], "sequence",
                        vec![i64_arg(100)], Some(100), "Numbers with volatile stats"),
                    fn_table("funny_numbers", &[("n", DataType::Int64)], "sequence",
                        vec![i64_arg(123456)], Some(123456), "123456 integers"),
                ],
            },
        ],
    }
}

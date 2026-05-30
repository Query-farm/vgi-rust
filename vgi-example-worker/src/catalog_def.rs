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

fn f(name: &str, ty: DataType) -> Field {
    Field::new(name, ty, true)
}
/// Field carrying one metadata key/value (default / comment / generated_expression).
fn fm(name: &str, ty: DataType, key: &str, val: &str) -> Field {
    Field::new(name, ty, true).with_metadata(std::collections::HashMap::from([
        (key.to_string(), val.to_string()),
    ]))
}
/// A row-id column (`is_row_id` marker).
fn frow(name: &str, ty: DataType) -> Field {
    Field::new(name, ty, true)
        .with_metadata(std::collections::HashMap::from([("is_row_id".to_string(), String::new())]))
}
/// Build a metadata-only data table (placeholder `sequence` scan — these tables
/// are exercised by catalog-metadata queries, not data scans).
fn dtable(name: &str, fields: Vec<Field>, comment: &str) -> CatTable {
    CatTable::new(name, Arc::new(Schema::new(fields)), "sequence", Vec::new(), Some(comment.to_string()), None)
}
/// Override a table's backing scan function (a no-arg static scan).
fn scan(mut t: CatTable, scan_fn: &str) -> CatTable {
    t.scan_function = scan_fn.to_string();
    t.scan_arguments = Vec::new();
    t
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
    CatView {
        name: name.to_string(),
        definition: def.to_string(),
        comment: None,
        tags: Vec::new(),
        column_comments: Vec::new(),
    }
}
fn kv(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
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

/// Multi-metadata field (e.g. a column that is both a default and commented).
fn fmm(name: &str, ty: DataType, kvs: &[(&str, &str)]) -> Field {
    Field::new(name, ty, true).with_metadata(
        kvs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
    )
}

/// The full `data`-schema table set (comments, constraints, tags, defaults,
/// generated columns, column comments, rowid markers).
fn data_tables() -> Vec<CatTable> {
    use DataType::{Float64, Int64, Utf8};
    let row_struct = DataType::Struct(
        vec![Field::new("a", Int64, true), Field::new("b", Utf8, true)].into(),
    );
    let mut tables = vec![
        fn_table("large_sequence", &[("n", Int64)], "sequence",
            vec![i64_arg(1_000_000)], Some(1_000_000),
            "A large sequence of integers from 0 to 1,000,000"),
        fn_table("ten_thousand_table", &[("n", Int64)], "ten_thousand",
            vec![], None, "Function-backed table over the no-arg ten_thousand function"),
        fn_table("cardinality_inlined_table", &[("n", Int64)], "ten_thousand",
            vec![], Some(10000), "Function-backed table with inlined cardinality (10000 rows)"),
        fn_table("numbers", &[("value", Int64)], "sequence",
            vec![i64_arg(100)], Some(100), "First 100 integers (demonstrates explicit columns)"),
        fn_table("volatile_numbers", &[("value", Int64)], "sequence",
            vec![i64_arg(100)], Some(100), "Numbers with volatile stats (TTL=0, always re-fetched)"),
        fn_table("funny_numbers", &[("n", Int64)], "sequence",
            vec![i64_arg(123456)], Some(123456),
            "123456 integers; stats served by the sequence function, not the table"),
        scan(dtable("colors", vec![f("id", Int64), f("color", Utf8), f("hex_code", Utf8)],
            "Colors table with ENUM-derived statistics"), "colors_scan"),
        dtable("generated_sequence", vec![
            f("n", Int64),
            fm("doubled", Int64, "generated_expression", "n * 2"),
            fm("label", Utf8, "generated_expression", "'item_' || CAST(n AS VARCHAR)"),
        ], "Table with generated columns backed by sequence(10)"),
        dtable("rowid_first", vec![frow("row_id", Int64), f("name", Utf8), f("value", Utf8)],
            "Table with row_id at column index 0"),
        dtable("rowid_middle", vec![f("name", Utf8), frow("row_id", Int64), f("value", Utf8)],
            "Table with row_id at column index 1"),
        dtable("rowid_last", vec![f("name", Utf8), f("value", Utf8), frow("row_id", Int64)],
            "Table with row_id at column index 2"),
        dtable("rowid_string", vec![frow("row_id", Utf8), f("value", Int64)],
            "Table with string row_id"),
        dtable("rowid_struct", vec![frow("row_id", row_struct), f("value", Utf8)],
            "Table with struct row_id"),
        dtable("late_mat", vec![frow("row_id", Int64), f("ord", Int64), f("payload", Utf8), f("pushed", Utf8)],
            "Late-materialization table (1000 rows, unique rowid)"),
        dtable("late_mat_dup", vec![frow("row_id", Int64), f("ord", Int64), f("payload", Utf8), f("pushed", Utf8)],
            "Late-materialization table with deliberately non-unique rowid (contract violation)"),
        dtable("late_mat_nulls", vec![frow("row_id", Int64), f("ord", Int64), f("payload", Utf8), f("pushed", Utf8)],
            "Late-materialization table with NULLs in the ord column"),
        dtable("versioned_data", vec![f("id", Int64), f("name", Utf8)],
            "Versioned data table demonstrating time travel with schema evolution"),
        dtable("versioned_constraints", vec![f("id", Int64), f("name", Utf8), f("email", Utf8), f("department_id", Int64)],
            "Table with constraints that evolve across versions"),
    ];

    // Multi-branch tables (registered for metadata; scans handled separately).
    for (name, comment) in [
        ("multi_branch_numbers", "Multi-branch: UNION of sequence(50) + sequence(50) — used by multi_branch_scan.test"),
        ("multi_branch_filtered_numbers", "Multi-branch with complementary branch_filters — exercises pruning"),
        ("multi_branch_hetero", "Multi-branch: sequence(50) + read_parquet — used by multi_branch_heterogeneous.test"),
        ("multi_branch_nopushdown", "Multi-branch: VGI + read_csv — used by multi_branch_pushdown_incapable.test"),
        ("multi_branch_empty", "Multi-branch: worker returns empty branches list — used by multi_branch_empty_branches.test"),
        ("multi_branch_recon", "Multi-branch: column reconciliation — used by multi_branch_reconciliation.test"),
        ("multi_branch_two_writable", "Multi-branch with two writable=True arms — used by multi_branch_two_writable.test"),
    ] {
        tables.push(dtable(name, vec![f("n", Int64)], comment));
    }

    // departments: PK(id), NOT NULL(id,name), UNIQUE(name), CHECK(budget>=0), default budget=0.
    let mut departments = dtable("departments", vec![
        f("id", Int64), f("name", Utf8), fm("budget", Float64, "default", "0"),
    ], "Department reference table");
    departments = scan(departments, "departments_scan");
    departments.primary_key = vec![vec![0]];
    departments.not_null = vec![0, 1];
    departments.unique = vec![vec![1]];
    departments.check = vec!["budget >= 0".to_string()];
    tables.push(departments);

    // products: defaults + column comments.
    let mut products = dtable("products", vec![
        fmm("id", Int64, &[("comment", "Unique product identifier")]),
        fmm("name", Utf8, &[("default", "'unknown'"), ("comment", "Product display name")]),
        fm("quantity", Int64, "default", "0"),
        fmm("price", Float64, &[("default", "9.99"), ("comment", "Unit price in USD")]),
    ], "Product table with column defaults");
    products = scan(products, "products_scan");
    products.primary_key = vec![vec![0]];
    products.not_null = vec![0];
    tables.push(products);

    // employees: PK(id), NOT NULL(id,name,email), UNIQUE(email), FK→departments.
    let mut employees = dtable("employees", vec![
        f("id", Int64), f("name", Utf8), f("email", Utf8), f("department_id", Int64),
    ], "Employee table with FK to departments");
    employees = scan(employees, "employees_scan");
    employees.primary_key = vec![vec![0]];
    employees.not_null = vec![0, 1, 2];
    employees.unique = vec![vec![2]];
    employees.foreign_keys = vec![vgi::catalog::ForeignKey {
        columns: vec!["department_id".to_string()],
        referenced_table: "departments".to_string(),
        referenced_columns: vec!["id".to_string()],
    }];
    tables.push(employees);

    // projects: composite PK, NOT NULL, FK→departments.
    let mut projects = dtable("projects", vec![
        f("department_id", Int64), f("project_code", Utf8), f("title", Utf8),
    ], "Projects with composite PK and FK to departments");
    projects = scan(projects, "projects_scan");
    projects.primary_key = vec![vec![0, 1]];
    projects.not_null = vec![0, 1, 2];
    projects.foreign_keys = vec![vgi::catalog::ForeignKey {
        columns: vec!["department_id".to_string()],
        referenced_table: "departments".to_string(),
        referenced_columns: vec!["id".to_string()],
    }];
    tables.push(projects);

    tables
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
                    CatView {
                        comment: Some("First 10 integers".to_string()),
                        tags: kv(&[("layer", "demo"), ("origin", "sequence")]),
                        column_comments: kv(&[("n", "Sequence index 0..9")]),
                        ..view("first_ten", "SELECT * FROM sequence(10)")
                    },
                    CatView {
                        comment: Some("Even numbers from 0 to 98".to_string()),
                        ..view("even_numbers", "SELECT * FROM sequence(100) WHERE n % 2 = 0")
                    },
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
                views: vec![CatView {
                    column_comments: kv(&[("value", "Single-digit value 0..9")]),
                    ..view("small_numbers", "SELECT * FROM numbers WHERE value < 10")
                }],
                macros: vec![],
                tables: data_tables(),
            },
        ],
    }
}

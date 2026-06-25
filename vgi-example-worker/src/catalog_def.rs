// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The declarative example catalog (schemas, views, macros, tables).

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::{DataType, Field, Schema};
use vgi::arguments::Arguments;
use vgi::catalog::{CatMacro, CatSchema, CatTable, CatView, CatalogModel, TimeTravelVersion};

/// One time-travel version (schema + parameterized scan function `scan_fn(version)`
/// + valid-from year). The single scan function returns the version-specific
/// schema/rows based on its `version` const argument.
fn ttv(version: i64, cols: Vec<Field>, scan_fn: &str, year: i32) -> TimeTravelVersion {
    TimeTravelVersion {
        version,
        columns: Arc::new(Schema::new(cols)),
        scan_function: scan_fn.to_string(),
        scan_arguments: Arguments::serialize_scan_args(&[i64_arg(version)]).unwrap_or_default(),
        timestamp_year: Some(year),
    }
}

/// A time-travel table: its current (highest-version) columns are the base
/// schema; `catalog_table_get`/`scan_function_get`/`scan_branches_get` select
/// per `at_value`. The scan is NOT inlined — the C++ re-invokes the legacy scan
/// path per query (carrying the AT clause), so each version resolves its own
/// scan arguments without colliding in the inlined-scan bind cache.
fn tt_table(name: &str, comment: &str, versions: Vec<TimeTravelVersion>) -> CatTable {
    let current = versions
        .iter()
        .max_by_key(|v| v.version)
        .expect("≥1 version");
    let mut t = dtable(
        name,
        current
            .columns
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect(),
        comment,
    );
    t.scan_function = current.scan_function.clone();
    t.scan_arguments = current.scan_arguments.clone();
    t.inline_scan = false;
    t.time_travel = versions;
    t
}

fn col_schema(fields: &[(&str, DataType)]) -> Arc<Schema> {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(n, t)| Field::new(*n, t.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

fn f(name: &str, ty: DataType) -> Field {
    Field::new(name, ty, true)
}
/// Field carrying one metadata key/value (default / comment / generated_expression).
fn fm(name: &str, ty: DataType, key: &str, val: &str) -> Field {
    Field::new(name, ty, true).with_metadata(std::collections::HashMap::from([(
        key.to_string(),
        val.to_string(),
    )]))
}
/// A row-id column (`is_row_id` marker).
fn frow(name: &str, ty: DataType) -> Field {
    Field::new(name, ty, true).with_metadata(std::collections::HashMap::from([(
        "is_row_id".to_string(),
        String::new(),
    )]))
}
/// Build a metadata-only data table (placeholder `sequence` scan — these tables
/// are exercised by catalog-metadata queries, not data scans).
fn dtable(name: &str, fields: Vec<Field>, comment: &str) -> CatTable {
    CatTable::new(
        name,
        Arc::new(Schema::new(fields)),
        "sequence",
        Vec::new(),
        Some(comment.to_string()),
        None,
    )
}
/// Mark a function-backed table to inline its scan function in `TableInfo`.
fn inline(mut t: CatTable) -> CatTable {
    t.inline_scan = true;
    t
}
/// Override a table's backing scan function (a no-arg static scan).
fn scan(mut t: CatTable, scan_fn: &str) -> CatTable {
    t.scan_function = scan_fn.to_string();
    t.scan_arguments = Vec::new();
    t
}
/// A rowid table backed by `rowid_sequence(20, layout, row_id_type)`.
fn rowid_table(
    name: &str,
    fields: Vec<Field>,
    layout: &str,
    rid_type: &str,
    comment: &str,
) -> CatTable {
    use std::sync::Arc as A;
    let mut t = dtable(name, fields, comment);
    t.scan_function = "rowid_sequence".to_string();
    let named: Vec<(&str, ArrayRef)> = vec![
        (
            "layout",
            A::new(arrow_array::StringArray::from(vec![layout])) as ArrayRef,
        ),
        (
            "row_id_type",
            A::new(arrow_array::StringArray::from(vec![rid_type])) as ArrayRef,
        ),
    ];
    t.scan_arguments =
        Arguments::serialize_scan_args_named(&[i64_arg(20)], &named).unwrap_or_default();
    t.cardinality = Some(20);
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

/// A `required_field_filter_paths` fixture table backed by a static `rff_*_scan`.
fn rff_table(
    name: &str,
    fields: Vec<Field>,
    scan_fn: &str,
    paths: &[&str],
    comment: &str,
) -> CatTable {
    let mut t = dtable(name, fields, comment);
    t.scan_function = scan_fn.to_string();
    t.required_field_filter_paths = paths.iter().map(|s| s.to_string()).collect();
    t.cardinality = Some(3);
    t
}

/// `s: struct{a: int64, b: int64}`.
fn fstruct_ab(name: &str) -> Field {
    f(
        name,
        DataType::Struct(vec![f("a", DataType::Int64), f("b", DataType::Int64)].into()),
    )
}

/// `bbox: struct{xmin, ymin, xmax, ymax: float32}` (Overture segment shape).
fn fbbox() -> Field {
    f(
        "bbox",
        DataType::Struct(
            vec![
                f("xmin", DataType::Float32),
                f("ymin", DataType::Float32),
                f("xmax", DataType::Float32),
                f("ymax", DataType::Float32),
            ]
            .into(),
        ),
    )
}

/// A `required_field_filter_paths` table that delegates its scan to a NATIVE
/// DuckDB function (e.g. `read_parquet`) — `scan_function_get` returns it so the
/// C++ binds the built-in reader directly.
fn rff_native(
    name: &str,
    fields: Vec<Field>,
    scan_fn: &str,
    positional: Vec<ArrayRef>,
    named: &[(&str, ArrayRef)],
    paths: &[&str],
    comment: &str,
) -> CatTable {
    let mut t = dtable(name, fields, comment);
    t.scan_function = scan_fn.to_string();
    t.scan_arguments = Arguments::serialize_scan_args_named(&positional, named).unwrap_or_default();
    t.required_field_filter_paths = paths.iter().map(|s| s.to_string()).collect();
    t
}

fn str_arg(s: &str) -> ArrayRef {
    Arc::new(arrow_array::StringArray::from(vec![s]))
}

/// WKB (little-endian) for `POINT(x y)` — the GEOMETRY internal blob format.
fn wkb_point(x: f64, y: f64) -> Vec<u8> {
    let mut v = vec![0x01, 0x01, 0x00, 0x00, 0x00];
    v.extend_from_slice(&x.to_le_bytes());
    v.extend_from_slice(&y.to_le_bytes());
    v
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
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}
fn smacro(name: &str, params: &[&str], def: &str) -> CatMacro {
    CatMacro {
        name: name.to_string(),
        parameters: params.iter().map(|s| s.to_string()).collect(),
        definition: def.to_string(),
        table_macro: false,
        comment: None,
        defaults: Vec::new(),
        parameter_docs: Vec::new(),
    }
}
fn tmacro(name: &str, params: &[&str], def: &str) -> CatMacro {
    CatMacro {
        table_macro: true,
        ..smacro(name, params, def)
    }
}

/// Multi-metadata field (e.g. a column that is both a default and commented).
fn fmm(name: &str, ty: DataType, kvs: &[(&str, &str)]) -> Field {
    Field::new(name, ty, true).with_metadata(
        kvs.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    )
}

/// The full `data`-schema table set (comments, constraints, tags, defaults,
/// generated columns, column comments, rowid markers).
fn data_tables() -> Vec<CatTable> {
    use DataType::{Float64, Int64, Utf8};
    // A late-materialization table backed by `late_materialization(1000, …)`,
    // inlined so the C++ Top-N→SEMI rewrite sees the rowid scan directly.
    let lm = |name: &str, comment: &str, named: Vec<(&str, ArrayRef)>| -> CatTable {
        let cols = vec![
            frow("row_id", Int64),
            f("ord", Int64),
            f("payload", Utf8),
            f("pushed", Utf8),
        ];
        let mut t = CatTable::new(
            name,
            Arc::new(Schema::new(cols)),
            "late_materialization",
            Arguments::serialize_scan_args_named(&[i64_arg(1000)], &named).unwrap_or_default(),
            Some(comment.to_string()),
            Some(1000),
        );
        t.inline_scan = true;
        t
    };
    let row_struct =
        DataType::Struct(vec![Field::new("a", Int64, true), Field::new("b", Utf8, true)].into());
    let mut tables = vec![
        inline(fn_table(
            "large_sequence",
            &[("n", Int64)],
            "sequence",
            vec![i64_arg(1_000_000)],
            Some(1_000_000),
            "A large sequence of integers from 0 to 1,000,000",
        )),
        // Inlined scan_function (inlined_scan_function.test wants the scan
        // trace + no scan_function_get RPC) but NO inlined cardinality: with
        // cardinality=None the wire value is NULL, so the C++ still fires the
        // per-bind table_function_cardinality RPC (ten_thousand returns 10000)
        // and emits no cardinality-inlined trace (inlined_cardinality.test).
        rff_table(
            "rff_simple",
            vec![f("a", Int64), f("b", Int64)],
            "rff_simple_scan",
            &["a"],
            "rff_simple — requires a filter referencing column 'a'.",
        ),
        rff_table(
            "rff_struct",
            vec![fstruct_ab("s"), f("other", Int64)],
            "rff_struct_scan",
            &["s.a", "s.b"],
            "rff_struct — requires filters on both struct subfields s.a and s.b.",
        ),
        rff_table(
            "rff_nested",
            vec![f(
                "wrapper",
                DataType::Struct(
                    vec![f("mid", DataType::Struct(vec![f("leaf", Int64)].into()))].into(),
                ),
            )],
            "rff_nested_scan",
            &["wrapper.mid.leaf"],
            "rff_nested — requires a filter on the 3-deep nested path wrapper.mid.leaf.",
        ),
        rff_table(
            "rff_multi",
            vec![fstruct_ab("s"), f("top", Int64)],
            "rff_multi_scan",
            &["top", "s.a"],
            "rff_multi — mixed top-level + struct subfield requirements.",
        ),
        rff_table(
            "rff_none",
            vec![f("a", Int64), f("b", Int64)],
            "rff_none_scan",
            &[],
            "rff_none — control table with no required_field_filter_paths (opt-out fast path).",
        ),
        rff_native(
            "rff_parquet",
            vec![fbbox(), f("other", Int64)],
            "read_parquet",
            vec![str_arg("/tmp/rff_seg.parquet")],
            &[],
            &["bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"],
            "rff_parquet — native read_parquet delegation with bbox.* required filters.",
        ),
        rff_native(
            "rff_hive",
            vec![
                f("id", Utf8),
                fbbox(),
                f("name", Utf8),
                f("num", Int64),
                f("theme", Utf8),
                f("type", Utf8),
            ],
            "read_parquet",
            vec![str_arg("/tmp/rff_hive/*/*/*.parquet")],
            &[(
                "hive_partitioning",
                Arc::new(arrow_array::BooleanArray::from(vec![true])) as ArrayRef,
            )],
            &["bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"],
            "rff_hive — native read_parquet over Hive glob with bbox.* required filters.",
        ),
        rff_native(
            "rff_hive_mixed",
            vec![
                f("id", Utf8),
                fbbox(),
                f("name", Utf8),
                f("num", Int64),
                f("theme", Utf8),
                f("type", Utf8),
            ],
            "read_parquet",
            vec![str_arg("/tmp/rff_hive/*/*/*.parquet")],
            &[(
                "hive_partitioning",
                Arc::new(arrow_array::BooleanArray::from(vec![true])) as ArrayRef,
            )],
            &["id", "bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"],
            "rff_hive_mixed — native read_parquet, top-level 'id' + bbox.* required filters.",
        ),
        rff_table(
            "rff_rowid",
            vec![frow("row_id", Int64), fbbox(), f("other", Int64)],
            "rff_rowid_scan",
            &["bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"],
            "rff_rowid — row_id virtual column + required bbox.* filters.",
        ),
        scan(
            dtable(
                "filter_echo_table",
                vec![f("n", Int64), f("s", Utf8), f("pushed_filters", Utf8)],
                "Catalog table echoing pushed-down filters (filter-pushdown-through-view tests).",
            ),
            "filter_echo_table_scan",
        ),
        inline(fn_table(
            "ten_thousand_table",
            &[("n", Int64)],
            "ten_thousand",
            vec![],
            None,
            "Function-backed table over the no-arg ten_thousand function",
        )),
        inline(fn_table(
            "cardinality_inlined_table",
            &[("n", Int64)],
            "ten_thousand",
            vec![],
            Some(10000),
            "Function-backed table with inlined cardinality (10000 rows)",
        )),
        fn_table(
            "numbers",
            &[("value", Int64)],
            "sequence",
            vec![i64_arg(100)],
            Some(100),
            "First 100 integers (demonstrates explicit columns)",
        ),
        fn_table(
            "volatile_numbers",
            &[("value", Int64)],
            "sequence",
            vec![i64_arg(100)],
            Some(100),
            "Numbers with volatile stats (TTL=0, always re-fetched)",
        ),
        fn_table(
            "funny_numbers",
            &[("n", Int64)],
            "sequence",
            vec![i64_arg(123456)],
            Some(123456),
            "123456 integers; stats served by the sequence function, not the table",
        ),
        scan(
            dtable(
                "colors",
                vec![f("id", Int64), f("color", Utf8), f("hex_code", Utf8)],
                "Colors table with ENUM-derived statistics",
            ),
            "colors_scan",
        ),
        {
            let mut t = dtable(
                "generated_sequence",
                vec![
                    f("n", Int64),
                    fm("doubled", Int64, "generated_expression", "n * 2"),
                    fm(
                        "label",
                        Utf8,
                        "generated_expression",
                        "'item_' || CAST(n AS VARCHAR)",
                    ),
                ],
                "Table with generated columns backed by sequence(10)",
            );
            t.scan_function = "sequence".to_string();
            t.scan_arguments = Arguments::serialize_scan_args(&[i64_arg(10)]).unwrap_or_default();
            t.cardinality = Some(10);
            t
        },
        rowid_table(
            "rowid_first",
            vec![frow("row_id", Int64), f("name", Utf8), f("value", Utf8)],
            "first",
            "int64",
            "Table with row_id at column index 0",
        ),
        rowid_table(
            "rowid_middle",
            vec![f("name", Utf8), frow("row_id", Int64), f("value", Utf8)],
            "middle",
            "int64",
            "Table with row_id at column index 1",
        ),
        rowid_table(
            "rowid_last",
            vec![f("name", Utf8), f("value", Utf8), frow("row_id", Int64)],
            "last",
            "int64",
            "Table with row_id at column index 2",
        ),
        rowid_table(
            "rowid_string",
            vec![frow("row_id", Utf8), f("value", Int64)],
            "first",
            "string",
            "Table with string row_id",
        ),
        rowid_table(
            "rowid_struct",
            vec![frow("row_id", row_struct), f("value", Utf8)],
            "first",
            "struct",
            "Table with struct row_id",
        ),
        lm(
            "late_mat",
            "Late-materialization table (1000 rows, unique rowid)",
            Vec::new(),
        ),
        lm(
            "late_mat_dup",
            "Late-materialization table with deliberately non-unique rowid (contract violation)",
            vec![(
                "dup_row_id",
                Arc::new(arrow_array::BooleanArray::from(vec![true])) as ArrayRef,
            )],
        ),
        lm(
            "late_mat_nulls",
            "Late-materialization table with NULLs in the ord column",
            vec![("null_ord_stride", i64_arg(7))],
        ),
        tt_table(
            "versioned_data",
            "Versioned data table demonstrating time travel with schema evolution",
            vec![
                ttv(1, vec![f("id", Int64)], "versioned_data_scan", 2020),
                ttv(
                    2,
                    vec![
                        f("id", Int64),
                        f("name", Utf8),
                        f("score", Float64),
                        f("active", DataType::Boolean),
                    ],
                    "versioned_data_scan",
                    2021,
                ),
                ttv(
                    3,
                    vec![f("id", Int64), f("score", Float64)],
                    "versioned_data_scan",
                    2022,
                ),
            ],
        ),
        {
            let mut t = tt_table(
                "versioned_constraints",
                "Table with constraints that evolve across versions",
                vec![
                    ttv(
                        1,
                        vec![f("id", Int64), f("name", Utf8)],
                        "versioned_constraints_scan",
                        2020,
                    ),
                    ttv(
                        2,
                        vec![f("id", Int64), f("name", Utf8), f("email", Utf8)],
                        "versioned_constraints_scan",
                        2021,
                    ),
                    ttv(
                        3,
                        vec![
                            f("id", Int64),
                            f("name", Utf8),
                            f("email", Utf8),
                            f("department_id", Int64),
                        ],
                        "versioned_constraints_scan",
                        2022,
                    ),
                ],
            );
            // V3 constraints (current): NOT NULL id/name, PK id, UNIQUE email,
            // FK department_id → data.departments(id).
            t.not_null = vec![0, 1];
            t.primary_key = vec![vec![0]];
            t.unique = vec![vec![2]];
            t.foreign_keys = vec![vgi::catalog::ForeignKey {
                columns: vec!["department_id".to_string()],
                referenced_table: "departments".to_string(),
                referenced_columns: vec!["id".to_string()],
            }];
            t
        },
        // Function-backed time-travel + filter pushdown. Reads AT at init via the
        // bind request; supports_time_travel lets it accept AT without
        // catalog-resolved versions (the function resolves the version itself).
        {
            let mut t = inline(fn_table(
                "tt_pushdown_fn",
                &[
                    ("id", Int64),
                    ("val", Int64),
                    ("seen_version", Int64),
                    ("pushed_filters", Utf8),
                ],
                "tt_pushdown_scan",
                vec![],
                None,
                "Function-backed: prunes by filter AND time-travels (AT read at init).",
            ));
            t.supports_time_travel = true;
            t
        },
    ];

    // Columns-based time-travel + filter pushdown: the scan_function_get path
    // resolves AT → version and passes it as `tt_pushdown_cols_scan(version)`.
    {
        let tt_cols = || {
            vec![
                f("id", Int64),
                f("val", Int64),
                f("seen_version", Int64),
                f("pushed_filters", Utf8),
            ]
        };
        tables.push(tt_table(
            "tt_pushdown_cols",
            "Columns-based: prunes by filter AND time-travels (AT → version arg).",
            vec![
                ttv(1, tt_cols(), "tt_pushdown_cols_scan", 2000),
                ttv(2, tt_cols(), "tt_pushdown_cols_scan", 2021),
            ],
        ));
    }

    // Multi-branch tables: each declares its physical branches.
    let seq = |count: i64| vgi::catalog::CatBranch {
        function_name: "sequence".to_string(),
        scan_arguments: Arguments::serialize_scan_args(&[i64_arg(count)]).unwrap_or_default(),
        branch_filter: None,
        writable: false,
    };
    let native = |func: &str, path: &str| vgi::catalog::CatBranch {
        function_name: func.to_string(),
        scan_arguments: Arguments::serialize_scan_args(&[std::sync::Arc::new(
            arrow_array::StringArray::from(vec![path]),
        ) as ArrayRef])
        .unwrap_or_default(),
        branch_filter: None,
        writable: false,
    };
    let mb = |name: &str, comment: &str, branches: Vec<vgi::catalog::CatBranch>| {
        let mut t = dtable(name, vec![f("n", Int64)], comment);
        t.branches = Some(branches);
        t
    };
    tables.push(mb(
        "multi_branch_numbers",
        "Multi-branch: UNION of sequence(50) + sequence(50) — used by multi_branch_scan.test",
        vec![seq(50), seq(50)],
    ));
    tables.push(mb(
        "multi_branch_filtered_numbers",
        "Multi-branch with complementary branch_filters — exercises pruning",
        vec![
            vgi::catalog::CatBranch {
                branch_filter: Some("n < 50".to_string()),
                ..seq(100)
            },
            vgi::catalog::CatBranch {
                branch_filter: Some("n >= 50".to_string()),
                ..seq(100)
            },
        ],
    ));
    tables.push(mb(
        "multi_branch_hetero",
        "Multi-branch: sequence(50) + read_parquet — used by multi_branch_heterogeneous.test",
        vec![
            seq(50),
            native("read_parquet", "/tmp/vgi_hetero_branch.parquet"),
        ],
    ));
    tables.push(mb(
        "multi_branch_nopushdown",
        "Multi-branch: VGI + read_csv — used by multi_branch_pushdown_incapable.test",
        vec![
            seq(50),
            native("read_csv_auto", "/tmp/vgi_nopushdown_branch.csv"),
        ],
    ));
    tables.push(mb(
        "multi_branch_empty",
        "Multi-branch: empty branches list — used by multi_branch_empty_branches.test",
        vec![],
    ));
    {
        let mut t = dtable(
            "multi_branch_recon",
            vec![f("a", Int64), f("b", Int64)],
            "Multi-branch: column reconciliation — used by multi_branch_reconciliation.test",
        );
        t.branches = Some(vec![
            native("read_parquet", "/tmp/vgi_recon_a_b.parquet"),
            native("read_parquet", "/tmp/vgi_recon_b_a.parquet"),
            native("read_parquet", "/tmp/vgi_recon_a_only.parquet"),
        ]);
        tables.push(t);
    }
    tables.push(mb(
        "multi_branch_two_writable",
        "Multi-branch with two writable=True arms — used by multi_branch_two_writable.test",
        vec![
            vgi::catalog::CatBranch {
                writable: true,
                ..seq(10)
            },
            vgi::catalog::CatBranch {
                writable: true,
                ..seq(10)
            },
        ],
    ));

    use vgi::statistics::{CatColStat, StatValue};
    let stat = |name: &str, min: StatValue, max: StatValue, distinct: i64| CatColStat {
        column_name: name.to_string(),
        min,
        max,
        has_null: false,
        has_not_null: true,
        distinct_count: Some(distinct),
        contains_unicode: None,
        max_string_length: None,
    };

    // `numbers` carries DuckDB-extracted stats (value 0..99); `colors` carries
    // ENUM-ordinal-derived stats (color: red(0)..blue(2)).
    for t in tables.iter_mut() {
        if t.name == "numbers" {
            t.statistics = vec![stat(
                "value",
                StatValue::Int64(0),
                StatValue::Int64(99),
                100,
            )];
        }
        if t.name == "colors" {
            t.statistics = vec![
                stat("id", StatValue::Int64(1), StatValue::Int64(3), 3),
                CatColStat {
                    contains_unicode: Some(false),
                    max_string_length: Some(5),
                    ..stat(
                        "color",
                        StatValue::Utf8("red".into()),
                        StatValue::Utf8("blue".into()),
                        3,
                    )
                },
                CatColStat {
                    contains_unicode: Some(false),
                    max_string_length: Some(7),
                    ..stat(
                        "hex_code",
                        StatValue::Utf8("#0000FF".into()),
                        StatValue::Utf8("#FF0000".into()),
                        3,
                    )
                },
            ];
        }
    }

    // geo_points: a GEOMETRY (geoarrow.point) column with spatial bounding-box
    // statistics (5x5 grid of points (0,0)..(4,4); stat min/max are WKB corner
    // points the extension reinterprets as GEOMETRY → BOX(0 0, 4 4)).
    {
        let xy = DataType::Struct(
            vec![
                Field::new("x", Float64, true),
                Field::new("y", Float64, true),
            ]
            .into(),
        );
        let geom = Field::new("geom", xy, true).with_metadata(std::collections::HashMap::from([(
            "ARROW:extension:name".to_string(),
            "geoarrow.point".to_string(),
        )]));
        let mut t = dtable(
            "geo_points",
            vec![f("id", Int64), geom],
            "Geometry table with spatial bounding-box statistics",
        );
        t.statistics = vec![
            stat("id", StatValue::Int64(1), StatValue::Int64(25), 25),
            stat(
                "geom",
                StatValue::Binary(wkb_point(0.0, 0.0)),
                StatValue::Binary(wkb_point(4.0, 4.0)),
                25,
            ),
        ];
        tables.push(t);
    }

    // departments: PK(id), NOT NULL(id,name), UNIQUE(name), CHECK(budget>=0), default budget=0.
    let mut departments = dtable(
        "departments",
        vec![
            f("id", Int64),
            f("name", Utf8),
            fm("budget", Float64, "default", "0"),
        ],
        "Department reference table",
    );
    departments.statistics = vec![
        stat("id", StatValue::Int64(1), StatValue::Int64(10), 10),
        CatColStat {
            contains_unicode: Some(false),
            max_string_length: Some(20),
            ..stat(
                "name",
                StatValue::Utf8("Accounting".into()),
                StatValue::Utf8("Sales".into()),
                10,
            )
        },
        stat(
            "budget",
            StatValue::Float64(50000.0),
            StatValue::Float64(500000.0),
            10,
        ),
    ];
    departments = scan(departments, "departments_scan");
    departments.primary_key = vec![vec![0]];
    departments.not_null = vec![0, 1];
    departments.unique = vec![vec![1]];
    departments.check = vec!["budget >= 0".to_string()];
    tables.push(departments);

    // products: defaults + column comments.
    let mut products = dtable(
        "products",
        vec![
            fmm("id", Int64, &[("comment", "Unique product identifier")]),
            fmm(
                "name",
                Utf8,
                &[
                    ("default", "'unknown'"),
                    ("comment", "Product display name"),
                ],
            ),
            fm("quantity", Int64, "default", "0"),
            fmm(
                "price",
                Float64,
                &[("default", "9.99"), ("comment", "Unit price in USD")],
            ),
        ],
        "Product table with column defaults",
    );
    products = scan(products, "products_scan");
    products.primary_key = vec![vec![0]];
    products.not_null = vec![0];
    products.statistics = vec![
        stat("id", StatValue::Int64(1), StatValue::Int64(100), 100),
        CatColStat {
            contains_unicode: Some(false),
            max_string_length: Some(30),
            ..stat(
                "name",
                StatValue::Utf8("Anvil".into()),
                StatValue::Utf8("Zebra Tape".into()),
                100,
            )
        },
        CatColStat {
            has_null: true,
            ..stat("quantity", StatValue::Int64(0), StatValue::Int64(10000), 50)
        },
        stat(
            "price",
            StatValue::Float64(0.99),
            StatValue::Float64(999.99),
            80,
        ),
    ];
    tables.push(products);

    // employees: PK(id), NOT NULL(id,name,email), UNIQUE(email), FK→departments.
    let mut employees = dtable(
        "employees",
        vec![
            f("id", Int64),
            f("name", Utf8),
            f("email", Utf8),
            f("department_id", Int64),
        ],
        "Employee table with FK to departments",
    );
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
    let mut projects = dtable(
        "projects",
        vec![
            f("department_id", Int64),
            f("project_code", Utf8),
            f("title", Utf8),
        ],
        "Projects with composite PK and FK to departments",
    );
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
/// The `versioned` catalog fixture — advertises version metadata and validates
/// `data_version_spec` / `implementation_version` at ATTACH time. Mirrors the
/// Python `versioned` fixture (impl 1.0.0, data range >=1.0.0,<2.0.0).
pub fn versioned() -> CatalogModel {
    CatalogModel {
        name: "versioned".to_string(),
        implementation_version: Some("1.0.0".to_string()),
        data_version_spec: Some(">=1.0.0,<2.0.0".to_string()),
        supported_data_versions: vec![
            "1.0.0".to_string(),
            "1.1.0".to_string(),
            "1.2.0".to_string(),
        ],
        default_data_version: Some("1.2.0".to_string()),
        npm_version_resolution: false,
        attach_option_specs: Vec::new(),
        attach_options_default_batch: None,
        supported_implementation_versions: Vec::new(),
        version_schemas: std::collections::HashMap::new(),
        comment: Some(
            "Example catalog demonstrating data_version_spec validation and cookie stickiness"
                .to_string(),
        ),
        tags: Vec::new(),
        source_url: None,
        supports_time_travel: false,
        schemas: vec![CatSchema {
            name: "main".to_string(),
            comment: None,
            tags: Vec::new(),
            views: Vec::new(),
            macros: Vec::new(),
            tables: Vec::new(),
        }],
    }
}

/// The `versioned_tables` catalog — visible tables vary by the resolved
/// `data_version` (1.0=animals, 1.1=animals+color, 2.0=animals+plants, 3.0=plants).
pub fn versioned_tables() -> CatalogModel {
    use arrow_schema::DataType::{Float64, Int64, Utf8};
    use std::collections::HashMap;
    let main = |tables: Vec<CatTable>| CatSchema {
        name: "main".to_string(),
        comment: None,
        tags: Vec::new(),
        views: Vec::new(),
        macros: Vec::new(),
        tables,
    };
    let animals = scan(
        dtable(
            "animals",
            vec![f("name", Utf8), f("legs", Int64), f("sound", Utf8)],
            "Animals table for data_version 1.0.0",
        ),
        "versioned_tables_animals_scan",
    );
    let animals_color = scan(
        dtable(
            "animals",
            vec![
                f("name", Utf8),
                f("legs", Int64),
                f("sound", Utf8),
                f("color", Utf8),
            ],
            "Animals table for data_version 1.1.0 (with color)",
        ),
        "versioned_tables_animals_color_scan",
    );
    let plants = scan(
        dtable(
            "plants",
            vec![f("name", Utf8), f("kind", Utf8), f("height_m", Float64)],
            "Plants table for data_version 2.0.0 and 3.0.0",
        ),
        "versioned_tables_plants_scan",
    );
    let mut version_schemas = HashMap::new();
    version_schemas.insert("1.0.0".to_string(), vec![main(vec![animals.clone()])]);
    version_schemas.insert("1.1.0".to_string(), vec![main(vec![animals_color.clone()])]);
    version_schemas.insert(
        "2.0.0".to_string(),
        vec![main(vec![animals.clone(), plants.clone()])],
    );
    version_schemas.insert("3.0.0".to_string(), vec![main(vec![plants.clone()])]);
    CatalogModel {
        name: "versioned_tables".to_string(),
        source_url: None,
        implementation_version: Some("11.0.0".to_string()),
        data_version_spec: Some(">=1.0.0,<4.0.0".to_string()),
        supported_data_versions: vec![
            "1.0.0".to_string(),
            "1.1.0".to_string(),
            "2.0.0".to_string(),
            "3.0.0".to_string(),
        ],
        default_data_version: Some("3.0.0".to_string()),
        supported_implementation_versions: vec![
            "10.0.0".to_string(),
            "10.1.0".to_string(),
            "11.0.0".to_string(),
        ],
        npm_version_resolution: true,
        attach_option_specs: Vec::new(),
        attach_options_default_batch: None,
        version_schemas,
        comment: Some(
            "Catalog whose visible tables depend on the resolved data version".to_string(),
        ),
        tags: Vec::new(),
        supports_time_travel: false,
        schemas: vec![main(Vec::new())],
    }
}

/// Select the catalog model by the `VGI_WORKER_CATALOG_NAME` env (default
/// `example`), mirroring vgi-java's single-binary + wrapper approach.
pub fn build_by_name(name: &str) -> CatalogModel {
    match name {
        "versioned" => versioned(),
        "versioned_tables" => versioned_tables(),
        _ => build(),
    }
}

pub fn build() -> CatalogModel {
    CatalogModel {
        name: "example".to_string(),
        source_url: None,
        implementation_version: None,
        data_version_spec: None,
        supported_data_versions: Vec::new(),
        default_data_version: None,
        npm_version_resolution: false,
        attach_option_specs: Vec::new(),
        attach_options_default_batch: None,
        supported_implementation_versions: Vec::new(),
        version_schemas: std::collections::HashMap::new(),
        comment: Some("Example VGI catalog for testing".to_string()),
        tags: vec![
            ("source".to_string(), "vgi-fixture-worker".to_string()),
            ("version".to_string(), "1".to_string()),
        ],
        supports_time_travel: true,
        schemas: vec![
            CatSchema {
                name: "main".to_string(),
                comment: Some("Example functions for testing VGI".to_string()),
                tags: Vec::new(),
                views: vec![
                    CatView {
                        comment: Some("First 10 integers".to_string()),
                        tags: kv(&[("layer", "demo"), ("origin", "sequence")]),
                        column_comments: kv(&[("n", "Sequence index 0..9")]),
                        ..view("first_ten", "SELECT * FROM sequence(10)")
                    },
                    CatView {
                        comment: Some("Even numbers from 0 to 98".to_string()),
                        ..view(
                            "even_numbers",
                            "SELECT * FROM sequence(100) WHERE n % 2 = 0",
                        )
                    },
                ],
                macros: vec![
                    CatMacro {
                        parameter_docs: vec![
                            ("x".to_string(), "Left operand".to_string()),
                            ("y".to_string(), "Right operand".to_string()),
                        ],
                        ..smacro("vgi_multiply", &["x", "y"], "x * y")
                    },
                    CatMacro {
                        defaults: vec![("lo".to_string(), 0), ("hi".to_string(), 100)],
                        ..smacro(
                            "vgi_clamp",
                            &["val", "lo", "hi"],
                            "GREATEST(lo, LEAST(hi, val))",
                        )
                    },
                    tmacro("vgi_range_table", &["n"], "SELECT * FROM range(n)"),
                ],
                tables: vec![],
            },
            CatSchema {
                name: "data".to_string(),
                comment: Some("Example tables backed by functions".to_string()),
                tags: Vec::new(),
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

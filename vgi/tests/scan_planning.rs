// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Protocol 3.2 filtered scan planning: the worker-side surface, driven by
//! vgi-python's golden `scan_planning_*` conformance records.

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::Date32Array;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use base64::Engine;
use vgi::catalog::{table_info, CatBranch, CatTable, ScanPlanning};
use vgi::protocol::dtos::{ScanBranch, TableInfo};
use vgi::scan_planning::{ScanBranchesPlan, ScanPlanRequest, ScanPlanningFilters};
use vgi::statistics::{deserialize_column_statistics, CatColStat, StatValue};

fn fixture(name: &str) -> Vec<u8> {
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("../../conformance/catalog-contracts.json")).unwrap();
    base64::engine::general_purpose::STANDARD
        .decode(corpus["records"][name].as_str().unwrap())
        .unwrap()
}

fn record<T: vgi::vgi_rpc::VgiArrow>(name: &str) -> T {
    let bytes = fixture(name);
    let batch = arrow_ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    vgi::wire::from_batch(&batch).unwrap()
}

/// The golden table's declared columns: `(event_date date32, n int64)`.
fn golden_columns() -> SchemaRef {
    let table: TableInfo = record("scan_planning_table_info");
    vgi::ipc::read_schema(&table.columns.0).unwrap()
}

/// `WHERE event_date >= DATE '2026-10-03' AND n < 100`.
fn golden_filters() -> ScanPlanningFilters {
    ScanPlanningFilters::new(fixture("scan_planning_pushdown_filters"), golden_columns())
}

const OCT_3: i32 = 20_729; // 2026-10-03 as days since the epoch

fn date(days: i32) -> StatValue {
    StatValue::arrow(Arc::new(Date32Array::from(vec![days])))
}

fn stat(name: &str, min: StatValue, max: StatValue) -> CatColStat {
    CatColStat {
        column_name: name.to_string(),
        min,
        max,
        has_null: false,
        has_not_null: true,
        distinct_count: None,
        contains_unicode: None,
        max_string_length: None,
    }
}

#[test]
fn golden_filters_decode_against_the_declared_columns() {
    let filters = golden_filters();
    assert_eq!(filters.raw(), fixture("scan_planning_pushdown_filters"));
    let decoded = filters.decode().unwrap();
    assert!(decoded.has_filter_for_column("event_date"));
    assert!(decoded.has_filter_for_column("n"));
    // Decoding is cached: the second call returns the same state.
    assert!(std::ptr::eq(decoded, filters.decode().unwrap()));
}

#[test]
fn golden_branch_statistics_may_match_the_golden_filters() {
    let branch: ScanBranch = record("scan_planning_branch");
    let stats = deserialize_column_statistics(&branch.column_statistics.unwrap().0).unwrap();
    assert_eq!(stats.len(), 2);
    assert!(matches!(&stats[0].min, StatValue::Arrow(v) if v.data_type() == &DataType::Date32));
    assert!(matches!(stats[1].min, StatValue::Int64(0)));
    assert!(matches!(stats[1].max, StatValue::Int64(999)));
    // 2026-10-03 satisfies event_date >= 2026-10-03; n in [0, 999] overlaps n < 100.
    assert!(golden_filters().may_match(&stats).unwrap());
}

#[test]
fn may_match_prunes_only_provably_excluded_candidates() {
    let filters = golden_filters();
    let may = |stats: &[CatColStat]| filters.may_match(stats).unwrap();

    // event_date >= 2026-10-03
    assert!(!may(&[stat(
        "event_date",
        date(OCT_3 - 2),
        date(OCT_3 - 1)
    )]));
    assert!(may(&[stat("event_date", date(OCT_3 - 1), date(OCT_3))]));
    assert!(may(&[stat("event_date", date(OCT_3 + 5), date(OCT_3 + 9))]));
    // n < 100 (exclusive)
    assert!(!may(&[stat(
        "n",
        StatValue::Int64(100),
        StatValue::Int64(500)
    )]));
    assert!(may(&[stat(
        "n",
        StatValue::Int64(99),
        StatValue::Int64(500)
    )]));
    // Any one excluded column excludes the candidate.
    assert!(!may(&[
        stat("event_date", date(OCT_3), date(OCT_3)),
        stat("n", StatValue::Int64(100), StatValue::Int64(200)),
    ]));
    // Entirely NULL: a comparison never matches NULL.
    let all_null = CatColStat {
        has_not_null: false,
        has_null: true,
        ..stat("n", StatValue::Int64(0), StatValue::Int64(0))
    };
    assert!(!may(&[all_null]));
    // A column the table does not declare, or no statistics at all: keep.
    assert!(may(&[stat(
        "other",
        StatValue::Int64(0),
        StatValue::Int64(1)
    )]));
    assert!(may(&[]));
    // A bound that only one side knows still prunes on that side.
    let unknown_min = stat(
        "n",
        StatValue::arrow(arrow_array::new_null_array(&DataType::Int64, 1)),
        StatValue::Int64(50),
    );
    assert!(may(std::slice::from_ref(&unknown_min)));
    let unknown_max = stat(
        "n",
        StatValue::Int64(100),
        StatValue::arrow(arrow_array::new_null_array(&DataType::Int64, 1)),
    );
    assert!(!may(&[unknown_max]));
    // A bound of another type converts only when the conversion is lossless.
    assert!(!may(&[stat(
        "n",
        StatValue::Float64(100.0),
        StatValue::Float64(200.0)
    )]));
    assert!(may(&[stat(
        "n",
        StatValue::Float64(99.5),
        StatValue::Float64(200.0)
    )]));
    assert!(!may(&[stat(
        "event_date",
        StatValue::Utf8("2026-10-01".into()),
        StatValue::Utf8("2026-10-02".into()),
    )]));
    // A type that cannot convert is unknown, so the candidate is kept.
    assert!(may(&[stat(
        "event_date",
        StatValue::Binary(vec![1, 2]),
        StatValue::Binary(vec![3, 4]),
    )]));
}

#[test]
fn may_match_honours_nulls_against_is_null_predicates() {
    // `n IS NULL`, as a hand-written Filter Encoding v2 snapshot.
    let columns: SchemaRef = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let document = r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"q","revision":0,"mode":"advisory","source":"query","expression":{"node":"is_null","expression":{"node":"column_ref","column_index":0,"column_name":"n"},"negated":false}}]}"#;
    let batch = arrow_array::RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            vec![Field::new("filter_spec", DataType::Utf8, false)],
            std::collections::HashMap::from([
                (
                    "vgi_filter_encoding".to_string(),
                    "vgi.filters.v2".to_string(),
                ),
                ("vgi_filter_version".to_string(), "2".to_string()),
                (
                    "vgi_evaluation_context".to_string(),
                    "vgi.none.v1".to_string(),
                ),
            ]),
        )),
        vec![Arc::new(arrow_array::StringArray::from(vec![document]))],
    )
    .unwrap();
    let filters = ScanPlanningFilters::new(vgi::ipc::write_batch(&batch).unwrap(), columns);
    let no_nulls = stat("n", StatValue::Int64(1), StatValue::Int64(2));
    assert!(!filters.may_match(std::slice::from_ref(&no_nulls)).unwrap());
    let some_nulls = CatColStat {
        has_null: true,
        ..no_nulls
    };
    assert!(filters.may_match(&[some_nulls]).unwrap());
}

#[test]
fn filtered_tables_serve_scan_planning_without_an_inlined_scan() {
    let columns = Arc::new(Schema::new(vec![
        Field::new("event_date", DataType::Date32, true),
        Field::new("n", DataType::Int64, true),
    ]));
    let base = CatTable::new("events", columns, "sequence", Vec::new(), None, None);
    let bind = table_info(&["main".to_string()], &base, None).unwrap();
    assert_eq!(bind.scan_planning, "bind");
    assert!(bind.scan_function.is_some());

    let mut inline = base.clone();
    inline.inline_scan = true;
    let filtered = inline
        .with_filtered_scan_planning(|request: &ScanPlanRequest<'_>| Ok(request.default_plan()));
    assert_eq!(filtered.scan_planning, ScanPlanning::Filtered);
    assert!(!filtered.inline_scan);
    let info = table_info(&["main".to_string()], &filtered, None).unwrap();
    assert_eq!(info.scan_planning, "filtered");
    assert!(info.scan_function.is_none());
    info.validate_scan_planning().unwrap();

    let mut bad = filtered.clone();
    bad.inline_scan = true;
    let error = table_info(&["main".to_string()], &bad, None).unwrap_err();
    assert!(error.message.contains("inline_scan"), "{error}");

    assert_eq!(
        ScanPlanning::parse("filtered").unwrap(),
        ScanPlanning::Filtered
    );
    assert_eq!(ScanPlanning::parse("bind").unwrap(), ScanPlanning::Bind);
    assert!(ScanPlanning::parse("Filtered").is_err());
}

#[test]
fn default_plan_wraps_the_scan_function_or_the_declared_branches() {
    let columns = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let mut table = CatTable::new("t", columns, "sequence", vec![1, 2], None, None);
    table.required_extensions = vec!["parquet".to_string()];
    let path = vec!["main".to_string()];
    let request = |table: &CatTable| -> ScanBranchesPlan {
        ScanPlanRequest {
            attach_opaque_data: None,
            transaction_opaque_data: None,
            schema_path: &path,
            table,
            at_unit: None,
            at_value: None,
            pushdown_filters: None,
        }
        .default_plan()
    };
    let single = request(&table);
    assert_eq!(single.branches.len(), 1);
    assert_eq!(single.branches[0].function_name, "sequence");
    assert_eq!(single.branches[0].scan_arguments, vec![1, 2]);
    assert_eq!(single.required_extensions, ["parquet"]);

    table.branches = Some(vec![CatBranch {
        function_name: "read_parquet".to_string(),
        row_count: Some(5),
        column_statistics: Some(vec![stat("n", StatValue::Int64(1), StatValue::Int64(5))]),
        ..Default::default()
    }]);
    let declared = request(&table);
    assert_eq!(declared.branches[0].row_count, Some(5));
    table.branches = Some(Vec::new());
    assert!(request(&table).branches.is_empty());
}

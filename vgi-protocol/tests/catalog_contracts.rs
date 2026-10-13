// Copyright 2025, 2026 Query Farm LLC - https://query.farm

use std::io::Cursor;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use base64::Engine;
use vgi_protocol::generated::protocol_types::{
    CatalogCapabilities, OptionChanges, OptionDefinitions, OptionState, RequiredFilterPaths,
    SortKey, SqlExpression, TableLayout, TableLayoutChange,
};
use vgi_protocol::wire::{from_batch, to_batch};
use vgi_rpc::VgiArrow;

fn fixture(name: &str) -> Vec<u8> {
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("../../conformance/catalog-contracts.json")).unwrap();
    base64::engine::general_purpose::STANDARD
        .decode(corpus["records"][name].as_str().unwrap())
        .unwrap()
}

fn round_trip<T: VgiArrow>(name: &str) -> T {
    let mut source = StreamReader::try_new(Cursor::new(fixture(name)), None).unwrap();
    let value: T = from_batch(&source.next().unwrap().unwrap()).unwrap();
    assert!(source.next().is_none());
    let batch = to_batch(value).unwrap();
    let mut encoded = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut encoded, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let mut reader = StreamReader::try_new(Cursor::new(encoded), None).unwrap();
    from_batch(&reader.next().unwrap().unwrap()).unwrap()
}

#[test]
fn options_preserve_python_payloads_and_explicit_null_overrides() {
    let changes: OptionChanges = round_trip("changes");
    assert_eq!(changes.set_values.unwrap().0, fixture("values"));
    assert_eq!(changes.reset_names, ["reset.me"]);
    let reset: OptionChanges = round_trip("reset_only");
    assert!(reset.set_values.is_none());
    assert_eq!(reset.reset_names, ["reset.me"]);
    let state: OptionState = round_trip("state");
    assert_eq!(state.values.0, fixture("values"));
    assert_eq!(state.explicit_names, ["optional"]);
    let empty: OptionState = round_trip("empty_state");
    assert_eq!(empty.values.0, fixture("empty_values"));
    assert!(empty.explicit_names.is_empty());
}

#[test]
fn paths_preserve_literal_names_and_or_groups() {
    let paths: RequiredFilterPaths = round_trip("paths");
    assert_eq!(
        paths.groups,
        vec![
            vec![vec!["a.b"], vec!["a", "b"]],
            vec![vec!["record", "literal.\"name"]]
        ]
    );
    let empty: RequiredFilterPaths = round_trip("empty_paths");
    assert!(empty.groups.is_empty());
}

#[test]
fn capability_profiles_and_option_definitions_round_trip() {
    let capabilities: CatalogCapabilities = round_trip("capabilities");
    assert_eq!(capabilities.read, ["vgi.schema.options.read.v1"]);
    assert_eq!(capabilities.ddl, ["vgi.schema.options.create.v1"]);
    assert!(capabilities.write.is_empty());
    let empty: CatalogCapabilities = round_trip("empty_capabilities");
    assert!(
        empty.read.is_empty()
            && empty.ddl.is_empty()
            && empty.write.is_empty()
            && empty.transaction.is_empty()
    );
    let definitions: OptionDefinitions = round_trip("definitions");
    assert_eq!(definitions.defaults.0, fixture("values"));
    assert_eq!(definitions.create_names, ["optional", "record"]);
    assert_eq!(definitions.alter_names, ["optional"]);
}

#[test]
fn layouts_preserve_keys_orders_and_clear_versus_unchanged() {
    let expr: SqlExpression = round_trip("expression");
    assert_eq!(expr.schema_path, ["one.two", "nested"]);
    let key: SortKey = round_trip("sort_key");
    assert_eq!(key.expression.0, fixture("expression"));
    assert_eq!(key.direction.0, "DESC");
    assert_eq!(key.null_order.0, "FIRST");
    let layout: TableLayout = round_trip("layout");
    assert_eq!(layout.partition_by[0].0, fixture("expression"));
    assert_eq!(layout.sorted_by[0].0, fixture("sort_key"));
    assert_eq!(layout.coverage.0, "ALL_ROWS");
    let empty: TableLayout = round_trip("empty_layout");
    assert!(empty.partition_by.is_empty() && empty.sorted_by.is_empty());
    let change: TableLayoutChange = round_trip("layout_change");
    assert_eq!(change.mode.0, "REWRITE");
    assert!(change.partition_by.unwrap().is_empty());
    assert_eq!(change.sorted_by.unwrap()[0].0, fixture("sort_key"));
    let clear: TableLayoutChange = round_trip("layout_clear");
    assert_eq!(clear.mode.0, "FUTURE_WRITES");
    assert!(clear.partition_by.is_none());
    assert!(clear.sorted_by.unwrap().is_empty());
}

#[test]
fn table_records_preserve_optional_metadata() {
    use vgi_protocol::protocol::dtos::{TableCreateRequest, TableInfo};
    let create: TableCreateRequest = round_trip("table_create");
    assert_eq!(create.options.unwrap().0, fixture("values"));
    assert_eq!(create.layout.unwrap().0, fixture("layout"));
    assert_eq!(create.comment.as_deref(), Some("kept"));
    let old: TableCreateRequest = round_trip("table_create_legacy");
    assert!(
        old.comment.is_none()
            && old.tags.is_none()
            && old.options.is_none()
            && old.layout.is_none()
    );
    let info: TableInfo = round_trip("table_info");
    assert_eq!(info.options.unwrap().0, fixture("state"));
    assert_eq!(info.layout.unwrap().0, fixture("layout"));
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

use std::io::Cursor;

use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use base64::Engine;
use vgi_protocol::generated::protocol_types::{
    CatalogCapabilities, CatalogMetadataChanges, CatalogObjectRef, DependencySnapshot,
    ObjectDependency, OptionChanges, OptionDefinitions, OptionState, RequiredFilterPaths, SortKey,
    SqlExpression, TableLayout, TableLayoutChange,
};
use vgi_protocol::wire::{from_batch, to_batch};
use vgi_rpc::VgiArrow;

#[test]
fn query_parameter_batches_preserve_mapping_metadata_and_empty_rows() {
    use arrow_array::Array;
    use arrow_schema::DataType;
    for name in ["query_parameters", "query_empty_parameters"] {
        let mut reader = StreamReader::try_new(Cursor::new(fixture(name)), None).unwrap();
        let original = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &original.schema()).unwrap();
            writer.write(&original).unwrap();
            writer.finish().unwrap();
        }
        let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
        let restored = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        assert_eq!(restored, original);
        assert_eq!(restored.num_rows(), 1);
        assert_eq!(
            restored.schema().metadata()["vgi.query.parameters.v1"],
            if name == "query_empty_parameters" {
                "positional"
            } else {
                "named"
            }
        );
        if name == "query_empty_parameters" {
            assert_eq!(restored.num_columns(), 0);
        } else {
            assert!(restored.column(0).is_null(0));
            let schema = restored.schema();
            let DataType::Struct(fields) = schema.field(1).data_type() else {
                panic!("struct parameter lost")
            };
            assert_eq!(fields[0].metadata()["nested"], "retained");
        }
    }
}

#[test]
fn column_request_params_preserve_raw_targets_and_nullable_fields() {
    use vgi_protocol::generated::request_params::*;
    macro_rules! check {
        ($ty:ty, $operation:literal) => {
            for (variant, path) in [
                ("literal", vec!["a.b"]),
                ("nested", vec!["a", "b.\"quoted"]),
            ] {
                let name = format!("{}_{}", $operation, variant);
                let value: $ty = round_trip(&name);
                assert_eq!(value.column_path, path);
                assert_eq!(value.schema_path, ["one.two", "nested"]);
                assert_eq!(value.transaction_opaque_data.is_some(), variant == "nested");
                let original = StreamReader::try_new(Cursor::new(fixture(&name)), None)
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap();
                assert_eq!(to_batch(value).unwrap(), original, "{name}");
            }
        };
    }
    check!(CatalogTableColumnAddParams, "column_add");
    check!(CatalogTableColumnDropParams, "column_drop");
    check!(CatalogTableColumnRenameParams, "column_rename");
    check!(CatalogTableColumnTypeChangeParams, "column_type_change");
    check!(CatalogTableColumnDefaultSetParams, "column_default_set");
    check!(CatalogTableColumnDefaultDropParams, "column_default_drop");
    check!(CatalogTableColumnCommentSetParams, "column_comment_set");
    check!(CatalogTableNotNullSetParams, "not_null_set");
    check!(CatalogTableNotNullDropParams, "not_null_drop");
}

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

#[test]
fn views_preserve_ordered_raw_aliases() {
    use vgi_protocol::protocol::dtos::ViewInfo;
    let view: ViewInfo = round_trip("view_info");
    assert_eq!(view.aliases, ["a.b", "with\"quote"]);
    let implicit: ViewInfo = round_trip("view_info_implicit");
    assert!(implicit.aliases.is_empty());
}

#[test]
fn view_security_survives_codec_round_trip() {
    use vgi_protocol::protocol::dtos::ViewInfo;
    let regular: ViewInfo = round_trip("view_info");
    let barrier: ViewInfo = round_trip("view_info_barrier");
    assert_eq!(regular.security, "regular");
    assert_eq!(barrier.security, "barrier");
}

#[test]
fn metadata_patches_preserve_comment_presence_and_literal_tags() {
    let patch: CatalogMetadataChanges = round_trip("metadata_patch");
    assert!(patch.set_comment);
    assert_eq!(patch.comment.as_deref(), Some(""));
    assert_eq!(patch.remove_tags, ["old", "OLD"]);
    assert_eq!(
        patch.set_tags,
        vec![
            ("a.b".into(), "".into()),
            ("__proto__".into(), "literal".into()),
            ("".into(), "empty-key".into())
        ]
    );
    let clear: CatalogMetadataChanges = round_trip("metadata_clear_comment");
    assert!(clear.set_comment && clear.comment.is_none() && clear.set_tags.is_empty());
    let noop: CatalogMetadataChanges = round_trip("metadata_noop");
    assert!(
        !noop.set_comment
            && noop.comment.is_none()
            && noop.set_tags.is_empty()
            && noop.remove_tags.is_empty()
    );
}

#[test]
fn dependency_records_preserve_nested_identity_and_graph_payloads() {
    let table: CatalogObjectRef = round_trip("object_ref");
    assert_eq!(table.schema_path, ["one.two", "nested"]);
    assert_eq!(table.name, "table.\"name");
    assert!(table.owner_table.is_none());
    let trigger: CatalogObjectRef = round_trip("trigger_ref");
    assert_eq!(trigger.owner_table.as_deref(), Some(table.name.as_str()));
    let edge: ObjectDependency = round_trip("object_dependency");
    assert_eq!(edge.kind, "owned");
    assert_eq!(edge.subject, vgi_rpc::Bytes(fixture("object_ref")));
    assert_eq!(edge.dependent, vgi_rpc::Bytes(fixture("trigger_ref")));
    let graph: DependencySnapshot = round_trip("dependency_snapshot");
    assert_eq!(graph.catalog_version, 42);
    assert_eq!(graph.objects.len(), 4);
    assert_eq!(graph.dependencies.len(), 3);
    let empty: DependencySnapshot = round_trip("empty_dependency_snapshot");
    assert!(empty.objects.is_empty() && empty.dependencies.is_empty());
}

#[test]
fn query_records_preserve_nested_contracts() {
    use vgi_protocol::generated::protocol_types::*;
    let _: ClientQueryCapabilities = round_trip("query_offer");
    let _: QueryCapabilities = round_trip("query_capabilities");
    let _: QueryEvaluationContext = round_trip("query_context");
    let _: QueryObjectReference = round_trip("query_object_reference");
    let _: CatalogQueryPrepareRequest = round_trip("query_prepare");
    let _: CatalogQueryPrepareResult = round_trip("query_accepted");
    let _: CatalogQueryPrepareResult = round_trip("query_declined");
    let request: CatalogQueryPrepareRequest = round_trip("query_prepare");
    assert_eq!(request.evaluation_context.timezone, "UTC");
    assert_eq!(
        request.referenced_objects[0].object.schema_path,
        ["one.two", "nested"]
    );
    assert_eq!(request.catalog_version, Some(0));
    let declined: CatalogQueryPrepareResult = round_trip("query_declined");
    assert!(declined.prepared_query_token.is_none());
}

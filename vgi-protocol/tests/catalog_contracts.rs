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
fn typed_indexes_preserve_raw_paths_context_and_options() {
    use vgi_protocol::generated::protocol_types::{IndexDefinition, IndexKey};
    let column: IndexKey = round_trip("index_key_column");
    let nested: IndexKey = round_trip("index_key_nested");
    assert_eq!(column.column_path.unwrap(), ["a.b"]);
    assert_eq!(nested.column_path.unwrap(), ["record", "a.b"]);
    let expression: IndexKey = round_trip("index_key_expression");
    assert!(expression.expression.is_some());
    let definition: IndexDefinition = round_trip("index_definition");
    let _: IndexDefinition = round_trip("index_definition_primary");
    assert_eq!(definition.keys[0].0, fixture("index_key_column"));
    assert_eq!(definition.keys[1].0, fixture("index_key_expression"));
    assert_eq!(definition.options.0, fixture("values"));
    let info: vgi_protocol::protocol::dtos::IndexInfo = round_trip("index_info");
    assert_eq!(info.definition.0, fixture("index_definition"));
    assert_eq!(info.options.0, fixture("state"));
    for name in ["index_create", "index_create_cross_schema"] {
        let source = StreamReader::try_new(Cursor::new(fixture(name)), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(
            source.schema(),
            vgi_protocol::generated::protocol_schemas::index_create_request_schema()
        );
        let definition = source
            .column_by_name("definition")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();
        assert_eq!(definition.value(0), fixture("index_definition"));
    }
}

#[test]
fn column_expression_metadata_retains_ipc_bytes() {
    let mut reader =
        StreamReader::try_new(Cursor::new(fixture("column_expression_metadata")), None).unwrap();
    let source = reader.next().unwrap().unwrap();
    let mut encoded = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut encoded, &source.schema()).unwrap();
        writer.write(&source).unwrap();
        writer.finish().unwrap();
    }
    let batch = StreamReader::try_new(Cursor::new(encoded), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(batch, source);
    for (index, key, name) in [
        (0, "vgi.column.default.v1", "column_default_expression"),
        (1, "vgi.column.generated.v1", "column_generated_expression"),
    ] {
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(&batch.schema().field(index).metadata()[key])
                .unwrap(),
            fixture(name)
        );
    }
    assert_eq!(
        batch.schema().field(1).metadata()["vgi.column.generation.v1"],
        "virtual"
    );
}

#[test]
fn canonical_constraints_preserve_all_fields_through_typed_codecs() {
    use vgi_protocol::generated::protocol_types::*;
    fn check<T: VgiArrow>(name: &str) {
        let value: T = round_trip(name);
        let original = StreamReader::try_new(Cursor::new(fixture(name)), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(to_batch(value).unwrap(), original, "{name}");
    }
    check::<SqlStatement>("sql_statement");
    check::<CatalogObjectRef>("constraint_ref");
    check::<ForeignKeyDefinition>("foreign_key_definition");
    check::<ConstraintDefinition>("constraint_definition_not_null");
    check::<ConstraintDefinition>("constraint_definition_unique");
    check::<ConstraintDefinition>("constraint_definition_primary");
    check::<ConstraintDefinition>("constraint_definition_check");
    check::<ConstraintDefinition>("constraint_definition_foreign");
    check::<ConstraintInfo>("constraint_info");
    check::<ConstraintInfo>("constraint_info_untrusted");
    check::<ConstraintChange>("constraint_change_add");
    check::<ConstraintChange>("constraint_change_drop");
    check::<ConstraintChange>("constraint_change_rename");
    check::<ConstraintChange>("constraint_change_unname");
    check::<ConstraintChange>("constraint_change_replace");
    check::<ConstraintSnapshot>("constraint_snapshot");
    check::<ConstraintSnapshot>("constraint_snapshot_empty");
    check::<ConstraintsApplyRequest>("constraints_apply");
    check::<ConstraintsApplyRequest>("constraints_apply_autocommit");
}

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

#[test]
fn column_expressions_preserve_canonical_payload_and_absent_using() {
    use vgi_protocol::generated::request_params::{
        CatalogTableColumnDefaultSetParams, CatalogTableColumnTypeChangeParams,
    };
    for variant in ["literal", "nested"] {
        let default_params: CatalogTableColumnDefaultSetParams =
            round_trip(&format!("column_default_set_{variant}"));
        let batch = StreamReader::try_new(Cursor::new(default_params.expression.0), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let expression: SqlExpression = from_batch(&batch).unwrap();
        assert_eq!(expression.schema_path, default_params.schema_path);
        assert!(!expression.profile.is_empty());
        let using: CatalogTableColumnTypeChangeParams =
            round_trip(&format!("column_type_change_{variant}"));
        if variant == "literal" {
            assert!(using.expression.is_none());
        } else {
            let batch = StreamReader::try_new(Cursor::new(using.expression.unwrap().0), None)
                .unwrap()
                .next()
                .unwrap()
                .unwrap();
            let expression: SqlExpression = from_batch(&batch).unwrap();
            assert_eq!(expression.schema_path, using.schema_path);
        }
    }
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
    assert_eq!(create.constraint_definitions.len(), 1);
    assert!(old.constraint_definitions.is_empty());
    assert_eq!(info.definition_revision.unwrap().0, b"revision\0\xff");
    assert_eq!(info.constraints.len(), 1);
    fn nested<T: VgiArrow>(bytes: &[u8]) -> T {
        let batch = StreamReader::try_new(Cursor::new(bytes), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        from_batch(&batch).unwrap()
    }
    let constraint: vgi_protocol::generated::protocol_types::ConstraintInfo =
        nested(&info.constraints[0].0);
    assert_eq!(constraint.definition.0, create.constraint_definitions[0].0);
    let reference: CatalogObjectRef = nested(&constraint.r#ref.0);
    assert_eq!(reference.name, "fixture-stable-key");
    let definition: vgi_protocol::generated::protocol_types::ConstraintDefinition =
        nested(&constraint.definition.0);
    assert_eq!(
        definition.column_paths,
        vec![vec!["record".to_string(), "a.b".to_string()]]
    );
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
    assert!(edge.blocks_alter);
    assert!(edge.subject_column_paths.is_none());
    assert!(edge.dependent_column_path.is_none());
    let membership: ObjectDependency = round_trip("dependency_membership");
    assert!(!membership.blocks_alter);
    assert!(membership.subject_column_paths.is_none());
    assert!(membership.dependent_column_path.is_none());
    let literal: ObjectDependency = round_trip("dependency_column_literal");
    assert!(literal.blocks_alter);
    assert_eq!(literal.subject_column_paths.unwrap(), [vec!["a.b"]]);
    assert!(literal.dependent_column_path.is_none());
    let nested: ObjectDependency = round_trip("dependency_column_nested");
    assert_eq!(
        nested.subject_column_paths.unwrap(),
        [vec!["record", "a.b"]]
    );
    let generated: ObjectDependency = round_trip("dependency_generated");
    assert_eq!(generated.dependent, generated.subject);
    assert_eq!(generated.dependent_column_path.unwrap(), ["generated"]);
    assert_eq!(
        generated.subject_column_paths.unwrap(),
        [vec!["a.b"], vec!["record", "a.b"]]
    );
    let chain: ObjectDependency = round_trip("dependency_generated_chain");
    assert_eq!(chain.dependent_column_path.unwrap(), ["second.generated"]);
    assert_eq!(chain.subject_column_paths.unwrap(), [vec!["generated"]]);
    let columns: DependencySnapshot = round_trip("dependency_column_snapshot");
    assert_eq!(
        columns.dependencies,
        [
            vgi_rpc::Bytes(fixture("dependency_column_literal")),
            vgi_rpc::Bytes(fixture("dependency_generated")),
            vgi_rpc::Bytes(fixture("dependency_generated_chain"))
        ]
    );

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

#[test]
fn macro_parameter_and_overload_codecs_preserve_typed_defaults_and_expressions() {
    use vgi_protocol::generated::protocol_types::{MacroOverload, MacroParameter};
    for name in [
        "macro_parameter",
        "macro_parameter_expression",
        "macro_parameter_untyped",
    ] {
        let original = StreamReader::try_new(Cursor::new(fixture(name)), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let restored: MacroParameter = round_trip(name);
        assert_eq!(to_batch(restored).unwrap(), original, "{name}");
    }
    let parameter: MacroParameter = round_trip("macro_parameter");
    assert!(parameter.declared_type.is_some() && parameter.default_value.is_some());
    assert!(parameter.default_expression.is_none());
    let parameter: MacroParameter = round_trip("macro_parameter_expression");
    assert!(parameter.declared_type.is_some() && parameter.default_expression.is_some());
    assert!(parameter.default_value.is_none());
    let parameter: MacroParameter = round_trip("macro_parameter_untyped");
    assert!(parameter.declared_type.is_none() && parameter.default_value.is_none());
    let overload: MacroOverload = round_trip("macro_overload");
    assert_eq!(
        overload
            .parameters
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        ["x", "fallback", "scale"]
    );
    let original = StreamReader::try_new(Cursor::new(fixture("macro_overload")), None)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(to_batch(overload).unwrap(), original);
}

#[test]
fn logical_type_and_opaque_macro_batches_preserve_metadata_and_variant_bytes() {
    use arrow_array::{Array, BinaryArray, StructArray};
    use arrow_schema::DataType;
    for name in [
        "logical_tuple",
        "logical_struct",
        "logical_variant_sql_null",
        "logical_variant_null",
        "logical_variant_absent",
        "logical_variant_null_member",
        "macro_create",
        "macro_create_legacy",
        "macro_info",
        "macro_info_legacy",
    ] {
        let original = StreamReader::try_new(Cursor::new(fixture(name)), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, &original.schema()).unwrap();
            writer.write(&original).unwrap();
            writer.finish().unwrap();
        }
        let restored = StreamReader::try_new(Cursor::new(bytes), None)
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(restored, original, "{name}");
        let schema = restored.schema();
        let marker = schema.field(0).metadata().get("vgi.logical_type.v1");
        if name == "logical_tuple" {
            assert_eq!(marker.map(String::as_str), Some("tuple"));
        }
        if name == "logical_struct" {
            assert!(marker.is_none());
        }
        if name.starts_with("logical_variant_") {
            let field = schema.field(0);
            assert!(marker.is_none());
            assert_eq!(
                field
                    .metadata()
                    .get("ARROW:extension:name")
                    .map(String::as_str),
                Some("arrow.parquet.variant")
            );
            assert_eq!(
                field
                    .metadata()
                    .get("ARROW:extension:metadata")
                    .map(String::as_str),
                Some("")
            );
            let DataType::Struct(fields) = field.data_type() else {
                panic!("variant carrier is not a struct");
            };
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0].name(), "metadata");
            assert!(!fields[0].is_nullable());
            assert_eq!(fields[1].name(), "value");
            assert!(fields[1].is_nullable());
            assert_eq!(
                restored.column(0).is_null(0),
                name == "logical_variant_sql_null"
            );
            if name != "logical_variant_sql_null" {
                let column = restored
                    .column(0)
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap();
                let (metadata, value): (&[u8], &[u8]) = match name {
                    "logical_variant_null" => (&[1, 0, 0], &[0]),
                    "logical_variant_absent" => (&[1, 0, 0], &[2, 0, 0]),
                    _ => (
                        &[1, 1, 0, 7, b'p', b'r', b'e', b's', b'e', b'n', b't'],
                        &[2, 1, 0, 0, 1, 0],
                    ),
                };
                assert_eq!(
                    column
                        .column(0)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .value(0),
                    metadata
                );
                assert_eq!(
                    column
                        .column(1)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .unwrap()
                        .value(0),
                    value
                );
            }
        }
    }
}

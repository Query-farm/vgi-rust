// Copyright 2026 Query Farm LLC - https://query.farm
//! Explicit immutable fixture identities; these declarations claim no enforcement.

use vgi::catalog::CatTable;
use vgi_protocol::generated::protocol_types::{
    CatalogObjectRef, ConstraintDefinition, ConstraintInfo, ForeignKeyDefinition,
    QueryEvaluationContext, SqlExpression,
};
use vgi_rpc::{Bytes, DictString, VgiArrow};

fn record<T: VgiArrow>(value: T) -> Bytes {
    let batch = vgi::wire::to_batch(value).expect("valid fixture record");
    Bytes::from(vgi::ipc::write_batch(&batch).expect("fixture IPC"))
}

fn reference(kind: &str, name: &str, owner: Option<&str>) -> Bytes {
    record(CatalogObjectRef {
        kind: kind.into(),
        schema_path: vec!["data".into()],
        name: name.into(),
        owner_table: owner.map(str::to_string),
    })
}

fn paths(columns: &[&str]) -> Vec<Vec<String>> {
    columns.iter().map(|name| vec![(*name).into()]).collect()
}

fn definition(kind: &str, columns: &[&str]) -> ConstraintDefinition {
    ConstraintDefinition {
        kind: DictString(kind.into()),
        column_paths: paths(columns),
        declared_name: None,
        check_expression: None,
        foreign_key: None,
    }
}

fn info(table: &str, id: &str, definition: ConstraintDefinition, target: Option<Bytes>) -> Bytes {
    record(ConstraintInfo {
        r#ref: reference("constraint", id, Some(table)),
        definition: record(definition),
        enforcement: DictString("METADATA_ONLY".into()),
        validation: DictString("UNVALIDATED".into()),
        referenced_constraint: target,
    })
}

pub fn constraint(table: &str, id: &str, kind: &str, columns: &[&str]) -> Bytes {
    info(table, id, definition(kind, columns), None)
}

pub fn check(table: &str, id: &str, sql: &str) -> Bytes {
    let mut def = definition("CHECK", &[]);
    def.check_expression = Some(record(SqlExpression {
        sql: sql.into(),
        profile: "vgi.expression.duckdb.scalar.v1".into(),
        schema_path: vec!["data".into()],
        evaluation_context: QueryEvaluationContext {
            timezone: "UTC".into(),
            collation: "binary".into(),
            default_order: "asc".into(),
            default_null_order: "nulls_last".into(),
            preserve_insertion_order: true,
        },
    }));
    info(table, id, def, None)
}

pub fn foreign_key(
    table: &str,
    id: &str,
    columns: &[&str],
    target: &str,
    key: &str,
    target_columns: &[&str],
) -> Bytes {
    let mut def = definition("FOREIGN_KEY", columns);
    def.foreign_key = Some(record(ForeignKeyDefinition {
        referenced_table: reference("table", target, None),
        referenced_column_paths: paths(target_columns),
        r#match: DictString("SIMPLE".into()),
        on_update: DictString("NO_ACTION".into()),
        on_delete: DictString("NO_ACTION".into()),
    }));
    info(
        table,
        id,
        def,
        Some(reference("constraint", key, Some(target))),
    )
}

pub fn set_constraints(table: &mut CatTable, constraints: Vec<Bytes>) {
    table.constraints = constraints;
    table.definition_revision = Some(Bytes::from(b"fixture.constraints.v1".to_vec()));
}

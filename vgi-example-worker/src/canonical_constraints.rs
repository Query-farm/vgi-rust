// Copyright 2026 Query Farm LLC - https://query.farm
//! Canonical fixture identities, qualified against the immutable scan arrays.

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

// This is deliberately a finite fixture validator, not a general constraint
// engine. The corresponding tables expose no mutating functions. Publication
// fails if a changed fixture stops satisfying any declared fact.
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{Field, Schema};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use vgi::catalog::CatalogModel;
use vgi_rpc::{Result, RpcError};

fn decode<T: VgiArrow>(raw: &Bytes) -> Result<T> {
    vgi::wire::from_batch(&vgi::ipc::read_batch(&raw.0)?)
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum Value {
    Integer(i64),
    Text(String),
}

fn columns(batch: &RecordBatch, paths: &[Vec<String>]) -> Result<Vec<ArrayRef>> {
    paths
        .iter()
        .map(|path| {
            if path.len() != 1 {
                return Err(RpcError::value_error(
                    "fixture constraint needs a top-level column",
                ));
            }
            batch
                .column_by_name(&path[0])
                .cloned()
                .ok_or_else(|| RpcError::value_error("fixture constraint column is absent"))
        })
        .collect()
}

fn key(columns: &[ArrayRef], row: usize) -> Result<Option<Vec<Value>>> {
    let mut key = Vec::new();
    for column in columns {
        if column.is_null(row) {
            return Ok(None);
        }
        if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            key.push(Value::Integer(values.value(row)));
        } else if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            key.push(Value::Text(values.value(row).to_owned()));
        } else {
            return Err(RpcError::value_error(
                "unsupported immutable fixture key type",
            ));
        }
    }
    Ok(Some(key))
}

fn qualify_records(
    records: &[Bytes],
    batch: &RecordBatch,
    targets: &HashMap<String, RecordBatch>,
) -> Result<(Vec<Bytes>, HashSet<String>)> {
    let mut required = HashSet::new();
    let mut result = Vec::new();
    for raw in records {
        let mut info: ConstraintInfo = decode(raw)?;
        let definition: ConstraintDefinition = decode(&info.definition)?;
        let cols = columns(batch, &definition.column_paths)?;
        match definition.kind.0.as_str() {
            "NOT_NULL" => {
                if cols.len() != 1 || cols[0].null_count() != 0 {
                    return Err(RpcError::value_error("immutable fixture violates NOT_NULL"));
                }
                required.insert(definition.column_paths[0][0].clone());
            }
            "PRIMARY_KEY" | "UNIQUE" => {
                if cols.is_empty() {
                    return Err(RpcError::value_error("empty immutable fixture key"));
                }
                let mut seen = HashSet::new();
                for row in 0..batch.num_rows() {
                    match key(&cols, row)? {
                        Some(value) => {
                            if !seen.insert(value) {
                                return Err(RpcError::value_error(
                                    "immutable fixture violates UNIQUE",
                                ));
                            }
                        }
                        None if definition.kind.0 == "PRIMARY_KEY" => {
                            return Err(RpcError::value_error(
                                "immutable fixture primary key is NULL",
                            ));
                        }
                        _ => (),
                    }
                }
                if definition.kind.0 == "PRIMARY_KEY" {
                    required.extend(definition.column_paths.iter().map(|path| path[0].clone()));
                }
            }
            "CHECK" => {
                let expression: SqlExpression = decode(
                    definition
                        .check_expression
                        .as_ref()
                        .ok_or_else(|| RpcError::value_error("missing fixture CHECK"))?,
                )?;
                let context = expression.evaluation_context;
                if expression.sql != "budget >= 0"
                    || expression.schema_path != ["data"]
                    || expression.profile != "vgi.expression.duckdb.scalar.v1"
                    || context.timezone != "UTC"
                    || context.collation != "binary"
                    || context.default_order != "asc"
                    || context.default_null_order != "nulls_last"
                    || !context.preserve_insertion_order
                {
                    return Err(RpcError::value_error("unsupported immutable fixture CHECK"));
                }
                let values = batch
                    .column_by_name("budget")
                    .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
                    .ok_or_else(|| RpcError::value_error("invalid fixture budget type"))?;
                if values
                    .iter()
                    .flatten()
                    .any(|value| !value.is_finite() || value < 0.0)
                {
                    return Err(RpcError::value_error("immutable fixture violates CHECK"));
                }
            }
            "FOREIGN_KEY" => {
                let fk: ForeignKeyDefinition = decode(
                    definition
                        .foreign_key
                        .as_ref()
                        .ok_or_else(|| RpcError::value_error("missing fixture FK"))?,
                )?;
                let target: CatalogObjectRef = decode(&fk.referenced_table)?;
                let target_key: CatalogObjectRef = decode(
                    info.referenced_constraint
                        .as_ref()
                        .ok_or_else(|| RpcError::value_error("missing referenced key"))?,
                )?;
                if target.kind != "table"
                    || target.schema_path != ["data"]
                    || target.name != "departments"
                    || target_key.owner_table.as_deref() != Some("departments")
                    || target_key.name != "fixture.departments.primary"
                    || fk.r#match.0 != "SIMPLE"
                    || fk.on_update.0 != "NO_ACTION"
                    || fk.on_delete.0 != "NO_ACTION"
                    || fk.referenced_column_paths != [vec!["id".to_string()]]
                {
                    return Err(RpcError::value_error(
                        "unsupported immutable fixture FK target",
                    ));
                }
                let target_batch = targets
                    .get(&target.name)
                    .ok_or_else(|| RpcError::value_error("missing immutable target rows"))?;
                let target_cols = columns(target_batch, &fk.referenced_column_paths)?;
                let mut keys = HashSet::new();
                for row in 0..target_batch.num_rows() {
                    let value = key(&target_cols, row)?
                        .ok_or_else(|| RpcError::value_error("referenced primary key NULL"))?;
                    if !keys.insert(value) {
                        return Err(RpcError::value_error("referenced primary key duplicate"));
                    }
                }
                for row in 0..batch.num_rows() {
                    if let Some(value) = key(&cols, row)? {
                        if !keys.contains(&value) {
                            return Err(RpcError::value_error(
                                "immutable fixture violates FOREIGN_KEY",
                            ));
                        }
                    }
                }
            }
            _ => {
                return Err(RpcError::value_error(
                    "unsupported immutable fixture constraint",
                ));
            }
        }
        info.enforcement = DictString("ENFORCED".into());
        info.validation = DictString("ALL_ROWS".into());
        result.push(record(info));
    }
    Ok((result, required))
}

fn required_schema(schema: &Schema, required: &HashSet<String>) -> Arc<Schema> {
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| {
                field
                    .as_ref()
                    .clone()
                    .with_nullable(!required.contains(field.name()))
            })
            .collect::<Vec<Field>>(),
        schema.metadata().clone(),
    ))
}

pub fn qualify_catalog(model: &mut CatalogModel) -> Result<()> {
    let targets = crate::table::static_scan::fixture_rows();
    for schema in &mut model.schemas {
        for table in &mut schema.tables {
            if table.constraints.is_empty() {
                continue;
            }
            if schema.name != "data" {
                return Err(RpcError::value_error("unexpected fixture constraint owner"));
            }
            if table.name == "versioned_constraints" {
                for version in &mut table.time_travel {
                    let definitions: Vec<Bytes> = table
                        .constraints
                        .iter()
                        .filter_map(|raw| {
                            let info: ConstraintInfo = decode(raw).expect("fixture info");
                            let definition: ConstraintDefinition =
                                decode(&info.definition).expect("fixture definition");
                            if version.version == 1
                                && (definition.kind.0 != "NOT_NULL"
                                    || definition.column_paths != paths(&["id"]))
                                || definition.kind.0 == "UNIQUE" && version.version < 2
                                || definition.kind.0 == "FOREIGN_KEY" && version.version < 3
                            {
                                None
                            } else {
                                Some(raw.clone())
                            }
                        })
                        .collect();
                    let (shape, cols) =
                        crate::table::versioned_scan::VersionedConstraintsScan::build(
                            version.version,
                        );
                    let rows = RecordBatch::try_new(shape, cols)
                        .map_err(|e| RpcError::value_error(e.to_string()))?;
                    let (records, required) = qualify_records(&definitions, &rows, &targets)?;
                    version.columns = required_schema(&version.columns, &required);
                    table.constraint_versions.insert(
                        version.version,
                        (
                            records,
                            Some(Bytes::from(
                                format!("immutable-fixture:v{}", version.version).into_bytes(),
                            )),
                        ),
                    );
                }
                let current = table
                    .time_travel
                    .iter()
                    .max_by_key(|value| value.version)
                    .expect("fixture versions");
                table.columns = current.columns.clone();
                let (records, revision) = table
                    .constraint_versions
                    .get(&current.version)
                    .expect("qualified current");
                table.constraints = records.clone();
                table.definition_revision = revision.clone();
            } else {
                let rows = targets.get(&table.name).ok_or_else(|| {
                    RpcError::value_error("no immutable scan authority for fixture")
                })?;
                let (records, required) = qualify_records(&table.constraints, rows, &targets)?;
                table.constraints = records;
                table.columns = required_schema(&table.columns, &required);
                table.definition_revision = Some(Bytes::from(b"immutable-fixture:v1".to_vec()));
            }
        }
    }
    model.capability_agreement = Some(capability_agreement);
    Ok(())
}

fn capability_agreement(client: Option<&[u8]>) -> Result<(Bytes, Bytes)> {
    use vgi::protocol::dtos::ClientCapabilities;
    use vgi_protocol::generated::protocol_types::CatalogCapabilities;
    let read = vec![
        "vgi.constraint.records.read.v1".to_string(),
        "vgi.expression.duckdb.scalar.v1".to_string(),
    ];
    let mut selected = Vec::new();
    if let Some(raw) = client {
        let client: ClientCapabilities = vgi::wire::from_batch(&vgi::ipc::read_batch(raw)?)?;
        if let Some(raw) = client.catalog {
            let offer: CatalogCapabilities = decode(&raw)?;
            selected.extend(
                read.iter()
                    .filter(|name| offer.read.contains(name))
                    .cloned(),
            );
        }
    }
    // This catalog's CHECK facts use the scalar expression profile. Do not
    // select their enclosing read contract without its prerequisite.
    if !selected
        .iter()
        .any(|name| name == "vgi.expression.duckdb.scalar.v1")
    {
        selected.retain(|name| name != "vgi.constraint.records.read.v1");
    }
    Ok((
        record(CatalogCapabilities {
            read,
            ddl: vec![],
            write: vec![],
            transaction: vec![],
        }),
        record(CatalogCapabilities {
            read: selected,
            ddl: vec![],
            write: vec![],
            transaction: vec![],
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn immutable_rows_qualify_every_current_and_historical_fact() {
        let model = crate::catalog_def::build();
        let data = model
            .schemas
            .iter()
            .find(|schema| schema.name == "data")
            .unwrap();
        let versioned = data
            .tables
            .iter()
            .find(|table| table.name == "versioned_constraints")
            .unwrap();
        assert_eq!(versioned.constraint_versions.len(), 3);
        assert_eq!(versioned.constraint_versions[&1].0.len(), 1);
        assert_eq!(versioned.constraint_versions[&2].0.len(), 4);
        assert_eq!(versioned.constraint_versions[&3].0.len(), 5);
        let first = versioned
            .time_travel
            .iter()
            .find(|v| v.version == 1)
            .unwrap();
        assert!(!first.columns.field_with_name("id").unwrap().is_nullable());
        assert!(first.columns.field_with_name("name").unwrap().is_nullable());
        for table in &data.tables {
            for raw in &table.constraints {
                let info: ConstraintInfo = decode(raw).unwrap();
                assert_eq!(info.enforcement.0, "ENFORCED");
                assert_eq!(info.validation.0, "ALL_ROWS");
            }
        }
    }
    #[test]
    fn changed_immutable_rows_cannot_publish_false_guarantees() {
        let targets = crate::table::static_scan::fixture_rows();
        let original = &targets["departments"];
        for (column, values) in [
            (
                0,
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            ),
            (0, Arc::new(Int64Array::from(vec![1, 1, 3])) as ArrayRef),
            (
                2,
                Arc::new(Float64Array::from(vec![1.0, -1.0, 3.0])) as ArrayRef,
            ),
        ] {
            let mut cols = original.columns().to_vec();
            cols[column] = values;
            let bad = RecordBatch::try_new(original.schema(), cols).unwrap();
            let records = vec![
                constraint("departments", "id", "PRIMARY_KEY", &["id"]),
                check("departments", "check", "budget >= 0"),
            ];
            assert!(qualify_records(&records, &bad, &targets).is_err());
        }
        let employees = &targets["employees"];
        let mut cols = employees.columns().to_vec();
        cols[3] = Arc::new(Int64Array::from(vec![999, 1, 2, 2, 3]));
        let bad = RecordBatch::try_new(employees.schema(), cols).unwrap();
        let records = vec![foreign_key(
            "employees",
            "fk",
            &["department_id"],
            "departments",
            "fixture.departments.primary",
            &["id"],
        )];
        assert!(qualify_records(&records, &bad, &targets).is_err());
    }
    #[test]
    fn absent_offer_selects_no_profiles() {
        let (_, selected) = capability_agreement(None).unwrap();
        let decoded: vgi_protocol::generated::protocol_types::CatalogCapabilities =
            decode(&selected).unwrap();
        assert!(decoded.read.is_empty());
        assert!(decoded.write.is_empty());
    }

    #[test]
    fn offered_profiles_are_intersected_with_check_prerequisite() {
        use vgi::protocol::dtos::ClientCapabilities;
        use vgi_protocol::generated::protocol_types::CatalogCapabilities;
        let read = "vgi.constraint.records.read.v1";
        let scalar = "vgi.expression.duckdb.scalar.v1";
        for (offered, expected) in [
            (vec![read], vec![]),
            (vec![scalar], vec![scalar]),
            (vec![read, scalar, "vgi.unknown.v1"], vec![read, scalar]),
        ] {
            let offer = record(ClientCapabilities {
                engine: "test".into(),
                native_formats: vec![],
                catalogs: vec![],
                can_stream: true,
                filter_encodings: vec![],
                query_pushdown: None,
                catalog: Some(record(CatalogCapabilities {
                    read: offered.into_iter().map(str::to_string).collect(),
                    ddl: vec![],
                    write: vec![],
                    transaction: vec![],
                })),
            });
            let (_, selected) = capability_agreement(Some(&offer.0)).unwrap();
            let selected: CatalogCapabilities = decode(&selected).unwrap();
            assert_eq!(selected.read, expected);
            assert!(
                selected.ddl.is_empty()
                    && selected.write.is_empty()
                    && selected.transaction.is_empty()
            );
        }
    }
}

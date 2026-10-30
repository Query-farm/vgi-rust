// Copyright 2026 Query Farm LLC - https://query.farm
//! Canonical fixture identities, qualified against the immutable scan arrays.

use vgi::catalog::CatTable;
use vgi::constraints::{
    constraint_record, read_constraint_record, ConstraintKind, ConstraintRecord,
    ForeignKeyReference,
};
use vgi_rpc::Bytes;

// Fixture declarations start as the honest METADATA_ONLY + UNVALIDATED
// attestation; `qualify_catalog` upgrades each to ENFORCED + ALL_ROWS only
// after checking it against the immutable rows the table serves.
fn spec(table: &str, id: &str, kind: ConstraintKind, columns: &[&str]) -> ConstraintRecord {
    let paths: Vec<&[&str]> = columns.iter().map(std::slice::from_ref).collect();
    ConstraintRecord::new(&["data"], table, id, kind).with_columns(&paths)
}

fn record(spec: &ConstraintRecord) -> Bytes {
    constraint_record(spec).expect("valid fixture record")
}

pub fn constraint(table: &str, id: &str, kind: &str, columns: &[&str]) -> Bytes {
    let kind = ConstraintKind::parse(kind).expect("fixture constraint kind");
    record(&spec(table, id, kind, columns))
}

pub fn check(table: &str, id: &str, sql: &str) -> Bytes {
    let mut check = spec(table, id, ConstraintKind::Check, &[]);
    check.check_sql = Some(sql.into());
    record(&check)
}

pub fn foreign_key(
    table: &str,
    id: &str,
    columns: &[&str],
    target: &str,
    key: &str,
    target_columns: &[&str],
) -> Bytes {
    let mut fk = spec(table, id, ConstraintKind::ForeignKey, columns);
    fk.references = Some(ForeignKeyReference {
        schema_path: vec!["data".into()],
        table: target.into(),
        columns: target_columns.iter().map(|c| vec![(*c).into()]).collect(),
        constraint_id: key.into(),
    });
    record(&fk)
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
        let record = read_constraint_record(&raw.0)?;
        let cols = columns(batch, &record.columns)?;
        match record.kind {
            ConstraintKind::NotNull => {
                if cols.len() != 1 || cols[0].null_count() != 0 {
                    return Err(RpcError::value_error("immutable fixture violates NOT_NULL"));
                }
                required.insert(record.columns[0][0].clone());
            }
            ConstraintKind::PrimaryKey | ConstraintKind::Unique => {
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
                        None if record.kind == ConstraintKind::PrimaryKey => {
                            return Err(RpcError::value_error(
                                "immutable fixture primary key is NULL",
                            ));
                        }
                        _ => (),
                    }
                }
                if record.kind == ConstraintKind::PrimaryKey {
                    required.extend(record.columns.iter().map(|path| path[0].clone()));
                }
            }
            ConstraintKind::Check => {
                if record.check_sql.as_deref() != Some("budget >= 0") {
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
            ConstraintKind::ForeignKey => {
                let target = record
                    .references
                    .as_ref()
                    .ok_or_else(|| RpcError::value_error("missing fixture FK"))?;
                if target.schema_path != ["data"]
                    || target.table != "departments"
                    || target.constraint_id != "fixture.departments.primary"
                    || target.columns != [vec!["id".to_string()]]
                {
                    return Err(RpcError::value_error(
                        "unsupported immutable fixture FK target",
                    ));
                }
                let target_batch = targets
                    .get(&target.table)
                    .ok_or_else(|| RpcError::value_error("missing immutable target rows"))?;
                let target_cols = columns(target_batch, &target.columns)?;
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
        }
        // Checked against every row the table can return: attest it.
        result.push(constraint_record(&record.enforced())?);
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

/// Qualify every declared fixture fact against the immutable rows and attest
/// it. The SDK negotiates the constraint read profiles itself (no
/// `capability_agreement` hook) and re-checks the records when serving.
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
                        .filter(|raw| {
                            let record = read_constraint_record(&raw.0).expect("fixture record");
                            !(version.version == 1
                                && (record.kind != ConstraintKind::NotNull
                                    || record.columns != [vec!["id".to_string()]])
                                || record.kind == ConstraintKind::Unique && version.version < 2
                                || record.kind == ConstraintKind::ForeignKey && version.version < 3)
                        })
                        .cloned()
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use vgi::constraints::{
        validate_table_constraints, ConstraintEnforcement, ConstraintValidation,
    };

    #[test]
    fn immutable_rows_qualify_every_current_and_historical_fact() {
        let model = crate::catalog_def::build();
        assert!(model.has_constraint_records());
        assert!(model.capability_agreement.is_none());
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
        for version in &versioned.time_travel {
            let (records, _) = &versioned.constraint_versions[&version.version];
            validate_table_constraints(&versioned.name, &version.columns, records).unwrap();
        }
        for table in &data.tables {
            validate_table_constraints(&table.name, &table.columns, &table.constraints).unwrap();
            for raw in &table.constraints {
                let record = read_constraint_record(&raw.0).unwrap();
                assert_eq!(record.enforcement, ConstraintEnforcement::Enforced);
                assert_eq!(record.validation, ConstraintValidation::AllRows);
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
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `example.contract`: the cross-SDK table-contract fixtures (protocol
//! `vgi.v3` / 3.4), served by every SDK's standard example worker and driven by
//! vgi's `visibility/example_table_only.test`,
//! `write_contract/example_insert_finalize.test` and
//! `write_contract/example_decimal_width.test`.
//!
//! - `contract.good` — read-only, `table_only` scan `contract_good_scan` with
//!   projection pushdown and a row id (`rowid = id`);
//! - `contract.ranged` — read-only, `table_only` scan named exactly `range`
//!   (the worker's function binds for the table; DuckDB's `range()` stays
//!   the host built-in);
//! - `contract.notes` — INSERT only (`count`), a NOT NULL constraint record on
//!   `id` the worker enforces, and a finalize phase that reports the call's
//!   total from execution-scoped shared storage;
//! - `contract.amounts` — INSERT only (`count` and `rows`) into decimal32 /
//!   decimal64 / decimal128 columns, refusing any column that arrives at a
//!   different width than declared.
//!
//! Nothing is stored: the insert functions validate, count and discard. Every
//! function is declared through its table ([`CatTable::with_function`],
//! [`CatTable::with_insert_function`]) and registered by
//! `Worker::set_catalog`, so all of them stay `table_only`.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, Decimal128Array, Decimal32Array, Decimal64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::catalog::{CatSchema, CatTable};
use vgi::constraints::{constraint_record, ConstraintKind, ConstraintRecord};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
use vgi::table_in_out::TableInOutFunction;
use vgi::write_results::{
    project_to_output, row_id_field, write_count_batch, write_function_arg_specs,
    write_result_schema, write_rows_batch, WriteOptions, WriteResultMode,
};
use vgi_rpc::{Result, RpcError};

/// The schema's name.
pub const SCHEMA: &str = "contract";

fn err(e: impl std::fmt::Display) -> RpcError {
    RpcError::runtime_error(e.to_string())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn volatile(description: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: description.to_string(),
        stability: Some("VOLATILE".to_string()),
        categories: vec!["testing".into()],
        ..Default::default()
    }
}

// ------------------------------------------------------------------- scans

/// A producer that emits one batch.
struct OneShot(Option<RecordBatch>);

impl TableProducer for OneShot {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        Ok(self.0.take())
    }
    fn resume_supported(&self) -> bool {
        false
    }
}

/// A no-argument scan emitting one fixed batch.
struct FixedScan {
    name: &'static str,
    description: &'static str,
    schema: fn() -> SchemaRef,
    projection_pushdown: bool,
    rows: fn() -> Result<RecordBatch>,
}

impl TableFunction for FixedScan {
    fn name(&self) -> &str {
        self.name
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            projection_pushdown: self.projection_pushdown,
            ..volatile(self.description)
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        Vec::new()
    }
    fn on_bind(&self, _p: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: (self.schema)(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let full = (self.rows)()?;
        // Position by position: a projection may name a column twice.
        let batch = project_to_output(
            &full,
            &params.output_schema,
            params.projection_ids.as_deref(),
        )?;
        Ok(Box::new(OneShot(Some(batch))))
    }
}

// -------------------------------------------------------------------- good

fn good_columns() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
        row_id_field("rowid"),
    ]))
}

fn good_rows() -> Result<RecordBatch> {
    let ids = [1i64, 2, 3];
    RecordBatch::try_new(
        good_columns(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Int64Array::from(ids.to_vec())),
        ],
    )
    .map_err(err)
}

// ------------------------------------------------------------------ ranged

fn ranged_columns() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("who", DataType::Utf8, true)]))
}

fn ranged_rows() -> Result<RecordBatch> {
    RecordBatch::try_new(
        ranged_columns(),
        vec![Arc::new(StringArray::from(vec!["vgi example worker"])) as ArrayRef],
    )
    .map_err(err)
}

// ------------------------------------------------------------------- notes

fn notes_columns() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("note", DataType::Utf8, true),
    ]))
}

fn notes_rows() -> Result<RecordBatch> {
    Ok(RecordBatch::new_empty(notes_columns()))
}

/// The shared-storage namespace of `contract_notes_insert`'s per-call counter.
const NOTES_NS: &[u8] = b"contract_notes_insert";

/// The storage scope of a write call: its execution id (lowercase hex), which
/// every batch and the finalize carry, whichever pooled process serves them.
fn call_scope(p: &ProcessParams) -> Vec<u8> {
    hex(&p.execution_id).into_bytes()
}

fn storage(p: &ProcessParams) -> Result<&vgi::storage::SharedStorage> {
    p.storage
        .as_ref()
        .ok_or_else(|| err("contract: no shared storage for this call"))
}

/// `contract_notes_insert`: backs `INSERT INTO contract.notes`. Validates and
/// counts the rows (in execution-scoped shared storage), answers each batch
/// with an empty count batch and reports the call's total from finalize.
struct NotesInsert;

impl TableInOutFunction for NotesInsert {
    fn name(&self) -> &str {
        "contract_notes_insert"
    }
    fn metadata(&self) -> FunctionMetadata {
        volatile("Backs INSERT INTO contract.notes: validates and counts the rows, stores nothing")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        write_function_arg_specs("The new rows, in the user columns of notes")
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let mode = WriteOptions::from_arguments(&params.arguments)?.result_mode;
        Ok(BindResponse {
            output_schema: write_result_schema(mode, &notes_columns()),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, p: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let schema = p.input_schema.clone().unwrap_or_else(|| batch.schema());
        for f in schema.fields().iter().chain(batch.schema().fields().iter()) {
            if !f.is_nullable() {
                return Err(err(format!(
                    "contract: notes_insert input field {} is non-nullable",
                    f.name()
                )));
            }
        }
        if batch
            .column_by_name("id")
            .is_none_or(|c| c.null_count() > 0)
        {
            return Err(err("contract: notes.id required"));
        }
        storage(p)?.append(
            &call_scope(p),
            NOTES_NS,
            b"",
            (batch.num_rows() as i64).to_le_bytes().to_vec(),
        );
        Ok(vec![RecordBatch::new_empty(write_result_schema(
            WriteResultMode::Count,
            &notes_columns(),
        ))])
    }
    fn has_finish(&self) -> bool {
        true
    }
    fn finish(&self, p: &ProcessParams) -> Result<Vec<RecordBatch>> {
        let store = storage(p)?;
        let scope = call_scope(p);
        let mut total: i64 = 0;
        let mut after = 0;
        loop {
            let page = store.scan(&scope, NOTES_NS, b"", after, 1024);
            let Some((last, _)) = page.last() else { break };
            after = *last;
            for (_, value) in &page {
                let bytes: [u8; 8] = value
                    .as_slice()
                    .try_into()
                    .map_err(|_| err("contract: corrupt notes counter entry"))?;
                total += i64::from_le_bytes(bytes);
            }
        }
        store.clear(&scope);
        Ok(vec![write_count_batch(total as u64)])
    }
}

// ----------------------------------------------------------------- amounts

fn amounts_columns() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d32", DataType::Decimal32(7, 2), true),
        Field::new("d64", DataType::Decimal64(15, 3), true),
        Field::new("d128", DataType::Decimal128(9, 2), true),
    ]))
}

fn amounts_rows() -> Result<RecordBatch> {
    let schema = amounts_columns();
    let ty = |i: usize| schema.field(i).data_type().clone();
    let d32 =
        Decimal32Array::from(vec![Some(1_234_567), Some(-9_999_999), None]).with_data_type(ty(1));
    let d64 = Decimal64Array::from(vec![-123_456_789_012_345i64, 999_999_999_999_999, 1])
        .with_data_type(ty(2));
    let d128 = Decimal128Array::from(vec![Some(123_456_789i128), Some(-999_999_999), None])
        .with_data_type(ty(3));
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1i64, 2, 3])) as ArrayRef,
            Arc::new(d32),
            Arc::new(d64),
            Arc::new(d128),
        ],
    )
    .map_err(err)
}

/// `contract_amounts_insert`: backs `INSERT INTO contract.amounts`. Refuses a
/// column that arrives at another type (bit width included) than declared;
/// answers with the count, or the rows at their declared types.
struct AmountsInsert;

impl TableInOutFunction for AmountsInsert {
    fn name(&self) -> &str {
        "contract_amounts_insert"
    }
    fn metadata(&self) -> FunctionMetadata {
        volatile("Backs INSERT INTO contract.amounts: checks each column's declared width, stores nothing")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        write_function_arg_specs("The new rows, in the user columns of amounts")
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let mode = WriteOptions::from_arguments(&params.arguments)?.result_mode;
        Ok(BindResponse {
            output_schema: write_result_schema(mode, &amounts_columns()),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, p: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let declared = amounts_columns();
        for f in declared.fields() {
            let arrived = batch
                .schema()
                .field_with_name(f.name())
                .map(|a| a.data_type().clone())
                .map_err(|_| err(format!("contract: amounts.{} is missing", f.name())))?;
            if &arrived != f.data_type() {
                return Err(err(format!(
                    "contract: amounts.{} arrived as {arrived}, declared {}",
                    f.name(),
                    f.data_type()
                )));
            }
        }
        let mode = WriteOptions::from_arguments(&p.arguments)?.result_mode;
        Ok(vec![match mode {
            WriteResultMode::Rows => write_rows_batch(&declared, batch)?,
            _ => write_count_batch(batch.num_rows() as u64),
        }])
    }
}

// ----------------------------------------------------------------- catalog

/// The `contract` schema of the `example` catalog (no comment: only `data`
/// and `main` are commented).
pub fn schema() -> CatSchema {
    let scan = |name, description, schema, projection_pushdown, rows| {
        Arc::new(FixedScan {
            name,
            description,
            schema,
            projection_pushdown,
            rows,
        })
    };
    let good = CatTable::with_function(
        "good",
        good_columns(),
        // DuckDB reads `rowid` as a virtual column, which needs projection pushdown.
        scan(
            "contract_good_scan",
            "The rows of contract.good, with rowid",
            good_columns,
            true,
            good_rows,
        ),
        None,
        None,
    );
    let ranged = CatTable::with_function(
        "ranged",
        ranged_columns(),
        scan(
            "range",
            "The row of contract.ranged (a table_only scan named like DuckDB's range())",
            ranged_columns,
            false,
            ranged_rows,
        ),
        None,
        None,
    );
    let mut notes = CatTable::with_function(
        "notes",
        notes_columns(),
        scan(
            "contract_notes_scan",
            "contract.notes stores nothing: always empty",
            notes_columns,
            false,
            notes_rows,
        ),
        None,
        None,
    )
    .with_insert_function(Arc::new(NotesInsert), WriteResultMode::Count);
    notes.constraints = vec![constraint_record(
        &ConstraintRecord::new(
            &[SCHEMA],
            "notes",
            "notes_id_not_null",
            ConstraintKind::NotNull,
        )
        .with_columns(&[&["id"]])
        .enforced(),
    )
    .expect("valid contract.notes constraint record")];
    let amounts = CatTable::with_function(
        "amounts",
        amounts_columns(),
        scan(
            "contract_amounts_scan",
            "The fixed rows of contract.amounts at their declared decimal widths",
            amounts_columns,
            false,
            amounts_rows,
        ),
        None,
        None,
    )
    .with_insert_function(Arc::new(AmountsInsert), WriteResultMode::Rows);
    CatSchema {
        path: Vec::new(),
        name: SCHEMA.to_string(),
        comment: None,
        tags: Vec::new(),
        views: Vec::new(),
        macros: Vec::new(),
        tables: vec![good, ranged, notes, amounts],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amounts_rows_match_declared_widths() {
        let rows = amounts_rows().unwrap();
        assert_eq!(rows.schema(), amounts_columns());
        assert_eq!(rows.num_rows(), 3);
    }

    #[test]
    fn notes_constraint_passes_serving_checks() {
        let s = schema();
        let notes = s.tables.iter().find(|t| t.name == "notes").unwrap();
        let served = vgi::constraints::served_columns(
            &[SCHEMA.to_string()],
            "notes",
            &notes.columns,
            &notes.constraints,
        )
        .unwrap();
        assert!(!served.field_with_name("id").unwrap().is_nullable());
        assert!(served.field_with_name("note").unwrap().is_nullable());
    }
}

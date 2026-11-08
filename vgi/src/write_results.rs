// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Writable catalog tables: result modes, the row-id column, and the helpers a
//! write function uses to read what DuckDB asked for and build its answer.
//!
//! A [`CatTable`](crate::catalog::CatTable) becomes writable by naming a
//! table-in-out function per operation in
//! [`CatTable::writes`](crate::catalog::CatTable::writes) (see
//! [`TableWrites`](crate::catalog::TableWrites)). DuckDB runs the scan (with the
//! `WHERE` clause), then streams the affected rows into that function:
//!
//! | operation | input rows                                      |
//! |-----------|-------------------------------------------------|
//! | INSERT    | the new rows, in the table's user columns        |
//! | UPDATE    | the `SET` columns, then the row-id column        |
//! | DELETE    | the row-id column only                           |
//!
//! Every write function also receives the named argument `write_options` (a
//! one-row IPC batch; decode it with [`WriteOptions::from_arguments`]), whose
//! `result_mode` says what the function must return:
//!
//! | mode      | statement                 | output ([`write_result_schema`])          |
//! |-----------|---------------------------|-------------------------------------------|
//! | `count`   | plain INSERT/UPDATE/DELETE | one row, `count BIGINT NOT NULL`          |
//! | `rows`    | `... RETURNING`            | the written rows, in the user columns     |
//! | `changes` | old/new pairs              | `old STRUCT(<user>)`, `new STRUCT(<user>)` |
//!
//! Modes are ordered `count < rows < changes`; a table advertises the richest
//! mode each operation supports and so promises every lower one. Bind the
//! output schema with [`write_result_schema`] and build the answer with
//! [`write_result_batch`] (or the per-mode helpers).
//!
//! ## The row-id column
//!
//! UPDATE and DELETE identify rows by the table's row-id column — the one
//! field carrying [`ROW_ID_METADATA_KEY`] metadata ([`row_id_field`]). DuckDB
//! reads it as a *virtual* column, which it only requests from a scan that
//! supports projection pushdown, so a table with UPDATE or DELETE needs a scan
//! function that (1) emits this field, (2) declares
//! [`FunctionMetadata::projection_pushdown`](crate::FunctionMetadata::projection_pushdown),
//! and (3) is registered on the worker. Row ids must be unique and stable for
//! the life of a statement.
//!
//! ## Projections may repeat a column
//!
//! The scan behind an `UPDATE ... RETURNING` (or any query) can be asked for
//! the same column twice: `ProcessParams::projection_ids` may repeat an index,
//! so `ProcessParams::output_schema` then carries two fields with the same
//! name. Never build output by inserting values into a name-keyed map and
//! reading them back per output field into a new batch keyed by name — a
//! duplicated name collapses. Either return batches in the full bind schema
//! (the producer adapter narrows them to `output_schema` position by position),
//! or project by position with [`project_to_output`].
//!
//! ```
//! use std::sync::Arc;
//! use arrow_array::{Int64Array, RecordBatch, StringArray};
//! use arrow_schema::{DataType, Field, Schema};
//! use vgi::write_results::{
//!     row_id_field, user_columns, write_result_batch, write_result_schema, WriteResultMode,
//! };
//!
//! let table = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//!     Field::new("name", DataType::Utf8, true),
//!     row_id_field("rowid"),
//! ]));
//! // RETURNING rows never include the row id.
//! assert_eq!(user_columns(&table).fields().len(), 2);
//!
//! let new_rows = RecordBatch::try_new(
//!     user_columns(&table),
//!     vec![
//!         Arc::new(Int64Array::from(vec![1])),
//!         Arc::new(StringArray::from(vec!["renamed"])),
//!     ],
//! )
//! .unwrap();
//! let count = write_result_batch(WriteResultMode::Count, &table, None, Some(&new_rows)).unwrap();
//! assert_eq!(count.schema(), write_result_schema(WriteResultMode::Count, &table));
//! assert_eq!(count.num_rows(), 1);
//! let rows = write_result_batch(WriteResultMode::Rows, &table, None, Some(&new_rows)).unwrap();
//! assert_eq!(rows.num_rows(), 1);
//! ```

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use vgi_rpc::{Result, RpcError};

/// Field metadata key that marks a table's row-id column.
pub const ROW_ID_METADATA_KEY: &str = "is_row_id";

/// The name of the named argument DuckDB passes every write function.
pub const WRITE_OPTIONS_ARG: &str = "write_options";

/// A DML operation a table can accept.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WriteOperation {
    Insert,
    Update,
    Delete,
}

impl WriteOperation {
    /// Every operation, in wire order.
    pub const ALL: [WriteOperation; 3] = [Self::Insert, Self::Update, Self::Delete];

    /// The wire spelling (`insert` / `update` / `delete`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

impl fmt::Display for WriteOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for WriteOperation {
    type Err = RpcError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "insert" => Ok(Self::Insert),
            "update" => Ok(Self::Update),
            "delete" => Ok(Self::Delete),
            other => Err(RpcError::value_error(format!(
                "Unknown write operation '{other}'"
            ))),
        }
    }
}

/// What a write function returns for one statement. Ordered
/// `Count < Rows < Changes`; supporting a mode promises every lower one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WriteResultMode {
    /// One row: `count BIGINT NOT NULL` (plain INSERT/UPDATE/DELETE).
    #[default]
    Count,
    /// The written rows in the table's user columns (`... RETURNING`).
    Rows,
    /// One `old` / `new` struct pair per written row.
    Changes,
}

impl WriteResultMode {
    /// Every mode, lowest first.
    pub const ALL: [WriteResultMode; 3] = [Self::Count, Self::Rows, Self::Changes];

    /// The wire spelling (`count` / `rows` / `changes`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Rows => "rows",
            Self::Changes => "changes",
        }
    }

    /// Whether a table whose maximum is `self` can answer `requested`.
    pub fn supports(self, requested: WriteResultMode) -> bool {
        self >= requested
    }
}

impl fmt::Display for WriteResultMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for WriteResultMode {
    type Err = RpcError;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "count" => Ok(Self::Count),
            "rows" => Ok(Self::Rows),
            "changes" => Ok(Self::Changes),
            other => Err(RpcError::value_error(format!(
                "Unknown write result mode '{other}'"
            ))),
        }
    }
}

/// A row-id field for a writable table's columns and its scan's output:
/// `row_id_field("rowid")` is DuckDB's conventional `rowid BIGINT NOT NULL`,
/// marked with [`ROW_ID_METADATA_KEY`]. See the [module docs](self) for what
/// the scan must also do.
pub fn row_id_field(name: &str) -> Field {
    row_id_field_typed(name, DataType::Int64)
}

/// [`row_id_field`] with a row-id type other than BIGINT.
pub fn row_id_field_typed(name: &str, data_type: DataType) -> Field {
    Field::new(name, data_type, false).with_metadata(HashMap::from([(
        ROW_ID_METADATA_KEY.to_string(),
        String::new(),
    )]))
}

/// Whether `field` is a row-id column.
pub fn is_row_id(field: &Field) -> bool {
    field.metadata().contains_key(ROW_ID_METADATA_KEY)
}

/// The index of `schema`'s row-id column, if it has one.
pub fn row_id_index(schema: &Schema) -> Option<usize> {
    schema.fields().iter().position(|f| is_row_id(f))
}

/// The table's user columns: `columns` without the row-id field. Write
/// results (`rows`, and both halves of `changes`) use this schema.
pub fn user_columns(columns: &Schema) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .fields()
            .iter()
            .filter(|f| !is_row_id(f))
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

/// The exact output schema a write function must bind for `mode`. The row-id
/// column of `table_columns` (if any) is left out.
pub fn write_result_schema(mode: WriteResultMode, table_columns: &Schema) -> SchemaRef {
    match mode {
        WriteResultMode::Count => Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Int64,
            false,
        )])),
        WriteResultMode::Rows => user_columns(table_columns),
        WriteResultMode::Changes => {
            let row = DataType::Struct(user_columns(table_columns).fields().clone());
            Arc::new(Schema::new(vec![
                Field::new("old", row.clone(), true),
                Field::new("new", row, true),
            ]))
        }
    }
}

/// A `count` result batch.
pub fn write_count_batch(count: u64) -> RecordBatch {
    RecordBatch::try_new(
        write_result_schema(WriteResultMode::Count, &Schema::empty()),
        vec![Arc::new(Int64Array::from(vec![count as i64])) as ArrayRef],
    )
    .expect("count batch matches its schema")
}

/// A `rows` result batch: `rows` re-shaped to the table's user columns.
///
/// Columns are taken from `rows` by name (each user column must be present;
/// extra columns such as the row id are dropped) and cast to the declared
/// type, so `rows` may come straight from the function's own state.
pub fn write_rows_batch(table_columns: &Schema, rows: &RecordBatch) -> Result<RecordBatch> {
    conform(&user_columns(table_columns), rows)
}

/// A `changes` result batch, one `old` / `new` pair per written row: `old`
/// is `None` for an insert (every `old` is NULL), `new` is `None` for a
/// delete. When both are given they must have the same row count.
pub fn write_changes_batch(
    table_columns: &Schema,
    old: Option<&RecordBatch>,
    new: Option<&RecordBatch>,
) -> Result<RecordBatch> {
    let schema = write_result_schema(WriteResultMode::Changes, table_columns);
    let user = user_columns(table_columns);
    let rows = match (old, new) {
        (Some(o), Some(n)) if o.num_rows() != n.num_rows() => {
            return Err(RpcError::value_error(format!(
                "write_changes_batch: old has {} rows but new has {}",
                o.num_rows(),
                n.num_rows()
            )))
        }
        (Some(b), _) | (None, Some(b)) => b.num_rows(),
        (None, None) => 0,
    };
    let half = |side: Option<&RecordBatch>| -> Result<ArrayRef> {
        let fields: Fields = user.fields().clone();
        match side {
            Some(b) => {
                let b = conform(&user, b)?;
                Ok(Arc::new(StructArray::new(fields, b.columns().to_vec(), None)) as ArrayRef)
            }
            None => Ok(Arc::new(StructArray::new_null(fields, rows)) as ArrayRef),
        }
    };
    RecordBatch::try_new_with_options(
        schema,
        vec![half(old)?, half(new)?],
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .map_err(|e| RpcError::runtime_error(format!("write_changes_batch: {e}")))
}

/// The result batch for `mode`, from the written rows' before (`old`) and
/// after (`new`) images in the table's columns: `old` is `None` for an
/// INSERT, `new` is `None` for a DELETE.
///
/// `count` counts the written rows; `rows` (RETURNING) shows each row as
/// written — the new image, or the old one for a delete; `changes` pairs them.
pub fn write_result_batch(
    mode: WriteResultMode,
    table_columns: &Schema,
    old: Option<&RecordBatch>,
    new: Option<&RecordBatch>,
) -> Result<RecordBatch> {
    match mode {
        WriteResultMode::Count => Ok(write_count_batch(
            new.or(old).map(|b| b.num_rows()).unwrap_or(0) as u64,
        )),
        WriteResultMode::Rows => match new.or(old) {
            Some(b) => write_rows_batch(table_columns, b),
            None => Ok(RecordBatch::new_empty(user_columns(table_columns))),
        },
        WriteResultMode::Changes => write_changes_batch(table_columns, old, new),
    }
}

/// Re-shape `batch` to `schema` by column name, casting to the declared types.
fn conform(schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch> {
    let mut cols = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        let col = batch.column_by_name(f.name()).ok_or_else(|| {
            RpcError::value_error(format!(
                "write result is missing column '{}' (have: {})",
                f.name(),
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?;
        let col = if col.data_type() == f.data_type() {
            col.clone()
        } else {
            arrow_cast::cast(col, f.data_type())
                .map_err(|e| RpcError::value_error(format!("column '{}': {e}", f.name())))?
        };
        if !f.is_nullable() && col.null_count() > 0 {
            return Err(RpcError::value_error(format!(
                "column '{}' is NOT NULL but the write result has NULLs",
                f.name()
            )));
        }
        cols.push(col);
    }
    RecordBatch::try_new_with_options(
        schema.clone(),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| RpcError::runtime_error(format!("write result: {e}")))
}

/// Narrow `full` (a batch in the bind's output schema) to `output_schema`,
/// position by position: output field `i` is `full`'s column
/// `projection_ids[i]` when ids are given, else the column named like output
/// field `i`. Safe when the projection names a column twice, which a
/// name-keyed rebuild is not (see the [module docs](self)).
pub fn project_to_output(
    full: &RecordBatch,
    output_schema: &SchemaRef,
    projection_ids: Option<&[i64]>,
) -> Result<RecordBatch> {
    let cols = match projection_ids.filter(|ids| !ids.is_empty()) {
        Some(ids) => {
            if ids.len() != output_schema.fields().len() {
                return Err(RpcError::value_error(format!(
                    "{} projection ids for {} output columns",
                    ids.len(),
                    output_schema.fields().len()
                )));
            }
            ids.iter()
                .map(|&i| {
                    usize::try_from(i)
                        .ok()
                        .filter(|&i| i < full.num_columns())
                        .map(|i| full.column(i).clone())
                        .ok_or_else(|| {
                            RpcError::value_error(format!("projection id {i} is out of range"))
                        })
                })
                .collect::<Result<Vec<_>>>()?
        }
        None => output_schema
            .fields()
            .iter()
            .map(|f| {
                full.column_by_name(f.name()).cloned().ok_or_else(|| {
                    RpcError::value_error(format!("projection column '{}' not found", f.name()))
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };
    RecordBatch::try_new_with_options(
        output_schema.clone(),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(full.num_rows())),
    )
    .map_err(|e| RpcError::runtime_error(format!("project_to_output: {e}")))
}

/// What DuckDB asked a write function for, decoded from its `write_options`
/// named argument.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOptions {
    /// The result the statement needs.
    pub result_mode: WriteResultMode,
    /// `throw` (the default), or the `INSERT ... ON CONFLICT` action DuckDB
    /// requested (`nothing`).
    pub on_conflict: String,
    /// The `ON CONFLICT (<columns>)` target, when one was named.
    pub on_conflict_columns: Vec<String>,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            result_mode: WriteResultMode::Count,
            on_conflict: "throw".into(),
            on_conflict_columns: Vec::new(),
        }
    }
}

impl WriteOptions {
    /// Decode `write_options` from a write function's call arguments (both
    /// [`BindParams::arguments`](crate::BindParams::arguments) and
    /// [`ProcessParams::arguments`](crate::ProcessParams::arguments) carry it).
    /// Missing options mean a plain `count` write; an unknown result mode is an
    /// error.
    pub fn from_arguments(args: &crate::arguments::Arguments) -> Result<Self> {
        let Some(raw) = args.named(WRITE_OPTIONS_ARG) else {
            return Ok(Self::default());
        };
        if raw.is_empty() || raw.is_null(0) {
            return Ok(Self::default());
        }
        let bytes: Vec<u8> =
            if let Some(b) = raw.as_any().downcast_ref::<arrow_array::BinaryArray>() {
                b.value(0).to_vec()
            } else if let Some(b) = raw.as_any().downcast_ref::<arrow_array::LargeBinaryArray>() {
                b.value(0).to_vec()
            } else if let Some(b) = raw.as_any().downcast_ref::<arrow_array::BinaryViewArray>() {
                b.value(0).to_vec()
            } else {
                return Err(RpcError::value_error(format!(
                    "write_options must be BLOB, got {}",
                    raw.data_type()
                )));
            };
        Self::decode(&bytes)
    }

    /// Decode the one-row IPC batch DuckDB serializes into `write_options`.
    pub fn decode(ipc_bytes: &[u8]) -> Result<Self> {
        let mut out = Self::default();
        if ipc_bytes.is_empty() {
            return Ok(out);
        }
        let batch = crate::ipc::read_batch(ipc_bytes)?;
        if batch.num_rows() == 0 {
            return Ok(out);
        }
        let string_at = |name: &str| -> Result<Option<String>> {
            let Some(col) = batch.column_by_name(name) else {
                return Ok(None);
            };
            if col.is_null(0) {
                return Ok(None);
            }
            let col = arrow_cast::cast(col, &DataType::Utf8)
                .map_err(|e| RpcError::value_error(format!("write_options.{name}: {e}")))?;
            let col = col
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .expect("cast to Utf8");
            Ok(Some(col.value(0).to_string()))
        };
        if let Some(mode) = string_at("result_mode")? {
            out.result_mode = mode.parse()?;
        }
        if let Some(action) = string_at("on_conflict")? {
            out.on_conflict = action;
        }
        if let Some(col) = batch.column_by_name("on_conflict_columns") {
            if !col.is_null(0) {
                let list = arrow_cast::cast(
                    col,
                    &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                )
                .map_err(|e| {
                    RpcError::value_error(format!("write_options.on_conflict_columns: {e}"))
                })?;
                let list = list
                    .as_any()
                    .downcast_ref::<arrow_array::ListArray>()
                    .expect("cast to List<Utf8>");
                let values = list.value(0);
                let values = values
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .expect("List<Utf8> values");
                out.on_conflict_columns = values.iter().flatten().map(str::to_string).collect();
            }
        }
        Ok(out)
    }
}

/// The argument specs every write function declares: the streamed input
/// table (`data`, positional 0) and the `write_options` named BLOB DuckDB
/// supplies. Return these (plus any of your own) from
/// [`TableInOutFunction::argument_specs`](crate::table_in_out::TableInOutFunction::argument_specs).
pub fn write_function_arg_specs(input_doc: &str) -> Vec<crate::ArgSpec> {
    vec![
        crate::ArgSpec::column("data", 0, "table", input_doc),
        write_options_arg_spec(),
    ]
}

/// The `write_options` named-argument spec (BLOB, default NULL).
pub fn write_options_arg_spec() -> crate::ArgSpec {
    crate::ArgSpec::const_arg(
        WRITE_OPTIONS_ARG,
        -1,
        "binary",
        "Write options supplied by DuckDB (result mode, ON CONFLICT action).",
    )
    .with_default(serde_json::Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;

    fn table() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("qty", DataType::Int64, true),
            row_id_field("rowid"),
        ]))
    }

    fn rows(ids: &[i32], names: &[&str], qty: &[Option<i64>]) -> RecordBatch {
        RecordBatch::try_new(
            user_columns(&table()),
            vec![
                Arc::new(arrow_array::Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Int64Array::from(qty.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn modes_are_ordered_and_parse() {
        assert!(WriteResultMode::Changes.supports(WriteResultMode::Rows));
        assert!(WriteResultMode::Rows.supports(WriteResultMode::Count));
        assert!(!WriteResultMode::Count.supports(WriteResultMode::Rows));
        for m in WriteResultMode::ALL {
            assert_eq!(m.as_str().parse::<WriteResultMode>().unwrap(), m);
        }
        for op in WriteOperation::ALL {
            assert_eq!(op.as_str().parse::<WriteOperation>().unwrap(), op);
        }
        assert!("bogus".parse::<WriteResultMode>().is_err());
        assert!("upsert".parse::<WriteOperation>().is_err());
    }

    #[test]
    fn row_id_field_is_marked_and_stripped_from_results() {
        let t = table();
        assert_eq!(row_id_index(&t), Some(3));
        assert!(is_row_id(t.field(3)));
        assert_eq!(user_columns(&t).fields().len(), 3);
        let rows_schema = write_result_schema(WriteResultMode::Rows, &t);
        assert!(rows_schema.fields().iter().all(|f| !is_row_id(f)));
        let changes = write_result_schema(WriteResultMode::Changes, &t);
        assert_eq!(changes.field(0).name(), "old");
        assert_eq!(changes.field(1).name(), "new");
        match changes.field(0).data_type() {
            DataType::Struct(children) => assert_eq!(children.len(), 3),
            other => panic!("expected struct, got {other}"),
        }
    }

    #[test]
    fn result_batches_match_their_schemas() {
        let t = table();
        let old = rows(&[1, 2], &["a", "b"], &[Some(10), Some(20)]);
        let new = rows(&[1, 2], &["a", "B"], &[Some(11), None]);
        for mode in WriteResultMode::ALL {
            let b = write_result_batch(mode, &t, Some(&old), Some(&new)).unwrap();
            assert_eq!(b.schema(), write_result_schema(mode, &t), "{mode}");
        }
        let count = write_result_batch(WriteResultMode::Count, &t, Some(&old), Some(&new)).unwrap();
        let c = count
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(c.value(0), 2);
        // RETURNING for a delete shows the old image.
        let deleted = write_result_batch(WriteResultMode::Rows, &t, Some(&old), None).unwrap();
        assert_eq!(deleted.num_rows(), 2);
        // An insert's changes have NULL old images.
        let ins = write_result_batch(WriteResultMode::Changes, &t, None, Some(&new)).unwrap();
        assert_eq!(ins.column(0).null_count(), 2);
        assert_eq!(ins.column(1).null_count(), 0);
    }

    #[test]
    fn rows_are_conformed_by_name_and_type() {
        let t = table();
        // Out of order, a wider id type, plus the row id: all fine.
        let messy = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("rowid", DataType::Int64, false),
                Field::new("qty", DataType::Int64, true),
                Field::new("name", DataType::Utf8, false),
                Field::new("id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![7])),
                Arc::new(Int64Array::from(vec![Some(3)])),
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .unwrap();
        let out = write_rows_batch(&t, &messy).unwrap();
        assert_eq!(out.schema(), user_columns(&t));
        // A missing column is named in the error.
        let missing = messy.project(&[0, 1, 3]).unwrap();
        let err = write_rows_batch(&t, &missing).unwrap_err();
        assert!(err.message.contains("'name'"), "{}", err.message);
        // Mismatched halves are refused.
        let one = rows(&[1], &["a"], &[None]);
        let two = rows(&[1, 2], &["a", "b"], &[None, None]);
        assert!(write_changes_batch(&t, Some(&one), Some(&two)).is_err());
    }

    #[test]
    fn projection_tolerates_a_repeated_column() {
        let full = rows(&[1, 2], &["a", "b"], &[Some(10), Some(20)]);
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("qty", DataType::Int64, true),
            Field::new("qty", DataType::Int64, true),
            Field::new("name", DataType::Utf8, false),
        ]));
        let by_id = project_to_output(&full, &out_schema, Some(&[2, 2, 1])).unwrap();
        assert_eq!(by_id.num_columns(), 3);
        assert_eq!(by_id.column(0).as_ref(), by_id.column(1).as_ref());
        let by_name = project_to_output(&full, &out_schema, None).unwrap();
        assert_eq!(by_name.column(0).as_ref(), by_name.column(1).as_ref());
        assert!(project_to_output(&full, &out_schema, Some(&[2, 9, 1])).is_err());
    }

    #[test]
    fn write_options_decode() {
        assert_eq!(WriteOptions::decode(&[]).unwrap(), WriteOptions::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("result_mode", DataType::Utf8, false),
            Field::new("on_conflict", DataType::Utf8, true),
            Field::new(
                "on_conflict_columns",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]));
        let mut list =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
        list.values().append_value("id");
        list.append(true);
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["rows"])),
                Arc::new(StringArray::from(vec!["nothing"])),
                Arc::new(list.finish()),
            ],
        )
        .unwrap();
        let bytes = crate::ipc::write_batch(&batch).unwrap();
        let opts = WriteOptions::decode(&bytes).unwrap();
        assert_eq!(opts.result_mode, WriteResultMode::Rows);
        assert_eq!(opts.on_conflict, "nothing");
        assert_eq!(opts.on_conflict_columns, vec!["id".to_string()]);
        // Through the named argument, as a function sees it.
        let mut args = crate::arguments::Arguments::default();
        args.named.insert(
            WRITE_OPTIONS_ARG.to_string(),
            Arc::new(arrow_array::BinaryArray::from(vec![bytes.as_slice()])),
        );
        assert_eq!(WriteOptions::from_arguments(&args).unwrap(), opts);
        // Absent: a plain count write.
        assert_eq!(
            WriteOptions::from_arguments(&crate::arguments::Arguments::default()).unwrap(),
            WriteOptions::default()
        );
    }
}

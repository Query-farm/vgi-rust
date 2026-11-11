// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `txfix`: the writable-table and transaction-hook conformance fixture
//! (vgi `docs/writable_tx_fixtures.md`), serving
//! `test/sql/integration/writable_tx/*.test` (`VGI_WRITABLE_TX_WORKER`).
//!
//! Built only from the SDK's public API:
//!
//! - ATTACH options `transactional` (BOOLEAN, default false) and `token`
//!   (VARCHAR, **secret**), read in every function and hook with
//!   [`vgi::attach_options::attach_options`] — the SDK encrypts the secret
//!   `token` inside `attach_opaque_data`, so it is never there in plaintext
//!   (`whoami().token_in_attach_data` checks that);
//! - `main.items` declares an UPDATE function and its result mode through
//!   [`CatTable::writes`](vgi::catalog::CatTable::writes); its scan emits the
//!   row id ([`vgi::write_results::row_id_field`]) with projection pushdown;
//!   its constraint records make the catalog negotiate
//!   `vgi.constraint.enforce.v1`;
//! - `main.labels` is read-only (constrained, no write function);
//! - [`CatalogTransactions`] decides `supports_transactions` per attach and
//!   implements begin / commit / rollback.
//!
//! ```sql
//! ATTACH 'txfix' AS t   (TYPE vgi, LOCATION '...', token 'tok-123');
//! ATTACH 'txfix' AS ttx (TYPE vgi, LOCATION '...', token 'tok-123', transactional true);
//! ```
//!
//! On a transactional attach UPDATE stages its changes under the plaintext
//! transaction id the function sees; scans on that attach read their own
//! writes; the commit hook applies them (or refuses a negative qty) and the
//! rollback hook discards them. State lives in one SQLite file under
//! `$VGI_TXFIX_STATE_DIR` (default `<tmpdir>/vgi-txfix`), partitioned by the
//! `token` option, because the extension routes the statements of one
//! transaction to different worker processes (a SQLite `BEGIN IMMEDIATE` is
//! the cross-process lock). Mirrors the TypeScript reference fixture.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rusqlite::{params, Connection, TransactionBehavior};
use vgi::catalog::{AttachOptionFlags, CatSchema, CatTable, CatalogModel, CatalogTransactions};
use vgi::constraints::{constraint_record, ConstraintKind, ConstraintRecord};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{TableFunction, TableProducer};
use vgi::table_in_out::TableInOutFunction;
use vgi::write_results::{
    project_to_output, row_id_field, write_function_arg_specs, write_result_batch,
    write_result_schema, WriteOptions, WriteResultMode,
};
use vgi_rpc::{Result, RpcError};

/// The catalog's name (and `VGI_WORKER_CATALOG_NAME` that selects it).
pub const CATALOG_NAME: &str = "txfix";

fn err(e: impl std::fmt::Display) -> RpcError {
    RpcError::runtime_error(e.to_string())
}

// ------------------------------------------------------------ ATTACH options

/// The attach's options as every function sees them.
#[derive(Clone, Debug, PartialEq)]
struct AttachOptions {
    token: Option<String>,
    transactional: bool,
}

/// Row 0 of option `name` (case-insensitively) cast to `to`, if supplied.
fn option(options: Option<&RecordBatch>, name: &str, to: &DataType) -> Option<ArrayRef> {
    let batch = options.filter(|b| b.num_rows() > 0)?;
    let (index, _) = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .find(|(_, f)| f.name().eq_ignore_ascii_case(name))?;
    let col = arrow_cast::cast(batch.column(index), to).ok()?;
    (!col.is_null(0)).then_some(col)
}

fn options_of(options: Option<&RecordBatch>) -> AttachOptions {
    let token = option(options, "token", &DataType::Utf8).map(|c| {
        c.as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8")
            .value(0)
            .to_string()
    });
    let transactional = option(options, "transactional", &DataType::Boolean)
        .map(|c| {
            c.as_any()
                .downcast_ref::<BooleanArray>()
                .expect("bool")
                .value(0)
        })
        .unwrap_or(false);
    AttachOptions {
        token,
        transactional,
    }
}

/// The attach's options through the SDK's public accessor (the secret `token`
/// decrypted). An attach that named no option has none.
fn decode_attach(attach_opaque_data: Option<&[u8]>) -> Result<AttachOptions> {
    let opts = vgi::attach_options::attach_options(attach_opaque_data.unwrap_or_default())?;
    Ok(AttachOptions {
        token: opts.as_ref().and_then(|o| o.string("token")),
        transactional: opts
            .as_ref()
            .and_then(|o| o.boolean("transactional"))
            .unwrap_or(false),
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The plaintext transaction id of a call (lowercase hex), or `None`.
fn tx_of(transaction_opaque_data: Option<&[u8]>) -> Option<String> {
    transaction_opaque_data.filter(|t| !t.is_empty()).map(hex)
}

/// The state partition of an attach: its token (NULL is a partition of its own).
fn part_of(o: &AttachOptions) -> String {
    match &o.token {
        Some(t) => format!("token:{t}"),
        None => "null:".to_string(),
    }
}

// ------------------------------------------------------------------- state

const INITIAL_ITEMS: [(i32, &str, i64); 3] = [(1, "a", 10), (2, "b", 20), (3, "c", 30)];
const INITIAL_LABELS: [(i32, &str); 2] = [(1, "x"), (2, "y")];

fn open_state() -> Result<Connection> {
    let dir = std::env::var_os("VGI_TXFIX_STATE_DIR")
        .filter(|d| !d.is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("vgi-txfix"));
    std::fs::create_dir_all(&dir).map_err(err)?;
    let db = Connection::open(dir.join("txfix.db")).map_err(err)?;
    db.busy_timeout(std::time::Duration::from_secs(10))
        .map_err(err)?;
    db.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE IF NOT EXISTS parts (part TEXT PRIMARY KEY);
         CREATE TABLE IF NOT EXISTS items (part TEXT NOT NULL, id INTEGER NOT NULL, name TEXT, qty INTEGER, PRIMARY KEY (part, id));
         CREATE TABLE IF NOT EXISTS labels (part TEXT NOT NULL, id INTEGER NOT NULL, label TEXT, PRIMARY KEY (part, id));
         CREATE TABLE IF NOT EXISTS staged (part TEXT NOT NULL, tx TEXT NOT NULL, id INTEGER NOT NULL, name TEXT, qty INTEGER, PRIMARY KEY (part, tx, id));
         CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, part TEXT NOT NULL, kind TEXT NOT NULL, tx TEXT, ok INTEGER NOT NULL);",
    )
    .map_err(err)?;
    Ok(db)
}

/// Run `f` in one SQLite write transaction (`BEGIN IMMEDIATE`: the
/// cross-process lock). `f`'s error rolls the state back.
fn with_state<T>(f: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T>) -> Result<T> {
    let mut db = open_state()?;
    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(err)?;
    let out = f(&tx)?;
    tx.commit().map_err(err)?;
    Ok(out)
}

/// Seed `part` with the initial rows the first time it is seen (or always,
/// clearing its staged changes and events, when `force`).
fn seed(db: &Connection, part: &str, force: bool) -> Result<()> {
    let fresh = db
        .execute("INSERT OR IGNORE INTO parts (part) VALUES (?1)", [part])
        .map_err(err)?
        == 1;
    if !fresh && !force {
        return Ok(());
    }
    for table in ["items", "labels", "staged", "events"] {
        db.execute(&format!("DELETE FROM {table} WHERE part = ?1"), [part])
            .map_err(err)?;
    }
    for (id, name, qty) in INITIAL_ITEMS {
        db.execute(
            "INSERT INTO items (part, id, name, qty) VALUES (?1, ?2, ?3, ?4)",
            params![part, id, name, qty],
        )
        .map_err(err)?;
    }
    for (id, label) in INITIAL_LABELS {
        db.execute(
            "INSERT INTO labels (part, id, label) VALUES (?1, ?2, ?3)",
            params![part, id, label],
        )
        .map_err(err)?;
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
struct Item {
    id: i32,
    name: Option<String>,
    qty: Option<i64>,
}

fn items_of(db: &Connection, sql: &str, args: &[&dyn rusqlite::ToSql]) -> Result<Vec<Item>> {
    let mut stmt = db.prepare(sql).map_err(err)?;
    let rows = stmt
        .query_map(args, |r| {
            Ok(Item {
                id: r.get(0)?,
                name: r.get(1)?,
                qty: r.get(2)?,
            })
        })
        .map_err(err)?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(err)
}

/// The rows `part` sees: committed, with transaction `tx`'s staged rows on top.
fn visible_items(db: &Connection, part: &str, tx: Option<&str>) -> Result<BTreeMap<i32, Item>> {
    seed(db, part, false)?;
    let mut rows: BTreeMap<i32, Item> = items_of(
        db,
        "SELECT id, name, qty FROM items WHERE part = ?1",
        &[&part],
    )?
    .into_iter()
    .map(|i| (i.id, i))
    .collect();
    if let Some(tx) = tx {
        for i in items_of(
            db,
            "SELECT id, name, qty FROM staged WHERE part = ?1 AND tx = ?2",
            &[&part, &tx],
        )? {
            rows.insert(i.id, i);
        }
    }
    Ok(rows)
}

fn log_event(db: &Connection, part: &str, kind: &str, tx: Option<&str>, ok: bool) -> Result<()> {
    db.execute(
        "INSERT INTO events (part, kind, tx, ok) VALUES (?1, ?2, ?3, ?4)",
        params![part, kind, tx, ok as i64],
    )
    .map_err(err)?;
    Ok(())
}

// ------------------------------------------------------------------ tables

/// `items`' user columns.
fn items_columns() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("qty", DataType::Int64, true),
    ]))
}

/// `items`' columns as served: the user columns plus the row id.
fn items_table_columns() -> SchemaRef {
    let mut fields: Vec<Field> = items_columns()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(row_id_field("rowid"));
    Arc::new(Schema::new(fields))
}

fn labels_columns() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, true),
    ]))
}

fn items_batch(rows: &[Item], with_rowid: bool) -> Result<RecordBatch> {
    let mut cols: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(
            rows.iter().map(|r| r.id).collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(
            rows.iter().map(|r| r.name.clone()).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.qty).collect::<Vec<_>>(),
        )),
    ];
    let schema = if with_rowid {
        cols.push(Arc::new(Int64Array::from(
            rows.iter().map(|r| r.id as i64).collect::<Vec<_>>(),
        )));
        items_table_columns()
    } else {
        items_columns()
    };
    RecordBatch::try_new(schema, cols).map_err(err)
}

fn volatile(description: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: description.to_string(),
        stability: Some("VOLATILE".to_string()),
        categories: vec!["testing".into()],
        ..Default::default()
    }
}

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

/// A no-argument table function whose single batch is computed at produce
/// time from the call's [`ProcessParams`] (attach and transaction included).
struct OneShotFn {
    name: &'static str,
    description: &'static str,
    schema: fn() -> SchemaRef,
    projection_pushdown: bool,
    rows: fn(&ProcessParams) -> Result<RecordBatch>,
}

impl TableFunction for OneShotFn {
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
        let full = (self.rows)(params)?;
        // Position-by-position: the projection may repeat a column.
        let batch = project_to_output(
            &full,
            &params.output_schema,
            params.projection_ids.as_deref(),
        )?;
        Ok(Box::new(OneShot(Some(batch))))
    }
}

fn items_scan_rows(p: &ProcessParams) -> Result<RecordBatch> {
    let attach = decode_attach(p.attach_opaque_data.as_deref())?;
    let part = part_of(&attach);
    let tx = tx_of(p.transaction_opaque_data.as_deref());
    let rows = with_state(|db| visible_items(db, &part, tx.as_deref()))?;
    items_batch(&rows.into_values().collect::<Vec<_>>(), true)
}

fn labels_scan_rows(p: &ProcessParams) -> Result<RecordBatch> {
    let part = part_of(&decode_attach(p.attach_opaque_data.as_deref())?);
    let rows: Vec<(i32, Option<String>)> = with_state(|db| {
        seed(db, &part, false)?;
        let mut stmt = db
            .prepare("SELECT id, label FROM labels WHERE part = ?1 ORDER BY id")
            .map_err(err)?;
        let rows = stmt
            .query_map([&part], |r| Ok((r.get(0)?, r.get(1)?)))
            .map_err(err)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(err)
    })?;
    RecordBatch::try_new(
        labels_columns(),
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(err)
}

/// `items_update`: backs `UPDATE main.items`. Applies the new values, or, in
/// a transaction on a transactional attach, stages them under its id.
struct ItemsUpdate;

/// Row `i` of `col` as an i64 (any integer type), or `None` when NULL.
fn int_at(col: &ArrayRef, i: usize) -> Result<Option<i64>> {
    if col.is_null(i) {
        return Ok(None);
    }
    let c = arrow_cast::cast(col, &DataType::Int64).map_err(err)?;
    Ok(Some(
        c.as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64")
            .value(i),
    ))
}

fn str_at(col: &ArrayRef, i: usize) -> Result<Option<String>> {
    if col.is_null(i) {
        return Ok(None);
    }
    let c = arrow_cast::cast(col, &DataType::Utf8).map_err(err)?;
    Ok(Some(
        c.as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8")
            .value(i)
            .to_string(),
    ))
}

impl TableInOutFunction for ItemsUpdate {
    fn name(&self) -> &str {
        "items_update"
    }
    fn metadata(&self) -> FunctionMetadata {
        volatile("Backs UPDATE main.items: applies (or, in a transaction, stages) the new values")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        write_function_arg_specs("The SET columns plus rowid of each row to update")
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let mode = WriteOptions::from_arguments(&params.arguments)?.result_mode;
        Ok(BindResponse {
            output_schema: write_result_schema(mode, &items_table_columns()),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, p: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let attach = decode_attach(p.attach_opaque_data.as_deref())?;
        let part = part_of(&attach);
        let tx = if attach.transactional {
            tx_of(p.transaction_opaque_data.as_deref())
        } else {
            None
        };
        let rowids = batch
            .column_by_name("rowid")
            .ok_or_else(|| RpcError::value_error("txfix: update input has no rowid"))?;
        let (before, after) = with_state(|db| {
            let mut current = visible_items(db, &part, tx.as_deref())?;
            let mut before = Vec::new();
            let mut after = Vec::new();
            // Validate every row before applying any.
            for i in 0..batch.num_rows() {
                let rowid = int_at(rowids, i)?.unwrap_or(-1);
                let old = i32::try_from(rowid)
                    .ok()
                    .and_then(|id| current.get(&id))
                    .cloned()
                    .ok_or_else(|| err(format!("txfix: no row with rowid {rowid}")))?;
                let mut next = old.clone();
                for (f, col) in batch.schema().fields().iter().zip(batch.columns()) {
                    match f.name().as_str() {
                        "rowid" => {}
                        "id" => {
                            if int_at(col, i)? != Some(old.id as i64) {
                                return Err(err("txfix: id is not updatable"));
                            }
                        }
                        "name" => {
                            next.name = Some(
                                str_at(col, i)?
                                    .ok_or_else(|| err("txfix: name must not be NULL"))?,
                            );
                        }
                        "qty" => next.qty = int_at(col, i)?,
                        other => {
                            return Err(err(format!("txfix: {other} is not a column of items")))
                        }
                    }
                }
                if tx.is_none() && next.qty.is_some_and(|q| q < 0) {
                    return Err(err("txfix: qty must be >= 0"));
                }
                current.insert(next.id, next.clone());
                before.push(old);
                after.push(next);
            }
            for r in &after {
                match &tx {
                    Some(tx) => db.execute(
                        "INSERT OR REPLACE INTO staged (part, tx, id, name, qty) VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![part, tx, r.id, r.name, r.qty],
                    ),
                    None => db.execute(
                        "UPDATE items SET name = ?1, qty = ?2 WHERE part = ?3 AND id = ?4",
                        params![r.name, r.qty, part, r.id],
                    ),
                }
                .map_err(err)?;
            }
            Ok((before, after))
        })?;
        let mode = WriteOptions::from_arguments(&p.arguments)?.result_mode;
        Ok(vec![write_result_batch(
            mode,
            &items_table_columns(),
            Some(&items_batch(&before, false)?),
            Some(&items_batch(&after, false)?),
        )?])
    }
}

/// `tag_tx(TABLE)`: every input column, plus the transaction id seen at
/// process time.
struct TagTx;

impl TableInOutFunction for TagTx {
    fn name(&self) -> &str {
        "tag_tx"
    }
    fn metadata(&self) -> FunctionMetadata {
        volatile("Pass every input row through, adding the transaction id seen while processing it")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::column("data", 0, "table", "Input table")]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        let input = params
            .input_schema
            .clone()
            .ok_or_else(|| RpcError::value_error("tag_tx requires an input table"))?;
        let mut fields: Vec<Field> = input.fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.push(Field::new("tx_id", DataType::Utf8, true));
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(fields)),
            opaque_data: Vec::new(),
        })
    }
    fn process(&self, p: &ProcessParams, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        let tx = tx_of(p.transaction_opaque_data.as_deref());
        let mut cols = batch.columns().to_vec();
        cols.push(Arc::new(StringArray::from(vec![tx; batch.num_rows()])));
        let mut fields: Vec<Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new("tx_id", DataType::Utf8, true));
        Ok(vec![RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            cols,
        )
        .map_err(err)?])
    }
}

fn ok_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("ok", DataType::Boolean, true)]))
}

fn reset_rows(p: &ProcessParams) -> Result<RecordBatch> {
    let part = part_of(&decode_attach(p.attach_opaque_data.as_deref())?);
    with_state(|db| seed(db, &part, true))?;
    RecordBatch::try_new(ok_schema(), vec![Arc::new(BooleanArray::from(vec![true]))]).map_err(err)
}

fn whoami_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("token", DataType::Utf8, true),
        Field::new("transactional", DataType::Boolean, true),
        Field::new("token_in_attach_data", DataType::Boolean, true),
    ]))
}

fn whoami_rows(p: &ProcessParams) -> Result<RecordBatch> {
    let raw = p.attach_opaque_data.as_deref().unwrap_or_default();
    let o = decode_attach(Some(raw))?;
    // Whether the token's bytes appear in the attach bytes this function was
    // handed (opened on HTTP, raw elsewhere): they must not.
    let in_data = o
        .token
        .as_ref()
        .map(|t| !t.is_empty() && raw.windows(t.len()).any(|w| w == t.as_bytes()));
    RecordBatch::try_new(
        whoami_schema(),
        vec![
            Arc::new(StringArray::from(vec![o.token])),
            Arc::new(BooleanArray::from(vec![o.transactional])),
            Arc::new(BooleanArray::from(vec![in_data.unwrap_or(false)])),
        ],
    )
    .map_err(err)
}

fn current_tx_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("in_tx", DataType::Boolean, true),
        Field::new("tx_id", DataType::Utf8, true),
    ]))
}

fn current_tx_rows(p: &ProcessParams) -> Result<RecordBatch> {
    let tx = tx_of(p.transaction_opaque_data.as_deref());
    RecordBatch::try_new(
        current_tx_schema(),
        vec![
            Arc::new(BooleanArray::from(vec![tx.is_some()])),
            Arc::new(StringArray::from(vec![tx])),
        ],
    )
    .map_err(err)
}

fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("seq", DataType::Int64, true),
        Field::new("kind", DataType::Utf8, true),
        Field::new("tx_id", DataType::Utf8, true),
        Field::new("ok", DataType::Boolean, true),
    ]))
}

type EventRow = (i64, String, Option<String>, i64);

fn events_rows(p: &ProcessParams) -> Result<RecordBatch> {
    let part = part_of(&decode_attach(p.attach_opaque_data.as_deref())?);
    let rows: Vec<EventRow> = with_state(|db| {
        let mut stmt = db
            .prepare("SELECT seq, kind, tx, ok FROM events WHERE part = ?1 ORDER BY seq")
            .map_err(err)?;
        let rows = stmt
            .query_map([&part], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .map_err(err)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(err)
    })?;
    RecordBatch::try_new(
        events_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.2.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.3 == 1).collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(err)
}

// ------------------------------------------------------------ transactions

/// The catalog's transaction hooks.
struct TxfixTransactions;

impl CatalogTransactions for TxfixTransactions {
    /// Per attach: only `transactional true` asks DuckDB for the hooks below.
    fn supports_transactions(&self, options: Option<&RecordBatch>) -> Result<bool> {
        Ok(options_of(options).transactional)
    }

    fn begin(&self, attach_opaque_data: &[u8]) -> Result<Option<Vec<u8>>> {
        let id: [u8; 16] = rand::random();
        let part = part_of(&decode_attach(Some(attach_opaque_data))?);
        with_state(|db| {
            seed(db, &part, false)?;
            log_event(db, &part, "begin", Some(&hex(&id)), true)
        })?;
        Ok(Some(id.to_vec()))
    }

    fn commit(&self, attach_opaque_data: &[u8], transaction_opaque_data: &[u8]) -> Result<()> {
        let part = part_of(&decode_attach(Some(attach_opaque_data))?);
        let tx = hex(transaction_opaque_data);
        let refusal = with_state(|db| {
            let rows = items_of(
                db,
                "SELECT id, name, qty FROM staged WHERE part = ?1 AND tx = ?2 ORDER BY id",
                &[&part, &tx],
            )?;
            db.execute(
                "DELETE FROM staged WHERE part = ?1 AND tx = ?2",
                params![part, tx],
            )
            .map_err(err)?;
            if let Some(bad) = rows.iter().find(|r| r.qty.is_some_and(|q| q < 0)) {
                log_event(db, &part, "commit", Some(&tx), false)?;
                return Ok(Some(format!(
                    "txfix: commit refused: qty {} for id {}",
                    bad.qty.unwrap_or_default(),
                    bad.id
                )));
            }
            for r in &rows {
                db.execute(
                    "UPDATE items SET name = ?1, qty = ?2 WHERE part = ?3 AND id = ?4",
                    params![r.name, r.qty, part, r.id],
                )
                .map_err(err)?;
            }
            log_event(db, &part, "commit", Some(&tx), true)?;
            Ok(None)
        })?;
        match refusal {
            Some(message) => Err(RpcError::runtime_error(message)),
            None => Ok(()),
        }
    }

    fn rollback(&self, attach_opaque_data: &[u8], transaction_opaque_data: &[u8]) -> Result<()> {
        let part = part_of(&decode_attach(Some(attach_opaque_data))?);
        let tx = hex(transaction_opaque_data);
        with_state(|db| {
            db.execute(
                "DELETE FROM staged WHERE part = ?1 AND tx = ?2",
                params![part, tx],
            )
            .map_err(err)?;
            log_event(db, &part, "rollback", Some(&tx), true)
        })
    }
}

// ----------------------------------------------------------------- catalog

fn table_constraints(table: &str, not_null: &[&str], key: &str) -> Vec<vgi_rpc::Bytes> {
    let mut records: Vec<ConstraintRecord> = not_null
        .iter()
        .map(|col| {
            ConstraintRecord::new(
                &["main"],
                table,
                &format!("{col}_not_null"),
                ConstraintKind::NotNull,
            )
            .with_columns(&[&[*col]])
            .enforced()
        })
        .collect();
    records.push(
        ConstraintRecord::new(&["main"], table, "pk", ConstraintKind::PrimaryKey)
            .with_columns(&[&[key]])
            .enforced(),
    );
    records
        .iter()
        .map(|r| constraint_record(r).expect("valid txfix constraint record"))
        .collect()
}

/// The `txfix` catalog model.
pub fn catalog() -> CatalogModel {
    let transactional_default: ArrayRef = Arc::new(BooleanArray::from(vec![false]));
    let transactional = vgi::catalog::serialize_attach_option_spec_with_flags(
        "transactional",
        "Stage UPDATEs in DuckDB transactions (supports_transactions for this attach)",
        &DataType::Boolean,
        Some(&transactional_default),
        AttachOptionFlags::default(),
    )
    .expect("transactional spec");
    let token = vgi::catalog::serialize_attach_option_spec_with_flags(
        "token",
        "A credential every function must receive",
        &DataType::Utf8,
        None,
        AttachOptionFlags {
            required: false,
            secret: true,
        },
    )
    .expect("token spec");

    let mut items = CatTable::with_function(
        "items",
        items_table_columns(),
        Arc::new(OneShotFn {
            name: "items_scan",
            description:
                "The rows of main.items (staged changes included inside a transaction), with rowid",
            schema: items_table_columns,
            // DuckDB reads `rowid` as a virtual column, which needs projection pushdown.
            projection_pushdown: true,
            rows: items_scan_rows,
        }),
        Some("Writable: UPDATE name/qty (staged per transaction on a transactional attach)".into()),
        None,
    )
    .with_update_function(Arc::new(ItemsUpdate), WriteResultMode::Changes);
    items.constraints = table_constraints("items", &["id", "name"], "id");

    let mut labels = CatTable::with_function(
        "labels",
        labels_columns(),
        Arc::new(OneShotFn {
            name: "labels_scan",
            description: "The rows of main.labels",
            schema: labels_columns,
            projection_pushdown: false,
            rows: labels_scan_rows,
        }),
        Some("Read-only".into()),
        None,
    );
    labels.constraints = table_constraints("labels", &["id"], "id");
    // Fixture contract: `labels_scan` is an ordinary SQL callable; `items_scan`
    // and `items_update` stay `table_only` (the default).
    labels.expose_scan = true;

    CatalogModel {
        name: CATALOG_NAME.to_string(),
        comment: Some(
            "Writable-table and transaction-hook fixture (vgi docs/writable_tx_fixtures.md)"
                .to_string(),
        ),
        attach_option_specs: vec![transactional, token],
        transactions: Some(Arc::new(TxfixTransactions)),
        schemas: vec![CatSchema {
            name: "main".to_string(),
            tables: vec![items, labels],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Register the fixture's functions. The scan and write functions ride in
/// the catalog's tables and are registered by `Worker::set_catalog`.
pub fn register(w: &mut vgi::Worker) {
    let one_shot = |name, description, schema, rows| OneShotFn {
        name,
        description,
        schema,
        projection_pushdown: false,
        rows,
    };
    w.register_table(one_shot(
        "reset",
        "Restore this token's initial rows; clear its staged changes and events",
        ok_schema,
        reset_rows,
    ));
    w.register_table(one_shot(
        "whoami",
        "The attach's options as functions see them",
        whoami_schema,
        whoami_rows,
    ));
    w.register_table(one_shot(
        "current_tx",
        "The plaintext transaction id this call received (lowercase hex)",
        current_tx_schema,
        current_tx_rows,
    ));
    w.register_table(one_shot(
        "events",
        "This token's transaction-hook event log",
        events_schema,
        events_rows,
    ));
    w.register_table_in_out(TagTx);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(token: Option<&str>, transactional: Option<bool>) -> RecordBatch {
        let mut fields = Vec::new();
        let mut cols: Vec<ArrayRef> = Vec::new();
        if let Some(t) = token {
            fields.push(Field::new("token", DataType::Utf8, true));
            cols.push(Arc::new(StringArray::from(vec![t])));
        }
        if let Some(t) = transactional {
            fields.push(Field::new("transactional", DataType::Boolean, true));
            cols.push(Arc::new(BooleanArray::from(vec![t])));
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    #[test]
    fn the_catalog_validates_and_negotiates_enforce() {
        let model = catalog();
        model.validate_tables().unwrap();
        assert!(model.has_constraint_records());
        assert!(model.has_writable_constrained_tables());
    }

    /// Fixture contract (vgi docs/writable_tx_fixtures.md): `items_scan` and
    /// `items_update` are listed `table_only`, `labels_scan` is public.
    #[test]
    fn items_functions_are_table_only_and_labels_scan_is_exposed() {
        let table_only = catalog().table_only_function_names();
        assert!(table_only.contains("items_scan"));
        assert!(table_only.contains("items_update"));
        assert!(!table_only.contains("labels_scan"));
    }

    #[test]
    fn transactions_are_decided_by_the_transactional_option() {
        assert!(TxfixTransactions
            .supports_transactions(Some(&options(Some("x"), Some(true))))
            .unwrap());
        assert!(!TxfixTransactions
            .supports_transactions(Some(&options(Some("x"), None)))
            .unwrap());
        assert!(!TxfixTransactions.supports_transactions(None).unwrap());
    }
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Filter pushdown: deserialize the `pushdown_filters` blob, evaluate it
//! against a batch, and apply it.
//!
//! Wire format: an IPC batch whose column 0 is a JSON array of filter specs
//! (field metadata `vgi_filter_version`), and columns 1.. carry the constant
//! values (`value_ref N` → column N+1, scalar at row 0). Top-level filters
//! combine with AND.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, Scalar, StringArray};
use arrow_buffer::BooleanBuffer;
use serde::Deserialize;
use vgi_rpc::{Result, RpcError};

use crate::ipc;

/// SQL comparison symbol for a VGI op token (matches Python `op.symbol`).
fn op_symbol(op: &str) -> &'static str {
    match op {
        "eq" => "=",
        "ne" => "!=",
        "lt" => "<",
        "le" => "<=",
        "gt" => ">",
        "ge" => ">=",
        _ => "?",
    }
}

/// Render a scalar at `i` the way the Python fixtures do: strings single-quoted,
/// booleans as `True`/`False`, nulls as `NULL`, everything else via its display.
fn fmt_scalar(arr: &ArrayRef, i: usize) -> String {
    use arrow_schema::DataType;
    if arr.is_null(i) {
        return "NULL".to_string();
    }
    match arr.data_type() {
        DataType::Utf8 => format!("'{}'", arr.as_string::<i32>().value(i)),
        DataType::LargeUtf8 => format!("'{}'", arr.as_string::<i64>().value(i)),
        DataType::Boolean => {
            if arr.as_boolean().value(i) {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        _ => arrow_cast::cast(&arr.slice(i, 1), &DataType::Utf8)
            .ok()
            .map(|a| a.as_string::<i32>().value(0).to_string())
            .unwrap_or_default(),
    }
}

#[derive(Deserialize)]
struct FilterSpec {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    column_name: String,
    #[serde(default)]
    column_index: i64,
    #[serde(default)]
    op: Option<String>,
    #[serde(default)]
    value_ref: Option<usize>,
    /// `join_keys` filters reference a column in the side join-keys batches.
    #[serde(default)]
    keys_column: Option<String>,
    #[serde(default)]
    children: Vec<FilterSpec>,
    #[serde(default)]
    child_filter: Option<Box<FilterSpec>>,
    /// For `struct` filters: which struct field the child filter targets.
    #[serde(default)]
    child_index: i64,
    #[serde(default)]
    child_name: String,
}

/// A parsed, evaluable set of pushdown filters.
pub struct PushdownFilters {
    specs: Vec<FilterSpec>,
    values: Vec<ArrayRef>, // value_ref N → values[N]
    /// `keys_column` → values, from the InitRequest join-keys batches.
    join_keys: std::collections::HashMap<String, ArrayRef>,
}

/// Numeric `[min, max]` bounds on a column implied by pushed-down comparison
/// filters (integer-coerced). Returned by
/// [`PushdownFilters::get_column_bounds`]; mirrors the Python/Go `ColumnBounds`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnBounds {
    pub min: Option<i64>,
    pub max: Option<i64>,
}

/// A read-only view of one pushed-down filter — a structural projection of the
/// internal filter AST, mirroring the Python/Go filter objects. Returned by
/// [`PushdownFilters::get_column_filters`] / [`PushdownFilters::filters`].
#[derive(Debug, Clone)]
pub enum Filter {
    /// `column <op> value` (`op` is one of `eq`/`ne`/`lt`/`le`/`gt`/`ge`).
    Constant { column_name: String, op: String },
    /// `column IN (...)`.
    In { column_name: String },
    /// `column IN (...)` resolved from side join keys.
    JoinKeys { column_name: String },
    /// `column IS NULL`.
    IsNull { column_name: String },
    /// `column IS NOT NULL`.
    IsNotNull { column_name: String },
    /// Conjunction of child filters.
    And(Vec<Filter>),
    /// Disjunction of child filters.
    Or(Vec<Filter>),
    /// A filter on `column.child_name` (a struct subfield).
    Struct {
        column_name: String,
        child_name: String,
        child: Box<Filter>,
    },
    /// Any other filter kind (e.g. an expression filter), with its raw tag.
    Other { kind: String, column_name: String },
}

impl Filter {
    fn from_spec(spec: &FilterSpec) -> Filter {
        match spec.kind.as_str() {
            "constant" => Filter::Constant {
                column_name: spec.column_name.clone(),
                op: spec.op.clone().unwrap_or_default(),
            },
            "in" => Filter::In {
                column_name: spec.column_name.clone(),
            },
            "join_keys" => Filter::JoinKeys {
                column_name: spec.column_name.clone(),
            },
            "is_null" => Filter::IsNull {
                column_name: spec.column_name.clone(),
            },
            "is_not_null" => Filter::IsNotNull {
                column_name: spec.column_name.clone(),
            },
            "and" => Filter::And(spec.children.iter().map(Filter::from_spec).collect()),
            "or" => Filter::Or(spec.children.iter().map(Filter::from_spec).collect()),
            "struct" => Filter::Struct {
                column_name: spec.column_name.clone(),
                child_name: spec.child_name.clone(),
                child: Box::new(
                    spec.child_filter
                        .as_ref()
                        .map(|c| Filter::from_spec(c))
                        .unwrap_or(Filter::Other {
                            kind: "struct".to_string(),
                            column_name: spec.column_name.clone(),
                        }),
                ),
            },
            other => Filter::Other {
                kind: other.to_string(),
                column_name: spec.column_name.clone(),
            },
        }
    }

    /// The column this filter references (empty string for `And`/`Or`).
    pub fn column_name(&self) -> &str {
        match self {
            Filter::Constant { column_name, .. }
            | Filter::In { column_name }
            | Filter::JoinKeys { column_name }
            | Filter::IsNull { column_name }
            | Filter::IsNotNull { column_name }
            | Filter::Struct { column_name, .. }
            | Filter::Other { column_name, .. } => column_name,
            Filter::And(_) | Filter::Or(_) => "",
        }
    }
}

impl PushdownFilters {
    /// Parse the `pushdown_filters` IPC blob (no join keys).
    pub fn parse(bytes: &[u8]) -> Result<PushdownFilters> {
        Self::parse_with_join_keys(bytes, &[])
    }

    /// Parse a per-tick dynamic filter from the base64-encoded IPC carried in
    /// the `vgi_pushdown_filters` request metadata. `None` for empty/invalid.
    pub fn parse_b64(encoded: &str, join_keys: &[Vec<u8>]) -> Option<PushdownFilters> {
        if encoded.is_empty() {
            return None;
        }
        let raw = b64_decode(encoded)?;
        Self::parse_with_join_keys(&raw, join_keys).ok()
    }

    /// Parse the filter blob, resolving `join_keys` filters against the
    /// supplied side join-keys IPC batches (one column each).
    pub fn parse_with_join_keys(bytes: &[u8], join_keys: &[Vec<u8>]) -> Result<PushdownFilters> {
        let batch = ipc::read_batch(bytes)?;
        if batch.num_columns() == 0 {
            return Err(RpcError::value_error("filter batch has no columns"));
        }
        let json = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| RpcError::value_error("filter column 0 is not a string"))?;
        if json.is_empty() {
            return Err(RpcError::value_error("filter column 0 is empty"));
        }
        if std::env::var("VGI_FILTER_DEBUG").is_ok() {
            eprintln!("[vgi-filter] json={}", json.value(0));
        }
        let specs: Vec<FilterSpec> = serde_json::from_str(json.value(0))
            .map_err(|e| RpcError::value_error(format!("parsing filter JSON: {e}")))?;
        // value_ref N resolves to column N+1.
        let values: Vec<ArrayRef> = (1..batch.num_columns())
            .map(|i| batch.column(i).clone())
            .collect();
        let mut jk = std::collections::HashMap::new();
        for blob in join_keys {
            if let Ok(b) = ipc::read_batch(blob) {
                for (i, f) in b.schema().fields().iter().enumerate() {
                    jk.insert(f.name().clone(), b.column(i).clone());
                }
            }
        }
        Ok(PushdownFilters {
            specs,
            values,
            join_keys: jk,
        })
    }

    /// Resolve the value array for a `join_keys` filter spec.
    fn join_value(&self, spec: &FilterSpec) -> Option<&ArrayRef> {
        spec.keys_column
            .as_ref()
            .and_then(|c| self.join_keys.get(c))
    }

    /// Summarize the filters on integer `column`: the total `IN`-list / join-key
    /// value count, and the min/max range bounds (from `>`/`<`/`=` constants).
    /// Used by late-materialization witnesses to report the pushed rowid filter.
    pub fn column_summary(&self, column: &str) -> (usize, Option<i64>, Option<i64>) {
        let mut in_count = 0usize;
        let mut lo: Option<i64> = None;
        let mut hi: Option<i64> = None;
        let val_i64 = |a: &ArrayRef| -> Option<i64> {
            let c = arrow_cast::cast(a, &arrow_schema::DataType::Int64).ok()?;
            let arr = c.as_any().downcast_ref::<arrow_array::Int64Array>()?;
            // A malformed/empty filter constant must not index row 0.
            (!arr.is_empty() && arr.is_valid(0)).then(|| arr.value(0))
        };
        let mut stack: Vec<&FilterSpec> = self.specs.iter().collect();
        while let Some(spec) = stack.pop() {
            match spec.kind.as_str() {
                "and" | "or" => stack.extend(spec.children.iter()),
                "in" if spec.column_name == column => {
                    if let Some(r) = spec.value_ref {
                        in_count += self.values.get(r).map(|a| a.len()).unwrap_or(0);
                    }
                }
                "join_keys" if spec.column_name == column => {
                    in_count += self.join_value(spec).map(|a| a.len()).unwrap_or(0);
                }
                "constant" if spec.column_name == column => {
                    let v = spec
                        .value_ref
                        .and_then(|r| self.values.get(r))
                        .and_then(val_i64);
                    if let Some(v) = v {
                        match spec.op.as_deref().unwrap_or("eq") {
                            "gt" | "ge" | "gteq" | ">" | ">=" => {
                                lo = Some(lo.map_or(v, |l| l.min(v)))
                            }
                            "lt" | "le" | "lteq" | "<" | "<=" => {
                                hi = Some(hi.map_or(v, |h| h.max(v)))
                            }
                            _ => {
                                lo = Some(v);
                                hi = Some(v);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        (in_count, lo, hi)
    }

    /// Filter `batch` to the rows that satisfy all top-level filters.
    pub fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mask = self.evaluate(batch)?;
        arrow_select::filter::filter_record_batch(batch, &mask)
            .map_err(|e| RpcError::runtime_error(format!("filter batch: {e}")))
    }

    /// Evaluate to a boolean mask (AND of all top-level filters).
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        let n = batch.num_rows();
        let mut acc: Option<BooleanArray> = None;
        for spec in &self.specs {
            let m = self.eval_spec(spec, batch)?;
            acc = Some(match acc {
                None => m,
                Some(a) => and_kleene(&a, &m)?,
            });
        }
        Ok(acc.unwrap_or_else(|| all_true(n)))
    }

    fn column<'a>(&self, spec: &FilterSpec, batch: &'a RecordBatch) -> Result<&'a ArrayRef> {
        if let Some(c) = batch.schema().column_with_name(&spec.column_name) {
            return Ok(batch.column(c.0));
        }
        let idx = spec.column_index as usize;
        batch.columns().get(idx).ok_or_else(|| {
            RpcError::value_error(format!("filter column {} not found", spec.column_name))
        })
    }

    fn value(&self, spec: &FilterSpec) -> Result<&ArrayRef> {
        let r = spec
            .value_ref
            .ok_or_else(|| RpcError::value_error("filter missing value_ref"))?;
        self.values
            .get(r)
            .ok_or_else(|| RpcError::value_error(format!("value_ref {r} out of range")))
    }

    /// Format the pushed-down filters as a human-readable SQL-like string,
    /// matching the Python fixtures' `_format_pushed_filters`. Returns
    /// `"(none)"` when there are no filters.
    pub fn format_pushed(&self) -> String {
        if self.specs.is_empty() {
            return "(none)".to_string();
        }
        let parts: Vec<String> = self
            .specs
            .iter()
            .map(|s| self.format_one(s, None))
            .collect();
        if parts.is_empty() {
            "(none)".to_string()
        } else {
            parts.join(" AND ")
        }
    }

    /// Format the filters as the Python `repr(PushdownFilters)`:
    /// `PushdownFilters([ConstantFilter(n < X), …])`. Returns `"(none)"` when
    /// empty (matching the dynamic-filter witness fixtures).
    pub fn format_repr(&self) -> String {
        if self.specs.is_empty() {
            return "(none)".to_string();
        }
        let parts: Vec<String> = self.specs.iter().map(|s| self.repr_one(s, None)).collect();
        format!("PushdownFilters([{}])", parts.join(", "))
    }

    fn repr_one(&self, spec: &FilterSpec, col_override: Option<&str>) -> String {
        let col = col_override.unwrap_or(&spec.column_name);
        match spec.kind.as_str() {
            "is_null" => format!("IsNullFilter({col} IS NULL)"),
            "is_not_null" => format!("IsNotNullFilter({col} IS NOT NULL)"),
            "constant" => {
                let sym = op_symbol(spec.op.as_deref().unwrap_or("eq"));
                let v = self
                    .value(spec)
                    .ok()
                    .map(|a| fmt_scalar(a, 0))
                    .unwrap_or_default();
                format!("ConstantFilter({col} {sym} {v})")
            }
            "in" => match self.value(spec) {
                Ok(vals) => {
                    let items: Vec<String> = (0..vals.len()).map(|i| fmt_scalar(vals, i)).collect();
                    format!("InFilter({col} IN [{}])", items.join(", "))
                }
                Err(_) => format!("InFilter({col} IN [])"),
            },
            "join_keys" => match self.join_value(spec) {
                Some(vals) => {
                    let items: Vec<String> = (0..vals.len()).map(|i| fmt_scalar(vals, i)).collect();
                    format!("InFilter({col} IN [{}])", items.join(", "))
                }
                None => format!("InFilter({col} IN [])"),
            },
            "and" => {
                let parts: Vec<String> = spec
                    .children
                    .iter()
                    .map(|c| self.repr_one(c, None))
                    .collect();
                format!("AndFilter({})", parts.join(" AND "))
            }
            "or" => {
                let parts: Vec<String> = spec
                    .children
                    .iter()
                    .map(|c| self.repr_one(c, None))
                    .collect();
                format!("OrFilter({})", parts.join(" OR "))
            }
            "struct" => match &spec.child_filter {
                Some(child) => {
                    let nested = format!("{}.{}", spec.column_name, spec.child_name);
                    self.repr_one(child, Some(&nested))
                }
                None => col.to_string(),
            },
            other => other.to_string(),
        }
    }

    fn format_one(&self, spec: &FilterSpec, col_override: Option<&str>) -> String {
        let col = col_override.unwrap_or(&spec.column_name);
        match spec.kind.as_str() {
            "is_null" => format!("{col} IS NULL"),
            "is_not_null" => format!("{col} IS NOT NULL"),
            "constant" => {
                let sym = op_symbol(spec.op.as_deref().unwrap_or("eq"));
                let v = self
                    .value(spec)
                    .ok()
                    .map(|a| fmt_scalar(a, 0))
                    .unwrap_or_default();
                format!("{col} {sym} {v}")
            }
            "in" => match self.value(spec) {
                Ok(vals) if vals.len() > 20 => format!("{col} IN ({} values)", vals.len()),
                Ok(vals) => {
                    let items: Vec<String> = (0..vals.len()).map(|i| fmt_scalar(vals, i)).collect();
                    format!("{col} IN ({})", items.join(", "))
                }
                Err(_) => format!("{col} IN ()"),
            },
            "join_keys" => match self.join_value(spec) {
                Some(vals) if vals.len() > 20 => format!("{col} IN ({} values)", vals.len()),
                Some(vals) => {
                    let items: Vec<String> = (0..vals.len()).map(|i| fmt_scalar(vals, i)).collect();
                    format!("{col} IN ({})", items.join(", "))
                }
                None => format!("{col} IN ()"),
            },
            "and" => {
                let parts: Vec<String> = spec
                    .children
                    .iter()
                    .map(|c| self.format_one(c, None))
                    .collect();
                format!("({})", parts.join(" AND "))
            }
            "or" => {
                let parts: Vec<String> = spec
                    .children
                    .iter()
                    .map(|c| self.format_one(c, None))
                    .collect();
                format!("({})", parts.join(" OR "))
            }
            "struct" => match &spec.child_filter {
                Some(child) => {
                    let nested = format!("{}.{}", spec.column_name, spec.child_name);
                    self.format_one(child, Some(&nested))
                }
                None => col.to_string(),
            },
            other => other.to_string(),
        }
    }

    /// Resolve the discrete value set for a column as `i64`s (the
    /// partition-pruning idiom; values coerced to integer). Returns `None` when
    /// the predicate is not enumerable (no filter, bare range, OR with a
    /// non-discrete branch). For string columns or to preserve the native type,
    /// use [`PushdownFilters::get_column_values`].
    pub fn get_column_values_i64(&self, column: &str) -> Option<Vec<i64>> {
        let mut acc: Option<Vec<i64>> = None;
        for spec in &self.specs {
            let vs = self.column_values_of(spec, column)?;
            // AND across top-level filters: intersect discrete sets.
            acc = Some(match acc {
                None => vs,
                Some(prev) => prev.into_iter().filter(|v| vs.contains(v)).collect(),
            });
        }
        acc
    }

    /// The set of column names referenced by the top-level filters (and their
    /// AND/OR/struct children). Mirrors Python `{f.column_name for f in pf}` and
    /// the Go `FilteredColumns()`. Lets a worker discover which columns a query
    /// constrains (e.g. to enforce a required-column rule).
    pub fn filtered_columns(&self) -> std::collections::HashSet<String> {
        let mut out = std::collections::HashSet::new();
        for spec in &self.specs {
            collect_columns(spec, &mut out);
        }
        out
    }

    /// Whether any pushed-down filter references `column`. Mirrors Python
    /// `column in pf` and the Go `HasFilterForColumn`.
    pub fn has_filter_for_column(&self, column: &str) -> bool {
        self.specs.iter().any(|s| spec_mentions(s, column))
    }

    /// Resolve the discrete `=`/`IN` value set for `column` as a *typed* Arrow
    /// array (preserving the column's native type, so string columns like `path`
    /// are usable). Descends one level into a top-level `AND`. Mirrors Python
    /// `PushdownFilters.get_column_values` and the Go `GetColumnValues`. Returns
    /// `None` when the predicate is not a simple enumerable equality/IN on
    /// `column`. For an integer-coerced `Vec<i64>`, use
    /// [`PushdownFilters::get_column_values_i64`].
    pub fn get_column_values(&self, column: &str) -> Option<ArrayRef> {
        for spec in &self.specs {
            if let Some(a) = self.column_values_array_of(spec, column) {
                return Some(a);
            }
        }
        None
    }

    fn column_values_array_of(&self, spec: &FilterSpec, column: &str) -> Option<ArrayRef> {
        match spec.kind.as_str() {
            "in" if spec.column_name == column => self.value(spec).ok().cloned(),
            "join_keys" if spec.column_name == column => self.join_value(spec).cloned(),
            "constant" if spec.column_name == column && spec.op.as_deref() == Some("eq") => {
                self.value(spec).ok().map(|v| v.slice(0, 1))
            }
            "and" => spec
                .children
                .iter()
                .find_map(|c| self.column_values_array_of(c, column)),
            _ => None,
        }
    }

    /// The single `=` constant for `column` as a length-1 Arrow array, or `None`
    /// if there is no equality filter on it. Mirrors Python
    /// `get_column_constant` and the Go `GetColumnConstant`. Descends one level
    /// into a top-level `AND`.
    pub fn get_column_constant(&self, column: &str) -> Option<ArrayRef> {
        fn find(this: &PushdownFilters, spec: &FilterSpec, column: &str) -> Option<ArrayRef> {
            match spec.kind.as_str() {
                "constant" if spec.column_name == column && spec.op.as_deref() == Some("eq") => {
                    this.value(spec).ok().map(|v| v.slice(0, 1))
                }
                "and" => spec.children.iter().find_map(|c| find(this, c, column)),
                _ => None,
            }
        }
        self.specs.iter().find_map(|s| find(self, s, column))
    }

    /// The `IN (...)` value set for `column` as a typed Arrow array, or `None` if
    /// there is no `IN` filter on it. Mirrors Python `get_column_in_values` and
    /// the Go `GetColumnInValues`. Descends one level into a top-level `AND`.
    pub fn get_column_in_values(&self, column: &str) -> Option<ArrayRef> {
        fn find(this: &PushdownFilters, spec: &FilterSpec, column: &str) -> Option<ArrayRef> {
            match spec.kind.as_str() {
                "in" if spec.column_name == column => this.value(spec).ok().cloned(),
                "join_keys" if spec.column_name == column => this.join_value(spec).cloned(),
                "and" => spec.children.iter().find_map(|c| find(this, c, column)),
                _ => None,
            }
        }
        self.specs.iter().find_map(|s| find(self, s, column))
    }

    /// The numeric `[min, max]` bounds implied by comparison filters on
    /// `column` (from `=`/`<`/`<=`/`>`/`>=`/`IN`), or `None` if the column is not
    /// constrained. Mirrors Python `get_column_bounds` and the Go
    /// `GetColumnBounds`. Bounds are integer-coerced (see [`ColumnBounds`]).
    pub fn get_column_bounds(&self, column: &str) -> Option<ColumnBounds> {
        let (count, lo, hi) = self.column_summary(column);
        if count == 0 && lo.is_none() && hi.is_none() {
            return None;
        }
        Some(ColumnBounds { min: lo, max: hi })
    }

    /// The top-level filters that reference `column`, as read-only [`Filter`]
    /// views. Mirrors Python `get_column_filters` and the Go `GetColumnFilters`.
    pub fn get_column_filters(&self, column: &str) -> Vec<Filter> {
        self.specs
            .iter()
            .filter(|s| spec_mentions(s, column))
            .map(Filter::from_spec)
            .collect()
    }

    /// All top-level filters as read-only [`Filter`] views (the conjunction
    /// DuckDB pushed down). Mirrors Python iterating a `PushdownFilters`.
    pub fn filters(&self) -> Vec<Filter> {
        self.specs.iter().map(Filter::from_spec).collect()
    }

    fn column_values_of(&self, spec: &FilterSpec, column: &str) -> Option<Vec<i64>> {
        match spec.kind.as_str() {
            "in" if spec.column_name == column => {
                let vals = self.value(spec).ok()?;
                let casted = arrow_cast::cast(vals, &arrow_schema::DataType::Int64).ok()?;
                let a = casted.as_primitive::<arrow_array::types::Int64Type>();
                Some(
                    (0..a.len())
                        .filter(|&i| a.is_valid(i))
                        .map(|i| a.value(i))
                        .collect(),
                )
            }
            "join_keys" if spec.column_name == column => {
                let vals = self.join_value(spec)?;
                let casted = arrow_cast::cast(vals, &arrow_schema::DataType::Int64).ok()?;
                let a = casted.as_primitive::<arrow_array::types::Int64Type>();
                Some(
                    (0..a.len())
                        .filter(|&i| a.is_valid(i))
                        .map(|i| a.value(i))
                        .collect(),
                )
            }
            "constant" if spec.column_name == column && spec.op.as_deref() == Some("eq") => {
                let vals = self.value(spec).ok()?;
                let casted = arrow_cast::cast(vals, &arrow_schema::DataType::Int64).ok()?;
                let a = casted.as_primitive::<arrow_array::types::Int64Type>();
                a.is_valid(0).then(|| vec![a.value(0)])
            }
            "and" => {
                // Any AND-child that enumerates the column resolves the set.
                let mut acc: Option<Vec<i64>> = None;
                for c in &spec.children {
                    if let Some(vs) = self.column_values_of(c, column) {
                        acc = Some(match acc {
                            None => vs,
                            Some(prev) => prev.into_iter().filter(|v| vs.contains(v)).collect(),
                        });
                    }
                }
                acc
            }
            "or" => {
                // Union — but only if EVERY branch enumerates the column.
                let mut out = Vec::new();
                for c in &spec.children {
                    let vs = self.column_values_of(c, column)?;
                    out.extend(vs);
                }
                out.sort_unstable();
                out.dedup();
                Some(out)
            }
            _ => None,
        }
    }

    fn eval_spec(&self, spec: &FilterSpec, batch: &RecordBatch) -> Result<BooleanArray> {
        let n = batch.num_rows();
        match spec.kind.as_str() {
            "constant" => {
                let col = self.column(spec, batch)?;
                let val = self.value(spec)?;
                compare(col, val, spec.op.as_deref().unwrap_or("eq"))
            }
            "is_null" => {
                let col = self.column(spec, batch)?;
                arrow_arith::boolean::is_null(col).map_err(cvt)
            }
            "is_not_null" => {
                let col = self.column(spec, batch)?;
                arrow_arith::boolean::is_not_null(col).map_err(cvt)
            }
            "in" => {
                let col = self.column(spec, batch)?;
                let vals = self.value(spec)?;
                in_list(col, vals)
            }
            "join_keys" => {
                let col = self.column(spec, batch)?;
                match self.join_value(spec) {
                    // No join-keys batch available — graceful degradation:
                    // pass every row through and let DuckDB filter client-side.
                    None => Ok(all_true(n)),
                    Some(vals) => in_list(col, vals),
                }
            }
            "and" => {
                let mut acc = all_true(n);
                for c in &spec.children {
                    let m = self.eval_spec(c, batch)?;
                    acc = and_kleene(&acc, &m)?;
                }
                Ok(acc)
            }
            "or" => {
                let mut acc = all_false(n);
                for c in &spec.children {
                    let m = self.eval_spec(c, batch)?;
                    acc = or_kleene(&acc, &m)?;
                }
                Ok(acc)
            }
            "struct" => {
                // Evaluate the child filter against the named struct field.
                let col = self.column(spec, batch)?;
                let sa = col
                    .as_any()
                    .downcast_ref::<arrow_array::StructArray>()
                    .ok_or_else(|| RpcError::value_error("struct filter on non-struct column"))?;
                let child = spec
                    .child_filter
                    .as_ref()
                    .ok_or_else(|| RpcError::value_error("struct filter missing child"))?;
                // The targeted struct field is named by the *struct* spec's
                // `child_name`/`child_index`, not by the child filter (whose
                // column ref still points at the outer struct column).
                let field = sa
                    .column_by_name(&spec.child_name)
                    .or_else(|| sa.columns().get(spec.child_index as usize))
                    .ok_or_else(|| RpcError::value_error("struct child field not found"))?
                    .clone();
                let sub = RecordBatch::try_new(
                    Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                        &spec.child_name,
                        field.data_type().clone(),
                        true,
                    )])),
                    vec![field],
                )
                .map_err(cvt)?;
                // Rewrite the child to target column 0 of the sub-batch.
                let mut child2 = clone_spec(child);
                child2.column_index = 0;
                self.eval_spec(&child2, &sub)
            }
            other => Err(RpcError::value_error(format!(
                "unsupported filter type {other}"
            ))),
        }
    }
}

/// Collect every column name referenced by a filter spec tree.
fn collect_columns(spec: &FilterSpec, out: &mut std::collections::HashSet<String>) {
    if !spec.column_name.is_empty() {
        out.insert(spec.column_name.clone());
    }
    for c in &spec.children {
        collect_columns(c, out);
    }
    if let Some(child) = &spec.child_filter {
        collect_columns(child, out);
    }
}

/// Whether a filter spec tree references `column`.
fn spec_mentions(spec: &FilterSpec, column: &str) -> bool {
    if spec.column_name == column {
        return true;
    }
    if spec.children.iter().any(|c| spec_mentions(c, column)) {
        return true;
    }
    spec.child_filter
        .as_ref()
        .map(|c| spec_mentions(c, column))
        .unwrap_or(false)
}

fn clone_spec(s: &FilterSpec) -> FilterSpec {
    FilterSpec {
        kind: s.kind.clone(),
        column_name: s.column_name.clone(),
        column_index: s.column_index,
        op: s.op.clone(),
        value_ref: s.value_ref,
        keys_column: s.keys_column.clone(),
        children: s.children.iter().map(clone_spec).collect(),
        child_filter: s.child_filter.as_ref().map(|c| Box::new(clone_spec(c))),
        child_index: s.child_index,
        child_name: s.child_name.clone(),
    }
}

fn compare(col: &ArrayRef, val: &ArrayRef, op: &str) -> Result<BooleanArray> {
    let scalar = Scalar::new(val.slice(0, 1));
    let r = match op {
        "eq" => arrow_ord::cmp::eq(col, &scalar),
        "ne" => arrow_ord::cmp::neq(col, &scalar),
        "lt" => arrow_ord::cmp::lt(col, &scalar),
        "le" => arrow_ord::cmp::lt_eq(col, &scalar),
        "gt" => arrow_ord::cmp::gt(col, &scalar),
        "ge" => arrow_ord::cmp::gt_eq(col, &scalar),
        other => return Err(RpcError::value_error(format!("unsupported op {other}"))),
    };
    r.map_err(cvt)
}

fn in_list(col: &ArrayRef, vals: &ArrayRef) -> Result<BooleanArray> {
    let mut acc = all_false(col.len());
    for i in 0..vals.len() {
        let scalar = Scalar::new(vals.slice(i, 1));
        let eq = arrow_ord::cmp::eq(col, &scalar).map_err(cvt)?;
        acc = or_kleene(&acc, &eq)?;
    }
    Ok(acc)
}

fn and_kleene(a: &BooleanArray, b: &BooleanArray) -> Result<BooleanArray> {
    arrow_arith::boolean::and_kleene(a, b).map_err(cvt)
}
fn or_kleene(a: &BooleanArray, b: &BooleanArray) -> Result<BooleanArray> {
    arrow_arith::boolean::or_kleene(a, b).map_err(cvt)
}
fn all_true(n: usize) -> BooleanArray {
    BooleanArray::new(BooleanBuffer::new_set(n), None)
}
fn all_false(n: usize) -> BooleanArray {
    BooleanArray::new(BooleanBuffer::new_unset(n), None)
}
fn cvt(e: arrow_schema::ArrowError) -> RpcError {
    RpcError::runtime_error(format!("filter: {e}"))
}

/// Standard base64 decode (ignores whitespace/padding). `None` on invalid char.
fn b64_decode(s: &str) -> Option<Vec<u8>> {
    let val = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => (c - b'A') as u32,
            b'a'..=b'z' => (c - b'a' + 26) as u32,
            b'0'..=b'9' => (c - b'0' + 52) as u32,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        })
    };
    let clean: Vec<u8> = s
        .bytes()
        .filter(|c| !c.is_ascii_whitespace() && *c != b'=')
        .collect();
    let mut out = Vec::with_capacity(clean.len() * 3 / 4);
    for chunk in clean.chunks(4) {
        if chunk.len() < 2 {
            break;
        }
        let mut n = 0u32;
        for &c in chunk {
            n = (n << 6) | val(c)?;
        }
        n <<= 6 * (4 - chunk.len()) as u32;
        for i in 0..chunk.len() - 1 {
            out.push((n >> (16 - 8 * i)) as u8);
        }
    }
    Some(out)
}

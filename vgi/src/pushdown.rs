//! Filter pushdown: deserialize the `pushdown_filters` blob, evaluate it
//! against a batch, and apply it (port of Go `filter_pushdown.go`).
//!
//! Wire format: an IPC batch whose column 0 is a JSON array of filter specs
//! (field metadata `vgi_filter_version`), and columns 1.. carry the constant
//! values (`value_ref N` → column N+1, scalar at row 0). Top-level filters
//! combine with AND.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, Scalar, StringArray};
use arrow_buffer::BooleanBuffer;
use serde::Deserialize;
use vgi_rpc::{Result, RpcError};

use crate::ipc;

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
    #[serde(default)]
    children: Vec<FilterSpec>,
    #[serde(default)]
    child_filter: Option<Box<FilterSpec>>,
}

/// A parsed, evaluable set of pushdown filters.
pub struct PushdownFilters {
    specs: Vec<FilterSpec>,
    values: Vec<ArrayRef>, // value_ref N → values[N]
}

impl PushdownFilters {
    /// Parse the `pushdown_filters` IPC blob.
    pub fn parse(bytes: &[u8]) -> Result<PushdownFilters> {
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
        let specs: Vec<FilterSpec> = serde_json::from_str(json.value(0))
            .map_err(|e| RpcError::value_error(format!("parsing filter JSON: {e}")))?;
        // value_ref N resolves to column N+1.
        let values: Vec<ArrayRef> = (1..batch.num_columns())
            .map(|i| batch.column(i).clone())
            .collect();
        Ok(PushdownFilters { specs, values })
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
        batch
            .columns()
            .get(idx)
            .ok_or_else(|| RpcError::value_error(format!("filter column {} not found", spec.column_name)))
    }

    fn value(&self, spec: &FilterSpec) -> Result<&ArrayRef> {
        let r = spec
            .value_ref
            .ok_or_else(|| RpcError::value_error("filter missing value_ref"))?;
        self.values
            .get(r)
            .ok_or_else(|| RpcError::value_error(format!("value_ref {r} out of range")))
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
                let field = sa
                    .column_by_name(&child.column_name)
                    .or_else(|| sa.columns().get(child.column_index as usize))
                    .ok_or_else(|| RpcError::value_error("struct child field not found"))?
                    .clone();
                let sub = RecordBatch::try_new(
                    Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                        &child.column_name,
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
            other => Err(RpcError::value_error(format!("unsupported filter type {other}"))),
        }
    }
}

fn clone_spec(s: &FilterSpec) -> FilterSpec {
    FilterSpec {
        kind: s.kind.clone(),
        column_name: s.column_name.clone(),
        column_index: s.column_index,
        op: s.op.clone(),
        value_ref: s.value_ref,
        children: s.children.iter().map(clone_spec).collect(),
        child_filter: s.child_filter.as_ref().map(|c| Box::new(clone_spec(c))),
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

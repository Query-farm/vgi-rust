// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Parsing of the `arguments` wire blob.
//!
//! DuckDB wraps a call's arguments in a single `args` struct column whose
//! fields are `positional_0`, `positional_1`, … and `named_<name>`. Const
//! (bind-time) arguments carry their value at row 0; column arguments carry a
//! null placeholder (the real column data arrives in the process input batch).
//! The field *types* always describe the argument types.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_schema::{Field, Schema, SchemaRef};
use vgi_rpc::{Result, RpcError};

use crate::ipc;

/// Parsed function arguments.
#[derive(Clone, Default)]
pub struct Arguments {
    /// Positional argument arrays (1 row each), indexed by position.
    pub positional: Vec<Option<ArrayRef>>,
    /// Named argument arrays (1 row each).
    pub named: HashMap<String, ArrayRef>,
    /// The schema of the positional args (field types), for bind-time use.
    pub schema: Option<SchemaRef>,
    /// The original positional fields (with metadata, e.g. `ARROW:extension:name`),
    /// indexed in lockstep with `positional`.
    pub positional_fields: Vec<Option<Field>>,
}

impl Arguments {
    /// Parse the IPC-serialized `arguments` blob. Empty input → no arguments.
    pub fn parse(bytes: &[u8]) -> Result<Arguments> {
        if bytes.is_empty() {
            return Ok(Arguments::default());
        }
        let batch = ipc::read_batch(bytes)?;
        let mut args = Arguments::default();

        // DuckDB form: single "args" struct column.
        if batch.num_columns() == 1 && batch.schema().field(0).name() == "args" {
            let sa = batch
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| RpcError::type_error("'args' column is not a struct"))?;
            let mut pos: Vec<(usize, ArrayRef)> = Vec::new();
            let mut sfields: Vec<(usize, Field)> = Vec::new();
            for (i, field) in sa.fields().iter().enumerate() {
                let col = sa.column(i).clone();
                let name = field.name();
                if let Some(idx) = name.strip_prefix("positional_") {
                    if let Ok(idx) = idx.parse::<usize>() {
                        pos.push((idx, col.clone()));
                        sfields.push((idx, field.as_ref().clone()));
                    }
                    args.named.insert(name.clone(), col);
                } else if let Some(actual) = name.strip_prefix("named_") {
                    args.named.insert(actual.to_string(), col.clone());
                    args.named.insert(name.clone(), col);
                } else {
                    args.named.insert(name.clone(), col);
                }
            }
            if !pos.is_empty() {
                let max_idx = pos.iter().map(|(i, _)| *i).max().unwrap();
                args.positional = vec![None; max_idx + 1];
                args.positional_fields = vec![None; max_idx + 1];
                for (i, a) in pos {
                    args.positional[i] = Some(a);
                }
                for (i, f) in &sfields {
                    args.positional_fields[*i] = Some(f.clone());
                }
                sfields.sort_by_key(|(i, _)| *i);
                let fields: Vec<Field> = sfields.into_iter().map(|(_, f)| f).collect();
                args.schema = Some(Arc::new(Schema::new(fields)));
            }
            return Ok(args);
        }

        // Fallback: direct column mapping.
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(i).clone();
            args.named.insert(field.name().clone(), col.clone());
            args.positional.push(Some(col));
        }
        args.schema = Some(batch.schema());
        Ok(args)
    }

    /// Serialize positional const arguments into the wire `arguments` blob
    /// (an IPC batch with a single `args` struct column whose fields are
    /// `positional_0`, `positional_1`, …). Inverse of [`Arguments::parse`].
    pub fn serialize_positional(values: &[ArrayRef]) -> Result<Vec<u8>> {
        use arrow_array::RecordBatch;
        if values.is_empty() {
            // Schema-only args batch (no positional arguments).
            let schema = Arc::new(Schema::new(vec![Field::new(
                "args",
                arrow_schema::DataType::Struct(arrow_schema::Fields::empty()),
                false,
            )]));
            return crate::ipc::write_schema(&schema);
        }
        let pairs: Vec<(Arc<Field>, ArrayRef)> = values
            .iter()
            .enumerate()
            .map(|(i, a)| {
                (
                    Arc::new(Field::new(
                        format!("positional_{i}"),
                        a.data_type().clone(),
                        true,
                    )),
                    a.clone(),
                )
            })
            .collect();
        let sa = StructArray::from(pairs);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "args",
            sa.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(sa)])
            .map_err(|e| RpcError::runtime_error(format!("serialize args: {e}")))?;
        crate::ipc::write_batch(&batch)
    }

    /// Serialize positional scan arguments in the catalog `ScanFunctionResult`
    /// format: a flat IPC batch with columns `arg_0`, `arg_1`, … (NOT the
    /// `args` struct used for direct calls).
    pub fn serialize_scan_args(values: &[ArrayRef]) -> Result<Vec<u8>> {
        Self::serialize_scan_args_named(values, &[])
    }

    /// Serialize positional (`arg_<i>`) plus named (bare-name) scan arguments
    /// into the flat `ScanFunctionResult.arguments` batch.
    pub fn serialize_scan_args_named(
        positional: &[ArrayRef],
        named: &[(&str, ArrayRef)],
    ) -> Result<Vec<u8>> {
        use arrow_array::RecordBatch;
        if positional.is_empty() && named.is_empty() {
            return crate::ipc::write_schema(&Schema::empty());
        }
        let mut fields: Vec<Field> = positional
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("arg_{i}"), a.data_type().clone(), false))
            .collect();
        let mut cols: Vec<ArrayRef> = positional.to_vec();
        for (name, a) in named {
            fields.push(Field::new(*name, a.data_type().clone(), false));
            cols.push(a.clone());
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
            .map_err(|e| RpcError::runtime_error(format!("serialize scan args: {e}")))?;
        crate::ipc::write_batch(&batch)
    }

    /// The 1-row array of the positional argument at `pos`, if present.
    pub fn arg(&self, pos: usize) -> Option<&ArrayRef> {
        self.positional.get(pos).and_then(|o| o.as_ref())
    }

    /// The original field (with metadata, e.g. extension names) of the
    /// positional argument at `pos`, if present.
    pub fn arg_field(&self, pos: usize) -> Option<&Field> {
        self.positional_fields.get(pos).and_then(|o| o.as_ref())
    }

    fn nonnull(&self, pos: usize) -> Option<&ArrayRef> {
        let a = self.arg(pos)?;
        if a.is_empty() || a.is_null(0) {
            None
        } else {
            Some(a)
        }
    }

    /// Read a const int (any integer type widened to i64).
    pub fn const_i64(&self, pos: usize) -> Option<i64> {
        let a = self.nonnull(pos)?;
        crate::numeric::array_value_i64(a, 0)
    }

    /// Read a const float (any float/int widened to f64).
    pub fn const_f64(&self, pos: usize) -> Option<f64> {
        let a = self.nonnull(pos)?;
        crate::numeric::array_value_f64(a, 0)
    }

    /// Read a const string.
    pub fn const_str(&self, pos: usize) -> Option<String> {
        let a = self.nonnull(pos)?;
        if let Some(s) = a.as_string_opt::<i32>() {
            return Some(s.value(0).to_string());
        }
        if let Some(s) = a.as_string_opt::<i64>() {
            return Some(s.value(0).to_string());
        }
        None
    }

    /// Read a const bool.
    pub fn const_bool(&self, pos: usize) -> Option<bool> {
        let a = self.nonnull(pos)?;
        if let Some(b) = a.as_boolean_opt() {
            return Some(b.value(0));
        }
        // `arrow_lossless_conversion` encodes BOOLEAN as Int8.
        crate::numeric::array_value_i64(a, 0).map(|v| v != 0)
    }

    /// Read const binary bytes.
    pub fn const_bytes(&self, pos: usize) -> Option<Vec<u8>> {
        let a = self.nonnull(pos)?;
        if let Some(b) = a.as_binary_opt::<i32>() {
            return Some(b.value(0).to_vec());
        }
        if let Some(b) = a.as_binary_opt::<i64>() {
            return Some(b.value(0).to_vec());
        }
        None
    }

    /// Number of positional arguments.
    pub fn num_positional(&self) -> usize {
        self.positional.len()
    }

    fn named_nonnull(&self, name: &str) -> Option<&ArrayRef> {
        let a = self.named.get(name)?;
        if a.is_empty() || a.is_null(0) {
            None
        } else {
            Some(a)
        }
    }

    /// The 1-row array of a named argument (non-null), if present. Lets callers
    /// read types without a dedicated accessor (e.g. INTERVAL month_day_nano).
    pub fn named(&self, name: &str) -> Option<&ArrayRef> {
        self.named_nonnull(name)
    }

    /// Read a named const int argument.
    pub fn named_i64(&self, name: &str) -> Option<i64> {
        crate::numeric::array_value_i64(self.named_nonnull(name)?, 0)
    }

    /// Read a named const float argument.
    pub fn named_f64(&self, name: &str) -> Option<f64> {
        crate::numeric::array_value_f64(self.named_nonnull(name)?, 0)
    }

    /// Read a named const string argument.
    pub fn named_str(&self, name: &str) -> Option<String> {
        let a = self.named_nonnull(name)?;
        if let Some(s) = a.as_string_opt::<i32>() {
            return Some(s.value(0).to_string());
        }
        a.as_string_opt::<i64>().map(|s| s.value(0).to_string())
    }

    /// Read a named const bool argument.
    pub fn named_bool(&self, name: &str) -> Option<bool> {
        let a = self.named_nonnull(name)?;
        if let Some(b) = a.as_boolean_opt() {
            return Some(b.value(0));
        }
        // `arrow_lossless_conversion` encodes BOOLEAN as Int8 — accept any
        // integer encoding (nonzero = true).
        crate::numeric::array_value_i64(a, 0).map(|v| v != 0)
    }

    /// Expand compacted const-only positional args to their declared positions
    /// DuckDB sends only the const values,
    /// indexed in send order; this maps them back onto the declared positions.
    pub fn remap_positional(&mut self, specs: &[crate::function::ArgSpec]) {
        if self.positional.is_empty() || specs.is_empty() {
            return;
        }
        let mut const_positions = Vec::new();
        let mut max_pos = 0i32;
        for s in specs {
            if s.position >= 0 && s.is_const {
                const_positions.push(s.position as usize);
            }
            if s.position > max_pos {
                max_pos = s.position;
            }
        }
        if const_positions.len() == specs.len() {
            return;
        }
        if self.positional.len() as i32 > max_pos {
            return;
        }
        let mut expanded: Vec<Option<ArrayRef>> = vec![None; max_pos as usize + 1];
        for (i, &orig) in const_positions.iter().enumerate() {
            if let Some(a) = self.positional.get(i).and_then(|o| o.clone()) {
                expanded[orig] = Some(a);
            }
        }
        self.positional = expanded;
    }
}

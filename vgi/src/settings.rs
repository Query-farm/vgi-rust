//! DuckDB session settings passed to functions.
//!
//! The `settings` wire blob is an IPC batch with one column per setting (name
//! = column name, value at row 0). Struct-typed settings arrive as a struct
//! column. Parsing mirrors Python's `_deserialize_settings`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use vgi_rpc::Result;

use crate::ipc;

/// Parsed settings: name → 1-row Arrow array (preserves type for struct/typed
/// access).
#[derive(Clone, Default)]
pub struct Settings {
    pub values: HashMap<String, ArrayRef>,
}

impl Settings {
    /// Parse the IPC settings blob (empty → no settings).
    pub fn parse(bytes: &[u8]) -> Result<Settings> {
        if bytes.is_empty() {
            return Ok(Settings::default());
        }
        let batch = ipc::read_batch(bytes)?;
        let mut values = HashMap::new();
        for (i, field) in batch.schema().fields().iter().enumerate() {
            values.insert(field.name().clone(), batch.column(i).clone());
        }
        Ok(Settings { values })
    }

    pub fn get(&self, name: &str) -> Option<&ArrayRef> {
        self.values.get(name)
    }

    pub fn get_i64(&self, name: &str) -> Option<i64> {
        let a = self.nonnull(name)?;
        crate::numeric::array_value_i64(a, 0)
    }

    pub fn get_f64(&self, name: &str) -> Option<f64> {
        let a = self.nonnull(name)?;
        crate::numeric::array_value_f64(a, 0)
    }

    pub fn get_bool(&self, name: &str) -> Option<bool> {
        let a = self.nonnull(name)?;
        a.as_boolean_opt().map(|b| b.value(0))
    }

    pub fn get_str(&self, name: &str) -> Option<String> {
        let a = self.nonnull(name)?;
        if let Some(s) = a.as_string_opt::<i32>() {
            return Some(s.value(0).to_string());
        }
        if let Some(s) = a.as_string_opt::<i64>() {
            return Some(s.value(0).to_string());
        }
        None
    }

    fn nonnull(&self, name: &str) -> Option<&ArrayRef> {
        let a = self.values.get(name)?;
        if a.is_empty() || a.is_null(0) {
            None
        } else {
            Some(a)
        }
    }
}

/// Wrap a `Schema` in an `Arc` (re-export convenience).
pub fn arc(schema: arrow_schema::Schema) -> Arc<arrow_schema::Schema> {
    Arc::new(schema)
}

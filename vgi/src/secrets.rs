//! Secret access for functions.
//!
//! VGI uses a two-phase bind: the first `bind` returns secret lookup requests
//! (`lookup_secret_types`/`scopes`/`names`); the extension resolves them and
//! re-binds with `resolved_secrets_provided=true` and a `secrets` blob (an IPC
//! batch keyed by secret name → struct of fields). This module parses that
//! blob and exposes field access.

use std::collections::HashMap;

use arrow_array::cast::AsArray;
use arrow_array::{Array, StructArray};
use vgi_rpc::Result;

use crate::ipc;

/// A request for a secret to be resolved at bind time.
#[derive(Clone, Debug)]
pub struct SecretLookup {
    pub secret_type: String,
    pub scope: Option<String>,
    pub name: Option<String>,
}

/// Parsed resolved secrets: secret name → field map (string-rendered values).
#[derive(Clone, Default)]
pub struct Secrets {
    /// name → { field → value-as-string }
    pub by_name: HashMap<String, HashMap<String, String>>,
}

impl Secrets {
    /// Parse the IPC secrets blob. The shape mirrors Python: one column per
    /// secret name, each a struct of the secret's fields.
    pub fn parse(bytes: &[u8]) -> Result<Secrets> {
        if bytes.is_empty() {
            return Ok(Secrets::default());
        }
        let batch = ipc::read_batch(bytes)?;
        let mut by_name = HashMap::new();
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let col = batch.column(i);
            let mut fields = HashMap::new();
            if let Some(sa) = col.as_any().downcast_ref::<StructArray>() {
                for (j, sf) in sa.fields().iter().enumerate() {
                    fields.insert(sf.name().clone(), render(sa.column(j), 0));
                }
            } else {
                fields.insert(field.name().clone(), render(col, 0));
            }
            by_name.insert(field.name().clone(), fields);
        }
        Ok(Secrets { by_name })
    }

    /// Get a secret field value (first matching secret of any name).
    pub fn field(&self, field: &str) -> Option<String> {
        self.by_name.values().find_map(|m| m.get(field).cloned())
    }

    /// Get a named secret's field.
    pub fn named_field(&self, name: &str, field: &str) -> Option<String> {
        self.by_name.get(name).and_then(|m| m.get(field).cloned())
    }
}

/// Render a single array element as a string (best-effort).
fn render(arr: &dyn Array, i: usize) -> String {
    if arr.is_null(i) {
        return String::new();
    }
    if let Some(s) = arr.as_string_opt::<i32>() {
        return s.value(i).to_string();
    }
    if let Some(s) = arr.as_string_opt::<i64>() {
        return s.value(i).to_string();
    }
    if let Some(b) = arr.as_boolean_opt() {
        return b.value(i).to_string();
    }
    if let Some(v) = crate::numeric::array_value_i64(&arr_to_ref(arr), i) {
        return v.to_string();
    }
    String::new()
}

fn arr_to_ref(arr: &dyn Array) -> arrow_array::ArrayRef {
    arrow_array::make_array(arr.to_data())
}

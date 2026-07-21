// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Settings-aware table fixtures: read DuckDB settings (scalar + struct) and
//! reflect them in the generated rows.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_function::{resume, TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(SettingsAwareFunction);
    w.register_table(StructSettingsFunction);
    w.register_table(SecretDemoFunction);
    w.register_table(ScopedSecretDemoFunction);
    w.register_table(MultiSecretDemoFunction);
}

// ---------------------------------------------------------------------------
// secret_demo() -> {key, value, arrow_type} from the vgi_example secret
// ---------------------------------------------------------------------------

fn arrow_type_of(field: &str) -> &'static str {
    match field {
        "port" => "int32",
        "use_ssl" => "bool",
        "timeout" => "double",
        _ => "string",
    }
}

struct SecretRows {
    schema: SchemaRef,
    rows: Vec<(String, String, String)>,
    emitted: bool,
}
impl TableProducer for SecretRows {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        if self.rows.is_empty() {
            return Ok(None);
        }
        let keys: Vec<&str> = self.rows.iter().map(|r| r.0.as_str()).collect();
        let vals: Vec<&str> = self.rows.iter().map(|r| r.1.as_str()).collect();
        let types: Vec<&str> = self.rows.iter().map(|r| r.2.as_str()).collect();
        Ok(Some(
            RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(StringArray::from(keys)) as ArrayRef,
                    Arc::new(StringArray::from(vals)) as ArrayRef,
                    Arc::new(StringArray::from(types)) as ArrayRef,
                ],
            )
            .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[if self.emitted { 1 } else { 0 }])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.emitted = v[0] != 0;
        }
    }
}

pub struct SecretDemoFunction;
impl TableFunction for SecretDemoFunction {
    fn name(&self) -> &str {
        "secret_demo"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Outputs secret contents as key-value rows")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![]
    }
    fn secret_lookups(&self, _params: &BindParams) -> Vec<vgi::secrets::SecretLookup> {
        vec![vgi::secrets::SecretLookup {
            secret_type: "vgi_example".into(),
            scope: None,
            name: None,
        }]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, true),
                Field::new("arrow_type", DataType::Utf8, true),
            ])),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let mut rows: Vec<(String, String, String)> = params
            .secrets
            .of_type("vgi_example")
            .next()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.clone(), arrow_type_of(k).to_string()))
                    .collect()
            })
            .unwrap_or_default();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Box::new(SecretRows {
            schema: params.output_schema.clone(),
            rows,
            emitted: false,
        }))
    }
}

// ---------------------------------------------------------------------------
// scoped_secret_demo(path) -> {scope, found, secret_keys}
// ---------------------------------------------------------------------------

struct ScopedRow {
    schema: SchemaRef,
    scope: String,
    found: bool,
    keys: String,
    emitted: bool,
}
impl TableProducer for ScopedRow {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(
            RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![self.scope.clone()])) as ArrayRef,
                    Arc::new(arrow_array::BooleanArray::from(vec![self.found])) as ArrayRef,
                    Arc::new(StringArray::from(vec![self.keys.clone()])) as ArrayRef,
                ],
            )
            .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[if self.emitted { 1 } else { 0 }])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.emitted = v[0] != 0;
        }
    }
}

pub struct ScopedSecretDemoFunction;
impl TableFunction for ScopedSecretDemoFunction {
    fn name(&self) -> &str {
        "scoped_secret_demo"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Demo: resolves scoped secret based on argument")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "path",
            0,
            "varchar",
            "Scope path for secret lookup",
        )]
    }
    fn secret_lookups(&self, params: &BindParams) -> Vec<vgi::secrets::SecretLookup> {
        let scope = params.arguments.const_str(0);
        vec![vgi::secrets::SecretLookup {
            secret_type: "vgi_example".into(),
            scope,
            name: None,
        }]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![
                Field::new("scope", DataType::Utf8, true),
                Field::new("found", DataType::Boolean, true),
                Field::new("secret_keys", DataType::Utf8, true),
            ])),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let scope = params.arguments.const_str(0).unwrap_or_default();
        let secret = params.secrets.of_type("vgi_example").next();
        let found = secret.map(|m| !m.is_empty()).unwrap_or(false);
        let mut keys: Vec<String> = secret
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        keys.sort();
        Ok(Box::new(ScopedRow {
            schema: params.output_schema.clone(),
            scope,
            found,
            keys: keys.join(","),
            emitted: false,
        }))
    }
}

fn meta(desc: &str) -> FunctionMetadata {
    FunctionMetadata {
        description: desc.to_string(),
        categories: vec!["generator".into(), "settings".into()],
        required_settings: Vec::new(),
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// multi_secret_demo(path) -> {api_key}
//
// Resolves TWO same-type scoped secrets in one bind, then selects the one
// matching the `path` argument via `Secrets::for_scope_of_type`. Phase 1
// requests the `vgi_example` secret for both `s3://bucket-a/` and
// `s3://bucket-b/`; because resolved secrets are keyed by name both survive,
// and the per-path scope selection picks the right `api_key`.
// ---------------------------------------------------------------------------

struct MultiSecretRow {
    schema: SchemaRef,
    api_key: String,
    emitted: bool,
}
impl TableProducer for MultiSecretRow {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(
            RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(StringArray::from(vec![self.api_key.clone()])) as ArrayRef],
            )
            .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[if self.emitted { 1 } else { 0 }])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.emitted = v[0] != 0;
        }
    }
}

pub struct MultiSecretDemoFunction;
impl TableFunction for MultiSecretDemoFunction {
    fn name(&self) -> &str {
        "multi_secret_demo"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Demo: two same-type scoped secrets resolved in one bind".to_string(),
            stability: Some(vgi::protocol::enums::stability::VOLATILE.to_string()),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg(
            "path",
            0,
            "varchar",
            "Path for scoped secret lookup",
        )]
    }
    fn secret_lookups(&self, _params: &BindParams) -> Vec<vgi::secrets::SecretLookup> {
        // Phase 1: request the vgi_example secret for two distinct scopes.
        vec![
            vgi::secrets::SecretLookup {
                secret_type: "vgi_example".into(),
                scope: Some("s3://bucket-a/".into()),
                name: None,
            },
            vgi::secrets::SecretLookup {
                secret_type: "vgi_example".into(),
                scope: Some("s3://bucket-b/".into()),
                name: None,
            },
        ]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new(
                "api_key",
                DataType::Utf8,
                true,
            )])),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let path = params.arguments.const_str(0).unwrap_or_default();
        let api_key = params
            .secrets
            .for_scope_of_type(&path, "vgi_example")
            .and_then(|m| m.get("api_key").cloned())
            .unwrap_or_default();
        Ok(Box::new(MultiSecretRow {
            schema: params.output_schema.clone(),
            api_key,
            emitted: false,
        }))
    }
}

// ---------------------------------------------------------------------------
// settings_aware(count) -> {id, greeting, value} (+ details when verbose)
// ---------------------------------------------------------------------------

fn verbose(params: &BindParams) -> bool {
    params
        .settings
        .get_bool("vgi_verbose_mode")
        .unwrap_or(false)
}
fn settings_aware_schema(verbose: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Int64, true),
        Field::new("greeting", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ];
    if verbose {
        fields.push(Field::new("details", DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

struct SettingsAwareProducer {
    schema: SchemaRef,
    count: i64,
    greeting: String,
    multiplier: i64,
    verbose: bool,
    emitted: bool,
}
impl TableProducer for SettingsAwareProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let ids: Vec<i64> = (0..self.count).collect();
        let greetings: Vec<&str> = vec![self.greeting.as_str(); self.count as usize];
        let values: Vec<f64> = ids
            .iter()
            .map(|i| *i as f64 * 2.5 * self.multiplier as f64)
            .collect();
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ids.clone())),
            Arc::new(StringArray::from(greetings)),
            Arc::new(Float64Array::from(values)),
        ];
        if self.verbose {
            let details: Vec<String> = ids.iter().map(|i| format!("row_{i}")).collect();
            cols.push(Arc::new(StringArray::from(details)));
        }
        Ok(Some(
            RecordBatch::try_new(self.schema.clone(), cols)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[if self.emitted { 1 } else { 0 }])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.emitted = v[0] != 0;
        }
    }
}

pub struct SettingsAwareFunction;
impl TableFunction for SettingsAwareFunction {
    fn name(&self) -> &str {
        "settings_aware"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Generates data demonstrating settings are passed")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "Number of rows")]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: settings_aware_schema(verbose(params)),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let v = params
            .settings
            .get_bool("vgi_verbose_mode")
            .unwrap_or(false);
        Ok(Box::new(SettingsAwareProducer {
            schema: settings_aware_schema(v),
            count: params.arguments.const_i64(0).unwrap_or(0).max(0),
            greeting: params
                .settings
                .get_str("greeting")
                .unwrap_or_else(|| "Hello".to_string()),
            multiplier: params.settings.get_i64("multiplier").unwrap_or(1),
            verbose: v,
            emitted: false,
        }))
    }
}

// ---------------------------------------------------------------------------
// struct_settings(count) -> {n, label} from the `config` struct setting
// ---------------------------------------------------------------------------

fn struct_settings_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]))
}

/// Read the `config` struct setting `{start, step, label}`.
fn read_config(params: &ProcessParams) -> (i64, i64, String) {
    let mut start = 0i64;
    let mut step = 1i64;
    let mut label = "item".to_string();
    if let Some(arr) = params.settings.get("config") {
        if let Some(sa) = arr.as_any().downcast_ref::<arrow_array::StructArray>() {
            if let Some(Ok(c)) = sa
                .column_by_name("start")
                .map(|c| arrow_cast::cast(c, &DataType::Int64))
            {
                start = c.as_primitive::<Int64Type>().value(0);
            }
            if let Some(Ok(c)) = sa
                .column_by_name("step")
                .map(|c| arrow_cast::cast(c, &DataType::Int64))
            {
                step = c.as_primitive::<Int64Type>().value(0);
            }
            if let Some(c) = sa.column_by_name("label") {
                if let Some(s) = c.as_string_opt::<i32>() {
                    label = s.value(0).to_string();
                }
            }
        }
    }
    (start, step, label)
}

struct StructSettingsProducer {
    schema: SchemaRef,
    count: i64,
    start: i64,
    step: i64,
    label: String,
    emitted: bool,
}
impl TableProducer for StructSettingsProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        let ns: Vec<i64> = (0..self.count)
            .map(|i| self.start + i * self.step)
            .collect();
        let labels: Vec<String> = (0..self.count)
            .map(|i| format!("{}_{i}", self.label))
            .collect();
        Ok(Some(
            RecordBatch::try_new(
                self.schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ns)) as ArrayRef,
                    Arc::new(StringArray::from(labels)) as ArrayRef,
                ],
            )
            .map_err(|e| RpcError::runtime_error(e.to_string()))?,
        ))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[if self.emitted { 1 } else { 0 }])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.emitted = v[0] != 0;
        }
    }
}

pub struct StructSettingsFunction;
impl TableFunction for StructSettingsFunction {
    fn name(&self) -> &str {
        "struct_settings"
    }
    fn metadata(&self) -> FunctionMetadata {
        meta("Generate a sequence configured by a struct setting")
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![ArgSpec::const_arg("count", 0, "int64", "Number of rows")]
    }
    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: struct_settings_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let (start, step, label) = read_config(params);
        Ok(Box::new(StructSettingsProducer {
            schema: struct_settings_schema(),
            count: params.arguments.const_i64(0).unwrap_or(0).max(0),
            start,
            step,
            label,
            emitted: false,
        }))
    }
}

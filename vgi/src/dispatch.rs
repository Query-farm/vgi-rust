// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The VGI dispatcher: owns the function registries + catalog identity and
//! implements every RPC handler. Mirrors Go's `Worker` dispatch (handleBind,
//! handleInit, registerCatalogMethods).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::SchemaRef;
use vgi_rpc::{
    Bytes, CallContext, ExchangeState, OutputCollector, Request, Result, RpcError, StreamResult,
    VgiArrow,
};

use crate::aggregate::{AggregateBindParams, AggregateFunction, GROUP_COLUMN_NAME};
use crate::buffering::{BufferingParams, BufferingStore, TableBufferingFunction};
use crate::catalog;
use crate::function::{BindParams, ProcessParams, ScalarFunction};
use crate::ipc;
use crate::protocol::dtos::*;
use crate::table_function::{TableFunction, TableProducer};
use crate::table_in_out::TableInOutFunction;
use crate::wire;

/// The `projection_repro` reproducer app — a distinct catalog served by the
/// example binary, selected by ATTACH name. Its functions (named with
/// [`PROJ_REPRO_PREFIX`]) are advertised only for this catalog.
const PROJ_REPRO_APP: &str = "projection_repro";
const PROJ_REPRO_PREFIX: &str = "proj_repro";

/// Shared dispatch state. Cloned (as `Arc`) into every RPC handler closure.
pub struct Dispatcher {
    /// Catalog name → also the attach opaque-data plaintext.
    pub catalog_name: String,
    /// Scalar function registry: name → overloads.
    pub scalars: HashMap<String, Vec<Arc<dyn ScalarFunction>>>,
    /// Table (producer) function registry.
    pub tables: HashMap<String, Vec<Arc<dyn TableFunction>>>,
    /// Table-in-out function registry.
    pub tableinouts: HashMap<String, Vec<Arc<dyn TableInOutFunction>>>,
    /// Table-buffering function registry.
    pub buffering: HashMap<String, Vec<Arc<dyn TableBufferingFunction>>>,
    /// Aggregate function registry.
    pub aggregates: HashMap<String, Vec<Arc<dyn AggregateFunction>>>,
    /// Shared cross-process state store (buffering + aggregate).
    pub store: Arc<BufferingStore>,
    /// Declarative catalog (views / macros / function-backed tables).
    pub catalog: catalog::CatalogModel,
    /// Secret types registered by the worker (surfaced in `catalog_attach`).
    pub secret_types: Vec<catalog::SecretTypeSpec>,
    /// Custom settings registered by the worker.
    pub settings: Vec<catalog::SettingSpec>,
    exec_counter: AtomicU64,
}

impl Dispatcher {
    pub fn new(catalog_name: impl Into<String>) -> Self {
        Dispatcher {
            catalog_name: catalog_name.into(),
            scalars: HashMap::new(),
            tables: HashMap::new(),
            tableinouts: HashMap::new(),
            buffering: HashMap::new(),
            aggregates: HashMap::new(),
            store: Arc::new(BufferingStore::new()),
            catalog: catalog::CatalogModel::default(),
            secret_types: Vec::new(),
            settings: Vec::new(),
            exec_counter: AtomicU64::new(1),
        }
    }

    pub fn set_catalog(&mut self, model: catalog::CatalogModel) {
        self.catalog = model;
    }

    pub fn register_secret_type(&mut self, spec: catalog::SecretTypeSpec) {
        self.secret_types.push(spec);
    }

    pub fn register_setting(&mut self, spec: catalog::SettingSpec) {
        self.settings.push(spec);
    }

    /// Schema names exposed by the catalog (always includes `main`).
    fn schema_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.catalog.schemas.iter().map(|s| s.name.clone()).collect();
        if !names.iter().any(|n| n == catalog::MAIN_SCHEMA) {
            names.insert(0, catalog::MAIN_SCHEMA.to_string());
        }
        names
    }

    pub fn register_aggregate(&mut self, f: Arc<dyn AggregateFunction>) {
        self.aggregates
            .entry(f.name().to_string())
            .or_default()
            .push(f);
    }

    fn resolve_aggregate(&self, name: &str) -> Result<Arc<dyn AggregateFunction>> {
        self.aggregates
            .get(name)
            .and_then(|v| v.first())
            .cloned()
            .ok_or_else(|| RpcError::value_error(format!("Unknown function: '{name}'")))
    }

    pub fn register_scalar(&mut self, f: Arc<dyn ScalarFunction>) {
        self.scalars.entry(f.name().to_string()).or_default().push(f);
    }

    pub fn register_table(&mut self, f: Arc<dyn TableFunction>) {
        self.tables.entry(f.name().to_string()).or_default().push(f);
    }

    pub fn register_table_in_out(&mut self, f: Arc<dyn TableInOutFunction>) {
        self.tableinouts
            .entry(f.name().to_string())
            .or_default()
            .push(f);
    }

    fn resolve_table_in_out(
        &self,
        name: &str,
        args: &crate::arguments::Arguments,
        input_schema: Option<&SchemaRef>,
    ) -> Result<Arc<dyn TableInOutFunction>> {
        let cands = self
            .tableinouts
            .get(name)
            .ok_or_else(|| RpcError::value_error(format!("Unknown function: '{name}'")))?;
        let idx = crate::overload::resolve_overload(
            cands.len(),
            |i| cands[i].argument_specs(),
            args,
            input_schema,
        )
        .ok_or_else(|| RpcError::value_error(format!("No matching overload for '{name}'")))?;
        Ok(cands[idx].clone())
    }

    pub fn register_buffering(&mut self, f: Arc<dyn TableBufferingFunction>) {
        self.buffering
            .entry(f.name().to_string())
            .or_default()
            .push(f);
    }

    fn resolve_buffering(&self, name: &str) -> Result<Arc<dyn TableBufferingFunction>> {
        self.buffering
            .get(name)
            .and_then(|v| v.first())
            .cloned()
            .ok_or_else(|| RpcError::value_error(format!("Unknown function: '{name}'")))
    }

    fn resolve_table(
        &self,
        name: &str,
        args: &crate::arguments::Arguments,
        input_schema: Option<&SchemaRef>,
    ) -> Result<Arc<dyn TableFunction>> {
        let cands = self
            .tables
            .get(name)
            .ok_or_else(|| RpcError::value_error(format!("Unknown function: '{name}'")))?;
        let idx = crate::overload::resolve_overload(
            cands.len(),
            |i| cands[i].argument_specs(),
            args,
            input_schema,
        )
        .ok_or_else(|| RpcError::value_error(format!("No matching overload for '{name}'")))?;
        Ok(cands[idx].clone())
    }

    /// Mint a globally-unique execution id (process id + time + counter), so
    /// the cross-process buffering store never collides between workers.
    fn next_execution_id(&self) -> Vec<u8> {
        let n = self.exec_counter.fetch_add(1, Ordering::Relaxed);
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let mut v = b"vgi-exec-".to_vec();
        v.extend_from_slice(&std::process::id().to_le_bytes());
        v.extend_from_slice(&t.to_le_bytes());
        v.extend_from_slice(&n.to_le_bytes());
        v
    }

    /// Resolve a scalar function by name with overload scoring.
    fn resolve_scalar(
        &self,
        name: &str,
        args: &crate::arguments::Arguments,
        input_schema: Option<&SchemaRef>,
    ) -> Result<Arc<dyn ScalarFunction>> {
        let cands = self
            .scalars
            .get(name)
            .ok_or_else(|| RpcError::value_error(format!("Unknown function: '{name}'")))?;
        let idx = crate::overload::resolve_overload(
            cands.len(),
            |i| cands[i].argument_specs(),
            args,
            input_schema,
        )
        .ok_or_else(|| RpcError::value_error(format!("No matching overload for '{name}'")))?;
        Ok(cands[idx].clone())
    }

    /// Build `BindParams` from a wire `BindRequest` + call context.
    fn bind_params(&self, dto: &BindRequest, ctx: &CallContext) -> Result<BindParams> {
        Ok(BindParams {
            input_schema: opt_schema(&dto.input_schema)?,
            arguments: crate::arguments::Arguments::parse(&dto.arguments.0)?,
            settings: parse_settings(&dto.settings)?,
            secrets: parse_secrets(&dto.secrets)?,
            resolved_secrets_provided: dto.resolved_secrets_provided,
            auth_principal: principal(ctx),
            attach_opaque_data: dto.attach_opaque_data.clone().map(|b| b.into()),
            transaction_opaque_data: dto.transaction_opaque_data.clone().map(|b| b.into()),
            storage: Some(self.store.clone()),
        })
    }

    // -- bind ---------------------------------------------------------------

    pub fn handle_bind(&self, req: &Request, ctx: &CallContext) -> Result<Option<RecordBatch>> {
        let dto: BindRequest = boxed(req)?;
        let mut params = self.bind_params(&dto, ctx)?;
        let ft = normalize_function_type(&dto.function_type.0).unwrap_or_default();

        // Table buffering.
        if self.buffering.contains_key(&dto.function_name) {
            let f = self.resolve_buffering(&dto.function_name)?;
            params.arguments.remap_positional(&f.argument_specs());
            let bind = f.on_bind(&params)?;
            let resp = BindResponse {
                output_schema: Bytes::from(ipc::write_schema_ref(&bind.output_schema)?),
                opaque_data: Bytes::from(bind.opaque_data),
                lookup_secret_types: Vec::new(),
                lookup_scopes: Vec::new(),
                lookup_names: Vec::new(),
            };
            return Ok(Some(wire::to_result_batch(resp)?));
        }

        // Table-in-out.
        if self.tableinouts.contains_key(&dto.function_name) {
            let f = self.resolve_table_in_out(&dto.function_name, &params.arguments, params.input_schema.as_ref())?;
            params.arguments.remap_positional(&f.argument_specs());
            let bind = f.on_bind(&params)?;
            let resp = BindResponse {
                output_schema: Bytes::from(ipc::write_schema_ref(&bind.output_schema)?),
                opaque_data: Bytes::from(bind.opaque_data),
                lookup_secret_types: Vec::new(),
                lookup_scopes: Vec::new(),
                lookup_names: Vec::new(),
            };
            return Ok(Some(wire::to_result_batch(resp)?));
        }

        // Table (producer) kind.
        if (ft == "table" || ft == "table_buffering")
            || (!self.scalars.contains_key(&dto.function_name)
                && self.tables.contains_key(&dto.function_name))
        {
            let f = self.resolve_table(&dto.function_name, &params.arguments, params.input_schema.as_ref())?;
            params.arguments.remap_positional(&f.argument_specs());
            // Two-phase secret bind: first pass requests the secret types; the
            // C++ resolves them and re-binds with `resolved_secrets_provided`.
            if !params.resolved_secrets_provided {
                let lookups = f.secret_lookups(&params);
                if !lookups.is_empty() {
                    let resp = BindResponse {
                        output_schema: Bytes::from(Vec::new()),
                        opaque_data: Bytes::from(Vec::new()),
                        lookup_secret_types: lookups.iter().map(|l| l.secret_type.clone()).collect(),
                        lookup_scopes: lookups
                            .iter()
                            .map(|l| l.scope.clone().unwrap_or_default())
                            .collect(),
                        lookup_names: lookups
                            .iter()
                            .map(|l| l.name.clone().unwrap_or_default())
                            .collect(),
                    };
                    return Ok(Some(wire::to_result_batch(resp)?));
                }
            }
            let bind = f.on_bind(&params)?;
            let resp = BindResponse {
                output_schema: Bytes::from(ipc::write_schema_ref(&bind.output_schema)?),
                opaque_data: Bytes::from(bind.opaque_data),
                lookup_secret_types: Vec::new(),
                lookup_scopes: Vec::new(),
                lookup_names: Vec::new(),
            };
            return Ok(Some(wire::to_result_batch(resp)?));
        }

        let f = self.resolve_scalar(&dto.function_name, &params.arguments, params.input_schema.as_ref())?;
        params.arguments.remap_positional(&f.argument_specs());

        // Two-phase secret resolution.
        if !params.resolved_secrets_provided {
            let lookups = f.secret_lookups(&params);
            if !lookups.is_empty() {
                let resp = BindResponse {
                    output_schema: Bytes::from(Vec::new()),
                    opaque_data: Bytes::from(Vec::new()),
                    lookup_secret_types: lookups.iter().map(|l| l.secret_type.clone()).collect(),
                    lookup_scopes: lookups
                        .iter()
                        .map(|l| l.scope.clone().unwrap_or_default())
                        .collect(),
                    lookup_names: lookups
                        .iter()
                        .map(|l| l.name.clone().unwrap_or_default())
                        .collect(),
                };
                return Ok(Some(wire::to_result_batch(resp)?));
            }
        }

        crate::function::validate_type_bounds(&f.argument_specs(), params.input_schema.as_ref())?;
        let bind = f.on_bind(&params)?;
        let resp = BindResponse {
            output_schema: Bytes::from(ipc::write_schema_ref(&bind.output_schema)?),
            opaque_data: Bytes::from(bind.opaque_data),
            lookup_secret_types: Vec::new(),
            lookup_scopes: Vec::new(),
            lookup_names: Vec::new(),
        };
        Ok(Some(wire::to_result_batch(resp)?))
    }

    // -- init ---------------------------------------------------------------

    pub fn handle_init(&self, req: &Request, ctx: &CallContext) -> Result<StreamResult> {
        let dto: InitRequest = boxed(req)?;
        // bind_call is an IPC-serialized BindRequest.
        let bind_call: BindRequest = wire::from_batch(&ipc::read_batch(&dto.bind_call.0)?)?;
        let mut bp = self.bind_params(&bind_call, ctx)?;
        // Projection pushdown: the C++ sends the full bind output schema plus
        // projection_ids; narrow the schema the worker emits to those columns.
        let output_schema = crate::table_function::project_schema(
            &ipc::read_schema(&dto.output_schema.0)?,
            &dto.projection_ids,
        );
        let input_schema = bp.input_schema.clone();
        let execution_id = dto
            .execution_id
            .clone()
            .map(|b| b.into())
            .unwrap_or_else(|| self.next_execution_id());
        let ft = normalize_function_type(&bind_call.function_type.0).unwrap_or_default();

        let build_params = |args: crate::arguments::Arguments, settings, secrets, auth| ProcessParams {
            output_schema: output_schema.clone(),
            input_schema: input_schema.clone(),
            execution_id: execution_id.clone(),
            init_opaque_data: dto.bind_opaque_data.clone().map(|b| b.into()).unwrap_or_default(),
            arguments: args,
            settings,
            secrets,
            auth_principal: auth,
            projection_ids: dto.projection_ids.clone(),
            pushdown_filters: dto.pushdown_filters.clone().map(|b| b.0),
            join_keys: dto
                .join_keys
                .clone()
                .map(|v| v.into_iter().map(|b| b.0).collect())
                .unwrap_or_default(),
            storage: Some(self.store.clone()),
            order_by_column: dto.order_by_column_name.clone(),
            order_by_direction: dto.order_by_direction.clone().map(|d| d.0),
            order_by_null_order: dto.order_by_null_order.clone().map(|d| d.0),
            order_by_limit: dto.order_by_limit,
            tablesample_percentage: dto.tablesample_percentage,
            tablesample_seed: dto.tablesample_seed,
            attach_opaque_data: bind_call.attach_opaque_data.clone().map(|b| b.into()),
            at_unit: bind_call.at_unit.clone().filter(|s| !s.is_empty()),
            at_value: bind_call.at_value.clone().filter(|s| !s.is_empty()),
        };

        // Table buffering: sink (header-only) or finalize source (producer).
        if self.buffering.contains_key(&bind_call.function_name) {
            let f = self.resolve_buffering(&bind_call.function_name)?;
            bp.arguments.remap_positional(&f.argument_specs());
            let phase = dto.phase.as_ref().map(|d| d.0.clone()).unwrap_or_default();
            let header = wire::to_batch(GlobalInitResponse {
                execution_id: Bytes::from(execution_id.clone()),
                max_workers: 1,
                opaque_data: None,
            })?;
            if phase == crate::protocol::enums::phase::TABLE_BUFFERING_FINALIZE {
                let fsid = dto.finalize_state_id.clone().map(|b| b.0).unwrap_or_default();
                let bparams = BufferingParams {
                    execution_id,
                    storage: self.store.clone(),
                    output_schema: output_schema.clone(),
                    arguments: bp.arguments,
                    settings: bp.settings,
                    batch_index: None,
                    logs: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
                };
                let auto_apply = f.metadata().auto_apply_filters;
                let filters = if auto_apply {
                    dto.pushdown_filters
                        .as_ref()
                        .map(|b| crate::pushdown::PushdownFilters::parse(&b.0))
                        .transpose()?
                } else {
                    None
                };
                let producer = f.finalize_producer(&bparams, fsid)?;
                let state = TableProducerState { inner: producer, filters, project_to: None, resume_blob: None };
                return Ok(StreamResult::producer(output_schema, Box::new(state))
                    .with_header(header));
            }
            // Sink phase: emit nothing; data arrives via process RPCs.
            // The process/combine RPCs carry no schema and may run in a
            // different pooled worker, so persist the bound output schema
            // (which may differ from the input, e.g. sum_all_columns) to the
            // file-backed store keyed by execution_id for them to read.
            self.store
                .kv_put(&execution_id, b"outsc", &ipc::write_schema_ref(&output_schema)?);
            // Persist named flags the process/combine RPCs need (e.g. `logging`),
            // since those RPCs carry no arguments and may run in another worker.
            self.store.kv_put(
                &execution_id,
                b"bufflags",
                &[bp.arguments.named_bool("logging").unwrap_or(false) as u8],
            );
            let state = TableProducerState {
                inner: Box::new(EmptyProducer),
                filters: None,
                project_to: None,
                resume_blob: None,
            };
            return Ok(StreamResult::producer(output_schema, Box::new(state)).with_header(header));
        }

        // Table-in-out (exchange) path.
        if self.tableinouts.contains_key(&bind_call.function_name) {
            let f = self.resolve_table_in_out(&bind_call.function_name, &bp.arguments, input_schema.as_ref())?;
            bp.arguments.remap_positional(&f.argument_specs());
            let auto_apply = f.metadata().auto_apply_filters;
            let params = build_params(bp.arguments, bp.settings, bp.secrets, bp.auth_principal);
            // FINALIZE phase: flush accumulated state as a producer stream.
            let phase = dto.phase.as_ref().map(|d| d.0.clone()).unwrap_or_default();
            if phase == crate::protocol::enums::phase::FINALIZE {
                let header = wire::to_batch(GlobalInitResponse {
                    execution_id: Bytes::from(execution_id.clone()),
                    max_workers: 1,
                    opaque_data: None,
                })?;
                let batches = f.finish(&params)?;
                let state = TableProducerState {
                    inner: Box::new(VecProducer { batches, pos: 0 }),
                    filters: None,
                    project_to: None,
                    resume_blob: None,
                };
                return Ok(StreamResult::producer(output_schema, Box::new(state)).with_header(header));
            }
            let filters = if auto_apply {
                params
                    .pushdown_filters
                    .as_ref()
                    .map(|b| crate::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys))
                    .transpose()?
            } else {
                None
            };
            let header = wire::to_batch(GlobalInitResponse {
                execution_id: Bytes::from(execution_id.clone()),
                max_workers: 1,
                opaque_data: None,
            })?;
            let blob = self.exchange_blob(
                "table_in_out",
                bind_call.function_name.clone(),
                &output_schema,
                input_schema.as_ref(),
                &bind_call,
                &dto,
                &execution_id,
                auto_apply,
            )?;
            let in_schema = input_schema.unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
            let state = TableInOutExchangeState { func: f, params, filters, blob };
            return Ok(StreamResult::exchange(output_schema, in_schema, Box::new(state))
                .with_header(header));
        }

        // Table (producer) path.
        if (ft == "table" || ft == "table_buffering")
            || (!self.scalars.contains_key(&bind_call.function_name)
                && self.tables.contains_key(&bind_call.function_name))
        {
            let f = self.resolve_table(&bind_call.function_name, &bp.arguments, input_schema.as_ref())?;
            bp.arguments.remap_positional(&f.argument_specs());
            let max_workers = f.max_workers(&bp);
            let auto_apply = f.metadata().auto_apply_filters;
            let params = build_params(bp.arguments, bp.settings, bp.secrets, bp.auth_principal);
            // Primary init (no execution_id on the request) runs the global
            // OnInit hook once — e.g. to push a parallel-scan work queue.
            if dto.execution_id.is_none() {
                f.on_init(&params)?;
            }
            let filters = if auto_apply {
                params
                    .pushdown_filters
                    .as_ref()
                    .map(|b| crate::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys))
                    .transpose()?
            } else {
                None
            };
            // Always narrow each emitted batch to the wire output schema by
            // name: producers may emit their full natural schema (so an
            // auto-applied filter can reference a projected-out column, e.g.
            // `SELECT pushed_filters ... WHERE n != 5`), while DuckDB has
            // pre-narrowed the output via projection pushdown.
            let project_to = Some(output_schema.clone());
            let producer = f.producer(&params)?;
            let resume_blob = if max_workers > 1 {
                Some(self.exchange_blob(
                    "table",
                    bind_call.function_name.clone(),
                    &output_schema,
                    None,
                    &bind_call,
                    &dto,
                    &execution_id,
                    auto_apply,
                )?)
            } else {
                None
            };
            let header = wire::to_batch(GlobalInitResponse {
                execution_id: Bytes::from(execution_id),
                max_workers,
                opaque_data: None,
            })?;
            let state = TableProducerState { inner: producer, filters, project_to, resume_blob };
            return Ok(StreamResult::producer(output_schema, Box::new(state)).with_header(header));
        }

        // Scalar (exchange) path.
        let f = self.resolve_scalar(&bind_call.function_name, &bp.arguments, input_schema.as_ref())?;
        bp.arguments.remap_positional(&f.argument_specs());
        let params = build_params(bp.arguments, bp.settings, bp.secrets, bp.auth_principal);

        let header = wire::to_batch(GlobalInitResponse {
            execution_id: Bytes::from(execution_id.clone()),
            max_workers: 1,
            opaque_data: None,
        })?;

        let blob = self.exchange_blob(
            "scalar",
            bind_call.function_name.clone(),
            &output_schema,
            input_schema.as_ref(),
            &bind_call,
            &dto,
            &execution_id,
            false,
        )?;
        let state = ScalarExchangeState { func: f, params, blob };
        let in_schema = input_schema.unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()));
        Ok(StreamResult::exchange(output_schema, in_schema, Box::new(state)).with_header(header))
    }

    /// Build the encoded HTTP-continuation blob for a stateless exchange
    /// stream (scalar / table-in-out). Carries everything needed to rebuild
    /// the handler from an AEAD state token on any pooled HTTP worker.
    #[allow(clippy::too_many_arguments)]
    fn exchange_blob(
        &self,
        kind: &str,
        function_name: String,
        output_schema: &arrow_schema::SchemaRef,
        input_schema: Option<&arrow_schema::SchemaRef>,
        bind_call: &BindRequest,
        dto: &InitRequest,
        execution_id: &[u8],
        auto_apply: bool,
    ) -> Result<Vec<u8>> {
        let blob = ExchangeBlob {
            kind: kind.to_string(),
            function_name,
            output_schema: ipc::write_schema_ref(output_schema)?,
            input_schema: match input_schema {
                Some(s) => ipc::write_schema_ref(s)?,
                None => Vec::new(),
            },
            arguments: bind_call.arguments.0.clone(),
            settings: bind_call.settings.clone().map(|b| b.0).unwrap_or_default(),
            secrets: bind_call.secrets.clone().map(|b| b.0).unwrap_or_default(),
            execution_id: execution_id.to_vec(),
            init_opaque: dto.bind_opaque_data.clone().map(|b| b.0).unwrap_or_default(),
            pushdown_filters: dto.pushdown_filters.clone().map(|b| b.0).unwrap_or_default(),
            auto_apply,
            inner_resume: Vec::new(),
            at_unit: bind_call.at_unit.clone().unwrap_or_default(),
            at_value: bind_call.at_value.clone().unwrap_or_default(),
        };
        vgi_rpc::stream_codec::bincode_encode(&blob)
    }

    /// Rebuild a stateless exchange stream from its HTTP-continuation blob.
    /// Registered as the `init` method's state decoder so a pooled HTTP
    /// worker can resume a scalar / table-in-out exchange from an AEAD token.
    pub fn decode_init_state(&self, bytes: &[u8]) -> Result<vgi_rpc::stream::StreamStateKind> {
        let blob: ExchangeBlob = vgi_rpc::stream_codec::bincode_decode(bytes)?;
        let output_schema = ipc::read_schema(&blob.output_schema)?;
        let input_schema = if blob.input_schema.is_empty() {
            None
        } else {
            Some(ipc::read_schema(&blob.input_schema)?)
        };
        let settings = if blob.settings.is_empty() {
            crate::settings::Settings::default()
        } else {
            crate::settings::Settings::parse(&blob.settings)?
        };
        let secrets = if blob.secrets.is_empty() {
            crate::secrets::Secrets::default()
        } else {
            crate::secrets::Secrets::parse(&blob.secrets)?
        };
        let pushdown = if blob.pushdown_filters.is_empty() {
            None
        } else {
            Some(blob.pushdown_filters.clone())
        };
        let mut args = crate::arguments::Arguments::parse(&blob.arguments)?;
        let make_params = |args: crate::arguments::Arguments| ProcessParams {
            output_schema: output_schema.clone(),
            input_schema: input_schema.clone(),
            execution_id: blob.execution_id.clone(),
            init_opaque_data: blob.init_opaque.clone(),
            arguments: args,
            settings: settings.clone(),
            secrets: secrets.clone(),
            auth_principal: None,
            projection_ids: None,
            pushdown_filters: pushdown.clone(),
            join_keys: Vec::new(),
            // The file-backed store is process-global; resumed states (e.g. a
            // distributed table-in-out's `process` appending partials, or a
            // work-queue producer) must keep access to it across HTTP
            // continuations, exactly as the init-time params do.
            storage: Some(self.store.clone()),
            order_by_column: None,
            order_by_direction: None,
            order_by_null_order: None,
            order_by_limit: None,
            tablesample_percentage: None,
            tablesample_seed: None,
            attach_opaque_data: None,
            at_unit: Some(blob.at_unit.clone()).filter(|s| !s.is_empty()),
            at_value: Some(blob.at_value.clone()).filter(|s| !s.is_empty()),
        };
        if blob.kind == "table" {
            let f = self.resolve_table(&blob.function_name, &args, input_schema.as_ref())?;
            args.remap_positional(&f.argument_specs());
            let params = make_params(args);
            let filters = if blob.auto_apply {
                params
                    .pushdown_filters
                    .as_ref()
                    .map(|b| crate::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys))
                    .transpose()?
            } else {
                None
            };
            let project_to = Some(output_schema.clone());
            let mut producer = f.producer(&params)?;
            // Restore the partial-chunk cursor so the producer resumes mid-chunk
            // (the chunk was destructively popped from the queue and lives only
            // in the token, not the queue).
            producer.restore_resume(&blob.inner_resume);
            return Ok(vgi_rpc::stream::StreamStateKind::Producer(Box::new(
                TableProducerState { inner: producer, filters, project_to, resume_blob: Some(bytes.to_vec()) },
            )));
        }
        if blob.kind == "table_in_out" {
            let f =
                self.resolve_table_in_out(&blob.function_name, &args, input_schema.as_ref())?;
            args.remap_positional(&f.argument_specs());
            let params = make_params(args);
            let filters = if blob.auto_apply {
                params
                    .pushdown_filters
                    .as_ref()
                    .map(|b| crate::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys))
                    .transpose()?
            } else {
                None
            };
            Ok(vgi_rpc::stream::StreamStateKind::Exchange(Box::new(
                TableInOutExchangeState {
                    func: f,
                    params,
                    filters,
                    blob: bytes.to_vec(),
                },
            )))
        } else {
            let f = self.resolve_scalar(&blob.function_name, &args, input_schema.as_ref())?;
            args.remap_positional(&f.argument_specs());
            let params = make_params(args);
            Ok(vgi_rpc::stream::StreamStateKind::Exchange(Box::new(
                ScalarExchangeState {
                    func: f,
                    params,
                    blob: bytes.to_vec(),
                },
            )))
        }
    }

    // -- catalog ------------------------------------------------------------

    fn attach_bytes(&self) -> Vec<u8> {
        self.catalog_name.as_bytes().to_vec()
    }

    /// `catalog_catalogs` — discovery: advertise this worker's catalog plus
    /// its version metadata so clients can inspect before attaching.
    pub fn handle_catalog_catalogs(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        let items = vec![Bytes::from(catalog::serialize_catalog_info(&self.catalog)?)];
        Ok(Some(wire::to_result_batch(ItemsResult { items })?))
    }

    pub fn handle_catalog_attach(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: CatalogAttachRequest = boxed(req)?;
        // Version negotiation: validate the requested versions against what this
        // worker serves, then echo the resolved concrete versions back.
        let (resolved_data_version, resolved_implementation_version) =
            self.resolve_versions(&dto)?;
        // Version-shaped catalogs encode the resolved data version into the
        // attach_opaque_data (`<version>\0<id>`) so per-request catalog handlers
        // can select the right object set without server-side session state.
        let attach_opaque_data = if let Some(default_bytes) = &self.catalog.attach_options_default_batch {
            // Merge the user-supplied options over the declared defaults and
            // encode the one-row result as `<16-byte id>\0<ipc batch>`.
            let default_batch = ipc::read_batch(default_bytes)?;
            let options = dto.options.as_ref().map(|b| ipc::read_batch(&b.0)).transpose()?;
            let cols: Vec<arrow_array::ArrayRef> = default_batch
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| -> Result<arrow_array::ArrayRef> {
                    match options.as_ref().and_then(|o| o.column_by_name(f.name())) {
                        // Cast to the declared type to normalize nested field
                        // names (DuckDB's list/struct field names differ).
                        Some(c) => arrow_cast::cast(c, f.data_type())
                            .map_err(|e| RpcError::runtime_error(e.to_string())),
                        None => Ok(default_batch.column(i).clone()),
                    }
                })
                .collect::<Result<_>>()?;
            let merged = RecordBatch::try_new(default_batch.schema(), cols)
                .map_err(|e| RpcError::runtime_error(e.to_string()))?;
            let id = self.attach_bytes();
            let mut v: Vec<u8> = id.iter().copied().chain(std::iter::repeat(0)).take(16).collect();
            v.push(0);
            v.extend_from_slice(&ipc::write_batch(&merged)?);
            v
        } else if !self.catalog.version_schemas.is_empty() {
            let mut v = resolved_data_version.clone().unwrap_or_default().into_bytes();
            v.push(0);
            v.extend_from_slice(&self.attach_bytes());
            v
        } else if dto.name == PROJ_REPRO_APP {
            // The `projection_repro` reproducer is a distinct "app" served by the
            // same binary, selected by ATTACH name. Echo it back so function
            // advertisement (which is otherwise global) can scope to it.
            PROJ_REPRO_APP.as_bytes().to_vec()
        } else {
            self.attach_bytes()
        };
        let result = CatalogAttachResult {
            attach_opaque_data: Bytes::from(attach_opaque_data),
            supports_transactions: true,
            supports_time_travel: self.catalog.supports_time_travel,
            catalog_version_frozen: false,
            catalog_version: 1,
            attach_opaque_data_required: true,
            default_schema: catalog::MAIN_SCHEMA.to_string(),
            settings: self
                .settings
                .iter()
                .map(|s| Ok(Bytes::from(catalog::serialize_setting(s)?)))
                .collect::<Result<Vec<_>>>()?,
            secret_types: self
                .secret_types
                .iter()
                .map(|s| Ok(Bytes::from(catalog::serialize_secret_type(s)?)))
                .collect::<Result<Vec<_>>>()?,
            comment: self.catalog.comment.clone(),
            tags: self.catalog.tags.clone(),
            supports_column_statistics: self
                .catalog
                .schemas
                .iter()
                .flat_map(|s| &s.tables)
                .any(|t| !t.statistics.is_empty()),
            resolved_data_version,
            resolved_implementation_version,
        };
        Ok(Some(wire::to_result_batch(result)?))
    }

    /// Validate the ATTACH-time version request against the catalog's declared
    /// support and return the concrete `(data_version, implementation_version)`.
    /// Mirrors the Python `versioned` fixture: implementation must match
    /// exactly; data_version must be one of `supported_data_versions` (or the
    /// default when omitted). Errors propagate as the ATTACH failure.
    fn resolve_versions(
        &self,
        dto: &CatalogAttachRequest,
    ) -> Result<(Option<String>, Option<String>)> {
        let cat = &self.catalog;
        // Implementation version: npm-resolved against the supported set when
        // opted in, else exact-match against the single declared version.
        let resolved_impl = if cat.npm_version_resolution
            && !cat.supported_implementation_versions.is_empty()
        {
            Some(catalog::resolve_version_npm(
                dto.implementation_version.as_deref(),
                &cat.supported_implementation_versions,
                cat.implementation_version.as_deref().unwrap_or(""),
                "implementation_version",
            )?)
        } else {
            match (&dto.implementation_version, &cat.implementation_version) {
                (Some(req), Some(have)) if req != have => {
                    return Err(RpcError::value_error(format!(
                        "Unsupported implementation_version {req:?}; this worker serves {have:?}"
                    )));
                }
                (_, have) => have.clone(),
            }
        };
        // Data version: npm-style resolution when opted in, else exact-match.
        let resolved_data = if cat.supported_data_versions.is_empty() {
            None
        } else if cat.npm_version_resolution {
            Some(catalog::resolve_version_npm(
                dto.data_version_spec.as_deref(),
                &cat.supported_data_versions,
                cat.default_data_version.as_deref().unwrap_or(""),
                "data_version_spec",
            )?)
        } else if let Some(req) = &dto.data_version_spec {
            if !cat.supported_data_versions.contains(req) {
                return Err(RpcError::value_error(format!(
                    "Unsupported data_version_spec {req:?}; this worker serves one of {:?}",
                    cat.supported_data_versions
                )));
            }
            Some(req.clone())
        } else {
            cat.default_data_version.clone()
        };
        Ok((resolved_data, resolved_impl))
    }

    /// Decode the resolved data version from a request's `attach_opaque_data`
    /// column (`<version>\0<id>`). Returns `None` for non-version-shaped
    /// catalogs or when the column is absent.
    fn req_version(&self, req: &Request) -> Option<String> {
        if self.catalog.version_schemas.is_empty() {
            return None;
        }
        let bytes = read_binary_col(req, "attach_opaque_data")?;
        let sep = bytes.iter().position(|&b| b == 0)?;
        if sep == 0 {
            return None;
        }
        String::from_utf8(bytes[..sep].to_vec()).ok()
    }

    /// Version-aware schema lookup: selects the object set for the request's
    /// resolved data version (falls back to the base schemas).
    fn schema_for_req<'a>(&'a self, req: &Request, name: &str) -> Option<&'a catalog::CatSchema> {
        let v = self.req_version(req);
        self.catalog.schemas_for(v.as_deref()).iter().find(|s| s.name == name)
    }

    pub fn handle_catalog_version(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        Ok(Some(wire::to_result_batch(CatalogVersionResult { version: 1 })?))
    }

    pub fn handle_transaction_begin(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        // A fresh id per BEGIN so transaction-scoped caches (tx_cached_value)
        // don't leak across transactions; in autocommit DuckDB passes None.
        Ok(Some(wire::to_result_batch(CatalogTransactionBeginResult {
            transaction_opaque_data: Some(Bytes::from(self.next_execution_id())),
        })?))
    }

    fn schema_info_for(&self, name: &str) -> SchemaInfo {
        let comment = self
            .catalog
            .schema(name)
            .and_then(|s| s.comment.as_deref())
            .or(if name == catalog::MAIN_SCHEMA {
                Some("Default schema containing all registered functions")
            } else {
                None
            });
        let mut si = catalog::schema_info(name, comment, &self.attach_bytes());
        // Version-shaped catalogs vary their object set per attach — the counts
        // here aren't version-aware, so don't advertise them (avoids wrongly
        // caching the base schema's empty table set).
        if !self.catalog.version_schemas.is_empty() {
            return si;
        }
        // Advertise per-kind object counts so the C++ extension caches
        // `kind_empty` and skips the bulk discovery RPC for empty kinds.
        let sch = self.catalog.schema(name);
        let len = |n: usize| n as i64;
        let (sf, af, tf) = if name == catalog::MAIN_SCHEMA {
            (
                len(self.scalars.len()),
                len(self.aggregates.len()),
                len(self.tables.len() + self.tableinouts.len() + self.buffering.len()),
            )
        } else {
            (0, 0, 0)
        };
        si.estimated_object_count = Some(vec![
            ("view".into(), len(sch.map(|s| s.views.len()).unwrap_or(0))),
            ("macro".into(), len(sch.map(|s| s.macros.len()).unwrap_or(0))),
            ("table".into(), len(sch.map(|s| s.tables.len()).unwrap_or(0))),
            ("scalar_function".into(), sf),
            ("aggregate_function".into(), af),
            ("table_function".into(), tf),
            ("index".into(), 0),
        ]);
        si
    }

    pub fn handle_catalog_schemas(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        let infos: Vec<SchemaInfo> = self.schema_names().iter().map(|n| self.schema_info_for(n)).collect();
        let items = catalog::serialize_items(infos)?;
        Ok(Some(wire::to_result_batch(ItemsResult { items })?))
    }

    pub fn handle_schema_get(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let p: CatalogSchemaNameParams = wire::from_batch(&req.batch)?;
        let items = if self.schema_names().iter().any(|n| n == &p.name) {
            catalog::serialize_items(vec![self.schema_info_for(&p.name)])?
        } else {
            Vec::new()
        };
        Ok(Some(wire::to_result_batch(ItemsResult { items })?))
    }

    pub fn handle_contents_views(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let name = read_string_col(req, "name")?;
        let infos: Vec<ViewInfo> = self
            .catalog
            .schema(&name)
            .map(|s| s.views.iter().map(|v| catalog::view_info(&name, v)).collect())
            .unwrap_or_default();
        Ok(Some(wire::to_result_batch(ItemsResult {
            items: catalog::serialize_items(infos)?,
        })?))
    }

    pub fn handle_contents_tables(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let name = read_string_col(req, "name")?;
        let infos: Vec<TableInfo> = match self.schema_for_req(req, &name) {
            Some(s) => s
                .tables
                .iter()
                .map(|t| catalog::table_info(&name, t))
                .collect::<Result<_>>()?,
            None => Vec::new(),
        };
        Ok(Some(wire::to_result_batch(ItemsResult {
            items: catalog::serialize_items(infos)?,
        })?))
    }

    pub fn handle_table_get(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let schema_name = read_string_col(req, "schema_name")?;
        let table_name = read_string_col(req, "name")?;
        let at_unit = read_opt_string_col(req, "at_unit");
        let at_value = read_opt_string_col(req, "at_value");
        let infos: Vec<TableInfo> = self
            .schema_for_req(req, &schema_name)
            .and_then(|s| s.tables.iter().find(|t| t.name == table_name))
            .map(|t| {
                let tt = Self::at_version(t, at_unit.as_deref(), at_value.as_deref())?;
                catalog::table_info(&schema_name, &tt)
            })
            .transpose()?
            .into_iter()
            .collect();
        Ok(Some(wire::to_result_batch(ItemsResult {
            items: catalog::serialize_items(infos)?,
        })?))
    }

    /// Lazy scan-function resolution for non-inlined function-backed tables.
    /// Returns a FLAT `ScanFunctionResult` batch (no `{result}` envelope).
    pub fn handle_table_scan_function_get(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let schema_name = read_string_col(req, "schema_name")?;
        let table_name = read_string_col(req, "name")?;
        let at_unit = read_opt_string_col(req, "at_unit");
        let at_value = read_opt_string_col(req, "at_value");
        let t = self
            .schema_for_req(req, &schema_name)
            .and_then(|s| s.tables.iter().find(|t| t.name == table_name))
            .ok_or_else(|| {
                RpcError::value_error(format!("Unknown table: '{schema_name}.{table_name}'"))
            })?;
        let t = Self::at_version(t, at_unit.as_deref(), at_value.as_deref())?;
        Ok(Some(wire::to_result_batch(catalog::scan_function_result(&t)?)?))
    }

    /// Return the table view for a requested time-travel `AT` clause (the
    /// version's columns + scan), or the table unchanged when not time-travel.
    fn at_version(
        t: &catalog::CatTable,
        at_unit: Option<&str>,
        at_value: Option<&str>,
    ) -> Result<catalog::CatTable> {
        match t.resolve_version(at_unit, at_value)? {
            Some(v) => {
                let mut tt = t.clone();
                tt.columns = v.columns.clone();
                tt.scan_function = v.scan_function.clone();
                tt.scan_arguments = v.scan_arguments.clone();
                // Constraints are defined against the current schema; drop them
                // for historical versions whose columns differ.
                if !t.is_current_version(v.version) {
                    tt.not_null.clear();
                    tt.primary_key.clear();
                    tt.unique.clear();
                    tt.check.clear();
                    tt.foreign_keys.clear();
                }
                Ok(tt)
            }
            None => Ok(t.clone()),
        }
    }

    /// Per-call cardinality for a function-backed table scan.
    pub fn handle_table_function_cardinality(&self, req: &Request, ctx: &CallContext) -> Result<Option<RecordBatch>> {
        let dto: CardinalityRequest = boxed(req)?;
        let bind_call: BindRequest = wire::from_batch(&ipc::read_batch(&dto.bind_call.0)?)?;
        let bp = self.bind_params(&bind_call, ctx)?;
        let card = self
            .tables
            .get(&bind_call.function_name)
            .and_then(|v| v.first())
            .and_then(|f| f.cardinality(&bp));
        let resp = crate::protocol::dtos::CardinalityResponse {
            estimate: Some(card.and_then(|c| c.estimate).unwrap_or(-1)),
            max: Some(card.and_then(|c| c.max).unwrap_or(-1)),
        };
        Ok(Some(wire::to_result_batch(resp)?))
    }

    /// Post-execution profiling info (EXPLAIN ANALYZE Extra Info).
    pub fn handle_table_function_dynamic_to_string(&self, req: &Request) -> Result<Option<RecordBatch>> {
        use crate::protocol::dtos::{DynamicToStringRequest, DynamicToStringResponse};
        let dto: DynamicToStringRequest = boxed(req)?;
        let bind_call: BindRequest = wire::from_batch(&ipc::read_batch(&dto.bind_call.0)?)?;
        let pairs = self
            .tables
            .get(&bind_call.function_name)
            .and_then(|v| v.first())
            .map(|f| f.dynamic_to_string(&dto.global_execution_id.0, &self.store))
            .unwrap_or_default();
        let (keys, values): (Vec<String>, Vec<String>) = pairs.into_iter().unzip();
        Ok(Some(wire::to_result_batch(DynamicToStringResponse { keys, values })?))
    }

    /// Per-call statistics for a function-backed table scan (e.g. `sequence`).
    pub fn handle_table_function_statistics(&self, req: &Request, ctx: &CallContext) -> Result<Option<RecordBatch>> {
        let dto: CardinalityRequest = boxed(req)?;
        let bind_call: BindRequest = wire::from_batch(&ipc::read_batch(&dto.bind_call.0)?)?;
        let bp = self.bind_params(&bind_call, ctx)?;
        let stats = self
            .tables
            .get(&bind_call.function_name)
            .and_then(|v| v.first())
            .and_then(|f| f.statistics(&bp))
            .unwrap_or_default();
        let bytes = crate::statistics::serialize_column_statistics(&stats)?;
        Ok(Some(wire::result_batch_from_bytes(&bytes)?))
    }

    /// Per-column optimizer statistics for a table. Returns the sparse-union
    /// IPC batch (result-wrapped), empty when the table declares no stats.
    pub fn handle_table_column_statistics_get(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let schema_name = read_string_col(req, "schema_name")?;
        let table_name = read_string_col(req, "name")?;
        let stats = self
            .catalog
            .schema(&schema_name)
            .and_then(|s| s.tables.iter().find(|t| t.name == table_name))
            .map(|t| t.statistics.clone())
            .unwrap_or_default();
        let bytes = crate::statistics::serialize_column_statistics(&stats)?;
        Ok(Some(wire::result_batch_from_bytes(&bytes)?))
    }

    /// Multi-branch scan resolution. A single-source table returns one branch
    /// wrapping its scan function; the list must be non-empty.
    pub fn handle_table_scan_branches_get(&self, req: &Request) -> Result<Option<RecordBatch>> {
        use crate::protocol::dtos::{ScanBranch, ScanBranchesResult};
        let schema_name = read_string_col(req, "schema_name")?;
        let table_name = read_string_col(req, "name")?;
        let at_unit = read_opt_string_col(req, "at_unit");
        let at_value = read_opt_string_col(req, "at_value");
        let base = self
            .schema_for_req(req, &schema_name)
            .and_then(|s| s.tables.iter().find(|t| t.name == table_name))
            .ok_or_else(|| {
                RpcError::value_error(format!("Unknown table: '{schema_name}.{table_name}'"))
            })?;
        // Resolve the time-travel version so the default branch wraps the
        // version's scan function + arguments (legacy non-inline path).
        let resolved = Self::at_version(base, at_unit.as_deref(), at_value.as_deref())?;
        let t = &resolved;
        let mk = |b: ScanBranch| -> Result<Bytes> {
            Ok(Bytes::from(ipc::write_batch(&wire::to_batch(b)?)?))
        };
        let branches: Vec<Bytes> = match &t.branches {
            // Explicit multi-branch sources (possibly empty — the empty case
            // exercises the C++ loud-fail rejection).
            Some(defs) => defs
                .iter()
                .map(|d| {
                    mk(ScanBranch {
                        function_name: d.function_name.clone(),
                        arguments: Bytes::from(d.scan_arguments.clone()),
                        branch_filter: d.branch_filter.clone(),
                        writable: d.writable,
                    })
                })
                .collect::<Result<_>>()?,
            // Single-source default: one branch wrapping the scan function.
            None => vec![mk(ScanBranch {
                function_name: t.scan_function.clone(),
                arguments: Bytes::from(t.scan_arguments.clone()),
                branch_filter: None,
                writable: false,
            })?],
        };
        Ok(Some(wire::to_result_batch(ScanBranchesResult {
            branches,
            required_extensions: Vec::new(),
        })?))
    }

    pub fn handle_contents_macros(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let name = read_string_col(req, "name")?;
        let want = normalize_function_type(&read_string_col(req, "type").unwrap_or_default());
        let infos: Vec<MacroInfo> = self
            .catalog
            .schema(&name)
            .map(|s| {
                s.macros
                    .iter()
                    .filter(|m| match want.as_deref() {
                        Some("table") => m.table_macro,
                        Some("scalar") => !m.table_macro,
                        _ => true,
                    })
                    .map(|m| catalog::macro_info(&name, m))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Some(wire::to_result_batch(ItemsResult {
            items: catalog::serialize_items(infos)?,
        })?))
    }

    pub fn handle_contents_functions(&self, req: &Request) -> Result<Option<RecordBatch>> {
        // `type` is a Rust reserved word; read the columns by name directly
        // rather than via a derived DTO (the derive can't emit a `type` field).
        let schema_name = read_string_col(req, "name")?;
        let type_filter = read_string_col(req, "type").unwrap_or_default();
        // The `projection_repro` app's functions are advertised only for that
        // catalog; every other catalog hides them (they share this binary).
        let is_proj_repro = read_binary_col(req, "attach_opaque_data")
            .map(|b| b == PROJ_REPRO_APP.as_bytes())
            .unwrap_or(false);
        let visible = |name: &str| name.starts_with(PROJ_REPRO_PREFIX) == is_proj_repro;
        let mut infos = Vec::new();
        if schema_name == catalog::MAIN_SCHEMA {
            let want = normalize_function_type(&type_filter);
            if want.as_deref() == Some("scalar") || want.is_none() {
                let mut names: Vec<&String> = self.scalars.keys().filter(|n| visible(n)).collect();
                names.sort();
                for name in names {
                    for f in &self.scalars[name] {
                        infos.push(catalog::scalar_function_info(f.as_ref())?);
                    }
                }
            }
            // Table-buffering functions also surface under a TABLE request.
            if matches!(want.as_deref(), Some("table") | Some("table_buffering")) || want.is_none() {
                let mut names: Vec<&String> = self.tables.keys().filter(|n| visible(n)).collect();
                names.sort();
                for name in names {
                    for f in &self.tables[name] {
                        infos.push(catalog::table_function_info(f.as_ref())?);
                    }
                }
                let mut tio: Vec<&String> = self.tableinouts.keys().filter(|n| visible(n)).collect();
                tio.sort();
                for name in tio {
                    for f in &self.tableinouts[name] {
                        infos.push(catalog::table_in_out_function_info(f.as_ref())?);
                    }
                }
                let mut buf: Vec<&String> = self.buffering.keys().filter(|n| visible(n)).collect();
                buf.sort();
                for name in buf {
                    for f in &self.buffering[name] {
                        infos.push(catalog::buffering_function_info(f.as_ref())?);
                    }
                }
            }
            if matches!(want.as_deref(), Some("aggregate")) || want.is_none() {
                let mut agg: Vec<&String> = self.aggregates.keys().filter(|n| visible(n)).collect();
                agg.sort();
                for name in agg {
                    for f in &self.aggregates[name] {
                        infos.push(catalog::aggregate_function_info(f.as_ref())?);
                    }
                }
            }
        }
        let items = catalog::serialize_items(infos)?;
        Ok(Some(wire::to_result_batch(ItemsResult { items })?))
    }

    // -- table buffering RPCs ----------------------------------------------

    /// The bound output schema for a buffering execution, persisted by the
    /// sink init to the file-backed store (process/combine carry no schema and
    /// may run in a different pooled worker). Falls back to `default` when no
    /// schema was persisted (e.g. echo-style functions where it is unused).
    fn buffering_output_schema(
        &self,
        execution_id: &[u8],
        default: arrow_schema::SchemaRef,
    ) -> arrow_schema::SchemaRef {
        self.store
            .kv_get(execution_id, b"outsc")
            .and_then(|b| ipc::read_schema(&b).ok())
            .unwrap_or(default)
    }

    pub fn handle_buffering_process(&self, req: &Request, ctx: &CallContext) -> Result<Option<RecordBatch>> {
        let dto: TableBufferingProcessRequest = boxed(req)?;
        let f = self.resolve_buffering(&dto.function_name)?;
        let batch = ipc::read_batch(&dto.input_batch.0)?;
        let output_schema = self.buffering_output_schema(&dto.execution_id.0, batch.schema());
        let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let params = BufferingParams {
            execution_id: dto.execution_id.0.clone(),
            storage: self.store.clone(),
            output_schema,
            arguments: crate::arguments::Arguments::default(),
            settings: crate::settings::Settings::default(),
            batch_index: dto.batch_index,
            logs: logs.clone(),
        };
        let state_id = f.process(&params, &batch)?;
        Self::drain_buffering_logs(&logs, ctx);
        Ok(Some(wire::to_result_batch(TableBufferingProcessResponse {
            state_id: Bytes::from(state_id),
        })?))
    }

    /// Forward queued buffering INFO logs to the call context (→ duckdb_logs()).
    fn drain_buffering_logs(logs: &std::sync::Arc<std::sync::Mutex<Vec<String>>>, ctx: &CallContext) {
        if let Ok(mut g) = logs.lock() {
            for msg in g.drain(..) {
                ctx.client_log(vgi_rpc::LogLevel::Info, msg);
            }
        }
    }

    pub fn handle_buffering_combine(&self, req: &Request, ctx: &CallContext) -> Result<Option<RecordBatch>> {
        let dto: TableBufferingCombineRequest = boxed(req)?;
        let f = self.resolve_buffering(&dto.function_name)?;
        let output_schema =
            self.buffering_output_schema(&dto.execution_id.0, Arc::new(arrow_schema::Schema::empty()));
        let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let params = BufferingParams {
            execution_id: dto.execution_id.0.clone(),
            storage: self.store.clone(),
            output_schema,
            arguments: crate::arguments::Arguments::default(),
            settings: crate::settings::Settings::default(),
            batch_index: None,
            logs: logs.clone(),
        };
        let state_ids: Vec<Vec<u8>> = dto.state_ids.into_iter().map(|b| b.0).collect();
        let finalize_ids = f.combine(&params, &state_ids)?;
        Self::drain_buffering_logs(&logs, ctx);
        Ok(Some(wire::to_result_batch(TableBufferingCombineResponse {
            finalize_state_ids: finalize_ids.into_iter().map(Bytes::from).collect(),
        })?))
    }

    pub fn handle_buffering_destructor(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: TableBufferingDestructorRequest = boxed(req)?;
        self.store.clear(&dto.execution_id.0);
        Ok(None)
    }

    // -- aggregate RPCs ----------------------------------------------------

    fn agg_key(gid: i64) -> Vec<u8> {
        gid.to_le_bytes().to_vec()
    }

    pub fn handle_aggregate_bind(&self, req: &Request, ctx: &CallContext) -> Result<Option<RecordBatch>> {
        let dto: AggregateBindRequest = boxed(req)?;
        let mut args = crate::arguments::Arguments::parse(&dto.arguments.0)?;
        let input_schema = opt_schema(&dto.input_schema)?;
        let f = self.resolve_aggregate(&dto.function_name)?;
        args.remap_positional(&f.argument_specs());
        let _ = ctx;
        let params = AggregateBindParams {
            arguments: args,
            input_schema,
            settings: parse_settings(&dto.settings)?,
        };
        let bind = f.on_bind(&params)?;
        let execution_id = self.next_execution_id();
        // Stash the raw bind-time arguments so `finalize` can rebuild const
        // params (e.g. `vgi_percentile`'s percentile) — update/finalize RPCs
        // don't resend arguments and may run in a different pooled worker.
        self.store.kv_put(&execution_id, b"aggargs", &dto.arguments.0);
        Ok(Some(wire::to_result_batch(AggregateBindResponse {
            output_schema: Bytes::from(ipc::write_schema_ref(&bind.output_schema)?),
            execution_id: Bytes::from(execution_id),
        })?))
    }

    pub fn handle_aggregate_update(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateUpdateRequest = boxed(req)?;
        let f = self.resolve_aggregate(&dto.function_name)?;
        let batch = ipc::read_batch(&dto.input_batch.0)?;
        let (gids, columns) = split_group_ids(&batch)?;
        // Pre-load only EXISTING states from prior batches; do NOT seed
        // `initial_state` for groups with no state yet. The function's
        // `update` creates an entry (via `or_insert_with`) only when it
        // actually folds in a value — so a group that received only NULLs
        // (DEFAULT null handling) leaves no state and finalizes to NULL
        // rather than a seeded 0.
        let mut states: HashMap<i64, Vec<u8>> = HashMap::new();
        for i in 0..gids.len() {
            let gid = gids.value(i);
            if !states.contains_key(&gid) {
                if let Some(s) = self.store.kv_get(&dto.execution_id.0, &Self::agg_key(gid)) {
                    states.insert(gid, s);
                }
            }
        }
        f.update(&mut states, &gids, &columns)?;
        for (gid, state) in states {
            self.store
                .kv_put(&dto.execution_id.0, &Self::agg_key(gid), &state);
        }
        Ok(Some(wire::empty_result_batch()?))
    }

    pub fn handle_aggregate_combine(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateCombineRequest = boxed(req)?;
        let f = self.resolve_aggregate(&dto.function_name)?;
        let batch = ipc::read_batch(&dto.merge_batch.0)?;
        let src = batch
            .column_by_name("source_group_id")
            .or_else(|| Some(batch.column(0)))
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| RpcError::type_error("combine: source_group_id"))?
            .clone();
        let tgt = batch
            .column_by_name("target_group_id")
            .or_else(|| batch.columns().get(1))
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| RpcError::type_error("combine: target_group_id"))?
            .clone();
        for i in 0..src.len() {
            let s = src.value(i);
            let t = tgt.value(i);
            let source = self.store.kv_get(&dto.execution_id.0, &Self::agg_key(s));
            let target = self.store.kv_get(&dto.execution_id.0, &Self::agg_key(t));
            // Both absent (e.g. an all-NULL group under DEFAULT null handling):
            // leave the target stateless so finalize yields NULL, not a seeded 0.
            let merged = match (target, source) {
                (None, None) => continue,
                (Some(t), None) => t,
                (None, Some(s)) => s,
                (Some(t), Some(s)) => f.combine(t, s)?,
            };
            self.store
                .kv_put(&dto.execution_id.0, &Self::agg_key(t), &merged);
        }
        Ok(Some(wire::empty_result_batch()?))
    }

    pub fn handle_aggregate_finalize(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateFinalizeRequest = boxed(req)?;
        let f = self.resolve_aggregate(&dto.function_name)?;
        let output_schema = ipc::read_schema(&dto.output_schema.0)?;
        let gid_batch = ipc::read_batch(&dto.group_ids_batch.0)?;
        let gids = gid_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| RpcError::type_error("finalize: group_ids not int64"))?
            .clone();
        let states: Vec<Option<Vec<u8>>> = (0..gids.len())
            .map(|i| self.store.kv_get(&dto.execution_id.0, &Self::agg_key(gids.value(i))))
            .collect();
        // Reload the bind-time arguments stashed at aggregate_bind, remapped
        // to the function's declared positions, for ConstParam finalize.
        let mut agg_args = self
            .store
            .kv_get(&dto.execution_id.0, b"aggargs")
            .and_then(|b| crate::arguments::Arguments::parse(&b).ok())
            .unwrap_or_default();
        agg_args.remap_positional(&f.argument_specs());
        let result = f.finalize_with_args(&output_schema, &gids, &states, &agg_args)?;
        Ok(Some(wire::to_result_batch(AggregateFinalizeResponse {
            result_batch: Bytes::from(ipc::write_batch(&result)?),
        })?))
    }

    pub fn handle_aggregate_destructor(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateDestructorRequest = boxed(req)?;
        self.store.clear(&dto.execution_id.0);
        Ok(Some(wire::empty_result_batch()?))
    }

    // -- aggregate window RPCs ---------------------------------------------

    fn win_key(partition_id: i64, suffix: &str) -> Vec<u8> {
        format!("win_{partition_id}_{suffix}").into_bytes()
    }

    pub fn handle_aggregate_window_init(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateWindowInitRequest = boxed(req)?;
        // Cache the partition (input columns + output schema + filter mask) so
        // the window / window_batch calls — possibly in another pooled worker —
        // can evaluate frames against it.
        self.store.kv_put(&dto.execution_id.0, &Self::win_key(dto.partition_id, "p"), &dto.partition_batch.0);
        self.store.kv_put(&dto.execution_id.0, &Self::win_key(dto.partition_id, "o"), &dto.output_schema.0);
        if let Some(m) = &dto.filter_mask {
            self.store.kv_put(&dto.execution_id.0, &Self::win_key(dto.partition_id, "m"), &m.0);
        }
        Ok(Some(wire::empty_result_batch()?))
    }

    /// Load the cached partition + output schema for a window call.
    fn load_window_partition(
        &self,
        exec: &[u8],
        partition_id: i64,
    ) -> Result<(RecordBatch, SchemaRef, Option<Vec<bool>>)> {
        let pb = self
            .store
            .kv_get(exec, &Self::win_key(partition_id, "p"))
            .ok_or_else(|| RpcError::runtime_error(format!(
                "aggregate_window: unknown partition_id={partition_id}"
            )))?;
        let os = self
            .store
            .kv_get(exec, &Self::win_key(partition_id, "o"))
            .ok_or_else(|| RpcError::runtime_error("aggregate_window: missing output schema"))?;
        let partition = ipc::read_batch(&pb)?;
        let output_schema = ipc::read_schema(&os)?;
        // `filter_mask` is an Arrow packed-bit boolean buffer (LSB-first),
        // length == partition row count. Empty/absent means "all rows valid".
        let n = partition.num_rows();
        let mask = self
            .store
            .kv_get(exec, &Self::win_key(partition_id, "m"))
            .filter(|b| !b.is_empty())
            .map(|bytes| {
                (0..n)
                    .map(|i| bytes.get(i / 8).map(|byte| byte & (1 << (i % 8)) != 0).unwrap_or(true))
                    .collect::<Vec<bool>>()
            });
        Ok((partition, output_schema, mask))
    }

    pub fn handle_aggregate_window(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateWindowRequest = boxed(req)?;
        let f = self.resolve_aggregate(&dto.function_name)?;
        let (partition, output_schema, mask) =
            self.load_window_partition(&dto.execution_id.0, dto.partition_id)?;
        let frames: Vec<(i64, i64)> = dto
            .frame_starts
            .iter()
            .zip(dto.frame_ends.iter())
            .map(|(&s, &e)| (s, e))
            .collect();
        let col = f.window(&partition, &output_schema, &[frames], mask.as_deref())?;
        let batch = RecordBatch::try_new(output_schema, vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(wire::to_result_batch(AggregateWindowResponse {
            result_batch: Bytes::from(ipc::write_batch(&batch)?),
        })?))
    }

    pub fn handle_aggregate_window_batch(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateWindowBatchRequest = boxed(req)?;
        let f = self.resolve_aggregate(&dto.function_name)?;
        let (partition, output_schema, mask) =
            self.load_window_partition(&dto.execution_id.0, dto.partition_id)?;
        // Split the flattened (start,end) arrays into per-row sub-frame lists.
        let mut frames: Vec<Vec<(i64, i64)>> = Vec::with_capacity(dto.count as usize);
        let mut off = 0usize;
        for r in 0..dto.count as usize {
            let n = dto.frames_per_row.get(r).copied().unwrap_or(0) as usize;
            let mut subs = Vec::with_capacity(n);
            for _ in 0..n {
                let s = dto.frame_starts.get(off).copied().unwrap_or(0);
                let e = dto.frame_ends.get(off).copied().unwrap_or(0);
                subs.push((s, e));
                off += 1;
            }
            frames.push(subs);
        }
        let col = f.window(&partition, &output_schema, &frames, mask.as_deref())?;
        let batch = RecordBatch::try_new(output_schema, vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(wire::to_result_batch(AggregateWindowResponse {
            result_batch: Bytes::from(ipc::write_batch(&batch)?),
        })?))
    }

    pub fn handle_aggregate_window_destructor(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateWindowDestructorRequest = boxed(req)?;
        for sfx in ["p", "o", "m"] {
            self.store.kv_del(&dto.execution_id.0, &Self::win_key(dto.partition_id, sfx));
        }
        Ok(Some(wire::empty_result_batch()?))
    }

    // -- aggregate streaming-partitioned RPCs ------------------------------

    fn ser_state_map(m: &std::collections::HashMap<Vec<u8>, Vec<u8>>) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(m.len() as u64).to_le_bytes());
        for (k, v) in m {
            out.extend_from_slice(&(k.len() as u64).to_le_bytes());
            out.extend_from_slice(k);
            out.extend_from_slice(&(v.len() as u64).to_le_bytes());
            out.extend_from_slice(v);
        }
        out
    }

    fn de_state_map(b: &[u8]) -> std::collections::HashMap<Vec<u8>, Vec<u8>> {
        let mut m = std::collections::HashMap::new();
        let rd = |b: &[u8], off: &mut usize, n: usize| -> Vec<u8> {
            let s = b[*off..*off + n].to_vec();
            *off += n;
            s
        };
        if b.len() < 8 {
            return m;
        }
        let mut off = 0usize;
        let count = u64::from_le_bytes(b[0..8].try_into().unwrap()) as usize;
        off += 8;
        for _ in 0..count {
            if off + 8 > b.len() { break; }
            let kl = u64::from_le_bytes(b[off..off + 8].try_into().unwrap()) as usize;
            off += 8;
            let k = rd(b, &mut off, kl);
            let vl = u64::from_le_bytes(b[off..off + 8].try_into().unwrap()) as usize;
            off += 8;
            let v = rd(b, &mut off, vl);
            m.insert(k, v);
        }
        m
    }

    pub fn handle_aggregate_streaming_open(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateStreamingOpenRequest = boxed(req)?;
        self.resolve_aggregate(&dto.function_name)?;
        let execution_id = self.next_execution_id();
        self.store.kv_put(&execution_id, b"strm_pkc", &dto.partition_key_count.to_le_bytes());
        self.store.kv_put(&execution_id, b"strm_okc", &dto.order_key_count.to_le_bytes());
        self.store.kv_put(&execution_id, b"strm_sos", &dto.output_schema.0);
        Ok(Some(wire::to_result_batch(AggregateStreamingOpenResponse {
            execution_id: Bytes::from(execution_id),
        })?))
    }

    pub fn handle_aggregate_streaming_chunk(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateStreamingChunkRequest = boxed(req)?;
        let f = self.resolve_aggregate(&dto.function_name)?;
        let chunk = ipc::read_batch(&dto.input_batch.0)?;
        let pkc = self
            .store
            .kv_get(&dto.execution_id.0, b"strm_pkc")
            .map(|b| i64::from_le_bytes(b[..8].try_into().unwrap()) as usize)
            .unwrap_or(0);
        let okc = self
            .store
            .kv_get(&dto.execution_id.0, b"strm_okc")
            .map(|b| i64::from_le_bytes(b[..8].try_into().unwrap()) as usize)
            .unwrap_or(0);
        let output_schema = self
            .store
            .kv_get(&dto.execution_id.0, b"strm_sos")
            .and_then(|b| ipc::read_schema(&b).ok());
        let mut states = self
            .store
            .kv_get(&dto.execution_id.0, b"strm_state")
            .map(|b| Self::de_state_map(&b))
            .unwrap_or_default();
        let col = f.streaming_chunk(&chunk, pkc, okc, &mut states)?;
        self.store
            .kv_put(&dto.execution_id.0, b"strm_state", &Self::ser_state_map(&states));
        let schema = output_schema.unwrap_or_else(|| {
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "result",
                col.data_type().clone(),
                true,
            )]))
        });
        let batch = RecordBatch::try_new(schema, vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(wire::to_result_batch(AggregateStreamingChunkResponse {
            result_batch: Bytes::from(ipc::write_batch(&batch)?),
        })?))
    }

    pub fn handle_aggregate_streaming_close(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateStreamingCloseRequest = boxed(req)?;
        for k in [b"strm_pkc".as_slice(), b"strm_okc", b"strm_sos", b"strm_state"] {
            self.store.kv_del(&dto.execution_id.0, k);
        }
        Ok(Some(wire::empty_result_batch()?))
    }

    /// Empty `ItemsResult` for the contents/get methods not yet implemented.
    pub fn handle_empty_items(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        Ok(Some(wire::to_result_batch(ItemsResult { items: Vec::new() })?))
    }

    /// Void result (commit / rollback / detach / drop).
    pub fn handle_void(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        Ok(None)
    }

    /// Every catalog-mutating DDL RPC ends here: the example catalog is
    /// read-only, so the request is accepted (proving the wire contract is
    /// intact) and rejected with a clear `catalog is read-only` error.
    pub fn handle_read_only(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        Err(RpcError::runtime_error("catalog is read-only"))
    }
}

/// Serializable rebuild info for an exchange stream, so HTTP continuations can
/// reconstruct the state from an AEAD token on any pooled worker.
#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ExchangeBlob {
    pub kind: String, // "scalar" | "table_in_out"
    pub function_name: String,
    pub output_schema: Vec<u8>,
    pub input_schema: Vec<u8>, // empty = none
    pub arguments: Vec<u8>,
    pub settings: Vec<u8>,
    pub secrets: Vec<u8>,
    pub execution_id: Vec<u8>,
    pub init_opaque: Vec<u8>,
    pub pushdown_filters: Vec<u8>, // empty = none
    pub auto_apply: bool,
    /// Producer-only: the inner producer's partial-chunk cursor
    /// ([`crate::table_function::TableProducer::encode_resume`]). Empty for
    /// exchange states and producers between chunks.
    pub inner_resume: Vec<u8>,
    /// Time-travel `AT` clause carried so a resumed function-backed producer
    /// still sees the version it was scanning (empty = no AT clause).
    pub at_unit: String,
    pub at_value: String,
}

/// Per-batch scalar exchange: calls `process` and emits the result.
struct ScalarExchangeState {
    func: Arc<dyn ScalarFunction>,
    params: ProcessParams,
    blob: Vec<u8>,
}

impl ExchangeState for ScalarExchangeState {
    fn exchange(
        &mut self,
        input: &RecordBatch,
        out: &mut OutputCollector,
        ctx: &CallContext,
    ) -> Result<()> {
        self.params.auth_principal = principal(ctx);
        let result = self.func.process(&self.params, input)?;
        out.emit(result)
    }

    fn encode_state(&self) -> Result<Vec<u8>> {
        Ok(self.blob.clone())
    }
}

/// A producer that yields nothing (the buffering sink emits via process RPCs).
struct EmptyProducer;
impl TableProducer for EmptyProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        Ok(None)
    }
}

/// Emits a fixed list of batches (table-in-out FINALIZE flush).
struct VecProducer {
    batches: Vec<RecordBatch>,
    pos: usize,
}
impl TableProducer for VecProducer {
    fn next_batch(&mut self, _out: &mut OutputCollector) -> Result<Option<RecordBatch>> {
        let b = self.batches.get(self.pos).cloned();
        if b.is_some() {
            self.pos += 1;
        }
        Ok(b)
    }
}

/// Per-input-batch table-in-out exchange. Applies auto-filter pushdown.
struct TableInOutExchangeState {
    func: Arc<dyn TableInOutFunction>,
    params: ProcessParams,
    filters: Option<crate::pushdown::PushdownFilters>,
    blob: Vec<u8>,
}

impl ExchangeState for TableInOutExchangeState {
    fn exchange(
        &mut self,
        input: &RecordBatch,
        out: &mut OutputCollector,
        ctx: &CallContext,
    ) -> Result<()> {
        self.params.auth_principal = principal(ctx);
        for batch in self.func.process(&self.params, input)? {
            let batch = match &self.filters {
                Some(f) => f.apply(&batch)?,
                None => batch,
            };
            out.emit(batch)?;
        }
        Ok(())
    }
    fn encode_state(&self) -> Result<Vec<u8>> {
        Ok(self.blob.clone())
    }
}

/// Adapter from a [`TableProducer`] to a vgi-rpc [`ProducerState`]. Applies
/// auto-filter pushdown to each batch before emitting.
struct TableProducerState {
    inner: Box<dyn TableProducer>,
    filters: Option<crate::pushdown::PushdownFilters>,
    /// When set, narrow each (post-filter) batch to this projected schema —
    /// the producer emitted the full schema so filters could see all columns.
    project_to: Option<arrow_schema::SchemaRef>,
    /// HTTP continuation token for resumable (work-queue) producers; `None`
    /// means drain fully in one response.
    resume_blob: Option<Vec<u8>>,
}

/// Per-response batch cap for resumable work-queue producers over HTTP: low
/// enough that the global init yields early (so the parallel secondary workers
/// each get a share of the shared queue) yet large enough to keep round-trips
/// reasonable.
const HTTP_WORKQUEUE_BATCH_LIMIT: usize = 4;

impl vgi_rpc::ProducerState for TableProducerState {
    fn produce(&mut self, out: &mut OutputCollector, ctx: &CallContext) -> Result<()> {
        // Per-tick dynamic filter (e.g. a tightening Top-N) arrives in the
        // request metadata; surface it to the producer and auto-apply it.
        let dynamic = ctx
            .tick_metadata("vgi_pushdown_filters")
            .and_then(|enc| crate::pushdown::PushdownFilters::parse_b64(&enc, &[]));
        self.inner.on_dynamic_filters(dynamic.as_ref());
        match self.inner.next_batch(out)? {
            None => {
                out.finish();
                Ok(())
            }
            Some(batch) => {
                let meta = self.inner.last_metadata();
                let active = dynamic.as_ref().or(self.filters.as_ref());
                let batch = match active {
                    Some(f) => f.apply(&batch)?,
                    None => batch,
                };
                let batch = match &self.project_to {
                    Some(ps) => crate::table_in_out::project_batch(&batch, ps)?,
                    None => batch,
                };
                match meta {
                    Some(m) => out.emit_with_metadata(batch, m),
                    None => out.emit(batch),
                }
            }
        }
    }
    fn batch_limit(&self) -> Option<usize> {
        self.resume_blob.as_ref().map(|_| HTTP_WORKQUEUE_BATCH_LIMIT)
    }
    fn encode_state(&self) -> Result<Vec<u8>> {
        match &self.resume_blob {
            None => Ok(Vec::new()),
            Some(bytes) => {
                // Re-encode the (static) bind blob with the producer's CURRENT
                // partial-chunk cursor so the continuation resumes mid-chunk.
                let mut blob: ExchangeBlob = vgi_rpc::stream_codec::bincode_decode(bytes)?;
                blob.inner_resume = self.inner.encode_resume();
                vgi_rpc::stream_codec::bincode_encode(&blob)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Read a "boxed" DTO from the `request` binary column (IPC stream).
fn boxed<T: VgiArrow>(req: &Request) -> Result<T> {
    let col = req
        .column("request")
        .ok_or_else(|| RpcError::type_error("request missing 'request' column"))?;
    let ba = col
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| RpcError::type_error("'request' column is not binary"))?;
    if ba.is_empty() || ba.is_null(0) {
        return Err(RpcError::type_error("'request' column is empty"));
    }
    let batch = ipc::read_batch(ba.value(0))?;
    if std::env::var("VGI_WIRE_DEBUG").is_ok() {
        eprintln!(
            "[vgi-wire] {} inner schema: {:?}",
            req.method,
            batch.schema().fields().iter().map(|f| format!("{}:{}", f.name(), f.data_type())).collect::<Vec<_>>()
        );
    }
    wire::from_batch::<T>(&batch)
}

/// A 0-column, 0-row batch for methods whose result is an empty struct.
fn empty_batch() -> RecordBatch {
    use arrow_array::RecordBatchOptions;
    RecordBatch::try_new_with_options(
        Arc::new(arrow_schema::Schema::empty()),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(0)),
    )
    .expect("empty batch")
}

/// Split an aggregate UPDATE batch into the group-id column and the remaining
/// input value columns (group-id column stripped).
fn split_group_ids(batch: &RecordBatch) -> Result<(Int64Array, Vec<ArrayRef>)> {
    let (gidx, _) = batch
        .schema()
        .column_with_name(GROUP_COLUMN_NAME)
        .ok_or_else(|| RpcError::type_error("update batch missing group-id column"))?;
    let gids = batch
        .column(gidx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| RpcError::type_error("group-id column not int64"))?
        .clone();
    let columns: Vec<ArrayRef> = (0..batch.num_columns())
        .filter(|&i| i != gidx)
        .map(|i| batch.column(i).clone())
        .collect();
    Ok((gids, columns))
}

/// Read a string (or dict-string) column at row 0 by name.
fn read_string_col(req: &Request, name: &str) -> Result<String> {
    let col = req
        .column(name)
        .ok_or_else(|| RpcError::type_error(format!("request missing '{name}' column")))?;
    <String as VgiArrow>::read(col, 0)
}

/// Read a nullable string column's row-0 value, if present and non-null.
fn read_opt_string_col(req: &Request, name: &str) -> Option<String> {
    let col = req.column(name)?;
    if col.is_null(0) {
        return None;
    }
    <String as VgiArrow>::read(col, 0).ok()
}

/// Read a (binary) column's row-0 bytes from a request, if present and non-null.
fn read_binary_col(req: &Request, name: &str) -> Option<Vec<u8>> {
    let col = req.column(name)?;
    col.as_any()
        .downcast_ref::<arrow_array::BinaryArray>()
        .filter(|a| a.len() > 0 && a.is_valid(0))
        .map(|a| a.value(0).to_vec())
}

fn parse_settings(field: &Option<Bytes>) -> Result<crate::settings::Settings> {
    match field {
        Some(b) => crate::settings::Settings::parse(&b.0),
        None => Ok(crate::settings::Settings::default()),
    }
}

fn parse_secrets(field: &Option<Bytes>) -> Result<crate::secrets::Secrets> {
    match field {
        Some(b) => crate::secrets::Secrets::parse(&b.0),
        None => Ok(crate::secrets::Secrets::default()),
    }
}

/// The authenticated principal, if any.
fn principal(ctx: &CallContext) -> Option<String> {
    if ctx.auth.authenticated || !ctx.auth.principal.is_empty() {
        Some(ctx.auth.principal.clone())
    } else {
        None
    }
}

/// Deserialize an optional IPC-serialized schema field.
fn opt_schema(field: &Option<Bytes>) -> Result<Option<SchemaRef>> {
    match field {
        Some(b) if !b.0.is_empty() => Ok(Some(ipc::read_schema(&b.0)?)),
        _ => Ok(None),
    }
}

/// Normalize a DuckDB function-type filter (`"SCALAR_FUNCTION"`, `"scalar"`,
/// …) to the short lowercase form; `None` means "no filter".
fn normalize_function_type(t: &str) -> Option<String> {
    if t.is_empty() {
        return None;
    }
    let lower = t.to_lowercase();
    let short = lower.strip_suffix("_function").unwrap_or(&lower);
    Some(short.to_string())
}

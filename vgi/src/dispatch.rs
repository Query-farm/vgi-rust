// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The VGI dispatcher: owns the function registries + catalog identity and
//! implements every RPC handler (bind, init, and the catalog discovery
//! methods).

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
use crate::buffering::{BufferingParams, TableBufferingFunction};
use crate::catalog;
use crate::function::{BindParams, ProcessParams, ScalarFunction};
use crate::ipc;
use crate::protocol::dtos::*;
use crate::storage::{default_storage, FunctionStorage};
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
    pub store: Arc<dyn FunctionStorage>,
    /// Declarative catalog (views / macros / function-backed tables).
    pub catalog: catalog::CatalogModel,
    /// Additional catalogs this worker serves (MetaWorker model). Each is
    /// advertised by `catalog_catalogs` and attachable by its name; an ATTACH
    /// mints a random per-session scope encoded into `attach_opaque_data`.
    pub secondary: Vec<catalog::CatalogModel>,
    /// Function names owned by each secondary catalog (parallel to `secondary`).
    /// Functions live in the worker-global registries, so these scope which
    /// names a catalog's `catalog_schema_contents_functions` advertises: a
    /// secondary shows only its own, and the primary hides every secondary's.
    pub(crate) secondary_functions: Vec<Vec<String>>,
    /// Function names registered for binding but hidden from
    /// `catalog_schema_contents_functions`. A function-backed catalog table
    /// needs its backing function resolvable at scan time, but the table may be
    /// the only intended entry point — advertising the function too would create
    /// a redundant SQL callable. See [`Dispatcher::hide_function`].
    pub(crate) hidden_functions: std::collections::HashSet<String>,
    /// Secret types registered by the worker (surfaced in `catalog_attach`).
    pub secret_types: Vec<catalog::SecretTypeSpec>,
    /// Custom settings registered by the worker.
    pub settings: Vec<catalog::SettingSpec>,
    /// Companion catalogs advertised for the client to ATTACH (surfaced in
    /// `catalog_attach.attach_catalogs`; lakehouse federation).
    pub attach_catalogs: Vec<crate::protocol::dtos::AttachCatalogInfo>,
    /// Custom `COPY ... FROM` format readers (advertised via
    /// `catalog_copy_from_formats`). Each is also registered as a table function
    /// in `tables` (under its handler name) by `Worker::register_copy_from`.
    pub copy_from_formats: Vec<Arc<dyn crate::copy_from::CopyFromFunction>>,
    /// Custom `COPY ... TO` format writers (advertised via
    /// `catalog_copy_from_formats` with `direction="to"`). Each is also
    /// registered as a table-buffering function in `buffering` (under its handler
    /// name) by `Worker::register_copy_to`.
    pub copy_to_formats: Vec<Arc<dyn crate::copy_to::CopyToFunction>>,
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
            store: default_storage(),
            catalog: catalog::CatalogModel::default(),
            secondary: Vec::new(),
            secondary_functions: Vec::new(),
            hidden_functions: std::collections::HashSet::new(),
            secret_types: Vec::new(),
            settings: Vec::new(),
            attach_catalogs: Vec::new(),
            copy_from_formats: Vec::new(),
            copy_to_formats: Vec::new(),
            exec_counter: AtomicU64::new(1),
        }
    }

    pub fn set_catalog(&mut self, model: catalog::CatalogModel) {
        self.catalog = model;
    }

    /// Add a secondary catalog (served alongside the primary, MetaWorker-style),
    /// declaring the worker-global function names it owns (so its function
    /// listing is scoped and the primary hides them).
    pub fn register_secondary_catalog(
        &mut self,
        model: catalog::CatalogModel,
        functions: Vec<String>,
    ) {
        self.secondary.push(model);
        self.secondary_functions.push(functions);
    }

    pub fn register_secret_type(&mut self, spec: catalog::SecretTypeSpec) {
        self.secret_types.push(spec);
    }

    pub fn register_setting(&mut self, spec: catalog::SettingSpec) {
        self.settings.push(spec);
    }

    /// Advertise a companion catalog for the client to ATTACH at VGI-attach time
    /// (surfaced in `catalog_attach.attach_catalogs`; lakehouse federation).
    pub fn register_attach_catalog(&mut self, info: crate::protocol::dtos::AttachCatalogInfo) {
        self.attach_catalogs.push(info);
    }

    /// Record a custom `COPY ... FROM` format reader for advertisement via
    /// `catalog_copy_from_formats`. The reader must also be registered as a
    /// table function under its handler name (done by
    /// `Worker::register_copy_from`).
    pub fn register_copy_from(&mut self, f: Arc<dyn crate::copy_from::CopyFromFunction>) {
        self.copy_from_formats.push(f);
    }

    /// Record a custom `COPY ... TO` format writer for advertisement via
    /// `catalog_copy_from_formats` (`direction="to"`). The writer must also be
    /// registered as a table-buffering function under its handler name (done by
    /// `Worker::register_copy_to`).
    pub fn register_copy_to(&mut self, f: Arc<dyn crate::copy_to::CopyToFunction>) {
        self.copy_to_formats.push(f);
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
        self.scalars
            .entry(f.name().to_string())
            .or_default()
            .push(f);
    }

    pub fn register_table(&mut self, f: Arc<dyn TableFunction>) {
        self.tables.entry(f.name().to_string()).or_default().push(f);
    }

    /// Hide `name` from `catalog_schema_contents_functions` without
    /// unregistering it. The function stays bindable — a function-backed catalog
    /// table still resolves its scan — but DuckDB never creates a SQL callable
    /// for it, so the table is the only entry point.
    pub fn hide_function(&mut self, name: impl Into<String>) {
        self.hidden_functions.insert(name.into());
    }

    /// Register `f` only if no table function with its name is registered yet.
    /// Used by `Worker::set_catalog` to auto-register catalog tables' embedded
    /// `scan_function_impl` without clobbering an explicit `register_table`.
    pub fn register_table_if_absent(&mut self, f: Arc<dyn TableFunction>) {
        if !self.tables.contains_key(f.name()) {
            self.tables.entry(f.name().to_string()).or_default().push(f);
        }
    }

    pub fn register_table_in_out(&mut self, f: Arc<dyn TableInOutFunction>) {
        // Blended ("UNNEST-style") foot-gun guards, mirroring the Python
        // resolve_metadata checks. Registration happens at worker startup, so
        // an authoring error fails loudly here rather than corrupting a query.
        if f.metadata().input_from_args {
            let name = f.name().to_string();
            let specs = f.argument_specs();
            assert!(
                !f.has_finish(),
                "{name}: a blended (input_from_args) table-in-out function cannot \
                 override finish() — it is a per-row map (DuckDB forbids FinalExecute \
                 under correlated LATERAL, one of the call shapes blended must serve). \
                 Use a classic TABLE-input table-in-out or a TableBufferingFunction \
                 for accumulating output."
            );
            assert!(
                specs.iter().all(|s| s.arrow_type != "table"),
                "{name}: a blended (input_from_args) function must not declare a \
                 TABLE arg — its positional args ARE the input columns."
            );
            assert!(
                specs.iter().all(|s| !(s.position >= 0 && s.is_const)),
                "{name}: a blended (input_from_args) function cannot take a \
                 positional const arg (in the column/LATERAL form DuckDB sweeps it \
                 into the input subquery; in the literal form it is indistinguishable \
                 from an input column). Use classic TABLE-input mode for a REQUIRED \
                 constant, or a named arg for optional config."
            );
            assert!(
                specs.iter().any(|s| s.position >= 0 && !s.is_const),
                "{name}: a blended (input_from_args) function needs at least one \
                 positional column arg (its per-row input column); found none."
            );
        }
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
        // Blended (input_from_args) overloads resolve by input-column count /
        // type against their declared positional args (which are the input
        // columns, absent from the wire args) — see `resolve_overload_blended`.
        let idx = crate::overload::resolve_overload_blended(
            cands.len(),
            |i| cands[i].argument_specs(),
            |i| cands[i].metadata().input_from_args,
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
        // wasm32-wasi has no process ids (`std::process::id()` aborts). A wasm
        // worker is single-process (stdio one-shot, or the shared TCP worker),
        // so time+counter already make the id unique; use 0 for the pid slot.
        #[cfg(not(target_arch = "wasm32"))]
        let pid = std::process::id();
        #[cfg(target_arch = "wasm32")]
        let pid: u32 = 0;
        let mut v = b"vgi-exec-".to_vec();
        v.extend_from_slice(&pid.to_le_bytes());
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
            // `copy_from` / `copy_to` are read out-of-band from the request batch
            // (the C++ extension omits the columns for non-COPY binds) and set by
            // the caller — see `handle_bind` / `handle_init`.
            copy_from: None,
            copy_to: None,
        })
    }

    // -- bind ---------------------------------------------------------------

    pub fn handle_bind(&self, req: &Request, ctx: &CallContext) -> Result<Option<RecordBatch>> {
        let inner = request_inner_batch(req)?;
        let dto: BindRequest = wire::from_batch(&inner)?;
        let mut params = self.bind_params(&dto, ctx)?;
        // The COPY ... FROM / ... TO contexts (when present) ride as out-of-band
        // nested-struct columns the C++ extension omits for ordinary binds. A
        // COPY-TO writer scopes its secret_lookups by the destination path here.
        params.copy_from = read_copy_from(&inner)?;
        params.copy_to = read_copy_to(&inner)?;
        let ft = normalize_function_type(&dto.function_type.0).unwrap_or_default();

        // Table buffering.
        if self.buffering.contains_key(&dto.function_name) {
            let f = self.resolve_buffering(&dto.function_name)?;
            params.arguments.remap_positional(&f.argument_specs());
            crate::function::validate_arg_constraints(&f.argument_specs(), &params.arguments)?;
            // Two-phase secret bind: first pass requests the secret types; the
            // C++ resolves them and re-binds with `resolved_secrets_provided`.
            if !params.resolved_secrets_provided {
                let lookups = f.secret_lookups(&params);
                if !lookups.is_empty() {
                    let resp = BindResponse {
                        output_schema: Bytes::from(Vec::new()),
                        opaque_data: Bytes::from(Vec::new()),
                        lookup_secret_types: lookups
                            .iter()
                            .map(|l| l.secret_type.clone())
                            .collect(),
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

        // Table-in-out.
        if self.tableinouts.contains_key(&dto.function_name) {
            let f = self.resolve_table_in_out(
                &dto.function_name,
                &params.arguments,
                params.input_schema.as_ref(),
            )?;
            params.arguments.remap_positional(&f.argument_specs());
            crate::function::validate_arg_constraints(&f.argument_specs(), &params.arguments)?;
            // Two-phase secret bind: first pass requests the secret types; the
            // C++ resolves them and re-binds with `resolved_secrets_provided`
            // and the same input schema (so the retry can derive an output
            // schema that extends the input). The resolved secret then reaches
            // `process` via `params.secrets`.
            if !params.resolved_secrets_provided {
                let lookups = f.secret_lookups(&params);
                if !lookups.is_empty() {
                    let resp = BindResponse {
                        output_schema: Bytes::from(Vec::new()),
                        opaque_data: Bytes::from(Vec::new()),
                        lookup_secret_types: lookups
                            .iter()
                            .map(|l| l.secret_type.clone())
                            .collect(),
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

        // Table (producer) kind.
        if (ft == "table" || ft == "table_buffering")
            || (!self.scalars.contains_key(&dto.function_name)
                && self.tables.contains_key(&dto.function_name))
        {
            let f = self.resolve_table(
                &dto.function_name,
                &params.arguments,
                params.input_schema.as_ref(),
            )?;
            params.arguments.remap_positional(&f.argument_specs());
            crate::function::validate_arg_constraints(&f.argument_specs(), &params.arguments)?;
            // Two-phase secret bind: first pass requests the secret types; the
            // C++ resolves them and re-binds with `resolved_secrets_provided`.
            if !params.resolved_secrets_provided {
                let lookups = f.secret_lookups(&params);
                if !lookups.is_empty() {
                    let resp = BindResponse {
                        output_schema: Bytes::from(Vec::new()),
                        opaque_data: Bytes::from(Vec::new()),
                        lookup_secret_types: lookups
                            .iter()
                            .map(|l| l.secret_type.clone())
                            .collect(),
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

        let f = self.resolve_scalar(
            &dto.function_name,
            &params.arguments,
            params.input_schema.as_ref(),
        )?;
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

        let specs = f.argument_specs();
        crate::function::validate_type_bounds(&specs, params.input_schema.as_ref())?;
        crate::function::validate_arg_constraints(&specs, &params.arguments)?;
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
        let bind_call_batch = ipc::read_batch(&dto.bind_call.0)?;
        let bind_call: BindRequest = wire::from_batch(&bind_call_batch)?;
        // COPY ... FROM context (out-of-band struct column, absent for ordinary
        // scans) — threaded onto every ProcessParams built below.
        let copy_from = read_copy_from(&bind_call_batch)?;
        // COPY ... TO context (out-of-band struct column, absent for ordinary
        // scans) — persisted at sink-init for the process/combine RPCs.
        let copy_to = read_copy_to(&bind_call_batch)?;
        let mut bp = self.bind_params(&bind_call, ctx)?;
        bp.copy_from = copy_from.clone();
        bp.copy_to = copy_to.clone();
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

        let build_params =
            |args: crate::arguments::Arguments, settings, secrets, auth| ProcessParams {
                output_schema: output_schema.clone(),
                input_schema: input_schema.clone(),
                execution_id: execution_id.clone(),
                substream_id: dto.substream_id.clone().map(|b| b.into()),
                init_opaque_data: dto
                    .bind_opaque_data
                    .clone()
                    .map(|b| b.into())
                    .unwrap_or_default(),
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
                copy_from: copy_from.clone(),
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
                let fsid = dto
                    .finalize_state_id
                    .clone()
                    .map(|b| b.0)
                    .unwrap_or_default();
                let bparams = BufferingParams {
                    execution_id,
                    storage: self.store.clone(),
                    output_schema: output_schema.clone(),
                    arguments: bp.arguments,
                    settings: bp.settings,
                    secrets: bp.secrets,
                    attach_opaque_data: bind_call.attach_opaque_data.clone().map(|b| b.into()),
                    batch_index: None,
                    copy_to: copy_to.clone(),
                    input_schema: input_schema.clone(),
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
                let state = TableProducerState {
                    inner: producer,
                    filters,
                    project_to: None,
                    resume_blob: None,
                    conditional_checked: false,
                };
                return Ok(
                    StreamResult::producer(output_schema, Box::new(state)).with_header(header)
                );
            }
            // Sink phase: emit nothing; data arrives via process RPCs.
            // The process/combine RPCs carry no schema and may run in a
            // different pooled worker, so persist the bound output schema
            // (which may differ from the input, e.g. sum_all_columns) to the
            // file-backed store keyed by execution_id for them to read. Also
            // persist the input schema so they can re-derive the output schema
            // via on_bind should the stored copy be unreadable (see
            // buffering_output_schema).
            self.store.kv_put(
                &execution_id,
                b"outsc",
                &ipc::write_schema_ref(&output_schema)?,
            );
            if let Some(insc) = input_schema.as_ref() {
                self.store
                    .kv_put(&execution_id, b"insc", &ipc::write_schema_ref(insc)?);
            }
            // COPY ... TO context (destination format + path): the process /
            // combine RPCs carry no bind_call, so persist it keyed by
            // execution_id for them to replay. See `handle_buffering_*`.
            if let Some(ct) = copy_to.as_ref() {
                self.store
                    .kv_put(&execution_id, b"copytofmt", ct.format.as_bytes());
                self.store
                    .kv_put(&execution_id, b"copytopath", ct.file_path.as_bytes());
            }
            // Resolved secrets (forwarded via the two-phase secret bind): the
            // process/combine RPCs carry no bind_call, so persist the secrets IPC
            // blob keyed by execution_id for them to replay onto BufferingParams.
            // This is what lets a COPY-TO writer's write()/close() read the
            // caller's CREATE SECRET credentials. See `handle_buffering_*`.
            if let Some(s) = bind_call.secrets.as_ref() {
                self.store.kv_put(&execution_id, b"bufsecrets", &s.0);
            }
            // Persist named flags the process/combine RPCs need (e.g. `logging`),
            // since those RPCs carry no arguments and may run in another worker.
            self.store.kv_put(
                &execution_id,
                b"bufflags",
                &[bp.arguments.named_bool("logging").unwrap_or(false) as u8],
            );
            // Replay the call arguments + attach scope to the process/combine
            // RPCs (they carry neither and may run in another pooled worker).
            // Stateful buffering functions (e.g. `accumulate`) read these.
            self.store
                .kv_put(&execution_id, b"bufargs", &bind_call.arguments.0);
            if let Some(a) = bind_call.attach_opaque_data.as_ref() {
                self.store.kv_put(&execution_id, b"bufattach", &a.0);
            }
            let state = TableProducerState {
                inner: Box::new(EmptyProducer),
                filters: None,
                project_to: None,
                resume_blob: None,
                conditional_checked: false,
            };
            return Ok(StreamResult::producer(output_schema, Box::new(state)).with_header(header));
        }

        // Table-in-out (exchange) path.
        if self.tableinouts.contains_key(&bind_call.function_name) {
            let f = self.resolve_table_in_out(
                &bind_call.function_name,
                &bp.arguments,
                input_schema.as_ref(),
            )?;
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
                    conditional_checked: false,
                };
                return Ok(
                    StreamResult::producer(output_schema, Box::new(state)).with_header(header)
                );
            }
            let filters = if auto_apply {
                params
                    .pushdown_filters
                    .as_ref()
                    .map(|b| {
                        crate::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys)
                    })
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
            let state = TableInOutExchangeState {
                func: f,
                params,
                filters,
                blob,
            };
            return Ok(
                StreamResult::exchange(output_schema, in_schema, Box::new(state))
                    .with_header(header),
            );
        }

        // Table (producer) path.
        if (ft == "table" || ft == "table_buffering")
            || (!self.scalars.contains_key(&bind_call.function_name)
                && self.tables.contains_key(&bind_call.function_name))
        {
            let f = self.resolve_table(
                &bind_call.function_name,
                &bp.arguments,
                input_schema.as_ref(),
            )?;
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
                    .map(|b| {
                        crate::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys)
                    })
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
            // Always carry the rebuild blob so any resumable producer can yield a
            // continuation token over HTTP (one batch per response, like the
            // Python/Go workers) instead of draining the whole scan into memory.
            // Producers that can't serialize their position drain anyway — see
            // `TableProducerState::batch_limit`.
            let resume_blob = Some(self.exchange_blob(
                "table",
                bind_call.function_name.clone(),
                &output_schema,
                None,
                &bind_call,
                &dto,
                &execution_id,
                auto_apply,
            )?);
            let header = wire::to_batch(GlobalInitResponse {
                execution_id: Bytes::from(execution_id),
                max_workers,
                opaque_data: None,
            })?;
            let state = TableProducerState {
                inner: producer,
                filters,
                project_to,
                resume_blob,
                conditional_checked: false,
            };
            return Ok(StreamResult::producer(output_schema, Box::new(state)).with_header(header));
        }

        // Scalar (exchange) path.
        let f = self.resolve_scalar(
            &bind_call.function_name,
            &bp.arguments,
            input_schema.as_ref(),
        )?;
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
        let state = ScalarExchangeState {
            func: f,
            params,
            blob,
        };
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
            substream_id: dto.substream_id.clone().map(|b| b.0).unwrap_or_default(),
            init_opaque: dto
                .bind_opaque_data
                .clone()
                .map(|b| b.0)
                .unwrap_or_default(),
            pushdown_filters: dto
                .pushdown_filters
                .clone()
                .map(|b| b.0)
                .unwrap_or_default(),
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
            // Folded into the blob so a rehydrated HTTP tick keeps the client's
            // per-substream identity (empty = the client sent none).
            substream_id: Some(blob.substream_id.clone()).filter(|v| !v.is_empty()),
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
            // COPY-FROM producers drain fully (no HTTP continuation token is
            // issued), so a resumed stream never carries copy_from context.
            copy_from: None,
        };
        if blob.kind == "table" {
            let f = self.resolve_table(&blob.function_name, &args, input_schema.as_ref())?;
            args.remap_positional(&f.argument_specs());
            let params = make_params(args);
            let filters = if blob.auto_apply {
                params
                    .pushdown_filters
                    .as_ref()
                    .map(|b| {
                        crate::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys)
                    })
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
                TableProducerState {
                    inner: producer,
                    filters,
                    project_to,
                    resume_blob: Some(bytes.to_vec()),
                    conditional_checked: false,
                },
            )));
        }
        if blob.kind == "table_in_out" {
            let f = self.resolve_table_in_out(&blob.function_name, &args, input_schema.as_ref())?;
            args.remap_positional(&f.argument_specs());
            let params = make_params(args);
            let filters = if blob.auto_apply {
                params
                    .pushdown_filters
                    .as_ref()
                    .map(|b| {
                        crate::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys)
                    })
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

    /// The catalog active for a request, decoded from its `attach_opaque_data`
    /// (a secondary catalog when the secondary marker is present, else the
    /// primary). See [`decode_secondary_opaque`].
    fn active_catalog<'a>(&'a self, req: &Request) -> &'a catalog::CatalogModel {
        if let Some((name, _)) = read_binary_col(req, "attach_opaque_data")
            .as_deref()
            .and_then(decode_secondary_opaque)
        {
            if let Some(c) = self.secondary.iter().find(|c| c.name == name) {
                return c;
            }
        }
        &self.catalog
    }

    /// Schema names exposed by a specific catalog model (always includes `main`).
    fn catalog_schema_names(cat: &catalog::CatalogModel) -> Vec<String> {
        let mut names: Vec<String> = cat.schemas.iter().map(|s| s.name.clone()).collect();
        if !names.iter().any(|n| n == catalog::MAIN_SCHEMA) {
            names.insert(0, catalog::MAIN_SCHEMA.to_string());
        }
        names
    }

    /// `catalog_catalogs` — discovery: advertise this worker's catalog plus
    /// its version metadata so clients can inspect before attaching.
    pub fn handle_catalog_catalogs(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        let mut items = vec![Bytes::from(catalog::serialize_catalog_info(&self.catalog)?)];
        for sec in &self.secondary {
            items.push(Bytes::from(catalog::serialize_catalog_info(sec)?));
        }
        Ok(Some(wire::to_result_batch(ItemsResult { items })?))
    }

    pub fn handle_catalog_attach(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: CatalogAttachRequest = boxed(req)?;
        // Secondary (MetaWorker) catalog: attached by its name, with a random
        // per-session scope id carried back on every request as the storage
        // scope (so two ATTACH sessions of the same catalog stay isolated).
        if let Some(sec) = self.secondary.iter().find(|c| c.name == dto.name) {
            let scope = self.next_execution_id();
            let result = CatalogAttachResult {
                attach_opaque_data: Bytes::from(encode_secondary_opaque(&sec.name, &scope)),
                supports_transactions: true,
                supports_time_travel: sec.supports_time_travel,
                catalog_version_frozen: false,
                catalog_version: 1,
                attach_opaque_data_required: true,
                default_schema: catalog::MAIN_SCHEMA.to_string(),
                settings: Vec::new(),
                secret_types: Vec::new(),
                attach_catalogs: Vec::new(),
                comment: sec.comment.clone(),
                tags: sec.tags.clone(),
                supports_column_statistics: false,
                resolved_data_version: sec.data_version_spec.clone(),
                resolved_implementation_version: sec.implementation_version.clone(),
            };
            return Ok(Some(wire::to_result_batch(result)?));
        }
        // Version negotiation: validate the requested versions against what this
        // worker serves, then echo the resolved concrete versions back.
        let (resolved_data_version, resolved_implementation_version) =
            self.resolve_versions(&dto)?;
        // Version-shaped catalogs encode the resolved data version into the
        // attach_opaque_data (`<version>\0<id>`) so per-request catalog handlers
        // can select the right object set without server-side session state.
        let attach_opaque_data =
            if let Some(default_bytes) = &self.catalog.attach_options_default_batch {
                // Merge the user-supplied options over the declared defaults and
                // encode the one-row result as `<16-byte id>\0<ipc batch>`.
                let default_batch = ipc::read_batch(default_bytes)?;
                let options = dto
                    .options
                    .as_ref()
                    .map(|b| ipc::read_batch(&b.0))
                    .transpose()?;
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
                let mut v: Vec<u8> = id
                    .iter()
                    .copied()
                    .chain(std::iter::repeat(0))
                    .take(16)
                    .collect();
                v.push(0);
                v.extend_from_slice(&ipc::write_batch(&merged)?);
                v
            } else if !self.catalog.version_schemas.is_empty() {
                let mut v = resolved_data_version
                    .clone()
                    .unwrap_or_default()
                    .into_bytes();
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
            attach_catalogs: self
                .attach_catalogs
                .iter()
                .map(|c| Ok(Bytes::from(catalog::serialize_attach_catalog(c)?)))
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
        let resolved_impl =
            if cat.npm_version_resolution && !cat.supported_implementation_versions.is_empty() {
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
        let cat = self.active_catalog(req);
        if std::ptr::eq(cat, &self.catalog) {
            let v = self.req_version(req);
            self.catalog
                .schemas_for(v.as_deref())
                .iter()
                .find(|s| s.name == name)
        } else {
            cat.schemas.iter().find(|s| s.name == name)
        }
    }

    pub fn handle_catalog_version(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        Ok(Some(wire::to_result_batch(CatalogVersionResult {
            version: 1,
        })?))
    }

    pub fn handle_transaction_begin(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        // A fresh id per BEGIN so transaction-scoped caches (tx_cached_value)
        // don't leak across transactions; in autocommit DuckDB passes None.
        Ok(Some(wire::to_result_batch(
            CatalogTransactionBeginResult {
                transaction_opaque_data: Some(Bytes::from(self.next_execution_id())),
            },
        )?))
    }

    fn schema_info_for(&self, cat: &catalog::CatalogModel, name: &str) -> SchemaInfo {
        let comment = cat.schema(name).and_then(|s| s.comment.as_deref()).or(
            if name == catalog::MAIN_SCHEMA {
                Some("Default schema containing all registered functions")
            } else {
                None
            },
        );
        let is_primary = std::ptr::eq(cat, &self.catalog);
        let attach = if is_primary {
            self.attach_bytes()
        } else {
            cat.name.as_bytes().to_vec()
        };
        let mut si = catalog::schema_info(name, comment, &attach);
        // Schema-level tags (e.g. vgi.description_llm / vgi.description_md) come
        // from the declarative CatSchema, surfaced via duckdb_schemas().tags.
        si.tags = cat.schema(name).map(|s| s.tags.clone()).unwrap_or_default();
        // Object counts come from the (primary) worker-global function
        // registries, so only advertise them for the primary, non-version-shaped
        // catalog. Version-shaped catalogs vary their object set per attach, and
        // a secondary's functions aren't counted here — let discovery RPCs run.
        if !is_primary || !self.catalog.version_schemas.is_empty() {
            return si;
        }
        // Advertise per-kind object counts so the C++ extension caches
        // `kind_empty` and skips the bulk discovery RPC for empty kinds.
        let sch = cat.schema(name);
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
            (
                "macro".into(),
                len(sch.map(|s| s.macros.len()).unwrap_or(0)),
            ),
            (
                "table".into(),
                len(sch.map(|s| s.tables.len()).unwrap_or(0)),
            ),
            ("scalar_function".into(), sf),
            ("aggregate_function".into(), af),
            ("table_function".into(), tf),
            ("index".into(), 0),
        ]);
        si
    }

    pub fn handle_catalog_schemas(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let cat = self.active_catalog(req);
        let infos: Vec<SchemaInfo> = Self::catalog_schema_names(cat)
            .iter()
            .map(|n| self.schema_info_for(cat, n))
            .collect();
        let items = catalog::serialize_items(infos)?;
        Ok(Some(wire::to_result_batch(ItemsResult { items })?))
    }

    pub fn handle_schema_get(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let p: CatalogSchemaNameParams = wire::from_batch(&req.batch)?;
        let cat = self.active_catalog(req);
        let items = if Self::catalog_schema_names(cat).iter().any(|n| n == &p.name) {
            catalog::serialize_items(vec![self.schema_info_for(cat, &p.name)])?
        } else {
            Vec::new()
        };
        Ok(Some(wire::to_result_batch(ItemsResult { items })?))
    }

    pub fn handle_contents_views(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let name = read_string_col(req, "name")?;
        let infos: Vec<ViewInfo> = self
            .schema_for_req(req, &name)
            .map(|s| {
                s.views
                    .iter()
                    .map(|v| catalog::view_info(&name, v))
                    .collect()
            })
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
        Ok(Some(wire::to_result_batch(catalog::scan_function_result(
            &t,
        )?)?))
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
    pub fn handle_table_function_cardinality(
        &self,
        req: &Request,
        ctx: &CallContext,
    ) -> Result<Option<RecordBatch>> {
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
    pub fn handle_table_function_dynamic_to_string(
        &self,
        req: &Request,
    ) -> Result<Option<RecordBatch>> {
        use crate::protocol::dtos::{DynamicToStringRequest, DynamicToStringResponse};
        let dto: DynamicToStringRequest = boxed(req)?;
        let bind_call: BindRequest = wire::from_batch(&ipc::read_batch(&dto.bind_call.0)?)?;
        let pairs = self
            .tables
            .get(&bind_call.function_name)
            .and_then(|v| v.first())
            .map(|f| f.dynamic_to_string(&dto.global_execution_id.0, self.store.as_ref()))
            .unwrap_or_default();
        let (keys, values): (Vec<String>, Vec<String>) = pairs.into_iter().unzip();
        Ok(Some(wire::to_result_batch(DynamicToStringResponse {
            keys,
            values,
        })?))
    }

    /// Per-call statistics for a function-backed table scan (e.g. `sequence`).
    pub fn handle_table_function_statistics(
        &self,
        req: &Request,
        ctx: &CallContext,
    ) -> Result<Option<RecordBatch>> {
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
                        source_catalog: d.source_catalog.clone(),
                        source_schema: d.source_schema.clone(),
                        source_table: d.source_table.clone(),
                    })
                })
                .collect::<Result<_>>()?,
            // Single-source default: one branch wrapping the scan function.
            None => vec![mk(ScanBranch {
                function_name: t.scan_function.clone(),
                arguments: Bytes::from(t.scan_arguments.clone()),
                branch_filter: None,
                writable: false,
                source_catalog: None,
                source_schema: None,
                source_table: None,
            })?],
        };
        Ok(Some(wire::to_result_batch(ScanBranchesResult {
            branches,
            required_extensions: t.required_extensions.clone(),
        })?))
    }

    pub fn handle_contents_macros(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let name = read_string_col(req, "name")?;
        let want = normalize_function_type(&read_string_col(req, "type").unwrap_or_default());
        let infos: Vec<MacroInfo> = self
            .schema_for_req(req, &name)
            .map(|s| {
                s.macros
                    .iter()
                    .filter(|m| match want.as_deref() {
                        // The C++ extension scans scalar and table macros via two
                        // separate RPCs (`type=SCALAR_MACRO` / `TABLE_MACRO`).
                        // Match both the `_macro`-suffixed wire values and the
                        // bare `scalar`/`table` forms so each kind is returned
                        // exactly once (returning all on a kind-scoped request
                        // double-counts every macro across the two RPCs).
                        Some("table") | Some("table_macro") => m.table_macro,
                        Some("scalar") | Some("scalar_macro") => !m.table_macro,
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
        // Scope functions to the active catalog: a secondary advertises only the
        // functions it owns; the primary hides every secondary's functions.
        let active = self.active_catalog(req);
        let active_sec_fns: Option<&[String]> = self
            .secondary
            .iter()
            .position(|c| std::ptr::eq(c, active))
            .and_then(|i| self.secondary_functions.get(i))
            .map(|v| v.as_slice());
        let all_sec_fns: std::collections::HashSet<&str> = self
            .secondary_functions
            .iter()
            .flatten()
            .map(|s| s.as_str())
            .collect();
        let visible = |name: &str| {
            if self.hidden_functions.contains(name) {
                return false;
            }
            if name.starts_with(PROJ_REPRO_PREFIX) != is_proj_repro {
                return false;
            }
            match active_sec_fns {
                Some(fns) => fns.iter().any(|f| f == name),
                None => !all_sec_fns.contains(name),
            }
        };
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
            if matches!(want.as_deref(), Some("table") | Some("table_buffering")) || want.is_none()
            {
                let mut names: Vec<&String> = self.tables.keys().filter(|n| visible(n)).collect();
                names.sort();
                for name in names {
                    for f in &self.tables[name] {
                        infos.push(catalog::table_function_info(f.as_ref())?);
                    }
                }
                let mut tio: Vec<&String> =
                    self.tableinouts.keys().filter(|n| visible(n)).collect();
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

    /// `catalog_copy_from_formats` — advertise the worker's custom
    /// `COPY ... FROM` formats. Catalog-level (not schema-scoped). Only the
    /// primary catalog owns these formats; secondaries advertise none.
    pub fn handle_catalog_copy_from_formats(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let active = self.active_catalog(req);
        let items = if std::ptr::eq(active, &self.catalog) {
            let mut infos: Vec<CopyFromFormatInfo> = self
                .copy_from_formats
                .iter()
                .map(|f| -> Result<CopyFromFormatInfo> {
                    let meta = f.metadata();
                    let arg_schema = catalog::build_arg_schema(&f.argument_specs());
                    Ok(CopyFromFormatInfo {
                        comment: f.comment(),
                        tags: meta.tags.clone(),
                        format_name: f.format().to_string(),
                        handler: f.handler_name().to_string(),
                        options: Bytes::from(ipc::write_schema(&arg_schema)?),
                        direction: "from".to_string(),
                        ordered: false,
                        description: meta.description.clone(),
                    })
                })
                .collect::<Result<_>>()?;
            // COPY ... TO writers advertise direction="to"; an ordered writer
            // (sink_order_dependent) sets ordered=true so the extension installs
            // a single-thread sink.
            for f in &self.copy_to_formats {
                let meta = f.metadata();
                let arg_schema = catalog::build_arg_schema(&f.argument_specs());
                infos.push(CopyFromFormatInfo {
                    comment: f.comment(),
                    tags: meta.tags.clone(),
                    format_name: f.format().to_string(),
                    handler: f.handler_name().to_string(),
                    options: Bytes::from(ipc::write_schema(&arg_schema)?),
                    direction: "to".to_string(),
                    ordered: f.ordered(),
                    description: meta.description.clone(),
                });
            }
            catalog::serialize_items(infos)?
        } else {
            Vec::new()
        };
        Ok(Some(wire::to_result_batch(ItemsResult { items })?))
    }

    // -- table buffering RPCs ----------------------------------------------

    /// The bound output schema for a buffering execution, persisted by the
    /// sink init to the file-backed store (process/combine carry no schema and
    /// may run in a different pooled worker). Falls back to `default` when no
    /// schema was persisted (e.g. echo-style functions where it is unused).
    /// Resolve the bound output schema for a buffering process/combine RPC.
    ///
    /// Persisted at sink-init under `outsc`, but a process/combine RPC may land
    /// on a different pooled worker that never wrote it (launcher transport).
    /// On a store miss, recompute it deterministically by re-running the
    /// function's `on_bind` with the same arguments and input schema sink-init
    /// saw — the process batch's schema, or the persisted `insc` for combine.
    ///
    /// This never silently substitutes a possibly-wrong schema: the previous
    /// behaviour fell back to the raw input schema, which is correct only when a
    /// function's output type matches its input and silently breaks otherwise
    /// (e.g. sum_all_columns, whose DECIMAL inputs map to FLOAT64 output —
    /// `sum_column` then rejected the unexpected DECIMAL type). If no input
    /// schema is available to rebind from, fail loudly rather than guess.
    fn buffering_output_schema(
        &self,
        execution_id: &[u8],
        f: &dyn TableBufferingFunction,
        input_schema: Option<arrow_schema::SchemaRef>,
    ) -> Result<arrow_schema::SchemaRef> {
        if let Some(s) = self
            .store
            .kv_get(execution_id, b"outsc")
            .and_then(|b| ipc::read_schema(&b).ok())
        {
            return Ok(s);
        }
        let input_schema = input_schema.or_else(|| {
            self.store
                .kv_get(execution_id, b"insc")
                .and_then(|b| ipc::read_schema(&b).ok())
        });
        let Some(input_schema) = input_schema else {
            return Err(RpcError::runtime_error(
                "table-buffering: bound output schema unavailable (sink-init state \
                 not found on this worker and no input schema to rebind from)"
                    .to_string(),
            ));
        };
        let bind = f.on_bind(&BindParams {
            input_schema: Some(input_schema),
            arguments: self.buffering_arguments(execution_id, f),
            attach_opaque_data: self.store.kv_get(execution_id, b"bufattach"),
            storage: Some(self.store.clone()),
            ..Default::default()
        })?;
        Ok(bind.output_schema)
    }

    /// Replay the buffering call arguments persisted by the sink init (the
    /// process/combine RPCs carry none). Remapped onto the function's declared
    /// positions, matching what `on_bind` saw. Empty when none were persisted.
    fn buffering_arguments(
        &self,
        execution_id: &[u8],
        f: &dyn TableBufferingFunction,
    ) -> crate::arguments::Arguments {
        let mut args = self
            .store
            .kv_get(execution_id, b"bufargs")
            .and_then(|b| crate::arguments::Arguments::parse(&b).ok())
            .unwrap_or_default();
        args.remap_positional(&f.argument_specs());
        args
    }

    /// Replay the `COPY ... TO` context persisted by the sink-init (process /
    /// combine carry no bind_call). `None` for ordinary buffered functions.
    fn buffering_copy_to(&self, execution_id: &[u8]) -> Option<CopyToContext> {
        let path = self.store.kv_get(execution_id, b"copytopath")?;
        let format = self
            .store
            .kv_get(execution_id, b"copytofmt")
            .unwrap_or_default();
        Some(CopyToContext {
            format: String::from_utf8_lossy(&format).into_owned(),
            file_path: String::from_utf8_lossy(&path).into_owned(),
        })
    }

    /// Replay the source (input) schema persisted by the sink-init.
    fn buffering_input_schema(&self, execution_id: &[u8]) -> Option<arrow_schema::SchemaRef> {
        self.store
            .kv_get(execution_id, b"insc")
            .and_then(|b| ipc::read_schema(&b).ok())
    }

    /// Replay the resolved secrets persisted by the sink-init (process / combine
    /// carry no bind_call). Empty when none were forwarded. This is what surfaces
    /// CREATE SECRET credentials on a COPY-TO writer's write()/close() params.
    fn buffering_secrets(&self, execution_id: &[u8]) -> crate::secrets::Secrets {
        self.store
            .kv_get(execution_id, b"bufsecrets")
            .and_then(|b| crate::secrets::Secrets::parse(&b).ok())
            .unwrap_or_default()
    }

    pub fn handle_buffering_process(
        &self,
        req: &Request,
        ctx: &CallContext,
    ) -> Result<Option<RecordBatch>> {
        let dto: TableBufferingProcessRequest = boxed(req)?;
        let f = self.resolve_buffering(&dto.function_name)?;
        let batch = ipc::read_batch(&dto.input_batch.0)?;
        let output_schema =
            self.buffering_output_schema(&dto.execution_id.0, f.as_ref(), Some(batch.schema()))?;
        let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let params = BufferingParams {
            execution_id: dto.execution_id.0.clone(),
            storage: self.store.clone(),
            output_schema,
            arguments: self.buffering_arguments(&dto.execution_id.0, f.as_ref()),
            settings: crate::settings::Settings::default(),
            // Resolved secrets replayed from the sink-init (the process RPC carries
            // no bind_call) — lets a COPY-TO writer read CREATE SECRET creds.
            secrets: self.buffering_secrets(&dto.execution_id.0),
            attach_opaque_data: self.store.kv_get(&dto.execution_id.0, b"bufattach"),
            batch_index: dto.batch_index,
            copy_to: self.buffering_copy_to(&dto.execution_id.0),
            input_schema: self.buffering_input_schema(&dto.execution_id.0),
            logs: logs.clone(),
        };
        let state_id = f.process(&params, &batch)?;
        Self::drain_buffering_logs(&logs, ctx);
        Ok(Some(wire::to_result_batch(
            TableBufferingProcessResponse {
                state_id: Bytes::from(state_id),
            },
        )?))
    }

    /// Forward queued buffering INFO logs to the call context (→ duckdb_logs()).
    fn drain_buffering_logs(
        logs: &std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        ctx: &CallContext,
    ) {
        if let Ok(mut g) = logs.lock() {
            for msg in g.drain(..) {
                ctx.client_log(vgi_rpc::LogLevel::Info, msg);
            }
        }
    }

    pub fn handle_buffering_combine(
        &self,
        req: &Request,
        ctx: &CallContext,
    ) -> Result<Option<RecordBatch>> {
        let dto: TableBufferingCombineRequest = boxed(req)?;
        let f = self.resolve_buffering(&dto.function_name)?;
        let output_schema = self.buffering_output_schema(&dto.execution_id.0, f.as_ref(), None)?;
        let logs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let params = BufferingParams {
            execution_id: dto.execution_id.0.clone(),
            storage: self.store.clone(),
            output_schema,
            arguments: self.buffering_arguments(&dto.execution_id.0, f.as_ref()),
            settings: crate::settings::Settings::default(),
            // Resolved secrets replayed from the sink-init (the combine RPC carries
            // no bind_call) — lets a COPY-TO writer's close() read CREATE SECRET creds.
            secrets: self.buffering_secrets(&dto.execution_id.0),
            attach_opaque_data: self.store.kv_get(&dto.execution_id.0, b"bufattach"),
            batch_index: None,
            copy_to: self.buffering_copy_to(&dto.execution_id.0),
            input_schema: self.buffering_input_schema(&dto.execution_id.0),
            logs: logs.clone(),
        };
        let state_ids: Vec<Vec<u8>> = dto.state_ids.into_iter().map(|b| b.0).collect();
        let finalize_ids = f.combine(&params, &state_ids)?;
        Self::drain_buffering_logs(&logs, ctx);
        Ok(Some(wire::to_result_batch(
            TableBufferingCombineResponse {
                finalize_state_ids: finalize_ids.into_iter().map(Bytes::from).collect(),
            },
        )?))
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

    pub fn handle_aggregate_bind(
        &self,
        req: &Request,
        ctx: &CallContext,
    ) -> Result<Option<RecordBatch>> {
        let dto: AggregateBindRequest = boxed(req)?;
        let mut args = crate::arguments::Arguments::parse(&dto.arguments.0)?;
        let input_schema = opt_schema(&dto.input_schema)?;
        let f = self.resolve_aggregate(&dto.function_name)?;
        args.remap_positional(&f.argument_specs());
        // Enforce declared const-argument constraints at bind (parity with the
        // scalar/table path); a violating value fails the aggregate_bind.
        crate::function::validate_arg_constraints(&f.argument_specs(), &args)?;
        let _ = ctx;
        let params = AggregateBindParams {
            arguments: args,
            input_schema,
            settings: parse_settings(&dto.settings)?,
            // The C++ pre-resolves any advertised required secret and delivers
            // it here (bind-time only). Reuses the same parser as the table
            // bind path.
            secrets: parse_secrets(&dto.secrets)?,
        };
        let bind = f.on_bind(&params)?;
        let execution_id = self.next_execution_id();
        // Stash the raw bind-time arguments so `finalize` can rebuild const
        // params (e.g. `vgi_percentile`'s percentile) — update/finalize RPCs
        // don't resend arguments and may run in a different pooled worker.
        self.store
            .kv_put(&execution_id, b"aggargs", &dto.arguments.0);
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
            if let std::collections::hash_map::Entry::Vacant(e) = states.entry(gid) {
                if let Some(s) = self.store.kv_get(&dto.execution_id.0, &Self::agg_key(gid)) {
                    e.insert(s);
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
            .map(|i| {
                self.store
                    .kv_get(&dto.execution_id.0, &Self::agg_key(gids.value(i)))
            })
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
        self.store.kv_put(
            &dto.execution_id.0,
            &Self::win_key(dto.partition_id, "p"),
            &dto.partition_batch.0,
        );
        self.store.kv_put(
            &dto.execution_id.0,
            &Self::win_key(dto.partition_id, "o"),
            &dto.output_schema.0,
        );
        if let Some(m) = &dto.filter_mask {
            self.store.kv_put(
                &dto.execution_id.0,
                &Self::win_key(dto.partition_id, "m"),
                &m.0,
            );
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
            .ok_or_else(|| {
                RpcError::runtime_error(format!(
                    "aggregate_window: unknown partition_id={partition_id}"
                ))
            })?;
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
                    .map(|i| {
                        bytes
                            .get(i / 8)
                            .map(|byte| byte & (1 << (i % 8)) != 0)
                            .unwrap_or(true)
                    })
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
            self.store
                .kv_del(&dto.execution_id.0, &Self::win_key(dto.partition_id, sfx));
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

    /// Decode the length-prefixed state map written by [`Self::ser_state_map`].
    ///
    /// Defensive against truncation/corruption: this blob lives in the
    /// cross-process file store (`$TMPDIR/...`), so a partial write, a disk
    /// fault, or a stale file could hand us a malformed buffer. Every read is
    /// bounds-checked; on any short/garbage length we stop and return what
    /// parsed cleanly rather than panicking (which on stdio would otherwise be
    /// converted to an opaque "handler panicked" RPC error).
    fn de_state_map(b: &[u8]) -> std::collections::HashMap<Vec<u8>, Vec<u8>> {
        let mut m = std::collections::HashMap::new();
        // Read `n` bytes at `*off`, advancing it; None if the slice is short.
        let rd = |b: &[u8], off: &mut usize, n: usize| -> Option<Vec<u8>> {
            let end = off.checked_add(n)?;
            let s = b.get(*off..end)?.to_vec();
            *off = end;
            Some(s)
        };
        // Read an 8-byte little-endian length at `*off`, advancing it.
        let rd_len = |b: &[u8], off: &mut usize| -> Option<usize> {
            let raw = rd(b, off, 8)?;
            let arr: [u8; 8] = raw.try_into().ok()?;
            Some(u64::from_le_bytes(arr) as usize)
        };
        let mut off = 0usize;
        let Some(count) = rd_len(b, &mut off) else {
            return m;
        };
        for _ in 0..count {
            let Some(kl) = rd_len(b, &mut off) else { break };
            let Some(k) = rd(b, &mut off, kl) else { break };
            let Some(vl) = rd_len(b, &mut off) else { break };
            let Some(v) = rd(b, &mut off, vl) else { break };
            m.insert(k, v);
        }
        m
    }

    pub fn handle_aggregate_streaming_open(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateStreamingOpenRequest = boxed(req)?;
        self.resolve_aggregate(&dto.function_name)?;
        let execution_id = self.next_execution_id();
        self.store.kv_put(
            &execution_id,
            b"strm_pkc",
            &dto.partition_key_count.to_le_bytes(),
        );
        self.store.kv_put(
            &execution_id,
            b"strm_okc",
            &dto.order_key_count.to_le_bytes(),
        );
        self.store
            .kv_put(&execution_id, b"strm_sos", &dto.output_schema.0);
        Ok(Some(wire::to_result_batch(
            AggregateStreamingOpenResponse {
                execution_id: Bytes::from(execution_id),
            },
        )?))
    }

    pub fn handle_aggregate_streaming_chunk(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateStreamingChunkRequest = boxed(req)?;
        let f = self.resolve_aggregate(&dto.function_name)?;
        let chunk = ipc::read_batch(&dto.input_batch.0)?;
        let pkc = self
            .store
            .kv_get(&dto.execution_id.0, b"strm_pkc")
            .and_then(|b| read_le_i64(&b))
            .unwrap_or(0) as usize;
        let okc = self
            .store
            .kv_get(&dto.execution_id.0, b"strm_okc")
            .and_then(|b| read_le_i64(&b))
            .unwrap_or(0) as usize;
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
        self.store.kv_put(
            &dto.execution_id.0,
            b"strm_state",
            &Self::ser_state_map(&states),
        );
        let schema = output_schema.unwrap_or_else(|| {
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "result",
                col.data_type().clone(),
                true,
            )]))
        });
        let batch = RecordBatch::try_new(schema, vec![col])
            .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        Ok(Some(wire::to_result_batch(
            AggregateStreamingChunkResponse {
                result_batch: Bytes::from(ipc::write_batch(&batch)?),
            },
        )?))
    }

    pub fn handle_aggregate_streaming_close(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateStreamingCloseRequest = boxed(req)?;
        for k in [
            b"strm_pkc".as_slice(),
            b"strm_okc",
            b"strm_sos",
            b"strm_state",
        ] {
            self.store.kv_del(&dto.execution_id.0, k);
        }
        Ok(Some(wire::empty_result_batch()?))
    }

    /// Empty `ItemsResult` for the contents/get methods not yet implemented.
    pub fn handle_empty_items(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        Ok(Some(wire::to_result_batch(ItemsResult {
            items: Vec::new(),
        })?))
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
    /// Client-minted per-substream id (empty = none) — folded in so a resumed
    /// HTTP tick keeps [`ProcessParams::substream_id`]. See
    /// `InitRequest::substream_id`.
    pub substream_id: Vec<u8>,
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
        let mut batches = self.func.process(&self.params, input)?;
        // 1:1 lockstep: the client reads exactly ONE output batch per input
        // batch, so an accumulate-only tick (process emitted nothing, e.g.
        // substream_partial_sum) still answers with a 0-row batch — parity
        // with the Python SDK's `empty_batch` padding. Without it the client
        // blocks forever on ReadDataBatch.
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(out.schema()));
        }
        for batch in batches {
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

/// Read one conditional-revalidation validator, preferring this tick's metadata
/// (subprocess) and falling back to the request metadata (HTTP `init`). An
/// empty string clears the key, matching the C++ client's "unset" encoding.
fn cond_validator(ctx: &CallContext, key: &str) -> Option<String> {
    ctx.tick_metadata(key)
        .or_else(|| ctx.transport_metadata.get(key).cloned())
        .filter(|v| !v.is_empty())
}

/// Adapter from a [`TableProducer`] to a vgi-rpc [`ProducerState`]. Applies
/// auto-filter pushdown to each batch before emitting.
struct TableProducerState {
    inner: Box<dyn TableProducer>,
    filters: Option<crate::pushdown::PushdownFilters>,
    /// When set, narrow each (post-filter) batch to this projected schema —
    /// the producer emitted the full schema so filters could see all columns.
    project_to: Option<arrow_schema::SchemaRef>,
    /// Rebuild blob for resuming this producer from an HTTP state token. `None`
    /// for producers that can't be rebuilt from bind params (buffering/finalize
    /// flushes), which always drain in one response.
    resume_blob: Option<Vec<u8>>,
    /// Whether the conditional-revalidation validators have been looked for yet.
    /// They only ever ride the first tick, so checking once keeps the per-batch
    /// hot path free of the two `tick_metadata` mutex acquisitions.
    conditional_checked: bool,
}

impl vgi_rpc::ProducerState for TableProducerState {
    fn produce(&mut self, out: &mut OutputCollector, ctx: &CallContext) -> Result<()> {
        // Per-tick dynamic filter (e.g. a tightening Top-N) arrives in the
        // request metadata; surface it to the producer and auto-apply it.
        let dynamic = ctx
            .tick_metadata("vgi_pushdown_filters")
            .and_then(|enc| crate::pushdown::PushdownFilters::parse_b64(&enc, &[]));
        self.inner.on_dynamic_filters(dynamic.as_ref());
        // Conditional-revalidation validators. The client sends them on the
        // FIRST producer tick over subprocess, and folds them into the `init`
        // request over HTTP (where there is no tick before the first batch) — so
        // look in both places, and only once: a later tick never carries them.
        if !self.conditional_checked {
            self.conditional_checked = true;
            let conditional = crate::cache_control::ConditionalRequest {
                if_none_match: cond_validator(ctx, crate::cache_control::CACHE_IF_NONE_MATCH_KEY),
                if_modified_since: cond_validator(
                    ctx,
                    crate::cache_control::CACHE_IF_MODIFIED_SINCE_KEY,
                ),
            };
            if conditional.is_conditional() {
                self.inner.on_conditional_request(&conditional);
            }
        }
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
        // Paginate (yield after the server-default batch count, i.e. one batch
        // per HTTP response — matching the Python/Go workers) only when we can
        // both rebuild the producer from a token AND the producer serializes its
        // scan position. Otherwise drain fully (`Some(0)` = unlimited) so a
        // producer never silently restarts from row 0 on resume.
        if self.resume_blob.is_some() && self.inner.resume_supported() {
            None
        } else {
            Some(0)
        }
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
/// Read a little-endian `i64` from the first 8 bytes of a store value, or
/// `None` if the buffer is shorter. Store blobs come off disk and could be
/// truncated/corrupt, so slicing `b[..8]` directly would risk a panic.
fn read_le_i64(b: &[u8]) -> Option<i64> {
    let arr: [u8; 8] = b.get(..8)?.try_into().ok()?;
    Some(i64::from_le_bytes(arr))
}

/// Decode the IPC batch carried in the request's `request` binary column.
fn request_inner_batch(req: &Request) -> Result<RecordBatch> {
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
    ipc::read_batch(ba.value(0))
}

fn boxed<T: VgiArrow>(req: &Request) -> Result<T> {
    let batch = request_inner_batch(req)?;
    if std::env::var("VGI_WIRE_DEBUG").is_ok() {
        eprintln!(
            "[vgi-wire] {} inner schema: {:?}",
            req.method,
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| format!("{}:{}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
        );
    }
    wire::from_batch::<T>(&batch)
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

/// Marker prefix for a secondary-catalog `attach_opaque_data`:
/// `\x00sec\x00<name>\x00<scope>`. The leading NUL distinguishes it from a
/// primary catalog (plaintext name) and a version-shaped one (`<version>\0…`,
/// whose version is non-empty so its first byte is never NUL).
const SEC_MARKER: &[u8] = b"\x00sec\x00";

/// Encode a secondary-catalog attach blob from its name + per-session scope id.
fn encode_secondary_opaque(name: &str, scope: &[u8]) -> Vec<u8> {
    let mut v = SEC_MARKER.to_vec();
    v.extend_from_slice(name.as_bytes());
    v.push(0);
    v.extend_from_slice(scope);
    v
}

/// Decode a secondary-catalog attach blob into `(catalog_name, scope)`, or
/// `None` when the marker is absent (a primary/version-shaped catalog).
fn decode_secondary_opaque(bytes: &[u8]) -> Option<(String, Vec<u8>)> {
    let rest = bytes.strip_prefix(SEC_MARKER)?;
    let sep = rest.iter().position(|&b| b == 0)?;
    let name = String::from_utf8(rest[..sep].to_vec()).ok()?;
    Some((name, rest[sep + 1..].to_vec()))
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

#[cfg(test)]
mod buffering_schema_tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};

    // A buffering function whose on_bind maps ANY input to a fixed FLOAT64 `s`
    // column — output type deliberately differs from input, the case the old
    // raw-input-schema fallback silently got wrong (sum_all_columns over DECIMAL).
    struct FixedOutput;
    impl crate::buffering::TableBufferingFunction for FixedOutput {
        fn name(&self) -> &str {
            "fixed_output"
        }
        fn metadata(&self) -> crate::function::FunctionMetadata {
            Default::default()
        }
        fn argument_specs(&self) -> Vec<crate::function::ArgSpec> {
            vec![]
        }
        fn on_bind(&self, _p: &BindParams) -> Result<crate::function::BindResponse> {
            Ok(crate::function::BindResponse {
                output_schema: Arc::new(Schema::new(vec![Field::new(
                    "s",
                    DataType::Float64,
                    true,
                )])),
                opaque_data: Vec::new(),
            })
        }
        fn process(
            &self,
            _p: &crate::buffering::BufferingParams,
            _b: &arrow_array::RecordBatch,
        ) -> Result<Vec<u8>> {
            unimplemented!()
        }
        fn combine(
            &self,
            _p: &crate::buffering::BufferingParams,
            _s: &[Vec<u8>],
        ) -> Result<Vec<Vec<u8>>> {
            unimplemented!()
        }
        fn finalize_producer(
            &self,
            _p: &crate::buffering::BufferingParams,
            _f: Vec<u8>,
        ) -> Result<Box<dyn crate::table_function::TableProducer>> {
            unimplemented!()
        }
    }

    // On a store miss the output schema must be recomputed via on_bind from the
    // input schema (FLOAT64 `s`), NOT fall back to the raw DECIMAL input.
    #[test]
    fn output_schema_recomputed_on_store_miss() {
        let d = Dispatcher::new("test");
        let exec = format!("test-recompute-{}", std::process::id()).into_bytes();
        d.store.clear(&exec); // ensure no `outsc`/`insc` from a prior run
        let decimal_input = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let out = d
            .buffering_output_schema(&exec, &FixedOutput, Some(decimal_input))
            .expect("recompute via on_bind");
        assert_eq!(out.fields().len(), 1);
        assert_eq!(out.field(0).data_type(), &DataType::Float64);
    }

    // No stored schema and no input to rebind from → fail loudly, never guess.
    #[test]
    fn output_schema_errors_without_any_input() {
        let d = Dispatcher::new("test");
        let exec = format!("test-error-{}", std::process::id()).into_bytes();
        d.store.clear(&exec);
        assert!(d
            .buffering_output_schema(&exec, &FixedOutput, None)
            .is_err());
    }
}

// Defensive-decoding tests: the streaming state blob and the small int store
// values live in the on-disk cross-process store, so a truncated write or a
// corrupt file must degrade to a default — never panic (which on stdio becomes
// an opaque "handler panicked" error, and over HTTP a bare 500).
#[cfg(test)]
mod malformed_input_tests {
    use super::*;

    #[test]
    fn de_state_map_roundtrips() {
        let mut m = std::collections::HashMap::new();
        m.insert(b"k1".to_vec(), b"value-one".to_vec());
        m.insert(b"".to_vec(), b"".to_vec());
        m.insert(vec![0xff, 0x00, 0xfe], vec![1, 2, 3, 4]);
        let enc = Dispatcher::ser_state_map(&m);
        assert_eq!(Dispatcher::de_state_map(&enc), m);
    }

    #[test]
    fn de_state_map_tolerates_truncation_at_every_offset() {
        let mut m = std::collections::HashMap::new();
        m.insert(b"alpha".to_vec(), b"beta".to_vec());
        m.insert(b"gamma".to_vec(), b"delta".to_vec());
        let enc = Dispatcher::ser_state_map(&m);
        // Cutting the buffer at any length must not panic; it returns whatever
        // prefix decoded cleanly (a subset of the original entries).
        for n in 0..=enc.len() {
            let got = Dispatcher::de_state_map(&enc[..n]);
            for (k, v) in &got {
                assert_eq!(
                    m.get(k),
                    Some(v),
                    "decoded a key/value that was never encoded"
                );
            }
        }
    }

    #[test]
    fn de_state_map_rejects_garbage_lengths() {
        // count = 1, then a key length of u64::MAX with no payload.
        let mut bad = Vec::new();
        bad.extend_from_slice(&1u64.to_le_bytes());
        bad.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(Dispatcher::de_state_map(&bad).is_empty());

        // Random short buffers of every small length must not panic.
        for len in 0..20usize {
            let buf: Vec<u8> = (0..len).map(|i| (i as u8).wrapping_mul(37)).collect();
            let _ = Dispatcher::de_state_map(&buf);
        }
    }

    #[test]
    fn read_le_i64_is_bounds_safe() {
        assert_eq!(read_le_i64(&7i64.to_le_bytes()), Some(7));
        assert_eq!(read_le_i64(&(-1i64).to_le_bytes()), Some(-1));
        // A value longer than 8 bytes reads the first 8.
        let mut long = 42i64.to_le_bytes().to_vec();
        long.extend_from_slice(b"trailing");
        assert_eq!(read_le_i64(&long), Some(42));
        // Anything shorter than 8 bytes is None, not a panic.
        for n in 0..8usize {
            assert_eq!(read_le_i64(&vec![0u8; n]), None);
        }
        assert_eq!(read_le_i64(&[]), None);
    }
}

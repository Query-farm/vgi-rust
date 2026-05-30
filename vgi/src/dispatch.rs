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
            exec_counter: AtomicU64::new(1),
        }
    }

    pub fn set_catalog(&mut self, model: catalog::CatalogModel) {
        self.catalog = model;
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
            order_by_column: dto.order_by_column_name.clone(),
            order_by_direction: dto.order_by_direction.clone().map(|d| d.0),
            order_by_null_order: dto.order_by_null_order.clone().map(|d| d.0),
            order_by_limit: dto.order_by_limit,
            tablesample_percentage: dto.tablesample_percentage,
            tablesample_seed: dto.tablesample_seed,
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
                let state = TableProducerState { inner: producer, filters, project_to: None };
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
            let state = TableProducerState {
                inner: Box::new(EmptyProducer),
                filters: None,
                project_to: None,
            };
            return Ok(StreamResult::producer(output_schema, Box::new(state)).with_header(header));
        }

        // Table-in-out (exchange) path.
        if self.tableinouts.contains_key(&bind_call.function_name) {
            let f = self.resolve_table_in_out(&bind_call.function_name, &bp.arguments, input_schema.as_ref())?;
            bp.arguments.remap_positional(&f.argument_specs());
            let auto_apply = f.metadata().auto_apply_filters;
            let params = build_params(bp.arguments, bp.settings, bp.secrets, bp.auth_principal);
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
            let header = wire::to_batch(GlobalInitResponse {
                execution_id: Bytes::from(execution_id),
                max_workers,
                opaque_data: None,
            })?;
            let state = TableProducerState { inner: producer, filters, project_to };
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
            order_by_column: None,
            order_by_direction: None,
            order_by_null_order: None,
            order_by_limit: None,
            tablesample_percentage: None,
            tablesample_seed: None,
        };
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

    pub fn handle_catalog_attach(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let _dto: CatalogAttachRequest = boxed(req)?;
        let result = CatalogAttachResult {
            attach_opaque_data: Bytes::from(self.attach_bytes()),
            supports_transactions: true,
            supports_time_travel: false,
            catalog_version_frozen: false,
            catalog_version: 1,
            attach_opaque_data_required: true,
            default_schema: catalog::MAIN_SCHEMA.to_string(),
            settings: Vec::new(),
            secret_types: Vec::new(),
            comment: None,
            tags: Vec::new(),
            supports_column_statistics: false,
            resolved_data_version: None,
            resolved_implementation_version: None,
        };
        Ok(Some(wire::to_result_batch(result)?))
    }

    pub fn handle_catalog_version(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        Ok(Some(wire::to_result_batch(CatalogVersionResult { version: 1 })?))
    }

    pub fn handle_transaction_begin(&self, _req: &Request) -> Result<Option<RecordBatch>> {
        Ok(Some(wire::to_result_batch(CatalogTransactionBeginResult {
            transaction_opaque_data: Some(Bytes::from(b"vgi-txn".to_vec())),
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
        catalog::schema_info(name, comment, &self.attach_bytes())
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
        let infos: Vec<TableInfo> = match self.catalog.schema(&name) {
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
        let infos: Vec<TableInfo> = self
            .catalog
            .schema(&schema_name)
            .and_then(|s| s.tables.iter().find(|t| t.name == table_name))
            .map(|t| catalog::table_info(&schema_name, t))
            .transpose()?
            .into_iter()
            .collect();
        Ok(Some(wire::to_result_batch(ItemsResult {
            items: catalog::serialize_items(infos)?,
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
        let mut infos = Vec::new();
        if schema_name == catalog::MAIN_SCHEMA {
            let want = normalize_function_type(&type_filter);
            if want.as_deref() == Some("scalar") || want.is_none() {
                let mut names: Vec<&String> = self.scalars.keys().collect();
                names.sort();
                for name in names {
                    for f in &self.scalars[name] {
                        infos.push(catalog::scalar_function_info(f.as_ref())?);
                    }
                }
            }
            // Table-buffering functions also surface under a TABLE request.
            if matches!(want.as_deref(), Some("table") | Some("table_buffering")) || want.is_none() {
                let mut names: Vec<&String> = self.tables.keys().collect();
                names.sort();
                for name in names {
                    for f in &self.tables[name] {
                        infos.push(catalog::table_function_info(f.as_ref())?);
                    }
                }
                let mut tio: Vec<&String> = self.tableinouts.keys().collect();
                tio.sort();
                for name in tio {
                    for f in &self.tableinouts[name] {
                        infos.push(catalog::table_in_out_function_info(f.as_ref())?);
                    }
                }
                let mut buf: Vec<&String> = self.buffering.keys().collect();
                buf.sort();
                for name in buf {
                    for f in &self.buffering[name] {
                        infos.push(catalog::buffering_function_info(f.as_ref())?);
                    }
                }
            }
            if matches!(want.as_deref(), Some("aggregate")) || want.is_none() {
                let mut agg: Vec<&String> = self.aggregates.keys().collect();
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

    pub fn handle_buffering_process(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: TableBufferingProcessRequest = boxed(req)?;
        let f = self.resolve_buffering(&dto.function_name)?;
        let batch = ipc::read_batch(&dto.input_batch.0)?;
        let output_schema = self.buffering_output_schema(&dto.execution_id.0, batch.schema());
        let params = BufferingParams {
            execution_id: dto.execution_id.0.clone(),
            storage: self.store.clone(),
            output_schema,
            arguments: crate::arguments::Arguments::default(),
            settings: crate::settings::Settings::default(),
            batch_index: dto.batch_index,
        };
        let state_id = f.process(&params, &batch)?;
        Ok(Some(wire::to_result_batch(TableBufferingProcessResponse {
            state_id: Bytes::from(state_id),
        })?))
    }

    pub fn handle_buffering_combine(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: TableBufferingCombineRequest = boxed(req)?;
        let f = self.resolve_buffering(&dto.function_name)?;
        let output_schema =
            self.buffering_output_schema(&dto.execution_id.0, Arc::new(arrow_schema::Schema::empty()));
        let params = BufferingParams {
            execution_id: dto.execution_id.0.clone(),
            storage: self.store.clone(),
            output_schema,
            arguments: crate::arguments::Arguments::default(),
            settings: crate::settings::Settings::default(),
            batch_index: None,
        };
        let state_ids: Vec<Vec<u8>> = dto.state_ids.into_iter().map(|b| b.0).collect();
        let finalize_ids = f.combine(&params, &state_ids)?;
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
        // Pre-load states for the distinct gids present.
        let mut states: HashMap<i64, Vec<u8>> = HashMap::new();
        for i in 0..gids.len() {
            let gid = gids.value(i);
            states.entry(gid).or_insert_with(|| {
                self.store
                    .kv_get(&dto.execution_id.0, &Self::agg_key(gid))
                    .unwrap_or_else(|| f.initial_state())
            });
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
            let source = self
                .store
                .kv_get(&dto.execution_id.0, &Self::agg_key(s))
                .unwrap_or_else(|| f.initial_state());
            let target = self
                .store
                .kv_get(&dto.execution_id.0, &Self::agg_key(t))
                .unwrap_or_else(|| f.initial_state());
            let merged = f.combine(target, source)?;
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
        let result = f.finalize(&output_schema, &gids, &states)?;
        Ok(Some(wire::to_result_batch(AggregateFinalizeResponse {
            result_batch: Bytes::from(ipc::write_batch(&result)?),
        })?))
    }

    pub fn handle_aggregate_destructor(&self, req: &Request) -> Result<Option<RecordBatch>> {
        let dto: AggregateDestructorRequest = boxed(req)?;
        self.store.clear(&dto.execution_id.0);
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
}

impl vgi_rpc::ProducerState for TableProducerState {
    fn produce(&mut self, out: &mut OutputCollector, _ctx: &CallContext) -> Result<()> {
        match self.inner.next_batch(out)? {
            None => {
                out.finish();
                Ok(())
            }
            Some(batch) => {
                let batch = match &self.filters {
                    Some(f) => f.apply(&batch)?,
                    None => batch,
                };
                let batch = match &self.project_to {
                    Some(ps) => crate::table_in_out::project_batch(&batch, ps)?,
                    None => batch,
                };
                out.emit(batch)
            }
        }
    }
    fn encode_state(&self) -> Result<Vec<u8>> {
        Ok(Vec::new())
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

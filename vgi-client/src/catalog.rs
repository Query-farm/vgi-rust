// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Attaching a catalog and discovering what it holds.

use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch, StringArray};
use arrow_schema::DataType;
use vgi_protocol::generated::request_params as p;
use vgi_protocol::protocol::dtos::{
    AttachCatalogInfo, CatalogAttachRequest, CatalogAttachResult, CatalogInfo,
    CatalogTransactionBeginResult, CatalogVersionResult, FunctionInfo, MacroInfo, ScanBranch,
    ScanBranchesResult, ScanFunctionResult, SchemaInfo, TableInfo, ViewInfo,
};
use vgi_rpc::errors::{Result, RpcError};
use vgi_rpc::{Bytes, DictString};

use crate::client::VgiClient;
use crate::wire_call::{call, call_batch, call_items, call_unit, envelope};

/// One attach-time option advertised by a catalog during discovery.
///
/// The wire carries the type as an IPC schema and the default as an optional
/// one-row IPC batch. Decoding that representation here keeps query-engine
/// integrations from each having to reproduce the protocol's nested IPC
/// format.
#[derive(Clone)]
pub struct AttachOptionSpec {
    /// Case-preserving option name.
    pub name: String,
    /// Human-readable description supplied by the catalog.
    pub description: String,
    /// Arrow type the supplied value must have.
    pub data_type: DataType,
    /// One-element default array, when the worker declares one.
    pub default_value: Option<ArrayRef>,
    /// Whether the caller must supply this option.
    pub required: bool,
}

impl std::fmt::Debug for AttachOptionSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttachOptionSpec")
            .field("name", &self.name)
            .field("description", &self.description)
            .field("data_type", &self.data_type)
            .field(
                "default_value",
                &self.default_value.as_ref().map(|_| "<redacted>"),
            )
            .field("required", &self.required)
            .finish()
    }
}

impl AttachOptionSpec {
    /// Decode one IPC-serialized `AttachOptionSpec` from `CatalogInfo`.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let batch = vgi_protocol::ipc::read_batch(bytes)?;
        if batch.num_rows() != 1 {
            return Err(vgi_rpc::errors::RpcError::type_error(format!(
                "AttachOptionSpec must contain one row, found {}",
                batch.num_rows()
            )));
        }
        let string = |name: &str| -> Result<String> {
            let array = batch
                .column_by_name(name)
                .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| {
                    vgi_rpc::errors::RpcError::type_error(format!(
                        "AttachOptionSpec.{name} is not Utf8"
                    ))
                })?;
            if array.is_null(0) {
                return Err(vgi_rpc::errors::RpcError::type_error(format!(
                    "AttachOptionSpec.{name} is null"
                )));
            }
            Ok(array.value(0).to_string())
        };
        let binary = batch
            .column_by_name("type")
            .and_then(|a| a.as_any().downcast_ref::<BinaryArray>())
            .ok_or_else(|| {
                vgi_rpc::errors::RpcError::type_error("AttachOptionSpec.type is not Binary")
            })?;
        if binary.is_null(0) {
            return Err(vgi_rpc::errors::RpcError::type_error(
                "AttachOptionSpec.type is null",
            ));
        }
        let type_schema = vgi_protocol::ipc::read_schema(binary.value(0))?;
        let field = type_schema.fields().first().ok_or_else(|| {
            vgi_rpc::errors::RpcError::type_error("AttachOptionSpec.type schema has no field")
        })?;

        let default_value = match batch.column_by_name("default_value") {
            Some(array) => {
                let values = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        vgi_rpc::errors::RpcError::type_error(
                            "AttachOptionSpec.default_value is not Binary",
                        )
                    })?;
                if values.is_null(0) {
                    None
                } else {
                    let default = vgi_protocol::ipc::read_batch(values.value(0))?;
                    if default.num_rows() != 1 || default.num_columns() != 1 {
                        return Err(vgi_rpc::errors::RpcError::type_error(
                            "AttachOptionSpec.default_value must contain one value",
                        ));
                    }
                    Some(default.column(0).clone())
                }
            }
            None => None,
        };
        let required = batch
            .column_by_name("required")
            .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
            .is_some_and(|a| !a.is_null(0) && a.value(0));

        Ok(Self {
            name: string("name")?,
            description: string("description")?,
            data_type: field.data_type().clone(),
            default_value,
            required,
        })
    }
}

/// Decode all attach-time option declarations in one catalog discovery row.
pub fn decode_attach_option_specs(info: &CatalogInfo) -> Result<Vec<AttachOptionSpec>> {
    info.attach_option_specs
        .iter()
        .map(|bytes| AttachOptionSpec::decode(&bytes.0))
        .collect()
}

/// Decode the typed, one-row default-value batch attached to a macro.
///
/// Only parameters that actually have defaults appear as columns. Keeping the
/// values as Arrow arrays lets engine adapters preserve temporal, decimal,
/// binary, and null types instead of round-tripping them through strings.
pub fn decode_macro_defaults(info: &MacroInfo) -> Result<Vec<(String, ArrayRef)>> {
    let Some(bytes) = info
        .parameter_default_values
        .as_ref()
        .filter(|bytes| !bytes.0.is_empty())
    else {
        return Ok(Vec::new());
    };
    let batch = vgi_protocol::ipc::read_batch(&bytes.0)?;
    if batch.num_rows() != 1 {
        return Err(RpcError::type_error(format!(
            "MacroInfo.parameter_default_values must contain one row, found {}",
            batch.num_rows()
        )));
    }
    let mut seen = std::collections::HashSet::new();
    let mut defaults = Vec::with_capacity(batch.num_columns());
    for (field, value) in batch.schema().fields().iter().zip(batch.columns()) {
        let parameter = info
            .parameters
            .iter()
            .find(|parameter| parameter.eq_ignore_ascii_case(field.name()))
            .ok_or_else(|| {
                RpcError::type_error(format!(
                    "macro {} default names unknown parameter {:?}",
                    info.name,
                    field.name()
                ))
            })?;
        let key = parameter.to_ascii_lowercase();
        if !seen.insert(key) {
            return Err(RpcError::type_error(format!(
                "macro {} declares duplicate default for parameter {:?}",
                info.name, parameter
            )));
        }
        defaults.push((parameter.clone(), value.clone()));
    }
    Ok(defaults)
}

/// Encode a one-row typed option batch for [`AttachOptions::options`].
pub fn encode_attach_options(batch: &RecordBatch) -> Result<Bytes> {
    if batch.num_rows() != 1 {
        return Err(vgi_rpc::errors::RpcError::value_error(format!(
            "attach options must contain exactly one row, found {}",
            batch.num_rows()
        )));
    }
    Ok(Bytes(vgi_protocol::ipc::write_batch(batch)?))
}

/// Which kind of function to list from a schema.
///
/// # Wire spelling
///
/// The `type` parameter of `catalog_schema_contents_functions` is Python's
/// `SchemaObjectType`, and an enum crosses the wire as its **member name**, not
/// its value — `vgi_rpc/rpc/_wire.py::_convert_for_arrow` is explicit about it
/// ("Enum → .name") and the reader is `base[value]`, a name lookup that raises
/// `KeyError` on anything else. So the spelling is `TABLE_FUNCTION`, never
/// `table`.
///
/// The Rust reference worker accepts both — `normalize_function_type` in
/// `vgi::dispatch` lowercases and strips a `_function` suffix — so a client that
/// sends the short form works there and fails against the canonical Python
/// worker. That leniency is why this was wrong for as long as it was.
///
/// # Why buffered and table-in-out are absent
///
/// They are not listing filters. `SchemaObjectType` has one `TABLE_FUNCTION`
/// member covering all three shapes; which shape a given function is comes back
/// on [`FunctionInfo::function_type`](crate::FunctionInfo) in the response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionKind {
    /// Any table function — producer, buffered, or streaming table-in-out.
    Table,
    /// A scalar function.
    Scalar,
    /// An aggregate function.
    Aggregate,
}

impl FunctionKind {
    /// The wire spelling: a `SchemaObjectType` member name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "TABLE_FUNCTION",
            Self::Scalar => "SCALAR_FUNCTION",
            Self::Aggregate => "AGGREGATE_FUNCTION",
        }
    }

    fn dict(self) -> DictString {
        DictString(self.as_str().to_string())
    }
}

/// Which kind of macro to list from a schema.
///
/// Same `SchemaObjectType` member-name spelling as [`FunctionKind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MacroKind {
    /// A scalar macro.
    Scalar,
    /// A table macro.
    Table,
}

impl MacroKind {
    /// The wire spelling: a `SchemaObjectType` member name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Scalar => "SCALAR_MACRO",
            Self::Table => "TABLE_MACRO",
        }
    }

    fn dict(self) -> DictString {
        DictString(self.as_str().to_string())
    }
}

/// A time-travel coordinate for a read.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct At {
    /// The unit — e.g. `"version"` or `"timestamp"`.
    pub unit: String,
    /// The value, in whatever spelling the unit implies.
    pub value: String,
}

/// How [`VgiClient::table_scan_branches`] resolved a table's physical sources.
///
/// Consumers can use this to distinguish a genuine one-branch response from a
/// legacy worker that only implements `catalog_table_scan_function_get`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanBranchesResolution {
    /// The worker answered `catalog_table_scan_branches_get`.
    BranchesRpc,
    /// The branches RPC was unavailable, so this call probed and fell back.
    LegacyFallbackAfterProbe,
    /// A previous call already established that this attach needs fallback.
    LegacyCached,
}

/// Decoded physical sources backing a catalog table.
#[derive(Debug, Clone)]
pub struct CatalogScanBranches {
    /// One branch per physical source, in the worker-declared order.
    pub branches: Vec<ScanBranch>,
    /// Extensions the host must make available before binding the branches.
    pub required_extensions: Vec<String>,
    /// Which protocol path produced this result.
    pub resolution: ScanBranchesResolution,
}

const BRANCHES_CAPABILITY_UNKNOWN: u8 = 0;
const BRANCHES_CAPABILITY_SUPPORTED: u8 = 1;
const BRANCHES_CAPABILITY_UNSUPPORTED: u8 = 2;

/// A live attach handle.
///
/// The `attach_opaque_data` blob is the worker's session token: every later
/// call echoes it back, and the worker uses it to find the catalog. Holding it
/// in a value type — rather than borrowing the client — keeps the API free of
/// lifetime tangles, since the handle really is just bytes.
#[derive(Debug, Clone)]
pub struct AttachedCatalog {
    handle: Bytes,
    info: CatalogAttachResult,
    transaction: Option<Bytes>,
    // Shared by clones because capability belongs to the remote attach, not a
    // particular Rust handle value.
    scan_branches_capability: Arc<AtomicU8>,
}

impl AttachedCatalog {
    /// The worker's session token for this attach.
    pub fn handle(&self) -> &Bytes {
        &self.handle
    }

    /// Everything the worker reported at attach time.
    pub fn info(&self) -> &CatalogAttachResult {
        &self.info
    }

    /// The schema a bare table name resolves in.
    pub fn default_schema(&self) -> &str {
        &self.info.default_schema
    }

    /// Whether the worker offered transactions.
    pub fn supports_transactions(&self) -> bool {
        self.info.supports_transactions
    }

    /// The transaction handle threaded onto reads, if one is open.
    pub fn transaction(&self) -> Option<&Bytes> {
        self.transaction.as_ref()
    }

    /// Worker-selected functions to publish in the host's global registry.
    pub fn global_functions(&self) -> Result<Vec<FunctionInfo>> {
        self.info
            .global_functions
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                let batch = vgi_protocol::ipc::read_batch(&bytes.0).map_err(|error| {
                    RpcError::runtime_error(format!(
                        "invalid global_functions[{index}] IPC: {}",
                        error.message
                    ))
                })?;
                vgi_protocol::wire::from_batch(&batch)
            })
            .collect()
    }

    /// Catalogs the worker asks the host to attach alongside this one.
    pub fn companion_catalogs(&self) -> Result<Vec<AttachCatalogInfo>> {
        self.info
            .attach_catalogs
            .iter()
            .enumerate()
            .map(|(index, bytes)| {
                let batch = vgi_protocol::ipc::read_batch(&bytes.0).map_err(|error| {
                    RpcError::runtime_error(format!(
                        "invalid attach_catalogs[{index}] IPC: {}",
                        error.message
                    ))
                })?;
                vgi_protocol::wire::from_batch(&batch)
            })
            .collect()
    }

    fn txn(&self) -> Option<Bytes> {
        self.transaction.clone()
    }
}

/// How to attach.
#[derive(Clone, Default)]
pub struct AttachOptions {
    /// IPC-encoded attach options, if the catalog declares any.
    pub options: Option<Bytes>,
    /// Pin the read to a published data version.
    pub data_version_spec: Option<String>,
    /// Pin the read to a worker implementation version.
    pub implementation_version: Option<String>,
}

impl std::fmt::Debug for AttachOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttachOptions")
            .field(
                "options",
                &self
                    .options
                    .as_ref()
                    .map(|b| format!("<ipc:{} bytes>", b.0.len())),
            )
            .field("data_version_spec", &self.data_version_spec)
            .field("implementation_version", &self.implementation_version)
            .finish()
    }
}

impl VgiClient {
    /// List the catalogs this worker serves.
    pub fn catalogs(&mut self) -> Result<Vec<CatalogInfo>> {
        // `catalog_catalogs` takes no params columns, so there is no generated
        // struct for it — `VgiArrow` cannot derive on a field-less type.
        let params = self.empty_params()?;
        crate::wire_call::call_items_raw(self.transport_mut(), "catalog_catalogs", &params)
    }

    /// Attach a catalog by name, returning the handle every later call needs.
    pub fn attach(&mut self, name: &str, options: AttachOptions) -> Result<AttachedCatalog> {
        let request = envelope(CatalogAttachRequest {
            name: name.to_string(),
            options: options.options,
            data_version_spec: options.data_version_spec,
            implementation_version: options.implementation_version,
        })?;
        let info: CatalogAttachResult = call(
            self.transport_mut(),
            "catalog_attach",
            p::CatalogAttachParams { request },
        )?;
        Ok(AttachedCatalog {
            handle: info.attach_opaque_data.clone(),
            info,
            transaction: None,
            scan_branches_capability: Arc::new(AtomicU8::new(BRANCHES_CAPABILITY_UNKNOWN)),
        })
    }

    /// Release an attach. The handle is dead afterwards.
    pub fn detach(&mut self, cat: &AttachedCatalog) -> Result<()> {
        call_unit(
            self.transport_mut(),
            "catalog_detach",
            p::CatalogDetachParams {
                attach_opaque_data: cat.handle.clone(),
            },
        )
    }

    /// The catalog's current version counter.
    pub fn catalog_version(&mut self, cat: &AttachedCatalog) -> Result<i64> {
        let r: CatalogVersionResult = call(
            self.transport_mut(),
            "catalog_version",
            p::CatalogVersionParams {
                attach_opaque_data: cat.handle.clone(),
                transaction_opaque_data: cat.txn(),
            },
        )?;
        Ok(r.version)
    }

    /// Every schema in the catalog.
    pub fn schemas(&mut self, cat: &AttachedCatalog) -> Result<Vec<SchemaInfo>> {
        call_items(
            self.transport_mut(),
            "catalog_schemas",
            p::CatalogSchemasParams {
                attach_opaque_data: cat.handle.clone(),
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// One schema by name, or `None` when the catalog has no such schema.
    pub fn schema_get(&mut self, cat: &AttachedCatalog, name: &str) -> Result<Option<SchemaInfo>> {
        let items: Vec<SchemaInfo> = call_items(
            self.transport_mut(),
            "catalog_schema_get",
            p::CatalogSchemaGetParams {
                attach_opaque_data: cat.handle.clone(),
                name: name.to_string(),
                transaction_opaque_data: cat.txn(),
            },
        )?;
        Ok(items.into_iter().next())
    }

    /// Tables in a schema.
    pub fn tables(&mut self, cat: &AttachedCatalog, schema: &str) -> Result<Vec<TableInfo>> {
        call_items(
            self.transport_mut(),
            "catalog_schema_contents_tables",
            p::CatalogSchemaContentsTablesParams {
                attach_opaque_data: cat.handle.clone(),
                name: schema.to_string(),
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// Views in a schema.
    pub fn views(&mut self, cat: &AttachedCatalog, schema: &str) -> Result<Vec<ViewInfo>> {
        call_items(
            self.transport_mut(),
            "catalog_schema_contents_views",
            p::CatalogSchemaContentsViewsParams {
                attach_opaque_data: cat.handle.clone(),
                name: schema.to_string(),
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// Functions of one kind in a schema.
    pub fn functions(
        &mut self,
        cat: &AttachedCatalog,
        schema: &str,
        kind: FunctionKind,
    ) -> Result<Vec<FunctionInfo>> {
        call_items(
            self.transport_mut(),
            "catalog_schema_contents_functions",
            p::CatalogSchemaContentsFunctionsParams {
                attach_opaque_data: cat.handle.clone(),
                name: schema.to_string(),
                r#type: kind.dict(),
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// Macros of one kind in a schema.
    pub fn macros(
        &mut self,
        cat: &AttachedCatalog,
        schema: &str,
        kind: MacroKind,
    ) -> Result<Vec<MacroInfo>> {
        call_items(
            self.transport_mut(),
            "catalog_schema_contents_macros",
            p::CatalogSchemaContentsMacrosParams {
                attach_opaque_data: cat.handle.clone(),
                name: schema.to_string(),
                r#type: kind.dict(),
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// One table by name, optionally at a past version.
    pub fn table_get(
        &mut self,
        cat: &AttachedCatalog,
        schema: &str,
        name: &str,
        at: Option<&At>,
    ) -> Result<Option<TableInfo>> {
        let items: Vec<TableInfo> = call_items(
            self.transport_mut(),
            "catalog_table_get",
            p::CatalogTableGetParams {
                attach_opaque_data: cat.handle.clone(),
                schema_name: schema.to_string(),
                name: name.to_string(),
                at_unit: at.map(|a| a.unit.clone()),
                at_value: at.map(|a| a.value.clone()),
                transaction_opaque_data: cat.txn(),
            },
        )?;
        Ok(items.into_iter().next())
    }

    /// How to scan a catalog table: which function to bind, with what arguments.
    ///
    /// A VGI catalog table is not storage the client reads directly — it is a
    /// *function call the worker chose*, so scanning one means binding that
    /// function with the worker's own arguments. Those arguments arrive
    /// already IPC-encoded and are forwarded verbatim
    /// ([`BindSpec::with_raw_arguments`]) rather than decoded and rebuilt,
    /// since they may carry types this client does not model.
    ///
    /// The worker may **inline** the answer on [`TableInfo::scan_function`] to
    /// save a round trip, or leave it empty, in which case this fires
    /// `catalog_table_scan_function_get`. Both are normal; inlining is an
    /// optimisation, not a different kind of table.
    pub fn table_scan_function(
        &mut self,
        cat: &AttachedCatalog,
        table: &TableInfo,
        at: Option<&At>,
    ) -> Result<ScanFunctionResult> {
        // Nullable on the wire: absent OR empty both mean "not inlined".
        if let Some(inlined) = table.scan_function.as_ref().filter(|b| !b.0.is_empty()) {
            let batch = vgi_protocol::ipc::read_batch(&inlined.0)?;
            return vgi_protocol::wire::from_batch(&batch);
        }
        call(
            self.transport_mut(),
            "catalog_table_scan_function_get",
            p::CatalogTableScanFunctionGetParams {
                attach_opaque_data: cat.handle.clone(),
                schema_name: table.schema_name.clone(),
                name: table.name.clone(),
                at_unit: at.map(|a| a.unit.clone()),
                at_value: at.map(|a| a.value.clone()),
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// Resolve every physical source backing a catalog table.
    ///
    /// New workers answer `catalog_table_scan_branches_get`; each nested IPC
    /// branch is decoded before it reaches the caller. For compatibility, a
    /// worker that reports `MethodNotImplementedError` is retried through the
    /// legacy single-function RPC and that answer is represented as one
    /// unconstrained function branch. The unsupported capability is cached on
    /// the attach so later tables avoid the doomed probe.
    ///
    /// The fallback is deliberately narrow: transport failures and all other
    /// worker errors are returned unchanged rather than being hidden behind a
    /// second RPC.
    pub fn table_scan_branches(
        &mut self,
        cat: &AttachedCatalog,
        table: &TableInfo,
        at: Option<&At>,
    ) -> Result<CatalogScanBranches> {
        if cat.scan_branches_capability.load(Ordering::Acquire) == BRANCHES_CAPABILITY_UNSUPPORTED {
            return self.table_scan_function(cat, table, at).map(|legacy| {
                scan_branches_from_legacy(legacy, ScanBranchesResolution::LegacyCached)
            });
        }

        let response: Result<ScanBranchesResult> = call(
            self.transport_mut(),
            "catalog_table_scan_branches_get",
            p::CatalogTableScanBranchesGetParams {
                attach_opaque_data: cat.handle.clone(),
                schema_name: table.schema_name.clone(),
                name: table.name.clone(),
                at_unit: at.map(|a| a.unit.clone()),
                at_value: at.map(|a| a.value.clone()),
                transaction_opaque_data: cat.txn(),
            },
        );

        match response {
            Ok(response) => {
                let decoded = decode_scan_branches(response, ScanBranchesResolution::BranchesRpc)?;
                cat.scan_branches_capability
                    .store(BRANCHES_CAPABILITY_SUPPORTED, Ordering::Release);
                Ok(decoded)
            }
            Err(error) if error.error_type == "MethodNotImplementedError" => {
                cat.scan_branches_capability
                    .store(BRANCHES_CAPABILITY_UNSUPPORTED, Ordering::Release);
                self.table_scan_function(cat, table, at).map(|legacy| {
                    scan_branches_from_legacy(
                        legacy,
                        ScanBranchesResolution::LegacyFallbackAfterProbe,
                    )
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Fetch the worker's optimizer statistics for one catalog table.
    ///
    /// Unlike most catalog responses, this RPC returns the canonical
    /// sparse-union statistics batch directly inside the result envelope.
    /// An empty-schema batch means the table declares no column statistics.
    pub fn table_column_statistics(
        &mut self,
        cat: &AttachedCatalog,
        schema: &str,
        name: &str,
    ) -> Result<RecordBatch> {
        call_batch(
            self.transport_mut(),
            "catalog_table_column_statistics_get",
            p::CatalogTableColumnStatisticsGetParams {
                attach_opaque_data: cat.handle.clone(),
                schema_name: schema.to_string(),
                name: name.to_string(),
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// Open a transaction, threading its handle onto later reads.
    ///
    /// A worker may legitimately return no handle — `supports_transactions` is
    /// advisory and some catalogs treat every read as its own snapshot. In that
    /// case this is a no-op and `cat.transaction()` stays `None`.
    pub fn begin_transaction(&mut self, cat: &mut AttachedCatalog) -> Result<()> {
        let r: CatalogTransactionBeginResult = call(
            self.transport_mut(),
            "catalog_transaction_begin",
            p::CatalogTransactionBeginParams {
                attach_opaque_data: cat.handle.clone(),
            },
        )?;
        cat.transaction = r.transaction_opaque_data;
        Ok(())
    }

    /// Commit the open transaction, if any.
    pub fn commit(&mut self, cat: &mut AttachedCatalog) -> Result<()> {
        self.end_transaction(cat, "catalog_transaction_commit")
    }

    /// Roll back the open transaction, if any.
    pub fn rollback(&mut self, cat: &mut AttachedCatalog) -> Result<()> {
        self.end_transaction(cat, "catalog_transaction_rollback")
    }

    fn end_transaction(&mut self, cat: &mut AttachedCatalog, method: &str) -> Result<()> {
        let Some(txn) = cat.transaction.take() else {
            return Ok(());
        };
        call_unit(
            self.transport_mut(),
            method,
            p::CatalogTransactionCommitParams {
                attach_opaque_data: cat.handle.clone(),
                transaction_opaque_data: txn,
            },
        )
    }
}

fn scan_branches_from_legacy(
    legacy: ScanFunctionResult,
    resolution: ScanBranchesResolution,
) -> CatalogScanBranches {
    CatalogScanBranches {
        branches: vec![ScanBranch {
            function_name: legacy.function_name,
            arguments: legacy.arguments,
            branch_filter: None,
            writable: false,
            source_catalog: None,
            source_schema: None,
            source_table: None,
            format_name: None,
            format_locations: None,
            format_options: None,
        }],
        required_extensions: legacy.required_extensions,
        resolution,
    }
}

fn decode_scan_branches(
    response: ScanBranchesResult,
    resolution: ScanBranchesResolution,
) -> Result<CatalogScanBranches> {
    if response.branches.is_empty() {
        return Err(RpcError::value_error(
            "VGI table returned zero scan branches",
        ));
    }

    let branches: Vec<ScanBranch> = response
        .branches
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            let batch = vgi_protocol::ipc::read_batch(&bytes.0).map_err(|error| {
                RpcError::type_error(format!(
                    "invalid ScanBranch #{index} IPC: {}",
                    error.message
                ))
            })?;
            if batch.num_rows() != 1 {
                return Err(RpcError::type_error(format!(
                    "ScanBranch #{index} must contain one row, found {}",
                    batch.num_rows()
                )));
            }
            vgi_protocol::wire::from_batch(&batch).map_err(|error| {
                RpcError::type_error(format!("invalid ScanBranch #{index}: {}", error.message))
            })
        })
        .collect::<Result<_>>()?;

    validate_scan_branches(&branches)?;
    Ok(CatalogScanBranches {
        branches,
        required_extensions: response.required_extensions,
        resolution,
    })
}

fn validate_scan_branches(branches: &[ScanBranch]) -> Result<()> {
    let mut writable_ordinals = Vec::new();
    for (index, branch) in branches.iter().enumerate() {
        let is_function = !branch.function_name.is_empty();
        let is_catalog_table = branch
            .source_table
            .as_deref()
            .is_some_and(|s| !s.is_empty());
        let is_format = branch.format_name.as_deref().is_some_and(|s| !s.is_empty());
        let source_kinds =
            usize::from(is_function) + usize::from(is_catalog_table) + usize::from(is_format);
        if source_kinds != 1 {
            return Err(RpcError::value_error(format!(
                "VGI scan branch {index} must name exactly one of function_name, source_table, or format_name"
            )));
        }
        if is_format
            && branch
                .format_locations
                .as_ref()
                .is_none_or(|locations| locations.is_empty())
        {
            return Err(RpcError::value_error(format!(
                "VGI scan branch {index} is a format branch but names no locations"
            )));
        }
        if branch.writable {
            writable_ordinals.push(index);
        }
    }

    if writable_ordinals.len() > 1 {
        return Err(RpcError::value_error(format!(
            "VGI multi-branch table declared {} writable branches (ordinals: {})",
            writable_ordinals.len(),
            writable_ordinals
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod attach_option_tests {
    use std::sync::{Arc, Mutex};

    use arrow_array::{Array, ArrayRef, BinaryArray, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use vgi_protocol::{ipc, wire};

    use crate::transport::{ExchangeStream, ProducerStream, VgiTransport};

    use super::*;

    #[test]
    fn decodes_type_default_and_required() {
        let default = Arc::new(StringArray::from(vec!["us-east-1"])) as ArrayRef;
        let raw = vgi::catalog::serialize_attach_option_spec(
            "region",
            "Cloud region",
            &DataType::Utf8,
            Some(&default),
            false,
        )
        .unwrap();
        let spec = AttachOptionSpec::decode(&raw).unwrap();
        assert_eq!(spec.name, "region");
        assert_eq!(spec.description, "Cloud region");
        assert_eq!(spec.data_type, DataType::Utf8);
        let default = spec.default_value.unwrap();
        assert_eq!(
            default
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "us-east-1"
        );
        assert!(!spec.required);

        let required = vgi::catalog::serialize_attach_option_spec(
            "api_key",
            "API key",
            &DataType::Utf8,
            None,
            true,
        )
        .unwrap();
        assert!(AttachOptionSpec::decode(&required).unwrap().required);
    }

    struct LegacyCatalogTransport {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl VgiTransport for LegacyCatalogTransport {
        fn call_unary(&mut self, method: &str, _params: &RecordBatch) -> Result<RecordBatch> {
            self.calls.lock().unwrap().push(method.to_string());
            match method {
                "catalog_table_scan_branches_get" => {
                    Err(RpcError::new("MethodNotImplementedError", "old worker"))
                }
                "catalog_table_scan_function_get" => {
                    let inner = wire::to_batch(ScanFunctionResult {
                        function_name: "legacy_sequence".to_string(),
                        arguments: Bytes(vec![42]),
                        required_extensions: vec!["legacy_ext".to_string()],
                    })?;
                    let encoded = ipc::write_batch(&inner)?;
                    RecordBatch::try_from_iter(vec![(
                        "result",
                        Arc::new(BinaryArray::from(vec![encoded.as_slice()])) as ArrayRef,
                    )])
                    .map_err(|error| RpcError::runtime_error(error.to_string()))
                }
                _ => Err(RpcError::runtime_error(format!("unexpected call {method}"))),
            }
        }

        fn open_producer<'a>(
            &'a mut self,
            _method: &str,
            _params: &RecordBatch,
            _metadata: Option<vgi_rpc::wire::Metadata>,
            _has_header: bool,
        ) -> Result<Box<dyn ProducerStream + 'a>> {
            Err(RpcError::runtime_error("no producer stream"))
        }

        fn open_exchange<'a>(
            &'a mut self,
            _method: &str,
            _params: &RecordBatch,
            _has_header: bool,
        ) -> Result<Box<dyn ExchangeStream + 'a>> {
            Err(RpcError::runtime_error("no exchange stream"))
        }

        fn label(&self) -> &str {
            "legacy-catalog-stub"
        }
    }

    fn attached_catalog_for_test() -> AttachedCatalog {
        let info = CatalogAttachResult {
            attach_opaque_data: Bytes(vec![1]),
            supports_transactions: false,
            supports_time_travel: false,
            catalog_version_frozen: false,
            catalog_version: 1,
            attach_opaque_data_required: true,
            default_schema: "data".to_string(),
            settings: Vec::new(),
            secret_types: Vec::new(),
            attach_catalogs: Vec::new(),
            comment: None,
            tags: Vec::new(),
            supports_column_statistics: false,
            global_functions: Vec::new(),
            global_function_prefix: String::new(),
            resolved_data_version: None,
            resolved_implementation_version: None,
        };
        AttachedCatalog {
            handle: info.attach_opaque_data.clone(),
            info,
            transaction: None,
            scan_branches_capability: Arc::new(AtomicU8::new(BRANCHES_CAPABILITY_UNKNOWN)),
        }
    }

    fn table_for_test() -> TableInfo {
        TableInfo {
            comment: None,
            tags: Vec::new(),
            name: "numbers".to_string(),
            schema_name: "data".to_string(),
            columns: Bytes(Vec::new()),
            not_null_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            primary_key_constraints: Vec::new(),
            foreign_key_constraints: Vec::new(),
            supports_insert: false,
            supports_update: false,
            supports_delete: false,
            supports_returning: false,
            supports_column_statistics: false,
            scan_function: Some(Bytes(Vec::new())),
            insert_function: None,
            update_function: None,
            delete_function: None,
            cardinality_estimate: None.into(),
            cardinality_max: None.into(),
            column_statistics: None,
            bind_result: None,
            required_filters: Vec::new(),
        }
    }

    #[test]
    fn legacy_branches_fallback_is_narrow_and_cached_per_attach() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut client = VgiClient::new(Box::new(LegacyCatalogTransport {
            calls: Arc::clone(&calls),
        }));
        let cat = attached_catalog_for_test();
        let table = table_for_test();

        let first = client.table_scan_branches(&cat, &table, None).unwrap();
        assert_eq!(
            first.resolution,
            ScanBranchesResolution::LegacyFallbackAfterProbe
        );
        assert_eq!(first.branches.len(), 1);
        assert_eq!(first.branches[0].function_name, "legacy_sequence");
        assert_eq!(first.required_extensions, ["legacy_ext"]);

        let second = client.table_scan_branches(&cat, &table, None).unwrap();
        assert_eq!(second.resolution, ScanBranchesResolution::LegacyCached);
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            [
                "catalog_table_scan_branches_get",
                "catalog_table_scan_function_get",
                "catalog_table_scan_function_get"
            ],
            "the second scan must skip the known-unsupported branches RPC"
        );
    }

    #[test]
    fn encodes_one_row_and_rejects_other_cardinality() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "answer",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![42])) as ArrayRef],
        )
        .unwrap();
        let encoded = encode_attach_options(&batch).unwrap();
        let decoded = vgi_protocol::ipc::read_batch(&encoded.0).unwrap();
        assert_eq!(decoded.num_rows(), 1);
        assert_eq!(decoded.schema().field(0).data_type(), &DataType::Int32);

        let empty = RecordBatch::new_empty(schema);
        assert!(encode_attach_options(&empty).is_err());
    }

    #[test]
    fn debug_does_not_render_default_or_option_payloads() {
        let spec = AttachOptionSpec {
            name: "api_key".into(),
            description: "secret-like".into(),
            data_type: DataType::Utf8,
            default_value: Some(Arc::new(StringArray::from(vec!["sentinel-secret"]))),
            required: false,
        };
        assert!(!format!("{spec:?}").contains("sentinel-secret"));

        let options = AttachOptions {
            options: Some(Bytes(b"sentinel-secret".to_vec())),
            ..Default::default()
        };
        assert!(!format!("{options:?}").contains("sentinel-secret"));
    }
}

#[cfg(test)]
mod macro_default_tests {
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn info(parameters: &[&str], defaults: Option<RecordBatch>) -> MacroInfo {
        MacroInfo {
            comment: None,
            tags: Vec::new(),
            name: "clamp".to_string(),
            schema_name: "main".to_string(),
            macro_type: DictString("scalar".to_string()),
            parameters: parameters.iter().map(|value| value.to_string()).collect(),
            parameter_default_values: defaults.map(|batch| {
                Bytes::from(vgi_protocol::ipc::write_batch(&batch).expect("encode defaults"))
            }),
            definition: "val".to_string(),
            arguments_schema: None,
        }
    }

    fn defaults(fields: &[&str], columns: Vec<ArrayRef>) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            fields
                .iter()
                .map(|name| Field::new(*name, DataType::Int64, false))
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, columns).expect("defaults batch")
    }

    #[test]
    fn decodes_named_typed_macro_defaults() {
        let macro_info = info(
            &["val", "lo", "hi"],
            Some(defaults(
                &["lo", "hi"],
                vec![
                    Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![100])) as ArrayRef,
                ],
            )),
        );
        let decoded = decode_macro_defaults(&macro_info).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, "lo");
        assert_eq!(
            decoded[1]
                .1
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            100
        );
    }

    #[test]
    fn empty_macro_defaults_are_absent() {
        assert!(decode_macro_defaults(&info(&["x"], None))
            .unwrap()
            .is_empty());
        let mut empty = info(&["x"], None);
        empty.parameter_default_values = Some(Bytes::from(Vec::new()));
        assert!(decode_macro_defaults(&empty).unwrap().is_empty());
    }

    #[test]
    fn preserves_the_declared_type_of_a_null_default() {
        let schema = Arc::new(Schema::new(vec![Field::new("lo", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![None])) as ArrayRef],
        )
        .unwrap();
        let decoded = decode_macro_defaults(&info(&["lo"], Some(batch))).unwrap();
        assert_eq!(decoded[0].1.data_type(), &DataType::Int64);
        assert!(decoded[0].1.is_null(0));
    }

    #[test]
    fn rejects_wrong_cardinality_unknown_and_duplicate_defaults() {
        let two_rows = info(
            &["lo"],
            Some(defaults(
                &["lo"],
                vec![Arc::new(Int64Array::from(vec![0, 1])) as ArrayRef],
            )),
        );
        assert!(decode_macro_defaults(&two_rows)
            .unwrap_err()
            .message
            .contains("one row"));

        let unknown = info(
            &["lo"],
            Some(defaults(
                &["other"],
                vec![Arc::new(Int64Array::from(vec![0])) as ArrayRef],
            )),
        );
        assert!(decode_macro_defaults(&unknown)
            .unwrap_err()
            .message
            .contains("unknown parameter"));

        let duplicate = info(
            &["lo"],
            Some(defaults(
                &["lo", "LO"],
                vec![
                    Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                ],
            )),
        );
        assert!(decode_macro_defaults(&duplicate)
            .unwrap_err()
            .message
            .contains("duplicate default"));
    }
}

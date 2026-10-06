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
    AttachCatalogInfo, CatalogAttachRequest, CatalogAttachResult, CatalogContentsResponse,
    CatalogInfo, CatalogTransactionBeginResult, CatalogVersionResult, ClientCapabilities,
    FunctionInfo, IndexInfo, MacroInfo, ScanBranch, ScanBranchesResult, ScanFunctionResult,
    SchemaContents, SchemaInfo, TableInfo, ViewInfo,
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
    /// Whether the option carries a credential (an API key, token or password).
    ///
    /// Read from the spec's nullable `secret` column by name; absent (a worker
    /// that predates the column) or null means `false`. It combines with
    /// [`required`](Self::required). A secret option may declare a default but
    /// normally has none.
    ///
    /// Workers must declare credential options secret. The value is passed
    /// inline as an attach option, so a client must treat it accordingly: mask
    /// it in any UI, and keep it out of cache keys, logs, telemetry and
    /// exported or shared configuration. The DuckDB extension redacts it from
    /// `duckdb_databases()`, keeps only a salted hash of it in its cache key,
    /// and never logs it. Writing the value as an expression keeps it out of
    /// the SQL text: `ATTACH 'sales' (TYPE vgi, LOCATION '…', api_key
    /// getenv('SALES_API_KEY'))`.
    pub secret: bool,
}

/// One typed session setting advertised by an attached VGI catalog.
///
/// The protocol intentionally transports setting declarations as nested IPC
/// rather than flattening Arrow types into strings. Keeping the decoded type
/// here lets query-engine adapters build a correctly typed one-row settings
/// batch without depending on the worker SDK crate.
#[derive(Clone)]
pub struct SettingSpec {
    /// Case-preserving setting name.
    pub name: String,
    /// Human-readable description supplied by the worker.
    pub description: String,
    /// Arrow type accepted by the worker.
    pub data_type: DataType,
    /// Typed one-element default, when a newer worker advertises one.
    pub default_value: Option<ArrayRef>,
}

impl std::fmt::Debug for SettingSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SettingSpec")
            .field("name", &self.name)
            .field("description", &self.description)
            .field("data_type", &self.data_type)
            .field(
                "default_value",
                &self.default_value.as_ref().map(|_| "<typed value>"),
            )
            .finish()
    }
}

impl SettingSpec {
    /// Decode the nested IPC shape used by `CatalogAttachResult.settings`.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let batch = vgi_protocol::ipc::read_batch(bytes)?;
        if batch.num_rows() != 1 {
            return Err(RpcError::type_error(format!(
                "SettingSpec must contain one row, found {}",
                batch.num_rows()
            )));
        }
        let string = |name: &str| -> Result<String> {
            let array = batch
                .column_by_name(name)
                .and_then(|array| array.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| RpcError::type_error(format!("SettingSpec.{name} is not Utf8")))?;
            if array.is_null(0) {
                return Err(RpcError::type_error(format!("SettingSpec.{name} is null")));
            }
            Ok(array.value(0).to_string())
        };
        let types = batch
            .column_by_name("type")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .ok_or_else(|| RpcError::type_error("SettingSpec.type is not Binary"))?;
        if types.is_null(0) {
            return Err(RpcError::type_error("SettingSpec.type is null"));
        }
        let schema = vgi_protocol::ipc::read_schema(types.value(0))?;
        let field = schema
            .fields()
            .first()
            .ok_or_else(|| RpcError::type_error("SettingSpec.type schema has no value field"))?;
        let default_value = match batch.column_by_name("default_value") {
            Some(array) => {
                let values = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        RpcError::type_error("SettingSpec.default_value is not Binary")
                    })?;
                if values.is_null(0) {
                    None
                } else {
                    let default = vgi_protocol::ipc::read_batch(values.value(0))?;
                    if default.num_rows() != 1 || default.num_columns() != 1 {
                        return Err(RpcError::type_error(
                            "SettingSpec.default_value must contain one value",
                        ));
                    }
                    if default.column(0).data_type() != field.data_type() {
                        return Err(RpcError::type_error(
                            "SettingSpec.default_value does not match its declared type",
                        ));
                    }
                    Some(default.column(0).clone())
                }
            }
            None => None,
        };
        Ok(Self {
            name: string("name")?,
            description: string("description")?,
            data_type: field.data_type().clone(),
            default_value,
        })
    }
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
            .field("secret", &self.secret)
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
        // Appended after `required`, nullable: absent or null means not secret.
        let secret = batch
            .column_by_name("secret")
            .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
            .is_some_and(|a| !a.is_null(0) && a.value(0));

        Ok(Self {
            name: string("name")?,
            description: string("description")?,
            data_type: field.data_type().clone(),
            default_value,
            required,
            secret,
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

/// Decode every setting declaration returned by `catalog_attach`.
pub fn decode_setting_specs(info: &CatalogAttachResult) -> Result<Vec<SettingSpec>> {
    info.settings
        .iter()
        .map(|bytes| SettingSpec::decode(&bytes.0))
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
/// on [`FunctionInfo::function_type`](crate::dtos::FunctionInfo::function_type) in the response.
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

/// One schema and everything in it, decoded — an entry of
/// [`CatalogSnapshot::schemas`].
///
/// Each list is complete: empty means "this schema has none of that kind".
#[derive(Debug, Clone)]
pub struct SchemaSnapshot {
    /// The schema itself (its path, comment, tags and object counts).
    pub schema: SchemaInfo,
    /// Tables.
    pub tables: Vec<TableInfo>,
    /// Views.
    pub views: Vec<ViewInfo>,
    /// Scalar functions.
    pub scalar_functions: Vec<FunctionInfo>,
    /// Aggregate functions.
    pub aggregate_functions: Vec<FunctionInfo>,
    /// Table functions (producer, buffered and streaming table-in-out).
    pub table_functions: Vec<FunctionInfo>,
    /// Scalar macros.
    pub scalar_macros: Vec<MacroInfo>,
    /// Table macros.
    pub table_macros: Vec<MacroInfo>,
    /// Indexes.
    pub indexes: Vec<IndexInfo>,
}

impl SchemaSnapshot {
    /// The schema's path.
    pub fn path(&self) -> &[String] {
        &self.schema.path
    }

    /// Decode one `catalog_contents` schema entry, checking that its `path`
    /// equals the `SchemaInfo.path` inside its `schema` item.
    pub fn decode(entry: &SchemaContents) -> Result<Self> {
        fn item<T: vgi_rpc::VgiArrow>(bytes: &Bytes, what: &str, path: &[String]) -> Result<T> {
            let batch = vgi_protocol::ipc::read_batch(&bytes.0).map_err(|e| {
                RpcError::type_error(format!(
                    "catalog_contents: {what} item of schema {path:?} is not a readable IPC batch: {}",
                    e.message
                ))
            })?;
            vgi_protocol::wire::from_batch(&batch)
        }
        fn items<T: vgi_rpc::VgiArrow>(
            list: &[Bytes],
            what: &str,
            path: &[String],
        ) -> Result<Vec<T>> {
            list.iter().map(|b| item(b, what, path)).collect()
        }
        let path = entry.path.as_slice();
        let schema: SchemaInfo = item(&entry.schema, "schema", path)?;
        if schema.path != entry.path {
            return Err(RpcError::type_error(format!(
                "catalog_contents: schema path {:?} differs from its SchemaInfo.path {:?}",
                entry.path, schema.path
            )));
        }
        Ok(SchemaSnapshot {
            schema,
            tables: items(&entry.tables, "tables", path)?,
            views: items(&entry.views, "views", path)?,
            scalar_functions: items(&entry.scalar_functions, "scalar_functions", path)?,
            aggregate_functions: items(&entry.aggregate_functions, "aggregate_functions", path)?,
            table_functions: items(&entry.table_functions, "table_functions", path)?,
            scalar_macros: items(&entry.scalar_macros, "scalar_macros", path)?,
            table_macros: items(&entry.table_macros, "table_macros", path)?,
            indexes: items(&entry.indexes, "indexes", path)?,
        })
    }
}

/// Which RPCs served a [`CatalogSnapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogLoadSource {
    /// One `catalog_contents` call.
    CatalogContents,
    /// `catalog_schemas` plus the per-schema `catalog_schema_contents_*` calls.
    PerSchema,
}

/// A whole catalog — every schema and everything in it — from
/// [`VgiClient::load_catalog`].
///
/// Hold on to it and pass it back as `previous` to revalidate: when it came
/// from `catalog_contents` with an etag, the next load sends `if_none_match`,
/// and a `not_modified` answer returns this same content without downloading
/// it again.
#[derive(Debug, Clone)]
pub struct CatalogSnapshot {
    /// One entry per schema, parents before children.
    pub schemas: Vec<SchemaSnapshot>,
    /// The catalog version the snapshot was taken at; `None` when it was
    /// assembled from the per-schema calls (which carry none).
    pub catalog_version: Option<i64>,
    /// The `catalog_contents` validator; `None` when the worker does not
    /// revalidate or the snapshot did not come from `catalog_contents`.
    pub etag: Option<String>,
    /// Which RPCs served it.
    pub source: CatalogLoadSource,
    /// `true` when this is `previous` confirmed current by a `not_modified`
    /// answer.
    pub not_modified: bool,
    /// Why an advertised `catalog_contents` was not used (its error, or a
    /// protocol violation); `None` otherwise.
    pub fallback_reason: Option<String>,
}

impl CatalogSnapshot {
    /// The schema at `path`, if the catalog has it.
    pub fn schema(&self, path: &[String]) -> Option<&SchemaSnapshot> {
        self.schemas.iter().find(|s| s.schema.path == path)
    }
}

/// The `estimated_object_count` key of each per-schema kind. A count of
/// exactly 0 is the worker's guarantee that the schema has none, so the
/// per-schema load skips that call (the rule the DuckDB extension applies).
fn kind_may_exist(schema: &SchemaInfo, key: &str) -> bool {
    !schema
        .estimated_object_count
        .as_ref()
        .is_some_and(|counts| counts.iter().any(|(k, n)| k == key && *n == 0))
}

/// Which kind of DDL conflict handling to request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnConflict {
    /// Fail when the object exists.
    #[default]
    Error,
    /// Do nothing when it exists (`IF NOT EXISTS`).
    Ignore,
    /// Replace it (`OR REPLACE`).
    Replace,
}

impl OnConflict {
    fn dict(self) -> DictString {
        DictString(
            match self {
                Self::Error => "error",
                Self::Ignore => "ignore",
                Self::Replace => "replace",
            }
            .to_string(),
        )
    }
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

    /// Whether the worker serves [`VgiClient::contents`] — the whole catalog in
    /// one call (protocol 2.1.0). `false` for an older worker.
    pub fn supports_catalog_contents(&self) -> bool {
        self.info.supports_catalog_contents
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
        let capabilities = ClientCapabilities {
            engine: "vgi-rust".to_string(),
            native_formats: Vec::new(),
            catalogs: Vec::new(),
            can_stream: true,
            filter_encodings: vec!["vgi.filters.v2".to_string()],
        };
        let request = envelope(CatalogAttachRequest {
            name: name.to_string(),
            options: options.options,
            data_version_spec: options.data_version_spec,
            implementation_version: options.implementation_version,
            client_capabilities: Some(Bytes::from(vgi_protocol::ipc::write_batch(
                &vgi_protocol::wire::to_batch(capabilities)?,
            )?)),
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

    /// Every schema and all of its contents in one call (`catalog_contents`,
    /// protocol 2.1.0), raw — refused when
    /// [`AttachedCatalog::supports_catalog_contents`] is false. Most callers
    /// want [`VgiClient::load_catalog`] instead.
    ///
    /// Each [`SchemaContents`] item is byte-for-byte what the matching
    /// per-schema call returns (`schemas`, `tables_path`, `functions_path`, …),
    /// so decode them with the same item decoders. Schemas come parents before
    /// children. Read with no transaction: the result is the committed catalog
    /// at `catalog_version`.
    pub fn contents(&mut self, cat: &AttachedCatalog) -> Result<Vec<SchemaContents>> {
        Ok(self.contents_response(cat, None)?.schemas)
    }

    /// The whole `catalog_contents` response: the snapshot plus the
    /// `catalog_version` it was taken at and its `etag`, the worker's validator
    /// for it (`None` when the worker does not revalidate).
    ///
    /// `if_none_match` is the etag of a snapshot the caller already holds: when
    /// it is still current the answer is `not_modified` with no schemas (keep
    /// what you have); otherwise it is the full, new snapshot. A worker that
    /// returns no etag ignores it.
    ///
    /// Checked on receipt: a `not_modified` answer carries the etag asked about
    /// and no schemas, and each schema's `path` equals the `SchemaInfo.path`
    /// inside its `schema` item.
    ///
    /// Refused without a round trip when the attach did not advertise
    /// [`AttachedCatalog::supports_catalog_contents`]. For whole-catalog
    /// enumeration prefer [`VgiClient::load_catalog`], which picks the path,
    /// falls back and revalidates.
    pub fn contents_response(
        &mut self,
        cat: &AttachedCatalog,
        if_none_match: Option<&str>,
    ) -> Result<CatalogContentsResponse> {
        // Never sent to a worker that did not advertise it: an older worker
        // has no such method, and a worker that opted out may not serve it
        // consistently. Refused locally, with no round trip.
        if !cat.supports_catalog_contents() {
            return Err(RpcError::value_error(
                "catalog_contents: this catalog's attach did not advertise \
                 supports_catalog_contents; use load_catalog (or the per-schema calls)",
            ));
        }
        let resp: CatalogContentsResponse = call(
            self.transport_mut(),
            "catalog_contents",
            p::CatalogContentsParams {
                attach_opaque_data: cat.handle.clone(),
                if_none_match: if_none_match.map(str::to_string),
            },
        )?;
        if resp.not_modified {
            if resp.etag.is_none() || resp.etag.as_deref() != if_none_match {
                return Err(RpcError::type_error(format!(
                    "catalog_contents: not_modified for etag {:?}, but asked about {:?}",
                    resp.etag, if_none_match
                )));
            }
            if !resp.schemas.is_empty() {
                return Err(RpcError::type_error(
                    "catalog_contents: a not_modified response carries schemas",
                ));
            }
            return Ok(resp);
        }
        for (i, entry) in resp.schemas.iter().enumerate() {
            let batch = vgi_protocol::ipc::read_batch(&entry.schema.0).map_err(|e| {
                RpcError::type_error(format!(
                    "catalog_contents: schemas[{i}].schema is not a readable IPC batch: {e}"
                ))
            })?;
            let info: SchemaInfo = vgi_protocol::wire::from_batch(&batch)?;
            if info.path != entry.path {
                return Err(RpcError::type_error(format!(
                    "catalog_contents: schemas[{i}].path {:?} differs from its SchemaInfo.path {:?}",
                    entry.path, info.path
                )));
            }
        }
        Ok(resp)
    }

    /// Load every schema and all of its contents, in as few calls as the
    /// worker allows — the client's whole-catalog enumeration.
    ///
    /// - When the attach advertised `supports_catalog_contents`, one
    ///   `catalog_contents` call. Never sent otherwise.
    /// - Inside a transaction (`cat.transaction()` is set), or when that call
    ///   fails or breaks the protocol, `catalog_schemas` plus the per-schema
    ///   `catalog_schema_contents_*` calls (skipping a kind whose
    ///   `estimated_object_count` is exactly 0); `fallback_reason` says why.
    ///   `catalog_contents` returns only the committed catalog, so a
    ///   transactional load always takes the transaction-aware per-schema path.
    ///
    /// Revalidation: pass a snapshot from an earlier load as `previous`. When
    /// it came from `catalog_contents` with an etag, the call sends
    /// `if_none_match`; a `not_modified` answer returns `previous`'s content
    /// (with `not_modified = true` and the current version) and a full answer
    /// replaces it. A snapshot older than `previous` (a lagging replica) is
    /// retried once, then the per-schema calls are used. Version 0 means
    /// "unknown" and is never treated as older.
    pub fn load_catalog(
        &mut self,
        cat: &AttachedCatalog,
        previous: Option<&CatalogSnapshot>,
    ) -> Result<CatalogSnapshot> {
        if !cat.supports_catalog_contents() || cat.transaction.is_some() {
            return self.load_catalog_per_schema(cat, None);
        }
        let if_none_match = previous
            .filter(|p| p.source == CatalogLoadSource::CatalogContents)
            .and_then(|p| p.etag.clone());
        let known_version = previous.and_then(|p| p.catalog_version).unwrap_or(0);
        let mut reason = String::new();
        for _attempt in 0..2 {
            let resp = match self.contents_response(cat, if_none_match.as_deref()) {
                Ok(resp) => resp,
                Err(e) => {
                    reason = e.message;
                    break;
                }
            };
            if resp.not_modified {
                // `contents_response` already checked the etag matches.
                let Some(previous) = previous.filter(|_| if_none_match.is_some()) else {
                    reason = "catalog_contents answered not_modified to a request it could not \
                              match"
                        .to_string();
                    break;
                };
                let mut kept = previous.clone();
                kept.catalog_version = Some(resp.catalog_version);
                kept.etag = resp.etag.or(kept.etag);
                kept.not_modified = true;
                kept.fallback_reason = None;
                return Ok(kept);
            }
            if known_version != 0
                && resp.catalog_version != 0
                && resp.catalog_version < known_version
            {
                reason = format!(
                    "catalog_contents returned version {}, older than the known version {known_version}",
                    resp.catalog_version
                );
                continue;
            }
            let decoded = resp
                .schemas
                .iter()
                .map(SchemaSnapshot::decode)
                .collect::<Result<Vec<_>>>();
            match decoded {
                Ok(schemas) => {
                    return Ok(CatalogSnapshot {
                        schemas,
                        catalog_version: Some(resp.catalog_version),
                        etag: resp.etag,
                        source: CatalogLoadSource::CatalogContents,
                        not_modified: false,
                        fallback_reason: None,
                    })
                }
                Err(e) => {
                    reason = e.message;
                    break;
                }
            }
        }
        log::debug!("catalog_contents not used, falling back to per-schema calls: {reason}");
        self.load_catalog_per_schema(cat, Some(reason))
    }

    /// Assemble a snapshot from `catalog_schemas` and the per-schema calls.
    fn load_catalog_per_schema(
        &mut self,
        cat: &AttachedCatalog,
        fallback_reason: Option<String>,
    ) -> Result<CatalogSnapshot> {
        let mut schemas = Vec::new();
        for schema in self.schemas(cat)? {
            let path = schema.path.clone();
            let has = |key: &str| kind_may_exist(&schema, key);
            let tables = if has("table") {
                self.tables_path(cat, &path)?
            } else {
                Vec::new()
            };
            let views = if has("view") {
                self.views_path(cat, &path)?
            } else {
                Vec::new()
            };
            let scalar_functions = if has("scalar_function") {
                self.functions_path(cat, &path, FunctionKind::Scalar)?
            } else {
                Vec::new()
            };
            let aggregate_functions = if has("aggregate_function") {
                self.functions_path(cat, &path, FunctionKind::Aggregate)?
            } else {
                Vec::new()
            };
            let table_functions = if has("table_function") {
                self.functions_path(cat, &path, FunctionKind::Table)?
            } else {
                Vec::new()
            };
            let (scalar_macros, table_macros) = if has("macro") {
                (
                    self.macros_path(cat, &path, MacroKind::Scalar)?,
                    self.macros_path(cat, &path, MacroKind::Table)?,
                )
            } else {
                (Vec::new(), Vec::new())
            };
            let indexes = if has("index") {
                self.indexes_path(cat, &path)?
            } else {
                Vec::new()
            };
            schemas.push(SchemaSnapshot {
                schema,
                tables,
                views,
                scalar_functions,
                aggregate_functions,
                table_functions,
                scalar_macros,
                table_macros,
                indexes,
            });
        }
        // Parents before children, as catalog_contents orders them.
        schemas.sort_by_key(|s| s.schema.path.len());
        Ok(CatalogSnapshot {
            schemas,
            catalog_version: None,
            etag: None,
            source: CatalogLoadSource::PerSchema,
            not_modified: false,
            fallback_reason,
        })
    }

    /// Indexes in an arbitrarily nested schema path.
    pub fn indexes_path(
        &mut self,
        cat: &AttachedCatalog,
        path: &[String],
    ) -> Result<Vec<IndexInfo>> {
        call_items(
            self.transport_mut(),
            "catalog_schema_contents_indexes",
            p::CatalogSchemaContentsIndexesParams {
                attach_opaque_data: cat.handle.clone(),
                path: path.to_vec(),
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// `CREATE VIEW` on a DDL-capable catalog.
    pub fn view_create(
        &mut self,
        cat: &AttachedCatalog,
        schema_path: &[String],
        name: &str,
        definition: &str,
        on_conflict: OnConflict,
    ) -> Result<()> {
        call_unit(
            self.transport_mut(),
            "catalog_view_create",
            p::CatalogViewCreateParams {
                attach_opaque_data: cat.handle.clone(),
                schema_path: schema_path.to_vec(),
                name: name.to_string(),
                definition: definition.to_string(),
                on_conflict: on_conflict.dict(),
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// `DROP VIEW` on a DDL-capable catalog.
    pub fn view_drop(
        &mut self,
        cat: &AttachedCatalog,
        schema_path: &[String],
        name: &str,
        ignore_not_found: bool,
    ) -> Result<()> {
        call_unit(
            self.transport_mut(),
            "catalog_view_drop",
            p::CatalogViewDropParams {
                attach_opaque_data: cat.handle.clone(),
                schema_path: schema_path.to_vec(),
                name: name.to_string(),
                ignore_not_found,
                cascade: false,
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// One schema by name, or `None` when the catalog has no such schema.
    pub fn schema_get(&mut self, cat: &AttachedCatalog, name: &str) -> Result<Option<SchemaInfo>> {
        self.schema_get_path(cat, &[name.to_string()])
    }

    /// One schema by its raw nested path.
    pub fn schema_get_path(
        &mut self,
        cat: &AttachedCatalog,
        path: &[String],
    ) -> Result<Option<SchemaInfo>> {
        let items: Vec<SchemaInfo> = call_items(
            self.transport_mut(),
            "catalog_schema_get",
            p::CatalogSchemaGetParams {
                attach_opaque_data: cat.handle.clone(),
                path: path.to_vec(),
                transaction_opaque_data: cat.txn(),
            },
        )?;
        Ok(items.into_iter().next())
    }

    /// Tables in a schema.
    pub fn tables(&mut self, cat: &AttachedCatalog, schema: &str) -> Result<Vec<TableInfo>> {
        self.tables_path(cat, &[schema.to_string()])
    }

    /// Tables in an arbitrarily nested schema path.
    pub fn tables_path(
        &mut self,
        cat: &AttachedCatalog,
        path: &[String],
    ) -> Result<Vec<TableInfo>> {
        call_items(
            self.transport_mut(),
            "catalog_schema_contents_tables",
            p::CatalogSchemaContentsTablesParams {
                attach_opaque_data: cat.handle.clone(),
                path: path.to_vec(),
                transaction_opaque_data: cat.txn(),
            },
        )
    }

    /// Views in a schema.
    pub fn views(&mut self, cat: &AttachedCatalog, schema: &str) -> Result<Vec<ViewInfo>> {
        self.views_path(cat, &[schema.to_string()])
    }

    /// Views in an arbitrarily nested schema path.
    pub fn views_path(&mut self, cat: &AttachedCatalog, path: &[String]) -> Result<Vec<ViewInfo>> {
        call_items(
            self.transport_mut(),
            "catalog_schema_contents_views",
            p::CatalogSchemaContentsViewsParams {
                attach_opaque_data: cat.handle.clone(),
                path: path.to_vec(),
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
        self.functions_path(cat, &[schema.to_string()], kind)
    }

    /// Functions of one kind in an arbitrarily nested schema path.
    pub fn functions_path(
        &mut self,
        cat: &AttachedCatalog,
        path: &[String],
        kind: FunctionKind,
    ) -> Result<Vec<FunctionInfo>> {
        call_items(
            self.transport_mut(),
            "catalog_schema_contents_functions",
            p::CatalogSchemaContentsFunctionsParams {
                attach_opaque_data: cat.handle.clone(),
                path: path.to_vec(),
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
        self.macros_path(cat, &[schema.to_string()], kind)
    }

    /// Macros of one kind in an arbitrarily nested schema path.
    pub fn macros_path(
        &mut self,
        cat: &AttachedCatalog,
        path: &[String],
        kind: MacroKind,
    ) -> Result<Vec<MacroInfo>> {
        call_items(
            self.transport_mut(),
            "catalog_schema_contents_macros",
            p::CatalogSchemaContentsMacrosParams {
                attach_opaque_data: cat.handle.clone(),
                path: path.to_vec(),
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
        self.table_get_path(cat, &[schema.to_string()], name, at)
    }

    /// One table in an arbitrarily nested schema path.
    pub fn table_get_path(
        &mut self,
        cat: &AttachedCatalog,
        schema_path: &[String],
        name: &str,
        at: Option<&At>,
    ) -> Result<Option<TableInfo>> {
        let items: Vec<TableInfo> = call_items(
            self.transport_mut(),
            "catalog_table_get",
            p::CatalogTableGetParams {
                attach_opaque_data: cat.handle.clone(),
                schema_path: schema_path.to_vec(),
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
    /// ([`BindSpec::with_raw_arguments`](crate::BindSpec::with_raw_arguments)) rather than decoded and rebuilt,
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
                schema_path: table.schema_path.clone(),
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
                schema_path: table.schema_path.clone(),
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
        self.table_column_statistics_path(cat, &[schema.to_string()], name)
    }

    /// Optimizer statistics for a table in an arbitrarily nested schema path.
    pub fn table_column_statistics_path(
        &mut self,
        cat: &AttachedCatalog,
        schema_path: &[String],
        name: &str,
    ) -> Result<RecordBatch> {
        call_batch(
            self.transport_mut(),
            "catalog_table_column_statistics_get",
            p::CatalogTableColumnStatisticsGetParams {
                attach_opaque_data: cat.handle.clone(),
                schema_path: schema_path.to_vec(),
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
            schema_path: legacy.schema_path,
            source_catalog: None,
            source_schema_path: None,
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

    fn spec_with_flags(required: bool, secret: bool) -> AttachOptionSpec {
        let raw = vgi::catalog::serialize_attach_option_spec_with_flags(
            "api_key",
            "API key",
            &DataType::Utf8,
            None,
            vgi::catalog::AttachOptionFlags { required, secret },
        )
        .unwrap();
        AttachOptionSpec::decode(&raw).unwrap()
    }

    #[test]
    fn secret_round_trips_true_and_false() {
        assert!(spec_with_flags(false, true).secret);
        assert!(!spec_with_flags(false, false).secret);
        // The plain serializer never marks an option secret.
        let raw =
            vgi::catalog::serialize_attach_option_spec("region", "", &DataType::Utf8, None, false)
                .unwrap();
        assert!(!AttachOptionSpec::decode(&raw).unwrap().secret);
    }

    #[test]
    fn secret_and_required_are_independent() {
        for required in [false, true] {
            for secret in [false, true] {
                let spec = spec_with_flags(required, secret);
                assert_eq!((spec.required, spec.secret), (required, secret));
            }
        }
    }

    /// Build a spec batch by hand, with or without a trailing `secret` column.
    fn hand_built_spec(secret: Option<Option<bool>>) -> Vec<u8> {
        let type_schema = Schema::new(vec![Field::new("value", DataType::Utf8, true)]);
        let type_bytes = ipc::write_schema_ref(&Arc::new(type_schema)).unwrap();
        let mut fields = vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, false),
            Field::new("type", DataType::Binary, false),
            Field::new("default_value", DataType::Binary, true),
            Field::new("required", DataType::Boolean, true),
        ];
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec!["api_key"])),
            Arc::new(StringArray::from(vec![""])),
            Arc::new(BinaryArray::from(vec![type_bytes.as_slice()])),
            Arc::new(BinaryArray::from(vec![None as Option<&[u8]>])),
            Arc::new(BooleanArray::from(vec![true])),
        ];
        if let Some(value) = secret {
            fields.push(Field::new("secret", DataType::Boolean, true));
            cols.push(Arc::new(BooleanArray::from(vec![value])));
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap();
        ipc::write_batch(&batch).unwrap()
    }

    #[test]
    fn missing_or_null_secret_column_reads_false() {
        let legacy = AttachOptionSpec::decode(&hand_built_spec(None)).unwrap();
        assert!(!legacy.secret);
        assert!(legacy.required, "an older peer's required flag still reads");
        assert!(
            !AttachOptionSpec::decode(&hand_built_spec(Some(None)))
                .unwrap()
                .secret
        );
        assert!(
            AttachOptionSpec::decode(&hand_built_spec(Some(Some(true))))
                .unwrap()
                .secret
        );
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
                        schema_path: Some(vec!["legacy_schema".to_string()]),
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
            supports_catalog_contents: false,
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
            schema_path: vec!["data".to_string()],
            columns: Bytes(Vec::new()),
            not_null_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            primary_key_constraints: Vec::new(),
            foreign_key_constraints: Vec::new(),
            write_result_modes: Vec::new(),
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
        assert_eq!(
            first.branches[0].schema_path.as_deref(),
            Some(["legacy_schema".to_string()].as_slice()),
            "scan_branches_from_legacy must carry the legacy response's schema_path through, not drop it"
        );
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
            secret: true,
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
mod setting_spec_tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn encoded_setting(default: Option<&RecordBatch>) -> Vec<u8> {
        let type_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            true,
        )]));
        let type_bytes = vgi_protocol::ipc::write_schema_ref(&type_schema).unwrap();
        let default_bytes = default.map(|batch| vgi_protocol::ipc::write_batch(batch).unwrap());
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, false),
            Field::new("type", DataType::Binary, false),
            Field::new("default_value", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["multiplier"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Multiplier"])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![type_bytes.as_slice()])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![default_bytes.as_deref()])) as ArrayRef,
            ],
        )
        .unwrap();
        vgi_protocol::ipc::write_batch(&batch).unwrap()
    }

    #[test]
    fn decodes_typed_setting_and_default() {
        let default = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap();
        let spec = SettingSpec::decode(&encoded_setting(Some(&default))).unwrap();
        assert_eq!(spec.name, "multiplier");
        assert_eq!(spec.data_type, DataType::Int64);
        assert_eq!(
            spec.default_value
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            7
        );
    }

    #[test]
    fn rejects_a_default_with_the_wrong_type() {
        let default = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["seven"]))],
        )
        .unwrap();
        assert!(SettingSpec::decode(&encoded_setting(Some(&default)))
            .unwrap_err()
            .message
            .contains("does not match"));
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
            schema_path: vec!["main".to_string()],
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

/// `load_catalog` against a scripted worker: the protocol-violation and
/// version-ordering branches a real worker cannot be made to take on demand.
#[cfg(test)]
mod load_catalog_tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use vgi_protocol::{ipc, wire};

    use crate::transport::{ExchangeStream, ProducerStream, VgiTransport};

    use super::*;

    type Reply = Box<dyn FnOnce() -> Result<RecordBatch> + Send>;

    /// Answers `catalog_contents` from a script; the per-schema calls with one
    /// empty `main` schema whose counts are all 0 (so no per-kind calls).
    struct Scripted {
        contents: VecDeque<Reply>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    fn schema_item(path: &[&str]) -> Bytes {
        let info = SchemaInfo {
            comment: None,
            tags: Vec::new(),
            attach_opaque_data: Bytes(vec![1]),
            path: path.iter().map(|s| s.to_string()).collect(),
            estimated_object_count: Some(
                [
                    "table",
                    "view",
                    "macro",
                    "index",
                    "scalar_function",
                    "aggregate_function",
                    "table_function",
                ]
                .iter()
                .map(|k| (k.to_string(), 0))
                .collect(),
            ),
        };
        Bytes(ipc::write_batch(&wire::to_batch(info).unwrap()).unwrap())
    }

    fn entry(path: &[&str], schema_path: &[&str]) -> SchemaContents {
        SchemaContents {
            path: path.iter().map(|s| s.to_string()).collect(),
            schema: schema_item(schema_path),
            tables: Vec::new(),
            views: Vec::new(),
            scalar_functions: Vec::new(),
            aggregate_functions: Vec::new(),
            table_functions: Vec::new(),
            scalar_macros: Vec::new(),
            table_macros: Vec::new(),
            indexes: Vec::new(),
        }
    }

    fn reply(resp: CatalogContentsResponse) -> Reply {
        Box::new(move || wire::to_result_batch(resp))
    }

    fn full(version: i64, etag: Option<&str>, schemas: Vec<SchemaContents>) -> Reply {
        reply(CatalogContentsResponse {
            catalog_version: version,
            etag: etag.map(str::to_string),
            not_modified: false,
            schemas,
        })
    }

    impl VgiTransport for Scripted {
        fn call_unary(&mut self, method: &str, _params: &RecordBatch) -> Result<RecordBatch> {
            self.calls.lock().unwrap().push(method.to_string());
            match method {
                "catalog_contents" => (self
                    .contents
                    .pop_front()
                    .expect("an unscripted catalog_contents call"))(
                ),
                "catalog_schemas" => {
                    wire::to_result_batch(vgi_protocol::protocol::dtos::ItemsResult {
                        items: vec![schema_item(&["main"])],
                    })
                }
                other => Err(RpcError::runtime_error(format!("unexpected call {other}"))),
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
            "scripted"
        }
    }

    fn client(script: Vec<Reply>) -> (VgiClient, Arc<Mutex<Vec<String>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let t = Scripted {
            contents: script.into(),
            calls: calls.clone(),
        };
        (VgiClient::new(Box::new(t)), calls)
    }

    fn attached(supports: bool) -> AttachedCatalog {
        let info = CatalogAttachResult {
            attach_opaque_data: Bytes(vec![1]),
            supports_transactions: false,
            supports_time_travel: false,
            catalog_version_frozen: false,
            catalog_version: 1,
            attach_opaque_data_required: true,
            default_schema: "main".to_string(),
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
            supports_catalog_contents: supports,
        };
        AttachedCatalog {
            handle: info.attach_opaque_data.clone(),
            info,
            transaction: None,
            scan_branches_capability: Arc::new(AtomicU8::new(BRANCHES_CAPABILITY_UNKNOWN)),
        }
    }

    fn calls(c: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
        std::mem::take(&mut *c.lock().unwrap())
    }

    #[test]
    fn a_path_that_disagrees_with_its_schema_info_falls_back() {
        let (mut client, log) = client(vec![full(2, None, vec![entry(&["main"], &["other"])])]);
        let snap = client.load_catalog(&attached(true), None).unwrap();
        assert_eq!(snap.source, CatalogLoadSource::PerSchema);
        assert!(snap
            .fallback_reason
            .unwrap()
            .contains("differs from its SchemaInfo.path"));
        assert_eq!(calls(&log), ["catalog_contents", "catalog_schemas"]);
    }

    #[test]
    fn an_older_snapshot_is_retried_once_then_falls_back() {
        let previous = CatalogSnapshot {
            schemas: Vec::new(),
            catalog_version: Some(5),
            etag: Some("e5".to_string()),
            source: CatalogLoadSource::CatalogContents,
            not_modified: false,
            fallback_reason: None,
        };
        // Older, then current: the retry wins.
        let (mut c, log) = client(vec![
            full(4, Some("e4"), vec![entry(&["main"], &["main"])]),
            full(6, Some("e6"), vec![entry(&["main"], &["main"])]),
        ]);
        let snap = c.load_catalog(&attached(true), Some(&previous)).unwrap();
        assert_eq!(snap.catalog_version, Some(6));
        assert_eq!(snap.etag.as_deref(), Some("e6"));
        assert_eq!(calls(&log), ["catalog_contents", "catalog_contents"]);
        // Older twice: per-schema.
        let (mut c, log) = client(vec![
            full(4, Some("e4"), vec![entry(&["main"], &["main"])]),
            full(3, Some("e3"), vec![entry(&["main"], &["main"])]),
        ]);
        let snap = c.load_catalog(&attached(true), Some(&previous)).unwrap();
        assert_eq!(snap.source, CatalogLoadSource::PerSchema);
        assert!(snap
            .fallback_reason
            .unwrap()
            .contains("older than the known version 5"));
        assert_eq!(
            calls(&log),
            ["catalog_contents", "catalog_contents", "catalog_schemas"]
        );
        // Version 0 is "unknown", never older.
        let (mut c, _) = client(vec![full(0, None, vec![entry(&["main"], &["main"])])]);
        let snap = c.load_catalog(&attached(true), Some(&previous)).unwrap();
        assert_eq!(snap.source, CatalogLoadSource::CatalogContents);
    }

    #[test]
    fn not_modified_keeps_previous_and_an_unasked_one_is_rejected() {
        let previous = CatalogSnapshot {
            schemas: vec![SchemaSnapshot::decode(&entry(&["main"], &["main"])).unwrap()],
            catalog_version: Some(2),
            etag: Some("e2".to_string()),
            source: CatalogLoadSource::CatalogContents,
            not_modified: false,
            fallback_reason: None,
        };
        let nm = |etag: &str| {
            reply(CatalogContentsResponse {
                catalog_version: 2,
                etag: Some(etag.to_string()),
                not_modified: true,
                schemas: Vec::new(),
            })
        };
        let (mut c, _) = client(vec![nm("e2")]);
        let kept = c.load_catalog(&attached(true), Some(&previous)).unwrap();
        assert!(kept.not_modified);
        assert_eq!(kept.schemas.len(), 1);
        // not_modified with no if_none_match sent: a protocol violation → fallback.
        let (mut c, _) = client(vec![nm("e2")]);
        let snap = c.load_catalog(&attached(true), None).unwrap();
        assert_eq!(snap.source, CatalogLoadSource::PerSchema);
        assert!(snap.fallback_reason.is_some());
        // A per-schema `previous` has no etag, so none is sent.
        let per_schema = CatalogSnapshot {
            source: CatalogLoadSource::PerSchema,
            etag: Some("stale".to_string()),
            ..previous.clone()
        };
        let (mut c, _) = client(vec![nm("stale")]);
        let snap = c.load_catalog(&attached(true), Some(&per_schema)).unwrap();
        assert_eq!(
            snap.source,
            CatalogLoadSource::PerSchema,
            "not_modified to no etag"
        );
    }

    #[test]
    fn not_advertised_never_sends_and_raw_call_refuses() {
        let (mut c, log) = client(Vec::new());
        let snap = c.load_catalog(&attached(false), None).unwrap();
        assert_eq!(snap.source, CatalogLoadSource::PerSchema);
        assert_eq!(snap.fallback_reason, None);
        assert!(c.contents_response(&attached(false), None).is_err());
        assert_eq!(calls(&log), ["catalog_schemas"]);
    }

    #[test]
    fn decodes_every_kind() {
        let table = TableInfo {
            comment: None,
            tags: Vec::new(),
            name: "t".to_string(),
            schema_path: vec!["main".to_string()],
            columns: Bytes(
                ipc::write_schema(&arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "x",
                    DataType::Int64,
                    true,
                )]))
                .unwrap(),
            ),
            not_null_constraints: Vec::new(),
            unique_constraints: Vec::new(),
            check_constraints: Vec::new(),
            primary_key_constraints: Vec::new(),
            foreign_key_constraints: Vec::new(),
            write_result_modes: Vec::new(),
            supports_column_statistics: false,
            scan_function: None,
            insert_function: None,
            update_function: None,
            delete_function: None,
            cardinality_estimate: None.into(),
            cardinality_max: None.into(),
            column_statistics: None,
            bind_result: None,
            required_filters: Vec::new(),
        };
        let index = IndexInfo {
            comment: None,
            tags: Vec::new(),
            name: "ix".to_string(),
            schema_path: vec!["main".to_string()],
            table_name: "t".to_string(),
            index_type: "ART".to_string(),
            constraint_type: DictString("NONE".to_string()),
            expressions: vec!["x".to_string()],
            options: Vec::new(),
        };
        let enc = |b: RecordBatch| Bytes(ipc::write_batch(&b).unwrap());
        let mut e = entry(&["main"], &["main"]);
        e.tables = vec![enc(wire::to_batch(table).unwrap())];
        e.indexes = vec![enc(wire::to_batch(index).unwrap())];
        let snap = SchemaSnapshot::decode(&e).unwrap();
        assert_eq!(snap.tables[0].name, "t");
        assert_eq!(snap.indexes[0].name, "ix");
        // A corrupt item is an error naming the kind.
        e.views = vec![Bytes(vec![1, 2, 3])];
        let err = SchemaSnapshot::decode(&e).unwrap_err();
        assert!(err.message.contains("views"), "{}", err.message);
    }
}

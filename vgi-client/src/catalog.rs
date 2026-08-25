// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Attaching a catalog and discovering what it holds.

use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, RecordBatch, StringArray};
use arrow_schema::DataType;
use vgi_protocol::generated::request_params as p;
use vgi_protocol::protocol::dtos::{
    AttachCatalogInfo, CatalogAttachRequest, CatalogAttachResult, CatalogInfo,
    CatalogTransactionBeginResult, CatalogVersionResult, FunctionInfo, MacroInfo,
    ScanFunctionResult, SchemaInfo, TableInfo, ViewInfo,
};
use vgi_rpc::errors::{Result, RpcError};
use vgi_rpc::{Bytes, DictString};

use crate::client::VgiClient;
use crate::wire_call::{call, call_items, call_unit, envelope};

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

#[cfg(test)]
mod attach_option_tests {
    use std::sync::Arc;

    use arrow_array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

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

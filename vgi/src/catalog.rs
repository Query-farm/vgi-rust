//! Default read-only catalog: auto-generates `SchemaInfo` + `FunctionInfo`
//! from the worker's registered functions (port of Go's
//! `DefaultReadOnlyCatalog` + `BuildArgSchema`).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use vgi_rpc::{Bytes, DictString, Result};

use crate::function::{ArgSpec, FunctionMetadata, ScalarFunction};
use crate::ipc;
use crate::protocol::dtos::{FunctionInfo, SchemaInfo};
use crate::protocol::enums;

/// The default schema name every registered function lives under.
pub const MAIN_SCHEMA: &str = "main";

/// A secret type the worker registers (surfaced in `catalog_attach`).
#[derive(Clone)]
pub struct SecretTypeSpec {
    pub name: String,
    pub description: String,
    /// Parameter schema; mark sensitive fields with metadata `redact=true`.
    pub parameters_schema: Arc<Schema>,
}

/// A DuckDB custom setting the worker registers (surfaced in `catalog_attach`).
#[derive(Clone)]
pub struct SettingSpec {
    pub name: String,
    pub description: String,
    pub data_type: DataType,
}

/// Serialize a [`SecretTypeSpec`] to its IPC `secret_types` entry.
pub fn serialize_secret_type(spec: &SecretTypeSpec) -> Result<Vec<u8>> {
    use crate::protocol::dtos::SecretTypeWire;
    let wire = SecretTypeWire {
        name: spec.name.clone(),
        description: spec.description.clone(),
        parameters_schema: Bytes::from(ipc::write_schema_ref(&spec.parameters_schema)?),
    };
    ipc::write_batch(&crate::wire::to_batch(wire)?)
}

/// Serialize a [`SettingSpec`] to its IPC `settings` entry. The batch schema is
/// `{name: string, description: string, type: binary, default_value: binary?}`
/// where `type` is the IPC schema of a single `value` field of the setting's
/// type (matches Go `serializeSettingSpec`).
pub fn serialize_setting(spec: &SettingSpec) -> Result<Vec<u8>> {
    use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
    let type_schema = Arc::new(Schema::new(vec![Field::new("value", spec.data_type.clone(), true)]));
    let type_bytes = ipc::write_schema_ref(&type_schema)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("type", DataType::Binary, false),
        Field::new("default_value", DataType::Binary, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![spec.name.clone()])),
        Arc::new(StringArray::from(vec![spec.description.clone()])),
        Arc::new(BinaryArray::from(vec![type_bytes.as_slice()])),
        Arc::new(BinaryArray::from(vec![None as Option<&[u8]>])),
    ];
    let batch =
        RecordBatch::try_new(schema, cols).map_err(|e| vgi_rpc::RpcError::runtime_error(e.to_string()))?;
    ipc::write_batch(&batch)
}

/// Build the wire arg schema (`FunctionInfo.arguments`) from arg specs,
/// attaching `vgi_*` field-metadata markers exactly as Go's `BuildArgSchema`.
pub fn build_arg_schema(specs: &[ArgSpec]) -> Schema {
    if specs.is_empty() {
        return Schema::empty();
    }
    let mut fields = Vec::with_capacity(specs.len());
    for spec in specs {
        let mut ty = spec
            .arrow_data_type
            .clone()
            .unwrap_or_else(|| arg_type_to_arrow(&spec.arrow_type));
        let mut meta: HashMap<String, String> = HashMap::new();

        if spec.position < 0 {
            meta.insert("vgi_arg".to_string(), "named".to_string());
        }

        if !spec.is_const {
            match spec.arrow_type.as_str() {
                "table" => {
                    meta.insert("vgi_type".to_string(), "table".to_string());
                }
                "any" | "" => {
                    meta.insert("vgi_type".to_string(), "any".to_string());
                    ty = DataType::Null;
                }
                _ => {}
            }
        } else if spec.arrow_data_type.is_none()
            && matches!(spec.arrow_type.as_str(), "struct" | "any" | "")
        {
            meta.insert("vgi_type".to_string(), "any".to_string());
            ty = DataType::Null;
        }

        if spec.is_const {
            meta.insert("vgi_const".to_string(), "true".to_string());
        }
        if spec.is_varargs {
            meta.insert("vgi_varargs".to_string(), "true".to_string());
        }

        let mut field = Field::new(&spec.name, ty, false);
        if !meta.is_empty() {
            field = field.with_metadata(meta);
        }
        fields.push(field);
    }
    Schema::new(fields)
}

/// Public wrapper for [`arg_type_to_arrow`] (used by overload scoring).
pub fn arg_type_to_arrow_pub(t: &str) -> DataType {
    arg_type_to_arrow(t)
}

fn arg_type_to_arrow(t: &str) -> DataType {
    match t {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" | "float" => DataType::Float32,
        "float64" | "double" => DataType::Float64,
        "bool" | "boolean" => DataType::Boolean,
        "varchar" | "string" | "utf8" => DataType::Utf8,
        "blob" | "binary" => DataType::Binary,
        _ => DataType::Null,
    }
}

/// A `FunctionInfo` with all the non-essential fields set to their canonical
/// defaults; callers override `name`, `function_type`, `arguments`,
/// `output_schema`, and the descriptive fields.
pub fn default_function_info(name: &str, function_type: &str) -> FunctionInfo {
    FunctionInfo {
        comment: None,
        tags: Vec::new(),
        name: name.to_string(),
        schema_name: MAIN_SCHEMA.to_string(),
        function_type: enums::dict(function_type),
        arguments: Bytes::from(Vec::new()),
        output_schema: Bytes::from(Vec::new()),
        stability: None,
        null_handling: None,
        description: String::new(),
        examples: Vec::new(),
        categories: Vec::new(),
        projection_pushdown: None,
        filter_pushdown: None,
        sampling_pushdown: None,
        late_materialization: None,
        supported_expression_filters: Vec::new(),
        order_preservation: None,
        max_workers: 0,
        supports_batch_index: false,
        partition_kind: enums::dict(enums::partition_kind::NOT_PARTITIONED),
        order_dependent: enums::dict(enums::order_dependence::NOT_ORDER_DEPENDENT),
        distinct_dependent: enums::dict(enums::distinct_dependence::NOT_DISTINCT_DEPENDENT),
        supports_window: false,
        streaming_partitioned: false,
        has_finalize: false,
        source_order_dependent: false,
        sink_order_dependent: false,
        requires_input_batch_index: false,
        required_settings: Vec::new(),
        required_secrets: Vec::new(),
    }
}

/// Apply common metadata fields onto a `FunctionInfo`.
fn apply_metadata(fi: &mut FunctionInfo, meta: &FunctionMetadata) {
    fi.description = meta.description.clone();
    fi.stability = meta.stability.as_deref().map(enums::dict);
    fi.null_handling = meta.null_handling.as_deref().map(enums::dict);
    fi.categories = meta.categories.clone();
    if meta.projection_pushdown {
        fi.projection_pushdown = Some(true);
    }
    if meta.filter_pushdown {
        fi.filter_pushdown = Some(true);
    }
    if meta.sampling_pushdown {
        fi.sampling_pushdown = Some(true);
    }
    fi.supports_batch_index = meta.supports_batch_index;
    if let Some(pk) = &meta.partition_kind {
        fi.partition_kind = enums::dict(pk);
    }
    fi.order_preservation = meta.order_preservation.as_deref().map(enums::dict);
    fi.sink_order_dependent = meta.sink_order_dependent;
    fi.source_order_dependent = meta.source_order_dependent;
    fi.requires_input_batch_index = meta.requires_input_batch_index;
    fi.required_settings = meta.required_settings.clone();
}

/// Build the `FunctionInfo` for a scalar function.
pub fn scalar_function_info(f: &dyn ScalarFunction) -> Result<FunctionInfo> {
    let meta = f.metadata();
    let mut fi = default_function_info(f.name(), enums::function_type::SCALAR);
    apply_metadata(&mut fi, &meta);

    let arg_schema = build_arg_schema(&f.argument_specs());
    fi.arguments = Bytes::from(ipc::write_schema(&arg_schema)?);

    // Scalar functions need a 1-field output schema for DuckDB. Use the fixed
    // return type if declared, else a `result: null` placeholder carrying the
    // `vgi:any` marker so DuckDB defers the type to bind.
    let out_schema = match &meta.return_type {
        Some(ty) => Schema::new(vec![Field::new("result", ty.clone(), true)]),
        None => {
            let mut m = HashMap::new();
            m.insert("vgi:any".to_string(), "true".to_string());
            Schema::new(vec![Field::new("result", DataType::Null, false).with_metadata(m)])
        }
    };
    fi.output_schema = Bytes::from(ipc::write_schema(&out_schema)?);
    Ok(fi)
}

/// Build the `FunctionInfo` for a table (producer) function.
pub fn table_function_info(f: &dyn crate::table_function::TableFunction) -> Result<FunctionInfo> {
    let meta = f.metadata();
    let mut fi = default_function_info(f.name(), enums::function_type::TABLE);
    apply_metadata(&mut fi, &meta);
    let arg_schema = build_arg_schema(&f.argument_specs());
    fi.arguments = Bytes::from(ipc::write_schema(&arg_schema)?);
    // Output schema is resolved at bind time; advertise an empty schema.
    fi.output_schema = Bytes::from(ipc::write_schema(&Schema::empty())?);
    Ok(fi)
}

/// Build the `FunctionInfo` for a table-in-out function (a DuckDB table fn).
pub fn table_in_out_function_info(
    f: &dyn crate::table_in_out::TableInOutFunction,
) -> Result<FunctionInfo> {
    let meta = f.metadata();
    let mut fi = default_function_info(f.name(), enums::function_type::TABLE);
    apply_metadata(&mut fi, &meta);
    fi.has_finalize = false;
    let arg_schema = build_arg_schema(&f.argument_specs());
    fi.arguments = Bytes::from(ipc::write_schema(&arg_schema)?);
    fi.output_schema = Bytes::from(ipc::write_schema(&Schema::empty())?);
    Ok(fi)
}

/// Build the `FunctionInfo` for a table-buffering function.
pub fn buffering_function_info(
    f: &dyn crate::buffering::TableBufferingFunction,
) -> Result<FunctionInfo> {
    let meta = f.metadata();
    let mut fi = default_function_info(f.name(), enums::function_type::TABLE_BUFFERING);
    apply_metadata(&mut fi, &meta);
    fi.has_finalize = true;
    let arg_schema = build_arg_schema(&f.argument_specs());
    fi.arguments = Bytes::from(ipc::write_schema(&arg_schema)?);
    fi.output_schema = Bytes::from(ipc::write_schema(&Schema::empty())?);
    Ok(fi)
}

/// Build the `FunctionInfo` for an aggregate function. Aggregates must
/// advertise a 1-field output schema at discovery time, so resolve it via
/// `on_bind` with empty params (a fixed-return aggregate ignores them).
pub fn aggregate_function_info(f: &dyn crate::aggregate::AggregateFunction) -> Result<FunctionInfo> {
    let meta = f.metadata();
    let mut fi = default_function_info(f.name(), enums::function_type::AGGREGATE);
    apply_metadata(&mut fi, &meta);
    let arg_schema = build_arg_schema(&f.argument_specs());
    fi.arguments = Bytes::from(ipc::write_schema(&arg_schema)?);
    let params = crate::aggregate::AggregateBindParams {
        arguments: crate::arguments::Arguments::default(),
        input_schema: None,
        settings: crate::settings::Settings::default(),
    };
    let out = match f.on_bind(&params) {
        Ok(b) => b.output_schema,
        Err(_) => Arc::new(Schema::new(vec![Field::new("result", DataType::Null, true)])),
    };
    fi.output_schema = Bytes::from(ipc::write_schema_ref(&out)?);
    Ok(fi)
}

/// The default `SchemaInfo` for the `main` schema.
pub fn main_schema_info(attach_opaque_data: &[u8]) -> SchemaInfo {
    schema_info(MAIN_SCHEMA, Some("Default schema containing all registered functions"), attach_opaque_data)
}

/// Build a `SchemaInfo` for an arbitrary schema.
pub fn schema_info(name: &str, comment: Option<&str>, attach_opaque_data: &[u8]) -> SchemaInfo {
    SchemaInfo {
        comment: comment.map(|s| s.to_string()),
        tags: Vec::new(),
        attach_opaque_data: Bytes::from(attach_opaque_data.to_vec()),
        name: name.to_string(),
        estimated_object_count: None,
    }
}

// ---------------------------------------------------------------------------
// Declarative catalog model (views / macros / function-backed tables)
// ---------------------------------------------------------------------------

/// A declarative catalog: named schemas with views, macros, and tables.
#[derive(Default, Clone)]
pub struct CatalogModel {
    pub schemas: Vec<CatSchema>,
    /// Database-level comment (surfaced via `duckdb_databases().comment`).
    pub comment: Option<String>,
    /// Database-level tags (surfaced via `duckdb_databases().tags`).
    pub tags: Vec<(String, String)>,
    /// Whether the catalog supports time-travel (`AT`) queries.
    pub supports_time_travel: bool,
}

#[derive(Clone)]
pub struct CatSchema {
    pub name: String,
    pub comment: Option<String>,
    pub views: Vec<CatView>,
    pub macros: Vec<CatMacro>,
    pub tables: Vec<CatTable>,
}

#[derive(Clone)]
pub struct CatView {
    pub name: String,
    pub definition: String,
    pub comment: Option<String>,
    pub tags: Vec<(String, String)>,
    pub column_comments: Vec<(String, String)>,
}

#[derive(Clone)]
pub struct CatMacro {
    pub name: String,
    pub parameters: Vec<String>,
    pub definition: String,
    pub table_macro: bool,
    pub comment: Option<String>,
    /// Default values for trailing parameters: `(param_name, int64 default)`.
    pub defaults: Vec<(String, i64)>,
}

/// A function-backed catalog table: scanned by `scan_function(scan_args)`.
#[derive(Clone)]
pub struct CatTable {
    pub name: String,
    pub columns: SchemaRef,
    pub scan_function: String,
    /// IPC-serialized `Arguments` for the scan function.
    pub scan_arguments: Vec<u8>,
    pub comment: Option<String>,
    pub cardinality: Option<i64>,
    pub not_null: Vec<i32>,
    pub primary_key: Vec<Vec<i32>>,
    pub unique: Vec<Vec<i32>>,
    pub check: Vec<String>,
    pub tags: Vec<(String, String)>,
    pub foreign_keys: Vec<ForeignKey>,
    /// When true, the scan function is inlined in `TableInfo.scan_function` and
    /// the C++ extension skips `catalog_table_scan_function_get`. When false
    /// (default), the scan is resolved lazily via that RPC.
    pub inline_scan: bool,
    /// Multi-branch sources. `Some` (even empty) overrides the single-branch
    /// default in `catalog_table_scan_branches_get`.
    pub branches: Option<Vec<CatBranch>>,
    /// Per-column optimizer statistics (served via
    /// `catalog_table_column_statistics_get`).
    pub statistics: Vec<crate::statistics::CatColStat>,
}

/// One physical branch of a multi-branch table.
#[derive(Clone)]
pub struct CatBranch {
    pub function_name: String,
    pub scan_arguments: Vec<u8>,
    pub branch_filter: Option<String>,
    pub writable: bool,
}

/// A foreign-key constraint (referenced table in the same schema by default).
#[derive(Clone)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

/// Serialize a foreign key to its IPC `foreign_key_constraints` entry.
pub fn serialize_foreign_key(schema: &str, fk: &ForeignKey) -> Result<Vec<u8>> {
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{ArrayRef, RecordBatch, StringArray};
    let list_of = |items: &[String]| -> ArrayRef {
        let mut b = ListBuilder::new(StringBuilder::new());
        for s in items {
            b.values().append_value(s);
        }
        b.append(true);
        Arc::new(b.finish())
    };
    let fields = vec![
        Field::new(
            "fk_columns",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "pk_columns",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("referenced_table", DataType::Utf8, true),
        Field::new("referenced_schema", DataType::Utf8, true),
    ];
    let cols: Vec<ArrayRef> = vec![
        list_of(&fk.columns),
        list_of(&fk.referenced_columns),
        Arc::new(StringArray::from(vec![fk.referenced_table.clone()])),
        Arc::new(StringArray::from(vec![schema.to_string()])),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
        .map_err(|e| vgi_rpc::RpcError::runtime_error(e.to_string()))?;
    ipc::write_batch(&batch)
}

impl CatTable {
    /// Constructor with the new metadata fields defaulted empty.
    pub fn new(
        name: &str,
        columns: SchemaRef,
        scan_function: &str,
        scan_arguments: Vec<u8>,
        comment: Option<String>,
        cardinality: Option<i64>,
    ) -> Self {
        CatTable {
            name: name.to_string(),
            columns,
            scan_function: scan_function.to_string(),
            scan_arguments,
            comment,
            cardinality,
            not_null: Vec::new(),
            primary_key: Vec::new(),
            unique: Vec::new(),
            check: Vec::new(),
            tags: Vec::new(),
            foreign_keys: Vec::new(),
            inline_scan: false,
            branches: None,
            statistics: Vec::new(),
        }
    }
}

impl CatalogModel {
    pub fn schema(&self, name: &str) -> Option<&CatSchema> {
        self.schemas.iter().find(|s| s.name == name)
    }
}

/// Build a `ViewInfo` DTO.
pub fn view_info(schema: &str, v: &CatView) -> crate::protocol::dtos::ViewInfo {
    crate::protocol::dtos::ViewInfo {
        comment: v.comment.clone(),
        tags: v.tags.clone(),
        name: v.name.clone(),
        schema_name: schema.to_string(),
        definition: v.definition.clone(),
        column_comments: v.column_comments.clone(),
    }
}

/// Build a `TableInfo` DTO for a function-backed catalog table, inlining the
/// scan function so DuckDB needn't call `catalog_table_scan_function_get`.
/// The flat `ScanFunctionResult` batch for a function-backed table (used both
/// for the inlined `TableInfo.scan_function` and the lazy
/// `catalog_table_scan_function_get` RPC response).
pub fn scan_function_result(t: &CatTable) -> Result<crate::protocol::dtos::ScanFunctionResult> {
    Ok(crate::protocol::dtos::ScanFunctionResult {
        function_name: t.scan_function.clone(),
        arguments: Bytes::from(t.scan_arguments.clone()),
        required_extensions: Vec::new(),
    })
}

pub fn table_info(schema: &str, t: &CatTable) -> Result<crate::protocol::dtos::TableInfo> {
    use crate::protocol::dtos::TableInfo;
    // Inline the scan function only for tables that opt in; otherwise the C++
    // extension resolves it lazily via `catalog_table_scan_function_get`.
    let scan = if t.inline_scan && !t.scan_function.is_empty() {
        ipc::write_batch(&crate::wire::to_batch(scan_function_result(t)?)?)?
    } else {
        Vec::new()
    };
    Ok(TableInfo {
        comment: t.comment.clone(),
        tags: t.tags.clone(),
        name: t.name.clone(),
        schema_name: schema.to_string(),
        columns: Bytes::from(ipc::write_schema_ref(&t.columns)?),
        not_null_constraints: t.not_null.clone(),
        unique_constraints: t.unique.clone(),
        check_constraints: t.check.clone(),
        primary_key_constraints: t.primary_key.clone(),
        foreign_key_constraints: t
            .foreign_keys
            .iter()
            .map(|fk| Ok(Bytes::from(serialize_foreign_key(schema, fk)?)))
            .collect::<Result<Vec<_>>>()?,
        supports_insert: false,
        supports_update: false,
        supports_delete: false,
        supports_returning: false,
        supports_column_statistics: !t.statistics.is_empty(),
        scan_function: Bytes::from(scan),
        insert_function: Bytes::from(Vec::new()),
        update_function: Bytes::from(Vec::new()),
        delete_function: Bytes::from(Vec::new()),
        cardinality_estimate: t.cardinality.unwrap_or(-1),
        cardinality_max: t.cardinality.unwrap_or(-1),
        column_statistics: Bytes::from(Vec::new()),
        bind_result: Bytes::from(Vec::new()),
    })
}

/// Build a `MacroInfo` DTO.
pub fn macro_info(schema: &str, m: &CatMacro) -> crate::protocol::dtos::MacroInfo {
    crate::protocol::dtos::MacroInfo {
        comment: m.comment.clone(),
        tags: Vec::new(),
        name: m.name.clone(),
        schema_name: schema.to_string(),
        macro_type: DictString(if m.table_macro { "table".into() } else { "scalar".into() }),
        parameters: m.parameters.clone(),
        parameter_default_values: Bytes::from(build_macro_defaults(&m.defaults).unwrap_or_default()),
        definition: m.definition.clone(),
    }
}

/// Build the 1-row IPC batch of macro parameter defaults (column per param).
fn build_macro_defaults(defaults: &[(String, i64)]) -> Result<Vec<u8>> {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    if defaults.is_empty() {
        return Ok(Vec::new());
    }
    let fields: Vec<Field> = defaults.iter().map(|(n, _)| Field::new(n, DataType::Int64, false)).collect();
    let cols: Vec<ArrayRef> = defaults.iter().map(|(_, v)| Arc::new(Int64Array::from(vec![*v])) as ArrayRef).collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
        .map_err(|e| vgi_rpc::RpcError::runtime_error(e.to_string()))?;
    ipc::write_batch(&batch)
}

use arrow_schema::SchemaRef;

/// Serialize a list of catalog item structs into `ItemsResult.items`.
pub fn serialize_items<T: vgi_rpc::VgiArrow>(items: Vec<T>) -> Result<Vec<Bytes>> {
    items
        .into_iter()
        .map(|item| {
            let batch = crate::wire::to_batch(item)?;
            Ok(Bytes::from(ipc::write_batch(&batch)?))
        })
        .collect()
}

/// Wrap a `DictString` enum value (re-export convenience).
pub fn dict(s: &str) -> DictString {
    enums::dict(s)
}

/// Build an `Arc<Schema>` from a `Schema` (convenience).
pub fn arc(schema: Schema) -> Arc<Schema> {
    Arc::new(schema)
}

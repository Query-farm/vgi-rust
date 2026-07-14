// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Default read-only catalog: auto-generates `SchemaInfo` + `FunctionInfo`
//! from the worker's registered functions.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use vgi_rpc::{Bytes, DictString, Result};

use crate::function::{ArgSpec, FunctionMetadata, ScalarFunction};
use crate::ipc;
use crate::protocol::dtos::{FunctionInfo, RequiredSecret, SchemaInfo};
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

/// Serialize an [`AttachCatalogInfo`](crate::protocol::dtos::AttachCatalogInfo)
/// to its IPC `attach_catalogs` entry (companion catalog for lakehouse federation).
pub fn serialize_attach_catalog(
    info: &crate::protocol::dtos::AttachCatalogInfo,
) -> Result<Vec<u8>> {
    ipc::write_batch(&crate::wire::to_batch(info.clone())?)
}

/// Serialize a [`SettingSpec`] to its IPC `settings` entry. The batch schema is
/// `{name: string, description: string, type: binary, default_value: binary?}`
/// where `type` is the IPC schema of a single `value` field of the setting's
/// type.
/// Serialize one `CatalogInfo` discovery record to IPC bytes. The schema
/// (field order/types) must match `generated.CatalogInfoSchema` exactly; the
/// `releases` element struct type is emitted even when the list is empty.
pub fn serialize_catalog_info(model: &CatalogModel) -> Result<Vec<u8>> {
    use arrow_array::{
        ArrayRef, BinaryArray, ListArray, RecordBatch, StringArray, StructArray,
        TimestampMicrosecondArray,
    };
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::TimeUnit;

    let one_empty_list = |elem_field: Arc<Field>, values: ArrayRef| -> ArrayRef {
        let offsets = OffsetBuffer::new(vec![0i32, 0].into());
        Arc::new(ListArray::new(elem_field, offsets, values, None))
    };

    let name = Arc::new(StringArray::from(vec![model.name.clone()])) as ArrayRef;
    let impl_ver = Arc::new(StringArray::from(vec![model
        .implementation_version
        .clone()])) as ArrayRef;
    let data_ver = Arc::new(StringArray::from(vec![model.data_version_spec.clone()])) as ArrayRef;

    // attach_option_specs: list<binary>, one list of the serialized specs.
    let aos_field = Arc::new(Field::new("item", DataType::Binary, true));
    let specs: Vec<&[u8]> = model
        .attach_option_specs
        .iter()
        .map(|v| v.as_slice())
        .collect();
    let aos_values = Arc::new(BinaryArray::from(specs)) as ArrayRef;
    let aos_offsets = OffsetBuffer::new(vec![0i32, model.attach_option_specs.len() as i32].into());
    let aos = Arc::new(ListArray::new(aos_field, aos_offsets, aos_values, None)) as ArrayRef;

    // releases: list<struct{version,released_at,summary,notes_url}>, one empty.
    let rel_fields: arrow_schema::Fields = vec![
        Field::new("version", DataType::Utf8, false),
        Field::new(
            "released_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("summary", DataType::Utf8, false),
        Field::new("notes_url", DataType::Utf8, true),
    ]
    .into();
    let rel_values = Arc::new(StructArray::new(
        rel_fields.clone(),
        vec![
            Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new()).with_timezone("UTC")),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
        None,
    )) as ArrayRef;
    let rel_field = Arc::new(Field::new("item", DataType::Struct(rel_fields), true));
    let releases = one_empty_list(rel_field, rel_values);

    let source_url = Arc::new(StringArray::from(vec![model.source_url.clone()])) as ArrayRef;

    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("implementation_version", DataType::Utf8, true),
        Field::new("data_version_spec", DataType::Utf8, true),
        Field::new("attach_option_specs", aos.data_type().clone(), false),
        Field::new("releases", releases.data_type().clone(), false),
        Field::new("source_url", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![name, impl_ver, data_ver, aos, releases, source_url],
    )
    .map_err(|e| vgi_rpc::RpcError::runtime_error(e.to_string()))?;
    ipc::write_batch(&batch)
}

/// Serialize one `AttachOptionSpec` (discovery record for an ATTACH option).
/// Schema `{name:str, description:str, type:binary (IPC schema of a single
/// `value` field), default_value:binary? (IPC 1-row batch of the default)}`.
pub fn serialize_attach_option_spec(
    name: &str,
    description: &str,
    arrow_type: &DataType,
    default: Option<&arrow_array::ArrayRef>,
) -> Result<Vec<u8>> {
    use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
    let type_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        arrow_type.clone(),
        true,
    )]));
    let type_bytes = ipc::write_schema_ref(&type_schema)?;
    let default_bytes: Option<Vec<u8>> = match default {
        Some(arr) => {
            let b = RecordBatch::try_new(type_schema.clone(), vec![arr.clone()])
                .map_err(|e| vgi_rpc::RpcError::runtime_error(e.to_string()))?;
            Some(ipc::write_batch(&b)?)
        }
        None => None,
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("type", DataType::Binary, false),
        Field::new("default_value", DataType::Binary, true),
    ]));
    let cols: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![name])),
        Arc::new(StringArray::from(vec![description])),
        Arc::new(BinaryArray::from(vec![type_bytes.as_slice()])),
        Arc::new(BinaryArray::from(vec![default_bytes.as_deref()])),
    ];
    let batch = RecordBatch::try_new(schema, cols)
        .map_err(|e| vgi_rpc::RpcError::runtime_error(e.to_string()))?;
    ipc::write_batch(&batch)
}

pub fn serialize_setting(spec: &SettingSpec) -> Result<Vec<u8>> {
    use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
    let type_schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        spec.data_type.clone(),
        true,
    )]));
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
    let batch = RecordBatch::try_new(schema, cols)
        .map_err(|e| vgi_rpc::RpcError::runtime_error(e.to_string()))?;
    ipc::write_batch(&batch)
}

// Arrow field-metadata keys carrying per-argument constraint metadata for agent
// discovery. Presence-only, value-encoded as UTF-8. Kept byte-for-byte in sync
// with the C++ reader in the vgi extension and `vgi/argument_spec.py` in the
// Python reference:
//   vgi_default — JSON scalar (the arg's default value)
//   vgi_choices — JSON array (the closed set of allowed values)
//   vgi_range   — interval notation built from ge/le/gt/lt (e.g. "[0, 100]",
//                 "(0, +inf)", "[1, 10)"); a discovery surface, not raw bounds
//   vgi_pattern — raw regex the value must match (open set)
const VGI_DEFAULT_KEY: &str = "vgi_default";
const VGI_CHOICES_KEY: &str = "vgi_choices";
const VGI_RANGE_KEY: &str = "vgi_range";
const VGI_PATTERN_KEY: &str = "vgi_pattern";

/// JSON-encode a value, falling back to the JSON of its `Debug` form rather than
/// dropping the constraint registration if serialization somehow fails. A
/// `serde_json::Value` always serializes, so the fallback is defensive.
fn json_or_debug(value: &serde_json::Value) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| serde_json::Value::String(format!("{value:?}")).to_string())
}

/// Format a single numeric bound without a trailing `.0` for whole numbers
/// (e.g. `0` rather than `0.0`), keeping a decimal for genuinely fractional
/// bounds (e.g. `0.5`).
fn format_bound(v: f64) -> String {
    if v.is_finite() && v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// Build interval notation from an argument's numeric bounds. Inclusive bounds
/// (`ge`/`le`) render as square brackets, exclusive bounds (`gt`/`lt`) as
/// parentheses, and an open side as `-inf`/`+inf`. Returns `None` when the
/// argument has no numeric bound at all. Mirrors `_format_range` in the Python
/// reference (`vgi/argument_spec.py`).
pub fn format_range(
    ge: Option<f64>,
    le: Option<f64>,
    gt: Option<f64>,
    lt: Option<f64>,
) -> Option<String> {
    if ge.is_none() && le.is_none() && gt.is_none() && lt.is_none() {
        return None;
    }
    let low = if let Some(gt) = gt {
        format!("({}", format_bound(gt))
    } else if let Some(ge) = ge {
        format!("[{}", format_bound(ge))
    } else {
        "(-inf".to_string()
    };
    let high = if let Some(lt) = lt {
        format!("{})", format_bound(lt))
    } else if let Some(le) = le {
        format!("{}]", format_bound(le))
    } else {
        "+inf)".to_string()
    };
    Some(format!("{low}, {high}"))
}

/// Build the wire arg schema (`FunctionInfo.arguments`) from arg specs,
/// attaching `vgi_*` field-metadata markers.
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
                // Only treat as ANY when no explicit Arrow type was given
                // (`column_typed` sets arrow_type="" but a concrete
                // arrow_data_type — those must keep their real type so DuckDB
                // can disambiguate overloads like `type_info(int32 vs int64)`).
                "any" | "" if spec.arrow_data_type.is_none() => {
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
        // Per-argument description (UTF-8; presence-only — omit when empty).
        if !spec.doc.is_empty() {
            meta.insert("vgi_doc".to_string(), spec.doc.clone());
        }

        // Per-argument constraint metadata for agent discovery. All keys are
        // presence-only (omitted when the constraint is absent) and value-encoded
        // as UTF-8. Kept byte-for-byte in sync with the C++ reader in the vgi
        // extension and the Python reference in `vgi/argument_spec.py`.
        if let Some(default) = &spec.default {
            meta.insert(VGI_DEFAULT_KEY.to_string(), json_or_debug(default));
        }
        if let Some(choices) = &spec.choices {
            let value = serde_json::Value::Array(choices.clone());
            meta.insert(VGI_CHOICES_KEY.to_string(), json_or_debug(&value));
        }
        if let Some(range) = format_range(spec.ge, spec.le, spec.gt, spec.lt) {
            meta.insert(VGI_RANGE_KEY.to_string(), range);
        }
        if let Some(pattern) = &spec.pattern {
            meta.insert(VGI_PATTERN_KEY.to_string(), pattern.clone());
        }

        let mut field = Field::new(&spec.name, ty, false);
        if !meta.is_empty() {
            field = field.with_metadata(meta);
        }
        fields.push(field);
    }
    Schema::new(fields)
}

/// Public wrapper for `arg_type_to_arrow` (used by overload scoring).
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
        input_from_args: false, // vgi-rust has no blended (RowTransformFunction) functions
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
    fi.examples = meta.examples.clone();
    fi.tags = meta.tags.clone();
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
    fi.supports_window = meta.supports_window;
    fi.streaming_partitioned = meta.streaming_partitioned;
    fi.late_materialization = Some(meta.late_materialization);
    fi.required_settings = meta.required_settings.clone();
    fi.required_secrets = meta
        .required_secrets
        .iter()
        .map(|s| RequiredSecret {
            secret_type: s.secret_type.clone(),
            scope: s.scope.clone(),
            secret_name: s.name.clone(),
        })
        .collect();
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
            Schema::new(vec![
                Field::new("result", DataType::Null, false).with_metadata(m)
            ])
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
    fi.has_finalize = f.has_finish();
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
pub fn aggregate_function_info(
    f: &dyn crate::aggregate::AggregateFunction,
) -> Result<FunctionInfo> {
    let meta = f.metadata();
    let mut fi = default_function_info(f.name(), enums::function_type::AGGREGATE);
    apply_metadata(&mut fi, &meta);
    let arg_schema = build_arg_schema(&f.argument_specs());
    fi.arguments = Bytes::from(ipc::write_schema(&arg_schema)?);
    let params = crate::aggregate::AggregateBindParams {
        arguments: crate::arguments::Arguments::default(),
        input_schema: None,
        settings: crate::settings::Settings::default(),
        secrets: crate::secrets::Secrets::default(),
    };
    // A fixed `return_type` in metadata wins (covers aggregates whose `on_bind`
    // requires an input schema but still have a concrete output type).
    let out = match (&meta.return_type, f.on_bind(&params)) {
        (Some(ty), _) => Arc::new(Schema::new(vec![Field::new("result", ty.clone(), true)])),
        (None, Ok(b)) => b.output_schema,
        // on_bind needs the (unknown-at-registration) input schema — defer the
        // type to bind via the `vgi:any` marker so DuckDB reports it as ANY.
        (None, Err(_)) => {
            let mut m = HashMap::new();
            m.insert("vgi:any".to_string(), "true".to_string());
            Arc::new(Schema::new(vec![Field::new(
                "result",
                DataType::Null,
                false,
            )
            .with_metadata(m)]))
        }
    };
    fi.output_schema = Bytes::from(ipc::write_schema_ref(&out)?);
    Ok(fi)
}

/// The default `SchemaInfo` for the `main` schema.
pub fn main_schema_info(attach_opaque_data: &[u8]) -> SchemaInfo {
    schema_info(
        MAIN_SCHEMA,
        Some("Default schema containing all registered functions"),
        attach_opaque_data,
    )
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
    /// Catalog name advertised by `catalog_catalogs` (discovery).
    pub name: String,
    pub schemas: Vec<CatSchema>,
    /// Database-level comment (surfaced via `duckdb_databases().comment`).
    pub comment: Option<String>,
    /// Database-level tags (surfaced via `duckdb_databases().tags`).
    pub tags: Vec<(String, String)>,
    /// Provenance URL (repo / docs / dataset homepage) advertised via
    /// `catalog_catalogs().source_url` so consumers can verify provenance.
    pub source_url: Option<String>,
    /// Whether the catalog supports time-travel (`AT`) queries.
    pub supports_time_travel: bool,
    /// Worker software version (singular per worker). `None` = no opinion.
    pub implementation_version: Option<String>,
    /// Semver range the catalog serves (e.g. ">=1.0.0,<2.0.0").
    pub data_version_spec: Option<String>,
    /// Concrete data versions this worker accepts at ATTACH time.
    pub supported_data_versions: Vec<String>,
    /// The data version used when the client omits `data_version_spec`.
    pub default_data_version: Option<String>,
    /// Concrete implementation versions accepted at ATTACH (npm-resolved when
    /// `npm_version_resolution`). Empty → exact-match against
    /// `implementation_version`.
    pub supported_implementation_versions: Vec<String>,
    /// Use npm-style spec resolution (exact / bare / `^` / `~`) instead of
    /// exact-match for `data_version_spec` / `implementation_version`.
    pub npm_version_resolution: bool,
    /// Per-resolved-version schema sets. When non-empty the catalog's visible
    /// objects vary by the resolved data version (encoded in attach_opaque_data).
    pub version_schemas: std::collections::HashMap<String, Vec<CatSchema>>,
    /// Serialized `AttachOptionSpec` records (discovery via `catalog_catalogs`).
    pub attach_option_specs: Vec<Vec<u8>>,
    /// IPC of the one-row default option batch (one column per declared option).
    /// When set, `catalog_attach` merges the user `options` over it and encodes
    /// the result into `attach_opaque_data` (`<16-byte id>\0<ipc>`).
    pub attach_options_default_batch: Option<Vec<u8>>,
}

impl CatalogModel {
    /// The schema set visible for a given resolved data `version` (or the base
    /// schemas when the catalog is not version-shaped / the version is unknown).
    pub fn schemas_for(&self, version: Option<&str>) -> &[CatSchema] {
        version
            .and_then(|v| self.version_schemas.get(v))
            .map(|v| v.as_slice())
            .unwrap_or(&self.schemas)
    }
}

fn parse_semver(v: &str) -> Option<(u32, u32, u32)> {
    let mut it = v.split('.');
    let a = it.next()?.parse().ok()?;
    let b = it.next()?.parse().ok()?;
    let c = it.next()?.parse().ok()?;
    if it.next().is_some() {
        return None;
    }
    Some((a, b, c))
}

/// Resolve an npm-style version `spec` to a concrete `supported` version
/// (exact `X.Y.Z`, bare `X` / `X.Y`, caret `^X.Y.Z`, tilde `~X.Y.Z`). Returns
/// `default` when `spec` is `None`. `Err` when nothing matches.
pub fn resolve_version_npm(
    spec: Option<&str>,
    supported: &[String],
    default: &str,
    label: &str,
) -> Result<String> {
    let spec = match spec {
        None | Some("") => return Ok(default.to_string()),
        Some(s) => s,
    };
    let mut sorted: Vec<((u32, u32, u32), &String)> = supported
        .iter()
        .filter_map(|v| parse_semver(v).map(|t| (t, v)))
        .collect();
    sorted.sort();
    let unsupported = || {
        vgi_rpc::RpcError::value_error(format!(
            "Unsupported {label} {spec:?}; this worker serves {supported:?}"
        ))
    };

    // Exact X.Y.Z
    if parse_semver(spec).is_some() {
        return if supported.iter().any(|s| s == spec) {
            Ok(spec.to_string())
        } else {
            Err(unsupported())
        };
    }
    let nums: Vec<&str> = spec.trim_start_matches(['^', '~']).split('.').collect();
    let prefix = spec.chars().next().unwrap_or(' ');
    // Bare major `X`
    if !matches!(prefix, '^' | '~') && nums.len() == 1 {
        let major: u32 = nums[0].parse().map_err(|_| unsupported())?;
        return sorted
            .iter()
            .filter(|(t, _)| t.0 == major)
            .next_back()
            .map(|(_, v)| v.to_string())
            .ok_or_else(unsupported);
    }
    // Bare major.minor `X.Y` → pin to X.Y.0
    if !matches!(prefix, '^' | '~') && nums.len() == 2 {
        let pinned = format!("{}.{}.0", nums[0], nums[1]);
        return if supported.contains(&pinned) {
            Ok(pinned)
        } else {
            Err(unsupported())
        };
    }
    // Caret `^X.Y.Z` (same major, >= base) / Tilde `~X.Y.Z` (same major.minor, >= base)
    if matches!(prefix, '^' | '~') {
        if let Some(base) = parse_semver(spec.trim_start_matches(['^', '~'])) {
            return sorted
                .iter()
                .filter(|(t, _)| t.0 == base.0 && *t >= base && (prefix == '^' || t.1 == base.1))
                .next_back()
                .map(|(_, v)| v.to_string())
                .ok_or_else(unsupported);
        }
    }
    Err(unsupported())
}

#[derive(Default, Clone)]
pub struct CatSchema {
    pub name: String,
    pub comment: Option<String>,
    /// Schema-level tags (surfaced via `duckdb_schemas().tags`), e.g.
    /// `vgi.description_llm` / `vgi.description_md`.
    pub tags: Vec<(String, String)>,
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
    /// Per-parameter descriptions: `(param_name, description)`. Names must
    /// appear in `parameters`. Descriptions flow over the wire via the macro
    /// `arguments_schema`'s `vgi_doc` field metadata (the same channel functions
    /// use for per-argument docs), so the DuckDB extension's
    /// `vgi_function_arguments()` can surface them. Empty = no per-parameter docs.
    pub parameter_docs: Vec<(String, String)>,
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
    /// DuckDB extensions the scan branches require (e.g. `["iceberg"]`);
    /// surfaced as `ScanBranchesResult.required_extensions` and via the C++
    /// `vgi_table_branches().table_required_extensions` diagnostic. Empty = none.
    pub required_extensions: Vec<String>,
    /// Per-column optimizer statistics (served via
    /// `catalog_table_column_statistics_get`).
    pub statistics: Vec<crate::statistics::CatColStat>,
    /// Time-travel versions (schema evolution). Empty = not time-travel.
    /// `catalog_table_get` / `catalog_table_scan_function_get` select the entry
    /// matching the request's `at_value` (or the highest version when absent).
    pub time_travel: Vec<TimeTravelVersion>,
    /// Required WHERE-filter groups in conjunctive normal form (CNF): an AND
    /// (outer list) of OR-groups (inner lists) of dotted-path column references.
    /// A group is satisfied when any one of its member paths carries a WHERE
    /// filter; every group must be satisfied. A single-path group is a plain
    /// mandatory filter; a multi-path group `["ticker", "cik"]` means "one of".
    /// Empty = no enforcement. Surfaced in `TableInfo.required_filters`.
    pub required_filters: Vec<Vec<String>>,
    /// Accept `AT` clauses even without declared `time_travel` versions: the
    /// backing function reads the AT clause itself (carried on the bind request)
    /// rather than the catalog resolving it to a version. Mirrors the Python
    /// `Table(supports_time_travel=True)` on a non-versioned table.
    pub supports_time_travel: bool,
    /// An optional embedded scan function. When set (e.g. via
    /// [`CatTable::with_function`]), [`crate::Worker::set_catalog`] auto-registers
    /// it into the dispatch table, so callers don't need a separate
    /// `register_table` call — mirroring the Go `CatalogTable.Function`
    /// ergonomics. Not serialized; only the `scan_function` name reaches the wire.
    pub scan_function_impl: Option<std::sync::Arc<dyn crate::table_function::TableFunction>>,
}

/// One historical version of a time-travel table.
#[derive(Clone)]
pub struct TimeTravelVersion {
    pub version: i64,
    pub columns: SchemaRef,
    pub scan_function: String,
    pub scan_arguments: Vec<u8>,
    /// The calendar year this version became valid (for `AT (TIMESTAMP => …)`).
    pub timestamp_year: Option<i32>,
}

impl CatTable {
    /// Whether `v` is the current (highest) time-travel version.
    pub fn is_current_version(&self, v: i64) -> bool {
        self.time_travel.iter().map(|t| t.version).max() == Some(v)
    }

    /// Resolve the time-travel version for an `AT` clause: `VERSION` (exact),
    /// `TIMESTAMP` (highest version whose `timestamp_year <= year`), or the
    /// current version when no clause. `Ok(None)` = not a time-travel table.
    pub fn resolve_version(
        &self,
        at_unit: Option<&str>,
        at_value: Option<&str>,
    ) -> Result<Option<&TimeTravelVersion>> {
        let has_at = at_unit.is_some_and(|u| !u.is_empty());
        if self.time_travel.is_empty() {
            // Multi-branch tables reject AT clauses downstream (C++ emits the
            // "not supported on multi-branch" error) — pass through here.
            // Function-backed tables that opt into `supports_time_travel` read
            // the AT clause themselves (carried on the bind request), so the
            // catalog leaves the schema/scan unchanged (pass-through).
            if has_at && self.branches.is_none() && !self.supports_time_travel {
                return Err(vgi_rpc::RpcError::value_error(
                    "this table does not support time travel",
                ));
            }
            return Ok(None);
        }
        let unit = at_unit.map(|u| u.to_uppercase());
        match unit.as_deref() {
            None | Some("") => Ok(self.time_travel.iter().max_by_key(|t| t.version)),
            Some("VERSION") => {
                let want: i64 = at_value
                    .and_then(|v| v.parse().ok())
                    .ok_or_else(|| vgi_rpc::RpcError::value_error("invalid AT VERSION value"))?;
                self.time_travel
                    .iter()
                    .find(|t| t.version == want)
                    .map(Some)
                    .ok_or_else(|| {
                        vgi_rpc::RpcError::value_error(format!("Unknown version: {want}"))
                    })
            }
            Some("TIMESTAMP") => {
                let year: i32 = at_value
                    .and_then(|v| v.get(..4))
                    .and_then(|y| y.parse().ok())
                    .ok_or_else(|| vgi_rpc::RpcError::value_error("invalid AT TIMESTAMP value"))?;
                self.time_travel
                    .iter()
                    .filter(|t| t.timestamp_year.is_some_and(|ty| ty <= year))
                    .max_by_key(|t| t.version)
                    .map(Some)
                    .ok_or_else(|| {
                        let min_year = self
                            .time_travel
                            .iter()
                            .filter_map(|t| t.timestamp_year)
                            .min()
                            .unwrap_or(0);
                        vgi_rpc::RpcError::value_error(format!(
                            "No version exists at timestamp {at_value:?}: table did not exist before {min_year}"
                        ))
                    })
            }
            Some(other) => Err(vgi_rpc::RpcError::value_error(format!(
                "Unsupported at_unit: {other:?}"
            ))),
        }
    }
}

/// One physical branch of a multi-branch table.
///
/// A *function* branch sets `function_name` (+ `scan_arguments`); a
/// *catalog-table* branch leaves `function_name` empty and sets
/// `source_catalog`/`source_schema`/`source_table` to scan a companion-catalog
/// base table.
#[derive(Clone, Default)]
pub struct CatBranch {
    pub function_name: String,
    pub scan_arguments: Vec<u8>,
    pub branch_filter: Option<String>,
    pub writable: bool,
    pub source_catalog: Option<String>,
    pub source_schema: Option<String>,
    pub source_table: Option<String>,
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
            required_extensions: Vec::new(),
            statistics: Vec::new(),
            time_travel: Vec::new(),
            required_filters: Vec::new(),
            supports_time_travel: false,
            scan_function_impl: None,
        }
    }

    /// Build a function-backed table from a [`TableFunction`](crate::table_function::TableFunction)
    /// instance: stores the function (auto-registered by
    /// [`crate::Worker::set_catalog`]) and sets `scan_function` to its `name()`.
    /// This is the one-call ergonomic equivalent of the Go
    /// `CatalogTable{ Function: ... }` (vs. the name-based [`CatTable::new`],
    /// which requires a separate `Worker::register_table`). The scan is inlined.
    pub fn with_function(
        name: &str,
        columns: SchemaRef,
        function: std::sync::Arc<dyn crate::table_function::TableFunction>,
        comment: Option<String>,
        cardinality: Option<i64>,
    ) -> Self {
        let scan_function = function.name().to_string();
        let mut t = CatTable::new(
            name,
            columns,
            &scan_function,
            Vec::new(),
            comment,
            cardinality,
        );
        t.inline_scan = true;
        t.scan_function_impl = Some(function);
        t
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

/// Validate a table's `required_filters` (CNF). Each OR-group must be
/// non-empty, must contain no empty strings, and the leading dotted segment of
/// every path must name a real column on the table. Struct subfield validity is
/// left to DuckDB's binder (the descriptor doesn't unpack STRUCT subfields).
fn validate_required_filters(
    name: &str,
    columns: &SchemaRef,
    required_filters: &[Vec<String>],
) -> Result<()> {
    for group in required_filters {
        if group.is_empty() {
            return Err(vgi_rpc::RpcError::value_error(format!(
                "Table '{name}': required_filters must not contain empty groups"
            )));
        }
        for path in group {
            if path.is_empty() {
                return Err(vgi_rpc::RpcError::value_error(format!(
                    "Table '{name}': required_filters must not contain empty strings"
                )));
            }
            let head = path.split('.').next().unwrap_or(path);
            if columns.field_with_name(head).is_err() {
                return Err(vgi_rpc::RpcError::value_error(format!(
                    "Table '{name}': required_filters path '{path}' references unknown column '{head}'"
                )));
            }
        }
    }
    Ok(())
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
    validate_required_filters(&t.name, &t.columns, &t.required_filters)?;
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
        cardinality_estimate: t.cardinality.into(),
        cardinality_max: t.cardinality.into(),
        required_filters: t.required_filters.clone(),
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
        macro_type: DictString(if m.table_macro {
            "table".into()
        } else {
            "scalar".into()
        }),
        parameters: m.parameters.clone(),
        parameter_default_values: Bytes::from(
            build_macro_defaults(&m.defaults).unwrap_or_default(),
        ),
        definition: m.definition.clone(),
        arguments_schema: Bytes::from(build_macro_arguments_schema(m).unwrap_or_default()),
    }
}

/// Build a macro `arguments_schema`: one nullable Arrow field per parameter, in
/// `parameters` order. A parameter's field type is the type of its default value
/// when one is known (else `Null`). The per-parameter description rides as
/// `vgi_doc` field metadata (UTF-8, presence-only — the key is omitted entirely
/// when there is no doc), the exact same mechanism functions use for per-argument
/// docs. Returns empty IPC bytes when the macro has no parameters and no docs,
/// so older readers are unaffected.
pub fn build_macro_arguments_schema(m: &CatMacro) -> Result<Vec<u8>> {
    // Nothing to carry: no parameters and no docs -> emit nothing.
    if m.parameters.is_empty() && m.parameter_docs.is_empty() {
        return Ok(Vec::new());
    }
    let schema = macro_arguments_schema(&m.parameters, &m.defaults, &m.parameter_docs);
    ipc::write_schema(&schema)
}

/// Construct the macro `arguments_schema` from the parameter names (order is
/// load-bearing), the typed defaults (`(param, int64)` — present params get an
/// `Int64` field type, others `Null`), and per-parameter docs (`vgi_doc` field
/// metadata, presence-only).
pub fn macro_arguments_schema(
    parameters: &[String],
    defaults: &[(String, i64)],
    parameter_docs: &[(String, String)],
) -> Schema {
    let default_names: std::collections::HashSet<&str> =
        defaults.iter().map(|(n, _)| n.as_str()).collect();
    let fields: Vec<Field> = parameters
        .iter()
        .map(|name| {
            // Type from the default value when known (macro defaults are int64),
            // else Null.
            let ty = if default_names.contains(name.as_str()) {
                DataType::Int64
            } else {
                DataType::Null
            };
            let mut field = Field::new(name, ty, true);
            // Per-parameter description (UTF-8; presence-only — omit when empty).
            if let Some((_, doc)) = parameter_docs.iter().find(|(n, _)| n == name) {
                if !doc.is_empty() {
                    let mut meta: HashMap<String, String> = HashMap::new();
                    meta.insert("vgi_doc".to_string(), doc.clone());
                    field = field.with_metadata(meta);
                }
            }
            field
        })
        .collect();
    Schema::new(fields)
}

/// Extract per-parameter descriptions from a macro `arguments_schema` (inverse of
/// [`macro_arguments_schema`]'s `vgi_doc` handling). Fields without the `vgi_doc`
/// key (undocumented) are omitted.
pub fn macro_parameter_docs_from_schema(schema: &Schema) -> Vec<(String, String)> {
    schema
        .fields()
        .iter()
        .filter_map(|f| {
            f.metadata()
                .get("vgi_doc")
                .map(|doc| (f.name().clone(), doc.clone()))
        })
        .collect()
}

/// Build the 1-row IPC batch of macro parameter defaults (column per param).
fn build_macro_defaults(defaults: &[(String, i64)]) -> Result<Vec<u8>> {
    use arrow_array::{ArrayRef, Int64Array, RecordBatch};
    if defaults.is_empty() {
        return Ok(Vec::new());
    }
    let fields: Vec<Field> = defaults
        .iter()
        .map(|(n, _)| Field::new(n, DataType::Int64, false))
        .collect();
    let cols: Vec<ArrayRef> = defaults
        .iter()
        .map(|(_, v)| Arc::new(Int64Array::from(vec![*v])) as ArrayRef)
        .collect();
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
            let schema = tighten_inline_schema(&batch.schema());
            Ok(Bytes::from(ipc::write_batch_with_schema(&batch, &schema)?))
        })
        .collect()
}

/// Mark the `cardinality_estimate` / `cardinality_max` columns non-nullable in
/// the item schema (the C++ extension's `catalog_schema_contents_*` result-schema
/// check requires `int64 not null`), while the arrays still carry any NULL
/// values. `arrow`'s safe `RecordBatch`/`StructArray` constructors reject a
/// non-nullable field with nulls, so these columns are built nullable and the
/// declared wire schema is tightened only at IPC-write time — matching the
/// canonical convention (NULL in a non-nullable column = "not inlined").
fn tighten_inline_schema(schema: &Schema) -> Schema {
    const TIGHTEN: [&str; 2] = ["cardinality_estimate", "cardinality_max"];
    let fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|f| {
            if TIGHTEN.contains(&f.name().as_str()) {
                Field::new(f.name(), f.data_type().clone(), false)
            } else {
                f.as_ref().clone()
            }
        })
        .collect();
    Schema::new(fields)
}

/// Wrap a `DictString` enum value (re-export convenience).
pub fn dict(s: &str) -> DictString {
    enums::dict(s)
}

/// Build an `Arc<Schema>` from a `Schema` (convenience).
pub fn arc(schema: Schema) -> Arc<Schema> {
    Arc::new(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::{ArgSpec, FunctionExample, ProcessParams};
    use arrow_array::RecordBatch;
    use vgi_rpc::VgiArrow;

    /// A minimal scalar whose `metadata()` advertises SQL examples.
    struct ExampleScalar;

    impl ScalarFunction for ExampleScalar {
        fn name(&self) -> &str {
            "example_scalar"
        }
        fn metadata(&self) -> FunctionMetadata {
            FunctionMetadata {
                description: "demonstrates examples".to_string(),
                examples: vec![
                    FunctionExample {
                        sql: "SELECT example_scalar(1)".to_string(),
                        description: "basic usage".to_string(),
                        expected_output: Some("1".to_string()),
                    },
                    FunctionExample {
                        sql: "SELECT example_scalar(x) FROM t".to_string(),
                        description: "column usage".to_string(),
                        expected_output: None,
                    },
                ],
                return_type: Some(DataType::Int64),
                ..Default::default()
            }
        }
        fn argument_specs(&self) -> Vec<ArgSpec> {
            vec![ArgSpec::column("x", 0, "int64", "input")]
        }
        fn process(&self, _params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
            Ok(batch.clone())
        }
    }

    #[test]
    fn test_build_arg_schema_emits_vgi_doc() {
        let documented_doc = "Integer value to multiply";
        let unicode_doc = "µ ≥ value — note";
        let specs = vec![
            ArgSpec::column("multiplier", 0, "int64", documented_doc),
            ArgSpec::column("plain", 1, "int64", ""),
            ArgSpec::column("scaled", 2, "int64", unicode_doc),
        ];

        let schema = build_arg_schema(&specs);

        // Documented arg carries vgi_doc equal to the doc string.
        let documented = schema.field(0);
        assert_eq!(documented.name(), "multiplier");
        assert_eq!(
            documented.metadata().get("vgi_doc").map(String::as_str),
            Some(documented_doc),
        );

        // Empty-doc arg has NO vgi_doc key (presence-only semantics).
        let plain = schema.field(1);
        assert_eq!(plain.name(), "plain");
        assert!(
            !plain.metadata().contains_key("vgi_doc"),
            "empty doc must not emit a vgi_doc metadata key",
        );

        // Unicode doc round-trips as UTF-8.
        let scaled = schema.field(2);
        assert_eq!(scaled.name(), "scaled");
        assert_eq!(
            scaled.metadata().get("vgi_doc").map(String::as_str),
            Some(unicode_doc),
        );
    }

    #[test]
    fn test_format_range_notation() {
        // Inclusive both sides -> square brackets.
        assert_eq!(
            format_range(Some(0.0), Some(100.0), None, None).as_deref(),
            Some("[0, 100]"),
        );
        // Exclusive lower, open upper.
        assert_eq!(
            format_range(None, None, Some(0.0), None).as_deref(),
            Some("(0, +inf)"),
        );
        // Inclusive lower, exclusive upper.
        assert_eq!(
            format_range(Some(1.0), None, None, Some(10.0)).as_deref(),
            Some("[1, 10)"),
        );
        // Open lower, inclusive upper.
        assert_eq!(
            format_range(None, Some(5.0), None, None).as_deref(),
            Some("(-inf, 5]"),
        );
        // Exclusive both sides.
        assert_eq!(
            format_range(None, None, Some(0.0), Some(1.0)).as_deref(),
            Some("(0, 1)"),
        );
        // gt takes precedence over ge; lt over le (mirrors the Python priority).
        assert_eq!(
            format_range(Some(2.0), Some(9.0), Some(3.0), Some(8.0)).as_deref(),
            Some("(3, 8)"),
        );
        // No bounds -> None.
        assert_eq!(format_range(None, None, None, None), None);
        // Fractional bounds keep their decimal; whole numbers drop the ".0".
        assert_eq!(
            format_range(Some(0.5), Some(2.5), None, None).as_deref(),
            Some("[0.5, 2.5]"),
        );
        // Negative whole numbers still drop the trailing ".0".
        assert_eq!(
            format_range(Some(-10.0), Some(10.0), None, None).as_deref(),
            Some("[-10, 10]"),
        );
    }

    #[test]
    fn test_build_arg_schema_emits_constraint_keys() {
        let specs = vec![
            ArgSpec::const_arg("unit", -1, "varchar", "Output unit")
                .with_choices(["mm", "cm", "m"])
                .with_default("mm")
                .with_pattern("^[a-z]+$"),
            ArgSpec::const_arg("precision", 0, "int64", "Decimals")
                .with_ge(0.0)
                .with_le(10.0),
            // A plain arg carries none of the constraint keys.
            ArgSpec::column("value", 1, "double", "Value"),
        ];

        let schema = build_arg_schema(&specs);

        let unit = schema.field(0).metadata();
        assert_eq!(
            unit.get("vgi_choices").map(String::as_str),
            Some(r#"["mm","cm","m"]"#)
        );
        assert_eq!(unit.get("vgi_default").map(String::as_str), Some(r#""mm""#));
        assert_eq!(
            unit.get("vgi_pattern").map(String::as_str),
            Some("^[a-z]+$")
        );
        // No numeric bounds -> no range key.
        assert!(!unit.contains_key("vgi_range"));

        let precision = schema.field(1).metadata();
        assert_eq!(
            precision.get("vgi_range").map(String::as_str),
            Some("[0, 10]")
        );
        assert!(!precision.contains_key("vgi_choices"));
        assert!(!precision.contains_key("vgi_default"));
        assert!(!precision.contains_key("vgi_pattern"));

        // Unconstrained arg: none of the four keys present (presence-only).
        let value = schema.field(2).metadata();
        for key in ["vgi_default", "vgi_choices", "vgi_range", "vgi_pattern"] {
            assert!(
                !value.contains_key(key),
                "unconstrained arg must not emit {key}",
            );
        }
    }

    #[test]
    fn test_constraint_keys_encode_typed_values() {
        // Numeric choices and default encode as JSON scalars/arrays (not strings).
        let specs = vec![ArgSpec::const_arg("level", 0, "int64", "Level")
            .with_choices([1, 2, 3])
            .with_default(2)];
        let schema = build_arg_schema(&specs);
        let meta = schema.field(0).metadata();
        assert_eq!(meta.get("vgi_choices").map(String::as_str), Some("[1,2,3]"));
        assert_eq!(meta.get("vgi_default").map(String::as_str), Some("2"));

        // Boolean and list defaults encode faithfully.
        let specs = vec![ArgSpec::const_arg("flag", -1, "boolean", "Flag")
            .with_default(true)
            .with_choices([true, false])];
        let schema = build_arg_schema(&specs);
        let meta = schema.field(0).metadata();
        assert_eq!(meta.get("vgi_default").map(String::as_str), Some("true"));
        assert_eq!(
            meta.get("vgi_choices").map(String::as_str),
            Some("[true,false]")
        );
    }

    #[test]
    fn scalar_function_info_carries_examples() {
        let fi = scalar_function_info(&ExampleScalar).expect("build FunctionInfo");
        assert_eq!(fi.examples.len(), 2);
        assert_eq!(fi.examples[0].sql, "SELECT example_scalar(1)");
        assert_eq!(fi.examples[0].description, "basic usage");
        assert_eq!(fi.examples[0].expected_output.as_deref(), Some("1"));
        assert_eq!(fi.examples[1].sql, "SELECT example_scalar(x) FROM t");
        assert_eq!(fi.examples[1].expected_output, None);
    }

    fn macro_with_docs() -> CatMacro {
        CatMacro {
            name: "vgi_clamp".to_string(),
            parameters: vec!["val".to_string(), "lo".to_string(), "hi".to_string()],
            definition: "GREATEST(lo, LEAST(hi, val))".to_string(),
            table_macro: false,
            comment: None,
            // `lo`/`hi` have int64 defaults; `val` does not.
            defaults: vec![("lo".to_string(), 0), ("hi".to_string(), 100)],
            parameter_docs: vec![
                ("val".to_string(), "Value to clamp".to_string()),
                ("hi".to_string(), "Upper bound — µ ≥ note".to_string()),
                // `lo` deliberately left undocumented.
            ],
        }
    }

    #[test]
    fn test_macro_arguments_schema_carries_vgi_doc_per_documented_param() {
        let m = macro_with_docs();
        let schema = macro_arguments_schema(&m.parameters, &m.defaults, &m.parameter_docs);

        // One field per parameter, in `parameters` order.
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "val");
        assert_eq!(schema.field(1).name(), "lo");
        assert_eq!(schema.field(2).name(), "hi");

        // Every field is nullable.
        assert!(schema.fields().iter().all(|f| f.is_nullable()));

        // Field type = default value type when known (int64), else Null.
        assert_eq!(schema.field(0).data_type(), &DataType::Null); // val: no default
        assert_eq!(schema.field(1).data_type(), &DataType::Int64); // lo: int64 default
        assert_eq!(schema.field(2).data_type(), &DataType::Int64); // hi: int64 default

        // Documented params carry vgi_doc (UTF-8, including non-ASCII).
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get("vgi_doc")
                .map(String::as_str),
            Some("Value to clamp"),
        );
        assert_eq!(
            schema
                .field(2)
                .metadata()
                .get("vgi_doc")
                .map(String::as_str),
            Some("Upper bound — µ ≥ note"),
        );

        // Undocumented param has NO vgi_doc key (presence-only semantics).
        assert!(
            !schema.field(1).metadata().contains_key("vgi_doc"),
            "undocumented parameter must not emit a vgi_doc metadata key",
        );

        // Round-trips through IPC and back to the docs map.
        let bytes = build_macro_arguments_schema(&m).expect("serialize arguments_schema");
        assert!(!bytes.is_empty());
        let decoded = ipc::read_schema(&bytes).expect("read arguments_schema");
        let docs = macro_parameter_docs_from_schema(&decoded);
        assert_eq!(docs.len(), 2);
        assert!(docs
            .iter()
            .any(|(n, d)| n == "val" && d == "Value to clamp"),);
        assert!(docs
            .iter()
            .any(|(n, d)| n == "hi" && d == "Upper bound — µ ≥ note"),);
        assert!(
            !docs.iter().any(|(n, _)| n == "lo"),
            "undocumented parameter must not appear in the decoded docs",
        );
    }

    #[test]
    fn test_macro_info_appends_arguments_schema_with_docs() {
        let info = macro_info("main", &macro_with_docs());
        // arguments_schema is populated for a documented macro.
        assert!(!info.arguments_schema.0.is_empty());
        let decoded =
            ipc::read_schema(&info.arguments_schema.0).expect("read MacroInfo.arguments_schema");
        assert_eq!(decoded.fields().len(), 3);
        assert_eq!(
            decoded
                .field(0)
                .metadata()
                .get("vgi_doc")
                .map(String::as_str),
            Some("Value to clamp"),
        );
    }

    #[test]
    fn test_macro_arguments_schema_empty_when_no_params_and_no_docs() {
        let m = CatMacro {
            name: "no_args".to_string(),
            parameters: vec![],
            definition: "42".to_string(),
            table_macro: false,
            comment: None,
            defaults: vec![],
            parameter_docs: vec![],
        };
        let bytes = build_macro_arguments_schema(&m).expect("serialize");
        assert!(
            bytes.is_empty(),
            "macro with no parameters and no docs emits empty arguments_schema",
        );
        // And the DTO carries empty bytes (older readers unaffected).
        let info = macro_info("main", &m);
        assert!(info.arguments_schema.0.is_empty());
    }

    fn ab_table(required: Vec<Vec<String>>) -> CatTable {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let mut t = CatTable::new("rff", schema, "rff_scan", Vec::new(), None, Some(3));
        t.required_filters = required;
        t
    }

    #[test]
    fn test_required_filters_cnf_wire_roundtrip() {
        use crate::protocol::dtos::TableInfo;
        // A CNF requirement: (a AND one-of(a,b)) — a singleton group plus a
        // genuine OR-group. Verifies the trailing `required_filters` field
        // survives the list<list<utf8>> wire round-trip intact.
        let t = ab_table(vec![
            vec!["a".to_string()],
            vec!["a".to_string(), "b".to_string()],
        ]);
        let info = table_info("data", &t).expect("table_info");
        assert_eq!(
            info.required_filters,
            vec![
                vec!["a".to_string()],
                vec!["a".to_string(), "b".to_string()]
            ]
        );

        // The wire schema for the field must be list<list<utf8>>.
        let dt = TableInfo::arrow_data_type();
        let arrow_schema::DataType::Struct(fields) = dt else {
            panic!("TableInfo is not a struct");
        };
        let rf = fields
            .iter()
            .find(|f| f.name() == "required_filters")
            .expect("required_filters field present");
        assert_eq!(rf, fields.last().unwrap(), "must be the trailing field");
        let arrow_schema::DataType::List(inner) = rf.data_type() else {
            panic!("required_filters is not a List, got {:?}", rf.data_type());
        };
        let arrow_schema::DataType::List(leaf) = inner.data_type() else {
            panic!("required_filters inner is not a List");
        };
        assert_eq!(leaf.data_type(), &DataType::Utf8);

        // Full DTO round-trip through the flat batch.
        let batch = crate::wire::to_batch(info).expect("to_batch");
        let back: TableInfo = crate::wire::from_batch(&batch).expect("from_batch");
        assert_eq!(
            back.required_filters,
            vec![
                vec!["a".to_string()],
                vec!["a".to_string(), "b".to_string()]
            ]
        );
    }

    #[test]
    fn test_required_filters_rejects_empty_group() {
        let t = ab_table(vec![vec![]]);
        let err = table_info("data", &t).expect_err("empty group must be rejected");
        assert!(err.to_string().contains("empty groups"), "got: {err}");
    }

    #[test]
    fn test_required_filters_rejects_empty_string() {
        let t = ab_table(vec![vec!["".to_string()]]);
        let err = table_info("data", &t).expect_err("empty string must be rejected");
        assert!(err.to_string().contains("empty strings"), "got: {err}");
    }

    #[test]
    fn test_required_filters_rejects_unknown_column() {
        let t = ab_table(vec![vec!["nope".to_string()]]);
        let err = table_info("data", &t).expect_err("unknown column must be rejected");
        assert!(err.to_string().contains("unknown column"), "got: {err}");
    }

    #[test]
    fn test_required_filters_accepts_struct_subfield_head() {
        // A dotted path's leading segment ('a') is a real column; the subfield
        // ('x') is left to DuckDB's binder — validation must accept it.
        let t = ab_table(vec![vec!["a.x".to_string()]]);
        assert!(table_info("data", &t).is_ok());
    }
}

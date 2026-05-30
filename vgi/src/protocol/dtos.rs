//! VGI wire DTOs. Field order and types mirror
//! `vgi-go/vgi/generated/protocol_schemas.go` exactly (the C++ extension
//! reads several result schemas positionally, so order matters).
//!
//! Serialize/deserialize the flat ones with [`crate::wire`]; the catalog
//! item structs (`SchemaInfo`, `FunctionInfo`, …) are IPC-serialized into
//! the `items: list<binary>` result via [`crate::ipc`].

use vgi_rpc::{Bytes, DictString, LargeBytes, VgiArrow};

/// `map<utf8, utf8>` payload (Python-canonical `keys`/`values` child names).
pub type StrMap = Vec<(String, String)>;
/// `map<utf8, int64>` payload.
pub type IntMap = Vec<(String, i64)>;

// ---------------------------------------------------------------------------
// bind
// ---------------------------------------------------------------------------

/// `BindRequest` — carried IPC-serialized inside the `request` binary column
/// of `bind`, and as the nested `bind_call` struct of `init` / cardinality.
#[derive(Debug, Clone, VgiArrow)]
pub struct BindRequest {
    pub function_name: String,
    pub arguments: Bytes,
    pub function_type: DictString,
    pub input_schema: Option<Bytes>,
    pub settings: Option<Bytes>,
    pub secrets: Option<Bytes>,
    pub attach_opaque_data: Option<Bytes>,
    pub transaction_opaque_data: Option<Bytes>,
    pub resolved_secrets_provided: bool,
}

/// `BindResponse` — flat result of `bind`.
#[derive(Debug, Clone, VgiArrow)]
pub struct BindResponse {
    pub output_schema: Bytes,
    pub opaque_data: Bytes,
    pub lookup_secret_types: Vec<String>,
    pub lookup_scopes: Vec<String>,
    pub lookup_names: Vec<String>,
}

// ---------------------------------------------------------------------------
// init
// ---------------------------------------------------------------------------

/// `InitRequest` — carried IPC-serialized inside the `request` binary column
/// of `init`. `bind_call` is itself an IPC-serialized [`BindRequest`] (Python
/// serializes nested dataclasses as binary), decoded separately.
#[derive(Debug, Clone, VgiArrow)]
pub struct InitRequest {
    pub bind_call: Bytes,
    pub output_schema: Bytes,
    pub bind_opaque_data: Option<Bytes>,
    pub projection_ids: Option<Vec<i64>>,
    pub pushdown_filters: Option<LargeBytes>,
    pub join_keys: Option<Vec<LargeBytes>>,
    pub phase: Option<DictString>,
    pub execution_id: Option<Bytes>,
    pub init_opaque_data: Option<Bytes>,
    pub order_by_column_name: Option<String>,
    pub order_by_direction: Option<DictString>,
    pub order_by_null_order: Option<DictString>,
    pub order_by_limit: Option<i64>,
    pub tablesample_percentage: Option<f64>,
    pub tablesample_seed: Option<i64>,
    pub finalize_state_id: Option<Bytes>,
}

/// `GlobalInitResponse` — the streaming header for `init`.
#[derive(Debug, Clone, VgiArrow)]
pub struct GlobalInitResponse {
    pub execution_id: Bytes,
    pub max_workers: i64,
    pub opaque_data: Option<Bytes>,
}

// ---------------------------------------------------------------------------
// catalog_attach
// ---------------------------------------------------------------------------

/// `CatalogAttachRequest` — IPC-serialized inside `request` of `catalog_attach`.
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogAttachRequest {
    pub name: String,
    pub options: Option<Bytes>,
    pub data_version_spec: Option<String>,
    pub implementation_version: Option<String>,
}

/// Request for `table_function_cardinality` / `table_function_statistics`
/// (boxes an IPC-serialized `BindRequest`).
#[derive(Debug, Clone, VgiArrow)]
pub struct CardinalityRequest {
    pub bind_call: Bytes,
    pub bind_opaque_data: Option<Bytes>,
}

/// Response for `table_function_cardinality`.
#[derive(Debug, Clone, VgiArrow)]
pub struct CardinalityResponse {
    pub estimate: Option<i64>,
    pub max: Option<i64>,
}

/// One physical source backing a (possibly multi-branch) table scan.
#[derive(Debug, Clone, VgiArrow)]
pub struct ScanBranch {
    pub function_name: String,
    pub arguments: Bytes,
    pub branch_filter: Option<String>,
    pub writable: bool,
}

/// Response for `catalog_table_scan_branches_get`. The `branches` list must be
/// non-empty (one entry per physical source; single-source tables return one).
#[derive(Debug, Clone, VgiArrow)]
pub struct ScanBranchesResult {
    pub branches: Vec<Bytes>,
    pub required_extensions: Vec<String>,
}

/// Secret-type registration entry, IPC-serialized into
/// `CatalogAttachResult.secret_types`.
#[derive(Debug, Clone, VgiArrow)]
pub struct SecretTypeWire {
    pub name: String,
    pub description: String,
    pub parameters_schema: Bytes,
}

/// `CatalogAttachResult` — flat result of `catalog_attach`.
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogAttachResult {
    pub attach_opaque_data: Bytes,
    pub supports_transactions: bool,
    pub supports_time_travel: bool,
    pub catalog_version_frozen: bool,
    pub catalog_version: i64,
    pub attach_opaque_data_required: bool,
    pub default_schema: String,
    pub settings: Vec<Bytes>,
    pub secret_types: Vec<Bytes>,
    pub comment: Option<String>,
    pub tags: StrMap,
    pub supports_column_statistics: bool,
    pub resolved_data_version: Option<String>,
    pub resolved_implementation_version: Option<String>,
}

// ---------------------------------------------------------------------------
// catalog transactions / version / detach
// ---------------------------------------------------------------------------

/// Flat params: `catalog_transaction_begin`.
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogTransactionBeginParams {
    pub attach_opaque_data: Bytes,
}

/// Flat result: `catalog_transaction_begin`.
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogTransactionBeginResult {
    pub transaction_opaque_data: Option<Bytes>,
}

/// Flat params: `catalog_transaction_commit` / `_rollback`.
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogTransactionEndParams {
    pub attach_opaque_data: Bytes,
    pub transaction_opaque_data: Bytes,
}

/// Flat params: `catalog_detach`.
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogDetachParams {
    pub attach_opaque_data: Bytes,
}

/// Flat params: `catalog_version`.
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogVersionParams {
    pub attach_opaque_data: Bytes,
    pub transaction_opaque_data: Option<Bytes>,
}

/// Flat result: `catalog_version`.
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogVersionResult {
    pub version: i64,
}

// ---------------------------------------------------------------------------
// catalog schema discovery
// ---------------------------------------------------------------------------

/// Flat params: `catalog_schemas`.
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogSchemasParams {
    pub attach_opaque_data: Bytes,
    pub transaction_opaque_data: Option<Bytes>,
}

/// Flat params: `catalog_schema_get` and the `catalog_schema_contents_{tables,
/// views,indexes}` family (attach + name + optional tx).
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogSchemaNameParams {
    pub attach_opaque_data: Bytes,
    pub name: String,
    pub transaction_opaque_data: Option<Bytes>,
}

/// Flat params: `catalog_schema_contents_functions` (and `_macros`).
#[derive(Debug, Clone, VgiArrow)]
pub struct CatalogSchemaContentsFunctionsParams {
    pub attach_opaque_data: Bytes,
    pub name: String,
    #[allow(non_snake_case)]
    pub r#type: DictString,
    pub transaction_opaque_data: Option<Bytes>,
}

/// Shared flat result for every `catalog_*_get` / `_contents_*` method:
/// `items` is a list of IPC-serialized item structs.
#[derive(Debug, Clone, VgiArrow)]
pub struct ItemsResult {
    pub items: Vec<Bytes>,
}

// ---------------------------------------------------------------------------
// catalog item structs (IPC-serialized into ItemsResult.items)
// ---------------------------------------------------------------------------

/// `SchemaInfo` item.
#[derive(Debug, Clone, VgiArrow)]
pub struct SchemaInfo {
    pub comment: Option<String>,
    pub tags: StrMap,
    pub attach_opaque_data: Bytes,
    pub name: String,
    pub estimated_object_count: Option<IntMap>,
}

/// One `examples` entry of `FunctionInfo`.
#[derive(Debug, Clone, VgiArrow)]
pub struct FunctionExample {
    pub sql: String,
    pub description: String,
    pub expected_output: Option<String>,
}

/// One `required_secrets` entry of `FunctionInfo`.
#[derive(Debug, Clone, VgiArrow)]
pub struct RequiredSecret {
    pub secret_type: String,
    pub scope: Option<String>,
    pub secret_name: Option<String>,
}

/// `TableInfo` item — describes a catalog table to DuckDB.
#[derive(Debug, Clone, VgiArrow)]
pub struct TableInfo {
    pub comment: Option<String>,
    pub tags: StrMap,
    pub name: String,
    pub schema_name: String,
    /// IPC-serialized Arrow schema of the table columns.
    pub columns: Bytes,
    pub not_null_constraints: Vec<i32>,
    pub unique_constraints: Vec<Vec<i32>>,
    pub check_constraints: Vec<String>,
    pub primary_key_constraints: Vec<Vec<i32>>,
    pub foreign_key_constraints: Vec<Bytes>,
    pub supports_insert: bool,
    pub supports_update: bool,
    pub supports_delete: bool,
    pub supports_returning: bool,
    pub supports_column_statistics: bool,
    /// IPC `ScanFunctionResult`, empty if not inlined.
    pub scan_function: Bytes,
    pub insert_function: Bytes,
    pub update_function: Bytes,
    pub delete_function: Bytes,
    pub cardinality_estimate: i64,
    pub cardinality_max: i64,
    pub column_statistics: Bytes,
    pub bind_result: Bytes,
}

/// `ViewInfo` item.
#[derive(Debug, Clone, VgiArrow)]
pub struct ViewInfo {
    pub comment: Option<String>,
    pub tags: StrMap,
    pub name: String,
    pub schema_name: String,
    pub definition: String,
    pub column_comments: StrMap,
}

/// `MacroInfo` item.
#[derive(Debug, Clone, VgiArrow)]
pub struct MacroInfo {
    pub comment: Option<String>,
    pub tags: StrMap,
    pub name: String,
    pub schema_name: String,
    pub macro_type: DictString,
    pub parameters: Vec<String>,
    pub parameter_default_values: Bytes,
    pub definition: String,
}

/// `ScanFunctionResult` — names the table function that scans a catalog table.
#[derive(Debug, Clone, VgiArrow)]
pub struct ScanFunctionResult {
    pub function_name: String,
    pub arguments: Bytes,
    pub required_extensions: Vec<String>,
}

/// `FunctionInfo` item — describes a function to DuckDB. Field order matches
/// `FunctionInfoSchema` in the generated Go schemas.
#[derive(Debug, Clone, VgiArrow)]
pub struct FunctionInfo {
    pub comment: Option<String>,
    pub tags: StrMap,
    pub name: String,
    pub schema_name: String,
    pub function_type: DictString,
    pub arguments: Bytes,
    pub output_schema: Bytes,
    pub stability: Option<DictString>,
    pub null_handling: Option<DictString>,
    pub description: String,
    pub examples: Vec<FunctionExample>,
    pub categories: Vec<String>,
    pub projection_pushdown: Option<bool>,
    pub filter_pushdown: Option<bool>,
    pub sampling_pushdown: Option<bool>,
    pub late_materialization: Option<bool>,
    pub supported_expression_filters: Vec<String>,
    pub order_preservation: Option<DictString>,
    pub max_workers: i32,
    pub supports_batch_index: bool,
    pub partition_kind: DictString,
    pub order_dependent: DictString,
    pub distinct_dependent: DictString,
    pub supports_window: bool,
    pub streaming_partitioned: bool,
    pub has_finalize: bool,
    pub source_order_dependent: bool,
    pub sink_order_dependent: bool,
    pub requires_input_batch_index: bool,
    pub required_settings: Vec<String>,
    pub required_secrets: Vec<RequiredSecret>,
}

// ---------------------------------------------------------------------------
// cardinality / statistics
// ---------------------------------------------------------------------------

/// Flat result: `table_function_cardinality`.
#[derive(Debug, Clone, VgiArrow)]
pub struct TableCardinality {
    pub estimate: Option<i64>,
    pub max: Option<i64>,
}

// ---------------------------------------------------------------------------
// table buffering (boxed in `request: binary`)
// ---------------------------------------------------------------------------

/// `TableBufferingProcessRequest` — sink one batch.
#[derive(Debug, Clone, VgiArrow)]
pub struct TableBufferingProcessRequest {
    pub function_name: String,
    pub execution_id: Bytes,
    pub input_batch: Bytes,
    pub attach_opaque_data: Option<Bytes>,
    pub transaction_id: Option<Bytes>,
    pub batch_index: Option<i64>,
}

/// `TableBufferingProcessResponse`.
#[derive(Debug, Clone, VgiArrow)]
pub struct TableBufferingProcessResponse {
    pub state_id: Bytes,
}

/// `TableBufferingCombineRequest`.
#[derive(Debug, Clone, VgiArrow)]
pub struct TableBufferingCombineRequest {
    pub function_name: String,
    pub execution_id: Bytes,
    pub state_ids: Vec<Bytes>,
    pub attach_opaque_data: Option<Bytes>,
    pub transaction_id: Option<Bytes>,
}

/// `TableBufferingCombineResponse`.
#[derive(Debug, Clone, VgiArrow)]
pub struct TableBufferingCombineResponse {
    pub finalize_state_ids: Vec<Bytes>,
}

/// `TableBufferingDestructorRequest`.
#[derive(Debug, Clone, VgiArrow)]
pub struct TableBufferingDestructorRequest {
    pub function_name: String,
    pub execution_id: Bytes,
}

// ---------------------------------------------------------------------------
// aggregate (boxed in `request: binary`)
// ---------------------------------------------------------------------------

/// `AggregateBindRequest`.
#[derive(Debug, Clone, VgiArrow)]
pub struct AggregateBindRequest {
    pub function_name: String,
    pub arguments: Bytes,
    pub input_schema: Option<Bytes>,
    pub settings: Option<Bytes>,
    pub secrets: Option<Bytes>,
    pub attach_opaque_data: Option<Bytes>,
}

/// `AggregateBindResponse`.
#[derive(Debug, Clone, VgiArrow)]
pub struct AggregateBindResponse {
    pub output_schema: Bytes,
    pub execution_id: Bytes,
}

/// `AggregateUpdateRequest`.
#[derive(Debug, Clone, VgiArrow)]
pub struct AggregateUpdateRequest {
    pub function_name: String,
    pub execution_id: Bytes,
    pub input_batch: Bytes,
    pub attach_opaque_data: Option<Bytes>,
}

/// `AggregateCombineRequest`.
#[derive(Debug, Clone, VgiArrow)]
pub struct AggregateCombineRequest {
    pub function_name: String,
    pub execution_id: Bytes,
    pub merge_batch: Bytes,
    pub attach_opaque_data: Option<Bytes>,
}

/// `AggregateFinalizeRequest`.
#[derive(Debug, Clone, VgiArrow)]
pub struct AggregateFinalizeRequest {
    pub function_name: String,
    pub execution_id: Bytes,
    pub group_ids_batch: Bytes,
    pub output_schema: Bytes,
    pub attach_opaque_data: Option<Bytes>,
}

/// `AggregateFinalizeResponse`.
#[derive(Debug, Clone, VgiArrow)]
pub struct AggregateFinalizeResponse {
    pub result_batch: Bytes,
}

/// `AggregateDestructorRequest`.
#[derive(Debug, Clone, VgiArrow)]
pub struct AggregateDestructorRequest {
    pub function_name: String,
    pub execution_id: Bytes,
}

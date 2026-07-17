// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Core function model shared by all VGI function kinds.
//!
//! Mirrors the canonical Python base classes.

use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef};
use vgi_rpc::Result;

pub use crate::protocol::dtos::FunctionExample;
use crate::protocol::enums;

/// A named type-bound predicate for ANY-typed arguments. Checked at bind:
/// the input field type must satisfy the predicate or bind errors with the
/// bound's `name` (mirrors Python's `type_bound=<predicate>`).
#[derive(Clone, Copy)]
pub struct TypeBound {
    pub name: &'static str,
    pub pred: fn(&DataType) -> bool,
}

impl std::fmt::Debug for TypeBound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TypeBound({})", self.name)
    }
}

/// `_is_addable_type`: integer | floating | decimal | temporal.
pub const ADDABLE: TypeBound = TypeBound {
    name: "_is_addable_type",
    pred: is_addable,
};
/// `_is_multipliable_type`: integer | floating | decimal (no temporal).
pub const MULTIPLIABLE: TypeBound = TypeBound {
    name: "_is_multipliable_type",
    pred: is_multipliable,
};

fn is_integer(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64
    )
}
fn is_floating(t: &DataType) -> bool {
    matches!(t, DataType::Float16 | DataType::Float32 | DataType::Float64)
}
fn is_decimal(t: &DataType) -> bool {
    matches!(t, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))
}
fn is_temporal(t: &DataType) -> bool {
    use DataType::*;
    matches!(
        t,
        Date32 | Date64 | Time32(_) | Time64(_) | Timestamp(_, _) | Duration(_) | Interval(_)
    )
}
fn is_addable(t: &DataType) -> bool {
    is_integer(t) || is_floating(t) || is_decimal(t) || is_temporal(t)
}
fn is_multipliable(t: &DataType) -> bool {
    is_integer(t) || is_floating(t) || is_decimal(t)
}

/// Per-argument specification, used to build the function's wire arg schema
/// (`FunctionInfo.arguments`) and validate type bounds at bind time.
#[derive(Debug, Clone)]
pub struct ArgSpec {
    /// Argument name (the struct field name; empty for positional-only).
    pub name: String,
    /// 0-based positional index; `-1` for named-only.
    pub position: i32,
    /// VGI arg type string: `"int64"`, `"varchar"`, `"any"`, `"table"`, …
    pub arrow_type: String,
    /// Doc string.
    pub doc: String,
    /// Constant (bind-time scalar) parameter.
    pub is_const: bool,
    /// Variadic parameter.
    pub is_varargs: bool,
    /// Optional concrete Arrow type (takes precedence over `arrow_type`).
    pub arrow_data_type: Option<DataType>,
    /// Optional bind-time type bound for ANY-typed args.
    pub type_bound: Option<TypeBound>,
    /// Discovery-facing validation constraints (surfaced via
    /// `vgi_function_arguments()` as Arrow field metadata). All optional;
    /// `None` = the constraint is absent. See [`crate::catalog::build_arg_schema`].
    ///
    /// Closed set of allowed values (`vgi_choices`).
    pub choices: Option<Vec<serde_json::Value>>,
    /// Inclusive lower bound, value >= ge (`vgi_range`).
    pub ge: Option<f64>,
    /// Inclusive upper bound, value <= le (`vgi_range`).
    pub le: Option<f64>,
    /// Exclusive lower bound, value > gt (`vgi_range`).
    pub gt: Option<f64>,
    /// Exclusive upper bound, value < lt (`vgi_range`).
    pub lt: Option<f64>,
    /// Regex the value must match (`vgi_pattern`).
    pub pattern: Option<String>,
    /// Default value for the argument (`vgi_default`).
    pub default: Option<serde_json::Value>,
}

impl ArgSpec {
    fn base(name: &str, position: i32, arrow_type: &str, doc: &str) -> Self {
        ArgSpec {
            name: name.to_string(),
            position,
            arrow_type: arrow_type.to_string(),
            doc: doc.to_string(),
            is_const: false,
            is_varargs: false,
            arrow_data_type: None,
            type_bound: None,
            choices: None,
            ge: None,
            le: None,
            gt: None,
            lt: None,
            pattern: None,
            default: None,
        }
    }

    /// A positional, non-const ANY-typed column argument.
    pub fn any_column(name: &str, position: i32, doc: &str) -> Self {
        Self::base(name, position, "any", doc)
    }

    /// A positional, non-const column argument of a concrete VGI type string
    /// (e.g. `"int32"`, `"varchar"`, `"binary"`).
    pub fn column(name: &str, position: i32, arrow_type: &str, doc: &str) -> Self {
        Self::base(name, position, arrow_type, doc)
    }

    /// A positional column argument with an explicit Arrow type.
    pub fn column_typed(name: &str, position: i32, ty: DataType, doc: &str) -> Self {
        let mut s = Self::base(name, position, "", doc);
        s.arrow_data_type = Some(ty);
        s
    }

    /// A positional const (bind-time scalar) argument of a concrete VGI type.
    pub fn const_arg(name: &str, position: i32, arrow_type: &str, doc: &str) -> Self {
        let mut s = Self::base(name, position, arrow_type, doc);
        s.is_const = true;
        s
    }

    /// A positional const argument with an explicit Arrow type.
    pub fn const_typed(name: &str, position: i32, ty: DataType, doc: &str) -> Self {
        let mut s = Self::base(name, position, "", doc);
        s.is_const = true;
        s.arrow_data_type = Some(ty);
        s
    }

    /// Mark this spec variadic (consumes all remaining columns).
    pub fn varargs(mut self) -> Self {
        self.is_varargs = true;
        self
    }

    /// Mark this spec const.
    pub fn as_const(mut self) -> Self {
        self.is_const = true;
        self
    }

    /// Attach a type bound.
    pub fn with_bound(mut self, bound: TypeBound) -> Self {
        self.type_bound = Some(bound);
        self
    }

    /// Declare the closed set of allowed values (`vgi_choices`).
    pub fn with_choices<I, V>(mut self, choices: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<serde_json::Value>,
    {
        self.choices = Some(choices.into_iter().map(Into::into).collect());
        self
    }

    /// Inclusive lower bound (value >= `v`), surfaced in `vgi_range`.
    pub fn with_ge(mut self, v: f64) -> Self {
        self.ge = Some(v);
        self
    }

    /// Inclusive upper bound (value <= `v`), surfaced in `vgi_range`.
    pub fn with_le(mut self, v: f64) -> Self {
        self.le = Some(v);
        self
    }

    /// Exclusive lower bound (value > `v`), surfaced in `vgi_range`.
    pub fn with_gt(mut self, v: f64) -> Self {
        self.gt = Some(v);
        self
    }

    /// Exclusive upper bound (value < `v`), surfaced in `vgi_range`.
    pub fn with_lt(mut self, v: f64) -> Self {
        self.lt = Some(v);
        self
    }

    /// Regex the value must match (`vgi_pattern`).
    pub fn with_pattern(mut self, pattern: &str) -> Self {
        self.pattern = Some(pattern.to_string());
        self
    }

    /// Default value for the argument (`vgi_default`).
    pub fn with_default<V: Into<serde_json::Value>>(mut self, default: V) -> Self {
        self.default = Some(default.into());
        self
    }
}

/// Validate each spec's type bound against the input schema. Errors (value
/// error) naming the failed bound, matching Python's `SchemaValidationError`.
pub fn validate_type_bounds(specs: &[ArgSpec], input_schema: Option<&SchemaRef>) -> Result<()> {
    let Some(schema) = input_schema else {
        return Ok(());
    };
    for spec in specs {
        let Some(bound) = spec.type_bound else {
            continue;
        };
        if spec.position < 0 {
            continue;
        }
        if let Some(field) = schema.fields().get(spec.position as usize) {
            if !(bound.pred)(field.data_type()) {
                return Err(vgi_rpc::RpcError::value_error(format!(
                    "{}: argument {} of type {} does not satisfy {}",
                    bound.name,
                    spec.name,
                    field.data_type(),
                    bound.name
                )));
            }
        }
    }
    Ok(())
}

/// Enforce a function's const-argument value constraints at bind time.
///
/// Const arguments are bind-time scalars, so their declared `choices`, numeric
/// range (`ge`/`le`/`gt`/`lt`), and `pattern` constraints are validated once
/// here — mirroring the Python SDK, so a discovered constraint (surfaced via
/// `vgi_function_arguments()`) is actually binding. A violating value returns an
/// `RpcError::value_error`; a null const value skips its value constraints, and
/// column (non-const) arguments are not enforced here (type bounds are
/// [`validate_type_bounds`]'s job).
pub fn validate_arg_constraints(
    specs: &[ArgSpec],
    args: &crate::arguments::Arguments,
) -> Result<()> {
    for spec in specs {
        if !spec.is_const || spec.position < 0 {
            continue;
        }
        let pos = spec.position as usize;

        // Numeric range — const_f64 widens int and float; None = null/non-numeric.
        if spec.ge.is_some() || spec.le.is_some() || spec.gt.is_some() || spec.lt.is_some() {
            if let Some(v) = args.const_f64(pos) {
                if let Some(ge) = spec.ge {
                    if v < ge {
                        return Err(constraint_err(
                            spec,
                            &format!("must be >= {}", fmt_bound(ge)),
                        ));
                    }
                }
                if let Some(le) = spec.le {
                    if v > le {
                        return Err(constraint_err(
                            spec,
                            &format!("must be <= {}", fmt_bound(le)),
                        ));
                    }
                }
                if let Some(gt) = spec.gt {
                    if v <= gt {
                        return Err(constraint_err(
                            spec,
                            &format!("must be > {}", fmt_bound(gt)),
                        ));
                    }
                }
                if let Some(lt) = spec.lt {
                    if v >= lt {
                        return Err(constraint_err(
                            spec,
                            &format!("must be < {}", fmt_bound(lt)),
                        ));
                    }
                }
            }
        }

        // Closed choice set.
        if let Some(choices) = &spec.choices {
            if !choices.is_empty() && !const_in_choices(args, pos, choices) {
                return Err(constraint_err(
                    spec,
                    &format!(
                        "must be one of {}",
                        serde_json::Value::Array(choices.clone())
                    ),
                ));
            }
        }

        // Regex pattern (string args).
        if let Some(pattern) = &spec.pattern {
            if let Some(s) = args.const_str(pos) {
                if let Ok(re) = regex::Regex::new(pattern) {
                    if !re.is_match(&s) {
                        return Err(constraint_err(
                            spec,
                            &format!("must match pattern {pattern}"),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn constraint_err(spec: &ArgSpec, detail: &str) -> vgi_rpc::RpcError {
    vgi_rpc::RpcError::value_error(format!("argument {}: {}", spec.name, detail))
}

/// Format a numeric bound, trimming a trailing `.0` (matches the `vgi_range` text).
fn fmt_bound(v: f64) -> String {
    if v.is_finite() && v.fract() == 0.0 {
        (v as i64).to_string()
    } else {
        v.to_string()
    }
}

/// Whether a present const value equals one of the declared choices. A null or
/// absent const returns true (the choice check is skipped, matching Python).
fn const_in_choices(
    args: &crate::arguments::Arguments,
    pos: usize,
    choices: &[serde_json::Value],
) -> bool {
    let s = args.const_str(pos);
    let f = args.const_f64(pos);
    // Only treat as bool when it isn't numeric (const_bool has an int8 fallback).
    let b = if f.is_none() {
        args.const_bool(pos)
    } else {
        None
    };
    if s.is_none() && f.is_none() && b.is_none() {
        return true; // null / absent — skip
    }
    choices.iter().any(|c| match c {
        serde_json::Value::String(cs) => s.as_deref() == Some(cs.as_str()),
        serde_json::Value::Number(n) => match (n.as_f64(), f) {
            (Some(a), Some(v)) => a == v,
            _ => false,
        },
        serde_json::Value::Bool(cb) => b == Some(*cb),
        _ => false,
    })
}

/// Optimizer- and discovery-facing function metadata (`FunctionInfo`).
#[derive(Debug, Clone)]
pub struct FunctionMetadata {
    pub description: String,
    pub stability: Option<String>,
    pub null_handling: Option<String>,
    pub categories: Vec<String>,
    /// SQL usage examples surfaced in `FunctionInfo` for discovery.
    pub examples: Vec<FunctionExample>,
    /// Arbitrary worker-set tags surfaced via `FunctionInfo.tags`
    /// (`duckdb_functions().tags`), e.g. `vgi.columns_md` documenting a table
    /// function's returned columns. Merged with any extension-derived tags.
    pub tags: Vec<(String, String)>,
    /// Fixed scalar return type, when not computed dynamically at bind.
    pub return_type: Option<DataType>,
    pub projection_pushdown: bool,
    pub filter_pushdown: bool,
    pub sampling_pushdown: bool,
    /// Worker-side: auto-apply pushed-down filters to emitted batches.
    pub auto_apply_filters: bool,
    pub supports_batch_index: bool,
    pub partition_kind: Option<String>,
    pub order_preservation: Option<String>,
    /// Table-buffering ordering knobs (surfaced in `FunctionInfo`).
    pub sink_order_dependent: bool,
    pub source_order_dependent: bool,
    pub requires_input_batch_index: bool,
    /// Aggregate window / streaming opt-ins.
    pub supports_window: bool,
    pub streaming_partitioned: bool,
    /// Rowid table participates in late-materialization (Top-N → SEMI rewrite).
    pub late_materialization: bool,
    /// Settings the function requires (surfaced in `FunctionInfo`).
    pub required_settings: Vec<String>,
    /// Secrets the function requires (surfaced in `FunctionInfo.required_secrets`).
    /// The extension pre-resolves each advertised secret and delivers it on the
    /// bind request. Used by aggregates (which cannot do two-phase `.get()`
    /// resolution) to read a secret *value* statically at bind time via
    /// `params.secrets`.
    pub required_secrets: Vec<crate::secrets::SecretLookup>,
    /// Upper bound on concurrent scan processes the extension may run for this
    /// function (surfaced as `FunctionInfo.max_workers`; drives the table scan's
    /// `MaxThreads`). 0 = unset (extension default). >1 lets a single scan fan out
    /// across DuckDB scan threads, each acquiring its own worker connection.
    pub max_workers: i32,
    /// Blended ("UNNEST-style") table-in-out: the function's positional args ARE
    /// its per-row input columns (real typed args, no synthetic TABLE
    /// placeholder), so ONE registration serves `f(52,13)` (literal → 1 input
    /// row), `FROM t, f(t.x, t.y)` (columns → streaming), and
    /// `LATERAL f(t.x, t.y)`. Mirrors the Python SDK's `RowTransformFunction`.
    /// Only meaningful on a [`TableInOutFunction`](crate::table_in_out::TableInOutFunction);
    /// the registration validates the blended contract (no `finish`, no TABLE
    /// arg, no positional const arg, ≥1 positional column arg). Positional args
    /// are read from the input `batch` in `process` (by declared name, or
    /// positionally for varargs); named args stay bind-time scalars on
    /// `ProcessParams::arguments`. Surfaced as `FunctionInfo.input_from_args`.
    pub input_from_args: bool,
}

impl Default for FunctionMetadata {
    fn default() -> Self {
        FunctionMetadata {
            description: String::new(),
            stability: Some(enums::stability::CONSISTENT.to_string()),
            null_handling: None,
            categories: Vec::new(),
            examples: Vec::new(),
            tags: Vec::new(),
            return_type: None,
            projection_pushdown: false,
            filter_pushdown: false,
            sampling_pushdown: false,
            auto_apply_filters: false,
            supports_batch_index: false,
            partition_kind: None,
            order_preservation: None,
            sink_order_dependent: false,
            source_order_dependent: false,
            requires_input_batch_index: false,
            supports_window: false,
            streaming_partitioned: false,
            late_materialization: false,
            required_settings: Vec::new(),
            required_secrets: Vec::new(),
            max_workers: 0,
            input_from_args: false,
        }
    }
}

/// Parameters delivered to `on_bind`.
#[derive(Clone, Default)]
pub struct BindParams {
    /// Input table schema (the argument columns for scalar functions).
    pub input_schema: Option<SchemaRef>,
    /// Parsed call arguments (const values + positional types).
    pub arguments: crate::arguments::Arguments,
    /// Parsed session settings.
    pub settings: crate::settings::Settings,
    /// Resolved secrets, when provided in a second-phase bind.
    pub secrets: crate::secrets::Secrets,
    /// Whether resolved secrets were provided.
    pub resolved_secrets_provided: bool,
    /// Authenticated principal name, if any.
    pub auth_principal: Option<String>,
    /// Sealed attach state.
    pub attach_opaque_data: Option<Vec<u8>>,
    /// Sealed transaction state.
    pub transaction_opaque_data: Option<Vec<u8>>,
    /// Cross-process kv/work store (for transaction-scoped caching, etc.).
    pub storage: Option<crate::storage::SharedStorage>,
    /// `COPY ... FROM` context — `Some` only when this bind opens a COPY-FROM
    /// scan (see [`crate::copy_from::CopyFromFunction`]). `None` otherwise.
    pub copy_from: Option<crate::protocol::dtos::CopyFromContext>,
    /// `COPY ... TO` context — `Some` only when this bind opens a COPY-TO sink
    /// (see [`crate::copy_to::CopyToFunction`]). A COPY-TO writer scopes its
    /// `secret_lookups` request to `copy_to.file_path`. `None` otherwise.
    pub copy_to: Option<crate::protocol::dtos::CopyToContext>,
}

/// Result of `on_bind`.
#[derive(Clone)]
pub struct BindResponse {
    pub output_schema: SchemaRef,
    pub opaque_data: Vec<u8>,
}

impl BindResponse {
    /// A single `result` column of `ty` (the canonical scalar bind result).
    pub fn result(ty: DataType) -> Self {
        BindResponse {
            output_schema: Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "result", ty, true,
            )])),
            opaque_data: Vec::new(),
        }
    }
}

/// Parameters delivered to `process`.
#[derive(Clone)]
pub struct ProcessParams {
    pub output_schema: SchemaRef,
    pub input_schema: Option<SchemaRef>,
    pub execution_id: Vec<u8>,
    /// Stable client-minted id for this streaming table-in-out substream.
    ///
    /// Present (identical across init / every `process` / `finish`) when the
    /// client fanned this function out across per-substream workers; use it to
    /// key per-substream accumulated state in shared storage so a `finish` that
    /// lands on a different HTTP backend than the `process` calls still finds
    /// it. `None` for the serial path or an old client that did not supply one.
    /// See `InitRequest::substream_id`.
    pub substream_id: Option<Vec<u8>>,
    pub init_opaque_data: Vec<u8>,
    /// Parsed call arguments (const values).
    pub arguments: crate::arguments::Arguments,
    /// Parsed session settings.
    pub settings: crate::settings::Settings,
    /// Resolved secrets.
    pub secrets: crate::secrets::Secrets,
    /// Authenticated principal name, if any.
    pub auth_principal: Option<String>,
    /// Projection pushdown: output column indices to emit (None = all).
    pub projection_ids: Option<Vec<i64>>,
    /// Serialized pushdown filters (large_binary), if any.
    pub pushdown_filters: Option<Vec<u8>>,
    /// Side join-keys IPC batches referenced by `join_keys` filters.
    pub join_keys: Vec<Vec<u8>>,
    /// Cross-process work-queue / kv store (for parallel-scan producers).
    pub storage: Option<crate::storage::SharedStorage>,
    /// ORDER BY pushdown hints.
    pub order_by_column: Option<String>,
    pub order_by_direction: Option<String>,
    pub order_by_null_order: Option<String>,
    pub order_by_limit: Option<i64>,
    /// TABLESAMPLE pushdown hints.
    pub tablesample_percentage: Option<f64>,
    pub tablesample_seed: Option<i64>,
    /// The (plaintext) attach state for this call, when carried by the request.
    pub attach_opaque_data: Option<Vec<u8>>,
    /// Time-travel `AT (TIMESTAMP|VERSION ...)` clause for this scan, read from
    /// the per-scan bind request carried on the init request. Both `None`
    /// without an AT clause. Function-backed tables read these to time-travel.
    pub at_unit: Option<String>,
    pub at_value: Option<String>,
    /// `COPY ... FROM` context — `Some` only when this scan is a COPY-FROM read
    /// (see [`crate::copy_from::CopyFromFunction`]). Carries the source
    /// `file_path` and the COPY target's `expected_schema`.
    pub copy_from: Option<crate::protocol::dtos::CopyFromContext>,
    /// Conditional-revalidation validator (exchange-mode result cache): the
    /// client holds a stale cached result for THIS input unit and asks the
    /// worker to confirm freshness cheaply. A table-in-out `process` that
    /// advertised [`CacheControl::with_revalidatable`](crate::cache_control::CacheControl::with_revalidatable)
    /// compares this against its current ETag and, when unchanged, answers with
    /// a 0-row batch carrying
    /// [`CacheControl::with_not_modified`](crate::cache_control::CacheControl::with_not_modified)
    /// instead of recomputing. `None` on a normal call. (The producer path
    /// surfaces the same validators via
    /// [`TableProducer::on_conditional_request`](crate::table_function::TableProducer::on_conditional_request).)
    pub if_none_match: Option<String>,
    /// Companion Last-Modified validator; see [`if_none_match`](Self::if_none_match).
    pub if_modified_since: Option<String>,
}

/// A scalar VGI function: one output row per input row.
///
/// A scalar function receives a [`RecordBatch`](arrow_array::RecordBatch) of its
/// argument columns and returns a single-column batch (the column is named
/// `result`) with the same number of rows. Implement [`name`](Self::name),
/// [`metadata`](Self::metadata), [`argument_specs`](Self::argument_specs), and
/// [`process`](Self::process); [`on_bind`](Self::on_bind) has a sensible default
/// and is only overridden when the return type is computed from the argument
/// types. Register the function with
/// [`Worker::register_scalar`](crate::Worker::register_scalar).
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use arrow_array::{cast::AsArray, ArrayRef, RecordBatch, StringArray};
/// use arrow_schema::DataType;
/// use vgi::{ArgSpec, FunctionMetadata, ProcessParams, ScalarFunction};
/// use vgi_rpc::{Result, RpcError};
///
/// struct UpperCase;
///
/// impl ScalarFunction for UpperCase {
///     fn name(&self) -> &str {
///         "upper_case"
///     }
///
///     fn metadata(&self) -> FunctionMetadata {
///         FunctionMetadata {
///             description: "Uppercase a string".into(),
///             return_type: Some(DataType::Utf8),
///             ..Default::default()
///         }
///     }
///
///     fn argument_specs(&self) -> Vec<ArgSpec> {
///         vec![ArgSpec::column("value", 0, "varchar", "String to uppercase")]
///     }
///
///     fn process(&self, params: &ProcessParams, batch: &RecordBatch) -> Result<RecordBatch> {
///         let col = batch.column(0).as_string::<i32>();
///         let out: ArrayRef = Arc::new(
///             col.iter().map(|v| v.map(str::to_uppercase)).collect::<StringArray>(),
///         );
///         RecordBatch::try_new(params.output_schema.clone(), vec![out])
///             .map_err(|e| RpcError::runtime_error(e.to_string()))
///     }
/// }
/// ```
pub trait ScalarFunction: Send + Sync {
    /// The SQL name this function is exposed as (e.g. `"upper_case"`). Multiple
    /// impls may share a name to form a typed overload set.
    fn name(&self) -> &str;

    /// Optimizer- and discovery-facing properties: description, return type,
    /// stability, null handling, and pushdown opt-ins. Start from
    /// [`FunctionMetadata::default`] and set only what you need.
    fn metadata(&self) -> FunctionMetadata;

    /// The argument list, built with the [`ArgSpec`] constructors
    /// ([`column`](ArgSpec::column), [`const_arg`](ArgSpec::const_arg), …).
    /// Positions are 0-based and match the columns read in
    /// [`process`](Self::process).
    fn argument_specs(&self) -> Vec<ArgSpec>;
    /// Secret lookups to request at bind (two-phase secret resolution). When
    /// non-empty and secrets are not yet resolved, `bind` returns these and the
    /// extension re-binds with the resolved values.
    fn secret_lookups(&self, _params: &BindParams) -> Vec<crate::secrets::SecretLookup> {
        Vec::new()
    }
    /// Resolve the output schema. Default: a `result` column whose type is the
    /// metadata `return_type` if fixed, else the first input field's type.
    /// Override to compute the return type from the argument types, returning
    /// [`BindResponse::result`] with the chosen type.
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        if let Some(ty) = self.metadata().return_type {
            return Ok(BindResponse::result(ty));
        }
        let ty = params
            .input_schema
            .as_ref()
            .and_then(|s| s.fields().first().map(|f| f.data_type().clone()))
            .unwrap_or(DataType::Int64);
        Ok(BindResponse::result(ty))
    }
    /// Transform one input batch into a single-column `result` batch with the
    /// same row count.
    ///
    /// Build the output against [`ProcessParams::output_schema`] (the schema
    /// chosen at bind). Const arguments are available via
    /// [`ProcessParams::arguments`]; column arguments arrive as the columns of
    /// `batch`, in [`argument_specs`](Self::argument_specs) order.
    fn process(
        &self,
        params: &ProcessParams,
        batch: &arrow_array::RecordBatch,
    ) -> Result<arrow_array::RecordBatch>;
}

#[cfg(test)]
mod constraint_tests {
    use super::*;
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use std::sync::Arc;

    fn args_i64(v: i64) -> crate::arguments::Arguments {
        crate::arguments::Arguments {
            positional: vec![Some(Arc::new(Int64Array::from(vec![v])) as ArrayRef)],
            ..Default::default()
        }
    }

    fn args_str(v: &str) -> crate::arguments::Arguments {
        crate::arguments::Arguments {
            positional: vec![Some(Arc::new(StringArray::from(vec![v])) as ArrayRef)],
            ..Default::default()
        }
    }

    #[test]
    fn const_range_enforced() {
        let specs = vec![ArgSpec::const_arg("precision", 0, "int64", "")
            .with_ge(0.0)
            .with_le(10.0)];
        assert!(validate_arg_constraints(&specs, &args_i64(5)).is_ok());
        assert!(validate_arg_constraints(&specs, &args_i64(99)).is_err());
        assert!(validate_arg_constraints(&specs, &args_i64(-1)).is_err());
    }

    #[test]
    fn const_choices_enforced() {
        let specs =
            vec![ArgSpec::const_arg("unit", 0, "varchar", "").with_choices(["mm", "cm", "m"])];
        assert!(validate_arg_constraints(&specs, &args_str("cm")).is_ok());
        assert!(validate_arg_constraints(&specs, &args_str("xx")).is_err());
    }

    #[test]
    fn const_pattern_enforced() {
        let specs = vec![ArgSpec::const_arg("code", 0, "varchar", "").with_pattern("^[A-Z]{2}$")];
        assert!(validate_arg_constraints(&specs, &args_str("AB")).is_ok());
        assert!(validate_arg_constraints(&specs, &args_str("abc")).is_err());
    }

    #[test]
    fn column_arg_not_enforced() {
        // A non-const column argument is never enforced, even if it carries bounds.
        let specs = vec![ArgSpec::column("value", 0, "int64", "")
            .with_ge(0.0)
            .with_le(10.0)];
        assert!(validate_arg_constraints(&specs, &args_i64(99)).is_ok());
    }
}

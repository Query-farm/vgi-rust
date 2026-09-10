// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Strict consumer and Arrow evaluator for VGI Filter Encoding v2.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray, StructArray};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde::Deserialize;
use serde_json::Value;
use vgi_rpc::{Result, RpcError};

use crate::ipc;

const ENCODING: &str = "vgi.filters.v2";
const VERSION: &str = "2";
const SEMANTICS: &str = "vgi.duckdb.standard.v1";
const NO_CONTEXT: &str = "vgi.none.v1";
const SESSION_CONTEXT: &str = "vgi.duckdb.session.v1";
const MAX_JSON: usize = 1 << 20;
const MAX_PAYLOAD: usize = 16 << 20;
const MAX_DEPTH: usize = 64;
const MAX_NODES: usize = 10_000;
const MAX_PREDICATES: usize = 1_024;
const MAX_IDS: usize = 4_096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PredicateMode {
    Required,
    Advisory,
}

#[derive(Clone)]
enum Expr {
    Column {
        index: usize,
        name: String,
        data_type: DataType,
    },
    Field {
        expression: Box<Expr>,
        index: usize,
        name: String,
        data_type: DataType,
    },
    Literal(ArrayRef),
    Comparison {
        op: String,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    IsNull {
        expression: Box<Expr>,
        negated: bool,
    },
    In {
        expression: Box<Expr>,
        values: ArrayRef,
        negated: bool,
    },
    Cast {
        expression: Box<Expr>,
        target: DataType,
    },
    Arithmetic {
        op: String,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Negate(Box<Expr>),
    Call {
        function: String,
        arguments: Vec<Expr>,
    },
    RuntimeFilter,
}

#[derive(Clone)]
struct Predicate {
    id: String,
    revision: u64,
    mode: PredicateMode,
    expression: Expr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EvaluationContext {
    profile: String,
    time_zone: Option<String>,
    calendar: Option<String>,
    default_collation: Option<String>,
    ieee_floating_point_ops: Option<bool>,
    integer_division: Option<bool>,
    provider_fingerprint: Option<String>,
}

#[derive(Clone)]
pub struct PushdownFilters {
    predicates: Vec<Predicate>,
    revisions: HashMap<String, u64>,
    required_ids: HashSet<String>,
    context: EvaluationContext,
    join_keys: Vec<RecordBatch>,
    output_schema: SchemaRef,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnBounds {
    pub min: Option<i64>,
    pub max: Option<i64>,
}

#[derive(Debug, Clone)]
pub enum Filter {
    Constant {
        column_name: String,
        op: String,
    },
    In {
        column_name: String,
    },
    JoinKeys {
        column_name: String,
    },
    IsNull {
        column_name: String,
    },
    IsNotNull {
        column_name: String,
    },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Struct {
        column_name: String,
        child_name: String,
        child: Box<Filter>,
    },
    Other {
        kind: String,
        column_name: String,
    },
}

impl Filter {
    pub fn column_name(&self) -> &str {
        match self {
            Self::Constant { column_name, .. }
            | Self::In { column_name }
            | Self::JoinKeys { column_name }
            | Self::IsNull { column_name }
            | Self::IsNotNull { column_name }
            | Self::Struct { column_name, .. }
            | Self::Other { column_name, .. } => column_name,
            Self::And(_) | Self::Or(_) => "",
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Document {
    #[serde(rename = "snapshot")]
    Snapshot {
        encoding: String,
        semantics: String,
        predicates: Vec<Value>,
    },
    #[serde(rename = "delta")]
    Delta {
        encoding: String,
        semantics: String,
        updates: Vec<Value>,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePredicate {
    id: String,
    revision: u64,
    mode: String,
    source: String,
    expression: WireExpr,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireUpdate {
    operation: String,
    id: String,
    revision: u64,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    expression: Option<WireExpr>,
}

#[derive(Deserialize)]
#[serde(tag = "node", rename_all = "snake_case", deny_unknown_fields)]
enum WireExpr {
    ColumnRef {
        column_index: u64,
        column_name: String,
    },
    FieldRef {
        expression: Box<WireExpr>,
        field_index: u64,
        field_name: String,
    },
    Literal {
        value_ref: u64,
    },
    Comparison {
        op: String,
        left: Box<WireExpr>,
        right: Box<WireExpr>,
    },
    And {
        children: Vec<WireExpr>,
    },
    Or {
        children: Vec<WireExpr>,
    },
    Not {
        expression: Box<WireExpr>,
    },
    IsNull {
        expression: Box<WireExpr>,
        negated: bool,
    },
    In {
        expression: Box<WireExpr>,
        set: WireSet,
        negated: bool,
    },
    Cast {
        expression: Box<WireExpr>,
        type_ref: u64,
    },
    Arithmetic {
        op: String,
        left: Box<WireExpr>,
        right: Box<WireExpr>,
    },
    Negate {
        expression: Box<WireExpr>,
    },
    Call {
        function: WireFunction,
        arguments: Vec<WireExpr>,
        #[serde(default)]
        options: Option<Value>,
    },
    RuntimeFilter {
        algorithm: Identity,
        input: Box<WireExpr>,
        artifact_ref: u64,
        null_handling: String,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum WireSet {
    Literal {
        value_ref: u64,
    },
    External {
        batch_index: u64,
        column_index: u64,
        column_name: String,
    },
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WireFunction {
    Standard(String),
    Extension(Identity),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    namespace: String,
    name: String,
    version: u64,
}

impl PushdownFilters {
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        Self::parse_with_join_keys(bytes, &[])
    }

    pub fn parse_b64_with_schema(
        encoded: &str,
        join_keys: &[Vec<u8>],
        output_schema: SchemaRef,
    ) -> Result<Self> {
        let bytes = b64_decode(encoded).ok_or_else(|| value_error("invalid base64 filter"))?;
        Self::parse_with_join_keys_and_schema(&bytes, join_keys, Some(output_schema))
    }

    pub fn apply_delta_b64(&mut self, encoded: &str) -> Result<()> {
        let bytes =
            b64_decode(encoded).ok_or_else(|| value_error("invalid base64 filter delta"))?;
        self.apply_delta(&bytes)
    }

    pub fn parse_with_join_keys(bytes: &[u8], join_keys: &[Vec<u8>]) -> Result<Self> {
        Self::parse_with_join_keys_and_schema(bytes, join_keys, None)
    }

    pub fn parse_with_schema(bytes: &[u8], output_schema: SchemaRef) -> Result<Self> {
        Self::parse_with_join_keys_and_schema(bytes, &[], Some(output_schema))
    }

    pub fn parse_with_join_keys_and_schema(
        bytes: &[u8],
        join_keys: &[Vec<u8>],
        output_schema: Option<SchemaRef>,
    ) -> Result<Self> {
        if bytes.len() > MAX_PAYLOAD {
            return Err(value_error("filter payload exceeds 16 MiB"));
        }
        validate_wire_schema(ipc::read_schema(bytes)?.as_ref())?;
        let batch = ipc::read_batch(bytes)?;
        let keys = join_keys
            .iter()
            .map(|v| ipc::read_batch(v))
            .collect::<Result<Vec<_>>>()?;
        Self::parse_snapshot_batch(&batch, keys, output_schema)
    }

    fn parse_snapshot_batch(
        batch: &RecordBatch,
        join_keys: Vec<RecordBatch>,
        output_schema: Option<SchemaRef>,
    ) -> Result<Self> {
        let (context, document) = validate_batch(batch)?;
        let Document::Snapshot {
            encoding,
            semantics,
            predicates,
        } = document
        else {
            return Err(value_error("initial filter document must be a snapshot"));
        };
        validate_header(&encoding, &semantics)?;
        if predicates.len() > MAX_PREDICATES {
            return Err(value_error("snapshot exceeds predicate limit"));
        }
        if !predicates.is_empty() && output_schema.is_none() {
            return Err(value_error(
                "Filter Encoding v2 requires the authoritative unprojected bind output schema",
            ));
        }
        let output_schema = output_schema.unwrap_or_else(|| Arc::new(Schema::empty()));
        let mut parser = Parser {
            batch,
            join_keys: &join_keys,
            output_schema: output_schema.as_ref(),
            nodes: 0,
        };
        let mut output = Vec::with_capacity(predicates.len());
        let mut revisions = HashMap::new();
        let mut required_ids = HashSet::new();
        for raw in predicates {
            let wire: WirePredicate = serde_json::from_value(raw).map_err(json_error)?;
            let predicate = parser.predicate(wire, false)?;
            if predicate.revision != 0 {
                return Err(value_error("snapshot predicate revisions must be zero"));
            }
            if revisions.insert(predicate.id.clone(), 0).is_some() {
                return Err(value_error("duplicate predicate ID"));
            }
            if predicate.mode == PredicateMode::Required {
                required_ids.insert(predicate.id.clone());
            }
            output.push(predicate);
        }
        Ok(Self {
            predicates: output,
            revisions,
            required_ids,
            context,
            join_keys,
            output_schema,
        })
    }

    /// Validate and atomically apply a v2 delta to this per-scan state.
    pub fn apply_delta(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX_PAYLOAD {
            return Err(value_error("filter payload exceeds 16 MiB"));
        }
        validate_wire_schema(ipc::read_schema(bytes)?.as_ref())?;
        let batch = ipc::read_batch(bytes)?;
        let (context, document) = validate_batch(&batch)?;
        if context != self.context {
            return Err(value_error("evaluation context changed within one scan"));
        }
        let Document::Delta {
            encoding,
            semantics,
            updates,
        } = document
        else {
            return Err(value_error("dynamic filter document must be a delta"));
        };
        validate_header(&encoding, &semantics)?;
        let mut next = self.clone();
        let mut seen = HashSet::new();
        let mut parser = Parser {
            batch: &batch,
            join_keys: &self.join_keys,
            output_schema: self.output_schema.as_ref(),
            nodes: 0,
        };
        for raw in updates {
            let update: WireUpdate = serde_json::from_value(raw).map_err(json_error)?;
            validate_id(&update.id)?;
            if !seen.insert(update.id.clone()) {
                return Err(value_error("duplicate delta predicate ID"));
            }
            if self.required_ids.contains(&update.id) {
                return Err(value_error("delta targets required predicate"));
            }
            match update.operation.as_str() {
                "remove" => {
                    if update.mode.is_some()
                        || update.source.is_some()
                        || update.expression.is_some()
                    {
                        return Err(value_error("remove update has predicate properties"));
                    }
                }
                "upsert" => {
                    if update.mode.as_deref() != Some("advisory")
                        || !update.source.as_deref().is_some_and(|source| {
                            matches!(
                                source,
                                "query" | "join" | "top_n" | "split_refinement" | "other"
                            )
                        })
                        || update.expression.is_none()
                    {
                        return Err(value_error("invalid delta upsert properties"));
                    }
                }
                _ => return Err(value_error("delta operation must be remove or upsert")),
            }
            if next
                .revisions
                .get(&update.id)
                .is_some_and(|old| update.revision <= *old)
            {
                continue;
            }
            match update.operation.as_str() {
                "remove" => {
                    next.predicates.retain(|p| p.id != update.id);
                }
                "upsert" => {
                    let predicate = parser.predicate(
                        WirePredicate {
                            id: update.id.clone(),
                            revision: update.revision,
                            mode: update
                                .mode
                                .ok_or_else(|| value_error("upsert missing mode"))?,
                            source: update
                                .source
                                .ok_or_else(|| value_error("upsert missing source"))?,
                            expression: update
                                .expression
                                .ok_or_else(|| value_error("upsert missing expression"))?,
                        },
                        true,
                    )?;
                    next.predicates.retain(|p| p.id != update.id);
                    next.predicates.push(predicate);
                }
                _ => return Err(value_error("delta operation must be remove or upsert")),
            }
            next.revisions.insert(update.id, update.revision);
        }
        if next.revisions.len() > MAX_IDS {
            return Err(value_error("delta exceeds predicate-ID limit"));
        }
        *self = next;
        Ok(())
    }

    pub fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        arrow_select::filter::filter_record_batch(batch, &self.evaluate(batch)?).map_err(cvt)
    }

    pub fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        let mut result = all_true(batch.num_rows());
        for predicate in &self.predicates {
            if matches!(predicate.expression, Expr::RuntimeFilter) {
                continue;
            }
            match eval_bool(&predicate.expression, batch) {
                Ok(current) => result = and_kleene(&result, &current)?,
                Err(_) if predicate.mode == PredicateMode::Advisory => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(result)
    }

    pub fn format_pushed(&self) -> String {
        if self.predicates.is_empty() {
            return "(none)".to_string();
        }
        self.predicates
            .iter()
            .map(|p| format_expr(&p.expression))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    pub fn format_repr(&self) -> String {
        if self.predicates.is_empty() {
            "(none)".into()
        } else {
            format!(
                "PushdownFilters([{}])",
                self.predicates
                    .iter()
                    .map(|predicate| format_filter_repr(&predicate.expression))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }

    pub fn filtered_columns(&self) -> HashSet<String> {
        let mut result = HashSet::new();
        for predicate in &self.predicates {
            collect_columns(&predicate.expression, &mut result);
        }
        result
    }

    pub fn has_filter_for_column(&self, column: &str) -> bool {
        self.filtered_columns().contains(column)
    }
    pub fn filters(&self) -> Vec<Filter> {
        self.predicates
            .iter()
            .map(|p| filter_view(&p.expression))
            .collect()
    }
    pub fn get_column_filters(&self, column: &str) -> Vec<Filter> {
        self.predicates
            .iter()
            .filter(|p| mentions(&p.expression, column))
            .map(|p| filter_view(&p.expression))
            .collect()
    }
    pub fn get_column_values(&self, column: &str) -> Option<ArrayRef> {
        self.predicates
            .iter()
            .find_map(|p| values_for(&p.expression, column))
    }
    pub fn get_column_in_values(&self, column: &str) -> Option<ArrayRef> {
        self.predicates
            .iter()
            .find_map(|p| in_values_for(&p.expression, column))
    }
    pub fn get_column_constant(&self, column: &str) -> Option<ArrayRef> {
        self.predicates
            .iter()
            .find_map(|p| constant_for(&p.expression, column))
    }
    pub fn get_column_values_i64(&self, column: &str) -> Option<Vec<i64>> {
        let casted = arrow_cast::cast(&self.get_column_values(column)?, &DataType::Int64).ok()?;
        let values = casted.as_primitive::<arrow_array::types::Int64Type>();
        Some(
            (0..values.len())
                .filter(|i| values.is_valid(*i))
                .map(|i| values.value(i))
                .collect(),
        )
    }
    pub fn column_summary(&self, column: &str) -> (usize, Option<i64>, Option<i64>) {
        let values = self.get_column_values_i64(column).unwrap_or_default();
        let bounds = self.get_column_bounds(column);
        let (min, max) = bounds
            .map(|bounds| (bounds.min, bounds.max))
            .unwrap_or((None, None));
        (values.len(), min, max)
    }
    pub fn get_column_bounds(&self, column: &str) -> Option<ColumnBounds> {
        self.predicates
            .iter()
            .filter_map(|predicate| bounds_for(&predicate.expression, column))
            .reduce(intersect_bounds)
    }
}

struct Parser<'a> {
    batch: &'a RecordBatch,
    join_keys: &'a [RecordBatch],
    output_schema: &'a Schema,
    nodes: usize,
}

impl Parser<'_> {
    fn expression_type(&self, expression: &Expr) -> Option<DataType> {
        expr_type(expression)
    }

    fn predicate(&mut self, wire: WirePredicate, delta: bool) -> Result<Predicate> {
        validate_id(&wire.id)?;
        let mode = match wire.mode.as_str() {
            "required" if !delta => PredicateMode::Required,
            "advisory" => PredicateMode::Advisory,
            "required" => return Err(value_error("delta upserts must be advisory")),
            _ => return Err(value_error("unknown predicate mode")),
        };
        if !matches!(
            wire.source.as_str(),
            "query" | "join" | "top_n" | "split_refinement" | "other"
        ) {
            return Err(value_error("unknown predicate source"));
        }
        let expression = self.expression(wire.expression, 1, true)?;
        if matches!(expression, Expr::RuntimeFilter) && mode != PredicateMode::Advisory {
            return Err(value_error("runtime_filter predicates must be advisory"));
        }
        if !matches!(expression, Expr::RuntimeFilter)
            && self.expression_type(&expression) != Some(DataType::Boolean)
        {
            return Err(value_error("predicate root must resolve to BOOLEAN"));
        }
        let profile = self
            .batch
            .schema()
            .metadata()
            .get("vgi_evaluation_context")
            .cloned()
            .unwrap_or_default();
        if profile == NO_CONTEXT && requires_context(&expression) {
            return Err(value_error(
                "context-dependent expression requires session context",
            ));
        }
        Ok(Predicate {
            id: wire.id,
            revision: wire.revision,
            mode,
            expression,
        })
    }

    fn expression(&mut self, wire: WireExpr, depth: usize, root: bool) -> Result<Expr> {
        if depth > MAX_DEPTH {
            return Err(value_error("expression exceeds nesting-depth limit"));
        }
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return Err(value_error("document exceeds expression-node limit"));
        }
        macro_rules! child {
            ($value:expr) => {
                self.expression($value, depth + 1, false)?
            };
        }
        Ok(match wire {
            WireExpr::ColumnRef {
                column_index,
                column_name,
            } => {
                let index = to_usize(column_index)?;
                let name = nonempty(column_name, "column name")?;
                let field = self.output_schema.fields().get(index).ok_or_else(|| {
                    value_error("column_ref index is outside the authoritative output schema")
                })?;
                if field.name() != &name {
                    return Err(value_error(
                        "column_ref name does not match authoritative index",
                    ));
                }
                Expr::Column {
                    index,
                    name,
                    data_type: field.data_type().clone(),
                }
            }
            WireExpr::FieldRef {
                expression,
                field_index,
                field_name,
            } => {
                let expression = child!(*expression);
                let index = to_usize(field_index)?;
                let name = nonempty(field_name, "field name")?;
                let DataType::Struct(fields) = self
                    .expression_type(&expression)
                    .ok_or_else(|| value_error("field_ref parent has no authoritative type"))?
                else {
                    return Err(value_error("field_ref parent is not STRUCT"));
                };
                let field = fields.get(index).ok_or_else(|| {
                    value_error("field_ref index is outside the authoritative struct")
                })?;
                if field.name() != &name {
                    return Err(value_error(
                        "field_ref name does not match authoritative index",
                    ));
                }
                Expr::Field {
                    expression: Box::new(expression),
                    index,
                    name,
                    data_type: field.data_type().clone(),
                }
            }
            WireExpr::Literal { value_ref } => Expr::Literal(self.payload("value", value_ref)?.1),
            WireExpr::Comparison { op, left, right } => {
                if !matches!(
                    op.as_str(),
                    "eq" | "ne" | "lt" | "le" | "gt" | "ge" | "distinct_from" | "not_distinct_from"
                ) {
                    return Err(value_error("unknown comparison operator"));
                }
                let left = child!(*left);
                let right = child!(*right);
                if self.expression_type(&left).as_ref().map(logical_value_type)
                    != self
                        .expression_type(&right)
                        .as_ref()
                        .map(logical_value_type)
                {
                    return Err(value_error("comparison operands have incompatible types"));
                }
                Expr::Comparison {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            WireExpr::And { children } => {
                if children.len() < 2 {
                    return Err(value_error("and requires at least two children"));
                }
                let values = children
                    .into_iter()
                    .map(|v| self.expression(v, depth + 1, false))
                    .collect::<Result<Vec<_>>>()?;
                if values
                    .iter()
                    .any(|value| self.expression_type(value) != Some(DataType::Boolean))
                {
                    return Err(value_error("and children must resolve to BOOLEAN"));
                }
                Expr::And(values)
            }
            WireExpr::Or { children } => {
                if children.len() < 2 {
                    return Err(value_error("or requires at least two children"));
                }
                let values = children
                    .into_iter()
                    .map(|v| self.expression(v, depth + 1, false))
                    .collect::<Result<Vec<_>>>()?;
                if values
                    .iter()
                    .any(|value| self.expression_type(value) != Some(DataType::Boolean))
                {
                    return Err(value_error("or children must resolve to BOOLEAN"));
                }
                Expr::Or(values)
            }
            WireExpr::Not { expression } => {
                let expression = child!(*expression);
                if self.expression_type(&expression) != Some(DataType::Boolean) {
                    return Err(value_error("not operand must resolve to BOOLEAN"));
                }
                Expr::Not(Box::new(expression))
            }
            WireExpr::IsNull {
                expression,
                negated,
            } => Expr::IsNull {
                expression: Box::new(child!(*expression)),
                negated,
            },
            WireExpr::In {
                expression,
                set,
                negated,
            } => {
                let expression = child!(*expression);
                let values = match set {
                    WireSet::Literal { value_ref } => {
                        let (_, array) = self.payload("value", value_ref)?;
                        match array.data_type() {
                            DataType::List(_) => {
                                let list = array.as_list::<i32>();
                                if list.is_null(0) {
                                    return Err(value_error("literal IN list must not be NULL"));
                                }
                                list.value(0)
                            }
                            DataType::LargeList(_) => {
                                let list = array.as_list::<i64>();
                                if list.is_null(0) {
                                    return Err(value_error("literal IN list must not be NULL"));
                                }
                                list.value(0)
                            }
                            _ => {
                                return Err(value_error("literal IN payload must be a list scalar"))
                            }
                        }
                    }
                    WireSet::External {
                        batch_index,
                        column_index,
                        column_name,
                    } => {
                        let batch = self
                            .join_keys
                            .get(to_usize(batch_index)?)
                            .ok_or_else(|| value_error("external IN batch unavailable"))?;
                        let index = to_usize(column_index)?;
                        let schema = batch.schema();
                        let field = schema
                            .fields()
                            .get(index)
                            .ok_or_else(|| value_error("external IN column out of range"))?;
                        if field.name() != &column_name {
                            return Err(value_error("external IN name does not match index"));
                        }
                        batch.column(index).clone()
                    }
                };
                if self
                    .expression_type(&expression)
                    .as_ref()
                    .map(logical_value_type)
                    != Some(logical_value_type(values.data_type()))
                {
                    return Err(value_error("IN expression and set have incompatible types"));
                }
                Expr::In {
                    expression: Box::new(expression),
                    values,
                    negated,
                }
            }
            WireExpr::Cast {
                expression,
                type_ref,
            } => {
                let (field, value) = self.payload("type", type_ref)?;
                if !value.is_null(0) {
                    return Err(value_error("cast type payload must contain NULL"));
                }
                Expr::Cast {
                    expression: Box::new(child!(*expression)),
                    target: field.data_type().clone(),
                }
            }
            WireExpr::Arithmetic { op, left, right } => {
                if !matches!(
                    op.as_str(),
                    "add" | "subtract" | "multiply" | "divide" | "modulo"
                ) {
                    return Err(value_error("unknown arithmetic operator"));
                }
                let left = child!(*left);
                let right = child!(*right);
                let left_type = self
                    .expression_type(&left)
                    .ok_or_else(|| value_error("arithmetic left operand has no type"))?;
                if !numeric_type(&left_type) || Some(left_type) != self.expression_type(&right) {
                    return Err(value_error(
                        "arithmetic operands require one exact numeric type",
                    ));
                }
                Expr::Arithmetic {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                }
            }
            WireExpr::Negate { expression } => {
                let expression = child!(*expression);
                if !self
                    .expression_type(&expression)
                    .as_ref()
                    .is_some_and(numeric_type)
                {
                    return Err(value_error("negate operand must be numeric"));
                }
                Expr::Negate(Box::new(expression))
            }
            WireExpr::Call {
                function,
                arguments,
                options,
            } => {
                if arguments.len() > 256 {
                    return Err(value_error("call exceeds argument-count limit"));
                }
                if options
                    .as_ref()
                    .is_some_and(|v| !matches!(v, Value::Object(map) if map.is_empty()))
                {
                    return Err(value_error("filter function does not accept options"));
                }
                let function = match function {
                    WireFunction::Standard(name)
                        if matches!(
                            name.as_str(),
                            "starts_with" | "ends_with" | "contains" | "list_contains"
                        ) =>
                    {
                        name
                    }
                    WireFunction::Standard(_) => {
                        return Err(value_error("unknown standard filter function"))
                    }
                    WireFunction::Extension(identity) => {
                        validate_identity(&identity)?;
                        return Err(value_error("extension filter function was not advertised"));
                    }
                };
                let arguments = arguments
                    .into_iter()
                    .map(|v| self.expression(v, depth + 1, false))
                    .collect::<Result<Vec<_>>>()?;
                if arguments.len() != 2 {
                    return Err(value_error(
                        "standard filter functions require exactly two arguments",
                    ));
                }
                match function.as_str() {
                    "starts_with" | "ends_with" | "contains"
                        if arguments
                            .iter()
                            .any(|v| self.expression_type(v) != Some(DataType::Utf8)) =>
                    {
                        return Err(value_error("string function arguments must be UTF8"));
                    }
                    "list_contains" => {
                        let Some(DataType::List(field)) = self.expression_type(&arguments[0])
                        else {
                            return Err(value_error("list_contains first argument must be LIST"));
                        };
                        if Some(field.data_type().clone()) != self.expression_type(&arguments[1]) {
                            return Err(value_error("list_contains element type mismatch"));
                        }
                    }
                    _ => {}
                }
                Expr::Call {
                    function,
                    arguments,
                }
            }
            WireExpr::RuntimeFilter {
                algorithm,
                input,
                artifact_ref,
                null_handling,
            } => {
                if !root {
                    return Err(value_error(
                        "runtime_filter may appear only at predicate root",
                    ));
                }
                validate_identity(&algorithm)?;
                if !matches!(
                    (
                        algorithm.namespace.as_str(),
                        algorithm.name.as_str(),
                        algorithm.version
                    ),
                    ("duckdb.runtime_filter", "bloom", 1)
                        | ("duckdb.runtime_filter", "prefix_range", 1)
                ) {
                    return Err(value_error("unknown runtime-filter algorithm"));
                }
                if !matches!(null_handling.as_str(), "pass" | "reject") {
                    return Err(value_error("invalid runtime_filter null_handling"));
                }
                self.payload("artifact", artifact_ref)?;
                child!(*input);
                Expr::RuntimeFilter
            }
        })
    }

    fn payload(&self, prefix: &str, reference: u64) -> Result<(Arc<Field>, ArrayRef)> {
        let name = format!("{prefix}_{reference}");
        let schema = self.batch.schema();
        let (index, field) = schema
            .fields()
            .find(&name)
            .ok_or_else(|| value_error(format!("missing payload field {name}")))?;
        Ok((field.clone(), self.batch.column(index).clone()))
    }
}

fn validate_batch(batch: &RecordBatch) -> Result<(EvaluationContext, Document)> {
    if batch.num_rows() != 1 {
        return Err(value_error(
            "filter RecordBatch must contain exactly one row",
        ));
    }
    let schema = batch.schema();
    let first = schema
        .fields()
        .first()
        .ok_or_else(|| value_error("filter RecordBatch has no filter_spec"))?;
    if first.name() != "filter_spec" || first.data_type() != &DataType::Utf8 {
        return Err(value_error(
            "first field must be filter_spec: utf8 not null",
        ));
    }
    let strings = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| value_error("filter_spec is not utf8"))?;
    if strings.is_null(0) {
        return Err(value_error("filter_spec must not be NULL"));
    }
    let raw = strings.value(0);
    if raw.len() > MAX_JSON {
        return Err(value_error("filter JSON exceeds 1 MiB"));
    }
    let mut names = HashSet::new();
    for (index, field) in schema.fields().iter().enumerate().skip(1) {
        if !names.insert(field.name()) || !canonical_payload_name(field.name()) {
            return Err(value_error("noncanonical or duplicate payload field name"));
        }
        if field.name().starts_with("type_") && !batch.column(index).is_null(0) {
            return Err(value_error("type payload must contain NULL"));
        }
    }
    Ok((
        parse_context(schema.as_ref())?,
        serde_json::from_str(raw).map_err(json_error)?,
    ))
}

fn validate_wire_schema(schema: &Schema) -> Result<()> {
    let first = schema
        .fields()
        .first()
        .ok_or_else(|| value_error("filter RecordBatch has no filter_spec"))?;
    if first.name() != "filter_spec" || first.data_type() != &DataType::Utf8 || first.is_nullable()
    {
        return Err(value_error(
            "first field must be filter_spec: utf8 not null",
        ));
    }
    Ok(())
}

fn parse_context(schema: &Schema) -> Result<EvaluationContext> {
    let metadata = schema.metadata();
    let get = |key: &str| {
        metadata
            .get(key)
            .cloned()
            .ok_or_else(|| value_error(format!("missing schema metadata {key}")))
    };
    if get("vgi_filter_encoding")? != ENCODING || get("vgi_filter_version")? != VERSION {
        return Err(value_error("unsupported filter encoding/version"));
    }
    let profile = get("vgi_evaluation_context")?;
    let keys = [
        "vgi_time_zone",
        "vgi_calendar",
        "vgi_default_collation",
        "vgi_ieee_floating_point_ops",
        "vgi_integer_division",
    ];
    if profile == NO_CONTEXT {
        if keys.iter().any(|key| metadata.contains_key(*key))
            || metadata.contains_key("vgi_context_provider_fingerprint")
        {
            return Err(value_error("vgi.none.v1 forbids session context metadata"));
        }
        return Ok(EvaluationContext {
            profile,
            time_zone: None,
            calendar: None,
            default_collation: None,
            ieee_floating_point_ops: None,
            integer_division: None,
            provider_fingerprint: None,
        });
    }
    if profile != SESSION_CONTEXT {
        return Err(value_error("unknown evaluation context"));
    }
    // The Arrow evaluator deliberately advertises no session-context provider.
    // Accepting settings here without applying them would violate required
    // predicate exactness.
    Err(value_error(
        "vgi.duckdb.session.v1 evaluation context was not advertised",
    ))
}

fn validate_header(encoding: &str, semantics: &str) -> Result<()> {
    if encoding != ENCODING {
        return Err(value_error("document encoding must be vgi.filters.v2"));
    }
    if semantics != SEMANTICS {
        return Err(value_error("unsupported filter semantics"));
    }
    Ok(())
}

fn validate_id(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128 {
        Err(value_error(
            "predicate ID must be nonempty and at most 128 bytes",
        ))
    } else {
        Ok(())
    }
}

fn validate_identity(id: &Identity) -> Result<()> {
    let valid = |v: &str| {
        !v.is_empty()
            && v.as_bytes()[0].is_ascii_lowercase()
            && v.bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'.')
    };
    if !valid(&id.namespace)
        || id.namespace.contains("..")
        || !valid(&id.name)
        || id.name.contains('.')
        || id.version == 0
    {
        Err(value_error("noncanonical function identity"))
    } else {
        Ok(())
    }
}

fn canonical_payload_name(name: &str) -> bool {
    ["value_", "type_", "artifact_"].iter().any(|prefix| {
        name.strip_prefix(prefix).is_some_and(|index| {
            index == "0" || (!index.starts_with('0') && index.bytes().all(|c| c.is_ascii_digit()))
        })
    })
}

fn eval(expression: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
    match expression {
        Expr::Column {
            index,
            name,
            data_type,
        } => {
            let schema = batch.schema();
            let resolved = if schema
                .fields()
                .get(*index)
                .is_some_and(|f| f.name() == name)
            {
                *index
            } else {
                let matches = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, field)| field.name() == name)
                    .map(|(index, _)| index)
                    .collect::<Vec<_>>();
                if matches.len() != 1 {
                    return Err(value_error(
                        "authoritatively bound column_ref is absent or ambiguous in evaluation batch",
                    ));
                }
                matches[0]
            };
            if schema.field(resolved).data_type() != data_type {
                return Err(value_error("column_ref type changed in evaluation batch"));
            }
            Ok(batch.column(resolved).clone())
        }
        Expr::Field {
            expression,
            index,
            name,
            data_type,
        } => {
            let parent = eval(expression, batch)?;
            let values = parent
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| value_error("field_ref input is not struct"))?;
            let field = values
                .fields()
                .get(*index)
                .ok_or_else(|| value_error("field_ref index out of range"))?;
            if field.name() != name {
                return Err(value_error(
                    "field_ref name does not match authoritative index",
                ));
            }
            if field.data_type() != data_type {
                return Err(value_error("field_ref type changed in evaluation batch"));
            }
            let child = values.column(*index);
            let nulls = NullBuffer::union(values.nulls(), child.nulls());
            let data = child
                .to_data()
                .into_builder()
                .nulls(nulls)
                .build()
                .map_err(cvt)?;
            Ok(arrow_array::make_array(data))
        }
        Expr::Literal(value) => {
            if value.len() == batch.num_rows() {
                Ok(value.clone())
            } else {
                repeat_scalar(value, batch.num_rows())
            }
        }
        Expr::Comparison { op, left, right } => Ok(Arc::new(compare_arrays(
            &eval(left, batch)?,
            &eval(right, batch)?,
            op,
        )?)),
        Expr::And(children) => Ok(Arc::new(fold_bool(children, batch, true)?)),
        Expr::Or(children) => Ok(Arc::new(fold_bool(children, batch, false)?)),
        Expr::Not(value) => Ok(Arc::new(
            arrow_arith::boolean::not(&eval_bool(value, batch)?).map_err(cvt)?,
        )),
        Expr::IsNull {
            expression,
            negated,
        } => {
            let value = eval(expression, batch)?;
            let result = if *negated {
                arrow_arith::boolean::is_not_null(&value)
            } else {
                arrow_arith::boolean::is_null(&value)
            }
            .map_err(cvt)?;
            Ok(Arc::new(result))
        }
        Expr::In {
            expression,
            values,
            negated,
        } => {
            let mut result = in_list(&eval(expression, batch)?, values)?;
            if *negated {
                result = arrow_arith::boolean::not(&result).map_err(cvt)?;
            }
            Ok(Arc::new(result))
        }
        Expr::Cast { expression, target } => {
            arrow_cast::cast(&eval(expression, batch)?, target).map_err(cvt)
        }
        Expr::Arithmetic { op, left, right } => {
            arithmetic(&eval(left, batch)?, &eval(right, batch)?, op)
        }
        Expr::Negate(expression) => negate(&eval(expression, batch)?),
        Expr::Call {
            function,
            arguments,
        } => Ok(Arc::new(eval_call(function, arguments, batch)?)),
        Expr::RuntimeFilter => Err(value_error("runtime filter has no negotiated evaluator")),
    }
}

fn eval_bool(expression: &Expr, batch: &RecordBatch) -> Result<BooleanArray> {
    eval(expression, batch)?
        .as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| value_error("predicate expression is not BOOLEAN"))
}

fn fold_bool(children: &[Expr], batch: &RecordBatch, and: bool) -> Result<BooleanArray> {
    let mut result = if and {
        all_true(batch.num_rows())
    } else {
        all_false(batch.num_rows())
    };
    for child in children {
        result = if and {
            and_kleene(&result, &eval_bool(child, batch)?)?
        } else {
            or_kleene(&result, &eval_bool(child, batch)?)?
        };
    }
    Ok(result)
}

fn compare_arrays(left: &ArrayRef, right: &ArrayRef, op: &str) -> Result<BooleanArray> {
    let left = dictionary_values(left)?;
    let right = dictionary_values(right)?;
    let (left, right) = broadcast(&left, &right)?;
    let result = match op {
        "eq" => arrow_ord::cmp::eq(&left, &right),
        "ne" => arrow_ord::cmp::neq(&left, &right),
        "lt" => arrow_ord::cmp::lt(&left, &right),
        "le" => arrow_ord::cmp::lt_eq(&left, &right),
        "gt" => arrow_ord::cmp::gt(&left, &right),
        "ge" => arrow_ord::cmp::gt_eq(&left, &right),
        "distinct_from" | "not_distinct_from" => {
            return distinct(&left, &right, op == "distinct_from")
        }
        _ => return Err(value_error("unknown comparison operator")),
    };
    result.map_err(cvt)
}

fn distinct(left: &ArrayRef, right: &ArrayRef, distinct: bool) -> Result<BooleanArray> {
    let equals = arrow_ord::cmp::eq(left, right).map_err(cvt)?;
    Ok(BooleanArray::from(
        (0..left.len())
            .map(|i| {
                let value = if left.is_null(i) || right.is_null(i) {
                    left.is_null(i) != right.is_null(i)
                } else {
                    !equals.value(i)
                };
                if distinct {
                    value
                } else {
                    !value
                }
            })
            .collect::<Vec<_>>(),
    ))
}

fn broadcast(left: &ArrayRef, right: &ArrayRef) -> Result<(ArrayRef, ArrayRef)> {
    if left.len() == right.len() {
        Ok((left.clone(), right.clone()))
    } else if left.len() == 1 {
        Ok((repeat_scalar(left, right.len())?, right.clone()))
    } else if right.len() == 1 {
        Ok((left.clone(), repeat_scalar(right, left.len())?))
    } else {
        Err(value_error("expression lengths are incompatible"))
    }
}

fn repeat_scalar(value: &ArrayRef, count: usize) -> Result<ArrayRef> {
    if value.len() != 1 {
        return Err(value_error("payload literal is not scalar"));
    }
    arrow_select::take::take(value, &arrow_array::UInt32Array::from(vec![0; count]), None)
        .map_err(cvt)
}

fn arithmetic(left: &ArrayRef, right: &ArrayRef, op: &str) -> Result<ArrayRef> {
    let (left, right) = broadcast(left, right)?;
    let left = arrow_cast::cast(&left, &DataType::Float64).map_err(cvt)?;
    let right = arrow_cast::cast(&right, &DataType::Float64).map_err(cvt)?;
    let left = left.as_primitive::<arrow_array::types::Float64Type>();
    let right = right.as_primitive::<arrow_array::types::Float64Type>();
    let result = match op {
        "add" => arrow_arith::numeric::add(left, right),
        "subtract" => arrow_arith::numeric::sub(left, right),
        "multiply" => arrow_arith::numeric::mul(left, right),
        "divide" => arrow_arith::numeric::div(left, right),
        "modulo" => arrow_arith::numeric::rem(left, right),
        _ => return Err(value_error("unknown arithmetic operator")),
    }
    .map_err(cvt)?;
    Ok(Arc::new(result))
}

fn negate(value: &ArrayRef) -> Result<ArrayRef> {
    let value = arrow_cast::cast(value, &DataType::Float64).map_err(cvt)?;
    Ok(Arc::new(
        arrow_arith::numeric::neg(value.as_primitive::<arrow_array::types::Float64Type>())
            .map_err(cvt)?,
    ))
}

fn eval_call(function: &str, arguments: &[Expr], batch: &RecordBatch) -> Result<BooleanArray> {
    if arguments.len() != 2 {
        return Err(value_error(
            "standard filter function requires two arguments",
        ));
    }
    if function == "list_contains" {
        return Err(value_error(
            "list_contains is not implemented for this Arrow type",
        ));
    }
    let (left, right) = broadcast(&eval(&arguments[0], batch)?, &eval(&arguments[1], batch)?)?;
    let left = left
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| value_error("string function input is not utf8"))?;
    let right = right
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| value_error("string function pattern is not utf8"))?;
    Ok(BooleanArray::from(
        (0..left.len())
            .map(|i| {
                if left.is_null(i) || right.is_null(i) {
                    None
                } else {
                    Some(match function {
                        "starts_with" => left.value(i).starts_with(right.value(i)),
                        "ends_with" => left.value(i).ends_with(right.value(i)),
                        "contains" => left.value(i).contains(right.value(i)),
                        _ => false,
                    })
                }
            })
            .collect::<Vec<_>>(),
    ))
}

fn in_list(column: &ArrayRef, values: &ArrayRef) -> Result<BooleanArray> {
    let column = dictionary_values(column)?;
    let values = dictionary_values(values)?;
    let mut result = all_false(column.len());
    for index in 0..values.len() {
        result = or_kleene(
            &result,
            &arrow_ord::cmp::eq(
                &column,
                &repeat_scalar(&values.slice(index, 1), column.len())?,
            )
            .map_err(cvt)?,
        )?;
    }
    Ok(result)
}

fn logical_value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value_type) => logical_value_type(value_type),
        value => value,
    }
}

fn dictionary_values(array: &ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(_, value_type) => arrow_cast::cast(array, value_type).map_err(cvt),
        _ => Ok(array.clone()),
    }
}

fn expr_type(expression: &Expr) -> Option<DataType> {
    match expression {
        Expr::Column { data_type, .. } | Expr::Field { data_type, .. } => Some(data_type.clone()),
        Expr::Literal(value) => Some(value.data_type().clone()),
        Expr::Comparison { .. }
        | Expr::And(_)
        | Expr::Or(_)
        | Expr::Not(_)
        | Expr::IsNull { .. }
        | Expr::In { .. }
        | Expr::Call { .. }
        | Expr::RuntimeFilter => Some(DataType::Boolean),
        Expr::Cast { target, .. } => Some(target.clone()),
        Expr::Arithmetic { left, .. } => expr_type(left),
        Expr::Negate(value) => expr_type(value),
    }
}

fn numeric_type(value: &DataType) -> bool {
    matches!(
        value,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

fn contextual_type(value: &DataType) -> bool {
    matches!(
        value,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
    )
}

fn requires_context(expression: &Expr) -> bool {
    match expression {
        Expr::Arithmetic { op, left, right } => {
            matches!(op.as_str(), "divide" | "modulo")
                || requires_context(left)
                || requires_context(right)
        }
        Expr::Cast { expression, target } => {
            let contextual_cast = expr_type(expression).as_ref().is_some_and(contextual_type)
                && contextual_type(target);
            contextual_cast || requires_context(expression)
        }
        Expr::Field { expression, .. }
        | Expr::Not(expression)
        | Expr::Negate(expression)
        | Expr::IsNull { expression, .. }
        | Expr::In { expression, .. } => requires_context(expression),
        Expr::Comparison { left, right, .. } => requires_context(left) || requires_context(right),
        Expr::And(values)
        | Expr::Or(values)
        | Expr::Call {
            arguments: values, ..
        } => values.iter().any(requires_context),
        _ => false,
    }
}

fn collect_columns(expression: &Expr, output: &mut HashSet<String>) {
    match expression {
        Expr::Column { name, .. } => {
            output.insert(name.clone());
        }
        Expr::Field { expression, .. }
        | Expr::Not(expression)
        | Expr::Negate(expression)
        | Expr::IsNull { expression, .. }
        | Expr::In { expression, .. }
        | Expr::Cast { expression, .. } => collect_columns(expression, output),
        Expr::Comparison { left, right, .. } | Expr::Arithmetic { left, right, .. } => {
            collect_columns(left, output);
            collect_columns(right, output);
        }
        Expr::And(values)
        | Expr::Or(values)
        | Expr::Call {
            arguments: values, ..
        } => {
            for value in values {
                collect_columns(value, output);
            }
        }
        _ => {}
    }
}

fn root_column(expression: &Expr) -> Option<&str> {
    match expression {
        Expr::Column { name, .. } => Some(name),
        Expr::Field { expression, .. } => root_column(expression),
        _ => None,
    }
}
fn mentions(expression: &Expr, column: &str) -> bool {
    let mut names = HashSet::new();
    collect_columns(expression, &mut names);
    names.contains(column)
}

fn filter_view(expression: &Expr) -> Filter {
    match expression {
        Expr::Comparison { op, left, .. } => Filter::Constant {
            column_name: root_column(left).unwrap_or("").into(),
            op: op.clone(),
        },
        Expr::In { expression, .. } => Filter::In {
            column_name: root_column(expression).unwrap_or("").into(),
        },
        Expr::IsNull {
            expression,
            negated: true,
        } => Filter::IsNotNull {
            column_name: root_column(expression).unwrap_or("").into(),
        },
        Expr::IsNull { expression, .. } => Filter::IsNull {
            column_name: root_column(expression).unwrap_or("").into(),
        },
        Expr::And(values) => Filter::And(values.iter().map(filter_view).collect()),
        Expr::Or(values) => Filter::Or(values.iter().map(filter_view).collect()),
        Expr::Field {
            expression, name, ..
        } => Filter::Struct {
            column_name: root_column(expression).unwrap_or("").into(),
            child_name: name.clone(),
            child: Box::new(Filter::Other {
                kind: "field_ref".into(),
                column_name: root_column(expression).unwrap_or("").into(),
            }),
        },
        _ => Filter::Other {
            kind: "expression".into(),
            column_name: root_column(expression).unwrap_or("").into(),
        },
    }
}

fn constant_for(expression: &Expr, column: &str) -> Option<ArrayRef> {
    match expression {
        Expr::Comparison { op, left, right } if op == "eq" && root_column(left) == Some(column) => {
            match right.as_ref() {
                Expr::Literal(value) => Some(value.clone()),
                _ => None,
            }
        }
        _ => None,
    }
}
fn in_values_for(expression: &Expr, column: &str) -> Option<ArrayRef> {
    match expression {
        Expr::In {
            expression,
            values,
            negated: false,
        } if root_column(expression) == Some(column) => Some(values.clone()),
        _ => None,
    }
}
fn values_for(expression: &Expr, column: &str) -> Option<ArrayRef> {
    match expression {
        Expr::And(children) => children.iter().find_map(|child| values_for(child, column)),
        Expr::Or(children) => {
            let arrays = children
                .iter()
                .map(|child| values_for(child, column))
                .collect::<Option<Vec<_>>>()?;
            let refs = arrays
                .iter()
                .map(|array| array.as_ref())
                .collect::<Vec<_>>();
            arrow_select::concat::concat(&refs).ok()
        }
        _ => constant_for(expression, column).or_else(|| in_values_for(expression, column)),
    }
}

fn bounds_for(expression: &Expr, column: &str) -> Option<ColumnBounds> {
    match expression {
        Expr::Comparison { op, left, right } => {
            if root_column(left) == Some(column) {
                return literal_bound(op, right, false);
            }
            if root_column(right) == Some(column) {
                return literal_bound(op, left, true);
            }
            None
        }
        Expr::In {
            expression,
            values,
            negated: false,
        } if root_column(expression) == Some(column) => array_bounds_i64(values),
        Expr::And(children) => children
            .iter()
            .filter_map(|child| bounds_for(child, column))
            .reduce(intersect_bounds),
        Expr::Or(children) => children
            .iter()
            .map(|child| bounds_for(child, column))
            .collect::<Option<Vec<_>>>()?
            .into_iter()
            .reduce(union_bounds),
        _ => None,
    }
}

fn literal_bound(op: &str, expression: &Expr, reversed: bool) -> Option<ColumnBounds> {
    let Expr::Literal(value) = expression else {
        return None;
    };
    let value = array_values_i64(value)?.into_iter().next()?;
    let op = if reversed {
        match op {
            "gt" => "lt",
            "ge" => "le",
            "lt" => "gt",
            "le" => "ge",
            value => value,
        }
    } else {
        op
    };
    Some(match op {
        "eq" => ColumnBounds {
            min: Some(value),
            max: Some(value),
        },
        "gt" => ColumnBounds {
            min: Some(value.saturating_add(1)),
            max: None,
        },
        "ge" => ColumnBounds {
            min: Some(value),
            max: None,
        },
        "lt" => ColumnBounds {
            min: None,
            max: Some(value.saturating_sub(1)),
        },
        "le" => ColumnBounds {
            min: None,
            max: Some(value),
        },
        _ => return None,
    })
}

fn array_values_i64(values: &ArrayRef) -> Option<Vec<i64>> {
    let casted = arrow_cast::cast(values, &DataType::Int64).ok()?;
    let values = casted.as_primitive::<arrow_array::types::Int64Type>();
    Some(
        (0..values.len())
            .filter(|index| values.is_valid(*index))
            .map(|index| values.value(index))
            .collect(),
    )
}

fn array_bounds_i64(values: &ArrayRef) -> Option<ColumnBounds> {
    let values = array_values_i64(values)?;
    (!values.is_empty()).then(|| ColumnBounds {
        min: values.iter().copied().min(),
        max: values.iter().copied().max(),
    })
}

fn intersect_bounds(left: ColumnBounds, right: ColumnBounds) -> ColumnBounds {
    ColumnBounds {
        min: match (left.min, right.min) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        },
        max: match (left.max, right.max) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        },
    }
}

fn union_bounds(left: ColumnBounds, right: ColumnBounds) -> ColumnBounds {
    ColumnBounds {
        min: match (left.min, right.min) {
            (Some(left), Some(right)) => Some(left.min(right)),
            _ => None,
        },
        max: match (left.max, right.max) {
            (Some(left), Some(right)) => Some(left.max(right)),
            _ => None,
        },
    }
}

fn format_expr(expression: &Expr) -> String {
    match expression {
        Expr::Column { name, .. } => name.clone(),
        Expr::Field {
            expression, name, ..
        } => format!("{}.{}", format_expr(expression), name),
        Expr::Literal(value) => {
            if value.is_empty() {
                "NULL".into()
            } else {
                fmt_scalar(value, 0)
            }
        }
        Expr::Comparison { op, left, right } => format!(
            "{} {} {}",
            format_expr(left),
            op_symbol(op),
            format_expr(right)
        ),
        Expr::And(values) => format!(
            "({})",
            values
                .iter()
                .map(format_expr)
                .collect::<Vec<_>>()
                .join(" AND ")
        ),
        Expr::Or(values) => format!(
            "({})",
            values
                .iter()
                .map(format_expr)
                .collect::<Vec<_>>()
                .join(" OR ")
        ),
        Expr::Not(value) => format!("NOT ({})", format_expr(value)),
        Expr::IsNull {
            expression,
            negated,
        } => format!(
            "{} IS {}NULL",
            format_expr(expression),
            if *negated { "NOT " } else { "" }
        ),
        Expr::In {
            expression,
            values,
            negated,
        } => {
            let values = (0..values.len())
                .map(|index| fmt_scalar(values, index))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "{} {}IN ({})",
                format_expr(expression),
                if *negated { "NOT " } else { "" },
                values
            )
        }
        _ => "(expression)".into(),
    }
}

fn format_filter_repr(expression: &Expr) -> String {
    match expression {
        Expr::Comparison { op, left, right } => format!(
            "ConstantFilter({} {} {})",
            format_expr(left),
            op_symbol(op),
            format_expr(right)
        ),
        Expr::And(children) => format!(
            "AndFilter([{}])",
            children
                .iter()
                .map(format_filter_repr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Or(children) => format!(
            "OrFilter([{}])",
            children
                .iter()
                .map(format_filter_repr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => format_expr(expression),
    }
}

fn op_symbol(op: &str) -> &'static str {
    match op {
        "eq" => "=",
        "ne" => "!=",
        "lt" => "<",
        "le" => "<=",
        "gt" => ">",
        "ge" => ">=",
        "distinct_from" => "IS DISTINCT FROM",
        "not_distinct_from" => "IS NOT DISTINCT FROM",
        _ => "?",
    }
}
fn fmt_scalar(array: &ArrayRef, index: usize) -> String {
    if array.is_null(index) {
        return "NULL".into();
    }
    match array.data_type() {
        DataType::Utf8 => format!("'{}'", array.as_string::<i32>().value(index)),
        DataType::Boolean => {
            if array.as_boolean().value(index) {
                "True".into()
            } else {
                "False".into()
            }
        }
        _ => arrow_cast::cast(&array.slice(index, 1), &DataType::Utf8)
            .ok()
            .map(|a| a.as_string::<i32>().value(0).to_string())
            .unwrap_or_default(),
    }
}
fn and_kleene(a: &BooleanArray, b: &BooleanArray) -> Result<BooleanArray> {
    arrow_arith::boolean::and_kleene(a, b).map_err(cvt)
}
fn or_kleene(a: &BooleanArray, b: &BooleanArray) -> Result<BooleanArray> {
    arrow_arith::boolean::or_kleene(a, b).map_err(cvt)
}
fn all_true(n: usize) -> BooleanArray {
    BooleanArray::new(BooleanBuffer::new_set(n), None)
}
fn all_false(n: usize) -> BooleanArray {
    BooleanArray::new(BooleanBuffer::new_unset(n), None)
}
fn to_usize(value: u64) -> Result<usize> {
    usize::try_from(value).map_err(|_| value_error("index out of range"))
}
fn nonempty(value: String, name: &str) -> Result<String> {
    if value.is_empty() {
        Err(value_error(format!("{name} must be nonempty")))
    } else {
        Ok(value)
    }
}
fn json_error(error: serde_json::Error) -> RpcError {
    value_error(format!("invalid filter JSON: {error}"))
}
fn value_error(message: impl Into<String>) -> RpcError {
    RpcError::value_error(message.into())
}
fn cvt(error: arrow_schema::ArrowError) -> RpcError {
    RpcError::runtime_error(format!("filter: {error}"))
}

fn b64_decode(value: &str) -> Option<Vec<u8>> {
    let code = |byte: u8| {
        Some(match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        })
    };
    let clean: Vec<_> = value
        .bytes()
        .filter(|c| !c.is_ascii_whitespace() && *c != b'=')
        .collect();
    if clean.len() % 4 == 1 {
        return None;
    }
    let mut output = Vec::with_capacity(clean.len() * 3 / 4);
    for chunk in clean.chunks(4) {
        let mut bits = 0u32;
        for byte in chunk {
            bits = (bits << 6) | u32::from(code(*byte)?);
        }
        bits <<= 6 * (4 - chunk.len());
        for index in 0..chunk.len().saturating_sub(1) {
            output.push((bits >> (16 - index * 8)) as u8);
        }
    }
    Some(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Int8Type;
    use arrow_array::{DictionaryArray, Int64Array, Int8Array, ListArray};
    use arrow_buffer::OffsetBuffer;

    fn metadata() -> HashMap<String, String> {
        [
            ("vgi_filter_encoding".into(), ENCODING.into()),
            ("vgi_filter_version".into(), VERSION.into()),
            ("vgi_evaluation_context".into(), NO_CONTEXT.into()),
        ]
        .into_iter()
        .collect()
    }

    fn encode(json: &str, fields: Vec<Arc<Field>>, arrays: Vec<ArrayRef>) -> Vec<u8> {
        let mut all_fields = vec![Arc::new(Field::new("filter_spec", DataType::Utf8, false))];
        all_fields.extend(fields);
        let mut all_arrays = vec![Arc::new(StringArray::from(vec![json])) as ArrayRef];
        all_arrays.extend(arrays);
        let schema = Arc::new(Schema::new_with_metadata(all_fields, metadata()));
        ipc::write_batch(&RecordBatch::try_new(schema, all_arrays).unwrap()).unwrap()
    }

    fn int64_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]))
    }

    #[test]
    fn snapshot_comparison_and_required_semantics() {
        let json = r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"comparison","op":"gt","left":{"node":"column_ref","column_index":0,"column_name":"n"},"right":{"node":"literal","value_ref":0}}}]}"#;
        let bytes = encode(
            json,
            vec![Arc::new(Field::new("value_0", DataType::Int64, true))],
            vec![Arc::new(Int64Array::from(vec![2]))],
        );
        let state = PushdownFilters::parse_with_schema(&bytes, int64_schema()).unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "n",
            Arc::new(Int64Array::from(vec![1, 3, 2])) as ArrayRef,
        )])
        .unwrap();
        assert_eq!(state.apply(&batch).unwrap().num_rows(), 1);
        assert_eq!(state.filtered_columns(), HashSet::from(["n".to_string()]));
        assert_eq!(
            state.get_column_bounds("n"),
            Some(ColumnBounds {
                min: Some(3),
                max: None,
            })
        );
        assert_eq!(
            state.format_repr(),
            "PushdownFilters([ConstantFilter(n > 2)])"
        );
    }

    #[test]
    fn inline_in_and_atomic_delta_tombstone() {
        let values = Arc::new(Int64Array::from(vec![2, 4])) as ArrayRef;
        let list = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int64, true)),
            OffsetBuffer::new(vec![0_i32, 2].into()),
            values,
            None,
        )) as ArrayRef;
        let snapshot = r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"advisory","source":"query","expression":{"node":"in","expression":{"node":"column_ref","column_index":0,"column_name":"n"},"set":{"kind":"literal","value_ref":0},"negated":false}}]}"#;
        let mut state = PushdownFilters::parse_with_schema(
            &encode(
                snapshot,
                vec![Arc::new(Field::new(
                    "value_0",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    true,
                ))],
                vec![list],
            ),
            int64_schema(),
        )
        .unwrap();
        let remove = r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"delta","updates":[{"operation":"remove","id":"p","revision":1}]}"#;
        state.apply_delta(&encode(remove, vec![], vec![])).unwrap();
        let stale = r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"delta","updates":[{"operation":"upsert","id":"p","revision":1,"mode":"advisory","source":"join","expression":{"node":"literal","value_ref":999}}]}"#;
        state.apply_delta(&encode(stale, vec![], vec![])).unwrap();
        assert_eq!(state.format_pushed(), "(none)");
        let malformed = r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"delta","updates":[{"operation":"upsert","id":"p","revision":1,"mode":"required","source":"join","expression":{"node":"literal","value_ref":999}}]}"#;
        assert!(state
            .apply_delta(&encode(malformed, vec![], vec![]))
            .is_err());
    }

    #[test]
    fn rejects_v1_and_nested_runtime_filter() {
        let v1 = encode(r#"[]"#, vec![], vec![]);
        assert!(PushdownFilters::parse(&v1).is_err());
        let nested = r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"advisory","source":"join","expression":{"node":"not","expression":{"node":"runtime_filter","algorithm":{"namespace":"duckdb.runtime_filter","name":"bloom","version":1},"input":{"node":"column_ref","column_index":0,"column_name":"n"},"artifact_ref":0,"null_handling":"pass"}}}]}"#;
        let bytes = encode(
            nested,
            vec![Arc::new(Field::new("artifact_0", DataType::Binary, true))],
            vec![Arc::new(arrow_array::BinaryArray::from(vec![Some(
                &b"x"[..],
            )]))],
        );
        assert!(PushdownFilters::parse(&bytes).is_err());
    }

    #[test]
    fn binds_authoritative_nested_schema_and_rejects_no_schema() {
        let nested = DataType::Struct(
            vec![Arc::new(Field::new(
                "inner",
                DataType::Struct(vec![Arc::new(Field::new("leaf", DataType::Int64, true))].into()),
                true,
            ))]
            .into(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("root", nested, true)]));
        let json = r#"{"encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[{"id":"p","revision":0,"mode":"required","source":"query","expression":{"node":"is_null","expression":{"node":"field_ref","expression":{"node":"field_ref","expression":{"node":"column_ref","column_index":0,"column_name":"root"},"field_index":0,"field_name":"inner"},"field_index":0,"field_name":"leaf"},"negated":false}}]}"#;
        let bytes = encode(json, vec![], vec![]);
        assert!(PushdownFilters::parse_with_schema(&bytes, schema.clone()).is_ok());
        assert!(PushdownFilters::parse(&bytes).is_err());
        let bad = json.replacen(r#""field_name":"leaf""#, r#""field_name":"wrong""#, 1);
        assert!(PushdownFilters::parse_with_schema(&encode(&bad, vec![], vec![]), schema).is_err());
    }

    #[test]
    fn dictionary_values_compare_with_their_logical_type() {
        let dictionary = Arc::new(
            DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(vec![0, 1, 0]),
                Arc::new(StringArray::from(vec!["red", "green"])),
            )
            .unwrap(),
        ) as ArrayRef;
        let literal = Arc::new(StringArray::from(vec!["green"])) as ArrayRef;
        let result = compare_arrays(&dictionary, &literal, "eq").unwrap();
        assert_eq!(result, BooleanArray::from(vec![false, true, false]));
    }

    #[test]
    fn discrete_values_descend_through_and_union_complete_or() {
        let column = || Expr::Column {
            index: 0,
            name: "n".into(),
            data_type: DataType::Int64,
        };
        let equality = |value| Expr::Comparison {
            op: "eq".into(),
            left: Box::new(column()),
            right: Box::new(Expr::Literal(Arc::new(Int64Array::from(vec![value])))),
        };
        let expression = Expr::And(vec![
            Expr::Or(vec![equality(2), equality(7)]),
            Expr::Comparison {
                op: "ge".into(),
                left: Box::new(column()),
                right: Box::new(Expr::Literal(Arc::new(Int64Array::from(vec![0])))),
            },
        ]);
        let values = values_for(&expression, "n").unwrap();
        assert_eq!(
            values
                .as_primitive::<arrow_array::types::Int64Type>()
                .values(),
            &[2, 7]
        );
        assert_eq!(
            bounds_for(&expression, "n"),
            Some(ColumnBounds {
                min: Some(2),
                max: Some(7),
            })
        );

        let incomplete = Expr::Or(vec![
            equality(2),
            Expr::Comparison {
                op: "gt".into(),
                left: Box::new(column()),
                right: Box::new(Expr::Literal(Arc::new(Int64Array::from(vec![7])))),
            },
        ]);
        assert!(values_for(&incomplete, "n").is_none());
        assert_eq!(
            bounds_for(&incomplete, "n"),
            Some(ColumnBounds {
                min: Some(2),
                max: None,
            })
        );
    }
}

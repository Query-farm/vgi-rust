// Copyright 2025, 2026 Query Farm LLC - https://query.farm

use std::cmp::Ordering;
use std::ops::Bound;

use arrow_array::Scalar;

use super::*;

/// A conservative set of non-NULL values in a [`ColumnDomain`].
///
/// Endpoints are one-element, non-NULL Arrow arrays with the domain's logical
/// type. Exclusive endpoints stay exclusive: integers and timestamps are never
/// converted to floating point or adjusted by an assumed step size.
#[derive(Debug, Clone)]
pub enum NonNullDomain {
    Empty,
    All,
    Range {
        lower: Bound<Scalar<ArrayRef>>,
        upper: Bound<Scalar<ArrayRef>>,
    },
}

/// Values a column could have in a row satisfying the pushed predicates.
///
/// This is a superset, not an exact filter. Disjoint sets become an interval
/// hull, and unsupported expressions leave the domain unrestricted. Dictionary
/// columns use their decoded logical type, never dictionary keys. SQL NULL is
/// represented separately from the non-NULL interval.
#[derive(Debug, Clone)]
pub struct ColumnDomain {
    data_type: DataType,
    non_null: NonNullDomain,
    may_be_null: bool,
}

impl ColumnDomain {
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn non_null(&self) -> &NonNullDomain {
        &self.non_null
    }

    pub fn may_be_null(&self) -> bool {
        self.may_be_null
    }

    /// No row can match if any column has an empty domain.
    pub fn is_empty(&self) -> bool {
        matches!(self.non_null, NonNullDomain::Empty) && !self.may_be_null
    }

    /// Test candidate values against this conservative domain using SQL order.
    ///
    /// Returns a non-null mask. `true` still requires exact filtering. Values
    /// must have the same logical Arrow type; dictionaries are decoded first.
    /// Prefer this to raw Arrow float comparison kernels, which distinguish
    /// signed zeros and NaN payloads that DuckDB considers equal.
    pub fn may_contain(&self, values: &ArrayRef) -> Result<BooleanArray> {
        let values = dictionary_values(values)?;
        if values.data_type() != &self.data_type {
            return Err(value_error("candidate type differs from column domain"));
        }
        let non_null = match &self.non_null {
            NonNullDomain::Empty => all_false(values.len()),
            NonNullDomain::All => all_true(values.len()),
            NonNullDomain::Range { lower, upper } => {
                let bound_mask = |bound: &Bound<Scalar<ArrayRef>>, inclusive, exclusive| match bound
                {
                    Bound::Unbounded => Ok(all_true(values.len())),
                    Bound::Included(value) => {
                        compare_arrays(&values, &value.clone().into_inner(), inclusive)
                    }
                    Bound::Excluded(value) => {
                        compare_arrays(&values, &value.clone().into_inner(), exclusive)
                    }
                };
                and_kleene(
                    &bound_mask(lower, "ge", "gt")?,
                    &bound_mask(upper, "le", "lt")?,
                )?
            }
        };
        let nulls = values.logical_nulls();
        Ok(BooleanArray::from(
            (0..values.len())
                .map(|index| {
                    if nulls.as_ref().is_some_and(|nulls| nulls.is_null(index)) {
                        self.may_be_null
                    } else {
                        non_null.value(index)
                    }
                })
                .collect::<Vec<_>>(),
        ))
    }

    fn empty(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            non_null: NonNullDomain::Empty,
            may_be_null: false,
        }
    }

    fn only_null(&self) -> Self {
        Self {
            non_null: NonNullDomain::Empty,
            ..self.clone()
        }
    }

    fn without_null(&self) -> Self {
        Self {
            may_be_null: false,
            ..self.clone()
        }
    }

    fn range(&self, lower: Bound<Scalar<ArrayRef>>, upper: Bound<Scalar<ArrayRef>>) -> Self {
        Self {
            data_type: self.data_type.clone(),
            non_null: NonNullDomain::Range { lower, upper },
            may_be_null: false,
        }
    }

    fn combine(&self, other: &Self, intersection: bool) -> Result<Self> {
        use NonNullDomain::*;
        let non_null = match (&self.non_null, &other.non_null) {
            (Empty, _) | (_, Empty) if intersection => Empty,
            (All, _) | (_, All) if !intersection => All,
            (All, value) | (value, All) if intersection => value.clone(),
            (Empty, value) | (value, Empty) => value.clone(),
            (
                Range {
                    lower: l1,
                    upper: u1,
                },
                Range {
                    lower: l2,
                    upper: u2,
                },
            ) => {
                let lower = choose_bound(l1, l2, true, intersection)?;
                let upper = choose_bound(u1, u2, false, !intersection)?;
                match (&lower, &upper) {
                    (Bound::Unbounded, Bound::Unbounded) => All,
                    (Bound::Unbounded, _) | (_, Bound::Unbounded) => Range { lower, upper },
                    (
                        Bound::Included(l) | Bound::Excluded(l),
                        Bound::Included(u) | Bound::Excluded(u),
                    ) => {
                        let order = scalar_order(l, u)?;
                        if order == Ordering::Greater
                            || (order == Ordering::Equal
                                && (matches!(lower, Bound::Excluded(_))
                                    || matches!(upper, Bound::Excluded(_))))
                        {
                            Empty
                        } else {
                            Range { lower, upper }
                        }
                    }
                }
            }
            _ => unreachable!("All/Empty cases handled above"),
        };
        Ok(Self {
            data_type: self.data_type.clone(),
            non_null,
            may_be_null: if intersection {
                self.may_be_null && other.may_be_null
            } else {
                self.may_be_null || other.may_be_null
            },
        })
    }
}

fn scalar_order(left: &Scalar<ArrayRef>, right: &Scalar<ArrayRef>) -> Result<Ordering> {
    let left = left.clone().into_inner();
    let right = right.clone().into_inner();
    if compare_arrays(&left, &right, "eq")?.value(0) {
        Ok(Ordering::Equal)
    } else if compare_arrays(&left, &right, "lt")?.value(0) {
        Ok(Ordering::Less)
    } else {
        Ok(Ordering::Greater)
    }
}

fn choose_bound(
    left: &Bound<Scalar<ArrayRef>>,
    right: &Bound<Scalar<ArrayRef>>,
    lower: bool,
    maximum: bool,
) -> Result<Bound<Scalar<ArrayRef>>> {
    use Bound::*;
    let order = match (left, right) {
        (Unbounded, Unbounded) => Ordering::Equal,
        (Unbounded, _) => {
            if lower {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Unbounded) => {
            if lower {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Included(l) | Excluded(l), Included(r) | Excluded(r)) => {
            scalar_order(l, r)?.then_with(|| {
                let excluded = matches!(left, Excluded(_)).cmp(&matches!(right, Excluded(_)));
                if lower {
                    excluded
                } else {
                    excluded.reverse()
                }
            })
        }
    };
    Ok(if (order == Ordering::Greater) == maximum {
        left
    } else {
        right
    }
    .clone())
}

// Indices are SQL TRUE, FALSE, NULL. Domains and partial-row truth sets use
// exactly the same truth tables; unknown is a set of possible outcomes.
const TRUE: usize = 0;
const FALSE: usize = 1;
const NULL: usize = 2;
const UNKNOWN: u8 = 0b111;

fn truth(left: usize, right: usize, and: bool) -> usize {
    if and {
        if left == FALSE || right == FALSE {
            FALSE
        } else if left == NULL || right == NULL {
            NULL
        } else {
            TRUE
        }
    } else if left == TRUE || right == TRUE {
        TRUE
    } else if left == NULL || right == NULL {
        NULL
    } else {
        FALSE
    }
}

fn truth_sets(left: u8, right: u8, and: bool) -> u8 {
    let mut result = 0;
    for l in 0..3 {
        for r in 0..3 {
            if left & (1 << l) != 0 && right & (1 << r) != 0 {
                result |= 1 << truth(l, r, and);
            }
        }
    }
    result
}

fn negate_set(value: u8) -> u8 {
    ((value & 1) << 1) | ((value & 2) >> 1) | (value & 4)
}

impl PushdownFilters {
    /// Infer a conservative typed domain for one top-level bind-schema column.
    ///
    /// Supports direct comparisons, Boolean composition, NULL tests, and IN /
    /// NOT IN, including reversed comparisons and NULL-safe equality. Other
    /// expressions widen the result. Arithmetic/cast inversion, nested-field
    /// domains, collations, and chunk-statistics evaluation are not inferred.
    /// Unknown or ambiguous names are errors. Results reflect current filters,
    /// including applied deltas; this method does not cache a stale domain.
    pub fn column_domain(&self, column: &str) -> Result<ColumnDomain> {
        let field = self.pruning_field(column)?;
        let universe = ColumnDomain {
            data_type: logical_value_type(field.data_type()).clone(),
            non_null: if field.data_type() == &DataType::Null {
                NonNullDomain::Empty
            } else {
                NonNullDomain::All
            },
            // Encoded arrays can have NULL values without a physical top-level
            // null bit, even when Arrow accepts them under a nonnullable field.
            may_be_null: field.is_nullable()
                || matches!(
                    field.data_type(),
                    DataType::Null
                        | DataType::Dictionary(..)
                        | DataType::Union(..)
                        | DataType::RunEndEncoded(..)
                ),
        };
        let mut result = universe.clone();
        for predicate in &self.predicates {
            // Exact evaluation ignores an advisory predicate on error. Unless
            // its entire expression is supported, no part may restrict pruning.
            if predicate.mode == PredicateMode::Advisory && !supported(&predicate.expression) {
                continue;
            }
            if let Ok(domains) = domains(&predicate.expression, column, &universe) {
                result = result.combine(&domains[TRUE], true)?;
            }
        }
        Ok(result)
    }

    /// Could each candidate row satisfy the filters, given only these columns?
    ///
    /// `known` can contain any subset of the authoritative bind schema, in any
    /// order. Its rows must describe aligned candidate rows (for example grid
    /// coordinates). Missing columns are unknown; a supplied NULL is SQL NULL.
    /// Unknown, duplicate, or mistyped fields are errors. Whole structs may be
    /// supplied; their parent validity is respected when accessing fields.
    ///
    /// Returns a non-null Boolean array. `false` proves the candidate cannot
    /// match; `true` means it still needs exact filtering. Unsupported expressions
    /// and evaluation failures conservatively retain candidates. This does not
    /// replace [`Self::apply`], and does not interpret rows as chunk min/max stats.
    pub fn may_match(&self, known: &RecordBatch) -> Result<BooleanArray> {
        let mut names = HashSet::new();
        for (field, values) in known.schema().fields().iter().zip(known.columns()) {
            if !names.insert(field.name().clone()) {
                return Err(value_error("duplicate column in pruning input"));
            }
            let authoritative = self.pruning_field(field.name())?;
            if authoritative.data_type() != field.data_type() {
                return Err(value_error("pruning input type differs from bind schema"));
            }
            if !authoritative.is_nullable() && values.null_count() > 0 {
                return Err(value_error(
                    "NULL pruning input violates nonnullable bind field",
                ));
            }
        }
        let mut possible = vec![1 << TRUE; known.num_rows()];
        for predicate in &self.predicates {
            if predicate.mode == PredicateMode::Advisory && !supported(&predicate.expression) {
                continue;
            }
            if let Ok(current) = partial(&predicate.expression, known) {
                for (row, value) in possible.iter_mut().zip(current) {
                    *row = truth_sets(*row, value, true);
                }
            }
        }
        Ok(BooleanArray::from(
            possible.into_iter().map(|p| p & 1 != 0).collect::<Vec<_>>(),
        ))
    }

    fn pruning_field(&self, name: &str) -> Result<&Field> {
        let mut fields = self
            .output_schema
            .fields()
            .iter()
            .filter(|f| f.name() == name);
        let field = fields
            .next()
            .ok_or_else(|| value_error(format!("unknown pruning column: {name}")))?;
        if fields.next().is_some() {
            return Err(value_error(format!("ambiguous pruning column: {name}")));
        }
        Ok(field)
    }
}

// Keep the first version deliberately within the scalar orderings implemented
// by the standard profile. Not all Arrow types have a SQL scalar ordering.
fn ordered(data_type: &DataType) -> bool {
    matches!(
        logical_value_type(data_type),
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(..)
            | DataType::Duration(_)
    )
}

fn simple_value(expr: &Expr) -> bool {
    match expr {
        Expr::Column { .. } | Expr::Literal(_) => true,
        Expr::Field { expression, .. } => simple_value(expression),
        _ => false,
    }
}

fn supported(expr: &Expr) -> bool {
    match expr {
        Expr::And(children) | Expr::Or(children) => children.iter().all(supported),
        Expr::Not(child) => supported(child),
        Expr::Comparison { left, right, .. } => {
            simple_value(left)
                && simple_value(right)
                && expr_type(left).is_some_and(|t| ordered(&t))
        }
        Expr::In { expression, .. } => {
            simple_value(expression) && expr_type(expression).is_some_and(|t| ordered(&t))
        }
        Expr::IsNull { expression, .. } => simple_value(expression),
        Expr::Column { .. } | Expr::Field { .. } | Expr::Literal(_) => {
            simple_value(expr)
                && expr_type(expr).is_some_and(|t| logical_value_type(&t) == &DataType::Boolean)
        }
        _ => false,
    }
}

fn available(expr: &Expr, known: &RecordBatch) -> bool {
    match expr {
        Expr::Column { name, .. } => known.column_by_name(name).is_some(),
        Expr::Literal(_) => true,
        Expr::Field { expression, .. }
        | Expr::IsNull { expression, .. }
        | Expr::In { expression, .. } => available(expression, known),
        Expr::Comparison { left, right, .. } => available(left, known) && available(right, known),
        _ => false,
    }
}

fn partial(expr: &Expr, known: &RecordBatch) -> Result<Vec<u8>> {
    match expr {
        Expr::And(children) | Expr::Or(children) => {
            let and = matches!(expr, Expr::And(_));
            let mut result = vec![1 << if and { TRUE } else { FALSE }; known.num_rows()];
            for child in children {
                for (row, value) in result.iter_mut().zip(partial(child, known)?) {
                    *row = truth_sets(*row, value, and);
                }
            }
            Ok(result)
        }
        Expr::Not(child) => Ok(partial(child, known)?.into_iter().map(negate_set).collect()),
        _ if supported(expr) && available(expr, known) => Ok(eval_bool(expr, known)?
            .iter()
            .map(|v| {
                1 << match v {
                    Some(true) => TRUE,
                    Some(false) => FALSE,
                    None => NULL,
                }
            })
            .collect()),
        _ => Ok(vec![UNKNOWN; known.num_rows()]),
    }
}

type Domains = [ColumnDomain; 3];

fn constant_domains(outcome: usize, universe: &ColumnDomain) -> Domains {
    std::array::from_fn(|index| {
        if index == outcome {
            universe.clone()
        } else {
            universe.empty()
        }
    })
}

fn combine_domains(
    left: &Domains,
    right: &Domains,
    and: bool,
    universe: &ColumnDomain,
) -> Result<Domains> {
    let mut result = constant_domains(usize::MAX, universe);
    for (l, ld) in left.iter().enumerate() {
        for (r, rd) in right.iter().enumerate() {
            let outcome = truth(l, r, and);
            result[outcome] = result[outcome].combine(&ld.combine(rd, true)?, false)?;
        }
    }
    Ok(result)
}

fn target(expr: &Expr, column: &str) -> bool {
    matches!(expr, Expr::Column { name, .. } if name == column)
}

fn domains(expr: &Expr, column: &str, universe: &ColumnDomain) -> Result<Domains> {
    match expr {
        Expr::And(children) | Expr::Or(children) => {
            let and = matches!(expr, Expr::And(_));
            let mut result = constant_domains(if and { TRUE } else { FALSE }, universe);
            for child in children {
                result =
                    combine_domains(&result, &domains(child, column, universe)?, and, universe)?;
            }
            Ok(result)
        }
        Expr::Not(child) => {
            let mut result = domains(child, column, universe)?;
            result.swap(TRUE, FALSE);
            Ok(result)
        }
        Expr::Comparison { op, left, right } if target(left, column) => {
            if let Expr::Literal(value) = right.as_ref() {
                return comparison_domains(op, value, universe);
            }
            Ok([universe.clone(), universe.clone(), universe.clone()])
        }
        Expr::Comparison { op, left, right } if target(right, column) => {
            if let Expr::Literal(value) = left.as_ref() {
                let reversed = match op.as_str() {
                    "lt" => "gt",
                    "le" => "ge",
                    "gt" => "lt",
                    "ge" => "le",
                    other => other,
                };
                return comparison_domains(reversed, value, universe);
            }
            Ok([universe.clone(), universe.clone(), universe.clone()])
        }
        Expr::IsNull {
            expression,
            negated,
        } if target(expression, column) => {
            let mut result = [
                universe.only_null(),
                universe.without_null(),
                universe.empty(),
            ];
            if *negated {
                result.swap(TRUE, FALSE);
            }
            Ok(result)
        }
        Expr::In {
            expression,
            values,
            negated,
        } if target(expression, column) => {
            let mut result = constant_domains(FALSE, universe);
            for index in 0..values.len() {
                result = combine_domains(
                    &result,
                    &comparison_domains("eq", &values.slice(index, 1), universe)?,
                    false,
                    universe,
                )?;
            }
            if *negated {
                result.swap(TRUE, FALSE);
            }
            Ok(result)
        }
        Expr::Column { .. } if target(expr, column) && universe.data_type == DataType::Boolean => {
            comparison_domains(
                "eq",
                &(Arc::new(BooleanArray::from(vec![true])) as ArrayRef),
                universe,
            )
        }
        Expr::Literal(value) if value.data_type() == &DataType::Boolean && value.len() == 1 => {
            let values = value.as_boolean();
            Ok(constant_domains(
                if values.is_null(0) {
                    NULL
                } else if values.value(0) {
                    TRUE
                } else {
                    FALSE
                },
                universe,
            ))
        }
        _ => Ok([universe.clone(), universe.clone(), universe.clone()]),
    }
}

fn comparison_domains(op: &str, value: &ArrayRef, universe: &ColumnDomain) -> Result<Domains> {
    let value = dictionary_values(value)?;
    if value.len() != 1 || value.data_type() != &universe.data_type || !ordered(value.data_type()) {
        return Ok([universe.clone(), universe.clone(), universe.clone()]);
    }
    let null_safe = matches!(op, "distinct_from" | "not_distinct_from");
    if value.is_null(0) {
        return Ok(match op {
            "distinct_from" => [
                universe.without_null(),
                universe.only_null(),
                universe.empty(),
            ],
            "not_distinct_from" => [
                universe.only_null(),
                universe.without_null(),
                universe.empty(),
            ],
            _ => constant_domains(NULL, universe),
        });
    }
    let scalar = Scalar::new(value);
    let point = universe.range(
        Bound::Included(scalar.clone()),
        Bound::Included(scalar.clone()),
    );
    let lower = |included| {
        universe.range(
            if included {
                Bound::Included(scalar.clone())
            } else {
                Bound::Excluded(scalar.clone())
            },
            Bound::Unbounded,
        )
    };
    let upper = |included| {
        universe.range(
            Bound::Unbounded,
            if included {
                Bound::Included(scalar.clone())
            } else {
                Bound::Excluded(scalar.clone())
            },
        )
    };
    let (yes, no) = match op {
        "eq" | "not_distinct_from" => (point, universe.without_null()),
        "ne" | "distinct_from" => (universe.without_null(), point),
        "lt" => (upper(false), lower(true)),
        "le" => (upper(true), lower(false)),
        "gt" => (lower(false), upper(true)),
        "ge" => (lower(true), upper(false)),
        _ => return Ok([universe.clone(), universe.clone(), universe.clone()]),
    };
    let mut result = [
        yes,
        no,
        if null_safe {
            universe.empty()
        } else {
            universe.only_null()
        },
    ];
    if null_safe {
        result[if op == "distinct_from" { TRUE } else { FALSE }].may_be_null = universe.may_be_null;
    }
    Ok(result)
}

#[cfg(test)]
mod tests;

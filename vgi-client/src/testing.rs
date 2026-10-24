// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Regression helpers for authors of VGI workers (`testing` feature).
//!
//! [`TestWorker`] exercises the normal attach/bind/scan protocol. [`ScanResult`]
//! compares schemas and unordered rows, including duplicate counts and NULLs.
//! [`FilterExpr`] builds typed v2 filter fixtures without handwritten IPC.
//!
//! Use independent expected fixtures or a reference implemented without the
//! worker's pushdown evaluator. Comparing two calls to the same evaluator can
//! reproduce the same bug on both sides. Tests collect results in memory and
//! are intended for small, deterministic fixtures.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeListArray, RecordBatch, RecordBatchOptions, StringArray};
use arrow_buffer::OffsetBuffer;
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use serde_json::{json, Value};

use crate::{AttachOptions, AttachedCatalog, BindSpec, Result, RpcError, ScanOptions, VgiClient};

fn error(message: impl Into<String>) -> RpcError {
    RpcError::value_error(message)
}

/// An attached client for small worker regression tests.
///
/// The worker must terminate scans; this harness collects all their rows.
/// For connection timeouts, logging, or another transport, configure a
/// [`VgiClient`] and use [`Self::attach`].
pub struct TestWorker {
    client: VgiClient,
    catalog: AttachedCatalog,
}

impl TestWorker {
    /// Spawn a worker and attach a catalog using default attach options.
    pub fn connect_subprocess<S: AsRef<OsStr>>(command: &[S], catalog: &str) -> Result<Self> {
        Self::attach(
            VgiClient::connect_subprocess(command)?,
            catalog,
            AttachOptions::default(),
        )
    }

    /// Attach a configured client, preserving its transport and log sink.
    pub fn attach(mut client: VgiClient, catalog: &str, options: AttachOptions) -> Result<Self> {
        let catalog = client.attach(catalog, options)?;
        Ok(Self { client, catalog })
    }

    /// The attached catalog, including its default schema.
    pub fn catalog(&self) -> &AttachedCatalog {
        &self.catalog
    }

    /// Access the underlying client for protocol operations beyond scans.
    pub fn client_mut(&mut self) -> &mut VgiClient {
        &mut self.client
    }

    /// Bind a table function and collect its scan, preserving an empty schema.
    pub fn scan(&mut self, spec: &BindSpec, options: &ScanOptions) -> Result<ScanResult> {
        let bound = self.client.bind(&self.catalog, spec)?;
        let mut scan = self.client.scan(&bound, options)?;
        let schema = Arc::clone(scan.schema());
        ScanResult::try_new(schema, scan.collect()?)
    }

    /// Compare a pushed scan with independently filtered full-scan rows.
    ///
    /// First scans with default options (no projection or pushdown). `reference`
    /// must return independently selected rows in that full, original schema.
    /// This helper then applies `options.projection` to the reference and
    /// compares it with the pushed scan. Include filter-only columns using
    /// [`scan_options`]. The source must remain unchanged between scans.
    ///
    /// This checks exact filter application, so use it for SDK-managed or
    /// worker-managed exact filtering, not an advisory-only scan. Limits,
    /// sampling, ordering, joins, and split redemption are outside this helper's
    /// scope; use [`Self::scan`] with explicit expected results for those cases.
    pub fn check_pushdown(
        &mut self,
        spec: &BindSpec,
        options: &ScanOptions,
        reference: impl FnOnce(&ScanResult) -> Result<ScanResult>,
    ) -> Result<()> {
        if options.pushdown_filters.is_none() {
            return Err(error("check_pushdown requires pushdown_filters"));
        }
        if options.order_by.is_some()
            || options.sample.is_some()
            || options.row_limit.is_some()
            || options.join_keys.is_some()
            || options.execution_id.is_some()
            || options.init_opaque_data.is_some()
            || options.substream_id.is_some()
            || options.split_tokens.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
        {
            return Err(error("check_pushdown accepts only filters and projection"));
        }
        let full = self.scan(spec, &ScanOptions::default())?;
        let mut expected = reference(&full)?;
        if expected.schema != full.schema {
            return Err(error("reference must preserve the full scan schema; projection is applied by check_pushdown"));
        }
        if let Some(projection) = &options.projection {
            let indices = projection
                .iter()
                .map(|index| {
                    usize::try_from(*index).map_err(|_| error("negative projection index"))
                })
                .collect::<Result<Vec<_>>>()?;
            expected = expected.project(&indices)?;
        }
        self.scan(spec, options)?.assert_matches(&expected)
    }
}

/// A collected scan with an explicit schema, including when it has no rows.
#[derive(Clone, Debug)]
pub struct ScanResult {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl ScanResult {
    /// Construct expected results or wrap a collected scan.
    ///
    /// Checks every batch's field names, types, and values against `schema`.
    /// Bind-time nullability and metadata are authoritative: transports may
    /// relax batch nullability, so batches are rebuilt with the supplied schema.
    pub fn try_new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        let batches = batches
            .into_iter()
            .enumerate()
            .map(|(index, batch)| {
                if batch.schema().fields().len() != schema.fields().len()
                    || batch
                        .schema()
                        .fields()
                        .iter()
                        .zip(schema.fields())
                        .any(|(a, b)| a.name() != b.name() || a.data_type() != b.data_type())
                {
                    return Err(error(format!(
                        "batch {index} names/types differ from declared schema"
                    )));
                }
                let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
                RecordBatch::try_new_with_options(
                    Arc::clone(&schema),
                    batch.columns().to_vec(),
                    &options,
                )
                .map_err(|e| error(format!("batch {index} violates declared schema: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { schema, batches })
    }

    /// The declared scan schema, independent of whether it returned rows.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Collected batches in the worker's output order.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Number of rows, including duplicates.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    /// Project columns in the supplied order, retaining zero-column row counts.
    pub fn project(&self, indices: &[usize]) -> Result<Self> {
        let schema = Arc::new(
            self.schema
                .project(indices)
                .map_err(|e| error(e.to_string()))?,
        );
        let batches = self
            .batches
            .iter()
            .map(|batch| batch.project(indices).map_err(|e| error(e.to_string())))
            .collect::<Result<_>>()?;
        Self::try_new(schema, batches)
    }

    /// Require identical schemas and the same multiset of typed rows.
    ///
    /// Batch boundaries and row order do not matter; column order, nullability,
    /// metadata, values, and duplicate counts do. Arrow row encoding compares
    /// values without converting integers to floats or displaying them as text.
    /// Floating-point bit patterns (including signed zero and NaN payloads) are
    /// distinguished; this is a data-fidelity assertion, not SQL equality.
    /// Types unsupported by Arrow's row encoder return an error.
    pub fn assert_matches(&self, expected: &Self) -> Result<()> {
        if self.schema != expected.schema {
            return Err(error(format!(
                "scan schemas differ\nexpected: {:?}\nactual: {:?}",
                expected.schema, self.schema
            )));
        }
        if self.num_rows() != expected.num_rows() {
            return Err(error(format!(
                "scan row counts differ: expected {}, actual {}",
                expected.num_rows(),
                self.num_rows()
            )));
        }
        // Arrow cannot infer the row count from zero columns.
        if self.schema.fields().is_empty() {
            return Ok(());
        }
        let converter = RowConverter::new(
            self.schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )
        .map_err(|e| error(format!("cannot compare scan rows: {e}")))?;
        let count_rows = |batches: &[RecordBatch]| -> Result<HashMap<Vec<u8>, usize>> {
            let mut counts = HashMap::new();
            for batch in batches {
                let rows = converter
                    .convert_columns(batch.columns())
                    .map_err(|e| error(format!("cannot encode scan rows: {e}")))?;
                for row in rows.iter() {
                    *counts.entry(row.as_ref().to_vec()).or_insert(0) += 1;
                }
            }
            Ok(counts)
        };
        let actual = count_rows(&self.batches)?;
        let expected = count_rows(&expected.batches)?;
        if actual != expected {
            let missing: usize = expected
                .iter()
                .map(|(row, count)| count.saturating_sub(*actual.get(row).unwrap_or(&0)))
                .sum();
            let unexpected: usize = actual
                .iter()
                .map(|(row, count)| count.saturating_sub(*expected.get(row).unwrap_or(&0)))
                .sum();
            return Err(error(format!("scan rows differ: {missing} missing and {unexpected} unexpected row occurrence(s); compare the fixture values, including NULLs and duplicate counts")));
        }
        Ok(())
    }
}

/// A comparison for a typed test filter.
#[derive(Clone, Copy, Debug)]
pub enum Comparison {
    /// SQL `=`.
    Eq,
    /// SQL `<>`.
    NotEq,
    /// SQL `<`.
    Lt,
    /// SQL `<=`.
    LtEq,
    /// SQL `>`.
    Gt,
    /// SQL `>=`.
    GtEq,
}

impl Comparison {
    fn wire(self) -> &'static str {
        match self {
            Self::Eq => "eq",
            Self::NotEq => "ne",
            Self::Lt => "lt",
            Self::LtEq => "le",
            Self::Gt => "gt",
            Self::GtEq => "ge",
        }
    }
}

/// A small typed expression builder for filter regression fixtures.
///
/// Supports comparisons, NULL tests, IN/NOT IN, and Boolean combinations.
/// Literals retain their Arrow type. It does not evaluate expressions and is
/// therefore independent of the worker's evaluator. Encoding uses required
/// predicates with standard, context-free VGI semantics.
#[derive(Clone, Debug)]
pub struct FilterExpr(Expr);

#[derive(Clone, Debug)]
enum Expr {
    Compare(String, Comparison, ArrayRef),
    IsNull(String, bool),
    In(String, ArrayRef, bool),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
}

impl FilterExpr {
    /// Compare a named column to an Arrow array containing exactly one scalar.
    /// Length, name, and exact type are checked by [`scan_options`].
    pub fn compare(column: impl Into<String>, comparison: Comparison, scalar: ArrayRef) -> Self {
        Self(Expr::Compare(column.into(), comparison, scalar))
    }

    /// SQL `column IS NULL`.
    pub fn is_null(column: impl Into<String>) -> Self {
        Self(Expr::IsNull(column.into(), false))
    }

    /// SQL `column IS NOT NULL`.
    pub fn is_not_null(column: impl Into<String>) -> Self {
        Self(Expr::IsNull(column.into(), true))
    }

    /// SQL `column IN (values...)`; NULLs in the typed array remain SQL NULLs.
    pub fn in_values(column: impl Into<String>, values: ArrayRef) -> Self {
        Self(Expr::In(column.into(), values, false))
    }

    /// SQL `column NOT IN (values...)`.
    pub fn not_in_values(column: impl Into<String>, values: ArrayRef) -> Self {
        Self(Expr::In(column.into(), values, true))
    }

    /// SQL `self AND other`.
    #[must_use]
    pub fn and(self, other: Self) -> Self {
        Self(Expr::And(Box::new(self), Box::new(other)))
    }

    /// SQL `self OR other`.
    #[must_use]
    pub fn or(self, other: Self) -> Self {
        Self(Expr::Or(Box::new(self), Box::new(other)))
    }
}

impl std::ops::Not for FilterExpr {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self(Expr::Not(Box::new(self)))
    }
}

/// Build scan options containing a typed v2 filter and optional projection.
///
/// `schema` is the full bind output schema. Projection indices are into that
/// schema; referenced filter columns are included automatically and removed
/// from client results if they were not selected. V2 expression column indices
/// always address the full schema, even when the projection reorders columns.
/// No scalar is coerced: use an Arrow literal with the column's exact type.
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::Int64Array;
/// use arrow_schema::{DataType, Field, Schema};
/// use vgi_client::testing::{scan_options, Comparison, FilterExpr};
/// # fn main() -> vgi_client::Result<()> {
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new("name", DataType::Utf8, true),
/// ]);
/// let filter = FilterExpr::compare("id", Comparison::Gt, Arc::new(Int64Array::from(vec![10])));
/// let options = scan_options(&schema, &filter, Some(vec![1]))?;
/// assert_eq!(options.wire_projection(), Some(vec![1, 0]));
/// # Ok(())
/// # }
/// ```
pub fn scan_options(
    schema: &Schema,
    filter: &FilterExpr,
    projection: Option<Vec<usize>>,
) -> Result<ScanOptions> {
    if let Some(indices) = &projection {
        schema
            .project(indices)
            .map_err(|e| error(format!("invalid test projection: {e}")))?;
    }
    let mut encoder = Encoder {
        schema,
        values: Vec::new(),
        columns: Vec::new(),
    };
    let expression = encoder.expression(filter)?;
    let document = json!({
        "encoding": "vgi.filters.v2", "semantics": "vgi.duckdb.standard.v1", "kind": "snapshot",
        "predicates": [{ "id": "test", "revision": 0, "mode": "required", "source": "query", "expression": expression }]
    }).to_string();
    let mut fields = vec![Field::new("filter_spec", DataType::Utf8, false)];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![document]))];
    for (index, value) in encoder.values.into_iter().enumerate() {
        fields.push(Field::new(
            format!("value_{index}"),
            value.data_type().clone(),
            true,
        ));
        arrays.push(value);
    }
    let metadata = HashMap::from([
        ("vgi_filter_encoding".into(), "vgi.filters.v2".into()),
        ("vgi_filter_version".into(), "2".into()),
        ("vgi_evaluation_context".into(), "vgi.none.v1".into()),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, metadata)),
        arrays,
    )
    .map_err(|e| error(e.to_string()))?;
    Ok(ScanOptions {
        projection: projection
            .map(|indices| indices.into_iter().map(|index| index as i64).collect()),
        filter_columns: Some(encoder.columns),
        pushdown_filters: Some(vgi_protocol::ipc::write_batch(&batch)?),
        ..ScanOptions::default()
    })
}

struct Encoder<'a> {
    schema: &'a Schema,
    values: Vec<ArrayRef>,
    columns: Vec<i64>,
}

impl Encoder<'_> {
    fn column(&mut self, name: &str, data_type: Option<&DataType>) -> Result<Value> {
        let indices: Vec<_> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field.name() == name)
            .collect();
        let [(index, field)] = indices.as_slice() else {
            return Err(error(format!(
                "test filter column {name:?} must occur exactly once in the bind schema"
            )));
        };
        if data_type.is_some_and(|dt| field.data_type() != dt) {
            return Err(error(format!(
                "test filter literal for {name:?} must have type {:?}, got {:?}",
                field.data_type(),
                data_type.unwrap()
            )));
        }
        let index = *index as i64;
        if !self.columns.contains(&index) {
            self.columns.push(index);
        }
        Ok(json!({"node": "column_ref", "column_index": index, "column_name": name}))
    }

    fn literal(&mut self, value: ArrayRef) -> usize {
        let index = self.values.len();
        self.values.push(value);
        index
    }

    fn expression(&mut self, filter: &FilterExpr) -> Result<Value> {
        Ok(match &filter.0 {
            Expr::Compare(column, comparison, scalar) => {
                if scalar.len() != 1 {
                    return Err(error(
                        "comparison literal must contain exactly one Arrow value",
                    ));
                }
                let left = self.column(column, Some(scalar.data_type()))?;
                let index = self.literal(Arc::clone(scalar));
                json!({"node": "comparison", "op": comparison.wire(), "left": left, "right": {"node": "literal", "value_ref": index}})
            }
            Expr::IsNull(column, negated) => {
                let expression = self.column(column, None)?;
                json!({"node": "is_null", "expression": expression, "negated": negated})
            }
            Expr::In(column, values, negated) => {
                let expression = self.column(column, Some(values.data_type()))?;
                let len =
                    i64::try_from(values.len()).map_err(|_| error("IN fixture is too large"))?;
                let list = LargeListArray::new(
                    Arc::new(Field::new("item", values.data_type().clone(), true)),
                    OffsetBuffer::new(vec![0, len].into()),
                    Arc::clone(values),
                    None,
                );
                let index = self.literal(Arc::new(list));
                json!({"node": "in", "expression": expression, "set": {"kind": "literal", "value_ref": index}, "negated": negated})
            }
            Expr::And(a, b) => {
                json!({"node": "and", "children": [self.expression(a)?, self.expression(b)?]})
            }
            Expr::Or(a, b) => {
                json!({"node": "or", "children": [self.expression(a)?, self.expression(b)?]})
            }
            Expr::Not(expr) => json!({"node": "not", "expression": self.expression(expr)?}),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        Decimal128Array, Float64Array, Int64Array, TimestampMicrosecondArray, UInt64Array,
    };

    fn fixture(values: Vec<Option<i64>>, labels: Vec<Option<&str>>) -> ScanResult {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(values)),
                Arc::new(StringArray::from(labels)),
            ],
        )
        .unwrap();
        ScanResult::try_new(schema, vec![batch]).unwrap()
    }

    #[test]
    fn compares_multisets_across_batch_boundaries_with_nulls_and_duplicates() {
        let a = fixture(
            vec![Some(1), None, Some(1)],
            vec![Some("a"), None, Some("a")],
        );
        let b = fixture(
            vec![None, Some(1), Some(1)],
            vec![None, Some("a"), Some("a")],
        );
        let split = ScanResult::try_new(
            Arc::clone(b.schema()),
            vec![b.batches[0].slice(0, 1), b.batches[0].slice(1, 2)],
        )
        .unwrap();
        a.assert_matches(&split).unwrap();
        let wrong = fixture(vec![Some(1), None, None], vec![Some("a"), None, None]);
        let err = a.assert_matches(&wrong).unwrap_err();
        assert!(err.message.contains("1 missing and 1 unexpected"));
    }

    #[test]
    fn detects_precision_loss_and_unsigned_corruption() {
        let schema = Arc::new(Schema::new(vec![Field::new("u", DataType::UInt64, false)]));
        let result = |value| {
            ScanResult::try_new(
                Arc::clone(&schema),
                vec![RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(UInt64Array::from(vec![value]))],
                )
                .unwrap()],
            )
            .unwrap()
        };
        assert!(result(9_007_199_254_740_993)
            .assert_matches(&result(9_007_199_254_740_992))
            .is_err());
        assert!(result(u64::MAX)
            .assert_matches(&result(i64::MAX as u64))
            .is_err());
    }

    #[test]
    fn float_comparison_preserves_signed_zero_and_nan_payloads() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float64, false)]));
        let result = |value| {
            ScanResult::try_new(
                Arc::clone(&schema),
                vec![RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Float64Array::from(vec![value]))],
                )
                .unwrap()],
            )
            .unwrap()
        };
        assert!(result(0.0).assert_matches(&result(-0.0)).is_err());
        let nan = f64::from_bits(0x7ff8_0000_0000_0001);
        result(nan).assert_matches(&result(nan)).unwrap();
        assert!(result(nan)
            .assert_matches(&result(f64::from_bits(0x7ff8_0000_0000_0002)))
            .is_err());
    }

    #[test]
    fn typed_literal_encoding_retains_decimal_and_timestamp_parameters() {
        let values: Vec<ArrayRef> = vec![
            Arc::new(
                Decimal128Array::from(vec![12_345])
                    .with_precision_and_scale(12, 3)
                    .unwrap(),
            ),
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_700_000_000_000_001]).with_timezone("UTC"),
            ),
            Arc::new(UInt64Array::from(vec![u64::MAX])),
        ];
        for value in values {
            let schema = Schema::new(vec![Field::new("x", value.data_type().clone(), true)]);
            let options = scan_options(
                &schema,
                &FilterExpr::compare("x", Comparison::GtEq, Arc::clone(&value)),
                None,
            )
            .unwrap();
            let encoded =
                vgi_protocol::ipc::read_batch(options.pushdown_filters.as_ref().unwrap()).unwrap();
            assert_eq!(encoded.column(1).to_data(), value.to_data());
            vgi::pushdown::PushdownFilters::parse_with_schema(
                options.pushdown_filters.as_ref().unwrap(),
                Arc::new(schema),
            )
            .unwrap();
        }
    }

    #[test]
    fn empty_results_retain_schema_and_projection_retains_zero_column_counts() {
        let full = fixture(vec![Some(1), Some(2)], vec![None, Some("b")]);
        let empty = ScanResult::try_new(Arc::clone(full.schema()), vec![]).unwrap();
        let other = ScanResult::try_new(Arc::new(Schema::empty()), vec![]).unwrap();
        assert!(empty.assert_matches(&other).is_err());
        assert_eq!(full.project(&[]).unwrap().num_rows(), 2);
        let reordered = full.project(&[1, 0]).unwrap();
        assert_eq!(reordered.schema().field(0).name(), "label");
        assert!(full.assert_matches(&reordered).is_err());
    }

    #[test]
    fn schema_contract_rejects_null_values_under_nonnullable_fields() {
        let full = fixture(vec![None], vec![Some("a")]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        assert!(ScanResult::try_new(schema, full.batches).is_err());
    }

    #[test]
    fn typed_filters_roundtrip_and_keep_bind_indices_under_projection() {
        let full = fixture(vec![Some(1), Some(2), None], vec![None, Some("a"), None]);
        let compare =
            FilterExpr::compare("id", Comparison::Gt, Arc::new(Int64Array::from(vec![1])));
        let filter = compare.or(FilterExpr::is_not_null("label"));
        let options = scan_options(full.schema(), &filter, Some(vec![1])).unwrap();
        assert_eq!(options.wire_projection(), Some(vec![1, 0]));
        let parsed = vgi::pushdown::PushdownFilters::parse_with_schema(
            options.pushdown_filters.as_ref().unwrap(),
            Arc::clone(full.schema()),
        )
        .unwrap();
        let actual = parsed.apply(&full.batches[0]).unwrap();
        assert_eq!(
            actual
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[2]
        );
        for comparison in [
            Comparison::Eq,
            Comparison::NotEq,
            Comparison::Lt,
            Comparison::LtEq,
            Comparison::Gt,
            Comparison::GtEq,
        ] {
            let filter = FilterExpr::compare("id", comparison, Arc::new(Int64Array::from(vec![1])));
            let options = scan_options(full.schema(), &filter, None).unwrap();
            vgi::pushdown::PushdownFilters::parse_with_schema(
                options.pushdown_filters.as_ref().unwrap(),
                Arc::clone(full.schema()),
            )
            .unwrap();
        }
    }

    #[test]
    fn in_not_in_and_not_preserve_sql_null_semantics() {
        let full = fixture(vec![Some(1), Some(2), None], vec![None, None, None]);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
        for (filter, expected) in [
            (FilterExpr::in_values("id", Arc::clone(&values)), 1),
            (FilterExpr::not_in_values("id", values), 0),
            (
                (!FilterExpr::is_null("id")).and(FilterExpr::is_null("label")),
                2,
            ),
        ] {
            let options = scan_options(full.schema(), &filter, None).unwrap();
            let parsed = vgi::pushdown::PushdownFilters::parse_with_schema(
                options.pushdown_filters.as_ref().unwrap(),
                Arc::clone(full.schema()),
            )
            .unwrap();
            assert_eq!(parsed.apply(&full.batches[0]).unwrap().num_rows(), expected);
        }
    }

    #[test]
    fn invalid_fixture_types_shapes_and_columns_fail_early() {
        let full = fixture(vec![], vec![]);
        for filter in [
            FilterExpr::compare("id", Comparison::Eq, Arc::new(Int64Array::from(vec![1, 2]))),
            FilterExpr::compare("id", Comparison::Eq, Arc::new(UInt64Array::from(vec![1]))),
            FilterExpr::is_null("missing"),
        ] {
            assert!(scan_options(full.schema(), &filter, None).is_err());
        }
        assert!(scan_options(full.schema(), &FilterExpr::is_null("id"), Some(vec![2])).is_err());
    }
}

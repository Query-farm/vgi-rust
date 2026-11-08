// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Protocol 3.2 filtered scan planning.
//!
//! A catalog table that declares
//! [`ScanPlanning::Filtered`](crate::catalog::ScanPlanning::Filtered) receives the
//! query's predicates in `catalog_table_scan_branches_get`. The worker hands them
//! to the table's [`ScanBranchPlanner`] as [`ScanPlanningFilters`], which may use
//! them to return fewer branches, or fewer files per branch.
//!
//! Every predicate is **advisory**: the client re-applies all of them to the rows
//! the branches produce, so a planner may ignore any predicate, but it must never
//! drop a branch or file that could hold a matching row.
//! [`ScanPlanningFilters::may_match`] prunes candidates described by min/max
//! statistics without writing an evaluator.
//!
//! Independently of filtered planning, a branch may carry
//! [`CatBranch::row_count`] and [`CatBranch::column_statistics`]; the client drops
//! a branch whose statistics prove that no row can match.
//!
//! See `docs/protocol/scan-planning.md` in vgi-python (normative) and
//! `docs/filtering-and-pruning.md` here.

use std::fmt;
use std::sync::{Arc, OnceLock};

use arrow_schema::SchemaRef;
use vgi_rpc::{Result, RpcError};

use crate::catalog::{CatBranch, CatTable};
use crate::pushdown::PushdownFilters;
use crate::statistics::CatColStat;

type ColumnsResolver = Box<dyn Fn() -> Result<SchemaRef> + Send + Sync>;

/// The query predicates sent with a filtered `catalog_table_scan_branches_get`.
///
/// Holds the raw wire bytes (an Arrow IPC stream with one VGI Filter Encoding v2
/// snapshot batch whose column indexes refer to the table's declared columns,
/// `TableInfo.columns`) and decodes them on first use, at most once.
pub struct ScanPlanningFilters {
    raw: Vec<u8>,
    columns: ColumnsResolver,
    decoded: OnceLock<std::result::Result<PushdownFilters, RpcError>>,
}

impl fmt::Debug for ScanPlanningFilters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanPlanningFilters")
            .field("raw_len", &self.raw.len())
            .field("decoded", &self.decoded.get().map(|d| d.is_ok()))
            .finish()
    }
}

impl ScanPlanningFilters {
    /// Filters over a table whose declared columns are `columns`.
    pub fn new(raw: impl Into<Vec<u8>>, columns: SchemaRef) -> Self {
        Self::with_columns_resolver(raw, move || Ok(columns.clone()))
    }

    /// Filters whose table columns are resolved by `columns`, called at most
    /// once, on the first [`decode`](Self::decode).
    pub fn with_columns_resolver(
        raw: impl Into<Vec<u8>>,
        columns: impl Fn() -> Result<SchemaRef> + Send + Sync + 'static,
    ) -> Self {
        Self {
            raw: raw.into(),
            columns: Box::new(columns),
            decoded: OnceLock::new(),
        }
    }

    /// The wire bytes, exactly as the client sent them.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    /// The predicates, decoded against the table's declared columns (cached).
    pub fn decode(&self) -> Result<&PushdownFilters> {
        self.decoded
            .get_or_init(|| {
                let columns = (self.columns)()?;
                PushdownFilters::parse_with_schema(&self.raw, columns)
            })
            .as_ref()
            .map_err(Clone::clone)
    }

    /// `false` only when the predicates provably exclude every row of a
    /// candidate described by `statistics`.
    ///
    /// `statistics` describes a candidate (a file, a partition, a branch) the
    /// way `ScanBranch.column_statistics` does: every non-NULL value of a column
    /// lies within `[min, max]`. Columns, bounds or predicates this cannot reason
    /// about count as "may match", so the answer errs towards keeping data. See
    /// [`PushdownFilters::may_match_statistics`].
    pub fn may_match(&self, statistics: &[CatColStat]) -> Result<bool> {
        self.decode()?.may_match_statistics(statistics)
    }
}

/// What a [`ScanBranchPlanner`] is asked to plan: one
/// `catalog_table_scan_branches_get` call.
pub struct ScanPlanRequest<'a> {
    /// The attach's `attach_opaque_data`.
    pub attach_opaque_data: Option<&'a [u8]>,
    /// The open transaction's `transaction_opaque_data`, if any.
    pub transaction_opaque_data: Option<&'a [u8]>,
    /// The schema containing the table.
    pub schema_path: &'a [String],
    /// The table, already resolved to the `AT` version the request names.
    pub table: &'a CatTable,
    /// Time-travel unit, if any.
    pub at_unit: Option<&'a str>,
    /// Time-travel value matching `at_unit`, if any.
    pub at_value: Option<&'a str>,
    /// The query's predicates on this table, or `None` when none were collected
    /// (bind-time planning, or no usable predicate).
    pub pushdown_filters: Option<&'a ScanPlanningFilters>,
}

impl ScanPlanRequest<'_> {
    /// The table's declared branches, ignoring the predicates: its
    /// [`CatTable::branches`] when set, otherwise one function branch wrapping
    /// its scan function. This is what the worker returns when the table has
    /// no planner — the protocol 3.1 answer.
    pub fn default_plan(&self) -> ScanBranchesPlan {
        let branches = match &self.table.branches {
            Some(branches) => branches.clone(),
            None => vec![CatBranch {
                function_name: self.table.scan_function.clone(),
                scan_arguments: self.table.scan_arguments.clone(),
                ..Default::default()
            }],
        };
        ScanBranchesPlan {
            branches,
            required_extensions: self.table.required_extensions.clone(),
        }
    }
}

/// The answer to a `catalog_table_scan_branches_get`.
#[derive(Clone, Default)]
pub struct ScanBranchesPlan {
    /// One entry per physical source. Empty is legal for a
    /// [`Filtered`](crate::catalog::ScanPlanning::Filtered) table and means the
    /// scan produces no rows.
    pub branches: Vec<CatBranch>,
    /// DuckDB extensions the branches require.
    pub required_extensions: Vec<String>,
}

/// Plans a table's scan branches with the query's predicates (protocol 3.2).
///
/// Set one on [`CatTable::scan_planner`]. The worker calls it for every
/// `catalog_table_scan_branches_get` of the table; a table without one gets
/// [`ScanPlanRequest::default_plan`]. Usually paired with
/// [`ScanPlanning::Filtered`](crate::catalog::ScanPlanning::Filtered), which makes
/// the client defer the call until it has the predicates; on a
/// [`Bind`](crate::catalog::ScanPlanning::Bind) table `pushdown_filters` is always
/// `None`.
///
/// Return any superset of the branches and files that could hold matching
/// rows, and never drop one that could: the client re-applies every predicate.
///
/// ```
/// use vgi::catalog::CatBranch;
/// use vgi::scan_planning::{ScanBranchPlanner, ScanBranchesPlan, ScanPlanRequest};
/// use vgi::statistics::CatColStat;
///
/// struct Days(Vec<(String, Vec<CatColStat>)>);
///
/// impl ScanBranchPlanner for Days {
///     fn plan(&self, request: &ScanPlanRequest<'_>) -> vgi::Result<ScanBranchesPlan> {
///         let mut files = Vec::new();
///         for (path, stats) in &self.0 {
///             let keep = match request.pushdown_filters {
///                 Some(filters) => filters.may_match(stats)?,
///                 None => true,
///             };
///             if keep {
///                 files.push(path.clone());
///             }
///         }
///         let branches = if files.is_empty() {
///             Vec::new() // legal for a filtered table: no rows
///         } else {
///             vec![CatBranch {
///                 format_name: Some("parquet".into()),
///                 format_locations: files,
///                 ..Default::default()
///             }]
///         };
///         Ok(ScanBranchesPlan { branches, required_extensions: vec!["parquet".into()] })
///     }
/// }
/// ```
pub trait ScanBranchPlanner: Send + Sync {
    /// Plan the branches. Default: [`ScanPlanRequest::default_plan`], ignoring
    /// the predicates.
    fn plan(&self, request: &ScanPlanRequest<'_>) -> Result<ScanBranchesPlan> {
        Ok(request.default_plan())
    }
}

/// The planner a table without [`CatTable::scan_planner`] uses: the declared
/// branches, predicates ignored.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultScanPlanner;

impl ScanBranchPlanner for DefaultScanPlanner {}

impl<F> ScanBranchPlanner for F
where
    F: Fn(&ScanPlanRequest<'_>) -> Result<ScanBranchesPlan> + Send + Sync,
{
    fn plan(&self, request: &ScanPlanRequest<'_>) -> Result<ScanBranchesPlan> {
        self(request)
    }
}

/// Shorthand for [`CatTable::scan_planner`]'s type.
pub type SharedScanBranchPlanner = Arc<dyn ScanBranchPlanner>;

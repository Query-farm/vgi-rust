// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Producer-mode scans: bind, init, drain.
//!
//! A table-function scan is three steps:
//!
//! 1. **bind** — a unary call that resolves the output schema before any data
//!    moves, and hands back an opaque blob the worker wants echoed at init.
//! 2. **init** — opens a producer stream. Its header carries the worker-minted
//!    `execution_id` and `max_workers`, the latter being how many parallel
//!    connections the worker will accept for this scan.
//! 3. **drain** — tick until the stream ends.
//!
//! Pushdown (projection, filters, ORDER BY, TABLESAMPLE) rides the init
//! request, so it is decided after bind and before the first row.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use vgi_protocol::cache_control::CacheControl;
use vgi_protocol::generated::request_params as p;
use vgi_protocol::protocol::dtos::{
    BindRequest, BindResponse, GlobalInitResponse, InitRequest, PlanResponse, ScanSplit,
    TableFunctionPlanRequest,
};
use vgi_protocol::{ipc, wire};
use vgi_rpc::errors::{Result, RpcError};
use vgi_rpc::{Bytes, DictString, LargeBytes};

use crate::args::Arguments;
use crate::catalog::{At, AttachedCatalog};
use crate::client::VgiClient;
use crate::transport::ProducerStream;
use crate::wire_call::{call, envelope};

/// Which flavour of function is being bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionType {
    /// A producer table function.
    Table,
    /// A buffered table function.
    TableBuffering,
    /// A streaming table-in-out function.
    TableInOut,
    /// A scalar function.
    Scalar,
    /// An aggregate function.
    Aggregate,
}

impl FunctionType {
    /// The wire spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::TableBuffering => "table_buffering",
            Self::TableInOut => "table_in_out",
            Self::Scalar => "scalar",
            Self::Aggregate => "aggregate",
        }
    }
}

/// What to bind.
#[derive(Debug, Clone)]
pub struct BindSpec {
    /// The function's name in its schema.
    pub function_name: String,
    /// Which flavour it is.
    pub function_type: FunctionType,
    /// The schema that owns it. Resolution dispatches on `(schema, name)`, so a
    /// name declared in two schemas of one catalog needs this to disambiguate.
    pub schema_name: Option<String>,
    /// Call arguments.
    pub arguments: Arguments,
    /// Pre-serialized call arguments, used in place of [`Self::arguments`].
    ///
    /// A catalog table's scan arguments arrive from the worker already IPC
    /// encoded (`ScanFunctionResult::arguments`) and can hold types this
    /// client's [`ArgValue`](crate::ArgValue) does not model. Re-encoding them
    /// would be lossy, so they are forwarded byte-for-byte.
    pub raw_arguments: Option<Bytes>,
    /// IPC-encoded settings, if the catalog declares any.
    pub settings: Option<Bytes>,
    /// Time travel for this bind.
    pub at: Option<At>,
}

impl BindSpec {
    /// A table-function bind with no arguments.
    pub fn table(function_name: impl Into<String>) -> Self {
        Self {
            function_name: function_name.into(),
            function_type: FunctionType::Table,
            schema_name: None,
            arguments: Arguments::new(),
            raw_arguments: None,
            settings: None,
            at: None,
        }
    }

    /// Use argument bytes the worker already encoded, verbatim.
    ///
    /// Takes precedence over [`Self::with_arguments`].
    #[must_use]
    pub fn with_raw_arguments(mut self, args: Bytes) -> Self {
        self.raw_arguments = Some(args);
        self
    }

    /// Set the owning schema.
    #[must_use]
    pub fn in_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema_name = Some(schema.into());
        self
    }

    /// Set the call arguments.
    #[must_use]
    pub fn with_arguments(mut self, args: Arguments) -> Self {
        self.arguments = args;
        self
    }
}

/// A bound function, ready to scan.
///
/// Holds the serialized bind request as well as the response, because `init`
/// echoes the whole bind call back — the worker re-reads it rather than keeping
/// per-bind state.
#[derive(Debug, Clone)]
pub struct BoundFunction {
    bind_call: Bytes,
    response: BindResponse,
    output_schema: SchemaRef,
}

impl BoundFunction {
    pub(crate) fn from_parts(
        bind_call: Bytes,
        response: BindResponse,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            bind_call,
            response,
            output_schema,
        }
    }

    /// The IPC bytes of the bind output schema, as the worker sent them.
    pub(crate) fn raw_output_schema(&self) -> &Bytes {
        &self.response.output_schema
    }

    /// Build the init request shared by every phase.
    pub(crate) fn init_request(
        &self,
        opts: &ScanOptions,
        phase: Option<&str>,
        finalize_state_id: Option<Bytes>,
    ) -> InitRequest {
        InitRequest {
            bind_call: self.bind_call.clone(),
            output_schema: self.response.output_schema.clone(),
            bind_opaque_data: Some(self.response.opaque_data.clone()),
            projection_ids: opts.projection.clone(),
            pushdown_filters: opts.pushdown_filters.clone().map(LargeBytes),
            join_keys: opts
                .join_keys
                .as_ref()
                .map(|ks| ks.iter().cloned().map(LargeBytes).collect()),
            phase: phase.map(|p| DictString(p.to_string())),
            execution_id: opts.execution_id.clone(),
            init_opaque_data: None,
            substream_id: opts.substream_id.clone(),
            order_by_column_name: opts.order_by.as_ref().map(|o| o.column.clone()),
            order_by_direction: opts
                .order_by
                .as_ref()
                .map(|o| DictString(o.direction.as_str().to_string())),
            order_by_null_order: opts
                .order_by
                .as_ref()
                .map(|o| DictString(o.null_order.as_str().to_string())),
            order_by_limit: opts.order_by.as_ref().and_then(|o| o.limit),
            tablesample_percentage: opts.sample.map(|s| s.percentage),
            tablesample_seed: opts.sample.and_then(|s| s.seed),
            finalize_state_id,
            // Split tokens ride the per-redemption init built by the split
            // path, not this shared one — a plain scan claims no splits.
            split_tokens: opts.split_tokens.clone(),
            row_limit: opts.row_limit,
        }
    }

    /// The scan's output schema, resolved at bind time.
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// The worker's opaque bind state.
    pub fn opaque_data(&self) -> &Bytes {
        &self.response.opaque_data
    }

    /// Secret types the worker asked the client to resolve, if any.
    ///
    /// A non-empty list means the worker wants a second bind carrying resolved
    /// secrets. This client does not yet drive that two-phase bind.
    pub fn required_secret_types(&self) -> &[String] {
        &self.response.lookup_secret_types
    }
}

/// Sort direction for an ORDER BY pushdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending.
    Ascending,
    /// Descending.
    Descending,
}

impl SortDirection {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ascending => "asc",
            Self::Descending => "desc",
        }
    }
}

/// Where nulls sort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullOrder {
    /// Nulls first.
    First,
    /// Nulls last.
    Last,
}

impl NullOrder {
    fn as_str(self) -> &'static str {
        match self {
            Self::First => "nulls_first",
            Self::Last => "nulls_last",
        }
    }
}

/// An ORDER BY (+ optional LIMIT) pushed into the scan.
#[derive(Debug, Clone)]
pub struct OrderBy {
    /// The column to order on.
    pub column: String,
    /// Ascending or descending.
    pub direction: SortDirection,
    /// Where nulls go.
    pub null_order: NullOrder,
    /// An optional row limit, so the worker can stop early.
    pub limit: Option<i64>,
}

/// A TABLESAMPLE pushed into the scan.
#[derive(Debug, Clone, Copy)]
pub struct Sample {
    /// Percentage of rows to keep, 0–100.
    pub percentage: f64,
    /// Optional seed for reproducibility.
    pub seed: Option<i64>,
}

/// Everything decided between bind and the first row.
#[derive(Debug, Clone, Default)]
pub struct ScanOptions {
    /// Indices into the bind output schema to actually emit.
    pub projection: Option<Vec<i64>>,
    /// Serialized filter pushdown, in the extension's filter encoding.
    pub pushdown_filters: Option<Vec<u8>>,
    /// Serialized join-key sets pushed as an IN filter.
    pub join_keys: Option<Vec<Vec<u8>>>,
    /// ORDER BY pushdown.
    pub order_by: Option<OrderBy>,
    /// TABLESAMPLE pushdown.
    pub sample: Option<Sample>,
    /// Join an existing scan rather than starting a new one.
    ///
    /// The first connection leaves this `None` and the worker mints an
    /// `execution_id`; parallel connections pass that id back so the worker
    /// knows they are the same scan.
    pub execution_id: Option<Bytes>,
    /// A client-minted per-substream identity, for the exchange shapes.
    pub substream_id: Option<Bytes>,
    /// The split envelopes this connection redeems, from a prior `plan()`.
    ///
    /// A LIST because an engine whose partition count IS its concurrency (
    /// DataFusion) bin-packs at planning time and reads a whole group per
    /// partition; without the list that is N sequential inits per partition.
    /// `None` on the ordinary primary/secondary path.
    pub split_tokens: Option<Vec<LargeBytes>>,
    /// A plain fetch limit, distinct from `order_by.limit` (a field OF the Top-N
    /// hint). Push the FULL limit into every split: over-production is legal and
    /// the engine re-applies above the coalesce, whereas dividing by N
    /// under-produces under skew.
    pub row_limit: Option<i64>,
}

/// An open producer stream.
pub struct Scan<'a> {
    stream: Box<dyn ProducerStream + 'a>,
    header: GlobalInitResponse,
    schema: SchemaRef,
    last_cache_control: Option<CacheControl>,
    finished: bool,
}

impl<'a> Scan<'a> {
    /// Open a producer stream from a pre-built init params batch.
    pub(crate) fn open(
        transport: &'a mut dyn crate::transport::VgiTransport,
        params: &RecordBatch,
        schema: SchemaRef,
    ) -> Result<Scan<'a>> {
        let stream = transport.open_producer("init", params, true)?;
        let header_batch = stream.header().ok_or_else(|| {
            RpcError::type_error("init did not return a GlobalInitResponse header")
        })?;
        let header: GlobalInitResponse = wire::from_batch(header_batch)?;
        Ok(Scan {
            stream,
            header,
            schema,
            last_cache_control: None,
            finished: false,
        })
    }
}

impl Scan<'_> {
    /// The worker-minted id for this scan. Pass it to parallel connections.
    pub fn execution_id(&self) -> &Bytes {
        &self.header.execution_id
    }

    /// How many connections the worker will accept for this scan.
    ///
    /// Advisory: it is an upper bound, not a requirement.
    pub fn max_workers(&self) -> i64 {
        self.header.max_workers
    }

    /// The output schema, as resolved at bind.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Cache directives the worker advertised on the most recent batch.
    ///
    /// Workers state these on the *first* data batch, so this is populated
    /// after the first [`Scan::next_batch`] and stays until superseded.
    pub fn cache_control(&self) -> Option<&CacheControl> {
        self.last_cache_control.as_ref()
    }

    /// Pull the next batch, or `None` at end of stream.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.finished {
            return Ok(None);
        }
        loop {
            match self.stream.tick()? {
                None => {
                    self.finished = true;
                    return Ok(None);
                }
                Some((batch, md)) => {
                    if let Some(cc) = CacheControl::from_metadata(&md) {
                        self.last_cache_control = Some(cc);
                    }
                    // A zero-row batch is a legitimate "nothing yet" tick in
                    // producer mode, not end of stream — that is signalled by
                    // the stream ending. Skip it and ask again rather than
                    // handing the caller an empty batch to special-case.
                    if batch.num_rows() == 0 {
                        continue;
                    }
                    return Ok(Some(batch));
                }
            }
        }
    }

    /// Collect every remaining batch.
    pub fn collect(&mut self) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        while let Some(b) = self.next_batch()? {
            out.push(b);
        }
        Ok(out)
    }

    /// Ask the worker to stop early.
    pub fn cancel(&mut self) -> Result<()> {
        self.finished = true;
        self.stream.cancel()
    }
}

impl VgiClient {
    /// Resolve a function's output schema before any data moves.
    pub fn bind(&mut self, cat: &AttachedCatalog, spec: &BindSpec) -> Result<BoundFunction> {
        let request = BindRequest {
            function_name: spec.function_name.clone(),
            arguments: match &spec.raw_arguments {
                Some(raw) => raw.clone(),
                None => spec.arguments.to_ipc()?,
            },
            function_type: DictString(spec.function_type.as_str().to_string()),
            input_schema: None,
            settings: spec.settings.clone(),
            secrets: None,
            attach_opaque_data: Some(cat.handle().clone()),
            transaction_opaque_data: cat.transaction().cloned(),
            resolved_secrets_provided: false,
            at_unit: spec.at.as_ref().map(|a| a.unit.clone()),
            at_value: spec.at.as_ref().map(|a| a.value.clone()),
            schema_name: spec.schema_name.clone(),
        };

        // `init` echoes the whole bind call back, so keep the exact bytes we
        // sent rather than re-serializing later — a re-serialization that
        // differed by even a field order would be a different call.
        let bind_call = envelope(request)?;
        let response: BindResponse = call(
            self.transport_mut(),
            "bind",
            p::BindParams {
                request: bind_call.clone(),
            },
        )?;

        let output_schema = ipc::read_schema(&response.output_schema.0).map_err(|e| {
            RpcError::type_error(format!("bind returned an unreadable output schema: {e}"))
        })?;

        Ok(BoundFunction {
            bind_call,
            response,
            output_schema,
        })
    }

    /// Open a scan over a bound function.
    pub fn scan<'a>(&'a mut self, bound: &BoundFunction, opts: &ScanOptions) -> Result<Scan<'a>> {
        // Projection narrows the emitted columns, so the caller's view of the
        // schema must narrow with it.
        let schema = match &opts.projection {
            None => Arc::clone(bound.output_schema()),
            Some(ids) => {
                let idx = ids
                    .iter()
                    .map(|i| {
                        usize::try_from(*i).map_err(|_| {
                            RpcError::type_error(format!("negative projection index {i}"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Arc::new(bound.output_schema().project(&idx).map_err(|e| {
                    RpcError::type_error(format!(
                        "projection is not valid for the bind output schema: {e}"
                    ))
                })?)
            }
        };

        let request = bound.init_request(opts, None, None);
        let params = wire::to_batch(p::InitParams {
            request: envelope(request)?,
        })?;
        Scan::open(self.transport_mut(), &params, schema)
    }
}

/// One named, independently redeemable unit of scan work, as the client sees it.
///
/// The `token` is what a redemption sends back — never `payload`, which is the
/// worker's own bytes sealed inside the envelope and unverifiable on its own.
#[derive(Debug, Clone)]
pub struct ScanSplitInfo {
    /// The framework-stamped envelope to send back on the redeeming init.
    pub token: Vec<u8>,
    /// Row estimate for this split, or `None` if the worker did not say.
    pub estimated_rows: Option<i64>,
    /// True when `estimated_rows` is exact — unlocks COUNT(*) from statistics.
    pub rows_exact: bool,
    /// Bin-packing weight for an engine whose partition count is fixed at
    /// planning time. `None` degrades such an engine to round-robin by count;
    /// a greedily-claiming engine needs no cost model at all.
    pub estimated_bytes: Option<i64>,
    /// `None` means UNBOUNDED — a shard read forever. An engine whose tasks must
    /// terminate has to refuse those rather than hang on one.
    pub end_position: Option<Vec<u8>>,
}

/// The result of dividing a scan into splits.
#[derive(Debug, Clone, Default)]
pub struct ScanPlan {
    /// One entry per unit of work. EMPTY is legal and means "no work" — a
    /// fully-pruned scan reaches it, and a caller must produce an empty result
    /// rather than an error. An engine that maps partitions to splits must still
    /// clamp to at least one (empty) partition.
    pub splits: Vec<ScanSplitInfo>,
    /// NORMATIVE cap on how many splits may be redeemed concurrently, not
    /// advisory. An engine whose partition count IS its concurrency must enforce
    /// it at planning time rather than relying on the worker to push back.
    pub max_workers: Option<i64>,
    /// Estimate of the total split count, for a caller sizing before enumeration finishes.
    pub estimated_total_splits: Option<i64>,
    /// Whole-scan row estimate, for cost-based planning.
    pub estimated_total_rows: Option<i64>,
    /// Whole-scan byte estimate, for cost-based planning.
    pub estimated_total_bytes: Option<i64>,
    /// The catalog counter this plan is pinned to.
    pub catalog_version: Option<i64>,
    /// `catalog` or `transaction`. A transaction-scoped plan is not cacheable
    /// and is not redeemable after commit or rollback.
    pub scope: String,
}

/// Sizing inputs for [`VgiClient::plan`].
#[derive(Debug, Clone, Default)]
pub struct PlanOptions {
    /// Indices into the bind output schema the scan will actually emit.
    pub projection: Option<Vec<i64>>,
    /// Serialized static filters, in the extension's filter encoding.
    pub pushdown_filters: Option<Vec<u8>>,
    /// The primary sizing lever: every engine is byte-driven.
    pub target_split_bytes: Option<i64>,
    /// The parallelism FLOOR. A small but expensive table still needs one reader
    /// per thread, which a byte target alone would never give it.
    pub min_splits: Option<i64>,
    /// A plain fetch limit. Push the FULL limit into every split rather than
    /// dividing it: over-production is legal and the engine re-applies above the
    /// coalesce, whereas dividing by N under-produces under skew.
    pub row_limit: Option<i64>,
}

impl VgiClient {
    /// Divide a bound scan into named, independently redeemable splits.
    ///
    /// Paginated: the worker may return a cursor, and this follows it until the
    /// enumeration is exhausted. Bounded by a page cap, because a worker that
    /// paginates forever would otherwise hang the caller — and the tempting
    /// mitigation is the wrong one. Stopping early and scanning what arrived
    /// would turn a hang into a SILENT SUBSET: a correct-looking answer missing
    /// rows, which is the failure class splits exist to prevent. So a breach is
    /// an error, never a truncation.
    ///
    /// Duplicate tokens are dropped. A worker returning overlapping cursors is
    /// a contract violation with no enforcement, and its symptom would be
    /// duplicate ROWS — arriving through the very mechanism meant to speed
    /// enumeration up. Deduping is cheap next to a plan RPC and turns a silent
    /// correctness bug into a droppable duplicate.
    pub fn plan(&mut self, bound: &BoundFunction, opts: &PlanOptions) -> Result<ScanPlan> {
        use std::collections::HashSet;

        /// Generous on purpose: the cap exists to turn a hang into a legible
        /// error, not to second-guess a worker's split count.
        const MAX_PAGES: usize = 1024;

        let mut plan = ScanPlan {
            scope: "catalog".to_string(),
            ..Default::default()
        };
        let mut cursor: Option<Bytes> = None;
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        let mut pages = 0usize;

        loop {
            if pages >= MAX_PAGES {
                return Err(RpcError::runtime_error(format!(
                    "worker exceeded the scan-planning page cap ({MAX_PAGES} pages) without \
                     exhausting its cursor; refusing to scan a partial split enumeration"
                )));
            }
            pages += 1;

            let request = TableFunctionPlanRequest {
                bind_call: bound.bind_call.clone(),
                bind_opaque_data: Some(bound.response.opaque_data.clone()),
                projection_ids: opts.projection.clone(),
                pushdown_filters: opts.pushdown_filters.clone().map(LargeBytes),
                join_keys: None,
                row_limit: opts.row_limit,
                target_split_bytes: opts.target_split_bytes,
                min_splits: opts.min_splits,
                max_splits_per_response: None,
                cursor: cursor.clone(),
                refined_filters: None,
                filters_complete: Some(true),
                start_position: None,
                end_position: None,
                order_by_column_name: None,
                order_by_direction: None,
                order_by_null_order: None,
                order_by_limit: None,
                tablesample_percentage: None,
                tablesample_seed: None,
            };

            let response: PlanResponse = call(
                self.transport_mut(),
                "table_function_plan",
                p::TableFunctionPlanParams {
                    request: envelope(request)?,
                },
            )?;

            for blob in &response.splits {
                let split: ScanSplit = wire::from_batch(&ipc::read_batch(&blob.0)?)?;
                if !seen.insert(split.token.0.clone()) {
                    continue;
                }
                plan.splits.push(ScanSplitInfo {
                    token: split.token.0.clone(),
                    estimated_rows: split.estimated_rows,
                    rows_exact: split.rows_exact,
                    estimated_bytes: split.estimated_bytes,
                    end_position: split.end_position.map(|b| b.0),
                });
            }

            plan.max_workers = plan.max_workers.or(response.max_workers);
            plan.estimated_total_splits = plan
                .estimated_total_splits
                .or(response.estimated_total_splits);
            plan.estimated_total_rows = plan.estimated_total_rows.or(response.estimated_total_rows);
            plan.estimated_total_bytes =
                plan.estimated_total_bytes.or(response.estimated_total_bytes);
            plan.catalog_version = plan.catalog_version.or(response.catalog_version);
            if !response.scope.is_empty() {
                plan.scope = response.scope.clone();
            }

            // Only the FIRST cursor is followed. Parallel enumeration is sound
            // only if the cursors partition the remaining work disjointly, and
            // that is a worker obligation with no enforcement — so this client
            // takes the serial path, which every worker supports, rather than
            // trusting a contract it cannot check.
            match response.next_cursors.first() {
                Some(next) if !next.0.is_empty() => cursor = Some(next.clone()),
                _ => break,
            }
        }

        Ok(plan)
    }
}

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
    function_name: String,
    bind_call: Bytes,
    response: BindResponse,
    output_schema: SchemaRef,
}

impl BoundFunction {
    pub(crate) fn from_parts(
        function_name: impl Into<String>,
        bind_call: Bytes,
        response: BindResponse,
        output_schema: SchemaRef,
    ) -> Self {
        Self {
            function_name: function_name.into(),
            bind_call,
            response,
            output_schema,
        }
    }

    /// The function this bind resolved, for error messages that have to say
    /// which of a query's several scans went wrong.
    pub fn function_name(&self) -> &str {
        &self.function_name
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
            projection_ids: opts.wire_projection(),
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
    /// Indices into the bind output schema that [`Self::pushdown_filters`]
    /// references but [`Self::projection`] does not ask for.
    ///
    /// A pushed filter is keyed by its column's position in the PROJECTED set,
    /// while an engine is free to push a predicate on a column the query never
    /// selects — `SELECT a FROM t WHERE b > 1` is the whole case. Left alone,
    /// the filter's key names some other column and a worker that honours it
    /// filters on the wrong data. So the init asks for the UNION, projection
    /// first, and the extra columns are dropped from each batch before it
    /// reaches the caller: [`Scan::schema`] still reports the projection alone.
    ///
    /// Ignored when `projection` is `None`, because every column is being
    /// emitted already.
    pub filter_columns: Option<Vec<i64>>,
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

/// The projection to put on the wire: the requested columns, then any
/// filter-only column, in first-reference order.
///
/// Order is load-bearing twice over. The requested columns come FIRST so
/// trimming a batch back to what the caller asked for is a prefix slice — get
/// that wrong and one column's values are read out of another's slot, which
/// nothing downstream can catch because both are the right length and often the
/// right type. And a filter's key is its column's position in THIS list, which
/// is what [`ScanOptions::wire_index_of`] computes.
///
/// `None` projection means "emit everything", so there is nothing to add: the
/// filter's columns are already there, and turning `None` into a list would
/// NARROW the scan to them.
fn union_projection(
    projection: Option<&Vec<i64>>,
    filter_columns: Option<&Vec<i64>>,
) -> Option<Vec<i64>> {
    let mut out = projection?.clone();
    for c in filter_columns.into_iter().flatten() {
        if !out.contains(c) {
            out.push(*c);
        }
    }
    Some(out)
}

impl ScanOptions {
    /// The projection this scan puts on the wire. See [`union_projection`].
    pub fn wire_projection(&self) -> Option<Vec<i64>> {
        union_projection(self.projection.as_ref(), self.filter_columns.as_ref())
    }

    /// Where `column` (an index into the bind output schema) lands in the
    /// projected set — the index a pushed filter must be keyed by.
    ///
    /// `None` when the scan projects everything, in which case a filter keeps
    /// the bind-schema index it already has.
    pub fn wire_index_of(&self, column: i64) -> Option<usize> {
        self.wire_projection()?.iter().position(|c| *c == column)
    }
}

/// An open producer stream.
pub struct Scan<'a> {
    stream: Box<dyn ProducerStream + 'a>,
    header: GlobalInitResponse,
    function: String,
    schema: SchemaRef,
    /// What the worker was asked to emit: the projection plus any filter-only
    /// column. Equal to `schema` unless those were added.
    emitted_schema: SchemaRef,
    last_cache_control: Option<CacheControl>,
    finished: bool,
}

impl<'a> Scan<'a> {
    /// Open a producer stream from a pre-built init params batch.
    pub(crate) fn open(
        transport: &'a mut dyn crate::transport::VgiTransport,
        params: &RecordBatch,
        function: impl Into<String>,
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
            function: function.into(),
            emitted_schema: Arc::clone(&schema),
            schema,
            last_cache_control: None,
            finished: false,
        })
    }

    /// Open a scan whose worker emits more columns than the caller asked for,
    /// the extras being filter-only columns that are trimmed on the way out.
    pub(crate) fn open_projected(
        transport: &'a mut dyn crate::transport::VgiTransport,
        params: &RecordBatch,
        function: impl Into<String>,
        schema: SchemaRef,
        emitted_schema: SchemaRef,
    ) -> Result<Scan<'a>> {
        let mut scan = Scan::open(transport, params, function, emitted_schema)?;
        // `open` points both at what the worker emits; narrow the caller-facing
        // one, which is the whole point of the trim.
        scan.schema = schema;
        Ok(scan)
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
                    check_batch_schema(&self.function, &self.emitted_schema, &batch)?;
                    return Ok(Some(self.trim_to_projection(batch)?));
                }
            }
        }
    }

    /// Drop the filter-only columns a batch carries.
    ///
    /// A prefix slice, and only because [`union_projection`] put the requested
    /// columns first. Cheap — the columns are `Arc`s, so this rebuilds a batch
    /// header and clones no data.
    fn trim_to_projection(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let keep = self.schema.fields().len();
        if keep == batch.num_columns() {
            return Ok(batch);
        }
        let idx: Vec<usize> = (0..keep).collect();
        batch.project(&idx).map_err(|e| {
            RpcError::runtime_error(format!(
                "function `{}`: could not drop the filter-only columns from a batch: {e}",
                self.function
            ))
        })
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

/// Narrow `schema` to `ids`, or hand it back whole when there is no projection.
fn project_schema(schema: &SchemaRef, ids: Option<&[i64]>) -> Result<SchemaRef> {
    let Some(ids) = ids else {
        return Ok(Arc::clone(schema));
    };
    let idx = ids
        .iter()
        .map(|i| {
            usize::try_from(*i)
                .map_err(|_| RpcError::type_error(format!("negative projection index {i}")))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(schema.project(&idx).map_err(|e| {
        RpcError::type_error(format!(
            "projection is not valid for the bind output schema: {e}"
        ))
    })?))
}

/// Refuse a batch whose schema is not the one the scan declared.
///
/// A scan's schema is resolved once, at bind, and every consumer downstream
/// takes it as a promise: DataFusion hands it to the physical plan and then
/// `concat_batches` the stream against it, so one non-conforming batch surfaces
/// as an Arrow error deep inside an operator, naming neither the worker, the
/// function, nor which column drifted. Checking here converts that into a
/// failure at its source, on the batch that caused it.
///
/// Names and types only. Nullability and field metadata are deliberately not
/// compared: response schemas are read with `relax_nullability`, which promotes
/// them, so a nullability difference here would be this client's own doing
/// rather than the worker's — and rejecting on it would fail scans that are
/// perfectly correct.
fn check_batch_schema(
    function: &str,
    expected: &arrow_schema::Schema,
    batch: &RecordBatch,
) -> Result<()> {
    let got = batch.schema();
    if got.fields().len() != expected.fields().len() {
        return Err(RpcError::type_error(format!(
            "function `{function}` emitted a batch with {} column(s) but its bind resolved {}: \
             every batch must match the schema the scan declared",
            got.fields().len(),
            expected.fields().len()
        )));
    }
    for (i, (want, have)) in expected.fields().iter().zip(got.fields()).enumerate() {
        if want.name() != have.name() || want.data_type() != have.data_type() {
            return Err(RpcError::type_error(format!(
                "function `{function}` emitted a batch that does not match the schema its bind \
                 resolved: column {i} arrived as `{}: {}` but the schema declares `{}: {}`",
                have.name(),
                have.data_type(),
                want.name(),
                want.data_type()
            )));
        }
    }
    Ok(())
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
            function_name: spec.function_name.clone(),
            bind_call,
            response,
            output_schema,
        })
    }

    /// Open a scan over a bound function.
    pub fn scan<'a>(&'a mut self, bound: &BoundFunction, opts: &ScanOptions) -> Result<Scan<'a>> {
        // Two schemas, and the difference between them is the filter-only
        // columns: the worker emits the union, the caller sees the projection.
        let schema = project_schema(bound.output_schema(), opts.projection.as_deref())?;
        let emitted = project_schema(bound.output_schema(), opts.wire_projection().as_deref())?;

        let request = bound.init_request(opts, None, None);
        let params = wire::to_batch(p::InitParams {
            request: envelope(request)?,
        })?;
        Scan::open_projected(
            self.transport_mut(),
            &params,
            bound.function_name(),
            schema,
            emitted,
        )
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
    /// Where this split's range in the DATA begins. Its presence is what makes
    /// an unbounded split recognizable: `end_position: None` alone cannot say,
    /// because it is also the default for every ordinary batch split.
    pub start_position: Option<Vec<u8>>,
    /// The upper bound of this split's range. `None` alongside a `start_position`
    /// means UNBOUNDED — a shard read forever — and an engine whose tasks must
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
    /// Filter-only columns, exactly as on [`ScanOptions::filter_columns`]. A
    /// plan carries the same filters as the scan it sizes, so it must key them
    /// against the same projected set or the worker prunes on the wrong column.
    pub filter_columns: Option<Vec<i64>>,
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

impl PlanOptions {
    /// The projection this plan puts on the wire. See [`union_projection`].
    pub fn wire_projection(&self) -> Option<Vec<i64>> {
        union_projection(self.projection.as_ref(), self.filter_columns.as_ref())
    }
}

impl VgiClient {
    /// Divide a bound scan into named, independently redeemable splits.
    ///
    /// Paginated: the worker may return a cursor, and this follows it until the
    /// enumeration is exhausted. Three caps bound that — pages, accumulated
    /// splits, and wall clock — because a worker that paginates forever, or
    /// paginates faster than it converges, would otherwise hang the caller
    /// while the vector grows.
    ///
    /// Each cap ERRORS. Stopping early and scanning what arrived is the
    /// tempting mitigation and it is the wrong one: it turns a hang into a
    /// SILENT SUBSET — a correct-looking answer missing rows, which is exactly
    /// the failure class splits exist to prevent — so a breach names the cap it
    /// broke and refuses. The time cap is here rather than an interrupt flag
    /// because this client has no cancellation channel to poll; the DuckDB
    /// extension polls its query's `interrupted` instead and is otherwise
    /// identical.
    ///
    /// Duplicate tokens are NOT dropped. Keeping the enumeration disjoint is
    /// the worker's obligation, and a client-side dedup could not discharge it:
    /// it costs a set holding a copy of every token on every scan, it compares
    /// token bytes and so never fires against a keyed worker (which seals each
    /// mint under a fresh nonce), and the most a client could do with a
    /// duplicate is refuse anyway. Should a stable split identity ever reach
    /// the wire, enforcing it becomes cheap and uniform and is worth
    /// revisiting.
    pub fn plan(&mut self, bound: &BoundFunction, opts: &PlanOptions) -> Result<ScanPlan> {
        /// Generous on purpose: the cap exists to turn a hang into a legible
        /// error, not to second-guess a worker's split count. Bounds PAGES, so
        /// the reachable split count is this times the worker's page size.
        const MAX_PAGES: usize = 1024;
        /// The one cap on what is actually held in memory, since a page count
        /// says nothing about page size. Matches the DuckDB extension's.
        const MAX_SPLITS: usize = 1_048_576;
        /// A worker that keeps returning small pages breaches neither of the
        /// other caps quickly, so planning also has to end in bounded time.
        const MAX_ELAPSED: std::time::Duration = std::time::Duration::from_secs(300);

        let started = std::time::Instant::now();

        let mut plan = ScanPlan {
            scope: "catalog".to_string(),
            ..Default::default()
        };
        let mut cursor: Option<Bytes> = None;
        let mut pages = 0usize;

        loop {
            if pages >= MAX_PAGES {
                return Err(RpcError::runtime_error(format!(
                    "function `{}`: worker exceeded the scan-planning page cap ({MAX_PAGES} \
                     pages) without exhausting its cursor; refusing to scan a partial split \
                     enumeration",
                    bound.function_name()
                )));
            }
            if started.elapsed() >= MAX_ELAPSED {
                return Err(RpcError::runtime_error(format!(
                    "function `{}`: scan planning ran past its {}s budget after {pages} page(s) \
                     without exhausting its cursor; refusing to scan a partial split enumeration",
                    bound.function_name(),
                    MAX_ELAPSED.as_secs()
                )));
            }
            pages += 1;

            let request = TableFunctionPlanRequest {
                bind_call: bound.bind_call.clone(),
                bind_opaque_data: Some(bound.response.opaque_data.clone()),
                projection_ids: opts.wire_projection(),
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
                plan.splits.push(ScanSplitInfo {
                    token: split.token.0.clone(),
                    estimated_rows: split.estimated_rows,
                    rows_exact: split.rows_exact,
                    estimated_bytes: split.estimated_bytes,
                    start_position: split.start_position.map(|b| b.0),
                    end_position: split.end_position.map(|b| b.0),
                });
            }
            if plan.splits.len() > MAX_SPLITS {
                return Err(RpcError::runtime_error(format!(
                    "function `{}`: worker returned more than {MAX_SPLITS} splits for one scan; \
                     refusing to buffer an unbounded split vector",
                    bound.function_name()
                )));
            }

            // Plan-level facts come from the FIRST page, keyed on the page
            // counter — one rule for all of them. `scope` used to be
            // last-non-empty-wins, and since it defaults to "catalog" it is never
            // empty: a page-1 "transaction" scope was overwritten by every
            // subsequent page's default, recording a transaction-scoped plan
            // (not cacheable, not redeemable after commit) as catalog-scoped.
            if pages == 1 {
                plan.max_workers = response.max_workers;
                plan.estimated_total_splits = response.estimated_total_splits;
                plan.estimated_total_rows = response.estimated_total_rows;
                plan.estimated_total_bytes = response.estimated_total_bytes;
                plan.catalog_version = response.catalog_version;
                if !response.scope.is_empty() {
                    plan.scope = response.scope.clone();
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    fn schema(fields: &[(&str, DataType)]) -> Schema {
        Schema::new(
            fields
                .iter()
                .map(|(n, t)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn a_conforming_batch_passes() {
        let s = schema(&[("n", DataType::Int64)]);
        let b = RecordBatch::try_new(
            Arc::new(s.clone()),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(check_batch_schema("f", &s, &b).is_ok());
    }

    #[test]
    fn a_drifted_type_names_the_function_the_column_and_both_types() {
        let declared = schema(&[("n", DataType::Int64)]);
        let emitted = schema(&[("n", DataType::Utf8)]);
        let b = RecordBatch::try_new(
            Arc::new(emitted),
            vec![Arc::new(StringArray::from(vec!["1"]))],
        )
        .unwrap();
        let e = check_batch_schema("rowid_sequence", &declared, &b).unwrap_err();
        assert!(e.message.contains("rowid_sequence"), "{}", e.message);
        assert!(e.message.contains("column 0"), "{}", e.message);
        assert!(e.message.contains("Utf8"), "{}", e.message);
        assert!(e.message.contains("Int64"), "{}", e.message);
    }

    #[test]
    fn a_renamed_column_is_refused() {
        let declared = schema(&[("n", DataType::Int64)]);
        let emitted = schema(&[("m", DataType::Int64)]);
        let b = RecordBatch::try_new(Arc::new(emitted), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        assert!(check_batch_schema("f", &declared, &b).is_err());
    }

    #[test]
    fn a_column_count_mismatch_is_refused() {
        let declared = schema(&[("n", DataType::Int64), ("s", DataType::Utf8)]);
        let emitted = schema(&[("n", DataType::Int64)]);
        let b = RecordBatch::try_new(Arc::new(emitted), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        let e = check_batch_schema("f", &declared, &b).unwrap_err();
        assert!(e.message.contains("1 column(s)"), "{}", e.message);
        assert!(e.message.contains("resolved 2"), "{}", e.message);
    }

    #[test]
    fn nullability_alone_is_not_drift() {
        // The transport promotes response schemas to fully-nullable, so a
        // difference here is this client's own doing; rejecting on it would
        // fail correct scans.
        let declared = Schema::new(vec![Field::new("n", DataType::Int64, false)]);
        let emitted = Schema::new(vec![Field::new("n", DataType::Int64, true)]);
        let b = RecordBatch::try_new(Arc::new(emitted), vec![Arc::new(Int64Array::from(vec![1]))])
            .unwrap();
        assert!(check_batch_schema("f", &declared, &b).is_ok());
    }

    // --- projection ∪ filter columns ------------------------------------

    /// A canned producer stream: one header, then the batches, then EOS.
    struct StubStream {
        header: RecordBatch,
        batches: std::collections::VecDeque<RecordBatch>,
    }

    impl ProducerStream for StubStream {
        fn header(&self) -> Option<&RecordBatch> {
            Some(&self.header)
        }
        fn tick(&mut self) -> Result<Option<(RecordBatch, vgi_rpc::wire::Metadata)>> {
            Ok(self
                .batches
                .pop_front()
                .map(|b| (b, vgi_rpc::wire::Metadata::default())))
        }
        fn cancel(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// A transport that answers `init` from a script and records what it was
    /// asked for, so a test can assert on the request as well as the answer.
    struct StubTransport {
        batches: Vec<RecordBatch>,
        seen: Arc<std::sync::Mutex<Option<InitRequest>>>,
    }

    impl crate::transport::VgiTransport for StubTransport {
        fn call_unary(&mut self, method: &str, _params: &RecordBatch) -> Result<RecordBatch> {
            Err(RpcError::runtime_error(format!("unexpected call {method}")))
        }
        fn open_producer<'a>(
            &'a mut self,
            _method: &str,
            params: &RecordBatch,
            _has_header: bool,
        ) -> Result<Box<dyn ProducerStream + 'a>> {
            let p: p::InitParams = wire::from_batch(params)?;
            let request: InitRequest = wire::from_batch(&ipc::read_batch(&p.request.0)?)?;
            *self.seen.lock().unwrap() = Some(request);
            let header = wire::to_batch(GlobalInitResponse {
                execution_id: Bytes(b"stub".to_vec()),
                max_workers: 1,
                opaque_data: None,
            })?;
            Ok(Box::new(StubStream {
                header,
                batches: self.batches.clone().into(),
            }))
        }
        fn open_exchange<'a>(
            &'a mut self,
            _method: &str,
            _params: &RecordBatch,
            _has_header: bool,
        ) -> Result<Box<dyn crate::transport::ExchangeStream + 'a>> {
            Err(RpcError::runtime_error("no exchange here"))
        }
        fn label(&self) -> &str {
            "stub"
        }
    }

    /// The bind schema the stub scans stand on: `a`, `b`, `c`.
    fn bound_stub() -> BoundFunction {
        let out: SchemaRef = Arc::new(schema(&[
            ("a", DataType::Int64),
            ("b", DataType::Int64),
            ("c", DataType::Utf8),
        ]));
        BoundFunction::from_parts(
            "stub_fn",
            Bytes(Vec::new()),
            vgi_protocol::protocol::dtos::BindResponse {
                output_schema: Bytes(ipc::write_schema(&out).unwrap()),
                opaque_data: Bytes(Vec::new()),
                lookup_secret_types: Vec::new(),
                lookup_scopes: Vec::new(),
                lookup_names: Vec::new(),
            },
            out,
        )
    }

    #[test]
    fn filter_only_columns_are_appended_after_the_projection() {
        let opts = ScanOptions {
            projection: Some(vec![2]),
            filter_columns: Some(vec![0, 2]),
            ..ScanOptions::default()
        };
        // `2` is already projected, so it is not requested twice; `0` lands
        // after the projection, never before it.
        assert_eq!(opts.wire_projection(), Some(vec![2, 0]));
        assert_eq!(opts.wire_index_of(2), Some(0));
        assert_eq!(opts.wire_index_of(0), Some(1));
        assert_eq!(opts.wire_index_of(1), None);
    }

    #[test]
    fn an_unprojected_scan_is_left_alone() {
        // Emitting everything already includes the filter's columns; building a
        // list here would NARROW the scan to them.
        let opts = ScanOptions {
            projection: None,
            filter_columns: Some(vec![0]),
            ..ScanOptions::default()
        };
        assert_eq!(opts.wire_projection(), None);
        assert_eq!(opts.wire_index_of(0), None);
    }

    #[test]
    fn a_filter_only_column_is_requested_and_then_trimmed_off() {
        let seen = Arc::new(std::sync::Mutex::new(None));
        // The worker answers in the order it was asked: `c`, then `a`.
        let emitted: SchemaRef = Arc::new(schema(&[("c", DataType::Utf8), ("a", DataType::Int64)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&emitted),
            vec![
                Arc::new(StringArray::from(vec!["kept"])),
                Arc::new(Int64Array::from(vec![7])),
            ],
        )
        .unwrap();
        let mut client = crate::VgiClient::new(Box::new(StubTransport {
            batches: vec![batch],
            seen: Arc::clone(&seen),
        }));

        let bound = bound_stub();
        let opts = ScanOptions {
            projection: Some(vec![2]),
            filter_columns: Some(vec![0]),
            ..ScanOptions::default()
        };
        let mut scan = client.scan(&bound, &opts).expect("scan");

        // The init asked for the union, projection first.
        let request = seen.lock().unwrap().clone().expect("init request");
        assert_eq!(request.projection_ids, Some(vec![2, 0]));

        // The caller's schema is the projection alone: the filter column is
        // this client's business, not the engine's.
        assert_eq!(scan.schema().fields().len(), 1);
        assert_eq!(scan.schema().field(0).name(), "c");

        let out = scan.next_batch().expect("batch").expect("one batch");
        assert_eq!(out.num_columns(), 1);
        // The VALUE, not just the type: an order slip that put `a` first would
        // trim `c` away and leave the filter column in its slot — and had both
        // columns been Int64, nothing but the value would have caught it.
        assert_eq!(
            out.column(0).as_string::<i32>().value(0),
            "kept",
            "trimming kept the wrong column"
        );
    }

    #[test]
    fn a_plain_projection_still_emits_exactly_what_it_asked_for() {
        let seen = Arc::new(std::sync::Mutex::new(None));
        let emitted: SchemaRef = Arc::new(schema(&[("c", DataType::Utf8)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&emitted),
            vec![Arc::new(StringArray::from(vec!["only"]))],
        )
        .unwrap();
        let mut client = crate::VgiClient::new(Box::new(StubTransport {
            batches: vec![batch],
            seen: Arc::clone(&seen),
        }));

        let bound = bound_stub();
        let opts = ScanOptions {
            projection: Some(vec![2]),
            ..ScanOptions::default()
        };
        let mut scan = client.scan(&bound, &opts).expect("scan");
        let request = seen.lock().unwrap().clone().expect("init request");
        assert_eq!(request.projection_ids, Some(vec![2]));
        let out = scan.next_batch().expect("batch").expect("one batch");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.column(0).as_string::<i32>().value(0), "only");
    }

    // --- bounded split enumeration ---------------------------------------

    /// A worker that always hands back another cursor, and never a last page.
    struct EndlessPlanTransport {
        pages: std::cell::Cell<usize>,
        splits_per_page: usize,
    }

    impl EndlessPlanTransport {
        fn plan_response(&self) -> Result<RecordBatch> {
            use arrow_array::BinaryArray;
            let split = wire::to_batch(vgi_protocol::protocol::dtos::ScanSplit {
                payload: Bytes(b"p".to_vec()),
                token: Bytes(b"t".to_vec()),
                estimated_rows: None,
                rows_exact: false,
                estimated_bytes: None,
                partition_bounds: None,
                column_statistics: None,
                location_ids: None,
                start_position: None,
                end_position: None,
            })?;
            let split = Bytes(ipc::write_batch(&split)?);
            let response = vgi_protocol::protocol::dtos::PlanResponse {
                splits: vec![split; self.splits_per_page],
                // Never exhausted: the failure this test is about.
                next_cursors: vec![Bytes(b"more".to_vec())],
                execution_id: None,
                init_opaque_data: Bytes(Vec::new()),
                max_workers: Some(4),
                estimated_total_splits: None,
                estimated_total_rows: None,
                estimated_total_bytes: None,
                catalog_version: None,
                scope: "catalog".to_string(),
                locations: None,
                partitioning: Vec::new(),
                sort_order: Vec::new(),
                cache_max_age_seconds: None,
                start_position: Bytes(Vec::new()),
                end_position: Bytes(Vec::new()),
            };
            let inner = ipc::write_batch(&wire::to_batch(response)?)?;
            let field = Field::new("result", DataType::Binary, false);
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![Arc::new(BinaryArray::from(vec![inner.as_slice()]))],
            )
            .map_err(|e| RpcError::runtime_error(e.to_string()))
        }
    }

    impl crate::transport::VgiTransport for EndlessPlanTransport {
        fn call_unary(&mut self, method: &str, _params: &RecordBatch) -> Result<RecordBatch> {
            assert_eq!(method, "table_function_plan");
            self.pages.set(self.pages.get() + 1);
            self.plan_response()
        }
        fn open_producer<'a>(
            &'a mut self,
            _method: &str,
            _params: &RecordBatch,
            _has_header: bool,
        ) -> Result<Box<dyn ProducerStream + 'a>> {
            Err(RpcError::runtime_error("no scan here"))
        }
        fn open_exchange<'a>(
            &'a mut self,
            _method: &str,
            _params: &RecordBatch,
            _has_header: bool,
        ) -> Result<Box<dyn crate::transport::ExchangeStream + 'a>> {
            Err(RpcError::runtime_error("no exchange here"))
        }
        fn label(&self) -> &str {
            "endless"
        }
    }

    #[test]
    fn an_unexhausted_cursor_breaches_the_page_cap_and_says_so() {
        let mut client = crate::VgiClient::new(Box::new(EndlessPlanTransport {
            pages: std::cell::Cell::new(0),
            splits_per_page: 1,
        }));
        let e = client
            .plan(&bound_stub(), &PlanOptions::default())
            .unwrap_err();
        // Naming the cap is the point: it is the difference between "raise it"
        // and "fix the worker".
        assert!(e.message.contains("page cap (1024"), "{}", e.message);
        assert!(e.message.contains("stub_fn"), "{}", e.message);
        assert!(
            e.message.contains("partial split enumeration"),
            "{}",
            e.message
        );
    }
}

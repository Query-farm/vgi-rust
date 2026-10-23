// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Continued-output table-in-out fixtures: one input batch answered across
//! several replies, the way a function backed by a paged API would.
//!
//! `process_out` emits the first page with `EmitOptions::more` and records the
//! resume position with `set_resume_state`; the framework then calls `resume`
//! for each continuation tick until a page goes out without `more`. The state
//! holds only the input's counts and a position in them, never output rows, so
//! it stays small in an HTTP state token. Fixture contracts (which every SDK's
//! worker implements exactly so) are in the headers of the extension's
//! `test/sql/integration/table_in_out/lateral_continuation.test`,
//! `input_rows_per_call.test`, `continuation_classic.test` and
//! `continuation_resume_error_revalidate.test`. Mirrors the Python SDK's
//! `BlendedPagedFunction` / `BlendedSlicedFunction` / `PagedExplodeFunction`.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use vgi::cache_control::CacheControl;
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::table_in_out::{decode_resume_state, EmitOptions, TableInOutFunction, TableInOutOutput};
use vgi_rpc::{Result, RpcError};

use super::blended::{i64_col, named_col};

/// Register the continued-output fixtures.
pub fn register(w: &mut vgi::Worker) {
    w.register_table_in_out(BlendedPagedFunction);
    w.register_table_in_out(BlendedSlicedFunction);
    w.register_table_in_out(PagedExplodeFunction);
}

/// The most input rows `blended_sliced` takes per call.
const SLICED_ROWS_PER_CALL: i64 = 3;

/// Where the next reply resumes: the input's fan-out counts and a position in
/// them.
#[derive(Serialize, Deserialize, Default)]
struct PagedState {
    counts: Vec<i64>,
    row: usize,
    offset: i64,
    /// `blended_sliced`: the input rows of the current call.
    call_rows: i64,
    /// Replies sent for the current input.
    page: i64,
    /// `not_modified_more`: the first reply is empty and says more.
    empty_first: bool,
    /// Added to every output value (revalidate: the data changed); negative
    /// means the whole output goes in one reply.
    bump: i64,
}

/// The named, bind-time arguments shared by the fixtures.
struct PagedArgs {
    page_size: i64,
    cache_ttl: i64,
    fail_on_page: i64,
    revalidate: bool,
    not_modified_more: bool,
}

impl PagedArgs {
    fn read(args: &vgi::arguments::Arguments, default_page_size: i64) -> Self {
        PagedArgs {
            page_size: args.named_i64("page_size").unwrap_or(default_page_size),
            cache_ttl: args.named_i64("cache_ttl").unwrap_or(0),
            fail_on_page: args.named_i64("fail_on_page").unwrap_or(0),
            revalidate: args.named_bool("revalidate").unwrap_or(false),
            not_modified_more: args.named_bool("not_modified_more").unwrap_or(false),
        }
    }

    /// The bind-time checks every fixture contract shares.
    fn validate(&self) -> Result<()> {
        if self.page_size < 1 {
            return Err(RpcError::value_error(format!(
                "page_size must be at least 1, got {}",
                self.page_size
            )));
        }
        if self.cache_ttl < 0 {
            return Err(RpcError::value_error(format!(
                "cache_ttl must not be negative, got {}",
                self.cache_ttl
            )));
        }
        Ok(())
    }

    /// `vgi.cache.ttl` for an input's first reply, when `cache_ttl` asks.
    fn ttl_cache_control(&self) -> Option<CacheControl> {
        (self.cache_ttl > 0).then(|| CacheControl::ttl(self.cache_ttl))
    }
}

/// The named arguments of `blended_paged` / `paged_explode`.
fn paged_named_specs() -> Vec<ArgSpec> {
    vec![
        ArgSpec::const_arg("page_size", -1, "int64", "Output rows per reply").with_default(2),
        ArgSpec::const_arg(
            "cache_ttl",
            -1,
            "int64",
            "When > 0, advertise vgi.cache.ttl on the first reply",
        )
        .with_default(0),
        ArgSpec::const_arg(
            "fail_on_page",
            -1,
            "int64",
            "When > 0, fail instead of sending that (1-based) reply",
        )
        .with_default(0),
        ArgSpec::const_arg(
            "revalidate",
            -1,
            "boolean",
            "Answer conditional requests with changed, paged output",
        )
        .with_default(false),
        ArgSpec::const_arg(
            "not_modified_more",
            -1,
            "boolean",
            "With revalidate, answer a conditional request with an empty not_modified reply that says more",
        )
        .with_default(false),
    ]
}

fn counts_of(col: &ArrayRef) -> Result<Vec<i64>> {
    let col = i64_col(col)?;
    Ok((0..col.len())
        .map(|i| {
            if col.is_valid(i) {
                col.value(i).max(0)
            } else {
                0
            }
        })
        .collect())
}

fn i_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("i", DataType::Int64, true)]))
}

/// Start a new input for `blended_paged` / `paged_explode`; return its first
/// reply's cache control.
///
/// `revalidate` makes the output depend on the request alone. A plain request
/// gets the whole output in one reply, cacheable but immediately stale
/// (`ttl = 0`, etag `v1`, revalidatable), so the client stores it and
/// revalidates every repeat. A conditional request (`if_none_match`) is told
/// the data changed: every value is 1000 larger, the etag is `v2`, and the
/// output is paged by `page_size`, so the client must return the new pages and
/// store nothing. `not_modified_more` breaks the protocol on purpose: it
/// answers a conditional request with an empty `not_modified` reply that also
/// says more, which a client must refuse.
fn paged_begin(
    params: &ProcessParams,
    args: &PagedArgs,
    state: &mut PagedState,
    counts: Vec<i64>,
) -> Option<CacheControl> {
    *state = PagedState {
        counts,
        ..Default::default()
    };
    if !args.revalidate {
        return args.ttl_cache_control();
    }
    let stale = CacheControl::ttl(0).with_revalidatable();
    if params.if_none_match.is_none() {
        state.bump = -1; // one reply: the whole output
        return Some(stale.with_etag("v1"));
    }
    state.bump = 1000;
    if args.not_modified_more {
        state.empty_first = true;
        let mut cc = stale.with_etag("v1");
        cc.not_modified = true;
        return Some(cc);
    }
    Some(stale.with_etag("v2"))
}

/// The next reply of `blended_paged` / `paged_explode`, or the failure
/// `fail_on_page` asks for.
fn paged_next(args: &PagedArgs, state: &mut PagedState) -> Result<(Vec<i64>, Vec<i32>, bool)> {
    state.page += 1;
    if state.page == args.fail_on_page {
        return Err(RpcError::runtime_error(format!(
            "failing on page {} as asked",
            state.page
        )));
    }
    if state.page == 1 && state.empty_first {
        return Ok((Vec::new(), Vec::new(), true));
    }
    if state.bump < 0 {
        return Ok(next_page(state, i64::MAX));
    }
    let (vals, parents, more) = next_page(state, args.page_size);
    let bump = state.bump;
    Ok((vals.into_iter().map(|v| v + bump).collect(), parents, more))
}

/// The next `page_size` rows of the output (`0..n-1` for each input row in
/// order, so a page may span input rows), and whether more follow. Input rows
/// with no output after the last non-empty page are skipped, so the last page
/// is never followed by an empty one.
fn next_page(state: &mut PagedState, page_size: i64) -> (Vec<i64>, Vec<i32>, bool) {
    let mut vals = Vec::new();
    let mut parents = Vec::new();
    while state.row < state.counts.len() && (vals.len() as i64) < page_size {
        let n = state.counts[state.row];
        let take = (n - state.offset).min(page_size - vals.len() as i64);
        vals.extend(state.offset..state.offset + take);
        parents.extend(std::iter::repeat_n(state.row as i32, take as usize));
        state.offset += take;
        if state.offset == n {
            state.row += 1;
            state.offset = 0;
        }
    }
    while state.row < state.counts.len() && state.counts[state.row] == 0 {
        state.row += 1;
    }
    let more = state.row < state.counts.len();
    (vals, parents, more)
}

/// Emit one page: `more` (with the advanced position) unless it is the last.
fn emit_page(
    out: &mut TableInOutOutput,
    state: &PagedState,
    batch: RecordBatch,
    parent_rows: Option<Vec<i32>>,
    cache_control: Option<CacheControl>,
    more: bool,
) -> Result<()> {
    if more {
        out.set_resume_state(state)?;
    }
    out.emit_with(
        batch,
        EmitOptions {
            parent_rows,
            cache_control,
            more,
            ..Default::default()
        },
    )
}

fn i_batch(params: &ProcessParams, vals: Vec<i64>) -> Result<RecordBatch> {
    RecordBatch::try_new(
        params.output_schema.clone(),
        vec![Arc::new(Int64Array::from(vals)) as ArrayRef],
    )
    .map_err(|e| RpcError::runtime_error(e.to_string()))
}

/// `blended_paged(n, page_size := 2, ...)` — `blended_explode`'s output (rows
/// `0..n-1` per input row, with `parent_rows` provenance), `page_size` rows
/// per reply.
pub struct BlendedPagedFunction;

impl BlendedPagedFunction {
    fn emit(
        &self,
        params: &ProcessParams,
        mut state: PagedState,
        out: &mut TableInOutOutput,
        cache_control: Option<CacheControl>,
    ) -> Result<()> {
        let args = PagedArgs::read(&params.arguments, 2);
        let (vals, parents, more) = paged_next(&args, &mut state)?;
        let batch = i_batch(params, vals)?;
        emit_page(out, &state, batch, Some(parents), cache_control, more)
    }
}

impl TableInOutFunction for BlendedPagedFunction {
    fn name(&self) -> &str {
        "blended_paged"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description:
                "Blended 1->N fan-out (emit 0..n-1 per input row), page_size rows per reply"
                    .to_string(),
            categories: vec!["blended".into(), "test".into()],
            input_from_args: true,
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        let mut specs = vec![ArgSpec::column(
            "n",
            0,
            "int64",
            "Fan-out count: emit rows 0..n-1 for this input row",
        )];
        specs.extend(paged_named_specs());
        specs
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        PagedArgs::read(&params.arguments, 2).validate()?;
        Ok(BindResponse {
            output_schema: i_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        let args = PagedArgs::read(&params.arguments, 2);
        let mut state = PagedState::default();
        let counts = counts_of(named_col(batch, "n", 0)?)?;
        let cc = paged_begin(params, &args, &mut state, counts);
        self.emit(params, state, out, cc)
    }
    fn resume(
        &self,
        params: &ProcessParams,
        state: &[u8],
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        self.emit(params, decode_resume_state(state)?, out, None)
    }
}

/// `blended_sliced(n, page_size := 1000000, cache_ttl := 0)` — `blended_paged`
/// declaring `input_rows_per_call = 3`, reporting in `call_rows` the row count
/// of the call that produced each output row, so a test can see the client's
/// split. With `cache_ttl > 0` each call's first reply is per-value cacheable.
pub struct BlendedSlicedFunction;

impl BlendedSlicedFunction {
    fn emit(
        &self,
        params: &ProcessParams,
        mut state: PagedState,
        out: &mut TableInOutOutput,
        cache_control: Option<CacheControl>,
    ) -> Result<()> {
        let args = PagedArgs::read(&params.arguments, 1_000_000);
        let (vals, parents, more) = next_page(&mut state, args.page_size);
        let call_rows = vec![state.call_rows; vals.len()];
        let batch = RecordBatch::try_new(
            params.output_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vals)) as ArrayRef,
                Arc::new(Int64Array::from(call_rows)) as ArrayRef,
            ],
        )
        .map_err(|e| RpcError::runtime_error(e.to_string()))?;
        emit_page(out, &state, batch, Some(parents), cache_control, more)
    }
}

impl TableInOutFunction for BlendedSlicedFunction {
    fn name(&self) -> &str {
        "blended_sliced"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description:
                "Blended 1->N fan-out taking at most 3 input rows per call, reporting each call's size"
                    .to_string(),
            categories: vec!["blended".into(), "test".into()],
            input_from_args: true,
            input_rows_per_call: Some(SLICED_ROWS_PER_CALL),
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::column(
                "n",
                0,
                "int64",
                "Fan-out count: emit rows 0..n-1 for this input row",
            ),
            ArgSpec::const_arg("page_size", -1, "int64", "Output rows per reply")
                .with_default(1_000_000),
            ArgSpec::const_arg(
                "cache_ttl",
                -1,
                "int64",
                "If > 0, per-value cacheable for this many seconds",
            )
            .with_default(0),
        ]
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        PagedArgs::read(&params.arguments, 1_000_000).validate()?;
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![
                Field::new("i", DataType::Int64, true),
                Field::new("call_rows", DataType::Int64, true),
            ])),
            opaque_data: Vec::new(),
        })
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        if batch.num_rows() as i64 > SLICED_ROWS_PER_CALL {
            return Err(RpcError::runtime_error(format!(
                "blended_sliced declares input_rows_per_call = {SLICED_ROWS_PER_CALL} but got {} rows",
                batch.num_rows()
            )));
        }
        let state = PagedState {
            counts: counts_of(named_col(batch, "n", 0)?)?,
            call_rows: batch.num_rows() as i64,
            ..Default::default()
        };
        let ttl = PagedArgs::read(&params.arguments, 1_000_000).cache_ttl;
        let cc = (ttl > 0).then(|| CacheControl::ttl(ttl).with_per_value());
        self.emit(params, state, out, cc)
    }
    fn resume(
        &self,
        params: &ProcessParams,
        state: &[u8],
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        self.emit(params, decode_resume_state(state)?, out, None)
    }
}

/// `paged_explode(data TABLE, page_size := 2, ...)` — the classic (TABLE-input)
/// counterpart of `blended_paged`: for each input row with count `n` (the
/// table's first column), rows `i = 0..n-1`, `page_size` per reply. It runs
/// through the extension's stock in-out path on a per-substream connection.
pub struct PagedExplodeFunction;

impl PagedExplodeFunction {
    fn emit(
        &self,
        params: &ProcessParams,
        mut state: PagedState,
        out: &mut TableInOutOutput,
        cache_control: Option<CacheControl>,
    ) -> Result<()> {
        let args = PagedArgs::read(&params.arguments, 2);
        let (vals, _parents, more) = paged_next(&args, &mut state)?;
        let batch = i_batch(params, vals)?;
        emit_page(out, &state, batch, None, cache_control, more)
    }
}

impl TableInOutFunction for PagedExplodeFunction {
    fn name(&self) -> &str {
        "paged_explode"
    }
    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Classic table-in-out 1->N fan-out, page_size rows per reply".to_string(),
            categories: vec!["table_in_out".into(), "test".into()],
            ..Default::default()
        }
    }
    fn argument_specs(&self) -> Vec<ArgSpec> {
        let mut specs = vec![ArgSpec::column(
            "data",
            0,
            "table",
            "Input table; its first column is the fan-out count",
        )];
        specs.extend(paged_named_specs());
        specs
    }
    fn on_bind(&self, params: &BindParams) -> Result<BindResponse> {
        PagedArgs::read(&params.arguments, 2).validate()?;
        Ok(BindResponse {
            output_schema: i_schema(),
            opaque_data: Vec::new(),
        })
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn process_out(
        &self,
        params: &ProcessParams,
        batch: &RecordBatch,
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        let args = PagedArgs::read(&params.arguments, 2);
        let counts = match batch.columns().first() {
            Some(col) => counts_of(col)?,
            None => Vec::new(),
        };
        let mut state = PagedState::default();
        let cc = paged_begin(params, &args, &mut state, counts);
        self.emit(params, state, out, cc)
    }
    fn resume(
        &self,
        params: &ProcessParams,
        state: &[u8],
        out: &mut TableInOutOutput,
    ) -> Result<()> {
        self.emit(params, decode_resume_state(state)?, out, None)
    }
}

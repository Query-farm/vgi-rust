// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Split-capable table generators, the Rust half of the cross-SDK splits suite.
//!
//! Every fixture here is a TWIN of something already in the suite:
//! `split_sequence(n)` must return row-for-row what `sequence(n)` returns,
//! because that equivalence is the baseline every other split test rests on.
//! If the twins ever disagree, nothing else in the suite means anything.
//!
//! The shapes cover the ways a split scan goes WRONG rather than the ways it
//! goes right: zero splits (legal, must be an empty result), zero-ROW splits
//! (the likelier shape — a filter pruned one — and the one that silently
//! truncates a scan if a reader treats an empty split as EOS), skew (so greedy
//! claiming is distinguishable from static assignment), and far more splits than
//! reader threads (which forces sequential re-init on a reused connection).

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use vgi::function::{ArgSpec, BindParams, BindResponse, FunctionMetadata, ProcessParams};
use vgi::protocol::dtos::{PartitionTransform, SortField, TableFunctionPlanRequest};
use vgi::split_token::{PlanOutcome, PlannedSplit};
use vgi::statistics::{CatColStat, StatValue};
use vgi::table_function::{resume, TableFunction, TableProducer};
use vgi_rpc::{DictString, Result, RpcError};

pub fn register(w: &mut vgi::Worker) {
    w.register_table(SplitScan::new(
        "split_sequence",
        "Split-capable twin of sequence(n): 0..n-1 divided into `splits` ranges",
        Shape::Even,
    ));
    w.register_table(SplitScan::new(
        "split_zero",
        "Returns zero splits: a legal empty result, not an error",
        Shape::NoSplits,
    ));
    w.register_table(SplitScan::new(
        "split_empty_ranges",
        "Some splits yield zero rows; the scan must not end early",
        Shape::EmptyInterleaved,
    ));
    w.register_table(SplitScan::new(
        "split_skewed",
        "One split ~100x the others: exercises greedy claiming under skew",
        Shape::Skewed,
    ));
    w.register_table(SplitFailAt);
    w.register_table(SplitEndlessCursor);
    w.register_table(SplitEchoFilters);
    w.register_table(SplitPaginated);
    w.register_table(SplitStalePlan);
    w.register_table(SplitShortTtl);
    w.register_table(SplitBatchIndex);
    w.register_table(SplitCacheable);
    w.register_table(SplitPartitioned);
    w.register_table(SplitDynamicFilter);
    w.register_table(SplitScan::new(
        "split_many",
        "Far more splits than threads: exercises greedy claiming and re-init",
        Shape::Many,
    ));
}

/// How a fixture divides `[0, n)`. Each shape targets a specific failure mode.
#[derive(Clone, Copy)]
enum Shape {
    Even,
    NoSplits,
    EmptyInterleaved,
    Skewed,
    Many,
}

/// The half-open range `[lo, hi)` one split owns.
///
/// This NAMES the work rather than describing it: a redemption reads the same
/// rows however many times it runs and whichever process runs it, which is
/// exactly what a retrying engine requires.
#[derive(Clone, Copy)]
struct Range {
    lo: i64,
    hi: i64,
}

fn encode(r: Range) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&r.lo.to_le_bytes());
    out.extend_from_slice(&r.hi.to_le_bytes());
    out
}

fn decode(payload: &[u8]) -> Result<Range> {
    if payload.len() != 16 {
        return Err(RpcError::runtime_error(format!(
            "split payload must be 16 bytes, got {}",
            payload.len()
        )));
    }
    let lo = i64::from_le_bytes(payload[0..8].try_into().unwrap());
    let hi = i64::from_le_bytes(payload[8..16].try_into().unwrap());
    Ok(Range { lo, hi })
}

/// Divide `[0, n)` into `k` contiguous ranges, remainder over the first few.
fn even_ranges(n: i64, k: i64) -> Vec<Range> {
    if k <= 0 {
        return Vec::new();
    }
    let n = n.max(0);
    let (base, extra) = (n / k, n % k);
    let mut out = Vec::with_capacity(k as usize);
    let mut lo = 0;
    for i in 0..k {
        let hi = lo + base + i64::from(i < extra);
        out.push(Range { lo, hi });
        lo = hi;
    }
    out
}

/// The one-column schema every sequence-shaped split fixture returns.
fn seq_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
}

/// The two named arguments the shared SQL suite binds by name across SDKs.
fn split_args() -> Vec<ArgSpec> {
    vec![
        ArgSpec::const_arg("n", -1, "int64", "Number of rows to produce").with_ge(0.0),
        ArgSpec::const_arg(
            "splits",
            -1,
            "int64",
            "How many splits to divide the scan into",
        )
        .with_ge(0.0)
        .with_default(4),
    ]
}

/// Decode this reader's claimed ranges and walk them, the shape every
/// sequence-shaped fixture shares.
///
/// Absent payloads mean the client stopped planning (`vgi_split_scans` off). A
/// split-only function has no way to know what to read then, and failing is the
/// point: quietly returning zero rows would be A DIFFERENT ANSWER to the same
/// query, which is worse than an error.
fn seq_producer(name: &str, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
    let Some(payloads) = params.split_payloads.as_ref() else {
        return Err(RpcError::runtime_error(format!(
            "{name} is split-only but was initialized with no split tokens; \
             vgi_split_scans is probably off, and this function has no \
             primary/secondary path to fall back to"
        )));
    };
    let ranges = payloads
        .iter()
        .map(|p| decode(p))
        .collect::<Result<Vec<_>>>()?;
    let cur = ranges.first().map(|r| r.lo).unwrap_or(0);
    Ok(Box::new(SplitProducer {
        schema: seq_schema(),
        ranges,
        idx: 0,
        cur,
    }))
}

struct SplitScan {
    name: &'static str,
    description: &'static str,
    shape: Shape,
    schema: SchemaRef,
}

impl SplitScan {
    fn new(name: &'static str, description: &'static str, shape: Shape) -> Self {
        Self {
            name,
            description,
            shape,
            schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
        }
    }

    fn plan_ranges(&self, n: i64, splits: i64) -> Vec<Range> {
        match self.shape {
            Shape::Even => even_ranges(n, splits),
            Shape::NoSplits => Vec::new(),
            Shape::EmptyInterleaved => even_ranges(n, splits)
                .into_iter()
                .flat_map(|r| [Range { lo: r.lo, hi: r.lo }, r])
                .collect(),
            Shape::Skewed => {
                if n <= 0 || splits <= 0 {
                    return Vec::new();
                }
                // The first split takes ~99% of the rows; the rest divide the tail.
                let head = n * 99 / 100;
                let mut out = vec![Range { lo: 0, hi: head }];
                out.extend(
                    even_ranges(n - head, splits - 1)
                        .into_iter()
                        .map(|r| Range {
                            lo: head + r.lo,
                            hi: head + r.hi,
                        }),
                );
                out
            }
            Shape::Many => even_ranges(n, if splits <= 0 { 1000 } else { splits }),
        }
    }
}

fn arg_i64(params: &BindParams, name: &str, default: i64) -> i64 {
    params.arguments.named_i64(name).unwrap_or(default)
}

impl TableFunction for SplitScan {
    fn name(&self) -> &str {
        self.name
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: self.description.to_string(),
            // The declaration is what a distributed engine reads to decide it
            // can retry a task against this function.
            supports_splits: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        // position -1 = a NAMED argument, so the shared SQL suite's
        // `split_sequence(n := 10, splits := 4)` binds identically across SDKs.
        vec![
            ArgSpec::const_arg("n", -1, "int64", "Number of rows to produce").with_ge(0.0),
            ArgSpec::const_arg(
                "splits",
                -1,
                "int64",
                "How many splits to divide the scan into",
            )
            .with_ge(0.0)
            .with_default(4),
        ]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: self.schema.clone(),
            opaque_data: Vec::new(),
        })
    }

    /// Divide the scan. Only `payload` is set: the framework stamps the
    /// consistency anchor, the bind fingerprint and (where a key exists) the
    /// seal, so a fixture cannot accidentally mint a token that skips them.
    fn on_plan(
        &self,
        params: &BindParams,
        _request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let n = arg_i64(params, "n", 0);
        let splits = arg_i64(params, "splits", 4);
        let ranges = self.plan_ranges(n, splits);
        let total: i64 = ranges.iter().map(|r| r.hi - r.lo).sum();
        Ok(Some(PlanOutcome {
            estimated_total_splits: Some(ranges.len() as i64),
            estimated_total_rows: Some(total),
            splits: ranges
                .iter()
                .map(|r| PlannedSplit {
                    payload: encode(*r),
                    estimated_rows: Some(r.hi - r.lo),
                    rows_exact: true,
                    estimated_bytes: Some((r.hi - r.lo) * 8),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }))
    }

    /// The explicit opt-in: a worker that mints splits must be able to redeem
    /// them. The ranges are read off `params.split_payloads` in `producer`, so
    /// there is nothing to do here beyond declaring the capability.
    fn on_split(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        // No payloads at all means the client stopped planning
        // (vgi_split_scans off). A split-only function has no way to know what
        // to read then, and failing here is the point: quietly returning zero
        // rows would be A DIFFERENT ANSWER to the same query, which is worse
        // than an error. Distinct from a plan that legitimately produced ZERO
        // splits — there the client never inits at all.
        let Some(payloads) = params.split_payloads.as_ref() else {
            return Err(RpcError::runtime_error(format!(
                "{} is split-only but was initialized with no split tokens; \
                 vgi_split_scans is probably off, and this function has no \
                 primary/secondary path to fall back to",
                self.name
            )));
        };
        let ranges = payloads
            .iter()
            .map(|p| decode(p))
            .collect::<Result<Vec<_>>>()?;
        let cur = ranges.first().map(|r| r.lo).unwrap_or(0);
        Ok(Box::new(SplitProducer {
            schema: self.schema.clone(),
            ranges,
            idx: 0,
            cur,
        }))
    }
}

/// Walks THIS reader's claimed ranges in order, one batch per call.
struct SplitProducer {
    schema: SchemaRef,
    ranges: Vec<Range>,
    idx: usize,
    cur: i64,
}

impl TableProducer for SplitProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        const MAX_BATCH: i64 = 1024;
        // A zero-row range is STEPPED OVER, never reported as end-of-stream:
        // returning None here would truncate the reader's remaining claims.
        while self.idx < self.ranges.len() {
            let r = self.ranges[self.idx];
            if self.cur >= r.hi {
                self.idx += 1;
                if self.idx < self.ranges.len() {
                    self.cur = self.ranges[self.idx].lo;
                }
                continue;
            }
            let size = (r.hi - self.cur).min(MAX_BATCH);
            let start = self.cur;
            self.cur += size;
            let arr: ArrayRef = Arc::new(Int64Array::from(
                (start..start + size).collect::<Vec<i64>>(),
            ));
            return RecordBatch::try_new(self.schema.clone(), vec![arr])
                .map(Some)
                .map_err(|e| RpcError::runtime_error(e.to_string()));
        }
        Ok(None)
    }
    fn resume_supported(&self) -> bool {
        true
    }
    /// `(idx, cur)` is the whole scan position: the ranges themselves are
    /// rebuilt from the split payloads the continuation blob carries, so only
    /// how far through them this reader got has to travel.
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.idx as i64, self.cur])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 2) {
            self.idx = v[0] as usize;
            self.cur = v[1];
        }
    }
}

// --- fixtures that exercise the CLIENT's split machinery -------------------

/// Fails on a chosen split, in either of the two places that matter.
///
/// They are genuinely different failure paths, not variations:
///
/// * `fail_in_init` fails while REDEEMING the token, before any row is produced.
///   The client must not return that connection to the pool — the init request
///   is on the wire with no answer, so a later checkout would read this split's
///   init response as its own stream header: silent cross-query corruption on
///   the pool-enabled default.
/// * Otherwise it fails MID-STREAM, after emitting rows, so the capture is
///   genuinely partial when it dies. A partial result committed as complete is
///   the failure class the never-partial gate exists to prevent.
struct SplitFailAt;

fn encode_fail(ordinal: i64, r: Range) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&ordinal.to_le_bytes());
    out.extend_from_slice(&r.lo.to_le_bytes());
    out.extend_from_slice(&r.hi.to_le_bytes());
    out
}

fn decode_fail(payload: &[u8]) -> Result<(i64, Range)> {
    if payload.len() != 24 {
        return Err(RpcError::runtime_error(format!(
            "fail payload must be 24 bytes, got {}",
            payload.len()
        )));
    }
    Ok((
        i64::from_le_bytes(payload[0..8].try_into().unwrap()),
        Range {
            lo: i64::from_le_bytes(payload[8..16].try_into().unwrap()),
            hi: i64::from_le_bytes(payload[16..24].try_into().unwrap()),
        },
    ))
}

impl TableFunction for SplitFailAt {
    fn name(&self) -> &str {
        "split_fail_at"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Fails on a chosen split, at init or mid-stream".to_string(),
            supports_splits: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("n", -1, "int64", "Number of rows to produce").with_ge(0.0),
            ArgSpec::const_arg(
                "splits",
                -1,
                "int64",
                "How many splits to divide the scan into",
            )
            .with_ge(0.0)
            .with_default(4),
            ArgSpec::const_arg(
                "fail_at",
                -1,
                "int64",
                "Split ordinal to fail on; -1 never fails",
            )
            .with_default(-1),
            ArgSpec::const_arg(
                "fail_in_init",
                -1,
                "bool",
                "Fail during the split's init rather than mid-stream",
            )
            .with_default(false),
        ]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            opaque_data: Vec::new(),
        })
    }

    fn on_plan(
        &self,
        params: &BindParams,
        _request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let ranges = even_ranges(arg_i64(params, "n", 0), arg_i64(params, "splits", 4));
        Ok(Some(PlanOutcome {
            estimated_total_splits: Some(ranges.len() as i64),
            splits: ranges
                .iter()
                .enumerate()
                .map(|(i, r)| PlannedSplit {
                    payload: encode_fail(i as i64, *r),
                    estimated_rows: Some(r.hi - r.lo),
                    rows_exact: true,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }))
    }

    /// Where the init-time failure lands, so the client's connection-poisoning
    /// path is exercised rather than the mid-stream one.
    fn on_split(&self, params: &ProcessParams) -> Result<()> {
        if !params.arguments.named_bool("fail_in_init").unwrap_or(false) {
            return Ok(());
        }
        let fail_at = params.arguments.named_i64("fail_at").unwrap_or(-1);
        for payload in params.split_payloads.as_deref().unwrap_or(&[]) {
            let (ordinal, _) = decode_fail(payload)?;
            if ordinal == fail_at {
                return Err(RpcError::runtime_error(format!(
                    "split {ordinal} refuses to initialize (fixture)"
                )));
            }
        }
        Ok(())
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let Some(payloads) = params.split_payloads.as_ref() else {
            return Err(RpcError::runtime_error(
                "split_fail_at is split-only but was initialized with no split tokens",
            ));
        };
        let mut ranges = Vec::with_capacity(payloads.len());
        let mut ordinals = Vec::with_capacity(payloads.len());
        for payload in payloads {
            let (ordinal, r) = decode_fail(payload)?;
            ranges.push(r);
            ordinals.push(ordinal);
        }
        let cur = ranges.first().map(|r| r.lo).unwrap_or(0);
        Ok(Box::new(FailProducer {
            schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            ranges,
            ordinals,
            idx: 0,
            cur,
            fail_at: params.arguments.named_i64("fail_at").unwrap_or(-1),
        }))
    }
}

struct FailProducer {
    schema: SchemaRef,
    ranges: Vec<Range>,
    ordinals: Vec<i64>,
    idx: usize,
    cur: i64,
    fail_at: i64,
}

impl TableProducer for FailProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        while self.idx < self.ranges.len() {
            let r = self.ranges[self.idx];
            if self.cur >= r.hi {
                self.idx += 1;
                if self.idx < self.ranges.len() {
                    self.cur = self.ranges[self.idx].lo;
                }
                continue;
            }
            // Fail AFTER at least one row of this split has gone out, so the
            // never-partial gate is tested against a genuinely partial capture
            // rather than an empty one.
            if self.fail_at >= 0 && self.ordinals[self.idx] == self.fail_at && self.cur > r.lo {
                return Err(RpcError::runtime_error(format!(
                    "split {} failed mid-stream (fixture)",
                    self.fail_at
                )));
            }
            let size = (r.hi - self.cur).min(8);
            let start = self.cur;
            self.cur += size;
            let arr: ArrayRef = Arc::new(Int64Array::from(
                (start..start + size).collect::<Vec<i64>>(),
            ));
            return RecordBatch::try_new(self.schema.clone(), vec![arr])
                .map(Some)
                .map_err(|e| RpcError::runtime_error(e.to_string()));
        }
        Ok(None)
    }
    fn resume_supported(&self) -> bool {
        true
    }
    /// `(idx, cur)` is the whole scan position: the ranges themselves are
    /// rebuilt from the split payloads the continuation blob carries, so only
    /// how far through them this reader got has to travel.
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.idx as i64, self.cur])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 2) {
            self.idx = v[0] as usize;
            self.cur = v[1];
        }
    }
}

/// Paginates forever: every plan page returns a cursor and never exhausts it.
///
/// A worker can hang a client this way by accident as easily as on purpose, and
/// the failure mode is the bad one: a client that stopped early would scan a
/// PARTIAL enumeration and report it as the whole answer. The client must hit
/// its page cap and throw an error naming it — never truncate and proceed.
struct SplitEndlessCursor;

impl TableFunction for SplitEndlessCursor {
    fn name(&self) -> &str {
        "split_endless_cursor"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Paginates forever: the client must hit its page cap".to_string(),
            supports_splits: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("n", -1, "int64", "Ignored").with_ge(0.0),
            ArgSpec::const_arg("splits", -1, "int64", "Ignored")
                .with_ge(0.0)
                .with_default(1),
        ]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            opaque_data: Vec::new(),
        })
    }

    fn on_plan(
        &self,
        _params: &BindParams,
        request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let page = request.cursor.as_ref().map(|c| c.0.len()).unwrap_or(0);
        Ok(Some(PlanOutcome {
            splits: vec![PlannedSplit {
                payload: encode(Range { lo: 0, hi: 1 }),
                ..Default::default()
            }],
            next_cursors: vec![vec![b'x'; page + 1]],
            ..Default::default()
        }))
    }

    fn on_split(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }

    fn producer(&self, _params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        Ok(Box::new(SplitProducer {
            schema: Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
            ranges: vec![Range { lo: 0, hi: 1 }],
            idx: 0,
            cur: 0,
        }))
    }
}

/// Reports, per split, what pushdown the PLAN call actually received.
///
/// A row-count assertion cannot catch a pushdown regression — the rows are the
/// same either way — so this fixture makes the pushdown itself the data. What it
/// reports is recorded at PLAN time and baked into each split's payload, which is
/// the claim under test: filters and projection must reach `plan()`, not merely
/// reach the per-split `init()` afterwards.
struct SplitEchoFilters;

fn echo_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("split_ordinal", DataType::Int64, false),
        Field::new("saw_filters", DataType::Boolean, false),
        Field::new("n_projection", DataType::Int64, false),
    ]))
}

impl TableFunction for SplitEchoFilters {
    fn name(&self) -> &str {
        "split_echo_filters"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Reports the pushdown each split's plan() call saw".to_string(),
            supports_splits: true,
            // filter_pushdown declares that this worker APPLIES the filter, so
            // DuckDB stops re-checking it above the scan. Declaring it while
            // only reporting the filter would be the "wrong answers if declared
            // falsely" hazard in miniature. auto_apply_filters makes it true.
            filter_pushdown: true,
            auto_apply_filters: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("splits", -1, "int64", "How many splits to report")
                .with_ge(1.0)
                .with_default(3),
        ]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: echo_schema(),
            opaque_data: Vec::new(),
        })
    }

    fn on_plan(
        &self,
        params: &BindParams,
        request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let splits = arg_i64(params, "splits", 3);
        let saw = i64::from(request.pushdown_filters.is_some());
        let nproj = request
            .projection_ids
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0) as i64;
        Ok(Some(PlanOutcome {
            estimated_total_splits: Some(splits),
            splits: (0..splits)
                .map(|i| {
                    let mut payload = Vec::with_capacity(24);
                    payload.extend_from_slice(&i.to_le_bytes());
                    payload.extend_from_slice(&saw.to_le_bytes());
                    payload.extend_from_slice(&nproj.to_le_bytes());
                    PlannedSplit {
                        payload,
                        ..Default::default()
                    }
                })
                .collect(),
            ..Default::default()
        }))
    }

    fn on_split(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let Some(payloads) = params.split_payloads.as_ref() else {
            return Err(RpcError::runtime_error(
                "split_echo_filters is split-only but was initialized with no split tokens",
            ));
        };
        let mut rows = Vec::with_capacity(payloads.len());
        for payload in payloads {
            if payload.len() != 24 {
                return Err(RpcError::runtime_error(format!(
                    "echo payload must be 24 bytes, got {}",
                    payload.len()
                )));
            }
            rows.push((
                i64::from_le_bytes(payload[0..8].try_into().unwrap()),
                i64::from_le_bytes(payload[8..16].try_into().unwrap()) != 0,
                i64::from_le_bytes(payload[16..24].try_into().unwrap()),
            ));
        }
        Ok(Box::new(EchoProducer { rows, done: false }))
    }
}

struct EchoProducer {
    rows: Vec<(i64, bool, i64)>,
    done: bool,
}

impl TableProducer for EchoProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        if self.done || self.rows.is_empty() {
            return Ok(None);
        }
        self.done = true;
        let ordinals: ArrayRef = Arc::new(Int64Array::from(
            self.rows.iter().map(|r| r.0).collect::<Vec<i64>>(),
        ));
        let saw: ArrayRef = Arc::new(arrow_array::BooleanArray::from(
            self.rows.iter().map(|r| r.1).collect::<Vec<bool>>(),
        ));
        let nproj: ArrayRef = Arc::new(Int64Array::from(
            self.rows.iter().map(|r| r.2).collect::<Vec<i64>>(),
        ));
        RecordBatch::try_new(echo_schema(), vec![ordinals, saw, nproj])
            .map(Some)
            .map_err(|e| RpcError::runtime_error(e.to_string()))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.done as i64])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.done = v[0] != 0;
        }
    }
}

// --- fixtures that exercise the CLIENT's split machinery, part 2 -----------
//
// Everything below is a twin of a vgi-python fixture of the same name. The
// shared SQL suite runs unchanged against every SDK's worker, so a wire
// disagreement between two SDKs shows up as the same named test failing under
// one of them — which only works if the fixtures agree on behaviour, not merely
// on name.

/// Enumerates its plan over several pages, each disjoint from the last.
///
/// Pagination is how a worker keeps one plan response bounded when a scan has
/// very many splits. What has to hold is that the pages compose: each split
/// appears exactly once across the whole enumeration, and the client keeps
/// asking until a page arrives with no cursor.
///
/// Disjointness is the worker's obligation and is NOT checked by any client —
/// a dedup was tried and removed, because it needed a copy of every token, it
/// compared token bytes and so could never fire on a keyed worker, and the most
/// a client can do with a duplicate is refuse anyway. This fixture is the
/// well-behaved side of that contract.
struct SplitPaginated;

const PER_PAGE: usize = 4;

impl TableFunction for SplitPaginated {
    fn name(&self) -> &str {
        "split_paginated"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Plan enumerated over several disjoint pages".to_string(),
            supports_splits: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        split_args()
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: seq_schema(),
            opaque_data: Vec::new(),
        })
    }

    fn on_plan(
        &self,
        params: &BindParams,
        request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let n = arg_i64(params, "n", 0);
        let splits = arg_i64(params, "splits", 4);
        let ranges = even_ranges(n, splits);

        // The cursor is the worker's own bytes; this one carries the page index
        // as an LE u64, which is enough because the range list is regenerable
        // from the bind arguments alone.
        let cursor: &[u8] = request
            .cursor
            .as_ref()
            .map(|c| c.0.as_slice())
            .unwrap_or(&[]);
        let page = if cursor.len() == 8 {
            u64::from_le_bytes(cursor.try_into().unwrap()) as usize
        } else {
            0
        };

        let lo = page * PER_PAGE;
        let window: Vec<Range> = ranges.iter().skip(lo).take(PER_PAGE).copied().collect();
        let done = lo + PER_PAGE >= ranges.len();
        Ok(Some(PlanOutcome {
            splits: window
                .iter()
                .map(|r| PlannedSplit {
                    payload: encode(*r),
                    estimated_rows: Some(r.hi - r.lo),
                    rows_exact: true,
                    ..Default::default()
                })
                .collect(),
            next_cursors: if done {
                Vec::new()
            } else {
                vec![((page + 1) as u64).to_le_bytes().to_vec()]
            },
            estimated_total_rows: if done { Some(n) } else { None },
            ..Default::default()
        }))
    }

    fn on_split(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        seq_producer("split_paginated", params)
    }
}

/// Pins its plan to a catalog version that has moved on.
///
/// The only way a bad split token is reachable through SQL, and deliberately so:
/// the framework owns the envelope, so a worker cannot mint a token with a wrong
/// fingerprint or a cleared seal even on purpose. What it CAN do is plan against
/// a snapshot that is no longer current — which is exactly the situation
/// `SPLIT_SNAPSHOT_EXPIRED` names, a plan outliving the version it was pinned to.
///
/// The refusal must stay distinguishable from `SPLIT_TOKEN_INVALID`, because only
/// this one means "re-run the query": re-planning mints a valid token, whereas
/// re-running a wrongly-bound one just reproduces it.
struct SplitStalePlan;

impl TableFunction for SplitStalePlan {
    fn name(&self) -> &str {
        "split_stale_plan"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Plans against a catalog version that is not the live one".to_string(),
            supports_splits: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        split_args()
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: seq_schema(),
            opaque_data: Vec::new(),
        })
    }

    fn on_plan(
        &self,
        params: &BindParams,
        _request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let ranges = even_ranges(arg_i64(params, "n", 0), arg_i64(params, "splits", 4));
        Ok(Some(PlanOutcome {
            splits: ranges
                .iter()
                .map(|r| PlannedSplit {
                    payload: encode(*r),
                    ..Default::default()
                })
                .collect(),
            // Any value the live catalog will not report. The fixture catalog's
            // version is small, so a large constant is reliably "not current"
            // without depending on what that version happens to be.
            catalog_version: Some(987_654_321),
            ..Default::default()
        }))
    }

    fn on_split(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        seq_producer("split_stale_plan", params)
    }
}

/// Declares a split-token lifetime shorter than any client's scheduling horizon.
///
/// An expired token is a failed query, not a degradation: nothing re-plans when
/// one expires, because a distributed engine retries the serialized task it was
/// handed and has no path back to the planner. So the only useful moment to
/// notice a too-short lifetime is BEFORE the plan is issued — a legible refusal
/// naming the shortfall, instead of a scan that dies partway with the work
/// already scheduled.
///
/// One second is unusable everywhere: even DuckDB, whose horizon is the shortest
/// of any engine because it plans at execution start, can take longer than that
/// to reach a split.
struct SplitShortTtl;

impl TableFunction for SplitShortTtl {
    fn name(&self) -> &str {
        "split_short_ttl"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Declares a 1s split-token TTL, below any client horizon".to_string(),
            supports_splits: true,
            split_token_ttl_seconds: Some(1),
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        split_args()
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: seq_schema(),
            opaque_data: Vec::new(),
        })
    }

    fn on_plan(
        &self,
        params: &BindParams,
        _request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let ranges = even_ranges(arg_i64(params, "n", 0), arg_i64(params, "splits", 4));
        Ok(Some(PlanOutcome {
            splits: ranges
                .iter()
                .map(|r| PlannedSplit {
                    payload: encode(*r),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }))
    }

    fn on_split(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        seq_producer("split_short_ttl", params)
    }
}

/// Split-capable AND `supports_batch_index`, which together are a contract.
///
/// A batch index must be globally monotonic per reader, and greedy per-split
/// claiming re-initializes the same connection for each split — so every split
/// starts a fresh stream, and a worker that restarted its numbering per split
/// would hand one reader a DECREASING index. Nothing in the transport prevents
/// that; the client throws when it happens, which is right but only useful if
/// the contract is written down and exercised.
///
/// What makes it work is that the client's `fetch_add` hands each reader strictly
/// ASCENDING split indices, so a worker deriving its index from the split's
/// position in a globally-ordered space is monotonic per reader by construction.
/// That is the whole reason claiming is greedy rather than grouped — and it is
/// NOT something multi-token init provides, since a group's tokens carry no
/// ordering of their own.
///
/// Each split owns a slice of the index space (`ordinal * STRIDE`), so indices
/// ascend across split boundaries as well as within them. The stride bounds how
/// many batches one split may emit before colliding with the next, and
/// `VGI_BATCH_INDEX_CAP` bounds the product — so choosing a stride is really
/// choosing `cap / n_splits`.
struct SplitBatchIndex;

const STRIDE: i64 = 1_000;

/// `(ordinal, lo, hi)` — the ordinal is what the index space keys on.
fn encode_ordinal(ordinal: i64, r: Range) -> Vec<u8> {
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&ordinal.to_le_bytes());
    out.extend_from_slice(&r.lo.to_le_bytes());
    out.extend_from_slice(&r.hi.to_le_bytes());
    out
}

fn decode_ordinal(payload: &[u8]) -> Result<(i64, Range)> {
    if payload.len() != 24 {
        return Err(RpcError::runtime_error(format!(
            "batch-index split payload must be 24 bytes, got {}",
            payload.len()
        )));
    }
    Ok((
        i64::from_le_bytes(payload[0..8].try_into().unwrap()),
        Range {
            lo: i64::from_le_bytes(payload[8..16].try_into().unwrap()),
            hi: i64::from_le_bytes(payload[16..24].try_into().unwrap()),
        },
    ))
}

impl TableFunction for SplitBatchIndex {
    fn name(&self) -> &str {
        "split_batch_index"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Split-capable with per-split batch_index space".to_string(),
            supports_splits: true,
            supports_batch_index: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        split_args()
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: seq_schema(),
            opaque_data: Vec::new(),
        })
    }

    fn on_plan(
        &self,
        params: &BindParams,
        _request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let ranges = even_ranges(arg_i64(params, "n", 0), arg_i64(params, "splits", 4));
        Ok(Some(PlanOutcome {
            estimated_total_splits: Some(ranges.len() as i64),
            splits: ranges
                .iter()
                .enumerate()
                .map(|(i, r)| PlannedSplit {
                    payload: encode_ordinal(i as i64, *r),
                    estimated_rows: Some(r.hi - r.lo),
                    rows_exact: true,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }))
    }

    fn on_split(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let Some(payloads) = params.split_payloads.as_ref() else {
            return Err(RpcError::runtime_error(
                "split_batch_index is split-only but was initialized with no split tokens",
            ));
        };
        let claims = payloads
            .iter()
            .map(|p| decode_ordinal(p))
            .collect::<Result<Vec<_>>>()?;
        let cur = claims.first().map(|(_, r)| r.lo).unwrap_or(0);
        Ok(Box::new(BatchIndexProducer {
            schema: seq_schema(),
            claims,
            idx: 0,
            cur,
            emitted_in_split: 0,
            last_index: 0,
        }))
    }
}

struct BatchIndexProducer {
    schema: SchemaRef,
    claims: Vec<(i64, Range)>,
    idx: usize,
    cur: i64,
    emitted_in_split: i64,
    last_index: i64,
}

impl TableProducer for BatchIndexProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        const MAX_BATCH: i64 = 64;
        while self.idx < self.claims.len() {
            let (ordinal, r) = self.claims[self.idx];
            if self.cur >= r.hi {
                self.idx += 1;
                self.emitted_in_split = 0;
                if self.idx < self.claims.len() {
                    self.cur = self.claims[self.idx].1.lo;
                }
                continue;
            }
            let size = (r.hi - self.cur).min(MAX_BATCH);
            let start = self.cur;
            self.cur += size;
            // The index space this split owns. Ascending claims make this
            // monotonic per reader across split boundaries too.
            self.last_index = ordinal * STRIDE + self.emitted_in_split;
            self.emitted_in_split += 1;
            let arr: ArrayRef = Arc::new(Int64Array::from(
                (start..start + size).collect::<Vec<i64>>(),
            ));
            return RecordBatch::try_new(self.schema.clone(), vec![arr])
                .map(Some)
                .map_err(|e| RpcError::runtime_error(e.to_string()));
        }
        Ok(None)
    }

    fn last_metadata(&self) -> Option<std::collections::HashMap<String, String>> {
        Some(std::collections::HashMap::from([(
            "vgi_batch_index".to_string(),
            self.last_index.to_string(),
        )]))
    }
    fn resume_supported(&self) -> bool {
        true
    }
    /// `last_index` rides along with the cursor: the client asserts the index
    /// is monotonic across the whole reader, so a continuation that restarted
    /// the counter would break the contract this fixture exists to prove.
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[
            self.idx as i64,
            self.cur,
            self.emitted_in_split,
            self.last_index,
        ])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 4) {
            self.idx = v[0] as usize;
            self.cur = v[1];
            self.emitted_in_split = v[2];
            self.last_index = v[3];
        }
    }
}

/// A split scan whose result is cacheable, so never-partial becomes assertable.
///
/// The result cache knows nothing about splits, deliberately: its key describes
/// the QUERY — identity, filters, projection, catalog version — while splits are
/// how the rows were produced. A split scan and a non-split scan of the same
/// query return the same rows and so share an entry, either able to serve what
/// the other populated.
///
/// What that makes testable is the never-partial gate. A scan abandoned partway —
/// by a LIMIT satisfied early, or by an error — leaves splits claimed but unread,
/// and committing what was captured would store a SUBSET under a key claiming to
/// be the whole answer. Every later identical query would then return missing
/// rows with no error at all.
///
/// Cache control rides the FIRST batch, and under splits every reader sees a
/// first batch of its own — so all of them advertise the same freshness. A result
/// is one entry with one lifetime; a per-split TTL would be decided by whichever
/// reader happened to arrive first.
struct SplitCacheable;

impl TableFunction for SplitCacheable {
    fn name(&self) -> &str {
        "split_cacheable"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Split-capable and cacheable, for the never-partial gate".to_string(),
            supports_splits: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        split_args()
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: seq_schema(),
            opaque_data: Vec::new(),
        })
    }

    fn on_plan(
        &self,
        params: &BindParams,
        _request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let ranges = even_ranges(arg_i64(params, "n", 0), arg_i64(params, "splits", 4));
        Ok(Some(PlanOutcome {
            estimated_total_splits: Some(ranges.len() as i64),
            splits: ranges
                .iter()
                .map(|r| PlannedSplit {
                    payload: encode(*r),
                    estimated_rows: Some(r.hi - r.lo),
                    rows_exact: true,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }))
    }

    fn on_split(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let Some(payloads) = params.split_payloads.as_ref() else {
            return Err(RpcError::runtime_error(
                "split_cacheable is split-only but was initialized with no split tokens",
            ));
        };
        let ranges = payloads
            .iter()
            .map(|p| decode(p))
            .collect::<Result<Vec<_>>>()?;
        let cur = ranges.first().map(|r| r.lo).unwrap_or(0);
        Ok(Box::new(CacheableProducer {
            schema: seq_schema(),
            ranges,
            idx: 0,
            cur,
            first: true,
            advertised: false,
        }))
    }
}

struct CacheableProducer {
    schema: SchemaRef,
    ranges: Vec<Range>,
    idx: usize,
    cur: i64,
    first: bool,
    advertised: bool,
}

impl TableProducer for CacheableProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        const MAX_BATCH: i64 = 16;
        while self.idx < self.ranges.len() {
            let r = self.ranges[self.idx];
            if self.cur >= r.hi {
                self.idx += 1;
                if self.idx < self.ranges.len() {
                    self.cur = self.ranges[self.idx].lo;
                }
                continue;
            }
            let size = (r.hi - self.cur).min(MAX_BATCH);
            let start = self.cur;
            self.cur += size;
            self.advertised = self.first;
            self.first = false;
            let arr: ArrayRef = Arc::new(Int64Array::from(
                (start..start + size).collect::<Vec<i64>>(),
            ));
            return RecordBatch::try_new(self.schema.clone(), vec![arr])
                .map(Some)
                .map_err(|e| RpcError::runtime_error(e.to_string()));
        }
        Ok(None)
    }

    fn last_metadata(&self) -> Option<std::collections::HashMap<String, String>> {
        // Only the first batch of this reader's stream carries it — the client
        // latches freshness there, and every reader advertises the same value.
        self.advertised.then(|| {
            std::collections::HashMap::from([("vgi.cache.ttl".to_string(), "300".to_string())])
        })
    }
    fn resume_supported(&self) -> bool {
        true
    }
    /// `first` travels too — freshness is advertised on this reader's FIRST
    /// batch only, and a continuation that reset the flag would re-advertise
    /// mid-stream.
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.idx as i64, self.cur, self.first as i64])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 3) {
            self.idx = v[0] as usize;
            self.cur = v[1];
            self.first = v[2] != 0;
        }
    }
}

/// One split per partition — the shape a partitioned table naturally takes.
///
/// A partition and a split are different things that usually coincide: a
/// partition is a property of the DATA (every row here shares a value), a split
/// is a unit of WORK. A worker that already stores data per partition has its
/// split boundaries handed to it, so this is the common case rather than a
/// contrived one.
///
/// What needs asserting is that the two survive each other. Splits are claimed
/// greedily, in an order nobody chose, by readers that each end up holding
/// several — so the association between a batch and the partition value it
/// carries has to hold through re-init on a reused connection and across the
/// boundary where one reader moves from one partition to the next. Losing it
/// does not raise: it produces a GROUP BY that silently mixes partitions.
struct SplitPartitioned;

const COUNTRIES: [&str; 4] = ["US", "DE", "JP", "BR"];
const SPLIT_PLAN_EXECUTION_ID: &[u8] = b"split-partitioned-execution";
const SPLIT_PLAN_OPAQUE: &[u8] = b"split-partitioned-plan-state";

impl SplitPartitioned {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            vgi::partition::partition_field("country", DataType::Utf8),
            Field::new("sales", DataType::Int64, false),
        ]))
    }
}

impl TableFunction for SplitPartitioned {
    fn name(&self) -> &str {
        "split_partitioned"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "One split per partition, with partition values on each batch".to_string(),
            supports_splits: true,
            partition_kind: Some(
                vgi::protocol::enums::partition_kind::SINGLE_VALUE_PARTITIONS.to_string(),
            ),
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        vec![
            ArgSpec::const_arg("rows_per_country", -1, "int64", "Rows in each partition")
                .with_ge(0.0)
                .with_default(5),
            ArgSpec::const_arg(
                "require_plan_context",
                -1,
                "boolean",
                "Reject redemption unless plan execution context is echoed",
            )
            .with_default(false),
        ]
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Self::schema(),
            opaque_data: Vec::new(),
        })
    }

    fn on_plan(
        &self,
        params: &BindParams,
        _request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let rows = params.arguments.named_i64("rows_per_country").unwrap_or(5);
        // The payload names the partition by INDEX, so a redemption reads the
        // same partition however many times it runs and in whichever process.
        Ok(Some(PlanOutcome {
            estimated_total_splits: Some(COUNTRIES.len() as i64),
            estimated_total_rows: Some(rows.max(0) * COUNTRIES.len() as i64),
            estimated_total_bytes: Some(rows.max(0) * COUNTRIES.len() as i64 * 16),
            execution_id: Some(SPLIT_PLAN_EXECUTION_ID.to_vec()),
            init_opaque_data: Some(SPLIT_PLAN_OPAQUE.to_vec()),
            locations: Some(vec!["local".to_string()]),
            partitioning: Some(vec![PartitionTransform {
                column: "country".to_string(),
                transform: "identity".to_string(),
                param: None,
            }]),
            sort_order: Some(vec![SortField {
                column: "sales".to_string(),
                direction: DictString("asc".to_string()),
                nulls: DictString("nulls_last".to_string()),
            }]),
            cache_max_age_seconds: Some(60),
            splits: (0..COUNTRIES.len() as i64)
                .map(|i| {
                    let country = COUNTRIES[i as usize];
                    let bounds = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new(
                            "country",
                            DataType::Utf8,
                            true,
                        )])),
                        vec![Arc::new(arrow_array::StringArray::from(vec![
                            country, country,
                        ]))],
                    )
                    .map_err(|error| RpcError::runtime_error(error.to_string()))?;
                    let base = i * 100;
                    PlannedSplit {
                        payload: i.to_le_bytes().to_vec(),
                        estimated_rows: Some(rows.max(0)),
                        rows_exact: true,
                        estimated_bytes: Some(rows.max(0) * 16),
                        location_ids: Some(vec![0]),
                        ..Default::default()
                    }
                    .with_partition_bounds(&bounds)?
                    .with_column_statistics(&[
                        CatColStat {
                            column_name: "country".to_string(),
                            min: StatValue::Utf8(country.to_string()),
                            max: StatValue::Utf8(country.to_string()),
                            has_null: false,
                            has_not_null: rows > 0,
                            distinct_count: Some((rows > 0) as i64),
                            contains_unicode: Some(false),
                            max_string_length: Some(country.len() as u64),
                        },
                        CatColStat {
                            column_name: "sales".to_string(),
                            min: StatValue::Int64(base + 1),
                            max: StatValue::Int64(base + rows.max(1)),
                            has_null: false,
                            has_not_null: rows > 0,
                            distinct_count: Some(rows.max(0)),
                            contains_unicode: None,
                            max_string_length: None,
                        },
                    ])
                })
                .collect::<Result<Vec<_>>>()?,
            ..Default::default()
        }))
    }

    fn on_split(&self, params: &ProcessParams) -> Result<()> {
        let require_plan_context = params
            .arguments
            .named_bool("require_plan_context")
            .unwrap_or(false);
        if require_plan_context
            && (params.execution_id != SPLIT_PLAN_EXECUTION_ID
                || params.init_opaque_data != SPLIT_PLAN_OPAQUE)
        {
            return Err(RpcError::runtime_error(
                "split_partitioned redemption did not echo its plan execution context",
            ));
        }
        Ok(())
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let Some(payloads) = params.split_payloads.as_ref() else {
            return Err(RpcError::runtime_error(
                "split_partitioned is split-only but was initialized with no split tokens",
            ));
        };
        let mut idxs = Vec::with_capacity(payloads.len());
        for p in payloads {
            if p.len() != 8 {
                return Err(RpcError::runtime_error(format!(
                    "partition split payload must be 8 bytes, got {}",
                    p.len()
                )));
            }
            idxs.push(i64::from_le_bytes(p[..8].try_into().unwrap()));
        }
        Ok(Box::new(PartitionedProducer {
            schema: Self::schema(),
            rows: params.arguments.named_i64("rows_per_country").unwrap_or(5),
            idxs,
            at: 0,
            last: None,
        }))
    }
}

struct PartitionedProducer {
    schema: SchemaRef,
    rows: i64,
    idxs: Vec<i64>,
    at: usize,
    last: Option<RecordBatch>,
}

impl TableProducer for PartitionedProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        // A partition with zero rows is STEPPED OVER, never reported as
        // end-of-stream — the same rule every split fixture follows, and here it
        // is reachable through `rows_per_country := 0`.
        while self.at < self.idxs.len() {
            let ci = self.idxs[self.at] as usize;
            self.at += 1;
            if self.rows <= 0 || ci >= COUNTRIES.len() {
                continue;
            }
            // Each partition's values are offset by its own index, so swapping
            // two splits' labels MOVES the per-partition sums. With identical
            // values everywhere a mislabelled partition would be invisible.
            let base = (ci as i64) * 100;
            let country: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![
                COUNTRIES[ci];
                self.rows
                    as usize
            ]));
            let sales: ArrayRef = Arc::new(Int64Array::from(
                (1..=self.rows).map(|i| base + i).collect::<Vec<i64>>(),
            ));
            let batch = RecordBatch::try_new(self.schema.clone(), vec![country, sales])
                .map_err(|e| RpcError::runtime_error(e.to_string()))?;
            self.last = Some(batch.clone());
            return Ok(Some(batch));
        }
        self.last = None;
        Ok(None)
    }

    fn last_metadata(&self) -> Option<std::collections::HashMap<String, String>> {
        // SINGLE_VALUE: min == max within the batch, which is what lets the
        // client read row 0 as the exact partition key.
        let batch = self.last.as_ref()?;
        vgi::partition::partition_metadata(&self.schema, batch)
            .ok()
            .flatten()
    }
    fn resume_supported(&self) -> bool {
        true
    }
    /// Only `at`: `last` is set by the `next_batch` that precedes every
    /// `last_metadata` call, so it never has to survive a continuation.
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.at as i64])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 1) {
            self.at = v[0] as usize;
        }
    }
}

/// Echoes the DYNAMIC filter each tick carried, per split.
///
/// A plan is built from STATIC filters only — join-key values are not known when
/// the plan RPC fires, so they cannot prune the split SET. They arrive later, per
/// tick, and prune WITHIN each split. Both halves have to keep working once a
/// reader re-initializes the same connection per split: the tick filter state is
/// a property of the connection, and a split that lost it would silently stop
/// pruning.
///
/// "Silently" is the operative word, and it is why this reports the filter as
/// DATA rather than leaving the test to infer it from row counts. A scan that
/// stopped receiving dynamic filters returns exactly the same rows — DuckDB
/// re-checks the predicate above the scan — just after shipping more of them. No
/// assertion about the result set can tell the difference.
struct SplitDynamicFilter;

impl SplitDynamicFilter {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("pushed_filters", DataType::Utf8, false),
        ]))
    }
}

impl TableFunction for SplitDynamicFilter {
    fn name(&self) -> &str {
        "split_dynamic_filter"
    }

    fn metadata(&self) -> FunctionMetadata {
        FunctionMetadata {
            description: "Echoes the dynamic filter each tick carried, per split".to_string(),
            supports_splits: true,
            filter_pushdown: true,
            auto_apply_filters: true,
            projection_pushdown: true,
            ..Default::default()
        }
    }

    fn argument_specs(&self) -> Vec<ArgSpec> {
        split_args()
    }

    fn on_bind(&self, _params: &BindParams) -> Result<BindResponse> {
        Ok(BindResponse {
            output_schema: Self::schema(),
            opaque_data: Vec::new(),
        })
    }

    /// Report the row count, which decides which side of a join this lands on.
    ///
    /// Without it DuckDB assumes a default (large) cardinality and puts this scan
    /// on the BUILD side of a hash join — where no join-key IN filter is pushed
    /// into it, because the filter goes to the probe side. The scan then reads
    /// everything and DuckDB filters above it: right answers, no pushdown, and
    /// nothing in the result to say so. Nothing about splits causes that; it is
    /// the ordinary consequence of a table function declining to estimate itself.
    fn cardinality(&self, params: &BindParams) -> Option<vgi::table_function::TableCardinality> {
        let n = arg_i64(params, "n", 0);
        Some(vgi::table_function::TableCardinality {
            estimate: Some(n),
            max: Some(n),
        })
    }

    fn on_plan(
        &self,
        params: &BindParams,
        _request: &TableFunctionPlanRequest,
    ) -> Result<Option<PlanOutcome>> {
        let ranges = even_ranges(arg_i64(params, "n", 0), arg_i64(params, "splits", 4));
        Ok(Some(PlanOutcome {
            estimated_total_splits: Some(ranges.len() as i64),
            splits: ranges
                .iter()
                .map(|r| PlannedSplit {
                    payload: encode(*r),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }))
    }

    fn on_split(&self, _params: &ProcessParams) -> Result<()> {
        Ok(())
    }

    fn producer(&self, params: &ProcessParams) -> Result<Box<dyn TableProducer>> {
        let Some(payloads) = params.split_payloads.as_ref() else {
            return Err(RpcError::runtime_error(
                "split_dynamic_filter is split-only but was initialized with no split tokens",
            ));
        };
        let ranges = payloads
            .iter()
            .map(|p| decode(p))
            .collect::<Result<Vec<_>>>()?;
        let cur = ranges.first().map(|r| r.lo).unwrap_or(0);
        let rendered = params
            .pushdown_filters
            .as_ref()
            .and_then(|b| {
                vgi::pushdown::PushdownFilters::parse_with_join_keys(b, &params.join_keys).ok()
            })
            .map(|pf| render_filters(&pf))
            .unwrap_or_else(|| "(none)".to_string());
        Ok(Box::new(DynFilterProducer {
            schema: Self::schema(),
            ranges,
            idx: 0,
            cur,
            rendered,
        }))
    }
}

/// The CANONICAL cross-SDK rendering of a pushed-down filter set.
///
/// Every SDK must produce this byte-for-byte, because the shared SQL suite
/// asserts on the string. A language's own debug formatting cannot be used —
/// `repr(PushdownFilters)` is Python-shaped and no other SDK can reproduce it —
/// so this renders from `get_column_bounds`, which every SDK mirrors: for each
/// filtered column in sorted order, `col>=min` and/or `col<=max`, joined by `,`.
/// Values are included deliberately: without them a tightening Top-N filter and
/// a loose one render identically, and the test could not tell them apart.
fn render_filters(pf: &vgi::pushdown::PushdownFilters) -> String {
    // Recursive: a compound predicate arrives as `And([Constant, Constant])`, so
    // collecting only top-level columns rendered `(none)` for exactly the
    // multi-clause filters worth asserting on.
    fn collect(f: &vgi::pushdown::Filter, out: &mut Vec<String>) {
        use vgi::pushdown::Filter as F;
        match f {
            F::Constant { column_name, .. }
            | F::In { column_name }
            | F::JoinKeys { column_name }
            | F::IsNull { column_name }
            | F::IsNotNull { column_name }
            | F::Other { column_name, .. } => out.push(column_name.clone()),
            F::And(children) | F::Or(children) => {
                for c in children {
                    collect(c, out);
                }
            }
            F::Struct { column_name, .. } => out.push(column_name.clone()),
        }
    }
    let mut cols: Vec<String> = Vec::new();
    for f in pf.filters().iter() {
        collect(f, &mut cols);
    }
    cols.sort();
    cols.dedup();
    let mut parts = Vec::new();
    for c in cols {
        if let Some(b) = pf.get_column_bounds(&c) {
            if let Some(min) = b.min {
                parts.push(format!("{c}>={min}"));
            }
            if let Some(max) = b.max {
                parts.push(format!("{c}<={max}"));
            }
        }
    }
    if parts.is_empty() {
        "(none)".to_string()
    } else {
        parts.join(",")
    }
}

struct DynFilterProducer {
    schema: SchemaRef,
    ranges: Vec<Range>,
    idx: usize,
    cur: i64,
    rendered: String,
}

impl TableProducer for DynFilterProducer {
    fn next_batch(&mut self, _out: &mut vgi_rpc::OutputCollector) -> Result<Option<RecordBatch>> {
        const MAX_BATCH: i64 = 4;
        while self.idx < self.ranges.len() {
            let r = self.ranges[self.idx];
            if self.cur >= r.hi {
                self.idx += 1;
                if self.idx < self.ranges.len() {
                    self.cur = self.ranges[self.idx].lo;
                }
                continue;
            }
            let size = (r.hi - self.cur).min(MAX_BATCH);
            let start = self.cur;
            self.cur += size;
            let n: ArrayRef = Arc::new(Int64Array::from(
                (start..start + size).collect::<Vec<i64>>(),
            ));
            let f: ArrayRef = Arc::new(arrow_array::StringArray::from(vec![
                self.rendered.as_str();
                size as usize
            ]));
            return RecordBatch::try_new(self.schema.clone(), vec![n, f])
                .map(Some)
                .map_err(|e| RpcError::runtime_error(e.to_string()));
        }
        Ok(None)
    }
    fn resume_supported(&self) -> bool {
        true
    }
    /// `(idx, cur)` is the whole scan position: the ranges themselves are
    /// rebuilt from the split payloads the continuation blob carries, so only
    /// how far through them this reader got has to travel.
    fn encode_resume(&self) -> Vec<u8> {
        resume::pack(&[self.idx as i64, self.cur])
    }
    fn restore_resume(&mut self, bytes: &[u8]) {
        if let Some(v) = resume::unpack(bytes, 2) {
            self.idx = v[0] as usize;
            self.cur = v[1];
        }
    }
}

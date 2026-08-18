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
use vgi::protocol::dtos::TableFunctionPlanRequest;
use vgi::split_token::{PlanOutcome, PlannedSplit};
use vgi::table_function::{TableFunction, TableProducer};
use vgi_rpc::{Result, RpcError};

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
                out.extend(even_ranges(n - head, splits - 1).into_iter().map(|r| Range {
                    lo: head + r.lo,
                    hi: head + r.hi,
                }));
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
            ArgSpec::const_arg("splits", -1, "int64", "How many splits to divide the scan into")
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
    fn next_batch(
        &mut self,
        _out: &mut vgi_rpc::OutputCollector,
    ) -> Result<Option<RecordBatch>> {
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
            let arr: ArrayRef =
                Arc::new(Int64Array::from((start..start + size).collect::<Vec<i64>>()));
            return RecordBatch::try_new(self.schema.clone(), vec![arr])
                .map(Some)
                .map_err(|e| RpcError::runtime_error(e.to_string()));
        }
        Ok(None)
    }
}

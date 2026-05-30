//! Shared helpers for scalar fixtures.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;
use vgi::function::{BindParams, ProcessParams};
use vgi_rpc::{Result, RpcError};

/// Build the single-column `result` output batch.
pub fn result(params: &ProcessParams, arr: ArrayRef) -> Result<RecordBatch> {
    RecordBatch::try_new(params.output_schema.clone(), vec![arr])
        .map_err(|e| RpcError::runtime_error(format!("build result batch: {e}")))
}

/// First input field type (defaults to int64 when absent).
pub fn first_input_type(params: &BindParams) -> DataType {
    nth_input_type(params, 0)
}

/// Nth input field type (defaults to int64 when absent).
pub fn nth_input_type(params: &BindParams, n: usize) -> DataType {
    params
        .input_schema
        .as_ref()
        .and_then(|s| s.fields().get(n).map(|f| f.data_type().clone()))
        .unwrap_or(DataType::Int64)
}

/// Number of output rows for this batch.
pub fn output_len(batch: &RecordBatch) -> usize {
    batch.num_rows()
}

/// A tiny deterministic SplitMix64 RNG for the seeded fixtures.
pub struct Rng(pub u64);
impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    /// Random i64 in [lo, hi] inclusive.
    pub fn range_i64(&mut self, lo: i64, hi: i64) -> i64 {
        if hi <= lo {
            return lo;
        }
        let span = (hi as i128 - lo as i128 + 1) as u128;
        lo.wrapping_add((self.next_u64() as u128 % span) as i64)
    }
    pub fn byte(&mut self) -> u8 {
        (self.next_u64() & 0xFF) as u8
    }
}

/// Process-volatile entropy source for VOLATILE fixtures.
pub fn volatile_seed() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static C: AtomicU64 = AtomicU64::new(0x1234_5678_9ABC_DEF0);
    let n = C.fetch_add(0x9E3779B97F4A7C15, Ordering::Relaxed);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    n ^ t
}

/// Read a column as a `&dyn Array`.
pub fn col<'a>(batch: &'a RecordBatch, i: usize) -> &'a dyn Array {
    batch.column(i).as_ref()
}

/// Re-export.
pub fn arc<T: Array + 'static>(a: T) -> ArrayRef {
    Arc::new(a)
}

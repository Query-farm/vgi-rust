// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Coverage flushing for the integration-suite worker (the `coverage` feature).
//!
//! Built with `-Cinstrument-coverage`, the LLVM profile runtime writes a
//! `.profraw` (to the `LLVM_PROFILE_FILE` path) via an `atexit` hook on a
//! *clean* process exit. But the pooled launcher worker and the long-lived http
//! worker are killed by the test harness without a clean exit, so that hook
//! never runs. A background thread here periodically calls the runtime's own
//! writer (`__llvm_profile_write_file`), so whenever (and however) the worker
//! dies, its latest counters are already on disk for the merge step.
//!
//! The FFI symbol is provided by the profiling runtime that `-Cinstrument-coverage`
//! links, which is why this module is gated behind the `coverage` feature — a
//! normal build links no such runtime.

use std::time::Duration;

extern "C" {
    fn __llvm_profile_write_file() -> std::os::raw::c_int;
}

/// Start periodic coverage flushes when `LLVM_PROFILE_FILE` is set (the env the
/// profiling runtime reads for its per-process output path, e.g.
/// `covdir/worker-%p.profraw`). A no-op otherwise.
pub fn start() {
    if std::env::var_os("LLVM_PROFILE_FILE").is_none() {
        return;
    }
    std::thread::Builder::new()
        .name("vgi-coverage".into())
        .spawn(|| loop {
            std::thread::sleep(Duration::from_secs(2));
            // SAFETY: the runtime writer is reentrant-safe to call repeatedly; a
            // concurrent counter update only risks a slightly stale snapshot.
            unsafe {
                __llvm_profile_write_file();
            }
        })
        .ok();
}

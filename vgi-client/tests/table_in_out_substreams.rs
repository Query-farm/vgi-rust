// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Table-in-out state reaches `finish()` from every substream of an execution.
//!
//! A client may fan one table-in-out execution across several connections —
//! each its own substream, with its own `substream_id` — and finalize it once,
//! carrying the primary's id (the Python reference client does). When one
//! worker process serves all of those connections, which is the normal case
//! under the launcher, TCP or HTTP, their state must neither overwrite each
//! other (the vgi-python bug: state keyed by process id) nor be invisible to
//! the single finalize (state scoped to the finalize's own substream).
//!
//! Driven against the real example worker, one process, over `--unix` so two
//! connections reach it: `substream_partial_sum` accumulates per batch and
//! emits one partial at finalize, so an undercount is a wrong number rather
//! than a missing row.

#![cfg(all(unix, feature = "unix"))]

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::{cast::AsArray, types::Int64Type, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use vgi_client::{AttachOptions, BindSpec, Bytes, FunctionType, ScanOptions, VgiClient};

fn example_worker() -> Option<PathBuf> {
    let mut dir = std::env::current_exe().ok()?;
    dir.pop();
    dir.pop();
    let exe = dir.join("vgi-example-worker");
    exe.exists().then_some(exe)
}

macro_rules! skip_without_worker {
    () => {
        if example_worker().is_none() {
            eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
            return;
        }
    };
}

/// One example-worker process serving an AF_UNIX socket, killed on drop.
struct UnixWorker {
    child: Child,
    path: PathBuf,
}

impl UnixWorker {
    fn start() -> Self {
        static N: AtomicUsize = AtomicUsize::new(0);
        let path = PathBuf::from(format!(
            "/tmp/vgi-tio-{}-{}.sock",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        let mut child = Command::new(example_worker().expect("worker"))
            .arg("--unix")
            .arg(&path)
            // A backstop only: the drop below kills it.
            .args(["--idle-timeout", "60"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn example worker");
        let mut line = String::new();
        BufReader::new(child.stdout.take().expect("stdout"))
            .read_line(&mut line)
            .expect("read discovery line");
        assert_eq!(
            line.trim_end(),
            format!("UNIX:{}", path.display()),
            "unexpected discovery line"
        );
        Self { child, path }
    }

    fn connect(&self) -> VgiClient {
        VgiClient::connect_unix(&self.path).expect("connect")
    }
}

impl Drop for UnixWorker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.path);
    }
}

fn batch(values: std::ops::Range<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(values.collect::<Vec<_>>()))],
    )
    .unwrap()
}

/// A fresh random-looking 16-byte substream id, as the clients mint them.
fn substream_id(tag: u8) -> Bytes {
    let mut id = vec![tag; 16];
    id[..4].copy_from_slice(&std::process::id().to_le_bytes());
    Bytes::from(id)
}

/// Fan one `substream_partial_sum` execution across two connections of one
/// worker process, then finalize once on the first, carrying `finalize_id`.
/// Returns the finalize's partial and the sum of everything sent.
fn fan_out_then_finalize(ids: [Option<Bytes>; 2], finalize_id: Option<Bytes>) -> (i64, i64) {
    let worker = UnixWorker::start();
    let mut primary = worker.connect();
    let mut secondary = worker.connect();
    let input_schema = Schema::new(vec![Field::new("n", DataType::Int64, true)]);

    let bind = |client: &mut VgiClient| {
        let cat = client
            .attach("example", AttachOptions::default())
            .expect("attach");
        let mut spec = BindSpec::table("substream_partial_sum").in_schema(cat.default_schema());
        spec.function_type = FunctionType::TableInOut;
        client
            .bind_with_input(&cat, &spec, &input_schema)
            .expect("bind substream_partial_sum")
    };
    let bound_primary = bind(&mut primary);
    let bound_secondary = bind(&mut secondary);

    // The primary opens the execution; the secondary joins it by id, as a
    // fanned-out connection does.
    let mut first = primary
        .open_exchange(
            &bound_primary,
            &ScanOptions {
                substream_id: ids[0].clone(),
                ..Default::default()
            },
        )
        .expect("open primary");
    let execution_id = first.execution_id().clone();
    let mut second = secondary
        .open_exchange(
            &bound_secondary,
            &ScanOptions {
                execution_id: Some(execution_id.clone()),
                substream_id: ids[1].clone(),
                ..Default::default()
            },
        )
        .expect("open secondary");
    assert_eq!(
        second.execution_id(),
        &execution_id,
        "the secondary joined another execution"
    );

    // Interleaved, so neither connection's batches all land before the other's.
    let mut expected = 0i64;
    for i in 0..10i64 {
        let (lo, hi) = (i * 100, i * 100 + 50);
        first.send(&batch(lo..hi)).expect("send primary");
        second.send(&batch(hi..hi + 50)).expect("send secondary");
        expected += (lo..hi + 50).sum::<i64>();
    }
    first.close().expect("close primary");
    second.close().expect("close secondary");
    drop(first);
    drop(second);
    drop(secondary);

    let opts = ScanOptions {
        substream_id: finalize_id,
        ..Default::default()
    };
    let out = primary
        .finalize_table_in_out_with_options(&bound_primary, &execution_id, &opts)
        .expect("finalize")
        .collect()
        .expect("drain finalize");
    let partials: Vec<i64> = out
        .iter()
        .filter(|b| b.num_rows() > 0)
        .flat_map(|b| {
            let a = b.column(0).as_primitive::<Int64Type>();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(
        partials.len(),
        1,
        "one finalize emits one partial: {partials:?}"
    );
    (partials[0], expected)
}

/// Two substreams of one execution, one process, distinct ids: `finish()`
/// must see both. Keyed by process (the reference's bug), one connection's
/// state overwrites the other's; scoped to the finalize's own substream (this
/// fixture's), the secondary's state is never read.
#[test]
fn two_substreams_of_one_execution_both_reach_finish() {
    skip_without_worker!();
    let primary = substream_id(1);
    let (got, want) = fan_out_then_finalize(
        [Some(primary.clone()), Some(substream_id(2))],
        Some(primary),
    );
    assert_eq!(
        got, want,
        "finish() undercounted: it did not see every substream of the execution"
    );
}

/// A client that sends no `substream_id` at all — an older client, or one
/// that does not fan out — still has every connection's state reach `finish()`.
#[test]
fn connections_that_send_no_substream_id_still_all_reach_finish() {
    skip_without_worker!();
    let (got, want) = fan_out_then_finalize([None, None], None);
    assert_eq!(got, want);
}

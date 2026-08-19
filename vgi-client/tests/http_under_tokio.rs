// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Driving the HTTP transport from inside a tokio runtime.
//!
//! Every call in this client blocks, so an async consumer — the DataFusion
//! provider, for one — reaches it through `spawn_blocking`. The HTTP transport
//! is `reqwest::blocking` underneath, which is exactly the combination that
//! panics when it is entered on a thread that is *driving* async tasks (it is
//! why the OAuth path was given `ureq` instead). Whether a tokio blocking-pool
//! thread counts as such a thread is not something to reason about from the
//! docs, so this asserts it against a real worker over a real socket.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

use vgi_client::{Arguments, AttachOptions, BindSpec, ScanOptions, VgiClient};

fn example_worker() -> Option<PathBuf> {
    let mut dir = std::env::current_exe().ok()?;
    dir.pop();
    dir.pop();
    let exe = dir.join(if cfg!(windows) {
        "vgi-example-worker.exe"
    } else {
        "vgi-example-worker"
    });
    exe.exists().then_some(exe)
}

/// A worker serving HTTP on an ephemeral port, killed on drop.
struct HttpWorker {
    child: Child,
    port: u16,
}

impl HttpWorker {
    fn start(exe: &PathBuf) -> Self {
        let mut child = Command::new(exe)
            .arg("--http")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn worker");
        // The worker announces its port on stdout before serving — that line is
        // its readiness contract, so reading it is also the wait.
        let stdout = child.stdout.take().expect("stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let port = loop {
            line.clear();
            let n = reader.read_line(&mut line).expect("read PORT line");
            assert!(n > 0, "worker exited before announcing a port");
            if let Some(p) = line.trim().strip_prefix("PORT:") {
                break p.parse::<u16>().expect("port");
            }
        };
        Self { child, port }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

impl Drop for HttpWorker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One whole scan, start to finish, over HTTP.
fn scan_over_http(url: &str) -> usize {
    let mut client = VgiClient::connect_http(url).expect("connect");
    scan_rows(&mut client)
}

/// The scan alone, with the client's construction and teardown left to the
/// caller — the two ends that behave differently under an ambient runtime.
fn scan_rows(client: &mut VgiClient) -> usize {
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();
    let spec = BindSpec::table("rowid_sequence")
        .in_schema(&schema_name)
        .with_arguments(Arguments::new().positional(5i64));
    let bound = client.bind(&cat, &spec).expect("bind");
    let mut scan = client.scan(&bound, &ScanOptions::default()).expect("init");
    scan.collect()
        .expect("drain")
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

/// The shape the DataFusion provider uses: a runtime, and the blocking client
/// entered through `spawn_blocking`.
#[test]
fn the_http_data_path_runs_inside_spawn_blocking() {
    let Some(exe) = example_worker() else {
        eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
        return;
    };
    let worker = HttpWorker::start(&exe);
    let url = worker.url();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let rows = rt.block_on(async move {
        tokio::task::spawn_blocking(move || scan_over_http(&url))
            .await
            .expect("the blocking HTTP path must not panic under an ambient runtime")
    });
    assert_eq!(rows, 5);
}

/// The shape a caller gets wrong: blocking directly on a runtime thread.
///
/// Recorded as a test rather than left to folklore, because the difference
/// between the two shapes is the entire reason `spawn_blocking` is not
/// optional. `catch_unwind` keeps the observation from taking the harness with
/// it if the answer is "panics".
#[test]
fn blocking_on_a_runtime_thread_is_the_shape_to_avoid() {
    let Some(exe) = example_worker() else {
        eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
        return;
    };
    let worker = HttpWorker::start(&exe);
    let url = worker.url();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    // Stages are recorded as they pass, so a panic can be attributed to the
    // step that raised it rather than to "somewhere in the scan".
    let stages: std::sync::Arc<std::sync::Mutex<Vec<&'static str>>> = Default::default();
    let seen = std::sync::Arc::clone(&stages);
    let outcome = rt.block_on(async move {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let mut client = VgiClient::connect_http(&url).expect("connect");
            seen.lock().unwrap().push("connect");
            let rows = scan_rows(&mut client);
            seen.lock().unwrap().push("scan");
            drop(client);
            seen.lock().unwrap().push("drop");
            rows
        }))
    });
    let reached = stages.lock().unwrap().clone();
    match outcome {
        Ok(rows) => assert_eq!(rows, 5),
        Err(_) => eprintln!(
            "note: the blocking HTTP path panics on a runtime thread after reaching {reached:?}; \
             enter it through spawn_blocking"
        ),
    }
}

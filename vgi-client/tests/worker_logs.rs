// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! A worker's in-band log batches reach the `log` facade.
//!
//! The channel only exists in one direction and only while a call is in
//! flight: a worker explains a slow or empty scan by emitting zero-row batches
//! carrying `vgi_rpc.log_level` beside its data. With no sink installed the
//! transport classified each one and dropped it, so this asserts against a real
//! worker that the messages survive the whole way to a `log` record.

use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use vgi_client::{Arguments, AttachOptions, BindSpec, ScanOptions, VgiClient};

fn captured() -> &'static Mutex<Vec<(log::Level, String)>> {
    static LINES: OnceLock<Mutex<Vec<(log::Level, String)>>> = OnceLock::new();
    LINES.get_or_init(|| Mutex::new(Vec::new()))
}

struct CapturingLogger;

impl log::Log for CapturingLogger {
    fn enabled(&self, _md: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if record.target() == "vgi::worker" {
            captured()
                .lock()
                .unwrap()
                .push((record.level(), record.args().to_string()));
        }
    }

    fn flush(&self) {}
}

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

#[test]
fn worker_log_batches_reach_the_log_facade() {
    let Some(worker) = example_worker() else {
        eprintln!("skipping: vgi-example-worker not built (run `cargo build --workspace`)");
        return;
    };
    // The global logger can be installed once per process, which is why this
    // test owns its own binary.
    static LOGGER: CapturingLogger = CapturingLogger;
    log::set_logger(&LOGGER).expect("install logger");
    log::set_max_level(log::LevelFilter::Trace);

    let mut client = VgiClient::connect_subprocess(&[worker.as_os_str()]).expect("connect");
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    let schema_name = cat.default_schema().to_string();

    let spec = BindSpec::table("logging_generator")
        .in_schema(&schema_name)
        .with_arguments(Arguments::new().positional(3i64));
    let bound = client.bind(&cat, &spec).expect("bind");
    let mut scan = client.scan(&bound, &ScanOptions::default()).expect("init");
    let batches = scan.collect().expect("drain");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);

    let lines = captured().lock().unwrap().clone();
    assert!(
        lines
            .iter()
            .any(|(lvl, m)| *lvl == log::Level::Info && m.contains("Starting generation of 3")),
        "the worker's opening log never arrived: {lines:?}"
    );
    assert!(
        lines.iter().any(|(_, m)| m.contains("Generation complete")),
        "the worker's closing log never arrived: {lines:?}"
    );
}

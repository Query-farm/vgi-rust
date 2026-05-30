//! VGI example worker binary — the integration-test fixture set.
//!
//! Registers every example function (scalar / table / table-in-out /
//! aggregate / buffering) and serves the catalog named by
//! `VGI_WORKER_CATALOG_NAME` (default `example`). Transport is selected from
//! argv: stdio (default) or `--unix <path>` (launcher).

mod aggregate;
mod catalog_def;
mod buffering;
mod scalar;
mod table;
mod table_in_out;

use vgi::Worker;

fn main() {
    // Logs go to stderr — stdout is the Arrow-IPC channel.
    let _ = env_logger::Builder::from_env(
        env_logger::Env::default().filter_or("VGI_LOG", "info"),
    )
    .format_timestamp_millis()
    .try_init();

    let mut worker = Worker::new();
    scalar::register(&mut worker);
    table::register(&mut worker);
    table_in_out::register(&mut worker);
    buffering::register(&mut worker);
    aggregate::register(&mut worker);
    worker.set_catalog(catalog_def::build());
    worker.run();
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! VGI example worker binary — the integration-test fixture set.
//!
//! Registers every example function (scalar / table / table-in-out /
//! aggregate / buffering) and serves the catalog named by
//! `VGI_WORKER_CATALOG_NAME` (default `example`). Transport is selected from
//! argv: stdio (default) or `--unix <path>` (launcher).

mod accumulate;
mod aggregate;
mod attach_options;
mod buffering;
mod catalog_def;
mod copy_from;
mod copy_to;
#[cfg(feature = "coverage")]
mod coverage;
mod narrow_bind;
mod scalar;
mod table;
mod table_in_out;

use vgi::Worker;

fn main() {
    // Coverage build only: start periodic .profraw snapshots so a worker the
    // harness kills abruptly still records what it exercised.
    #[cfg(feature = "coverage")]
    coverage::start();

    // Logs go to stderr — stdout is the Arrow-IPC channel.
    let _ = env_logger::Builder::from_env(env_logger::Env::default().filter_or("VGI_LOG", "info"))
        .format_timestamp_millis()
        .try_init();

    let catalog_name =
        std::env::var("VGI_WORKER_CATALOG_NAME").unwrap_or_else(|_| "example".into());

    let mut worker = Worker::new();
    scalar::register(&mut worker);
    table::register(&mut worker, &catalog_name);
    table_in_out::register(&mut worker);
    buffering::register(&mut worker);
    aggregate::register(&mut worker);
    register_secrets_and_settings(&mut worker);
    // `echo_attach_options` is only part of the attach_options catalog's surface.
    if catalog_name == "attach_options" {
        attach_options::register(&mut worker);
    }
    let catalog = if catalog_name == "attach_options" {
        attach_options::catalog()
    } else {
        catalog_def::build_by_name(&catalog_name)
    };
    // The `accumulate` fixture catalog is served (MetaWorker-style) alongside
    // the example catalog — the accumulate tests attach it via the plain worker.
    if catalog.name == "example" {
        // Custom COPY ... FROM format reader (example_lines) — only on the
        // primary `example` catalog, matching the Python fixture worker.
        copy_from::register(&mut worker);
        // Custom COPY ... TO format writers (example_lines_out +
        // example_lines_ordered_out) — only on the primary `example` catalog.
        copy_to::register(&mut worker);
        accumulate::register(&mut worker);
        worker.register_secondary_catalog(accumulate::catalog(), accumulate::function_names());
        narrow_bind::register(&mut worker);
        worker.register_secondary_catalog(narrow_bind::catalog(), narrow_bind::function_names());
    }
    worker.set_catalog(catalog);
    worker.run();
}

/// Register the `vgi_example` secret type and the custom settings the
/// settings/secret fixtures exercise.
fn register_secrets_and_settings(worker: &mut Worker) {
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;
    use vgi::catalog::{SecretTypeSpec, SettingSpec};

    let redact = || HashMap::from([("redact".to_string(), "true".to_string())]);
    let params = Schema::new(vec![
        Field::new("secret_string", DataType::Utf8, true).with_metadata(redact()),
        Field::new("api_key", DataType::Utf8, true).with_metadata(redact()),
        Field::new("port", DataType::Int32, true),
        Field::new("use_ssl", DataType::Boolean, true),
        Field::new("timeout", DataType::Float64, true),
    ]);
    worker.register_secret_type(SecretTypeSpec {
        name: "vgi_example".to_string(),
        description: "Example VGI secret for testing".to_string(),
        parameters_schema: Arc::new(params),
    });

    let config_struct = DataType::Struct(
        vec![
            Field::new("start", DataType::Int64, true),
            Field::new("step", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ]
        .into(),
    );
    for (name, ty) in [
        ("vgi_verbose_mode", DataType::Boolean),
        ("greeting", DataType::Utf8),
        ("multiplier", DataType::Int64),
        ("threshold", DataType::Int64),
        ("scale_factor", DataType::Float64),
        ("config", config_struct),
    ] {
        worker.register_setting(SettingSpec {
            name: name.to_string(),
            description: format!("{name} setting"),
            data_type: ty,
        });
    }
}

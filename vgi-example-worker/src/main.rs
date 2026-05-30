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
    register_secrets_and_settings(&mut worker);
    let catalog_name = std::env::var("VGI_WORKER_CATALOG_NAME").unwrap_or_else(|_| "example".into());
    worker.set_catalog(catalog_def::build_by_name(&catalog_name));
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
        ("config", config_struct),
    ] {
        worker.register_setting(SettingSpec {
            name: name.to_string(),
            description: format!("{name} setting"),
            data_type: ty,
        });
    }
}

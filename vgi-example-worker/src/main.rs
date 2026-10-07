// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! VGI example worker binary — the integration-test fixture set.
//!
//! Registers every example function (scalar / table / table-in-out /
//! aggregate / buffering) and serves the catalog named by
//! `VGI_WORKER_CATALOG_NAME` (default `example`). Transport is selected from
//! argv: stdio (default) or `--unix <path>` (launcher). `VGI_CATALOG_CONTENTS=0`
//! stops advertising the `catalog_contents` bulk-load RPC;
//! `VGI_CATALOG_CONTENTS_ETAG` / `VGI_CATALOG_CONTENTS_CACHE` choose how it
//! revalidates and whether it is cached (see `contents_reval.rs`).

mod accumulate;
mod aggregate;
mod attach_options;
mod buffering;
mod catalog_def;
mod contents_fixtures;
mod contents_reval;
mod copy_from;
mod copy_to;
#[cfg(feature = "coverage")]
mod coverage;
mod datafusion_companion;
mod global_functions;
mod narrow_bind;
mod same_name;
mod scalar;
mod secret_cache;
mod table;
mod table_in_out;
mod ticket_probe;
mod twin_catalogs;

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
    // Advertise `catalog_contents` (protocol 2.1.0; the framework default) unless
    // VGI_CATALOG_CONTENTS=0, which forces the client back onto the per-schema
    // RPCs — so the integration suite can be run over both load paths.
    worker.set_catalog_contents(catalog_contents_enabled());
    host_conformance_protocols(&mut worker);
    if catalog_name == datafusion_companion::ROOT_CATALOG {
        datafusion_companion::register(&mut worker);
        worker.set_catalog(datafusion_companion::root_catalog());
        run_worker(worker);
        return;
    }
    scalar::register(&mut worker);
    table::register(&mut worker, &catalog_name);
    table_in_out::register(&mut worker);
    // Secret-dependent cacheable fixtures, one per kind (producer / scalar /
    // blended map): cached per secret fingerprint. See secret_cache.rs.
    secret_cache::register(&mut worker);
    buffering::register(&mut worker);
    aggregate::register(&mut worker);
    register_secrets_and_settings(&mut worker);
    // `echo_attach_options` is only part of the attach_options catalog's surface.
    if catalog_name == "attach_options" {
        attach_options::register(&mut worker);
    }
    let mut catalog = if catalog_name == "attach_options" {
        attach_options::catalog()
    } else {
        catalog_def::build_by_name(&catalog_name)
    };
    // The gated catalog rides alongside `attach_options` on the same worker, so
    // one LOCATION serves both — which is what attach_options_required.test
    // asserts when it queries vgi_catalogs() for the two of them.
    if catalog_name == "attach_options" {
        worker.register_secondary_catalog(attach_options::required_catalog(), Vec::new());
    }
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
        // One name in two schemas of `example` (main + data) — only the schema
        // the caller names can tell the two implementations apart. The scalar
        // pair binds; the exchange / buffered / aggregate pairs also *run*
        // through RPCs that re-resolve by name (protocol 1.2.0).
        scalar::same_name::register(&mut worker);
        same_name::register(&mut worker);
        // ... and the same name in the `main` schema of two *catalogs*, where
        // only the attachment tells them apart.
        twin_catalogs::register(&mut worker);
        // ticket_probe: attach tickets (vgi.attach_tickets.v1) -- one plain and
        // one secret attach option whose effect a table reveals. Served by
        // every SDK's fixture worker; see ticket_probe.rs.
        ticket_probe::register(&mut worker);
        // The six catalog_contents fixture catalogs (contents_probe / _broken /
        // _legacy / _memory / _reval / _hash) the cross-SDK
        // catalog_contents*.test files attach. See contents_fixtures.rs.
        contents_fixtures::register(&mut worker);
        // Global-registration probes — one per function type. They are ordinary
        // `main`-schema members of the example catalog (a global function must
        // be schema-resident, since bind dispatch is keyed on (schema, name))
        // AND are listed in the catalog's `global_functions` so the client is
        // asked to publish them under `vgi_example_*`.
        global_functions::register(&mut worker);
    }
    // Revalidating catalog_contents (a generation-counter etag by default);
    // see contents_reval.rs.
    contents_reval::configure(&mut catalog);
    worker.set_catalog(catalog);
    run_worker(worker);
}

/// `VGI_CATALOG_CONTENTS`: unset or anything but `0` / `false` / `off` → on.
fn catalog_contents_enabled() -> bool {
    !matches!(
        std::env::var("VGI_CATALOG_CONTENTS")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "0" | "false" | "off"
    )
}

/// The cross-SDK conformance surface (vgi-rpc's MULTI_PROTOCOL_HOSTING.md §7).
///
/// `conformance.Secondary.v1` is hosted beside `vgi.v2` through the
/// [`Worker::hosted_protocols`] hook on every transport -- additive: no
/// existing fixture function changes, and `vgi.v2` dispatch cannot be
/// affected because requests route on their protocol key.
///
/// With `--identity`, the worker also opts into `vgi_rpc.Identity.v1` (HTTP
/// only) under the fixed `IDENTITY_CONFORMANCE_FIXTURE.md` policy, including
/// the auth-unavailable token and purpose, and authenticates callers from the
/// fixture's `X-Conformance-Principal` / `X-Conformance-Auth-Time` headers.
/// That authenticator is trivially spoofable: test use only.
fn host_conformance_protocols(worker: &mut Worker) {
    use vgi_rpc::conformance_identity as fixture;

    worker.hosted_protocols(|| {
        vec![vgi_rpc::conformance_secondary::conformance_secondary_protocol()]
    });
    if !std::env::args().any(|arg| arg == "--identity") {
        if let Some(auth) = optional_test_bearers() {
            worker.authenticate(auth);
        }
    }
    if std::env::args().any(|arg| arg == "--identity") {
        worker.authenticate(std::sync::Arc::new(fixture::authenticate_from_headers));
        worker.resolve_token(fixture::conformance_resolve_token);
        worker.mint_grant(fixture::conformance_mint_grant);
        worker.introspect_principals([fixture::INTROSPECTOR_PRINCIPAL]);
    }
}

/// The fixture's optional test bearers (`VGI_OPTIONAL_BEARER_TOKENS`,
/// `token=principal,…`, e.g. `vgi-test-alice=alice,vgi-test-bob=bob`).
///
/// Mirrors vgi-python's fixture HTTP server: a known token is a **fresh login**
/// (it stamps `auth_time` = now), so a client can call `issue_grant` with
/// nothing but a bearer DuckDB can send -- which the attach-ticket tests need.
/// A `Bearer vgig1.…` is not ours: it stays anonymous here so the sealed-grant
/// authenticator vgi-rpc appends after this one gets it. No token, a blank one
/// or an unknown one is anonymous, never a 401 from this callback.
fn optional_test_bearers() -> Option<vgi_rpc::Authenticate> {
    let raw = std::env::var("VGI_OPTIONAL_BEARER_TOKENS").ok()?;
    let tokens: std::collections::HashMap<String, String> = raw
        .split(',')
        .filter_map(|pair| pair.split_once('='))
        .map(|(t, p)| (t.trim().to_string(), p.trim().to_string()))
        .collect();
    assert!(
        !tokens.is_empty(),
        "VGI_OPTIONAL_BEARER_TOKENS is set but contains no `token=principal` pair"
    );
    Some(std::sync::Arc::new(
        move |req: &vgi_rpc::AuthRequest<'_>| {
            let token = req
                .header("authorization")
                .and_then(|h| {
                    h.strip_prefix("Bearer ")
                        .or_else(|| h.strip_prefix("bearer "))
                })
                .map(str::trim);
            Ok(match token.and_then(|t| tokens.get(t)) {
                Some(principal) => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or(0.0);
                    vgi_rpc::AuthContext::for_principal("bearer", principal.clone())
                        .with_claim("auth_time", now.to_string())
                }
                None => vgi_rpc::AuthContext::anonymous(),
            })
        },
    ))
}

/// Serve the example catalog behind the local Iroh HTTP bridge used by the
/// browser demo. This mode is deliberately explicit: ordinary `--http`
/// retains its existing authentication configuration.
fn run_worker(worker: Worker) {
    if !std::env::args().any(|arg| arg == "--http-iroh-demo") {
        worker.run();
        return;
    }

    let provider = vgi_rpc::iroh_forwarded_header_provider(
        vgi_rpc::IrohForwardedHeaderConfig::new("iroh:browser-demo", ["127.0.0.1"])
            .expect("valid browser demo Iroh identity config"),
    )
    .expect("valid browser demo Iroh provider");
    let state = vgi_rpc::http::HttpState::builder()
        .server(std::sync::Arc::new(
            worker.build_server_for(vgi::ServeTransport::Http),
        ))
        .peer_identity_providers([provider])
        .peer_authentication_policy(vgi_rpc::peer_identity_primary("iroh"))
        .enable_sticky(true)
        .producer_batch_limit(1)
        .build();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind browser demo worker");
        println!(
            "PORT:{}",
            listener.local_addr().expect("local address").port()
        );
        use std::io::Write as _;
        std::io::stdout().flush().ok();
        vgi_rpc::http::serve_with_shutdown(state, listener)
            .await
            .expect("serve browser demo worker");
    });
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

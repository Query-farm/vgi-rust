// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The VGI worker: owns function registries (via [`Dispatcher`]), builds the
//! RPC server, and drives transport selection from argv.

use std::sync::Arc;

use vgi_rpc::RpcServer;

use crate::dispatch::Dispatcher;
use crate::function::ScalarFunction;
use crate::protocol::register;

/// VGI wire protocol version advertised to the C++ extension.
pub const VGI_PROTOCOL_VERSION: &str = "1.0.0";
/// RPC protocol name; must match the Python `VgiProtocol`.
pub const VGI_PROTOCOL_NAME: &str = "VgiProtocol";

/// A VGI worker: the process DuckDB launches and talks to.
///
/// Build one with [`Worker::new`], register one or more functions
/// ([`register_scalar`](Self::register_scalar),
/// [`register_table`](Self::register_table),
/// [`register_aggregate`](Self::register_aggregate), …) and/or a catalog
/// ([`set_catalog`](Self::set_catalog)), then call [`run`](Self::run) to serve.
/// `run` does not return — it serves until DuckDB disconnects.
///
/// # Examples
///
/// ```no_run
/// use vgi::Worker;
/// # use vgi::{ArgSpec, FunctionMetadata, ProcessParams, ScalarFunction};
/// # use vgi_rpc::Result;
/// # struct UpperCase;
/// # impl ScalarFunction for UpperCase {
/// #     fn name(&self) -> &str { "upper_case" }
/// #     fn metadata(&self) -> FunctionMetadata { Default::default() }
/// #     fn argument_specs(&self) -> Vec<ArgSpec> { vec![] }
/// #     fn process(&self, _p: &ProcessParams, b: &arrow_array::RecordBatch)
/// #         -> Result<arrow_array::RecordBatch> { Ok(b.clone()) }
/// # }
/// fn main() {
///     let mut worker = Worker::new();
///     worker.register_scalar(UpperCase);
///     worker.run(); // never returns
/// }
/// ```
pub struct Worker {
    disp: Dispatcher,
    server_id: Option<String>,
}

impl Default for Worker {
    fn default() -> Self {
        Worker::new()
    }
}

impl Worker {
    /// Create a worker.
    ///
    /// The catalog name DuckDB sees in `ATTACH 'name' (TYPE vgi, …)` defaults to
    /// `example` and can be overridden with the `VGI_WORKER_CATALOG_NAME`
    /// environment variable. (In SQL you qualify functions by the *alias* you
    /// give `ATTACH`, not by this internal name.)
    pub fn new() -> Self {
        let catalog_name =
            std::env::var("VGI_WORKER_CATALOG_NAME").unwrap_or_else(|_| "example".to_string());
        Worker {
            disp: Dispatcher::new(catalog_name),
            server_id: None,
        }
    }

    /// Override the server id.
    pub fn server_id(mut self, id: impl Into<String>) -> Self {
        self.server_id = Some(id.into());
        self
    }

    /// Register a scalar function.
    pub fn register_scalar(&mut self, f: impl ScalarFunction + 'static) {
        self.disp.register_scalar(Arc::new(f));
    }

    /// Register a table (producer) function.
    pub fn register_table(&mut self, f: impl crate::table_function::TableFunction + 'static) {
        self.disp.register_table(Arc::new(f));
    }

    /// Register a table-in-out function.
    pub fn register_table_in_out(
        &mut self,
        f: impl crate::table_in_out::TableInOutFunction + 'static,
    ) {
        self.disp.register_table_in_out(Arc::new(f));
    }

    /// Register a table-buffering function.
    pub fn register_buffering(
        &mut self,
        f: impl crate::buffering::TableBufferingFunction + 'static,
    ) {
        self.disp.register_buffering(Arc::new(f));
    }

    /// Register an aggregate function.
    pub fn register_aggregate(&mut self, f: impl crate::aggregate::AggregateFunction + 'static) {
        self.disp.register_aggregate(Arc::new(f));
    }

    /// Install the declarative catalog (views / macros / tables).
    ///
    /// Any catalog table built with [`crate::catalog::CatTable::with_function`]
    /// carries an embedded scan function; these are auto-registered into the
    /// dispatch table here (deduped by name), so a function-backed table needs no
    /// separate [`Worker::register_table`] call — parity with the Go
    /// `CatalogTable.Function` ergonomics.
    pub fn set_catalog(&mut self, model: crate::catalog::CatalogModel) {
        let base = model.schemas.iter();
        let versioned = model.version_schemas.values().flatten();
        for schema in base.chain(versioned) {
            for table in &schema.tables {
                if let Some(f) = &table.scan_function_impl {
                    self.disp.register_table_if_absent(f.clone());
                }
            }
        }
        self.disp.set_catalog(model);
    }

    /// Add a secondary catalog served alongside the primary (MetaWorker model):
    /// advertised by `catalog_catalogs` and attachable by its name. `functions`
    /// names the worker-global functions it owns (scopes its function listing).
    pub fn register_secondary_catalog(
        &mut self,
        model: crate::catalog::CatalogModel,
        functions: Vec<String>,
    ) {
        self.disp.register_secondary_catalog(model, functions);
    }

    /// Register a secret type (surfaced via `catalog_attach`).
    pub fn register_secret_type(&mut self, spec: crate::catalog::SecretTypeSpec) {
        self.disp.register_secret_type(spec);
    }

    /// Register a custom setting (surfaced via `catalog_attach`).
    pub fn register_setting(&mut self, spec: crate::catalog::SettingSpec) {
        self.disp.register_setting(spec);
    }

    /// Build the configured [`RpcServer`], registering every VGI method.
    pub fn build_server(self) -> RpcServer {
        let server_id = self
            .server_id
            .clone()
            .unwrap_or_else(|| "vgi-rust-worker".to_string());
        // The `bad_protocol` fixture advertises an incompatible version via
        // this env override so the C++ ATTACH fails with a clear mismatch.
        let protocol_version = std::env::var("VGI_PROTOCOL_VERSION_OVERRIDE")
            .unwrap_or_else(|_| VGI_PROTOCOL_VERSION.to_string());
        let mut srv = RpcServer::builder()
            .server_id(server_id)
            .protocol_name(VGI_PROTOCOL_NAME)
            .protocol_version(protocol_version)
            .enable_describe(true)
            .build();
        register::register(&mut srv, Arc::new(self.disp));
        srv
    }

    /// Parse `argv` and serve over the selected transport, blocking until the
    /// connection closes.
    ///
    /// DuckDB launches the worker with the right flags; you normally just call
    /// `run()` from `main`. The transport is chosen from `argv`:
    ///
    /// - *(none)* — **stdio** (the default).
    /// - `--unix <path>` — **Unix-socket** launcher transport
    ///   (`--idle-timeout <secs>` optional; Unix only).
    /// - `--tcp [<host>:]<port>` — **TCP** launcher transport (raw Arrow-IPC
    ///   framing, no auth/TLS; host defaults to `127.0.0.1`, port `0`
    ///   auto-selects; `--idle-timeout <secs>` optional).
    /// - `--http` — **HTTP** transport (Arrow-IPC over HTTP). Bearer auth is
    ///   enabled by setting `VGI_BEARER_TOKENS` (`token=principal,…`).
    pub fn run(self) {
        let args: Vec<String> = std::env::args().collect();
        let server = Arc::new(self.build_server());

        if args.iter().any(|a| a == "--http") {
            crate::transport::serve_http(server, build_authenticate());
            return;
        }

        if let Some(i) = args.iter().position(|a| a == "--tcp") {
            let spec = args.get(i + 1).expect("--tcp requires [HOST:]PORT").clone();
            let (host, port) = parse_tcp_spec(&spec);
            let idle = args
                .iter()
                .position(|a| a == "--idle-timeout")
                .and_then(|j| args.get(j + 1))
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(300.0);
            crate::transport::serve_tcp(server, &host, port, idle);
            return;
        }

        if let Some(i) = args.iter().position(|a| a == "--unix") {
            #[cfg(unix)]
            {
                let path = args
                    .get(i + 1)
                    .expect("--unix requires a socket path")
                    .clone();
                let idle = args
                    .iter()
                    .position(|a| a == "--idle-timeout")
                    .and_then(|j| args.get(j + 1))
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(300.0);
                crate::transport::serve_unix(server, &path, idle);
                return;
            }
            #[cfg(not(unix))]
            {
                let _ = i;
                eprintln!("the --unix launcher transport is only supported on Unix platforms");
                std::process::exit(1);
            }
        }

        crate::transport::serve_stdio(server);
    }
}

/// Parse a `[HOST:]PORT` `--tcp` bind spec. A bare `PORT` (no colon) binds
/// `127.0.0.1`; an empty host (leading `":"`) also defaults to loopback.
fn parse_tcp_spec(spec: &str) -> (String, u16) {
    match spec.rsplit_once(':') {
        Some((host, port)) => {
            let host = if host.is_empty() { "127.0.0.1" } else { host };
            (
                host.to_string(),
                port.parse::<u16>().expect("--tcp expects [HOST:]PORT"),
            )
        }
        None => (
            "127.0.0.1".to_string(),
            spec.parse::<u16>().expect("--tcp expects [HOST:]PORT"),
        ),
    }
}

/// Build the HTTP bearer-auth callback from the environment. Returns `None`
/// (anonymous-only) unless `VGI_BEARER_TOKENS` (`token=principal,…`) or
/// `VGI_TEST_BEARER_TOKEN` is set. A bearer token that is *present but invalid*
/// is rejected (401); a missing token is allowed as anonymous so the same
/// server can serve the non-auth tests.
fn build_authenticate() -> Option<vgi_rpc::Authenticate> {
    let mut tokens: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    if let Ok(pairs) = std::env::var("VGI_BEARER_TOKENS") {
        for pair in pairs.split(',') {
            if let Some((tok, principal)) = pair.split_once('=') {
                tokens.insert(tok.trim().to_string(), principal.trim().to_string());
            }
        }
    }
    if let Ok(tok) = std::env::var("VGI_TEST_BEARER_TOKEN") {
        tokens
            .entry(tok)
            .or_insert_with(|| "test-principal".to_string());
    }
    if tokens.is_empty() {
        return None;
    }
    Some(std::sync::Arc::new(
        move |req: &vgi_rpc::AuthRequest<'_>| {
            let token = req
                .header("authorization")
                .and_then(|h| {
                    h.strip_prefix("Bearer ")
                        .or_else(|| h.strip_prefix("bearer "))
                })
                .map(|t| t.trim());
            match token {
                // A server with tokens configured is bearer-protected: reject
                // anonymous (no/blank token) access. A server with NO tokens never
                // installs this callback, so it allows all (the non-auth tests).
                None => Err(vgi_rpc::RpcError::permission_error(
                    "bearer token required but not provided",
                )),
                Some(tok) => match tokens.get(tok) {
                    Some(principal) => Ok(vgi_rpc::AuthContext {
                        domain: "bearer".to_string(),
                        authenticated: true,
                        principal: principal.clone(),
                        claims: Default::default(),
                    }),
                    None => Err(vgi_rpc::RpcError::permission_error(
                        "bearer token was rejected",
                    )),
                },
            }
        },
    ))
}

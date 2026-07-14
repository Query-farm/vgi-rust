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

    /// Hide an already-registered function from the catalog's advertised
    /// function list. It stays bindable, so a function-backed catalog table can
    /// still resolve it as a scan function, but the client creates no SQL
    /// callable for it — use this when the table is the only intended entry
    /// point.
    pub fn hide_function(&mut self, name: impl Into<String>) {
        self.disp.hide_function(name);
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

    /// Register a custom `COPY ... FROM` format reader.
    ///
    /// The reader is exposed two ways: as a producer-mode table function (so the
    /// whole table bind/init/scan path is reused) and as an advertised
    /// `COPY ... FROM` format via `catalog_copy_from_formats`. Users then run
    /// `COPY target FROM 'path' (FORMAT <alias>.<format>, opt val, ...)`.
    /// See [`crate::copy_from::CopyFromFunction`].
    pub fn register_copy_from(&mut self, f: impl crate::copy_from::CopyFromFunction + 'static) {
        let arc: Arc<dyn crate::copy_from::CopyFromFunction> = Arc::new(f);
        self.disp
            .register_table(Arc::new(crate::copy_from::CopyFromTable(arc.clone())));
        self.disp.register_copy_from(arc);
    }

    /// Register a custom `COPY ... TO` format writer.
    ///
    /// The writer is exposed two ways: as a table-buffering (Sink+Combine)
    /// function (so the whole buffering RPC path is reused — `write()` per shard,
    /// `close()` for the terminal destination write; no Source phase) and as an
    /// advertised `COPY ... TO` format via `catalog_copy_from_formats`
    /// (`direction="to"`). Users then run
    /// `COPY (source) TO 'path' (FORMAT <alias>.<format>, opt val, ...)`.
    /// See [`crate::copy_to::CopyToFunction`].
    pub fn register_copy_to(&mut self, f: impl crate::copy_to::CopyToFunction + 'static) {
        let arc: Arc<dyn crate::copy_to::CopyToFunction> = Arc::new(f);
        self.disp
            .register_buffering(Arc::new(crate::copy_to::CopyToBuffering(arc.clone())));
        self.disp.register_copy_to(arc);
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

    /// Advertise a companion catalog for the client to ATTACH at VGI-attach time
    /// (surfaced via `catalog_attach.attach_catalogs`; lakehouse federation).
    pub fn register_attach_catalog(&mut self, info: crate::protocol::dtos::AttachCatalogInfo) {
        self.disp.register_attach_catalog(info);
    }

    /// Build the configured [`RpcServer`], registering every VGI method.
    pub fn build_server(self) -> RpcServer {
        self.build_parts().0
    }

    /// Build the [`RpcServer`] and return the shared [`Dispatcher`] handle
    /// alongside it. The HTTP transport reuses the dispatcher to serve the
    /// landing contract (`describe.json`) via catalog introspection.
    fn build_parts(self) -> (RpcServer, Arc<Dispatcher>) {
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
        let disp = Arc::new(self.disp);
        register::register(&mut srv, disp.clone());
        (srv, disp)
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
        // Capture the worker's display name / doc from the primary catalog
        // before the dispatcher is moved into the server (used by the HTTP
        // landing contract).
        let worker_name = self.disp.catalog.name.clone();
        let worker_doc = self.disp.catalog.comment.clone().unwrap_or_default();
        let (server, disp) = self.build_parts();
        let server = Arc::new(server);

        #[cfg(feature = "transport-http")]
        if args.iter().any(|a| a == "--http") {
            let provider: Arc<dyn vgi_rpc::http::DescribeProvider> = Arc::new(
                crate::describe::VgiDescribeProvider::new(disp, worker_name, worker_doc),
            );
            crate::transport::serve_http(server, build_authenticate(), Some(provider));
            return;
        }
        // The dispatcher handle is only needed by the HTTP landing contract.
        let _ = (&disp, &worker_name, &worker_doc);

        // Native thread-per-connection TCP. (A wasm single-thread serve_tcp is
        // wired separately for the wasip2 shared-worker path.)
        #[cfg(not(target_arch = "wasm32"))]
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

    /// Serve the worker's RPC protocol over an arbitrary byte stream (used by the
    /// SAB transport and native tests). Blocking; consumes the worker.
    pub fn serve_reader_writer<R: std::io::Read, W: std::io::Write>(self, mut r: R, mut w: W) {
        let (server, _disp) = self.build_parts();
        std::sync::Arc::new(server).serve(&mut r, &mut w);
    }
}

/// Parse a `[HOST:]PORT` `--tcp` bind spec. A bare `PORT` (no colon) binds
/// `127.0.0.1`; an empty host (leading `":"`) also defaults to loopback.
#[cfg(not(target_arch = "wasm32"))]
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

/// Parse a `token=principal,…` environment value into a lookup map.
///
/// Returns `None` when `var` is unset or blank. Returns `Some(map)` when it is
/// set — and **panics** when a set value yields no usable entries, because the
/// alternative is to silently serve a worker the operator believes is protected.
/// A `token` with no `=principal` is a config error, not an empty config.
#[cfg(feature = "transport-http")]
fn parse_token_map(var: &str) -> Option<std::collections::HashMap<String, String>> {
    let raw = std::env::var(var).ok()?;
    if raw.trim().is_empty() {
        return None;
    }
    let mut tokens = std::collections::HashMap::new();
    for pair in raw.split(',') {
        if let Some((tok, principal)) = pair.split_once('=') {
            tokens.insert(tok.trim().to_string(), principal.trim().to_string());
        }
    }
    assert!(
        !tokens.is_empty(),
        "{var} is set but contains no `token=principal` pair (got {raw:?}); \
         refusing to start rather than serve an unprotected worker"
    );
    Some(tokens)
}

/// Extract the bearer token from an `Authorization` header, if present.
///
/// A present-but-blank token (`Authorization: Bearer `) yields `Some("")`, not
/// `None`: the caller *did* offer a bearer credential, it is simply not a valid
/// one. The required-bearer path must reject it as such, and the optional path
/// must fall through to anonymous — collapsing it to `None` here would change
/// which of those two answers the required path gives.
#[cfg(feature = "transport-http")]
fn bearer_token<'a>(req: &'a vgi_rpc::AuthRequest<'a>) -> Option<&'a str> {
    req.header("authorization")
        .and_then(|h| {
            h.strip_prefix("Bearer ")
                .or_else(|| h.strip_prefix("bearer "))
        })
        .map(str::trim)
}

/// Build the HTTP bearer-auth callback from the environment. Returns `None`
/// (anonymous-only) unless one of two variables is set.
///
/// `VGI_BEARER_TOKENS` (`token=principal,…`) makes the server **bearer-protected**:
/// a missing or invalid token is rejected (401).
///
/// `VGI_OPTIONAL_BEARER_TOKENS` (same format) makes bearer identity **optional**:
/// a known token resolves to its principal, and no/blank/unknown token falls back
/// to anonymous — never a 401. That lets one shared server host both anonymous
/// tests and tests that need distinct principals (e.g. the result cache's
/// identity-isolation test attaching the same worker as alice and as bob).
/// `VGI_BEARER_TOKENS` wins when both are set.
///
/// Either variable set to an unparseable value aborts startup (see
/// [`parse_token_map`]) rather than quietly serving everyone.
///
/// `VGI_TEST_BEARER_TOKEN` is deliberately NOT read here: it is the token *value*
/// the integration tests send in the `ATTACH ... bearer_token '…'` option, not
/// worker configuration. Reading it would bearer-protect the shared example
/// worker the whole suite attaches — the integration harness exports it globally,
/// so every non-auth test over http would then 401 (and skip on the "HTTP"
/// error). The bearer-auth suite boots its own dedicated worker with
/// `VGI_BEARER_TOKENS`.
#[cfg(feature = "transport-http")]
fn build_authenticate() -> Option<vgi_rpc::Authenticate> {
    let principal_of = |tokens: &std::collections::HashMap<String, String>, tok: &str| {
        tokens.get(tok).map(|principal| vgi_rpc::AuthContext {
            domain: "bearer".to_string(),
            authenticated: true,
            principal: principal.clone(),
            claims: Default::default(),
        })
    };

    if let Some(required) = parse_token_map("VGI_BEARER_TOKENS") {
        return Some(std::sync::Arc::new(
            move |req: &vgi_rpc::AuthRequest<'_>| match bearer_token(req) {
                // A server with tokens configured is bearer-protected: reject
                // anonymous (no token) access. A server with NO tokens never
                // installs this callback, so it allows all (the non-auth tests).
                None => Err(vgi_rpc::RpcError::permission_error(
                    "bearer token required but not provided",
                )),
                // A blank token reaches here as `Some("")` and falls out of the
                // map lookup as "rejected", not "not provided".
                Some(tok) => principal_of(&required, tok).ok_or_else(|| {
                    vgi_rpc::RpcError::permission_error("bearer token was rejected")
                }),
            },
        ));
    }

    if let Some(optional) = parse_token_map("VGI_OPTIONAL_BEARER_TOKENS") {
        return Some(std::sync::Arc::new(
            move |req: &vgi_rpc::AuthRequest<'_>| {
                // No/blank/unknown token → anonymous. This callback never errors,
                // so an optional-bearer server can never 401.
                Ok(bearer_token(req)
                    .and_then(|tok| principal_of(&optional, tok))
                    .unwrap_or_else(vgi_rpc::AuthContext::anonymous))
            },
        ));
    }
    None
}

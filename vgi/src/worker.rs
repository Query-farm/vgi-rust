// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The VGI worker: owns function registries (via [`Dispatcher`]), builds the
//! RPC server, and drives transport selection from argv.

use std::sync::Arc;
use vgi_protocol::{VGI_PROTOCOL_NAME, VGI_PROTOCOL_VERSION};

use vgi_rpc::server::HostedProtocol;
use vgi_rpc::token_identity::{IdentityImpl, IssuedGrant, TokenIdentity};
use vgi_rpc::RpcServer;

use crate::dispatch::Dispatcher;
use crate::function::ScalarFunction;
use crate::protocol::register;

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
    hosted_protocols: Option<HostedProtocolsHook>,
    resolve_token: Option<vgi_rpc::token_identity::TokenResolver>,
    mint_grant: Option<vgi_rpc::token_identity::GrantMinter>,
    introspect_principals: Option<Vec<String>>,
    /// Read only by the HTTP transport.
    #[cfg_attr(not(feature = "transport-http"), allow(dead_code))]
    authenticate: Option<vgi_rpc::Authenticate>,
}

/// The hook a worker supplies its additional application protocols through.
/// See [`Worker::hosted_protocols`].
pub type HostedProtocolsHook = Arc<dyn Fn() -> Vec<HostedProtocol> + Send + Sync>;

/// The transport a worker's server is built for.
///
/// Only [`ServeTransport::Http`] changes what is hosted (`vgi_rpc.Identity.v1`,
/// when opted in); the rest are named so the decision is made in one place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServeTransport {
    /// stdin/stdout, and any byte stream handed to
    /// [`Worker::serve_reader_writer`] (the SAB/wasm path).
    Pipe,
    /// The AF_UNIX launcher transport.
    Unix,
    /// Raw TCP.
    Tcp,
    /// The raw upstream behind `vgi-iroh-bridge`.
    Iroh,
    /// HTTP.
    Http,
}

impl ServeTransport {
    /// Transports that authenticate their callers, and so may host
    /// `vgi_rpc.Identity.v1`: its allowlist is a list of *principals*, which
    /// a transport without caller identity cannot check.
    fn authenticates_callers(self) -> bool {
        matches!(self, ServeTransport::Http)
    }
}

/// Environment variable naming the principals allowed to call
/// `vgi_rpc.Identity.v1`'s `introspect_token`, comma-separated.
pub const INTROSPECT_PRINCIPALS_ENV: &str = "VGI_INTROSPECT_PRINCIPALS";

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
            hosted_protocols: None,
            resolve_token: None,
            mint_grant: None,
            introspect_principals: None,
            authenticate: None,
        }
    }

    /// Override the server id.
    pub fn server_id(mut self, id: impl Into<String>) -> Self {
        self.server_id = Some(id.into());
        self
    }

    /// Register a scalar function.
    ///
    /// It is declared in this worker's own catalog, in the `main` schema —
    /// every function has exactly one home. Use
    /// [`register_scalar_in`](Self::register_scalar_in) to place it in a
    /// different catalog schema.
    pub fn register_scalar(&mut self, f: impl ScalarFunction + 'static) {
        self.disp.register_scalar(Arc::new(f));
    }

    /// Register a scalar function declared in `schema` of `catalog`.
    ///
    /// A function name is not a unique key: the same name may be declared in
    /// two schemas of one catalog, or in two catalogs served by one worker
    /// process. The home is what tells them apart — the function is advertised
    /// only in that schema, and only a bind naming that schema resolves to it.
    /// See [`FunctionScope`](crate::FunctionScope).
    pub fn register_scalar_in(
        &mut self,
        catalog: &str,
        schema: &str,
        f: impl ScalarFunction + 'static,
    ) {
        self.disp.register_scalar_scoped(
            Arc::new(f),
            crate::dispatch::FunctionScope::new(catalog, schema),
        );
    }

    /// Register a table (producer) function.
    pub fn register_table(&mut self, f: impl crate::table_function::TableFunction + 'static) {
        self.disp.register_table(Arc::new(f));
    }

    /// Register a table (producer) function declared in `schema` of `catalog`.
    /// See [`register_scalar_in`](Self::register_scalar_in).
    pub fn register_table_in(
        &mut self,
        catalog: &str,
        schema: &str,
        f: impl crate::table_function::TableFunction + 'static,
    ) {
        self.disp.register_table_scoped(
            Arc::new(f),
            crate::dispatch::FunctionScope::new(catalog, schema),
        );
    }

    /// Hide an already-registered function from the catalog's advertised
    /// function list. It stays bindable, so a function-backed catalog table can
    /// still resolve it as a scan function, but the client creates no SQL
    /// callable for it — use this when the table is the only intended entry
    /// point.
    pub fn hide_function(&mut self, name: impl Into<String>) {
        self.disp.hide_function(name);
    }

    /// Whether `catalog_attach` advertises `supports_catalog_contents`
    /// (protocol 2.1.0), letting the client load every schema and all of its
    /// contents with one `catalog_contents` call instead of one
    /// `catalog_schema_contents_*` call per schema and kind.
    ///
    /// On by default — the declarative catalog is read-only and its listings
    /// don't depend on the transaction. Turn it off to force the client back
    /// onto the per-schema RPCs (e.g. to compare the two paths); the
    /// `catalog_contents` RPC itself stays registered either way.
    ///
    /// Revalidation (`if_none_match` / etag) and the worker-side cache are
    /// per catalog: see [`crate::catalog::CatalogModel::contents_provider`],
    /// `catalog_contents_etag` and `catalog_contents_attach_independent`.
    pub fn set_catalog_contents(&mut self, enabled: bool) {
        self.disp.catalog_contents = enabled;
    }

    /// Replace the shared cross-process state store.
    ///
    /// The default comes from [`crate::storage::default_storage`], selected by
    /// `VGI_WORKER_SHARED_STORAGE`. Override it to embed a worker with a store
    /// the host already owns — or, in a test, to observe what the framework
    /// reads and writes.
    ///
    /// Whatever is installed must be shared by every worker instance behind an
    /// HTTP endpoint and must outlive a single request: a continuation is
    /// served by whichever instance receives it, and buffering, aggregate and
    /// FINALIZE-flush state all live here.
    pub fn set_storage(&mut self, store: crate::storage::SharedStorage) {
        self.disp.store = store;
    }

    /// Register a table-in-out function.
    pub fn register_table_in_out(
        &mut self,
        f: impl crate::table_in_out::TableInOutFunction + 'static,
    ) {
        self.disp.register_table_in_out(Arc::new(f));
    }

    /// Register a table-in-out function declared in `schema` of `catalog`.
    /// See [`register_scalar_in`](Self::register_scalar_in).
    pub fn register_table_in_out_in(
        &mut self,
        catalog: &str,
        schema: &str,
        f: impl crate::table_in_out::TableInOutFunction + 'static,
    ) {
        self.disp.register_table_in_out_scoped(
            Arc::new(f),
            crate::dispatch::FunctionScope::new(catalog, schema),
        );
    }

    /// Register a table-buffering function.
    pub fn register_buffering(
        &mut self,
        f: impl crate::buffering::TableBufferingFunction + 'static,
    ) {
        self.disp.register_buffering(Arc::new(f));
    }

    /// Register a table-buffering function declared in `schema` of `catalog`.
    /// See [`register_scalar_in`](Self::register_scalar_in).
    pub fn register_buffering_in(
        &mut self,
        catalog: &str,
        schema: &str,
        f: impl crate::buffering::TableBufferingFunction + 'static,
    ) {
        self.disp.register_buffering_scoped(
            Arc::new(f),
            crate::dispatch::FunctionScope::new(catalog, schema),
        );
    }

    /// Register an aggregate function.
    pub fn register_aggregate(&mut self, f: impl crate::aggregate::AggregateFunction + 'static) {
        self.disp.register_aggregate(Arc::new(f));
    }

    /// Register an aggregate function declared in `schema` of `catalog`.
    /// See [`register_scalar_in`](Self::register_scalar_in).
    pub fn register_aggregate_in(
        &mut self,
        catalog: &str,
        schema: &str,
        f: impl crate::aggregate::AggregateFunction + 'static,
    ) {
        self.disp.register_aggregate_scoped(
            Arc::new(f),
            crate::dispatch::FunctionScope::new(catalog, schema),
        );
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

    /// Host additional application protocols beside `vgi.v2`.
    ///
    /// `hook` returns the protocols -- each a
    /// [`HostedProtocol`](vgi_rpc::server::HostedProtocol): a name, an optional
    /// version, and its methods (a `#[vgi_rpc::service]` type registers into
    /// one with its generated `register_with`). It is called **once**, when the
    /// worker's server is built, and may consult configuration or the
    /// environment; what it returns is then fixed for the life of the process
    /// and hosted on **every** transport the worker serves (stdio, unix, TCP,
    /// the Iroh upstream, HTTP and the SAB/wasm path), listed by reflection
    /// after `vgi.v2` in the order returned.
    ///
    /// Requests route on their `vgi_rpc.protocol` key with no fallback, so an
    /// added protocol cannot change how `vgi.v2` (the DuckDB extension's
    /// protocol) dispatches -- even when its method names repeat `vgi.v2`'s.
    ///
    /// A returned name that is malformed, repeats another, is `vgi.v2`, or
    /// claims the reserved `vgi_rpc.` prefix stops the worker at startup with
    /// an error naming this hook. Framework protocols are never supplied here:
    /// reflection is always hosted, and `vgi_rpc.Identity.v1` is enabled with
    /// [`resolve_token`](Self::resolve_token) / [`mint_grant`](Self::mint_grant).
    ///
    /// ```
    /// use vgi::Worker;
    /// use vgi_rpc::server::HostedProtocol;
    /// use vgi_rpc::MethodInfo;
    /// # fn empty_schema() -> arrow_schema::SchemaRef { std::sync::Arc::new(arrow_schema::Schema::empty()) }
    ///
    /// let mut worker = Worker::new();
    /// worker.hosted_protocols(|| {
    ///     vec![HostedProtocol::new("example.Health.v1").with_method(MethodInfo::unary(
    ///         "ping",
    ///         empty_schema(),
    ///         empty_schema(),
    ///         |_req, _ctx| Ok(None),
    ///     ))]
    /// });
    /// let server = worker.build_server();
    /// assert_eq!(server.hosted_protocol_names()[..2], ["vgi.v2", "example.Health.v1"]);
    /// ```
    pub fn hosted_protocols(
        &mut self,
        hook: impl Fn() -> Vec<HostedProtocol> + Send + Sync + 'static,
    ) {
        self.hosted_protocols = Some(Arc::new(hook));
    }

    /// Opt into `vgi_rpc.Identity.v1`'s `introspect_token`: resolve an opaque
    /// bearer credential to the principal it authenticates as.
    ///
    /// For a reverse proxy that terminates the only public listener and must
    /// know the caller before it can authorize anything. Hosted over **HTTP
    /// only** (the transport that authenticates callers), and only when this
    /// or [`mint_grant`](Self::mint_grant) is set: absent, the protocol is not
    /// hosted at all, rather than hosted and refusing.
    ///
    /// Return `Ok(None)` for "the store answered and this credential is
    /// unknown". For "the answer is not knowable" -- the store is down, a
    /// timeout, a 5xx -- return
    /// [`RpcError::auth_unavailable`](vgi_rpc::RpcError::auth_unavailable)
    /// (optionally `.with_retry_after(secs)`), the same error an HTTP
    /// authenticator returns for an outage. The framework sends it as
    /// `identity_unavailable` (`UNAVAILABLE`) carrying your retry hint as
    /// `RetryInfo`, so a caller retries instead of negative-caching an outage
    /// as "unknown". Never put claims or the credential in the error.
    ///
    /// Enabling this requires an allowlist of principals permitted to ask
    /// ([`introspect_principals`](Self::introspect_principals) or
    /// `VGI_INTROSPECT_PRINCIPALS`); without one the worker **refuses to
    /// start** on HTTP. There is no permissive default: authenticating and
    /// introspecting are different capabilities, and "any authenticated
    /// caller" lets any user resolve any other user's credential to its owner.
    pub fn resolve_token(
        &mut self,
        hook: impl Fn(&str) -> vgi_rpc::Result<Option<TokenIdentity>> + Send + Sync + 'static,
    ) {
        self.resolve_token = Some(Arc::new(hook));
    }

    /// Opt into `vgi_rpc.Identity.v1`'s `issue_grant`: mint a standing
    /// delegation credential for the calling user.
    ///
    /// The hook receives `(principal, purpose, scopes, ttl_seconds)`; the
    /// principal is always the authenticated caller. Refuse with
    /// [`grant_refused`](vgi_rpc::token_identity::grant_refused); for a
    /// transient failure return
    /// [`RpcError::auth_unavailable`](vgi_rpc::RpcError::auth_unavailable),
    /// which -- as for [`resolve_token`](Self::resolve_token) -- reaches the
    /// caller as `identity_unavailable` with your retry hint. HTTP only; needs
    /// no allowlist, because a grant is only ever about the caller.
    pub fn mint_grant(
        &mut self,
        hook: impl Fn(&str, &str, &[String], i64) -> vgi_rpc::Result<IssuedGrant>
            + Send
            + Sync
            + 'static,
    ) {
        self.mint_grant = Some(Arc::new(hook));
    }

    /// Principals permitted to call `introspect_token`. Overrides
    /// `VGI_INTROSPECT_PRINCIPALS`. See [`resolve_token`](Self::resolve_token).
    pub fn introspect_principals<I, S>(&mut self, principals: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.introspect_principals = Some(principals.into_iter().map(Into::into).collect());
    }

    /// Authenticate HTTP callers with `authenticate` instead of the
    /// environment-derived bearer configuration (`VGI_BEARER_TOKENS` /
    /// `VGI_OPTIONAL_BEARER_TOKENS`). `vgi_rpc.Identity.v1` needs an
    /// authenticated caller -- the introspector allowlist names principals --
    /// so a worker opting into identity normally sets this too.
    pub fn authenticate(&mut self, authenticate: vgi_rpc::Authenticate) {
        self.authenticate = Some(authenticate);
    }

    /// Build the configured [`RpcServer`] for stdio, registering every VGI
    /// method. See [`build_server_for`](Self::build_server_for).
    pub fn build_server(self) -> RpcServer {
        self.build_server_for(ServeTransport::Pipe)
    }

    /// Build the configured [`RpcServer`] for `transport`.
    ///
    /// The one construction path every transport uses: `vgi.v2`, then the
    /// [`hosted_protocols`](Self::hosted_protocols) (every transport), then
    /// `vgi_rpc.Reflection.v1` (every transport), then -- on HTTP, when opted
    /// in -- `vgi_rpc.Identity.v1`.
    pub fn build_server_for(self, transport: ServeTransport) -> RpcServer {
        self.build_parts(transport).0
    }

    /// Build the [`RpcServer`] and return the shared [`Dispatcher`] handle
    /// alongside it. The HTTP transport reuses the dispatcher to serve the
    /// landing contract (`describe.json`) via catalog introspection.
    fn build_parts(self, transport: ServeTransport) -> (RpcServer, Arc<Dispatcher>) {
        let server_id = self
            .server_id
            .clone()
            .unwrap_or_else(|| "vgi-rust-worker".to_string());
        // The `bad_protocol` fixture advertises an incompatible version via
        // this env override so the C++ ATTACH fails with a clear mismatch.
        let protocol_version = std::env::var("VGI_PROTOCOL_VERSION_OVERRIDE")
            .unwrap_or_else(|_| VGI_PROTOCOL_VERSION.to_string());
        let extra = match &self.hosted_protocols {
            Some(hook) => validated_hosted_protocols(hook()),
            None => Vec::new(),
        };
        let identity = if transport.authenticates_callers() {
            build_identity(
                self.resolve_token.clone(),
                self.mint_grant.clone(),
                self.introspect_principals.clone(),
            )
        } else {
            None
        };
        let mut builder = RpcServer::builder()
            .server_id(server_id)
            .protocol_name(VGI_PROTOCOL_NAME)
            .protocol_version(protocol_version)
            // No `enable_describe` knob since vgi-rpc 0.25.0: the hardcoded
            // `__describe__` method was retired in favour of the co-hosted
            // `vgi_rpc.Reflection.v1` protocol, which every server hosts
            // unconditionally and addresses through the ordinary routing key.
            .add_protocols(extra);
        if let Some(identity) = identity {
            builder = builder.identity(identity);
        }
        let mut srv = builder
            .try_build()
            .unwrap_or_else(|err| panic!("{HOOK_NAME}: {}", err.message));
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
    /// - `--http` — **HTTP** transport (Arrow-IPC over HTTP). Optional
    ///   `--host` / `--port` select the bind address. Bearer auth is enabled by
    ///   setting `VGI_BEARER_TOKENS` (`token=principal,…`).
    /// - `--iroh-raw-upstream [<host>:]<port> --iroh-issuer <namespace>` —
    ///   loopback raw upstream for `vgi-iroh-bridge`. Add `--iroh-observe` to
    ///   expose peer evidence without making it the application principal.
    pub fn run(self) {
        let args: Vec<String> = std::env::args().collect();
        // Capture the worker's display name / doc from the primary catalog
        // before the dispatcher is moved into the server (used by the HTTP
        // landing contract).
        let worker_name = self.disp.catalog.name.clone();
        let worker_doc = self.disp.catalog.comment.clone().unwrap_or_default();
        #[cfg(feature = "transport-http")]
        let explicit_authenticate = self.authenticate.clone();
        let (server, disp) = self.build_parts(transport_from_args(&args));
        let server = Arc::new(server);

        let iroh_bridge = args
            .iter()
            .position(|arg| arg == "--iroh-issuer")
            .map(|index| {
                let issuer = args
                    .get(index + 1)
                    .expect("--iroh-issuer requires a value")
                    .clone();
                let trusted_proxy_addresses = args
                    .iter()
                    .enumerate()
                    .filter(|(_, arg)| *arg == "--iroh-trusted-proxy")
                    .map(|(index, _)| {
                        args.get(index + 1)
                            .expect("--iroh-trusted-proxy requires an exact IP")
                            .clone()
                    })
                    .collect::<Vec<_>>();
                crate::transport::IrohBridgeOptions {
                    issuer,
                    trusted_proxy_addresses: if trusted_proxy_addresses.is_empty() {
                        vec!["127.0.0.1".to_string()]
                    } else {
                        trusted_proxy_addresses
                    },
                    authenticate: !args.iter().any(|arg| arg == "--iroh-observe"),
                }
            });

        #[cfg(feature = "transport-http")]
        if args.iter().any(|a| a == "--http") {
            let info = vgi_rpc::http::LandingInfo {
                name: worker_name,
                doc: worker_doc,
                version: env!("CARGO_PKG_VERSION").to_string(),
            };
            let _ = &disp;
            let explicit_bind = args.iter().any(|arg| arg == "--host" || arg == "--port");
            let host = args
                .iter()
                .position(|arg| arg == "--host")
                .map(|index| {
                    args.get(index + 1)
                        .expect("--host requires a value")
                        .as_str()
                })
                .unwrap_or("127.0.0.1");
            let port = args
                .iter()
                .position(|arg| arg == "--port")
                .map(|index| {
                    args.get(index + 1)
                        .expect("--port requires a value")
                        .parse::<u16>()
                        .expect("--port must be in 0..65535")
                })
                .unwrap_or(0);
            if let Some(bridge) = iroh_bridge.clone() {
                if explicit_bind {
                    crate::transport::serve_http_behind_iroh_at(
                        server,
                        explicit_authenticate.clone().or_else(build_authenticate),
                        Some(info),
                        bridge,
                        host,
                        port,
                    );
                } else {
                    crate::transport::serve_http_behind_iroh(
                        server,
                        explicit_authenticate.clone().or_else(build_authenticate),
                        Some(info),
                        bridge,
                    );
                }
            } else if explicit_bind {
                crate::transport::serve_http_at(
                    server,
                    explicit_authenticate.clone().or_else(build_authenticate),
                    Some(info),
                    host,
                    port,
                );
            } else {
                crate::transport::serve_http(
                    server,
                    explicit_authenticate.clone().or_else(build_authenticate),
                    Some(info),
                );
            }
            return;
        }
        // The dispatcher handle is only needed by the HTTP landing contract.
        let _ = (&disp, &worker_name, &worker_doc);

        #[cfg(not(target_arch = "wasm32"))]
        if let Some(i) = args.iter().position(|arg| arg == "--iroh-raw-upstream") {
            let spec = args
                .get(i + 1)
                .expect("--iroh-raw-upstream requires [HOST:]PORT")
                .clone();
            let bridge = iroh_bridge.expect("--iroh-raw-upstream requires --iroh-issuer");
            let (host, port) = parse_tcp_spec(&spec);
            let idle = args
                .iter()
                .position(|a| a == "--idle-timeout")
                .and_then(|j| args.get(j + 1))
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            crate::transport::serve_iroh_tcp_upstream(server, &host, port, idle, bridge);
            return;
        }

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
        let (server, _disp) = self.build_parts(ServeTransport::Pipe);
        std::sync::Arc::new(server).serve(&mut r, &mut w);
    }
}

/// How [`Worker::hosted_protocols`] is named in startup errors.
const HOOK_NAME: &str = "Worker::hosted_protocols hook";

/// Check the hook's protocols before the server sees them, so an error names
/// the hook to fix rather than only a protocol. vgi-rpc checks these too.
fn validated_hosted_protocols(protocols: Vec<HostedProtocol>) -> Vec<HostedProtocol> {
    let mut seen = std::collections::HashSet::new();
    for (index, protocol) in protocols.iter().enumerate() {
        let name = protocol.name();
        if name.starts_with(vgi_rpc::binding::RESERVED_PROTOCOL_PREFIX) {
            panic!(
                "{HOOK_NAME}: entry {index} is named {name:?}, which claims the reserved \
                 {:?} prefix. Framework protocols are not supplied through this hook: \
                 reflection is hosted automatically, and vgi_rpc.Identity.v1 is enabled with \
                 Worker::resolve_token and/or Worker::mint_grant.",
                vgi_rpc::binding::RESERVED_PROTOCOL_PREFIX
            );
        }
        if let Err(err) = vgi_rpc::binding::validate_protocol_name(name, false) {
            panic!("{HOOK_NAME}: entry {index}: {err}");
        }
        if name == VGI_PROTOCOL_NAME {
            panic!(
                "{HOOK_NAME}: entry {index} is named {name:?}, the worker's own protocol. \
                 Give it a distinct name."
            );
        }
        if !seen.insert(name.to_string()) {
            panic!(
                "{HOOK_NAME}: protocol name {name:?} is listed twice. The name is the \
                 routing key, so each hosted protocol needs a distinct one."
            );
        }
    }
    protocols
}

/// Build `vgi_rpc.Identity.v1`, or `None` when the worker set neither hook --
/// in which case the protocol is not hosted at all. Absent beats
/// routed-and-refusing: it is what keeps a dependency upgrade from growing a
/// credential-to-identity oracle on every existing worker.
fn build_identity(
    resolve: Option<vgi_rpc::token_identity::TokenResolver>,
    mint: Option<vgi_rpc::token_identity::GrantMinter>,
    explicit_principals: Option<Vec<String>>,
) -> Option<IdentityImpl> {
    if resolve.is_none() && mint.is_none() {
        return None;
    }
    let mut builder = IdentityImpl::builder();
    if let Some(resolve) = resolve {
        // Only introspection needs an allowlist: a worker that mints but
        // resolves nothing is not an oracle.
        builder = builder.resolve_token(resolve).introspect_principals(
            resolve_introspect_principals(explicit_principals).unwrap_or_else(|message| {
                eprintln!("{message}");
                std::process::exit(1);
            }),
        );
    }
    if let Some(mint) = mint {
        builder = builder.mint_grant(mint);
    }
    Some(builder.build())
}

/// The introspector allowlist, or the actionable message the worker exits
/// with.
///
/// Fail-closed and loud rather than defaulting to "any authenticated caller":
/// a worker that implements `resolve_token` and forgets the allowlist must not
/// start.
fn resolve_introspect_principals(explicit: Option<Vec<String>>) -> Result<Vec<String>, String> {
    let principals: Vec<String> = match explicit {
        Some(list) => list,
        None => std::env::var(INTROSPECT_PRINCIPALS_ENV)
            .unwrap_or_default()
            .split(',')
            .map(str::to_string)
            .collect(),
    }
    .into_iter()
    .map(|p| p.trim().to_string())
    .filter(|p| !p.is_empty())
    .collect();
    if principals.is_empty() {
        return Err(format!(
            "Error: this worker sets Worker::resolve_token, which hosts the\n  \
             vgi_rpc.Identity.v1 protocol, but no introspector allowlist was\n  \
             configured. Set {INTROSPECT_PRINCIPALS_ENV} (comma-separated) or call\n  \
             Worker::introspect_principals.\n\n  \
             There is no permissive default on purpose: introspection is a\n  \
             separate capability from authentication, and allowing every\n  \
             authenticated caller lets any user resolve any other user's\n  \
             credential to its owner. Remove resolve_token to leave the\n  \
             protocol unhosted entirely."
        ));
    }
    Ok(principals)
}

/// The transport `run` will serve, decided before the server is built so the
/// one build path knows whether to host identity.
fn transport_from_args(args: &[String]) -> ServeTransport {
    let has = |flag: &str| args.iter().any(|a| a == flag);
    if cfg!(feature = "transport-http") && has("--http") {
        ServeTransport::Http
    } else if has("--iroh-raw-upstream") {
        ServeTransport::Iroh
    } else if has("--tcp") {
        ServeTransport::Tcp
    } else if has("--unix") {
        ServeTransport::Unix
    } else {
        ServeTransport::Pipe
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

#[cfg(test)]
mod hosting_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use vgi_rpc::MethodInfo;

    fn ping(name: &str) -> HostedProtocol {
        HostedProtocol::new(name).with_method(MethodInfo::unary(
            "ping",
            Arc::new(arrow_schema::Schema::empty()),
            Arc::new(arrow_schema::Schema::empty()),
            |_req, _ctx| Ok(None),
        ))
    }

    fn opted_in() -> Worker {
        let mut worker = Worker::new();
        worker.resolve_token(|_token| Ok(None));
        worker.introspect_principals(["proxy"]);
        worker
    }

    #[test]
    fn the_hook_is_called_once_and_hosted_after_vgi_v2() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let mut worker = Worker::new();
        worker.hosted_protocols(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            vec![ping("example.A.v1"), ping("example.B.v1")]
        });
        let server = worker.build_server_for(ServeTransport::Unix);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let names = server.hosted_protocol_names();
        assert_eq!(names[..3], ["vgi.v2", "example.A.v1", "example.B.v1"]);
        assert!(names.contains(&vgi_rpc::reflection::REFLECTION_PROTOCOL_NAME));
    }

    #[test]
    #[should_panic(expected = "Worker::hosted_protocols hook: entry 0")]
    fn a_reserved_name_is_refused_naming_the_hook() {
        let mut worker = Worker::new();
        worker.hosted_protocols(|| vec![ping("vgi_rpc.Reflection.v1")]);
        let _ = worker.build_server();
    }

    #[test]
    #[should_panic(expected = "the worker's own protocol")]
    fn the_primary_name_is_refused() {
        let mut worker = Worker::new();
        worker.hosted_protocols(|| vec![ping(VGI_PROTOCOL_NAME)]);
        let _ = worker.build_server();
    }

    #[test]
    #[should_panic(expected = "listed twice")]
    fn a_repeated_name_is_refused() {
        let mut worker = Worker::new();
        worker.hosted_protocols(|| vec![ping("example.A.v1"), ping("example.A.v1")]);
        let _ = worker.build_server();
    }

    #[test]
    fn identity_is_hosted_on_http_only_and_only_when_opted_in() {
        let identity = vgi_rpc::token_identity::IDENTITY_PROTOCOL_NAME;
        assert!(opted_in()
            .build_server_for(ServeTransport::Http)
            .hosted_protocol_names()
            .contains(&identity));
        for transport in [
            ServeTransport::Pipe,
            ServeTransport::Unix,
            ServeTransport::Tcp,
            ServeTransport::Iroh,
        ] {
            assert!(!opted_in()
                .build_server_for(transport)
                .hosted_protocol_names()
                .contains(&identity));
        }
        assert!(!Worker::new()
            .build_server_for(ServeTransport::Http)
            .hosted_protocol_names()
            .contains(&identity));
    }

    #[test]
    fn a_minter_alone_needs_no_allowlist() {
        let mut worker = Worker::new();
        worker.mint_grant(|_p, _purpose, _s, _t| Err(vgi_rpc::token_identity::grant_refused("no")));
        let server = worker.build_server_for(ServeTransport::Http);
        assert!(server
            .hosted_protocol_names()
            .contains(&vgi_rpc::token_identity::IDENTITY_PROTOCOL_NAME));
    }

    /// Introspection without an allowlist refuses to start: an explicit empty
    /// list and blank entries do not count as one.
    #[test]
    fn introspection_without_an_allowlist_refuses() {
        let err = resolve_introspect_principals(Some(vec![" ".into(), String::new()])).unwrap_err();
        assert!(err.contains("VGI_INTROSPECT_PRINCIPALS"), "{err}");
        assert_eq!(
            resolve_introspect_principals(Some(vec![" proxy ".into()])).unwrap(),
            ["proxy"]
        );
    }
}

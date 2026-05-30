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

/// A VGI worker. Register functions, then `run()` to serve.
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
    /// New worker serving the catalog named by `VGI_WORKER_CATALOG_NAME`
    /// (default `example`).
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
    pub fn set_catalog(&mut self, model: crate::catalog::CatalogModel) {
        self.disp.set_catalog(model);
    }

    /// Build the configured [`RpcServer`], registering every VGI method.
    pub fn build_server(self) -> RpcServer {
        let server_id = self
            .server_id
            .clone()
            .unwrap_or_else(|| "vgi-rust-worker".to_string());
        let mut srv = RpcServer::builder()
            .server_id(server_id)
            .protocol_name(VGI_PROTOCOL_NAME)
            .protocol_version(VGI_PROTOCOL_VERSION)
            .enable_describe(true)
            .build();
        register::register(&mut srv, Arc::new(self.disp));
        srv
    }

    /// Parse argv and serve over the selected transport.
    pub fn run(self) {
        let args: Vec<String> = std::env::args().collect();
        let server = Arc::new(self.build_server());

        if let Some(i) = args.iter().position(|a| a == "--unix") {
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

        crate::transport::serve_stdio(server);
    }
}

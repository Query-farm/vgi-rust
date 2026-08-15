// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The client handle and how to connect one.

use std::ffi::OsStr;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use vgi_rpc::errors::Result;
use vgi_rpc_client::RpcClient;

use crate::transport::{StreamTransport, VgiTransport};

/// A connection to a VGI worker.
///
/// One client owns one connection. The worker is single-threaded per
/// connection, so a caller that wants parallelism opens several — which is also
/// how a scan fans out across the worker's advertised `max_workers`.
pub struct VgiClient {
    transport: Box<dyn VgiTransport>,
}

impl VgiClient {
    /// Build a client over any transport.
    pub fn new(transport: Box<dyn VgiTransport>) -> Self {
        Self { transport }
    }

    /// Spawn a worker as a child process and talk over its stdin/stdout.
    pub fn connect_subprocess<S: AsRef<OsStr>>(cmd: &[S]) -> Result<Self> {
        let label = cmd
            .first()
            .map(|s| s.as_ref().to_string_lossy().into_owned())
            .unwrap_or_else(|| "subprocess".to_string());
        let client = RpcClient::connect(cmd)?;
        Ok(Self::new(Box::new(StreamTransport::new(client, label))))
    }

    /// Connect to a worker listening on TCP.
    pub fn connect_tcp(host: &str, port: u16) -> Result<Self> {
        let client = RpcClient::tcp_connect(host, port)?;
        Ok(Self::new(Box::new(StreamTransport::new(
            client,
            format!("tcp://{host}:{port}"),
        ))))
    }

    /// Connect to a worker on a Unix domain socket.
    #[cfg(feature = "unix")]
    pub fn connect_unix(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let label = format!("unix://{}", path.as_ref().display());
        let client = RpcClient::unix_connect(path)?;
        Ok(Self::new(Box::new(StreamTransport::new(client, label))))
    }

    /// Connect to a worker serving VGI over HTTP.
    #[cfg(feature = "http")]
    pub fn connect_http(base_url: &str) -> Result<Self> {
        use crate::transport::HttpTransport;
        let client = vgi_rpc_client::HttpClient::connect(base_url.to_string()).build()?;
        Ok(Self::new(Box::new(HttpTransport::new(
            client,
            base_url.to_string(),
        ))))
    }

    /// Connect over HTTP, presenting a credential and recovering from a 401.
    ///
    /// The credential is whatever [`CatalogAuth`](crate::auth::CatalogAuth)
    /// holds: a static bearer token, or an OAuth identity that can refresh and
    /// run an interactive flow.
    #[cfg(all(feature = "http", feature = "oauth"))]
    pub fn connect_http_with_auth(
        base_url: &str,
        auth: std::sync::Arc<dyn crate::auth::CatalogAuth>,
    ) -> Self {
        Self::new(Box::new(crate::auth::AuthenticatedHttpTransport::new(
            base_url, auth,
        )))
    }

    /// A short label for this connection, for error messages and logs.
    pub fn label(&self) -> &str {
        self.transport.label()
    }

    pub(crate) fn transport_mut(&mut self) -> &mut dyn VgiTransport {
        self.transport.as_mut()
    }

    /// A one-row batch with no columns, for methods that take no params.
    ///
    /// A unary request is conceptually one row, so the row count is 1 even
    /// though there is nothing in it — a zero-row batch would read as "no
    /// request" to a handler that checks cardinality.
    pub(crate) fn empty_params(&self) -> Result<RecordBatch> {
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )
        .map_err(|e| vgi_rpc::errors::RpcError::runtime_error(format!("empty params batch: {e}")))
    }
}

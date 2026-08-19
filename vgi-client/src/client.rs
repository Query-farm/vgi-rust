// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The client handle and how to connect one.

use std::ffi::OsStr;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::Schema;
use vgi_rpc::errors::Result;
use vgi_rpc_client::RpcClient;

use crate::location::VgiLocation;
use crate::transport::{StreamTransport, VgiTransport};

/// A connection to a VGI worker.
///
/// One client owns one connection. The worker is single-threaded per
/// connection, so a caller that wants parallelism opens several — which is also
/// how a scan fans out across the worker's advertised `max_workers`.
pub struct VgiClient {
    transport: Box<dyn VgiTransport>,
}

/// Apply the settings every VGI connection needs, whatever the transport.
///
/// Two of them, and both are contracts with the worker rather than tuning:
///
/// * **`protocol_version`** — a VGI worker rejects a request that does not
///   declare `vgi_rpc.protocol_version`, matching major+minor exactly at the
///   dispatch boundary. Omitting it is not a lenient default; the Python
///   reference worker answers "the client did not send a
///   vgi_rpc.protocol_version metadata key … or a non-VGI client connecting to
///   a VGI worker" and drops the connection.
/// * **`relax_nullability`** — the Python worker declares some response fields
///   non-nullable and then legitimately sends nulls in them, so response
///   schemas are promoted to fully-nullable on read.
///
/// Neither is optional in practice, which is why this is applied centrally
/// rather than left to each `connect_*`: a new transport added later inherits
/// both instead of rediscovering them.
fn configure(client: RpcClient) -> RpcClient {
    client
        .protocol_version(vgi_protocol::VGI_PROTOCOL_VERSION)
        .relax_nullability(true)
}

impl VgiClient {
    /// Build a client over any transport.
    pub fn new(transport: Box<dyn VgiTransport>) -> Self {
        Self { transport }
    }

    /// Connect to wherever a `LOCATION` string points.
    ///
    /// The one entry point that turns the extension's `LOCATION` spelling into
    /// a connection, so a caller (or a test corpus) names a worker the same way
    /// whichever client is reading the string. See [`VgiLocation`] for the
    /// scheme table.
    pub fn connect_location(location: &VgiLocation) -> Result<Self> {
        match location {
            VgiLocation::Subprocess(argv) => Self::connect_subprocess(argv),
            VgiLocation::Tcp { host, port } => Self::connect_tcp(host, *port),
            #[cfg(feature = "http")]
            VgiLocation::Http(url) => Self::connect_http(url),
            #[cfg(not(feature = "http"))]
            VgiLocation::Http(_) => Err(vgi_rpc::errors::RpcError::value_error(
                "http:// LOCATIONs need the `http` feature",
            )),
            #[cfg(feature = "unix")]
            VgiLocation::Unix(path) => Self::connect_unix(path),
            #[cfg(not(feature = "unix"))]
            VgiLocation::Unix(_) => Err(vgi_rpc::errors::RpcError::value_error(
                "unix:// LOCATIONs need the `unix` feature",
            )),
            #[cfg(feature = "launcher")]
            VgiLocation::Launch(argv) => {
                let sock = crate::launcher::ensure_worker(argv, &Default::default())?;
                Self::connect_unix(sock)
            }
            #[cfg(not(feature = "launcher"))]
            VgiLocation::Launch(_) => Err(vgi_rpc::errors::RpcError::value_error(
                "launch: LOCATIONs need the `launcher` feature",
            )),
        }
    }

    /// Parse a `LOCATION` string and connect to it.
    pub fn connect_to(location: &str) -> Result<Self> {
        Self::connect_location(&VgiLocation::parse(location)?)
    }

    /// Spawn a worker as a child process and talk over its stdin/stdout.
    pub fn connect_subprocess<S: AsRef<OsStr>>(cmd: &[S]) -> Result<Self> {
        let label = cmd
            .first()
            .map(|s| s.as_ref().to_string_lossy().into_owned())
            .unwrap_or_else(|| "subprocess".to_string());
        let client = configure(RpcClient::connect(cmd)?);
        Ok(Self::new(Box::new(StreamTransport::new(client, label))))
    }

    /// Connect to a worker listening on TCP.
    pub fn connect_tcp(host: &str, port: u16) -> Result<Self> {
        let client = configure(RpcClient::tcp_connect(host, port)?);
        Ok(Self::new(Box::new(StreamTransport::new(
            client,
            format!("tcp://{host}:{port}"),
        ))))
    }

    /// Connect to a worker on a Unix domain socket.
    #[cfg(feature = "unix")]
    pub fn connect_unix(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let label = format!("unix://{}", path.as_ref().display());
        let client = configure(RpcClient::unix_connect(path)?);
        Ok(Self::new(Box::new(StreamTransport::new(client, label))))
    }

    /// Connect to a worker serving VGI over HTTP.
    ///
    /// Wrapped in a [`RetryTransport`](crate::retry::RetryTransport): HTTP is
    /// the only transport that can answer `429` + `Retry-After`, which is how
    /// the protocol's `max_workers` cap is enforced against an engine that
    /// over-fans. Nothing else in the stack reads that status, so an unwrapped
    /// HTTP client meets a cap with a decode error instead of a wait.
    #[cfg(feature = "http")]
    pub fn connect_http(base_url: &str) -> Result<Self> {
        Self::connect_http_with_retry(base_url, crate::retry::RetryPolicy::default())
    }

    /// As [`Self::connect_http`], with an explicit retry policy.
    #[cfg(feature = "http")]
    pub fn connect_http_with_retry(
        base_url: &str,
        policy: crate::retry::RetryPolicy,
    ) -> Result<Self> {
        use crate::transport::HttpTransport;
        let client = vgi_rpc_client::HttpClient::connect(base_url.to_string())
            .protocol_version(vgi_protocol::VGI_PROTOCOL_VERSION)
            .build()?;
        let http = Box::new(HttpTransport::new(client, base_url.to_string()));
        Ok(Self::new(Box::new(crate::retry::RetryTransport::new(
            http, policy,
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
        let inner = Box::new(crate::auth::AuthenticatedHttpTransport::new(base_url, auth));
        // Retry sits OUTSIDE the 401 recovery, not inside it: a 401 is fatal to
        // the retry classifier and passes straight through to the credential
        // refresh, while a 429 that the refresh would have no answer for is
        // absorbed here.
        Self::new(Box::new(crate::retry::RetryTransport::new(
            inner,
            crate::retry::RetryPolicy::default(),
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

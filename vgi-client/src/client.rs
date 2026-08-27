// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The client handle and how to connect one.

use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

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
    worker_logs: Option<WorkerLogRouter>,
    /// Cleared when an exchange fails in a state where transport framing may
    /// no longer be synchronized. A pooled owner checks this bit on drop and
    /// discards the connection even when its caller forgot to call `poison`.
    exchange_reusable: Arc<AtomicBool>,
}

/// A destination for structured log messages emitted in-band by a VGI worker.
pub type WorkerLogSink = Arc<dyn Fn(vgi_rpc_client::LogMessage) + Send + Sync + 'static>;

#[derive(Clone)]
pub(crate) struct WorkerLogRouter(Arc<RwLock<WorkerLogSink>>);

impl Default for WorkerLogRouter {
    fn default() -> Self {
        Self(Arc::new(RwLock::new(default_worker_log_sink())))
    }
}

impl WorkerLogRouter {
    pub(crate) fn callback(&self) -> vgi_rpc_client::OnLog {
        let router = self.clone();
        Box::new(move |message| router.emit(message))
    }

    pub(crate) fn emit(&self, message: vgi_rpc_client::LogMessage) {
        let sink = self.0.read().ok().map(|current| Arc::clone(&*current));
        if let Some(sink) = sink {
            sink(message);
        }
    }

    fn replace(&self, sink: WorkerLogSink) {
        if let Ok(mut current) = self.0.write() {
            *current = sink;
        }
    }

    fn reset(&self) {
        self.replace(default_worker_log_sink());
    }
}

/// Per-attachment transport settings that affect how a worker is reached.
///
/// These mirror the local VGI `ATTACH` options. They are deliberately kept
/// separate from protocol attach options: changing one can select or spawn a
/// different local worker connection, so a pool must include them in its key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ConnectionOptions {
    /// Inherit a subprocess worker's stderr when true; discard it otherwise.
    pub worker_debug: bool,
    /// Override the idle timeout for a `launch:` worker.
    pub launcher_idle_timeout: Option<Duration>,
    /// Override the state directory for a `launch:` worker.
    pub launcher_state_dir: Option<PathBuf>,
    /// Optional per-request/read deadline. For subprocess pipes this bounds
    /// response reads and kills/poisons a timed-out child; `std` cannot
    /// interrupt a blocked write to an anonymous pipe. `None` disables the
    /// transport's built-in default so the embedding engine owns policy.
    pub rpc_timeout: Option<Duration>,
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
fn configure(client: RpcClient, worker_logs: &WorkerLogRouter) -> RpcClient {
    client
        .protocol_version(vgi_protocol::VGI_PROTOCOL_VERSION)
        .relax_nullability(true)
        .on_log(worker_logs.callback())
}

/// Where a worker's in-band log batches go.
///
/// A worker reports progress, pruning decisions and warnings by emitting
/// zero-row batches carrying `vgi_rpc.log_level` alongside its data — the only
/// diagnostic channel it has on a transport where stderr is either the RPC pipe
/// itself or on another machine. Without a sink installed the transport
/// classifies each one and then drops it, so everything a worker author wrote
/// to explain a slow or empty scan vanished between the worker and the caller.
///
/// EXCEPTION level never arrives here, and that is deliberate rather than
/// incidental: the transport turns an EXCEPTION batch into an `Err` before any
/// sink is consulted, so a worker failure is a failed call and not a line in a
/// log nobody read. The mapping below exists for a peer that mislabels one, and
/// it is loud for the same reason.
fn default_worker_log_sink() -> WorkerLogSink {
    Arc::new(|msg: vgi_rpc_client::LogMessage| {
        log::log!(
            target: "vgi::worker",
            worker_log_level(msg.level),
            "{}",
            format_worker_log(&msg)
        );
    })
}

/// A worker level as a `log` level.
fn worker_log_level(level: vgi_rpc_client::LogLevel) -> log::Level {
    use vgi_rpc_client::LogLevel;
    match level {
        LogLevel::Trace => log::Level::Trace,
        LogLevel::Debug => log::Level::Debug,
        LogLevel::Info => log::Level::Info,
        LogLevel::Warn => log::Level::Warn,
        LogLevel::Error | LogLevel::Exception => log::Level::Error,
    }
}

/// Render one worker log line.
///
/// The extras are the structured half of the message (row counts, split ids,
/// timings), so they travel with it rather than being dropped for want of a
/// structured sink.
fn format_worker_log(msg: &vgi_rpc_client::LogMessage) -> String {
    if msg.extras.is_empty() {
        msg.message.clone()
    } else {
        format!("{} {}", msg.message, msg.extras_json())
    }
}

impl VgiClient {
    /// Build a client over any transport.
    pub fn new(transport: Box<dyn VgiTransport>) -> Self {
        Self {
            transport,
            worker_logs: None,
            exchange_reusable: Arc::new(AtomicBool::new(true)),
        }
    }

    fn configured_stream(client: RpcClient, label: String) -> Self {
        let worker_logs = WorkerLogRouter::default();
        let client = configure(client, &worker_logs);
        Self::with_worker_log_router(Box::new(StreamTransport::new(client, label)), worker_logs)
    }

    pub(crate) fn with_worker_log_router(
        transport: Box<dyn VgiTransport>,
        worker_logs: WorkerLogRouter,
    ) -> Self {
        Self {
            transport,
            worker_logs: Some(worker_logs),
            exchange_reusable: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Replace the log destination for a built-in VGI transport.
    ///
    /// Returns false for a custom [`VgiTransport`], which has no standard
    /// in-band log callback to replace.
    pub fn set_worker_log_sink(&mut self, sink: WorkerLogSink) -> bool {
        let Some(router) = &self.worker_logs else {
            return false;
        };
        router.replace(sink);
        true
    }

    pub(crate) fn reset_worker_log_sink(&mut self) {
        if let Some(router) = &self.worker_logs {
            router.reset();
        }
    }

    pub(crate) fn exchange_reuse_guard(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.exchange_reusable)
    }

    pub(crate) fn exchange_is_reusable(&self) -> bool {
        self.exchange_reusable.load(Ordering::Acquire) && self.transport.is_reusable()
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

    /// Connect using local per-attachment transport settings.
    ///
    /// Unlike [`Self::connect_location`], subprocess stderr defaults to being
    /// discarded here, matching the VGI `worker_debug = false` ATTACH default.
    pub fn connect_location_with_options(
        location: &VgiLocation,
        options: &ConnectionOptions,
    ) -> Result<Self> {
        match location {
            VgiLocation::Subprocess(argv) => Self::connect_subprocess_with_debug_and_timeout(
                argv,
                options.worker_debug,
                options.rpc_timeout,
            ),
            VgiLocation::Tcp { host, port } => {
                let client = RpcClient::tcp_connect_with_timeout(host, *port, options.rpc_timeout)?;
                Ok(Self::configured_stream(
                    client,
                    format!("tcp://{host}:{port}"),
                ))
            }
            #[cfg(feature = "http")]
            VgiLocation::Http(url) => Self::connect_http_with_retry_and_timeout(
                url,
                crate::retry::RetryPolicy::default(),
                options.rpc_timeout,
            ),
            #[cfg(not(feature = "http"))]
            VgiLocation::Http(_) => Err(vgi_rpc::errors::RpcError::value_error(
                "http:// LOCATIONs need the `http` feature",
            )),
            #[cfg(feature = "unix")]
            VgiLocation::Unix(path) => {
                let label = format!("unix://{}", path.display());
                let client = RpcClient::unix_connect_with_timeout(path, options.rpc_timeout)?;
                Ok(Self::configured_stream(client, label))
            }
            #[cfg(not(feature = "unix"))]
            VgiLocation::Unix(_) => Err(vgi_rpc::errors::RpcError::value_error(
                "unix:// LOCATIONs need the `unix` feature",
            )),
            #[cfg(feature = "launcher")]
            VgiLocation::Launch(argv) => {
                let mut config = crate::launcher::LaunchConfig::default();
                if let Some(timeout) = options.launcher_idle_timeout {
                    config.idle_timeout = timeout;
                }
                config.state_dir = options.launcher_state_dir.clone();
                let sock = crate::launcher::ensure_worker(argv, &config)?;
                let label = format!("unix://{}", sock.display());
                let client = RpcClient::unix_connect_with_timeout(sock, options.rpc_timeout)?;
                Ok(Self::configured_stream(client, label))
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
        let client = RpcClient::connect(cmd)?;
        Ok(Self::configured_stream(client, label))
    }

    /// Spawn a subprocess with explicit control over whether stderr is shown.
    pub fn connect_subprocess_with_debug<S: AsRef<OsStr>>(
        cmd: &[S],
        worker_debug: bool,
    ) -> Result<Self> {
        Self::connect_subprocess_with_debug_and_timeout(cmd, worker_debug, None)
    }

    fn connect_subprocess_with_debug_and_timeout<S: AsRef<OsStr>>(
        cmd: &[S],
        worker_debug: bool,
        rpc_timeout: Option<Duration>,
    ) -> Result<Self> {
        let label = cmd
            .first()
            .map(|s| s.as_ref().to_string_lossy().into_owned())
            .unwrap_or_else(|| "subprocess".to_string());
        let stderr = if worker_debug {
            vgi_rpc_client::StderrMode::Inherit
        } else {
            vgi_rpc_client::StderrMode::Null
        };
        let transport = vgi_rpc_client::SubprocessTransport::spawn_with_stderr_and_timeout(
            cmd,
            stderr,
            rpc_timeout,
        )?;
        let client = RpcClient::from_transport(Box::new(transport));
        Ok(Self::configured_stream(client, label))
    }

    /// Connect to a worker listening on TCP.
    pub fn connect_tcp(host: &str, port: u16) -> Result<Self> {
        let client = RpcClient::tcp_connect(host, port)?;
        Ok(Self::configured_stream(
            client,
            format!("tcp://{host}:{port}"),
        ))
    }

    /// Connect to a worker on a Unix domain socket.
    #[cfg(feature = "unix")]
    pub fn connect_unix(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let label = format!("unix://{}", path.as_ref().display());
        let client = RpcClient::unix_connect(path)?;
        Ok(Self::configured_stream(client, label))
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
        Self::connect_http_with_retry_and_timeout(base_url, policy, Some(Duration::from_secs(30)))
    }

    /// As [`Self::connect_http_with_retry`], with an explicit request timeout.
    #[cfg(feature = "http")]
    pub fn connect_http_with_retry_and_timeout(
        base_url: &str,
        policy: crate::retry::RetryPolicy,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        use crate::transport::HttpTransport;
        let worker_logs = WorkerLogRouter::default();
        let client = vgi_rpc_client::HttpClient::connect(base_url.to_string())
            .protocol_version(vgi_protocol::VGI_PROTOCOL_VERSION)
            .on_log(worker_logs.callback())
            .timeout(timeout)
            .build()?;
        let http = Box::new(HttpTransport::new(client, base_url.to_string()));
        Ok(Self::with_worker_log_router(
            Box::new(crate::retry::RetryTransport::new(http, policy)),
            worker_logs,
        ))
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
        timeout: Option<Duration>,
    ) -> Self {
        let worker_logs = WorkerLogRouter::default();
        let inner = Box::new(
            crate::auth::AuthenticatedHttpTransport::new_with_worker_logs(
                base_url,
                auth,
                timeout,
                worker_logs.clone(),
            ),
        );
        // Retry sits OUTSIDE the 401 recovery, not inside it: a 401 is fatal to
        // the retry classifier and passes straight through to the credential
        // refresh, while a 429 that the refresh would have no answer for is
        // absorbed here.
        Self::with_worker_log_router(
            Box::new(crate::retry::RetryTransport::new(
                inner,
                crate::retry::RetryPolicy::default(),
            )),
            worker_logs,
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use vgi_rpc_client::{LogLevel, LogMessage};

    #[test]
    fn worker_levels_map_across_and_exception_stays_loud() {
        assert_eq!(worker_log_level(LogLevel::Trace), log::Level::Trace);
        assert_eq!(worker_log_level(LogLevel::Debug), log::Level::Debug);
        assert_eq!(worker_log_level(LogLevel::Info), log::Level::Info);
        assert_eq!(worker_log_level(LogLevel::Warn), log::Level::Warn);
        assert_eq!(worker_log_level(LogLevel::Error), log::Level::Error);
        // An EXCEPTION reaching a log sink at all means a peer mislabelled it —
        // the transport turns a real one into a failed call — so it must not
        // land below Error, where a default filter would hide it.
        assert_eq!(worker_log_level(LogLevel::Exception), log::Level::Error);
    }

    #[test]
    fn extras_travel_with_the_message() {
        let bare = LogMessage::new(LogLevel::Info, "pruned 3 splits");
        assert_eq!(format_worker_log(&bare), "pruned 3 splits");

        let rich = LogMessage::new(LogLevel::Info, "pruned").with_extra("splits", "3");
        assert_eq!(format_worker_log(&rich), r#"pruned {"splits":"3"}"#);
    }

    #[test]
    fn worker_log_router_can_be_replaced_and_reset() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let delivered = Arc::new(AtomicUsize::new(0));
        let router = WorkerLogRouter::default();
        let seen = Arc::clone(&delivered);
        let reentrant = router.clone();
        router.replace(Arc::new(move |message| {
            assert_eq!(message.level, LogLevel::Warn);
            assert_eq!(message.message, "slow split");
            assert!(message
                .extras
                .iter()
                .any(|(key, value)| key == "split" && value == "7"));
            seen.fetch_add(1, Ordering::Relaxed);
            reentrant.reset();
        }));
        router.emit(LogMessage::new(LogLevel::Warn, "slow split").with_extra("split", "7"));
        assert_eq!(delivered.load(Ordering::Relaxed), 1);

        router.emit(LogMessage::new(LogLevel::Warn, "not routed to prior owner"));
        assert_eq!(delivered.load(Ordering::Relaxed), 1);
    }
}

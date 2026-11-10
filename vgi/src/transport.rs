// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Worker transport selection: stdio (default), AF_UNIX (launcher), TCP, HTTP.
//!
//! Mirrors the conformance worker contract:
//! - stdio: serve a single sequential Arrow-IPC stream over stdin/stdout.
//! - `--unix <path>`: bind the socket, print `UNIX:<path>\n`, serve each
//!   connection on its own thread until SIGTERM/SIGINT.
//! - `--tcp [<host>:]<port>`: bind a TCP socket, print `TCP:<host>:<port>\n`,
//!   serve each connection on its own thread (raw framing, no auth/TLS).
//! - `--http`: print `PORT:<n>\n`, serve axum (added with the `http` feature).

use std::io::{self, Write};
use std::sync::Arc;

/// Trust boundary between a loopback VGI worker and `vgi-iroh-bridge`.
#[derive(Clone, Debug)]
pub struct IrohBridgeOptions {
    pub issuer: String,
    pub trusted_proxy_addresses: Vec<String>,
    pub authenticate: bool,
}

impl IrohBridgeOptions {
    pub fn loopback(issuer: impl Into<String>) -> Self {
        Self {
            issuer: issuer.into(),
            trusted_proxy_addresses: vec!["127.0.0.1".to_string()],
            authenticate: true,
        }
    }
}

use vgi_rpc::{RpcServer, TransportCapabilities, TransportKind};

/// Join every per-connection thread that has already returned.
///
/// The unix/tcp accept loops keep a `JoinHandle` per connection so shutdown
/// can wait for in-flight calls. A thread that has exited but was neither
/// joined nor detached keeps its stack mapping (and guard page) until it is,
/// so without reaping a long-lived pooled worker leaks two memory mappings
/// per connection it ever served. After ~32k connections that reaches Linux's
/// default `vm.max_map_count` (65530): the next `thread::spawn` cannot map its
/// signal-stack guard page, the Rust runtime aborts, and the client blocks on
/// the socket until the core dump of the bloated process finishes.
fn reap_finished(threads: &mut Vec<std::thread::JoinHandle<()>>) {
    let mut index = 0;
    while index < threads.len() {
        if threads[index].is_finished() {
            let _ = threads.swap_remove(index).join();
        } else {
            index += 1;
        }
    }
}

/// Serve a single sequential Arrow-IPC stream over stdin/stdout until EOF.
pub fn serve_stdio(server: Arc<RpcServer>) {
    server.notify_transport(TransportKind::Pipe, TransportCapabilities::none());
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut r = io::BufReader::with_capacity(1024 * 1024, stdin.lock());
    let mut w = io::BufWriter::with_capacity(1024 * 1024, stdout.lock());
    server.serve(&mut r, &mut w);
    let _ = w.flush();
}

/// Bind an AF_UNIX socket, announce it with `UNIX:<path>`, and serve each
/// inbound connection on a worker thread. `idle_timeout` (seconds, 0 =
/// never) self-shuts the worker after that long without a new connection,
/// matching the launcher protocol.
///
/// Unix-only: the launcher transport relies on AF_UNIX sockets. On other
/// platforms the worker falls back to stdio/HTTP (see [`crate::Worker::run`]).
#[cfg(unix)]
pub fn serve_unix(server: Arc<RpcServer>, path: &str, idle_timeout: f64) {
    use std::os::unix::net::UnixListener;
    use std::sync::atomic::{AtomicBool, Ordering};

    server.notify_transport(TransportKind::Unix, TransportCapabilities::none());
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path).expect("bind unix socket");
    listener.set_nonblocking(true).ok();
    println!("UNIX:{path}");
    io::stdout().flush().ok();

    let shutdown = Arc::new(AtomicBool::new(false));
    // ctrlc is target-gated out on wasm32 (no signal handler there); emscripten is
    // unix-family, so this #[cfg(unix)] body still compiles for it — gate the
    // handler install so the wasm build doesn't reference the unlinked crate. On
    // wasm the worker serves over stdio and exits on EOF, so no handler is needed.
    #[cfg(not(target_arch = "wasm32"))]
    {
        let sd = shutdown.clone();
        let _ = ctrlc::try_set_handler(move || sd.store(true, Ordering::Relaxed));
    }

    let idle = if idle_timeout > 0.0 {
        Some(std::time::Duration::from_secs_f64(idle_timeout))
    } else {
        None
    };
    let mut last_activity = std::time::Instant::now();
    let mut threads: Vec<std::thread::JoinHandle<()>> = Vec::new();

    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut conn, _)) => {
                last_activity = std::time::Instant::now();
                conn.set_nonblocking(false).ok();
                let srv = server.clone();
                reap_finished(&mut threads);
                threads.push(std::thread::spawn(move || {
                    let Ok(mut reader) = conn.try_clone() else {
                        return;
                    };
                    srv.serve(&mut reader, &mut conn);
                }));
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                if let Some(timeout) = idle {
                    if last_activity.elapsed() >= timeout {
                        break;
                    }
                }
                // Poll frequently: a parallel scan opens its secondary worker
                // connections in a burst shortly after the primary's, and a
                // long poll here would accept them late — letting the primary
                // drain the shared work queue before they start (collapsing the
                // scan onto one connection over the launcher transport).
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(_) => break,
        }
    }
    drop(listener);
    let _ = std::fs::remove_file(path);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    for t in threads {
        if std::time::Instant::now() >= deadline {
            break;
        }
        let _ = t.join();
    }
}

/// Bind a TCP socket, announce it with `TCP:<host>:<port>` (the actual bound
/// port, so `port == 0` ephemeral binds are discoverable), and serve each
/// inbound connection on a worker thread. `idle_timeout` (seconds, 0 = never)
/// self-shuts the worker after that long without a new connection, matching the
/// launcher protocol.
///
/// Raw TCP framing carries no authentication or TLS — bind loopback / a trusted
/// network only; use [`serve_http`] for untrusted networks.
///
/// Native only: this thread-per-connection variant needs `std::thread`, which
/// wasm lacks. A single-thread wasm `serve_tcp` (wasip2 `std::net`, no threads)
/// is added separately for the in-browser/wasmtime shared-worker path.
#[cfg(not(target_arch = "wasm32"))]
pub fn serve_tcp(server: Arc<RpcServer>, host: &str, port: u16, idle_timeout: f64) {
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, Ordering};

    server.notify_transport(TransportKind::Tcp, TransportCapabilities::none());
    let listener = TcpListener::bind((host, port)).expect("bind tcp socket");
    let bound_port = listener.local_addr().expect("tcp local_addr").port();
    listener.set_nonblocking(true).ok();
    println!("TCP:{host}:{bound_port}");
    io::stdout().flush().ok();

    let shutdown = Arc::new(AtomicBool::new(false));
    // ctrlc is target-gated out on wasm32 (no signal handler there); emscripten is
    // unix-family, so this #[cfg(unix)] body still compiles for it — gate the
    // handler install so the wasm build doesn't reference the unlinked crate. On
    // wasm the worker serves over stdio and exits on EOF, so no handler is needed.
    #[cfg(not(target_arch = "wasm32"))]
    {
        let sd = shutdown.clone();
        let _ = ctrlc::try_set_handler(move || sd.store(true, Ordering::Relaxed));
    }

    let idle = if idle_timeout > 0.0 {
        Some(std::time::Duration::from_secs_f64(idle_timeout))
    } else {
        None
    };
    let mut last_activity = std::time::Instant::now();
    let mut threads: Vec<std::thread::JoinHandle<()>> = Vec::new();

    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((mut conn, _)) => {
                last_activity = std::time::Instant::now();
                conn.set_nonblocking(false).ok();
                // Disable Nagle so the lockstep request/response framing is not
                // delayed waiting to coalesce writes.
                conn.set_nodelay(true).ok();
                let srv = server.clone();
                reap_finished(&mut threads);
                threads.push(std::thread::spawn(move || {
                    let Ok(mut reader) = conn.try_clone() else {
                        return;
                    };
                    srv.serve(&mut reader, &mut conn);
                }));
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                if let Some(timeout) = idle {
                    if last_activity.elapsed() >= timeout {
                        break;
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(_) => break,
        }
    }
    drop(listener);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    for t in threads {
        if std::time::Instant::now() >= deadline {
            break;
        }
        let _ = t.join();
    }
}

/// Serve the identity-preserving raw upstream consumed by `vgi-iroh-bridge`.
#[cfg(not(target_arch = "wasm32"))]
pub fn serve_iroh_tcp_upstream(
    server: Arc<RpcServer>,
    host: &str,
    port: u16,
    idle_timeout: f64,
    bridge: IrohBridgeOptions,
) {
    use std::collections::BTreeSet;
    use std::net::IpAddr;
    use std::sync::atomic::{AtomicBool, Ordering};

    let bind_ip = host.parse::<IpAddr>().unwrap_or_else(|_| {
        if host == "localhost" {
            IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        } else {
            panic!("Iroh bridge upstream host must be a loopback IP")
        }
    });
    assert!(
        bind_ip.is_loopback(),
        "Iroh bridge upstream must bind loopback"
    );
    let trusted_proxy_addresses = bridge
        .trusted_proxy_addresses
        .iter()
        .map(|value| {
            value
                .parse::<IpAddr>()
                .expect("trusted Iroh proxy must be an exact IP")
        })
        .collect::<BTreeSet<_>>();
    let identity = vgi_rpc::tcp::TcpIdentityOptions {
        proxy_protocol_v2_required: true,
        trusted_proxy_addresses,
        iroh_proxy_issuer: Some(bridge.issuer),
        policy: Some(if bridge.authenticate {
            vgi_rpc::peer_identity_primary("iroh")
        } else {
            vgi_rpc::observe_peer_identity()
        }),
        ..Default::default()
    };
    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        let _ = ctrlc::try_set_handler(move || shutdown.store(true, Ordering::Relaxed));
    }
    let idle = (idle_timeout > 0.0).then(|| std::time::Duration::from_secs_f64(idle_timeout));
    vgi_rpc::tcp::serve_tcp_with_identity(
        server,
        host,
        port,
        idle,
        shutdown,
        identity,
        |bound_host, bound_port| {
            println!("TCP:{bound_host}:{bound_port}");
            io::stdout().flush().ok();
        },
    )
    .expect("serve Iroh bridge upstream");
}

/// Serve over HTTP: bind a TCP port, announce it with `PORT:<n>`, and serve
/// the axum router. An optional `authenticate` callback enables bearer auth.
///
/// Gated on `transport-http` (tokio/axum) so the crate stays wasm-buildable.
#[cfg(feature = "transport-http")]
pub fn serve_http(
    server: Arc<RpcServer>,
    authenticate: Option<vgi_rpc::Authenticate>,
    landing_info: Option<vgi_rpc::http::LandingInfo>,
) {
    serve_http_inner(server, authenticate, landing_info, None, None);
}

/// Serve HTTP at an explicit address while preserving the standard HTTP state machine.
#[cfg(feature = "transport-http")]
pub fn serve_http_at(
    server: Arc<RpcServer>,
    authenticate: Option<vgi_rpc::Authenticate>,
    landing_info: Option<vgi_rpc::http::LandingInfo>,
    host: &str,
    port: u16,
) {
    serve_http_inner(
        server,
        authenticate,
        landing_info,
        None,
        Some(format_http_bind(host, port)),
    );
}

/// Serve HTTP behind `vgi-iroh-bridge`, retaining the authenticated EndpointId.
#[cfg(feature = "transport-http")]
pub fn serve_http_behind_iroh(
    server: Arc<RpcServer>,
    authenticate: Option<vgi_rpc::Authenticate>,
    landing_info: Option<vgi_rpc::http::LandingInfo>,
    bridge: IrohBridgeOptions,
) {
    serve_http_inner(server, authenticate, landing_info, Some(bridge), None);
}

/// Serve an Iroh bridge's HTTP upstream at an explicit loopback address.
#[cfg(feature = "transport-http")]
pub fn serve_http_behind_iroh_at(
    server: Arc<RpcServer>,
    authenticate: Option<vgi_rpc::Authenticate>,
    landing_info: Option<vgi_rpc::http::LandingInfo>,
    bridge: IrohBridgeOptions,
    host: &str,
    port: u16,
) {
    let bind_ip = host.parse::<std::net::IpAddr>().unwrap_or_else(|_| {
        if host == "localhost" {
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
        } else {
            panic!("Iroh HTTP bridge upstream host must be a loopback IP")
        }
    });
    assert!(
        bind_ip.is_loopback(),
        "Iroh HTTP bridge upstream must bind loopback"
    );
    serve_http_inner(
        server,
        authenticate,
        landing_info,
        Some(bridge),
        Some(format_http_bind(host, port)),
    );
}

#[cfg(feature = "transport-http")]
fn format_http_bind(host: &str, port: u16) -> String {
    if host.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[cfg(feature = "transport-http")]
fn serve_http_inner(
    server: Arc<RpcServer>,
    authenticate: Option<vgi_rpc::Authenticate>,
    landing_info: Option<vgi_rpc::http::LandingInfo>,
    bridge: Option<IrohBridgeOptions>,
    explicit_bind: Option<String>,
) {
    if std::env::var("VGI_HTTP_PANIC_TRACE").is_ok() {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            eprintln!("[VGI HTTP PANIC] {info}");
            prev(info);
        }));
    }
    server.notify_transport(TransportKind::Http, TransportCapabilities::none());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async move {
        let mut builder = vgi_rpc::http::HttpState::builder()
            .server(server)
            // Sticky sessions for the versioned HTTP fixtures' cookie routing.
            .enable_sticky(true)
            // One output batch per producer HTTP response, then yield a
            // continuation token — matching the Python and Go workers
            // (`SetProducerBatchLimit(1)`). Resumable table scans serialize their
            // position into the token so the whole result set never has to fit in
            // memory; producers that can't serialize their position drain fully
            // via a per-producer override (see `TableProducerState::batch_limit`).
            .producer_batch_limit(1);
        if let Some(auth) = authenticate {
            builder = builder.authenticate(auth);
        }
        if let Some(bridge) = bridge {
            let provider = vgi_rpc::iroh_forwarded_header_provider(
                vgi_rpc::IrohForwardedHeaderConfig::new(
                    bridge.issuer,
                    bridge.trusted_proxy_addresses,
                )
                .expect("valid Iroh HTTP bridge trust"),
            )
            .expect("valid Iroh HTTP identity provider");
            builder = builder
                .peer_identity_providers([provider])
                .peer_authentication_policy(if bridge.authenticate {
                    vgi_rpc::peer_identity_primary("iroh")
                } else {
                    vgi_rpc::observe_peer_identity()
                });
        }
        // Standardized landing surface: the shared page plus the browser client
        // build it reads the catalog with. Only the worker's identity is
        // supplied here; the catalog is read over the protocol.
        if let Some(info) = landing_info {
            builder = builder.landing_info(info);
        }
        let state = builder.build();
        // Default to loopback + ephemeral port (the local test harness reads the
        // `PORT:` line). A deployed worker (e.g. on fly.io, reached by remote
        // DuckDB clients) sets `VGI_HTTP_BIND=0.0.0.0:8080`.
        let bind = explicit_bind.unwrap_or_else(|| {
            std::env::var("VGI_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1:0".to_string())
        });
        let listener = tokio::net::TcpListener::bind(&bind)
            .await
            .unwrap_or_else(|e| panic!("bind {bind}: {e}"));
        let port = listener.local_addr().unwrap().port();
        println!("PORT:{port}");
        io::stdout().flush().ok();
        vgi_rpc::http::serve_with_shutdown(state, listener)
            .await
            .expect("axum serve");
    });
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    fn mapping_count() -> usize {
        std::fs::read_to_string("/proc/self/maps")
            .expect("read /proc/self/maps")
            .lines()
            .count()
    }

    /// A long-lived launcher worker must not accumulate per-connection thread
    /// stacks. Unreaped, every short connection left two mappings behind
    /// (stack + guard page), so a pooled worker serving the whole integration
    /// suite hit `vm.max_map_count` and aborted mid-suite.
    #[test]
    fn serve_unix_releases_finished_connection_threads() {
        use std::os::unix::net::UnixStream;

        let dir = std::env::temp_dir().join(format!("vgi-reap-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("w.sock");
        let path_str = path.to_str().unwrap().to_string();
        let server = Arc::new(crate::Worker::new().build_server());
        let serve_path = path_str.clone();
        let serving = std::thread::spawn(move || serve_unix(server, &serve_path, 3.0));

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while UnixStream::connect(&path).is_err() {
            assert!(std::time::Instant::now() < deadline, "worker never bound");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        // Let the probe connection's thread spawn and finish before measuring.
        std::thread::sleep(std::time::Duration::from_millis(200));
        let before = mapping_count();

        const CONNECTIONS: usize = 2000;
        for _ in 0..CONNECTIONS {
            // Connect and hang up: the server's serve() sees EOF and returns.
            drop(UnixStream::connect(&path).expect("connect"));
            // Pace a little so threads finish and are reaped as we go.
            std::thread::sleep(std::time::Duration::from_micros(300));
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        // One more accept runs the reaper over everything that has finished.
        drop(UnixStream::connect(&path).expect("connect"));
        std::thread::sleep(std::time::Duration::from_millis(100));
        let after = mapping_count();

        // Unreaped this grows by ~2 * CONNECTIONS; allow generous slack for
        // other tests' threads and allocator arenas in the same process.
        let growth = after.saturating_sub(before);
        assert!(
            growth < CONNECTIONS / 2,
            "serve_unix leaked {growth} mappings over {CONNECTIONS} connections"
        );

        serving.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Worker transport selection: stdio (default), AF_UNIX (launcher), HTTP.
//!
//! Mirrors the conformance worker contract:
//! - stdio: serve a single sequential Arrow-IPC stream over stdin/stdout.
//! - `--unix <path>`: bind the socket, print `UNIX:<path>\n`, serve each
//!   connection on its own thread until SIGTERM/SIGINT.
//! - `--http`: print `PORT:<n>\n`, serve axum (added with the `http` feature).

use std::io::{self, Write};
use std::sync::Arc;

use vgi_rpc::{RpcServer, TransportCapabilities, TransportKind};

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

/// Serve over HTTP: bind a TCP port, announce it with `PORT:<n>`, and serve
/// the axum router. An optional `authenticate` callback enables bearer auth.
pub fn serve_http(server: Arc<RpcServer>, authenticate: Option<vgi_rpc::Authenticate>) {
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
            // Drain each producer entirely within the init response so table
            // scans never require a stateless continuation token (only the
            // stateless scalar/table-in-out exchange paths need a state
            // decoder; producers carry scan position that we don't serialize).
            .producer_batch_limit(0);
        if let Some(auth) = authenticate {
            builder = builder.authenticate(auth);
        }
        let state = builder.build();
        // Default to loopback + ephemeral port (the local test harness reads the
        // `PORT:` line). A deployed worker (e.g. on fly.io, reached by remote
        // DuckDB clients) sets `VGI_HTTP_BIND=0.0.0.0:8080`.
        let bind = std::env::var("VGI_HTTP_BIND").unwrap_or_else(|_| "127.0.0.1:0".to_string());
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

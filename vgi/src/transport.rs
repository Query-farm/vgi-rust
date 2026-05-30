//! Worker transport selection: stdio (default), AF_UNIX (launcher), HTTP.
//!
//! Mirrors the Go / conformance worker contract:
//! - stdio: serve a single sequential Arrow-IPC stream over stdin/stdout.
//! - `--unix <path>`: bind the socket, print `UNIX:<path>\n`, serve each
//!   connection on its own thread until SIGTERM/SIGINT.
//! - `--http`: print `PORT:<n>\n`, serve axum (added with the `http` feature).

use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
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
pub fn serve_unix(server: Arc<RpcServer>, path: &str, idle_timeout: f64) {
    use std::os::unix::net::UnixListener;

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
                std::thread::sleep(std::time::Duration::from_millis(50));
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

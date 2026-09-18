// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The `launch:` transport — one warm worker shared system-wide.
//!
//! # What it buys over the pool
//!
//! [`WorkerPool`](crate::pool::WorkerPool) amortises worker startup across one
//! *process*. The launcher amortises it across **every process on the machine**
//! pointing at the same worker tuple: the first one to arrive spawns the worker
//! on an AF_UNIX socket, and everyone else — other Rust clients, other DuckDB
//! processes, a test runner and the editor's language server — connects to that
//! same interpreter. The extension measures this at roughly 5× the per-process
//! subprocess pool for real test workloads.
//!
//! # This is a cross-language contract
//!
//! `vgi/docs/launcher-protocol.md` is the single source of truth, shared with
//! the Python reference launcher (`vgi-rpc/vgi_rpc/launcher.py`) and the C++
//! launcher inside the DuckDB extension. Sharing a warm worker means agreeing
//! byte-for-byte on:
//!
//! * the **hash** of `(argv, cwd, VGI_RPC_* env)` — it names the socket, so a
//!   one-byte difference silently starts a second worker instead of joining the
//!   first. [`compute_hash`] is asserted against the same golden vectors the
//!   C++ port uses.
//! * the **state directory** layout and its `0700` ownership check.
//! * `flock(2)` specifically — *not* `fcntl(F_SETLK)`. The two do not
//!   interlock, so a port that picks the other one appears to work and then
//!   races.
//! * the worker CLI (`--unix PATH --idle-timeout SEC`) and the single
//!   `UNIX:<path>` discovery line on stdout.
//! * what a busy worker looks like: a listener whose accept queue is full is
//!   **alive**, and its socket must never be unlinked. See [`ensure_worker`]
//!   for how the probe tells the two apart, and [`connect`] for how a client
//!   waits one out.
//!
//! # Known divergence
//!
//! For bytes ≥ `0x7F` the C++ launcher passes raw UTF-8 through the canonical
//! JSON while Python escapes as `\uXXXX`, so their hashes already differ for
//! non-ASCII input. This port follows **C++**, because sharing a worker with
//! the DuckDB extension is the reason it exists. Every golden vector is ASCII,
//! where all three agree. `VGI_RPC_*` values are ASCII by convention; a
//! non-ASCII `cwd` is the realistic trigger.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};
use vgi_rpc::errors::{Result, RpcError};
use vgi_rpc_client::RpcClient;

/// The scheme prefix a POSIX worker prints to advertise its socket.
const DISCOVERY_PREFIX: &str = "UNIX:";

/// Cap on pre-discovery stdout noise before the worker is considered broken.
const MAX_PREAMBLE_BYTES: usize = 1024 * 1024;

/// Pauses between re-probes of a socket that refused — see [`probe`]. macOS
/// reports a full accept queue as `ECONNREFUSED`, the same as no listener, so a
/// refusal is believed only after these; a socket left by a dead worker pays
/// the 350 ms once, before it is replaced. The same schedule as the Python and
/// C++ launchers.
const PROBE_REFUSED_BACKOFF: [Duration; 3] = [
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(200),
];

/// Ceiling on the pause between connect attempts while a worker's accept queue
/// is full — see [`connect`]. The first pause is 1 ms and doubles up to this.
const BUSY_BACKOFF_CAP: Duration = Duration::from_millis(50);

/// How a `launch:` worker should be started.
#[derive(Debug, Clone)]
pub struct LaunchConfig {
    /// Self-shutdown after this long with no connected clients.
    ///
    /// [`Duration::ZERO`] means "never" — the wire encoding is `0`.
    pub idle_timeout: Duration,
    /// Override the state directory. Escape valve only; it does **not**
    /// isolate this client from other processes using the same argv, because
    /// they will not be looking here.
    pub state_dir: Option<PathBuf>,
    /// How long to wait for the worker's discovery line.
    pub spawn_timeout: Duration,
    /// Where the spawned worker's stderr goes. `None` means `/dev/null`.
    ///
    /// It cannot be inherited: the worker outlives this process by design, so
    /// holding our stderr would keep the fd open for its whole idle timeout —
    /// which hangs any parent waiting for our output to end, `cargo test`
    /// included. The C++ launcher makes the same choice, with the same escape
    /// valve.
    pub stderr_path: Option<PathBuf>,
    /// How long a client connect waits for a slot in a busy worker's accept
    /// queue before giving up. See [`connect`].
    pub connect_timeout: Duration,
}

impl Default for LaunchConfig {
    fn default() -> Self {
        Self {
            // Matches the extension's `launcher_idle_timeout` default.
            idle_timeout: Duration::from_secs(300),
            state_dir: None,
            spawn_timeout: Duration::from_secs(60),
            stderr_path: None,
            // The C++ launcher's `ResolveAndConnect` default, sized to absorb
            // accept-queue delays under a burst of connections.
            connect_timeout: Duration::from_secs(10),
        }
    }
}

/// Per-process cache of resolved socket paths, so a repeat `launch:` is a
/// hash lookup rather than a flock-and-probe.
///
/// Invalidated by [`ensure_worker`] when the cached socket refuses a
/// connection — the worker idle-shut-down and a fresh launch is due.
static RESOLVED: Mutex<Option<BTreeMap<String, PathBuf>>> = Mutex::new(None);

/// Hash the `(argv, cwd, VGI_RPC_* env)` tuple that names a worker.
///
/// The first 16 hex characters of the SHA-256 of a canonical JSON object with
/// keys `cmd`, `cwd`, `env` — sorted, no whitespace. See the module docs for
/// why this must match the other implementations exactly.
pub fn compute_hash(argv: &[String], cwd: &str, env: &BTreeMap<String, String>) -> String {
    let mut json = String::from("{\"cmd\":[");
    for (i, a) in argv.iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        push_json_string(&mut json, a);
    }
    json.push_str("],\"cwd\":");
    push_json_string(&mut json, cwd);
    json.push_str(",\"env\":{");
    for (i, (k, v)) in env.iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        push_json_string(&mut json, k);
        json.push(':');
        push_json_string(&mut json, v);
    }
    json.push_str("}}");

    let digest = Sha256::digest(json.as_bytes());
    digest.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// Append `s` as a JSON string literal, matching Python's `json.dumps`
/// defaults for ASCII and the C++ launcher's raw passthrough above it.
fn push_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// The `VGI_RPC_*` subset of the environment, which is what the hash covers.
///
/// Everything else (`PATH`, `HOME`, …) is deliberately excluded: only settings
/// a worker reads out of band and that change its behaviour should fork the
/// worker identity.
pub fn hashed_env() -> BTreeMap<String, String> {
    std::env::vars()
        .filter(|(k, _)| k.starts_with("VGI_RPC_"))
        .collect()
}

/// Resolve the per-user state directory, creating it `0700` if absent.
///
/// Refuses a directory owned by another uid — it holds sockets that would
/// otherwise let another user's worker answer our RPCs.
pub fn state_dir(override_dir: Option<&Path>) -> Result<PathBuf> {
    let dir = match override_dir {
        Some(d) => d.to_path_buf(),
        None => default_state_dir(),
    };
    if !dir.exists() {
        fs::create_dir_all(&dir)
            .map_err(|e| RpcError::runtime_error(format!("create {}: {e}", dir.display())))?;
        set_mode_0700(&dir)?;
    }
    let meta = fs::metadata(&dir)
        .map_err(|e| RpcError::runtime_error(format!("stat {}: {e}", dir.display())))?;
    let me = unsafe { libc::geteuid() };
    if meta.uid() != me {
        return Err(RpcError::runtime_error(format!(
            "launcher state dir {} is owned by uid {}, not {me}",
            dir.display(),
            meta.uid()
        )));
    }
    Ok(dir)
}

fn default_state_dir() -> PathBuf {
    // Linux with a runtime dir gets the unsuffixed name; everything else gets
    // a uid suffix because $TMPDIR may be shared.
    if let Ok(xdg) = std::env::var("XDG_RUNTIME_DIR") {
        if !xdg.is_empty() {
            return PathBuf::from(xdg).join("vgi-rpc");
        }
    }
    let tmp = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
    let uid = unsafe { libc::geteuid() };
    PathBuf::from(tmp).join(format!("vgi-rpc-{uid}"))
}

fn set_mode_0700(dir: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
        .map_err(|e| RpcError::runtime_error(format!("chmod 0700 {}: {e}", dir.display())))
}

/// Encode an idle timeout the way the worker CLI expects.
///
/// Plain decimal seconds, trailing zeros stripped — never scientific notation,
/// so every language's parser reads the same bytes. `0` means unbounded.
pub fn encode_idle_timeout(d: Duration) -> String {
    if d.is_zero() {
        return "0".to_string();
    }
    let s = format!("{:.3}", d.as_secs_f64());
    let s = s.trim_end_matches('0').trim_end_matches('.');
    s.to_string()
}

/// Ensure a worker is serving for `argv`, and return its socket path.
///
/// Fast path is the per-process cache. Otherwise: take the per-tuple `flock`,
/// probe for an existing listener, and spawn only if there is none — so
/// concurrent starters elect exactly one launcher.
///
/// A worker whose accept queue is full is **alive**, only busy, and is left
/// alone. Reading it as dead is destructive: the socket gets unlinked out from
/// under the live worker and a duplicate spawned in its place, orphaning the
/// original and racing clients onto a vanished path — the Python reference
/// launcher did exactly that, 64 workers for 2 commands under a 32-process test
/// run. The probe tells the two apart with one non-blocking connect: `EAGAIN`
/// is alive, and `ECONNREFUSED` — which is how macOS reports a full queue — is
/// re-probed after 50, 100 and 200 ms before it is believed.
pub fn ensure_worker(argv: &[String], config: &LaunchConfig) -> Result<PathBuf> {
    let cwd = std::env::current_dir()
        .map_err(|e| RpcError::runtime_error(format!("getcwd: {e}")))?
        .to_string_lossy()
        .into_owned();
    let hash = compute_hash(argv, &cwd, &hashed_env());

    if let Some(path) = cached(&hash) {
        if probe(&path) {
            return Ok(path);
        }
        invalidate(&hash);
    }

    let dir = state_dir(config.state_dir.as_deref())?;
    let sock = dir.join(format!("{hash}.sock"));
    let lock = dir.join(format!("{hash}.lock"));

    let _guard = FlockGuard::acquire(&lock)?;

    // Someone may have started it between our probe and our lock.
    if probe(&sock) {
        remember(&hash, &sock);
        return Ok(sock);
    }
    // A socket file that exists but refuses connect — and kept refusing
    // through `probe`'s re-probes — is stale; the protocol requires unlinking
    // it before the new worker binds.
    if sock.exists() {
        let _ = fs::remove_file(&sock);
    }

    spawn_worker(argv, &sock, config)?;
    write_meta(&dir, &hash, argv, &cwd, &sock);
    remember(&hash, &sock);
    Ok(sock)
}

/// Spawn the worker and wait for its discovery line.
fn spawn_worker(argv: &[String], sock: &Path, config: &LaunchConfig) -> Result<()> {
    let (cmd, args) = argv
        .split_first()
        .ok_or_else(|| RpcError::value_error("launch: has an empty argv"))?;

    let stderr = match &config.stderr_path {
        Some(path) => fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map(Stdio::from)
            .map_err(|e| {
                RpcError::runtime_error(format!("open worker stderr {}: {e}", path.display()))
            })?,
        None => Stdio::null(),
    };

    let mut child = Command::new(cmd)
        .args(args)
        .arg("--unix")
        .arg(sock)
        .arg("--idle-timeout")
        .arg(encode_idle_timeout(config.idle_timeout))
        // stdin null so the worker cannot block on it; stderr never inherited
        // (see `LaunchConfig::stderr_path`).
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(stderr)
        .spawn()
        .map_err(|e| RpcError::runtime_error(format!("spawn launcher worker {cmd}: {e}")))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| RpcError::runtime_error("launcher worker has no stdout"))?;

    match read_discovery_line(stdout, config.spawn_timeout) {
        Ok(path) => {
            if path != sock.to_string_lossy() {
                // Not fatal — the worker is authoritative about where it bound.
                // But it means our hash-derived path is not where it listens,
                // which would desync us from the C++ launcher, so say so.
                return Err(RpcError::runtime_error(format!(
                    "launcher worker bound {path} but the protocol requires {}",
                    sock.display()
                )));
            }
            // The launcher deliberately does not reap: the worker outlives us
            // and reparents to init. Dropping the handle closes our read end,
            // which is what makes any further stdout write kill the worker —
            // the protocol relies on that.
            Ok(())
        }
        Err(e) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(e)
        }
    }
}

/// Read stdout until the `UNIX:` line, skipping any preamble noise.
fn read_discovery_line(stdout: std::process::ChildStdout, timeout: Duration) -> Result<String> {
    // A worker that never prints would hang us forever, so the read runs on a
    // helper thread and the timeout is enforced on the channel.
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut seen = 0usize;
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = tx.send(Err("worker exited before advertising a socket".to_string()));
                    return;
                }
                Ok(n) => {
                    seen += n;
                    if let Some(path) = line.trim_end().strip_prefix(DISCOVERY_PREFIX) {
                        let _ = tx.send(Ok(path.to_string()));
                        // Stop reading and drop the pipe. The protocol requires
                        // exactly this: the worker must stay silent afterwards,
                        // and a write to our closed read end SIGPIPEs it, which
                        // is the intended kill. Draining instead would hold the
                        // fd (and this thread) for the worker's whole life.
                        return;
                    }
                    if seen > MAX_PREAMBLE_BYTES {
                        let _ = tx.send(Err(format!(
                            "worker wrote {seen} bytes to stdout without a {DISCOVERY_PREFIX} line"
                        )));
                        return;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(format!("reading worker stdout: {e}")));
                    return;
                }
            }
        }
    });

    match rx.recv_timeout(timeout) {
        Ok(Ok(path)) => Ok(path),
        Ok(Err(e)) => Err(RpcError::runtime_error(format!("launcher: {e}"))),
        Err(_) => Err(RpcError::runtime_error(format!(
            "launcher: worker did not advertise a socket within {timeout:?}"
        ))),
    }
}

/// Best-effort debugging metadata, mirroring the C++ launcher's `.meta` file.
fn write_meta(dir: &Path, hash: &str, argv: &[String], cwd: &str, sock: &Path) {
    let started = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut cmd = String::from("[");
    for (i, a) in argv.iter().enumerate() {
        if i > 0 {
            cmd.push(',');
        }
        push_json_string(&mut cmd, a);
    }
    cmd.push(']');
    let mut json = format!("{{\"cmd\":{cmd},\"cwd\":");
    push_json_string(&mut json, cwd);
    json.push_str(&format!(
        ",\"started_at\":{started},\"launcher_pid\":{},\"socket\":",
        std::process::id()
    ));
    push_json_string(&mut json, &sock.to_string_lossy());
    json.push('}');
    let _ = fs::write(dir.join(format!("{hash}.meta")), json);
}

/// Is a worker listening on `sock`? The only reliable liveness test — a socket
/// file may outlive the worker that bound it.
///
/// One **non-blocking** connect, classified:
///
/// * connected (or still connecting) — alive;
/// * `EAGAIN`/`EWOULDBLOCK` — alive: Linux's answer when the listener's accept
///   queue is full, distinct from the `ECONNREFUSED` of an unbound socket;
/// * `ECONNREFUSED` — re-probed after 50, 100 and 200 ms before it is believed,
///   because macOS reports a full queue that way too;
/// * anything else (no such file, not a socket, …) — dead.
///
/// Non-blocking because a *blocking* `connect(2)` on Linux waits for a slot in
/// a full queue for as long as it takes — forever, for a wedged worker — which
/// would hang the launcher inside its `flock` and every starter queued behind
/// it. This asks "is anything listening", not "is it responsive": a connect
/// lands in the queue of a worker that never accepts, so a successful one never
/// proved that either. Mirrors `vgi_rpc.launcher._probe` and the extension's
/// `ProbeAlive`.
fn probe(sock: &Path) -> bool {
    for attempt in 0..=PROBE_REFUSED_BACKOFF.len() {
        if attempt > 0 {
            std::thread::sleep(PROBE_REFUSED_BACKOFF[attempt - 1]);
        }
        match start_connect(sock) {
            Ok(_) => return true,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return true,
            Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => continue,
            Err(_) => return false,
        }
    }
    false
}

/// Connect to a launched worker's socket, waiting out a full accept queue.
///
/// A connect that meets a full queue (`EAGAIN` on Linux) is a busy worker, not
/// a dead one: it is retried with a capped backoff (1 ms doubling to 50 ms)
/// until `timeout`, rather than failed — a failure here would read as a dead
/// worker to anything that relaunches on a failed connect. Any other failure is
/// returned at once. The returned stream is in blocking mode.
///
/// `std`'s `UnixStream::connect` cannot do this: it is a blocking `connect(2)`,
/// which on Linux waits on a full queue with no bound at all, and on macOS
/// fails immediately.
pub fn connect(sock: &Path, timeout: Duration) -> Result<UnixStream> {
    connect_io(sock, timeout).map_err(|e| {
        RpcError::new(
            "TransportError",
            format!("connect unix socket {}: {e}", sock.display()),
        )
    })
}

fn connect_io(sock: &Path, timeout: Duration) -> io::Result<UnixStream> {
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(1);
    loop {
        match start_connect(sock) {
            Ok(Started::Connected(stream)) => {
                stream.set_nonblocking(false)?;
                return Ok(stream);
            }
            Ok(Started::InProgress(stream)) => {
                finish_connect(&stream, deadline.saturating_duration_since(Instant::now()))?;
                stream.set_nonblocking(false)?;
                return Ok(stream);
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                let now = Instant::now();
                if now >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!(
                            "timed out after {timeout:?}: the worker's accept queue stayed full"
                        ),
                    ));
                }
                std::thread::sleep(backoff.min(deadline - now));
                backoff = (backoff * 2).min(BUSY_BACKOFF_CAP);
            }
            Err(e) => return Err(e),
        }
    }
}

/// What one non-blocking `connect(2)` achieved.
enum Started {
    Connected(UnixStream),
    /// `EINPROGRESS`. Linux never reports it for `AF_UNIX`; handled anyway, as
    /// the C++ launcher does, rather than assumed impossible everywhere.
    InProgress(UnixStream),
}

/// One non-blocking `connect(2)` to `sock`, leaving the stream non-blocking.
fn start_connect(sock: &Path) -> io::Result<Started> {
    use std::os::unix::ffi::OsStrExt;

    let path = sock.as_os_str().as_bytes();
    // SAFETY: an all-zero `sockaddr_un` is a valid (empty) address.
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    if path.is_empty() || path.contains(&0) || path.len() >= addr.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a usable AF_UNIX socket path",
        ));
    }
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (dst, src) in addr.sun_path.iter_mut().zip(path) {
        *dst = *src as libc::c_char;
    }
    // The family, the path and its terminating NUL — the length `std` passes.
    let sun_path_offset = std::mem::size_of_val(&addr) - std::mem::size_of_val(&addr.sun_path);
    let len = (sun_path_offset + path.len() + 1) as libc::socklen_t;

    // Close-on-exec, as `std` does for its own sockets: a worker this process
    // spawns must not inherit a connection to another one. Atomic where the
    // platform allows it; the `fcntl` below covers the rest.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let ty = libc::SOCK_STREAM | libc::SOCK_CLOEXEC;
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let ty = libc::SOCK_STREAM;
    // SAFETY: plain socket(2); the fd is owned by `stream` from the next line,
    // so every early return below closes it.
    let fd = unsafe { libc::socket(libc::AF_UNIX, ty, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let stream = unsafe { UnixStream::from_raw_fd(fd) };
    // SAFETY: fcntl on an fd we own.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }
    stream.set_nonblocking(true)?;
    // SAFETY: `addr` is a valid `sockaddr_un` and `len` is within it.
    let rc = unsafe {
        libc::connect(
            fd,
            &addr as *const libc::sockaddr_un as *const libc::sockaddr,
            len,
        )
    };
    if rc == 0 {
        return Ok(Started::Connected(stream));
    }
    let err = io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EINPROGRESS) {
        return Ok(Started::InProgress(stream));
    }
    Err(err)
}

/// Wait up to `remaining` for an in-progress connect, then report its result.
fn finish_connect(stream: &UnixStream, remaining: Duration) -> io::Result<()> {
    let mut pfd = libc::pollfd {
        fd: stream.as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    let ms = remaining.as_millis().min(libc::c_int::MAX as u128) as libc::c_int;
    // SAFETY: one valid pollfd.
    match unsafe { libc::poll(&mut pfd, 1, ms) } {
        0 => {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for the connect to complete",
            ))
        }
        n if n < 0 => return Err(io::Error::last_os_error()),
        _ => {}
    }
    match stream.take_error()? {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Connect to a launched worker and wrap the stream as an RPC client.
///
/// `vgi-rpc-client`'s own unix transport can only connect by path, with the
/// blocking `connect(2)` [`connect`] exists to avoid, so this hands it an
/// already-connected stream instead. `read_timeout` is the per-read bound the
/// path-based transport applies.
pub(crate) fn connect_client(
    sock: &Path,
    connect_timeout: Duration,
    read_timeout: Option<Duration>,
) -> Result<RpcClient> {
    let stream = connect(sock, connect_timeout)?;
    // Both ends have to ask, not just the worker: an AF_UNIX write is bounded
    // by space in the *receiver's* buffer.
    vgi_rpc::unix::widen_socket_buffers(&stream);
    if let Some(t) = read_timeout {
        stream
            .set_read_timeout(Some(t))
            .map_err(|e| RpcError::new("TransportError", format!("set unix read timeout: {e}")))?;
    }
    let writer = stream
        .try_clone()
        .map_err(|e| RpcError::new("TransportError", format!("clone unix socket: {e}")))?;
    Ok(RpcClient::from_transport(Box::new(LaunchedTransport {
        reader: BufReader::new(stream),
        writer,
    })))
}

/// A connected launcher socket as a `vgi-rpc-client` transport — the same
/// shape as that crate's `UnixTransport`, built from a stream rather than a
/// path.
struct LaunchedTransport {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
}

impl vgi_rpc_client::transport::Transport for LaunchedTransport {
    fn split(&mut self) -> (&mut dyn Read, &mut dyn Write) {
        (&mut self.reader, &mut self.writer)
    }

    fn close(&mut self) -> Result<()> {
        self.writer.flush()?;
        let _ = self.writer.shutdown(std::net::Shutdown::Write);
        Ok(())
    }
}

fn cached(hash: &str) -> Option<PathBuf> {
    RESOLVED.lock().ok()?.as_ref()?.get(hash).cloned()
}

fn remember(hash: &str, sock: &Path) {
    if let Ok(mut g) = RESOLVED.lock() {
        g.get_or_insert_with(BTreeMap::new)
            .insert(hash.to_string(), sock.to_path_buf());
    }
}

fn invalidate(hash: &str) {
    if let Ok(mut g) = RESOLVED.lock() {
        if let Some(m) = g.as_mut() {
            m.remove(hash);
        }
    }
}

/// An advisory `flock(2)` held for the life of the guard.
///
/// `flock` specifically, not `fcntl(F_SETLK)`: the two are different syscalls
/// that do **not** interlock, so using the wrong one would let this client and
/// the C++ launcher both believe they hold the lock.
struct FlockGuard {
    file: fs::File,
}

impl FlockGuard {
    fn acquire(path: &Path) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|e| RpcError::runtime_error(format!("open {}: {e}", path.display())))?;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if rc != 0 {
            return Err(RpcError::runtime_error(format!(
                "flock {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            )));
        }
        Ok(Self { file })
    }
}

impl Drop for FlockGuard {
    fn drop(&mut self) {
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    /// The golden vectors from `vgi/test/cpp/launcher_parity_vectors.hpp`,
    /// generated by `vgi-rpc/scripts/regenerate_launcher_parity_vectors.py`.
    ///
    /// These are the whole contract: a hash that does not match here shares no
    /// worker with the DuckDB extension or the Python launcher, and the failure
    /// mode is silent (a second worker starts and everything "works", slowly).
    #[test]
    fn hashes_match_the_python_golden_vectors() {
        type HashCase = (
            &'static str,
            Vec<String>,
            &'static str,
            BTreeMap<String, String>,
            &'static str,
        );
        let cases: &[HashCase] = &[
            (
                "empty_argv_empty_env",
                argv(&[]),
                "/tmp",
                env(&[]),
                "21499d847854c192",
            ),
            (
                "single_arg",
                argv(&["python"]),
                "/tmp",
                env(&[]),
                "13ddf92fa852a381",
            ),
            (
                "many_args",
                argv(&["python", "-m", "foo", "--bar", "baz"]),
                "/tmp",
                env(&[]),
                "1d95f2117bce8c2d",
            ),
            (
                "argv_with_spaces",
                argv(&["python", "/path with spaces/foo.py"]),
                "/tmp",
                env(&[]),
                "23664770f5414889",
            ),
            (
                "cwd_with_special_chars",
                argv(&["python"]),
                "/tmp/has spaces and \"quotes\"",
                env(&[]),
                "e87a8168b8665401",
            ),
            (
                "env_single",
                argv(&["python"]),
                "/tmp",
                env(&[("VGI_RPC_FOO", "bar")]),
                "70118f0ad5ea8bf3",
            ),
            (
                "env_multiple_sorted_by_python",
                argv(&["python"]),
                "/tmp",
                env(&[("VGI_RPC_Z", "z"), ("VGI_RPC_A", "a"), ("VGI_RPC_M", "m")]),
                "1000503273c593e4",
            ),
            (
                "env_with_quotes_and_backslash",
                argv(&["python"]),
                "/tmp",
                env(&[("VGI_RPC_FOO", "a\"b\\c")]),
                "f688dc41e1a4416d",
            ),
            (
                "env_value_with_spaces",
                argv(&["python"]),
                "/tmp",
                env(&[("VGI_RPC_FLAG", "value with spaces")]),
                "48522da323b1a55d",
            ),
            (
                "argv_with_quotes_and_backslash",
                argv(&["echo", "a\"b\\c"]),
                "/tmp",
                env(&[]),
                "cfcf140ab2f01b74",
            ),
            (
                "long_path",
                argv(&["/usr/local/bin/very/long/path/to/the/worker/executable"]),
                "/tmp",
                env(&[]),
                "b6f2736f279afd0b",
            ),
            (
                "deep_cwd",
                argv(&["python"]),
                "/var/folders/5z/abcdefghijklmnop/T/working/directory/deep/nesting",
                env(&[]),
                "a37badbdf41d0559",
            ),
            (
                "many_args_many_env",
                argv(&["java", "-jar", "/opt/foo.jar", "-Dlog.level=INFO"]),
                "/var/folders/work",
                env(&[
                    ("VGI_RPC_TOKEN", "secret"),
                    ("VGI_RPC_REGION", "us-west-2"),
                    ("VGI_RPC_BUCKET", "my-bucket"),
                ]),
                "8abb635d646af180",
            ),
        ];
        for (name, a, cwd, e, expected) in cases {
            assert_eq!(&compute_hash(a, cwd, e), expected, "vector {name}");
        }
    }

    #[test]
    fn canonical_json_escapes_match_python() {
        let mut s = String::new();
        push_json_string(&mut s, r#"a"b\c"#);
        assert_eq!(s, r#""a\"b\\c""#);

        let mut s = String::new();
        push_json_string(&mut s, "tab\there\nnl");
        assert_eq!(s, r#""tab\there\nnl""#);

        // Control characters below 0x20 that lack a short escape use
        // lowercase \u00xx — uppercase hex would change the hash.
        let mut s = String::new();
        push_json_string(&mut s, "\u{01}\u{1f}");
        assert_eq!(s, "\"\\u0001\\u001f\"");

        // The ones that do have a short escape must use it, not \u.
        let mut s = String::new();
        push_json_string(&mut s, "\u{08}\u{0c}\r");
        assert_eq!(s, "\"\\b\\f\\r\"");
    }

    #[test]
    fn the_hash_covers_every_tuple_component() {
        let base = compute_hash(&argv(&["w"]), "/tmp", &env(&[]));
        assert_ne!(base, compute_hash(&argv(&["w", "-x"]), "/tmp", &env(&[])));
        assert_ne!(base, compute_hash(&argv(&["w"]), "/other", &env(&[])));
        assert_ne!(
            base,
            compute_hash(&argv(&["w"]), "/tmp", &env(&[("VGI_RPC_A", "1")]))
        );
        assert_eq!(base.len(), 16, "16 hex chars, per the protocol");
    }

    #[test]
    fn env_order_does_not_change_the_hash() {
        // A BTreeMap sorts, which is what makes this true — the protocol
        // requires ASCII-lex key order.
        let a = env(&[("VGI_RPC_A", "a"), ("VGI_RPC_Z", "z")]);
        let b = env(&[("VGI_RPC_Z", "z"), ("VGI_RPC_A", "a")]);
        assert_eq!(
            compute_hash(&argv(&["w"]), "/tmp", &a),
            compute_hash(&argv(&["w"]), "/tmp", &b)
        );
    }

    #[test]
    fn idle_timeout_encodes_in_plain_decimal() {
        assert_eq!(encode_idle_timeout(Duration::ZERO), "0");
        assert_eq!(encode_idle_timeout(Duration::from_secs(300)), "300");
        assert_eq!(encode_idle_timeout(Duration::from_millis(1500)), "1.5");
        // Large values must not go scientific — that is why %g is banned.
        let big = encode_idle_timeout(Duration::from_secs(86_400_000));
        assert!(!big.contains('e'), "scientific notation on the wire: {big}");
        assert_eq!(big, "86400000");
    }

    #[test]
    fn a_probe_of_a_nonexistent_socket_is_false() {
        assert!(!probe(Path::new("/nonexistent/vgi-test.sock")));
    }
}

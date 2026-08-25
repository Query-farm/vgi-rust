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
use std::io::{BufRead, BufReader};
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;

use sha2::{Digest, Sha256};
use vgi_rpc::errors::{Result, RpcError};

/// The scheme prefix a POSIX worker prints to advertise its socket.
const DISCOVERY_PREFIX: &str = "UNIX:";

/// Cap on pre-discovery stdout noise before the worker is considered broken.
const MAX_PREAMBLE_BYTES: usize = 1024 * 1024;

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
}

impl Default for LaunchConfig {
    fn default() -> Self {
        Self {
            // Matches the extension's `launcher_idle_timeout` default.
            idle_timeout: Duration::from_secs(300),
            state_dir: None,
            spawn_timeout: Duration::from_secs(60),
            stderr_path: None,
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
    // A socket file that exists but refuses connect is stale; the protocol
    // requires unlinking it before the new worker binds.
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

/// Can we connect? The only reliable liveness test — a socket file may outlive
/// the worker that bound it.
fn probe(sock: &Path) -> bool {
    std::os::unix::net::UnixStream::connect(sock).is_ok()
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

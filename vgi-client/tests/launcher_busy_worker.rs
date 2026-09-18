// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! A full accept queue is a busy worker, not a dead one.
//!
//! Under a burst of connections a launched worker's accept queue fills. Linux
//! then fails a non-blocking `AF_UNIX` connect with `EAGAIN`; macOS answers
//! `ECONNREFUSED`, the same as no listener at all. A launcher that reads either
//! as "dead" unlinks the live worker's socket and spawns a duplicate — the
//! Python reference did, 64 workers for 2 commands at `-n 32`. These mirror
//! the reference tests (`vgi-rpc` `tests/test_launcher.py`, the extension's
//! `test/cpp/test_launcher_e2e.cpp`).
//!
//! The "worker" here is a real listener with backlog 0 that never accepts on
//! its own, its queue filled with non-blocking connects held open. Nothing is
//! simulated above the socket: the launcher sees exactly what the kernel says.
//! The tests that need to tell `EAGAIN` from `ECONNREFUSED` are Linux-only,
//! because only Linux can.

#![cfg(all(unix, feature = "launcher"))]

use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use vgi_client::launcher::{compute_hash, ensure_worker, hashed_env, LaunchConfig};

/// A private, short-pathed state directory (`AF_UNIX` paths are capped near
/// 104 bytes), removed on drop.
struct StateDir(PathBuf);

impl StateDir {
    fn new() -> Self {
        static N: AtomicUsize = AtomicUsize::new(0);
        let dir = PathBuf::from(format!(
            "/tmp/vgi-lb-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("state dir");
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).expect("chmod");
        Self(dir)
    }
}

impl Drop for StateDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A listener at `path` that never accepts on its own, with its accept queue
/// filled by non-blocking connects it holds open — a worker too busy to take
/// another connection. Full means the next connect failed: `EAGAIN` on Linux,
/// `ECONNREFUSED` on macOS.
struct FullListener {
    listener: UnixListener,
    _queued: Vec<UnixStream>,
}

impl FullListener {
    fn bind(path: &Path) -> Self {
        // Backlog 0, which `UnixListener::bind` (backlog 128) cannot express.
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        assert!(fd >= 0, "socket: {}", std::io::Error::last_os_error());
        let listener = unsafe { UnixListener::from_raw_fd(fd) };
        let (addr, len) = sockaddr(path);
        let rc = unsafe {
            libc::bind(
                fd,
                &addr as *const libc::sockaddr_un as *const libc::sockaddr,
                len,
            )
        };
        assert_eq!(rc, 0, "bind: {}", std::io::Error::last_os_error());
        assert_eq!(unsafe { libc::listen(fd, 0) }, 0, "listen");

        let mut queued = Vec::new();
        for _ in 0..256 {
            match connect_nonblocking(path) {
                Ok(stream) => queued.push(stream),
                Err(_) => {
                    return Self {
                        listener,
                        _queued: queued,
                    }
                }
            }
        }
        panic!("the accept queue never filled");
    }

    /// Accept (and drop) one queued connection after `delay`, freeing a slot.
    fn accept_one_after(&self, delay: Duration) -> std::thread::JoinHandle<()> {
        let listener = self.listener.try_clone().expect("clone listener");
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            let _ = listener.accept();
        })
    }
}

fn sockaddr(path: &Path) -> (libc::sockaddr_un, libc::socklen_t) {
    let bytes = path.as_os_str().as_bytes();
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    assert!(bytes.len() < addr.sun_path.len(), "socket path too long");
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (dst, src) in addr.sun_path.iter_mut().zip(bytes) {
        *dst = *src as libc::c_char;
    }
    let len = std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t;
    (addr, len)
}

fn connect_nonblocking(path: &Path) -> std::io::Result<UnixStream> {
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
    assert!(fd >= 0);
    let stream = unsafe { UnixStream::from_raw_fd(fd) };
    stream.set_nonblocking(true)?;
    let (addr, len) = sockaddr(path);
    let rc = unsafe {
        libc::connect(
            stream.as_raw_fd(),
            &addr as *const libc::sockaddr_un as *const libc::sockaddr,
            len,
        )
    };
    if rc == 0 {
        Ok(stream)
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// An argv whose worker would fail loudly ("exited before advertising a
/// socket") if the launcher spawned it — so a test that expects the existing
/// listener to be kept fails if the launcher replaced it instead. `tag` keeps
/// each test's hash, and so its slot in the per-process path cache, distinct.
fn never_spawn(tag: &str) -> Vec<String> {
    ["/bin/sh", "-c", "exit 3", tag]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// Where `ensure_worker` will look for `argv`'s worker in `dir`.
fn socket_for(argv: &[String], dir: &Path) -> PathBuf {
    let cwd = std::env::current_dir()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    dir.join(format!("{}.sock", compute_hash(argv, &cwd, &hashed_env())))
}

fn config(dir: &Path) -> LaunchConfig {
    LaunchConfig {
        state_dir: Some(dir.to_path_buf()),
        spawn_timeout: Duration::from_secs(5),
        ..LaunchConfig::default()
    }
}

fn inode(path: &Path) -> u64 {
    std::fs::symlink_metadata(path).expect("stat socket").ino()
}

/// Run `ensure_worker` with a deadline, so a launcher that blocks on a full
/// queue fails the test instead of hanging it. A blocking `connect(2)` on Linux
/// waits for a slot with no bound, which is what the probe used to do.
fn ensure_worker_within(
    argv: Vec<String>,
    config: LaunchConfig,
    deadline: Duration,
) -> vgi_client::Result<PathBuf> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(ensure_worker(&argv, &config));
    });
    rx.recv_timeout(deadline)
        .unwrap_or_else(|_| panic!("ensure_worker did not return within {deadline:?}"))
}

/// Linux only: only Linux tells a full accept queue (`EAGAIN`) from no
/// listener at all.
#[cfg(target_os = "linux")]
#[test]
fn ensure_worker_leaves_a_worker_with_a_full_accept_queue_alone() {
    let dir = StateDir::new();
    let argv = never_spawn("full-queue");
    let sock = socket_for(&argv, &dir.0);
    let _busy = FullListener::bind(&sock);
    let before = inode(&sock);

    let path = ensure_worker_within(argv, config(&dir.0), Duration::from_secs(10))
        .expect("a busy worker must be reused, not replaced");
    assert_eq!(path, sock);
    assert_eq!(
        inode(&sock),
        before,
        "the live worker's socket was unlinked and replaced"
    );
}

/// Linux only, for the same reason. A failure here would read as a dead
/// worker to anything that relaunches on a failed connect.
#[cfg(target_os = "linux")]
#[test]
fn connect_waits_out_a_full_accept_queue() {
    let dir = StateDir::new();
    let sock = dir.0.join("busy.sock");
    let busy = FullListener::bind(&sock);
    let drain = busy.accept_one_after(Duration::from_millis(100));
    let conn = vgi_client::launcher::connect(&sock, Duration::from_secs(5));
    drain.join().unwrap();
    conn.expect("connect should have waited for the slot the drain freed");
}

#[cfg(target_os = "linux")]
#[test]
fn connect_gives_up_on_an_accept_queue_that_stays_full() {
    let dir = StateDir::new();
    let sock = dir.0.join("busy.sock");
    let _busy = FullListener::bind(&sock);
    let started = Instant::now();
    let err = vgi_client::launcher::connect(&sock, Duration::from_millis(200))
        .expect_err("the queue never drains");
    assert!(
        err.message.contains("accept queue stayed full"),
        "unexpected error: {}",
        err.message
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "gave up late: {:?}",
        started.elapsed()
    );
}

/// However the platform reports a full queue — `EAGAIN`, or macOS's
/// `ECONNREFUSED`, which the probe re-tries before believing — a queue that
/// drains is a live worker.
#[test]
fn ensure_worker_counts_a_momentarily_full_accept_queue_as_alive() {
    let dir = StateDir::new();
    let argv = never_spawn("momentary");
    let sock = socket_for(&argv, &dir.0);
    let busy = FullListener::bind(&sock);
    let before = inode(&sock);
    let drain = busy.accept_one_after(Duration::from_millis(30));

    let path = ensure_worker_within(argv, config(&dir.0), Duration::from_secs(10))
        .expect("a momentarily busy worker must be reused, not replaced");
    drain.join().unwrap();
    assert_eq!(path, sock);
    assert_eq!(inode(&sock), before);
}

/// The other half, so the probe cannot pass the tests above by calling
/// everything alive: a socket nobody listens on still refuses after the
/// re-probes, and is unlinked and replaced.
#[test]
fn ensure_worker_replaces_a_socket_nobody_listens_on() {
    let dir = StateDir::new();
    // A stand-in worker that advertises the path it was handed and exits soon.
    // `$2` is the path: the launcher appends `--unix <path> --idle-timeout <s>`.
    let argv: Vec<String> = [
        "/bin/sh",
        "-c",
        "echo \"UNIX:$2\"; exec sleep 2",
        "stale-socket",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let sock = socket_for(&argv, &dir.0);
    // Bound and closed: the file outlives the listener, which is exactly what
    // a crashed worker leaves behind.
    drop(UnixListener::bind(&sock).expect("bind"));
    assert!(sock.exists());

    let started = Instant::now();
    let path = ensure_worker_within(argv, config(&dir.0), Duration::from_secs(10))
        .expect("a stale socket is replaced by a fresh worker");
    assert_eq!(path, sock);
    assert!(
        started.elapsed() >= Duration::from_millis(350),
        "a refusal was believed without the re-probes macOS needs: {:?}",
        started.elapsed()
    );
    // The stand-in never binds, so the stale file must simply be gone.
    assert!(
        !sock.exists(),
        "the stale socket was not unlinked before the spawn"
    );
}

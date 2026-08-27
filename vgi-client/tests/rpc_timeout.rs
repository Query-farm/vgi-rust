// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Transport deadlines against a peer that accepts a connection but never
//! answers. This is the failure mode query-engine cancellation needs bounded:
//! connection refusal is already immediate, while a silent peer otherwise
//! leaves attach, bind, planning, or a scan tick blocked indefinitely.

use std::net::TcpListener;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use vgi_client::{ConnectionOptions, VgiClient, VgiLocation, WorkerPool};

#[test]
fn connection_options_bound_a_stalled_rpc_read() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback");
    let address = listener.local_addr().unwrap();
    let (release_tx, release_rx) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().expect("accept client");
        // Two seconds is only a regression backstop. The configured client
        // deadline below should release the call much sooner.
        let _ = release_rx.recv_timeout(Duration::from_secs(2));
        drop(stream);
    });

    let location = VgiLocation::Tcp {
        host: address.ip().to_string(),
        port: address.port(),
    };
    let options = ConnectionOptions {
        rpc_timeout: Some(Duration::from_millis(75)),
        ..Default::default()
    };
    let mut client = VgiClient::connect_location_with_options(&location, &options)
        .expect("TCP connect succeeds");

    let started = Instant::now();
    let error = client.catalogs().expect_err("silent peer must time out");
    let elapsed = started.elapsed();
    let _ = release_tx.send(());
    server.join().unwrap();

    // The raw stream maps the OS deadline to IOError today, while wrapped
    // transports may normalize the same failure to TransportError. The
    // contract under test is the bounded read, not that adapter-level label.
    assert!(
        matches!(error.error_type.as_str(), "IOError" | "TransportError"),
        "unexpected timeout error: {error}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "75ms RPC deadline took {elapsed:?}"
    );
}

#[cfg(unix)]
#[test]
fn subprocess_timeout_is_honored_and_never_reenters_the_pool() {
    let entered = std::env::temp_dir().join(format!(
        "vgi-client-subprocess-timeout-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let location = VgiLocation::Subprocess(vec![
        "sh".into(),
        "-c".into(),
        "dd bs=1 count=1 of=/dev/null 2>/dev/null; : > \"$0\"; sleep 60".into(),
        entered.display().to_string(),
    ]);
    let options = ConnectionOptions {
        rpc_timeout: Some(Duration::from_millis(200)),
        ..Default::default()
    };
    let pool = WorkerPool::default();
    let mut client = pool
        .acquire_with_options(&location, options)
        .expect("subprocess starts");
    let started = Instant::now();
    let error = client.catalogs().expect_err("silent child must time out");
    assert!(error.to_string().contains("deadline elapsed"), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2));
    drop(client);

    let stats = pool.stats();
    assert_eq!(stats.discarded_poisoned, 1);
    assert_eq!(stats.idle, 0);
    assert!(
        entered.exists(),
        "the worker consumed the request before stalling"
    );
    let _ = std::fs::remove_file(entered);
}

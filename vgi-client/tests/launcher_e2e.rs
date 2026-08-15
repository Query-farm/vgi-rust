// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The `launch:` transport against a real worker.
//!
//! The unit tests pin the hash against the golden vectors; these prove the rest
//! of the contract actually works — that the worker accepts `--unix` /
//! `--idle-timeout`, prints the discovery line where we look for it, and that a
//! second client joins the *same* process instead of starting another.
//!
//! Set `VGI_TEST_WORKER` to a worker command to run these; they skip otherwise,
//! matching the `require-env` convention the DuckDB extension's suite uses.

#![cfg(all(unix, feature = "launcher"))]

use std::time::Instant;

use vgi_client::{AttachOptions, VgiClient, VgiLocation};

fn worker_cmd() -> Option<String> {
    match std::env::var("VGI_TEST_WORKER") {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => {
            eprintln!("skipping: set VGI_TEST_WORKER to run the launcher e2e tests");
            None
        }
    }
}

/// Attach and detach once, returning how long it took.
fn round_trip(location: &VgiLocation) -> std::time::Duration {
    let start = Instant::now();
    let mut client = VgiClient::connect_location(location).expect("connect");
    let cat = client
        .attach("example", AttachOptions::default())
        .expect("attach");
    client.schemas(&cat).expect("schemas");
    client.detach(&cat).expect("detach");
    start.elapsed()
}

#[test]
fn launch_spawns_a_worker_and_a_second_client_reuses_it() {
    let Some(cmd) = worker_cmd() else { return };
    let location = VgiLocation::parse(&format!("launch:{cmd}")).expect("parse");
    assert!(matches!(location, VgiLocation::Launch(_)));

    // First client pays for the spawn; the socket is now live.
    let cold = round_trip(&location);

    // Second client must find the existing listener. If the hash, the state
    // dir, or the probe were wrong this would spawn a second interpreter and
    // take about as long as the first.
    let warm = round_trip(&location);

    eprintln!("launcher: cold={cold:?} warm={warm:?}");
    assert!(
        warm < cold,
        "second connect ({warm:?}) was not faster than the spawn ({cold:?}) — \
         it probably started its own worker instead of joining the warm one"
    );
}

#[test]
fn a_launch_location_and_a_subprocess_location_do_not_collide() {
    let Some(cmd) = worker_cmd() else { return };
    // Same command, different transport: these must stay distinct so a pool
    // keyed on location never hands a launcher socket to a subprocess caller.
    assert_ne!(
        VgiLocation::parse(&format!("launch:{cmd}")).unwrap(),
        VgiLocation::parse(&cmd).unwrap()
    );
}

#[test]
fn the_pool_reuses_launcher_connections() {
    let Some(cmd) = worker_cmd() else { return };
    let location = VgiLocation::parse(&format!("launch:{cmd}")).expect("parse");
    let pool = vgi_client::WorkerPool::default();

    {
        let mut c = pool.acquire(&location).expect("acquire");
        let cat = c
            .attach("example", AttachOptions::default())
            .expect("attach");
        c.detach(&cat).expect("detach");
    }
    assert_eq!(pool.stats().idle, 1, "connection returned to the pool");

    {
        let mut c = pool.acquire(&location).expect("acquire");
        let cat = c
            .attach("example", AttachOptions::default())
            .expect("attach");
        c.detach(&cat).expect("detach");
    }
    let s = pool.stats();
    assert_eq!((s.hits, s.misses), (1, 1), "second acquire was a pool hit");
}

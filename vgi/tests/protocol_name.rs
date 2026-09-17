// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The VGI wire name is `vgi.v2`, and a built worker really hosts it.
//!
//! The name had never been decided, so each implementation inherited whatever
//! its own protocol type happened to be called — Python `VgiProtocol`, Java and
//! C# `VgiService`, Go `Service`, TypeScript `vgi`, and this port `VgiProtocol`.
//! Since vgi-rpc 0.25.0 every request carries a `vgi_rpc.protocol` routing key,
//! so those four answers meant no client could address all six servers. `vgi.v2`
//! is the canonical name; the major is in it so an incompatible major is a
//! different name (an honest 404) and `vgi.v2` can be served beside a future
//! `vgi.v3`.
//!
//! Two assertions, because the constant being right and the *server* announcing
//! it are different facts: the routing key is bound from the constant in three
//! client call sites, but the server side flows through
//! `RpcServer::builder().protocol_name(..)`, and a rename that missed that line
//! would leave a worker answering on a name no client asks for.

use vgi::worker::Worker;
use vgi::VGI_PROTOCOL_NAME;

#[test]
fn wire_name_is_the_canonical_one() {
    assert_eq!(
        VGI_PROTOCOL_NAME, "vgi.v2",
        "the VGI wire name is shared across every implementation; changing it \
         here strands this port's clients and servers from the rest",
    );
}

#[test]
fn a_built_worker_hosts_the_canonical_name() {
    let server = Worker::new().build_server();
    let hosted = server.hosted_protocol_names();
    // Mirrors the text a client sees in vgi-rpc's "not specified" / "not
    // supported" diagnostics, which is where a rename that missed the server
    // builder would first be noticed.
    println!("This server hosts: {hosted:?}");
    assert_eq!(
        hosted.first().copied(),
        Some("vgi.v2"),
        "the application protocol must be announced first and by its wire name",
    );
    assert!(
        hosted.contains(&"vgi_rpc.Reflection.v1"),
        "reflection is co-hosted unconditionally since vgi-rpc 0.25.0",
    );
}

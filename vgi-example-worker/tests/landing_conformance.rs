// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Boots the example worker over HTTP and asserts the standardized VGI
//! landing surface conforms to the contract in
//! `~/Development/vgi/docs/http-landing-contract.md`:
//!
//! * `GET /` serves the pinned shared `landing.html` (asset marker) for
//!   browsers, and a JSON status for `?format=json`.
//! * `GET /describe.json` is well-formed (versioned; `lang: "rust"`; catalogs
//!   with tags / counts / schemas / functions).
//! * `GET /describe/{catalog}/{schema}/{table}.json` returns valid columns.
//!
//! This is the dependency-free Rust guard; the full JSON-schema + Python
//! golden validation runs in CI via `run_landing_conformance.py`. Uses a
//! minimal blocking HTTP/1.1 client over `std::net::TcpStream` so it pulls in
//! no new crates.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::Value;

/// A booted worker process bound to an ephemeral loopback port.
struct Worker {
    child: Child,
    port: u16,
}

impl Drop for Worker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Worker {
    /// Spawn the example worker with `--http`, reading the announced port
    /// from its `PORT:<n>` stdout line.
    fn boot() -> Worker {
        let mut child = Command::new(env!("CARGO_BIN_EXE_vgi-example-worker"))
            .arg("--http")
            .env("VGI_HTTP_BIND", "127.0.0.1:0")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn example worker");

        let stdout = child.stdout.take().expect("worker stdout");
        let mut reader = BufReader::new(stdout);
        let mut port = None;
        let mut line = String::new();
        for _ in 0..50 {
            line.clear();
            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            if let Some(rest) = line.trim().strip_prefix("PORT:") {
                port = rest.parse::<u16>().ok();
                break;
            }
        }
        let port = port.expect("worker did not announce a PORT");
        Worker { child, port }
    }

    /// Minimal blocking HTTP/1.1 GET. Returns `(status_code, body)`.
    fn get(&self, path: &str, accept: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(("127.0.0.1", self.port)).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let req = format!(
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nAccept: {accept}\r\nConnection: close\r\n\r\n"
        );
        stream.write_all(req.as_bytes()).expect("write request");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("read response");
        let text = String::from_utf8_lossy(&raw).into_owned();
        let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
        let status = head
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|s| s.parse::<u16>().ok())
            .expect("status line");
        (status, body.to_string())
    }
}

#[test]
fn landing_surface_conforms() {
    let worker = Worker::boot();

    // 1) GET / for a browser → the pinned shared landing.html.
    let (status, body) = worker.get("/", "text/html");
    assert_eq!(status, 200, "GET / (text/html)");
    assert!(
        body.contains("vgi-landing-asset v"),
        "GET / must serve the pinned landing.html (asset marker missing)"
    );

    // 2) GET /?format=json → JSON status object.
    let (status, body) = worker.get("/?format=json", "application/json");
    assert_eq!(status, 200, "GET /?format=json");
    let v: Value = serde_json::from_str(&body).expect("json status");
    assert_eq!(v["status"], "ok");
    assert!(v.get("server_id").is_some());

    // 3) GET /describe.json → the versioned contract document.
    let (status, body) = worker.get("/describe.json", "application/json");
    assert_eq!(status, 200, "GET /describe.json");
    let doc: Value = serde_json::from_str(&body).expect("describe.json");
    assert_eq!(doc["landing_schema_version"], 1);
    assert_eq!(doc["worker"]["lang"], "rust");
    assert!(doc["worker"]["name"].is_string());
    assert!(doc["cupola_base"].is_string());
    assert!(doc["oauth"].is_boolean());
    let catalogs = doc["catalogs"].as_array().expect("catalogs array");
    assert!(!catalogs.is_empty(), "at least one catalog");

    // Sample one table + one view per schema and validate the lazy columns
    // endpoint, mirroring run_landing_conformance.py.
    let mut checked_any = false;
    for cat in catalogs {
        let cat_name = cat["name"].as_str().unwrap();
        for sch in cat["schemas"].as_array().unwrap() {
            let sch_name = sch["name"].as_str().unwrap();
            let tables = sch["tables"].as_array().unwrap();
            let views = sch["views"].as_array().unwrap();
            for obj in tables.iter().take(1).chain(views.iter().take(1)) {
                let obj_name = obj["name"].as_str().unwrap();
                let (cstatus, cbody) = worker.get(
                    &format!("/describe/{cat_name}/{sch_name}/{obj_name}.json"),
                    "application/json",
                );
                assert_eq!(
                    cstatus, 200,
                    "columns {cat_name}/{sch_name}/{obj_name} status"
                );
                let cols: Value = serde_json::from_str(&cbody).expect("columns json");
                let arr = cols["columns"].as_array().expect("columns array");
                for c in arr {
                    assert!(c["name"].is_string(), "column name");
                    assert!(c["type"].is_string(), "column type");
                }
                checked_any = true;
            }
        }
    }
    assert!(checked_any, "sampled at least one table/view");

    // A missing object yields 404.
    let (status, _) = worker.get(
        "/describe/example/main/does_not_exist.json",
        "application/json",
    );
    assert_eq!(status, 404, "unknown object → 404");
}

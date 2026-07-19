// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! `vgi-storage-server` — a durable storage service for VGI workers.
//!
//! Stateless workers (e.g. multiple fly.io instances) point their `http`
//! [`FunctionStorage`](vgi::storage::FunctionStorage) backend at this service so
//! they share one durable, SQLite-backed store. It speaks the bincode protocol
//! defined in `vgi::storage::http` over `POST /rpc`.
//!
//! Config (env):
//! - `VGI_STORAGE_BIND`  — listen addr (default `127.0.0.1:8080`).
//! - `VGI_STORAGE_DB`    — SQLite file (default `$TMPDIR/vgi-storage/state.db`).
//! - `VGI_STORAGE_TOKEN` — bearer token; when set, `/rpc` requires it.
//!
//! This service is the *single* writer to its database, so SQLite sees no
//! cross-process write contention. Run one instance with a persistent volume
//! (see the fly.io deploy docs); horizontal HA via LiteFS or shard routing is a
//! later addition.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;

use vgi::storage::http::{apply_op, StorageRequest, WIRE};
use vgi::storage::{FunctionStorage, SqliteStorage};

/// Cap on the idempotency cache; cleared wholesale when exceeded (an idempotency
/// key is only useful for the brief window of a client's retry burst).
const IDEM_CAP: usize = 100_000;

struct AppState {
    store: SqliteStorage,
    token: Option<String>,
    /// idempotency_key -> bincode-serialized reply, for replay-safe retries of
    /// non-idempotent ops. In-memory: sufficient for a single instance; a
    /// durable/shared cache is a follow-up for the multi-instance topology.
    idem: Mutex<HashMap<String, Vec<u8>>>,
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let bind = std::env::var("VGI_STORAGE_BIND").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let db_path = std::env::var("VGI_STORAGE_DB")
        .map(Into::into)
        .unwrap_or_else(|_| {
            let mut p = std::env::temp_dir();
            p.push("vgi-storage");
            let _ = std::fs::create_dir_all(&p);
            p.push("state.db");
            p
        });
    let token = std::env::var("VGI_STORAGE_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());

    log::info!(
        "vgi-storage-server: db={}, auth={}",
        db_path.display(),
        token.is_some()
    );

    let state = Arc::new(AppState {
        store: SqliteStorage::open(db_path),
        token,
        idem: Mutex::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/rpc", post(rpc))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .unwrap_or_else(|e| panic!("bind {bind}: {e}"));
    log::info!("vgi-storage-server listening on {bind}");
    // Announce the bound port so a supervisor / test harness can read it.
    println!(
        "PORT:{}",
        listener.local_addr().map(|a| a.port()).unwrap_or(0)
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("serve");
}

fn authorized(state: &AppState, headers: &HeaderMap) -> bool {
    match &state.token {
        None => true,
        Some(expected) => headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|t| t == expected)
            .unwrap_or(false),
    }
}

async fn rpc(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    if !authorized(&state, &headers) {
        return (StatusCode::UNAUTHORIZED, "missing or invalid bearer token").into_response();
    }
    let req: StorageRequest = match bincode::serde::decode_from_slice(&body, WIRE) {
        Ok((r, _)) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("decode request: {e}")).into_response(),
    };

    // Idempotency replay: a non-idempotent op carries a key; if we've already
    // applied it, return the original reply instead of applying again.
    if let Some(key) = &req.idempotency_key {
        if let Some(cached) = state.idem.lock().unwrap().get(key).cloned() {
            return ok_bytes(cached);
        }
    }

    let reply = apply_op(&state.store as &dyn FunctionStorage, req.op);
    let bytes = match bincode::serde::encode_to_vec(&reply, WIRE) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("encode reply: {e}"),
            )
                .into_response()
        }
    };

    if let Some(key) = req.idempotency_key {
        let mut idem = state.idem.lock().unwrap();
        if idem.len() >= IDEM_CAP {
            idem.clear();
        }
        idem.insert(key, bytes.clone());
    }
    ok_bytes(bytes)
}

fn ok_bytes(bytes: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [("content-type", "application/octet-stream")],
        bytes,
    )
        .into_response()
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The HTTP worker's blob-vendor wiring (feature `blob-vendor`).
//!
//! With `VGI_BLOB_VENDOR_URL` (and `VGI_BLOB_VENDOR_TOKEN`, falling back to
//! `VGI_CF_DO_TOKEN`) set, `--http` serving:
//!
//! - externalizes result batches of at least
//!   `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES` (default 1 MiB) to the vendor,
//!   replacing them with pointers that carry `vgi_rpc.location.sha256`, and
//!   compresses them per `VGI_RPC_EXTERNAL_COMPRESSION`;
//! - vends client upload URLs (`__upload_url__/init`) from the vendor and
//!   accepts requests sent as upload pointers, fetching only from the vendor's
//!   storage hosts;
//! - advertises `VGI_RPC_MAX_UPLOAD_BYTES`.
//!
//! `VGI_RPC_MAX_REQUEST_BYTES` is honoured on every HTTP worker (see
//! `transport::serve_http`). Function-storage externalization is separate:
//! see [`crate::storage::blobs`].

use std::sync::{Arc, OnceLock};

use vgi_rpc::external::ExternalLocationConfig;
use vgi_rpc_s3::vendor::VendorStorage;

type Vendor = Option<(ExternalLocationConfig, Arc<VendorStorage>)>;

/// The process's vendor config, built once from the environment so the RPC
/// server (results) and the HTTP layer (upload URLs) share one client. Panics
/// on a misconfiguration: a worker must not start with a vendor it cannot use.
pub(crate) fn http_vendor() -> &'static Vendor {
    static VENDOR: OnceLock<Vendor> = OnceLock::new();
    VENDOR.get_or_init(|| {
        vgi_rpc_s3::vendor::external_config_from_env().unwrap_or_else(|e| {
            panic!("blob vendor configuration: {}", e.message);
        })
    })
}

/// `VGI_RPC_MAX_UPLOAD_BYTES`, when set. Panics on a malformed value.
pub(crate) fn max_upload_bytes() -> Option<usize> {
    vgi_rpc_s3::vendor::max_upload_bytes_from_env()
        .unwrap_or_else(|e| panic!("{}", e.message))
        .map(|n| usize::try_from(n).unwrap_or(usize::MAX))
}

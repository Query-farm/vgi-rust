// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! The HTTP worker's external-storage wiring (feature `blob-vendor`; direct
//! S3 also needs `storage-s3`).
//!
//! The backend comes from the environment (direct-storage contract v1):
//! `VGI_STORAGE_BACKEND` = `vendor` | `s3` | `gcs` | `none`, or, unset, the
//! blob vendor when `VGI_BLOB_VENDOR_URL` is set, the operator's S3 bucket when
//! `VGI_STORAGE_S3_BUCKET` is set, else none (more than one set is refused).
//! See `vgi_rpc_s3::backend` and `vgi_rpc_s3::s3` for the variables. With a
//! backend, `--http` serving:
//!
//! - externalizes result batches of at least
//!   `VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES` (default 1 MiB), replacing them with
//!   pointers that carry `vgi_rpc.location.sha256`, and compresses them per
//!   `VGI_RPC_EXTERNAL_COMPRESSION`;
//! - vends client upload URLs (`__upload_url__/init`, `VGI-Upload-URL-Support:
//!   2`), each signed for the client's declared size and SHA-256 so the storage
//!   refuses any other body, accepts requests sent as upload pointers (fetched
//!   only from the backend's storage hosts), and advertises argument
//!   references and `VGI_RPC_MAX_UPLOAD_BYTES` -- except with
//!   `VGI_STORAGE_S3_CHECKSUM=none`, which issues no upload grants (large
//!   inputs stay inline) but still externalizes results and stores
//!   `StateRef` objects.
//!
//! `VGI_RPC_MAX_REQUEST_BYTES` is honoured on every HTTP worker (see
//! `transport::serve_http`). Function-storage externalization is separate:
//! see [`crate::storage::blobs`].

use std::sync::{Arc, OnceLock};

use vgi_rpc::external::{ExternalLocationConfig, UploadUrlProvider};
use vgi_rpc_s3::backend::{Backend, StorageWiring};

/// The pieces of the selected backend the worker uses.
pub(crate) struct HttpStorage {
    pub(crate) config: ExternalLocationConfig,
    pub(crate) upload_url_provider: Option<Arc<dyn UploadUrlProvider>>,
}

/// The process's storage backend, built once from the environment so the RPC
/// server (results) and the HTTP layer (upload URLs) share one client. Panics
/// on a misconfiguration: a worker must not start with storage it cannot use.
pub(crate) fn http_storage() -> &'static Option<HttpStorage> {
    static STORAGE: OnceLock<Option<HttpStorage>> = OnceLock::new();
    STORAGE.get_or_init(|| {
        let wiring = vgi_rpc_s3::backend::storage_from_env().unwrap_or_else(|e| {
            panic!("storage configuration: {}", e.message);
        });
        wiring.map(
            |StorageWiring {
                 backend,
                 config,
                 upload_url_provider,
                 ..
             }| {
                if backend != Backend::Vendor && crate::hosted::hosted_mode_enabled() {
                    log::warn!(
                    "Orchard hosted mode with VGI_STORAGE_BACKEND={}: externalized values go to \
                     the operator's bucket, not the Orchard blob vendor",
                    backend.as_str()
                );
                }
                log::info!("storage backend: {}", backend.as_str());
                HttpStorage {
                    config,
                    upload_url_provider,
                }
            },
        )
    })
}

/// `VGI_RPC_MAX_UPLOAD_BYTES`, when set. Panics on a malformed value.
pub(crate) fn max_upload_bytes() -> Option<usize> {
    vgi_rpc_s3::vendor::max_upload_bytes_from_env()
        .unwrap_or_else(|e| panic!("{}", e.message))
        .map(|n| usize::try_from(n).unwrap_or(usize::MAX))
}

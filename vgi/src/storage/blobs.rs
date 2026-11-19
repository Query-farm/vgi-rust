// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Value envelope and blob-vendor externalization for any [`FunctionStorage`].
//!
//! [`ExternalizingStorage`] wraps a backend and keeps values too large for it
//! in the *blob vendor* (`VGI_BLOB_VENDOR_URL`), storing only a small pointer
//! in the backend. The [`FunctionStorage`] API does not change. Port of
//! vgi-python's `vgi.function_storage_blobs`; the stored bytes are identical
//! (`docs/testing/blob-test-catalog.md` in the vgi extension is the contract,
//! `test/vectors/storage_envelope_v1.json` its vectors).
//!
//! # Envelope
//!
//! Every value the wrapper writes (`kv_*` values, log items, queue items)
//! carries a one-byte tag:
//!
//! - `0x00 || raw`: the value, inline;
//! - `0x01 || u64be(size) || sha256(32) || object_id`: a pointer to a blob in
//!   the vendor (about 73 bytes).
//!
//! With a vendor, a value of `threshold` bytes or more is uploaded *before*
//! the backend call; reads fetch it back and verify its size and SHA-256
//! against the pointer. Without one every value is inline, and when the
//! backend has a per-value cap (the Cloudflare DO's 1 MiB) a value whose
//! envelope exceeds it is refused before it is sent, naming
//! `VGI_BLOB_VENDOR_URL`.
//!
//! Decoding is strict: an empty value, an unknown tag (a raw value written
//! without the wrapper), a short pointer or an object id outside
//! `[A-Za-z0-9._:-]{1,128}` is an error, as is a pointer read with no vendor
//! configured. User bytes that start with `0x01` are safe because every value
//! is enveloped.
//!
//! # Where it applies ([`wrap_storage_from_env`])
//!
//! - `cloudflare-do`: always wrapped, vendor or not, so the stored format
//!   never depends on a per-process variable.
//! - Any other backend: only with `VGI_STORAGE_EXTERNALIZE=1`, which then
//!   requires the vendor. SQLite's on-disk format is unchanged by default.
//!
//! # Errors
//!
//! The [`FunctionStorage`] methods are infallible, so a failure panics with an
//! [`RpcError`] payload (`std::panic::panic_any`), which vgi-rpc's handler
//! isolation returns to the client as that error. A blob that cannot be read
//! fails the query with the SDK-neutral messages:
//!
//! - `storage blob <id> failed its integrity check: sha256 mismatch` (or
//!   `size mismatch ...`);
//! - `storage blob <id> is missing: not found in the blob vendor (expired or
//!   deleted?)`;
//! - `storage blob <id> could not be read: <detail>`.
//!
//! # Vendor scopes and clean-up
//!
//! Blobs are uploaded under the vendor scope `(shard, "q.<hex scope>")` for
//! queue items and `(shard, "s.<hex scope>")` for key/value and log values,
//! where the shard is [`shard_key_for_scope`] of
//! the scope (the Rust storage trait carries no attach context; see
//! [`cfdo`](super::cfdo)). Each upload carries the idempotency key
//! `<per-call uuid hex>.<index>`. [`clear`](FunctionStorage::clear) deletes
//! both scopes and [`queue_pop`](FunctionStorage::queue_pop) the blob it
//! claimed, best effort and on a background thread; `kv_del` leaves blobs
//! until the scope is cleared. The vendor's idle sweep and the bucket
//! lifecycle rules are the backstop.

use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use sha2::{Digest, Sha256};
use vgi_rpc::RpcError;

use super::{hex, shard_key_for_scope, FunctionStorage};

/// Values of at least this many bytes go to the blob vendor by default.
pub const DEFAULT_EXTERNALIZE_BYTES: usize = 512 * 1024;
/// The Cloudflare Durable Object backend's per-value cap.
pub const CF_DO_MAX_VALUE_BYTES: usize = 1024 * 1024;

/// Blob vendor base URL.
pub const ENV_VENDOR_URL: &str = "VGI_BLOB_VENDOR_URL";
/// `1` wraps any backend.
pub const ENV_EXTERNALIZE: &str = "VGI_STORAGE_EXTERNALIZE";
/// The externalization threshold.
pub const ENV_EXTERNALIZE_BYTES: &str = "VGI_STORAGE_EXTERNALIZE_BYTES";
/// Alias of [`ENV_EXTERNALIZE_BYTES`].
pub const ENV_EXTERNALIZE_BYTES_ALIAS: &str = "VGI_CF_DO_EXTERNALIZE_BYTES";

/// Tag of an inline value.
pub const TAG_INLINE: u8 = 0x00;
/// Tag of a blob pointer.
pub const TAG_POINTER: u8 = 0x01;
/// Bytes before a pointer's object id: tag, size, SHA-256.
pub const POINTER_FIXED: usize = 1 + 8 + 32;
/// Concurrent vendor transfers per storage call.
const TRANSFER_PARALLELISM: usize = 8;

// ---------------------------------------------------------------------------
// The blob store the wrapper talks to
// ---------------------------------------------------------------------------

/// A reference to one stored blob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobRef {
    /// The vendor's object id.
    pub object_id: String,
    /// Exact byte length.
    pub size: u64,
    /// Lowercase hex SHA-256 of the bytes.
    pub sha256: String,
}

/// Why a blob operation failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobErrorKind {
    /// The downloaded bytes' SHA-256 differs from the reference.
    Sha256Mismatch,
    /// The downloaded bytes' size differs from the reference.
    SizeMismatch,
    /// The blob is gone (expired, deleted, never uploaded).
    Missing,
    /// Anything else.
    Other,
}

/// A failed blob operation. The message carries no credential or signed URL.
#[derive(Clone, Debug)]
pub struct BlobError {
    /// What went wrong.
    pub kind: BlobErrorKind,
    /// Diagnostic text.
    pub message: String,
}

/// The blob service [`ExternalizingStorage`] keeps large values in: the
/// vendor client in production (`vgi_rpc_s3::vendor::VendorBlobClient`, with
/// the `blob-vendor` feature), anything else in tests.
pub trait BlobStore: Send + Sync {
    /// Store `data` under `(shard, execution_id)` and return its reference. A
    /// retry with the same `idempotency_key` and bytes reuses the object.
    fn put_bytes(
        &self,
        data: &[u8],
        shard: &str,
        execution_id: &str,
        idempotency_key: &str,
    ) -> Result<BlobRef, BlobError>;
    /// Fetch the blob and verify it against the reference's size and SHA-256.
    fn get_bytes(&self, blob: &BlobRef) -> Result<Vec<u8>, BlobError>;
    /// Delete blobs by object id (missing ones are ignored).
    fn delete(&self, object_ids: &[String]) -> Result<(), BlobError>;
    /// Delete every blob stored under `(shard, execution_id)`.
    fn delete_scope(&self, shard: &str, execution_id: &str) -> Result<(), BlobError>;
}

#[cfg(feature = "blob-vendor")]
impl BlobStore for vgi_rpc_s3::vendor::VendorBlobClient {
    fn put_bytes(
        &self,
        data: &[u8],
        shard: &str,
        execution_id: &str,
        idempotency_key: &str,
    ) -> Result<BlobRef, BlobError> {
        let scope = vgi_rpc_s3::vendor::BlobScope {
            shard: shard.to_string(),
            execution_id: execution_id.to_string(),
        };
        let r = vgi_rpc_s3::vendor::VendorBlobClient::put_bytes(
            self,
            data,
            Some(&scope),
            Some(idempotency_key),
        )
        .map_err(vendor_error)?;
        Ok(BlobRef {
            object_id: r.object_id,
            size: r.size,
            sha256: r.sha256,
        })
    }

    fn get_bytes(&self, blob: &BlobRef) -> Result<Vec<u8>, BlobError> {
        vgi_rpc_s3::vendor::VendorBlobClient::get_bytes(
            self,
            &vgi_rpc_s3::vendor::BlobRef {
                object_id: blob.object_id.clone(),
                size: blob.size,
                sha256: blob.sha256.clone(),
            },
        )
        .map_err(vendor_error)
    }

    fn delete(&self, object_ids: &[String]) -> Result<(), BlobError> {
        vgi_rpc_s3::vendor::VendorBlobClient::delete(self, object_ids)
            .map(|_| ())
            .map_err(vendor_error)
    }

    fn delete_scope(&self, shard: &str, execution_id: &str) -> Result<(), BlobError> {
        vgi_rpc_s3::vendor::VendorBlobClient::delete_scope(self, shard, execution_id)
            .map(|_| ())
            .map_err(vendor_error)
    }
}

#[cfg(feature = "blob-vendor")]
fn vendor_error(e: vgi_rpc_s3::vendor::VendorError) -> BlobError {
    let kind = match e.code.as_deref() {
        Some("sha256_mismatch") => BlobErrorKind::Sha256Mismatch,
        Some("size_mismatch") => BlobErrorKind::SizeMismatch,
        _ if e.is_missing() => BlobErrorKind::Missing,
        _ => BlobErrorKind::Other,
    };
    BlobError {
        kind,
        message: e.message,
    }
}

/// The SDK-neutral error for a blob that could not be read.
fn blob_read_error(blob: &BlobRef, e: &BlobError) -> RpcError {
    let id = &blob.object_id;
    RpcError::runtime_error(match e.kind {
        BlobErrorKind::Sha256Mismatch => {
            format!("storage blob {id} failed its integrity check: sha256 mismatch")
        }
        BlobErrorKind::SizeMismatch => format!(
            "storage blob {id} failed its integrity check: size mismatch (expected {} bytes)",
            blob.size
        ),
        BlobErrorKind::Missing => format!(
            "storage blob {id} is missing: not found in the blob vendor (expired or deleted?)"
        ),
        BlobErrorKind::Other => format!("storage blob {id} could not be read: {}", e.message),
    })
}

/// Fail the storage call with `err` as the client-visible error. Logged
/// first: the panic hook prints an `RpcError` payload only as `Box<dyn Any>`.
fn fail(err: RpcError) -> ! {
    log::error!("vgi storage: {}", err.message);
    std::panic::panic_any(err)
}

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

/// Why a stored value is not a valid envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvelopeError {
    /// An empty stored value.
    Empty,
    /// A tag other than `0x00` / `0x01`.
    UnknownTag(u8),
    /// A truncated pointer or one whose object id is malformed.
    MalformedPointer(usize),
}

impl std::fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnvelopeError::Empty => f.write_str(
                "empty stored value: not a value envelope (written without the envelope?)",
            ),
            EnvelopeError::UnknownTag(t) => write!(
                f,
                "stored value has unknown envelope tag 0x{t:02x} (written by an older client or \
                 without the envelope? mixing client versions within one query is not supported)"
            ),
            EnvelopeError::MalformedPointer(n) => {
                write!(f, "malformed blob pointer in stored value ({n} bytes)")
            }
        }
    }
}

/// A decoded envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Envelope<'a> {
    /// The value itself.
    Inline(&'a [u8]),
    /// A blob in the vendor.
    Pointer(BlobRef),
}

fn object_id_ok(id: &[u8]) -> bool {
    (1..=128).contains(&id.len())
        && id
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-'))
}

/// The inline envelope of `value`.
pub fn encode_inline(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 1);
    out.push(TAG_INLINE);
    out.extend_from_slice(value);
    out
}

/// The pointer envelope of a vendored blob, or `None` when its object id is
/// outside the pointer charset or its digest is not 32 bytes of hex.
pub fn encode_pointer(blob: &BlobRef) -> Option<Vec<u8>> {
    let id = blob.object_id.as_bytes();
    let digest = decode_hex(&blob.sha256)?;
    if !object_id_ok(id) || digest.len() != 32 {
        return None;
    }
    let mut out = Vec::with_capacity(POINTER_FIXED + id.len());
    out.push(TAG_POINTER);
    out.extend_from_slice(&blob.size.to_be_bytes());
    out.extend_from_slice(&digest);
    out.extend_from_slice(id);
    Some(out)
}

/// Decode a stored value.
///
/// # Errors
///
/// An empty value, an unknown tag, or a malformed pointer.
pub fn decode(envelope: &[u8]) -> Result<Envelope<'_>, EnvelopeError> {
    match envelope.first() {
        None => Err(EnvelopeError::Empty),
        Some(&TAG_INLINE) => Ok(Envelope::Inline(&envelope[1..])),
        Some(&TAG_POINTER) => {
            let id = envelope.get(POINTER_FIXED..).unwrap_or_default();
            if envelope.len() <= POINTER_FIXED || !object_id_ok(id) {
                return Err(EnvelopeError::MalformedPointer(envelope.len()));
            }
            let size = u64::from_be_bytes(envelope[1..9].try_into().expect("8 bytes"));
            Ok(Envelope::Pointer(BlobRef {
                object_id: String::from_utf8(id.to_vec()).expect("ASCII object id"),
                size,
                sha256: hex(&envelope[9..POINTER_FIXED]),
            }))
        }
        Some(&t) => Err(EnvelopeError::UnknownTag(t)),
    }
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// The vendor `scope.execution_id` for a queue (`q`) or key/value and log
/// (`s`) scope: `<kind>.<hex ident>`, or `<kind>.h<hex sha256(ident)>` when
/// that is longer than 128 characters.
pub fn blob_execution_id(kind: &str, ident: &[u8]) -> String {
    let text = format!("{kind}.{}", hex(ident));
    if text.len() <= 128 {
        text
    } else {
        format!("{kind}.h{}", hex(&Sha256::digest(ident)))
    }
}

/// The vendor `scope.shard` for a storage shard key: the key itself when it
/// matches `[A-Za-z0-9._-]{1,128}`, `loc-anon` when empty, else
/// `h-<hex sha256(key)>`.
pub fn blob_shard(shard_key: &str) -> String {
    let shard = if shard_key.is_empty() {
        "loc-anon"
    } else {
        shard_key
    };
    let ok = shard.len() <= 128
        && shard
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        shard.to_string()
    } else {
        format!("h-{}", hex(&Sha256::digest(shard.as_bytes())))
    }
}

/// Run `f` over `items`, up to [`TRANSFER_PARALLELISM`] at a time, in order.
/// Failures come back as values: a panic inside a scoped thread would lose its
/// payload.
fn parallel_map<T: Sync, R: Send>(items: &[T], f: impl Fn(&T) -> R + Sync) -> Vec<R> {
    if items.len() <= 1 {
        return items.iter().map(&f).collect();
    }
    let mut out = Vec::with_capacity(items.len());
    for chunk in items.chunks(TRANSFER_PARALLELISM) {
        let results: Vec<R> = std::thread::scope(|s| {
            let handles: Vec<_> = chunk.iter().map(|item| s.spawn(|| f(item))).collect();
            handles
                .into_iter()
                .map(|h| h.join().expect("blob transfer thread"))
                .collect()
        });
        out.extend(results);
    }
    out
}

// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

fn externalize_bytes_from_env() -> usize {
    for name in [ENV_EXTERNALIZE_BYTES, ENV_EXTERNALIZE_BYTES_ALIAS] {
        let Ok(raw) = std::env::var(name) else {
            continue;
        };
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        match raw.parse::<i64>() {
            Ok(v) if v > 0 => return v as usize,
            Ok(v) => panic!("{name} must be positive, got {v}"),
            Err(_) => panic!("{name} must be an integer, got {raw:?}"),
        }
    }
    DEFAULT_EXTERNALIZE_BYTES
}

fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// The vendor blob client from `VGI_BLOB_VENDOR_URL` and its token, or `None`
/// when the URL is unset. Panics on a misconfiguration (a URL without a token,
/// or a build without the `blob-vendor` feature): a worker must not start with
/// a vendor it cannot use.
pub fn blob_store_from_env() -> Option<Arc<dyn BlobStore>> {
    let url = std::env::var(ENV_VENDOR_URL).unwrap_or_default();
    if url.trim().is_empty() {
        return None;
    }
    #[cfg(feature = "blob-vendor")]
    {
        match vgi_rpc_s3::vendor::VendorBlobClient::from_env() {
            Ok(Some(client)) => Some(Arc::new(client)),
            Ok(None) => None,
            Err(e) => panic!("{ENV_VENDOR_URL}: {}", e.message),
        }
    }
    #[cfg(not(feature = "blob-vendor"))]
    {
        panic!("{ENV_VENDOR_URL} is set but vgi was built without the `blob-vendor` feature")
    }
}

/// Wrap `base` in [`ExternalizingStorage`] when the environment asks for it.
///
/// A backend with a per-value cap (`max_value_bytes`, i.e. the Cloudflare DO)
/// is always wrapped, vendor or not, so its stored format is the same in every
/// process. Any other backend is wrapped only with `VGI_STORAGE_EXTERNALIZE=1`,
/// which then requires `VGI_BLOB_VENDOR_URL`. Panics on a misconfiguration.
pub fn wrap_storage_from_env(
    base: Arc<dyn FunctionStorage>,
    max_value_bytes: Option<usize>,
) -> Arc<dyn FunctionStorage> {
    let enabled = env_flag(ENV_EXTERNALIZE);
    if max_value_bytes.is_none() && !enabled {
        return base;
    }
    let blobs = blob_store_from_env();
    if enabled && blobs.is_none() {
        panic!("{ENV_EXTERNALIZE}=1 needs the blob vendor: set {ENV_VENDOR_URL}");
    }
    let mut threshold = externalize_bytes_from_env();
    if let Some(cap) = max_value_bytes {
        threshold = threshold.min(cap);
    }
    Arc::new(ExternalizingStorage::new(
        base,
        blobs,
        threshold,
        max_value_bytes,
    ))
}

// ---------------------------------------------------------------------------
// The wrapper
// ---------------------------------------------------------------------------

type CleanupJob = Box<dyn FnOnce() + Send>;

/// A [`FunctionStorage`] that envelopes values and externalizes large ones.
/// See the [module docs](self).
pub struct ExternalizingStorage {
    base: Arc<dyn FunctionStorage>,
    blobs: Option<Arc<dyn BlobStore>>,
    threshold: usize,
    max_value_bytes: Option<usize>,
    cleanup: OnceLock<Mutex<mpsc::Sender<CleanupJob>>>,
}

impl ExternalizingStorage {
    /// Wrap `base`. With `blobs`, values of at least `threshold` bytes are
    /// stored as blobs. `max_value_bytes` is the backend's per-value cap: with
    /// no blob store, a value whose envelope exceeds it is refused.
    ///
    /// # Panics
    ///
    /// If `threshold` is zero or exceeds `max_value_bytes`.
    pub fn new(
        base: Arc<dyn FunctionStorage>,
        blobs: Option<Arc<dyn BlobStore>>,
        threshold: usize,
        max_value_bytes: Option<usize>,
    ) -> Self {
        assert!(
            threshold >= 1 && max_value_bytes.is_none_or(|cap| threshold <= cap),
            "externalize threshold ({ENV_EXTERNALIZE_BYTES}) must be between 1 and {}, got {threshold}",
            max_value_bytes.map_or("unbounded".to_string(), |c| c.to_string())
        );
        ExternalizingStorage {
            base,
            blobs,
            threshold,
            max_value_bytes,
            cleanup: OnceLock::new(),
        }
    }

    /// Values of at least this many bytes are externalized (with a vendor).
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// Envelope each value, uploading large ones first (in parallel).
    fn encode_values(&self, values: &[&[u8]], shard: &str, execution_id: &str) -> Vec<Vec<u8>> {
        let large: Vec<usize> = match &self.blobs {
            Some(_) => (0..values.len())
                .filter(|&i| values[i].len() >= self.threshold)
                .collect(),
            None => Vec::new(),
        };
        let mut out: Vec<Vec<u8>> = values
            .iter()
            .enumerate()
            .map(|(i, v)| {
                if large.binary_search(&i).is_ok() {
                    Vec::new()
                } else {
                    encode_inline(v)
                }
            })
            .collect();
        if self.blobs.is_none() {
            if let Some(cap) = self.max_value_bytes {
                if let Some(env) = out.iter().find(|e| e.len() > cap) {
                    fail(RpcError::runtime_error(format!(
                        "storage value of {} bytes exceeds the backend's {cap}-byte limit; set \
                         {ENV_VENDOR_URL} (the blob vendor) so large values are stored as blobs",
                        env.len() - 1
                    )));
                }
            }
        }
        if let (Some(blobs), false) = (&self.blobs, large.is_empty()) {
            let shard = blob_shard(shard);
            let call_id = hex(&rand::random::<[u8; 16]>());
            let results = parallel_map(&large, |&i| {
                blobs.put_bytes(values[i], &shard, execution_id, &format!("{call_id}.{i}"))
            });
            for (&i, result) in large.iter().zip(results) {
                let blob = result.unwrap_or_else(|e| {
                    fail(RpcError::runtime_error(format!(
                        "storage blob upload failed: {}",
                        e.message
                    )))
                });
                out[i] = encode_pointer(&blob).unwrap_or_else(|| {
                    fail(RpcError::runtime_error(format!(
                        "blob vendor returned an unusable object id {:?}",
                        blob.object_id
                    )))
                });
            }
        }
        out
    }

    /// Decode envelopes, fetching pointers in parallel. `claimed` receives
    /// the references read.
    fn decode_values(
        &self,
        envelopes: &[&[u8]],
        claimed: Option<&mut Vec<BlobRef>>,
    ) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = Vec::with_capacity(envelopes.len());
        let mut pointers: Vec<(usize, BlobRef)> = Vec::new();
        for (i, env) in envelopes.iter().enumerate() {
            match decode(env) {
                Ok(Envelope::Inline(v)) => out.push(v.to_vec()),
                Ok(Envelope::Pointer(r)) => {
                    out.push(Vec::new());
                    pointers.push((i, r));
                }
                Err(e) => fail(RpcError::runtime_error(e.to_string())),
            }
        }
        if pointers.is_empty() {
            return out;
        }
        let Some(blobs) = &self.blobs else {
            fail(RpcError::runtime_error(format!(
                "stored value is a blob pointer but {ENV_VENDOR_URL} is not configured in this process"
            )));
        };
        let results = parallel_map(&pointers, |(_, r)| blobs.get_bytes(r));
        for ((i, r), result) in pointers.iter().zip(results) {
            out[*i] = result.unwrap_or_else(|e| fail(blob_read_error(r, &e)));
        }
        if let Some(c) = claimed {
            c.extend(pointers.into_iter().map(|(_, r)| r));
        }
        out
    }

    /// Run `job` on the background clean-up thread (best effort, never on the
    /// query's critical path).
    fn submit_cleanup(
        &self,
        what: &'static str,
        job: impl FnOnce() -> Result<(), BlobError> + Send + 'static,
    ) {
        let sender = self.cleanup.get_or_init(|| {
            let (tx, rx) = mpsc::channel::<CleanupJob>();
            let _ = std::thread::Builder::new()
                .name("vgi-blob-cleanup".into())
                .spawn(move || {
                    for job in rx {
                        job();
                    }
                });
            Mutex::new(tx)
        });
        let wrapped: CleanupJob = Box::new(move || {
            if let Err(e) = job() {
                log::warn!("blob vendor clean-up after {what} failed: {}", e.message);
            }
        });
        let _ = sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .send(wrapped);
    }

    /// Wait for queued clean-up to finish (tests and shutdown).
    pub fn flush_blob_cleanup(&self) {
        if self.cleanup.get().is_none() {
            return;
        }
        let (tx, rx) = mpsc::channel();
        self.submit_cleanup("flush", move || {
            let _ = tx.send(());
            Ok(())
        });
        let _ = rx.recv_timeout(Duration::from_secs(60));
    }

    fn delete_blobs(&self, refs: Vec<BlobRef>, what: &'static str) {
        let (Some(blobs), false) = (self.blobs.clone(), refs.is_empty()) else {
            return;
        };
        let ids: Vec<String> = refs.into_iter().map(|r| r.object_id).collect();
        self.submit_cleanup(what, move || blobs.delete(&ids));
    }

    fn delete_blob_scope(&self, scope: &[u8], kind: &str, what: &'static str) {
        let Some(blobs) = self.blobs.clone() else {
            return;
        };
        let shard = blob_shard(&shard_key_for_scope(scope));
        let execution_id = blob_execution_id(kind, scope);
        self.submit_cleanup(what, move || blobs.delete_scope(&shard, &execution_id));
    }

    fn state_ids(scope: &[u8]) -> (String, String) {
        (shard_key_for_scope(scope), blob_execution_id("s", scope))
    }
}

impl FunctionStorage for ExternalizingStorage {
    fn kv_get(&self, scope: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        let env = self.base.kv_get(scope, key)?;
        self.decode_values(&[&env], None).pop()
    }

    fn kv_put(&self, scope: &[u8], key: &[u8], value: &[u8]) {
        let (shard, exec) = Self::state_ids(scope);
        let env = self
            .encode_values(&[value], &shard, &exec)
            .pop()
            .expect("one value");
        self.base.kv_put(scope, key, &env);
    }

    fn kv_del(&self, scope: &[u8], key: &[u8]) {
        self.base.kv_del(scope, key);
    }

    fn kv_get_many(&self, scope: &[u8], keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        let stored = self.base.kv_get_many(scope, keys);
        let present: Vec<&[u8]> = stored.iter().flatten().map(Vec::as_slice).collect();
        let mut decoded = self.decode_values(&present, None).into_iter();
        stored
            .iter()
            .map(|s| {
                s.as_ref()
                    .map(|_| decoded.next().expect("one value per envelope"))
            })
            .collect()
    }

    fn kv_put_many(&self, scope: &[u8], items: &[(Vec<u8>, Vec<u8>)]) {
        if items.is_empty() {
            self.base.kv_put_many(scope, items);
            return;
        }
        let (shard, exec) = Self::state_ids(scope);
        let values: Vec<&[u8]> = items.iter().map(|(_, v)| v.as_slice()).collect();
        let encoded = self.encode_values(&values, &shard, &exec);
        let wrapped: Vec<(Vec<u8>, Vec<u8>)> = items
            .iter()
            .zip(encoded)
            .map(|((k, _), e)| (k.clone(), e))
            .collect();
        self.base.kv_put_many(scope, &wrapped);
    }

    fn append(&self, scope: &[u8], ns: &[u8], key: &[u8], value: Vec<u8>) -> i64 {
        let (shard, exec) = Self::state_ids(scope);
        let env = self
            .encode_values(&[&value], &shard, &exec)
            .pop()
            .expect("one value");
        self.base.append(scope, ns, key, env)
    }

    fn append_many(&self, scope: &[u8], ns: &[u8], key: &[u8], values: Vec<Vec<u8>>) -> Vec<i64> {
        let (shard, exec) = Self::state_ids(scope);
        let refs: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
        let encoded = self.encode_values(&refs, &shard, &exec);
        self.base.append_many(scope, ns, key, encoded)
    }

    fn scan(
        &self,
        scope: &[u8],
        ns: &[u8],
        key: &[u8],
        after_id: i64,
        limit: usize,
    ) -> Vec<(i64, Vec<u8>)> {
        let rows = self.base.scan(scope, ns, key, after_id, limit);
        let envs: Vec<&[u8]> = rows.iter().map(|(_, v)| v.as_slice()).collect();
        let values = self.decode_values(&envs, None);
        rows.iter().map(|(id, _)| *id).zip(values).collect()
    }

    fn queue_push(&self, scope: &[u8], items: &[Vec<u8>]) {
        let shard = shard_key_for_scope(scope);
        let exec = blob_execution_id("q", scope);
        let refs: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
        let encoded = self.encode_values(&refs, &shard, &exec);
        self.base.queue_push(scope, &encoded);
    }

    fn queue_pop(&self, scope: &[u8]) -> Option<Vec<u8>> {
        let env = self.base.queue_pop(scope)?;
        let mut claimed = Vec::new();
        let value = self.decode_values(&[&env], Some(&mut claimed)).pop();
        // The item is gone from the queue: its blob is never read again.
        self.delete_blobs(claimed, "queue_pop");
        value
    }

    fn clear(&self, scope: &[u8]) {
        self.base.clear(scope);
        self.delete_blob_scope(scope, "s", "clear");
        self.delete_blob_scope(scope, "q", "clear");
    }

    fn gc(&self, ttl: Duration) {
        self.base.gc(ttl);
    }
}

#[cfg(test)]
mod tests;

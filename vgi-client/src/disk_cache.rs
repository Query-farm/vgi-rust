// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Persistent, bounded Arrow IPC result storage.
//!
//! This module deliberately stores only complete producer results. A capture
//! writes one Arrow IPC file per producer partition and publishes a reference
//! only after every file and its manifest are durable. Dropping an unfinished
//! capture removes its temporary directory.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(test)]
use std::sync::Condvar;

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReader;
use arrow_ipc::writer::{DictionaryTracker, FileWriter, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use vgi_protocol::cache_control::{CacheControl, CACHE_SCOPE_CATALOG, CACHE_SCOPE_TRANSACTION};

use crate::{CacheFreshness, CacheKey};

const FORMAT_VERSION: u32 = 1;
const FORMAT_DIR: &str = "v1";
const SECRET_FILE: &str = "secret";
const INITIALIZATION_FILE: &str = ".initialize";
const OPERATIONS_FILE: &str = ".operations";
const TEMP_GRACE: Duration = Duration::from_secs(60 * 60);

type HmacSha256 = Hmac<Sha256>;

/// Errors produced by the persistent result cache.
#[derive(Debug)]
pub enum DiskCacheError {
    /// Filesystem operation failed.
    Io(io::Error),
    /// Arrow IPC encoding or decoding failed.
    Arrow(ArrowError),
    /// Persistent metadata was invalid.
    Metadata(serde_json::Error),
    /// A caller supplied an invalid capture operation or option.
    Invalid(String),
    /// An incremental capture crossed the configured encoded-byte bound.
    EntryTooLarge {
        /// Encoded bytes observed so far.
        bytes: u64,
        /// Configured disk-byte bound.
        limit: u64,
    },
}

impl fmt::Display for DiskCacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "disk cache I/O error: {error}"),
            Self::Arrow(error) => write!(f, "disk cache Arrow IPC error: {error}"),
            Self::Metadata(error) => write!(f, "disk cache metadata error: {error}"),
            Self::Invalid(error) => write!(f, "invalid disk cache operation: {error}"),
            Self::EntryTooLarge { bytes, limit } => write!(
                f,
                "disk cache capture reached {bytes} bytes, exceeding its {limit}-byte bound"
            ),
        }
    }
}

impl Error for DiskCacheError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Arrow(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Invalid(_) | Self::EntryTooLarge { .. } => None,
        }
    }
}

impl From<io::Error> for DiskCacheError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ArrowError> for DiskCacheError {
    fn from(value: ArrowError) -> Self {
        Self::Arrow(value)
    }
}

impl From<serde_json::Error> for DiskCacheError {
    fn from(value: serde_json::Error) -> Self {
        Self::Metadata(value)
    }
}

/// Result type used by the persistent result cache.
pub type DiskCacheResult<T> = Result<T, DiskCacheError>;

/// Arrow IPC compression used for newly captured entries.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiskCacheCodec {
    /// Write uncompressed Arrow IPC.
    None,
    /// Write Zstandard-compressed Arrow IPC at level one.
    #[default]
    Zstd,
    /// Write LZ4-frame-compressed Arrow IPC.
    Lz4,
}

/// Host-owned persistent cache configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskCacheOptions {
    /// Durable cache root. It must not be DataFusion's temporary spill root.
    pub root: PathBuf,
    /// Admission bound for encoded Arrow payloads referenced by committed
    /// entries or retained by active replay leases. Metadata and in-progress
    /// captures are excluded, so this is not a filesystem quota.
    pub max_bytes: u64,
    /// Admission bound for committed references plus leased orphan objects.
    pub max_entries: usize,
    /// Codec for newly captured Arrow IPC files.
    pub codec: DiskCacheCodec,
}

impl DiskCacheOptions {
    /// Conservative defaults rooted at the supplied host-owned path.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            max_bytes: 1024 * 1024 * 1024,
            max_entries: 131_072,
            codec: DiskCacheCodec::default(),
        }
    }
}

/// Why a completed capture was not published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskCacheSkip {
    /// The worker did not opt into storage or explicitly prohibited it.
    NotCacheable,
    /// The result was transaction-scoped.
    TransactionScoped,
    /// The worker named a reuse scope this client does not understand.
    UnsupportedScope,
    /// The result was already stale or named no positive freshness lifetime.
    NoFreshness,
    /// The encoded entry cannot fit under the configured disk-byte bound.
    EntryTooLarge,
    /// Entry storage is disabled by a zero count bound.
    Disabled,
    /// Existing leased generations leave insufficient disk capacity.
    Capacity,
}

/// Result of committing a complete capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskCacheCommit {
    /// The reference was durably published.
    Stored {
        /// Encoded Arrow bytes, excluding the small JSON manifest and ref.
        stored_bytes: u64,
        /// Rows across all partitions.
        rows: u64,
        /// Number of physical producer partitions.
        partitions: usize,
    },
    /// The worker policy or configured bounds refused storage.
    Skipped(DiskCacheSkip),
}

impl DiskCacheCommit {
    /// Whether this commit durably published a reference.
    pub fn is_stored(self) -> bool {
        matches!(self, Self::Stored { .. })
    }
}

/// Persistent cache counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DiskCacheStats {
    /// Fresh lookups served.
    pub hits: u64,
    /// Lookups with no fresh valid entry.
    pub misses: u64,
    /// Entries durably published.
    pub inserts: u64,
    /// Entries removed to meet count or byte bounds.
    pub evictions_lru: u64,
    /// Expired entries removed.
    pub evictions_ttl: u64,
    /// Captures refused by policy or bounds.
    pub refusals: u64,
    /// Corrupt entries removed before serving any rows.
    pub corruptions: u64,
    /// Captures explicitly or implicitly aborted.
    pub capture_aborts: u64,
    /// Stale entries served under an explicit worker grace window.
    pub stale_serves: u64,
    /// Conditional validations completed with not-modified.
    pub revalidations: u64,
    /// Currently committed references.
    pub entries: usize,
    /// Encoded Arrow bytes currently referenced.
    pub total_bytes: u64,
}

/// Safe diagnostics for one persistent entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskCacheEntryInfo {
    /// HMAC fingerprint of the complete result key.
    pub key_fingerprint: String,
    /// Catalog name, without credentials.
    pub catalog: String,
    /// Schema-qualified function name.
    pub function: String,
    /// Encoded Arrow bytes.
    pub stored_bytes: u64,
    /// Rows across all partitions.
    pub rows: u64,
    /// Arrow record batches across all partitions.
    pub batches: u64,
    /// Physical producer partitions.
    pub partitions: usize,
    /// Codec recorded for diagnostics.
    pub codec: DiskCacheCodec,
    /// Age since this generation's publication metadata was prepared.
    pub age: Duration,
    /// Remaining freshness, saturated at zero.
    pub freshness_remaining: Duration,
    /// Time-travel coordinate included in this result's identity.
    pub at: Option<(String, String)>,
    /// Whether an ETag validator is retained (the value is never exposed).
    pub has_etag: bool,
    /// Whether a Last-Modified validator is retained.
    pub has_last_modified: bool,
    /// Whether the worker permits conditional validation.
    pub revalidatable: bool,
}

/// A persistent result cache.
#[derive(Clone)]
pub struct DiskCache {
    inner: Arc<Inner>,
}

struct Inner {
    options: DiskCacheOptions,
    format_root: PathBuf,
    secret: [u8; 32],
    operation_file: File,
    operations: Mutex<()>,
    leases: Mutex<HashMap<PathBuf, usize>>,
    active_temps: Mutex<HashSet<PathBuf>>,
    access: Mutex<HashMap<PathBuf, u64>>,
    stats: Mutex<DiskCacheStats>,
    #[cfg(test)]
    commit_fault: Mutex<Option<CommitFault>>,
    #[cfg(test)]
    commit_gate: Mutex<Option<Arc<CommitGate>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitFault {
    AfterObjectRename,
    AfterRefPublish,
}

#[cfg(test)]
struct CommitGate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

#[cfg(test)]
impl CommitGate {
    fn new() -> Self {
        Self {
            state: Mutex::new((false, false)),
            changed: Condvar::new(),
        }
    }

    fn enter_and_wait(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn wait_until_entered(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.0 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        self.changed.notify_all();
    }
}

/// An incremental, never-partial capture.
pub struct DiskCapture {
    owner_root: PathBuf,
    temp_dir: Option<PathBuf>,
    temp_lease: Option<File>,
    schema: SchemaRef,
    schema_hash: String,
    writers: Vec<Option<FileWriter<File>>>,
    batch_counts: Vec<u64>,
    rows: u64,
    codec: DiskCacheCodec,
    max_bytes: u64,
    limit_exceeded: bool,
    active_temps: Arc<Inner>,
}

/// A fully validated cache hit. Fresh lookups return fresh entries, while the
/// explicit revalidation lookup may return a stale, leased generation.
pub struct DiskCacheHit {
    schema: SchemaRef,
    object_dir: PathBuf,
    manifest: Manifest,
    record: RefRecord,
    lease: Arc<Lease>,
}

/// Iterator over one cached producer partition.
pub struct DiskPartitionReader {
    reader: FileReader<BufReader<File>>,
    _lease: Arc<Lease>,
}

impl Iterator for DiskPartitionReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

struct Lease {
    object_dir: PathBuf,
    file: File,
    inner: Arc<Inner>,
}

struct RootOperation<'a> {
    file: &'a File,
}

struct PublicationRollback<'a> {
    cache: &'a DiskCache,
    object_dir: PathBuf,
    ref_path: PathBuf,
    old_ref: Option<Vec<u8>>,
    published: RefRecord,
    armed: bool,
}

impl PublicationRollback<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PublicationRollback<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let current_is_published =
            read_ref(&self.ref_path).is_ok_and(|current| current == self.published);
        let ref_rolled_back = if current_is_published {
            match &self.old_ref {
                Some(old) => write_replace_synced(&self.ref_path, old).is_ok(),
                None => remove_file_synced(&self.ref_path).is_ok(),
            }
        } else {
            true
        };
        if ref_rolled_back {
            let _ = self.cache.remove_object_if_unleased(&self.object_dir);
        }
    }
}

impl Drop for RootOperation<'_> {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let _ = self.file.unlock();
        let mut leases = self.inner.leases.lock().unwrap();
        if let Some(count) = leases.get_mut(&self.object_dir) {
            *count -= 1;
            if *count == 0 {
                leases.remove(&self.object_dir);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RefRecord {
    version: u32,
    key_digest: String,
    identity_digest: String,
    generation: String,
    manifest_sha256: String,
    created_ms: u64,
    expires_ms: u64,
    catalog: String,
    function: String,
    stored_bytes: u64,
    rows: u64,
    batches: u64,
    partitions: usize,
    codec: DiskCacheCodec,
    at: Option<(String, String)>,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    last_modified: Option<String>,
    #[serde(default)]
    revalidatable: bool,
    #[serde(default)]
    stale_if_error_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Manifest {
    version: u32,
    key_digest: String,
    schema_sha256: String,
    codec: DiskCacheCodec,
    rows: u64,
    stored_bytes: u64,
    parts: Vec<PartRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartRecord {
    file: String,
    bytes: u64,
    rows: u64,
    batches: u64,
    sha256: String,
}

impl DiskCache {
    /// Open or create a durable cache at a host-owned path.
    pub fn open(options: DiskCacheOptions) -> DiskCacheResult<Self> {
        if options.root.as_os_str().is_empty() {
            return Err(DiskCacheError::Invalid(
                "the durable cache root must not be empty".into(),
            ));
        }
        create_private_dir(&options.root)?;
        let format_root = options.root.join(FORMAT_DIR);
        create_private_dir(&format_root)?;
        create_private_dir(&format_root.join("tmp"))?;
        let initialization_file = open_private_shared(&format_root.join(INITIALIZATION_FILE))?;
        initialization_file.lock()?;
        let initialization = RootOperation {
            file: &initialization_file,
        };
        let secret = load_or_create_secret(&format_root)?;
        drop(initialization);
        let operation_file = open_private_shared(&format_root.join(OPERATIONS_FILE))?;
        let cache = Self {
            inner: Arc::new(Inner {
                options,
                format_root,
                secret,
                operation_file,
                operations: Mutex::new(()),
                leases: Mutex::new(HashMap::new()),
                active_temps: Mutex::new(HashSet::new()),
                access: Mutex::new(HashMap::new()),
                stats: Mutex::new(DiskCacheStats::default()),
                #[cfg(test)]
                commit_fault: Mutex::new(None),
                #[cfg(test)]
                commit_gate: Mutex::new(None),
            }),
        };
        cache.reconcile_on_open()?;
        Ok(cache)
    }

    /// Start a capture with an explicit schema and physical partition count.
    ///
    /// Writers are created immediately, so a zero-row result is still a valid
    /// cache entry with one empty Arrow IPC file per declared partition.
    pub fn begin_capture(
        &self,
        schema: SchemaRef,
        partitions: usize,
    ) -> DiskCacheResult<DiskCapture> {
        if partitions == 0 {
            return Err(DiskCacheError::Invalid(
                "a capture must declare at least one partition".into(),
            ));
        }
        let generation = self.unique_generation()?;
        let temp_dir = self.inner.format_root.join("tmp").join(generation);
        create_private_dir(&temp_dir)?;
        write_new_synced(&temp_dir.join(".lease"), &[])?;
        let temp_lease = OpenOptions::new()
            .read(true)
            .write(true)
            .open(temp_dir.join(".lease"))?;
        temp_lease.lock_shared()?;
        let options = ipc_options(self.inner.options.codec)?;
        let mut writers = Vec::with_capacity(partitions);
        for partition in 0..partitions {
            let path = temp_dir.join(part_name(partition));
            let file = create_private_file(&path)?;
            match FileWriter::try_new_with_options(file, schema.as_ref(), options.clone()) {
                Ok(writer) => writers.push(Some(writer)),
                Err(error) => {
                    let _ = fs::remove_dir_all(&temp_dir);
                    return Err(error.into());
                }
            }
        }
        self.inner
            .active_temps
            .lock()
            .unwrap()
            .insert(temp_dir.clone());
        Ok(DiskCapture {
            owner_root: self.inner.format_root.clone(),
            temp_dir: Some(temp_dir),
            temp_lease: Some(temp_lease),
            schema_hash: schema_hash(schema.as_ref()),
            schema,
            writers,
            batch_counts: vec![0; partitions],
            rows: 0,
            codec: self.inner.options.codec,
            max_bytes: self.inner.options.max_bytes,
            limit_exceeded: false,
            active_temps: Arc::clone(&self.inner),
        })
    }

    /// Publish a complete capture if worker policy and configured bounds allow.
    pub fn commit(
        &self,
        key: CacheKey,
        capture: DiskCapture,
        ttl: Duration,
        control: Option<&CacheControl>,
    ) -> DiskCacheResult<DiskCacheCommit> {
        let freshness = CacheFreshness::from_lifetime(ttl).ok_or_else(|| {
            DiskCacheError::Invalid("cache freshness lifetime exceeds SystemTime".into())
        })?;
        self.commit_freshness(key, capture, freshness, control)
    }

    /// Publish using a receipt-time wall and monotonic freshness decision.
    ///
    /// This is the preferred API for a caller that also populates an in-memory
    /// tier: both tiers derive their remaining lifetime from the same receipt.
    pub fn commit_freshness(
        &self,
        key: CacheKey,
        capture: DiskCapture,
        freshness: CacheFreshness,
        control: Option<&CacheControl>,
    ) -> DiskCacheResult<DiskCacheCommit> {
        self.commit_with_deadline(
            key,
            capture,
            freshness.expires_at(),
            Some(freshness),
            control,
        )
    }

    /// Publish a complete capture against one absolute receipt deadline.
    ///
    /// The deadline must be computed when the complete worker result and its
    /// cache control are received. Finalization and root-lock contention do not
    /// grant additional freshness; an entry that expires before publication is
    /// refused.
    pub fn commit_until(
        &self,
        key: CacheKey,
        capture: DiskCapture,
        expires_at: SystemTime,
        control: Option<&CacheControl>,
    ) -> DiskCacheResult<DiskCacheCommit> {
        self.commit_with_deadline(key, capture, expires_at, None, control)
    }

    fn commit_with_deadline(
        &self,
        key: CacheKey,
        mut capture: DiskCapture,
        expires_at: SystemTime,
        freshness: Option<CacheFreshness>,
        control: Option<&CacheControl>,
    ) -> DiskCacheResult<DiskCacheCommit> {
        let expired = || {
            freshness.map_or_else(
                || expires_at <= SystemTime::now(),
                CacheFreshness::is_expired,
            )
        };
        let immediate_revalidation = freshness.is_some_and(CacheFreshness::is_immediately_stale)
            && control.is_some_and(valid_revalidation_control);
        if capture.owner_root != self.inner.format_root {
            return Err(DiskCacheError::Invalid(
                "capture belongs to a different disk cache".into(),
            ));
        }
        if capture.limit_exceeded {
            capture.remove_temp(false)?;
            return Ok(DiskCacheCommit::Skipped(DiskCacheSkip::EntryTooLarge));
        }
        let skip = match control {
            None => Some(DiskCacheSkip::NotCacheable),
            Some(control) if control.no_store => Some(DiskCacheSkip::NotCacheable),
            Some(control) if control.scope == CACHE_SCOPE_TRANSACTION => {
                Some(DiskCacheSkip::TransactionScoped)
            }
            Some(control) if control.scope != CACHE_SCOPE_CATALOG => {
                Some(DiskCacheSkip::UnsupportedScope)
            }
            _ if expired() && !immediate_revalidation => Some(DiskCacheSkip::NoFreshness),
            _ if self.inner.options.max_entries == 0 || self.inner.options.max_bytes == 0 => {
                Some(DiskCacheSkip::Disabled)
            }
            _ => None,
        };
        if let Some(reason) = skip {
            self.inner.stats.lock().unwrap().refusals += 1;
            capture.remove_temp(false)?;
            return Ok(DiskCacheCommit::Skipped(reason));
        }

        let temp_dir = capture.temp_dir.as_ref().unwrap().clone();
        let parts = capture.finish_parts()?;
        let stored_bytes = parts.iter().map(|part| part.bytes).sum();
        if stored_bytes > self.inner.options.max_bytes {
            self.inner.stats.lock().unwrap().refusals += 1;
            capture.remove_temp(false)?;
            return Ok(DiskCacheCommit::Skipped(DiskCacheSkip::EntryTooLarge));
        }

        let key_digest = self.digest(&key.stable_bytes());
        let identity_digest = self.digest(key.identity_scope.as_bytes());
        let generation = temp_dir
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| DiskCacheError::Invalid("invalid capture generation".into()))?
            .to_string();
        let manifest = Manifest {
            version: FORMAT_VERSION,
            key_digest: key_digest.clone(),
            schema_sha256: capture.schema_hash.clone(),
            codec: capture.codec,
            rows: capture.rows,
            stored_bytes,
            parts,
        };
        let manifest_bytes = serde_json::to_vec(&manifest)?;
        let manifest_path = temp_dir.join("manifest.json");
        write_new_synced(&manifest_path, &manifest_bytes)?;
        sync_dir(&temp_dir)?;

        let shard = self.identity_root(&identity_digest)?;
        let objects = shard.join("objects");
        let refs = shard.join("refs");
        create_private_dir(&objects)?;
        create_private_dir(&refs)?;
        let object_dir = objects.join(&generation);
        let ref_path = refs.join(format!("{key_digest}.ref"));
        let now = epoch_ms(SystemTime::now());
        let record = RefRecord {
            version: FORMAT_VERSION,
            key_digest: key_digest.clone(),
            identity_digest,
            generation,
            manifest_sha256: sha256_hex(&manifest_bytes),
            created_ms: now,
            expires_ms: epoch_ms(expires_at),
            catalog: key.catalog,
            function: key.function,
            stored_bytes,
            rows: capture.rows,
            batches: manifest.parts.iter().map(|part| part.batches).sum(),
            partitions: manifest.parts.len(),
            codec: capture.codec,
            at: key.at,
            etag: control.and_then(|control| control.etag.clone()),
            last_modified: control.and_then(|control| control.last_modified.clone()),
            revalidatable: control.is_some_and(valid_revalidation_control),
            stale_if_error_seconds: control
                .and_then(|control| control.stale_if_error)
                .and_then(|seconds| u64::try_from(seconds).ok())
                .filter(|seconds| *seconds > 0),
        };
        let ref_bytes = serde_json::to_vec(&record)?;

        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_exclusive()?;
        if expired() && !immediate_revalidation {
            self.inner.stats.lock().unwrap().refusals += 1;
            drop(_root_operation);
            drop(_operation);
            capture.remove_temp(false)?;
            return Ok(DiskCacheCommit::Skipped(DiskCacheSkip::NoFreshness));
        }
        self.remove_invalid_refs()?;
        self.remove_orphan_objects()?;
        if !self.make_room(stored_bytes, Some(&ref_path))? {
            self.inner.stats.lock().unwrap().refusals += 1;
            drop(_root_operation);
            drop(_operation);
            capture.remove_temp(false)?;
            return Ok(DiskCacheCommit::Skipped(DiskCacheSkip::Capacity));
        }
        if expired() && !immediate_revalidation {
            self.inner.stats.lock().unwrap().refusals += 1;
            drop(_root_operation);
            drop(_operation);
            capture.remove_temp(false)?;
            return Ok(DiskCacheCommit::Skipped(DiskCacheSkip::NoFreshness));
        }
        let old_ref = fs::read(&ref_path).ok();
        let old_record = old_ref.as_deref().and_then(|bytes| parse_ref(bytes).ok());
        fs::rename(&temp_dir, &object_dir)?;
        capture.disarm_temp();
        let mut rollback = PublicationRollback {
            cache: self,
            object_dir: object_dir.clone(),
            ref_path: ref_path.clone(),
            old_ref,
            published: record.clone(),
            armed: true,
        };
        sync_dir(&objects)?;
        sync_dir(&self.inner.format_root.join("tmp"))?;
        self.maybe_fail_commit(CommitFault::AfterObjectRename)?;

        write_replace_synced(&ref_path, &ref_bytes)?;
        sync_dir(&refs)?;
        self.maybe_fail_commit(CommitFault::AfterRefPublish)?;
        self.wait_after_ref_publish();
        if expired() && !immediate_revalidation {
            self.inner.stats.lock().unwrap().refusals += 1;
            return Ok(DiskCacheCommit::Skipped(DiskCacheSkip::NoFreshness));
        }
        self.inner.access.lock().unwrap().insert(ref_path, now);
        let mut stats = self.inner.stats.lock().unwrap();
        stats.inserts += 1;
        let (entries, bytes) = self.current_size_unlocked()?;
        stats.entries = entries;
        stats.total_bytes = bytes;
        drop(stats);
        rollback.disarm();
        if let Some(old) = old_record {
            let old_object = objects.join(old.generation);
            if old_object != object_dir {
                // The new generation is already committed. Cleanup is
                // best-effort; an active lease or transient unlink failure is
                // reconciled by a later reap/open.
                let _ = self.remove_object_if_unleased(&old_object);
            }
        }
        Ok(DiskCacheCommit::Stored {
            stored_bytes,
            rows: record.rows,
            partitions: record.partitions,
        })
    }

    /// Find and fully validate a fresh entry before returning a replay handle.
    ///
    /// Expired or corrupt entries are removed and returned as a clean miss, so
    /// a caller can recompute without ever mixing cached and live rows.
    pub fn lookup(&self, key: &CacheKey) -> DiskCacheResult<Option<DiskCacheHit>> {
        self.lookup_inner(key, None, false)
    }

    /// Find a fresh entry whose stored schema exactly matches the caller's
    /// expected output schema.
    ///
    /// A mismatch is a clean miss and removes only the generation observed by
    /// this lookup. A concurrent replacement therefore cannot be invalidated
    /// by a stale planner.
    pub fn lookup_expected_schema(
        &self,
        key: &CacheKey,
        expected_schema: &SchemaRef,
    ) -> DiskCacheResult<Option<DiskCacheHit>> {
        self.lookup_inner(key, Some(expected_schema.as_ref()), false)
    }

    /// Find and fully validate a retained entry for conditional revalidation.
    ///
    /// Unlike a fresh lookup, this may return an expired entry, but only when
    /// the stored worker policy opted into revalidation and supplied an ETag or
    /// Last-Modified validator. Corruption and schema mismatch remain clean
    /// misses with generation-conditional cleanup.
    pub fn lookup_for_revalidation_expected_schema(
        &self,
        key: &CacheKey,
        expected_schema: &SchemaRef,
    ) -> DiskCacheResult<Option<DiskCacheHit>> {
        self.lookup_inner(key, Some(expected_schema.as_ref()), true)
    }

    fn lookup_inner(
        &self,
        key: &CacheKey,
        expected_schema: Option<&Schema>,
        allow_stale_revalidation: bool,
    ) -> DiskCacheResult<Option<DiskCacheHit>> {
        let key_digest = self.digest(&key.stable_bytes());
        let identity_digest = self.digest(key.identity_scope.as_bytes());
        let ref_path = self
            .inner
            .format_root
            .join(&identity_digest)
            .join("refs")
            .join(format!("{key_digest}.ref"));
        let operation = self.inner.operations.lock().unwrap();
        let root_operation = self.lock_root_shared()?;
        let ref_bytes = match fs::read(&ref_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.inner.stats.lock().unwrap().misses += 1;
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let record = match parse_ref(&ref_bytes) {
            Ok(record) => record,
            Err(_) => {
                drop(root_operation);
                let _root_operation = self.lock_root_exclusive()?;
                if fs::read(&ref_path).ok().as_deref() == Some(ref_bytes.as_slice()) {
                    let _ = remove_file_synced(&ref_path);
                }
                let mut stats = self.inner.stats.lock().unwrap();
                stats.misses += 1;
                stats.corruptions += 1;
                return Ok(None);
            }
        };
        if record.version != FORMAT_VERSION
            || record.key_digest != key_digest
            || record.identity_digest != identity_digest
            || !safe_component(&record.generation)
        {
            drop(root_operation);
            let _root_operation = self.lock_root_exclusive()?;
            self.remove_record_if_current(&ref_path, &record)?;
            let mut stats = self.inner.stats.lock().unwrap();
            stats.misses += 1;
            stats.corruptions += 1;
            return Ok(None);
        }
        if record.expires_ms <= epoch_ms(SystemTime::now()) {
            if record.revalidatable && (record.etag.is_some() || record.last_modified.is_some()) {
                if allow_stale_revalidation {
                    // Continue through full integrity/schema validation. The
                    // caller may send these validators but must not serve the
                    // bytes unless validation succeeds or stale-if-error allows it.
                } else {
                    self.inner.stats.lock().unwrap().misses += 1;
                    return Ok(None);
                }
            } else {
                drop(root_operation);
                let _root_operation = self.lock_root_exclusive()?;
                self.remove_record_if_current(&ref_path, &record)?;
                let mut stats = self.inner.stats.lock().unwrap();
                stats.misses += 1;
                stats.evictions_ttl += 1;
                return Ok(None);
            }
        } else if allow_stale_revalidation {
            // A caller should normally try the fresh path first. Returning a
            // fresh validator is still safe and keeps this API race tolerant.
            if !record.revalidatable || (record.etag.is_none() && record.last_modified.is_none()) {
                self.inner.stats.lock().unwrap().misses += 1;
                return Ok(None);
            }
        }

        let object_dir = self
            .inner
            .format_root
            .join(&identity_digest)
            .join("objects")
            .join(&record.generation);
        let lease = match self.acquire_lease(&object_dir) {
            Ok(lease) => lease,
            Err(DiskCacheError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                drop(root_operation);
                let _root_operation = self.lock_root_exclusive()?;
                self.remove_record_if_current(&ref_path, &record)?;
                let mut stats = self.inner.stats.lock().unwrap();
                stats.misses += 1;
                stats.corruptions += 1;
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        // The shared root operation lock made acquiring the object lease and
        // reading its ref one atomic observation with respect to mutations.
        // The object lease now protects replay, so ref mutations may proceed.
        drop(root_operation);
        drop(operation);
        match self.validate_hit(&record, &object_dir, Arc::clone(&lease), expected_schema) {
            Ok(hit) => {
                if !allow_stale_revalidation {
                    let now = epoch_ms(SystemTime::now());
                    self.inner.access.lock().unwrap().insert(ref_path, now);
                    self.inner.stats.lock().unwrap().hits += 1;
                }
                Ok(Some(hit))
            }
            Err(_) => {
                drop(lease);
                let _operation = self.inner.operations.lock().unwrap();
                let _root_operation = self.lock_root_exclusive()?;
                self.remove_record_if_current(&ref_path, &record)?;
                let mut stats = self.inner.stats.lock().unwrap();
                stats.misses += 1;
                stats.corruptions += 1;
                Ok(None)
            }
        }
    }

    /// Remove one exact persistent key.
    pub fn remove(&self, key: &CacheKey) -> DiskCacheResult<bool> {
        let key_digest = self.digest(&key.stable_bytes());
        let identity_digest = self.digest(key.identity_scope.as_bytes());
        let ref_path = self
            .inner
            .format_root
            .join(identity_digest)
            .join("refs")
            .join(format!("{key_digest}.ref"));
        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_exclusive()?;
        let record = match read_ref(&ref_path) {
            Ok(record) => record,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        self.remove_record(&ref_path, &record)?;
        self.refresh_current_stats()?;
        Ok(true)
    }

    /// Slide the observed generation's freshness after a successful
    /// conditional `not_modified` response.
    ///
    /// The update is generation conditional: if another process published a
    /// replacement after this hit was read, that newer ref is left untouched.
    pub fn revalidate_freshness(
        &self,
        hit: &DiskCacheHit,
        freshness: CacheFreshness,
        control: &CacheControl,
    ) -> DiskCacheResult<bool> {
        if control.no_store
            || control.scope != CACHE_SCOPE_CATALOG
            || (freshness.is_immediately_stale() && !valid_revalidation_control(control))
        {
            return Ok(false);
        }
        if !hit.record.revalidatable
            || (hit.record.etag.is_none() && hit.record.last_modified.is_none())
        {
            return Ok(false);
        }
        if freshness.is_expired() && !freshness.is_immediately_stale() {
            return Ok(false);
        }
        let ref_path = self
            .inner
            .format_root
            .join(&hit.record.identity_digest)
            .join("refs")
            .join(format!("{}.ref", hit.record.key_digest));
        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_exclusive()?;
        let current = match read_ref(&ref_path) {
            Ok(current) if current == hit.record => current,
            Ok(_) => return Ok(false),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let now = SystemTime::now();
        let mut refreshed = current;
        refreshed.created_ms = epoch_ms(now);
        refreshed.expires_ms = epoch_ms(freshness.expires_at());
        refreshed.etag = control.etag.clone();
        refreshed.last_modified = control.last_modified.clone();
        refreshed.revalidatable = valid_revalidation_control(control);
        refreshed.stale_if_error_seconds = if refreshed.revalidatable {
            control
                .stale_if_error
                .and_then(|seconds| u64::try_from(seconds).ok())
                .filter(|seconds| *seconds > 0)
        } else {
            None
        };
        write_replace_synced(&ref_path, &serde_json::to_vec(&refreshed)?)?;
        if !freshness.is_immediately_stale() && freshness.is_expired() {
            // A positive receipt lifetime elapsed while the ref was being
            // synchronized. Remove only the generation we just observed.
            self.remove_record_if_current(&ref_path, &refreshed)?;
            self.refresh_current_stats()?;
            return Ok(false);
        }
        self.inner
            .access
            .lock()
            .unwrap()
            .insert(ref_path, epoch_ms(now));
        self.inner.stats.lock().unwrap().revalidations += 1;
        Ok(true)
    }

    /// Remove exactly the generation represented by a validated hit.
    ///
    /// Used when a conditional response revokes cache eligibility. Concurrent
    /// replacements are preserved.
    pub fn remove_hit(&self, hit: &DiskCacheHit) -> DiskCacheResult<bool> {
        let ref_path = self
            .inner
            .format_root
            .join(&hit.record.identity_digest)
            .join("refs")
            .join(format!("{}.ref", hit.record.key_digest));
        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_exclusive()?;
        let removed = self.remove_record_if_current(&ref_path, &hit.record)?;
        self.refresh_current_stats()?;
        Ok(removed)
    }

    /// Count a worker-authorized stale-if-error replay.
    pub fn record_stale_serve(&self) {
        self.inner.stats.lock().unwrap().stale_serves += 1;
    }

    /// Remove every entry for one authenticated catalog scope.
    pub fn flush_scope(&self, identity_scope: &str) -> DiskCacheResult<usize> {
        let identity_digest = self.digest(identity_scope.as_bytes());
        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_exclusive()?;
        let refs = self.inner.format_root.join(identity_digest).join("refs");
        let removed = self.remove_matching_refs(|path, _| path.starts_with(&refs))?;
        self.refresh_current_stats()?;
        Ok(removed)
    }

    /// Remove every identity's entries for one catalog.
    pub fn flush_catalog(&self, catalog: &str) -> DiskCacheResult<usize> {
        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_exclusive()?;
        let removed = self.remove_matching_refs(|_, record| record.catalog == catalog)?;
        self.refresh_current_stats()?;
        Ok(removed)
    }

    /// Remove all committed entries. Active replay objects are removed after
    /// their leases are released and a later reap runs.
    pub fn flush_all(&self) -> DiskCacheResult<usize> {
        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_exclusive()?;
        let removed = self.remove_matching_refs(|_, _| true)? + self.remove_invalid_refs()?;
        self.refresh_current_stats()?;
        Ok(removed)
    }

    /// Remove expired entries, old abandoned captures, unreferenced objects,
    /// and least-recently-used entries needed to enforce current bounds.
    pub fn reap(&self) -> DiskCacheResult<usize> {
        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_exclusive()?;
        let before = self.current_size_unlocked()?.0;
        self.remove_invalid_refs()?;
        self.remove_expired()?;
        self.remove_old_temps()?;
        self.remove_orphan_objects()?;
        let _ = self.make_room(0, None)?;
        self.refresh_current_stats()?;
        let after = self.current_size_unlocked()?.0;
        Ok(before.saturating_sub(after))
    }

    /// Snapshot counters and current occupancy.
    pub fn stats(&self) -> DiskCacheResult<DiskCacheStats> {
        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_shared()?;
        self.refresh_current_stats()?;
        Ok(*self.inner.stats.lock().unwrap())
    }

    /// Snapshot committed entries without exposing identity or validator data.
    pub fn entries(&self) -> DiskCacheResult<Vec<DiskCacheEntryInfo>> {
        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_shared()?;
        let now = epoch_ms(SystemTime::now());
        let mut entries = Vec::new();
        for (path, record) in self.refs()? {
            let _ = path;
            entries.push(DiskCacheEntryInfo {
                key_fingerprint: record.key_digest,
                catalog: record.catalog,
                function: record.function,
                stored_bytes: record.stored_bytes,
                rows: record.rows,
                batches: record.batches,
                partitions: record.partitions,
                codec: record.codec,
                age: Duration::from_millis(now.saturating_sub(record.created_ms)),
                freshness_remaining: Duration::from_millis(record.expires_ms.saturating_sub(now)),
                at: record.at,
                has_etag: record.etag.is_some(),
                has_last_modified: record.last_modified.is_some(),
                revalidatable: record.revalidatable,
            });
        }
        Ok(entries)
    }

    fn unique_generation(&self) -> DiskCacheResult<String> {
        let mut random = [0_u8; 32];
        getrandom::fill(&mut random).map_err(|error| {
            DiskCacheError::Io(io::Error::other(format!(
                "cannot create cache generation: {error}"
            )))
        })?;
        let mut seed = Vec::with_capacity(48);
        seed.extend_from_slice(&epoch_ms(SystemTime::now()).to_le_bytes());
        seed.extend_from_slice(&std::process::id().to_le_bytes());
        seed.extend_from_slice(&random);
        Ok(self.digest(&seed))
    }

    #[cfg(test)]
    fn maybe_fail_commit(&self, point: CommitFault) -> DiskCacheResult<()> {
        let mut fault = self.inner.commit_fault.lock().unwrap();
        if *fault == Some(point) {
            *fault = None;
            return Err(DiskCacheError::Io(io::Error::other(format!(
                "injected commit failure at {point:?}"
            ))));
        }
        Ok(())
    }

    #[cfg(test)]
    fn wait_after_ref_publish(&self) {
        let gate = self.inner.commit_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.enter_and_wait();
        }
    }

    #[cfg(not(test))]
    fn maybe_fail_commit(&self, _point: CommitFault) -> DiskCacheResult<()> {
        Ok(())
    }

    #[cfg(not(test))]
    fn wait_after_ref_publish(&self) {}

    fn digest(&self, bytes: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(&self.inner.secret).expect("HMAC accepts any key");
        mac.update(bytes);
        hex(&mac.finalize().into_bytes())
    }

    fn identity_root(&self, identity_digest: &str) -> DiskCacheResult<PathBuf> {
        let path = self.inner.format_root.join(identity_digest);
        create_private_dir(&path)?;
        Ok(path)
    }

    fn validate_hit(
        &self,
        record: &RefRecord,
        object_dir: &Path,
        lease: Arc<Lease>,
        expected_schema: Option<&Schema>,
    ) -> DiskCacheResult<DiskCacheHit> {
        let manifest_bytes = fs::read(object_dir.join("manifest.json"))?;
        if sha256_hex(&manifest_bytes) != record.manifest_sha256 {
            return Err(DiskCacheError::Invalid("manifest digest mismatch".into()));
        }
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes)?;
        if manifest.version != FORMAT_VERSION
            || manifest.key_digest != record.key_digest
            || manifest.rows != record.rows
            || manifest.stored_bytes != record.stored_bytes
            || manifest.parts.len() != record.partitions
            || manifest.codec != record.codec
        {
            return Err(DiskCacheError::Invalid("manifest/ref mismatch".into()));
        }
        let mut schema = None;
        let mut stored_bytes = 0_u64;
        let mut rows = 0_u64;
        for (index, part) in manifest.parts.iter().enumerate() {
            if part.file != part_name(index) {
                return Err(DiskCacheError::Invalid("unsafe partition filename".into()));
            }
            let path = object_dir.join(&part.file);
            let metadata = fs::metadata(&path)?;
            if !metadata.is_file()
                || metadata.len() != part.bytes
                || sha256_file(&path)? != part.sha256
            {
                return Err(DiskCacheError::Invalid("partition digest mismatch".into()));
            }
            let reader = FileReader::try_new_buffered(File::open(&path)?, None)?;
            let part_schema = reader.schema();
            if schema_hash(part_schema.as_ref()) != manifest.schema_sha256 {
                return Err(DiskCacheError::Invalid("partition schema mismatch".into()));
            }
            if let Some(expected) = &schema {
                if expected != &part_schema {
                    return Err(DiskCacheError::Invalid(
                        "partition schemas are inconsistent".into(),
                    ));
                }
            } else {
                schema = Some(part_schema);
            }
            stored_bytes = stored_bytes.saturating_add(part.bytes);
            rows = rows.saturating_add(part.rows);
        }
        if stored_bytes != manifest.stored_bytes || rows != manifest.rows {
            return Err(DiskCacheError::Invalid("partition totals mismatch".into()));
        }
        let schema =
            schema.ok_or_else(|| DiskCacheError::Invalid("entry has no partitions".into()))?;
        if expected_schema.is_some_and(|expected| expected != schema.as_ref()) {
            return Err(DiskCacheError::Invalid(
                "cached schema differs from the expected output schema".into(),
            ));
        }
        Ok(DiskCacheHit {
            schema,
            object_dir: object_dir.to_path_buf(),
            manifest,
            record: record.clone(),
            lease,
        })
    }

    fn acquire_lease(&self, object_dir: &Path) -> DiskCacheResult<Arc<Lease>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(object_dir.join(".lease"))?;
        file.lock_shared()?;
        *self
            .inner
            .leases
            .lock()
            .unwrap()
            .entry(object_dir.to_path_buf())
            .or_default() += 1;
        Ok(Arc::new(Lease {
            object_dir: object_dir.to_path_buf(),
            file,
            inner: Arc::clone(&self.inner),
        }))
    }

    fn remove_record(&self, ref_path: &Path, record: &RefRecord) -> DiskCacheResult<()> {
        remove_file_synced(ref_path)?;
        self.inner.access.lock().unwrap().remove(ref_path);
        let object = self
            .inner
            .format_root
            .join(&record.identity_digest)
            .join("objects")
            .join(&record.generation);
        self.remove_object_if_unleased(&object)
    }

    fn remove_record_if_current(
        &self,
        ref_path: &Path,
        expected: &RefRecord,
    ) -> DiskCacheResult<bool> {
        match read_ref(ref_path) {
            Ok(current) if &current == expected => {
                self.remove_record(ref_path, expected)?;
                Ok(true)
            }
            Ok(_) => Ok(false),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(_) => {
                // A corrupt replacement is not the generation this lookup
                // observed. Leave it for its own lookup or the reaper.
                Ok(false)
            }
        }
    }

    fn remove_object_if_unleased(&self, object: &Path) -> DiskCacheResult<()> {
        if self.inner.leases.lock().unwrap().contains_key(object) {
            return Ok(());
        }
        let lease_path = object.join(".lease");
        let lease = match OpenOptions::new().read(true).write(true).open(&lease_path) {
            Ok(file) => Some(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(lease) = &lease {
            match lease.try_lock() {
                Ok(()) => {}
                Err(fs::TryLockError::WouldBlock) => return Ok(()),
                Err(fs::TryLockError::Error(error)) => return Err(error.into()),
            }
        }
        let result = remove_dir_all_synced(object)
            .map(|_| ())
            .map_err(Into::into);
        if let Some(lease) = lease {
            let _ = lease.unlock();
        }
        result
    }

    fn refs(&self) -> DiskCacheResult<Vec<(PathBuf, RefRecord)>> {
        let mut refs = Vec::new();
        for shard in read_dirs(&self.inner.format_root)? {
            if shard.file_name().and_then(|n| n.to_str()) == Some("tmp") {
                continue;
            }
            let refs_dir = shard.join("refs");
            if !refs_dir.is_dir() {
                continue;
            }
            for path in read_files(&refs_dir)? {
                if path.extension().and_then(|ext| ext.to_str()) != Some("ref") {
                    continue;
                }
                if let Ok(record) = read_ref(&path) {
                    refs.push((path, record));
                }
            }
        }
        Ok(refs)
    }

    fn remove_matching_refs(
        &self,
        predicate: impl Fn(&Path, &RefRecord) -> bool,
    ) -> DiskCacheResult<usize> {
        let mut removed = 0;
        for (path, record) in self.refs()? {
            if predicate(&path, &record) {
                self.remove_record(&path, &record)?;
                removed += 1;
            }
        }
        Ok(removed)
    }

    fn remove_expired(&self) -> DiskCacheResult<usize> {
        let now = epoch_ms(SystemTime::now());
        let mut removed = 0;
        for (path, record) in self.refs()? {
            if record.expires_ms <= now && !record.revalidatable {
                self.remove_record(&path, &record)?;
                removed += 1;
            }
        }
        self.inner.stats.lock().unwrap().evictions_ttl += removed as u64;
        Ok(removed)
    }

    fn remove_invalid_refs(&self) -> DiskCacheResult<usize> {
        let mut removed = 0;
        for path in self.ref_paths()? {
            let valid = read_ref(&path).is_ok_and(|record| {
                let expected_name = format!("{}.ref", record.key_digest);
                let shard = path
                    .parent()
                    .and_then(Path::parent)
                    .and_then(Path::file_name)
                    .and_then(|name| name.to_str());
                let object = self
                    .inner
                    .format_root
                    .join(&record.identity_digest)
                    .join("objects")
                    .join(&record.generation);
                record.version == FORMAT_VERSION
                    && path.file_name().and_then(|name| name.to_str())
                        == Some(expected_name.as_str())
                    && shard == Some(record.identity_digest.as_str())
                    && object.join(".lease").is_file()
                    && object.join("manifest.json").is_file()
            });
            if !valid && remove_file_synced(&path)? {
                removed += 1;
            }
        }
        if removed > 0 {
            self.inner.stats.lock().unwrap().corruptions += removed as u64;
        }
        Ok(removed)
    }

    fn make_room(&self, incoming: u64, replacing: Option<&Path>) -> DiskCacheResult<bool> {
        self.remove_expired()?;
        loop {
            let refs = self.refs()?;
            let referenced_bytes: u64 = refs
                .iter()
                .filter(|(path, _)| replacing != Some(path.as_path()))
                .map(|(_, record)| record.stored_bytes)
                .sum();
            let referenced_entries = refs
                .iter()
                .filter(|(path, _)| replacing != Some(path.as_path()))
                .count();
            let (orphan_entries, orphan_bytes) = self.orphan_usage(&refs)?;
            let (retained_entries, retained_bytes) = match replacing.and_then(|replacing| {
                refs.iter()
                    .find(|(path, _)| path == replacing)
                    .map(|(_, record)| record)
            }) {
                Some(record) if self.replacement_will_be_retained(record)? => {
                    (1_usize, record.stored_bytes)
                }
                _ => (0, 0),
            };
            if referenced_bytes
                .saturating_add(orphan_bytes)
                .saturating_add(retained_bytes)
                .saturating_add(incoming)
                <= self.inner.options.max_bytes
                && referenced_entries
                    .saturating_add(orphan_entries)
                    .saturating_add(retained_entries)
                    .saturating_add(usize::from(incoming > 0))
                    <= self.inner.options.max_entries
            {
                return Ok(true);
            }
            let access = self.inner.access.lock().unwrap();
            let victim = refs
                .iter()
                .filter(|(path, _)| replacing != Some(path.as_path()))
                .min_by_key(|(path, record)| access.get(path).copied().unwrap_or(record.created_ms))
                .cloned();
            drop(access);
            let Some((path, record)) = victim else {
                return Ok(false);
            };
            self.remove_record(&path, &record)?;
            self.inner.stats.lock().unwrap().evictions_lru += 1;
        }
    }

    fn replacement_will_be_retained(&self, record: &RefRecord) -> DiskCacheResult<bool> {
        let object = self
            .inner
            .format_root
            .join(&record.identity_digest)
            .join("objects")
            .join(&record.generation);
        if self.inner.leases.lock().unwrap().contains_key(&object) {
            return Ok(true);
        }
        let lease = match OpenOptions::new()
            .read(true)
            .write(true)
            .open(object.join(".lease"))
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        match lease.try_lock() {
            Ok(()) => {
                lease.unlock()?;
                Ok(false)
            }
            Err(fs::TryLockError::WouldBlock) => Ok(true),
            Err(fs::TryLockError::Error(error)) => Err(error.into()),
        }
    }

    fn orphan_usage(&self, refs: &[(PathBuf, RefRecord)]) -> DiskCacheResult<(usize, u64)> {
        let referenced = refs
            .iter()
            .map(|(_, record)| {
                self.inner
                    .format_root
                    .join(&record.identity_digest)
                    .join("objects")
                    .join(&record.generation)
            })
            .collect::<HashSet<_>>();
        let mut entries = 0;
        let mut bytes = 0_u64;
        for shard in read_dirs(&self.inner.format_root)? {
            for object in read_dirs(&shard.join("objects"))? {
                if referenced.contains(&object) {
                    continue;
                }
                entries += 1;
                for file in read_files(&object)? {
                    if file.extension().and_then(|ext| ext.to_str()) == Some("arrow") {
                        bytes = bytes.saturating_add(fs::metadata(file)?.len());
                    }
                }
            }
        }
        Ok((entries, bytes))
    }

    fn remove_old_temps(&self) -> DiskCacheResult<()> {
        let active = self.inner.active_temps.lock().unwrap().clone();
        let now = SystemTime::now();
        for temp in read_dirs(&self.inner.format_root.join("tmp"))? {
            if active.contains(&temp) {
                continue;
            }
            let lease = match OpenOptions::new()
                .read(true)
                .write(true)
                .open(temp.join(".lease"))
            {
                Ok(file) => Some(file),
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            if let Some(lease) = &lease {
                match lease.try_lock() {
                    Ok(()) => {}
                    Err(fs::TryLockError::WouldBlock) => continue,
                    Err(fs::TryLockError::Error(error)) => return Err(error.into()),
                }
            }
            let modified = fs::metadata(&temp)?.modified().unwrap_or(UNIX_EPOCH);
            if now.duration_since(modified).unwrap_or_default() >= TEMP_GRACE {
                remove_dir_all_synced(&temp)?;
            }
            if let Some(lease) = lease {
                let _ = lease.unlock();
            }
        }
        Ok(())
    }

    fn remove_orphan_objects(&self) -> DiskCacheResult<()> {
        let referenced: HashSet<PathBuf> = self
            .refs()?
            .into_iter()
            .map(|(_, record)| {
                self.inner
                    .format_root
                    .join(record.identity_digest)
                    .join("objects")
                    .join(record.generation)
            })
            .collect();
        for shard in read_dirs(&self.inner.format_root)? {
            let objects = shard.join("objects");
            if !objects.is_dir() {
                continue;
            }
            for object in read_dirs(&objects)? {
                if !referenced.contains(&object) {
                    self.remove_object_if_unleased(&object)?;
                }
            }
        }
        Ok(())
    }

    fn reconcile_on_open(&self) -> DiskCacheResult<()> {
        let _operation = self.inner.operations.lock().unwrap();
        let _root_operation = self.lock_root_exclusive()?;
        self.remove_invalid_refs()?;
        self.remove_expired()?;
        self.remove_old_temps()?;
        self.remove_orphan_objects()?;
        let _ = self.make_room(0, None)?;
        self.refresh_current_stats()
    }

    fn current_size_unlocked(&self) -> DiskCacheResult<(usize, u64)> {
        let refs = self.refs()?;
        Ok((refs.len(), refs.iter().map(|(_, r)| r.stored_bytes).sum()))
    }

    fn ref_paths(&self) -> DiskCacheResult<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for shard in read_dirs(&self.inner.format_root)? {
            if shard.file_name().and_then(|n| n.to_str()) == Some("tmp") {
                continue;
            }
            let refs_dir = shard.join("refs");
            if refs_dir.is_dir() {
                paths.extend(
                    read_files(&refs_dir)?.into_iter().filter(|path| {
                        path.extension().and_then(|ext| ext.to_str()) == Some("ref")
                    }),
                );
            }
        }
        Ok(paths)
    }

    fn refresh_current_stats(&self) -> DiskCacheResult<()> {
        let (entries, total_bytes) = self.current_size_unlocked()?;
        let mut stats = self.inner.stats.lock().unwrap();
        stats.entries = entries;
        stats.total_bytes = total_bytes;
        Ok(())
    }

    fn lock_root_shared(&self) -> DiskCacheResult<RootOperation<'_>> {
        self.inner.operation_file.lock_shared()?;
        Ok(RootOperation {
            file: &self.inner.operation_file,
        })
    }

    fn lock_root_exclusive(&self) -> DiskCacheResult<RootOperation<'_>> {
        self.inner.operation_file.lock()?;
        Ok(RootOperation {
            file: &self.inner.operation_file,
        })
    }
}

impl DiskCapture {
    /// Write one batch to its declared producer partition.
    pub fn push_batch(&mut self, partition: usize, batch: &RecordBatch) -> DiskCacheResult<()> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(DiskCacheError::Invalid(
                "batch schema differs from the declared capture schema".into(),
            ));
        }
        let writer = self
            .writers
            .get_mut(partition)
            .and_then(Option::as_mut)
            .ok_or_else(|| DiskCacheError::Invalid(format!("invalid partition {partition}")))?;
        writer.write(batch)?;
        self.batch_counts[partition] += 1;
        self.rows = self.rows.saturating_add(batch.num_rows() as u64);
        let written: u64 = self
            .writers
            .iter()
            .filter_map(Option::as_ref)
            .filter_map(|writer| writer.get_ref().metadata().ok())
            .map(|metadata| metadata.len())
            .sum();
        if written > self.max_bytes {
            if !self.limit_exceeded {
                self.active_temps.stats.lock().unwrap().refusals += 1;
                self.limit_exceeded = true;
            }
            return Err(DiskCacheError::EntryTooLarge {
                bytes: written,
                limit: self.max_bytes,
            });
        }
        Ok(())
    }

    /// Abort and eagerly remove an unfinished capture. Drop has the same
    /// cleanup guarantee and is safe after this method.
    pub fn abort(mut self) -> DiskCacheResult<()> {
        self.remove_temp(true)
    }

    fn finish_parts(&mut self) -> DiskCacheResult<Vec<PartRecord>> {
        let temp = self.temp_dir.as_ref().unwrap();
        let mut parts = Vec::with_capacity(self.writers.len());
        for (partition, writer) in self.writers.iter_mut().enumerate() {
            let writer = writer
                .take()
                .ok_or_else(|| DiskCacheError::Invalid("capture already finished".into()))?;
            let file = writer.into_inner()?;
            file.sync_all()?;
            let path = temp.join(part_name(partition));
            let bytes = fs::metadata(&path)?.len();
            parts.push(PartRecord {
                file: part_name(partition),
                bytes,
                rows: 0,
                batches: self.batch_counts[partition],
                sha256: sha256_file(&path)?,
            });
        }
        // Row totals are stored at entry level. Compute per-partition rows by
        // reading file metadata without retaining any batches in memory.
        for (index, part) in parts.iter_mut().enumerate() {
            let reader =
                FileReader::try_new_buffered(File::open(temp.join(part_name(index)))?, None)?;
            part.rows = reader
                .map(|batch| batch.map(|b| b.num_rows() as u64))
                .try_fold(0_u64, |sum, rows| rows.map(|rows| sum.saturating_add(rows)))?;
        }
        Ok(parts)
    }

    fn remove_temp(&mut self, count_abort: bool) -> DiskCacheResult<()> {
        self.writers.clear();
        // Serialize the unlock-and-remove transition with cross-process
        // reaping. A reaper therefore cannot acquire the exclusive temp lease
        // and race this removal after the capture releases its shared lease.
        let _operation = self.active_temps.operations.lock().unwrap();
        self.active_temps.operation_file.lock()?;
        let _root_operation = RootOperation {
            file: &self.active_temps.operation_file,
        };
        if let Some(lease) = self.temp_lease.take() {
            let _ = lease.unlock();
        }
        if let Some(temp) = self.temp_dir.take() {
            self.active_temps.active_temps.lock().unwrap().remove(&temp);
            match fs::remove_dir_all(temp) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            if count_abort {
                self.active_temps.stats.lock().unwrap().capture_aborts += 1;
            }
        }
        Ok(())
    }

    fn disarm_temp(&mut self) {
        if let Some(lease) = self.temp_lease.take() {
            let _ = lease.unlock();
        }
        if let Some(temp) = self.temp_dir.take() {
            self.active_temps.active_temps.lock().unwrap().remove(&temp);
        }
    }
}

impl Drop for DiskCapture {
    fn drop(&mut self) {
        if self.temp_dir.is_some() {
            let _ = self.remove_temp(true);
        }
    }
}

impl DiskCacheHit {
    /// Schema validated across every partition before this hit was returned.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Number of physical producer partitions.
    pub fn partitions(&self) -> usize {
        self.manifest.parts.len()
    }

    /// Rows across all partitions.
    pub fn rows(&self) -> u64 {
        self.manifest.rows
    }

    /// Encoded Arrow bytes across all partitions.
    pub fn stored_bytes(&self) -> u64 {
        self.manifest.stored_bytes
    }

    /// Opaque ETag to send on a conditional request.
    pub fn etag(&self) -> Option<&str> {
        self.record.etag.as_deref()
    }

    /// Opaque Last-Modified validator to send when no ETag is available.
    pub fn last_modified(&self) -> Option<&str> {
        self.record.last_modified.as_deref()
    }

    /// Whether the stored worker policy permits conditional validation.
    pub fn revalidatable(&self) -> bool {
        self.record.revalidatable
    }

    /// Whether worker policy permits replay after a failed conditional request.
    pub fn may_serve_on_error_at(&self, now: SystemTime) -> bool {
        let Some(grace_seconds) = self.record.stale_if_error_seconds else {
            return false;
        };
        let stale_age_ms = epoch_ms(now).saturating_sub(self.record.expires_ms);
        stale_age_ms <= grace_seconds.saturating_mul(1_000)
            && self.record.expires_ms <= epoch_ms(now)
    }

    /// Open one partition for bounded streaming replay.
    pub fn open_partition(&self, partition: usize) -> DiskCacheResult<DiskPartitionReader> {
        let part = self
            .manifest
            .parts
            .get(partition)
            .ok_or_else(|| DiskCacheError::Invalid(format!("invalid partition {partition}")))?;
        let reader =
            FileReader::try_new_buffered(File::open(self.object_dir.join(&part.file))?, None)?;
        Ok(DiskPartitionReader {
            reader,
            _lease: Arc::clone(&self.lease),
        })
    }
}

fn valid_revalidation_control(control: &CacheControl) -> bool {
    control.revalidatable && (control.etag.is_some() || control.last_modified.is_some())
}

fn ipc_options(codec: DiskCacheCodec) -> DiskCacheResult<IpcWriteOptions> {
    let options = match codec {
        DiskCacheCodec::None => IpcWriteOptions::default(),
        DiskCacheCodec::Zstd => IpcWriteOptions::default()
            .try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))?
            .try_with_compression_level(Some(1))?,
        DiskCacheCodec::Lz4 => IpcWriteOptions::default()
            .try_with_compression(Some(arrow_ipc::CompressionType::LZ4_FRAME))?,
    };
    Ok(options)
}

fn schema_hash(schema: &Schema) -> String {
    let mut dictionaries = DictionaryTracker::new(true);
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut dictionaries,
        &IpcWriteOptions::default(),
    );
    sha256_hex(&encoded.ipc_message)
}

fn part_name(partition: usize) -> String {
    format!("part-{partition:05}.arrow")
}

fn epoch_ms(time: SystemTime) -> u64 {
    u64::try_from(
        time.duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn sha256_file(path: &Path) -> io::Result<String> {
    let mut file = BufReader::new(File::open(path)?);
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex(&digest.finalize()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

fn safe_component(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn read_ref(path: &Path) -> io::Result<RefRecord> {
    let bytes = fs::read(path)?;
    parse_ref(&bytes)
}

fn parse_ref(bytes: &[u8]) -> io::Result<RefRecord> {
    let record: RefRecord = serde_json::from_slice(bytes)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if !safe_component(&record.key_digest)
        || !safe_component(&record.identity_digest)
        || !safe_component(&record.generation)
        || !safe_component(&record.manifest_sha256)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "disk cache ref contains an unsafe digest",
        ));
    }
    Ok(record)
}

fn read_dirs(path: &Path) -> io::Result<Vec<PathBuf>> {
    match fs::read_dir(path) {
        Ok(entries) => collect_classified_paths(entries.map(|entry| {
            let entry = entry?;
            let is_match = entry.file_type()?.is_dir();
            Ok((entry.path(), is_match))
        })),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

fn read_files(path: &Path) -> io::Result<Vec<PathBuf>> {
    match fs::read_dir(path) {
        Ok(entries) => collect_classified_paths(entries.map(|entry| {
            let entry = entry?;
            let is_match = entry.file_type()?.is_file();
            Ok((entry.path(), is_match))
        })),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}

fn collect_classified_paths(
    paths: impl IntoIterator<Item = io::Result<(PathBuf, bool)>>,
) -> io::Result<Vec<PathBuf>> {
    let mut matching = Vec::new();
    for path in paths {
        let (path, is_match) = path?;
        if is_match {
            matching.push(path);
        }
    }
    Ok(matching)
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = create_private_file_new(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn write_replace_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "ref path has no parent"))?;
    for _ in 0..4 {
        let mut random = [0_u8; 16];
        getrandom::fill(&mut random)
            .map_err(|error| io::Error::other(format!("cannot create ref generation: {error}")))?;
        let temp = parent.join(format!(
            ".{}.tmp-{}",
            path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("ref"),
            hex(&random)
        ));
        match write_new_synced(&temp, bytes) {
            Ok(()) => {
                let result = fs::rename(&temp, path);
                if result.is_err() {
                    let _ = fs::remove_file(&temp);
                } else {
                    sync_dir(parent)?;
                }
                return result;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique temporary ref",
    ))
}

fn remove_file_synced(path: &Path) -> io::Result<bool> {
    remove_file_synced_with(path, sync_dir)
}

fn remove_file_synced_with(
    path: &Path,
    mut sync_parent: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<bool> {
    match fs::remove_file(path) {
        Ok(()) => {
            sync_parent(created_directory_parent(path))?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            sync_parent(created_directory_parent(path))?;
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn remove_dir_all_synced(path: &Path) -> io::Result<bool> {
    remove_dir_all_synced_with(path, sync_dir)
}

fn remove_dir_all_synced_with(
    path: &Path,
    mut sync_parent: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<bool> {
    match fs::remove_dir_all(path) {
        Ok(()) => {
            sync_parent(created_directory_parent(path))?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            sync_parent(created_directory_parent(path))?;
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn load_or_create_secret(format_root: &Path) -> io::Result<[u8; 32]> {
    let path = format_root.join(SECRET_FILE);
    for stale in read_files(format_root)? {
        if stale
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(".secret.tmp-"))
        {
            remove_file_synced(&stale)?;
        }
    }
    match fs::read(&path) {
        Ok(bytes) => {
            set_private_file(&path)?;
            return bytes.try_into().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "disk cache secret is not 32 bytes",
                )
            });
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut secret = [0_u8; 32];
    getrandom::fill(&mut secret)
        .map_err(|error| io::Error::other(format!("cannot create cache secret: {error}")))?;
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)
        .map_err(|error| io::Error::other(format!("cannot create secret generation: {error}")))?;
    let temp = format_root.join(format!(".secret.tmp-{}", hex(&random)));
    write_new_synced(&temp, &secret)?;
    if let Err(error) = fs::rename(&temp, &path) {
        let _ = remove_file_synced(&temp);
        return Err(error);
    }
    sync_dir(format_root)?;
    set_private_file(&path)?;
    Ok(secret)
}

fn create_private_dir(path: &Path) -> io::Result<()> {
    create_private_dir_observed(path, |_, _| Ok(()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectoryCreateStep {
    Created,
    Private,
    ParentSynced,
}

fn create_private_dir_observed(
    path: &Path,
    mut observe: impl FnMut(&Path, DirectoryCreateStep) -> io::Result<()>,
) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "private cache path exists but is not a directory",
                ));
            }
            set_private_dir(path)?;
            sync_created_directory_parent(path)?;
            observe(
                created_directory_parent(path),
                DirectoryCreateStep::ParentSynced,
            )?;
            return Ok(());
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    let mut missing = Vec::new();
    let mut cursor = path.to_path_buf();
    loop {
        if cursor.as_os_str().is_empty() {
            cursor = PathBuf::from(".");
        }
        match fs::symlink_metadata(&cursor) {
            Ok(metadata) if metadata.file_type().is_dir() => break,
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "private cache ancestor is not a directory",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(cursor.clone());
                let Some(parent) = cursor.parent() else {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "private cache path has no existing ancestor",
                    ));
                };
                cursor = parent.to_path_buf();
            }
            Err(error) => return Err(error),
        }
    }

    // The closest existing component may itself have been created by a prior
    // attempt whose parent fsync failed. Resync its parent before descending;
    // this is harmless for an old ancestor and closes that retry window.
    sync_created_directory_parent(&cursor)?;
    observe(
        created_directory_parent(&cursor),
        DirectoryCreateStep::ParentSynced,
    )?;

    for directory in missing.into_iter().rev() {
        match create_one_private_dir(&directory) {
            Ok(()) => {
                observe(&directory, DirectoryCreateStep::Created)?;
                set_private_dir(&directory)?;
                observe(&directory, DirectoryCreateStep::Private)?;
                sync_created_directory_parent(&directory)?;
                observe(
                    created_directory_parent(&directory),
                    DirectoryCreateStep::ParentSynced,
                )?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&directory)?;
                if !metadata.file_type().is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "raced cache path creation produced a non-directory",
                    ));
                }
                set_private_dir(&directory)?;
                sync_created_directory_parent(&directory)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn create_one_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700).create(path)
}

#[cfg(not(unix))]
fn create_one_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir(path)
}

#[cfg(unix)]
fn sync_created_directory_parent(path: &Path) -> io::Result<()> {
    sync_dir(created_directory_parent(path))
}

fn created_directory_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

#[cfg(not(unix))]
fn sync_created_directory_parent(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn create_private_file(path: &Path) -> io::Result<File> {
    let file = open_private(path, false)?;
    set_private_file(path)?;
    Ok(file)
}

fn create_private_file_new(path: &Path) -> io::Result<File> {
    let file = open_private(path, true)?;
    set_private_file(path)?;
    Ok(file)
}

fn open_private_shared(path: &Path) -> io::Result<File> {
    let file = open_private_existing_or_create(path)?;
    set_private_file(path)?;
    Ok(file)
}

#[cfg(unix)]
fn open_private_existing_or_create(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .read(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_private_existing_or_create(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .read(true)
        .open(path)
}

#[cfg(unix)]
fn open_private(path: &Path, create_new: bool) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .create(!create_new)
        .create_new(create_new)
        .truncate(!create_new)
        .write(true)
        .read(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn open_private(path: &Path, create_new: bool) -> io::Result<File> {
    OpenOptions::new()
        .create(!create_new)
        .create_new(create_new)
        .truncate(!create_new)
        .write(true)
        .read(true)
        .open(path)
}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_private_file(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_file(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    use std::process::{Child, Command};
    use std::sync::atomic::{AtomicU64, Ordering};

    use arrow_array::{ArrayRef, Int64Array};
    use arrow_schema::{DataType, Field};

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(name: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let path = std::env::temp_dir().join(format!(
                "vgi-disk-cache-{name}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![Arc::new(Int64Array::from(values.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    fn key(scope: &str, function: &str) -> CacheKey {
        CacheKey {
            catalog: "cat".into(),
            identity_scope: scope.into(),
            worker_label: "worker".into(),
            function: function.into(),
            arguments: vec![1, 2, 3],
            projection: None,
            filters: None,
            catalog_version: 7,
            at: None,
            settings: vec![],
            attach_options: vec![],
            row_limit: None,
            ordering: None,
            sample: None,
            plan: None,
        }
    }

    fn open_cache(root: &TestRoot) -> DiskCache {
        let mut options = DiskCacheOptions::new(&root.0);
        options.codec = DiskCacheCodec::None;
        DiskCache::open(options).unwrap()
    }

    const SUBPROCESS_ROOT: &str = "VGI_DISK_CACHE_TEST_ROOT";
    const SUBPROCESS_OUTPUT: &str = "VGI_DISK_CACHE_TEST_OUTPUT";
    const SUBPROCESS_MODE: &str = "VGI_DISK_CACHE_TEST_MODE";

    fn spawn_cache_child(root: &Path, output: &Path, mode: &str) -> Child {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "disk_cache::tests::subprocess_cache_open_helper",
                "--nocapture",
            ])
            .env(SUBPROCESS_ROOT, root)
            .env(SUBPROCESS_OUTPUT, output)
            .env(SUBPROCESS_MODE, mode)
            .spawn()
            .unwrap()
    }

    fn store(cache: &DiskCache, key: CacheKey, parts: &[&[i64]], ttl: Duration) -> DiskCacheCommit {
        let mut capture = cache.begin_capture(schema(), parts.len()).unwrap();
        for (partition, values) in parts.iter().enumerate() {
            if !values.is_empty() {
                capture.push_batch(partition, &batch(values)).unwrap();
            }
        }
        cache
            .commit(key, capture, ttl, Some(&CacheControl::ttl(60)))
            .unwrap()
    }

    fn count_object_dirs(root: &Path) -> usize {
        read_dirs(&root.join(FORMAT_DIR))
            .unwrap()
            .into_iter()
            .flat_map(|shard| read_dirs(&shard.join("objects")).unwrap())
            .count()
    }

    fn arrow_files(root: &Path) -> Vec<PathBuf> {
        read_dirs(&root.join(FORMAT_DIR))
            .unwrap()
            .into_iter()
            .flat_map(|shard| read_dirs(&shard.join("objects")).unwrap())
            .flat_map(|object| read_files(&object).unwrap())
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("arrow"))
            .collect()
    }

    #[test]
    fn persists_and_replays_partitioned_results_after_reopen() {
        let root = TestRoot::new("reopen");
        let cache = open_cache(&root);
        let cache_key = key("alice", "f");
        let outcome = store(
            &cache,
            cache_key.clone(),
            &[&[1, 2, 3], &[]],
            Duration::from_secs(60),
        );
        assert_eq!(
            outcome,
            DiskCacheCommit::Stored {
                stored_bytes: match outcome {
                    DiskCacheCommit::Stored { stored_bytes, .. } => stored_bytes,
                    _ => unreachable!(),
                },
                rows: 3,
                partitions: 2,
            }
        );
        drop(cache);

        let reopened = open_cache(&root);
        let hit = reopened
            .lookup(&cache_key)
            .unwrap()
            .expect("persistent hit");
        assert_eq!(hit.partitions(), 2);
        assert_eq!(hit.rows(), 3);
        let rows: usize = hit
            .open_partition(0)
            .unwrap()
            .map(|batch| batch.unwrap().num_rows())
            .sum();
        assert_eq!(rows, 3);
        assert_eq!(hit.open_partition(1).unwrap().count(), 0);
        let [entry] = reopened.entries().unwrap().try_into().unwrap();
        assert_eq!(entry.batches, 1);
        assert_eq!(entry.at, None);
    }

    #[test]
    fn commits_an_all_empty_result_from_its_declared_schema() {
        let root = TestRoot::new("empty");
        let cache = open_cache(&root);
        let cache_key = key("alice", "empty");
        let capture = cache.begin_capture(schema(), 2).unwrap();
        let outcome = cache
            .commit(
                cache_key.clone(),
                capture,
                Duration::from_secs(60),
                Some(&CacheControl::ttl(60)),
            )
            .unwrap();
        assert!(outcome.is_stored());
        let hit = cache.lookup(&cache_key).unwrap().unwrap();
        assert_eq!(hit.rows(), 0);
        assert_eq!(hit.partitions(), 2);
        assert_eq!(hit.schema().as_ref(), schema().as_ref());
        assert_eq!(hit.open_partition(0).unwrap().count(), 0);
        assert_eq!(hit.open_partition(1).unwrap().count(), 0);
    }

    #[test]
    fn validates_each_pushed_batch_schema() {
        let root = TestRoot::new("schema");
        let cache = open_cache(&root);
        let mut capture = cache.begin_capture(schema(), 1).unwrap();
        let other = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "other",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        assert!(matches!(
            capture.push_batch(0, &other),
            Err(DiskCacheError::Invalid(_))
        ));
    }

    #[test]
    fn expected_schema_mismatch_is_a_clean_conditional_miss() {
        let root = TestRoot::new("expected-schema");
        let cache = open_cache(&root);
        let cache_key = key("alice", "f");
        store(&cache, cache_key.clone(), &[&[1]], Duration::from_secs(60));
        let expected = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));

        assert!(cache
            .lookup_expected_schema(&cache_key, &expected)
            .unwrap()
            .is_none());
        assert!(cache.lookup(&cache_key).unwrap().is_none());
        let stats = cache.stats().unwrap();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.corruptions, 1);
        assert_eq!(stats.entries, 0);
    }

    #[test]
    fn unknown_and_transaction_scopes_fail_closed() {
        let root = TestRoot::new("scope-policy");
        let cache = open_cache(&root);
        for (scope, expected) in [
            ("future-global", DiskCacheSkip::UnsupportedScope),
            (CACHE_SCOPE_TRANSACTION, DiskCacheSkip::TransactionScoped),
        ] {
            let capture = cache.begin_capture(schema(), 1).unwrap();
            let mut control = CacheControl::ttl(60);
            control.scope = scope.into();
            assert_eq!(
                cache
                    .commit(
                        key("alice", scope),
                        capture,
                        Duration::from_secs(60),
                        Some(&control),
                    )
                    .unwrap(),
                DiskCacheCommit::Skipped(expected)
            );
        }
        assert_eq!(cache.stats().unwrap().entries, 0);
    }

    #[test]
    fn publication_failures_roll_back_new_and_replacement_generations() {
        let root = TestRoot::new("rollback");
        let cache = open_cache(&root);
        let first_key = key("alice", "new");
        *cache.inner.commit_fault.lock().unwrap() = Some(CommitFault::AfterObjectRename);
        let capture = cache.begin_capture(schema(), 1).unwrap();
        assert!(cache
            .commit(
                first_key.clone(),
                capture,
                Duration::from_secs(60),
                Some(&CacheControl::ttl(60)),
            )
            .is_err());
        assert!(cache.lookup(&first_key).unwrap().is_none());
        assert_eq!(count_object_dirs(&root.0), 0);

        let replace_key = key("alice", "replace");
        store(
            &cache,
            replace_key.clone(),
            &[&[1]],
            Duration::from_secs(60),
        );
        *cache.inner.commit_fault.lock().unwrap() = Some(CommitFault::AfterRefPublish);
        let mut capture = cache.begin_capture(schema(), 1).unwrap();
        capture.push_batch(0, &batch(&[2])).unwrap();
        assert!(cache
            .commit(
                replace_key.clone(),
                capture,
                Duration::from_secs(60),
                Some(&CacheControl::ttl(60)),
            )
            .is_err());
        let hit = cache.lookup(&replace_key).unwrap().unwrap();
        let values = hit.open_partition(0).unwrap().next().unwrap().unwrap();
        assert_eq!(
            values
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        drop(hit);
        cache.reap().unwrap();
        assert_eq!(count_object_dirs(&root.0), 1);
    }

    #[test]
    fn expiry_after_ref_fsync_rolls_publication_back() {
        use std::thread;

        let root = TestRoot::new("publish-expiry");
        let cache = open_cache(&root);
        let gate = Arc::new(CommitGate::new());
        *cache.inner.commit_gate.lock().unwrap() = Some(Arc::clone(&gate));
        let cache_key = key("alice", "expires-during-publish");
        let capture = cache.begin_capture(schema(), 1).unwrap();
        let freshness = CacheFreshness::from_lifetime(Duration::from_secs(1)).unwrap();
        let publisher = {
            let cache = cache.clone();
            thread::spawn(move || {
                cache.commit_freshness(cache_key, capture, freshness, Some(&CacheControl::ttl(1)))
            })
        };
        gate.wait_until_entered();
        while !freshness.is_expired() {
            thread::yield_now();
        }
        gate.release();
        assert_eq!(
            publisher.join().unwrap().unwrap(),
            DiskCacheCommit::Skipped(DiskCacheSkip::NoFreshness)
        );
        *cache.inner.commit_gate.lock().unwrap() = None;
        let stats = cache.stats().unwrap();
        assert_eq!(stats.inserts, 0);
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(count_object_dirs(&root.0), 0);
        assert!(cache
            .lookup(&key("alice", "expires-during-publish"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn identity_and_time_travel_are_isolated() {
        let root = TestRoot::new("identity");
        let cache = open_cache(&root);
        let alice = key("alice", "f");
        store(&cache, alice.clone(), &[&[1]], Duration::from_secs(60));
        assert!(cache.lookup(&key("bob", "f")).unwrap().is_none());
        let mut at = alice.clone();
        at.at = Some(("version".into(), "42".into()));
        assert!(cache.lookup(&at).unwrap().is_none());
        store(&cache, at.clone(), &[&[42]], Duration::from_secs(60));
        let time_travel = cache
            .entries()
            .unwrap()
            .into_iter()
            .find(|entry| entry.at.is_some())
            .unwrap();
        assert_eq!(time_travel.batches, 1);
        assert_eq!(time_travel.at, at.at);
        assert!(cache.lookup(&alice).unwrap().is_some());
    }

    #[test]
    fn abort_and_drop_remove_temporary_captures() {
        let root = TestRoot::new("abort");
        let cache = open_cache(&root);
        let mut explicit = cache.begin_capture(schema(), 1).unwrap();
        explicit.push_batch(0, &batch(&[1])).unwrap();
        explicit.abort().unwrap();
        {
            let _implicit = cache.begin_capture(schema(), 1).unwrap();
        }
        assert!(read_dirs(&root.0.join(FORMAT_DIR).join("tmp"))
            .unwrap()
            .is_empty());
        assert_eq!(cache.stats().unwrap().capture_aborts, 2);
    }

    #[test]
    fn cross_instance_reap_skips_an_old_but_live_capture_lease() {
        use std::fs::FileTimes;

        let root = TestRoot::new("live-temp");
        let owner = open_cache(&root);
        let reaper = open_cache(&root);
        let capture = owner.begin_capture(schema(), 1).unwrap();
        let live_temp = capture.temp_dir.as_ref().unwrap().clone();
        File::open(&live_temp)
            .unwrap()
            .set_times(FileTimes::new().set_modified(UNIX_EPOCH))
            .unwrap();

        reaper.reap().unwrap();
        assert!(live_temp.is_dir(), "shared lease protects a long capture");
        capture.abort().unwrap();

        let abandoned = owner.inner.format_root.join("tmp").join("abandoned");
        create_private_dir(&abandoned).unwrap();
        write_new_synced(&abandoned.join(".lease"), &[]).unwrap();
        File::open(&abandoned)
            .unwrap()
            .set_times(FileTimes::new().set_modified(UNIX_EPOCH))
            .unwrap();
        reaper.reap().unwrap();
        assert!(!abandoned.exists(), "an unlocked stale temp is reaped");
    }

    #[test]
    fn corrupt_arrow_is_evicted_as_a_clean_miss() {
        let root = TestRoot::new("corrupt");
        let cache = open_cache(&root);
        let cache_key = key("alice", "f");
        store(
            &cache,
            cache_key.clone(),
            &[&[1, 2]],
            Duration::from_secs(60),
        );
        let [path] = arrow_files(&root.0).try_into().expect("one Arrow object");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let offset = file.metadata().unwrap().len() / 2;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&[0xff]).unwrap();
        file.sync_all().unwrap();

        assert!(cache.lookup(&cache_key).unwrap().is_none());
        let stats = cache.stats().unwrap();
        assert_eq!(stats.corruptions, 1);
        assert_eq!(stats.entries, 0);
    }

    #[test]
    fn missing_lease_file_is_a_clean_corruption_miss() {
        let root = TestRoot::new("missing-lease");
        let cache = open_cache(&root);
        let cache_key = key("alice", "f");
        store(&cache, cache_key.clone(), &[&[1]], Duration::from_secs(60));
        let object = read_dirs(&root.0.join(FORMAT_DIR))
            .unwrap()
            .into_iter()
            .flat_map(|shard| read_dirs(&shard.join("objects")).unwrap())
            .next()
            .unwrap();
        fs::remove_file(object.join(".lease")).unwrap();

        assert!(cache.lookup(&cache_key).unwrap().is_none());
        assert_eq!(cache.stats().unwrap().corruptions, 1);
        assert_eq!(cache.stats().unwrap().entries, 0);
    }

    #[test]
    fn concurrent_cache_instances_publish_one_valid_atomic_ref() {
        use std::sync::Barrier;
        use std::thread;

        let root = TestRoot::new("concurrent-ref");
        let first = open_cache(&root);
        let second = open_cache(&root);
        let barrier = Arc::new(Barrier::new(2));
        let cache_key = key("alice", "same");
        let first_key = cache_key.clone();
        let first_barrier = Arc::clone(&barrier);
        let first_thread = thread::spawn(move || {
            first_barrier.wait();
            store(&first, first_key, &[&[1]], Duration::from_secs(60))
        });
        let second_key = cache_key.clone();
        let second_thread = thread::spawn(move || {
            barrier.wait();
            store(&second, second_key, &[&[2]], Duration::from_secs(60))
        });
        assert!(first_thread.join().unwrap().is_stored());
        assert!(second_thread.join().unwrap().is_stored());

        let reopened = open_cache(&root);
        let hit = reopened.lookup(&cache_key).unwrap().expect("atomic ref");
        let values: Vec<i64> = hit
            .open_partition(0)
            .unwrap()
            .flat_map(|batch| {
                let batch = batch.unwrap();
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert!(values == [1] || values == [2]);
        drop(hit);
        reopened.reap().unwrap();
        assert_eq!(count_object_dirs(&root.0), 1);
    }

    #[test]
    fn cross_instance_mutations_preserve_live_refs_and_global_bounds() {
        use std::sync::Barrier;
        use std::thread;

        let root = TestRoot::new("cross-mutations");
        let mut options = DiskCacheOptions::new(&root.0);
        options.codec = DiskCacheCodec::None;
        options.max_entries = 2;
        let replacer = DiskCache::open(options.clone()).unwrap();
        let mutator = DiskCache::open(options).unwrap();
        let live_key = key("alice", "live");
        let churn_key = key("alice", "churn");
        let barrier = Arc::new(Barrier::new(2));

        let replace_barrier = Arc::clone(&barrier);
        let replace_key = live_key.clone();
        let replace_thread = thread::spawn(move || {
            replace_barrier.wait();
            for value in 0..20 {
                assert!(store(
                    &replacer,
                    replace_key.clone(),
                    &[&[value]],
                    Duration::from_secs(60),
                )
                .is_stored());
            }
        });
        let mutate_thread = thread::spawn(move || {
            barrier.wait();
            for value in 0..20 {
                assert!(store(
                    &mutator,
                    churn_key.clone(),
                    &[&[value]],
                    Duration::from_secs(60),
                )
                .is_stored());
                assert!(mutator.remove(&churn_key).unwrap());
                mutator.reap().unwrap();
            }
        });
        replace_thread.join().unwrap();
        mutate_thread.join().unwrap();

        let reopened = DiskCache::open({
            let mut options = DiskCacheOptions::new(&root.0);
            options.codec = DiskCacheCodec::None;
            options.max_entries = 2;
            options
        })
        .unwrap();
        assert!(reopened.lookup(&live_key).unwrap().is_some());
        let stats = reopened.stats().unwrap();
        assert!(stats.entries <= 2);
        assert!(stats.total_bytes <= reopened.inner.options.max_bytes);
        for (_, record) in reopened.refs().unwrap() {
            assert!(reopened
                .inner
                .format_root
                .join(record.identity_digest)
                .join("objects")
                .join(record.generation)
                .is_dir());
        }
    }

    #[test]
    fn count_bound_evicts_the_oldest_entry() {
        let root = TestRoot::new("bounds");
        let mut options = DiskCacheOptions::new(&root.0);
        options.codec = DiskCacheCodec::None;
        options.max_entries = 1;
        let cache = DiskCache::open(options).unwrap();
        let first = key("alice", "first");
        let second = key("alice", "second");
        store(&cache, first.clone(), &[&[1]], Duration::from_secs(60));
        store(&cache, second.clone(), &[&[2]], Duration::from_secs(60));
        assert!(cache.lookup(&first).unwrap().is_none());
        assert!(cache.lookup(&second).unwrap().is_some());
        assert_eq!(cache.stats().unwrap().evictions_lru, 1);
    }

    #[test]
    fn encoded_byte_bound_stops_capture_before_commit() {
        let root = TestRoot::new("byte-bound");
        let mut options = DiskCacheOptions::new(&root.0);
        options.codec = DiskCacheCodec::None;
        options.max_bytes = 1;
        let cache = DiskCache::open(options).unwrap();
        let mut capture = cache.begin_capture(schema(), 1).unwrap();
        assert!(matches!(
            capture.push_batch(0, &batch(&[1, 2, 3])),
            Err(DiskCacheError::EntryTooLarge { limit: 1, .. })
        ));
        drop(capture);
        assert!(read_dirs(&root.0.join(FORMAT_DIR).join("tmp"))
            .unwrap()
            .is_empty());
        let stats = cache.stats().unwrap();
        assert_eq!(stats.refusals, 1);
        assert_eq!(stats.capture_aborts, 1);
        assert_eq!(stats.entries, 0);
    }

    #[test]
    fn reap_removes_a_malformed_ref_without_following_its_contents() {
        let root = TestRoot::new("bad-ref");
        let cache = open_cache(&root);
        let shard = cache.inner.format_root.join(cache.digest(b"scope"));
        let refs = shard.join("refs");
        create_private_dir(&refs).unwrap();
        let path = refs.join(format!("{}.ref", cache.digest(b"key")));
        write_new_synced(&path, br#"{"identity_digest":"../../outside"}"#).unwrap();

        cache.reap().unwrap();
        assert!(!path.exists());
        assert_eq!(cache.stats().unwrap().corruptions, 1);
    }

    #[test]
    fn reap_removes_structurally_inconsistent_refs() {
        let root = TestRoot::new("structural-ref");
        let cache = open_cache(&root);
        let cache_key = key("alice", "f");
        store(&cache, cache_key, &[&[1]], Duration::from_secs(60));
        let [(path, mut record)] = cache.refs().unwrap().try_into().unwrap();
        record.version = FORMAT_VERSION + 1;
        write_replace_synced(&path, &serde_json::to_vec(&record).unwrap()).unwrap();

        cache.reap().unwrap();
        assert!(!path.exists());
        assert_eq!(cache.stats().unwrap().entries, 0);
        assert_eq!(cache.stats().unwrap().corruptions, 1);
    }

    #[test]
    fn concurrent_open_initializes_one_shared_secret() {
        use std::thread;

        let root = TestRoot::new("secret-race");
        let paths = (0..8).map(|_| root.0.clone()).collect::<Vec<_>>();
        let secrets = paths
            .into_iter()
            .map(|path| {
                thread::spawn(move || {
                    let mut options = DiskCacheOptions::new(path);
                    options.codec = DiskCacheCodec::None;
                    DiskCache::open(options).unwrap().inner.secret
                })
            })
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        assert!(secrets.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(
            fs::read(root.0.join(FORMAT_DIR).join(SECRET_FILE))
                .unwrap()
                .len(),
            32
        );
    }

    #[test]
    fn subprocess_cache_open_helper() {
        let (Some(root), Some(output), Some(mode)) = (
            std::env::var_os(SUBPROCESS_ROOT),
            std::env::var_os(SUBPROCESS_OUTPUT),
            std::env::var_os(SUBPROCESS_MODE),
        ) else {
            return;
        };
        let mut options = DiskCacheOptions::new(PathBuf::from(root));
        options.codec = DiskCacheCodec::None;
        let cache = DiskCache::open(options).unwrap();
        if mode == "lookup" {
            let hit = cache
                .lookup(&key("alice", "subprocess"))
                .unwrap()
                .expect("a separately opened process can validate and replay the entry");
            assert_eq!(hit.rows(), 1);
        }
        fs::write(output, cache.digest(b"shared-secret-probe")).unwrap();
    }

    #[test]
    fn simultaneous_first_open_and_reopen_work_across_processes() {
        let root = TestRoot::new("subprocess-secret");
        let format_root = root.0.join(FORMAT_DIR);
        create_private_dir(&format_root).unwrap();
        let stale_initializer = format_root.join(".secret.tmp-dead-initializer");
        write_new_synced(&stale_initializer, b"partial").unwrap();

        let outputs = (0..6)
            .map(|index| root.0.join(format!("child-{index}.fingerprint")))
            .collect::<Vec<_>>();
        let children = outputs
            .iter()
            .map(|output| spawn_cache_child(&root.0, output, "open"))
            .collect::<Vec<_>>();
        for mut child in children {
            assert!(child.wait().unwrap().success());
        }
        let fingerprints = outputs
            .iter()
            .map(fs::read_to_string)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(fingerprints.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(fs::read(format_root.join(SECRET_FILE)).unwrap().len(), 32);
        assert!(!stale_initializer.exists());

        let cache = open_cache(&root);
        store(
            &cache,
            key("alice", "subprocess"),
            &[&[7]],
            Duration::from_secs(60),
        );
        drop(cache);
        let replay_output = root.0.join("reopen.fingerprint");
        let mut replay = spawn_cache_child(&root.0, &replay_output, "lookup");
        assert!(replay.wait().unwrap().success());
        assert_eq!(fs::read_to_string(replay_output).unwrap(), fingerprints[0]);
    }

    #[test]
    fn recursive_private_creation_is_ordered_and_retry_durable() {
        let root = TestRoot::new("mkdir-order");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root.0, fs::Permissions::from_mode(0o755)).unwrap();
        }
        let target = root.0.join("one").join("two");
        let mut events = Vec::new();
        create_private_dir_observed(&target, |path, step| {
            events.push((path.to_path_buf(), step));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            events,
            vec![
                (
                    root.0.parent().unwrap().to_path_buf(),
                    DirectoryCreateStep::ParentSynced,
                ),
                (root.0.join("one"), DirectoryCreateStep::Created),
                (root.0.join("one"), DirectoryCreateStep::Private),
                (root.0.clone(), DirectoryCreateStep::ParentSynced),
                (target.clone(), DirectoryCreateStep::Created),
                (target.clone(), DirectoryCreateStep::Private),
                (root.0.join("one"), DirectoryCreateStep::ParentSynced),
            ]
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&root.0).unwrap().permissions().mode() & 0o777,
                0o755,
                "an existing ancestor is never chmodded"
            );
        }

        let interrupted_parent = root.0.join("interrupted");
        let interrupted = interrupted_parent.join("leaf");
        assert!(create_private_dir_observed(&interrupted, |path, step| {
            if path == interrupted_parent && step == DirectoryCreateStep::Private {
                Err(io::Error::other("injected failure before parent fsync"))
            } else {
                Ok(())
            }
        })
        .is_err());
        let mut retry_events = Vec::new();
        create_private_dir_observed(&interrupted, |path, step| {
            retry_events.push((path.to_path_buf(), step));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            retry_events,
            vec![
                (root.0.clone(), DirectoryCreateStep::ParentSynced),
                (interrupted.clone(), DirectoryCreateStep::Created),
                (interrupted.clone(), DirectoryCreateStep::Private),
                (interrupted_parent, DirectoryCreateStep::ParentSynced),
            ]
        );
    }

    #[test]
    fn deletion_retry_fsyncs_a_parent_after_not_found() {
        let root = TestRoot::new("delete-retry");
        let file = root.0.join("entry.ref");
        fs::write(&file, b"ref").unwrap();
        let mut syncs = 0;
        assert!(remove_file_synced_with(&file, |_| {
            syncs += 1;
            Err(io::Error::other("injected parent fsync failure"))
        })
        .is_err());
        assert!(!file.exists());
        assert!(!remove_file_synced_with(&file, |_| {
            syncs += 1;
            Ok(())
        })
        .unwrap());
        assert_eq!(syncs, 2);

        let directory = root.0.join("object");
        fs::create_dir(&directory).unwrap();
        let mut syncs = 0;
        assert!(remove_dir_all_synced_with(&directory, |_| {
            syncs += 1;
            Err(io::Error::other("injected parent fsync failure"))
        })
        .is_err());
        assert!(!directory.exists());
        assert!(!remove_dir_all_synced_with(&directory, |_| {
            syncs += 1;
            Ok(())
        })
        .unwrap());
        assert_eq!(syncs, 2);
    }

    #[test]
    fn directory_enumeration_propagates_entry_errors() {
        let expected = io::Error::new(io::ErrorKind::PermissionDenied, "injected entry error");
        let result = collect_classified_paths([Ok((PathBuf::from("first"), true)), Err(expected)]);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let root = TestRoot::new("direct-entry-types");
            let direct = root.0.join("direct");
            fs::create_dir(&direct).unwrap();
            symlink(&direct, root.0.join("linked")).unwrap();
            assert_eq!(read_dirs(&root.0).unwrap(), vec![direct]);
        }
    }

    #[test]
    fn active_replay_lease_defers_orphan_removal() {
        let root = TestRoot::new("lease");
        let cache = open_cache(&root);
        let other_process_view = open_cache(&root);
        let cache_key = key("alice", "f");
        store(&cache, cache_key.clone(), &[&[1]], Duration::from_secs(60));
        let old_hit = cache.lookup(&cache_key).unwrap().unwrap();
        store(
            &other_process_view,
            cache_key.clone(),
            &[&[2]],
            Duration::from_secs(60),
        );
        assert_eq!(count_object_dirs(&root.0), 2);
        other_process_view.reap().unwrap();
        assert_eq!(count_object_dirs(&root.0), 2, "lease protects old object");
        drop(old_hit);
        other_process_view.reap().unwrap();
        assert_eq!(count_object_dirs(&root.0), 1);
    }

    #[test]
    fn leased_orphans_count_against_admission_bounds() {
        let root = TestRoot::new("lease-capacity");
        let mut options = DiskCacheOptions::new(&root.0);
        options.codec = DiskCacheCodec::None;
        options.max_entries = 1;
        let cache = DiskCache::open(options).unwrap();
        let old_key = key("alice", "old");
        store(&cache, old_key.clone(), &[&[1]], Duration::from_secs(60));
        let lease = cache.lookup(&old_key).unwrap().unwrap();
        assert!(cache.remove(&old_key).unwrap());

        let capture = cache.begin_capture(schema(), 1).unwrap();
        assert_eq!(
            cache
                .commit(
                    key("alice", "new"),
                    capture,
                    Duration::from_secs(60),
                    Some(&CacheControl::ttl(60)),
                )
                .unwrap(),
            DiskCacheCommit::Skipped(DiskCacheSkip::Capacity)
        );
        drop(lease);
        cache.reap().unwrap();
        assert_eq!(count_object_dirs(&root.0), 0);
    }

    #[test]
    fn a_leased_replacement_generation_is_preaccounted() {
        let root = TestRoot::new("leased-replacement");
        let mut options = DiskCacheOptions::new(&root.0);
        options.codec = DiskCacheCodec::None;
        options.max_entries = 1;
        let cache = DiskCache::open(options).unwrap();
        let cache_key = key("alice", "replace");
        store(&cache, cache_key.clone(), &[&[1]], Duration::from_secs(60));
        let old_hit = cache.lookup(&cache_key).unwrap().unwrap();

        let mut capture = cache.begin_capture(schema(), 1).unwrap();
        capture.push_batch(0, &batch(&[2])).unwrap();
        assert_eq!(
            cache
                .commit(
                    cache_key.clone(),
                    capture,
                    Duration::from_secs(60),
                    Some(&CacheControl::ttl(60)),
                )
                .unwrap(),
            DiskCacheCommit::Skipped(DiskCacheSkip::Capacity)
        );
        assert_eq!(count_object_dirs(&root.0), 1);
        let current = cache.lookup(&cache_key).unwrap().unwrap();
        assert_eq!(current.rows(), 1);
        assert_eq!(cache.stats().unwrap().inserts, 1);
        drop(current);
        drop(old_hit);
    }

    #[test]
    fn a_leased_replacement_generation_counts_against_byte_bounds() {
        let measuring_root = TestRoot::new("measure-replacement");
        let measuring_cache = open_cache(&measuring_root);
        let measured = match store(
            &measuring_cache,
            key("alice", "measure"),
            &[&[1]],
            Duration::from_secs(60),
        ) {
            DiskCacheCommit::Stored { stored_bytes, .. } => stored_bytes,
            other => panic!("expected measured entry to store, got {other:?}"),
        };

        let root = TestRoot::new("leased-replacement-bytes");
        let mut options = DiskCacheOptions::new(&root.0);
        options.codec = DiskCacheCodec::None;
        options.max_bytes = measured;
        let cache = DiskCache::open(options).unwrap();
        let cache_key = key("alice", "replace");
        assert!(store(&cache, cache_key.clone(), &[&[1]], Duration::from_secs(60),).is_stored());
        let old_hit = cache.lookup(&cache_key).unwrap().unwrap();
        let mut capture = cache.begin_capture(schema(), 1).unwrap();
        capture.push_batch(0, &batch(&[2])).unwrap();
        assert_eq!(
            cache
                .commit(
                    cache_key,
                    capture,
                    Duration::from_secs(60),
                    Some(&CacheControl::ttl(60)),
                )
                .unwrap(),
            DiskCacheCommit::Skipped(DiskCacheSkip::Capacity)
        );
        assert_eq!(count_object_dirs(&root.0), 1);
        drop(old_hit);
    }

    #[test]
    fn immediately_stale_validated_results_persist_for_revalidation() {
        let root = TestRoot::new("stale");
        let cache = open_cache(&root);
        let mut capture = cache.begin_capture(schema(), 2).unwrap();
        capture.push_batch(0, &batch(&[7])).unwrap();
        let control = CacheControl {
            ttl_seconds: Some(0),
            etag: Some("opaque-etag".into()),
            revalidatable: true,
            stale_if_error: Some(60),
            ..Default::default()
        };
        let cache_key = key("alice", "f");
        assert!(cache
            .commit(cache_key.clone(), capture, Duration::ZERO, Some(&control))
            .unwrap()
            .is_stored());
        drop(cache);

        let reopened = open_cache(&root);
        assert!(reopened.lookup(&cache_key).unwrap().is_none());
        let stale = reopened
            .lookup_for_revalidation_expected_schema(&cache_key, &schema())
            .unwrap()
            .expect("validator survives restart");
        assert_eq!(stale.etag(), Some("opaque-etag"));
        assert_eq!(stale.last_modified(), None);
        assert!(stale.revalidatable());
        assert!(stale.may_serve_on_error_at(SystemTime::now()));
        assert_eq!(stale.partitions(), 2);
        let [entry] = reopened.entries().unwrap().try_into().unwrap();
        assert!(entry.has_etag);
        assert!(!entry.has_last_modified);
        assert!(entry.revalidatable);

        let refreshed = CacheFreshness::from_lifetime(Duration::ZERO).unwrap();
        let rotated = CacheControl::ttl(0)
            .with_etag("rotated-etag")
            .with_revalidatable();
        assert!(reopened
            .revalidate_freshness(&stale, refreshed, &rotated)
            .unwrap());
        drop(stale);
        assert!(reopened.lookup(&cache_key).unwrap().is_none());
        let refreshed = reopened
            .lookup_for_revalidation_expected_schema(&cache_key, &schema())
            .unwrap()
            .unwrap();
        assert_eq!(refreshed.etag(), Some("rotated-etag"));
        assert!(
            !refreshed.may_serve_on_error_at(SystemTime::now()),
            "omitting stale-if-error on not_modified withdraws the old grace"
        );
        assert_eq!(reopened.stats().unwrap().revalidations, 1);
    }

    #[test]
    fn default_zero_stale_if_error_never_authorizes_replay() {
        let root = TestRoot::new("zero-stale-grace");
        let cache = open_cache(&root);
        let cache_key = key("alice", "zero-grace");
        let control = CacheControl::ttl(0)
            .with_etag("opaque-etag")
            .with_revalidatable();
        let capture = cache.begin_capture(schema(), 1).unwrap();
        assert!(cache
            .commit(cache_key.clone(), capture, Duration::ZERO, Some(&control))
            .unwrap()
            .is_stored());
        let hit = cache
            .lookup_for_revalidation_expected_schema(&cache_key, &schema())
            .unwrap()
            .unwrap();
        assert!(!hit.may_serve_on_error_at(SystemTime::now()));
    }

    #[test]
    fn revocation_removes_only_the_observed_validator_generation() {
        let root = TestRoot::new("validator-revocation");
        let cache = open_cache(&root);
        let cache_key = key("alice", "f");
        let control = CacheControl::ttl(0)
            .with_etag("opaque-etag")
            .with_revalidatable();
        let mut capture = cache.begin_capture(schema(), 1).unwrap();
        capture.push_batch(0, &batch(&[1])).unwrap();
        assert!(cache
            .commit(cache_key.clone(), capture, Duration::ZERO, Some(&control))
            .unwrap()
            .is_stored());
        let stale = cache
            .lookup_for_revalidation_expected_schema(&cache_key, &schema())
            .unwrap()
            .unwrap();
        assert!(cache.remove_hit(&stale).unwrap());
        assert!(cache
            .lookup_for_revalidation_expected_schema(&cache_key, &schema())
            .unwrap()
            .is_none());
        assert!(!cache.remove_hit(&stale).unwrap());
    }

    #[test]
    fn every_supported_codec_is_self_describing() {
        for codec in [
            DiskCacheCodec::None,
            DiskCacheCodec::Zstd,
            DiskCacheCodec::Lz4,
        ] {
            let root = TestRoot::new("codec");
            let mut options = DiskCacheOptions::new(&root.0);
            options.codec = codec;
            let cache = DiskCache::open(options).unwrap();
            let cache_key = key("alice", "f");
            store(
                &cache,
                cache_key.clone(),
                &[&[1, 2, 3]],
                Duration::from_secs(60),
            );
            let hit = cache.lookup(&cache_key).unwrap().unwrap();
            assert_eq!(hit.open_partition(0).unwrap().count(), 1, "codec {codec:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn root_and_secret_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let root = TestRoot::new("permissions");
        let cache = open_cache(&root);
        assert_eq!(
            fs::metadata(&root.0).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(root.0.join(FORMAT_DIR).join(SECRET_FILE))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );

        let secret = root.0.join(FORMAT_DIR).join(SECRET_FILE);
        fs::set_permissions(&root.0, fs::Permissions::from_mode(0o755)).unwrap();
        fs::set_permissions(&secret, fs::Permissions::from_mode(0o644)).unwrap();
        drop(cache);
        let _reopened = open_cache(&root);
        assert_eq!(
            fs::metadata(&root.0).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(secret).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

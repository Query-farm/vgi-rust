// Copyright 2026 Query Farm LLC - https://query.farm
//! Opt-in, validated publications of committed catalog metadata.
//! Attachment authorization is performed by the dispatcher before this layer.
use crate::protocol::dtos::CatalogContentsResponse;
use arrow_array::{Array, BinaryArray};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use vgi_rpc::external::{
    publish_external, resolve_external_location, Compression, ExternalLocationConfig,
    ExternalStorage, UploadResult,
};
use vgi_rpc::external_ref::ExternalRef;
use vgi_rpc::{Result, RpcError};

const CODEC: &str = "catalog-collections-1";
const WIRE: &str = "3.0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotIdentity {
    pub catalog_namespace: String,
    pub source_revision: String,
    /// None means attachment-local. Shared scopes are an explicit author declaration.
    pub visibility_scope: Option<String>,
    pub catalog_version: i64,
}
pub struct SnapshotRequest<'a> {
    pub attachment_scope: &'a [u8],
    pub attach_opaque_data: &'a [u8],
    pub catalog_version: i64,
}
pub type IdentityHook = Arc<dyn Fn(&SnapshotRequest<'_>) -> Result<SnapshotIdentity> + Send + Sync>;
pub type ValidatorHook = Arc<dyn Fn(&SnapshotRequest<'_>) -> Result<Option<String>> + Send + Sync>;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PublicationLocation {
    pub object_id: Option<String>,
    pub expires_at: Option<f64>,
    pub retain_until: Option<f64>,
    pub stable: bool,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct CatalogPublicationManifest {
    pub cache_key: String,
    pub manifest_id: String,
    pub protocol_name: String,
    pub wire_revision: String,
    pub contract_hash: String,
    pub codec_revision: String,
    pub catalog_version: i64,
    pub etag: Option<String>,
    pub sha256: String,
    pub raw_size: usize,
    pub stored_size: usize,
    pub url: String,
    pub object_id: Option<String>,
    pub expires_at: Option<f64>,
    pub retain_until: Option<f64>,
    pub stable: bool,
    pub format_version: u32,
    pub validator_revision: u32,
}
impl std::fmt::Debug for CatalogPublicationManifest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogPublicationManifest")
            .field("cache_key", &self.cache_key)
            .field("manifest_id", &self.manifest_id)
            .finish_non_exhaustive()
    }
}
fn validate_renewal(
    old: &CatalogPublicationManifest,
    new: &CatalogPublicationManifest,
) -> Result<()> {
    if old.manifest_id == new.manifest_id {
        let mut normalized = new.clone();
        normalized.url = old.url.clone();
        normalized.expires_at = old.expires_at;
        if serde_json::to_value(old).unwrap() != serde_json::to_value(normalized).unwrap() {
            return Err(RpcError::value_error(
                "catalog URL renewal changed immutable publication metadata",
            ));
        }
    }
    Ok(())
}
impl CatalogPublicationManifest {
    pub fn reference(&self) -> Result<ExternalRef> {
        ExternalRef::new(self.url.clone(), Some(self.sha256.clone()))
    }
}

/// Durable implementations must make commit and conditional invalidation atomic.
/// Object invalidation removes discovery only: issued URLs must remain fetchable.
pub trait CatalogPublicationRepository: Send + Sync {
    fn profile(&self) -> &str;
    fn storage(&self) -> &dyn ExternalStorage;
    fn lookup(&self, key: &str) -> Result<Option<CatalogPublicationManifest>>;
    fn commit_if_absent(
        &self,
        manifest: CatalogPublicationManifest,
    ) -> Result<CatalogPublicationManifest>;
    fn invalidate(&self, key: &str, manifest_id: &str) -> Result<bool>;
    fn location(&self, reference: &ExternalRef) -> Result<PublicationLocation>;
    /// A renewal must persist conditionally and return its current winning descriptor.
    fn renew(
        &self,
        _manifest: &CatalogPublicationManifest,
    ) -> Result<Option<CatalogPublicationManifest>> {
        Ok(None)
    }
}

pub struct MemoryCatalogPublicationRepository {
    pub storage: Arc<dyn ExternalStorage>,
    pub profile: String,
    pub location: PublicationLocation,
    manifests: Mutex<HashMap<String, CatalogPublicationManifest>>,
}
impl MemoryCatalogPublicationRepository {
    pub fn new(
        storage: Arc<dyn ExternalStorage>,
        profile: String,
        location: PublicationLocation,
    ) -> Self {
        Self {
            storage,
            profile,
            location,
            manifests: Mutex::new(HashMap::new()),
        }
    }
}
impl CatalogPublicationRepository for MemoryCatalogPublicationRepository {
    fn profile(&self) -> &str {
        &self.profile
    }
    fn storage(&self) -> &dyn ExternalStorage {
        self.storage.as_ref()
    }
    fn lookup(&self, key: &str) -> Result<Option<CatalogPublicationManifest>> {
        Ok(self.manifests.lock().unwrap().get(key).cloned())
    }
    fn commit_if_absent(
        &self,
        manifest: CatalogPublicationManifest,
    ) -> Result<CatalogPublicationManifest> {
        Ok(self
            .manifests
            .lock()
            .unwrap()
            .entry(manifest.cache_key.clone())
            .or_insert(manifest)
            .clone())
    }
    fn invalidate(&self, key: &str, id: &str) -> Result<bool> {
        let mut values = self.manifests.lock().unwrap();
        if values.get(key).map(|m| m.manifest_id.as_str()) != Some(id) {
            return Ok(false);
        }
        values.remove(key);
        Ok(true)
    }
    fn location(&self, _reference: &ExternalRef) -> Result<PublicationLocation> {
        Ok(self.location.clone())
    }
}

/// Policy is installed per catalog; omitted means ordinary delivery.
pub struct CatalogSnapshotPolicy {
    pub repository: Arc<dyn CatalogPublicationRepository>,
    pub identity: IdentityHook,
    pub validator: Option<ValidatorHook>,
    pub external: ExternalLocationConfig,
    pub ordinary_on_failure: bool,
    pub max_raw_bytes: usize,
    pub max_stored_bytes: usize,
    pub expiry_margin_seconds: f64,
    pub max_cached_manifests: usize,
    manager: CatalogSnapshotManager,
}
impl CatalogSnapshotPolicy {
    pub fn new(
        repository: Arc<dyn CatalogPublicationRepository>,
        identity: IdentityHook,
        external: ExternalLocationConfig,
    ) -> Self {
        Self {
            repository,
            identity,
            external,
            validator: None,
            ordinary_on_failure: false,
            max_raw_bytes: 128 * 1024 * 1024,
            max_stored_bytes: 128 * 1024 * 1024,
            expiry_margin_seconds: 60.0,
            max_cached_manifests: 256,
            manager: CatalogSnapshotManager::default(),
        }
    }
    pub fn serve(
        &self,
        request: &SnapshotRequest<'_>,
        if_none_match: Option<&str>,
        build: impl Fn() -> Result<CatalogContentsResponse>,
    ) -> Result<SnapshotDelivery> {
        if self.max_raw_bytes == 0
            || self.max_stored_bytes == 0
            || self.max_cached_manifests == 0
            || !self.expiry_margin_seconds.is_finite()
            || self.expiry_margin_seconds < 0.0
        {
            return Err(RpcError::value_error(
                "invalid catalog snapshot policy limits",
            ));
        }
        if let Some(validator) = &self.validator {
            if let Some(etag) = validator(request)? {
                if Some(etag.as_str()) == if_none_match {
                    return Ok(SnapshotDelivery::Inline(
                        CatalogContentsResponse::unchanged(request.catalog_version, etag),
                    ));
                }
            }
        }
        let result = self.manager.serve(self, request, if_none_match, &build);
        match result {
            Err(error) if self.ordinary_on_failure => {
                let before = (self.identity)(request)?;
                let value = build()?;
                let after = (self.identity)(request)?;
                if before != after || value.catalog_version != before.catalog_version {
                    return Err(error);
                }
                value.schema_contents()?;
                if (self.identity)(request)? != before {
                    return Err(error);
                }
                if let Some(etag) = value
                    .etag
                    .as_ref()
                    .filter(|etag| Some(etag.as_str()) == if_none_match)
                {
                    return Ok(SnapshotDelivery::Inline(
                        CatalogContentsResponse::unchanged(value.catalog_version, etag.clone()),
                    ));
                }
                Ok(SnapshotDelivery::Inline(value))
            }
            result => result,
        }
    }
    pub fn warm(
        &self,
        request: &SnapshotRequest<'_>,
        build: impl Fn() -> Result<CatalogContentsResponse>,
    ) -> Result<SnapshotDelivery> {
        self.serve(request, None, build)
    }
    pub fn invalidate(&self, key: &str, manifest_id: &str) -> Result<bool> {
        let lease = self.manager.lease(key);
        let _gate = lease.gate.lock().unwrap();
        let mut cache = self.manager.validated.lock().unwrap();
        if cache.get(key).map(|m| m.manifest_id.as_str()) == Some(manifest_id) {
            cache.remove(key);
        }
        drop(cache);
        self.repository.invalidate(key, manifest_id)
    }
    pub fn import(
        &self,
        request: &SnapshotRequest<'_>,
        manifest: CatalogPublicationManifest,
    ) -> Result<()> {
        let identity = (self.identity)(request)?;
        let key = cache_key(self, request, &identity)?;
        let lease = self.manager.lease(&key);
        let _gate = lease.gate.lock().unwrap();
        self.manager.validate(self, &key, &identity, &manifest)?;
        if (self.identity)(request)? != identity {
            return Err(RpcError::value_error(
                "catalog changed during publication import",
            ));
        }
        let manifest_id = manifest.manifest_id.clone();
        let winner = self.repository.commit_if_absent(manifest)?;
        if winner.manifest_id != manifest_id {
            self.manager.validate(self, &key, &identity, &winner)?;
        }
        if (self.identity)(request)? != identity {
            return Err(RpcError::value_error(
                "catalog changed during publication import",
            ));
        }
        self.manager.remember(self, key, winner);
        Ok(())
    }
}
pub enum SnapshotDelivery {
    Inline(CatalogContentsResponse),
    Published(ExternalRef),
}
#[derive(Default)]
pub struct CatalogSnapshotManager {
    validated: Mutex<HashMap<String, CatalogPublicationManifest>>,
    gates: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}
struct GateLease<'a> {
    manager: &'a CatalogSnapshotManager,
    key: String,
    gate: Arc<Mutex<()>>,
}
impl Drop for GateLease<'_> {
    fn drop(&mut self) {
        let mut gates = self.manager.gates.lock().unwrap();
        if Arc::strong_count(&self.gate) == 2 {
            gates.remove(&self.key);
        }
    }
}

impl CatalogSnapshotManager {
    fn lease(&self, key: &str) -> GateLease<'_> {
        let gate = self
            .gates
            .lock()
            .unwrap()
            .entry(key.into())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        GateLease {
            manager: self,
            key: key.into(),
            gate,
        }
    }
    fn serve(
        &self,
        policy: &CatalogSnapshotPolicy,
        request: &SnapshotRequest<'_>,
        inm: Option<&str>,
        build: &dyn Fn() -> Result<CatalogContentsResponse>,
    ) -> Result<SnapshotDelivery> {
        // Active key gates are removed after the last waiter, keeping lock state bounded.
        for _ in 0..3 {
            let identity = (policy.identity)(request)?;
            let key = cache_key(policy, request, &identity)?;
            let lease = self.lease(&key);
            let _gate = lease.gate.lock().unwrap();
            let mut manifest = self.validated.lock().unwrap().get(&key).cloned();
            if manifest.is_none() {
                if let Some(mut found) = policy.repository.lookup(&key)? {
                    if !usable(&found, policy.expiry_margin_seconds) {
                        if let Some(renewed) = policy.repository.renew(&found)? {
                            validate_renewal(&found, &renewed)?;
                            found = renewed;
                        }
                    }
                    match self.validate(policy, &key, &identity, &found) {
                        Ok(()) => manifest = Some(found),
                        Err(_) => {
                            policy.repository.invalidate(&key, &found.manifest_id)?;
                        }
                    }
                }
            }
            if let Some(found) = manifest {
                if usable(&found, policy.expiry_margin_seconds) {
                    if (policy.identity)(request)? != identity {
                        continue;
                    }
                    self.remember(policy, key.clone(), found.clone());
                    if found.etag.as_deref().is_some() && found.etag.as_deref() == inm {
                        return Ok(SnapshotDelivery::Inline(
                            CatalogContentsResponse::unchanged(
                                found.catalog_version,
                                found.etag.unwrap(),
                            ),
                        ));
                    }
                    return Ok(SnapshotDelivery::Published(found.reference()?));
                }
                if let Some(renewed) = policy.repository.renew(&found)? {
                    validate_renewal(&found, &renewed)?;
                    self.validate(policy, &key, &identity, &renewed)?;
                    self.remember(policy, key.clone(), renewed);
                    continue;
                }
                policy.repository.invalidate(&key, &found.manifest_id)?;
                self.validated.lock().unwrap().remove(&key);
            }
            let mut value = build()?;
            if value.not_modified {
                return Err(RpcError::value_error(
                    "publication build must return a complete snapshot",
                ));
            }
            value.schema_contents()?;
            if value.catalog_version != identity.catalog_version
                || (policy.identity)(request)? != identity
            {
                continue;
            }
            if value.etag.is_none() {
                value.etag = Some(response_digest(&value));
            }
            if value.etag.as_deref() == inm {
                return Ok(SnapshotDelivery::Inline(
                    CatalogContentsResponse::unchanged(value.catalog_version, value.etag.unwrap()),
                ));
            }
            let envelope = crate::wire::to_result_batch(value.clone())?;
            let bounded = BoundedStorage {
                backend: policy.repository.storage(),
                raw_limit: policy.max_raw_bytes,
                stored_limit: policy.max_stored_bytes,
                sizes: Mutex::new((0, 0)),
            };
            let reference =
                publish_external(&envelope, &bounded, policy.external.compression, true)?;
            let location = policy.repository.location(&reference)?;
            let (raw_size, stored_size) = *bounded.sizes.lock().unwrap();
            let manifest = CatalogPublicationManifest {
                cache_key: key.clone(),
                manifest_id: format!("{:032x}", rand::random::<u128>()),
                protocol_name: vgi_protocol::VGI_PROTOCOL_NAME.into(),
                wire_revision: WIRE.into(),
                contract_hash: contract_hash(),
                codec_revision: CODEC.into(),
                catalog_version: value.catalog_version,
                etag: value.etag.clone(),
                sha256: reference.sha256().unwrap().into(),
                raw_size,
                stored_size,
                url: reference.url().into(),
                object_id: location.object_id,
                expires_at: location.expires_at,
                retain_until: location.retain_until,
                stable: location.stable,
                format_version: 1,
                validator_revision: 1,
            };
            let readback = self.read(policy, &key, &identity, &manifest)?;
            if readback.catalog_version != value.catalog_version
                || readback.etag != value.etag
                || readback.streams() != value.streams()
            {
                return Err(RpcError::value_error(
                    "published catalog differs from collected snapshot",
                ));
            }
            if (policy.identity)(request)? != identity {
                continue;
            }
            let winner = policy.repository.commit_if_absent(manifest.clone())?;
            if winner.manifest_id != manifest.manifest_id {
                self.validate(policy, &key, &identity, &winner)?;
            }
            if (policy.identity)(request)? != identity {
                continue;
            }
            self.remember(policy, key, winner.clone());
            return Ok(SnapshotDelivery::Published(winner.reference()?));
        }
        Err(RpcError::runtime_error(
            "catalog revision or publication changed repeatedly",
        ))
    }
    fn remember(&self, p: &CatalogSnapshotPolicy, key: String, value: CatalogPublicationManifest) {
        let mut cache = self.validated.lock().unwrap();
        if cache.len() >= p.max_cached_manifests.max(1) {
            cache.clear();
        }
        cache.insert(key, value);
    }
    fn validate(
        &self,
        p: &CatalogSnapshotPolicy,
        key: &str,
        id: &SnapshotIdentity,
        m: &CatalogPublicationManifest,
    ) -> Result<()> {
        self.read(p, key, id, m).map(|_| ())
    }
    fn read(
        &self,
        p: &CatalogSnapshotPolicy,
        key: &str,
        id: &SnapshotIdentity,
        m: &CatalogPublicationManifest,
    ) -> Result<CatalogContentsResponse> {
        if m.cache_key != key
            || m.catalog_version != id.catalog_version
            || m.protocol_name != vgi_protocol::VGI_PROTOCOL_NAME
            || m.wire_revision != WIRE
            || m.contract_hash != contract_hash()
            || m.codec_revision != CODEC
            || m.format_version != 1
            || m.validator_revision != 1
            || m.raw_size > p.max_raw_bytes
            || m.stored_size > p.max_stored_bytes
            || m.raw_size == 0
            || m.stored_size == 0
            || m.sha256.len() != 64
            || !usable(m, p.expiry_margin_seconds)
        {
            return Err(RpcError::value_error(
                "invalid or expired catalog publication manifest",
            ));
        }
        let (pointer, metadata) = m
            .reference()?
            .pointer_batch(&crate::wire::result_binary_schema())?;
        let mut config = p.external.clone();
        config.fetcher = Arc::new(ValidationFetcher {
            base: config.fetcher.clone(),
            raw_size: m.raw_size,
            raw_limit: p.max_raw_bytes,
        });
        config.max_encoded_bytes = config.max_encoded_bytes.min(p.max_stored_bytes);
        config.max_decompressed_bytes = config.max_decompressed_bytes.min(p.max_raw_bytes);
        let (batch, _) = resolve_external_location(&pointer, &metadata, &config)?;
        if batch.num_rows() != 1 || batch.schema() != crate::wire::result_binary_schema() {
            return Err(RpcError::value_error(
                "published catalog has invalid RPC result envelope",
            ));
        }
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        if column.is_null(0) {
            return Err(RpcError::value_error("published catalog result is null"));
        }
        let bytes = column.value(0);
        let mut cursor = std::io::Cursor::new(bytes);
        let mut reader = vgi_rpc::wire::StreamReader::new(&mut cursor)?.relax_nullability();
        let inner = reader
            .read_next()?
            .ok_or_else(|| RpcError::value_error("catalog response missing row"))?
            .0;
        if inner.num_rows() != 1
            || reader.read_next()?.is_some()
            || crate::ipc::read_schema(bytes)?
                != crate::wire::flat_schema::<CatalogContentsResponse>()
        {
            return Err(RpcError::value_error(
                "catalog response requires exactly one typed row",
            ));
        }
        drop(reader);
        if cursor.position() != bytes.len() as u64 {
            return Err(RpcError::value_error("trailing catalog response bytes"));
        }
        let response: CatalogContentsResponse = crate::wire::from_batch(&inner)?;
        if response.not_modified
            || response.catalog_version != m.catalog_version
            || response.etag != m.etag
        {
            return Err(RpcError::value_error(
                "catalog publication metadata mismatch",
            ));
        }
        response.schema_contents()?;
        Ok(response)
    }
}
fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}
fn usable(m: &CatalogPublicationManifest, margin: f64) -> bool {
    let deadline = now() + margin;
    (m.stable || m.expires_at.is_some())
        && m.expires_at
            .map(|v| v.is_finite() && v > deadline)
            .unwrap_or(true)
        && m.retain_until
            .map(|v| v.is_finite() && v > deadline)
            .unwrap_or(true)
}
fn hex(bytes: impl AsRef<[u8]>) -> String {
    Sha256::digest(bytes.as_ref())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn contract_hash() -> String {
    vgi_protocol::generated::protocol_schemas::VGI_CATALOG_CONTRACT_HASH.into()
}

fn cache_key(
    p: &CatalogSnapshotPolicy,
    req: &SnapshotRequest<'_>,
    id: &SnapshotIdentity,
) -> Result<String> {
    if id.catalog_namespace.is_empty()
        || id.source_revision.is_empty()
        || id.catalog_version != req.catalog_version
    {
        return Err(RpcError::value_error(
            "publication needs a nonempty namespace, revision and matching catalog version",
        ));
    }
    let scope = id
        .visibility_scope
        .clone()
        .unwrap_or_else(|| hex(req.attachment_scope));
    Ok(hex(serde_json::to_vec(&(
        vgi_protocol::VGI_PROTOCOL_NAME,
        WIRE,
        contract_hash(),
        CODEC,
        &id.catalog_namespace,
        &id.source_revision,
        scope,
        id.catalog_version,
        p.repository.profile(),
        format!("{:?}", p.external.compression),
    ))
    .unwrap()))
}
pub fn response_digest(response: &CatalogContentsResponse) -> String {
    crate::catalog_contents::catalog_response_digest(response)
}

struct ValidationFetcher {
    base: Arc<dyn vgi_rpc::external::Fetcher>,
    raw_size: usize,
    raw_limit: usize,
}
impl vgi_rpc::external::Fetcher for ValidationFetcher {
    fn fetch(&self, url: &str, compression: Compression, max: usize) -> Result<Vec<u8>> {
        self.base.fetch(url, compression, max)
    }
    fn fetch_with_policy(
        &self,
        url: &str,
        compression: Compression,
        max: usize,
        validator: &vgi_rpc::external::UrlValidator,
        redirects: usize,
    ) -> Result<vgi_rpc::external::FetchedPayload> {
        let payload = self
            .base
            .fetch_with_policy(url, compression, max, validator, redirects)?;
        if payload.bytes.len() > max {
            return Err(RpcError::value_error("catalog encoded size limit exceeded"));
        }
        let raw = match payload.compression {
            Compression::None => payload.bytes.clone(),
            Compression::Zstd(_) => {
                let mut output = Vec::new();
                zstd::stream::read::Decoder::new(payload.bytes.as_slice())
                    .map_err(|e| RpcError::value_error(e.to_string()))?
                    .take(self.raw_limit.saturating_add(1) as u64)
                    .read_to_end(&mut output)
                    .map_err(|e| RpcError::value_error(e.to_string()))?;
                output
            }
        };
        if raw.len() != self.raw_size || raw.len() > self.raw_limit {
            return Err(RpcError::value_error(
                "catalog raw size mismatch or limit exceeded",
            ));
        }
        let mut cursor = std::io::Cursor::new(&raw);
        let mut reader = vgi_rpc::wire::StreamReader::new(&mut cursor)?;
        let (batch, metadata) = reader
            .read_next()?
            .ok_or_else(|| RpcError::value_error("missing catalog envelope"))?;
        if metadata.keys().any(|key| {
            key.starts_with("vgi_rpc.location")
                || key.starts_with("vgi_rpc.log_")
                || key.starts_with("vgi_rpc.error_")
        }) || batch.num_rows() != 1
            || reader.read_next()?.is_some()
        {
            return Err(RpcError::value_error(
                "catalog publication must contain one ordinary result batch",
            ));
        }
        drop(reader);
        if cursor.position() != raw.len() as u64 {
            return Err(RpcError::value_error("trailing catalog publication bytes"));
        }
        Ok(payload)
    }
}

struct BoundedStorage<'a> {
    backend: &'a dyn ExternalStorage,
    raw_limit: usize,
    stored_limit: usize,
    sizes: Mutex<(usize, usize)>,
}
impl ExternalStorage for BoundedStorage<'_> {
    fn upload(&self, bytes: &[u8], compression: Compression) -> Result<UploadResult> {
        if bytes.len() > self.stored_limit {
            return Err(RpcError::value_error(
                "catalog publication exceeds stored size limit",
            ));
        }
        let raw_size = match compression {
            Compression::None => bytes.len(),
            Compression::Zstd(_) => {
                let mut raw = zstd::stream::read::Decoder::new(bytes)
                    .map_err(|e| RpcError::runtime_error(e.to_string()))?
                    .take(self.raw_limit.saturating_add(1) as u64);
                std::io::copy(&mut raw, &mut std::io::sink())
                    .map_err(|e| RpcError::runtime_error(e.to_string()))? as usize
            }
        };
        if raw_size > self.raw_limit {
            return Err(RpcError::value_error(
                "catalog publication exceeds raw size limit",
            ));
        }
        *self.sizes.lock().unwrap() = (raw_size, bytes.len());
        self.backend.upload(bytes, compression)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use vgi_rpc::external::{any_url_validator, Fetcher};
    struct CountingStore {
        inner: Mutex<HashMap<String, Vec<u8>>>,
        uploads: AtomicUsize,
        gets: AtomicUsize,
    }
    impl ExternalStorage for CountingStore {
        fn upload(&self, data: &[u8], compression: Compression) -> Result<UploadResult> {
            let _ = compression;
            let id = self.uploads.fetch_add(1, Ordering::SeqCst);
            let url = format!("https://memory.test/{id}");
            self.inner
                .lock()
                .unwrap()
                .insert(url.clone(), data.to_vec());
            Ok(UploadResult {
                url,
                sha256: hex(data),
            })
        }
    }
    impl Fetcher for CountingStore {
        fn fetch(&self, url: &str, compression: Compression, max: usize) -> Result<Vec<u8>> {
            let _ = compression;
            self.gets.fetch_add(1, Ordering::SeqCst);
            let data = self.inner.lock().unwrap().get(url).unwrap().clone();
            if data.len() > max {
                return Err(RpcError::value_error("size limit"));
            }
            Ok(data)
        }
    }
    fn setup() -> (
        Arc<CountingStore>,
        Arc<MemoryCatalogPublicationRepository>,
        ExternalLocationConfig,
    ) {
        let store = Arc::new(CountingStore {
            inner: Mutex::new(HashMap::new()),
            uploads: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
        });
        let repo = Arc::new(MemoryCatalogPublicationRepository::new(
            store.clone(),
            "test".into(),
            PublicationLocation {
                stable: true,
                ..Default::default()
            },
        ));
        let config = ExternalLocationConfig::new(store.clone(), store.clone())
            .with_url_validator(any_url_validator());
        (store, repo, config)
    }
    fn identity() -> IdentityHook {
        Arc::new(|r| {
            Ok(SnapshotIdentity {
                catalog_namespace: "catalog".into(),
                source_revision: "r1".into(),
                visibility_scope: None,
                catalog_version: r.catalog_version,
            })
        })
    }
    fn response() -> CatalogContentsResponse {
        let functions = (0..4)
            .map(|i| {
                let mut function =
                    crate::catalog::default_function_info(&format!("f{i}"), "scalar");
                function.schema_path = vec!["main".into()];
                function.arguments = vgi_rpc::Bytes(vec![0xff, 0, 0x80]);
                function.output_schema = vgi_rpc::Bytes(vec![]);
                function.parameter_default_values = if i % 2 == 0 {
                    None
                } else {
                    Some(vgi_rpc::Bytes(vec![]))
                };
                function
            })
            .collect();
        CatalogContentsResponse {
            catalog_version: 1,
            etag: Some("e1".into()),
            not_modified: false,
            schemas: vgi_protocol::catalog::encode_collection(vec![
                crate::catalog::main_schema_info(&[]),
            ])
            .unwrap(),
            tables: None,
            views: None,
            functions: vgi_protocol::catalog::encode_collection_with_batch_size(functions, 2)
                .unwrap(),
            macros: None,
            indexes: None,
        }
    }
    #[test]
    fn warm_recovery_scopes_and_conditional_hits_avoid_object_work() {
        let (store, repo, config) = setup();
        let policy = CatalogSnapshotPolicy::new(repo.clone(), identity(), config.clone());
        let request = SnapshotRequest {
            attachment_scope: b"sealed-a",
            attach_opaque_data: b"payload",
            catalog_version: 1,
        };
        let builds = AtomicUsize::new(0);
        let build = || {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok(response())
        };
        assert!(matches!(
            policy.serve(&request, None, build).unwrap(),
            SnapshotDelivery::Published(_)
        ));
        assert_eq!(store.gets.load(Ordering::SeqCst), 1);
        assert!(
            matches!(policy.serve(&request,Some("e1"),build).unwrap(),SnapshotDelivery::Inline(r) if r.not_modified)
        );
        policy.warm(&request, build).unwrap();
        assert_eq!(builds.load(Ordering::SeqCst), 1);
        assert_eq!(store.gets.load(Ordering::SeqCst), 1);
        let restarted = CatalogSnapshotPolicy::new(repo, identity(), config);
        restarted
            .warm(&request, || panic!("recovery rebuilt"))
            .unwrap();
        assert_eq!(store.gets.load(Ordering::SeqCst), 2);
        assert_eq!(store.uploads.load(Ordering::SeqCst), 1);
        let other = SnapshotRequest {
            attachment_scope: b"sealed-b",
            ..request
        };
        restarted.warm(&other, build).unwrap();
        assert_eq!(store.uploads.load(Ordering::SeqCst), 2);
    }
    #[test]
    fn caps_and_revision_races_never_commit_or_upload_invalid_snapshots() {
        let (store, repo, config) = setup();
        let mut policy = CatalogSnapshotPolicy::new(repo.clone(), identity(), config.clone());
        policy.max_raw_bytes = 1;
        let request = SnapshotRequest {
            attachment_scope: b"a",
            attach_opaque_data: b"p",
            catalog_version: 1,
        };
        assert!(policy.warm(&request, || Ok(response())).is_err());
        assert_eq!(store.uploads.load(Ordering::SeqCst), 0);
        assert!(repo.manifests.lock().unwrap().is_empty());
        let revision = Arc::new(AtomicUsize::new(0));
        let counter = revision.clone();
        let policy = CatalogSnapshotPolicy::new(
            repo.clone(),
            Arc::new(move |r| {
                Ok(SnapshotIdentity {
                    catalog_namespace: "catalog".into(),
                    source_revision: counter.load(Ordering::SeqCst).to_string(),
                    visibility_scope: None,
                    catalog_version: r.catalog_version,
                })
            }),
            config,
        );
        assert!(policy
            .warm(&request, || {
                revision.fetch_add(1, Ordering::SeqCst);
                Ok(response())
            })
            .is_err());
        assert_eq!(store.uploads.load(Ordering::SeqCst), 0);
        assert!(repo.manifests.lock().unwrap().is_empty());
    }
    #[test]
    fn blocked_publication_does_not_block_an_unrelated_key() {
        let (_, repo, config) = setup();
        let policy = Arc::new(CatalogSnapshotPolicy::new(repo, identity(), config));
        let other_scope = "other".to_string();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let worker = policy.clone();
        let blocked = std::thread::spawn(move || {
            worker
                .warm(
                    &SnapshotRequest {
                        attachment_scope: b"blocked",
                        attach_opaque_data: b"p",
                        catalog_version: 1,
                    },
                    || {
                        started_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                        Ok(response())
                    },
                )
                .unwrap()
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = policy.clone();
        let other = std::thread::spawn(move || {
            worker
                .warm(
                    &SnapshotRequest {
                        attachment_scope: other_scope.as_bytes(),
                        attach_opaque_data: b"p",
                        catalog_version: 1,
                    },
                    || Ok(response()),
                )
                .unwrap();
            done_tx.send(()).unwrap();
        });
        let finished = done_rx.recv_timeout(std::time::Duration::from_secs(2));
        release_tx.send(()).unwrap();
        blocked.join().unwrap();
        other.join().unwrap();
        assert!(finished.is_ok());
    }

    #[test]
    fn import_rejects_log_extra_and_trailing_data_even_with_matching_hash() {
        let (store, repo, config) = setup();
        let policy = CatalogSnapshotPolicy::new(repo.clone(), identity(), config);
        let request = SnapshotRequest {
            attachment_scope: b"strict",
            attach_opaque_data: b"p",
            catalog_version: 1,
        };
        policy.warm(&request, || Ok(response())).unwrap();
        let manifest = repo
            .manifests
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .clone();
        for mode in 0..3 {
            let envelope = crate::wire::to_result_batch(response()).unwrap();
            let mut bytes = Vec::new();
            {
                let mut writer =
                    vgi_rpc::wire::StreamWriter::new(&mut bytes, envelope.schema().as_ref())
                        .unwrap();
                let metadata = HashMap::from([("vgi_rpc.log_level".into(), "INFO".into())]);
                writer
                    .write(&envelope, if mode == 0 { Some(&metadata) } else { None })
                    .unwrap();
                if mode == 1 {
                    writer.write(&envelope, None).unwrap();
                }
                writer.finish().unwrap();
            }
            if mode == 2 {
                bytes.push(1);
            }
            let mut forged = manifest.clone();
            forged.sha256 = hex(&bytes);
            forged.raw_size = bytes.len();
            forged.stored_size = bytes.len();
            store
                .inner
                .lock()
                .unwrap()
                .insert(forged.url.clone(), bytes);
            assert!(
                policy.import(&request, forged).is_err(),
                "invalid mode {mode} was accepted"
            );
        }
    }
    #[test]
    fn compressed_publication_uses_decoded_hash_and_size() {
        let (_, repo, config) = setup();
        let policy = CatalogSnapshotPolicy::new(
            repo,
            identity(),
            config.with_compression(Compression::Zstd(3)),
        );
        let request = SnapshotRequest {
            attachment_scope: b"zstd",
            attach_opaque_data: b"p",
            catalog_version: 1,
        };
        assert!(matches!(
            policy.warm(&request, || Ok(response())).unwrap(),
            SnapshotDelivery::Published(_)
        ));
    }
}

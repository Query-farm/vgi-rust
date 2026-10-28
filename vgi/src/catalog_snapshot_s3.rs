// Copyright 2026 Query Farm LLC - https://query.farm
//! S3/R2 publication repository with conditional manifests and immutable objects.
//!
//! The injected client bridges the application's S3 SDK. It must honor the
//! conditional-write and bounded-read requirements below. Configure storage
//! lifecycle retention to exceed `retention_seconds`; discovery manifests use
//! a separate prefix and are tombstoned rather than deleting issued objects.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};
use vgi_rpc::external::{Compression, ExternalStorage, UploadResult};
use vgi_rpc::external_ref::ExternalRef;
use vgi_rpc::{Result, RpcError};

use crate::catalog_snapshot::{
    CatalogPublicationManifest, CatalogPublicationRepository, PublicationLocation,
};

/// An object and its opaque conditional-write version (S3 ETag).
pub struct S3VersionedObject {
    pub bytes: Vec<u8>,
    pub etag: String,
}

/// Translate these to S3 PutObject IfNoneMatch / IfMatch, never GET-then-PUT.
pub enum S3WriteCondition<'a> {
    Unconditional,
    Absent,
    Matches(&'a str),
}

/// Minimal synchronous bridge to a configured S3-compatible SDK client.
///
/// Implementations handle credentials and signing. Return `None` only for
/// object-not-found, and `false` only for a conditional write conflict; surface
/// authentication, transport and backend failures as errors.
pub trait CatalogS3Client: Send + Sync {
    /// Abort before buffering more than `max_bytes`, even for chunked bodies.
    fn get_object(
        &self,
        bucket: &str,
        key: &str,
        max_bytes: usize,
    ) -> Result<Option<S3VersionedObject>>;
    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        bytes: &[u8],
        content_type: &str,
        content_encoding: Option<&str>,
        condition: S3WriteCondition<'_>,
    ) -> Result<bool>;
    fn presign_get(&self, bucket: &str, key: &str, expires_in_seconds: u64) -> Result<String>;
}

pub struct S3CatalogPublicationRepository {
    client: Arc<dyn CatalogS3Client>,
    bucket: String,
    prefix: String,
    profile: String,
    url_ttl_seconds: u64,
    retention_seconds: u64,
    locations: Mutex<HashMap<String, PublicationLocation>>,
}

impl S3CatalogPublicationRepository {
    pub fn new(
        client: Arc<dyn CatalogS3Client>,
        bucket: String,
        prefix: String,
        profile: String,
        url_ttl_seconds: u64,
        retention_seconds: u64,
    ) -> Result<Self> {
        if bucket.is_empty()
            || profile.is_empty()
            || url_ttl_seconds == 0
            || retention_seconds <= url_ttl_seconds
        {
            return Err(RpcError::value_error(
                "S3 publication requires bucket/profile and retention exceeding URL lifetime",
            ));
        }
        Ok(Self {
            client,
            bucket,
            prefix: format!("{}/", prefix.trim_end_matches('/')),
            profile,
            url_ttl_seconds,
            retention_seconds,
            locations: Mutex::new(HashMap::new()),
        })
    }

    fn manifest_key(&self, key: &str) -> String {
        format!("{}manifests/{key}.json", self.prefix)
    }

    fn load(&self, key: &str) -> Result<(Option<CatalogPublicationManifest>, Option<String>)> {
        let Some(object) =
            self.client
                .get_object(&self.bucket, &self.manifest_key(key), 1024 * 1024)?
        else {
            return Ok((None, None));
        };
        if object.bytes.len() > 1024 * 1024 {
            return Err(RpcError::value_error(
                "catalog publication manifest exceeds size limit",
            ));
        }
        let value: serde_json::Value = serde_json::from_slice(&object.bytes).map_err(|e| {
            RpcError::value_error(format!("invalid catalog publication manifest: {e}"))
        })?;
        let manifest = if value.get("tombstone").and_then(|v| v.as_bool()) == Some(true) {
            None
        } else {
            Some(serde_json::from_value(value).map_err(|e| {
                RpcError::value_error(format!("invalid catalog publication manifest: {e}"))
            })?)
        };
        Ok((manifest, Some(object.etag)))
    }

    fn put_manifest(
        &self,
        manifest: &CatalogPublicationManifest,
        condition: S3WriteCondition<'_>,
    ) -> Result<bool> {
        let bytes =
            serde_json::to_vec(manifest).map_err(|e| RpcError::value_error(e.to_string()))?;
        self.client.put_object(
            &self.bucket,
            &self.manifest_key(&manifest.cache_key),
            &bytes,
            "application/json",
            None,
            condition,
        )
    }
}

impl ExternalStorage for S3CatalogPublicationRepository {
    fn upload(&self, bytes: &[u8], compression: Compression) -> Result<UploadResult> {
        let object_id = format!(
            "{}objects/{:032x}.arrow",
            self.prefix,
            rand::random::<u128>()
        );
        let encoding = match compression {
            Compression::None => None,
            Compression::Zstd(_) => Some("zstd"),
        };
        if !self.client.put_object(
            &self.bucket,
            &object_id,
            bytes,
            "application/vnd.apache.arrow.stream",
            encoding,
            S3WriteCondition::Absent,
        )? {
            return Err(RpcError::runtime_error(
                "immutable catalog object name collision",
            ));
        }
        let url = self
            .client
            .presign_get(&self.bucket, &object_id, self.url_ttl_seconds)?;
        let now = now();
        self.locations.lock().unwrap().insert(
            url.clone(),
            PublicationLocation {
                object_id: Some(object_id),
                expires_at: Some(now + self.url_ttl_seconds as f64),
                retain_until: Some(now + self.retention_seconds as f64),
                stable: false,
            },
        );
        // The RPC publisher computes its own digest. Populate the storage
        // result correctly too for callers that use this adapter directly.
        let raw = match compression {
            Compression::None => bytes.to_vec(),
            Compression::Zstd(_) => {
                zstd::stream::decode_all(bytes).map_err(|e| RpcError::value_error(e.to_string()))?
            }
        };
        let sha256 = Sha256::digest(&raw)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        Ok(UploadResult { url, sha256 })
    }
}

impl CatalogPublicationRepository for S3CatalogPublicationRepository {
    fn profile(&self) -> &str {
        &self.profile
    }
    fn storage(&self) -> &dyn ExternalStorage {
        self
    }
    fn lookup(&self, key: &str) -> Result<Option<CatalogPublicationManifest>> {
        Ok(self.load(key)?.0)
    }

    fn commit_if_absent(
        &self,
        manifest: CatalogPublicationManifest,
    ) -> Result<CatalogPublicationManifest> {
        for _ in 0..8 {
            let (current, etag) = self.load(&manifest.cache_key)?;
            if let Some(current) = current {
                return Ok(current);
            }
            let condition = match etag.as_deref() {
                Some(etag) => S3WriteCondition::Matches(etag),
                None => S3WriteCondition::Absent,
            };
            if self.put_manifest(&manifest, condition)? {
                return Ok(manifest);
            }
        }
        Err(RpcError::runtime_error(
            "catalog publication manifest contention",
        ))
    }

    fn invalidate(&self, key: &str, manifest_id: &str) -> Result<bool> {
        let (current, etag) = self.load(key)?;
        if current.as_ref().map(|m| m.manifest_id.as_str()) != Some(manifest_id) {
            return Ok(false);
        }
        self.client.put_object(
            &self.bucket,
            &self.manifest_key(key),
            b"{\"tombstone\":true}",
            "application/json",
            None,
            S3WriteCondition::Matches(etag.as_deref().unwrap()),
        )
    }

    fn location(&self, reference: &ExternalRef) -> Result<PublicationLocation> {
        self.locations
            .lock()
            .unwrap()
            .remove(reference.url())
            .ok_or_else(|| RpcError::value_error("reference was not uploaded by this repository"))
    }

    fn renew(
        &self,
        manifest: &CatalogPublicationManifest,
    ) -> Result<Option<CatalogPublicationManifest>> {
        for _ in 0..8 {
            let (current, etag) = self.load(&manifest.cache_key)?;
            let Some(mut current) = current else {
                return Ok(None);
            };
            if current.manifest_id != manifest.manifest_id {
                return Ok(Some(current));
            }
            let (Some(object_id), Some(retain_until)) = (&current.object_id, current.retain_until)
            else {
                return Ok(None);
            };
            let remaining = retain_until - now();
            if !remaining.is_finite() || remaining <= 60.0 {
                return Ok(None);
            }
            let lifetime = self.url_ttl_seconds.min(remaining as u64);
            current.url = self.client.presign_get(&self.bucket, object_id, lifetime)?;
            current.expires_at = Some(now() + lifetime as f64);
            if self.put_manifest(
                &current,
                S3WriteCondition::Matches(etag.as_deref().unwrap()),
            )? {
                return Ok(Some(current));
            }
        }
        Err(RpcError::runtime_error(
            "catalog reference renewal contention",
        ))
    }
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[derive(Default)]
    struct Client {
        objects: Mutex<HashMap<String, (Vec<u8>, u64)>>,
    }
    impl CatalogS3Client for Client {
        fn get_object(&self, _: &str, key: &str, max: usize) -> Result<Option<S3VersionedObject>> {
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .map(|(bytes, version)| {
                    if bytes.len() > max {
                        return Err(RpcError::value_error("object exceeds read limit"));
                    }
                    Ok(S3VersionedObject {
                        bytes: bytes.clone(),
                        etag: version.to_string(),
                    })
                })
                .transpose()
        }
        fn put_object(
            &self,
            _: &str,
            key: &str,
            bytes: &[u8],
            _: &str,
            _: Option<&str>,
            condition: S3WriteCondition<'_>,
        ) -> Result<bool> {
            let mut objects = self.objects.lock().unwrap();
            let old = objects.get(key).map(|(_, version)| *version);
            let matches = match condition {
                S3WriteCondition::Unconditional => true,
                S3WriteCondition::Absent => old.is_none(),
                S3WriteCondition::Matches(etag) => {
                    old.map(|v| v.to_string()).as_deref() == Some(etag)
                }
            };
            if !matches {
                return Ok(false);
            }
            objects.insert(key.into(), (bytes.to_vec(), old.unwrap_or(0) + 1));
            Ok(true)
        }
        fn presign_get(&self, _: &str, key: &str, _: u64) -> Result<String> {
            Ok(format!("https://objects.example/{key}"))
        }
    }
    #[test]
    fn durable_discovery_cas_and_renewal() -> Result<()> {
        let client = Arc::new(Client::default());
        let repo = S3CatalogPublicationRepository::new(
            client.clone(),
            "bucket".into(),
            "catalog".into(),
            "test".into(),
            3600,
            86400,
        )?;
        let upload = repo.upload(b"payload", Compression::None)?;
        let location = repo.location(&ExternalRef::new(
            upload.url.clone(),
            Some(upload.sha256.clone()),
        )?)?;
        let manifest = CatalogPublicationManifest {
            cache_key: "key".into(),
            manifest_id: "generation".into(),
            protocol_name: "vgi.v3".into(),
            wire_revision: "3.0".into(),
            contract_hash: "hash".into(),
            codec_revision: "catalog-collections-1".into(),
            catalog_version: 1,
            etag: None,
            sha256: upload.sha256,
            raw_size: 7,
            stored_size: 7,
            url: upload.url,
            object_id: location.object_id,
            expires_at: location.expires_at,
            retain_until: location.retain_until,
            stable: false,
            format_version: 1,
            validator_revision: 1,
        };
        repo.commit_if_absent(manifest.clone())?;
        let recovered = S3CatalogPublicationRepository::new(
            client,
            "bucket".into(),
            "catalog".into(),
            "test".into(),
            3600,
            86400,
        )?;
        assert_eq!(recovered.lookup("key")?.unwrap().manifest_id, "generation");
        let mut losing = manifest.clone();
        losing.manifest_id = "losing".into();
        assert_eq!(
            recovered.commit_if_absent(losing)?.manifest_id,
            "generation"
        );
        assert!(recovered.renew(&manifest)?.is_some());
        assert!(!recovered.invalidate("key", "losing")?);
        assert!(recovered.invalidate("key", "generation")?);
        assert!(recovered.lookup("key")?.is_none());
        recovered.commit_if_absent(manifest)?;
        assert!(recovered.lookup("key")?.is_some());
        Ok(())
    }
}

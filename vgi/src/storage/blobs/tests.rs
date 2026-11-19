// Copyright 2025, 2026 Query Farm LLC - https://query.farm

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::storage::MemoryStorage;

/// The committed copy of the vgi extension's `test/vectors/storage_envelope_v1.json`.
const VECTORS: &str = include_str!("storage_envelope_v1.json");

fn unhex(s: &str) -> Vec<u8> {
    decode_hex(s).expect("hex")
}

fn vectors() -> serde_json::Value {
    serde_json::from_str(VECTORS).unwrap()
}

#[test]
fn envelope_vectors_inline() {
    let v = vectors();
    assert_eq!(v["pointer_fixed_bytes"], POINTER_FIXED);
    assert_eq!(v["tag_inline"], TAG_INLINE);
    assert_eq!(v["tag_pointer"], TAG_POINTER);
    assert_eq!(v["default_externalize_bytes"], DEFAULT_EXTERNALIZE_BYTES);
    for case in v["inline"].as_array().unwrap() {
        let value = unhex(case["value_hex"].as_str().unwrap());
        let envelope = unhex(case["envelope_hex"].as_str().unwrap());
        assert_eq!(encode_inline(&value), envelope, "{}", case["name"]);
        assert_eq!(
            decode(&envelope),
            Ok(Envelope::Inline(&value[..])),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn envelope_vectors_pointers() {
    for case in vectors()["pointers"].as_array().unwrap() {
        let blob = BlobRef {
            object_id: case["object_id"].as_str().unwrap().to_string(),
            size: case["size"].as_u64().unwrap(),
            sha256: case["sha256"].as_str().unwrap().to_string(),
        };
        let envelope = unhex(case["envelope_hex"].as_str().unwrap());
        assert_eq!(
            encode_pointer(&blob).as_deref(),
            Some(&envelope[..]),
            "{}",
            case["name"]
        );
        assert_eq!(
            decode(&envelope),
            Ok(Envelope::Pointer(blob)),
            "{}",
            case["name"]
        );
    }
}

#[test]
fn envelope_vectors_malformed() {
    for case in vectors()["malformed"].as_array().unwrap() {
        let envelope = unhex(case["envelope_hex"].as_str().unwrap());
        let err = decode(&envelope).unwrap_err();
        let kind = match err {
            EnvelopeError::Empty => "empty",
            EnvelopeError::UnknownTag(_) => "unknown_tag",
            EnvelopeError::MalformedPointer(_) => "malformed_pointer",
        };
        assert_eq!(kind, case["error"].as_str().unwrap(), "{}", case["name"]);
    }
}

#[test]
fn envelope_vectors_scopes_and_shards() {
    let v = vectors();
    for case in v["scope_execution_ids"]["cases"].as_array().unwrap() {
        let ident = unhex(case["ident_hex"].as_str().unwrap());
        assert_eq!(
            blob_execution_id(case["kind"].as_str().unwrap(), &ident),
            case["execution_id"].as_str().unwrap()
        );
    }
    for case in v["scope_shards"]["cases"].as_array().unwrap() {
        assert_eq!(
            blob_shard(case["shard_key"].as_str().unwrap()),
            case["shard"].as_str().unwrap()
        );
    }
    // The shard a Rust scope actually uses is inside the charset unchanged.
    let shard = shard_key_for_scope(b"any scope");
    assert_eq!(blob_shard(&shard), shard);
}

/// The committed vectors must be the vgi extension's, byte for byte, when a
/// checkout is at hand (`VGI_SRC`, as the integration lanes set it).
#[test]
fn committed_vectors_match_the_extension() {
    let Ok(src) = std::env::var("VGI_SRC") else {
        return;
    };
    let path = std::path::Path::new(&src).join("test/vectors/storage_envelope_v1.json");
    let Ok(upstream) = std::fs::read_to_string(&path) else {
        return;
    };
    assert_eq!(
        upstream,
        VECTORS,
        "{} drifted from the committed copy",
        path.display()
    );
}

// ---------------------------------------------------------------------------
// The wrapper, over an in-memory blob store that verifies like the vendor
// ---------------------------------------------------------------------------

type StoredBlob = (Vec<u8>, String, String);

#[derive(Default)]
struct FakeBlobs {
    /// object id -> (bytes, shard, execution id)
    objects: Mutex<HashMap<String, StoredBlob>>,
    puts: AtomicUsize,
    gets: AtomicUsize,
    deletes: Mutex<Vec<String>>,
    scope_deletes: Mutex<Vec<(String, String)>>,
    /// `corrupt` / `missing` the next download.
    fault: Mutex<Option<&'static str>>,
    next: AtomicUsize,
}

impl BlobStore for FakeBlobs {
    fn put_bytes(
        &self,
        data: &[u8],
        shard: &str,
        exec: &str,
        _key: &str,
    ) -> Result<BlobRef, BlobError> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        let id = format!("{:032x}", self.next.fetch_add(1, Ordering::SeqCst));
        self.objects.lock().unwrap().insert(
            id.clone(),
            (data.to_vec(), shard.to_string(), exec.to_string()),
        );
        Ok(BlobRef {
            object_id: id,
            size: data.len() as u64,
            sha256: hex(&Sha256::digest(data)),
        })
    }

    fn get_bytes(&self, blob: &BlobRef) -> Result<Vec<u8>, BlobError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        let err = |kind, message: &str| BlobError {
            kind,
            message: message.to_string(),
        };
        let mut data = match self.objects.lock().unwrap().get(&blob.object_id) {
            Some((d, _, _)) => d.clone(),
            None => return Err(err(BlobErrorKind::Missing, "not found")),
        };
        match self.fault.lock().unwrap().take() {
            Some("missing") => return Err(err(BlobErrorKind::Missing, "HTTP 404 NoSuchKey")),
            Some("corrupt") => data.iter_mut().for_each(|b| *b ^= 0xff),
            _ => {}
        }
        if data.len() as u64 != blob.size {
            return Err(err(BlobErrorKind::SizeMismatch, "size"));
        }
        if hex(&Sha256::digest(&data)) != blob.sha256 {
            return Err(err(BlobErrorKind::Sha256Mismatch, "sha"));
        }
        Ok(data)
    }

    fn delete(&self, ids: &[String]) -> Result<(), BlobError> {
        let mut objects = self.objects.lock().unwrap();
        for id in ids {
            objects.remove(id);
        }
        self.deletes.lock().unwrap().extend(ids.iter().cloned());
        Ok(())
    }

    fn delete_scope(&self, shard: &str, exec: &str) -> Result<(), BlobError> {
        self.objects
            .lock()
            .unwrap()
            .retain(|_, (_, s, e)| !(s == shard && e == exec));
        self.scope_deletes
            .lock()
            .unwrap()
            .push((shard.to_string(), exec.to_string()));
        Ok(())
    }
}

fn wrapped(threshold: usize) -> (Arc<MemoryStorage>, Arc<FakeBlobs>, ExternalizingStorage) {
    let base = Arc::new(MemoryStorage::new());
    let blobs = Arc::new(FakeBlobs::default());
    let store = ExternalizingStorage::new(base.clone(), Some(blobs.clone()), threshold, None);
    (base, blobs, store)
}

/// The panic payload a failing storage call raises, as the client sees it.
fn storage_error(f: impl FnOnce()) -> RpcError {
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).expect_err("must fail");
    *payload.downcast::<RpcError>().expect("an RpcError payload")
}

#[test]
fn wrapper_passes_the_backend_conformance_suite() {
    // Every value through the vendor, then every value inline.
    let (_, blobs, store) = wrapped(1);
    crate::storage::conformance::run_all(&store);
    assert!(blobs.puts.load(Ordering::SeqCst) > 0);
    let (_, blobs, store) = wrapped(usize::MAX);
    crate::storage::conformance::run_all(&store);
    assert_eq!(blobs.puts.load(Ordering::SeqCst), 0);
}

#[test]
fn threshold_boundary_counts_bytes() {
    let threshold = 4096;
    let (base, blobs, store) = wrapped(threshold);
    let items: Vec<(Vec<u8>, Vec<u8>)> = [threshold - 1, threshold, threshold + 1, 0]
        .iter()
        .enumerate()
        .map(|(i, n)| (vec![i as u8], vec![b'x'; *n]))
        .collect();
    store.kv_put_many(b"exec", &items);
    assert_eq!(
        blobs.puts.load(Ordering::SeqCst),
        2,
        "only the two at or over"
    );
    let raw = base.kv_get_many(
        b"exec",
        &items.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
    );
    let tags: Vec<u8> = raw.iter().map(|v| v.as_ref().unwrap()[0]).collect();
    assert_eq!(tags, vec![TAG_INLINE, TAG_POINTER, TAG_POINTER, TAG_INLINE]);
    assert!(raw[1].as_ref().unwrap().len() < 100, "a pointer is small");
    let back = store.kv_get_many(
        b"exec",
        &items.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
    );
    assert_eq!(blobs.gets.load(Ordering::SeqCst), 2, "one get per blob");
    for ((_, v), got) in items.iter().zip(back) {
        assert_eq!(got.as_deref(), Some(&v[..]));
    }
    // A missing key passes through as None.
    assert_eq!(store.kv_get_many(b"exec", &[b"nope".to_vec()]), vec![None]);
}

#[test]
fn blobs_are_scoped_and_cleaned_up() {
    let (_, blobs, store) = wrapped(8);
    store.kv_put(b"scope-1", b"k", &[7u8; 64]);
    store.queue_push(b"scope-1", &[vec![1u8; 64]]);
    let scopes: Vec<(String, String)> = blobs
        .objects
        .lock()
        .unwrap()
        .values()
        .map(|(_, s, e)| (s.clone(), e.clone()))
        .collect();
    let shard = shard_key_for_scope(b"scope-1");
    assert!(scopes.contains(&(shard.clone(), blob_execution_id("s", b"scope-1"))));
    assert!(scopes.contains(&(shard.clone(), blob_execution_id("q", b"scope-1"))));

    // queue_pop deletes the blob it claimed.
    assert_eq!(store.queue_pop(b"scope-1"), Some(vec![1u8; 64]));
    store.flush_blob_cleanup();
    assert_eq!(blobs.deletes.lock().unwrap().len(), 1);

    // clear deletes both scopes.
    store.clear(b"scope-1");
    store.flush_blob_cleanup();
    let deleted = blobs.scope_deletes.lock().unwrap().clone();
    assert!(deleted.contains(&(shard.clone(), blob_execution_id("s", b"scope-1"))));
    assert!(deleted.contains(&(shard, blob_execution_id("q", b"scope-1"))));
    assert!(blobs.objects.lock().unwrap().is_empty());
}

#[test]
fn unreadable_blobs_fail_with_the_contract_messages() {
    let integrity = regex::Regex::new(r"(?s).*storage blob.*(sha256|size) mismatch.*").unwrap();
    let missing = regex::Regex::new(r"(?s).*storage blob.*(missing|not found).*").unwrap();
    let (_, blobs, store) = wrapped(8);
    store.append(b"e", b"ns", b"", vec![3u8; 100]);

    *blobs.fault.lock().unwrap() = Some("corrupt");
    let err = storage_error(|| {
        store.scan(b"e", b"ns", b"", -1, 10);
    });
    assert!(integrity.is_match(&err.message), "{}", err.message);
    assert!(err.message.starts_with("storage blob "), "{}", err.message);

    *blobs.fault.lock().unwrap() = Some("missing");
    let err = storage_error(|| {
        store.scan(b"e", b"ns", b"", -1, 10);
    });
    assert!(missing.is_match(&err.message), "{}", err.message);

    // A pointer whose reference disagrees with the bytes in size.
    let raw = store.base.scan(b"e", b"ns", b"", -1, 10)[0].1.clone();
    let Ok(Envelope::Pointer(mut blob)) = decode(&raw) else {
        panic!("expected a pointer")
    };
    blob.size += 1;
    store
        .base
        .kv_put(b"e", b"lying", &encode_pointer(&blob).unwrap());
    let err = storage_error(|| {
        store.kv_get(b"e", b"lying");
    });
    assert!(integrity.is_match(&err.message), "{}", err.message);

    // With no fault the value reads back.
    assert_eq!(store.scan(b"e", b"ns", b"", -1, 10)[0].1, vec![3u8; 100]);
}

#[test]
fn strict_decoding_and_no_vendor() {
    let base = Arc::new(MemoryStorage::new());
    let store = ExternalizingStorage::new(base.clone(), None, 8, None);
    store.kv_put(b"e", b"big", &[1u8; 100]);
    assert_eq!(
        base.kv_get(b"e", b"big").unwrap()[0],
        TAG_INLINE,
        "no vendor: inline"
    );
    assert_eq!(store.kv_get(b"e", b"big"), Some(vec![1u8; 100]));

    base.kv_put(b"e", b"raw", b"\x02written without the wrapper");
    assert!(storage_error(|| {
        store.kv_get(b"e", b"raw");
    })
    .message
    .contains("unknown envelope tag 0x02"));
    base.kv_put(b"e", b"empty", b"");
    assert!(storage_error(|| {
        store.kv_get(b"e", b"empty");
    })
    .message
    .contains("empty stored value"));

    let pointer = encode_pointer(&BlobRef {
        object_id: "abc".into(),
        size: 3,
        sha256: hex(&Sha256::digest(b"xyz")),
    })
    .unwrap();
    base.kv_put(b"e", b"ptr", &pointer);
    assert!(storage_error(|| {
        store.kv_get(b"e", b"ptr");
    })
    .message
    .contains("VGI_BLOB_VENDOR_URL is not configured"));
}

#[test]
fn a_capped_backend_without_a_vendor_refuses_oversize_values() {
    let store = ExternalizingStorage::new(Arc::new(MemoryStorage::new()), None, 16, Some(16));
    store.kv_put(b"e", b"fits", &[0u8; 15]);
    let err = storage_error(|| store.kv_put(b"e", b"big", &[0u8; 16]));
    assert!(
        err.message.contains("VGI_BLOB_VENDOR_URL"),
        "{}",
        err.message
    );
    assert!(err.message.contains("16-byte limit"), "{}", err.message);
}

#[test]
#[should_panic(expected = "externalize threshold")]
fn threshold_over_the_cap_is_refused() {
    ExternalizingStorage::new(Arc::new(MemoryStorage::new()), None, 17, Some(16));
}

#[test]
fn many_values_in_one_call_round_trip() {
    let (_, blobs, store) = wrapped(64);
    let values: Vec<Vec<u8>> = (0..300).map(|i| vec![(i % 256) as u8; 10 + i]).collect();
    store.append_many(b"m", b"ns", b"k", values.clone());
    let back: Vec<Vec<u8>> = store
        .scan(b"m", b"ns", b"k", -1, usize::MAX)
        .into_iter()
        .map(|(_, v)| v)
        .collect();
    assert_eq!(back, values);
    assert_eq!(blobs.puts.load(Ordering::SeqCst), 300 - 54);
}

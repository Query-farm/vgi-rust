// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Sealed framework payloads: authenticated encryption of the context the
//! framework itself writes to [`FunctionStorage`].
//!
//! A buffered table function's init record (output/input schema, call
//! arguments, the resolved secrets, the `COPY ... TO` destination and the
//! attach scope), an aggregate's bind-time arguments and a streaming
//! aggregate's session all live in shared storage so any worker process can
//! serve any RPC of the execution. That storage may be a SQLite file, a
//! directory, or a service reached over the network, so none of it may sit
//! there in plaintext, and nothing read back may be trusted unauthenticated:
//! a rewritten `COPY ... TO` path would send a writer's output, with the
//! caller's credentials, wherever the attacker liked.
//!
//! # Sealed framework payload v1 (the same format in every VGI SDK)
//!
//! - `K = HKDF-SHA256(ikm, salt = "", info = "vgi.storage.payload.v1", L = 32)`.
//!   `ikm` is the worker's signing key exactly as it resolved it (the UTF-8
//!   bytes of `VGI_SIGNING_KEY`, or the key an HTTP worker generated). A
//!   worker with no signing key (stdio, unix, TCP) uses the host-local key
//!   file every VGI worker process of the user on the host shares
//!   ([`crate::attach_options::host_local_key_path`]), because one query's
//!   RPCs are spread over several pooled worker processes.
//! - AES-256-GCM, random 12-byte nonce, 16-byte tag.
//! - `AAD = "vgi.storage.payload.v1" || u32be(len) || shard_key ||
//!   u32be(len) || scope_id || u32be(len) || namespace || u32be(len) || key`,
//!   so a payload copied to another execution, key or shard fails to open.
//! - Wire: `0x01 || nonce(12) || ciphertext || tag(16)`.
//!
//! Opening never falls back to plaintext: an unknown version byte, a short
//! envelope or a failed tag is a [`SealError`].
//!
//! The Rust storage trait has no attach shard, so a Rust slot's coordinates
//! are the ones it is actually stored at: `shard_key` is
//! [`shard_key_for_scope`] of the scope (the
//! Durable Object routing key the `cloudflare-do` backend uses), `namespace`
//! is [`KV_NAMESPACE`] and `key` is the key/value key.
//!
//! Data a function writes for itself (aggregate group states, buffered
//! partials, window partitions, finalize output, queue items) is the
//! function's own and is not sealed.

use std::fmt;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};

use super::{shard_key_for_scope, FunctionStorage};

/// HKDF `info` label, and the AAD prefix.
pub const INFO: &[u8] = b"vgi.storage.payload.v1";
/// The envelope's version byte.
pub const VERSION: u8 = 0x01;
/// The namespace a key/value slot occupies in the AAD (and, on the
/// `cloudflare-do` backend, on the server).
pub const KV_NAMESPACE: &[u8] = b"kv";

const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

/// A sealed payload that could not be opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealError(pub String);

impl fmt::Display for SealError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SealError {}

impl From<SealError> for vgi_rpc::RpcError {
    fn from(e: SealError) -> Self {
        vgi_rpc::RpcError::runtime_error(e.0)
    }
}

/// `HKDF-SHA256(ikm, salt = "", info = "vgi.storage.payload.v1", 32)`.
pub fn derive_key(ikm: &[u8]) -> [u8; 32] {
    // An empty salt is HashLen zero bytes (RFC 5869 §2.2); HMAC zero-pads its
    // key, so the two are the same PRK.
    crate::attach_options::hkdf_sha256(ikm, INFO)
}

/// Where a payload lives; bound into the AAD.
#[derive(Debug, Clone, Copy)]
pub struct Slot<'a> {
    /// The shard (Durable Object routing key) the slot lives in.
    pub shard_key: &'a str,
    /// The execution (or transaction) id.
    pub scope_id: &'a [u8],
    /// The storage namespace.
    pub namespace: &'a [u8],
    /// The key within the namespace.
    pub key: &'a [u8],
}

fn push_lp(out: &mut Vec<u8>, b: &[u8]) {
    let len = u32::try_from(b.len()).expect("sealed-slot component exceeds 4 GiB");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(b);
}

/// The associated data binding a payload to its slot.
pub fn aad(slot: &Slot<'_>) -> Vec<u8> {
    let mut out = Vec::with_capacity(
        INFO.len()
            + 16
            + slot.shard_key.len()
            + slot.scope_id.len()
            + slot.namespace.len()
            + slot.key.len(),
    );
    out.extend_from_slice(INFO);
    push_lp(&mut out, slot.shard_key.as_bytes());
    push_lp(&mut out, slot.scope_id);
    push_lp(&mut out, slot.namespace);
    push_lp(&mut out, slot.key);
    out
}

/// Seal `plaintext` for `slot` under an explicit nonce (test vectors only;
/// [`seal`] draws a random one).
pub fn seal_with_nonce(
    key: &[u8; 32],
    nonce: &[u8; NONCE_LEN],
    slot: &Slot<'_>,
    plaintext: &[u8],
) -> Vec<u8> {
    let cipher = Aes256Gcm::new(key.into());
    let aad = aad(slot);
    let body = cipher
        .encrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .expect("AES-256-GCM encryption cannot fail for in-memory input");
    let mut out = Vec::with_capacity(1 + NONCE_LEN + body.len());
    out.push(VERSION);
    out.extend_from_slice(nonce);
    out.extend_from_slice(&body);
    out
}

/// Seal `plaintext` for `slot` with a fresh random nonce.
pub fn seal(key: &[u8; 32], slot: &Slot<'_>, plaintext: &[u8]) -> Vec<u8> {
    seal_with_nonce(key, &rand::random::<[u8; NONCE_LEN]>(), slot, plaintext)
}

/// Open an envelope from [`seal`] for the same slot and key.
pub fn open(key: &[u8; 32], slot: &Slot<'_>, blob: &[u8]) -> Result<Vec<u8>, SealError> {
    let what = || {
        format!(
            "framework storage payload {:?} for execution {}",
            String::from_utf8_lossy(slot.key),
            super::hex(slot.scope_id)
        )
    };
    if blob.len() < 1 + NONCE_LEN + TAG_LEN {
        return Err(SealError(format!(
            "{} is too short to be a sealed payload",
            what()
        )));
    }
    if blob[0] != VERSION {
        return Err(SealError(format!(
            "{} has unknown seal version {:#04x}",
            what(),
            blob[0]
        )));
    }
    let cipher = Aes256Gcm::new(key.into());
    let aad = aad(slot);
    cipher
        .decrypt(
            Nonce::from_slice(&blob[1..1 + NONCE_LEN]),
            Payload {
                msg: &blob[1 + NONCE_LEN..],
                aad: &aad,
            },
        )
        .map_err(|_| {
            SealError(format!(
                "{} failed authentication (tampered, moved, or sealed under another key)",
                what()
            ))
        })
}

/// Seal `plaintext` and store it at the key/value slot `(scope, key)`.
pub fn sealed_kv_put(
    store: &dyn FunctionStorage,
    seal_key: &[u8; 32],
    scope: &[u8],
    key: &[u8],
    plaintext: &[u8],
) {
    let shard = shard_key_for_scope(scope);
    let slot = Slot {
        shard_key: &shard,
        scope_id: scope,
        namespace: KV_NAMESPACE,
        key,
    };
    store.kv_put(scope, key, &seal(seal_key, &slot, plaintext));
}

/// Read and open the sealed key/value slot `(scope, key)`: `Ok(None)` when
/// absent, an error when present but not openable.
pub fn sealed_kv_get(
    store: &dyn FunctionStorage,
    seal_key: &[u8; 32],
    scope: &[u8],
    key: &[u8],
) -> Result<Option<Vec<u8>>, SealError> {
    let Some(blob) = store.kv_get(scope, key) else {
        return Ok(None);
    };
    open_kv(seal_key, scope, key, &blob).map(Some)
}

/// Open a sealed value read from the key/value slot `(scope, key)`.
pub fn open_kv(
    seal_key: &[u8; 32],
    scope: &[u8],
    key: &[u8],
    blob: &[u8],
) -> Result<Vec<u8>, SealError> {
    let shard = shard_key_for_scope(scope);
    open(
        seal_key,
        &Slot {
            shard_key: &shard,
            scope_id: scope,
            namespace: KV_NAMESPACE,
            key,
        },
        blob,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;

    fn hexs(b: &[u8]) -> String {
        super::super::hex(b)
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
            .collect()
    }

    const SLOT: Slot<'static> = Slot {
        shard_key: "att-0123",
        scope_id: &[1u8; 16],
        namespace: b"_vgi/buffering_init",
        key: b"k",
    };

    /// The cross-SDK vector (vgi-python `tests/test_storage_seal.py`
    /// `VECTOR`): Go, Java, TypeScript, Python and Rust must agree on these
    /// bytes. AES-256-GCM is deterministic for a fixed key and nonce.
    #[test]
    fn cross_sdk_vector() {
        let ikm = b"vgi-test-signing-key-0123456789abcdef";
        let scope: Vec<u8> = (0u8..16).collect();
        // vgi-python's BoundStorage.pack_int_key(-1): big-endian i64.
        let key = (-1i64).to_be_bytes();
        let slot = Slot {
            shard_key: "att-00112233445566778899aabbccddeeff",
            scope_id: &scope,
            namespace: b"_vgi/buffering_init",
            key: &key,
        };
        let nonce: [u8; 12] = core::array::from_fn(|i| i as u8);
        let derived = derive_key(ikm);
        assert_eq!(
            hexs(&derived),
            "f5f759ad2009e91cf4b963a20b524a800224d2ac4e01472396dc1a2dab78bea9"
        );
        assert_eq!(
            hexs(&aad(&slot)),
            "7667692e73746f726167652e7061796c6f61642e7631000000246174742d30303131323233333434353536363737383839396161\
             6262636364646565666600000010000102030405060708090a0b0c0d0e0f000000135f7667692f627566666572696e675f696e6974\
             00000008ffffffffffffffff"
        );
        let wire_hex = "01000102030405060708090a0bc79698647483a87eb533cf7332d1c527275cc7470dc6a3bdf1f72f624a97e246416d4de55281bd0eee\
                        6d6491796a40";
        let wire = seal_with_nonce(&derived, &nonce, &slot, b"vgi sealed framework payload v1");
        assert_eq!(hexs(&wire), wire_hex);
        assert_eq!(
            open(&derived, &slot, &unhex(wire_hex)).unwrap(),
            b"vgi sealed framework payload v1"
        );
    }

    /// `VGI_SIGNING_KEY` is used as its configured UTF-8 bytes: a 64-hex
    /// value is 64 ASCII bytes of ikm, not the 32 bytes it would hex-decode
    /// to. Checked against vgi-python's `vgi._storage_seal.seal_with` for a
    /// Rust-shaped slot (`exe-` shard of the scope, namespace `kv`, the
    /// buffering-init key), so the two SDKs agree on a real Rust payload too.
    #[test]
    fn hex_looking_signing_key_is_its_ascii_bytes() {
        let configured = "0123456789abcdef".repeat(4);
        let ikm = crate::worker::signing_key_bytes(Some(configured.clone())).unwrap();
        assert_eq!(ikm, configured.as_bytes());
        assert_eq!(ikm.len(), 64);
        assert_eq!(crate::worker::signing_key_bytes(Some(String::new())), None);
        let key = derive_key(&ikm);
        assert_eq!(
            hexs(&key),
            "bcc6b0475564abef4aacbbd08c02a68829f72b91dd31733801a81ed3a8de0006"
        );
        assert_ne!(key, derive_key(&unhex(&configured)));

        let scope: Vec<u8> = [b"vgi-exec-".as_slice(), &(0u8..20).collect::<Vec<u8>>()].concat();
        let shard = shard_key_for_scope(&scope);
        assert_eq!(shard, "exe-6fdeabb734f76d4b8f743992d7dba095");
        let slot = Slot {
            shard_key: &shard,
            scope_id: &scope,
            namespace: KV_NAMESPACE,
            key: b"_vgi/buffering_init",
        };
        let wire = seal_with_nonce(&key, &[7; 12], &slot, br#"{"copy_to":"/safe/out"}"#);
        assert_eq!(
            hexs(&wire),
            "010707070707070707070707073e26c64f1af326d961d5920d1fcd1ee4d2e3868d7e4397ade00b12c5a38a2f79154061c11773d6"
        );
    }

    #[test]
    fn round_trip_hides_plaintext() {
        let k = derive_key(b"k");
        let sealed = seal(&k, &SLOT, b"secret context");
        assert_eq!(sealed[0], VERSION);
        assert_eq!(sealed.len(), 1 + 12 + b"secret context".len() + 16);
        assert!(!sealed
            .windows(b"secret context".len())
            .any(|w| w == b"secret context"));
        assert_eq!(open(&k, &SLOT, &sealed).unwrap(), b"secret context");
        // Fresh nonce per seal.
        assert_ne!(sealed, seal(&k, &SLOT, b"secret context"));
    }

    #[test]
    fn tampered_payload_is_refused() {
        let k = derive_key(b"k");
        let sealed = seal(&k, &SLOT, b"payload");
        for i in 1..sealed.len() {
            let mut t = sealed.clone();
            t[i] ^= 1;
            let err = open(&k, &SLOT, &t).unwrap_err();
            assert!(err.0.contains("failed authentication"), "{err}");
        }
    }

    #[test]
    fn moved_payload_is_refused() {
        let k = derive_key(b"k");
        let sealed = seal(&k, &SLOT, b"payload");
        let other_scope = [2u8; 16];
        for slot in [
            Slot {
                scope_id: &other_scope,
                ..SLOT
            },
            Slot {
                key: b"other",
                ..SLOT
            },
            Slot {
                namespace: b"kv",
                ..SLOT
            },
            Slot {
                shard_key: "att-9999",
                ..SLOT
            },
        ] {
            assert!(open(&k, &slot, &sealed).is_err(), "{slot:?}");
        }
    }

    #[test]
    fn wrong_key_is_refused() {
        let sealed = seal(&derive_key(b"k"), &SLOT, b"payload");
        assert!(open(&derive_key(b"z"), &SLOT, &sealed).is_err());
    }

    #[test]
    fn unknown_version_short_and_plaintext_are_refused() {
        let k = derive_key(b"k");
        let mut sealed = seal(&k, &SLOT, b"payload");
        sealed[0] = 0x02;
        assert!(open(&k, &SLOT, &sealed)
            .unwrap_err()
            .0
            .contains("unknown seal version"));
        assert!(open(&k, &SLOT, &sealed[..20])
            .unwrap_err()
            .0
            .contains("too short"));
        assert!(open(&k, &SLOT, b"").is_err());
        // An unsealed value written by an older release is never passed through.
        let plain = vec![0x01; 64];
        assert!(open(&k, &SLOT, &plain).is_err());
    }

    #[test]
    fn kv_helpers_bind_scope_and_key() {
        let store = MemoryStorage::new();
        let k = derive_key(b"k");
        sealed_kv_put(&store, &k, b"exec-a", b"_vgi/x", b"hello");
        assert_eq!(
            sealed_kv_get(&store, &k, b"exec-a", b"_vgi/x").unwrap(),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            sealed_kv_get(&store, &k, b"exec-a", b"_vgi/y").unwrap(),
            None
        );
        // Copying the envelope to another execution does not carry it over.
        let blob = store.kv_get(b"exec-a", b"_vgi/x").unwrap();
        store.kv_put(b"exec-b", b"_vgi/x", &blob);
        assert!(sealed_kv_get(&store, &k, b"exec-b", b"_vgi/x").is_err());
    }
}

// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Cross-SDK conformance for the split-token envelope.
//!
//! The envelope is the one part of the splits change where five independent
//! implementations can silently diverge AND where diverging is a vulnerability.
//! Behavioural tests miss that: each SDK is self-consistent, so a disagreement
//! on `anchor_len` endianness or fingerprint truncation only surfaces when a
//! token crosses SDKs. These vectors are byte-level and shared — every SDK
//! parses them and reproduces the deterministic ones byte-for-byte.

use std::fs;
use std::path::PathBuf;

use serde_json::Value;
use vgi::split_token::{
    build_split_token, open_split_token, SplitTokenError, SPLIT_TOKEN_FORMAT_VERSION,
};

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data/split_tokens")
}

fn manifest() -> Value {
    let raw = fs::read_to_string(fixtures_dir().join("manifest.json"))
        .expect("reading split-token manifest");
    serde_json::from_str(&raw).expect("parsing split-token manifest")
}

fn from_hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("bad hex"))
        .collect()
}

#[test]
fn shared_vectors_reach_their_recorded_verdict() {
    let m = manifest();
    assert_eq!(
        m["format_version"].as_u64().unwrap() as u8,
        SPLIT_TOKEN_FORMAT_VERSION,
        "fixture format_version disagrees with this SDK",
    );
    let key = from_hex(m["key_hex"].as_str().unwrap());
    let fingerprint = from_hex(m["fingerprint_hex"].as_str().unwrap());
    let anchor = from_hex(m["anchor_hex"].as_str().unwrap());
    let payload = m["payload"].as_str().unwrap().as_bytes().to_vec();

    for case in m["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let verdict = case["verdict"].as_str().unwrap();
        let note = case["note"].as_str().unwrap_or("");
        let token = fs::read(fixtures_dir().join(format!("{name}.bin")))
            .unwrap_or_else(|e| panic!("reading fixture {name}: {e}"));

        // The manifest states the key state rather than each SDK inferring it:
        // the alg:none vector is a structurally VALID unsealed token whose whole
        // point is that a KEYED worker refuses it, so guessing from the token
        // would test the opposite of the rule.
        let worker_key = if case["worker_keyed"].as_bool().unwrap_or(false) {
            Some(key.as_slice())
        } else {
            None
        };

        let result = open_split_token(&token, worker_key, None, Some(&fingerprint), Some(&anchor));

        if verdict == "ok" {
            let opened =
                result.unwrap_or_else(|e| panic!("{name}: expected accept, got {e} ({note})"));
            assert_eq!(opened, payload, "{name}: payload mismatch");
            continue;
        }

        let err = result.expect_err(&format!(
            "{name}: expected {verdict}, was ACCEPTED ({note})"
        ));
        assert_eq!(err.kind(), verdict, "{name}: wrong error kind ({note})");
    }
}

#[test]
fn deterministic_vectors_are_reproduced_byte_for_byte() {
    // Proves the STAMPING side agrees too: a parser can be permissive enough to
    // accept another SDK's bytes while emitting bytes that SDK would reject.
    // Only the unsealed vector applies — a sealed token carries a random nonce.
    let m = manifest();
    let fingerprint = from_hex(m["fingerprint_hex"].as_str().unwrap());
    let anchor = from_hex(m["anchor_hex"].as_str().unwrap());
    let payload = m["payload"].as_str().unwrap().as_bytes().to_vec();

    let want = fs::read(fixtures_dir().join("valid_unsealed.bin")).unwrap();
    let got = build_split_token(&payload, &fingerprint, &anchor, None, None).unwrap();
    assert_eq!(
        got, want,
        "valid_unsealed bytes differ from the shared vector"
    );
}

#[test]
fn a_keyed_worker_refuses_an_unsealed_token() {
    // The alg:none rule stated directly rather than via a fixture name, because
    // it is the rule most likely to be "simplified" away by a later reader:
    // flags is attacker-controlled plaintext, so a parser that trusts bit 0 lets
    // any caller forge a split against a fully-keyed worker.
    let key = [0x2au8; 32];
    let fingerprint = [0x07u8; 16];
    let anchor = 47i64.to_le_bytes();

    let forged = build_split_token(b"file=evil", &fingerprint, &anchor, None, None).unwrap();
    assert!(
        open_split_token(&forged, Some(&key), None, Some(&fingerprint), None).is_err(),
        "a keyed worker accepted an UNSEALED token: alg:none downgrade",
    );

    let sealed = build_split_token(b"file=ok", &fingerprint, &anchor, Some(&key), None).unwrap();
    assert!(
        open_split_token(&sealed, None, None, Some(&fingerprint), None).is_err(),
        "a keyless worker claimed to open a SEALED token",
    );

    let opened = open_split_token(&sealed, Some(&key), None, Some(&fingerprint), None).unwrap();
    assert_eq!(opened, b"file=ok");

    let wrong = [0x2bu8; 32];
    assert!(
        open_split_token(&sealed, Some(&wrong), None, Some(&fingerprint), None).is_err(),
        "a token opened under the WRONG key",
    );
}

#[test]
fn a_token_is_bound_to_the_principal_it_was_minted_for() {
    // Dropping this while keeping it on attach would be a regression, and a
    // split token names data (files, offsets, tenant partitions).
    let key = [0x11u8; 32];
    let fingerprint = [0x05u8; 16];
    let anchor = 1i64.to_le_bytes();

    let alice = Some(("test", "alice"));
    let bob = Some(("test", "bob"));

    let token =
        build_split_token(b"tenant=alice", &fingerprint, &anchor, Some(&key), alice).unwrap();
    assert!(
        open_split_token(&token, Some(&key), alice, Some(&fingerprint), None).is_ok(),
        "alice could not redeem her own split",
    );
    assert!(
        open_split_token(&token, Some(&key), bob, Some(&fingerprint), None).is_err(),
        "bob redeemed a split minted for alice",
    );
}

#[test]
fn expiry_and_invalidity_are_distinguishable() {
    // Only one of them means "re-run the query", and keeping the anchor in the
    // PLAINTEXT header is what makes the distinction expressible at all.
    let fingerprint = [0x09u8; 16];
    let old = 47i64.to_le_bytes();
    let current = 48i64.to_le_bytes();

    let token = build_split_token(b"file=1", &fingerprint, &old, None, None).unwrap();

    let err = open_split_token(&token, None, None, Some(&fingerprint), Some(&current)).unwrap_err();
    assert!(
        matches!(err, SplitTokenError::SnapshotExpired(_)),
        "got {err:?}"
    );

    let other = [0x0au8; 16];
    let err = open_split_token(&token, None, None, Some(&other), Some(&old)).unwrap_err();
    assert!(matches!(err, SplitTokenError::Invalid(_)), "got {err:?}");
}

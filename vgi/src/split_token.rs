// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Split-token envelope: the framework's wrapper around a worker's split payload.
//!
//! A split token *names* a unit of scan work so a distributed engine can
//! re-request exactly the work it was handed. The worker supplies only the
//! payload; everything around it is stamped here, so an author cannot forget the
//! consistency anchor or mis-bind the fingerprint, and never writes crypto.
//!
//! Layout (little-endian, fixed prefix) — byte-identical across every SDK:
//!
//! ```text
//! offset  size  field
//! 0       1     format_version      currently 1
//! 1       1     flags               bit0 = payload_sealed; bits 1-7 reserved, MUST be 0
//! 2       2     anchor_len          u16 LE
//! 4       16    bind_fingerprint    truncated SHA-256 of the bind identity
//! 20      var   consistency_anchor  anchor_len bytes
//! 20+n    var   payload             the worker's own bytes
//! ```
//!
//! **The header is plaintext on every transport; only the payload is sealed.**
//! That is not a preference: a worker has no signing key on subprocess and unix,
//! which is DuckDB's primary path, so a header readable only through AEAD would
//! be unreadable exactly where DuckDB runs. It also matters for streaming — a
//! checkpointed position must survive key rotation.

use sha2::{Digest, Sha256};

/// Envelope format version. Checked unconditionally, before anything else.
pub const SPLIT_TOKEN_FORMAT_VERSION: u8 = 1;

/// bit0 of `flags`: the payload is AEAD-sealed rather than plaintext.
const FLAG_PAYLOAD_SEALED: u8 = 0x01;

/// bits 1-7 are reserved and MUST be zero; a set bit is a forward-compat violation.
const RESERVED_FLAGS_MASK: u8 = 0xFE;

const FINGERPRINT_LEN: usize = 16;
const HEADER_LEN: usize = 4 + FINGERPRINT_LEN;

/// Matches the Python default (`crypto.seal_bytes` version=1) so a token sealed
/// by one SDK opens in another.
const SEAL_VERSION: u8 = 1;

const AAD_PREFIX: &[u8] = b"vgi.split_token.v1\x00";

/// Why a split token was refused.
///
/// The kind matters to a connector: only `SnapshotExpired` means "re-run the
/// query", and neither kind is retriable in place. Keeping the anchor in the
/// PLAINTEXT header rather than in the AAD is what makes the distinction
/// expressible — inside the AAD both collapse into one tag-check failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SplitTokenError {
    /// Malformed, bound to a different bind, or forged. Non-retriable, and
    /// distinct from expiry: re-running the query would mint the same token.
    Invalid(String),
    /// The consistency anchor this token names is gone.
    SnapshotExpired(String),
    /// A transaction-scoped token redeemed after commit or rollback.
    TransactionEnded(String),
}

impl SplitTokenError {
    /// The stable error-kind string, identical across SDKs.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "SPLIT_TOKEN_INVALID",
            Self::SnapshotExpired(_) => "SPLIT_SNAPSHOT_EXPIRED",
            Self::TransactionEnded(_) => "SPLIT_TRANSACTION_ENDED",
        }
    }
}

impl std::fmt::Display for SplitTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(m) | Self::SnapshotExpired(m) | Self::TransactionEnded(m) => {
                write!(f, "{m}")
            }
        }
    }
}

impl std::error::Error for SplitTokenError {}

/// The identity portion of the AAD.
///
/// Mirrors the `(domain, principal)` convention the attach envelope already
/// uses: an unauthenticated caller gets a fixed anonymous tail, so it cannot
/// open an envelope sealed for a real principal.
pub fn identity_tail(auth: Option<(&str, &str)>) -> Vec<u8> {
    match auth {
        None => b"\x00anonymous".to_vec(),
        Some((domain, principal)) => {
            let mut out = Vec::with_capacity(2 + domain.len() + principal.len());
            out.push(0x01);
            out.extend_from_slice(domain.as_bytes());
            out.push(0x00);
            out.extend_from_slice(principal.as_bytes());
            out
        }
    }
}

/// Derive the 16-byte binding check for a bind call.
///
/// Minted **and** verified by the same worker, so it needs self-consistency
/// only — it does not have to agree with any client, which is why the cross-SDK
/// byte fixtures do not cover it. 16 bytes is a binding check, not a MAC:
/// forgery resistance comes from the seal where a key exists, and from the uid
/// trust boundary where one does not.
pub fn bind_fingerprint(
    schema_name: &str,
    function_name: &str,
    arguments: &[u8],
    settings: &[u8],
    projection: &[u8],
) -> [u8; FINGERPRINT_LEN] {
    let mut h = Sha256::new();
    h.update(AAD_PREFIX);
    let mut feed = |label: &[u8], value: &[u8]| {
        h.update(label);
        h.update([0u8]);
        h.update(value);
        h.update([0u8]);
    };
    feed(b"schema_name", schema_name.as_bytes());
    feed(b"function_name", function_name.as_bytes());
    feed(b"arguments", arguments);
    feed(b"settings", settings);
    feed(b"projection_ids", projection);
    let digest = h.finalize();
    let mut out = [0u8; FINGERPRINT_LEN];
    out.copy_from_slice(&digest[..FINGERPRINT_LEN]);
    out
}

/// AAD for a sealed split payload: the plaintext header plus the caller
/// identity. The identity half is load-bearing — it stops a token minted for one
/// principal being replayed by another, exactly as the attach envelope does. A
/// split token names data (files, offsets, tenant partitions), so dropping it
/// here while keeping it on attach would be a regression.
fn split_token_aad(header: &[u8], auth: Option<(&str, &str)>) -> Vec<u8> {
    let mut out = header.to_vec();
    out.extend_from_slice(&identity_tail(auth));
    out
}

/// Stamp (and, when a key exists, seal) a worker payload into a split token.
pub fn build_split_token(
    payload: &[u8],
    fingerprint: &[u8],
    anchor: &[u8],
    signing_key: Option<&[u8]>,
    auth: Option<(&str, &str)>,
) -> Result<Vec<u8>, SplitTokenError> {
    if fingerprint.len() != FINGERPRINT_LEN {
        return Err(SplitTokenError::Invalid(format!(
            "bind_fingerprint must be {FINGERPRINT_LEN} bytes, got {}",
            fingerprint.len()
        )));
    }
    if anchor.len() > u16::MAX as usize {
        return Err(SplitTokenError::Invalid(format!(
            "consistency_anchor too long: {} bytes exceeds u16",
            anchor.len()
        )));
    }

    let flags = if signing_key.is_some() {
        FLAG_PAYLOAD_SEALED
    } else {
        0
    };
    let mut body = Vec::with_capacity(HEADER_LEN + anchor.len() + payload.len());
    body.push(SPLIT_TOKEN_FORMAT_VERSION);
    body.push(flags);
    body.extend_from_slice(&(anchor.len() as u16).to_le_bytes());
    body.extend_from_slice(fingerprint);
    body.extend_from_slice(anchor);

    match signing_key {
        None => {
            body.extend_from_slice(payload);
            Ok(body)
        }
        Some(key) => {
            let aad = split_token_aad(&body, auth);
            let sealed = vgi_rpc::crypto::seal_bytes(payload, key, &aad, SEAL_VERSION);
            body.extend_from_slice(&sealed);
            Ok(body)
        }
    }
}

/// Verify a split token and return the worker's payload.
///
/// `expected_fingerprint` and `current_anchor` are optional; `None` skips that
/// check.
pub fn open_split_token(
    token: &[u8],
    signing_key: Option<&[u8]>,
    auth: Option<(&str, &str)>,
    expected_fingerprint: Option<&[u8]>,
    current_anchor: Option<&[u8]>,
) -> Result<Vec<u8>, SplitTokenError> {
    if token.len() < HEADER_LEN {
        return Err(SplitTokenError::Invalid(format!(
            "split token too short: {} bytes, need at least {HEADER_LEN}",
            token.len()
        )));
    }
    let version = token[0];
    let flags = token[1];
    let anchor_len = u16::from_le_bytes([token[2], token[3]]) as usize;

    if version != SPLIT_TOKEN_FORMAT_VERSION {
        return Err(SplitTokenError::Invalid(format!(
            "unsupported split-token format_version {version}; this worker speaks \
             {SPLIT_TOKEN_FORMAT_VERSION}"
        )));
    }
    if flags & RESERVED_FLAGS_MASK != 0 {
        return Err(SplitTokenError::Invalid(format!(
            "split token sets reserved flag bits (flags=0x{flags:02x})"
        )));
    }
    let sealed = flags & FLAG_PAYLOAD_SEALED != 0;

    // ---- The alg:none refusal. Load-bearing; do not relax. ----
    // `flags` is attacker-controlled plaintext, so it may say "not sealed" on a
    // token an attacker wrote by hand. A keyed worker that honoured that would
    // redeem forged work without ever opening an envelope. The WORKER'S OWN KEY
    // STATE decides, never the token.
    if signing_key.is_some() && !sealed {
        return Err(SplitTokenError::Invalid(
            "split token is unsealed but this worker holds a signing key; refusing. An \
             unsealed token cannot be authenticated, so accepting one here would let any \
             caller forge a split (alg:none)."
                .to_string(),
        ));
    }
    if signing_key.is_none() && sealed {
        return Err(SplitTokenError::Invalid(
            "split token is sealed but this worker holds no signing key; cannot open it"
                .to_string(),
        ));
    }

    let end_of_anchor = HEADER_LEN + anchor_len;
    if token.len() < end_of_anchor {
        return Err(SplitTokenError::Invalid(format!(
            "split token truncated: anchor_len={anchor_len} exceeds token length {}",
            token.len()
        )));
    }

    let fingerprint = &token[4..HEADER_LEN];
    let anchor = &token[HEADER_LEN..end_of_anchor];
    let body = &token[..end_of_anchor];
    let rest = &token[end_of_anchor..];

    if let Some(expected) = expected_fingerprint {
        if fingerprint != expected {
            return Err(SplitTokenError::Invalid(
                "split token was minted for a different bind (fingerprint mismatch)".to_string(),
            ));
        }
    }
    // Anchor check AFTER the bind check, and as its own kind: "read version N"
    // is a different situation from "this token is not yours".
    if let Some(current) = current_anchor {
        if anchor != current {
            return Err(SplitTokenError::SnapshotExpired(
                "split snapshot expired; re-run the query".to_string(),
            ));
        }
    }

    match signing_key {
        None => Ok(rest.to_vec()),
        Some(key) => {
            let aad = split_token_aad(body, auth);
            vgi_rpc::crypto::open_bytes(rest, key, &aad, SEAL_VERSION).map_err(|e| {
                SplitTokenError::Invalid(format!("split token failed authentication: {e}"))
            })
        }
    }
}

/// Encode the consistency anchor.
///
/// `catalog_version` is the counter that MOVES within an attach, so it is what a
/// plan is pinned to; `resolved_data_version` is fixed at attach and would say
/// nothing about staleness.
pub fn split_anchor(catalog_version: i64) -> Vec<u8> {
    catalog_version.to_le_bytes().to_vec()
}

// --- The author-facing plan result ----------------------------------------

/// What [`crate::table_function::TableFunction::on_plan`] returns.
///
/// A worker sets each split's `payload` and the scan-wide estimates; the
/// framework stamps the token envelope and serializes the wire form. Keeping the
/// envelope out of this type is the point: an author cannot forget the
/// consistency anchor, cannot mis-bind the fingerprint, and never writes crypto
/// — and the envelope stays a private implementation detail whose layout can
/// change without touching worker code in five languages.
#[derive(Debug, Clone, Default)]
pub struct PlanOutcome {
    /// One entry per unit of work. EMPTY is legal and means "no work": a
    /// fully-pruned scan reaches it, and the client must produce an empty result
    /// rather than an error.
    pub splits: Vec<PlannedSplit>,
    /// Continued enumeration. If more than one is returned they MUST partition
    /// the remaining enumeration disjointly and exhaustively. Clients deliberately
    /// do not deduplicate opaque or randomly sealed tokens, so violating this
    /// contract produces duplicate rows.
    pub next_cursors: Vec<Vec<u8>>,
    /// NORMATIVE cap on redemption concurrency, not advisory.
    pub max_workers: Option<i64>,
    pub estimated_total_splits: Option<i64>,
    pub estimated_total_rows: Option<i64>,
    pub estimated_total_bytes: Option<i64>,
    /// The counter a stale token is detected against. Also the anchor the
    /// framework stamps into every token in this plan.
    pub catalog_version: Option<i64>,
    /// `catalog` (the default) or `transaction`. A transaction-scoped plan is
    /// not cacheable and is not redeemable after commit or rollback.
    pub scope: Option<String>,
    /// Identifier for state shared by planning and every split redemption.
    /// Workers that set it must keep the corresponding state cross-process.
    pub execution_id: Option<Vec<u8>>,
    /// Opaque planning state echoed unchanged on every split init.
    pub init_opaque_data: Option<Vec<u8>>,
    /// Hoisted location names. Individual splits refer to these by index through
    /// [`PlannedSplit::location_ids`].
    pub locations: Option<Vec<String>>,
    /// How single-valued partition columns are derived. Report this only when
    /// every split has exact single-valued [`PlannedSplit::partition_bounds`].
    pub partitioning: Option<Vec<crate::protocol::dtos::PartitionTransform>>,
    /// Ordering guaranteed within each individual split.
    pub sort_order: Option<Vec<crate::protocol::dtos::SortField>>,
    pub cache_max_age_seconds: Option<i64>,
    /// Exclusive lower bound actually used for the plan's data range.
    pub start_position: Option<Vec<u8>>,
    /// Inclusive frontier resolved at planning time.
    pub end_position: Option<Vec<u8>>,
}

/// One planned unit of work, before the framework stamps its token.
#[derive(Debug, Clone, Default)]
pub struct PlannedSplit {
    /// The worker's own bytes NAMING this unit of work. "These three files at
    /// version 47" survives a retry; "rows 0-999 of whatever this returns now"
    /// does not.
    pub payload: Vec<u8>,
    pub estimated_rows: Option<i64>,
    /// True if `estimated_rows` is exact — unlocks COUNT(*) from statistics.
    pub rows_exact: bool,
    /// Load-bearing for engines that bin-pack; `None` degrades them to
    /// round-robin by count. A greedily-claiming client needs no cost model.
    pub estimated_bytes: Option<i64>,
    /// 2-row (min, max) batch in the existing `vgi_partition_values` encoding.
    pub partition_bounds: Option<Vec<u8>>,
    pub column_statistics: Option<Vec<u8>>,
    pub location_ids: Option<Vec<i64>>,
    pub start_position: Option<Vec<u8>>,
    /// `None` means UNBOUNDED — a shard read forever.
    pub end_position: Option<Vec<u8>>,
}

impl PlannedSplit {
    /// Attach a two-row `(min, max)` Arrow batch of partition bounds.
    pub fn with_partition_bounds(
        mut self,
        bounds: &arrow_array::RecordBatch,
    ) -> vgi_rpc::Result<Self> {
        self.partition_bounds = Some(crate::ipc::write_batch(bounds)?);
        Ok(self)
    }

    /// Attach optimizer column statistics using VGI's canonical IPC encoding.
    pub fn with_column_statistics(
        mut self,
        statistics: &[crate::statistics::CatColStat],
    ) -> vgi_rpc::Result<Self> {
        self.column_statistics = Some(crate::statistics::serialize_column_statistics(statistics)?);
        Ok(self)
    }
}

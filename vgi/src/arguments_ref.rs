// Copyright 2025, 2026 Query Farm LLC - https://query.farm

//! Argument references (protocol 3.4): resolving `BindRequest.arguments_location`.
//!
//! A client binding a function with a large argument over HTTP uploads the
//! arguments once, through this worker's `__upload_url__` grant, and sends a
//! `BindRequest` whose `arguments` is null and whose `arguments_location`
//! carries the grant's sealed `ref` plus the upload's size and SHA-256 (vgi
//! `docs/large-values-3.4.md` §2, §5a). Every request that embeds that bind
//! call (cardinality, statistics, plan, projection, dynamic-to-string and
//! init) carries the small location, never the value.
//!
//! The dispatcher resolves the location **at each turn's entry**, where a
//! [`CallContext`] exists, into a per-call view of the bind call
//! (`OpenedBindCall`): its `arguments` hold the resolved bytes and its
//! attach / transaction values are opened. The view is never encoded. What
//! goes back on the wire, into stream state (`init: StateRef`) and into
//! table-buffering storage is always the bind call as received: the
//! reference, never the value. vgi-rpc opens the sealed reference, keeps the
//! object cache (one `get` per object id per process, single-flight) and
//! checks size and SHA-256.
//!
//! A worker advertises support with the `VGI-Arguments-Ref-Bytes` capability
//! header ([`advertised_arguments_ref_bytes`]); without it the extension never
//! externalizes arguments.

use vgi_protocol::protocol::dtos::{ArgumentsLocation, BindRequest};
use vgi_rpc::{Bytes, CallContext, Code, Result, RpcError};

/// The capability header that tells the client this worker accepts argument
/// references, and above how many bytes of arguments IPC to use one.
pub const ARGUMENTS_REF_BYTES_HEADER: &str = "VGI-Arguments-Ref-Bytes";
/// Operator override of the advertised value.
pub const ARGUMENTS_REF_BYTES_ENV: &str = "VGI_ARGUMENTS_REF_BYTES";
/// The default advertised value: 4 MiB.
pub const DEFAULT_ARGUMENTS_REF_BYTES: u64 = 4 * 1024 * 1024;
/// Test switch: the HTTP server stops sending the header (the extension's
/// `arguments_ref_fallback.test`), behaving like a worker without argument
/// references while still offering upload URLs v2.
pub const DISABLE_ARGUMENTS_REF_ENV: &str = "VGI_TEST_DISABLE_ARGUMENTS_REF";

/// Kind of the refusal for an argument object storage reports gone.
pub const ARGUMENTS_REF_MISSING_KIND: &str = "arguments_ref_missing";
/// The exact message for it (large-values spec §5a, "Prepared statements"):
/// argument objects live about a day, so a prepared statement's large
/// argument can expire, and there is no transparent rebind.
pub const ARGUMENTS_REF_MISSING_MESSAGE: &str =
    "this prepared statement's large argument has expired; prepare it again";

const INVALID_ARGUMENTS_REF_KIND: &str = "invalid_arguments_ref";
const MAX_HEADER_DIGITS: usize = 19;

/// Parse a `VGI_ARGUMENTS_REF_BYTES` value: a positive decimal integer of
/// bytes, no sign, no leading zero, at most 19 digits (the header's grammar).
/// `None` (or blank) is the default.
///
/// # Errors
///
/// Any other value, so a typo stops startup instead of silently advertising
/// nothing.
pub fn parse_arguments_ref_bytes(raw: Option<&str>) -> std::result::Result<u64, String> {
    let raw = raw.map(str::trim).unwrap_or("");
    if raw.is_empty() {
        return Ok(DEFAULT_ARGUMENTS_REF_BYTES);
    }
    let parsed = (raw.len() <= MAX_HEADER_DIGITS
        && !raw.starts_with('0')
        && raw.bytes().all(|b| b.is_ascii_digit()))
    .then(|| raw.parse::<u64>().ok())
    .flatten();
    parsed.ok_or_else(|| {
        format!(
            "{ARGUMENTS_REF_BYTES_ENV} must be a positive integer number of bytes (no sign, no \
             leading zero, at most {MAX_HEADER_DIGITS} digits), got {raw:?}"
        )
    })
}

/// The `VGI-Arguments-Ref-Bytes` value an HTTP worker advertises, or `None`
/// when it advertises nothing.
///
/// The header is the mandatory capability signal (large-values spec §5): sent
/// whenever the worker vends upload URLs (`VGI-Upload-URL-Support: 2`) **and**
/// its storage can fetch what the grant minted back by object id (vgi-rpc
/// `ObjectStore`), and never otherwise. `VGI_TEST_DISABLE_ARGUMENTS_REF=1`
/// turns it off.
///
/// # Panics
///
/// On a malformed `VGI_ARGUMENTS_REF_BYTES` (see [`parse_arguments_ref_bytes`]).
pub fn advertised_arguments_ref_bytes(
    vends_upload_urls: bool,
    storage_fetches_by_id: bool,
) -> Option<u64> {
    if !vends_upload_urls {
        return None;
    }
    if !storage_fetches_by_id {
        log::warn!(
            "argument references disabled: the configured storage cannot fetch objects by id \
             (no ObjectStore)"
        );
        return None;
    }
    let disabled = std::env::var(DISABLE_ARGUMENTS_REF_ENV)
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    if disabled {
        return None;
    }
    let raw = std::env::var(ARGUMENTS_REF_BYTES_ENV).ok();
    Some(parse_arguments_ref_bytes(raw.as_deref()).unwrap_or_else(|e| panic!("{e}")))
}

/// Fetch the arguments an `arguments_location` names (large-values spec §5a,
/// steps 2-5): open the sealed reference under this request's principal and
/// check its size and SHA-256 equal the location's, look the object up in
/// vgi-rpc's object cache by `object_id`, fetch it once on a miss, verify
/// length and SHA-256.
///
/// # Errors
///
/// `invalid_arguments_ref` (`INVALID_ARGUMENT`) for a malformed location, a
/// reference that does not open or match, or a worker without storage;
/// `arguments_ref_integrity` (`DATA_LOSS`) for fetched bytes that differ;
/// `arguments_ref_missing` (`NOT_FOUND`) with the prepared-statement message
/// when storage reports the object gone. vgi-rpc's `storage_unavailable`
/// passes through.
pub fn fetch_arguments(location: &ArgumentsLocation, ctx: &CallContext) -> Result<Vec<u8>> {
    location.validate()?;
    ctx.resolve_argument_ref(&location.r#ref.0, location.size as u64, &location.sha256)
        .map(|data| data.to_vec())
        .map_err(refusal)
}

/// Shape a vgi-rpc argument-reference refusal for the client.
fn refusal(err: RpcError) -> RpcError {
    let kind = err.error_kind().to_string();
    match kind.as_str() {
        ARGUMENTS_REF_MISSING_KIND => RpcError::value_error(ARGUMENTS_REF_MISSING_MESSAGE)
            .with_status(Code::NotFound, ARGUMENTS_REF_MISSING_KIND),
        // Same kind and code; the message also names the kind, because a
        // client that shows only "<type>: <message>" (the DuckDB extension)
        // would otherwise never show the reason a caller can act on.
        INVALID_ARGUMENTS_REF_KIND | "arguments_ref_integrity"
            if !err.message.starts_with(kind.as_str()) =>
        {
            let code = if err.error_code() == "DATA_LOSS" {
                Code::DataLoss
            } else {
                Code::InvalidArgument
            };
            let message = format!("{kind}: {}", err.message);
            let mut out = err;
            out.message = message;
            out.with_status(code, kind)
        }
        _ => err,
    }
}

/// A bind call as the handlers see it: the per-call overlay beside the
/// raw-preserving `bind_call` wrapper (large-values spec §3.3 rule 11).
///
/// `view` is the decoded bind call with its sealed attach / transaction
/// values opened and its arguments resolved. It exists only in memory: it is
/// never encoded, so the plaintext and the resolved value never reach the
/// wire or a state token. Re-encode the held bytes instead.
#[derive(Debug, Clone)]
pub(crate) struct OpenedBindCall {
    /// The opened, resolved bind call (never encoded).
    pub view: BindRequest,
    /// Whether the peer predates `schema_path` (protocol < 1.1.0).
    pub legacy_peer: bool,
    /// The location the client sent, when the arguments travelled by
    /// reference (the split-token fingerprint hashes it, not the value).
    pub location: Option<ArgumentsLocation>,
}

impl OpenedBindCall {
    /// The resolved arguments IPC bytes.
    pub fn arguments(&self) -> &[u8] {
        self.view
            .arguments
            .as_ref()
            .map(|b| b.0.as_slice())
            .unwrap_or(&[])
    }

    /// The split-token fingerprint input for the arguments (large-values spec
    /// §5a, "Split-token fingerprints"): `0x01 || u64le(size) || sha256[32]`
    /// for a location (never the sealed `ref`, which holds a random nonce),
    /// `0x00 || u64le(len) ||` the IPC bytes for inline arguments. Plan and
    /// init carry the same bind-call bytes, so both sides take the same
    /// branch.
    pub fn fingerprint_arguments(&self) -> Vec<u8> {
        match &self.location {
            Some(location) => {
                let mut out = Vec::with_capacity(41);
                out.push(0x01);
                out.extend_from_slice(&(location.size as u64).to_le_bytes());
                out.extend_from_slice(&hex_digest(&location.sha256));
                out
            }
            None => {
                let raw = self.arguments();
                let mut out = Vec::with_capacity(9 + raw.len());
                out.push(0x00);
                out.extend_from_slice(&(raw.len() as u64).to_le_bytes());
                out.extend_from_slice(raw);
                out
            }
        }
    }
}

/// The 32 raw bytes of a validated 64-hex-digit SHA-256.
fn hex_digest(hex: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).take(32).enumerate() {
        let nibble = |c: u8| match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            _ => 0,
        };
        out[i] = nibble(chunk[0]) << 4 | nibble(*chunk.get(1).unwrap_or(&b'0'));
    }
    out
}

/// Resolve `dto`'s arguments in place: validate "exactly one of `arguments`
/// and `arguments_location`", and when the arguments travelled by reference,
/// fetch them through `ctx` and set `arguments` to the value. Returns the
/// location the client sent, if any.
pub(crate) fn resolve_in_place(
    dto: &mut BindRequest,
    ctx: &CallContext,
) -> Result<Option<ArgumentsLocation>> {
    dto.validate_arguments()?;
    let Some(location) = dto.arguments_location.clone() else {
        return Ok(None);
    };
    let data = fetch_arguments(&location, ctx)?;
    dto.arguments = Some(Bytes(data));
    Ok(Some(location))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arguments_ref_bytes_grammar() {
        assert_eq!(
            parse_arguments_ref_bytes(None),
            Ok(DEFAULT_ARGUMENTS_REF_BYTES)
        );
        assert_eq!(
            parse_arguments_ref_bytes(Some("  ")),
            Ok(DEFAULT_ARGUMENTS_REF_BYTES)
        );
        assert_eq!(parse_arguments_ref_bytes(Some("65536")), Ok(65536));
        for bad in ["0", "012", "-1", "+5", "1.5", "abc", "12345678901234567890"] {
            assert!(parse_arguments_ref_bytes(Some(bad)).is_err(), "{bad}");
        }
    }

    #[test]
    fn fingerprint_inputs_are_domain_separated() {
        let mut view = BindRequest {
            function_name: "f".into(),
            arguments: Some(Bytes(b"abc".to_vec())),
            function_type: vgi_rpc::DictString("TABLE".into()),
            input_schema: None,
            settings: None,
            secrets: None,
            attach_opaque_data: None,
            transaction_opaque_data: None,
            resolved_secrets_provided: false,
            at_unit: None,
            at_value: None,
            copy_from: None,
            copy_to: None,
            schema_path: None,
            argument_names: None,
            arguments_location: None,
        };
        let inline = OpenedBindCall {
            view: view.clone(),
            legacy_peer: false,
            location: None,
        };
        assert_eq!(inline.fingerprint_arguments(), b"\x00\x03\0\0\0\0\0\0\0abc");
        let sha = "ab".repeat(32);
        let location = ArgumentsLocation {
            r#ref: Bytes(vec![1, 2, 3]),
            size: 3,
            sha256: sha,
        };
        view.arguments_location = Some(location.clone());
        let by_ref = OpenedBindCall {
            view,
            legacy_peer: false,
            location: Some(location),
        };
        let fp = by_ref.fingerprint_arguments();
        assert_eq!(fp.len(), 41);
        assert_eq!(&fp[..9], b"\x01\x03\0\0\0\0\0\0\0");
        assert!(fp[9..].iter().all(|b| *b == 0xab));
    }
}
